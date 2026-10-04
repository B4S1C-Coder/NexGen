# NexGen RAG Service (port 8002)

Finds the runbooks, incident reports and chat threads relevant to a question and returns them as a
`KnowledgeResult`. See [../STUDY_GUIDE.md](../STUDY_GUIDE.md) for the design and
[tests/eval/RESULTS.md](tests/eval/RESULTS.md) for evaluation numbers.

```mermaid
flowchart LR
    subgraph Ingest["POST /ingest (offline)"]
        F[data/docs/*.md] --> C[connector] --> P[preprocessor<br/>chunk + tag IDs] --> Q[(Qdrant<br/>dense + BM25)]
    end
    subgraph Query["POST /knowledge"]
        K([question]) --> D[dense search] & S[BM25 search]
        D & S --> W[weighted rank fusion] --> R[cross-encoder re-rank] --> A[authority score]
        A --> N{NLI conflict?} -- yes --> B[LLM debate] --> X
        N -- no --> X[LLMLingua-2 compress<br/>+ restore technical IDs] --> O([KnowledgeResult])
    end
```

```bash
cd rag
uv sync
LLAMACPP_EMBED_SERVER_URL=http://localhost:11434 uv run uvicorn src.main:app --port 8002   # needs Qdrant + Ollama
uv run pytest            # unit + integration tests (mocked); the live eval is opt-in
scripts/eval.sh          # RAGAS-style evaluation -> tests/eval/RESULTS.md
```
