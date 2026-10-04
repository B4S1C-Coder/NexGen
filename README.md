# NexGen

Ask a plain-English question about a production incident, for example *"Why is payments returning 500s?"*,
and get a root-cause report backed by log evidence and runbooks.

| Service | Port | Job |
|---|---|---|
| [`master/`](master/) | 8000 | Routes the question, fetches logs + docs in parallel, finds and verifies the root cause |
| [`query/`](query/) | 8001 | Natural language → KQL → Elasticsearch → masked log lines |
| [`rag/`](rag/) | 8002 | Hybrid search over runbooks, re-ranking, conflict check, compression |
| [`nexgen_shared/`](nexgen_shared/) | — | Shared request/response models and error codes |

**Start here: [STUDY_GUIDE.md](STUDY_GUIDE.md).** It covers the architecture, a request walkthrough,
design trade-offs, results and interview Q&A.

Results:
- [master/eval/RESULTS.md](master/eval/RESULTS.md): Master benchmark.
- [tests/e2e/RESULTS.md](tests/e2e/RESULTS.md): full-stack run.
- [rag/tests/eval/RESULTS.md](rag/tests/eval/RESULTS.md): RAG evaluation.

```bash
cd master && cp .env.example .env && uv sync
MOCK_SERVICES=true uv run uvicorn src.main:app --port 8000   # Master alone, no other services needed
tests/e2e/run.sh --limit 3                                     # whole stack (Docker + Ollama + Groq key)
```
