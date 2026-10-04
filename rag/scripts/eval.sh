#!/usr/bin/env bash
# RAGAS evaluation of the RAG service. Usage: rag/scripts/eval.sh
# Needs Docker, Ollama and an LLM key: EVAL_LLM_API_KEY, or else OPENAI_API_KEY from master/.env (Groq).
# Starts Qdrant + Ollama + RAG, runs tests/eval/test_ragas.py, writes tests/eval/RESULTS.md, stops everything.
set -euo pipefail
RAG="$(cd "$(dirname "$0")/.." && pwd)"; ROOT="$(dirname "$RAG")"
KEY="${EVAL_LLM_API_KEY:-$(grep '^OPENAI_API_KEY=' "$ROOT/master/.env" 2>/dev/null | cut -d= -f2-)}"
[ -n "$KEY" ] || { echo "Set EVAL_LLM_API_KEY (or OPENAI_API_KEY in master/.env)"; exit 1; }

cleanup() { kill "${RAG_PID:-}" 2>/dev/null || true; pkill -f 'uvicorn src.main:app --port 8002' 2>/dev/null || true
            docker compose -f "$ROOT/docker-compose.yml" stop qdrant >/dev/null; }
trap cleanup EXIT

docker compose -f "$ROOT/docker-compose.yml" up -d qdrant
pgrep -x ollama >/dev/null || (ollama serve >/dev/null 2>&1 &)
until curl -sf http://localhost:11434/api/tags >/dev/null; do sleep 1; done
ollama pull nomic-embed-text >/dev/null
cd "$RAG"
LLAMACPP_EMBED_SERVER_URL=http://localhost:11434 uv run uvicorn src.main:app --port 8002 >/tmp/nexgen-rag-eval.log 2>&1 &
RAG_PID=$!
until curl -sf http://localhost:8002/health >/dev/null; do sleep 2; done
PYTHONUNBUFFERED=1 RUN_RAGAS=1 EVAL_LLM_API_KEY="$KEY" uv run pytest tests/eval/test_ragas.py -s -q -p no:cacheprovider
