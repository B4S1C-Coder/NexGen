#!/usr/bin/env bash
# End-to-end test of the whole stack. Usage: tests/e2e/run.sh [--limit N] [--delay SECONDS]
# Needs: Docker running, Ollama installed, and OPENAI_API_KEY (a Groq key) in master/.env.
# Starts Elasticsearch + Qdrant (Docker), Query (8001), RAG (8002) and Master (8000); stops them all at the end.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
LOGS="$ROOT/tests/e2e/logs"; mkdir -p "$LOGS"
KEY="$(grep '^OPENAI_API_KEY=' "$ROOT/master/.env" | cut -d= -f2-)"
MODEL="$(grep '^OPENAI_MODEL_NAME=' "$ROOT/master/.env" | cut -d= -f2-)"
[ -n "$KEY" ] || { echo "Set OPENAI_API_KEY in master/.env first"; exit 1; }

wait_for() {  # wait_for <name> <url> <seconds>
  for _ in $(seq 1 "$3"); do curl -sf "$2" >/dev/null 2>&1 && { echo "  $1 is up"; return; }; sleep 1; done
  echo "  $1 did not start; see $LOGS"; exit 1
}

PIDS=()
cleanup() {
  echo "Stopping services..."
  for pid in "${PIDS[@]}"; do pkill -P "$pid" 2>/dev/null || true; kill "$pid" 2>/dev/null || true; done
  docker compose -f "$ROOT/docker-compose.yml" -f "$ROOT/tests/e2e/compose.e2e.yml" stop elasticsearch qdrant >/dev/null
}
trap cleanup EXIT

echo "1/4 Infrastructure"
docker compose -f "$ROOT/docker-compose.yml" -f "$ROOT/tests/e2e/compose.e2e.yml" up -d elasticsearch qdrant
pgrep -x ollama >/dev/null || (ollama serve >"$LOGS/ollama.log" 2>&1 &)
wait_for Elasticsearch http://localhost:9200 120
wait_for Qdrant http://localhost:6333/healthz 60
wait_for Ollama http://localhost:11434/api/tags 30
ollama pull nomic-embed-text >/dev/null

echo "2/4 Test data"
(cd "$ROOT/master" && uv run python ../tests/e2e/e2e.py setup)

echo "3/4 Services (RAG downloads its models on the first run, this can take a few minutes)"
(cd "$ROOT/query" && GROQ_API_KEY="$KEY" GROQ_MODEL="$MODEL" GENERATOR_MAX_TOKENS=2000 \
  uv run uvicorn src.main:app --port 8001 >"$LOGS/query.log" 2>&1) & PIDS+=($!)
(cd "$ROOT/rag" && LLAMACPP_EMBED_SERVER_URL=http://localhost:11434 \
  OLLAMA_BASE_URL=https://api.groq.com/openai DEBATE_LLM_MODEL="$MODEL" DEBATE_LLM_API_KEY="$KEY" \
  DEBATE_LLM_MAX_TOKENS=2000 uv run uvicorn src.main:app --port 8002 >"$LOGS/rag.log" 2>&1) & PIDS+=($!)
(cd "$ROOT/master" && MOCK_SERVICES=false uv run uvicorn src.main:app --port 8000 >"$LOGS/master.log" 2>&1) & PIDS+=($!)
wait_for Query http://localhost:8001/health 120
wait_for RAG http://localhost:8002/health 900
wait_for Master http://localhost:8000/health 60

echo "4/4 Incidents"
(cd "$ROOT/master" && uv run python ../tests/e2e/e2e.py run "$@")
