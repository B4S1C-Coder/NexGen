#!/usr/bin/env bash
# Start NexGen Query (:8001) and Master (:8000) against the demo logs.
cd ~/NexGen
KEY=$(grep "^OPENAI_API_KEY=" master/.env | cut -d= -f2-)
(cd query && GROQ_API_KEY="$KEY" GROQ_MODEL=openai/gpt-oss-20b GENERATOR_MAX_TOKENS=2000 \
  setsid nohup ~/.local/bin/uv run uvicorn src.main:app --port 8001 > ~/logs/query.log 2>&1 < /dev/null &)
(cd master && MOCK_SERVICES=false OPENAI_MODEL_NAME=openai/gpt-oss-120b HTTP_TIMEOUT_SECONDS=90 \
  TOPOLOGY_CONFIG_PATH=$HOME/NexGen/tests/faults/topology.json REDIS_URL=redis://localhost:1/0 \
  setsid nohup ~/.local/bin/uv run uvicorn src.main:app --port 8000 > ~/logs/master.log 2>&1 < /dev/null &)
for p in 8001 8000; do for i in $(seq 1 90); do curl -sf localhost:$p/health >/dev/null && break; sleep 1; done; done
echo "query: $(curl -s localhost:8001/health)  master: $(curl -s localhost:8000/health)"
