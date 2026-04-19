FROM python:3.12-slim AS base
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl build-essential \
  && rm -rf /var/lib/apt/lists/*

COPY nexgen_shared/ /app/nexgen_shared/
COPY rag/ /app/rag/

RUN pip install --no-cache-dir -e /app/nexgen_shared \
  && pip install --no-cache-dir -e /app/rag

WORKDIR /app/rag

ENV RAG_PORT=8002
EXPOSE 8002

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8002"]
