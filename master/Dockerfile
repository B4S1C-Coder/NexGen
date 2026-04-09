FROM python:3.12-slim AS base
WORKDIR /app

# Install deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl build-essential \
  && rm -rf /var/lib/apt/lists/*

COPY nexgen_shared/ /app/nexgen_shared/
COPY master/ /app/master/

RUN pip install --no-cache-dir -e /app/nexgen_shared \
  && pip install --no-cache-dir -e /app/master

WORKDIR /app/master

ENV MASTER_PORT=8000
EXPOSE 8000

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]