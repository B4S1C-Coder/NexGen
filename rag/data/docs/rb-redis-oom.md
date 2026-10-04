---
title: "Runbook: redis-cache out of memory"
doc_id: "rb-redis-oom"
source_type: "runbook"
source_uri: "confluence://runbooks/redis-cache-oom"
authority_tier: "A"
created_at: "2026-08-01T00:00:00Z"
updated_at: "2026-08-01T00:00:00Z"
author: "ops-team"
---

# Runbook: redis-cache out of memory

OOM command not allowed means redis-cache reached maxmemory. Set the eviction policy to allkeys-lru and raise maxmemory. Flush session keys only if on-call approves.
