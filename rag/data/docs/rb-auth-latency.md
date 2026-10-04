---
title: "Runbook: auth-service slow token validation"
doc_id: "rb-auth-latency"
source_type: "runbook"
source_uri: "confluence://runbooks/auth-service-latency"
authority_tier: "A"
created_at: "2026-08-01T00:00:00Z"
updated_at: "2026-08-01T00:00:00Z"
author: "ops-team"
---

# Runbook: auth-service slow token validation

Gateway 504s on login usually mean auth-service workers are saturated or paused. Restart the stuck auth-service pods. Scale auth-service to 4 replicas and check heap usage.
