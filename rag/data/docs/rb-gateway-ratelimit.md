---
title: "Runbook: gateway global rate limit"
doc_id: "rb-gateway-ratelimit"
source_type: "runbook"
source_uri: "confluence://runbooks/gateway-rate-limit"
authority_tier: "A"
created_at: "2026-08-01T00:00:00Z"
updated_at: "2026-08-01T00:00:00Z"
author: "ops-team"
---

# Runbook: gateway global rate limit

If gateway rejects many requests with 429, check rate_limit.global in the gateway config; the production value is 500rps. Revert a bad config with configctl rollback gateway.
