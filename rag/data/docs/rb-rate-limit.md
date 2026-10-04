---
title: "Runbook: auth-service token quota (HTTP 429)"
doc_id: "rb-rate-limit"
source_type: "runbook"
source_uri: "confluence://runbooks/auth-service-quota"
authority_tier: "A"
created_at: "2026-08-01T00:00:00Z"
updated_at: "2026-08-01T00:00:00Z"
author: "ops-team"
---

# Runbook: auth-service token quota (HTTP 429)

When auth-service returns 429 Too Many Requests, the per-client token quota is exhausted. Raise rate_limit.per_client in the auth-service config. Ask the calling team to batch token checks.
