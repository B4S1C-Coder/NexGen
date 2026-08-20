---
title: "Incident Report — Gateway Timeout Cascade (2026-08-05)"
doc_id: "incident-gw-timeout-20260805"
source_type: "runbook"
source_uri: "confluence://incidents/GW-TIMEOUT-20260805"
authority_tier: "A"
created_at: "2026-08-05T11:00:00Z"
updated_at: "2026-08-06T09:15:00Z"
author: "sre-team"
resolution_status: "resolved"
---

# Incident: Gateway Timeout Cascade — 2026-08-05

## Timeline

- **09:57 UTC** — `db-primary` (10.0.2.10) stops responding to TCP health checks.
- **09:57:30 UTC** — Payments service connection pool exhausted; first HTTP 500 logged (trace_id=abc123def456).
- **09:58 UTC** — API gateway (10.0.1.50) begins returning 502 for all `/api/payments/*` routes.
- **10:01 UTC** — On-call engineer paged via PagerDuty (incident INC-4821).
- **10:03 UTC** — Manual failover triggered to `db-replica-1` (10.0.2.11).
- **10:05 UTC** — Error rate returns to baseline (0.02%).

## Impact

- **Duration:** 8 minutes (09:57–10:05 UTC).
- **Affected services:** payments, checkout, refunds.
- **Error count:** 47 HTTP 500 responses.
- **Revenue impact:** Estimated $12,400 in delayed settlements.

## Root Cause

Identical to runbook `payments-db-failover`: IOPS exhaustion on `db-primary` during the morning batch settlement window caused PostgreSQL to stop accepting new connections. The payments service's connection pool (HikariCP, max 100) was saturated within 30 seconds.

## Action Items

- [x] Increase IOPS allocation on db-primary from 3000 to 6000.
- [x] Deploy TCP keepalive tuning (60s) to all service configs.
- [ ] Implement automatic failover via Patroni instead of manual ops-console procedure.
- [ ] Add circuit breaker to payments→db connection with 5s timeout.
