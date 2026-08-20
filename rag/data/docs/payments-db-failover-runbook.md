---
title: "Payments Service — Database Failover Runbook"
doc_id: "runbook-payments-db-failover"
source_type: "runbook"
source_uri: "confluence://runbooks/payments-db-failover"
authority_tier: "A"
created_at: "2026-07-15T08:00:00Z"
updated_at: "2026-08-01T14:30:00Z"
author: "ops-team"
resolution_status: "resolved"
---

# Payments Service — Database Failover Procedure

## Symptoms

- HTTP 500 errors spike on the payments service (service.name: payments).
- Log message: `Connection refused: db-primary:5432` with trace_id=abc123def456.
- Error code ERR_DB_CONN_REFUSED appears in structured logs.
- Upstream gateway (10.0.1.50) receives 502 responses from payments (10.0.2.30).

## Root Cause

The primary PostgreSQL instance (`db-primary`, IP 10.0.2.10) becomes unreachable due to:

1. TCP keepalive timeout (300s default) not tuned for the VPC.
2. Disk IOPS exhaustion on the primary during peak batch settlement (09:00–10:00 UTC).
3. Connection pool saturation (`max_connections=100`, 98 active during incident).

## Resolution Steps

1. **Verify primary health:**
   ```bash
   ssh ops@10.0.2.10 "systemctl status postgresql"
   ```

2. **Check connection pool saturation:**
   ```sql
   SELECT count(*) FROM pg_stat_activity WHERE state = 'active';
   ```

3. **Trigger manual failover** to db-replica-1 via ops-console:
   ```bash
   nexgen-ops failover --target db-replica-1 --service payments
   ```

4. **Update DNS** to point `db-primary.internal` to the new leader (10.0.2.11).

5. **Validate recovery:** Monitor `nexgen_payments_error_rate` metric — should drop below 0.1% within 2 minutes.

## Post-Incident

- Tune TCP keepalive to 60s across all database clients.
- Increase `max_connections` to 200 on both primary and replica.
- Add alerting rule: fire PagerDuty when active connections exceed 80% capacity.
