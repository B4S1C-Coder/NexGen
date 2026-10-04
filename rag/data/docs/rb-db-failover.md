---
title: "Runbook: db-primary connection failures"
doc_id: "rb-db-failover"
source_type: "runbook"
source_uri: "confluence://runbooks/db-primary-failover"
authority_tier: "A"
created_at: "2026-08-01T00:00:00Z"
updated_at: "2026-08-01T00:00:00Z"
author: "ops-team"
---

# Runbook: db-primary connection failures

If services report connection refused to db-primary:5432, check postgres with systemctl status postgresql on db-primary. If the primary is down, promote db-replica-1 via ops-console and restart the payments and orders connection pools.
