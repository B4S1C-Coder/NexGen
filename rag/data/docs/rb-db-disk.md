---
title: "Runbook: db-primary disk full"
doc_id: "rb-db-disk"
source_type: "runbook"
source_uri: "confluence://runbooks/db-primary-disk-full"
authority_tier: "A"
created_at: "2026-08-01T00:00:00Z"
updated_at: "2026-08-01T00:00:00Z"
author: "ops-team"
---

# Runbook: db-primary disk full

When postgres logs No space left on device, remove old WAL archives with pg_archivecleanup. Then expand the data volume; writes resume once usage is below 85%.
