---
title: "Runbook: application disk full"
doc_id: "rb-app-disk"
source_type: "runbook"
source_uri: "confluence://runbooks/app-disk-full"
authority_tier: "A"
created_at: "2026-08-01T00:00:00Z"
updated_at: "2026-08-01T00:00:00Z"
author: "ops-team"
---

# Runbook: application disk full

If a service such as payments logs No space left on device for its local volume, delete files older than 30 days under /data. Then expand the volume and alert the service owner.
