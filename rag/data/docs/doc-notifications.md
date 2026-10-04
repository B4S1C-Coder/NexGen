---
title: "Thread: notifications email retries"
doc_id: "doc-notifications"
source_type: "slack"
source_uri: "slack://ops/notifications-retries"
authority_tier: "B"
created_at: "2026-08-01T00:00:00Z"
updated_at: "2026-08-01T00:00:00Z"
author: "ops-team"
---

# Thread: notifications email retries

notifications retries SMTP delivery 5 times with exponential backoff. Failed emails go to the dead-letter queue.
