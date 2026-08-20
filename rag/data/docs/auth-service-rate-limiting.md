---
title: "Auth Service — Rate Limiting Configuration"
doc_id: "runbook-auth-rate-limiting"
source_type: "runbook"
source_uri: "confluence://runbooks/auth-rate-limiting"
authority_tier: "A"
created_at: "2026-06-20T10:00:00Z"
updated_at: "2026-07-10T16:00:00Z"
author: "platform-team"
resolution_status: "resolved"
---

# Auth Service — Rate Limiting and Brute-Force Protection

## Overview

The auth service (service.name: auth, running on 10.0.3.20) enforces rate limits to prevent credential-stuffing attacks. When a client exceeds 10 failed login attempts within 5 minutes, the service returns HTTP 429 (Too Many Requests) with error code ERR_RATE_LIMIT_EXCEEDED.

## Configuration

Rate limits are stored in Redis (10.0.3.5:6379) under the key pattern `ratelimit:{client_ip}:{endpoint}`.

- **Default limit:** 100 requests/minute per IP for `/api/auth/login`.
- **Burst tolerance:** 20 additional requests (token bucket algorithm).
- **Lockout duration:** 15 minutes after 10 consecutive 401 responses.

## Troubleshooting

If legitimate users report being locked out:

1. Check Redis for the rate-limit key:
   ```bash
   redis-cli -h 10.0.3.5 GET "ratelimit:${CLIENT_IP}:/api/auth/login"
   ```

2. Clear the lockout manually:
   ```bash
   redis-cli -h 10.0.3.5 DEL "ratelimit:${CLIENT_IP}:/api/auth/login"
   ```

3. Review auth service logs for the client's trace_id to determine whether the requests were legitimate or attack traffic.

## Known Issues

- The rate limiter does not account for requests proxied through the CDN — all CDN traffic appears as a single IP (10.0.1.1). Fix is tracked in JIRA ticket AUTH-892.
