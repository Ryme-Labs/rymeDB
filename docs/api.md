---
slug: /api-guide
title: REST and GraphQL API
---

The [REST API reference](https://db.rymelabs.dev/api/) uses Scalar to render
the checked-in OpenAPI contract, with interactive requests, schemas, and
authorization controls.

The source contract is
[`schemas/openapi/rest.yaml`](https://github.com/Ryme-Labs/rymeDB/blob/main/schemas/openapi/rest.yaml).
The published artifacts are available as [OpenAPI YAML](https://db.rymelabs.dev/openapi/rest.yaml)
and [OpenAPI JSON](https://db.rymelabs.dev/openapi/rest.json).

## Authentication

Send `Authorization: Bearer <key>`, `x-api-key: <key>`, or `apikey: <key>`:

```bash
curl -fsS -H 'Authorization: Bearer local-dev-key' \
  http://127.0.0.1:8080/v1/scan/messages?limit=20
```

WebSocket clients may use `?api_key=` only with an `anon`-scoped key; other
credentials must be sent in headers and are rejected in URLs. Mutating,
operator, and archive routes require the appropriate principal. See
[authentication](auth.md).

## Main API groups

| Group | Examples |
| --- | --- |
| Health and monitoring | `/health`, `/ready`; `/metrics`, `/metrics/prometheus`, `/v1/audit` (platform superuser) |
| Data | `/v1/kv`, `/v1/sql`, `/v1/sql/copy`, `/v1/scan` |
| Supabase-style | `/rest/v1/{table}`, `/graphql` |
| Realtime | `/v1/stream`, `/v1/query-stream`, `/v1/broadcast`, `/v1/presence`, `/realtime/v1/websocket` |
| Search | `/v1/vector/*`, `/v1/text/*`, `/v1/index/stats` |
| Branches | `/v1/branches` and branch headers on data routes |
| Recovery | `/v1/backups/*`, `/v1/snapshots` |
| Operations | `/v1/cluster/*`, `/v1/ranges/*`, `/v1/shards/*`, `/v1/observe/*` |

`/health` and `/ready` are suitable for liveness/readiness probes. Metrics,
region status, autoscaling advice, traces, and other operational inspection
routes require an authenticated platform superuser. `/v1/cluster/members` also
works in standalone mode and reports `mode: "standalone"` with an empty member
list. `/v1/audit` returns the newest persisted authentication and administrative
events, including privileged SQL statement outcomes from HTTP and PostgreSQL
wire connections. Events include actor and tenant when available; raw SQL,
request bodies, and credentials are excluded.

The platform-superuser-only `/v1/index/stats` response includes process-wide
estimated memory use and limits for full-text and vector indexes, plus usage by
tenant. These are conservative index accounting estimates, not process RSS.

## Embedded admin dashboard

Open `http://127.0.0.1:8080/dashboard` (or the configured HTTP address) to use
the dashboard served by the database process. The static page itself is public;
dashboard API calls require an authenticated session. On a fresh data directory,
create the one-time platform administrator in the dashboard, or bootstrap it
through the API:

```bash
curl -fsS http://127.0.0.1:8080/v1/dashboard/setup
curl -fsS -X POST http://127.0.0.1:8080/v1/dashboard/bootstrap \
  -H 'Content-Type: application/json' \
  -d '{"id":"admin","password":"replace-with-a-long-password"}'
```

Unauthenticated bootstrap is allowed only from a loopback client address. Remote
bootstrap requests must authenticate as a platform superuser (for example,
with `Authorization: Bearer "$RYME_API_KEY"`). A TLS-protected public network
alone is not sufficient. The administrator can then sign in at `/dashboard`.
The dashboard's Observe page combines
`/metrics`, `/v1/ranges`, `/v1/ranges/loads`, `/v1/observe/slow`, `/v1/traces`,
and `/v1/audit`. Metrics, operational inspection, and audit data are restricted
to a platform superuser.

The operator CLI wraps these paths:

```bash
rymedb-cli --base http://127.0.0.1:8080 --api-key local-dev-key ready
rymedb-cli --api-key local-dev-key sql 'SELECT count(*) FROM messages'
rymedb-cli --api-key local-dev-key observe slow --limit 20
```

## Branch selection

Send `X-Ryme-Branch: preview` to HTTP SQL, KV, REST, GraphQL, and migration
routes. `main` is the default. WebSockets accept `branch=preview`; if both a
header and query parameter are present they must agree.
