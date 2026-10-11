---
slug: /api-guide
title: REST and GraphQL API
---

The dedicated [REST/OpenAPI explorer](https://db.rymelabs.dev/api/) is a
standalone Swagger UI for the checked-in OpenAPI contract. It is deliberately
separate from this documentation site so the API explorer gets the full
viewport and Swagger's native request, schema, and authorization controls.

The source contract is
[`schemas/openapi/rest.yaml`](https://github.com/Ryme-Labs/rymeDB/blob/main/schemas/openapi/rest.yaml).
The published artifacts are available as [OpenAPI YAML](https://db.rymelabs.dev/openapi/rest.yaml)
and [OpenAPI JSON](https://db.rymelabs.dev/openapi/rest.json).

## Authentication

Send `Authorization: Bearer <key>` or `x-api-key: <key>`:

```bash
curl -fsS -H 'Authorization: Bearer local-dev-key' \
  http://127.0.0.1:8080/v1/scan/messages?limit=20
```

WebSocket clients may use `?api_key=`. Mutating, operator, and archive routes
require the appropriate principal. See [authentication](auth.md).

## Main API groups

| Group | Examples |
| --- | --- |
| Health | `/health`, `/ready`, `/metrics`, `/metrics/prometheus` |
| Data | `/v1/kv`, `/v1/sql`, `/v1/sql/copy`, `/v1/scan` |
| Supabase-style | `/rest/v1/{table}`, `/graphql` |
| Realtime | `/v1/stream`, `/v1/query-stream`, `/v1/broadcast`, `/v1/presence`, `/realtime/v1/websocket` |
| Search | `/v1/vector/*`, `/v1/text/*`, `/v1/index/stats` |
| Branches | `/v1/branches` and branch headers on data routes |
| Recovery | `/v1/backups/*`, `/v1/snapshots` |
| Operations | `/v1/cluster/*`, `/v1/ranges/*`, `/v1/shards/*`, `/v1/observe/*` |

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
