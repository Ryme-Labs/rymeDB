# Schema contracts

These files are public compatibility contracts, but they do not all have the
same role:

| File | Status | Source of truth |
| --- | --- | --- |
| `openapi/rest.yaml` | Current | `crates/ryme-server/src/lib.rs` router and HTTP handlers |
| `json/cdc-event.schema.json` | Current | `ryme_realtime::ChangeRecord` serialization |
| `proto/ryme/v1/kv.proto` | Current | `crates/ryme-wire-grpc` gRPC service and generated code |
| `sql/system.sql` | Reference projection | JSON metadata snapshots under the configured data directory |

The REST contract is checked for route and component-reference drift by
`schemas/openapi/validate.py`. The gRPC crate compiles the canonical protobuf
file directly from this directory. The SQL file is intentionally not loaded
at startup: rymeDB's internal catalog is currently file-backed rather than a
set of user-visible system relations.
