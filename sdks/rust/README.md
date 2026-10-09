# rymeDB Rust SDK

Async Rust client for the rymeDB HTTP API. It uses `reqwest`, `serde`, and
Tokio and exposes typed errors while retaining JSON values for flexible API
responses.

```toml
[dependencies]
rymedb-client = "0.1"
```

For a local checkout:

```bash
cargo test --manifest-path sdks/rust/Cargo.toml --locked
```
