# rymeDB Rust SDK

Async Rust client for the rymeDB HTTP and WebSocket APIs. It uses `reqwest`,
`tokio-tungstenite`, `serde`, and Tokio and exposes typed errors while
retaining JSON values for flexible API responses.

```toml
[dependencies]
rymedb-client = "0.1"
```

For a local checkout:

```bash
cargo test --manifest-path sdks/rust/Cargo.toml --locked
```

Realtime subscriptions return a `RealtimeSubscription` whose `recv` method
returns complete JSON text frames and automatically answers server pings:

```rust,no_run
let mut stream = client
    .subscribe_table("messages", Some("main"), None, Some(42))
    .await?;
while let Some(frame) = stream.recv().await? {
    println!("{frame}");
}
```

`subscribe_query` and `subscribe_broadcast` are also available. HTTP and
WebSocket URLs automatically switch from `http`/`https` to `ws`/`wss`.
