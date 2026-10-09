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

For automatic reconnects with exact sequence-based replay, use
`subscribe_table_resumable`. Its `recv` method keeps retrying with bounded
backoff until the subscription is closed or the retry limit is reached.

`subscribe_query`, `subscribe_broadcast`, and `subscribe_presence` are also
available. Presence streams begin with a state snapshot followed by live
join/leave events. HTTP and
WebSocket URLs automatically switch from `http`/`https` to `ws`/`wss`.

Supabase-style REST CRUD is available through `rest_insert`, `rest_upsert`,
`rest_update`, and `rest_delete_where`, using `serde_json::Value` request
bodies and PostgREST query strings.
