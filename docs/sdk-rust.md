# Rust SDK

Crate: `rymedb-client`. It is an async client for the HTTP and WebSocket
interfaces and is independent from the server workspace.

## Install

```toml
[dependencies]
rymedb-client = "0.1"
```

For a checkout:

```bash
cargo test --manifest-path sdks/rust/Cargo.toml --locked
```

## HTTP and realtime

```rust,no_run
use rymedb_client::RymeClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = RymeClient::new(
        "http://127.0.0.1:8080".into(),
        Some("local-dev-key".into()),
    );
    let ready = client.ready().await?;
    println!("{ready:?}");

    let mut stream = client
        .subscribe_table("messages", Some("main"), None, None)
        .await?;
    while let Some(frame) = stream.recv().await? {
        println!("{frame}");
    }
    Ok(())
}
```

`RymeClient` covers KV, SQL, REST CRUD, branches, backups, migrations,
observability, placement, and health endpoints. WebSocket subscriptions return
complete JSON frames and answer server pings.

Available realtime methods include `subscribe_table`,
`subscribe_table_resumable`, `subscribe_query`, `subscribe_broadcast`,
`subscribe_presence`, `subscribe_durable_topic`, and
`subscribe_supabase_channel`. HTTP and WebSocket URL schemes are converted
automatically (`http` to `ws`, `https` to `wss`).

Use `serde_json::Value` for flexible response bodies and handle `40001`,
cursor expiry, authentication, and transport errors explicitly.
