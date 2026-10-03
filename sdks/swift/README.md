# RymeDB Swift SDK

Thin `URLSession` client over the rymeDB REST surface. Requires Swift 5.9+.

```swift
let client = RymeClient(base: "http://127.0.0.1:8080", apiKey: "ryme-dev-key")
let rows = try await client.restList(table: "docs", query: "limit=10")
```

File: `Sources/RymeDB/RymeClient.swift` (part of `sdks/swift`, no dependencies).
Covers health, KV, SQL/COPY/EXPLAIN, Supabase-style REST, GraphQL, metering
and autoscale.
