# RymeDB Kotlin SDK

Zero-dependency (`java.net.HttpURLConnection`) thin client over the rymeDB
REST surface. Package `rymedb`, file
`src/main/kotlin/rymedb/RymeClient.kt`.

```kotlin
val client = RymeClient("http://127.0.0.1:8080", "ryme-dev-key")
println(client.restList("docs", "limit=10"))
```

Covers health, KV, SQL/COPY/EXPLAIN, Supabase-style REST, GraphQL, metering
and autoscale.
