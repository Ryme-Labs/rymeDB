# RymeDB C# SDK

Thin `HttpClient` client over the rymeDB REST surface (net8+, no packages).
File: `src/RymeDB/RymeClient.cs`.

```csharp
var client = new RymeClient("http://127.0.0.1:8080", "ryme-dev-key");
Console.WriteLine(await client.RestListAsync("docs", "limit=10"));
```

Covers health, KV, SQL/COPY/EXPLAIN, Supabase-style REST, GraphQL, metering
and autoscale.
