# RymeDB Dart SDK

Dependency-free (`dart:io`) thin client over the rymeDB REST surface.
File: `lib/ryme_client.dart`.

```dart
final client = RymeClient(base: 'http://127.0.0.1:8080', apiKey: 'ryme-dev-key');
print(await client.restList('docs', 'limit=10'));
```

Covers health, KV, SQL/COPY/EXPLAIN, Supabase-style REST, GraphQL, metering
and autoscale.
