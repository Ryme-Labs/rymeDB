# rymeDB Java SDK

The Java SDK is a dependency-free HTTP client for the rymeDB REST, SQL,
branching, vector, auth, and realtime endpoints. It requires Java 17 or newer.

```xml
<dependency>
  <groupId>com.rymelabs</groupId>
  <artifactId>rymedb-client</artifactId>
  <version>0.1.0</version>
</dependency>
```

For a local checkout:

```bash
mvn -f sdks/java/pom.xml verify
```

The client returns JSON response bodies as strings, so applications can use
their existing Jackson, Gson, or JSON-B setup. Set `RYME_API_KEY` or pass the
API key to `new RymeClient(baseUrl, apiKey)`.

REST CRUD supports both the legacy key/value overload and ordinary JSON
objects or arrays: `restInsert`, `restUpsert`, `restUpdate(table, query,
json)`, and `restDeleteWhere(table, query)`.

Java 17's built-in WebSocket client is used for realtime subscriptions:

```java
var subscription = client.subscribeTable("messages", "main", null, null,
    frame -> System.out.println(frame)).join();
subscription.sendClose(WebSocket.NORMAL_CLOSURE, "done");
```

`subscribeTable` accepts `from` and `fromSequence` replay cursors. The same
client also exposes `subscribeQuery`, `subscribeBroadcast`, and
`subscribePresence`; presence subscriptions begin with a snapshot and then
deliver join/leave frames. `presenceJoin` also accepts raw JSON state and an
optional TTL for presence refreshes. Callbacks receive complete JSON text
frames.

For Supabase-compatible channels, use `subscribeSupabaseChannel`. It supports
the broadcast and `postgres_changes` join configuration while keeping callback
frames as raw JSON so applications can use their existing JSON library:

```java
var channel = client.subscribeSupabaseChannel(
    "room",
    new RymeClient.SupabaseChannelOptions(true, false,
        List.of(new RymeClient.SupabasePostgresChange("INSERT", "public", "messages", null, null))),
    System.out::println).join();
channel.sendBroadcast("typing", "{\"user\":\"ada\"}").join();
channel.close();
```

For a table subscription that reconnects automatically and advances its
sequence cursor, use `subscribeTableResumable`. The returned
`RealtimeSubscription` implements `AutoCloseable` and stops retrying when
`close()` is called.
