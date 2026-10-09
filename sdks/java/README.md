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

Java 17's built-in WebSocket client is used for realtime subscriptions:

```java
var subscription = client.subscribeTable("messages", "main", null, null,
    frame -> System.out.println(frame)).join();
subscription.sendClose(WebSocket.NORMAL_CLOSURE, "done");
```

`subscribeTable` accepts `from` and `fromSequence` replay cursors. The same
client also exposes `subscribeQuery` and `subscribeBroadcast`; callbacks
receive complete JSON text frames.
