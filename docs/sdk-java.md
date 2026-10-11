# Java SDK

Artifact: `com.rymelabs:rymedb-client`. The client requires Java 17 or newer,
uses `java.net.http.HttpClient`, and returns JSON response text so applications
can choose Jackson, Gson, or JSON-B.

## Maven

```xml
<dependency>
  <groupId>com.rymelabs</groupId>
  <artifactId>rymedb-client</artifactId>
  <version>0.1.0</version>
</dependency>
```

The published Java artifact is currently distributed through the repository's
GitHub Maven registry. For a checkout:

```bash
mvn -f sdks/java/pom.xml verify
```

## Client and realtime

```java
var client = new RymeClient("http://127.0.0.1:8080", "local-dev-key");
System.out.println(client.ready().join());

var subscription = client.subscribeTable(
    "messages", "main", null, null,
    System.out::println
).join();

subscription.sendClose(java.net.http.WebSocket.NORMAL_CLOSURE, "done");
```

The client supports KV, SQL, REST CRUD, branches, migrations, backup helpers,
and WebSocket `subscribeTable`, `subscribeTableResumable`, `subscribeQuery`,
`subscribeBroadcast`, `subscribePresence`, `subscribeDurableTopic`, and
`subscribeSupabaseChannel`. Callbacks receive complete JSON frames.

For Supabase channels, configure broadcast, presence, and `postgres_changes`
subscriptions with `RymeClient.SupabaseChannelOptions`. Keep owner keys on the
server side and use the auth/tenant controls described in [auth](auth.md).
