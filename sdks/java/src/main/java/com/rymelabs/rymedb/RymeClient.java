package com.rymelabs.rymedb;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.WebSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Small, dependency-free Java client for the rymeDB HTTP API.
 *
 * <p>Methods return the server's JSON response as a string so applications can
 * choose Jackson, Gson, or another JSON library without pulling one into the
 * SDK. The client uses {@link HttpClient} and is safe to reuse across threads.
 */
public final class RymeClient {
    private static final ScheduledExecutorService REALTIME_RECONNECTOR =
            Executors.newScheduledThreadPool(1, runnable -> {
                Thread thread = new Thread(runnable, "rymedb-realtime-reconnect");
                thread.setDaemon(true);
                return thread;
            });
    private static final Pattern SEQUENCE_FIELD = Pattern.compile("\\\"sequence\\\"\\s*:\\s*(\\d+)");
    private final String base;
    private final String apiKey;
    private final HttpClient http;

    public RymeClient(String base) {
        this(base, null);
    }

    public RymeClient(String base, String apiKey) {
        this.base = Objects.requireNonNull(base, "base").replaceAll("/+$", "");
        String environmentKey = System.getenv("RYME_API_KEY");
        this.apiKey = apiKey == null || apiKey.isEmpty()
                ? (environmentKey == null ? "" : environmentKey)
                : apiKey;
        this.http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(30))
                .build();
    }

    public boolean health() {
        try {
            HttpRequest request = HttpRequest.newBuilder(uri("/health"))
                    .timeout(Duration.ofSeconds(5))
                    .GET()
                    .build();
            return http.send(request, HttpResponse.BodyHandlers.discarding()).statusCode() / 100 == 2;
        } catch (IOException e) {
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    public String kvGet(String table, String key) {
        return request("GET", "/v1/kv/" + segment(table) + "/" + segment(key), null);
    }

    public String kvPut(String table, String key, String value) {
        return kvPut(table, key, value, null);
    }

    public String kvPut(String table, String key, String value, Integer ttl) {
        String suffix = ttl == null ? "" : "?ttl=" + ttl;
        return request("PUT", "/v1/kv/" + segment(table) + "/" + segment(key) + suffix, value);
    }

    public String kvDelete(String table, String key) {
        return request("DELETE", "/v1/kv/" + segment(table) + "/" + segment(key), null);
    }

    public String sql(String statement) {
        return request("POST", "/v1/sql", "{\"sql\":" + json(statement) + "}");
    }

    public String sqlCopy(String table, List<CopyRow> rows) {
        String values = rows.stream()
                .map(row -> "{\"key\":" + json(row.key()) + ",\"value\":" + json(row.value()) + "}")
                .collect(Collectors.joining(","));
        return request("POST", "/v1/sql/copy", "{\"table\":" + json(table) + ",\"rows\":[" + values + "]}");
    }

    public String sqlExplain(String statement) {
        return request("POST", "/v1/sql/explain", "{\"sql\":" + json(statement) + "}");
    }

    public String restList(String table) {
        return restList(table, "");
    }

    public String restList(String table, String query) {
        return request("GET", "/rest/v1/" + segment(table) + (query == null || query.isEmpty() ? "" : "?" + query), null);
    }

    public String restInsert(String table, String key, String value) {
        return request("POST", "/rest/v1/" + segment(table),
                "{\"key\":" + json(key) + ",\"value\":" + json(value) + "}");
    }

    public String restDelete(String table, String key) {
        return request("DELETE", "/rest/v1/" + segment(table) + "?key=eq." + encode(key), null);
    }

    public String graphql(String query) {
        return request("POST", "/graphql", "{\"query\":" + json(query) + "}");
    }

    public String metering() { return request("GET", "/v1/metering", null); }

    public String autoscale() { return autoscale(""); }

    public String autoscale(String query) {
        return request("GET", "/v1/autoscale" + querySuffix(query), null);
    }

    public String slowLog(Integer limit, String table) {
        List<String> params = new ArrayList<>();
        if (limit != null) params.add("limit=" + limit);
        if (table != null) params.add("table=" + encode(table));
        return request("GET", "/v1/observe/slow" + queryParams(params), null);
    }

    public String traces(Integer limit, String name, String table) {
        List<String> params = new ArrayList<>();
        if (limit != null) params.add("limit=" + limit);
        if (name != null) params.add("name=" + encode(name));
        if (table != null) params.add("table=" + encode(table));
        return request("GET", "/v1/traces" + queryParams(params), null);
    }

    public String branchList() { return request("GET", "/v1/branches", null); }

    public String branchReset(String id, long baseCommitTs) {
        return request("POST", "/v1/branches/" + segment(id) + "/reset", "{\"base_commit_ts\":" + baseCommitTs + "}");
    }

    public String branchPromote(String id) {
        return request("POST", "/v1/branches/" + segment(id) + "/promote", null);
    }

    public String billingSummary() { return request("GET", "/v1/billing/summary", null); }

    public String vectorAnnSearch(String table, List<Double> vector, int topK, int ef) {
        return request("POST", "/v1/vector/ann-search", "{\"table\":" + json(table)
                + ",\"vector\":[" + numbers(vector) + "],\"top_k\":" + topK + ",\"ef\":" + ef + "}");
    }

    public String vectorUpsert(String table, String id, List<Double> vector) {
        return request("POST", "/v1/vector/upsert", "{\"table\":" + json(table)
                + ",\"id\":" + json(id) + ",\"vector\":[" + numbers(vector) + "]}");
    }

    public String vectorSearch(String table, List<Double> vector, int topK) {
        return request("POST", "/v1/vector/search", "{\"table\":" + json(table)
                + ",\"vector\":[" + numbers(vector) + "],\"top_k\":" + topK + "}");
    }

    public String textIndex(String table, String id, String text) {
        return request("POST", "/v1/text/index", "{\"table\":" + json(table)
                + ",\"id\":" + json(id) + ",\"text\":" + json(text) + "}");
    }

    public String textSearch(String table, String query, int topK) {
        return request("POST", "/v1/text/search", "{\"table\":" + json(table)
                + ",\"query\":" + json(query) + ",\"top_k\":" + topK + "}");
    }

    public String oidcLogin(String redirectUri, String state) {
        return request("POST", "/v1/auth/oidc/login", "{\"redirect_uri\":" + json(redirectUri)
                + ",\"state\":" + nullableJson(state) + "}");
    }

    public String oidcToken(String idToken) {
        return request("POST", "/v1/auth/oidc/token", "{\"id_token\":" + json(idToken) + "}");
    }

    public String migrateSupabase(String dump) {
        return request("POST", "/v1/migrate/supabase", "{\"dump\":" + json(dump) + "}");
    }

    public String backupVerify(String backupId) {
        return request("GET", "/v1/backups/verify?backup_id=" + encode(backupId), null);
    }

    public String backupDrill() {
        return request("GET", "/v1/backups/drill", null);
    }

    public String backupCopy(String backupId) {
        return request("POST", "/v1/backups/copy", "{\"backup_id\":" + json(backupId) + "}");
    }

    public String checkpoint() {
        return checkpoint(null);
    }

    public String checkpoint(String manifestId) {
        return request("POST", "/v1/backups/checkpoint", "{\"manifest_id\":" + nullableJson(manifestId) + "}");
    }

    public String snapshot() {
        return request("POST", "/v1/snapshots", null);
    }

    public String latestCheckpoint() {
        return request("GET", "/v1/backups/latest", null);
    }

    public String pitr(long target) {
        return request("GET", "/v1/backups/pitr?target=" + target, null);
    }

    public String restore(long target) {
        return request("POST", "/v1/backups/restore?target=" + target, null);
    }

    public String archive() {
        return archive(null);
    }

    public String archive(String backupId) {
        return request("POST", "/v1/backups/archive", "{\"backup_id\":" + nullableJson(backupId) + "}");
    }

    public String archives() {
        return request("GET", "/v1/backups/archives", null);
    }

    public String shardLayout() {
        return request("GET", "/v1/shards", null);
    }

    public String shardMove(String table, Integer target) {
        return request("POST", "/v1/shards/move", "{\"table\":" + json(table)
                + ",\"target\":" + (target == null ? "null" : target) + "}");
    }

    public String ranges() {
        return ranges(null);
    }

    public String ranges(String key) {
        return request("GET", "/v1/ranges" + (key == null ? "" : "?key=" + encode(key)), null);
    }

    public String rangeSplit(String id, String mid, String leftId, String rightId, long expectedEpoch) {
        return request("POST", "/v1/ranges/split", "{\"id\":" + json(id) + ",\"mid\":" + json(mid)
                + ",\"left_id\":" + json(leftId) + ",\"right_id\":" + json(rightId)
                + ",\"expected_epoch\":" + expectedEpoch + "}");
    }

    public String rangeMerge(String leftId, String rightId, String mergedId,
                             long expectedLeftEpoch, long expectedRightEpoch) {
        return request("POST", "/v1/ranges/merge", "{\"left_id\":" + json(leftId)
                + ",\"right_id\":" + json(rightId) + ",\"merged_id\":" + json(mergedId)
                + ",\"expected_left_epoch\":" + expectedLeftEpoch
                + ",\"expected_right_epoch\":" + expectedRightEpoch + "}");
    }

    public String rangeAutosplit() {
        return rangeAutosplit(null);
    }

    public String rangeAutosplit(Long minWrites) {
        return request("POST", "/v1/ranges/autosplit"
                + (minWrites == null ? "" : "?min_writes=" + minWrites), null);
    }

    public String rangeLoads() {
        return request("GET", "/v1/ranges/loads", null);
    }

    public String migrateNeon(String branches) {
        return request("POST", "/v1/migrate/neon", "{\"branches\":" + json(branches) + "}");
    }

    public String migrateApply(String id, String sql, String author) {
        return request("POST", "/v1/migrate/apply", "{\"id\":" + json(id)
                + ",\"sql\":" + json(sql) + ",\"author\":" + nullableJson(author) + "}");
    }

    public String migrateLedger() {
        return request("GET", "/v1/migrate/ledger", null);
    }

    public String indexStats() {
        return request("GET", "/v1/index/stats", null);
    }

    public String billingInvoice() {
        return billingInvoice("");
    }

    public String billingInvoice(String tenant) {
        return request("GET", "/v1/billing/invoice"
                + (tenant == null || tenant.isEmpty() ? "" : "?tenant=" + encode(tenant)), null);
    }

    public String regions() {
        return request("GET", "/v1/regions", null);
    }

    public String authRegister(String id, String password) {
        return request("POST", "/v1/auth/register", "{\"id\":" + json(id) + ",\"password\":" + json(password) + "}");
    }

    public String authToken(String id, String password, String code) {
        return request("POST", "/v1/auth/token", "{\"id\":" + json(id) + ",\"password\":" + json(password)
                + ",\"code\":" + nullableJson(code) + "}");
    }

    public String authRevoke(String key) {
        return request("DELETE", "/v1/auth/keys", "{\"key\":" + json(key) + "}");
    }

    public String passkeyRegister(String user, String credentialId, String publicKey) {
        return request("POST", "/v1/auth/passkey/register", "{\"user\":" + json(user)
                + ",\"credential_id\":" + json(credentialId) + ",\"public_key\":" + json(publicKey) + "}");
    }

    public String passkeyVerify(String user, String credentialId, String authenticatorData,
                                String clientDataJson, String signature) {
        return request("POST", "/v1/auth/passkey/verify", "{\"user\":" + json(user)
                + ",\"credential_id\":" + json(credentialId)
                + ",\"authenticator_data\":" + json(authenticatorData)
                + ",\"client_data_json\":" + json(clientDataJson)
                + ",\"signature\":" + json(signature) + "}");
    }

    public String presenceJoin(String channel, String member) {
        return request("POST", "/v1/presence/join", "{\"channel\":" + json(channel) + ",\"member\":" + json(member) + "}");
    }

    public String broadcast(String channel, String payloadJson) {
        return request("POST", "/v1/broadcast", "{\"channel\":" + json(channel) + ",\"payload\":" + payloadJson + "}");
    }

    public String topicAppend(String partition, String key, String value) {
        return request("POST", "/v1/topics/append", "{\"partition\":" + json(partition)
                + ",\"key\":" + json(key) + ",\"value\":" + json(value) + "}");
    }

    /** Subscribe to committed table changes. Each complete text frame is passed to {@code onMessage}. */
    public CompletableFuture<WebSocket> subscribeTable(String table, Consumer<String> onMessage) {
        return subscribeTable(table, null, null, null, onMessage);
    }

    /** Subscribe to committed table changes with branch and replay cursors. */
    public CompletableFuture<WebSocket> subscribeTable(String table, String branch, Long from,
                                                       Long fromSequence, Consumer<String> onMessage) {
        List<String> params = new ArrayList<>();
        params.add("table=" + encode(table));
        if (branch != null) params.add("branch=" + encode(branch));
        if (from != null) params.add("from=" + from);
        if (fromSequence != null) params.add("from_sequence=" + fromSequence);
        return subscribe("/v1/stream", params, onMessage);
    }

    /** Subscribe to a table and resume from the last sequence after a disconnect. */
    public CompletableFuture<RealtimeSubscription> subscribeTableResumable(
            String table, Consumer<String> onMessage) {
        return subscribeTableResumable(table, null, null, null, onMessage);
    }

    /** Subscribe to a table with branch/replay options and automatic sequence-based reconnects. */
    public CompletableFuture<RealtimeSubscription> subscribeTableResumable(
            String table, String branch, Long from, Long fromSequence, Consumer<String> onMessage) {
        return new RealtimeSubscription(table, branch, from, fromSequence, onMessage).start();
    }

    /** Subscribe to ephemeral broadcast frames for one channel. */
    public CompletableFuture<WebSocket> subscribeBroadcast(String channel, Consumer<String> onMessage) {
        return subscribe("/v1/broadcast/" + segment(channel), List.of(), onMessage);
    }

    /** Subscribe to a live query snapshot/update stream. */
    public CompletableFuture<WebSocket> subscribeQuery(String table, String branch, Integer limit,
                                                        Consumer<String> onMessage) {
        List<String> params = new ArrayList<>();
        params.add("table=" + encode(table));
        if (branch != null) params.add("branch=" + encode(branch));
        if (limit != null) params.add("limit=" + limit);
        return subscribe("/v1/query-stream", params, onMessage);
    }

    private CompletableFuture<WebSocket> subscribe(String path, List<String> params,
                                                    Consumer<String> onMessage) {
        Objects.requireNonNull(onMessage, "onMessage");
        List<String> query = new ArrayList<>(params);
        if (!apiKey.isEmpty()) query.add("api_key=" + encode(apiKey));
        String suffix = query.isEmpty() ? "" : "?" + String.join("&", query);
        return http.newWebSocketBuilder().buildAsync(websocketUri(path + suffix), new WebSocket.Listener() {
            private final StringBuilder frame = new StringBuilder();

            @Override
            public void onOpen(WebSocket webSocket) {
                webSocket.request(1);
            }

            @Override
            public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
                frame.append(data);
                if (last) {
                    onMessage.accept(frame.toString());
                    frame.setLength(0);
                }
                webSocket.request(1);
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public void onError(WebSocket webSocket, Throwable error) {
                webSocket.abort();
            }
        });
    }

    /** A reconnecting table subscription. Call {@link #close()} to stop retries. */
    public final class RealtimeSubscription implements AutoCloseable {
        private final String table;
        private final String branch;
        private final Long from;
        private final Consumer<String> onMessage;
        private final AtomicBoolean closed = new AtomicBoolean();
        private final AtomicBoolean reconnectScheduled = new AtomicBoolean();
        private final AtomicBoolean everConnected = new AtomicBoolean();
        private final AtomicLong generation = new AtomicLong();
        private final CompletableFuture<RealtimeSubscription> ready = new CompletableFuture<>();
        private volatile Long cursor;
        private volatile WebSocket current;
        private volatile long reconnectDelayMillis = 250;

        private RealtimeSubscription(String table, String branch, Long from, Long fromSequence,
                                     Consumer<String> onMessage) {
            this.table = Objects.requireNonNull(table, "table");
            this.branch = branch;
            this.from = from;
            this.cursor = fromSequence;
            this.onMessage = Objects.requireNonNull(onMessage, "onMessage");
        }

        private CompletableFuture<RealtimeSubscription> start() {
            connect();
            return ready;
        }

        private List<String> params() {
            List<String> params = new ArrayList<>();
            params.add("table=" + encode(table));
            if (branch != null) params.add("branch=" + encode(branch));
            Long currentCursor = cursor;
            if (currentCursor != null) {
                params.add("from_sequence=" + currentCursor);
            } else if (from != null) {
                params.add("from=" + from);
            }
            if (!apiKey.isEmpty()) params.add("api_key=" + encode(apiKey));
            return params;
        }

        private void connect() {
            if (closed.get()) return;
            long connection = generation.incrementAndGet();
            String suffix = "?" + String.join("&", params());
            http.newWebSocketBuilder().buildAsync(websocketUri("/v1/stream" + suffix),
                    new WebSocket.Listener() {
                        private final StringBuilder frame = new StringBuilder();

                        @Override
                        public void onOpen(WebSocket webSocket) {
                            current = webSocket;
                            everConnected.set(true);
                            reconnectDelayMillis = 250;
                            webSocket.request(1);
                        }

                        @Override
                        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data,
                                                          boolean last) {
                            frame.append(data);
                            if (last) {
                                String text = frame.toString();
                                frame.setLength(0);
                                updateCursor(text);
                                onMessage.accept(text);
                            }
                            webSocket.request(1);
                            return CompletableFuture.completedFuture(null);
                        }

                        @Override
                        public void onError(WebSocket webSocket, Throwable error) {
                            if (generation.get() == connection) scheduleReconnect(connection);
                        }

                        @Override
                        public CompletionStage<?> onClose(WebSocket webSocket, int statusCode,
                                                           String reason) {
                            if (generation.get() == connection) scheduleReconnect(connection);
                            return CompletableFuture.completedFuture(null);
                        }
                    }).whenComplete((webSocket, error) -> {
                        if (error != null) {
                            if (!everConnected.get()) {
                                ready.completeExceptionally(error);
                            } else if (generation.get() == connection) {
                                scheduleReconnect(connection);
                            }
                        } else if (!ready.isDone()) {
                            ready.complete(this);
                        }
                    });
        }

        private void updateCursor(String text) {
            Matcher matcher = SEQUENCE_FIELD.matcher(text);
            if (!matcher.find()) return;
            try {
                long sequence = Long.parseLong(matcher.group(1));
                Long currentCursor = cursor;
                if (currentCursor == null || sequence > currentCursor) cursor = sequence;
            } catch (NumberFormatException ignored) {
                // The server emits unsigned 64-bit cursors; an out-of-range value is not resumable.
            }
        }

        private void scheduleReconnect(long connection) {
            if (closed.get() || generation.get() != connection || !everConnected.get()) return;
            if (!reconnectScheduled.compareAndSet(false, true)) return;
            long delay = reconnectDelayMillis;
            reconnectDelayMillis = Math.min(reconnectDelayMillis * 2, 5_000);
            REALTIME_RECONNECTOR.schedule(() -> {
                reconnectScheduled.set(false);
                connect();
            }, delay, TimeUnit.MILLISECONDS);
        }

        @Override
        public void close() {
            if (!closed.compareAndSet(false, true)) return;
            generation.incrementAndGet();
            WebSocket webSocket = current;
            if (webSocket != null) webSocket.abort();
        }
    }

    public String requestJson(String method, String path, String body) {
        return request(method, path, body);
    }

    private String request(String method, String path, String body) {
        HttpRequest.Builder builder = HttpRequest.newBuilder(uri(path))
                .timeout(Duration.ofSeconds(30))
                .header("Accept", "application/json");
        if (!apiKey.isEmpty()) builder.header("Authorization", "Bearer " + apiKey);
        if (body == null) {
            builder.method(method, HttpRequest.BodyPublishers.noBody());
        } else {
            builder.header("Content-Type", "application/json");
            builder.method(method, HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8));
        }
        try {
            HttpResponse<String> response = http.send(builder.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (response.statusCode() / 100 != 2) throw new RymeError(response.statusCode(), response.body());
            return response.body();
        } catch (IOException e) {
            throw new IllegalStateException("Unable to reach rymeDB", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while calling rymeDB", e);
        }
    }

    private URI uri(String path) {
        return URI.create(base + (path.startsWith("/") ? path : "/" + path));
    }

    private URI websocketUri(String path) {
        String websocketBase = base.startsWith("https://")
                ? "wss://" + base.substring("https://".length())
                : base.startsWith("http://")
                    ? "ws://" + base.substring("http://".length())
                    : base;
        return URI.create(websocketBase + (path.startsWith("/") ? path : "/" + path));
    }

    private static String segment(String value) {
        return encode(value).replace("+", "%20");
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private static String querySuffix(String query) {
        return query == null || query.isEmpty() ? "" : "?" + query;
    }

    private static String queryParams(List<String> params) {
        return params.isEmpty() ? "" : "?" + String.join("&", params);
    }

    private static String json(String value) {
        if (value == null) return "null";
        return "\"" + value.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\b", "\\b")
                .replace("\f", "\\f")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t") + "\"";
    }

    private static String nullableJson(String value) {
        return value == null ? "null" : json(value);
    }

    private static String numbers(List<Double> values) {
        return values.stream().map(String::valueOf).collect(Collectors.joining(","));
    }
}
