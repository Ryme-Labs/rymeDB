package com.rymelabs.rymedb;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Small, dependency-free Java client for the rymeDB HTTP API.
 *
 * <p>Methods return the server's JSON response as a string so applications can
 * choose Jackson, Gson, or another JSON library without pulling one into the
 * SDK. The client uses {@link HttpClient} and is safe to reuse across threads.
 */
public final class RymeClient {
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
