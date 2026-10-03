using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;

namespace RymeDB;

public sealed class RymeError : Exception
{
    public int Status { get; }
    public string Body { get; }
    public RymeError(int status, string body) : base($"rymeDB HTTP {status}: {body}")
    {
        Status = status;
        Body = body;
    }
}

public sealed record CopyRow(string Key, string Value);

public sealed class RymeClient : IDisposable
{
    private readonly string _base;
    private readonly string _apiKey;
    private readonly HttpClient _http;

    public RymeClient(string base, string apiKey = "", HttpClient? http = null)
    {
        _base = base.TrimEnd('/');
        _apiKey = string.IsNullOrEmpty(apiKey)
            ? (Environment.GetEnvironmentVariable("RYME_API_KEY") ?? "")
            : apiKey;
        _http = http ?? new HttpClient { Timeout = TimeSpan.FromSeconds(30) };
    }

    private async Task<string> SendAsync(HttpMethod method, string path, object? body, CancellationToken ct = default)
    {
        using var req = new HttpRequestMessage(method, _base + path);
        if (!string.IsNullOrEmpty(_apiKey))
            req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", _apiKey);
        if (body is not null)
            req.Content = new StringContent(JsonSerializer.Serialize(body), Encoding.UTF8, "application/json");
        using var resp = await _http.SendAsync(req, ct).ConfigureAwait(false);
        var text = await resp.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
        if (!resp.IsSuccessStatusCode)
            throw new RymeError((int)resp.StatusCode, text);
        return text;
    }

    public async Task<bool> HealthAsync(CancellationToken ct = default)
    {
        try
        {
            using var resp = await _http.GetAsync(_base + "/health", ct).ConfigureAwait(false);
            return resp.IsSuccessStatusCode;
        }
        catch
        {
            return false;
        }
    }

    public Task<string> KvGetAsync(string table, string key, CancellationToken ct = default)
        => SendAsync(HttpMethod.Get, $"/v1/kv/{table}/{key}", null, ct);

    public async Task<string> KvPutAsync(string table, string key, string value, int? ttl = null, CancellationToken ct = default)
    {
        var suffix = ttl.HasValue ? $"?ttl={ttl.Value}" : "";
        using var req = new HttpRequestMessage(HttpMethod.Put, $"{_base}/v1/kv/{table}/{key}{suffix}");
        if (!string.IsNullOrEmpty(_apiKey))
            req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", _apiKey);
        req.Content = new StringContent(value, Encoding.UTF8);
        using var resp = await _http.SendAsync(req, ct).ConfigureAwait(false);
        var text = await resp.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
        if (!resp.IsSuccessStatusCode)
            throw new RymeError((int)resp.StatusCode, text);
        return text;
    }

    public Task<string> KvDeleteAsync(string table, string key, CancellationToken ct = default)
        => SendAsync(HttpMethod.Delete, $"/v1/kv/{table}/{key}", null, ct);

    public Task<string> SqlAsync(string sql, CancellationToken ct = default)
        => SendAsync(HttpMethod.Post, "/v1/sql", new { sql }, ct);

    public Task<string> SqlCopyAsync(string table, IReadOnlyList<CopyRow> rows, CancellationToken ct = default)
        => SendAsync(HttpMethod.Post, "/v1/sql/copy", new { table, rows }, ct);

    public Task<string> SqlExplainAsync(string sql, CancellationToken ct = default)
        => SendAsync(HttpMethod.Post, "/v1/sql/explain", new { sql }, ct);

    public Task<string> RestListAsync(string table, string query = "", CancellationToken ct = default)
        => SendAsync(HttpMethod.Get, "/rest/v1/" + table + (string.IsNullOrEmpty(query) ? "" : "?" + query), null, ct);

    public Task<string> RestInsertAsync(string table, string key, string value, CancellationToken ct = default)
        => SendAsync(HttpMethod.Post, "/rest/v1/" + table, new { key, value }, ct);

    public Task<string> RestDeleteAsync(string table, string key, CancellationToken ct = default)
        => SendAsync(HttpMethod.Delete, "/rest/v1/" + table + "?key=eq." + Uri.EscapeDataString(key), null, ct);

    public Task<string> GraphqlAsync(string query, CancellationToken ct = default)
        => SendAsync(HttpMethod.Post, "/graphql", new { query }, ct);

    public Task<string> MeteringAsync(CancellationToken ct = default)
        => SendAsync(HttpMethod.Get, "/v1/metering", null, ct);

    public Task<string> AutoscaleAsync(string query = "", CancellationToken ct = default)
        => SendAsync(HttpMethod.Get, "/v1/autoscale" + (string.IsNullOrEmpty(query) ? "" : "?" + query), null, ct);

    public Task<string> SlowLogAsync(int? limit = null, string? table = null, CancellationToken ct = default)
    {
        var query = new List<string>();
        if (limit is not null) query.Add("limit=" + limit);
        if (table is not null) query.Add("table=" + Uri.EscapeDataString(table));
        var suffix = query.Count == 0 ? "" : "?" + string.Join("&", query);
        return SendAsync(HttpMethod.Get, "/v1/observe/slow" + suffix, null, ct);
    }

    public Task<string> TracesAsync(int? limit = null, string? name = null, string? table = null, CancellationToken ct = default)
    {
        var query = new List<string>();
        if (limit is not null) query.Add("limit=" + limit);
        if (name is not null) query.Add("name=" + Uri.EscapeDataString(name));
        if (table is not null) query.Add("table=" + Uri.EscapeDataString(table));
        var suffix = query.Count == 0 ? "" : "?" + string.Join("&", query);
        return SendAsync(HttpMethod.Get, "/v1/traces" + suffix, null, ct);
    }

    public Task<string> BranchListAsync(CancellationToken ct = default)
        => SendAsync(HttpMethod.Get, "/v1/branches", null, ct);

    public Task<string> BranchResetAsync(string id, ulong baseCommitTs, CancellationToken ct = default)
        => SendAsync(HttpMethod.Post, $"/v1/branches/{id}/reset", new { base_commit_ts = baseCommitTs }, ct);

    public Task<string> BranchPromoteAsync(string id, CancellationToken ct = default)
        => SendAsync(HttpMethod.Post, $"/v1/branches/{id}/promote", null, ct);

    public Task<string> BillingSummaryAsync(CancellationToken ct = default)
        => SendAsync(HttpMethod.Get, "/v1/billing/summary", null, ct);

    public Task<string> VectorAnnSearchAsync(string table, IReadOnlyList<double> vector, int topK = 10, int ef = 64, CancellationToken ct = default)
        => SendAsync(HttpMethod.Post, "/v1/vector/ann-search", new { table, vector, top_k = topK, ef }, ct);

    public Task<string> OidcLoginAsync(string redirectUri, string? state = null, CancellationToken ct = default)
        => SendAsync(HttpMethod.Post, "/v1/auth/oidc/login", new { redirect_uri = redirectUri, state }, ct);

    public Task<string> OidcTokenAsync(string idToken, CancellationToken ct = default)
        => SendAsync(HttpMethod.Post, "/v1/auth/oidc/token", new { id_token = idToken }, ct);

    public Task<string> MigrateSupabaseAsync(string dump, CancellationToken ct = default)
        => SendAsync(HttpMethod.Post, "/v1/migrate/supabase", new { dump }, ct);

    public Task<string> BackupVerifyAsync(string backupId, CancellationToken ct = default)
        => SendAsync(HttpMethod.Get, "/v1/backups/verify?backup_id=" + Uri.EscapeDataString(backupId), null, ct);

    public Task<string> BackupDrillAsync(CancellationToken ct = default)
        => SendAsync(HttpMethod.Get, "/v1/backups/drill", null, ct);

    public Task<string> BackupCopyAsync(string backupId, CancellationToken ct = default)
        => SendAsync(HttpMethod.Post, "/v1/backups/copy", new { backup_id = backupId }, ct);

    public Task<string> CheckpointAsync(string? manifestId = null, CancellationToken ct = default)
        => SendAsync(HttpMethod.Post, "/v1/backups/checkpoint", new { manifest_id = manifestId }, ct);

    public Task<string> SnapshotAsync(CancellationToken ct = default)
        => SendAsync(HttpMethod.Post, "/v1/snapshots", null, ct);

    public Task<string> LatestCheckpointAsync(CancellationToken ct = default)
        => SendAsync(HttpMethod.Get, "/v1/backups/latest", null, ct);

    public Task<string> PitrAsync(ulong target, CancellationToken ct = default)
        => SendAsync(HttpMethod.Get, "/v1/backups/pitr?target=" + target, null, ct);

    public Task<string> RestoreAsync(ulong target, CancellationToken ct = default)
        => SendAsync(HttpMethod.Post, "/v1/backups/restore?target=" + target, null, ct);

    public Task<string> ArchiveAsync(string? backupId = null, CancellationToken ct = default)
        => SendAsync(HttpMethod.Post, "/v1/backups/archive", new { backup_id = backupId }, ct);

    public Task<string> ArchivesAsync(CancellationToken ct = default)
        => SendAsync(HttpMethod.Get, "/v1/backups/archives", null, ct);

    public Task<string> ShardLayoutAsync(CancellationToken ct = default)
        => SendAsync(HttpMethod.Get, "/v1/shards", null, ct);

    public Task<string> ShardMoveAsync(string table, int? target = null, CancellationToken ct = default)
        => SendAsync(HttpMethod.Post, "/v1/shards/move", new { table, target }, ct);

    public Task<string> RangesAsync(string? key = null, CancellationToken ct = default)
        => SendAsync(HttpMethod.Get, "/v1/ranges" + (key is null ? "" : "?key=" + Uri.EscapeDataString(key)), null, ct);

    public Task<string> RangeSplitAsync(string id, string mid, string leftId, string rightId, ulong expectedEpoch, CancellationToken ct = default)
        => SendAsync(HttpMethod.Post, "/v1/ranges/split", new { id, mid, left_id = leftId, right_id = rightId, expected_epoch = expectedEpoch }, ct);

    public Task<string> RangeMergeAsync(string leftId, string rightId, string mergedId, ulong expectedLeftEpoch, ulong expectedRightEpoch, CancellationToken ct = default)
        => SendAsync(HttpMethod.Post, "/v1/ranges/merge", new { left_id = leftId, right_id = rightId, merged_id = mergedId, expected_left_epoch = expectedLeftEpoch, expected_right_epoch = expectedRightEpoch }, ct);

    public Task<string> RangeAutosplitAsync(ulong? minWrites = null, CancellationToken ct = default)
        => SendAsync(HttpMethod.Post, "/v1/ranges/autosplit" + (minWrites is null ? "" : "?min_writes=" + minWrites), null, ct);

    public Task<string> RangeLoadsAsync(CancellationToken ct = default)
        => SendAsync(HttpMethod.Get, "/v1/ranges/loads", null, ct);

    public Task<string> MigrateNeonAsync(string branches, CancellationToken ct = default)
        => SendAsync(HttpMethod.Post, "/v1/migrate/neon", new { branches }, ct);

    public Task<string> MigrateApplyAsync(string id, string sql, string? author = null, CancellationToken ct = default)
        => SendAsync(HttpMethod.Post, "/v1/migrate/apply", new { id, sql, author }, ct);

    public Task<string> MigrateLedgerAsync(CancellationToken ct = default)
        => SendAsync(HttpMethod.Get, "/v1/migrate/ledger", null, ct);

    public Task<string> IndexStatsAsync(CancellationToken ct = default)
        => SendAsync(HttpMethod.Get, "/v1/index/stats", null, ct);

    public Task<string> BillingInvoiceAsync(string tenant = "", CancellationToken ct = default)
        => SendAsync(HttpMethod.Get, "/v1/billing/invoice" + (string.IsNullOrEmpty(tenant) ? "" : "?tenant=" + Uri.EscapeDataString(tenant)), null, ct);

    public Task<string> RegionsAsync(CancellationToken ct = default)
        => SendAsync(HttpMethod.Get, "/v1/regions", null, ct);

    public Task<string> AuthRegisterAsync(string id, string password, CancellationToken ct = default)
        => SendAsync(HttpMethod.Post, "/v1/auth/register", new { id, password }, ct);

    public Task<string> AuthTokenAsync(string id, string password, string? code = null, CancellationToken ct = default)
        => SendAsync(HttpMethod.Post, "/v1/auth/token", new { id, password, code }, ct);

    public Task<string> AuthRevokeAsync(string key, CancellationToken ct = default)
        => SendAsync(HttpMethod.Delete, "/v1/auth/keys", new { key }, ct);

    public Task<string> PasskeyRegisterAsync(string user, string credentialId, string publicKey, CancellationToken ct = default)
        => SendAsync(HttpMethod.Post, "/v1/auth/passkey/register", new { user, credential_id = credentialId, public_key = publicKey }, ct);

    public Task<string> PasskeyVerifyAsync(string user, string credentialId, string authenticatorData, string clientDataJson, string signature, CancellationToken ct = default)
        => SendAsync(HttpMethod.Post, "/v1/auth/passkey/verify", new { user, credential_id = credentialId, authenticator_data = authenticatorData, client_data_json = clientDataJson, signature }, ct);

    public Task<string> PresenceJoinAsync(string channel, string member, CancellationToken ct = default)
        => SendAsync(HttpMethod.Post, "/v1/presence/join", new { channel, member }, ct);

    public Task<string> BroadcastAsync(string channel, object payload, CancellationToken ct = default)
        => SendAsync(HttpMethod.Post, "/v1/broadcast", new { channel, payload }, ct);

    public Task<string> TopicAppendAsync(string partition, string key, string value, CancellationToken ct = default)
        => SendAsync(HttpMethod.Post, "/v1/topics/append", new { partition, key, value }, ct);

    public Task<string> VectorUpsertAsync(string table, string id, IReadOnlyList<double> vector, CancellationToken ct = default)
        => SendAsync(HttpMethod.Post, "/v1/vector/upsert", new { table, id, vector }, ct);

    public Task<string> VectorSearchAsync(string table, IReadOnlyList<double> vector, int topK = 10, CancellationToken ct = default)
        => SendAsync(HttpMethod.Post, "/v1/vector/search", new { table, vector, top_k = topK }, ct);

    public Task<string> TextIndexAsync(string table, string id, string text, CancellationToken ct = default)
        => SendAsync(HttpMethod.Post, "/v1/text/index", new { table, id, text }, ct);

    public Task<string> TextSearchAsync(string table, string query, int topK = 10, CancellationToken ct = default)
        => SendAsync(HttpMethod.Post, "/v1/text/search", new { table, query, top_k = topK }, ct);

    public void Dispose() => _http.Dispose();
}
