import 'dart:convert';
import 'dart:io';

class RymeError implements Exception {
  final int status;
  final String body;
  RymeError(this.status, this.body);
  @override
  String toString() => 'rymeDB HTTP $status: $body';
}

class CopyRow {
  final String key;
  final String value;
  CopyRow(this.key, this.value);
  Map<String, String> toJson() => {'key': key, 'value': value};
}

class RymeClient {
  final String base;
  final String apiKey;
  final HttpClient _http;

  RymeClient({required String base, String apiKey = ''})
      : base = base.endsWith('/') ? base.substring(0, base.length - 1) : base,
        apiKey = apiKey.isEmpty
            ? (Platform.environment['RYME_API_KEY'] ?? '')
            : apiKey,
        _http = HttpClient();

  Future<String> _request(String method, String path, [Object? body]) async {
    final req = await _http.openUrl(method, Uri.parse('$base$path'));
    if (apiKey.isNotEmpty) req.headers.set('authorization', 'Bearer $apiKey');
    if (body != null) {
      req.headers.contentType = ContentType.json;
      req.write(jsonEncode(body));
    }
    final resp = await req.close();
    final text = await resp.transform(utf8.decoder).join();
    if (resp.statusCode < 200 || resp.statusCode >= 300) {
      throw RymeError(resp.statusCode, text);
    }
    return text;
  }

  Future<String> _rawPut(String path, String value) async {
    final req = await _http.openUrl('PUT', Uri.parse('$base$path'));
    if (apiKey.isNotEmpty) req.headers.set('authorization', 'Bearer $apiKey');
    req.write(value);
    final resp = await req.close();
    final text = await resp.transform(utf8.decoder).join();
    if (resp.statusCode < 200 || resp.statusCode >= 300) {
      throw RymeError(resp.statusCode, text);
    }
    return text;
  }

  Future<bool> health() async {
    try {
      final req = await _http.getUrl(Uri.parse('$base/health'));
      final resp = await req.close();
      await resp.drain();
      return resp.statusCode >= 200 && resp.statusCode < 300;
    } catch (_) {
      return false;
    }
  }

  Future<String> kvGet(String table, String key) => _request('GET', '/v1/kv/$table/$key');

  Future<String> kvPut(String table, String key, String value, {int? ttl}) {
    final suffix = ttl == null ? '' : '?ttl=$ttl';
    return _rawPut('/v1/kv/$table/$key$suffix', value);
  }

  Future<String> kvDelete(String table, String key) => _request('DELETE', '/v1/kv/$table/$key');

  Future<String> sql(String statement) => _request('POST', '/v1/sql', {'sql': statement});

  Future<String> sqlCopy(String table, List<CopyRow> rows) => _request(
      'POST', '/v1/sql/copy', {'table': table, 'rows': rows.map((r) => r.toJson()).toList()});

  Future<String> sqlExplain(String statement) =>
      _request('POST', '/v1/sql/explain', {'sql': statement});

  Future<String> restList(String table, [String query = '']) =>
      _request('GET', '/rest/v1/$table${query.isEmpty ? '' : '?$query'}');

  Future<String> restInsert(String table, String key, String value) =>
      _request('POST', '/rest/v1/$table', {'key': key, 'value': value});

  Future<String> restDelete(String table, String key) =>
      _request('DELETE', '/rest/v1/$table?key=eq.${Uri.encodeQueryComponent(key)}');

  Future<String> graphql(String query) => _request('POST', '/graphql', {'query': query});

  Future<String> metering() => _request('GET', '/v1/metering');

  Future<String> autoscale([String query = '']) =>
      _request('GET', '/v1/autoscale${query.isEmpty ? '' : '?$query'}');

  Future<String> slowLog([int? limit, String? table]) {
    final params = <String>[];
    if (limit != null) params.add('limit=$limit');
    if (table != null) params.add('table=${Uri.encodeQueryComponent(table)}');
    final suffix = params.isEmpty ? '' : '?${params.join('&')}';
    return _request('GET', '/v1/observe/slow$suffix');
  }

  Future<String> traces([int? limit, String? name, String? table]) {
    final params = <String>[];
    if (limit != null) params.add('limit=$limit');
    if (name != null) params.add('name=${Uri.encodeQueryComponent(name)}');
    if (table != null) params.add('table=${Uri.encodeQueryComponent(table)}');
    final suffix = params.isEmpty ? '' : '?${params.join('&')}';
    return _request('GET', '/v1/traces$suffix');
  }

  Future<String> branchList() => _request('GET', '/v1/branches');

  Future<String> branchReset(String id, int baseCommitTs) =>
      _request('POST', '/v1/branches/$id/reset', {'base_commit_ts': baseCommitTs});

  Future<String> branchPromote(String id) => _request('POST', '/v1/branches/$id/promote');

  Future<String> billingSummary() => _request('GET', '/v1/billing/summary');

  Future<String> vectorAnnSearch(String table, List<double> vector, [int topK = 10, int ef = 64]) =>
      _request('POST', '/v1/vector/ann-search', {'table': table, 'vector': vector, 'top_k': topK, 'ef': ef});

  Future<String> oidcLogin(String redirectUri, [String? state]) =>
      _request('POST', '/v1/auth/oidc/login', {'redirect_uri': redirectUri, 'state': state});

  Future<String> oidcToken(String idToken) =>
      _request('POST', '/v1/auth/oidc/token', {'id_token': idToken});

  Future<String> migrateSupabase(String dump) =>
      _request('POST', '/v1/migrate/supabase', {'dump': dump});

  Future<String> backupVerify(String backupId) =>
      _request('GET', '/v1/backups/verify?backup_id=$backupId');

  Future<String> backupDrill() => _request('GET', '/v1/backups/drill');

  Future<String> backupCopy(String backupId) =>
      _request('POST', '/v1/backups/copy', {'backup_id': backupId});

  Future<String> checkpoint([String? manifestId]) =>
      _request('POST', '/v1/backups/checkpoint', {'manifest_id': manifestId});

  Future<String> snapshot() => _request('POST', '/v1/snapshots');

  Future<String> latestCheckpoint() => _request('GET', '/v1/backups/latest');

  Future<String> pitr(int target) => _request('GET', '/v1/backups/pitr?target=$target');

  Future<String> restore(int target) => _request('POST', '/v1/backups/restore?target=$target');

  Future<String> archive([String? backupId]) =>
      _request('POST', '/v1/backups/archive', {'backup_id': backupId});

  Future<String> archives() => _request('GET', '/v1/backups/archives');

  Future<String> shardLayout() => _request('GET', '/v1/shards');

  Future<String> shardMove(String table, [int? target]) =>
      _request('POST', '/v1/shards/move', {'table': table, 'target': target});

  Future<String> ranges([String? key]) => _request('GET',
      key == null ? '/v1/ranges' : '/v1/ranges?key=${Uri.encodeQueryComponent(key)}');

  Future<String> rangeSplit(
          String id, String mid, String leftId, String rightId, int expectedEpoch) =>
      _request('POST', '/v1/ranges/split', {
        'id': id,
        'mid': mid,
        'left_id': leftId,
        'right_id': rightId,
        'expected_epoch': expectedEpoch
      });

  Future<String> rangeMerge(String leftId, String rightId, String mergedId,
          int expectedLeftEpoch, int expectedRightEpoch) =>
      _request('POST', '/v1/ranges/merge', {
        'left_id': leftId,
        'right_id': rightId,
        'merged_id': mergedId,
        'expected_left_epoch': expectedLeftEpoch,
        'expected_right_epoch': expectedRightEpoch
      });

  Future<String> rangeAutosplit([int? minWrites]) => _request('POST',
      minWrites == null ? '/v1/ranges/autosplit' : '/v1/ranges/autosplit?min_writes=$minWrites');

  Future<String> rangeLoads() => _request('GET', '/v1/ranges/loads');

  Future<String> migrateNeon(String branches) =>
      _request('POST', '/v1/migrate/neon', {'branches': branches});

  Future<String> migrateApply(String id, String sql, [String? author]) =>
      _request('POST', '/v1/migrate/apply', {'id': id, 'sql': sql, 'author': author});

  Future<String> migrateLedger() => _request('GET', '/v1/migrate/ledger');

  Future<String> indexStats() => _request('GET', '/v1/index/stats');

  Future<String> billingInvoice([String tenant = '']) =>
      _request('GET', '/v1/billing/invoice${tenant.isEmpty ? '' : '?tenant=$tenant'}');

  Future<String> regions() => _request('GET', '/v1/regions');

  Future<String> authRegister(String id, String password) =>
      _request('POST', '/v1/auth/register', {'id': id, 'password': password});

  Future<String> authToken(String id, String password, [String? code]) =>
      _request('POST', '/v1/auth/token', {'id': id, 'password': password, 'code': code});

  Future<String> authRevoke(String key) =>
      _request('DELETE', '/v1/auth/keys', {'key': key});

  Future<String> passkeyRegister(String user, String credentialId, String publicKey) =>
      _request('POST', '/v1/auth/passkey/register', {'user': user, 'credential_id': credentialId, 'public_key': publicKey});

  Future<String> passkeyVerify(String user, String credentialId, String authenticatorData, String clientDataJson, String signature) =>
      _request('POST', '/v1/auth/passkey/verify', {'user': user, 'credential_id': credentialId, 'authenticator_data': authenticatorData, 'client_data_json': clientDataJson, 'signature': signature});

  Future<String> presenceJoin(String channel, String member) =>
      _request('POST', '/v1/presence/join', {'channel': channel, 'member': member});

  Future<String> broadcast(String channel, Object payload) =>
      _request('POST', '/v1/broadcast', {'channel': channel, 'payload': payload});

  Future<String> topicAppend(String partition, String key, String value) =>
      _request('POST', '/v1/topics/append', {'partition': partition, 'key': key, 'value': value});

  Future<String> vectorUpsert(String table, String id, List<double> vector) =>
      _request('POST', '/v1/vector/upsert', {'table': table, 'id': id, 'vector': vector});

  Future<String> vectorSearch(String table, List<double> vector, [int topK = 10]) =>
      _request('POST', '/v1/vector/search', {'table': table, 'vector': vector, 'top_k': topK});

  Future<String> textIndex(String table, String id, String text) =>
      _request('POST', '/v1/text/index', {'table': table, 'id': id, 'text': text});

  Future<String> textSearch(String table, String query, [int topK = 10]) =>
      _request('POST', '/v1/text/search', {'table': table, 'query': query, 'top_k': topK});

  void close() => _http.close();
}
