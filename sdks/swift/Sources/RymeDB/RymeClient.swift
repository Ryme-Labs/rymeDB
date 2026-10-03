import Foundation

public struct RymeError: Error, Sendable {
    public let status: Int
    public let body: String
    public init(status: Int, body: String) {
        self.status = status
        self.body = body
    }
}

public struct CopyRow: Codable, Sendable {
    public let key: String
    public let value: String
    public init(key: String, value: String) {
        self.key = key
        self.value = value
    }
}

public final class RymeClient: Sendable {
    private let base: String
    private let apiKey: String
    private let session: URLSession

    public init(base: String, apiKey: String = "", session: URLSession = .shared) {
        self.base = base.hasSuffix("/") ? String(base.dropLast()) : base
        self.apiKey = apiKey.isEmpty ? (ProcessInfo.processInfo.environment["RYME_API_KEY"] ?? "") : apiKey
        self.session = session
    }

    private func request(method: String, path: String, body: Data? = nil) async throws -> Data {
        guard let url = URL(string: base + path) else {
            throw RymeError(status: 0, body: "bad url")
        }
        var req = URLRequest(url: url)
        req.httpMethod = method
        if !apiKey.isEmpty {
            req.setValue("Bearer \(apiKey)", forHTTPHeaderField: "authorization")
        }
        if let body {
            req.setValue("application/json", forHTTPHeaderField: "content-type")
            req.httpBody = body
        }
        let (data, response) = try await session.data(for: req)
        guard let http = response as? HTTPURLResponse else {
            throw RymeError(status: 0, body: "no response")
        }
        guard (200..<300).contains(http.statusCode) else {
            throw RymeError(status: http.statusCode, body: String(data: data, encoding: .utf8) ?? "")
        }
        return data
    }

    private func json(_ value: Any) -> Data {
        (try? JSONSerialization.data(withJSONObject: value)) ?? Data()
    }

    public func health() async -> Bool {
        guard let url = URL(string: base + "/health") else { return false }
        do {
            let (_, response) = try await session.data(from: url)
            return (response as? HTTPURLResponse).map { (200..<300).contains($0.statusCode) } ?? false
        } catch {
            return false
        }
    }

    public func kvGet(table: String, key: String) async throws -> String {
        let data = try await request(method: "GET", path: "/v1/kv/\(table)/\(key)")
        return String(data: data, encoding: .utf8) ?? ""
    }

    public func kvPut(table: String, key: String, value: String, ttl: Int? = nil) async throws -> Data {
        let suffix = ttl.map { "?ttl=\($0)" } ?? ""
        var req = URLRequest(url: URL(string: base + "/v1/kv/\(table)/\(key)" + suffix)!)
        req.httpMethod = "PUT"
        if !apiKey.isEmpty {
            req.setValue("Bearer \(apiKey)", forHTTPHeaderField: "authorization")
        }
        req.httpBody = value.data(using: .utf8)
        let (data, response) = try await session.data(for: req)
        guard let http = response as? HTTPURLResponse, (200..<300).contains(http.statusCode) else {
            throw RymeError(status: 0, body: "put failed")
        }
        return data
    }

    public func kvDelete(table: String, key: String) async throws -> Data {
        try await request(method: "DELETE", path: "/v1/kv/\(table)/\(key)")
    }

    public func sql(_ statement: String) async throws -> Data {
        try await request(method: "POST", path: "/v1/sql", body: json(["sql": statement]))
    }

    public func sqlCopy(table: String, rows: [CopyRow]) async throws -> Data {
        let payload = ["table": table, "rows": rows.map { ["key": $0.key, "value": $0.value] }] as [String: Any]
        return try await request(method: "POST", path: "/v1/sql/copy", body: json(payload))
    }

    public func sqlExplain(_ statement: String) async throws -> Data {
        try await request(method: "POST", path: "/v1/sql/explain", body: json(["sql": statement]))
    }

    public func restList(table: String, query: String = "") async throws -> Data {
        try await request(method: "GET", path: "/rest/v1/\(table)" + (query.isEmpty ? "" : "?\(query)"))
    }

    public func restInsert(table: String, key: String, value: String) async throws -> Data {
        try await request(method: "POST", path: "/rest/v1/\(table)", body: json(["key": key, "value": value]))
    }

    public func restDelete(table: String, key: String) async throws -> Data {
        let encoded = key.addingPercentEncoding(withAllowedCharacters: .urlQueryAllowed) ?? key
        return try await request(method: "DELETE", path: "/rest/v1/\(table)?key=eq.\(encoded)")
    }

    public func graphql(_ query: String) async throws -> Data {
        try await request(method: "POST", path: "/graphql", body: json(["query": query]))
    }

    public func metering() async throws -> Data {
        try await request(method: "GET", path: "/v1/metering")
    }

    public func autoscale(query: String = "") async throws -> Data {
        try await request(method: "GET", path: "/v1/autoscale" + (query.isEmpty ? "" : "?\(query)"))
    }

    public func slowLog(limit: Int? = nil, table: String? = nil) async throws -> Data {
        var params: [String] = []
        if let limit { params.append("limit=\(limit)") }
        if let table {
            let encoded = table.addingPercentEncoding(withAllowedCharacters: .urlQueryAllowed) ?? table
            params.append("table=\(encoded)")
        }
        let suffix = params.isEmpty ? "" : "?\(params.joined(separator: "&"))"
        return try await request(method: "GET", path: "/v1/observe/slow\(suffix)")
    }

    public func traces(limit: Int? = nil, name: String? = nil, table: String? = nil) async throws -> Data {
        var params: [String] = []
        if let limit { params.append("limit=\(limit)") }
        if let name {
            let encoded = name.addingPercentEncoding(withAllowedCharacters: .urlQueryAllowed) ?? name
            params.append("name=\(encoded)")
        }
        if let table {
            let encoded = table.addingPercentEncoding(withAllowedCharacters: .urlQueryAllowed) ?? table
            params.append("table=\(encoded)")
        }
        let suffix = params.isEmpty ? "" : "?\(params.joined(separator: "&"))"
        return try await request(method: "GET", path: "/v1/traces\(suffix)")
    }

    public func branchList() async throws -> Data {
        try await request(method: "GET", path: "/v1/branches")
    }

    public func branchReset(id: String, baseCommitTs: UInt64) async throws -> Data {
        try await request(method: "POST", path: "/v1/branches/\(id)/reset", body: json(["base_commit_ts": baseCommitTs]))
    }

    public func branchPromote(id: String) async throws -> Data {
        try await request(method: "POST", path: "/v1/branches/\(id)/promote")
    }

    public func billingSummary() async throws -> Data {
        try await request(method: "GET", path: "/v1/billing/summary")
    }

    public func vectorAnnSearch(table: String, vector: [Double], topK: Int = 10, ef: Int = 64) async throws -> Data {
        try await request(method: "POST", path: "/v1/vector/ann-search", body: json(["table": table, "vector": vector, "top_k": topK, "ef": ef]))
    }

    public func oidcLogin(redirectUri: String, state: String? = nil) async throws -> Data {
        try await request(method: "POST", path: "/v1/auth/oidc/login", body: json(["redirect_uri": redirectUri, "state": state as Any]))
    }

    public func migrateSupabase(dump: String) async throws -> Data {
        try await request(method: "POST", path: "/v1/migrate/supabase", body: json(["dump": dump]))
    }

    public func backupVerify(backupId: String) async throws -> Data {
        try await request(method: "GET", path: "/v1/backups/verify?backup_id=\(backupId)")
    }

    public func backupDrill() async throws -> Data {
        try await request(method: "GET", path: "/v1/backups/drill")
    }

    public func backupCopy(backupId: String) async throws -> Data {
        try await request(method: "POST", path: "/v1/backups/copy", body: json(["backup_id": backupId]))
    }

    public func checkpoint(manifestId: String? = nil) async throws -> Data {
        try await request(method: "POST", path: "/v1/backups/checkpoint", body: json(["manifest_id": manifestId as Any]))
    }

    public func snapshot() async throws -> Data {
        try await request(method: "POST", path: "/v1/snapshots")
    }

    public func latestCheckpoint() async throws -> Data {
        try await request(method: "GET", path: "/v1/backups/latest")
    }

    public func pitr(target: UInt64) async throws -> Data {
        try await request(method: "GET", path: "/v1/backups/pitr?target=\(target)")
    }

    public func restore(target: UInt64) async throws -> Data {
        try await request(method: "POST", path: "/v1/backups/restore?target=\(target)")
    }

    public func archive(backupId: String? = nil) async throws -> Data {
        try await request(method: "POST", path: "/v1/backups/archive", body: json(["backup_id": backupId as Any]))
    }

    public func archives() async throws -> Data {
        try await request(method: "GET", path: "/v1/backups/archives")
    }

    public func shardLayout() async throws -> Data {
        try await request(method: "GET", path: "/v1/shards")
    }

    public func shardMove(table: String, target: Int? = nil) async throws -> Data {
        try await request(method: "POST", path: "/v1/shards/move", body: json(["table": table, "target": target as Any]))
    }

    public func ranges(key: String? = nil) async throws -> Data {
        let suffix = key.map { "?key=\($0.addingPercentEncoding(withAllowedCharacters: .urlQueryAllowed) ?? $0)" } ?? ""
        return try await request(method: "GET", path: "/v1/ranges\(suffix)")
    }

    public func rangeSplit(id: String, mid: String, leftId: String, rightId: String, expectedEpoch: UInt64) async throws -> Data {
        try await request(method: "POST", path: "/v1/ranges/split", body: json(["id": id, "mid": mid, "left_id": leftId, "right_id": rightId, "expected_epoch": expectedEpoch]))
    }

    public func rangeMerge(leftId: String, rightId: String, mergedId: String, expectedLeftEpoch: UInt64, expectedRightEpoch: UInt64) async throws -> Data {
        try await request(method: "POST", path: "/v1/ranges/merge", body: json(["left_id": leftId, "right_id": rightId, "merged_id": mergedId, "expected_left_epoch": expectedLeftEpoch, "expected_right_epoch": expectedRightEpoch]))
    }

    public func rangeAutosplit(minWrites: Int? = nil) async throws -> Data {
        let suffix = minWrites.map { "?min_writes=\($0)" } ?? ""
        return try await request(method: "POST", path: "/v1/ranges/autosplit\(suffix)")
    }

    public func rangeLoads() async throws -> Data {
        try await request(method: "GET", path: "/v1/ranges/loads")
    }

    public func migrateNeon(branches: String) async throws -> Data {
        try await request(method: "POST", path: "/v1/migrate/neon", body: json(["branches": branches]))
    }

    public func migrateApply(id: String, sql: String, author: String? = nil) async throws -> Data {
        try await request(method: "POST", path: "/v1/migrate/apply", body: json(["id": id, "sql": sql, "author": author as Any]))
    }

    public func migrateLedger() async throws -> Data {
        try await request(method: "GET", path: "/v1/migrate/ledger")
    }

    public func indexStats() async throws -> Data {
        try await request(method: "GET", path: "/v1/index/stats")
    }

    public func billingInvoice(tenant: String = "") async throws -> Data {
        let suffix = tenant.isEmpty ? "" : "?tenant=\(tenant)"
        return try await request(method: "GET", path: "/v1/billing/invoice\(suffix)")
    }

    public func regions() async throws -> Data {
        try await request(method: "GET", path: "/v1/regions")
    }

    public func authRegister(id: String, password: String) async throws -> Data {
        try await request(method: "POST", path: "/v1/auth/register", body: json(["id": id, "password": password]))
    }

    public func authToken(id: String, password: String, code: String? = nil) async throws -> Data {
        try await request(method: "POST", path: "/v1/auth/token", body: json(["id": id, "password": password, "code": code as Any]))
    }

    public func authRevoke(key: String) async throws -> Data {
        try await request(method: "DELETE", path: "/v1/auth/keys", body: json(["key": key]))
    }

    public func passkeyRegister(user: String, credentialId: String, publicKey: String) async throws -> Data {
        try await request(method: "POST", path: "/v1/auth/passkey/register", body: json(["user": user, "credential_id": credentialId, "public_key": publicKey]))
    }

    public func passkeyVerify(user: String, credentialId: String, authenticatorData: String, clientDataJson: String, signature: String) async throws -> Data {
        try await request(method: "POST", path: "/v1/auth/passkey/verify", body: json(["user": user, "credential_id": credentialId, "authenticator_data": authenticatorData, "client_data_json": clientDataJson, "signature": signature]))
    }

    public func presenceJoin(channel: String, member: String) async throws -> Data {
        try await request(method: "POST", path: "/v1/presence/join", body: json(["channel": channel, "member": member]))
    }

    public func broadcast(channel: String, payload: Any) async throws -> Data {
        try await request(method: "POST", path: "/v1/broadcast", body: json(["channel": channel, "payload": payload]))
    }

    public func topicAppend(partition: String, key: String, value: String) async throws -> Data {
        try await request(method: "POST", path: "/v1/topics/append", body: json(["partition": partition, "key": key, "value": value]))
    }

    public func vectorUpsert(table: String, id: String, vector: [Double]) async throws -> Data {
        try await request(method: "POST", path: "/v1/vector/upsert", body: json(["table": table, "id": id, "vector": vector]))
    }

    public func vectorSearch(table: String, vector: [Double], topK: Int = 10) async throws -> Data {
        try await request(method: "POST", path: "/v1/vector/search", body: json(["table": table, "vector": vector, "top_k": topK]))
    }

    public func textIndex(table: String, id: String, text: String) async throws -> Data {
        try await request(method: "POST", path: "/v1/text/index", body: json(["table": table, "id": id, "text": text]))
    }

    public func textSearch(table: String, query: String, topK: Int = 10) async throws -> Data {
        try await request(method: "POST", path: "/v1/text/search", body: json(["table": table, "query": query, "top_k": topK]))
    }
}
