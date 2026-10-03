package rymedb

import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.OutputStreamWriter
import java.net.HttpURLConnection
import java.net.URL
import java.net.URLEncoder

class RymeError(val status: Int, val body: String) : RuntimeException("rymeDB HTTP $status: $body")

data class CopyRow(val key: String, val value: String)

class RymeClient(base: String, apiKey: String = "") {
    private val base: String = base.trimEnd('/')
    private val apiKey: String = apiKey.ifEmpty { System.getenv("RYME_API_KEY") ?: "" }

    private fun request(method: String, path: String, body: String? = null): String {
        val conn = URL(base + path).openConnection() as HttpURLConnection
        try {
            conn.requestMethod = method
            conn.connectTimeout = 30_000
            conn.readTimeout = 30_000
            if (apiKey.isNotEmpty()) conn.setRequestProperty("Authorization", "Bearer $apiKey")
            if (body != null) {
                conn.doOutput = true
                conn.setRequestProperty("Content-Type", "application/json")
                OutputStreamWriter(conn.outputStream, Charsets.UTF_8).use { it.write(body) }
            }
            val status = conn.responseCode
            val stream = if (status in 200..299) conn.inputStream else conn.errorStream
            val text = BufferedReader(InputStreamReader(stream, Charsets.UTF_8)).readText()
            if (status !in 200..299) throw RymeError(status, text)
            return text
        } finally {
            conn.disconnect()
        }
    }

    private fun esc(value: String): String {
        return value.replace("\\", "\\\\").replace("\"", "\\\"")
    }

    fun health(): Boolean {
        return try {
            val conn = URL("$base/health").openConnection() as HttpURLConnection
            try {
                conn.connectTimeout = 5_000
                conn.readTimeout = 5_000
                conn.responseCode in 200..299
            } finally {
                conn.disconnect()
            }
        } catch (e: Exception) {
            false
        }
    }

    fun kvGet(table: String, key: String): String = request("GET", "/v1/kv/$table/$key")

    fun kvPut(table: String, key: String, value: String, ttl: Int? = null): String {
        val suffix = if (ttl != null) "?ttl=$ttl" else ""
        val conn = URL("$base/v1/kv/$table/$key$suffix").openConnection() as HttpURLConnection
        try {
            conn.requestMethod = "PUT"
            conn.doOutput = true
            conn.connectTimeout = 30_000
            conn.readTimeout = 30_000
            if (apiKey.isNotEmpty()) conn.setRequestProperty("Authorization", "Bearer $apiKey")
            OutputStreamWriter(conn.outputStream, Charsets.UTF_8).use { it.write(value) }
            val status = conn.responseCode
            val text = BufferedReader(InputStreamReader(
                if (status in 200..299) conn.inputStream else conn.errorStream, Charsets.UTF_8)).readText()
            if (status !in 200..299) throw RymeError(status, text)
            return text
        } finally {
            conn.disconnect()
        }
    }

    fun kvDelete(table: String, key: String): String = request("DELETE", "/v1/kv/$table/$key")

    fun sql(statement: String): String = request("POST", "/v1/sql", "{\"sql\":\"${esc(statement)}\"}")

    fun sqlCopy(table: String, rows: List<CopyRow>): String {
        val items = rows.joinToString(",") { "{\"key\":\"${esc(it.key)}\",\"value\":\"${esc(it.value)}\"}" }
        return request("POST", "/v1/sql/copy", "{\"table\":\"${esc(table)}\",\"rows\":[$items]}")
    }

    fun sqlExplain(statement: String): String =
        request("POST", "/v1/sql/explain", "{\"sql\":\"${esc(statement)}\"}")

    fun restList(table: String, query: String = ""): String {
        val suffix = if (query.isEmpty()) "" else "?$query"
        return request("GET", "/rest/v1/$table$suffix")
    }

    fun restInsert(table: String, key: String, value: String): String =
        request("POST", "/rest/v1/$table", "{\"key\":\"${esc(key)}\",\"value\":\"${esc(value)}\"}")

    fun restDelete(table: String, key: String): String {
        val encoded = URLEncoder.encode(key, "UTF-8")
        return request("DELETE", "/rest/v1/$table?key=eq.$encoded")
    }

    fun graphql(query: String): String = request("POST", "/graphql", "{\"query\":\"${esc(query)}\"}")

    fun metering(): String = request("GET", "/v1/metering")

    fun autoscale(query: String = ""): String {
        val suffix = if (query.isEmpty()) "" else "?$query"
        return request("GET", "/v1/autoscale$suffix")
    }

    fun slowLog(limit: Int? = null, table: String? = null): String {
        val params = mutableListOf<String>()
        if (limit != null) params.add("limit=$limit")
        if (table != null) params.add("table=" + URLEncoder.encode(table, "UTF-8"))
        val suffix = if (params.isEmpty()) "" else "?" + params.joinToString("&")
        return request("GET", "/v1/observe/slow$suffix")
    }

    fun traces(limit: Int? = null, name: String? = null, table: String? = null): String {
        val params = mutableListOf<String>()
        if (limit != null) params.add("limit=$limit")
        if (name != null) params.add("name=" + URLEncoder.encode(name, "UTF-8"))
        if (table != null) params.add("table=" + URLEncoder.encode(table, "UTF-8"))
        val suffix = if (params.isEmpty()) "" else "?" + params.joinToString("&")
        return request("GET", "/v1/traces$suffix")
    }

    fun branchList(): String = request("GET", "/v1/branches")

    fun branchReset(id: String, baseCommitTs: Long): String =
        request("POST", "/v1/branches/$id/reset", "{\"base_commit_ts\":$baseCommitTs}")

    fun branchPromote(id: String): String = request("POST", "/v1/branches/$id/promote", null)

    fun billingSummary(): String = request("GET", "/v1/billing/summary")

    fun vectorAnnSearch(table: String, vector: List<Double>, topK: Int = 10, ef: Int = 64): String =
        request("POST", "/v1/vector/ann-search", "{\"table\":\"${esc(table)}\",\"vector\":[${vector.joinToString(",")}],\"top_k\":$topK,\"ef\":$ef}")

    fun oidcLogin(redirectUri: String, state: String? = null): String =
        request("POST", "/v1/auth/oidc/login", "{\"redirect_uri\":\"${esc(redirectUri)}\",\"state\":${if (state == null) "null" else "\"${esc(state)}\""}}")

    fun oidcToken(idToken: String): String =
        request("POST", "/v1/auth/oidc/token", "{\"id_token\":\"${esc(idToken)}\"}")

    fun migrateSupabase(dump: String): String =
        request("POST", "/v1/migrate/supabase", "{\"dump\":\"${esc(dump)}\"}")

    fun backupVerify(backupId: String): String =
        request("GET", "/v1/backups/verify?backup_id=$backupId")

    fun backupDrill(): String =
        request("GET", "/v1/backups/drill")

    fun backupCopy(backupId: String): String =
        request("POST", "/v1/backups/copy", "{\"backup_id\":\"${esc(backupId)}\"}")

    fun checkpoint(manifestId: String? = null): String {
        val manifest = if (manifestId == null) "null" else "\"${esc(manifestId)}\""
        return request("POST", "/v1/backups/checkpoint", "{\"manifest_id\":$manifest}")
    }

    fun snapshot(): String = request("POST", "/v1/snapshots", null)

    fun latestCheckpoint(): String = request("GET", "/v1/backups/latest")

    fun pitr(target: Long): String = request("GET", "/v1/backups/pitr?target=$target")

    fun restore(target: Long): String = request("POST", "/v1/backups/restore?target=$target", null)

    fun archive(backupId: String? = null): String {
        val id = if (backupId == null) "null" else "\"${esc(backupId)}\""
        return request("POST", "/v1/backups/archive", "{\"backup_id\":$id}")
    }

    fun archives(): String = request("GET", "/v1/backups/archives")

    fun shardLayout(): String = request("GET", "/v1/shards")

    fun shardMove(table: String, target: Int? = null): String {
        val to = target?.toString() ?: "null"
        return request("POST", "/v1/shards/move", "{\"table\":\"${esc(table)}\",\"target\":$to}")
    }

    fun ranges(key: String? = null): String {
        val suffix =
            if (key == null) "" else "?key=" + URLEncoder.encode(key, "UTF-8")
        return request("GET", "/v1/ranges$suffix")
    }

    fun rangeSplit(
        id: String,
        mid: String,
        leftId: String,
        rightId: String,
        expectedEpoch: Long,
    ): String = request(
        "POST",
        "/v1/ranges/split",
        "{\"id\":\"${esc(id)}\",\"mid\":\"${esc(mid)}\",\"left_id\":\"${esc(leftId)}\",\"right_id\":\"${esc(rightId)}\",\"expected_epoch\":$expectedEpoch}",
    )

    fun rangeMerge(
        leftId: String,
        rightId: String,
        mergedId: String,
        expectedLeftEpoch: Long,
        expectedRightEpoch: Long,
    ): String = request(
        "POST",
        "/v1/ranges/merge",
        "{\"left_id\":\"${esc(leftId)}\",\"right_id\":\"${esc(rightId)}\",\"merged_id\":\"${esc(mergedId)}\",\"expected_left_epoch\":$expectedLeftEpoch,\"expected_right_epoch\":$expectedRightEpoch}",
    )

    fun rangeAutosplit(minWrites: Long? = null): String {
        val suffix = if (minWrites == null) "" else "?min_writes=$minWrites"
        return request("POST", "/v1/ranges/autosplit$suffix")
    }

    fun rangeLoads(): String = request("GET", "/v1/ranges/loads")

    fun migrateNeon(branches: String): String =
        request("POST", "/v1/migrate/neon", "{\"branches\":\"${esc(branches)}\"}")

    fun migrateApply(id: String, sql: String, author: String? = null): String =
        request("POST", "/v1/migrate/apply", "{\"id\":\"${esc(id)}\",\"sql\":\"${esc(sql)}\",\"author\":${if (author == null) "null" else "\"${esc(author)}\""}}")

    fun migrateLedger(): String = request("GET", "/v1/migrate/ledger")

    fun indexStats(): String = request("GET", "/v1/index/stats")

    fun billingInvoice(tenant: String = ""): String {
        val suffix = if (tenant.isEmpty()) "" else "?tenant=$tenant"
        return request("GET", "/v1/billing/invoice$suffix")
    }

    fun regions(): String = request("GET", "/v1/regions")

    fun authRegister(id: String, password: String): String =
        request("POST", "/v1/auth/register", "{\"id\":\"${esc(id)}\",\"password\":\"${esc(password)}\"}")

    fun authToken(id: String, password: String, code: String? = null): String =
        request("POST", "/v1/auth/token", "{\"id\":\"${esc(id)}\",\"password\":\"${esc(password)}\",\"code\":${if (code == null) "null" else "\"${esc(code)}\""}}")

    fun authRevoke(key: String): String =
        request("DELETE", "/v1/auth/keys", "{\"key\":\"${esc(key)}\"}")

    fun passkeyRegister(user: String, credentialId: String, publicKey: String): String =
        request("POST", "/v1/auth/passkey/register", "{\"user\":\"${esc(user)}\",\"credential_id\":\"${esc(credentialId)}\",\"public_key\":\"${esc(publicKey)}\"}")

    fun passkeyVerify(user: String, credentialId: String, authenticatorData: String, clientDataJson: String, signature: String): String =
        request("POST", "/v1/auth/passkey/verify", "{\"user\":\"${esc(user)}\",\"credential_id\":\"${esc(credentialId)}\",\"authenticator_data\":\"${esc(authenticatorData)}\",\"client_data_json\":\"${esc(clientDataJson)}\",\"signature\":\"${esc(signature)}\"}")

    fun presenceJoin(channel: String, member: String): String =
        request("POST", "/v1/presence/join", "{\"channel\":\"${esc(channel)}\",\"member\":\"${esc(member)}\"}")

    fun broadcast(channel: String, payloadJson: String): String =
        request("POST", "/v1/broadcast", "{\"channel\":\"${esc(channel)}\",\"payload\":$payloadJson}")

    fun topicAppend(partition: String, key: String, value: String): String =
        request("POST", "/v1/topics/append", "{\"partition\":\"${esc(partition)}\",\"key\":\"${esc(key)}\",\"value\":\"${esc(value)}\"}")

    fun vectorUpsert(table: String, id: String, vector: List<Double>): String =
        request("POST", "/v1/vector/upsert", "{\"table\":\"${esc(table)}\",\"id\":\"${esc(id)}\",\"vector\":[${vector.joinToString(",")}]}")

    fun vectorSearch(table: String, vector: List<Double>, topK: Int = 10): String =
        request("POST", "/v1/vector/search", "{\"table\":\"${esc(table)}\",\"vector\":[${vector.joinToString(",")}],\"top_k\":$topK}")

    fun textIndex(table: String, id: String, text: String): String =
        request("POST", "/v1/text/index", "{\"table\":\"${esc(table)}\",\"id\":\"${esc(id)}\",\"text\":\"${esc(text)}\"}")

    fun textSearch(table: String, query: String, topK: Int = 10): String =
        request("POST", "/v1/text/search", "{\"table\":\"${esc(table)}\",\"query\":\"${esc(query)}\",\"top_k\":$topK}")
}
