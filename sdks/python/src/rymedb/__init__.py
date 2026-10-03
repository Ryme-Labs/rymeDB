import json
import os
import socket
from typing import Any
from urllib.parse import quote

import httpx
import websocket


class RymeError(RuntimeError):
    def __init__(self, status: int, body: str) -> None:
        super().__init__(f"rymeDB HTTP {status}: {body}")
        self.status = status
        self.body = body


class Subscription:
    def __init__(self, url: str, timeout: float) -> None:
        self._ws = websocket.create_connection(url, timeout=timeout)

    def __iter__(self) -> Any:
        return self

    def __next__(self) -> Any:
        try:
            message = self._ws.recv()
        except websocket.WebSocketException:
            raise StopIteration
        if not message:
            raise StopIteration
        return json.loads(message)

    def close(self) -> None:
        self._ws.close()


class RymeHttpClient:
    def __init__(self, base: str, api_key: str = "") -> None:
        self.base = base.rstrip("/")
        self.api_key = api_key or os.environ.get("RYME_API_KEY", "")
        self.client = httpx.Client(base_url=self.base)

    def _headers(self) -> dict[str, str]:
        if self.api_key:
            return {"authorization": f"Bearer {self.api_key}"}
        return {}

    def _request(self, method: str, path: str, body: Any = None) -> Any:
        response = self.client.request(method, path, headers=self._headers(), json=body)
        if response.status_code < 200 or response.status_code >= 300:
            raise RymeError(response.status_code, response.text)
        if not response.content:
            return None
        try:
            return response.json()
        except ValueError:
            return response.text

    def health(self) -> bool:
        response = self.client.get("/health", headers=self._headers())
        return 200 <= response.status_code < 300

    def ready(self) -> dict[str, Any]:
        return self._request("GET", "/ready")

    def metrics(self) -> dict[str, Any]:
        return self._request("GET", "/metrics")

    def slow_log(self, limit: int | None = None, table: str | None = None) -> Any:
        params = []
        if limit is not None:
            params.append(f"limit={limit}")
        if table is not None:
            params.append(f"table={quote(table)}")
        path = "/v1/observe/slow" + ("?" + "&".join(params) if params else "")
        return self._request("GET", path)

    def traces(
        self, limit: int | None = None, name: str | None = None, table: str | None = None
    ) -> Any:
        params = []
        if limit is not None:
            params.append(f"limit={limit}")
        if name is not None:
            params.append(f"name={quote(name)}")
        if table is not None:
            params.append(f"table={quote(table)}")
        path = "/v1/traces" + ("?" + "&".join(params) if params else "")
        return self._request("GET", path)

    def kv_get(self, table: str, key: str) -> str:
        response = self.client.get(f"/v1/kv/{table}/{key}", headers=self._headers())
        if response.status_code < 200 or response.status_code >= 300:
            raise RymeError(response.status_code, response.text)
        return response.text

    def kv_put(self, table: str, key: str, value: str, ttl: int | None = None) -> Any:
        path = f"/v1/kv/{table}/{key}"
        if ttl is not None:
            path += f"?ttl={ttl}"
        response = self.client.put(
            path, headers=self._headers(), content=value.encode("utf8")
        )
        if response.status_code < 200 or response.status_code >= 300:
            raise RymeError(response.status_code, response.text)
        try:
            return response.json()
        except ValueError:
            return response.text

    def kv_delete(self, table: str, key: str) -> Any:
        return self._request("DELETE", f"/v1/kv/{table}/{key}")

    def kv_ttl(self, table: str, key: str) -> dict[str, Any]:
        return self._request("GET", f"/v1/kv/{table}/{key}/ttl")

    def sql(self, sql: str, params: list[str] | None = None) -> Any:
        return self._request("POST", "/v1/sql", {"sql": sql, "params": params})

    def scan(self, table: str, limit: int | None = None) -> Any:
        path = f"/v1/scan/{table}"
        if limit is not None:
            path += f"?limit={limit}"
        return self._request("GET", path)

    def branch_create(self, id: str, parent: str, base_commit_ts: int) -> Any:
        return self._request(
            "POST",
            "/v1/branches",
            {"id": id, "parent": parent, "base_commit_ts": base_commit_ts},
        )

    def branch_get(self, id: str) -> Any:
        return self._request("GET", f"/v1/branches/{id}")

    def branch_delete(self, id: str) -> Any:
        return self._request("DELETE", f"/v1/branches/{id}")

    def branch_list(self) -> Any:
        return self._request("GET", "/v1/branches")

    def branch_reset(self, id: str, base_commit_ts: int) -> Any:
        return self._request(
            "POST", f"/v1/branches/{id}/reset", {"base_commit_ts": base_commit_ts}
        )

    def branch_promote(self, id: str) -> Any:
        return self._request("POST", f"/v1/branches/{id}/promote")

    def branch_diff(self, id: str, against: str) -> Any:
        return self._request("GET", f"/v1/branches/{id}/diff?against={quote(against)}")

    def billing_summary(self) -> Any:
        return self._request("GET", "/v1/billing/summary")

    def vector_ann_search(self, table: str, vector: list[float], top_k: int = 10, ef: int = 64) -> Any:
        return self._request(
            "POST",
            "/v1/vector/ann-search",
            {"table": table, "vector": vector, "top_k": top_k, "ef": ef},
        )

    def oidc_login(self, redirect_uri: str, state: str | None = None) -> Any:
        return self._request(
            "POST", "/v1/auth/oidc/login", {"redirect_uri": redirect_uri, "state": state}
        )

    def oidc_token(self, id_token: str) -> Any:
        return self._request("POST", "/v1/auth/oidc/token", {"id_token": id_token})

    def migrate_supabase(self, dump: str) -> Any:
        return self._request("POST", "/v1/migrate/supabase", {"dump": dump})

    def backup_verify(self, backup_id: str) -> Any:
        return self._request("GET", f"/v1/backups/verify?backup_id={quote(backup_id)}")

    def backup_drill(self) -> Any:
        return self._request("GET", "/v1/backups/drill")

    def backup_copy(self, backup_id: str) -> Any:
        return self._request("POST", "/v1/backups/copy", {"backup_id": backup_id})

    def migrate_neon(self, branches: str) -> Any:
        return self._request("POST", "/v1/migrate/neon", {"branches": branches})

    def migrate_apply(self, id: str, sql: str, author: str | None = None) -> Any:
        return self._request(
            "POST", "/v1/migrate/apply", {"id": id, "sql": sql, "author": author}
        )

    def migrate_ledger(self) -> Any:
        return self._request("GET", "/v1/migrate/ledger")

    def index_stats(self) -> Any:
        return self._request("GET", "/v1/index/stats")

    def billing_invoice(self, tenant: str = "") -> Any:
        suffix = f"?tenant={quote(tenant)}" if tenant else ""
        return self._request("GET", f"/v1/billing/invoice{suffix}")

    def regions(self) -> Any:
        return self._request("GET", "/v1/regions")

    def vector_upsert(self, table: str, id: str, vector: list[float]) -> Any:
        return self._request("POST", "/v1/vector/upsert", {"table": table, "id": id, "vector": vector})

    def vector_search(self, table: str, vector: list[float], top_k: int = 10) -> Any:
        return self._request(
            "POST", "/v1/vector/search", {"table": table, "vector": vector, "top_k": top_k}
        )

    def vector_delete(self, table: str, id: str) -> Any:
        return self._request("DELETE", f"/v1/vector/{table}/{id}")

    def text_index(self, table: str, id: str, text: str) -> Any:
        return self._request("POST", "/v1/text/index", {"table": table, "id": id, "text": text})

    def text_search(self, table: str, query: str, top_k: int = 10) -> Any:
        return self._request(
            "POST", "/v1/text/search", {"table": table, "query": query, "top_k": top_k}
        )

    def text_delete(self, table: str, id: str) -> Any:
        return self._request("DELETE", f"/v1/text/{table}/{id}")

    def checkpoint(self, manifest_id: str | None = None) -> Any:
        return self._request(
            "POST", "/v1/backups/checkpoint", {"manifest_id": manifest_id}
        )

    def snapshot(self) -> Any:
        return self._request("POST", "/v1/snapshots")

    def latest_checkpoint(self) -> Any:
        return self._request("GET", "/v1/backups/latest")

    def pitr(self, target: int) -> Any:
        return self._request("GET", f"/v1/backups/pitr?target={target}")

    def restore(self, target: int) -> Any:
        return self._request("POST", f"/v1/backups/restore?target={target}")

    def archive(self, backup_id: str | None = None) -> Any:
        return self._request("POST", "/v1/backups/archive", {"backup_id": backup_id})

    def archives(self) -> Any:
        return self._request("GET", "/v1/backups/archives")

    def shard_layout(self) -> Any:
        return self._request("GET", "/v1/shards")

    def shard_move(self, table: str, target: int | None = None) -> Any:
        return self._request(
            "POST", "/v1/shards/move", {"table": table, "target": target}
        )

    def ranges(self, key: str | None = None) -> Any:
        path = "/v1/ranges" if key is None else f"/v1/ranges?key={quote(key)}"
        return self._request("GET", path)

    def range_split(
        self, range_id: str, mid: str, left_id: str, right_id: str, expected_epoch: int
    ) -> Any:
        return self._request(
            "POST",
            "/v1/ranges/split",
            {
                "id": range_id,
                "mid": mid,
                "left_id": left_id,
                "right_id": right_id,
                "expected_epoch": expected_epoch,
            },
        )

    def range_merge(
        self,
        left_id: str,
        right_id: str,
        merged_id: str,
        expected_left_epoch: int,
        expected_right_epoch: int,
    ) -> Any:
        return self._request(
            "POST",
            "/v1/ranges/merge",
            {
                "left_id": left_id,
                "right_id": right_id,
                "merged_id": merged_id,
                "expected_left_epoch": expected_left_epoch,
                "expected_right_epoch": expected_right_epoch,
            },
        )

    def range_autosplit(self, min_writes: int | None = None) -> Any:
        path = (
            "/v1/ranges/autosplit"
            if min_writes is None
            else f"/v1/ranges/autosplit?min_writes={min_writes}"
        )
        return self._request("POST", path)

    def range_loads(self) -> Any:
        return self._request("GET", "/v1/ranges/loads")

    def cluster_members(self) -> dict[str, Any]:
        return self._request("GET", "/v1/cluster/members")

    def cluster_add(self, id: int, addr: str) -> Any:
        return self._request("POST", "/v1/cluster/members", {"id": id, "addr": addr})

    def cluster_remove(self, id: int) -> Any:
        return self._request("DELETE", f"/v1/cluster/members/{id}")

    def cluster_transfer(self, target: int) -> Any:
        return self._request("POST", "/v1/cluster/transfer", {"target": target})

    def cluster_replace(self, members: list[dict[str, Any]]) -> Any:
        return self._request("POST", "/v1/cluster/replace", {"members": members})

    def sql_copy(self, table: str, rows: list[dict[str, str]]) -> Any:
        return self._request("POST", "/v1/sql/copy", {"table": table, "rows": rows})

    def sql_explain(self, sql: str, params: list[str] | None = None) -> Any:
        return self._request("POST", "/v1/sql/explain", {"sql": sql, "params": params})

    def rest_list(self, table: str, query: str = "") -> Any:
        suffix = f"?{query}" if query else ""
        return self._request("GET", f"/rest/v1/{table}{suffix}")

    def rest_insert(self, table: str, row: Any) -> Any:
        return self._request("POST", f"/rest/v1/{table}", row)

    def rest_delete(self, table: str, key: str) -> Any:
        return self._request("DELETE", f"/rest/v1/{table}?key=eq.{quote(key)}")

    def graphql(self, query: str) -> Any:
        return self._request("POST", "/graphql", {"query": query})

    def metering(self) -> Any:
        return self._request("GET", "/v1/metering")

    def autoscale(self, query: str = "") -> Any:
        suffix = f"?{query}" if query else ""
        return self._request("GET", f"/v1/autoscale{suffix}")

    def prometheus(self) -> str:
        response = self.client.get("/metrics/prometheus", headers=self._headers())
        if response.status_code < 200 or response.status_code >= 300:
            raise RymeError(response.status_code, response.text)
        return response.text

    def qos(self) -> Any:
        return self._request("GET", "/v1/qos")

    def qos_set_tier(self, tenant: str, tier: str) -> Any:
        return self._request("POST", "/v1/qos/tier", {"tenant": tenant, "tier": tier})

    def auth_register(
        self, id: str, password: str, tenant: str | None = None, roles: list[str] | None = None
    ) -> Any:
        return self._request(
            "POST",
            "/v1/auth/register",
            {"id": id, "password": password, "tenant": tenant, "roles": roles},
        )

    def auth_verify(self, id: str, password: str) -> Any:
        return self._request("POST", "/v1/auth/verify", {"id": id, "password": password})

    def auth_token(self, id: str, password: str, code: str | None = None) -> Any:
        return self._request(
            "POST", "/v1/auth/token", {"id": id, "password": password, "code": code}
        )

    def auth_revoke(self, key: str) -> Any:
        return self._request("DELETE", "/v1/auth/keys", {"key": key})

    def passkey_register(self, user: str, credential_id: str, public_key: str) -> Any:
        return self._request(
            "POST",
            "/v1/auth/passkey/register",
            {"user": user, "credential_id": credential_id, "public_key": public_key},
        )

    def passkey_verify(
        self,
        user: str,
        credential_id: str,
        authenticator_data: str,
        client_data_json: str,
        signature: str,
    ) -> Any:
        return self._request(
            "POST",
            "/v1/auth/passkey/verify",
            {
                "user": user,
                "credential_id": credential_id,
                "authenticator_data": authenticator_data,
                "client_data_json": client_data_json,
                "signature": signature,
            },
        )

    def otp_setup(self, id: str) -> Any:
        return self._request("POST", "/v1/auth/otp/setup", {"id": id})

    def otp_verify(self, id: str, code: str) -> Any:
        return self._request("POST", "/v1/auth/otp/verify", {"id": id, "code": code})

    def passkey_challenge(self, user: str) -> Any:
        return self._request("POST", "/v1/auth/passkey/challenge", {"user": user})

    def mask_set(self, table: str, fields: list[str]) -> Any:
        return self._request("POST", "/v1/auth/mask", {"table": table, "fields": fields})

    def presence_join(
        self, channel: str, member: str, state: Any = None, ttl_secs: int | None = None
    ) -> Any:
        return self._request(
            "POST",
            "/v1/presence/join",
            {"channel": channel, "member": member, "state": state, "ttl_secs": ttl_secs},
        )

    def presence_leave(self, channel: str, member: str) -> Any:
        return self._request("POST", "/v1/presence/leave", {"channel": channel, "member": member})

    def presence_list(self, channel: str) -> Any:
        return self._request("GET", f"/v1/presence/{quote(channel)}")

    def broadcast(self, channel: str, payload: Any, sender: str | None = None) -> Any:
        return self._request(
            "POST", "/v1/broadcast", {"channel": channel, "payload": payload, "from": sender}
        )

    def topic_append(
        self, partition: str, key: str, value: str, retention: int | None = None
    ) -> Any:
        return self._request(
            "POST",
            "/v1/topics/append",
            {"partition": partition, "key": key, "value": value, "retention": retention},
        )

    def topic_read(self, partition: str, start: int = 0, limit: int = 100) -> Any:
        return self._request(
            "GET", f"/v1/topics/read?partition={quote(partition)}&from={start}&limit={limit}"
        )

    def subscribe_table(
        self, table: str, timeout: float = 30.0, from_commit: int | None = None
    ) -> Subscription:
        path = f"/v1/stream?table={quote(table)}"
        if from_commit is not None:
            path += f"&from={from_commit}"
        return Subscription(self._socket_url(path), timeout)

    def subscribe_query(
        self, table: str, limit: int | None = None, timeout: float = 30.0
    ) -> Subscription:
        path = f"/v1/query-stream?table={quote(table)}"
        if limit is not None:
            path += f"&limit={limit}"
        return Subscription(self._socket_url(path), timeout)

    def _socket_url(self, path: str) -> str:
        url = self.base
        if url.startswith("https://"):
            url = "wss://" + url[len("https://") :]
        elif url.startswith("http://"):
            url = "ws://" + url[len("http://") :]
        if self.api_key:
            sep = "&" if "?" in path else "?"
            path += f"{sep}api_key={quote(self.api_key)}"
        return url + path

    def subscribe_broadcast(self, channel: str, timeout: float = 30.0) -> Subscription:
        return Subscription(
            self._socket_url(f"/v1/broadcast/{quote(channel)}"), timeout
        )

    def close(self) -> None:
        self.client.close()


class RymeClient:
    def __init__(self, host: str, port: int, timeout: float = 5.0) -> None:
        self.host = host
        self.port = port
        self.timeout = timeout

    def command(self, *parts: str) -> str:
        payload = encode(parts)
        with socket.create_connection(
            (self.host, self.port), timeout=self.timeout
        ) as sock:
            sock.sendall(payload)
            chunks: list[bytes] = []
            while True:
                data = sock.recv(65536)
                if not data:
                    break
                chunks.append(data)
                if b"\r\n" in data:
                    text = b"".join(chunks).decode("utf8", errors="strict")
                    if text.strip():
                        break
            return b"".join(chunks).decode("utf8", errors="strict").strip()

    def get(self, key: str) -> str | None:
        reply = self.command("GET", key)
        if reply == "$-1":
            return None
        lines = reply.split("\r\n")
        if len(lines) >= 2:
            return lines[1]
        return None

    def set(self, key: str, value: str) -> None:
        reply = self.command("SET", key, value)
        if not reply.startswith("+OK"):
            raise RuntimeError(reply)


def encode(parts: tuple[str, ...]) -> bytes:
    out = f"*{len(parts)}\r\n"
    for part in parts:
        encoded = part.encode("utf8")
        out += f"${len(encoded)}\r\n"
        out += part + "\r\n"
    return out.encode("utf8")
