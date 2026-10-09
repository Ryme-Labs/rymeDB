import json

import httpx
import pytest
from rymedb import RymeError, RymeHttpClient


def make_client(handler):
    transport = httpx.MockTransport(handler)
    client = RymeHttpClient("http://test", api_key="k")
    client.client = httpx.Client(base_url="http://test", transport=transport)
    return client


def test_reads_ready_and_metrics():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url).replace("http://test", "")
        if request.url.path == "/ready":
            return httpx.Response(
                200,
                json={
                    "ready": True,
                    "node": "ryme-0",
                    "commit": 9,
                    "leader": True,
                    "cluster": False,
                },
            )
        if request.url.path == "/v1/observe/slow":
            return httpx.Response(200, json={"entries": []})
        if request.url.path == "/v1/traces":
            return httpx.Response(200, json={"spans": []})
        return httpx.Response(
            200,
            json={
                "node": "ryme-0",
                "uptime_secs": 31,
                "commit": 9,
                "rest_count": 4,
                "rest_mean_micros": 12.5,
                "rest_max_micros": 40,
            },
        )

    client = make_client(handler)
    ready = client.ready()
    assert ready == {
        "ready": True,
        "node": "ryme-0",
        "commit": 9,
        "leader": True,
        "cluster": False,
    }
    assert seen["url"] == "/ready"
    metrics = client.metrics()
    assert metrics["node"] == "ryme-0"
    assert metrics["uptime_secs"] == 31
    assert metrics["rest_count"] == 4
    assert seen["url"] == "/metrics"
    assert client.slow_log()["entries"] == []
    assert seen["url"] == "/v1/observe/slow"
    client.slow_log(5)
    assert seen["url"] == "/v1/observe/slow?limit=5"
    client.slow_log(5, "docs")
    assert seen["url"] == "/v1/observe/slow?limit=5&table=docs"
    assert client.traces()["spans"] == []
    assert seen["url"] == "/v1/traces"
    client.traces(5)
    assert seen["url"] == "/v1/traces?limit=5"
    client.traces(5, "kv_put", "docs")
    assert seen["url"] == "/v1/traces?limit=5&name=kv_put&table=docs"


def test_kv_put_sends_ttl_query_and_bearer_auth():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["method"] = request.method
        seen["url"] = str(request.url).replace("http://test", "")
        seen["auth"] = request.headers.get("authorization", "")
        seen["body"] = request.content.decode("utf8")
        return httpx.Response(200, json={"commit": 3})

    client = make_client(handler)
    out = client.kv_put("docs", "a", '{"x":1}', ttl=60)
    assert out == {"commit": 3}
    assert seen["method"] == "PUT"
    assert seen["url"] == "/v1/kv/docs/a?ttl=60"
    assert seen["auth"] == "Bearer k"
    assert seen["body"] == '{"x":1}'


def test_sql_and_cluster_replace_payloads():
    bodies = []

    def handler(request: httpx.Request) -> httpx.Response:
        bodies.append((str(request.url), json.loads(request.content.decode("utf8"))))
        return httpx.Response(200, json={"ok": True})

    client = make_client(handler)
    client.sql("SELECT * FROM docs KEY $1", ["a"])
    assert bodies[0][0].endswith("/v1/sql")
    assert bodies[0][1] == {"sql": "SELECT * FROM docs KEY $1", "params": ["a"]}
    client.cluster_replace(
        [
            {"id": 0, "addr": "127.0.0.1:9000"},
            {"id": 1, "addr": "127.0.0.1:9001"},
        ]
    )
    assert bodies[1][0].endswith("/v1/cluster/replace")
    assert bodies[1][1] == {
        "members": [
            {"id": 0, "addr": "127.0.0.1:9000"},
            {"id": 1, "addr": "127.0.0.1:9001"},
        ]
    }


def test_error_maps_to_typed_ryme_error():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(400, json={"error": "cluster"})

    client = make_client(handler)
    with pytest.raises(RymeError) as exc:
        client.cluster_members()
    assert exc.value.status == 400
    assert "cluster" in exc.value.body


def test_backup_endpoints():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["method"] = request.method
        seen["url"] = str(request.url).replace("http://test", "")
        seen["body"] = request.content.decode("utf8")
        return httpx.Response(200, json={"ok": True})
    client = make_client(handler)
    client.latest_checkpoint()
    assert (seen["method"], seen["url"]) == ("GET", "/v1/backups/latest")
    client.pitr(1735689600)
    assert (seen["method"], seen["url"]) == (
        "GET",
        "/v1/backups/pitr?target=1735689600",
    )
    client.restore(1735689600)
    assert (seen["method"], seen["url"]) == (
        "POST",
        "/v1/backups/restore?target=1735689600",
    )
    client.archive("nightly-042")
    assert seen["url"] == "/v1/backups/archive"
    assert json.loads(seen["body"]) == {"backup_id": "nightly-042"}
    client.archives()
    assert (seen["method"], seen["url"]) == ("GET", "/v1/backups/archives")
    client.backup_drill()
    assert (seen["method"], seen["url"]) == ("GET", "/v1/backups/drill")
    client.backup_copy("nightly-042")
    assert (seen["method"], seen["url"]) == ("POST", "/v1/backups/copy")
    assert json.loads(seen["body"]) == {"backup_id": "nightly-042"}
    client.ranges()
    assert (seen["method"], seen["url"]) == ("GET", "/v1/ranges")
    client.ranges("m")
    assert (seen["method"], seen["url"]) == ("GET", "/v1/ranges?key=m")
    client.range_split("range-0", "m", "range-a", "range-b", 0)
    assert (seen["method"], seen["url"]) == ("POST", "/v1/ranges/split")
    assert json.loads(seen["body"]) == {
        "id": "range-0",
        "mid": "m",
        "left_id": "range-a",
        "right_id": "range-b",
        "expected_epoch": 0,
    }
    client.range_merge("range-a", "range-b", "range-c", 1, 1)
    assert (seen["method"], seen["url"]) == ("POST", "/v1/ranges/merge")
    assert json.loads(seen["body"]) == {
        "left_id": "range-a",
        "right_id": "range-b",
        "merged_id": "range-c",
        "expected_left_epoch": 1,
        "expected_right_epoch": 1,
    }
    client.range_autosplit()
    assert (seen["method"], seen["url"]) == ("POST", "/v1/ranges/autosplit")
    client.range_autosplit(10)
    assert (seen["method"], seen["url"]) == ("POST", "/v1/ranges/autosplit?min_writes=10")
    client.range_loads()
    assert (seen["method"], seen["url"]) == ("GET", "/v1/ranges/loads")
    client.auth_token("ada", "correct-horse")
    assert (seen["method"], seen["url"]) == ("POST", "/v1/auth/token")
    assert json.loads(seen["body"]) == {"id": "ada", "password": "correct-horse", "code": None}
    client.auth_token("ada", "correct-horse", "123456")
    assert json.loads(seen["body"])["code"] == "123456"
    client.auth_revoke("ryme_deadbeef")
    assert (seen["method"], seen["url"]) == ("DELETE", "/v1/auth/keys")
    assert json.loads(seen["body"]) == {"key": "ryme_deadbeef"}
    client.passkey_register("ada", "cred-9", "cHVi")
    assert (seen["method"], seen["url"]) == ("POST", "/v1/auth/passkey/register")
    assert json.loads(seen["body"]) == {"user": "ada", "credential_id": "cred-9", "public_key": "cHVi"}
    client.passkey_verify("ada", "cred-9", "YXV0aA", "Y2xpZW50", "c2ln")
    assert (seen["method"], seen["url"]) == ("POST", "/v1/auth/passkey/verify")
    assert json.loads(seen["body"])["signature"] == "c2ln"


def test_migrate_apply_and_ledger():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["method"] = request.method
        seen["url"] = str(request.url).replace("http://test", "")
        seen["body"] = request.content.decode("utf8")
        return httpx.Response(200, json={"ok": True})

    client = make_client(handler)
    client.migrate_apply("m1", "INSERT INTO docs KEY 'k1' VALUE 'v1'")
    assert (seen["method"], seen["url"]) == ("POST", "/v1/migrate/apply")
    assert json.loads(seen["body"]) == {
        "id": "m1",
        "sql": "INSERT INTO docs KEY 'k1' VALUE 'v1'",
        "author": None,
    }
    client.migrate_apply("m1", "INSERT INTO docs KEY 'k1' VALUE 'v1'", "ada")
    assert json.loads(seen["body"])["author"] == "ada"
    client.migrate_ledger()
    assert (seen["method"], seen["url"]) == ("GET", "/v1/migrate/ledger")


def test_socket_url_uses_ws_scheme_and_api_key_query():
    client = RymeHttpClient("https://db.example.com:8080/", api_key="k")
    assert (
        client._socket_url("/v1/stream?table=docs")
        == "wss://db.example.com:8080/v1/stream?table=docs&api_key=k"
    )
    plain = RymeHttpClient("http://127.0.0.1:8080")
    plain.api_key = ""
    assert (
        plain._socket_url("/v1/stream?table=docs")
        == "ws://127.0.0.1:8080/v1/stream?table=docs"
    )


def _ws_stub(frames):
    import base64
    import hashlib
    import socket
    import threading

    requests = []
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    port = listener.getsockname()[1]

    def serve():
        conn, _ = listener.accept()
        raw = b""
        while b"\r\n\r\n" not in raw:
            chunk = conn.recv(4096)
            if not chunk:
                return
            raw += chunk
        head = raw.split(b"\r\n\r\n")[0].decode("utf8")
        requests.append(head.split("\r\n")[0].split(" ")[1])
        key = ""
        for line in head.split("\r\n"):
            if line.lower().startswith("sec-websocket-key:"):
                key = line.split(":", 1)[1].strip()
        accept = base64.b64encode(
            hashlib.sha1(
                (key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode("utf8")
            ).digest()
        ).decode("utf8")
        conn.sendall(
            (
                "HTTP/1.1 101 Switching Protocols\r\n"
                "Upgrade: websocket\r\n"
                "Connection: Upgrade\r\n"
                f"Sec-WebSocket-Accept: {accept}\r\n\r\n"
            ).encode()
        )
        for frame in frames:
            payload = frame.encode("utf8")
            conn.sendall(bytes([0x81, len(payload)]) + payload)
        conn.sendall(bytes([0x88, 0x00]))
        conn.close()
        listener.close()

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    return port, requests, thread


def test_subscribe_table_receives_change_records():
    from rymedb import Subscription

    frames = [
        (
            '{"tenant":"t","database":"d","branch":"main","table":"docs",'
            '"op":"INSERT","pk":[49],"after":[50],"commit_ts":7,"sequence":1}'
        )
    ]
    port, requests, thread = _ws_stub(frames)
    client = RymeHttpClient(f"http://127.0.0.1:{port}", api_key="k")
    sub = client.subscribe_table("docs", timeout=5.0)
    assert isinstance(sub, Subscription)
    messages = list(sub)
    sub.close()
    thread.join(timeout=5.0)
    assert requests[0] == "/v1/stream?table=docs&api_key=k"
    assert len(messages) == 1
    assert messages[0]["op"] == "INSERT"
    assert messages[0]["commit_ts"] == 7


def test_subscribe_broadcast_receives_messages():
    from rymedb import Subscription

    frames = ['{"channel":"lobby","from":"ada","payload":{"hello":true},"commit_ts":3,"sequence":1}']
    port, requests, thread = _ws_stub(frames)
    client = RymeHttpClient(f"http://127.0.0.1:{port}", api_key="k")
    sub = client.subscribe_broadcast("lobby", timeout=5.0)
    assert isinstance(sub, Subscription)
    messages = list(sub)
    sub.close()
    thread.join(timeout=5.0)
    assert requests[0] == "/v1/broadcast/lobby?api_key=k"
    assert len(messages) == 1
    assert messages[0]["channel"] == "lobby"
    assert messages[0]["payload"] == {"hello": True}


def test_subscribe_table_appends_from_watermark():
    from rymedb import Subscription

    port, requests, thread = _ws_stub([])
    client = RymeHttpClient(f"http://127.0.0.1:{port}", api_key="k")
    sub = client.subscribe_table("docs", timeout=5.0, from_commit=7)
    assert isinstance(sub, Subscription)
    sub.close()
    thread.join(timeout=5.0)
    assert requests[0] == "/v1/stream?table=docs&from=7&api_key=k"


def test_subscribe_table_appends_exact_sequence_cursor():
    from rymedb import Subscription

    port, requests, thread = _ws_stub([])
    client = RymeHttpClient(f"http://127.0.0.1:{port}", api_key="k")
    sub = client.subscribe_table("docs", timeout=5.0, from_sequence=11)
    assert isinstance(sub, Subscription)
    sub.close()
    thread.join(timeout=5.0)
    assert requests[0] == "/v1/stream?table=docs&from_sequence=11&api_key=k"


def test_subscribe_query_receives_snapshot_then_update():
    frames = [
        '{"type":"snapshot","commit":7,"rows":[{"pk":"k1","value":"one"}]}',
        (
            '{"type":"update","commit":8,"rows":[{"pk":"k2","value":"two"}],'
            '"truncated":false}'
        ),
    ]
    port, requests, thread = _ws_stub(frames)
    client = RymeHttpClient(f"http://127.0.0.1:{port}", api_key="k")
    sub = client.subscribe_query("docs", limit=100, timeout=5.0)
    messages = list(sub)
    sub.close()
    thread.join(timeout=5.0)
    assert requests[0] == "/v1/query-stream?table=docs&limit=100&api_key=k"
    assert [m["type"] for m in messages] == ["snapshot", "update"]
    assert messages[1]["commit"] == 8


def test_kv_get_returns_raw_text():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["method"] = request.method
        seen["url"] = str(request.url).replace("http://test", "")
        return httpx.Response(200, content=b'{"n":1}')

    client = make_client(handler)
    got = client.kv_get("docs", "a")
    assert got == '{"n":1}'
    assert isinstance(got, str)
    assert seen["method"] == "GET"
    assert seen["url"] == "/v1/kv/docs/a"
