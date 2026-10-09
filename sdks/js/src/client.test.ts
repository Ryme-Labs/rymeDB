import { strict as assert } from "node:assert";
import { createHash } from "node:crypto";
import { createServer } from "node:http";
import { createServer as createTcpServer, Socket } from "node:net";
import { describe, it } from "node:test";
import {
  RymeError,
  RymeHttpClient,
  subscribeBroadcast,
  subscribePresence,
  subscribeQuery,
  subscribeTable,
} from "./index.js";

interface Seen {
  method: string;
  url: string;
  auth: string;
  body: string;
}

function stub(handler: (seen: Seen) => { status: number; body: string }) {
  let last: Seen | null = null;
  const server = createServer((req, res) => {
    let body = "";
    req.on("data", (chunk: Buffer) => {
      body += chunk.toString("utf8");
    });
    req.on("end", () => {
      last = {
        method: req.method ?? "",
        url: req.url ?? "",
        auth: String(req.headers["authorization"] ?? ""),
        body,
      };
      const out = handler(last);
      res.writeHead(out.status, { "content-type": "application/json" });
      res.end(out.body);
    });
  });
  return {
    server,
    seen: () => last as unknown as Seen,
  };
}

describe("RymeHttpClient", () => {
  it("returns kv values as raw text", async () => {
    const { server, seen } = stub(() => ({ status: 200, body: `{"n":1}` }));
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const port = (server.address() as { port: number }).port;
    const client = new RymeHttpClient({ base: `http://127.0.0.1:${port}` });
    const got = await client.kvGet("docs", "a");
    assert.equal(got, `{"n":1}`);
    assert.equal(seen().method, "GET");
    assert.equal(seen().url, "/v1/kv/docs/a");
    server.close();
  });

  it("reads ready and metrics", async () => {
    const { server, seen } = stub((s) =>
      s.url === "/ready"
        ? {
            status: 200,
            body: `{"ready":true,"node":"ryme-0","commit":9,"leader":true,"cluster":false}`,
          }
        : s.url.startsWith("/v1/observe/slow")
          ? { status: 200, body: `{"entries":[]}` }
          : s.url.startsWith("/v1/traces")
            ? { status: 200, body: `{"spans":[]}` }
            : {
              status: 200,
              body: `{"node":"ryme-0","uptime_secs":31,"commit":9,"rest_count":4,"rest_mean_micros":12.5,"rest_max_micros":40}`,
            },
    );
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const port = (server.address() as { port: number }).port;
    const client = new RymeHttpClient({ base: `http://127.0.0.1:${port}` });
    const ready = await client.ready();
    assert.equal(ready.ready, true);
    assert.equal(ready.node, "ryme-0");
    assert.equal(ready.commit, 9);
    assert.equal(ready.leader, true);
    assert.equal(ready.cluster, false);
    assert.equal(seen().url, "/ready");
    const metrics = await client.metrics();
    assert.equal(metrics.node, "ryme-0");
    assert.equal(metrics.uptime_secs, 31);
    assert.equal(metrics.rest_count, 4);
    assert.equal(seen().url, "/metrics");
    assert.deepEqual(await client.slowLog(), { entries: [] });
    assert.equal(seen().url, "/v1/observe/slow");
    await client.slowLog(5);
    assert.equal(seen().url, "/v1/observe/slow?limit=5");
    await client.slowLog(5, "docs");
    assert.equal(seen().url, "/v1/observe/slow?limit=5&table=docs");
    assert.deepEqual(await client.traces(), { spans: [] });
    assert.equal(seen().url, "/v1/traces");
    await client.traces(5);
    assert.equal(seen().url, "/v1/traces?limit=5");
    await client.traces(5, "kv_put", "docs");
    assert.equal(seen().url, "/v1/traces?limit=5&name=kv_put&table=docs");
    server.close();
  });

  it("sends kv put with ttl query and bearer auth", async () => {
    const { server, seen } = stub(() => ({ status: 200, body: `{"commit":3}` }));
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const port = (server.address() as { port: number }).port;
    const client = new RymeHttpClient({ base: `http://127.0.0.1:${port}`, apiKey: "k" });
    const out = (await client.kvPut("docs", "a", `{"x":1}`, 60)) as { commit: number };
    assert.equal(out.commit, 3);
    assert.equal(seen().method, "PUT");
    assert.equal(seen().url, "/v1/kv/docs/a?ttl=60");
    assert.equal(seen().auth, "Bearer k");
    assert.equal(seen().body, `{"x":1}`);
    server.close();
  });

  it("sends Supabase-style REST CRUD requests", async () => {
    const { server, seen } = stub(() => ({ status: 200, body: `{"ok":true}` }));
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const port = (server.address() as { port: number }).port;
    const client = new RymeHttpClient({ base: `http://127.0.0.1:${port}`, apiKey: "k" });

    await client.restInsert("people", [{ id: "p1", name: "Ada" }]);
    assert.equal(seen().method, "POST");
    assert.equal(seen().url, "/rest/v1/people");
    assert.deepEqual(JSON.parse(seen().body), [{ id: "p1", name: "Ada" }]);
    await client.restUpsert("people", { id: "p1", name: "Ada" });
    assert.equal(seen().method, "POST");
    await client.restUpdate("people", "id=eq.p1", { name: "Grace" });
    assert.equal(seen().method, "PATCH");
    assert.equal(seen().url, "/rest/v1/people?id=eq.p1");
    await client.restDeleteWhere("people", "id=eq.p1");
    assert.equal(seen().method, "DELETE");
    assert.equal(seen().url, "/rest/v1/people?id=eq.p1");
    server.close();
  });

  it("maps sql params and cluster replace payloads", async () => {
    const { server, seen } = stub(() => ({ status: 200, body: `{"ok":true}` }));
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const port = (server.address() as { port: number }).port;
    const client = new RymeHttpClient({ base: `http://127.0.0.1:${port}` });
    await client.sql("SELECT * FROM docs KEY $1", ["a"]);
    assert.equal(seen().url, "/v1/sql");
    assert.deepEqual(JSON.parse(seen().body), { sql: "SELECT * FROM docs KEY $1", params: ["a"] });
    await client.clusterReplace([
      { id: 0, addr: "127.0.0.1:9000" },
      { id: 1, addr: "127.0.0.1:9001" },
    ]);
    assert.equal(seen().url, "/v1/cluster/replace");
    assert.deepEqual(JSON.parse(seen().body), {
      members: [
        { id: 0, addr: "127.0.0.1:9000" },
        { id: 1, addr: "127.0.0.1:9001" },
      ],
    });
    const status = await client.clusterMembers().then(
      () => "unexpected",
      (error: unknown) => (error instanceof RymeError ? "typed" : "untyped"),
    );
    assert.equal(status, "unexpected");
    server.close();
  });

  it("covers the backup endpoints", async () => {
    const { server, seen } = stub(() => ({ status: 200, body: `{"ok":true}` }));
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const port = (server.address() as { port: number }).port;
    const client = new RymeHttpClient({ base: `http://127.0.0.1:${port}`, apiKey: "k" });
    await client.latestCheckpoint();
    assert.equal(seen().method, "GET");
    assert.equal(seen().url, "/v1/backups/latest");
    await client.pitr(1735689600);
    assert.equal(seen().url, "/v1/backups/pitr?target=1735689600");
    await client.restore(1735689600);
    assert.equal(seen().method, "POST");
    assert.equal(seen().url, "/v1/backups/restore?target=1735689600");
    await client.archive("nightly-042");
    assert.equal(seen().url, "/v1/backups/archive");
    assert.deepEqual(JSON.parse(seen().body), { backup_id: "nightly-042" });
    await client.archives();
    assert.equal(seen().method, "GET");
    assert.equal(seen().url, "/v1/backups/archives");
    await client.backupDrill();
    assert.equal(seen().method, "GET");
    assert.equal(seen().url, "/v1/backups/drill");
    await client.backupCopy("nightly-042");
    assert.equal(seen().method, "POST");
    assert.equal(seen().url, "/v1/backups/copy");
    assert.deepEqual(JSON.parse(seen().body), { backup_id: "nightly-042" });
    await client.ranges();
    assert.equal(seen().method, "GET");
    assert.equal(seen().url, "/v1/ranges");
    await client.ranges("m");
    assert.equal(seen().method, "GET");
    assert.equal(seen().url, "/v1/ranges?key=m");
    await client.rangeSplit("range-0", "m", "range-a", "range-b", 0);
    assert.equal(seen().method, "POST");
    assert.equal(seen().url, "/v1/ranges/split");
    assert.deepEqual(JSON.parse(seen().body), {
      id: "range-0",
      mid: "m",
      left_id: "range-a",
      right_id: "range-b",
      expected_epoch: 0,
    });
    await client.rangeMerge("range-a", "range-b", "range-c", 1, 1);
    assert.equal(seen().method, "POST");
    assert.equal(seen().url, "/v1/ranges/merge");
    assert.deepEqual(JSON.parse(seen().body), {
      left_id: "range-a",
      right_id: "range-b",
      merged_id: "range-c",
      expected_left_epoch: 1,
      expected_right_epoch: 1,
    });
    await client.rangeAutosplit();
    assert.equal(seen().method, "POST");
    assert.equal(seen().url, "/v1/ranges/autosplit");
    await client.rangeAutosplit(10);
    assert.equal(seen().method, "POST");
    assert.equal(seen().url, "/v1/ranges/autosplit?min_writes=10");
    await client.rangeLoads();
    assert.equal(seen().method, "GET");
    assert.equal(seen().url, "/v1/ranges/loads");
    await client.authToken("ada", "correct-horse");
    assert.equal(seen().method, "POST");
    assert.equal(seen().url, "/v1/auth/token");
    assert.deepEqual(JSON.parse(seen().body), { id: "ada", password: "correct-horse", code: null });
    await client.authToken("ada", "correct-horse", "123456");
    assert.deepEqual(JSON.parse(seen().body).code, "123456");
    await client.authRevoke("ryme_deadbeef");
    assert.equal(seen().method, "DELETE");
    assert.equal(seen().url, "/v1/auth/keys");
    assert.deepEqual(JSON.parse(seen().body), { key: "ryme_deadbeef" });
    await client.passkeyRegister("ada", "cred-9", "cHVi");
    assert.equal(seen().method, "POST");
    assert.equal(seen().url, "/v1/auth/passkey/register");
    assert.deepEqual(JSON.parse(seen().body), { user: "ada", credential_id: "cred-9", public_key: "cHVi" });
    await client.passkeyVerify("ada", "cred-9", "YXV0aA", "Y2xpZW50", "c2ln");
    assert.equal(seen().url, "/v1/auth/passkey/verify");
    assert.deepEqual(JSON.parse(seen().body).signature, "c2ln");
    await client.migrateApply("m1", "INSERT INTO docs KEY 'k1' VALUE 'v1'");
    assert.equal(seen().method, "POST");
    assert.equal(seen().url, "/v1/migrate/apply");
    assert.deepEqual(JSON.parse(seen().body), {
      id: "m1",
      sql: "INSERT INTO docs KEY 'k1' VALUE 'v1'",
      author: null,
    });
    await client.migrateApply("m1", "INSERT INTO docs KEY 'k1' VALUE 'v1'", "ada");
    assert.deepEqual(JSON.parse(seen().body).author, "ada");
    await client.migrateLedger();
    assert.equal(seen().method, "GET");
    assert.equal(seen().url, "/v1/migrate/ledger");
    server.close();
  });

  it("throws RymeError with status on failure", async () => {    const { server } = stub(() => ({ status: 400, body: `{"error":"cluster"}` }));
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const port = (server.address() as { port: number }).port;
    const client = new RymeHttpClient({ base: `http://127.0.0.1:${port}` });
    await assert.rejects(client.clusterMembers(), (error: unknown) => {
      assert.ok(error instanceof RymeError);
      assert.equal(error.status, 400);
      assert.ok(error.body.includes("cluster"));
      return true;
    });
    server.close();
  });

  it("streams table changes over websocket", async () => {
    const frames = [
      `{"tenant":"t","database":"d","branch":"main","table":"docs","op":"INSERT","pk":[49],"before":null,"after":[50],"commit_ts":7,"tx_id":7,"sequence":1}`,
    ];
    const { server, requests, close } = wsStub(frames);
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const port = (server.address() as { port: number }).port;
    const received: unknown[] = [];
    const sub = subscribeTable(`http://127.0.0.1:${port}`, "docs", (record) => {
      received.push(record);
    });
    await sub.ready;
    await waitFor(() => received.length > 0);
    sub.close();
    assert.equal(requests[0], "/v1/stream?table=docs");
    assert.equal((received[0] as { op: string }).op, "INSERT");
    assert.equal((received[0] as { commit_ts: number }).commit_ts, 7);
    close();
  });

  it("streams broadcast messages over websocket", async () => {
    const frames = [
      `{"channel":"lobby","from":"ada","payload":{"hello":true},"commit_ts":3,"sequence":1}`,
    ];
    const { server, requests, close } = wsStub(frames);
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const port = (server.address() as { port: number }).port;
    const received: unknown[] = [];
    const sub = subscribeBroadcast(`http://127.0.0.1:${port}`, "lobby", (record) => {
      received.push(record);
    });
    await sub.ready;
    await waitFor(() => received.length > 0);
    sub.close();
    assert.equal(requests[0], "/v1/broadcast/lobby");
    assert.equal((received[0] as { channel: string }).channel, "lobby");
    close();
  });

  it("streams presence snapshots and events over websocket", async () => {
    const frames = [
      `{"type":"presence_state","channel":"room","members":[]}`,
      `{"type":"join","channel":"room","member":"ada","state":{"typing":true},"expires_unix":99}`,
    ];
    const { server, requests, close } = wsStub(frames);
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const port = (server.address() as { port: number }).port;
    const received: Array<{ type: string; member?: string }> = [];
    const sub = subscribePresence(`http://127.0.0.1:${port}`, "room", (event) => {
      received.push(event);
    });
    await sub.ready;
    await waitFor(() => received.length === 2);
    sub.close();
    assert.equal(requests[0], "/v1/presence/room/stream");
    assert.equal(received[0]?.type, "presence_state");
    assert.equal(received[1]?.member, "ada");
    close();
  });

  it("appends the resume watermark to stream urls", async () => {
    const frames: string[] = [];
    const { server, requests, close } = wsStub(frames);
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const port = (server.address() as { port: number }).port;
    const sub = subscribeTable(
      `http://127.0.0.1:${port}`,
      "docs",
      () => {},
      { from: 7 },
    );
    await sub.ready;
    sub.close();
    assert.equal(requests[0], "/v1/stream?table=docs&from=7");
    close();
  });

  it("appends the exact sequence cursor to stream urls", async () => {
    const frames: string[] = [];
    const { server, requests, close } = wsStub(frames);
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const port = (server.address() as { port: number }).port;
    const sub = subscribeTable(
      `http://127.0.0.1:${port}`,
      "docs",
      () => {},
      { fromSequence: 11 },
    );
    await sub.ready;
    sub.close();
    assert.equal(requests[0], "/v1/stream?table=docs&from_sequence=11");
    close();
  });

  it("resumes table streams from the latest sequence after disconnect", async () => {
    const first = `{"tenant":"t","database":"d","branch":"main","table":"docs","op":"INSERT","pk":[49],"before":null,"after":[50],"commit_ts":7,"tx_id":7,"sequence":1}`;
    const second = `{"tenant":"t","database":"d","branch":"main","table":"docs","op":"INSERT","pk":[51],"before":null,"after":[52],"commit_ts":8,"tx_id":8,"sequence":2}`;
    const { server, requests, close } = wsStub([first], {
      closeAfterFrames: true,
      reconnectFrames: [second],
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const port = (server.address() as { port: number }).port;
    const received: ChangeRecordLike[] = [];
    const sub = subscribeTable(
      `http://127.0.0.1:${port}`,
      "docs",
      (record) => received.push(record),
      { reconnectDelayMs: 10 },
    );
    await sub.ready;
    await waitFor(() => received.length === 2);
    sub.close();
    assert.equal(requests[0], "/v1/stream?table=docs");
    assert.equal(requests[1], "/v1/stream?table=docs&from_sequence=1");
    assert.equal(received[1]?.sequence, 2);
    close();
  });

  it("streams query snapshots and updates over websocket", async () => {
    const frames = [
      `{"type":"snapshot","commit":7,"rows":[{"pk":"k1","value":"one"}]}`,
      `{"type":"update","commit":8,"rows":[{"pk":"k1","value":"one"},{"pk":"k2","value":"two"}],"truncated":false}`,
    ];
    const { server, requests, close } = wsStub(frames);
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const port = (server.address() as { port: number }).port;
    const received: Array<{ type: string; commit: number }> = [];
    const sub = subscribeQuery(
      `http://127.0.0.1:${port}`,
      "docs",
      (message) => {
        received.push(message);
      },
      { limit: 100 },
    );
    await sub.ready;
    await waitFor(() => received.length === 2);
    sub.close();
    assert.equal(requests[0], "/v1/query-stream?table=docs&limit=100");
    assert.equal(received[0]?.type, "snapshot");
    assert.equal(received[1]?.type, "update");
    assert.equal(received[1]?.commit, 8);
    close();
  });
});

interface ChangeRecordLike {
  sequence: number;
}

function wsStub(
  frames: string[],
  options: { closeAfterFrames?: boolean; reconnectFrames?: string[] } = {},
) {
  const requests: string[] = [];
  const sockets = new Set<Socket>();
  let connection = 0;
  const server = createTcpServer((socket: Socket) => {
    sockets.add(socket);
    socket.on("close", () => sockets.delete(socket));
    let buffer = Buffer.alloc(0);
    let upgraded = false;
    socket.on("data", (chunk: Buffer) => {
      if (upgraded) return;
      buffer = Buffer.concat([buffer, chunk]);
      const head = buffer.indexOf("\r\n\r\n");
      if (head < 0) return;
      const text = buffer.subarray(0, head).toString("utf8");
      const target = text.split("\r\n")[0]?.split(" ")[1] ?? "/";
      requests.push(target);
      const key = /sec-websocket-key:\s*(.+)/i.exec(text)?.[1]?.trim() ?? "";
      const accept = createHash("sha1")
        .update(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11")
        .digest("base64");
      socket.write(
        "HTTP/1.1 101 Switching Protocols\r\n" +
          "Upgrade: websocket\r\n" +
          "Connection: Upgrade\r\n" +
          `Sec-WebSocket-Accept: ${accept}\r\n\r\n`,
      );
      upgraded = true;
      const outgoing = connection++ === 0 ? frames : (options.reconnectFrames ?? frames);
      for (const frame of outgoing) {
        const payload = Buffer.from(frame, "utf8");
        const header = Buffer.from([0x81, payload.length]);
        socket.write(Buffer.concat([header, payload]));
      }
      if (options.closeAfterFrames) setImmediate(() => socket.end());
    });
  });
  return {
    server,
    requests,
    close: () => {
      for (const socket of sockets) socket.destroy();
      server.close();
    },
  };
}

async function waitFor(done: () => boolean): Promise<void> {
  const deadline = Date.now() + 5000;
  while (!done()) {
    if (Date.now() > deadline) throw new Error("timed out waiting for frames");
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}
