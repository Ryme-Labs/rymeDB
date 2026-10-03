import { strict as assert } from "node:assert";
import { createServer } from "node:http";
import { describe, it } from "node:test";
import { fetchOverview, renderOverview } from "./overview.js";

function stub(routes: Record<string, { status: number; body: string }>) {
  const server = createServer((req, res) => {
    const route = routes[`${req.method} ${req.url}`];
    if (!route) {
      res.writeHead(404, { "content-type": "application/json" });
      res.end(`{"error":"no route ${req.method} ${req.url}"}`);
      return;
    }
    res.writeHead(route.status, { "content-type": "application/json" });
    res.end(route.body);
  });
  return server;
}

describe("dashboard overview", () => {
  it("merges health, leader, cluster and shards", async () => {
    const server = stub({
      "GET /health": { status: 200, body: `"ok"` },
      "GET /ready": { status: 200, body: `{"leader":true}` },
      "GET /v1/cluster/members": {
        status: 200,
        body: `{"term":7,"leader":true,"commit":42,"members":[{"id":0,"addr":"127.0.0.1:9000","self":true}],"joint":[]}`,
      },
      "GET /v1/shards": { status: 200, body: `{"mode":"sharded","shards":2,"placement":[{"table":"docs","shard":1}]}` },
      "GET /v1/ranges": { status: 200, body: `[{"id":"range-a","start":[],"end":[109],"leader":"ryme-0","epoch":1}]` },
      "GET /v1/ranges/loads": { status: 200, body: `[{"id":"range-a","epoch":1,"writes":4}]` },
      "GET /v1/observe/slow?limit=5": { status: 200, body: `{"entries":[]}` },
      "GET /v1/traces?limit=5": { status: 200, body: `{"spans":[]}` },
      "GET /v1/backups/drill": {
        status: 200,
        body: `{"backup_id":"drill-1","verified_files":4,"at_unix":1790999999,"error":null}`,
      },
      "GET /metrics": { status: 404, body: `{}` },
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const port = (server.address() as { port: number }).port;
    const state = await fetchOverview(`http://127.0.0.1:${port}`, "k");
    assert.equal(state.health, true);
    assert.equal(state.ready, true);
    assert.equal(state.leader, true);
    assert.equal(state.cluster?.term, 7);
    assert.equal(state.cluster?.members.length, 1);
    assert.equal(state.shards?.mode, "sharded");
    assert.equal(state.shards?.placement?.length, 1);
    assert.equal(state.ranges?.length, 1);
    assert.equal(state.ranges?.[0]?.id, "range-a");
    assert.equal(state.loads?.length, 1);
    assert.equal(state.loads?.[0]?.writes, 4);
    assert.deepEqual(state.slow, []);
    assert.deepEqual(state.traces, []);
    assert.equal(state.drill?.backup_id, "drill-1");
    assert.equal(state.drill?.verified_files, 4);
    const text = renderOverview(state);
    assert.ok(text.includes("term=7"));
    assert.ok(text.includes("#0 127.0.0.1:9000 (self)"));
    assert.ok(text.includes("docs shard=1"));
    assert.ok(text.includes("ranges: count=1"));
    assert.ok(text.includes("range-a epoch=1"));
    assert.ok(text.includes("loads: count=1"));
    assert.ok(text.includes("range-a epoch=1 writes=4"));
    assert.ok(text.includes("slow: count=0"));
    assert.ok(text.includes("traces: count=0"));
    assert.ok(text.includes("drill: drill-1 files=4 at=1790999999"));
    server.close();
  });

  it("renders slow entries and degrades both gracefully", async () => {
    const server = stub({
      "GET /health": { status: 200, body: `"ok"` },
      "GET /ready": { status: 200, body: `{"leader":false}` },
      "GET /v1/cluster/members": { status: 400, body: `{"error":"cluster"}` },
      "GET /v1/shards": { status: 200, body: `{"mode":"single","shards":1}` },
      "GET /v1/ranges": { status: 500, body: `{"error":"lock"}` },
      "GET /v1/ranges/loads": { status: 200, body: `[]` },
      "GET /v1/observe/slow?limit=5": {
        status: 200,
        body: `{"entries":[{"kind":"sql","fingerprint":"COPY bulk","table":"bulk","micros":30931,"at_unix":1790956494}]}`,
      },
      "GET /v1/traces?limit=5": {
        status: 200,
        body: `{"spans":[{"trace_id":"t","span_id":"s","parent":null,"name":"kv_put","started_unix":1,"duration_micros":12,"attributes":[["table","docs"]]}]}`,
      },
      "GET /metrics": { status: 404, body: `{}` },
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const port = (server.address() as { port: number }).port;
    const state = await fetchOverview(`http://127.0.0.1:${port}`, "");
    assert.equal(state.ranges, null);
    assert.equal(state.slow?.length, 1);
    assert.equal(state.traces?.length, 1);
    assert.ok(state.errors.some((error) => error.startsWith("ranges:")));
    const text = renderOverview(state);
    assert.ok(text.includes("slow: count=1"));
    assert.ok(text.includes("sql COPY bulk 30931us"));
    assert.ok(text.includes("traces: count=1"));
    assert.ok(text.includes("kv_put 12us"));
    assert.ok(text.includes("warn: ranges:"));
    assert.ok(text.includes("drill: never run"));
    server.close();
  });

  it("degrades when the cluster endpoint is unavailable", async () => {
    const server = stub({
      "GET /health": { status: 200, body: `"ok"` },
      "GET /ready": { status: 200, body: `{"leader":false}` },
      "GET /v1/cluster/members": { status: 400, body: `{"error":"cluster"}` },
      "GET /v1/shards": { status: 200, body: `{"mode":"single","shards":1}` },
      "GET /v1/ranges/loads": { status: 200, body: `[]` },
      "GET /metrics": { status: 404, body: `{}` },
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const port = (server.address() as { port: number }).port;
    const state = await fetchOverview(`http://127.0.0.1:${port}`, "");
    assert.equal(state.health, true);
    assert.equal(state.cluster, null);
    assert.ok(state.errors.some((error) => error.startsWith("cluster:")));
    assert.ok(renderOverview(state).includes("warn: cluster:"));
    server.close();
  });
});
