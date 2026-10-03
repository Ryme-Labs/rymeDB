import { strict as assert } from "node:assert";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { describe, it } from "node:test";
import { fileURLToPath } from "node:url";

const root = join(dirname(fileURLToPath(import.meta.url)), "..", "..", "..");
const html = readFileSync(join(root, "apps", "dashboard", "web", "index.html"), "utf8");
const spec = readFileSync(join(root, "schemas", "openapi", "rest.yaml"), "utf8");

function specPaths(): Set<string> {
  const out = new Set<string>();
  for (const line of spec.split("\n")) {
    const match = /^  (\/[^ :]*)/.exec(line);
    if (match) out.add(match[1]!.replace(/{[^}]*}/g, ":param"));
  }
  return out;
}

describe("dashboard web console", () => {
  it("covers every admin section", () => {
    for (const section of [
      "v-overview",
      "v-kv",
      "v-sql",
      "v-branches",
      "v-backups",
      "v-stream",
      "v-search",
      "v-ops",
      "v-observe",
    ]) {
      assert.ok(html.includes(`id="${section}"`), section);
    }
  });

  it("wires the scheduled drill button to its pane", () => {
    assert.ok(html.includes(`data-run="drill"`));
    assert.ok(html.includes('"/v1/backups/drill"'));
    assert.ok(html.includes('id="bk-out"'));
  });

  it("wires stream actions including broadcast", () => {
    for (const action of ["streamGo", "streamStop", "topicAppend", "topicRead", "castPost", "castGo", "castStop", "presenceJoin", "presenceList"]) {
      assert.ok(html.includes(`data-run="${action}"`), action);
    }
    assert.ok(html.includes('"/v1/broadcast"'));
    assert.ok(html.includes('"/v1/presence/join"'));
    assert.ok(html.includes('id="bc-channel"'));
    assert.ok(html.includes('id="cast-out"'));
    assert.ok(html.includes('id="ps-channel"'));
    assert.ok(html.includes('id="ps-out"'));
  });

  it("wires observe actions to documented paths", () => {
    for (const action of ["rangeList", "rangeRoute", "rangeAutosplit", "rangeLoads", "slowLog", "traceList"]) {
      assert.ok(html.includes(`data-run="${action}"`), action);
    }
    assert.ok(html.includes('"/v1/ranges"'));
    assert.ok(html.includes('"/v1/ranges/autosplit"'));
    assert.ok(html.includes('"/v1/ranges/loads"'));
    assert.ok(html.includes('id="sl-table"'));
    assert.ok(html.includes('id="tr-table"'));
    assert.ok(html.includes('"/v1/observe/slow?limit=20"'));
    assert.ok(html.includes('"/v1/traces?limit=20"'));
  });

  it("only calls documented REST paths", () => {
    const documented = specPaths();
    const literals = new Set<string>();
    for (const match of html.matchAll(/"(\/(?:v1|rest|graphql|metrics|health|ready)[^"]*)"/g)) {
      literals.add(match[1]!.split("?")[0]!);
    }
    assert.ok(literals.size > 20, `expected many API paths, got ${literals.size}`);
    for (const literal of literals) {
      const covered = [...documented].some(
        (path) => path === literal || path.startsWith(literal) || literal.startsWith(path),
      );
      assert.ok(covered, `undocumented path ${literal}`);
    }
  });

  it("wires auth actions including token issuance", () => {
    for (const action of ["authReg", "authVerify", "authToken"]) {
      assert.ok(html.includes(`data-run="${action}"`), action);
    }
    assert.ok(html.includes('"/v1/auth/token"'));
    assert.ok(html.includes('id="au-id"'));
  });

  it("streams over the documented websocket path", () => {
    assert.ok(html.includes("/v1/stream?table="));
    assert.ok(html.includes("/v1/broadcast/"));
  });
});
