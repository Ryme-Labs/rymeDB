import { strict as assert } from "node:assert";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { describe, it } from "node:test";
import { fileURLToPath } from "node:url";

const root = join(dirname(fileURLToPath(import.meta.url)), "..", "..", "..");
const cli = readFileSync(join(root, "apps", "cli", "src", "cli.ts"), "utf8");
const spec = readFileSync(join(root, "schemas", "openapi", "rest.yaml"), "utf8");

function specPaths(): Set<string> {
  const out = new Set<string>();
  for (const line of spec.split("\n")) {
    const match = /^  (\/[^ :]*)/.exec(line);
    if (match) out.add(match[1]!.replace(/{[^}]*}/g, ":param"));
  }
  return out;
}

function cliPaths(): Set<string> {
  const out = new Set<string>();
  for (const match of cli.matchAll(/"(\/(?:v1|rest|graphql|metrics|health|ready)[^"]*)"/g)) {
    out.add(match[1]!.split("?")[0]!);
  }
  for (const match of cli.matchAll(/`(\/(?:v1|rest)[^`$]*)/g)) {
    out.add(match[1]!);
  }
  return out;
}

describe("ryme cli", () => {
  it("only calls documented REST paths", () => {
    const documented = specPaths();
    const literals = cliPaths();
    assert.ok(literals.size > 20, `expected many API paths, got ${literals.size}`);
    for (const literal of literals) {
      const covered = [...documented].some(
        (path) => path === literal || path.startsWith(literal) || literal.startsWith(path),
      );
      assert.ok(covered, `undocumented path ${literal}`);
    }
  });

  it("covers recent observability and placement endpoints", () => {
    for (const path of [
      "/v1/ranges/loads",
      "/v1/ranges/autosplit",
      "/v1/traces",
      "/v1/observe/slow",
    ]) {
      assert.ok(cli.includes(`"${path}`) || cli.includes(`\`${path}`), path);
    }
  });
});
