import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const requested = process.argv[2];
const version = requested?.replace(/^rymedb-v/, "").replace(/^v/, "");

if (!version || !/^\d+\.\d+\.\d+$/.test(version)) {
  throw new Error("Usage: node scripts/validate-release-version.mjs <version|release-tag>");
}

function read(relativePath) {
  return fs.readFileSync(path.join(root, relativePath), "utf8");
}

function assertVersion(label, actual) {
  if (actual !== version) {
    throw new Error(`${label} is ${actual ?? "missing"}; release tag is ${version}`);
  }
}

const cargo = read("Cargo.toml");
const workspaceVersion = cargo.match(/^\[workspace\.package\][\s\S]*?^version = "([^"]+)"/m)?.[1];
assertVersion("Cargo workspace", workspaceVersion);
const workspaceDependencies = cargo.match(/^\[workspace\.dependencies\]([\s\S]*?)(?=^\[|$)/m)?.[1] ?? "";
for (const match of workspaceDependencies.matchAll(/^[^\r\n]*path\s*=\s*"[^"]+"[^\r\n]*version\s*=\s*"([^"]+)"/gm)) {
  assertVersion("Cargo workspace path dependency", match[1]);
}

const cargoLock = read("Cargo.lock");
const memberPaths = [...cargo.matchAll(/^\s*"((?:crates\/[^"\r\n]+)|xtask)"\s*,?\s*$/gm)]
  .map((match) => match[1]);
if (memberPaths.length === 0) {
  throw new Error("Cargo workspace members are missing");
}
for (const memberPath of memberPaths) {
  const manifest = read(path.join(memberPath, "Cargo.toml"));
  const name = manifest.match(/^name = "([^"]+)"/m)?.[1];
  const packageVersion = manifest.match(/^version = "([^"]+)"/m)?.[1];
  if (!name) {
    throw new Error(`${memberPath}/Cargo.toml has no package name`);
  }
  if (packageVersion && packageVersion !== "${workspace.package.version}") {
    assertVersion(`${name} package`, packageVersion);
  } else if (!/^version\.workspace = true$/m.test(manifest)) {
    throw new Error(`${name} must inherit the workspace version or declare it explicitly`);
  }
  const escapedName = name.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const lockVersion = cargoLock.match(
    new RegExp(`^\\[\\[package\\]\\]\\r?\\nname = "${escapedName}"\\r?\\nversion = "([^"]+)"`, "m"),
  )?.[1];
  assertVersion(`${name} Cargo.lock entry`, lockVersion);
}

for (const relativePath of ["package.json", "apps/cli/package.json", "dashboard/package.json"]) {
  const manifest = JSON.parse(read(relativePath));
  assertVersion(relativePath, manifest.version);
}
const dashboardLock = JSON.parse(read("dashboard/package-lock.json"));
assertVersion("dashboard package-lock root", dashboardLock.packages?.[""]?.version);

const chart = read("deploy/k8s/helm/rymeDB/Chart.yaml");
assertVersion("Helm chart", chart.match(/^version:\s*"?([^"\r\n]+)"?\s*$/m)?.[1]);
assertVersion("Helm appVersion", chart.match(/^appVersion:\s*"?([^"\r\n]+)"?\s*$/m)?.[1]);
const values = read("deploy/k8s/helm/rymeDB/values.yaml");
assertVersion("Helm image tag", values.match(/^\s+tag:\s*"?([^"\r\n]+)"?\s*$/m)?.[1]);
const openapi = read("schemas/openapi/rest.yaml");
assertVersion("OpenAPI version", openapi.match(/^  version:\s*"?([^"\r\n]+)"?\s*$/m)?.[1]);
const statefulset = read("deploy/k8s/base/statefulset.yaml");
assertVersion(
  "Kubernetes image tag",
  statefulset.match(/^\s+image:\s+ghcr\.io\/ryme-labs\/rymedb:([^\s]+)\s*$/m)?.[1],
);
if (!read("CHANGELOG.md").includes(`## [${version}]`)) {
  throw new Error(`CHANGELOG.md has no ${version} release entry`);
}

console.log(`Release version ${version} matches ${memberPaths.length} Rust packages and deployment metadata.`);
