import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const version = process.argv[2];

if (!version || !/^\d+\.\d+\.\d+$/.test(version)) {
  throw new Error("Usage: node scripts/set-sdk-version.mjs <major.minor.patch>");
}

function replaceOnce(file, pattern, replacement) {
  const source = fs.readFileSync(file, "utf8");
  if (!pattern.test(source)) {
    throw new Error(`Expected version field not found in ${path.relative(root, file)}`);
  }
  const updated = source.replace(pattern, replacement);
  if (source !== updated) {
    fs.writeFileSync(file, updated);
  }
}

const npmManifest = path.join(root, "sdks/js/package.json");
const npmPackage = JSON.parse(fs.readFileSync(npmManifest, "utf8"));
npmPackage.version = version;
const updatedNpmManifest = `${JSON.stringify(npmPackage, null, 2)}\n`;
if (fs.readFileSync(npmManifest, "utf8") !== updatedNpmManifest) {
  fs.writeFileSync(npmManifest, updatedNpmManifest);
}

replaceOnce(
  path.join(root, "sdks/rust/Cargo.toml"),
  /(^\[package\][\s\S]*?^version = ")[^"]+("\s*$)/m,
  `$1${version}$2`,
);
replaceOnce(
  path.join(root, "sdks/rust/Cargo.lock"),
  /(^\[\[package\]\]\nname = "rymedb-client"\nversion = ")[^"]+("$)/m,
  `$1${version}$2`,
);
