# SDK releases

The supported SDK surface is intentionally limited to two packages:

| Ecosystem | Directory | Package |
| --- | --- | --- |
| npm/TypeScript | `sdks/js` | `@rymelabs/rymedb` |
| Rust | `sdks/rust` | `rymedb-client` |

No Java, Python, Go, Swift, or extra SDK directory is published by the
repository workflows. Those clients can use stable PostgreSQL, RESP, HTTP,
and WebSocket protocols directly.

## Release checks

```bash
pnpm --dir sdks/js build && pnpm --dir sdks/js test
cargo test --manifest-path sdks/rust/Cargo.toml --locked
```

SDK releases are started from **Actions → sdk-release → Run workflow** on
`main`. The workflow picks the next major automatically (`0.1.0` → `1.0.0` →
`2.0.0`), tests both SDKs at that version, publishes npm and Cargo packages,
commits the synchronized version fields, creates the matching `v*` tag and
GitHub release, and attaches package archives with checksums. Do not create
release tags manually. The first release uses `sdks/RELEASE_BASE_VERSION`;
later versions follow completed GitHub SDK releases.

The workflow is idempotent if a registry already contains the exact artifact;
it rejects a version whose published contents differ. npm uses Trusted
Publishing through GitHub OIDC. Cargo publishing uses the repository Actions
secret `CARGO_REGISTRY_TOKEN`.

The npm trusted publisher must name the workflow file `sdk-release.yml` (not a
tag-trigger workflow). If an older trusted-publisher entry names a removed
workflow, delete that entry and create a new one for `sdk-release.yml`.

Pushes and pull requests run build/test/package checks only; they do not publish.
The Pages build verifies that only `js` and `rust` exist under `sdks/`.

SDK methods should remain thin protocol clients. Add database semantics to the
server/API contract first, then update both clients and their examples.
