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

Version each manifest before creating a `v*` tag. The tag workflow publishes
npm and Cargo artifacts and uploads checksums to the GitHub release. npm uses
Trusted Publishing through GitHub OIDC; Cargo publishing uses the configured
registry token. The release workflow tests artifacts before publishing and the
Pages build verifies that only `js` and `rust` exist under `sdks/`.

SDK methods should remain thin protocol clients. Add database semantics to the
server/API contract first, then update both clients and their examples.
