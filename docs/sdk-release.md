# SDK releases

The supported SDK surface is intentionally limited to three packages:

| Ecosystem | Directory | Package |
| --- | --- | --- |
| npm/TypeScript | `sdks/js` | `@rymelabs/rymedb` |
| Rust | `sdks/rust` | `rymedb-client` |
| Java | `sdks/java` | `com.rymelabs:rymedb-client` |

No Python, Go, Swift, or extra SDK directory is published by the repository
workflows. Those clients can use stable PostgreSQL, RESP, HTTP, and WebSocket
protocols directly.

## Release checks

```bash
pnpm --dir sdks/js build && pnpm --dir sdks/js test
cargo test --manifest-path sdks/rust/Cargo.toml --locked
mvn -f sdks/java/pom.xml verify
```

Version each manifest before creating a `v*` tag. The tag workflow publishes
npm, Cargo, and Maven artifacts and uploads checksums to the GitHub release.
Publishing requires the repository's npm and Cargo tokens; Java uses GitHub
Packages. The release workflow tests artifacts before publishing and the Pages
build verifies that only `java`, `js`, and `rust` exist under `sdks/`.

SDK methods should remain thin protocol clients. Add database semantics to the
server/API contract first, then update all three clients and their examples.
