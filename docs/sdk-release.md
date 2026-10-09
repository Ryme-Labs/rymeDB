# SDK releases

The repository publishes only three SDKs:

- npm package `@rymelabs/rymedb` from `sdks/js`
- Rust crate `rymedb-client` from `sdks/rust`
- Java package `com.rymelabs:rymedb-client` from `sdks/java`

The `sdk-release` workflow builds and tests all three on changes to `sdks/`.
Push a `v*` tag after updating the version in each manifest to publish them.
The workflow uploads npm, Cargo, and Maven build artifacts to the GitHub
release. Publishing also requires these repository configuration values:

- `NPM_TOKEN`: an npm automation token with publish access
- `CARGO_REGISTRY_TOKEN`: a crates.io API token
- `GITHUB_TOKEN`: supplied by Actions for GitHub Packages and release uploads

Java packages are published to the repository's GitHub Maven registry. npm
and Cargo packages are published to their public registries. A manual run can
set the `publish` input when a tag is not being pushed.
