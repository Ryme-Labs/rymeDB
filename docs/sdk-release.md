# SDK releases

The repository publishes only three SDKs:

- npm package `@rymelabs/rymedb` from `sdks/js`
- Rust crate `rymedb-client` from `sdks/rust`
- Java package `com.rymelabs:rymedb-client` from `sdks/java`

Those are the complete supported SDK set. New SDK directories are intentionally
not published by the release workflow; the Pages site also links these three
README files directly.

The `sdk-release` workflow builds and tests all three on changes to `sdks/`.
The separate `sdk-tag-release` workflow handles `v*` tags without a path
filter, so a tag always publishes the tested SDK set. Update the version in
each manifest before pushing a tag. The workflow uploads npm, Cargo, and
Maven build artifacts to the GitHub release. Java publishing deploys the
tested JAR, sources, and Javadoc artifacts rather than rebuilding a separate
package. Rust publishing downloads and publishes the exact tested Cargo
package artifact rather than rebuilding from a second checkout. Publishing
also requires these
repository configuration values:

- `NPM_TOKEN`: an npm automation token with publish access
- `CARGO_REGISTRY_TOKEN`: a crates.io API token
- `GITHUB_TOKEN`: supplied by Actions for GitHub Packages and release uploads

Java packages are published to the repository's GitHub Maven registry. npm
and Cargo packages are published to their public registries. A manual run can
set the `publish` input when a tag is not being pushed.

The Pages builder and release workflow fail if `sdks/` contains anything other
than `java`, `js` (npm), and `rust`, keeping the supported SDK surface explicit.
