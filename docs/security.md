# Security and vulnerability reports

Do not report an unpatched vulnerability in a public GitHub issue, discussion,
or chat. Use GitHub's private advisory form:

[Report a vulnerability privately](https://github.com/Ryme-Labs/rymeDB/security/advisories/new)

## Include

- A short impact statement and affected component/commit.
- Reproduction steps or a minimal proof of concept.
- Required authentication, tenant, network, and configuration assumptions.
- Whether the issue can read, modify, delete, escape a tenant, bypass TLS,
  execute code, or exhaust resources.
- A safe contact channel and a suggested disclosure timeline.

Encrypt sensitive attachments and remove credentials, customer data, JWTs,
private URLs, and production archive details. We may ask for a sanitized
reproduction or a temporary test key.

## Deployment baseline

- Use a non-root container or service account; the published image runs as
  `ryme` and stores data under `/var/lib/rymedb`.
- Verify the keyless Sigstore signature before deploying a GHCR image:

  ```bash
  cosign verify \
    --certificate-identity-regexp '^https://github.com/Ryme-Labs/rymeDB/.github/workflows/docker.yml@refs/(heads/main|tags/v.*)$' \
    --certificate-oidc-issuer https://token.actions.githubusercontent.com \
    ghcr.io/ryme-labs/rymedb:latest
  ```

  Pin deployments to the verified image digest after verification.
- Keep API, JWT, password, KMS, and archive credentials in a secret manager.
- Bind to loopback or a private interface until TLS and authentication are
  configured.
- Use HTTPS/WSS and TLS RESP/PostgreSQL/native listeners outside a trusted
  network.
- Enable mutual TLS for the Raft mesh when peers cross trust boundaries.
- Restrict superuser credentials and archive/membership routes; tenant
  owner/admin roles are not platform operators.
- Apply filesystem and object-store least privilege to `data_dir` and backups.
- Run restore verification drills and rotate keys without publishing secrets.

## Scope notes

The server is early-stage. A compatibility gap, denial-of-service behavior,
missing hardening, or data-integrity problem can be security-relevant even if
it is not a code-execution bug. When unsure, use the private channel.
