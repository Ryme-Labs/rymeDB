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
  `rymedb-cli` and stores data under `/var/lib/rymedb`.
- Keep API, JWT, password, KMS, and archive credentials in a secret manager.
- Bind to loopback or a private interface until TLS and authentication are
  configured.
- Use HTTPS/WSS and TLS RESP/PostgreSQL/native listeners outside a trusted
  network.
- Enable mutual TLS for the Raft mesh when peers cross trust boundaries.
- Restrict owner/admin credentials and archive/membership routes.
- Apply filesystem and object-store least privilege to `data_dir` and backups.
- Run restore verification drills and rotate keys without publishing secrets.

## Scope notes

The server is early-stage. A compatibility gap, denial-of-service behavior,
missing hardening, or data-integrity problem can be security-relevant even if
it is not a code-execution bug. When unsure, use the private channel.
