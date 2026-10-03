# Security policy

## Supported versions

| Version | Supported |
| --- | --- |
| 0.1.x | Yes, best effort pre-1.0 |

## Reporting a vulnerability

> **Do not open a public GitHub Issue for a vulnerability — Issues are
> public and readable by everyone. GitHub has no per-template private
> Issue.** The `Security report` Issue template is for non-sensitive
> chores only and says so up front.

For staff-only visibility, use one of these private channels. Only
Rymelabs staff (repo maintainers with security access) can read reports
submitted this way:

1. **Preferred: GitHub Private Vulnerability Reporting**
   `https://github.com/Ryme-Labs/rymeDB/security/advisories/new`
   (Security tab -> Report a vulnerability). This creates a private
   Security Advisory visible only to you and Rymelabs staff until a fix
   is published.

2. **Fallback: email rymedb@rymelabs.dev** with:

- Affected component and version (`ryme-server --version`, commit if built from source).
- Steps to reproduce against a default single-node config.
- Impact assessment (confidentiality, integrity, availability).

Expect an acknowledgement within 3 business days and a fix timeline within
14 days for confirmed high-severity issues. Reports stay private between
you and Rymelabs staff until a patch is available — do not disclose
publicly (Issues, Discussions, social media) before then.

## Scope

In scope: authentication bypass, authorization bypass (RLS, masking, QoS
isolation escape), Raft safety violations, snapshot/backup decryption
without the DEK, TLS verification bypass, and remote crash defects in any
gateway.

Out of scope: the `ryme-dev-key` default credential (single-node dev only;
production requires `RYME_API_KEY`), test-only fixtures under
`*/tests/fixtures/`, and denial of service by authenticated principals
within their own quota (use QoS tiers).

## Hardening notes for operators

- Set `RYME_API_KEY` (and `RYME_KMS_KEY` for wrapped DEK persistence).
- Terminate public traffic on `https_listen` / `native_tls_listen` /
  `resp_tls_listen`; require `tls_client_ca_pem` for service meshes.
- Run `ryme backup verify <backup-id>` at least weekly.
