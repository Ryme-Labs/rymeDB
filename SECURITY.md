# Security policy

## Supported versions

| Version | Supported |
| --- | --- |
| 0.1.x | Yes, best effort pre-1.0 |

## Reporting a vulnerability

Email **security@rymelabs.example** with:

- Affected component and version (`ryme-server --version`, commit if built from source).
- Steps to reproduce against a default single-node config.
- Impact assessment (confidentiality, integrity, availability).

Expect an acknowledgement within 3 business days and a fix timeline within
14 days for confirmed high-severity issues. Do not open public issues for
unpatched vulnerabilities.

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
