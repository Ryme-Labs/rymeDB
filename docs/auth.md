# Authentication, tenants, and RLS

Every HTTP and WebSocket request is evaluated before its body is parsed.
PostgreSQL and RESP use their native startup/auth flows. The same principal
model supplies a tenant, roles, and capabilities across all gateways.

## Credentials

| Credential | Configure with | Use |
| --- | --- | --- |
| Development API key | `RYME_API_KEY` | Bearer header, `x-api-key`, or WebSocket `api_key` query parameter |
| PostgreSQL password | `RYME_PG_PASSWORD` | Cleartext startup password; unset means local trust mode |
| RESP password | `RYME_RESP_PASSWORD` | `AUTH` command; unset means local trust mode |
| HMAC JWT | `RYME_JWT_SECRET` | HS256 bearer tokens |
| JWKS JWT | `RYME_JWT_JWKS_FILE` or `RYME_JWT_JWKS_URL` | RS256-style verification from a mounted key set or URL |
| OIDC | `RYME_OIDC_*` | OIDC login/token helpers and issuer/audience validation; configure `RYME_OIDC_REDIRECT_URIS` as a comma-separated exact allowlist |
| OTP/passkey | REST auth endpoints | Application sign-in and WebAuthn assertions |

For HTTP:

```bash
curl -H "Authorization: Bearer $RYME_API_KEY" https://db.example.com/ready
curl -H "x-api-key: $RYME_API_KEY" https://db.example.com/v1/scan/messages
```

Browser WebSocket clients can use `?api_key=` because the browser WebSocket
constructor cannot attach arbitrary headers:

```text
wss://db.example.com/v1/stream?table=messages&api_key=...&branch=main
```

Never put a long-lived owner key in browser code. Mint a narrowly scoped
application credential or use a server-side proxy.

## Roles and tenants

JWTs accept rymeDB claims (`tenant`, `roles`) and common Supabase claims
(`tenant_id`, `role`, and `app_metadata.roles`). Common role behavior:

| Role | Default capability |
| --- | --- |
| `anon` | Read-only public access |
| `authenticated` | Authenticated application reads and writes |
| `service_role` / `superuser` | Platform and tenant administration; keep server-side |
| `owner` / `admin` | Tenant-local administration; no cluster, archive, or platform access |

The exact policy is still enforced by the server's configured principal and
table rules. A valid token is not automatically permission to administer the
cluster. Tokens without a recognized data-access role authenticate without
read or write capability.

## JWT settings

```bash
export RYME_JWT_JWKS_FILE=/run/secrets/rymedb-jwks.json
export RYME_JWT_ISSUER=https://issuer.example.com/
export RYME_JWT_AUDIENCE=rymedb
export RYME_JWT_JWK_KID=current
```

Use either a secret or a JWKS source. Configure issuer and audience together.
JWKS URLs must use HTTPS; loopback HTTP is accepted only for local development
and test fixtures.
HMAC JWT secrets must contain at least 16 bytes; production deployments should
use a randomly generated secret of substantially greater length. If only one
of issuer or audience is configured, startup fails rather than silently
disabling claim validation.
Expired, not-yet-valid, and future-issued tokens are rejected with clock-skew
checks. Mount secrets read-only and rotate them through your deployment
system.

## Password and application auth

```bash
rymedb-cli auth register ada '$PASSWORD'
rymedb-cli auth verify ada '$PASSWORD'
rymedb-cli auth token ada '$PASSWORD'
```

The REST surface also exposes `/v1/auth/register`, `/verify`, `/token`, OTP
setup/verify, passkey challenge/register/verify, OIDC login/token, and key
revocation. OIDC login returns a server-generated one-time `state`; send that
value with `id_token` to `/v1/auth/oidc/token`. The state expires after ten
minutes and cannot be reused. A caller-supplied login `state` is returned as
`client_state` for application correlation, but is not used as the security
state. The server stores password hashes, API-key digests, refresh-token
digests, and auth metadata under `data_dir/auth.json`; raw secrets and
WebAuthn challenges are not written there.

When OIDC is enabled, `RYME_OIDC_REDIRECT_URIS` is required. It must contain
the exact callback URLs accepted by `/v1/auth/oidc/login`; arbitrary callback
URLs are rejected.

## Row-level security

For simple tenant tables, configure a tenant column:

```json
{ "rls_tables": { "messages": "tenant_id" } }
```

Rows are visible and writable only when that field matches the authenticated
tenant. SQL migrations can also use PostgreSQL-shaped policy declarations:

```sql
ALTER TABLE messages ENABLE ROW LEVEL SECURITY;
CREATE POLICY messages_tenant ON messages
  FOR ALL
  USING (auth.uid() = tenant_id)
  WITH CHECK (auth.uid() = tenant_id);
```

RLS affects SQL, REST, scans, and realtime snapshots/updates. An enabled table
without a matching policy follows default-deny behavior. Inspect policy
metadata through `pg_catalog.pg_policies`. Complex policy expressions need a
manual compatibility review.

SQL schema, role, and privilege changes operate on the shared database
catalog and require a platform `superuser`/`service_role` credential. Tenant
`owner` and `admin` principals remain tenant-local: they can manage their
tenant's data and users, but cannot change global DDL or SQL grants.

## Security baseline

- Bind development listeners to loopback.
- Use HTTPS/WSS, TLS RESP, and TLS PostgreSQL/native listeners outside a
  private network.
- Use `tls_client_ca_pem` for mutual TLS where clients are managed.
- Use separate tenants and keys for applications, operators, and CI.
- Keep `data_dir`, `auth.json`, `keyring.json`, and archive credentials private.
- Test authorization failures as well as successful requests.

See [security and vulnerability reports](security.md) for disclosure rules.
