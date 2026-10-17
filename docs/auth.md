# Authentication, tenants, and RLS

Every HTTP and WebSocket request is evaluated before its body is parsed.
PostgreSQL and RESP use their native startup/auth flows. The same principal
model supplies a tenant, roles, and capabilities across all gateways.

## Credentials

| Credential | Configure with | Use |
| --- | --- | --- |
| Development API key | `RYME_API_KEY` | Bearer header, `x-api-key`, or WebSocket `api_key` query parameter |
| Public project key | `RYME_PUBLIC_API_KEY` | Bearer header or `x-api-key`; intentionally public and limited to SQL `anon` / read-only |
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

HTTP database API responses use `Cache-Control: no-store`, including login
responses and returned application data. This limits accidental cache
retention; it does not hide browser credentials from DevTools.

Browser WebSocket clients can use `?api_key=` because the browser WebSocket
constructor cannot attach arbitrary headers:

```text
wss://db.example.com/v1/stream?table=messages&api_key=...&branch=main
```

URL credentials may be captured by reverse-proxy/access logs and monitoring.
For browser WebSockets, use only a short-lived, least-privilege user
credential, and configure every proxy in the path to redact `api_key` query
parameters. `Authorization` is checked before `x-api-key`. If an explicit
header credential fails authentication, the request does not downgrade to a
query-string key.
Never put an operator or owner key in a WebSocket URL.

Never put a long-lived owner key in browser code. Mint a narrowly scoped
application credential or use a server-side proxy.

When a listener is bound beyond loopback, its API key or protocol password must
contain at least 16 bytes. Use a randomly generated value rather than a sample
or human-memorable password.

## Browser applications

A credential embedded in JavaScript is public: users can read it in DevTools
or extract it from the shipped files. Supabase's public `anon` key works on
this same principle; the key is not what protects rows. Do not expose
`RYME_API_KEY`, a platform `superuser` key, or any other broad operator
credential in a browser.

For intentionally public browser access, configure `RYME_PUBLIC_API_KEY` and
ship that value as a project identifier. RymeDB maps it only to the read-only
SQL `anon` role; it cannot write or administer the platform. Access still
depends on SQL grants and RLS: grant only the required table privileges, and
enable RLS with explicit policies for tables that contain user-scoped data.
With RLS enabled and no matching policy, access is denied. A table granted to
`PUBLIC` or `anon` without RLS can still be read by anyone holding the public
key, so treat those grants as deliberate public exposure. The public key is
not user authentication and must never be treated as a secret.

For a browser-to-database app, use a short-lived user credential and enforce
access in RymeDB with tenant restrictions or row-level security (RLS). One
option is to issue user JWTs from your identity provider and configure RymeDB
to verify its JWKS, issuer, and audience. Give each token only the user's
identity (`sub`), tenant, and required data-access role; add policies that
default-deny rows the user should not see or change. The JWT remains visible
to that user, so keep its lifetime short and its permissions narrow. A
publicly visible credential is safe only when it grants no access beyond the
policies you intend.

Alternatively, put a backend-for-frontend (BFF) between the browser and
RymeDB. Keep the operator key in the server environment, authenticate the
user with a `Secure`, `HttpOnly`, `SameSite` session cookie, and have the BFF
authorize each operation before calling the database. Do not forward a broad
operator credential to the browser. RymeDB's built-in password sign-in
returns a 15-minute access key and rotating refresh token; other sign-in
methods may not provide refresh tokens. The access key is still inspectable
by the browser, and a refresh token is a bearer secret. Prefer keeping
refresh tokens server-side rather than persisting them in browser storage.

## Roles and tenants

JWTs accept rymeDB claims (`tenant`, `roles`) and common Supabase claims
(`tenant_id`, `role`, and `app_metadata.roles`). Common role behavior:

| Role | Default capability |
| --- | --- |
| `anon` | Read-only public access |
| `authenticated` | Authenticated application reads and writes |
| `service_role` | Tenant-scoped application read/write; RLS still applies; keep server-side |
| `superuser` | Platform administration; JWT claim is ignored unless explicitly enabled |
| `owner` / `admin` | Tenant-local administration; no cluster, archive, or platform access |

The exact policy is still enforced by the server's configured principal and
table rules. A valid token is not automatically permission to administer the
cluster. Tokens without a recognized data-access role authenticate without
read or write capability.

Tenant `owner` and `admin` principals use the SQL `authenticated` role; only a
platform `superuser` uses SQL `postgres`. Tenant administration does not bypass
SQL grants or row-level security.

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

JWTs cannot grant platform-admin (`superuser`) privileges by default, even if
the verified token contains that role. To enable it, set
`RYME_JWT_ALLOW_PLATFORM_ADMIN=true` only when this verifier is dedicated to a
trusted operator-only issuer, with `RYME_JWT_ISSUER` and
`RYME_JWT_AUDIENCE` explicitly configured; startup rejects the opt-in without
both claims. Do not share that verifier or signing key with application-user
tokens. Prefer the configured `RYME_API_KEY` for operator access when
application and operator identities share an issuer.

## Password and application auth

```bash
rymedb-cli auth register ada '$PASSWORD'
rymedb-cli auth verify ada '$PASSWORD'
rymedb-cli auth token ada '$PASSWORD'
```

The REST surface also exposes `/v1/auth/register`, `/verify`, `/token`, OTP
setup/verify, passkey challenge/register/verify, OIDC login/token, API-key
revocation, and refresh-session revocation. Password, passkey, and OIDC login
return an application access key that expires after 15 minutes, plus a refresh
token where supported. Refresh tokens rotate on use and expire after 30 days.
Operator-created API keys remain
long-lived until revoked and must stay server-side. Browser access keys are
visible to their user by design; limit their roles and enforce tenant access
with RLS. Never ship `RYME_API_KEY` to a browser. OIDC login returns a
server-generated one-time `state`; send that
value with `id_token` to `/v1/auth/oidc/token`. The state expires after ten
minutes and cannot be reused. A caller-supplied login `state` is returned as
`client_state` for application correlation, but is not used as the security
state. The server stores password hashes, API-key digests, refresh-token
digests, and auth metadata under `data_dir/auth.json`; raw secrets and
WebAuthn challenges are not written there.

OIDC authorization URLs include a random per-login `nonce`; the signed ID token
must return the same nonce. Bind the returned `state` to the initiating
browser session in your application or BFF. The database validates that state
is pending and single-use, but does not itself bind it to a browser session.

Passkey challenge and assertion endpoints are usable without an existing API
key so they can start a login. Challenge responses do not reveal whether a user
exists, and challenge creation is rate-limited by client and user. Passkey
registration still requires an authenticated principal authorized to manage
that user.

To log out and invalidate a rotating refresh token, send it in the body of
`DELETE /v1/auth/session` over HTTPS:

```bash
curl -X DELETE https://db.example.com/v1/auth/session \
  -H 'Content-Type: application/json' \
  -d '{"refresh_token":"<refresh-token>"}'
```

The refresh token is a bearer credential, so possession authorizes revoking
only that token. The operation does not require an unexpired 15-minute access
key, is safe to repeat, and never accepts the token in a URL.

When OIDC is enabled, `RYME_OIDC_REDIRECT_URIS` is required. It must contain
the exact callback URLs accepted by `/v1/auth/oidc/login`; arbitrary callback
URLs are rejected.

Authentication attempts are limited to 5 per credential and client IP, and
120 total per client IP, per 60-second window. OIDC login initiation is
included to prevent one client from filling the pending-state table. Limits
are local to each server process; multi-node deployments should also rate
limit authentication routes at a trusted edge.

## Row-level security

For simple tenant tables, configure a tenant column:

```json
{ "rls_tables": { "messages": "tenant_id" } }
```

Rows are visible and writable only when that field matches the authenticated
tenant. If multiple JSON fields match the configured field name without regard
to case, RymeDB denies the row. SQL migrations can also use PostgreSQL-shaped
policy declarations:

```sql
ALTER TABLE messages ENABLE ROW LEVEL SECURITY;
CREATE POLICY messages_owner ON messages
  FOR ALL TO authenticated
  USING (auth.uid() = user_id)
  WITH CHECK (auth.uid() = user_id);
```

Omitting `TO` or using `TO PUBLIC` applies the policy to every role. A policy
targeted with `TO role[, role...]` applies only when the request uses one of
those roles; the role names must already exist. Role membership inheritance is
not implemented, so target the effective request role directly.

For SQL policies, `auth.uid()` is the authenticated JWT `sub` identity. It is
distinct from the tenant claim. Configure `rls_tables` when rows should be
restricted by tenant instead. Requests authenticated only with an operator API
key have no user identity; do not use them to test user-scoped policies.

RLS affects SQL, REST, scans, and realtime snapshots/updates. An enabled table
without a matching policy follows default-deny behavior. Inspect policy
metadata through `pg_catalog.pg_policies`. Complex policy expressions need a
manual compatibility review. Supported permissive `auth.uid()` equality
policies combine with PostgreSQL's OR semantics; configured `rls_tables`
tenant restrictions remain an additional mandatory filter. Unsupported
composite policy expressions are rejected instead of being approximated.
Policies scoped with `TO role` or declared `AS RESTRICTIVE` are also rejected
until those semantics are supported; use the default permissive mode or
`TO PUBLIC` for currently supported policies.

SQL schema, role, and privilege changes operate on the shared database
catalog and require a platform `superuser` credential. Tenant
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

When deploying behind a reverse proxy, set `RYME_TRUSTED_PROXY_CIDRS` to the
proxy's exact source ranges and configure it to send a valid
`X-Forwarded-For` chain. RymeDB ignores forwarded addresses from untrusted
peers. The loopback-only first-admin bootstrap also requires a valid trusted
proxy chain for proxied loopback clients; do not expose the bootstrap route
through an unconfigured proxy.

See [security and vulnerability reports](security.md) for disclosure rules.
