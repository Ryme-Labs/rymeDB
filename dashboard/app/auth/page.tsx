"use client";

import { useEffect, useState } from "react";
import {
  Button,
  Card,
  CardContent,
  CardHeader,
  CodeBlock,
  Field,
  Input,
  QueryError,
  Row,
  SectionHeader,
} from "@/components/studio/ui";
import {
  authRegister,
  authToken,
  authVerify,
  createApiKey,
  fetchApiKeys,
  revokeApiKey,
  fetchBillingSummary,
  fetchInvoice,
  fetchMetering,
  fetchQos,
  fetchRegions,
} from "@/lib/api";
import { useConnection } from "@/lib/connection";

export default function AuthPage() {
  const conn = useConnection();
  const [userId, setUserId] = useState("ada");
  const [password, setPassword] = useState("correct-horse");
  const [out, setOut] = useState<unknown>(null);
  const [err, setErr] = useState<unknown>(null);
  const [pending, setPending] = useState(false);
  const [keyName, setKeyName] = useState("");
  const [apiKeys, setApiKeys] = useState<Awaited<ReturnType<typeof fetchApiKeys>>["keys"]>([]);
  const [newKey, setNewKey] = useState("");

  async function refreshKeys() {
    const result = await fetchApiKeys(conn);
    setApiKeys(result.keys);
  }

  useEffect(() => {
    if (conn.key) void refreshKeys().catch(setErr);
  }, [conn.key]);

  async function act(fn: () => Promise<unknown>) {
    setPending(true);
    setErr(null);
    try {
      setOut(await fn());
    } catch (e) {
      setErr(e);
    } finally {
      setPending(false);
    }
  }

  return (
    <div>
      <SectionHeader
        title="Auth & Billing"
        description="Local password identities, API tokens, tenancy quotas, metering and billing — Studio's Authentication section for rymeDB."
      />
      <div className="grid gap-3 lg:grid-cols-2">
        <Card>
          <CardHeader title="Users" description="POST /v1/auth/register · /verify · /token" />
          <CardContent>
            <Row>
              <Field label="User id">
                <Input value={userId} onChange={(e) => setUserId(e.target.value)} className="w-36" />
              </Field>
              <Field label="Password" className="min-w-44 flex-1">
                <Input
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  type="password"
                />
              </Field>
            </Row>
            <Row className="mt-2">
              <Button variant="primary" disabled={pending} onClick={() => void act(() => authRegister(conn, userId, password))}>
                Register
              </Button>
              <Button disabled={pending} onClick={() => void act(() => authVerify(conn, userId, password))}>
                Verify
              </Button>
              <Button disabled={pending} onClick={() => void act(() => authToken(conn, userId, password))}>
                Token
              </Button>
            </Row>
          </CardContent>
        </Card>

        <Card>
          <CardHeader title="Tenancy & billing" description="Quotas, metering and invoices" />
          <CardContent>
            <Row>
              <Button disabled={pending} onClick={() => void act(() => fetchQos(conn))}>QoS</Button>
              <Button disabled={pending} onClick={() => void act(() => fetchMetering(conn))}>
                Metering
              </Button>
              <Button disabled={pending} onClick={() => void act(() => fetchBillingSummary(conn))}>
                Billing summary
              </Button>
              <Button disabled={pending} onClick={() => void act(() => fetchInvoice(conn))}>
                Invoice
              </Button>
              <Button disabled={pending} onClick={() => void act(() => fetchRegions(conn))}>
                Regions
              </Button>
            </Row>
          </CardContent>
        </Card>
      </div>
      <Card className="mt-3">
        <CardHeader title="API keys" description="Create named keys for apps and services. Each secret is shown once and expires after 90 days; revocation is immediate." />
        <CardContent>
          <Row>
            <Field label="Key name"><Input value={keyName} maxLength={64} onChange={(e) => setKeyName(e.target.value)} placeholder="e.g. website production" /></Field>
            <Button variant="primary" disabled={pending || !keyName.trim()} onClick={() => void act(async () => {
              const created = await createApiKey(conn, keyName);
              setNewKey(created.key);
              setKeyName("");
              await refreshKeys();
              return { id: created.id, name: created.name, expires_at: created.expires_at };
            })}>Create key</Button>
            <Button disabled={pending} onClick={() => void act(refreshKeys)}>Refresh</Button>
          </Row>
          {newKey ? <p className="mt-3 break-all rounded border border-warning/30 bg-warning/10 p-3 text-xs">Copy this secret now; it cannot be retrieved later: <code>{newKey}</code></p> : null}
          <div className="mt-3 divide-y divide-border-muted">
            {apiKeys.map((apiKey) => <div key={apiKey.id} className="flex flex-wrap items-center justify-between gap-2 py-2 text-sm">
              <span><strong>{apiKey.name}</strong><span className="ml-2 text-xs text-foreground-muted">expires {apiKey.expires_at ? new Date(apiKey.expires_at * 1000).toLocaleDateString() : "never"}</span><code className="ml-2 text-xs text-foreground-muted">{apiKey.id.slice(0, 12)}…</code></span>
              <Button variant="danger" disabled={pending} onClick={() => void act(async () => {
                await revokeApiKey(conn, apiKey.id);
                await refreshKeys();
                return { revoked: apiKey.name };
              })}>Revoke</Button>
            </div>)}
            {apiKeys.length === 0 ? <p className="py-2 text-xs text-foreground-muted">No named keys yet.</p> : null}
          </div>
        </CardContent>
      </Card>
      <div className="mt-3 grid gap-2">
        <QueryError error={err} />
        {out !== null && !err ? <CodeBlock data={out} /> : null}
      </div>
    </div>
  );
}
