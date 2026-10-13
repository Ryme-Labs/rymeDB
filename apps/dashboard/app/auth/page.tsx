"use client";

import { useState } from "react";
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
      <div className="mt-3 grid gap-2">
        <QueryError error={err} />
        {out !== null && !err ? <CodeBlock data={out} /> : null}
      </div>
    </div>
  );
}
