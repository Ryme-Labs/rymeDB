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
  Textarea,
} from "@/components/studio/ui";
import { copyDemoRows, explainSql, restList, runGraphql, runSql } from "@/lib/api";
import { useConnection } from "@/lib/connection";

export default function SqlPage() {
  const conn = useConnection();
  const [sql, setSql] = useState("SELECT * FROM docs LIMIT 10");
  const [restTable, setRestTable] = useState("docs");
  const [gql, setGql] = useState("{ docs(limit: 5) }");
  const [out, setOut] = useState<unknown>(null);
  const [err, setErr] = useState<unknown>(null);
  const [restOut, setRestOut] = useState<unknown>(null);
  const [restErr, setRestErr] = useState<unknown>(null);
  const [pending, setPending] = useState(false);

  async function runSqlAction(fn: () => Promise<unknown>) {
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

  async function runRestAction(fn: () => Promise<unknown>) {
    setPending(true);
    setRestErr(null);
    try {
      setRestOut(await fn());
    } catch (e) {
      setRestErr(e);
    } finally {
      setPending(false);
    }
  }

  return (
    <div>
      <SectionHeader
        title="SQL Editor"
        description="Run SQL, explain plans and seed demo rows — Studio's SQL Editor backed by /v1/sql."
      />
      <Card>
        <CardHeader title="Query" description="POST /v1/sql · /v1/sql/explain · /v1/sql/copy" />
        <CardContent>
          <Textarea
            value={sql}
            onChange={(e) => setSql(e.target.value)}
            rows={4}
            spellCheck={false}
          />
          <Row className="mt-2">
            <Button variant="primary" disabled={pending} onClick={() => void runSqlAction(() => runSql(conn, sql))}>
              Run
            </Button>
            <Button disabled={pending} onClick={() => void runSqlAction(() => explainSql(conn, sql))}>
              Explain
            </Button>
            <Button disabled={pending} onClick={() => void runSqlAction(() => copyDemoRows(conn))}>
              COPY demo rows
            </Button>
          </Row>
          <div className="mt-3 grid gap-2">
            <QueryError error={err} />
            {out !== null && !err ? <CodeBlock data={out} /> : null}
          </div>
        </CardContent>
      </Card>

      <Card className="mt-3">
        <CardHeader title="REST & GraphQL" description="PostgREST-style lists and GraphQL reads" />
        <CardContent>
          <Row>
            <Field label="Table">
              <Input value={restTable} onChange={(e) => setRestTable(e.target.value)} className="w-36" />
            </Field>
            <Button disabled={pending} onClick={() => void runRestAction(() => restList(conn, restTable))}>
              REST list
            </Button>
            <Field label="GraphQL" className="min-w-52 flex-1">
              <Input
                value={gql}
                onChange={(e) => setGql(e.target.value)}
                className="mono"
                spellCheck={false}
              />
            </Field>
            <Button disabled={pending} onClick={() => void runRestAction(() => runGraphql(conn, gql))}>
              GraphQL
            </Button>
          </Row>
          <div className="mt-3 grid gap-2">
            <QueryError error={restErr} />
            {restOut !== null && !restErr ? <CodeBlock data={restOut} /> : null}
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
