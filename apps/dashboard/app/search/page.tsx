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
  indexStats,
  textIndex,
  textSearch,
  vectorAnnSearch,
  vectorSearch,
  vectorUpsert,
} from "@/lib/api";
import { useConnection } from "@/lib/connection";

function parseVector(raw: string): number[] {
  return raw
    .split(",")
    .map((s) => Number(s.trim()))
    .filter((n) => !Number.isNaN(n));
}

export default function SearchPage() {
  const conn = useConnection();
  const [vxTable, setVxTable] = useState("emb");
  const [vxId, setVxId] = useState("a");
  const [vxVec, setVxVec] = useState("1,0");
  const [txTable, setTxTable] = useState("docs");
  const [txId, setTxId] = useState("d1");
  const [txText, setTxText] = useState("the quick brown fox");
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
        title="Search"
        description="Vector similarity, full-text search and index layout for this database."
      />
      <div className="grid gap-3 lg:grid-cols-2">
        <Card>
          <CardHeader title="Vector search" description="POST /v1/vector/upsert · /search · /ann-search" />
          <CardContent>
            <Row>
              <Field label="Table">
                <Input value={vxTable} onChange={(e) => setVxTable(e.target.value)} className="w-28" />
              </Field>
              <Field label="ID">
                <Input value={vxId} onChange={(e) => setVxId(e.target.value)} className="mono w-24" />
              </Field>
              <Field label="Vector" className="min-w-36 flex-1">
                <Input
                  value={vxVec}
                  onChange={(e) => setVxVec(e.target.value)}
                  className="mono"
                  spellCheck={false}
                />
              </Field>
            </Row>
            <Row className="mt-2">
              <Button variant="primary" disabled={pending} onClick={() => void act(() => vectorUpsert(conn, vxTable, vxId, parseVector(vxVec)))}>
                Upsert
              </Button>
              <Button disabled={pending} onClick={() => void act(() => vectorSearch(conn, vxTable, parseVector(vxVec)))}>
                Search
              </Button>
              <Button disabled={pending} onClick={() => void act(() => vectorAnnSearch(conn, vxTable, parseVector(vxVec)))}>
                ANN search
              </Button>
            </Row>
          </CardContent>
        </Card>

        <Card>
          <CardHeader title="Full-text search" description="POST /v1/text/index · /v1/text/search" />
          <CardContent>
            <Row>
              <Field label="Table">
                <Input value={txTable} onChange={(e) => setTxTable(e.target.value)} className="w-28" />
              </Field>
              <Field label="ID">
                <Input value={txId} onChange={(e) => setTxId(e.target.value)} className="mono w-24" />
              </Field>
              <Field label="Text" className="min-w-40 flex-1">
                <Input value={txText} onChange={(e) => setTxText(e.target.value)} />
              </Field>
            </Row>
            <Row className="mt-2">
              <Button variant="primary" disabled={pending} onClick={() => void act(() => textIndex(conn, txTable, txId, txText))}>
                Index
              </Button>
              <Button disabled={pending} onClick={() => void act(() => textSearch(conn, txTable, txText))}>
                Search
              </Button>
              <Button disabled={pending} onClick={() => void act(() => indexStats(conn))}>
                Index stats
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
