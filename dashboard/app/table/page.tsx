"use client";

import { useMutation, useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { DataTable, cell } from "@/components/studio/data";
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
import { kvDelete, kvGet, kvPut, kvTtl, restList, scanTable } from "@/lib/api";
import { useConnection } from "@/lib/connection";

interface RestRow {
  key: string;
  value: unknown;
}

function toRows(data: unknown): RestRow[] {
  if (Array.isArray(data)) {
    return data.map((r) => {
      if (Array.isArray(r)) return { key: String(r[0]), value: r[1] };
      if (typeof r === "object" && r !== null) {
        const o = r as Record<string, unknown>;
        return { key: String(o.key ?? o.id ?? "?"), value: o.value ?? o };
      }
      return { key: "?", value: r };
    });
  }
  if (typeof data === "object" && data !== null) {
    const o = data as Record<string, unknown>;
    if (Array.isArray(o.rows)) return toRows(o.rows);
    if (Array.isArray(o.data)) return toRows(o.data);
  }
  return [];
}

export default function TablePage() {
  const conn = useConnection();
  const [table, setTable] = useState("docs");
  const [rowKey, setRowKey] = useState("hello");
  const [value, setValue] = useState('{"a":1}');
  const [query, setQuery] = useState("limit=10");
  const [kvOut, setKvOut] = useState<unknown>(null);
  const [kvErr, setKvErr] = useState<unknown>(null);

  const browse = useQuery({
    queryKey: ["rest", conn.base, conn.key, table, query],
    queryFn: () => restList(conn, table, query),
    enabled: false,
  });
  const scanned = useQuery({
    queryKey: ["scan", conn.base, conn.key, table],
    queryFn: () => scanTable(conn, table),
    enabled: false,
  });

  const mutate = useMutation({
    mutationFn: async (action: "put" | "get" | "del" | "ttl") => {
      if (action === "put") return kvPut(conn, table, rowKey, value);
      if (action === "get") return kvGet(conn, table, rowKey);
      if (action === "del") return kvDelete(conn, table, rowKey);
      return kvTtl(conn, table, rowKey);
    },
    onSuccess: (data) => {
      setKvErr(null);
      setKvOut(data);
    },
    onError: setKvErr,
  });

  const rows = toRows(browse.data ?? scanned.data);

  return (
    <div>
      <SectionHeader
        title="Table Editor"
        description="Browse rows and run key operations against any table. Mirrors Studio's Table Editor, backed by /v1/kv and /rest/v1."
      />

      <Card>
        <CardHeader title="Rows" description="REST list and storage scan of the current table" />
        <CardContent>
          <Row>
            <Field label="Table">
              <Input value={table} onChange={(e) => setTable(e.target.value)} className="w-40" />
            </Field>
            <Field label="Query">
              <Input value={query} onChange={(e) => setQuery(e.target.value)} className="mono w-44" />
            </Field>
            <Button variant="primary" onClick={() => void browse.refetch()}>
              Run REST list
            </Button>
            <Button onClick={() => void scanned.refetch()}>Scan</Button>
          </Row>
          <div className="mt-3 grid gap-3">
            <QueryError error={browse.error ?? scanned.error} />
            <DataTable
              columns={[
                { header: "Key", accessorKey: "key", cell: (c) => <span className="mono">{cell(c.getValue())}</span> },
                { header: "Value", accessorKey: "value", cell: (c) => <span className="mono">{cell(c.getValue())}</span> },
              ]}
              rows={rows}
              empty="Run a list or scan to load rows"
              dense
            />
          </div>
        </CardContent>
      </Card>

      <Card className="mt-3">
        <CardHeader title="Key operations" description="PUT / GET / DEL / TTL on /v1/kv/{table}/{key}" />
        <CardContent>
          <Row>
            <Field label="Key">
              <Input value={rowKey} onChange={(e) => setRowKey(e.target.value)} className="mono w-44" />
            </Field>
            <Field label="Value" className="min-w-64 flex-1">
              <Textarea value={value} onChange={(e) => setValue(e.target.value)} rows={2} />
            </Field>
          </Row>
          <Row className="mt-2">
            <Button variant="primary" disabled={mutate.isPending} onClick={() => mutate.mutate("put")}>
              PUT
            </Button>
            <Button disabled={mutate.isPending} onClick={() => mutate.mutate("get")}>GET</Button>
            <Button disabled={mutate.isPending} onClick={() => mutate.mutate("del")}>DEL</Button>
            <Button disabled={mutate.isPending} onClick={() => mutate.mutate("ttl")}>TTL</Button>
          </Row>
          <div className="mt-3 grid gap-2">
            <QueryError error={kvErr} />
            {kvOut !== null && !kvErr ? <CodeBlock data={kvOut} /> : null}
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
