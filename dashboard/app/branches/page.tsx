"use client";

import { useQuery } from "@tanstack/react-query";
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
} from "@/components/studio/ui";
import {
  createBranch,
  deleteBranch,
  diffBranch,
  listBranches,
  promoteBranch,
  type BranchInfo,
} from "@/lib/api";
import { useConnection } from "@/lib/connection";

export default function BranchesPage() {
  const conn = useConnection();
  const [id, setId] = useState("preview-1");
  const [parent, setParent] = useState("main");
  const [baseTs, setBaseTs] = useState("1");
  const [out, setOut] = useState<unknown>(null);
  const [err, setErr] = useState<unknown>(null);
  const [pending, setPending] = useState(false);

  const branches = useQuery({
    queryKey: ["branches", conn.base, conn.key],
    queryFn: () => listBranches(conn),
  });

  async function act(fn: () => Promise<unknown>) {
    setPending(true);
    setErr(null);
    try {
      const data = await fn();
      setOut(data);
      await branches.refetch();
    } catch (e) {
      setErr(e);
    } finally {
      setPending(false);
    }
  }

  const rows = Array.isArray(branches.data) ? (branches.data as BranchInfo[]) : [];

  return (
    <div>
      <SectionHeader
        title="Branches"
        description="Copy-on-write database branches — create previews, diff them and promote to main."
      />
      <Card>
        <CardHeader title="All branches" description="GET /v1/branches" />
        <CardContent>
          <QueryError error={branches.error} />
          <DataTable
            columns={[
              { header: "ID", accessorKey: "id", cell: (c) => <span className="mono">{cell(c.getValue())}</span> },
              { header: "Parent", accessorKey: "parent", cell: (c) => cell(c.getValue()) },
              { header: "Base commit", accessorKey: "base_commit_ts", cell: (c) => cell(c.getValue()) },
            ]}
            rows={rows}
            empty="No branches yet"
            dense
          />
          <Row className="mt-3">
            <Field label="Branch id">
              <Input value={id} onChange={(e) => setId(e.target.value)} className="mono w-36" />
            </Field>
            <Field label="Parent">
              <Input value={parent} onChange={(e) => setParent(e.target.value)} className="w-28" />
            </Field>
            <Field label="Base commit ts">
              <Input value={baseTs} onChange={(e) => setBaseTs(e.target.value)} className="mono w-24" />
            </Field>
          </Row>
          <Row className="mt-2">
            <Button
              variant="primary"
              disabled={pending}
              onClick={() => void act(() => createBranch(conn, id, parent, Number(baseTs)))}
            >
              Create
            </Button>
            <Button disabled={pending} onClick={() => void act(() => promoteBranch(conn, id))}>
              Promote
            </Button>
            <Button disabled={pending} onClick={() => void act(() => diffBranch(conn, id))}>
              Diff
            </Button>
            <Button
              variant="danger"
              disabled={pending}
              onClick={() => void act(() => deleteBranch(conn, id))}
            >
              Delete
            </Button>
          </Row>
          <div className="mt-3 grid gap-2">
            <QueryError error={err} />
            {out !== null && !err ? <CodeBlock data={out} /> : null}
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
