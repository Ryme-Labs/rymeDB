"use client";

import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { DataTable, cell } from "@/components/studio/data";
import {
  Button,
  Card,
  CardContent,
  CardHeader,
  Field,
  Input,
  QueryError,
  Row,
  SectionHeader,
} from "@/components/studio/ui";
import {
  autosplitRanges,
  fetchAudit,
  fetchMetrics,
  fetchRangeLoads,
  fetchRanges,
  fetchSlow,
  fetchTraces,
} from "@/lib/api";
import { useConnection } from "@/lib/connection";

export default function ObservePage() {
  const conn = useConnection();
  const [slowTable, setSlowTable] = useState("");
  const [traceTable, setTraceTable] = useState("");

  const ranges = useQuery({
    queryKey: ["ranges", conn.base, conn.key],
    queryFn: () => fetchRanges(conn),
  });
  const metrics = useQuery({
    queryKey: ["metrics", conn.base, conn.key],
    queryFn: () => fetchMetrics(conn),
    refetchInterval: 5_000,
  });
  const audit = useQuery({
    queryKey: ["audit", conn.base, conn.key],
    queryFn: () => fetchAudit(conn, 100),
    refetchInterval: 15_000,
  });
  const loads = useQuery({
    queryKey: ["loads", conn.base, conn.key],
    queryFn: () => fetchRangeLoads(conn),
  });
  const slow = useQuery({
    queryKey: ["slow", conn.base, conn.key, slowTable],
    queryFn: () => fetchSlow(conn, slowTable || undefined),
  });
  const traces = useQuery({
    queryKey: ["traces", conn.base, conn.key, traceTable],
    queryFn: () => fetchTraces(conn, traceTable || undefined),
  });

  const slowRows = Array.isArray((slow.data as { entries?: unknown[] } | undefined)?.entries)
    ? ((slow.data as { entries: Record<string, unknown>[] }).entries ?? [])
    : [];
  const traceRows = Array.isArray((traces.data as { spans?: unknown[] } | undefined)?.spans)
    ? ((traces.data as { spans: Record<string, unknown>[] }).spans ?? [])
    : [];

  return (
    <div>
      <SectionHeader
        title="Observe"
        description="Live request health, placement ranges, slow-query log and distributed traces."
      />
      <div className="mb-3 grid grid-cols-2 gap-3 lg:grid-cols-4">
        <MetricCard label="HTTP 4xx responses" value={metrics.data?.http_4xx_count} detail="since server start" />
        <MetricCard label="HTTP 5xx responses" value={metrics.data?.http_5xx_count} detail="since server start" />
        <MetricCard label="HTTP requests" value={metrics.data?.http_request_count} detail="since server start" />
        <MetricCard
          label="p99 latency"
          value={metrics.data ? `${metrics.data.p99_micros ?? 0}µs` : undefined}
          detail="recent rolling sample"
        />
      </div>
      <QueryError error={metrics.error} />
      <div className="grid gap-3 lg:grid-cols-2">
        <Card>
          <CardHeader
            title="Placement ranges"
            description="GET /v1/ranges · /v1/ranges/loads"
            actions={
              <Button
                onClick={() => void Promise.all([ranges.refetch(), loads.refetch()])}
              >
                Refresh
              </Button>
            }
          />
          <CardContent>
            <QueryError error={ranges.error ?? loads.error} />
            <DataTable
              columns={[
                { header: "Range", accessorKey: "id", cell: (c) => <span className="mono">{cell(c.getValue())}</span> },
                { header: "Epoch", accessorKey: "epoch", cell: (c) => cell(c.getValue()) },
                { header: "Leader", accessorKey: "leader", cell: (c) => cell(c.getValue()) },
                { header: "Writes", accessorKey: "writes", cell: (c) => cell(c.getValue()) },
              ]}
              rows={(Array.isArray(ranges.data) ? ranges.data : []) as unknown as Record<string, unknown>[]}
              empty="No ranges reported"
              dense
            />
            <Row className="mt-2">
              <Button
                onClick={() =>
                  void autosplitRanges(conn).then(
                    () => void Promise.all([ranges.refetch(), loads.refetch()]),
                  )
                }
              >
                Split hot ranges
              </Button>
            </Row>
          </CardContent>
        </Card>

        <Card>
          <CardHeader title="Slow log" description="GET /v1/observe/slow" />
          <CardContent>
            <Row>
              <Field label="Table filter" className="min-w-40 flex-1">
                <Input
                  value={slowTable}
                  onChange={(e) => setSlowTable(e.target.value)}
                  placeholder="table filter"
                />
              </Field>
              <Button onClick={() => void slow.refetch()}>Refresh</Button>
            </Row>
            <div className="mt-3">
              <QueryError error={slow.error} />
              <DataTable
                columns={[
                  { header: "Kind", accessorKey: "kind", cell: (c) => cell(c.getValue()) },
                  { header: "Fingerprint", accessorKey: "fingerprint", cell: (c) => <span className="mono">{cell(c.getValue())}</span> },
                  { header: "Table", accessorKey: "table", cell: (c) => cell(c.getValue()) },
                  { header: "µs", accessorKey: "micros", cell: (c) => cell(c.getValue()) },
                ]}
                rows={slowRows as Record<string, unknown>[]}
                empty="No slow statements"
                dense
              />
            </div>
          </CardContent>
        </Card>
      </div>

      <Card className="mt-3">
        <CardHeader title="Security audit log" description="Recent authentication and administrative actions. The server retains the latest 512 events on disk." />
        <CardContent>
          <QueryError error={audit.error} />
          <DataTable
            columns={[
              { header: "Time", accessorKey: "timestamp_secs", cell: (c) => new Date(Number(c.getValue()) * 1000).toLocaleString() },
              { header: "Actor", accessorKey: "actor", cell: (c) => cell(c.getValue() ?? "unauthenticated") },
              { header: "Action", accessorKey: "action", cell: (c) => <span className="mono">{cell(c.getValue())}</span> },
              { header: "Method", accessorKey: "method", cell: (c) => cell(c.getValue()) },
              {
                header: "Result",
                accessorKey: "status",
                cell: (c) => {
                  const status = Number(c.getValue());
                  return (
                    <span className={status >= 400 ? "text-danger" : "text-success"}>
                      {status} {status >= 400 ? "failed" : "ok"}
                    </span>
                  );
                },
              },
            ]}
            rows={audit.data?.events ?? []}
            empty="No tracked security actions yet"
            dense
          />
        </CardContent>
      </Card>

      <Card className="mt-3">
        <CardHeader title="Traces" description="GET /v1/traces" />
        <CardContent>
          <Row>
            <Field label="Table filter" className="min-w-40 flex-1">
              <Input
                value={traceTable}
                onChange={(e) => setTraceTable(e.target.value)}
                placeholder="table filter"
              />
            </Field>
            <Button onClick={() => void traces.refetch()}>Refresh</Button>
          </Row>
          <div className="mt-3">
            <QueryError error={traces.error} />
            <DataTable
              columns={[
                { header: "Span", accessorKey: "name", cell: (c) => <span className="mono">{cell(c.getValue())}</span> },
                { header: "Trace", accessorKey: "trace_id", cell: (c) => <span className="mono">{cell(c.getValue())}</span> },
                { header: "Duration µs", accessorKey: "duration_micros", cell: (c) => cell(c.getValue()) },
              ]}
              rows={traceRows as Record<string, unknown>[]}
              empty="No spans recorded"
              dense
            />
          </div>
        </CardContent>
      </Card>
    </div>
  );
}

function MetricCard({ label, value, detail }: { label: string; value?: string | number; detail: string }) {
  return (
    <Card>
      <CardContent>
        <p className="text-xs text-foreground-muted">{label}</p>
        <p className="mt-1 text-xl font-semibold">{value ?? "—"}</p>
        <p className="text-xs text-foreground-muted">{detail}</p>
      </CardContent>
    </Card>
  );
}
