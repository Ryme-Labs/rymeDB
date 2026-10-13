"use client";

import { useQuery } from "@tanstack/react-query";
import type { ColumnDef } from "@tanstack/react-table";
import { useMemo } from "react";
import { DataTable, cell } from "@/components/studio/data";
import {
  Card,
  CardContent,
  CardHeader,
  CodeBlock,
  Pill,
  QueryError,
  SectionHeader,
  Stat,
} from "@/components/studio/ui";
import {
  fetchAutoscale,
  fetchCluster,
  fetchMetrics,
  fetchReady,
  type ClusterMember,
} from "@/lib/api";
import { useConnection } from "@/lib/connection";

const memberColumns: ColumnDef<ClusterMember, unknown>[] = [
  { header: "ID", accessorKey: "id", cell: (c) => cell(c.getValue()) },
  { header: "Addr", accessorKey: "addr", cell: (c) => cell(c.getValue()) },
  {
    header: "Self",
    accessorKey: "self",
    cell: (c) => (c.getValue() ? <Pill tone="info">yes</Pill> : <span className="text-foreground-muted">—</span>),
  },
];

export default function HomePage() {
  const conn = useConnection();
  const ready = useQuery({ queryKey: ["ready", conn.base], queryFn: () => fetchReady(conn) });
  const metrics = useQuery({
    queryKey: ["metrics", conn.base, conn.key],
    queryFn: () => fetchMetrics(conn),
  });
  const cluster = useQuery({
    queryKey: ["cluster", conn.base, conn.key],
    queryFn: () => fetchCluster(conn),
  });
  const autoscale = useQuery({
    queryKey: ["autoscale", conn.base, conn.key],
    queryFn: () => fetchAutoscale(conn),
  });

  const members = useMemo(() => cluster.data?.members ?? [], [cluster.data]);

  return (
    <div>
      <SectionHeader
        title="Project overview"
        description="Node health, cluster membership, shard placement and tail latency for this rymeDB database."
      />
      <QueryError error={ready.error ?? metrics.error ?? cluster.error} />

      <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
        <Stat
          label="Node"
          value={
            ready.data ? (
              <>
                {String(ready.data.node ?? "?")}{" "}
                {ready.data.leader ? <Pill tone="info">leader</Pill> : null}
              </>
            ) : (
              "—"
            )
          }
          sub={
            ready.data
              ? `commit ${String(ready.data.commit ?? "?")} · ${ready.data.cluster ? "clustered" : "single"}`
              : "waiting for /ready"
          }
        />
        <Stat
          label="Commit index"
          value={String(ready.data?.commit ?? metrics.data?.commit ?? "—")}
          sub={metrics.data ? `uptime ${metrics.data.uptime_secs ?? "?"}s` : undefined}
        />
        <Stat
          label="p99 latency"
          value={metrics.data?.p99_micros !== undefined ? `${metrics.data.p99_micros}µs` : "—"}
          sub={
            metrics.data
              ? `p50 ${metrics.data.p50_micros}µs · p90 ${metrics.data.p90_micros}µs · n=${metrics.data.rest_count}`
              : undefined
          }
        />
        <Stat
          label="Cluster term"
          value={cluster.data ? String(cluster.data.term) : "—"}
          sub={cluster.data ? `${members.length} member(s)` : "waiting for /v1/cluster/members"}
        />
      </div>

      <div className="mt-3 grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-6">
        <Stat label="Requests observed" value={String(metrics.data?.rest_count ?? "—")} sub="since process start" />
        <Stat label="Mean latency" value={metrics.data ? `${metrics.data.rest_mean_micros ?? 0}µs` : "—"} />
        <Stat label="Max latency" value={metrics.data ? `${metrics.data.rest_max_micros ?? 0}µs` : "—"} />
        <Stat label="p95 latency" value={metrics.data ? `${metrics.data.p95_micros ?? 0}µs` : "—"} />
        <Stat label="p999 latency" value={metrics.data ? `${metrics.data.p999_micros ?? 0}µs` : "—"} />
        <Stat label="Process uptime" value={metrics.data ? `${Math.floor((metrics.data.uptime_secs ?? 0) / 3600)}h ${Math.floor(((metrics.data.uptime_secs ?? 0) % 3600) / 60)}m` : "—"} />
      </div>

      <div className="mt-3 grid gap-3 lg:grid-cols-2">
        <Card>
          <CardHeader title="Cluster members" description="Raft membership via /v1/cluster/members" />
          <CardContent>
            <DataTable columns={memberColumns} rows={members} empty="No members reported" dense />
          </CardContent>
        </Card>
        <Card>
          <CardHeader title="Autoscale" description="Placement policy via /v1/autoscale" />
          <CardContent>
            <CodeBlock data={autoscale.data} empty="Autoscale state will appear here" />
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
