export interface ClusterMemberView {
  id: number;
  addr: string;
  self: boolean;
}

export interface ClusterView {
  term: number;
  leader: boolean;
  commit: number;
  members: ClusterMemberView[];
  joint: number[];
}

export interface ShardPlacement {
  tenant?: string;
  database?: string;
  table: string;
  shard?: number;
  tier?: string;
  bytes?: number;
}

export interface ShardView {
  mode: string;
  shards?: number;
  placement?: ShardPlacement[];
}

export interface RangeView {
  id: string;
  start: number[];
  end: number[];
  leader: string;
  epoch: number;
}

export interface RangeLoadView {
  id: string;
  epoch: number;
  writes: number;
}

export interface SlowEntryView {
  kind: string;
  fingerprint: string;
  table: string;
  micros: number;
  at_unix: number;
}

export interface TraceView {
  trace_id: string;
  span_id: string;
  name: string;
  duration_micros: number;
  attributes: string[][];
}

export interface DrillView {
  backup_id: string;
  verified_files: number;
  at_unix: number;
  error?: string | null;
}

export interface OverviewState {
  base: string;
  health: boolean;
  ready: boolean;
  leader: boolean;
  cluster: ClusterView | null;
  shards: ShardView | null;
  ranges: RangeView[] | null;
  loads: RangeLoadView[] | null;
  slow: SlowEntryView[] | null;
  traces: TraceView[] | null;
  drill: DrillView | null;
  metrics: string | null;
  p99: number | null;
  metering: string | null;
  autoscale: string | null;
  errors: string[];
}

async function getJson(base: string, key: string, path: string): Promise<unknown> {
  const headers: Record<string, string> = {};
  if (key) headers["authorization"] = `Bearer ${key}`;
  const response = await fetch(`${base}${path}`, { headers });
  if (!response.ok) throw new Error(`HTTP ${response.status} on ${path}`);
  return response.json() as Promise<unknown>;
}

export async function fetchOverview(base: string, key: string): Promise<OverviewState> {
  const state: OverviewState = {
    base,
    health: false,
    ready: false,
    leader: false,
    cluster: null,
    shards: null,
    ranges: null,
    loads: null,
    slow: null,
    traces: null,
    drill: null,
    metrics: null,
    p99: null,
    metering: null,
    autoscale: null,
    errors: [],
  };
  try {
    const response = await fetch(`${base}/health`);
    state.health = response.ok;
  } catch (error) {
    state.errors.push(`health: ${error instanceof Error ? error.message : String(error)}`);
    return state;
  }
  const tasks: Array<Promise<void>> = [
    fetch(`${base}/ready`)
      .then(async (response) => {
        state.ready = response.ok;
        if (response.ok) {
          const body = (await response.json()) as { leader?: boolean };
          state.leader = body.leader === true;
        }
      })
      .catch((error: unknown) => {
        state.errors.push(`ready: ${error instanceof Error ? error.message : String(error)}`);
      }),
    getJson(base, key, "/v1/cluster/members")
      .then((body) => {
        state.cluster = body as ClusterView;
      })
      .catch((error: unknown) => {
        state.errors.push(
          `cluster: ${error instanceof Error ? error.message : String(error)}`,
        );
      }),
    getJson(base, key, "/v1/shards")
      .then((body) => {
        state.shards = body as ShardView;
      })
      .catch((error: unknown) => {
        state.errors.push(`shards: ${error instanceof Error ? error.message : String(error)}`);
      }),
    getJson(base, key, "/v1/ranges")
      .then((body) => {
        state.ranges = (body ?? []) as RangeView[];
      })
      .catch((error: unknown) => {
        state.errors.push(`ranges: ${error instanceof Error ? error.message : String(error)}`);
      }),
    getJson(base, key, "/v1/ranges/loads")
      .then((body) => {
        state.loads = (body ?? []) as RangeLoadView[];
      })
      .catch((error: unknown) => {
        state.errors.push(`loads: ${error instanceof Error ? error.message : String(error)}`);
      }),
    getJson(base, key, "/v1/observe/slow?limit=5")
      .then((body) => {
        state.slow = ((body ?? {}) as { entries?: SlowEntryView[] }).entries ?? [];
      })
      .catch((error: unknown) => {
        state.errors.push(`slow: ${error instanceof Error ? error.message : String(error)}`);
      }),
    getJson(base, key, "/v1/traces?limit=5")
      .then((body) => {
        state.traces = ((body ?? {}) as { spans?: TraceView[] }).spans ?? [];
      })
      .catch((error: unknown) => {
        state.errors.push(`traces: ${error instanceof Error ? error.message : String(error)}`);
      }),
    (async () => {
      try {
        const headers: Record<string, string> = {};
        if (key) headers["authorization"] = `Bearer ${key}`;
        const response = await fetch(`${base}/v1/backups/drill`, { headers });
        if (response.status === 404) return;
        if (!response.ok) throw new Error(`HTTP ${response.status} on /v1/backups/drill`);
        state.drill = (await response.json()) as DrillView;
      } catch (error: unknown) {
        state.errors.push(`drill: ${error instanceof Error ? error.message : String(error)}`);
      }
    })(),
    fetch(`${base}/metrics`)
      .then(async (response) => {
        if (response.ok) {
          state.metrics = await response.text();
          try {
            const body = JSON.parse(state.metrics) as { p99_micros?: number };
            if (typeof body.p99_micros === "number") state.p99 = body.p99_micros;
          } catch {
            state.p99 = null;
          }
        }
      })
      .catch((error: unknown) => {
        state.errors.push(`metrics: ${error instanceof Error ? error.message : String(error)}`);
      }),
    getJson(base, key, "/v1/metering")
      .then((body) => {
        state.metering = JSON.stringify(body);
      })
      .catch((error: unknown) => {
        state.errors.push(`metering: ${error instanceof Error ? error.message : String(error)}`);
      }),
    getJson(base, key, "/v1/autoscale")
      .then((body) => {
        state.autoscale = JSON.stringify(body);
      })
      .catch((error: unknown) => {
        state.errors.push(`autoscale: ${error instanceof Error ? error.message : String(error)}`);
      }),
  ];
  await Promise.all(tasks);
  return state;
}

export function renderOverview(state: OverviewState): string {
  const lines = [`rymeDB @ ${state.base}`, `health: ${state.health ? "ok" : "down"}`];
  if (!state.health) {
    for (const error of state.errors) lines.push(`error: ${error}`);
    return lines.join("\n");
  }
  lines.push(`ready: ${state.ready ? "yes" : "no"}${state.leader ? " (leader)" : ""}`);
  if (state.cluster) {
    lines.push(
      `cluster: term=${state.cluster.term} commit=${state.cluster.commit} members=${state.cluster.members.length}`,
    );
    for (const member of state.cluster.members) {
      lines.push(`  - #${member.id} ${member.addr}${member.self ? " (self)" : ""}`);
    }
    if (state.cluster.joint.length > 0) {
      lines.push(`  joint in flight: ${state.cluster.joint.join(",")}`);
    }
  }
  if (state.shards) {
    const placement = state.shards.placement ?? [];
    const count =
      state.shards.shards !== undefined ? ` shards=${state.shards.shards}` : "";
    lines.push(`layout: mode=${state.shards.mode}${count} tables=${placement.length}`);
    for (const entry of placement.slice(0, 10)) {
      const detail = entry.shard !== undefined ? `shard=${entry.shard}` : `tier=${entry.tier ?? "?"}`;
      lines.push(`  - ${entry.table} ${detail}`);
    }
  }
  if (state.p99 !== null) lines.push(`p99: ${state.p99}us`);
  if (state.ranges) {
    lines.push(`ranges: count=${state.ranges.length}`);
    for (const range of state.ranges.slice(0, 10)) {
      lines.push(`  - ${range.id} epoch=${range.epoch} leader=${range.leader}`);
    }
  }
  if (state.loads) {
    lines.push(`loads: count=${state.loads.length}`);
    for (const load of state.loads.slice(0, 10)) {
      lines.push(`  - ${load.id} epoch=${load.epoch} writes=${load.writes}`);
    }
  }
  if (state.slow) {
    lines.push(`slow: count=${state.slow.length}`);
    for (const entry of state.slow.slice(0, 5)) {
      lines.push(`  - ${entry.kind} ${entry.fingerprint} ${entry.micros}us`);
    }
  }
  if (state.traces) {
    lines.push(`traces: count=${state.traces.length}`);
    for (const span of state.traces.slice(0, 5)) {
      lines.push(`  - ${span.name} ${span.duration_micros}us`);
    }
  }
  if (state.drill) {
    const status = state.drill.error ? ` ERROR ${state.drill.error}` : "";
    lines.push(
      `drill: ${state.drill.backup_id} files=${state.drill.verified_files} at=${state.drill.at_unix}${status}`,
    );
  } else if (!state.errors.some((error) => error.startsWith("drill:"))) {
    lines.push(`drill: never run`);
  }
  if (state.metering) lines.push(`metering: ${state.metering.slice(0, 200)}`);
  if (state.autoscale) lines.push(`autoscale: ${state.autoscale}`);
  if (state.errors.length > 0) {
    for (const error of state.errors) lines.push(`warn: ${error}`);
  }
  return lines.join("\n");
}
