/**
 * Typed client for the rymeDB operator API.
 *
 * Every path here is served by `ryme-server` and documented in
 * `schemas/openapi/rest.yaml`. The Studio-clone pages (and the embedded
 * single-file console in `web/`) must only call these documented paths.
 */

export interface Connection {
  base: string;
  key: string;
}

export function normalizeBase(base: string): string {
  return base.trim().replace(/\/$/, "") || "http://127.0.0.1:8080";
}

export class ApiError extends Error {
  status: number;
  body: string;
  constructor(status: number, body: string, path: string) {
    super(`HTTP ${status} ${path} ${body.slice(0, 300)}`);
    this.name = "ApiError";
    this.status = status;
    this.body = body;
  }
}

function headers(key: string, json: boolean): Record<string, string> {
  const out: Record<string, string> = {};
  if (key) out["authorization"] = `Bearer ${key}`;
  if (json) out["content-type"] = "application/json";
  return out;
}

export async function api<T = unknown>(
  conn: Connection,
  method: string,
  path: string,
  body?: unknown,
): Promise<T> {
  const res = await fetch(`${normalizeBase(conn.base)}${path}`, {
    method,
    headers: headers(conn.key, body !== undefined),
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const text = await res.text();
  if (!res.ok) throw new ApiError(res.status, text, path);
  if (!text) return undefined as T;
  try {
    return JSON.parse(text) as T;
  } catch {
    return text as unknown as T;
  }
}

export const get = <T = unknown>(c: Connection, p: string) => api<T>(c, "GET", p);
export const post = <T = unknown>(c: Connection, p: string, b: unknown = {}) =>
  api<T>(c, "POST", p, b);
export const del = <T = unknown>(c: Connection, p: string) => api<T>(c, "DELETE", p);

/* ---------- shaped responses (loose on purpose: server owns the schema) ---------- */

export interface ReadyInfo {
  ready?: boolean;
  node?: number | string;
  commit?: number;
  leader?: boolean;
  cluster?: boolean;
}
export interface MetricsInfo {
  node?: number | string;
  uptime_secs?: number;
  commit?: number;
  rest_count?: number;
  rest_mean_micros?: number;
  rest_max_micros?: number;
  http_request_count?: number;
  http_4xx_count?: number;
  http_5xx_count?: number;
  p50_micros?: number;
  p90_micros?: number;
  p95_micros?: number;
  p99_micros?: number;
  p999_micros?: number;
}
export interface ClusterMember {
  id: number;
  addr: string;
  self?: boolean;
}
export interface ClusterInfo {
  mode?: "standalone" | "raft";
  term: number;
  leader?: boolean;
  commit: number;
  members: ClusterMember[];
  joint?: number[];
}
export interface AuditEvent {
  timestamp_secs: number;
  actor?: string | null;
  method: string;
  action: string;
  status: number;
  outcome: "success" | "failure";
}
export interface ShardPlacement {
  table: string;
  shard?: number;
  tier?: string;
  bytes?: number;
}
export interface ShardsInfo {
  mode: string;
  shards?: number;
  placement?: ShardPlacement[];
}
export interface RangeInfo {
  id: string;
  epoch: number;
  leader: string;
}
export interface RangeLoad {
  id: string;
  epoch: number;
  writes: number;
}
export interface BranchInfo {
  id: string;
  parent?: string;
  base_commit_ts?: number;
}

/* ---------- endpoint helpers ---------- */

export const fetchHealth = (c: Connection) =>
  fetch(`${normalizeBase(c.base)}/health`).then((r) => {
    if (!r.ok) throw new ApiError(r.status, "", "/health");
  });
export const fetchReady = (c: Connection) => get<ReadyInfo>(c, "/ready");
export const fetchMetrics = (c: Connection) => get<MetricsInfo>(c, "/metrics");
export const fetchAudit = (c: Connection, limit = 100) =>
  get<{ events: AuditEvent[] }>(c, `/v1/audit?limit=${limit}`);
export const fetchCluster = (c: Connection) => get<ClusterInfo>(c, "/v1/cluster/members");
export const fetchShards = (c: Connection) => get<ShardsInfo>(c, "/v1/shards");
export const fetchAutoscale = (c: Connection) => get(c, "/v1/autoscale");
export const fetchRanges = (c: Connection) => get<RangeInfo[]>(c, "/v1/ranges");
export const fetchRangeLoads = (c: Connection) => get<RangeLoad[]>(c, "/v1/ranges/loads");
export const autosplitRanges = (c: Connection) => post(c, "/v1/ranges/autosplit");
export const fetchSlow = (c: Connection, table?: string) =>
  get(c, `/v1/observe/slow?limit=20${table ? `&table=${encodeURIComponent(table)}` : ""}`);
export const fetchTraces = (c: Connection, table?: string) =>
  get(c, `/v1/traces?limit=20${table ? `&table=${encodeURIComponent(table)}` : ""}`);

export const runSql = (c: Connection, sql: string) => post(c, "/v1/sql", { sql });
export const explainSql = (c: Connection, sql: string) =>
  post(c, "/v1/sql/explain", { sql });
export const copyDemoRows = (c: Connection) =>
  post(c, "/v1/sql/copy", { table: "docs", rows: [{ key: "demo-1", value: "hello" }] });
export const restList = (c: Connection, table: string, query = "limit=10") =>
  get(c, `/rest/v1/${encodeURIComponent(table)}?${query || "limit=10"}`);
export const scanTable = (c: Connection, table: string, limit = 20) =>
  get(c, `/v1/scan/${encodeURIComponent(table)}?limit=${limit}`);
export const runGraphql = (c: Connection, query: string) =>
  post(c, "/graphql", { query });

export async function kvGet(c: Connection, table: string, key: string): Promise<string> {
  const res = await fetch(
    `${normalizeBase(c.base)}/v1/kv/${encodeURIComponent(table)}/${encodeURIComponent(key)}`,
    { headers: headers(c.key, false) },
  );
  const text = await res.text();
  if (!res.ok) throw new ApiError(res.status, text, "/v1/kv/{table}/{key}");
  return text;
}
export async function kvPut(
  c: Connection,
  table: string,
  key: string,
  value: string,
): Promise<string> {
  const res = await fetch(
    `${normalizeBase(c.base)}/v1/kv/${encodeURIComponent(table)}/${encodeURIComponent(key)}`,
    { method: "PUT", headers: headers(c.key, false), body: value },
  );
  const text = await res.text();
  if (!res.ok) throw new ApiError(res.status, text, "/v1/kv/{table}/{key}");
  return text || "ok";
}
export const kvDelete = (c: Connection, table: string, key: string) =>
  del(c, `/v1/kv/${encodeURIComponent(table)}/${encodeURIComponent(key)}`);
export const kvTtl = (c: Connection, table: string, key: string) =>
  get(c, `/v1/kv/${encodeURIComponent(table)}/${encodeURIComponent(key)}/ttl`);

export const listBranches = (c: Connection) => get<BranchInfo[]>(c, "/v1/branches");
export const createBranch = (c: Connection, id: string, parent: string, base_commit_ts: number) =>
  post(c, "/v1/branches", { id, parent, base_commit_ts });
export const promoteBranch = (c: Connection, id: string) =>
  post(c, `/v1/branches/${encodeURIComponent(id)}/promote`);
export const deleteBranch = (c: Connection, id: string) =>
  del(c, `/v1/branches/${encodeURIComponent(id)}`);
export const diffBranch = (c: Connection, id: string) =>
  get(c, `/v1/branches/${encodeURIComponent(id)}/diff`);

export const checkpoint = (c: Connection) => post(c, "/v1/backups/checkpoint");
export const latestBackup = (c: Connection) => get(c, "/v1/backups/latest");
export const takeSnapshot = (c: Connection) => post(c, "/v1/snapshots");
export const archiveBackup = (c: Connection, backup_id: string | null) =>
  post(c, "/v1/backups/archive", { backup_id });
export const listArchives = (c: Connection) => get(c, "/v1/backups/archives");
export const verifyBackup = (c: Connection, backup_id: string) =>
  get(c, `/v1/backups/verify?backup_id=${encodeURIComponent(backup_id)}`);
export const scheduledDrill = (c: Connection) => get(c, "/v1/backups/drill");

export const topicAppend = (c: Connection, partition: string, key: string, value: string) =>
  post(c, "/v1/topics/append", { partition, key, value });
export const topicRead = (c: Connection, partition: string) =>
  get(c, `/v1/topics/read?partition=${encodeURIComponent(partition)}&from=0&limit=20`);
export const broadcastPost = (c: Connection, channel: string, payload: unknown) =>
  post(c, "/v1/broadcast", { channel, payload });
export const presenceJoin = (c: Connection, channel: string, member: string) =>
  post(c, "/v1/presence/join", { channel, member });
export const presenceList = (c: Connection, channel: string) =>
  get(c, `/v1/presence/${encodeURIComponent(channel)}`);

export const vectorUpsert = (c: Connection, table: string, id: string, vector: number[]) =>
  post(c, "/v1/vector/upsert", { table, id, vector });
export const vectorSearch = (c: Connection, table: string, vector: number[], top_k = 5) =>
  post(c, "/v1/vector/search", { table, vector, top_k });
export const vectorAnnSearch = (
  c: Connection,
  table: string,
  vector: number[],
  top_k = 5,
  ef = 64,
) => post(c, "/v1/vector/ann-search", { table, vector, top_k, ef });
export const textIndex = (c: Connection, table: string, id: string, text: string) =>
  post(c, "/v1/text/index", { table, id, text });
export const textSearch = (c: Connection, table: string, query: string, top_k = 5) =>
  post(c, "/v1/text/search", { table, query, top_k });
export const indexStats = (c: Connection) => get(c, "/v1/index/stats");

export const fetchQos = (c: Connection) => get(c, "/v1/qos");
export const fetchMetering = (c: Connection) => get(c, "/v1/metering");
export const fetchBillingSummary = (c: Connection) => get(c, "/v1/billing/summary");
export const fetchInvoice = (c: Connection) => get(c, "/v1/billing/invoice");
export const fetchRegions = (c: Connection) => get(c, "/v1/regions");

export const authRegister = (c: Connection, id: string, password: string) =>
  post(c, "/v1/auth/register", { id, password });
export const authVerify = (c: Connection, id: string, password: string) =>
  post(c, "/v1/auth/verify", { id, password });
export const authToken = (c: Connection, id: string, password: string) =>
  post<{ key: string; tenant: string; refresh_token?: string }>(c, "/v1/auth/token", { id, password });
export interface ApiKeyInfo {
  id: string;
  name: string;
  roles: string[];
  created_at: number;
  expires_at: number | null;
}
export const fetchApiKeys = (c: Connection) => get<{ keys: ApiKeyInfo[] }>(c, "/v1/auth/keys");
export const createApiKey = (c: Connection, name: string, role?: string) =>
  post<{ id: string; name: string; roles: string[]; key: string; created_at: number; expires_at: number }>(
    c,
    "/v1/auth/keys",
    { name, ...(role ? { role } : {}) },
  );
export const revokeApiKey = (c: Connection, id: string) =>
  api(c, "DELETE", `/v1/auth/keys/${encodeURIComponent(id)}`);

/** Build an authenticated WebSocket URL for the documented stream paths. */
export function wsUrl(c: Connection, path: string): string {
  const base = normalizeBase(c.base).replace(/^http/, "ws");
  const sep = path.includes("?") ? "&" : "?";
  return `${base}${path}${c.key ? `${sep}api_key=${encodeURIComponent(c.key)}` : ""}`;
}

export function pretty(value: unknown): string {
  if (typeof value === "string") {
    try {
      return JSON.stringify(JSON.parse(value), null, 2);
    } catch {
      return value;
    }
  }
  return JSON.stringify(value, null, 2);
}
