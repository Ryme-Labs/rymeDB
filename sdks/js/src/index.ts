import net from "node:net";

export interface RymeClientOptions {
  host: string;
  port: number;
  tenant: string;
  database: string;
}

export class RymeClient {
  private host: string;
  private port: number;
  readonly tenant: string;
  readonly database: string;

  constructor(options: RymeClientOptions) {
    this.host = options.host;
    this.port = options.port;
    this.tenant = options.tenant;
    this.database = options.database;
  }

  async health(httpBase: string): Promise<boolean> {
    const response = await fetch(`${httpBase}/health`);
    return response.ok;
  }

  async command(parts: string[]): Promise<string> {
    return new Promise((resolve, reject) => {
      const socket = net.createConnection({ host: this.host, port: this.port }, () => {
        socket.write(encode(parts));
      });
      let buffer = Buffer.alloc(0);
      socket.on("data", (chunk: Buffer) => {
        buffer = Buffer.concat([buffer, chunk]);
        const text = buffer.toString("utf8");
        if (text.endsWith("\r\n")) {
          socket.end();
          resolve(text.trim());
        }
      });
      socket.on("error", reject);
    });
  }

  async get(key: string): Promise<string | null> {
    const reply = await this.command(["GET", key]);
    if (reply === "$-1") {
      return null;
    }
    return decodeBulk(reply);
  }

  async set(key: string, value: string): Promise<void> {
    const reply = await this.command(["SET", key, value]);
    if (!reply.startsWith("+OK")) {
      throw new Error(reply);
    }
  }
}

function encode(parts: string[]): Buffer {
  let out = `*${parts.length}\r\n`;
  for (const part of parts) {
    out += `$${Buffer.byteLength(part)}\r\n${part}\r\n`;
  }
  return Buffer.from(out, "utf8");
}

function decodeBulk(reply: string): string | null {
  const lines = reply.split("\r\n");
  if (lines.length < 2) {
    return null;
  }
  return lines[1] ?? null;
}

export interface RymeHttpOptions {
  base: string;
  apiKey?: string;
}

export interface ClusterMember {
  id: number;
  addr: string;
  self: boolean;
}

export interface ClusterStatus {
  term: number;
  leader: boolean;
  commit: number;
  members: ClusterMember[];
  joint: number[];
}

export interface ReadyStatus {
  ready: boolean;
  node: string;
  commit: number;
  leader: boolean;
  cluster: boolean;
}

export interface NodeMetrics {
  node: string;
  uptime_secs: number;
  commit: number;
  rest_count: number;
  rest_mean_micros: number;
  rest_max_micros: number;
  p50_micros?: number;
  p90_micros?: number;
  p95_micros?: number;
  p99_micros?: number;
}

export class RymeError extends Error {
  readonly status: number;
  readonly body: string;
  constructor(status: number, body: string) {
    super(`rymeDB HTTP ${status}: ${body}`);
    this.status = status;
    this.body = body;
  }
}

export class RymeHttpClient {
  private base: string;
  private apiKey: string;

  constructor(options: RymeHttpOptions) {
    this.base = options.base.replace(/\/$/, "");
    this.apiKey = options.apiKey ?? process.env["RYME_API_KEY"] ?? "";
  }

  private async request<T>(method: string, path: string, body?: unknown): Promise<T> {
    const headers: Record<string, string> = {};
    if (this.apiKey) headers["authorization"] = `Bearer ${this.apiKey}`;
    const payload = body === undefined ? undefined : JSON.stringify(body);
    if (payload !== undefined) headers["content-type"] = "application/json";
    const response = await fetch(`${this.base}${path}`, { method, headers, body: payload });
    const text = await response.text();
    if (!response.ok) throw new RymeError(response.status, text);
    if (text.length === 0) return undefined as T;
    try {
      return JSON.parse(text) as T;
    } catch {
      return text as T;
    }
  }

  async raw(method: string, path: string, body?: unknown): Promise<Response> {
    const headers: Record<string, string> = {};
    if (this.apiKey) headers["authorization"] = `Bearer ${this.apiKey}`;
    const payload = body === undefined ? undefined : JSON.stringify(body);
    if (payload !== undefined) headers["content-type"] = "application/json";
    return fetch(`${this.base}${path}`, { method, headers, body: payload });
  }

  health(): Promise<boolean> {
    return this.request<string>("GET", "/health").then(() => true);
  }

  ready(): Promise<ReadyStatus> {
    return this.request<ReadyStatus>("GET", "/ready");
  }

  metrics(): Promise<NodeMetrics> {
    return this.request<NodeMetrics>("GET", "/metrics");
  }

  slowLog(limit?: number, table?: string): Promise<unknown> {
    const params = new URLSearchParams();
    if (limit !== undefined) params.set("limit", String(limit));
    if (table !== undefined) params.set("table", table);
    const query = params.toString();
    return this.request<unknown>("GET", query ? `/v1/observe/slow?${query}` : "/v1/observe/slow");
  }

  traces(limit?: number, name?: string, table?: string): Promise<unknown> {
    const params = new URLSearchParams();
    if (limit !== undefined) params.set("limit", String(limit));
    if (name !== undefined) params.set("name", name);
    if (table !== undefined) params.set("table", table);
    const query = params.toString();
    return this.request<unknown>("GET", query ? `/v1/traces?${query}` : "/v1/traces");
  }

  kvGet(table: string, key: string): Promise<string> {
    const headers: Record<string, string> = {};
    if (this.apiKey) headers["authorization"] = `Bearer ${this.apiKey}`;
    return fetch(`${this.base}/v1/kv/${table}/${key}`, { headers }).then(async (response) => {
      const text = await response.text();
      if (!response.ok) throw new RymeError(response.status, text);
      return text;
    });
  }

  async kvPut(table: string, key: string, value: string, ttl?: number): Promise<unknown> {
    const query = ttl === undefined ? "" : `?ttl=${ttl}`;
    const headers: Record<string, string> = {};
    if (this.apiKey) headers["authorization"] = `Bearer ${this.apiKey}`;
    const response = await fetch(`${this.base}/v1/kv/${table}/${key}${query}`, {
      method: "PUT",
      headers,
      body: value,
    });
    const text = await response.text();
    if (!response.ok) throw new RymeError(response.status, text);
    try {
      return JSON.parse(text);
    } catch {
      return text;
    }
  }

  kvDelete(table: string, key: string): Promise<unknown> {
    return this.request<unknown>("DELETE", `/v1/kv/${table}/${key}`);
  }

  kvTtl(table: string, key: string): Promise<{ ttl: number }> {
    return this.request<{ ttl: number }>("GET", `/v1/kv/${table}/${key}/ttl`);
  }

  sql(sql: string, params?: string[]): Promise<unknown> {
    return this.request<unknown>("POST", "/v1/sql", { sql, params: params ?? null });
  }

  scan(table: string, limit?: number): Promise<unknown> {
    const query = limit === undefined ? "" : `?limit=${limit}`;
    return this.request<unknown>("GET", `/v1/scan/${table}${query}`);
  }

  branchCreate(id: string, parent: string, baseCommitTs: number): Promise<unknown> {
    return this.request<unknown>("POST", "/v1/branches", {
      id,
      parent,
      base_commit_ts: baseCommitTs,
    });
  }

  branchGet(id: string): Promise<unknown> {
    return this.request<unknown>("GET", `/v1/branches/${id}`);
  }

  branchDelete(id: string): Promise<unknown> {
    return this.request<unknown>("DELETE", `/v1/branches/${id}`);
  }

  branchList(): Promise<unknown> {
    return this.request<unknown>("GET", "/v1/branches");
  }

  branchReset(id: string, baseCommitTs: number): Promise<unknown> {
    return this.request<unknown>("POST", `/v1/branches/${id}/reset`, {
      base_commit_ts: baseCommitTs,
    });
  }

  branchPromote(id: string): Promise<unknown> {
    return this.request<unknown>("POST", `/v1/branches/${id}/promote`);
  }

  branchDiff(id: string, against: string): Promise<unknown> {
    return this.request<unknown>("GET", `/v1/branches/${id}/diff?against=${encodeURIComponent(against)}`);
  }

  billingSummary(): Promise<unknown> {
    return this.request<unknown>("GET", "/v1/billing/summary");
  }

  vectorAnnSearch(table: string, vector: number[], topK = 10, ef = 64): Promise<unknown> {
    return this.request<unknown>("POST", "/v1/vector/ann-search", { table, vector, top_k: topK, ef });
  }

  oidcLogin(redirectUri: string, state?: string): Promise<unknown> {
    return this.request<unknown>("POST", "/v1/auth/oidc/login", {
      redirect_uri: redirectUri,
      state: state ?? null,
    });
  }

  oidcToken(idToken: string): Promise<unknown> {
    return this.request<unknown>("POST", "/v1/auth/oidc/token", { id_token: idToken });
  }

  migrateSupabase(dump: string): Promise<unknown> {
    return this.request<unknown>("POST", "/v1/migrate/supabase", { dump });
  }

  backupVerify(backupId: string): Promise<unknown> {
    return this.request<unknown>("GET", `/v1/backups/verify?backup_id=${encodeURIComponent(backupId)}`);
  }

  backupDrill(): Promise<unknown> {
    return this.request<unknown>("GET", "/v1/backups/drill");
  }

  backupCopy(backupId: string): Promise<unknown> {
    return this.request<unknown>("POST", "/v1/backups/copy", { backup_id: backupId });
  }

  migrateNeon(branches: string): Promise<unknown> {
    return this.request<unknown>("POST", "/v1/migrate/neon", { branches });
  }

  migrateApply(id: string, sql: string, author?: string): Promise<unknown> {
    return this.request<unknown>("POST", "/v1/migrate/apply", {
      id,
      sql,
      author: author ?? null,
    });
  }

  migrateLedger(): Promise<unknown> {
    return this.request<unknown>("GET", "/v1/migrate/ledger");
  }

  indexStats(): Promise<unknown> {
    return this.request<unknown>("GET", "/v1/index/stats");
  }

  billingInvoice(tenant = ""): Promise<unknown> {
    const suffix = tenant ? `?tenant=${encodeURIComponent(tenant)}` : "";
    return this.request<unknown>("GET", `/v1/billing/invoice${suffix}`);
  }

  regions(): Promise<unknown> {
    return this.request<unknown>("GET", "/v1/regions");
  }

  vectorUpsert(table: string, id: string, vector: number[]): Promise<unknown> {
    return this.request<unknown>("POST", "/v1/vector/upsert", { table, id, vector });
  }

  vectorSearch(table: string, vector: number[], topK = 10): Promise<unknown> {
    return this.request<unknown>("POST", "/v1/vector/search", { table, vector, top_k: topK });
  }

  vectorDelete(table: string, id: string): Promise<unknown> {
    return this.request<unknown>("DELETE", `/v1/vector/${table}/${id}`);
  }

  textIndex(table: string, id: string, text: string): Promise<unknown> {
    return this.request<unknown>("POST", "/v1/text/index", { table, id, text });
  }

  textSearch(table: string, query: string, topK = 10): Promise<unknown> {
    return this.request<unknown>("POST", "/v1/text/search", { table, query, top_k: topK });
  }

  textDelete(table: string, id: string): Promise<unknown> {
    return this.request<unknown>("DELETE", `/v1/text/${table}/${id}`);
  }

  checkpoint(manifestId?: string): Promise<unknown> {
    return this.request<unknown>("POST", "/v1/backups/checkpoint", {
      manifest_id: manifestId ?? null,
    });
  }

  snapshot(): Promise<{ commit: number; files: string[] }> {
    return this.request<{ commit: number; files: string[] }>("POST", "/v1/snapshots");
  }

  latestCheckpoint(): Promise<unknown> {
    return this.request<unknown>("GET", "/v1/backups/latest");
  }

  pitr(target: number | string): Promise<unknown> {
    return this.request<unknown>("GET", `/v1/backups/pitr?target=${target}`);
  }

  restore(target: number | string): Promise<unknown> {
    return this.request<unknown>("POST", `/v1/backups/restore?target=${target}`);
  }

  archive(backupId?: string): Promise<unknown> {
    return this.request<unknown>("POST", "/v1/backups/archive", {
      backup_id: backupId ?? null,
    });
  }

  archives(): Promise<unknown> {
    return this.request<unknown>("GET", "/v1/backups/archives");
  }

  shardLayout(): Promise<unknown> {
    return this.request<unknown>("GET", "/v1/shards");
  }

  shardMove(table: string, target?: number): Promise<unknown> {
    return this.request<unknown>("POST", "/v1/shards/move", {
      table,
      target: target ?? null,
    });
  }

  ranges(key?: string): Promise<unknown> {
    const path = key === undefined ? "/v1/ranges" : `/v1/ranges?key=${encodeURIComponent(key)}`;
    return this.request<unknown>("GET", path);
  }

  rangeSplit(
    id: string,
    mid: string,
    leftId: string,
    rightId: string,
    expectedEpoch: number,
  ): Promise<unknown> {
    return this.request<unknown>("POST", "/v1/ranges/split", {
      id,
      mid,
      left_id: leftId,
      right_id: rightId,
      expected_epoch: expectedEpoch,
    });
  }

  rangeMerge(
    leftId: string,
    rightId: string,
    mergedId: string,
    expectedLeftEpoch: number,
    expectedRightEpoch: number,
  ): Promise<unknown> {
    return this.request<unknown>("POST", "/v1/ranges/merge", {
      left_id: leftId,
      right_id: rightId,
      merged_id: mergedId,
      expected_left_epoch: expectedLeftEpoch,
      expected_right_epoch: expectedRightEpoch,
    });
  }

  rangeAutosplit(minWrites?: number): Promise<unknown> {
    const path =
      minWrites === undefined ? "/v1/ranges/autosplit" : `/v1/ranges/autosplit?min_writes=${minWrites}`;
    return this.request<unknown>("POST", path);
  }

  rangeLoads(): Promise<unknown> {
    return this.request<unknown>("GET", "/v1/ranges/loads");
  }

  clusterMembers(): Promise<ClusterStatus> {
    return this.request<ClusterStatus>("GET", "/v1/cluster/members");
  }

  clusterAdd(id: number, addr: string): Promise<unknown> {
    return this.request<unknown>("POST", "/v1/cluster/members", { id, addr });
  }

  clusterRemove(id: number): Promise<unknown> {
    return this.request<unknown>("DELETE", `/v1/cluster/members/${id}`);
  }

  clusterTransfer(target: number): Promise<unknown> {
    return this.request<unknown>("POST", "/v1/cluster/transfer", { target });
  }

  clusterReplace(members: Array<{ id: number; addr: string }>): Promise<unknown> {
    return this.request<unknown>("POST", "/v1/cluster/replace", { members });
  }

  sqlCopy(table: string, rows: Array<{ key: string; value: string }>): Promise<unknown> {
    return this.request<unknown>("POST", "/v1/sql/copy", { table, rows });
  }

  sqlExplain(sql: string, params?: string[]): Promise<{ plan: string }> {
    return this.request<{ plan: string }>("POST", "/v1/sql/explain", {
      sql,
      params: params ?? null,
    });
  }

  restList(table: string, query = ""): Promise<unknown> {
    const suffix = query ? `?${query}` : "";
    return this.request<unknown>("GET", `/rest/v1/${table}${suffix}`);
  }

  restInsert(table: string, row: unknown): Promise<unknown> {
    return this.request<unknown>("POST", `/rest/v1/${table}`, row);
  }

  restUpsert(table: string, row: unknown): Promise<unknown> {
    return this.request<unknown>("POST", `/rest/v1/${table}`, row);
  }

  restUpdate(table: string, query: string, changes: unknown): Promise<unknown> {
    const suffix = query ? `?${query.replace(/^\?/, "")}` : "";
    return this.request<unknown>("PATCH", `/rest/v1/${table}${suffix}`, changes);
  }

  restDelete(table: string, key: string): Promise<unknown> {
    return this.request<unknown>("DELETE", `/rest/v1/${table}?key=eq.${encodeURIComponent(key)}`);
  }

  restDeleteWhere(table: string, query: string): Promise<unknown> {
    const suffix = query ? `?${query.replace(/^\?/, "")}` : "";
    return this.request<unknown>("DELETE", `/rest/v1/${table}${suffix}`);
  }

  graphql(query: string): Promise<unknown> {
    return this.request<unknown>("POST", "/graphql", { query });
  }

  metering(): Promise<unknown> {
    return this.request<unknown>("GET", "/v1/metering");
  }

  autoscale(query = ""): Promise<unknown> {
    const suffix = query ? `?${query}` : "";
    return this.request<unknown>("GET", `/v1/autoscale${suffix}`);
  }

  prometheus(): Promise<string> {
    const headers: Record<string, string> = {};
    if (this.apiKey) headers["authorization"] = `Bearer ${this.apiKey}`;
    return fetch(`${this.base}/metrics/prometheus`, { headers }).then(async (response) => {
      const text = await response.text();
      if (!response.ok) throw new RymeError(response.status, text);
      return text;
    });
  }

  qos(): Promise<unknown> {
    return this.request<unknown>("GET", "/v1/qos");
  }

  qosSetTier(tenant: string, tier: string): Promise<unknown> {
    return this.request<unknown>("POST", "/v1/qos/tier", { tenant, tier });
  }

  authRegister(id: string, password: string, tenant?: string, roles?: string[]): Promise<unknown> {
    return this.request<unknown>("POST", "/v1/auth/register", {
      id,
      password,
      tenant: tenant ?? null,
      roles: roles ?? null,
    });
  }

  authVerify(id: string, password: string): Promise<unknown> {
    return this.request<unknown>("POST", "/v1/auth/verify", { id, password });
  }

  authToken(id: string, password: string, code?: string): Promise<{ key: string; tenant: string }> {
    return this.request<{ key: string; tenant: string }>("POST", "/v1/auth/token", {
      id,
      password,
      code: code ?? null,
    });
  }

  authRevoke(key: string): Promise<unknown> {
    return this.request<unknown>("DELETE", "/v1/auth/keys", { key });
  }

  otpSetup(id: string): Promise<{ secret: string }> {
    return this.request<{ secret: string }>("POST", "/v1/auth/otp/setup", { id });
  }

  otpVerify(id: string, code: string): Promise<unknown> {
    return this.request<unknown>("POST", "/v1/auth/otp/verify", { id, code });
  }

  passkeyChallenge(user: string): Promise<{ challenge: string }> {
    return this.request<{ challenge: string }>("POST", "/v1/auth/passkey/challenge", { user });
  }

  passkeyRegister(user: string, credentialId: string, publicKey: string): Promise<unknown> {
    return this.request<unknown>("POST", "/v1/auth/passkey/register", {
      user,
      credential_id: credentialId,
      public_key: publicKey,
    });
  }

  passkeyVerify(
    user: string,
    credentialId: string,
    authenticatorData: string,
    clientDataJson: string,
    signature: string,
  ): Promise<{ key: string; tenant: string }> {
    return this.request<{ key: string; tenant: string }>("POST", "/v1/auth/passkey/verify", {
      user,
      credential_id: credentialId,
      authenticator_data: authenticatorData,
      client_data_json: clientDataJson,
      signature,
    });
  }

  maskSet(table: string, fields: string[]): Promise<unknown> {
    return this.request<unknown>("POST", "/v1/auth/mask", { table, fields });
  }

  presenceJoin(channel: string, member: string, state?: unknown, ttlSecs?: number): Promise<unknown> {
    return this.request<unknown>("POST", "/v1/presence/join", {
      channel,
      member,
      state: state ?? null,
      ttl_secs: ttlSecs ?? null,
    });
  }

  presenceLeave(channel: string, member: string): Promise<unknown> {
    return this.request<unknown>("POST", "/v1/presence/leave", { channel, member });
  }

  presenceList(channel: string): Promise<unknown> {
    return this.request<unknown>("GET", `/v1/presence/${encodeURIComponent(channel)}`);
  }

  broadcast(channel: string, payload: unknown, from?: string): Promise<unknown> {
    return this.request<unknown>("POST", "/v1/broadcast", {
      channel,
      payload,
      from: from ?? null,
    });
  }

  topicAppend(partition: string, key: string, value: string, retention?: number): Promise<unknown> {
    return this.request<unknown>("POST", "/v1/topics/append", {
      partition,
      key,
      value,
      retention: retention ?? null,
    });
  }

  topicRead(partition: string, from = 0, limit = 100): Promise<unknown> {
    return this.request<unknown>(
      "GET",
      `/v1/topics/read?partition=${encodeURIComponent(partition)}&from=${from}&limit=${limit}`,
    );
  }
}

export interface ChangeRecord {
  tenant: string;
  database: string;
  branch: string;
  table: string;
  op: "INSERT" | "UPDATE" | "DELETE";
  pk: number[];
  before: number[] | null;
  after: number[] | null;
  commit_ts: number;
  tx_id: number;
  sequence: number;
}

export interface BroadcastRecord {
  channel: string;
  from: string;
  payload: unknown;
  commit_ts: number;
  sequence: number;
}

export interface PresenceMember {
  member: string;
  state: unknown;
  expires_unix: number;
}

export interface PresenceEvent {
  type: "presence_state" | "join" | "leave";
  channel: string;
  sequence: number;
  member?: string;
  state?: unknown;
  expires_unix?: number;
  members?: PresenceMember[];
}

export interface DurableTopicMessage {
  partition: string;
  cursor: number;
  key: string;
  value: string;
  commit_ts: number;
}

export interface QueryRow {
  pk: string;
  value: string;
}

export interface QueryMessage {
  type: "snapshot" | "update";
  commit: number;
  branch?: string;
  rows: QueryRow[];
  truncated?: boolean;
}

export interface SubscribeOptions {
  apiKey?: string;
  branch?: string;
  limit?: number;
  from?: number;
  fromSequence?: number;
  reconnect?: boolean;
  reconnectDelayMs?: number;
}

export interface Subscription {
  ready: Promise<void>;
  close(): void;
}

export function subscribeTable(
  base: string,
  table: string,
  onMessage: (record: ChangeRecord) => void,
  options?: SubscribeOptions,
): Subscription {
  let cursor = options?.fromSequence;
  const query = () => {
    let path = `/v1/stream?table=${encodeURIComponent(table)}`;
    if (options?.branch !== undefined) path += `&branch=${encodeURIComponent(options.branch)}`;
    if (options?.from !== undefined) path += `&from=${options.from}`;
    if (cursor !== undefined) path += `&from_sequence=${cursor}`;
    return path;
  };
  return openSocket(base, query, { ...options, reconnect: options?.reconnect ?? true }, (data) => {
    const record = JSON.parse(data) as ChangeRecord;
    if (typeof record.sequence === "number" && Number.isSafeInteger(record.sequence)) {
      cursor = Math.max(cursor ?? 0, record.sequence);
    }
    onMessage(record);
  });
}

export function subscribeBroadcast(
  base: string,
  channel: string,
  onMessage: (record: BroadcastRecord) => void,
  options?: SubscribeOptions,
): Subscription {
  const query = `/v1/broadcast/${encodeURIComponent(channel)}`;
  return openSocket(base, query, options, (data) => {
    onMessage(JSON.parse(data) as BroadcastRecord);
  });
}

export function subscribePresence(
  base: string,
  channel: string,
  onMessage: (event: PresenceEvent) => void,
  options?: SubscribeOptions,
): Subscription {
  const query = `/v1/presence/${encodeURIComponent(channel)}/stream`;
  return openSocket(base, query, { ...options, reconnect: options?.reconnect ?? true }, (data) => {
    onMessage(JSON.parse(data) as PresenceEvent);
  });
}

export function subscribeDurableTopic(
  base: string,
  partition: string,
  onMessage: (message: DurableTopicMessage) => void,
  options?: SubscribeOptions,
): Subscription {
  let cursor = options?.from;
  const query = () => {
    let path = `/v1/topics/${encodeURIComponent(partition)}/stream`;
    if (cursor !== undefined) path += `?from=${cursor}`;
    return path;
  };
  return openSocket(base, query, { ...options, reconnect: options?.reconnect ?? true }, (data) => {
    const message = JSON.parse(data) as DurableTopicMessage;
    if (typeof message.cursor === "number" && Number.isSafeInteger(message.cursor)) {
      cursor = Math.max(cursor ?? 0, message.cursor + 1);
    }
    onMessage(message);
  });
}

export function subscribeQuery(
  base: string,
  table: string,
  onMessage: (message: QueryMessage) => void,
  options?: SubscribeOptions,
): Subscription {
  let query = `/v1/query-stream?table=${encodeURIComponent(table)}`;
  if (options?.branch !== undefined) query += `&branch=${encodeURIComponent(options.branch)}`;
  if (options?.limit !== undefined) query += `&limit=${options.limit}`;
  return openSocket(base, query, options, (data) => {
    onMessage(JSON.parse(data) as QueryMessage);
  });
}

function openSocket(
  base: string,
  path: string | (() => string),
  options: SubscribeOptions | undefined,
  onText: (data: string) => void,
): Subscription {
  const key = options?.apiKey ?? process.env["RYME_API_KEY"] ?? "";
  const reconnect = options?.reconnect ?? false;
  const delay = Math.max(10, Math.min(options?.reconnectDelayMs ?? 250, 60_000));
  const makeUrl = () => {
    const currentPath = typeof path === "function" ? path() : path;
    const sep = currentPath.includes("?") ? "&" : "?";
    return base.replace(/\/$/, "").replace(/^http/, "ws") + currentPath +
      (key ? `${sep}api_key=${encodeURIComponent(key)}` : "");
  };
  let socket: WebSocket | undefined;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let closed = false;
  let connected = false;
  let generation = 0;
  let resolveReady!: () => void;
  let rejectReady!: (error: Error) => void;
  const ready = new Promise<void>((resolve, reject) => {
    resolveReady = resolve;
    rejectReady = reject;
  });

  const scheduleReconnect = () => {
    if (closed || !reconnect || timer !== undefined) return;
    timer = setTimeout(() => {
      timer = undefined;
      connect();
    }, delay);
  };

  const connect = () => {
    if (closed) return;
    const currentGeneration = ++generation;
    const current = new WebSocket(makeUrl());
    socket = current;
    current.addEventListener("open", () => {
      if (currentGeneration !== generation) return;
      connected = true;
      resolveReady();
    }, { once: true });
    current.addEventListener("error", () => {
      if (currentGeneration !== generation) return;
      if (!connected) rejectReady(new Error("rymeDB websocket connection failed"));
      scheduleReconnect();
    });
    current.addEventListener("close", () => {
      if (currentGeneration !== generation) return;
      scheduleReconnect();
    });
    current.addEventListener("message", (event) => {
      if (currentGeneration === generation) onText(String(event.data));
    });
  };

  connect();
  return {
    ready,
    close: () => {
      closed = true;
      if (timer !== undefined) clearTimeout(timer);
      timer = undefined;
      socket?.close();
    },
  };
}

export function createHttpClient(options: RymeHttpOptions): RymeHttpClient {
  return new RymeHttpClient(options);
}

export function createClient(options: RymeClientOptions): RymeClient {
  return new RymeClient(options);
}
