#!/usr/bin/env node
type Method = "GET" | "POST" | "PUT" | "DELETE";

const HELP = `ryme — rymeDB operator CLI

usage: ryme [--base URL] [--api-key KEY] <group> <action> [args] [flags]

groups:
  health                              GET /health
  ready                               GET /ready
  metrics                             GET /metrics
  observe slow [--limit N] [--table T]  recent slow operations
  traces [--limit N] [--name OP] [--table T]
                                        recent request spans
  kv get <table> <key>                fetch a value
  kv put <table> <key> <value>        store a value (--stdin reads value from stdin)
           [--ttl SECS]
  kv delete <table> <key>             delete a value
  kv ttl <table> <key>                remaining TTL
  sql <statement...>                  POST /v1/sql
  sql copy <table> <k=v...>          bulk ingest via /v1/sql/copy
  sql explain <statement...>          planned access path
  scan <table> [--limit N]            GET /v1/scan/:table
  rest list <table> [query]           GET /rest/v1/:table
  rest insert <table> <key> <value>   POST /rest/v1/:table
  rest delete <table> <key>           DELETE /rest/v1/:table?key=eq.<key>
  graphql <query...>                  POST /graphql
  metering                            GET /v1/metering
  autoscale [--cpu N] [--disk N]       GET /v1/autoscale
  qos [tier <tenant> <tier>]           QoS snapshot or tier set
  auth register <id> <password>        register password credential
  auth verify <id> <password>          verify password credential
  auth token <id> <password> [--code C]
                                      issue a usable API key
  auth revoke <key>                   revoke an API key by value
  auth passkey-register <user> <cred-id> <pubkey-b64>
                                      enroll a P-256 credential
  auth passkey-verify <user> <cred-id> <auth-data-b64> <client-data-b64> <sig-b64>
                                      verify an assertion, mint a key
  auth mask <table> <field...>         mask JSON fields on reads
  auth oidc-login <uri> [--state S]   build OIDC authorization URL
  auth oidc-token <id-token>          verify OIDC ID token
  presence join <channel> <member>     join presence channel
  presence list <channel>              list live members
  topics append <part> <key> <value>   append durable topic message
  topics read <part> [--from N]        read durable topic from cursor
  vector upsert <table> <id> <v,...>   upsert dense vector (exact cosine)
  vector search <table> <v,...>        nearest vectors [--top-k N]
  vector ann-search <table> <v,...>   HNSW approximate [--top-k N] [--ef N]
  vector delete <table> <id>           delete a vector
  text index <table> <id> <text...>    index a document (TF-IDF)
  text search <table> <query...>       ranked full-text search [--top-k N]
  text delete <table> <id>             delete a document
  index stats                         index partitions and namespaces
  branch create <id> <parent> <ts>     create a branch at a commit timestamp
  branch list                         list all branches
  branch get <id>                     describe a branch
  branch delete <id>                  delete a branch
  branch reset <id> <ts>              move a branch base commit
  branch promote <id>                 swap a child branch into its parent
  branch diff <id> <against>          segment sets unique to each side
  billing                             per-tenant usage rollup
  billing invoice [tenant]            priced micro-USD invoice
  migrate supabase <dump-file>        analyze Supabase dump to tables+policies
  migrate neon <branches-json-file>   map Neon branches to rymeDB plans
  migrate apply <id> <sql> [--author A]
                                      execute SQL and record it in the ledger
  migrate ledger                      list recorded migrations
  regions                             region placement and follower status
  backup checkpoint [--manifest ID]   write a PITR checkpoint
  backup latest                       newest checkpoint manifest
  backup pitr <target>                query checkpoints covering a timestamp
  backup restore <target>             restore to a timestamp (single/sharded only)
  backup archive [--backup ID]         archive latest backup now
  backup archives                     list archives
  backup verify <backup-id>           restore drill: sha + envelope decrypt check
  backup drill                        latest scheduled drill report
  backup copy <backup-id>             copy an archive to the replica target
  snapshot create                     snapshot backend files
  shards layout                       current shard layout
  shards move <table> [--target N]    move a table between shards
  ranges [list]                       list placement ranges
  ranges route <key>                  range owning a key
  ranges split <id> <mid> <left> <right> <epoch>
                                      split a range (epoch-fenced)
  ranges merge <left> <right> <merged> <lepoch> <repoch>
                                      merge adjacent ranges (epoch-fenced)
  ranges autosplit [min-writes]       split hot ranges (threshold override)
  ranges loads                        per-range counted write load
  cluster members                     raft membership, term, commit
  cluster add <id> <addr>             admit a learner/catch-up node
  cluster remove <id>                 remove a member
  cluster transfer <target>           hand leadership to a member
  cluster replace <id=addr...>        swap the member set via joint consensus
  stream watch <table> [--count N]   print live change records for a table
  stream query <table> [--limit N]   print live query snapshots and updates
  stream broadcast <channel> [--count N]
                                      print live broadcast messages
           [--count N]

flags:
  --base URL      server base (default http://127.0.0.1:8080)
  --api-key KEY   bearer key (default $RYME_API_KEY)
  --stdin         read kv value from stdin
  --ttl SECS      expiry on kv put
  --limit N       scan row limit
  --target N      shard move target
  --manifest ID   checkpoint manifest id
  --backup ID     archive source backup id
  --count N       exit after N stream messages (default: run until interrupted)
`;

interface Options {
  base: string;
  apiKey: string;
  rest: string[];
  flags: Map<string, string | boolean>;
}

function parseArgs(argv: string[]): Options {
  const flags = new Map<string, string | boolean>();
  const rest: string[] = [];
  let base = process.env["RYME_BASE"] ?? "http://127.0.0.1:8080";
  let apiKey = process.env["RYME_API_KEY"] ?? "";
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i]!;
    if (arg === "--base" || arg === "--api-key") {
      const value = argv[++i];
      if (value === undefined) fail(`missing value for ${arg}`);
      if (arg === "--base") base = value;
      else apiKey = value;
    } else if (arg.startsWith("--")) {
      const eq = arg.indexOf("=");
      if (eq > 0) flags.set(arg.slice(2, eq), arg.slice(eq + 1));
      else if (i + 1 < argv.length && !argv[i + 1]!.startsWith("--")) flags.set(arg.slice(2), argv[++i]!);
      else flags.set(arg.slice(2), true);
    } else {
      rest.push(arg);
    }
  }
  return { base, apiKey, rest, flags };
}

function fail(message: string): never {
  console.error(`error: ${message}`);
  process.exit(1);
}

function need(args: string[], count: number, usage: string): void {
  if (args.length < count) fail(`usage: ${usage}`);
}

function flag(options: Options, name: string): string | undefined {
  const value = options.flags.get(name);
  return typeof value === "string" ? value : undefined;
}

async function readStdin(): Promise<string> {
  let data = "";
  process.stdin.setEncoding("utf8");
  for await (const chunk of process.stdin) data += chunk;
  return data;
}

async function readFile(path: string): Promise<string> {
  const fs = await import("node:fs/promises");
  try {
    return await fs.readFile(path, "utf8");
  } catch {
    fail(`cannot read file ${path}`);
  }
}

async function call(
  options: Options,
  method: Method,
  path: string,
  body?: string,
): Promise<void> {
  const headers: Record<string, string> = {};
  if (options.apiKey) headers["authorization"] = `Bearer ${options.apiKey}`;
  if (body !== undefined) headers["content-type"] = "application/json";
  let response: Response;
  try {
    response = await fetch(`${options.base}${path}`, { method, headers, body });
  } catch (error) {
    fail(`request failed: ${error instanceof Error ? error.message : String(error)}`);
  }
  const text = await response.text();
  if (!response.ok) {
    console.error(`error: HTTP ${response.status} ${text}`);
    process.exit(1);
  }
  try {
    console.log(JSON.stringify(JSON.parse(text), null, 2));
  } catch {
    process.stdout.write(text + (text.endsWith("\n") ? "" : "\n"));
  }
}

async function main(): Promise<void> {
  const options = parseArgs(process.argv.slice(2));
  const [group, action, ...args] = options.rest;
  if (group === undefined || group === "--help" || group === "-h" || group === "help") {
    console.log(HELP);
    return;
  }
  switch (group) {
    case "health":
    case "ready":
    case "metrics":
      await call(options, "GET", `/${group}`);
      return;
    case "observe": {
      if (action !== "slow" && action !== undefined) fail("usage: ryme observe slow [--limit N] [--table T]");
      const limit = flag(options, "limit");
      const table = flag(options, "table");
      const params = new URLSearchParams();
      if (limit !== undefined) params.set("limit", limit);
      if (table !== undefined) params.set("table", table);
      const query = params.toString();
      await call(options, "GET", `/v1/observe/slow${query ? `?${query}` : ""}`);
      return;
    }
    case "traces": {
      if (action !== undefined) fail("usage: ryme traces [--limit N] [--name OP] [--table T]");
      const limit = flag(options, "limit");
      const name = flag(options, "name");
      const table = flag(options, "table");
      const params = new URLSearchParams();
      if (limit !== undefined) params.set("limit", limit);
      if (name !== undefined) params.set("name", name);
      if (table !== undefined) params.set("table", table);
      const query = params.toString();
      await call(options, "GET", `/v1/traces${query ? `?${query}` : ""}`);
      return;
    }
    case "kv": {
      need(args, 2, "ryme kv <get|put|delete|ttl> <table> <key> [value]");
      const op = action!;
      const [table, key, ...rest] = args as [string, string, ...string[]];
      const query = flag(options, "ttl") !== undefined ? `?ttl=${flag(options, "ttl")}` : "";
      if (op === "get") await call(options, "GET", `/v1/kv/${table}/${key}`);
      else if (op === "delete") await call(options, "DELETE", `/v1/kv/${table}/${key}`);
      else if (op === "ttl") await call(options, "GET", `/v1/kv/${table}/${key}/ttl`);
      else if (op === "put") {
        let value = rest.join(" ");
        if (options.flags.has("stdin")) value = await readStdin();
        else if (!value) fail("usage: ryme kv put <table> <key> <value> [--stdin]");
        await call(options, "PUT", `/v1/kv/${table}/${key}${query}`, value);
      } else fail(`unknown kv op ${op}`);
      return;
    }
    case "sql": {
      if (action === "copy") {
        need(args, 2, "ryme sql copy <table> <k=v...>");
        const [table, ...pairs] = args;
        const rows = pairs.map((pair) => {
          const eq = pair.indexOf("=");
          if (eq < 0) fail(`bad pair ${pair}, want <k=v>`);
          return { key: pair.slice(0, eq), value: pair.slice(eq + 1) };
        });
        await call(options, "POST", "/v1/sql/copy", JSON.stringify({ table, rows }));
        return;
      }
      if (action === "explain") {
        const statement = args.join(" ");
        if (!statement) fail("usage: ryme sql explain <statement...>");
        await call(options, "POST", "/v1/sql/explain", JSON.stringify({ sql: statement }));
        return;
      }
      const statement = [action, ...args].filter((part) => part !== undefined).join(" ");
      if (!statement) fail("usage: ryme sql <statement...>");
      await call(options, "POST", "/v1/sql", JSON.stringify({ sql: statement }));
      return;
    }
    case "rest": {
      if (action === "list") {
        need(args, 1, "ryme rest list <table> [query]");
        const [table, query] = args;
        await call(options, "GET", `/rest/v1/${table}${query ? `?${query}` : ""}`);
      } else if (action === "insert") {
        need(args, 3, "ryme rest insert <table> <key> <value>");
        await call(
          options,
          "POST",
          `/rest/v1/${args[0]}`,
          JSON.stringify({ key: args[1], value: args[2] }),
        );
      } else if (action === "delete") {
        need(args, 2, "ryme rest delete <table> <key>");
        await call(options, "DELETE", `/rest/v1/${args[0]}?key=eq.${encodeURIComponent(args[1]!)}`);
      } else fail("usage: ryme rest <list|insert|delete> ...");
      return;
    }
    case "graphql": {
      const query = [action, ...args].filter((part) => part !== undefined).join(" ");
      if (!query) fail("usage: ryme graphql <query...>");
      await call(options, "POST", "/graphql", JSON.stringify({ query }));
      return;
    }
    case "metering":
      await call(options, "GET", "/v1/metering");
      return;
    case "qos": {
      if (action === "tier") {
        need(args, 2, "ryme qos tier <tenant> <shared|dedicated_shard|dedicated_cluster>");
        await call(
          options,
          "POST",
          "/v1/qos/tier",
          JSON.stringify({ tenant: args[0], tier: args[1] }),
        );
      } else {
        await call(options, "GET", "/v1/qos");
      }
      return;
    }
    case "auth": {
      if (action === "register") {
        need(args, 2, "ryme auth register <id> <password>");
        await call(
          options,
          "POST",
          "/v1/auth/register",
          JSON.stringify({ id: args[0], password: args[1] }),
        );
      } else if (action === "verify") {
        need(args, 2, "ryme auth verify <id> <password>");
        await call(
          options,
          "POST",
          "/v1/auth/verify",
          JSON.stringify({ id: args[0], password: args[1] }),
        );
      } else if (action === "token") {
        need(args, 2, "ryme auth token <id> <password> [--code C]");
        const code = flag(options, "code") ?? null;
        await call(
          options,
          "POST",
          "/v1/auth/token",
          JSON.stringify({ id: args[0], password: args[1], code }),
        );
      } else if (action === "revoke") {
        need(args, 1, "ryme auth revoke <key>");
        await call(
          options,
          "DELETE",
          "/v1/auth/keys",
          JSON.stringify({ key: args[0] }),
        );
      } else if (action === "passkey-register") {
        need(args, 3, "ryme auth passkey-register <user> <cred-id> <pubkey-b64>");
        await call(
          options,
          "POST",
          "/v1/auth/passkey/register",
          JSON.stringify({ user: args[0], credential_id: args[1], public_key: args[2] }),
        );
      } else if (action === "passkey-verify") {
        need(args, 5, "ryme auth passkey-verify <user> <cred-id> <auth-data-b64> <client-data-b64> <sig-b64>");
        await call(
          options,
          "POST",
          "/v1/auth/passkey/verify",
          JSON.stringify({
            user: args[0],
            credential_id: args[1],
            authenticator_data: args[2],
            client_data_json: args[3],
            signature: args[4],
          }),
        );
      } else if (action === "mask") {
        need(args, 2, "ryme auth mask <table> <field...>");
        await call(
          options,
          "POST",
          "/v1/auth/mask",
          JSON.stringify({ table: args[0], fields: args.slice(1) }),
        );
      } else if (action === "oidc-login") {
        need(args, 1, "ryme auth oidc-login <redirect-uri> [--state S]");
        await call(
          options,
          "POST",
          "/v1/auth/oidc/login",
          JSON.stringify({ redirect_uri: args[0], state: flag(options, "state") ?? null }),
        );
      } else if (action === "oidc-token") {
        need(args, 1, "ryme auth oidc-token <id-token>");
        await call(
          options,
          "POST",
          "/v1/auth/oidc/token",
          JSON.stringify({ id_token: args[0] }),
        );
      } else fail("usage: ryme auth <register|verify|mask|oidc-login|oidc-token> ...");
      return;
    }
    case "presence": {
      if (action === "join") {
        need(args, 2, "ryme presence join <channel> <member>");
        await call(
          options,
          "POST",
          "/v1/presence/join",
          JSON.stringify({ channel: args[0], member: args[1] }),
        );
      } else if (action === "list") {
        need(args, 1, "ryme presence list <channel>");
        await call(options, "GET", `/v1/presence/${encodeURIComponent(args[0]!)}`);
      } else fail("usage: ryme presence <join|list> ...");
      return;
    }
    case "topics": {
      if (action === "append") {
        need(args, 3, "ryme topics append <partition> <key> <value>");
        await call(
          options,
          "POST",
          "/v1/topics/append",
          JSON.stringify({ partition: args[0], key: args[1], value: args[2] }),
        );
      } else if (action === "read") {
        need(args, 1, "ryme topics read <partition> [--from N]");
        const from = flag(options, "from") ?? "0";
        await call(options, "GET", `/v1/topics/read?partition=${encodeURIComponent(args[0]!)}&from=${from}&limit=100`);
      } else fail("usage: ryme topics <append|read> ...");
      return;
    }
    case "vector": {
      if (action === "upsert") {
        need(args, 3, "ryme vector upsert <table> <id> <v1,v2,...>");
        const vector = args[2]!.split(",").map((v) => Number(v.trim()));
        if (vector.some((v) => Number.isNaN(v))) fail("vector must be comma-separated numbers");
        await call(
          options,
          "POST",
          "/v1/vector/upsert",
          JSON.stringify({ table: args[0], id: args[1], vector }),
        );
      } else if (action === "search") {
        need(args, 2, "ryme vector search <table> <v1,v2,...> [--top-k N]");
        const vector = args[1]!.split(",").map((v) => Number(v.trim()));
        if (vector.some((v) => Number.isNaN(v))) fail("vector must be comma-separated numbers");
        const topK = Number(flag(options, "top-k") ?? "10");
        await call(
          options,
          "POST",
          "/v1/vector/search",
          JSON.stringify({ table: args[0], vector, top_k: topK }),
        );
      } else if (action === "delete") {
        need(args, 2, "ryme vector delete <table> <id>");
        await call(options, "DELETE", `/v1/vector/${args[0]}/${args[1]}`);
      } else if (action === "ann-search") {
        need(args, 2, "ryme vector ann-search <table> <v1,v2,...> [--top-k N] [--ef N]");
        const vector = args[1]!.split(",").map((v) => Number(v.trim()));
        if (vector.some((v) => Number.isNaN(v))) fail("vector must be comma-separated numbers");
        const topK = Number(flag(options, "top-k") ?? "10");
        const ef = Number(flag(options, "ef") ?? "64");
        await call(
          options,
          "POST",
          "/v1/vector/ann-search",
          JSON.stringify({ table: args[0], vector, top_k: topK, ef }),
        );
      } else fail("usage: ryme vector <upsert|search|ann-search|delete> ...");
      return;
    }
    case "text": {
      if (action === "index") {
        need(args, 3, "ryme text index <table> <id> <text...>");
        await call(
          options,
          "POST",
          "/v1/text/index",
          JSON.stringify({ table: args[0], id: args[1], text: args.slice(2).join(" ") }),
        );
      } else if (action === "search") {
        need(args, 2, "ryme text search <table> <query...>");
        const topK = Number(flag(options, "top-k") ?? "10");
        await call(
          options,
          "POST",
          "/v1/text/search",
          JSON.stringify({ table: args[0], query: args.slice(1).join(" "), top_k: topK }),
        );
      } else if (action === "delete") {
        need(args, 2, "ryme text delete <table> <id>");
        await call(options, "DELETE", `/v1/text/${args[0]}/${args[1]}`);
      } else fail("usage: ryme text <index|search|delete> ...");
      return;
    }
    case "index": {
      if (action === "stats" || action === undefined) {
        await call(options, "GET", "/v1/index/stats");
      } else fail("usage: ryme index stats");
      return;
    }
    case "autoscale": {
      const params: string[] = [];
      const cpu = flag(options, "cpu");
      const disk = flag(options, "disk");
      if (cpu !== undefined) params.push(`cpu_pct=${cpu}`);
      if (disk !== undefined) params.push(`disk_used_pct=${disk}`);
      await call(options, "GET", `/v1/autoscale${params.length ? `?${params.join("&")}` : ""}`);
      return;
    }
    case "scan": {
      if (!action) fail("usage: ryme scan <table> [--limit N]");
      const limit = flag(options, "limit");
      await call(options, "GET", `/v1/scan/${action}${limit !== undefined ? `?limit=${limit}` : ""}`);
      return;
    }
    case "branch": {
      if (action === "create") {
        need(args, 3, "ryme branch create <id> <parent> <base_commit_ts>");
        const [id, parent, ts] = args;
        await call(
          options,
          "POST",
          "/v1/branches",
          JSON.stringify({ id, parent, base_commit_ts: Number(ts) }),
        );
      } else if (action === "list" || action === undefined) {
        await call(options, "GET", "/v1/branches");
      } else if (action === "get" || action === "delete") {
        need(args, 1, `ryme branch ${action} <id>`);
        await call(options, action === "get" ? "GET" : "DELETE", `/v1/branches/${args[0]}`);
      } else if (action === "reset") {
        need(args, 2, "ryme branch reset <id> <base_commit_ts>");
        await call(
          options,
          "POST",
          `/v1/branches/${args[0]}/reset`,
          JSON.stringify({ base_commit_ts: Number(args[1]) }),
        );
      } else if (action === "promote") {
        need(args, 1, "ryme branch promote <id>");
        await call(options, "POST", `/v1/branches/${args[0]}/promote`);
      } else if (action === "diff") {
        need(args, 2, "ryme branch diff <id> <against>");
        await call(options, "GET", `/v1/branches/${args[0]}/diff?against=${encodeURIComponent(args[1]!)}`);
      } else fail("usage: ryme branch <create|list|get|delete|reset|promote|diff> ...");
      return;
    }
    case "billing":
      if (action === "invoice") {
        const tenant = args[0];
        await call(options, "GET", `/v1/billing/invoice${tenant ? `?tenant=${encodeURIComponent(tenant)}` : ""}`);
      } else {
        await call(options, "GET", "/v1/billing/summary");
      }
      return;
    case "migrate": {
      if (action === "supabase") {
        need(args, 1, "ryme migrate supabase <dump-file>");
        const dump = await readFile(args[0]!);
        await call(options, "POST", "/v1/migrate/supabase", JSON.stringify({ dump }));
      } else if (action === "neon") {
        need(args, 1, "ryme migrate neon <branches-json-file>");
        const branches = await readFile(args[0]!);
        await call(options, "POST", "/v1/migrate/neon", JSON.stringify({ branches }));
      } else if (action === "apply") {
        need(args, 2, "ryme migrate apply <id> <sql> [--author A]");
        const author = flag(options, "author") ?? null;
        await call(
          options,
          "POST",
          "/v1/migrate/apply",
          JSON.stringify({ id: args[0], sql: args[1], author }),
        );
      } else if (action === "ledger") {
        await call(options, "GET", "/v1/migrate/ledger");
      } else fail("usage: ryme migrate <supabase|neon|apply|ledger> ...");
      return;
    }
    case "regions":
      await call(options, "GET", "/v1/regions");
      return;
    case "backup": {
      if (action === "checkpoint") {
        const manifest = flag(options, "manifest");
        await call(
          options,
          "POST",
          "/v1/backups/checkpoint",
          JSON.stringify({ manifest_id: manifest ?? null }),
        );
      } else if (action === "latest") {
        await call(options, "GET", "/v1/backups/latest");
      } else if (action === "pitr") {
        need(args, 1, "ryme backup pitr <target>");
        await call(options, "GET", `/v1/backups/pitr?target=${args[0]}`);
      } else if (action === "restore") {
        need(args, 1, "ryme backup restore <target>");
        await call(options, "POST", `/v1/backups/restore?target=${args[0]}`);
      } else if (action === "archive") {
        const backup = flag(options, "backup");
        await call(
          options,
          "POST",
          "/v1/backups/archive",
          JSON.stringify({ backup_id: backup ?? null }),
        );
      } else if (action === "archives") {
        await call(options, "GET", "/v1/backups/archives");
      } else if (action === "verify") {
        need(args, 1, "ryme backup verify <backup-id>");
        await call(options, "GET", `/v1/backups/verify?backup_id=${encodeURIComponent(args[0]!)}`);
      } else if (action === "drill") {
        await call(options, "GET", "/v1/backups/drill");
      } else if (action === "copy") {
        need(args, 1, "ryme backup copy <backup-id>");
        await call(
          options,
          "POST",
          "/v1/backups/copy",
          JSON.stringify({ backup_id: args[0] }),
        );
      } else fail("usage: ryme backup <checkpoint|latest|pitr|restore|archive|archives|verify|drill|copy> ...");
      return;
    }
    case "snapshot":
      if (action !== "create") fail("usage: ryme snapshot create");
      await call(options, "POST", "/v1/snapshots");
      return;
    case "shards":
      if (action === "layout" || action === undefined) {
        await call(options, "GET", "/v1/shards");
      } else if (action === "move") {
        need(args, 1, "ryme shards move <table> [--target N]");
        const target = flag(options, "target");
        await call(
          options,
          "POST",
          "/v1/shards/move",
          JSON.stringify({ table: args[0], target: target === undefined ? null : Number(target) }),
        );
      } else fail("usage: ryme shards <layout|move> ...");
      return;
    case "ranges": {
      if (action === "list" || action === undefined) {
        await call(options, "GET", "/v1/ranges");
      } else if (action === "route") {
        need(args, 1, "ryme ranges route <key>");
        await call(options, "GET", `/v1/ranges?key=${encodeURIComponent(args[0]!)}`);
      } else if (action === "split") {
        need(args, 5, "ryme ranges split <id> <mid> <left> <right> <epoch>");
        await call(
          options,
          "POST",
          "/v1/ranges/split",
          JSON.stringify({
            id: args[0],
            mid: args[1],
            left_id: args[2],
            right_id: args[3],
            expected_epoch: Number(args[4]),
          }),
        );
      } else if (action === "merge") {
        need(args, 5, "ryme ranges merge <left> <right> <merged> <lepoch> <repoch>");
        await call(
          options,
          "POST",
          "/v1/ranges/merge",
          JSON.stringify({
            left_id: args[0],
            right_id: args[1],
            merged_id: args[2],
            expected_left_epoch: Number(args[3]),
            expected_right_epoch: Number(args[4]),
          }),
        );
      } else if (action === "autosplit") {
        const min = args[0] === undefined ? "" : `?min_writes=${Number(args[0])}`;
        await call(options, "POST", `/v1/ranges/autosplit${min}`);
      } else if (action === "loads") {
        await call(options, "GET", "/v1/ranges/loads");
      } else fail("usage: ryme ranges <list|route|split|merge|autosplit|loads> [min-writes]");
      return;
    }
    case "cluster": {
      if (action === "members" || action === undefined) {
        await call(options, "GET", "/v1/cluster/members");
      } else if (action === "add") {
        need(args, 2, "ryme cluster add <id> <addr>");
        await call(
          options,
          "POST",
          "/v1/cluster/members",
          JSON.stringify({ id: Number(args[0]), addr: args[1] }),
        );
      } else if (action === "remove") {
        need(args, 1, "ryme cluster remove <id>");
        await call(options, "DELETE", `/v1/cluster/members/${args[0]}`);
      } else if (action === "transfer") {
        need(args, 1, "ryme cluster transfer <target>");
        await call(options, "POST", "/v1/cluster/transfer", JSON.stringify({ target: Number(args[0]) }));
      } else if (action === "replace") {
        if (args.length === 0) fail("usage: ryme cluster replace <id=addr>...");
        const members = args.map((entry) => {
          const eq = entry.indexOf("=");
          if (eq < 0) fail(`bad member ${entry}, want <id=addr>`);
          return { id: Number(entry.slice(0, eq)), addr: entry.slice(eq + 1) };
        });
        await call(options, "POST", "/v1/cluster/replace", JSON.stringify({ members }));
      } else fail("usage: ryme cluster <members|add|remove|transfer|replace> ...");
      return;
    }
    case "stream": {
      if (action !== "watch" && action !== "query" && action !== "broadcast") {
        fail("usage: ryme stream <watch|query|broadcast> <table|channel> [--limit N] [--count N]");
      }
      need(
        args,
        1,
        action === "watch"
          ? "ryme stream watch <table> [--count N]"
          : action === "query"
            ? "ryme stream query <table> [--limit N] [--count N]"
            : "ryme stream broadcast <channel> [--count N]",
      );
      const table = args[0]!;
      const limit = flag(options, "limit");
      const countRaw = flag(options, "count");
      const count = countRaw === undefined ? -1 : Number(countRaw);
      if (Number.isNaN(count)) fail("--count must be a number");
      let path =
        action === "watch"
          ? `/v1/stream?table=${encodeURIComponent(table)}`
          : action === "query"
            ? `/v1/query-stream?table=${encodeURIComponent(table)}`
            : `/v1/broadcast/${encodeURIComponent(table)}`;
      if (action === "query" && limit !== undefined) path += `&limit=${limit}`;
      await watch(options, path, count);
      return;
    }
    default:
      fail(`unknown command ${group} (try: ryme help)`);
  }
}

async function watch(options: Options, path: string, count: number): Promise<void> {
  const sep = path.includes("?") ? "&" : "?";
  const url =
    options.base.replace(/\/$/, "").replace(/^http/, "ws") +
    path +
    (options.apiKey ? `${sep}api_key=${encodeURIComponent(options.apiKey)}` : "");
  const socket = new WebSocket(url);
  await new Promise<void>((resolve, reject) => {
    socket.addEventListener("open", () => resolve(), { once: true });
    socket.addEventListener("error", () => reject(new Error("websocket connect failed")), {
      once: true,
    });
  }).catch((error) => fail(error instanceof Error ? error.message : String(error)));
  let seen = 0;
  await new Promise<void>((resolve) => {
    socket.addEventListener("message", (event) => {
      const text = String(event.data);
      try {
        console.log(JSON.stringify(JSON.parse(text)));
      } catch {
        process.stdout.write(text + (text.endsWith("\n") ? "" : "\n"));
      }
      seen += 1;
      if (count >= 0 && seen >= count) {
        socket.close();
        resolve();
      }
    });
    socket.addEventListener("close", () => resolve());
    socket.addEventListener("error", () => resolve());
    process.on("SIGINT", () => {
      socket.close();
      resolve();
    });
  });
}

await main();
