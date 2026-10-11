# Cluster operations

rymeDB clusters use Raft with joint-consensus membership changes. Every
membership mutation goes through the leader over the HTTP API
(`crates/ryme-server/src/lib.rs`, routes at the `router()` definition) or the
matching `ryme` CLI commands (`apps/cli/src/cli.ts`).

Auth: `GET /v1/cluster/members` requires any valid credential. All mutations
(add, remove, transfer, replace) require an `Owner`/`Admin` principal,
otherwise `403`, so application keys can never eject nodes or join a
data-receiving member. Non-cluster nodes answer
`400 {"error":"cluster"}`.

## Bootstrapping a cluster

Each node needs a JSON config file (`ryme_config::Config::from_file`) with a
`cluster` section:

```json
{
  "cluster": {
    "node_index": 0,
    "raft_listen": "127.0.0.1:9080",
    "peers": [
      { "id": 1, "addr": "127.0.0.1:9081" },
      { "id": 2, "addr": "127.0.0.1:9082" }
    ],
    "learner": false,
    "advertise_addr": null
  }
}
```

Peer `addr` values are hostnames or IPs (`String`, resolved at dial time), so
stable DNS names work — the Helm chart relies on this
(`deploy/k8s/helm/rymeDB`). Start each node with:

```sh
ryme-server --config node0.json --data-dir ./data0
```

CLI flags override the file: `--pg-listen`, `--resp-listen`, `--http-listen`,
`--raft-listen`, `--advertise-addr`, `--data-dir`. A node whose config has
`raft_listen` set boots `serve_cluster`; without it, it boots single-node
`serve` (`crates/ryme-server/src/main.rs`).

`advertise_addr` is the address other members dial to reach this node. When
unset, the node falls back to `raft_listen` unless that is a wildcard address
(`0.0.0.0`), in which case the node reports an empty self addr. Always set
`advertise_addr` when listening on a wildcard socket. The working E2E recipe
is `crates/ryme-server/tests/cluster_e2e.rs`.

## Inspecting membership

```sh
ryme cluster members
```

Returns term, local leadership, commit index, the member list (`id`, `addr`,
`self`), and any in-flight joint config as member ids:

```json
{
  "term": 1, "leader": true, "commit": 42,
  "members": [{ "id": 0, "addr": "127.0.0.1:9080", "self": true }],
  "joint": []
}
```

A non-empty `joint` array means a membership transition is mid-flight; wait
for it to drain before starting another change. `/ready` also reports
`leader` per node and is suitable for readiness probes.

## Adding a member

Boot the joiner with `learner: true` and the current members as peers. A
learner replicates without campaigning, so it is safe to start before being
admitted. Then, against the leader:

```sh
ryme cluster add 3 127.0.0.1:9083
```

The leader catch-ups the joiner, then commits a joint-consensus transition
(`Node::add_member`). Empty addrs are rejected with `400`.

## Removing a member

```sh
ryme cluster remove 3
```

The leader commits the removal the same joint-consensus way
(`Node::remove_member`). Do not remove the leader without transferring first
unless the remaining quorum can elect on its own.

## Transferring leadership

```sh
ryme cluster transfer 1
```

Hands leadership to the target member (`Node::transfer`). Useful before
removing or restarting the current leader. Poll `/ready` on the target until
it reports `"leader": true`.

## Replacing the member set

```sh
ryme cluster replace 0=127.0.0.1:9000 1=127.0.0.1:9001
```

Swaps the whole member set in one joint-consensus transition
(`Node::replace_members`). Prefer this over add+remove pairs when reshaping a
cluster: the set changes atomically instead of passing through an intermediate
3-of-4 quorum.

## Failure recovery

- **Follower loss:** no action needed while a quorum remains; replace the
  member when a fresh node is ready.
- **Leader loss:** survivors elect a new leader automatically; point clients
  and operator commands at the new leader (find it via `/ready`).
- **Quorum loss:** restore at least a quorum of data dirs from backup/snapshot
  and restart those nodes; a single surviving copy cannot commit on its own.
- **Restore caveat:** `POST /v1/backups/restore` is unavailable in cluster
  mode (`503`). Rebuild clusters by re-adding fresh nodes, not by restoring
  over a live quorum. See `docs/backup-restore.md`.

## Placement ranges

The control plane keeps an ordered key-range map (`ryme_router::Router`):
every range has an id, start/end key, leader and epoch. Reads need any valid
credential; splits, merges, and autosplit triggers need an `Owner`/`Admin`
principal (`403` otherwise). Authentication runs before body parsing, so unauthenticated
calls get `401` even with a malformed body, and malformed bodies from
authenticated callers get `400`.

- `GET /v1/ranges` lists ranges ordered by start key.
- `GET /v1/ranges/loads` shows the counted write load per range
  (`id`, `epoch`, `writes` since the range was created) that drives
  auto-split.
- `GET /v1/ranges?key=<text>` returns the range owning that key (`404` when
  no range covers it).
- `POST /v1/ranges/split` cuts a range at `mid` into `left_id`/`right_id`.
- `POST /v1/ranges/merge` fuses two adjacent same-leader ranges.

Every mutation bumps the affected epochs. Mutations are epoch-fenced: pass
the `expected_epoch` you read, and a stale caller gets `409` naming the
current epoch, so it re-reads placement and retries instead of forking the
map. Merges additionally require adjacency and a shared leader.

Automatic splitting: every range counts routed writes. REST KV put/delete,
SQL writes and COPY row counts feed the counters, and so do the wire
gateways: RESP notes committed staged keys (single commands, `EXEC`
batches and blocking pops/moves), PG, native, and gRPC note row keys when
the SQL statement exposes them and use a table-level fallback for predicates.
When
`autosplit_writes` is non-zero,
`POST /v1/ranges/autosplit` splits every range at or above the threshold
(`?min_writes=` overrides it for one call), and a background task repeats
the pass every `autosplit_interval_secs` (default 30s, `0` disables the
loop). A threshold of zero never splits: with the default off config the
trigger endpoint is a no-op, and an explicit `?min_writes=0` is treated
the same way, so a bare `POST /v1/ranges/autosplit` cannot surprise-split
a quiet cluster. The loop is covered by an e2e that writes without ever calling the
trigger endpoint and polls placement until the children appear. Splits use the key-space midpoint with the same epoch fencing as
manual splits; child ids are `{id}-a-{epoch}` / `{id}-b-{epoch}`, child
load counters start at zero, and unsplittable or contended ranges are
skipped. Both settings default to off (`autosplit_writes: 0`), in which case
write accounting is skipped entirely. Merges stay manual: only an operator
can fuse ranges.

On a local sharded backend (`shards > 1` without cluster mode), the persisted
range topology is also the data-plane placement map once range splitting has
been activated. A fresh deployment exposes a synthetic `range-0` through the
control API but leaves data-plane routing in whole-table mode, so
`POST /v1/shards/move` remains available until the first split or autosplit.
After activation, point reads/writes route
by `table\0primary-key`; a split or merge migrates affected rows between local
shards before the new topology is persisted, and range assignments survive a
restart. `POST /v1/shards/move` remains available for whole-table placement.
Pure cluster backends replicate range metadata through the Raft log. Split,
merge, and autosplit requests must reach the current leader and are only
acknowledged after the metadata entry reaches quorum; followers apply the same
range map before serving the updated range. This keeps routing metadata
consistent across nodes, but it does not yet move range-owned data between
nodes. The leader now exposes `POST /v1/ranges/verify` as the first migration
safety gate: it takes a consistent key-range snapshot from the requested Raft
member, compares it with the leader at the supplied `expected_epoch`, and
returns row/byte counts plus `matching` and `ready_for_transfer`. The endpoint
is read-only; it never changes ownership, and a placement change during the
check returns `409` so a future transfer cannot copy against a stale range
epoch. Hybrid backends apply the same local range placement to their
non-replicated tier, while replicated tables continue to follow the Raft-backed
placement.

`POST /v1/ranges/transfer` is the controlled next step for a pure cluster. It
installs the complete bounded MVCC snapshot—including version chains and
tombstones—on the target member, reconciles stale visible keys, verifies the
target at the source commit timestamp, and then bumps the range epoch and
records the target as the logical range leader (for example, `raft-2`). The
operation is leader-only and requires the expected epoch. Once that metadata is
committed, main-branch point reads through the HTTP, native, gRPC, and RESP
gateways use the range owner and fetch from the target over the authenticated
mesh, including RLS checks at the serving gateway where applicable.
PostgreSQL autocommit primary-key selects use the same owner path; PostgreSQL
transactions, PostgreSQL scans, and transactional RESP commands still use the
existing transaction backend so their MVCC snapshots are unchanged. Ordered
scans through HTTP, native, gRPC, GraphQL, and PostgREST fan out to owners and
merge bounded pages by primary key. Followers with a complete range map now
materialize live committed writes only for their local owners while retaining
global conflict metadata; leaders and recovery still keep the complete state.
This does not yet delete source replicas or claim full shared-nothing storage
scale. Single-owner write-only transactions now route through the configured
owner and then the current Raft leader; read-dependent, multi-owner, and
write-only multi-owner transactions use the current global log for atomic
forwarding. This is not yet independent per-range Raft 2PC, and read-dependent cross-range
transaction routing remains separate work. Transactions that include a read
set now forward their snapshot and OCC metadata to the current leader, so a
follower-started transaction can be validated against intervening commits.
Multi-owner transactions now run an explicit prepare phase at each configured
range owner. Each participant writes a durable intent record before the
coordinator appends the global Raft data decision, and commit/abort resolution
cleans those records; unresolved records are recovered on participant restart.
The global Raft log is still the decision authority, so this is a durable
participant protocol, not yet independent per-range Raft groups.
Cluster transactions also carry the range-topology generation observed at
begin. Changing the configured range map invalidates an older transaction at
commit, including write-only owner forwarding; the mesh returns a typed
`conflict: range topology changed` response so callers can retry against the
new placement.
Point reads and point mutations in the PostgreSQL transaction path can now
fetch missing rows from their configured range owner and add the result to the
same transaction read set. Filtered scans, joins that depend on remote rows,
and read-only aggregate queries now fan out bounded range pages, merge them by
primary key, and preserve the transaction snapshot. Scan-based UPDATE/DELETE,
UPDATE FROM, DELETE USING, and INSERT SELECT now use those pages before
staging their writes. Constraint cascades that require additional remote
reads, schema-dependent views, and per-range 2PC remain separate work, so this
is not yet complete owner-aware transaction routing.

Broadcast realtime is separate from durable Raft data: `POST /v1/broadcast`
must reach the current leader, which fans the event over the cluster mesh to
the other live gateways. It is intentionally ephemeral and is not
replayed after a member joins or restarts; use durable topics for resumable
messages.

Presence mutations follow the same leader-routed mesh path. `POST
/v1/presence/join` and `POST /v1/presence/leave` are accepted by the current
leader and applied to every live gateway with one shared expiry timestamp.
Presence is intentionally ephemeral: a gateway that is down during a join or
leave does not receive that event until the client sends its next heartbeat
or leave, and presence is not replayed from the Raft log.

Durable topic appends use the durable Raft path instead of the ephemeral mesh.
The leader returns a cursor only after the entry reaches quorum and applies it;
followers replay the entry and persist their bounded `topics.json` snapshot.

Committed database writes also emit CDC on every live gateway. The leader's
executor emits its local change after commit, while followers reconstruct the
same change from the committed Raft write set after applying it, so clients can
connect to any gateway and receive the database-change stream.
