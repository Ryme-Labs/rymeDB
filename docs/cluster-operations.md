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
range topology is also the data-plane placement map. Point reads/writes route
by `table\0primary-key`; a split or merge migrates affected rows between local
shards before the new topology is persisted, and range assignments survive a
restart. `POST /v1/shards/move` remains available for whole-table placement.
Cluster and hybrid backends still use range metadata for routing/load control;
their cross-node range transfer requires the cluster data-movement protocol.
