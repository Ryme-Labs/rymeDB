# Cluster operations

Cluster mode uses Raft for membership and quorum-committed replication. The
leader owns membership changes and range metadata. A three-node cluster keeps
committing through one node failure; a quorum loss is a recovery event.

## Bootstrap three nodes

Create one JSON file per node. Use real reachable addresses in production:

```json
{
  "node_id": "node-0",
  "pg_listen": "10.0.0.10:5433",
  "resp_listen": "10.0.0.10:6380",
  "http_listen": "10.0.0.10:8080",
  "data_dir": "/var/lib/rymedb",
  "durability": "local-durable",
  "raft_tls": true,
  "tls_cert_pem": "/etc/rymedb/tls/tls.crt",
  "tls_key_pem": "/etc/rymedb/tls/tls.key",
  "tls_client_ca_pem": "/etc/rymedb/tls/ca.crt",
  "cluster": {
    "node_index": 0,
    "raft_listen": "10.0.0.10:9080",
    "advertise_addr": "10.0.0.10:9080",
    "peers": [
      { "id": 1, "addr": "10.0.0.11:9080" },
      { "id": 2, "addr": "10.0.0.12:9080" }
    ]
  }
}
```

Change the node id, listener addresses, and `node_index` for nodes 1 and 2.
Each node's `peers` list must contain every other node exactly once, omit that
node's own ID, and use a non-empty address for each peer. Mismatched or
duplicate peer IDs and blank addresses prevent a fresh cluster node from
starting.
Start them with `ryme-server --config node-0.json`. Set
`advertise_addr` whenever a listener binds to a wildcard address. Use TLS/mTLS
for a network that is not already private; see [configuration](configuration.md).

## Inspect and change membership

```bash
rymedb-cli --api-key "$RYME_API_KEY" cluster members
rymedb-cli --api-key "$RYME_API_KEY" cluster add 3 10.0.0.13:9080
rymedb-cli --api-key "$RYME_API_KEY" cluster transfer 1
rymedb-cli --api-key "$RYME_API_KEY" cluster remove 3
rymedb-cli --api-key "$RYME_API_KEY" cluster replace 0=10.0.0.10:9080 1=10.0.0.11:9080 2=10.0.0.12:9080
```

Boot a new member as a learner when possible. Wait for catch-up before
admitting it. A non-empty joint-consensus state means another membership
change must wait. Transfer leadership before planned maintenance or removal.

## Ranges and placement

```bash
rymedb-cli --api-key "$RYME_API_KEY" ranges
rymedb-cli --api-key "$RYME_API_KEY" ranges loads
rymedb-cli --api-key "$RYME_API_KEY" ranges autosplit
rymedb-cli --api-key "$RYME_API_KEY" shards layout
```

Range mutations are epoch-fenced. Read the current epoch, pass it to a split,
merge, or transfer, and retry after a `409` with fresh placement data.
`autosplit_writes: 0` keeps automatic splitting disabled. Range transfer
copies the MVCC snapshot, verifies it at the source commit, commits placement
metadata, and only then cleans up the old owner.

## Failure runbook

| Event | Response |
| --- | --- |
| Follower down | Keep serving while quorum remains; replace it after data is safe. |
| Leader down | Wait for election, discover the new leader through `/ready` or members, then redirect operator writes. |
| Quorum down | Do not force a single node to commit; restore a quorum from verified data. |
| Split-brain suspicion | Stop writes, preserve logs, compare terms/commit indexes, and do not delete a data directory. |
| Range lag | Inspect range loads and membership, then use controlled transfer/repair rather than manual file copying. |

Back up the cluster before membership surgery. Read [backup and restore](backup-restore.md)
for cluster restore scope and [troubleshooting](troubleshooting.md) for logs
and diagnostics.
