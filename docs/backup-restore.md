# Backup, PITR, snapshots, restore

rymeDB separates four related mechanisms. Handlers live in
`crates/ryme-server/src/lib.rs`; the checkpoint record is
`{ id, commit_ts, manifest_id, created_unix }`.

## Checkpoints (PITR markers)

Creating a checkpoint requires a write-capable principal. The id is derived
from the current backend commit (`ckpt-{commit}`); `manifest_id` defaults to
`"genesis"` when omitted:

```sh
ryme backup checkpoint
ryme backup checkpoint --manifest nightly-042
```

`GET /v1/backups/latest` and `GET /v1/backups/pitr?target=<unix-ts>` need
any valid credential and return `404 {"error":"checkpoint"}` when nothing
matches:

```sh
ryme backup latest
ryme backup pitr 1735689600
```

`pitr` selects the checkpoint covering the target timestamp
(`backups.select_pitr`). Record checkpoints on a schedule (the `interval_secs`
archiver does this when an archive target is configured) so PITR targets
actually resolve. The control-plane snapshot in `data_dir/control.json` keeps
the newest 128 markers
(`BACKUP_LOG_KEEP`); effective PITR depth is additionally bounded by
snapshot/WAL retention on disk.

## Snapshots

Snapshots capture backend files with their max commit. Requires write auth:

```sh
ryme snapshot create
```

Response shape: `{ "commit": <u64>, "files": [<names>] }`. Snapshots are the
unit that gets shipped to the archive and the unit you copy back for
single-node disaster recovery.

Durable managers append an immutable checksummed delta `.sst` segment under
`<wal-dir>/segments` for each committed write, and write a full sorted base
segment for each snapshot. Segments carry a sparse key index plus Bloom filter
metadata. Startup merges the ordered base-plus-delta set when the snapshot
pointer is unavailable; WAL replay still covers commits newer than the newest
segment.

When a shard reaches 64 segment files, the retention loop compacts its current
MVCC state into one new full base segment and removes superseded files. The
compaction holds the transaction commit gate while taking its consistent base,
so it cannot publish a partial state.

Point reads use a bounded decoded-segment LRU sized by `cache_bytes`. The
storage layer exposes occupancy, hit, and miss counters through
`SegmentStore::cache_stats`; entries are invalidated when a segment is
rewritten, compacted, or pruned. Set `storage_mode` to `standard` to avoid
materializing the full segment history at startup; point reads and bounded
scans resolve versions directly from the immutable segment chain.

PITR restore publishes a new immutable base at the selected commit, removes
future segment/snapshot state, truncates future WAL records, and updates the
latest snapshot pointer. A restart therefore keeps the restored state instead
of replaying writes that occurred after the restore target.

Archives include the SQL and control-plane metadata files: `schema.json`,
`branches.json`, `control.json`, `topics.json`, and `auth.json`. Branch schema
snapshots are stored under `branch-schemas/<tenant>/<branch>.json`. Restore
preserves those relative paths, so branch definitions and branch-only tables
survive a disaster-recovery restore alongside the committed rows. The server
also archives immutable `.sst` segment artifacts, and rebuilds
secondary-index entries from the committed rows.

## Archive

With an archive target configured (`archive.local_dir` or the S3
`endpoint`/`bucket`/`region` settings), the server archives on
`interval_secs` (default 300s, keeping `keep` = 7) and on demand:

```sh
ryme backup archive
ryme backup archive --backup <backup-id>
ryme backup archives
```

Archiving requires an `Owner`/`Admin` principal: on-demand archives and
cross-region copies drive full data reads plus object-store egress, so
application keys cannot trigger them. Checkpoints stay write-level since
they are metadata-only. Listing requires any valid credential, and `400`
when no archive target is configured.

## Cross-region copies

Set `archive_replica` (same shape as `archive`: a second local dir or a
different S3 endpoint/bucket/region) and copy any archived backup to it
with hash verification on both sides:

```sh
ryme backup copy <backup-id>
```

The copy replays every file through SHA-256 checks against the source
manifest, verifies the bytes read back from the replica, then writes
the manifest, so a half-finished copy never leaves a valid-looking
manifest behind. Missing replica configuration answers `400`;
unknown backup IDs answer `404`.

## Envelope encryption and restore drills

Every archived file is sealed with the node's tenant DEK (`ryme-crypto`
key ring, `data_dir/keyring.json`, mode 0600, rotated per tenant). Set
`RYME_KMS_KEY` (base64url or raw, >= 16 bytes) to persist the ring
KEK-wrapped instead of plaintext; without it the log warns and volume
encryption remains the outer layer. Manifests record per-file
`dek_id`/`nonce`/`tag` plus sha256 of the sealed bytes.

Drill restores without touching live data:

```sh
ryme backup verify <backup-id>
```

`GET /v1/backups/verify?backup_id=` re-reads every file, checks sha256,
decrypts each envelope with the ring, and returns
`{backup_id, commit, verified, encrypted}`. Run it at least weekly; alert
when `verified` drops below the file count or the call fails.

Set `archive.verify_interval_secs` (`--verify-interval-secs`, `0`
disables) to run the same verification automatically against the latest
backup on a cadence
(weekly `604800` in production). The last report is served at
`GET /v1/backups/drill` (`404` until the first drill completes) as
`{backup_id, verified_files, at_unix, error}`; alert when `error` is set
or `at_unix` goes stale.

## Restore

```sh
ryme backup restore <target-unix-ts>
```

Restore requires write auth and replays the WAL to the target timestamp,
returning per-shard replay counts (`{ "target": ..., "replayed": [...] }`).

## Retention

Every sweep interval the server collects MVCC versions invisible to all live
transactions, prunes local snapshots down to `archive.snapshot_keep` (default
8) per shard, and deletes WAL segments whose records all precede the oldest
retained snapshot. Point-in-time restore therefore reaches back to the oldest
retained snapshot; older targets return `404 {"error":"snapshot"}`. Sealed
archives are unaffected and remain the long-term recovery path.

**Hard rule: restore is unavailable in cluster mode.** Both `Cluster` and
`Hybrid` backends answer `503 {"error":"restore unavailable in cluster
mode"}`. Only `Single` and `Sharded` backends restore. To recover a cluster,
bring up fresh nodes and re-add them through the membership API instead (see
`docs/cluster-operations.md`).

## S3 settings

Archive to S3 with `archive.s3_endpoint`, `archive.s3_bucket`,
`archive.s3_region`, and `archive.s3_path_style`. Validation rejects an
endpoint without bucket and region. For local archives, set
`archive.local_dir`.
