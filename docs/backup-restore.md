# Backups, snapshots, and point-in-time recovery

rymeDB has separate mechanisms for a checkpoint marker, a local snapshot, an
archive copy, and a restore. Test all four in a disposable data directory
before declaring a recovery plan.

## Recovery layers

| Layer | Purpose | Typical command |
| --- | --- | --- |
| Checkpoint | Record a commit/timestamp marker | `rymedb-cli backup checkpoint` |
| Snapshot | Capture backend files at a commit | `rymedb-cli snapshot create` |
| Archive | Copy snapshots and metadata to local/S3 storage | `rymedb-cli backup archive` |
| PITR restore | Rewind WAL/segments to a timestamp | `rymedb-cli backup restore <unix-ts>` |
| Restore drill | Verify hashes/decryption without touching live data | `rymedb-cli backup verify <backup-id>` |

Checkpoints are metadata. They do not create a durable copy by themselves;
retention must keep the relevant snapshot and WAL segments available.
WAL recovery visits records in LSN order one segment at a time, so replay does
not keep every historical record payload in memory at once.
WAL segment paths must be regular files; symlinks and other file types are
rejected during discovery and recovery. Segment discovery accepts only the
canonical `seg-` plus 20-digit ID plus `.wal` filename format.
WAL archive enumeration also fails on directory read errors and rejects
symlink or non-file entries that use a segment filename.
Local archive objects and restored files are written through unique temporary
files created exclusively in their destination directories.
On Unix, local archive writes sync the published object's containing directory
and any newly created directory entries before reporting success.
The configured local archive root must be a directory, not a symlink; missing
roots are created when objects are written.
Local and S3 object listings are capped at 100,000 keys and 64 MiB of key
names. Local listing prefixes must be relative paths; traversal also caps
visited directories and nesting depth so a large or deeply nested archive
directory cannot grow the listing stack without bound.

## Configure retention

```json
{
  "archive": {
    "local_dir": "/backup/rymedb",
    "interval_secs": 300,
    "verify_interval_secs": 604800,
    "keep": 7,
    "snapshot_keep": 8
  },
  "archive_replica": {
    "s3_endpoint": "https://s3.example.com",
    "s3_bucket": "rymedb-secondary",
    "s3_region": "us-east-1",
    "s3_path_style": true
  }
}
```

An S3 target needs endpoint, bucket, and region. `keep` controls archived
manifests; `snapshot_keep` controls local snapshots and therefore the local
PITR window. HTTPS is required by default; use `s3_allow_insecure_http` only
for an intentionally isolated local/private development endpoint. Long-term
recovery requires archived copies.

## Operate a backup

```bash
rymedb-cli backup checkpoint --manifest before-release
rymedb-cli snapshot create
rymedb-cli backup archive
rymedb-cli backup archives
rymedb-cli backup latest
rymedb-cli backup pitr 1760000000
```

Archive and restore operations require a platform superuser. Listing and
read-only inspection also require a platform superuser because archives include schema,
branch, control-plane, auth, topic, placement, WAL, and immutable segment
artifacts as applicable.

## Restore

Always stop application writes or put the service in a controlled maintenance
state before a planned restore:

```bash
rymedb-cli backup restore 1760000000
curl -fsS -H "Authorization: Bearer $RYME_API_KEY" http://127.0.0.1:8080/ready
```

The restore target must not be newer than the latest commit and must be within
the retained snapshot/WAL window. Future segment state and WAL records are
trimmed. In a Raft cluster the restore command is coordinated through the
leader; range-sharded and hybrid deployments report the exact scope restored.

## Verify and drill

```bash
rymedb-cli backup verify backup-2026-01-01
rymedb-cli backup drill
curl -H "Authorization: Bearer $RYME_API_KEY" \
  http://127.0.0.1:8080/v1/backups/drill
```

Verification checks manifest hashes and decrypts archived files into a unique
temporary directory. Schedule it at least weekly and alert on a stale or
failed report. A backup that has never been restored is an assumption, not a
recovery plan.

## Encryption and permissions

Archive files are sealed with tenant data-encryption keys in
`data_dir/keyring.json`. Set `RYME_KMS_KEY` to wrap the key ring with an
external key; otherwise rely on disk/object-store encryption and strict file
permissions. Keep the key ring and KMS material separate from the data copy.

## Recovery checklist

1. Identify the last verified backup and target timestamp.
2. Confirm the target is older than the latest commit and inside retention.
3. Preserve current logs and data directory before an emergency restore.
4. Restore into a disposable directory first when time allows.
5. Verify row counts, schema, branches, auth, and application invariants.
6. Reopen traffic gradually and record the recovery point and duration.
