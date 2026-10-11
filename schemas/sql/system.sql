-- rymeDB internal-state reference projection
--
-- The server does not install these tables. Runtime metadata is persisted in
-- authenticated, atomically replaced files under data_dir (schema.json,
-- branches.json, control.json, topics.json, auth.json, and keyring.json).
-- This SQL is a stable relational projection for tooling and migrations; it
-- must not be treated as the bootstrap schema for an application database.

CREATE TABLE IF NOT EXISTS rymedb_system_branches (
  tenant TEXT NOT NULL,
  branch_id TEXT NOT NULL,
  parent_id TEXT,
  base_commit_ts BIGINT NOT NULL,
  storage_epoch BIGINT NOT NULL,
  manifest_id TEXT NOT NULL,
  schema_version BIGINT NOT NULL,
  PRIMARY KEY (tenant, branch_id)
);

CREATE TABLE IF NOT EXISTS rymedb_system_manifests (
  manifest_id TEXT PRIMARY KEY,
  segments JSONB NOT NULL,
  wal_start BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS rymedb_system_migrations (
  tenant TEXT NOT NULL,
  branch_id TEXT NOT NULL,
  migration_id TEXT NOT NULL,
  checksum TEXT NOT NULL,
  parent_checksum TEXT NOT NULL,
  applied_unix BIGINT NOT NULL,
  result_schema_version BIGINT NOT NULL,
  author TEXT NOT NULL,
  PRIMARY KEY (tenant, branch_id, migration_id)
);

CREATE TABLE IF NOT EXISTS rymedb_system_topics (
  tenant TEXT NOT NULL,
  partition_name TEXT NOT NULL,
  next_cursor BIGINT NOT NULL,
  retention BIGINT NOT NULL,
  PRIMARY KEY (tenant, partition_name)
);

CREATE TABLE IF NOT EXISTS rymedb_system_topic_messages (
  tenant TEXT NOT NULL,
  partition_name TEXT NOT NULL,
  cursor BIGINT NOT NULL,
  message_key BYTEA NOT NULL,
  message_value BYTEA NOT NULL,
  commit_ts BIGINT NOT NULL,
  PRIMARY KEY (tenant, partition_name, cursor)
);
