CREATE TABLE IF NOT EXISTS rymesys_databases (
  tenant TEXT NOT NULL,
  database TEXT NOT NULL,
  branch TEXT NOT NULL,
  consistency TEXT NOT NULL,
  created_unix BIGINT NOT NULL,
  PRIMARY KEY (tenant, database, branch)
);
CREATE TABLE IF NOT EXISTS rymesys_branches (
  branch_id TEXT PRIMARY KEY,
  parent_id TEXT,
  base_commit_ts BIGINT NOT NULL,
  manifest_id TEXT NOT NULL,
  schema_version BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS rymesys_migrations (
  migration_id TEXT PRIMARY KEY,
  checksum TEXT NOT NULL,
  applied_unix BIGINT NOT NULL,
  result_schema_version BIGINT NOT NULL
);
