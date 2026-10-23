use ryme_error::{Result, RymeError};
pub use ryme_storage::StorageMode;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;

const MAX_CONFIG_FILE_BYTES: u64 = 4 * 1024 * 1024;

fn loopback(port: u16) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub node_id: String,
    pub pg_listen: SocketAddr,
    pub resp_listen: SocketAddr,
    pub http_listen: SocketAddr,
    #[serde(default)]
    pub native_listen: Option<SocketAddr>,
    #[serde(default)]
    pub grpc_tls_listen: Option<SocketAddr>,
    #[serde(default)]
    pub grpc_listen: Option<SocketAddr>,
    #[serde(default)]
    pub native_tls_listen: Option<SocketAddr>,
    #[serde(default)]
    pub tls_cert_pem: Option<PathBuf>,
    #[serde(default)]
    pub tls_key_pem: Option<PathBuf>,
    #[serde(default)]
    pub tls_client_ca_pem: Option<PathBuf>,
    #[serde(default)]
    pub https_listen: Option<SocketAddr>,
    #[serde(default)]
    pub resp_tls_listen: Option<SocketAddr>,
    #[serde(default)]
    pub raft_tls: bool,
    pub data_dir: PathBuf,
    pub durability: Durability,
    #[serde(default)]
    pub storage_mode: StorageMode,
    pub cache_bytes: u64,
    pub max_connections: u32,
    #[serde(default = "default_http_max_body_bytes")]
    pub http_max_body_bytes: u64,
    #[serde(default)]
    pub archive: ArchiveConfig,
    #[serde(default)]
    pub archive_replica: Option<ArchiveConfig>,
    #[serde(default)]
    pub cluster: ClusterConfig,
    #[serde(default = "default_sweep_interval")]
    pub sweep_interval_secs: u64,
    #[serde(default = "default_shards")]
    pub shards: usize,
    #[serde(default)]
    pub replicated_tables: Vec<String>,
    #[serde(default = "default_index_partitions")]
    pub index_partitions: usize,
    #[serde(default = "default_region")]
    pub region: String,
    #[serde(default)]
    pub read_only: bool,
    #[serde(default)]
    pub otel: OtelConfig,
    #[serde(default)]
    pub autosplit_writes: u64,
    #[serde(default = "default_autosplit_interval")]
    pub autosplit_interval_secs: u64,
    #[serde(default)]
    pub passkey_rp_id: String,
    #[serde(default)]
    pub passkey_origins: Vec<String>,
    #[serde(default)]
    pub rls_tables: HashMap<String, String>,
}

fn default_autosplit_interval() -> u64 {
    30
}

fn default_region() -> String {
    String::from("local-1")
}

const fn default_http_max_body_bytes() -> u64 {
    2 * 1024 * 1024
}

const fn max_http_body_bytes() -> u64 {
    64 * 1024 * 1024
}

const MAX_RLS_NAME_BYTES: usize = 256;

fn default_otel_service() -> String {
    String::from("rymedb")
}

fn default_otel_interval() -> u64 {
    30
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OtelConfig {
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default = "default_otel_service")]
    pub service: String,
    #[serde(default = "default_otel_interval")]
    pub interval_secs: u64,
}

impl Default for OtelConfig {
    fn default() -> Self {
        Self {
            endpoint: None,
            service: default_otel_service(),
            interval_secs: default_otel_interval(),
        }
    }
}

fn default_shards() -> usize {
    1
}

fn default_index_partitions() -> usize {
    4
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchiveConfig {
    #[serde(default)]
    pub local_dir: Option<PathBuf>,
    #[serde(default)]
    pub s3_endpoint: Option<String>,
    #[serde(default)]
    pub s3_bucket: Option<String>,
    #[serde(default)]
    pub s3_region: Option<String>,
    #[serde(default)]
    pub s3_path_style: bool,
    #[serde(default)]
    pub s3_allow_insecure_http: bool,
    #[serde(default = "default_archive_interval")]
    pub interval_secs: u64,
    #[serde(default = "default_verify_interval")]
    pub verify_interval_secs: u64,
    #[serde(default = "default_archive_keep")]
    pub keep: usize,
    #[serde(default = "default_snapshot_keep")]
    pub snapshot_keep: usize,
}

fn default_snapshot_keep() -> usize {
    8
}

fn default_archive_interval() -> u64 {
    300
}

fn default_verify_interval() -> u64 {
    0
}

fn default_archive_keep() -> usize {
    7
}

fn default_sweep_interval() -> u64 {
    30
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftPeer {
    pub id: usize,
    pub addr: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterConfig {
    #[serde(default)]
    pub node_index: usize,
    #[serde(default)]
    pub raft_listen: Option<SocketAddr>,
    #[serde(default)]
    pub peers: Vec<RaftPeer>,
    #[serde(default)]
    pub learner: bool,
    #[serde(default)]
    pub advertise_addr: Option<String>,
}

impl Default for ArchiveConfig {
    fn default() -> Self {
        Self {
            local_dir: None,
            s3_endpoint: None,
            s3_bucket: None,
            s3_region: None,
            s3_path_style: false,
            s3_allow_insecure_http: false,
            interval_secs: default_archive_interval(),
            verify_interval_secs: default_verify_interval(),
            keep: default_archive_keep(),
            snapshot_keep: default_snapshot_keep(),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum Durability {
    Strict,
    RegionalFast,
    LocalDurable,
    Memory,
}

impl Durability {
    pub fn is_durable(self) -> bool {
        !matches!(self, Durability::Memory)
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            node_id: String::from("ryme-0"),
            pg_listen: loopback(5433),
            resp_listen: loopback(6380),
            native_listen: None,
            grpc_tls_listen: None,
            grpc_listen: None,
            native_tls_listen: None,
            tls_cert_pem: None,
            tls_key_pem: None,
            tls_client_ca_pem: None,
            https_listen: None,
            resp_tls_listen: None,
            raft_tls: false,
            http_listen: loopback(8080),
            data_dir: PathBuf::from("/var/lib/rymedb"),
            durability: Durability::LocalDurable,
            storage_mode: StorageMode::Hot,
            cache_bytes: 64 * 1024 * 1024,
            max_connections: 10000,
            http_max_body_bytes: default_http_max_body_bytes(),
            archive: ArchiveConfig::default(),
            archive_replica: None,
            cluster: ClusterConfig::default(),
            sweep_interval_secs: default_sweep_interval(),
            shards: default_shards(),
            index_partitions: default_index_partitions(),
            replicated_tables: Vec::new(),
            region: default_region(),
            read_only: false,
            otel: OtelConfig::default(),
            autosplit_writes: 0,
            autosplit_interval_secs: default_autosplit_interval(),
            passkey_rp_id: String::new(),
            passkey_origins: Vec::new(),
            rls_tables: HashMap::new(),
        }
    }
}

impl Config {
    pub fn validate(&self) -> Result<()> {
        if self.node_id.is_empty() {
            return Err(RymeError::InvalidArgument(String::from("node_id")));
        }
        if self.cache_bytes < 1024 * 1024 {
            return Err(RymeError::InvalidArgument(String::from("cache_bytes")));
        }
        if self.max_connections == 0 {
            return Err(RymeError::InvalidArgument(String::from("max_connections")));
        }
        if self.http_max_body_bytes == 0 || self.http_max_body_bytes > max_http_body_bytes() {
            return Err(RymeError::InvalidArgument(String::from("http_max_body_bytes")));
        }
        if self.shards == 0 || self.shards > 256 {
            return Err(RymeError::InvalidArgument(String::from("shards")));
        }
        if self.index_partitions == 0 || self.index_partitions > 64 {
            return Err(RymeError::InvalidArgument(String::from("index_partitions")));
        }
        if self.rls_tables.iter().any(|(table, column)| {
            table.is_empty()
                || table.len() > MAX_RLS_NAME_BYTES
                || table.contains('\0')
                || column.is_empty()
                || column.len() > MAX_RLS_NAME_BYTES
                || column.contains('\0')
        }) {
            return Err(RymeError::InvalidArgument(String::from("rls_tables")));
        }
        if self.cluster.raft_listen.is_some() && self.shards > 1 {
            return Err(RymeError::InvalidArgument(String::from("cluster sharding")));
        }
        if let Some(raft_listen) = self.cluster.raft_listen {
            if !raft_listen.ip().is_loopback() && !self.raft_tls {
                return Err(RymeError::InvalidArgument(String::from("raft tls")));
            }
        }
        if !self.replicated_tables.is_empty() && self.cluster.raft_listen.is_none() {
            return Err(RymeError::InvalidArgument(String::from("replicated_tables")));
        }
        if self.archive.s3_endpoint.is_some()
            && (self.archive.s3_bucket.is_none() || self.archive.s3_region.is_none())
        {
            return Err(RymeError::InvalidArgument(String::from("s3 bucket/region")));
        }
        if (self.native_tls_listen.is_some()
            || self.https_listen.is_some()
            || self.resp_tls_listen.is_some())
            && (self.tls_cert_pem.is_none() || self.tls_key_pem.is_none())
        {
            return Err(RymeError::InvalidArgument(String::from("tls cert/key")));
        }
        if self.tls_cert_pem.is_some() != self.tls_key_pem.is_some() {
            return Err(RymeError::InvalidArgument(String::from("tls cert/key")));
        }
        if self.tls_client_ca_pem.is_some() && self.tls_cert_pem.is_none() {
            return Err(RymeError::InvalidArgument(String::from("tls client ca")));
        }
        if self.raft_tls {
            if self.cluster.raft_listen.is_none() {
                return Err(RymeError::InvalidArgument(String::from("raft tls")));
            }
            if self.tls_cert_pem.is_none()
                || self.tls_key_pem.is_none()
                || self.tls_client_ca_pem.is_none()
            {
                return Err(RymeError::InvalidArgument(String::from("tls cert/key")));
            }
        }
        Ok(())
    }

    pub fn from_file(path: &std::path::Path) -> Result<Self> {
        let file = File::open(path)?;
        let metadata = file.metadata()?;
        if !metadata.is_file() {
            return Err(RymeError::InvalidArgument(String::from("config file")));
        }
        if metadata.len() > MAX_CONFIG_FILE_BYTES {
            return Err(RymeError::Overload(String::from("config file")));
        }
        let mut raw = String::new();
        raw.try_reserve_exact(metadata.len() as usize)
            .map_err(|_| RymeError::Overload(String::from("config file")))?;
        file.take(MAX_CONFIG_FILE_BYTES + 1).read_to_string(&mut raw)?;
        if raw.len() as u64 > MAX_CONFIG_FILE_BYTES {
            return Err(RymeError::Overload(String::from("config file")));
        }
        serde_json::from_str(&raw).map_err(|e| RymeError::InvalidArgument(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_validate() {
        let config = Config::default();
        assert!(!config.raft_tls);
        assert!(config.validate().is_ok());
        assert!(config.otel.endpoint.is_none());
        assert_eq!(config.otel.service, "rymedb");
        assert_eq!(config.otel.interval_secs, 30);
        assert_eq!(config.http_max_body_bytes, 2 * 1024 * 1024);
    }

    #[test]
    fn raft_tls_requires_cluster_and_certs() {
        let base = Config::default();
        let bare = Config { raft_tls: true, ..Config::default() };
        assert!(bare.validate().is_err());
        let mut cluster = base.cluster.clone();
        cluster.raft_listen = Some(loopback(9080));
        let no_certs = Config { raft_tls: true, cluster, ..Config::default() };
        assert!(no_certs.validate().is_err());
        let full = Config {
            raft_tls: true,
            cluster: no_certs.cluster.clone(),
            tls_cert_pem: Some(PathBuf::from("cert.pem")),
            tls_key_pem: Some(PathBuf::from("key.pem")),
            tls_client_ca_pem: Some(PathBuf::from("ca.pem")),
            ..Config::default()
        };
        assert!(full.validate().is_ok());
    }

    #[test]
    fn non_loopback_raft_listener_requires_tls() {
        let cluster = ClusterConfig {
            raft_listen: Some(SocketAddr::from(([10, 0, 0, 10], 9080))),
            ..ClusterConfig::default()
        };
        let config = Config { cluster, ..Config::default() };
        assert!(config.validate().is_err());

        let loopback_cluster =
            ClusterConfig { raft_listen: Some(loopback(9080)), ..ClusterConfig::default() };
        let loopback_config = Config { cluster: loopback_cluster, ..Config::default() };
        assert!(loopback_config.validate().is_ok());
    }

    #[test]
    fn index_partitions_bounded() {
        for (partitions, valid) in [(0, false), (65, false), (8, true), (64, true)] {
            let config = Config { index_partitions: partitions, ..Config::default() };
            assert_eq!(config.validate().is_ok(), valid, "{partitions}");
        }
    }

    #[test]
    fn http_body_limit_is_bounded() {
        for (limit, valid) in [
            (0, false),
            (2 * 1024 * 1024, true),
            (64 * 1024 * 1024, true),
            (64 * 1024 * 1024 + 1, false),
        ] {
            let config = Config { http_max_body_bytes: limit, ..Config::default() };
            assert_eq!(config.validate().is_ok(), valid, "{limit}");
        }
    }

    #[test]
    fn rls_table_and_tenant_field_names_must_be_valid() {
        for rls_tables in [
            HashMap::from([(String::from("messages"), String::new())]),
            HashMap::from([(String::new(), String::from("tenant_id"))]),
            HashMap::from([(String::from("messages"), String::from("tenant\0id"))]),
            HashMap::from([("t".repeat(MAX_RLS_NAME_BYTES + 1), String::from("tenant_id"))]),
            HashMap::from([(String::from("messages"), "c".repeat(MAX_RLS_NAME_BYTES + 1))]),
        ] {
            let config = Config { rls_tables, ..Config::default() };
            assert!(
                matches!(config.validate(), Err(RymeError::InvalidArgument(name)) if name == "rls_tables")
            );
        }

        let config = Config {
            rls_tables: HashMap::from([(String::from("messages"), String::from("tenant_id"))]),
            ..Config::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn only_memory_relaxes_durability() {
        assert!(Durability::Strict.is_durable());
        assert!(Durability::RegionalFast.is_durable());
        assert!(Durability::LocalDurable.is_durable());
        assert!(!Durability::Memory.is_durable());
    }

    #[test]
    fn storage_mode_roundtrips() {
        let config = Config { storage_mode: StorageMode::Standard, ..Config::default() };
        let raw = serde_json::to_string(&config).unwrap();
        assert!(raw.contains("\"storage_mode\":\"standard\""));
        let loaded: Config = serde_json::from_str(&raw).unwrap();
        assert_eq!(loaded.storage_mode, StorageMode::Standard);
    }

    #[test]
    fn unknown_fields_rejected() {
        let dir = std::env::temp_dir();
        let typo = dir.join("ryme-config-typo.json");
        std::fs::write(
            &typo,
            r#"{"node_id":"n","pg_listen":"127.0.0.1:5433","resp_listen":"127.0.0.1:6380","http_listen":"127.0.0.1:8080","data_dir":"/tmp/ryme-config","durability":"local-durable","cache_bytes":1048576,"max_connections":1,"autosplit_write":5}"#,
        )
        .unwrap();
        let err = Config::from_file(&typo).unwrap_err().to_string();
        assert!(err.contains("autosplit_write"), "{err}");
        let nested = dir.join("ryme-config-nested-typo.json");
        std::fs::write(
            &nested,
            r#"{"node_id":"n","pg_listen":"127.0.0.1:5433","resp_listen":"127.0.0.1:6380","http_listen":"127.0.0.1:8080","data_dir":"/tmp/ryme-config","durability":"local-durable","cache_bytes":1048576,"max_connections":1,"otel":{"endpoin":"x"}}"#,
        )
        .unwrap();
        assert!(Config::from_file(&nested).is_err());
        let rls = dir.join("ryme-config-rls.json");
        std::fs::write(
            &rls,
            r#"{"node_id":"n","pg_listen":"127.0.0.1:5433","resp_listen":"127.0.0.1:6380","http_listen":"127.0.0.1:8080","data_dir":"/tmp/ryme-config","durability":"local-durable","cache_bytes":1048576,"max_connections":1,"rls_tables":{"messages":"tenant_id"}}"#,
        )
        .unwrap();
        let parsed = Config::from_file(&rls).unwrap();
        assert_eq!(parsed.rls_tables.get("messages"), Some(&String::from("tenant_id")));
        let valid = dir.join("ryme-config-valid.json");
        let config = Config {
            autosplit_writes: 7,
            otel: OtelConfig { service: String::from("svc"), ..OtelConfig::default() },
            ..Config::default()
        };
        std::fs::write(&valid, serde_json::to_string(&config).unwrap()).unwrap();
        let loaded = Config::from_file(&valid).unwrap();
        assert_eq!(loaded.autosplit_writes, 7);
        assert_eq!(loaded.otel.service, "svc");
        assert!(loaded.validate().is_ok());
        let _ = std::fs::remove_file(&typo);
        let _ = std::fs::remove_file(&nested);
        let _ = std::fs::remove_file(&valid);
    }
}
