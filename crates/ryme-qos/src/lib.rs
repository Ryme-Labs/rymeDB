use ryme_error::{Result, RymeError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Tier {
    Shared,
    DedicatedShard,
    DedicatedCluster,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Quota {
    pub read_qps: u64,
    pub write_qps: u64,
    pub egress_bytes_per_sec: u64,
    pub realtime_msg_per_sec: u64,
    pub max_connections: u64,
    pub max_storage_bytes: u64,
}

impl Quota {
    pub fn for_tier(tier: Tier) -> Self {
        match tier {
            Tier::Shared => Self {
                read_qps: 5_000,
                write_qps: 1_000,
                egress_bytes_per_sec: 4 * 1024 * 1024,
                realtime_msg_per_sec: 10_000,
                max_connections: 500,
                max_storage_bytes: 10 * 1024 * 1024 * 1024,
            },
            Tier::DedicatedShard => Self {
                read_qps: 100_000,
                write_qps: 30_000,
                egress_bytes_per_sec: 256 * 1024 * 1024,
                realtime_msg_per_sec: 200_000,
                max_connections: 10_000,
                max_storage_bytes: 1024 * 1024 * 1024 * 1024,
            },
            Tier::DedicatedCluster => Self {
                read_qps: 1_000_000,
                write_qps: 300_000,
                egress_bytes_per_sec: 2 * 1024 * 1024 * 1024,
                realtime_msg_per_sec: 1_000_000,
                max_connections: 100_000,
                max_storage_bytes: u64::MAX,
            },
        }
    }
}

#[derive(Debug, Clone)]
struct Bucket {
    capacity: f64,
    tokens: f64,
    refill_per_sec: f64,
    last_nanos: u64,
}

impl Bucket {
    fn new(rate_per_sec: u64, now_nanos: u64) -> Self {
        let rate = rate_per_sec.max(1) as f64;
        Self { capacity: rate, tokens: rate, refill_per_sec: rate, last_nanos: now_nanos }
    }

    fn take(&mut self, amount: f64, now_nanos: u64) -> bool {
        let elapsed = now_nanos.saturating_sub(self.last_nanos) as f64 / 1_000_000_000.0;
        self.last_nanos = now_nanos;
        self.tokens = (self.tokens + elapsed * self.refill_per_sec).min(self.capacity);
        if self.tokens >= amount {
            self.tokens -= amount;
            true
        } else {
            false
        }
    }
}

#[derive(Debug, Clone)]
struct TenantState {
    tier: Tier,
    quota: Quota,
    read_bucket: Bucket,
    write_bucket: Bucket,
    egress_bucket: Bucket,
    realtime_bucket: Bucket,
    active_connections: u64,
    stored_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantView {
    pub tenant: String,
    pub tier: Tier,
    pub quota: Quota,
    pub active_connections: u64,
    pub stored_bytes: u64,
}

#[derive(Debug, Default)]
pub struct QosRegistry {
    tenants: HashMap<String, TenantState>,
}

impl QosRegistry {
    pub fn new() -> Self {
        Self { tenants: HashMap::new() }
    }

    pub fn set_tier(&mut self, tenant: &str, tier: Tier, now_nanos: u64) {
        let quota = Quota::for_tier(tier);
        if let Some(state) = self.tenants.get_mut(tenant) {
            state.tier = tier;
            state.quota = quota;
            state.read_bucket = Bucket::new(quota.read_qps, now_nanos);
            state.write_bucket = Bucket::new(quota.write_qps, now_nanos);
            state.egress_bucket = Bucket::new(quota.egress_bytes_per_sec, now_nanos);
            state.realtime_bucket = Bucket::new(quota.realtime_msg_per_sec, now_nanos);
        } else {
            self.tenants.insert(
                tenant.to_string(),
                TenantState {
                    tier,
                    quota,
                    read_bucket: Bucket::new(quota.read_qps, now_nanos),
                    write_bucket: Bucket::new(quota.write_qps, now_nanos),
                    egress_bucket: Bucket::new(quota.egress_bytes_per_sec, now_nanos),
                    realtime_bucket: Bucket::new(quota.realtime_msg_per_sec, now_nanos),
                    active_connections: 0,
                    stored_bytes: 0,
                },
            );
        }
    }

    pub fn set_quota(&mut self, tenant: &str, quota: Quota, now_nanos: u64) {
        let entry = self.tenants.entry(tenant.to_string()).or_insert_with(|| TenantState {
            tier: Tier::Shared,
            quota,
            read_bucket: Bucket::new(quota.read_qps, now_nanos),
            write_bucket: Bucket::new(quota.write_qps, now_nanos),
            egress_bucket: Bucket::new(quota.egress_bytes_per_sec, now_nanos),
            realtime_bucket: Bucket::new(quota.realtime_msg_per_sec, now_nanos),
            active_connections: 0,
            stored_bytes: 0,
        });
        entry.quota = quota;
        entry.read_bucket = Bucket::new(quota.read_qps, now_nanos);
        entry.write_bucket = Bucket::new(quota.write_qps, now_nanos);
        entry.egress_bucket = Bucket::new(quota.egress_bytes_per_sec, now_nanos);
        entry.realtime_bucket = Bucket::new(quota.realtime_msg_per_sec, now_nanos);
    }

    fn state_mut(&mut self, tenant: &str, now_nanos: u64) -> &mut TenantState {
        self.tenants.entry(tenant.to_string()).or_insert_with(|| {
            let quota = Quota::for_tier(Tier::Shared);
            TenantState {
                tier: Tier::Shared,
                quota,
                read_bucket: Bucket::new(quota.read_qps, now_nanos),
                write_bucket: Bucket::new(quota.write_qps, now_nanos),
                egress_bucket: Bucket::new(quota.egress_bytes_per_sec, now_nanos),
                realtime_bucket: Bucket::new(quota.realtime_msg_per_sec, now_nanos),
                active_connections: 0,
                stored_bytes: 0,
            }
        })
    }

    pub fn admit_read(&mut self, tenant: &str, now_nanos: u64) -> Result<()> {
        let state = self.state_mut(tenant, now_nanos);
        if state.read_bucket.take(1.0, now_nanos) {
            Ok(())
        } else {
            Err(RymeError::Overload(String::from("read quota")))
        }
    }

    pub fn admit_write(&mut self, tenant: &str, bytes: u64, now_nanos: u64) -> Result<()> {
        let state = self.state_mut(tenant, now_nanos);
        if state.stored_bytes.saturating_add(bytes) > state.quota.max_storage_bytes {
            return Err(RymeError::Overload(String::from("storage quota")));
        }
        if !state.write_bucket.take(1.0, now_nanos) {
            return Err(RymeError::Overload(String::from("write quota")));
        }
        if !state.egress_bucket.take(bytes as f64, now_nanos) {
            return Err(RymeError::Overload(String::from("egress quota")));
        }
        state.stored_bytes = state.stored_bytes.saturating_add(bytes);
        Ok(())
    }

    pub fn admit_realtime(&mut self, tenant: &str, messages: u64, now_nanos: u64) -> Result<()> {
        let state = self.state_mut(tenant, now_nanos);
        if state.realtime_bucket.take(messages as f64, now_nanos) {
            Ok(())
        } else {
            Err(RymeError::Overload(String::from("realtime quota")))
        }
    }

    pub fn connection_open(&mut self, tenant: &str, now_nanos: u64) -> Result<()> {
        let state = self.state_mut(tenant, now_nanos);
        if state.active_connections >= state.quota.max_connections {
            return Err(RymeError::Overload(String::from("connection quota")));
        }
        state.active_connections += 1;
        Ok(())
    }

    pub fn connection_close(&mut self, tenant: &str) {
        if let Some(state) = self.tenants.get_mut(tenant) {
            state.active_connections = state.active_connections.saturating_sub(1);
        }
    }

    pub fn release_bytes(&mut self, tenant: &str, bytes: u64) {
        if let Some(state) = self.tenants.get_mut(tenant) {
            state.stored_bytes = state.stored_bytes.saturating_sub(bytes);
        }
    }

    pub fn set_stored_bytes(&mut self, tenant: &str, bytes: u64) {
        if let Some(state) = self.tenants.get_mut(tenant) {
            state.stored_bytes = bytes;
        }
    }

    pub fn view(&self, tenant: &str) -> Option<TenantView> {
        self.tenants.get(tenant).map(|state| TenantView {
            tenant: tenant.to_string(),
            tier: state.tier,
            quota: state.quota,
            active_connections: state.active_connections,
            stored_bytes: state.stored_bytes,
        })
    }

    pub fn snapshot(&self) -> Vec<TenantView> {
        let mut out: Vec<TenantView> = self
            .tenants
            .iter()
            .map(|(tenant, state)| TenantView {
                tenant: tenant.clone(),
                tier: state.tier,
                quota: state.quota,
                active_connections: state.active_connections,
                stored_bytes: state.stored_bytes,
            })
            .collect();
        out.sort_by(|a, b| a.tenant.cmp(&b.tenant));
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_write_throttle() {
        let mut registry = QosRegistry::new();
        registry.set_quota(
            "t",
            Quota {
                read_qps: 1000,
                write_qps: 2,
                egress_bytes_per_sec: u64::MAX,
                realtime_msg_per_sec: 1000,
                max_connections: 10,
                max_storage_bytes: u64::MAX,
            },
            0,
        );
        assert!(registry.admit_write("t", 10, 0).is_ok());
        assert!(registry.admit_write("t", 10, 0).is_ok());
        assert!(registry.admit_write("t", 10, 0).is_err());
        assert!(registry.admit_write("t", 10, 2_000_000_000).is_ok());
    }

    #[test]
    fn tiers_raise_limits() {
        let shared = Quota::for_tier(Tier::Shared);
        let dedicated = Quota::for_tier(Tier::DedicatedShard);
        assert!(dedicated.write_qps > shared.write_qps);
        assert!(dedicated.max_connections > shared.max_connections);
    }

    #[test]
    fn connections_bounded() {
        let mut registry = QosRegistry::new();
        registry.set_quota(
            "t",
            Quota {
                read_qps: 1000,
                write_qps: 1000,
                egress_bytes_per_sec: u64::MAX,
                realtime_msg_per_sec: 1000,
                max_connections: 1,
                max_storage_bytes: u64::MAX,
            },
            0,
        );
        assert!(registry.connection_open("t", 0).is_ok());
        assert!(registry.connection_open("t", 0).is_err());
        registry.connection_close("t");
        assert!(registry.connection_open("t", 0).is_ok());
    }
}

#[cfg(test)]
mod reconcile_tests {
    use super::*;

    #[test]
    fn stored_bytes_reconcile_unblocks_writes() {
        let mut registry = QosRegistry::new();
        registry.set_quota(
            "t",
            Quota {
                read_qps: 1000,
                write_qps: u64::MAX,
                egress_bytes_per_sec: u64::MAX,
                realtime_msg_per_sec: 1000,
                max_connections: 10,
                max_storage_bytes: 100,
            },
            0,
        );
        assert!(registry.admit_write("t", 80, 0).is_ok());
        assert!(registry.admit_write("t", 80, 0).is_err());
        registry.set_stored_bytes("t", 10);
        assert!(registry.admit_write("t", 80, 0).is_ok());
        assert_eq!(registry.view("t").map(|v| v.stored_bytes), Some(90));
    }
}
