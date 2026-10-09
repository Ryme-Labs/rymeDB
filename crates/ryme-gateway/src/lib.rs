use ryme_auth::{PolicyEngine, Principal};
use ryme_error::{Result, RymeError};
use ryme_realtime::{NewChange, Operation, Realtime};
use ryme_storage::RecordKey;
use ryme_txn::{TxnBackend, TxnManager};
use std::sync::Arc;

pub const MAX_KEY_BYTES: usize = 1024;
pub const MAX_VALUE_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ttl {
    Missing,
    Persistent,
    Seconds(u64),
}

#[derive(Debug, Clone)]
pub struct Gateway<B = TxnManager> {
    manager: B,
    realtime: Realtime,
    policies: Arc<PolicyEngine>,
    tenant: String,
    database: String,
    branch: String,
    read_only: bool,
}

impl Gateway<TxnManager> {
    pub fn new(
        tenant: String,
        database: String,
        branch: String,
        policies: PolicyEngine,
        realtime: Realtime,
    ) -> Self {
        Self {
            manager: TxnManager::new(),
            realtime,
            policies: Arc::new(policies),
            tenant,
            database,
            branch,
            read_only: false,
        }
    }

    pub fn with_manager(
        tenant: String,
        database: String,
        branch: String,
        policies: PolicyEngine,
        realtime: Realtime,
        manager: TxnManager,
    ) -> Self {
        Self {
            manager,
            realtime,
            policies: Arc::new(policies),
            tenant,
            database,
            branch,
            read_only: false,
        }
    }
}

impl<B> Gateway<B>
where
    B: TxnBackend,
{
    pub fn with_backend(
        tenant: String,
        database: String,
        branch: String,
        policies: PolicyEngine,
        realtime: Realtime,
        manager: B,
    ) -> Self {
        Self {
            manager,
            realtime,
            policies: Arc::new(policies),
            tenant,
            database,
            branch,
            read_only: false,
        }
    }

    pub fn set_read_only(&mut self, read_only: bool) {
        self.read_only = read_only;
    }

    pub fn is_read_only(&self) -> bool {
        self.read_only
    }

    fn reject_if_read_only(&self) -> Result<()> {
        if self.read_only {
            return Err(RymeError::ReadOnly(String::from("read-only follower")));
        }
        Ok(())
    }

    pub fn get(&self, principal: &Principal, table: &str, pk: &[u8]) -> Result<Option<Vec<u8>>> {
        self.policies.predicate(principal, table)?;
        let key = RecordKey::new(&principal.tenant, &self.database, table, pk);
        let mut txn = self.manager.begin();
        let Some(value) = self.manager.get(&mut txn, &key)? else {
            return Ok(None);
        };
        if self.policies.row_allowed(principal, table, &value)? {
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    pub async fn put(
        &self,
        principal: &Principal,
        table: &str,
        pk: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<u64> {
        self.put_with_ttl(principal, table, pk, value, None).await
    }

    pub async fn put_with_ttl(
        &self,
        principal: &Principal,
        table: &str,
        pk: Vec<u8>,
        value: Vec<u8>,
        expires_at: Option<u64>,
    ) -> Result<u64> {
        self.reject_if_read_only()?;
        if table.is_empty() {
            return Err(RymeError::InvalidArgument(String::from("table")));
        }
        if pk.is_empty() || pk.len() > MAX_KEY_BYTES {
            return Err(RymeError::InvalidArgument(String::from("key")));
        }
        if value.len() > MAX_VALUE_BYTES {
            return Err(RymeError::Overload(String::from("value")));
        }
        self.policies.check_write_row(principal, table, &value)?;
        let key = RecordKey::new(&principal.tenant, &self.database, table, &pk);
        let mut txn = self.manager.begin();
        let existed = self.manager.get(&mut txn, &key)?.is_some();
        match expires_at {
            Some(ts) => self.manager.put_with_ttl(&mut txn, key, value.clone(), ts),
            None => self.manager.put(&mut txn, key, value.clone()),
        }
        let commit_ts = self.manager.commit(txn).await?;
        let op = if existed { Operation::Update } else { Operation::Insert };
        self.realtime.publish(NewChange {
            tenant: principal.tenant.clone(),
            database: self.database.clone(),
            branch: self.branch.clone(),
            table: table.to_string(),
            op,
            pk,
            after: Some(value),
            commit_ts,
        })?;
        self.refresh_table(principal, table, commit_ts);
        Ok(commit_ts)
    }

    pub async fn expire(
        &self,
        principal: &Principal,
        table: &str,
        pk: Vec<u8>,
        expires_at: Option<u64>,
    ) -> Result<bool> {
        self.reject_if_read_only()?;
        self.policies.check_write(principal, table)?;
        let key = RecordKey::new(&principal.tenant, &self.database, table, &pk);
        let mut txn = self.manager.begin();
        let Some(current) = self.manager.get(&mut txn, &key)? else {
            return Ok(false);
        };
        if !self.policies.row_allowed(principal, table, &current)? {
            return Ok(false);
        }
        match expires_at {
            Some(ts) => self.manager.put_with_ttl(&mut txn, key, current.clone(), ts),
            None => self.manager.put(&mut txn, key, current.clone()),
        }
        let commit_ts = self.manager.commit(txn).await?;
        self.realtime.publish(NewChange {
            tenant: principal.tenant.clone(),
            database: self.database.clone(),
            branch: self.branch.clone(),
            table: table.to_string(),
            op: Operation::Update,
            pk,
            after: Some(current),
            commit_ts,
        })?;
        self.refresh_table(principal, table, commit_ts);
        Ok(true)
    }

    pub fn ttl_of(&self, principal: &Principal, table: &str, pk: &[u8]) -> Result<Ttl> {
        self.policies.predicate(principal, table)?;
        let key = RecordKey::new(&principal.tenant, &self.database, table, pk);
        match self.manager.expires_at(&key)? {
            None => Ok(Ttl::Missing),
            Some(0) => Ok(Ttl::Persistent),
            Some(ts) => {
                let now = ryme_txn::now_unix();
                if ts <= now {
                    Ok(Ttl::Missing)
                } else {
                    Ok(Ttl::Seconds(ts - now))
                }
            }
        }
    }

    pub async fn delete(&self, principal: &Principal, table: &str, pk: Vec<u8>) -> Result<u64> {
        self.reject_if_read_only()?;
        self.policies.check_write(principal, table)?;
        let key = RecordKey::new(&principal.tenant, &self.database, table, &pk);
        let mut txn = self.manager.begin();
        let Some(current) = self.manager.get(&mut txn, &key)? else {
            return Err(RymeError::NotFound(String::from("row")));
        };
        if !self.policies.row_allowed(principal, table, &current)? {
            return Err(RymeError::NotFound(String::from("row")));
        }
        self.manager.delete(&mut txn, key);
        let commit_ts = self.manager.commit(txn).await?;
        self.realtime.publish(NewChange {
            tenant: principal.tenant.clone(),
            database: self.database.clone(),
            branch: self.branch.clone(),
            table: table.to_string(),
            op: Operation::Delete,
            pk,
            after: None,
            commit_ts,
        })?;
        self.refresh_table(principal, table, commit_ts);
        Ok(commit_ts)
    }

    pub fn masked(&self, table: &str, value: Vec<u8>) -> Vec<u8> {
        self.policies.masked_value(table, &value)
    }

    pub fn set_mask(&self, table: String, fields: Vec<String>) {
        self.policies.mask_fields(table, fields);
    }

    pub fn scan(
        &self,
        principal: &Principal,
        table: &str,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.policies.predicate(principal, table)?;
        let mut txn = self.manager.begin();
        if !self.policies.has_table_policy(table) {
            return self.manager.scan(&mut txn, &principal.tenant, &self.database, table, limit);
        }
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut visible = Vec::with_capacity(limit);
        let mut start_after = None;
        loop {
            let page = match start_after.as_deref() {
                Some(start_after) => self.manager.scan_after(
                    &mut txn,
                    &principal.tenant,
                    &self.database,
                    table,
                    start_after,
                    limit,
                )?,
                None => {
                    self.manager.scan(&mut txn, &principal.tenant, &self.database, table, limit)?
                }
            };
            if page.is_empty() {
                break;
            }
            let page_len = page.len();
            start_after = page.last().map(|(pk, _)| pk.clone());
            for (pk, value) in page {
                if self.policies.row_allowed(principal, table, &value)? {
                    visible.push((pk, value));
                    if visible.len() == limit {
                        return Ok(visible);
                    }
                }
            }
            if page_len < limit {
                break;
            }
        }
        Ok(visible)
    }

    pub fn manager_clone(&self) -> B
    where
        B: Clone,
    {
        self.manager.clone()
    }

    pub fn realtime(&self) -> Realtime {
        self.realtime.clone()
    }

    pub fn tenant_name(&self) -> &str {
        &self.tenant
    }

    pub fn database_name(&self) -> &str {
        &self.database
    }

    fn refresh_table(&self, principal: &Principal, table: &str, commit_ts: u64) {
        let Some(limit) = self.realtime.query_limit(&principal.tenant, &self.database, table)
        else {
            return;
        };
        let rows = self.scan(principal, table, limit).unwrap_or_default();
        let _ = self.realtime.publish_query(
            &principal.tenant,
            &self.database,
            table,
            commit_ts,
            rows,
            limit,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn owner() -> Principal {
        Principal {
            id: String::from("u"),
            tenant: String::from("t"),
            roles: [ryme_auth::Role::Owner].into_iter().collect(),
        }
    }

    fn gateway() -> Gateway {
        Gateway::new(
            String::from("t"),
            String::from("d"),
            String::from("main"),
            PolicyEngine::new(),
            Realtime::new(16),
        )
    }

    #[tokio::test]
    async fn put_rejects_shape_violations() {
        let gateway = gateway();
        let principal = owner();
        assert!(gateway.put(&principal, "", b"k".to_vec(), b"v".to_vec()).await.is_err());
        assert!(gateway.put(&principal, "docs", Vec::new(), b"v".to_vec()).await.is_err());
        assert!(gateway
            .put(&principal, "docs", vec![b'k'; MAX_KEY_BYTES + 1], b"v".to_vec())
            .await
            .is_err());
        assert!(gateway
            .put(&principal, "docs", b"k".to_vec(), vec![b'v'; MAX_VALUE_BYTES + 1])
            .await
            .is_err());
    }

    #[tokio::test]
    async fn tenant_policy_filters_reads_and_writes() {
        let mut policies = PolicyEngine::new();
        policies.allow_table(String::from("messages"), String::from("tenant_id"));
        let gateway = Gateway::new(
            String::from("tenant-a"),
            String::from("d"),
            String::from("main"),
            policies,
            Realtime::new(16),
        );
        let principal = Principal {
            id: String::from("ada"),
            tenant: String::from("tenant-a"),
            roles: [ryme_auth::Role::ReadWrite].into_iter().collect(),
        };
        gateway
            .put(
                &principal,
                "messages",
                b"visible".to_vec(),
                br#"{"tenant_id":"tenant-a","body":"hello"}"#.to_vec(),
            )
            .await
            .unwrap();
        assert!(gateway.get(&principal, "messages", b"visible").unwrap().is_some());
        assert!(gateway
            .put(
                &principal,
                "messages",
                b"hidden".to_vec(),
                br#"{"tenant_id":"tenant-b","body":"secret"}"#.to_vec(),
            )
            .await
            .is_err());

        let manager = gateway.manager_clone();
        let mut txn = manager.begin();
        manager.put(
            &mut txn,
            RecordKey::new("tenant-a", "d", "messages", b"hidden"),
            br#"{"tenant_id":"tenant-b","body":"secret"}"#.to_vec(),
        );
        manager.commit(txn).unwrap();
        assert!(gateway.get(&principal, "messages", b"hidden").unwrap().is_none());
        let rows = gateway.scan(&principal, "messages", 10).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, b"visible");
        let other_tenant = Principal {
            id: String::from("mallory"),
            tenant: String::from("tenant-b"),
            roles: [ryme_auth::Role::ReadOnly].into_iter().collect(),
        };
        assert!(gateway.get(&other_tenant, "messages", b"visible").unwrap().is_none());
    }

    #[tokio::test]
    async fn reactive_updates_filter_hidden_rows() {
        let mut policies = PolicyEngine::new();
        policies.allow_table(String::from("messages"), String::from("tenant_id"));
        let gateway = Gateway::new(
            String::from("default"),
            String::from("d"),
            String::from("main"),
            policies,
            Realtime::new(16),
        );
        let principal = Principal {
            id: String::from("ada"),
            tenant: String::from("tenant-a"),
            roles: [ryme_auth::Role::ReadWrite].into_iter().collect(),
        };
        let mut updates = gateway.realtime().query_subscribe("tenant-a", "d", "messages", 10);
        let manager = gateway.manager_clone();
        let mut txn = manager.begin();
        manager.put(
            &mut txn,
            RecordKey::new("tenant-a", "d", "messages", b"hidden"),
            br#"{"tenant_id":"tenant-b","body":"secret"}"#.to_vec(),
        );
        manager.commit(txn).unwrap();

        gateway
            .put(
                &principal,
                "messages",
                b"visible".to_vec(),
                br#"{"tenant_id":"tenant-a","body":"hello"}"#.to_vec(),
            )
            .await
            .unwrap();
        let update = updates.recv().await.unwrap();
        assert_eq!(update.rows.len(), 1);
        assert_eq!(update.rows[0].pk, b"visible");
    }

    #[tokio::test]
    async fn put_accepts_boundary_shapes() {
        let gateway = gateway();
        let principal = owner();
        assert!(gateway
            .put(&principal, "docs", vec![b'k'; MAX_KEY_BYTES], b"v".to_vec())
            .await
            .is_ok());
        assert!(gateway.put(&principal, "docs", b"k".to_vec(), b"v".to_vec()).await.is_ok());
    }
}
