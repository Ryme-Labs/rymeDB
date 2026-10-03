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
        let key = RecordKey::new(&self.tenant, &self.database, table, pk);
        let mut txn = self.manager.begin();
        self.manager.get(&mut txn, &key)
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
        self.policies.check_write(principal, table)?;
        let key = RecordKey::new(&self.tenant, &self.database, table, &pk);
        let mut txn = self.manager.begin();
        let existed = self.manager.get(&mut txn, &key)?.is_some();
        match expires_at {
            Some(ts) => self.manager.put_with_ttl(&mut txn, key, value.clone(), ts),
            None => self.manager.put(&mut txn, key, value.clone()),
        }
        let commit_ts = self.manager.commit(txn).await?;
        let op = if existed { Operation::Update } else { Operation::Insert };
        self.realtime.publish(NewChange {
            tenant: self.tenant.clone(),
            database: self.database.clone(),
            branch: self.branch.clone(),
            table: table.to_string(),
            op,
            pk,
            after: Some(value),
            commit_ts,
        })?;
        self.refresh_table(table, commit_ts);
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
        let key = RecordKey::new(&self.tenant, &self.database, table, &pk);
        let mut txn = self.manager.begin();
        let Some(current) = self.manager.get(&mut txn, &key)? else {
            return Ok(false);
        };
        match expires_at {
            Some(ts) => self.manager.put_with_ttl(&mut txn, key, current.clone(), ts),
            None => self.manager.put(&mut txn, key, current.clone()),
        }
        let commit_ts = self.manager.commit(txn).await?;
        self.realtime.publish(NewChange {
            tenant: self.tenant.clone(),
            database: self.database.clone(),
            branch: self.branch.clone(),
            table: table.to_string(),
            op: Operation::Update,
            pk,
            after: Some(current),
            commit_ts,
        })?;
        self.refresh_table(table, commit_ts);
        Ok(true)
    }

    pub fn ttl_of(&self, principal: &Principal, table: &str, pk: &[u8]) -> Result<Ttl> {
        self.policies.predicate(principal, table)?;
        let key = RecordKey::new(&self.tenant, &self.database, table, pk);
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
        let key = RecordKey::new(&self.tenant, &self.database, table, &pk);
        let mut txn = self.manager.begin();
        if self.manager.get(&mut txn, &key)?.is_none() {
            return Err(RymeError::NotFound(String::from("row")));
        }
        self.manager.delete(&mut txn, key);
        let commit_ts = self.manager.commit(txn).await?;
        self.realtime.publish(NewChange {
            tenant: self.tenant.clone(),
            database: self.database.clone(),
            branch: self.branch.clone(),
            table: table.to_string(),
            op: Operation::Delete,
            pk,
            after: None,
            commit_ts,
        })?;
        self.refresh_table(table, commit_ts);
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
        self.manager.scan(&mut txn, &self.tenant, &self.database, table, limit)
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

    fn refresh_table(&self, table: &str, commit_ts: u64) {
        let Some(limit) = self.realtime.query_limit(&self.tenant, &self.database, table) else {
            return;
        };
        let mut txn = self.manager.begin();
        let rows = self
            .manager
            .scan(&mut txn, &self.tenant, &self.database, table, limit)
            .unwrap_or_default();
        let _ = self.realtime.publish_query(
            &self.tenant,
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
