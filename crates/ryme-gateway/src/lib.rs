use futures_util::future::join_all;
use ryme_auth::{PolicyEngine, Principal};
use ryme_error::{Result, RymeError};
use ryme_realtime::{NewChange, Operation, Realtime};
use ryme_storage::RecordKey;
use ryme_txn::{TxnBackend, TxnManager};
use std::future::Future;
use std::pin::Pin;
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
pub enum RemoteRead {
    Local,
    Value { value: Option<Vec<u8>>, expires_at: Option<u64> },
}

#[derive(Debug, Clone)]
pub struct RemoteScanPage {
    pub rows: Vec<(Vec<u8>, Vec<u8>)>,
    pub next: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub enum RemoteScan {
    Local,
    Page(RemoteScanPage),
}

type RemoteReadFn = Arc<
    dyn Fn(RecordKey) -> Pin<Box<dyn Future<Output = Result<RemoteRead>> + Send>> + Send + Sync,
>;
type RemoteReadManyFn = Arc<
    dyn Fn(Vec<RecordKey>) -> Pin<Box<dyn Future<Output = Result<Vec<RemoteRead>>> + Send>>
        + Send
        + Sync,
>;
type RemoteScanFn = Arc<
    dyn Fn(
            String,
            Option<Vec<u8>>,
            usize,
            u64,
        ) -> Pin<Box<dyn Future<Output = Result<RemoteScan>> + Send>>
        + Send
        + Sync,
>;

#[derive(Clone)]
pub struct RemoteReader {
    single: RemoteReadFn,
    batch: Option<RemoteReadManyFn>,
}

impl std::fmt::Debug for RemoteReader {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("RemoteReader(..)")
    }
}

impl RemoteReader {
    pub fn new<F, Fut>(reader: F) -> Self
    where
        F: Fn(RecordKey) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<RemoteRead>> + Send + 'static,
    {
        Self { single: Arc::new(move |key| Box::pin(reader(key))), batch: None }
    }

    pub fn with_batch_reader<F, Fut>(mut self, reader: F) -> Self
    where
        F: Fn(Vec<RecordKey>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Vec<RemoteRead>>> + Send + 'static,
    {
        self.batch = Some(Arc::new(move |keys| Box::pin(reader(keys))));
        self
    }

    pub async fn read(&self, key: RecordKey) -> Result<RemoteRead> {
        (self.single)(key).await
    }

    pub async fn read_many(&self, keys: Vec<RecordKey>) -> Result<Vec<RemoteRead>> {
        if let Some(batch) = self.batch.as_ref() {
            return batch(keys).await;
        }
        join_all(keys.into_iter().map(|key| self.read(key))).await.into_iter().collect()
    }
}

#[derive(Clone)]
pub struct RemoteScanner(RemoteScanFn);

impl std::fmt::Debug for RemoteScanner {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("RemoteScanner(..)")
    }
}

impl RemoteScanner {
    pub fn new<F, Fut>(scanner: F) -> Self
    where
        F: Fn(String, Option<Vec<u8>>, usize, u64) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<RemoteScan>> + Send + 'static,
    {
        Self(Arc::new(move |table, start_after, limit, read_ts| {
            Box::pin(scanner(table, start_after, limit, read_ts))
        }))
    }

    pub async fn scan(
        &self,
        table: String,
        start_after: Option<Vec<u8>>,
        limit: usize,
        read_ts: u64,
    ) -> Result<RemoteScan> {
        (self.0)(table, start_after, limit, read_ts).await
    }
}

#[derive(Debug, Clone)]
pub struct Gateway<B = TxnManager> {
    manager: B,
    realtime: Realtime,
    policies: Arc<PolicyEngine>,
    tenant: String,
    database: String,
    branch: String,
    read_ts: Option<u64>,
    read_only: bool,
    remote_reader: Option<RemoteReader>,
    remote_scanner: Option<RemoteScanner>,
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
            read_ts: None,
            read_only: false,
            remote_reader: None,
            remote_scanner: None,
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
            read_ts: None,
            read_only: false,
            remote_reader: None,
            remote_scanner: None,
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
            read_ts: None,
            read_only: false,
            remote_reader: None,
            remote_scanner: None,
        }
    }

    pub fn set_read_only(&mut self, read_only: bool) {
        self.read_only = read_only;
    }

    pub fn is_read_only(&self) -> bool {
        self.read_only
    }

    pub fn with_read_ts(mut self, read_ts: u64) -> Self {
        self.read_ts = Some(read_ts);
        self
    }

    pub fn with_branch(mut self, branch: String) -> Self {
        self.branch = branch;
        self
    }

    pub fn with_backend_manager<C>(self, manager: C) -> Gateway<C> {
        Gateway {
            manager,
            realtime: self.realtime,
            policies: self.policies,
            tenant: self.tenant,
            database: self.database,
            branch: self.branch,
            read_ts: None,
            read_only: self.read_only,
            remote_reader: self.remote_reader,
            remote_scanner: self.remote_scanner,
        }
    }

    pub fn with_remote_reader(mut self, reader: RemoteReader) -> Self {
        self.remote_reader = Some(reader);
        self
    }

    pub fn with_remote_scanner(mut self, scanner: RemoteScanner) -> Self {
        self.remote_scanner = Some(scanner);
        self
    }

    fn begin(&self) -> ryme_txn::Transaction {
        let mut txn = self.manager.begin();
        if let Some(read_ts) = self.read_ts {
            txn.restamp(read_ts);
        }
        txn
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
        let mut txn = self.begin();
        let Some(value) = self.manager.get(&mut txn, &key)? else {
            return Ok(None);
        };
        if self.policies.row_allowed(principal, table, &value)? {
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    pub async fn get_async(
        &self,
        principal: &Principal,
        table: &str,
        pk: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        let Some(reader) = self.remote_reader.as_ref() else {
            return self.get(principal, table, pk);
        };
        self.policies.predicate(principal, table)?;
        let key = RecordKey::new(&principal.tenant, &self.database, table, pk);
        match reader.read(key).await? {
            RemoteRead::Local => self.get(principal, table, pk),
            RemoteRead::Value { value, .. } => match value {
                Some(value) if self.policies.row_allowed(principal, table, &value)? => {
                    Ok(Some(value))
                }
                _ => Ok(None),
            },
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
        let mut txn = self.begin();
        let before = self.manager.get(&mut txn, &key)?;
        match expires_at {
            Some(ts) => self.manager.put_with_ttl(&mut txn, key, value.clone(), ts),
            None => self.manager.put(&mut txn, key, value.clone()),
        }
        let commit_ts = self.manager.commit(txn).await?;
        let op = if before.is_some() { Operation::Update } else { Operation::Insert };
        self.realtime.publish(NewChange {
            tenant: principal.tenant.clone(),
            database: self.database.clone(),
            branch: self.branch.clone(),
            table: table.to_string(),
            op,
            pk,
            before,
            after: Some(value),
            commit_ts,
            tx_id: commit_ts,
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
        let mut txn = self.begin();
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
            before: Some(current.clone()),
            after: Some(current),
            commit_ts,
            tx_id: commit_ts,
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

    pub async fn ttl_of_async(&self, principal: &Principal, table: &str, pk: &[u8]) -> Result<Ttl> {
        let Some(reader) = self.remote_reader.as_ref() else {
            return self.ttl_of(principal, table, pk);
        };
        self.policies.predicate(principal, table)?;
        let key = RecordKey::new(&principal.tenant, &self.database, table, pk);
        match reader.read(key).await? {
            RemoteRead::Local => self.ttl_of(principal, table, pk),
            RemoteRead::Value { value, expires_at } => {
                if value.is_none() {
                    return Ok(Ttl::Missing);
                }
                match expires_at.unwrap_or(0) {
                    0 => Ok(Ttl::Persistent),
                    expires_at if expires_at <= ryme_txn::now_unix() => Ok(Ttl::Missing),
                    expires_at => Ok(Ttl::Seconds(expires_at - ryme_txn::now_unix())),
                }
            }
        }
    }

    pub async fn delete(&self, principal: &Principal, table: &str, pk: Vec<u8>) -> Result<u64> {
        self.reject_if_read_only()?;
        self.policies.check_write(principal, table)?;
        let key = RecordKey::new(&principal.tenant, &self.database, table, &pk);
        let mut txn = self.begin();
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
            before: Some(current),
            after: None,
            commit_ts,
            tx_id: commit_ts,
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
        let mut txn = self.begin();
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

    pub async fn scan_async(
        &self,
        principal: &Principal,
        table: &str,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let Some(scanner) =
            self.remote_scanner.as_ref().filter(|_| self.branch.eq_ignore_ascii_case("main"))
        else {
            return self.scan(principal, table, limit);
        };
        self.policies.predicate(principal, table)?;
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut visible = Vec::with_capacity(limit);
        let mut start_after = None;
        loop {
            let page = match scanner
                .scan(table.to_string(), start_after.clone(), limit, self.read_ts.unwrap_or(0))
                .await?
            {
                RemoteScan::Local => return self.scan(principal, table, limit),
                RemoteScan::Page(page) => page,
            };
            for (pk, value) in page.rows {
                if !self.policies.has_table_policy(table)
                    || self.policies.row_allowed(principal, table, &value)?
                {
                    visible.push((pk, value));
                    if visible.len() == limit {
                        return Ok(visible);
                    }
                }
            }
            let Some(next) = page.next else { break };
            start_after = Some(next);
        }
        Ok(visible)
    }

    /// Read one ordered storage page and return the internal cursor needed for
    /// the next page. The cursor is never exposed to callers as a data value;
    /// it only lets higher-level APIs keep filtering past rows that did not
    /// match their predicate.
    pub fn scan_page(
        &self,
        principal: &Principal,
        table: &str,
        start_after: Option<&[u8]>,
        limit: usize,
    ) -> Result<(Vec<(Vec<u8>, Vec<u8>)>, Option<Vec<u8>>)> {
        self.policies.predicate(principal, table)?;
        if limit == 0 {
            return Ok((Vec::new(), None));
        }
        let mut txn = self.begin();
        let page = match start_after {
            Some(start_after) => self.manager.scan_after(
                &mut txn,
                &principal.tenant,
                &self.database,
                table,
                start_after,
                limit,
            )?,
            None => self.manager.scan(&mut txn, &principal.tenant, &self.database, table, limit)?,
        };
        let next = if page.len() == limit { page.last().map(|(pk, _)| pk.clone()) } else { None };
        if !self.policies.has_table_policy(table) {
            return Ok((page, next));
        }
        let visible = page
            .into_iter()
            .filter_map(|(pk, value)| match self.policies.row_allowed(principal, table, &value) {
                Ok(true) => Some(Ok((pk, value))),
                Ok(false) => None,
                Err(error) => Some(Err(error)),
            })
            .collect::<Result<Vec<_>>>()?;
        Ok((visible, next))
    }

    pub async fn scan_page_async(
        &self,
        principal: &Principal,
        table: &str,
        start_after: Option<&[u8]>,
        limit: usize,
    ) -> Result<(Vec<(Vec<u8>, Vec<u8>)>, Option<Vec<u8>>)> {
        let Some(scanner) =
            self.remote_scanner.as_ref().filter(|_| self.branch.eq_ignore_ascii_case("main"))
        else {
            return self.scan_page(principal, table, start_after, limit);
        };
        self.policies.predicate(principal, table)?;
        if limit == 0 {
            return Ok((Vec::new(), None));
        }
        let page = match scanner
            .scan(
                table.to_string(),
                start_after.map(ToOwned::to_owned),
                limit,
                self.read_ts.unwrap_or(0),
            )
            .await?
        {
            RemoteScan::Local => return self.scan_page(principal, table, start_after, limit),
            RemoteScan::Page(page) => page,
        };
        if !self.policies.has_table_policy(table) {
            return Ok((page.rows, page.next));
        }
        let visible = page
            .rows
            .into_iter()
            .filter_map(|(pk, value)| match self.policies.row_allowed(principal, table, &value) {
                Ok(true) => Some(Ok((pk, value))),
                Ok(false) => None,
                Err(error) => Some(Err(error)),
            })
            .collect::<Result<Vec<_>>>()?;
        Ok((visible, page.next))
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
        let Some(limit) = self.realtime.query_limit_branch(
            &principal.tenant,
            &self.database,
            &self.branch,
            table,
        ) else {
            return;
        };
        let rows = self.scan(principal, table, limit).unwrap_or_default();
        let _ = self.realtime.publish_query_branch(
            &principal.tenant,
            &self.database,
            &self.branch,
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
    async fn async_reads_use_remote_owner_and_keep_ttl_semantics() {
        let gateway = gateway().with_remote_reader(RemoteReader::new(|key| async move {
            assert_eq!(key.table, "docs");
            assert_eq!(key.pk, b"k".to_vec());
            Ok(RemoteRead::Value {
                value: Some(b"v".to_vec()),
                expires_at: Some(ryme_txn::now_unix() + 60),
            })
        }));
        let principal = owner();
        assert_eq!(gateway.get_async(&principal, "docs", b"k").await.unwrap(), Some(b"v".to_vec()));
        assert!(matches!(
            gateway.ttl_of_async(&principal, "docs", b"k").await.unwrap(),
            Ttl::Seconds(seconds) if seconds > 0 && seconds <= 60
        ));
    }

    #[tokio::test]
    async fn remote_reader_uses_batch_transport_when_available() {
        let reader = RemoteReader::new(|_| async {
            Err::<RemoteRead, _>(RymeError::Internal(String::from("single path")))
        })
        .with_batch_reader(|keys| async move {
            Ok(keys
                .into_iter()
                .map(|key| RemoteRead::Value { value: Some(key.pk), expires_at: Some(0) })
                .collect())
        });
        let values = reader
            .read_many(vec![
                RecordKey::new("t", "d", "docs", b"a"),
                RecordKey::new("t", "d", "docs", b"b"),
            ])
            .await
            .unwrap();
        assert!(matches!(
            &values[..],
            [
                RemoteRead::Value { value: Some(first), .. },
                RemoteRead::Value { value: Some(second), .. }
            ] if first == b"a" && second == b"b"
        ));
    }

    #[tokio::test]
    async fn async_scan_uses_remote_pages_and_cursors() {
        let gateway = gateway().with_remote_scanner(RemoteScanner::new(
            |table, start_after, limit, _read_ts| async move {
                assert_eq!(table, "docs");
                assert_eq!(limit, 3);
                let page = if start_after.is_none() {
                    RemoteScanPage {
                        rows: vec![
                            (b"a".to_vec(), b"one".to_vec()),
                            (b"b".to_vec(), b"two".to_vec()),
                        ],
                        next: Some(b"b".to_vec()),
                    }
                } else {
                    RemoteScanPage { rows: vec![(b"c".to_vec(), b"three".to_vec())], next: None }
                };
                Ok(RemoteScan::Page(page))
            },
        ));
        let rows = gateway.scan_async(&owner(), "docs", 3).await.unwrap();
        assert_eq!(
            rows,
            vec![
                (b"a".to_vec(), b"one".to_vec()),
                (b"b".to_vec(), b"two".to_vec()),
                (b"c".to_vec(), b"three".to_vec()),
            ]
        );
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
    async fn scan_page_advances_past_storage_pages() {
        let gateway = gateway();
        let principal = owner();
        for key in ["a", "b", "c"] {
            gateway
                .put(&principal, "docs", key.as_bytes().to_vec(), key.as_bytes().to_vec())
                .await
                .unwrap();
        }
        let (first, cursor) = gateway.scan_page(&principal, "docs", None, 2).unwrap();
        assert_eq!(first.len(), 2);
        let cursor = cursor.expect("full page has a cursor");
        let (second, next) = gateway.scan_page(&principal, "docs", Some(&cursor), 2).unwrap();
        assert_eq!(second.len(), 1);
        assert!(next.is_none());
        assert_eq!(second[0].0, b"c");
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
