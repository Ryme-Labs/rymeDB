use futures_util::future::join_all;
use ryme_auth::{PolicyEngine, Principal};
use ryme_error::{Result, RymeError};
use ryme_realtime::{NewChange, Operation, Realtime};
use ryme_storage::RecordKey;
use ryme_txn::{TxnBackend, TxnManager};
use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

pub const MAX_KEY_BYTES: usize = 1024;
pub const MAX_VALUE_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_BATCH_ROWS: usize = 10_000;
pub const MAX_TABLE_BYTES: usize = 256;
pub const MAX_SCAN_ROWS: usize = 1_000;

fn validate_table(table: &str) -> Result<()> {
    if table.is_empty() || table.len() > MAX_TABLE_BYTES || table.contains('\0') {
        return Err(RymeError::InvalidArgument(String::from("table")));
    }
    Ok(())
}

fn validate_key(pk: &[u8]) -> Result<()> {
    if pk.is_empty() || pk.len() > MAX_KEY_BYTES {
        return Err(RymeError::InvalidArgument(String::from("key")));
    }
    Ok(())
}

fn validate_scan_limit(limit: usize) -> Result<()> {
    if limit > MAX_SCAN_ROWS {
        return Err(RymeError::Overload(String::from("scan limit")));
    }
    Ok(())
}

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
    dyn Fn(RecordKey, u64) -> Pin<Box<dyn Future<Output = Result<RemoteRead>> + Send>>
        + Send
        + Sync,
>;
type RemoteReadManyFn = Arc<
    dyn Fn(Vec<RecordKey>, u64) -> Pin<Box<dyn Future<Output = Result<Vec<RemoteRead>>> + Send>>
        + Send
        + Sync,
>;
type RemoteScanFn = Arc<
    dyn Fn(
            String,
            String,
            Option<Vec<u8>>,
            usize,
            u64,
        ) -> Pin<Box<dyn Future<Output = Result<RemoteScan>> + Send>>
        + Send
        + Sync,
>;
type ScanPageResult = (Vec<(Vec<u8>, Vec<u8>)>, Option<Vec<u8>>);

fn remote_value_if_live(value: Option<Vec<u8>>, expires_at: Option<u64>) -> Option<Vec<u8>> {
    if expires_at.is_some_and(|expires_at| expires_at != 0 && expires_at <= ryme_txn::now_unix()) {
        None
    } else {
        value
    }
}

#[derive(Clone)]
pub struct RemoteReader {
    single: RemoteReadFn,
    batch: Option<RemoteReadManyFn>,
    single_supports_read_ts: bool,
    batch_supports_read_ts: bool,
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
        Self {
            single: Arc::new(move |key, _| Box::pin(reader(key))),
            batch: None,
            single_supports_read_ts: false,
            batch_supports_read_ts: false,
        }
    }

    pub fn new_with_read_ts<F, Fut>(reader: F) -> Self
    where
        F: Fn(RecordKey, u64) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<RemoteRead>> + Send + 'static,
    {
        Self {
            single: Arc::new(move |key, read_ts| Box::pin(reader(key, read_ts))),
            batch: None,
            single_supports_read_ts: true,
            batch_supports_read_ts: false,
        }
    }

    pub fn with_batch_reader<F, Fut>(mut self, reader: F) -> Self
    where
        F: Fn(Vec<RecordKey>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Vec<RemoteRead>>> + Send + 'static,
    {
        self.batch = Some(Arc::new(move |keys, _| Box::pin(reader(keys))));
        self.batch_supports_read_ts = false;
        self
    }

    pub fn with_batch_reader_at<F, Fut>(mut self, reader: F) -> Self
    where
        F: Fn(Vec<RecordKey>, u64) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Vec<RemoteRead>>> + Send + 'static,
    {
        self.batch = Some(Arc::new(move |keys, read_ts| Box::pin(reader(keys, read_ts))));
        self.batch_supports_read_ts = true;
        self
    }

    pub async fn read(&self, key: RecordKey) -> Result<RemoteRead> {
        self.read_at(key, 0).await
    }

    pub async fn read_at(&self, key: RecordKey, read_ts: u64) -> Result<RemoteRead> {
        if read_ts != 0 && !self.single_supports_read_ts {
            return Err(RymeError::Unavailable(String::from(
                "remote reader does not support historical reads",
            )));
        }
        (self.single)(key, read_ts).await
    }

    pub async fn read_many(&self, keys: Vec<RecordKey>) -> Result<Vec<RemoteRead>> {
        self.read_many_at(keys, 0).await
    }

    pub async fn read_many_at(
        &self,
        keys: Vec<RecordKey>,
        read_ts: u64,
    ) -> Result<Vec<RemoteRead>> {
        if read_ts == 0 || self.batch_supports_read_ts {
            if let Some(batch) = self.batch.as_ref() {
                return batch(keys, read_ts).await;
            }
        }
        join_all(keys.into_iter().map(|key| self.read_at(key, read_ts))).await.into_iter().collect()
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
        F: Fn(String, String, Option<Vec<u8>>, usize, u64) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<RemoteScan>> + Send + 'static,
    {
        Self(Arc::new(move |tenant, table, start_after, limit, read_ts| {
            Box::pin(scanner(tenant, table, start_after, limit, read_ts))
        }))
    }

    pub async fn scan(
        &self,
        tenant: String,
        table: String,
        start_after: Option<Vec<u8>>,
        limit: usize,
        read_ts: u64,
    ) -> Result<RemoteScan> {
        (self.0)(tenant, table, start_after, limit, read_ts).await
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
            read_ts: self.read_ts,
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

    async fn read_for_write(
        &self,
        txn: &mut ryme_txn::Transaction,
        key: &RecordKey,
    ) -> Result<Option<Vec<u8>>> {
        let Some(reader) =
            self.remote_reader.as_ref().filter(|_| self.branch.eq_ignore_ascii_case("main"))
        else {
            return self.manager.get(txn, key);
        };
        match reader.read(key.clone()).await? {
            RemoteRead::Local => self.manager.get(txn, key),
            RemoteRead::Value { value, expires_at } => {
                let value = remote_value_if_live(value, expires_at);
                txn.record_read(key.clone(), value.is_some())?;
                Ok(value)
            }
        }
    }

    fn reject_hidden_existing(
        &self,
        principal: &Principal,
        table: &str,
        before: Option<&Vec<u8>>,
    ) -> Result<()> {
        if let Some(before) = before {
            if !self.policies.row_allowed(principal, table, before)? {
                return Err(RymeError::NotFound(String::from("row")));
            }
        }
        Ok(())
    }

    fn reject_if_read_only(&self) -> Result<()> {
        if self.read_only {
            return Err(RymeError::ReadOnly(String::from("read-only follower")));
        }
        Ok(())
    }

    pub fn get(&self, principal: &Principal, table: &str, pk: &[u8]) -> Result<Option<Vec<u8>>> {
        validate_table(table)?;
        validate_key(pk)?;
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
        validate_table(table)?;
        validate_key(pk)?;
        let Some(reader) =
            self.remote_reader.as_ref().filter(|_| self.branch.eq_ignore_ascii_case("main"))
        else {
            return self.get(principal, table, pk);
        };
        self.policies.predicate(principal, table)?;
        let key = RecordKey::new(&principal.tenant, &self.database, table, pk);
        match reader.read_at(key, self.read_ts.unwrap_or(0)).await? {
            RemoteRead::Local => self.get(principal, table, pk),
            RemoteRead::Value { value, expires_at } => {
                match remote_value_if_live(value, expires_at) {
                    Some(value) if self.policies.row_allowed(principal, table, &value)? => {
                        Ok(Some(value))
                    }
                    _ => Ok(None),
                }
            }
        }
    }

    pub async fn get_many_async(
        &self,
        principal: &Principal,
        table: &str,
        pks: &[Vec<u8>],
    ) -> Result<Vec<Option<Vec<u8>>>> {
        validate_table(table)?;
        if pks.len() > MAX_BATCH_ROWS {
            return Err(RymeError::Overload(String::from("batch read rows")));
        }
        for pk in pks {
            validate_key(pk)?;
        }
        self.policies.predicate(principal, table)?;
        if pks.is_empty() {
            return Ok(Vec::new());
        }

        let Some(reader) =
            self.remote_reader.as_ref().filter(|_| self.branch.eq_ignore_ascii_case("main"))
        else {
            return pks.iter().map(|pk| self.get(principal, table, pk)).collect();
        };

        let keys = pks
            .iter()
            .map(|pk| RecordKey::new(&principal.tenant, &self.database, table, pk))
            .collect();
        let remote_reads = reader.read_many_at(keys, self.read_ts.unwrap_or(0)).await?;
        if remote_reads.len() != pks.len() {
            return Err(RymeError::Corrupt(String::from("range read batch count")));
        }

        remote_reads
            .into_iter()
            .zip(pks)
            .map(|(remote, pk)| match remote {
                RemoteRead::Local => self.get(principal, table, pk),
                RemoteRead::Value { value, expires_at } => {
                    match remote_value_if_live(value, expires_at) {
                        Some(value) if self.policies.row_allowed(principal, table, &value)? => {
                            Ok(Some(value))
                        }
                        _ => Ok(None),
                    }
                }
            })
            .collect()
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

    /// Apply a group of puts under one MVCC commit. This is the transaction
    /// boundary used by bulk REST writes and keeps partial batches from
    /// becoming visible when a later row fails validation.
    pub async fn put_many(
        &self,
        principal: &Principal,
        table: &str,
        rows: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<u64> {
        self.put_many_inner(principal, table, rows, false, |_, _, value| Ok(value.to_vec()))
            .await
            .map(|(commit_ts, _)| commit_ts)
    }

    /// Atomically transforms and writes a batch using each row's current value
    /// from the same transaction that commits the write. The callback can
    /// enforce application authorization and safely apply partial updates.
    pub async fn put_many_transform<F>(
        &self,
        principal: &Principal,
        table: &str,
        rows: Vec<(Vec<u8>, Vec<u8>)>,
        transform: F,
    ) -> Result<(u64, Vec<(Vec<u8>, Vec<u8>)>)>
    where
        F: FnMut(&[u8], Option<&[u8]>, &[u8]) -> Result<Vec<u8>>,
    {
        self.put_many_inner(principal, table, rows, true, transform).await
    }

    async fn put_many_inner<F>(
        &self,
        principal: &Principal,
        table: &str,
        rows: Vec<(Vec<u8>, Vec<u8>)>,
        include_written_rows: bool,
        mut transform: F,
    ) -> Result<(u64, Vec<(Vec<u8>, Vec<u8>)>)>
    where
        F: FnMut(&[u8], Option<&[u8]>, &[u8]) -> Result<Vec<u8>>,
    {
        self.reject_if_read_only()?;
        validate_table(table)?;
        if rows.is_empty() {
            return Err(RymeError::InvalidArgument(String::from("rows")));
        }
        if rows.len() > MAX_BATCH_ROWS {
            return Err(RymeError::Overload(String::from("rows")));
        }
        self.policies.check_write(principal, table)?;
        let mut txn = self.begin();
        let mut seen = HashSet::with_capacity(rows.len());
        let mut changes = Vec::with_capacity(rows.len());
        for (pk, proposed) in rows {
            if pk.is_empty() || pk.len() > MAX_KEY_BYTES {
                return Err(RymeError::InvalidArgument(String::from("key")));
            }
            if proposed.len() > MAX_VALUE_BYTES {
                return Err(RymeError::Overload(String::from("value")));
            }
            if !seen.insert(pk.clone()) {
                return Err(RymeError::Conflict(String::from("duplicate key")));
            }
            let key = RecordKey::new(&principal.tenant, &self.database, table, &pk);
            let before = self.read_for_write(&mut txn, &key).await?;
            self.reject_hidden_existing(principal, table, before.as_ref())?;
            let value = transform(&pk, before.as_deref(), &proposed)?;
            if value.len() > MAX_VALUE_BYTES {
                return Err(RymeError::Overload(String::from("value")));
            }
            self.policies.check_write_row(principal, table, &value)?;
            let op = if before.is_some() { Operation::Update } else { Operation::Insert };
            self.manager.try_put(&mut txn, key, value.clone())?;
            changes.push((pk, before, value, op));
        }
        let commit_ts = self.manager.commit(txn).await?;
        let written = if include_written_rows {
            changes.iter().map(|(pk, _, after, _)| (pk.clone(), after.clone())).collect()
        } else {
            Vec::new()
        };
        for (pk, before, after, op) in changes {
            self.publish_change(NewChange {
                tenant: principal.tenant.clone(),
                database: self.database.clone(),
                branch: self.branch.clone(),
                table: table.to_string(),
                op,
                pk,
                before,
                after: Some(after),
                commit_ts,
                tx_id: commit_ts,
            });
        }
        self.refresh_table(principal, table, commit_ts);
        Ok((commit_ts, written))
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
        validate_table(table)?;
        validate_key(&pk)?;
        if value.len() > MAX_VALUE_BYTES {
            return Err(RymeError::Overload(String::from("value")));
        }
        self.policies.check_write_row(principal, table, &value)?;
        let key = RecordKey::new(&principal.tenant, &self.database, table, &pk);
        let mut txn = self.begin();
        let before = self.read_for_write(&mut txn, &key).await?;
        self.reject_hidden_existing(principal, table, before.as_ref())?;
        match expires_at {
            Some(ts) => self.manager.try_put_with_ttl(&mut txn, key, value.clone(), ts)?,
            None => self.manager.try_put(&mut txn, key, value.clone())?,
        }
        let commit_ts = self.manager.commit(txn).await?;
        let op = if before.is_some() { Operation::Update } else { Operation::Insert };
        self.publish_change(NewChange {
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
        });
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
        validate_table(table)?;
        validate_key(&pk)?;
        self.policies.check_write(principal, table)?;
        let key = RecordKey::new(&principal.tenant, &self.database, table, &pk);
        let mut txn = self.begin();
        let Some(current) = self.read_for_write(&mut txn, &key).await? else {
            return Ok(false);
        };
        if !self.policies.row_allowed(principal, table, &current)? {
            return Ok(false);
        }
        match expires_at {
            Some(ts) => self.manager.try_put_with_ttl(&mut txn, key, current.clone(), ts)?,
            None => self.manager.try_put(&mut txn, key, current.clone())?,
        }
        let commit_ts = self.manager.commit(txn).await?;
        self.publish_change(NewChange {
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
        });
        self.refresh_table(principal, table, commit_ts);
        Ok(true)
    }

    pub fn ttl_of(&self, principal: &Principal, table: &str, pk: &[u8]) -> Result<Ttl> {
        validate_table(table)?;
        validate_key(pk)?;
        self.policies.predicate(principal, table)?;
        let key = RecordKey::new(&principal.tenant, &self.database, table, pk);
        let mut txn = self.begin();
        let Some(value) = self.manager.get(&mut txn, &key)? else {
            return Ok(Ttl::Missing);
        };
        if !self.policies.row_allowed(principal, table, &value)? {
            return Ok(Ttl::Missing);
        }
        match self.manager.expires_at_at(&key, txn.read_ts)? {
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
        validate_table(table)?;
        validate_key(pk)?;
        let Some(reader) =
            self.remote_reader.as_ref().filter(|_| self.branch.eq_ignore_ascii_case("main"))
        else {
            return self.ttl_of(principal, table, pk);
        };
        self.policies.predicate(principal, table)?;
        let key = RecordKey::new(&principal.tenant, &self.database, table, pk);
        match reader.read(key).await? {
            RemoteRead::Local => self.ttl_of(principal, table, pk),
            RemoteRead::Value { value, expires_at } => {
                let Some(value) = value else {
                    return Ok(Ttl::Missing);
                };
                if !self.policies.row_allowed(principal, table, &value)? {
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
        self.delete_many(principal, table, vec![pk]).await
    }

    /// Delete a group of rows under one MVCC commit. Missing or unauthorized
    /// rows abort the complete batch instead of leaving a partial delete.
    pub async fn delete_many(
        &self,
        principal: &Principal,
        table: &str,
        pks: Vec<Vec<u8>>,
    ) -> Result<u64> {
        self.delete_many_transform(principal, table, pks, |_, _| Ok(())).await
    }

    /// Atomically validates the current rows and deletes them under one MVCC
    /// commit. The callback runs after each transactional read and before its delete.
    pub async fn delete_many_transform<F>(
        &self,
        principal: &Principal,
        table: &str,
        pks: Vec<Vec<u8>>,
        mut validate: F,
    ) -> Result<u64>
    where
        F: FnMut(&[u8], &[u8]) -> Result<()>,
    {
        self.reject_if_read_only()?;
        validate_table(table)?;
        self.policies.check_write(principal, table)?;
        if pks.is_empty() {
            return Err(RymeError::InvalidArgument(String::from("keys")));
        }
        if pks.len() > MAX_BATCH_ROWS {
            return Err(RymeError::Overload(String::from("keys")));
        }
        let mut txn = self.begin();
        let mut seen = HashSet::with_capacity(pks.len());
        let mut changes = Vec::with_capacity(pks.len());
        for pk in pks {
            validate_key(&pk)?;
            if !seen.insert(pk.clone()) {
                return Err(RymeError::Conflict(String::from("duplicate key")));
            }
            let key = RecordKey::new(&principal.tenant, &self.database, table, &pk);
            let Some(current) = self.read_for_write(&mut txn, &key).await? else {
                return Err(RymeError::NotFound(String::from("row")));
            };
            if !self.policies.row_allowed(principal, table, &current)? {
                return Err(RymeError::NotFound(String::from("row")));
            }
            validate(&pk, &current)?;
            self.manager.try_delete(&mut txn, key)?;
            changes.push((pk, current));
        }
        let commit_ts = self.manager.commit(txn).await?;
        for (pk, current) in changes {
            self.publish_change(NewChange {
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
            });
        }
        self.refresh_table(principal, table, commit_ts);
        Ok(commit_ts)
    }

    pub fn masked(&self, table: &str, value: Vec<u8>) -> Vec<u8> {
        self.policies.masked_value(table, &value)
    }

    pub fn set_mask(&self, table: String, fields: Vec<String>) -> Result<()> {
        self.policies.mask_fields(table, fields)
    }

    pub fn scan(
        &self,
        principal: &Principal,
        table: &str,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        validate_table(table)?;
        validate_scan_limit(limit)?;
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
        validate_table(table)?;
        validate_scan_limit(limit)?;
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
                .scan(
                    principal.tenant.clone(),
                    table.to_string(),
                    start_after.clone(),
                    limit,
                    self.read_ts.unwrap_or(0),
                )
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
    ) -> Result<ScanPageResult> {
        validate_table(table)?;
        validate_scan_limit(limit)?;
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
        validate_table(table)?;
        validate_scan_limit(limit)?;
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
                principal.tenant.clone(),
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

    /// Realtime is a bounded derived stream. Storage has already committed
    /// before this is called, so a stream admission failure must not turn a
    /// successful write into an error that callers may retry.
    fn publish_change(&self, change: NewChange) {
        let _ = self.realtime.publish(change);
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
        assert!(gateway
            .put(&principal, &"t".repeat(MAX_TABLE_BYTES + 1), b"k".to_vec(), b"v".to_vec())
            .await
            .is_err());
        assert!(gateway.put(&principal, "bad\0table", b"k".to_vec(), b"v".to_vec()).await.is_err());
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
    async fn reads_and_scans_reject_oversized_inputs_at_the_gateway_boundary() {
        let gateway = gateway();
        let principal = owner();
        let oversized_key = vec![b'k'; MAX_KEY_BYTES + 1];

        assert!(gateway.get(&principal, "docs", &oversized_key).is_err());
        assert!(gateway.ttl_of(&principal, "docs", &oversized_key).is_err());
        assert!(gateway.expire(&principal, "docs", oversized_key, None).await.is_err());
        assert!(gateway.scan(&principal, "docs", MAX_SCAN_ROWS + 1).is_err());
        assert!(gateway.scan_page(&principal, "docs", None, MAX_SCAN_ROWS + 1).is_err());
    }

    #[tokio::test]
    async fn batch_mutations_commit_atomically_and_share_commit_timestamp() {
        let gateway = gateway();
        let principal = owner();
        let too_large = vec![b'x'; MAX_VALUE_BYTES + 1];
        assert!(gateway
            .put_many(
                &principal,
                "docs",
                vec![(b"first".to_vec(), b"ok".to_vec()), (b"second".to_vec(), too_large)],
            )
            .await
            .is_err());
        assert!(gateway.get(&principal, "docs", b"first").unwrap().is_none());

        gateway
            .put_many(
                &principal,
                "docs",
                vec![(b"first".to_vec(), b"one".to_vec()), (b"second".to_vec(), b"two".to_vec())],
            )
            .await
            .unwrap();
        assert!(gateway
            .delete_many(&principal, "docs", vec![b"first".to_vec(), b"missing".to_vec()])
            .await
            .is_err());
        assert!(gateway.get(&principal, "docs", b"first").unwrap().is_some());
        assert!(gateway
            .delete_many_transform(
                &principal,
                "docs",
                vec![b"first".to_vec(), b"second".to_vec()],
                |key, _| {
                    if key == b"second" {
                        Err(RymeError::Forbidden)
                    } else {
                        Ok(())
                    }
                },
            )
            .await
            .is_err());
        assert!(gateway.get(&principal, "docs", b"first").unwrap().is_some());
        assert!(gateway.get(&principal, "docs", b"second").unwrap().is_some());

        let mut changes = gateway.realtime().subscribe("t", "d", "docs");
        let commit = gateway
            .put_many(
                &principal,
                "docs",
                vec![
                    (b"third".to_vec(), b"three".to_vec()),
                    (b"fourth".to_vec(), b"four".to_vec()),
                ],
            )
            .await
            .unwrap();
        let first = changes.recv().await.unwrap();
        let second = changes.recv().await.unwrap();
        assert_eq!(first.commit_ts, commit);
        assert_eq!(second.commit_ts, commit);
    }

    #[tokio::test]
    async fn batch_mutations_reject_oversized_input_before_allocating_transaction_state() {
        let gateway = gateway();
        let principal = owner();
        let rows = (0..=MAX_BATCH_ROWS)
            .map(|index| (index.to_string().into_bytes(), b"value".to_vec()))
            .collect();
        assert!(matches!(
            gateway.put_many(&principal, "docs", rows).await,
            Err(RymeError::Overload(_))
        ));

        let keys = (0..=MAX_BATCH_ROWS).map(|index| index.to_string().into_bytes()).collect();
        assert!(matches!(
            gateway.delete_many(&principal, "docs", keys).await,
            Err(RymeError::Overload(_))
        ));
    }

    #[tokio::test]
    async fn async_reads_use_remote_owner_and_keep_ttl_semantics() {
        let gateway = gateway().with_remote_reader(RemoteReader::new_with_read_ts(
            |key, _read_ts| async move {
                assert_eq!(key.table, "docs");
                assert_eq!(key.pk, b"k".to_vec());
                Ok(RemoteRead::Value {
                    value: Some(b"v".to_vec()),
                    expires_at: Some(ryme_txn::now_unix() + 60),
                })
            },
        ));
        let principal = owner();
        assert_eq!(gateway.get_async(&principal, "docs", b"k").await.unwrap(), Some(b"v".to_vec()));
        assert!(matches!(
            gateway.ttl_of_async(&principal, "docs", b"k").await.unwrap(),
            Ttl::Seconds(seconds) if seconds > 0 && seconds <= 60
        ));
    }

    #[tokio::test]
    async fn historical_read_timestamp_reaches_single_and_batch_remote_reads() {
        let reader = RemoteReader::new_with_read_ts(|_, read_ts| async move {
            assert_eq!(read_ts, 42);
            Ok(RemoteRead::Value { value: Some(b"snapshot".to_vec()), expires_at: Some(0) })
        })
        .with_batch_reader_at(|keys, read_ts| async move {
            assert_eq!(read_ts, 42);
            Ok(keys
                .into_iter()
                .map(|_| RemoteRead::Value {
                    value: Some(b"snapshot".to_vec()),
                    expires_at: Some(0),
                })
                .collect())
        });
        let gateway = gateway().with_read_ts(42).with_remote_reader(reader);
        let principal = owner();

        assert_eq!(
            gateway.get_async(&principal, "docs", b"single").await.unwrap(),
            Some(b"snapshot".to_vec())
        );
        assert_eq!(
            gateway.get_many_async(&principal, "docs", &[b"batch".to_vec()]).await.unwrap(),
            vec![Some(b"snapshot".to_vec())]
        );
    }

    #[tokio::test]
    async fn legacy_remote_reader_fails_closed_for_historical_reads() {
        let reader = RemoteReader::new(|_| async {
            Ok(RemoteRead::Value { value: Some(b"latest".to_vec()), expires_at: None })
        });

        assert!(matches!(
            reader.read_at(RecordKey::new("t", "d", "docs", b"row"), 42).await,
            Err(RymeError::Unavailable(message)) if message == "remote reader does not support historical reads"
        ));
    }

    #[tokio::test]
    async fn async_batch_reads_use_one_remote_batch_and_apply_row_policies() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let batch_calls = Arc::new(AtomicUsize::new(0));
        let observed_calls = batch_calls.clone();
        let reader = RemoteReader::new_with_read_ts(|_, _read_ts| async {
            Err(RymeError::Internal(String::from("single read path used")))
        })
        .with_batch_reader_at(move |keys, _read_ts| {
            observed_calls.fetch_add(1, Ordering::Relaxed);
            async move {
                Ok(keys
                    .into_iter()
                    .map(|key| {
                        let value = match key.pk.as_slice() {
                            b"visible" => Some(br#"{"tenant_id":"t"}"#.to_vec()),
                            b"hidden" => Some(br#"{"tenant_id":"other"}"#.to_vec()),
                            _ => None,
                        };
                        RemoteRead::Value { value, expires_at: Some(ryme_txn::now_unix() + 60) }
                    })
                    .collect())
            }
        });
        let mut policies = PolicyEngine::new();
        policies.allow_table(String::from("messages"), String::from("tenant_id"));
        let gateway = Gateway::new(
            String::from("t"),
            String::from("d"),
            String::from("main"),
            policies,
            Realtime::new(16),
        )
        .with_remote_reader(reader);

        let values = gateway
            .get_many_async(
                &owner(),
                "messages",
                &[b"visible".to_vec(), b"hidden".to_vec(), b"missing".to_vec()],
            )
            .await
            .unwrap();
        assert_eq!(batch_calls.load(Ordering::Relaxed), 1);
        assert_eq!(values[0], Some(br#"{"tenant_id":"t"}"#.to_vec()));
        assert_eq!(values[1], None);
        assert_eq!(values[2], None);
    }

    #[tokio::test]
    async fn branch_gateways_do_not_use_main_remote_reader() {
        let gateway = gateway();
        let principal = owner();
        gateway.put(&principal, "docs", b"k".to_vec(), b"branch-value".to_vec()).await.unwrap();
        let branch = gateway.clone().with_branch(String::from("feature")).with_remote_reader(
            RemoteReader::new_with_read_ts(|_, _read_ts| async {
                Ok(RemoteRead::Value {
                    value: Some(b"main-value".to_vec()),
                    expires_at: Some(ryme_txn::now_unix() + 600),
                })
            }),
        );

        assert_eq!(
            branch.get_async(&principal, "docs", b"k").await.unwrap(),
            Some(b"branch-value".to_vec())
        );
        assert_eq!(
            branch.get_many_async(&principal, "docs", &[b"k".to_vec()]).await.unwrap(),
            vec![Some(b"branch-value".to_vec())]
        );
        assert_eq!(branch.ttl_of_async(&principal, "docs", b"k").await.unwrap(), Ttl::Persistent);
        let mut changes = branch.realtime().subscribe_branch("t", "d", "feature", "docs");
        branch.put(&principal, "docs", b"k".to_vec(), b"updated".to_vec()).await.unwrap();
        let change = changes.recv().await.unwrap();
        assert_eq!(change.before, Some(b"branch-value".to_vec()));
    }

    #[tokio::test]
    async fn remote_write_reads_participate_in_occ() {
        let gateway =
            gateway().with_remote_reader(RemoteReader::new_with_read_ts(|_, _read_ts| async {
                Ok(RemoteRead::Value { value: Some(b"remote".to_vec()), expires_at: Some(0) })
            }));
        let key = RecordKey::new("t", "d", "docs", b"k");
        let mut txn = gateway.begin();
        assert_eq!(gateway.read_for_write(&mut txn, &key).await.unwrap(), Some(b"remote".to_vec()));
        assert!(txn.read_keys().contains(&key));
    }

    #[tokio::test]
    async fn backend_wrapping_preserves_historical_reads() {
        let gateway = gateway();
        let principal = owner();
        let first_commit =
            gateway.put(&principal, "docs", b"k".to_vec(), b"old".to_vec()).await.unwrap();
        gateway.put(&principal, "docs", b"k".to_vec(), b"new".to_vec()).await.unwrap();

        let historical = gateway
            .clone()
            .with_read_ts(first_commit)
            .with_backend_manager(gateway.manager_clone());
        assert_eq!(historical.get(&principal, "docs", b"k").unwrap(), Some(b"old".to_vec()));
    }

    #[tokio::test]
    async fn remote_reader_uses_batch_transport_when_available() {
        let reader = RemoteReader::new_with_read_ts(|_, _read_ts| async {
            Err::<RemoteRead, _>(RymeError::Internal(String::from("single path")))
        })
        .with_batch_reader_at(|keys, _read_ts| async move {
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
            |tenant, table, start_after, limit, _read_ts| async move {
                assert_eq!(tenant, "t");
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
    async fn async_scan_page_uses_remote_scanner_and_cursor() {
        let gateway = gateway().with_remote_scanner(RemoteScanner::new(
            |tenant, table, start_after, limit, _read_ts| async move {
                assert_eq!(tenant, "t");
                assert_eq!(table, "docs");
                assert_eq!(limit, 2);
                if start_after.is_none() {
                    Ok(RemoteScanPage {
                        rows: vec![
                            (b"a".to_vec(), b"one".to_vec()),
                            (b"b".to_vec(), b"two".to_vec()),
                        ],
                        next: Some(b"b".to_vec()),
                    })
                } else {
                    assert_eq!(start_after.as_deref(), Some(b"b".as_slice()));
                    Ok(RemoteScanPage {
                        rows: vec![(b"c".to_vec(), b"three".to_vec())],
                        next: None,
                    })
                }
                .map(RemoteScan::Page)
            },
        ));

        let (first, cursor) = gateway.scan_page_async(&owner(), "docs", None, 2).await.unwrap();
        assert_eq!(first.len(), 2);
        assert_eq!(cursor, Some(b"b".to_vec()));

        let (second, next) =
            gateway.scan_page_async(&owner(), "docs", cursor.as_deref(), 2).await.unwrap();
        assert_eq!(second, vec![(b"c".to_vec(), b"three".to_vec())]);
        assert!(next.is_none());
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
        manager.put_with_ttl(
            &mut txn,
            RecordKey::new("tenant-a", "d", "messages", b"hidden"),
            br#"{"tenant_id":"tenant-b","body":"secret"}"#.to_vec(),
            ryme_txn::now_unix() + 60,
        );
        manager.commit(txn).unwrap();
        assert!(gateway.get(&principal, "messages", b"hidden").unwrap().is_none());
        assert_eq!(gateway.ttl_of(&principal, "messages", b"hidden").unwrap(), Ttl::Missing);
        assert!(matches!(
            gateway
                .put_with_ttl(
                    &principal,
                    "messages",
                    b"hidden".to_vec(),
                    br#"{"tenant_id":"tenant-a","body":"overwrite"}"#.to_vec(),
                    Some(ryme_txn::now_unix() + 60),
                )
                .await,
            Err(RymeError::NotFound(_))
        ));
        assert!(gateway
            .put_many(
                &principal,
                "messages",
                vec![
                    (b"batch-new".to_vec(), br#"{"tenant_id":"tenant-a"}"#.to_vec()),
                    (b"hidden".to_vec(), br#"{"tenant_id":"tenant-a"}"#.to_vec()),
                ],
            )
            .await
            .is_err());
        assert!(gateway.get(&principal, "messages", b"batch-new").unwrap().is_none());
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
    async fn remote_ttl_hides_rows_rejected_by_row_policy() {
        let mut policies = PolicyEngine::new();
        policies.allow_table(String::from("messages"), String::from("tenant_id"));
        let gateway = Gateway::new(
            String::from("tenant-a"),
            String::from("d"),
            String::from("main"),
            policies,
            Realtime::new(16),
        )
        .with_remote_reader(RemoteReader::new_with_read_ts(|_, _read_ts| async {
            Ok(RemoteRead::Value {
                value: Some(br#"{"tenant_id":"tenant-b"}"#.to_vec()),
                expires_at: Some(ryme_txn::now_unix() + 60),
            })
        }));
        let principal = Principal {
            id: String::from("ada"),
            tenant: String::from("tenant-a"),
            roles: [ryme_auth::Role::ReadWrite].into_iter().collect(),
        };
        assert_eq!(
            gateway.ttl_of_async(&principal, "messages", b"hidden").await.unwrap(),
            Ttl::Missing
        );
    }

    #[tokio::test]
    async fn async_remote_reads_hide_expired_values_before_reads_and_writes() {
        let gateway =
            gateway().with_remote_reader(RemoteReader::new_with_read_ts(|_, _read_ts| async {
                Ok(RemoteRead::Value {
                    value: Some(b"expired".to_vec()),
                    expires_at: Some(ryme_txn::now_unix().saturating_sub(1)),
                })
            }));
        let principal = owner();

        assert_eq!(gateway.get_async(&principal, "docs", b"k").await.unwrap(), None);
        let mut changes = gateway.realtime().subscribe("t", "d", "docs");
        gateway.put(&principal, "docs", b"k".to_vec(), b"replacement".to_vec()).await.unwrap();
        assert_eq!(changes.recv().await.unwrap().op, Operation::Insert);
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
