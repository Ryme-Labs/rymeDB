use ryme_error::{Result, RymeError};
use ryme_metering::{MeterRegistry, Metric, UsageEvent};
use ryme_observe::{
    query_fingerprint, Histogram, LatencyWindow, SlowEntry, SlowLog, TraceCollector, TraceSpan,
    SLOW_THRESHOLD_MICROS,
};
use ryme_qos::QosRegistry;
use ryme_router::RangeLoadHook;
use ryme_sql::{
    bind, parse, Executor, Field, ForeignKeyAction, InsertValue, QueryResult, ReturningField,
    RoleDefinition, Statement, TransactionChange, SQL_NULL_SENTINEL,
};
use ryme_txn::{Isolation, Transaction, TransactionCheckpoint, TxnBackend, TxnManager};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, LazyLock, Mutex, Weak};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

pub type PgAuthenticator = Arc<dyn Fn(&str, &str) -> Result<String> + Send + Sync>;

pub struct PgGateway<B = TxnManager> {
    executor: Arc<Executor<B>>,
    tls: Option<ryme_wire_native::TlsAcceptor>,
    authenticator: Option<PgAuthenticator>,
    tenant: String,
    database: String,
    qos: Option<Arc<Mutex<QosRegistry>>>,
    metering: Option<Arc<Mutex<MeterRegistry>>>,
    latency: Option<LatencyWindow>,
    histogram: Option<Histogram>,
    slow_log: Option<SlowLog>,
    traces: Option<Arc<Mutex<TraceCollector>>>,
    range_hook: RangeLoadHook,
}

#[derive(Debug, Clone)]
struct Portal {
    statement: String,
    params: Vec<String>,
    cached: Option<(String, Statement)>,
}

enum CopyInState {
    Receiving { table: String, columns: Vec<String>, data: Vec<u8> },
    Failed { message: String },
}

enum DecodedCopy {
    KeyValue(Vec<(Vec<u8>, Vec<u8>)>),
    Columns { columns: Vec<String>, rows: Vec<Vec<InsertValue>> },
}

struct SessionTransaction {
    txn: Transaction,
    changes: Vec<TransactionChange>,
    failed: bool,
    savepoints: Vec<Savepoint>,
}

fn note_range_changes(
    hook: &RangeLoadHook,
    table: &str,
    changes: &[TransactionChange],
    count: u64,
) {
    if changes.is_empty() {
        hook.note(table.as_bytes(), count.max(1));
    } else {
        for change in changes {
            hook.note(&change.pk, 1);
        }
    }
}

fn statement_range_keys(statement: &Statement) -> Option<Vec<Vec<u8>>> {
    match statement {
        Statement::Insert { pk, .. }
        | Statement::Upsert { pk, .. }
        | Statement::InsertIgnore { pk, .. }
        | Statement::InsertConflict { pk, .. }
        | Statement::Update { pk, .. }
        | Statement::UpdateRow { pk, .. }
        | Statement::Delete { pk, .. } => Some(vec![pk.clone()]),
        Statement::CopyFrom { rows, .. } => Some(rows.iter().map(|(pk, _)| pk.clone()).collect()),
        Statement::Returning { statement, .. } => statement_range_keys(statement),
        _ => None,
    }
}

fn note_statement_range(hook: &RangeLoadHook, table: &str, keys: Option<&[Vec<u8>]>, count: u64) {
    if let Some(keys) = keys {
        for key in keys {
            hook.note(key, 1);
        }
    } else {
        hook.note(table.as_bytes(), count.max(1));
    }
}

struct Savepoint {
    name: String,
    checkpoint: TransactionCheckpoint,
    changes_len: usize,
}

enum TransactionControl {
    Begin(Isolation),
    Commit,
    Rollback,
    Savepoint(String),
    ReleaseSavepoint(String),
    RollbackToSavepoint(String),
}

static NEXT_BACKEND_PID: AtomicU32 = AtomicU32::new(1);
static BACKEND_CANCELLATIONS: LazyLock<Mutex<HashMap<(u32, u32), Weak<AtomicBool>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn register_backend() -> (u32, u32, Arc<AtomicBool>) {
    let pid = NEXT_BACKEND_PID.fetch_add(1, Ordering::Relaxed).max(1);
    let secret = pid.rotate_left(13) ^ 0x9e37_79b9;
    let cancellation = Arc::new(AtomicBool::new(false));
    if let Ok(mut backends) = BACKEND_CANCELLATIONS.lock() {
        backends.retain(|_, signal| signal.strong_count() > 0);
        backends.insert((pid, secret), Arc::downgrade(&cancellation));
    }
    (pid, secret, cancellation)
}

fn unregister_backend(pid: u32, secret: u32) {
    if let Ok(mut backends) = BACKEND_CANCELLATIONS.lock() {
        backends.remove(&(pid, secret));
    }
}

fn cancel_backend(pid: u32, secret: u32) {
    let signal = BACKEND_CANCELLATIONS
        .lock()
        .ok()
        .and_then(|backends| backends.get(&(pid, secret)).and_then(Weak::upgrade));
    if let Some(signal) = signal {
        signal.store(true, Ordering::Release);
    }
}

#[derive(Debug, Clone)]
struct ConnLimits {
    tenant: String,
    database: String,
    qos: Option<Arc<Mutex<QosRegistry>>>,
    metering: Option<Arc<Mutex<MeterRegistry>>>,
    latency: Option<LatencyWindow>,
    histogram: Option<Histogram>,
    slow_log: Option<SlowLog>,
    traces: Option<Arc<Mutex<TraceCollector>>>,
    range_hook: RangeLoadHook,
}

impl ConnLimits {
    fn with_tenant(mut self, tenant: String) -> Self {
        self.tenant = tenant;
        self
    }
}

#[derive(Debug, Clone)]
struct StartupParams {
    user: String,
}

fn qos_now_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

fn admit_statement(limits: &ConnLimits, statement: &Statement, bytes: u64) -> Option<Vec<u8>> {
    let qos = limits.qos.as_ref()?;
    let now = qos_now_nanos();
    let admitted = match qos.lock() {
        Ok(mut registry) if statement.is_write() => {
            registry.admit_write(&limits.tenant, bytes, now)
        }
        Ok(mut registry) => registry.admit_read(&limits.tenant, now),
        Err(_) => Err(RymeError::Internal(String::from("qos lock"))),
    };
    admitted.err().map(|e| encode_error_code("53400", e.to_string()))
}

fn admit_response(limits: &ConnLimits, bytes: u64) -> Option<Vec<u8>> {
    let qos = limits.qos.as_ref()?;
    let now = qos_now_nanos();
    let denied = match qos.lock() {
        Ok(mut registry) => registry.admit_egress(&limits.tenant, bytes, now).err(),
        Err(_) => Some(RymeError::Internal(String::from("qos lock"))),
    };
    denied.map(|e| encode_error_code("53400", e.to_string()))
}

fn observe_statement(limits: &ConnLimits, write: bool) {
    let Some(metering) = limits.metering.as_ref() else { return };
    let metric = if write { Metric::WriteUnit } else { Metric::ReadUnit };
    let event =
        UsageEvent::new(limits.tenant.clone(), limits.database.clone(), metric, 1, String::new());
    if let Ok(mut registry) = metering.lock() {
        registry.ingest(event);
    }
}

fn record_timing(limits: &ConnLimits, fingerprint: Option<String>, table: String, micros: u64) {
    if let Some(latency) = limits.latency.as_ref() {
        latency.observe_micros(micros);
    }
    if let Some(histogram) = limits.histogram.as_ref() {
        histogram.record(micros);
    }
    if micros > SLOW_THRESHOLD_MICROS {
        if let (Some(slow_log), Some(fingerprint)) = (limits.slow_log.as_ref(), fingerprint) {
            slow_log.record(SlowEntry {
                kind: String::from("sql"),
                fingerprint,
                table: table.clone(),
                micros,
                at_unix: slow_now_secs(),
            });
        }
    }
    if let Some(traces) = limits.traces.as_ref() {
        let mut span = TraceSpan::root(String::from("pg"), slow_now_secs());
        span.attr(String::from("table"), table.clone());
        span.finish(micros);
        if let Ok(mut collector) = traces.lock() {
            collector.push(span);
        }
    }
}

fn slow_now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

impl PgGateway<TxnManager> {
    pub fn new(tenant: String, database: String) -> Self {
        Self {
            executor: Arc::new(Executor::new(tenant.clone(), database.clone())),
            tls: None,
            authenticator: None,
            tenant,
            database,
            qos: None,
            metering: None,
            latency: None,
            histogram: None,
            slow_log: None,
            traces: None,
            range_hook: RangeLoadHook::default(),
        }
    }

    pub fn with_executor(executor: Executor<TxnManager>) -> Self {
        Self {
            tenant: executor.tenant_name().to_string(),
            database: executor.database_name().to_string(),
            executor: Arc::new(executor),
            tls: None,
            authenticator: None,
            qos: None,
            metering: None,
            latency: None,
            histogram: None,
            slow_log: None,
            traces: None,
            range_hook: RangeLoadHook::default(),
        }
    }

    pub fn with_tls(mut self, acceptor: ryme_wire_native::TlsAcceptor) -> Self {
        self.tls = Some(acceptor);
        self
    }
}

impl<B> PgGateway<B>
where
    B: TxnBackend,
{
    pub fn with_backend_executor(executor: Executor<B>) -> Self {
        Self {
            tenant: executor.tenant_name().to_string(),
            database: executor.database_name().to_string(),
            executor: Arc::new(executor),
            tls: None,
            authenticator: None,
            qos: None,
            metering: None,
            latency: None,
            histogram: None,
            slow_log: None,
            traces: None,
            range_hook: RangeLoadHook::default(),
        }
    }

    pub fn with_backend_tls(
        executor: Executor<B>,
        acceptor: ryme_wire_native::TlsAcceptor,
    ) -> Self {
        Self {
            tenant: executor.tenant_name().to_string(),
            database: executor.database_name().to_string(),
            executor: Arc::new(executor),
            tls: Some(acceptor),
            authenticator: None,
            qos: None,
            metering: None,
            latency: None,
            histogram: None,
            slow_log: None,
            traces: None,
            range_hook: RangeLoadHook::default(),
        }
    }

    pub fn with_qos(mut self, qos: Arc<Mutex<QosRegistry>>) -> Self {
        self.qos = Some(qos);
        self
    }

    pub fn with_authenticator<F>(mut self, authenticator: F) -> Self
    where
        F: Fn(&str, &str) -> Result<String> + Send + Sync + 'static,
    {
        self.authenticator = Some(Arc::new(authenticator));
        self
    }

    pub fn with_metering(mut self, metering: Arc<Mutex<MeterRegistry>>) -> Self {
        self.metering = Some(metering);
        self
    }

    pub fn with_observe(
        mut self,
        latency: LatencyWindow,
        histogram: Histogram,
        slow_log: SlowLog,
    ) -> Self {
        self.latency = Some(latency);
        self.histogram = Some(histogram);
        self.slow_log = Some(slow_log);
        self
    }

    pub fn with_traces(mut self, traces: Arc<Mutex<TraceCollector>>) -> Self {
        self.traces = Some(traces);
        self
    }

    pub fn with_range_hook(mut self, hook: RangeLoadHook) -> Self {
        self.range_hook = hook;
        self
    }

    fn limits(&self) -> ConnLimits {
        ConnLimits {
            tenant: self.tenant.clone(),
            database: self.database.clone(),
            qos: self.qos.clone(),
            metering: self.metering.clone(),
            latency: self.latency.clone(),
            histogram: self.histogram.clone(),
            slow_log: self.slow_log.clone(),
            traces: self.traces.clone(),
            range_hook: self.range_hook.clone(),
        }
    }

    pub async fn serve(&self, listener: TcpListener) -> Result<()>
    where
        B: Send + Sync + 'static,
    {
        loop {
            let (socket, _) = listener.accept().await.map_err(|e| RymeError::Io(e.to_string()))?;
            socket.set_nodelay(true).map_err(|e| RymeError::Io(e.to_string()))?;
            let executor = self.executor.clone();
            let tls = self.tls.clone();
            let limits = self.limits();
            let authenticator = self.authenticator.clone();
            tokio::spawn(async move {
                let _ = handle_connection(socket, executor, tls, limits, authenticator).await;
            });
        }
    }

    pub async fn serve_limited(&self, listener: TcpListener, max_connections: usize) -> Result<()>
    where
        B: Send + Sync + 'static,
    {
        let limit = Arc::new(tokio::sync::Semaphore::new(max_connections.max(1)));
        loop {
            let (mut socket, _) =
                listener.accept().await.map_err(|e| RymeError::Io(e.to_string()))?;
            let Ok(permit) = limit.clone().try_acquire_owned() else {
                let response = encode_error_code("53300", String::from("too many connections"));
                let _ = socket.write_all(&response).await;
                continue;
            };
            socket.set_nodelay(true).map_err(|e| RymeError::Io(e.to_string()))?;
            let executor = self.executor.clone();
            let tls = self.tls.clone();
            let limits = self.limits();
            let authenticator = self.authenticator.clone();
            tokio::spawn(async move {
                let _permit = permit;
                let _ = handle_connection(socket, executor, tls, limits, authenticator).await;
            });
        }
    }
}

async fn serve_authenticated<B, S>(
    mut socket: S,
    executor: Arc<Executor<B>>,
    limits: ConnLimits,
    startup: StartupParams,
    authenticator: Option<PgAuthenticator>,
) -> std::result::Result<(), RymeError>
where
    B: TxnBackend,
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (executor, limits) =
        authenticate_startup(&mut socket, executor, limits, &startup, authenticator.as_ref())
            .await?;
    let (pid, secret, cancellation) = register_backend();
    let result = async {
        send_auth_ok(&mut socket).await?;
        send_greeting(&mut socket).await?;
        send_backend_key_data(&mut socket, pid, secret).await?;
        send_ready(&mut socket).await?;
        serve_loop(socket, executor, limits, cancellation).await
    }
    .await;
    unregister_backend(pid, secret);
    result
}

async fn handle_connection<B>(
    mut socket: TcpStream,
    executor: Arc<Executor<B>>,
    tls: Option<ryme_wire_native::TlsAcceptor>,
    limits: ConnLimits,
    authenticator: Option<PgAuthenticator>,
) -> std::result::Result<(), RymeError>
where
    B: TxnBackend,
{
    const CANCEL_REQUEST_CODE: u32 = 80877102;
    let mut length_buffer = [0u8; 4];
    socket.read_exact(&mut length_buffer).await.map_err(|e| RymeError::Io(e.to_string()))?;
    let startup_len = u32::from_be_bytes(length_buffer) as usize;
    if startup_len == 8 {
        let mut magic = [0u8; 4];
        socket.read_exact(&mut magic).await.map_err(|e| RymeError::Io(e.to_string()))?;
        let code = u32::from_be_bytes(magic);
        if code == 80877103 || code == 80877102 {
            if let Some(acceptor) = tls {
                socket.write_all(b"S").await.map_err(|e| RymeError::Io(e.to_string()))?;
                let mut tls_stream = acceptor
                    .acceptor()
                    .accept(socket)
                    .await
                    .map_err(|e| RymeError::Io(e.to_string()))?;
                let mut encrypted_len = [0u8; 4];
                tls_stream
                    .read_exact(&mut encrypted_len)
                    .await
                    .map_err(|e| RymeError::Io(e.to_string()))?;
                let startup_len = u32::from_be_bytes(encrypted_len) as usize;
                if !(8..=10240).contains(&startup_len) {
                    return Err(RymeError::InvalidArgument(String::from("startup")));
                }
                let mut rest = vec![0u8; startup_len - 4];
                tls_stream.read_exact(&mut rest).await.map_err(|e| RymeError::Io(e.to_string()))?;
                let startup = parse_startup(&rest)?;
                return serve_authenticated(tls_stream, executor, limits, startup, authenticator)
                    .await;
            }
            socket.write_all(b"N").await.map_err(|e| RymeError::Io(e.to_string()))?;
            socket
                .read_exact(&mut length_buffer)
                .await
                .map_err(|e| RymeError::Io(e.to_string()))?;
            let retry = u32::from_be_bytes(length_buffer) as usize;
            if !(8..=10240).contains(&retry) {
                return Err(RymeError::InvalidArgument(String::from("startup")));
            }
            let mut rest = vec![0u8; retry - 4];
            socket.read_exact(&mut rest).await.map_err(|e| RymeError::Io(e.to_string()))?;
            let startup = parse_startup(&rest)?;
            return serve_authenticated(socket, executor, limits, startup, authenticator).await;
        }
        return Err(RymeError::InvalidArgument(String::from("startup")));
    }
    if !(8..=10240).contains(&startup_len) {
        return Err(RymeError::InvalidArgument(String::from("startup")));
    }
    let mut rest = vec![0u8; startup_len - 4];
    socket.read_exact(&mut rest).await.map_err(|e| RymeError::Io(e.to_string()))?;
    // CancelRequest is a standalone 16-byte startup packet containing the
    // backend pid and secret. It has no response; signal the matching live
    // session and close this short-lived cancel connection.
    if startup_len == 16
        && rest.len() == 12
        && u32::from_be_bytes([rest[0], rest[1], rest[2], rest[3]]) == CANCEL_REQUEST_CODE
    {
        let pid = u32::from_be_bytes([rest[4], rest[5], rest[6], rest[7]]);
        let secret = u32::from_be_bytes([rest[8], rest[9], rest[10], rest[11]]);
        cancel_backend(pid, secret);
        return Ok(());
    }
    let startup = parse_startup(&rest)?;
    serve_authenticated(socket, executor, limits, startup, authenticator).await
}

fn parse_startup(payload: &[u8]) -> Result<StartupParams> {
    if payload.len() < 5 {
        return Err(RymeError::InvalidArgument(String::from("startup")));
    }
    let version = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
    if version != 196608 {
        return Err(RymeError::InvalidArgument(String::from("unsupported protocol")));
    }
    let mut values = HashMap::new();
    let mut offset = 4;
    while offset < payload.len() {
        let Some(end) = payload[offset..].iter().position(|byte| *byte == 0) else {
            return Err(RymeError::InvalidArgument(String::from("startup parameters")));
        };
        if end == 0 {
            break;
        }
        let key = String::from_utf8(payload[offset..offset + end].to_vec())
            .map_err(|_| RymeError::InvalidArgument(String::from("startup parameters")))?;
        offset += end + 1;
        let Some(value_end) = payload[offset..].iter().position(|byte| *byte == 0) else {
            return Err(RymeError::InvalidArgument(String::from("startup parameters")));
        };
        let value = String::from_utf8(payload[offset..offset + value_end].to_vec())
            .map_err(|_| RymeError::InvalidArgument(String::from("startup parameters")))?;
        offset += value_end + 1;
        values.insert(key, value);
    }
    let user = values
        .remove("user")
        .filter(|user| !user.is_empty())
        .ok_or_else(|| RymeError::InvalidArgument(String::from("startup user")))?;
    Ok(StartupParams { user })
}

async fn authenticate_startup<B, S>(
    socket: &mut S,
    executor: Arc<Executor<B>>,
    limits: ConnLimits,
    startup: &StartupParams,
    authenticator: Option<&PgAuthenticator>,
) -> Result<(Arc<Executor<B>>, ConnLimits)>
where
    B: TxnBackend,
    S: AsyncRead + AsyncWrite + Unpin,
{
    let Some(authenticator) = authenticator else {
        return Ok((executor, limits));
    };
    send_auth_cleartext_password(socket).await?;
    let (tag, payload) = read_client_message(socket).await?;
    if tag != b'p' {
        let error = RymeError::Unauthorized;
        socket
            .write_all(&encode_error_code("28P01", String::from("password required")))
            .await
            .map_err(|e| RymeError::Io(e.to_string()))?;
        return Err(error);
    }
    let password = payload
        .split(|byte| *byte == 0)
        .next()
        .filter(|value| !value.is_empty())
        .ok_or(RymeError::Unauthorized)
        .and_then(|value| String::from_utf8(value.to_vec()).map_err(|_| RymeError::Unauthorized));
    let password = match password {
        Ok(password) => password,
        Err(error) => {
            socket
                .write_all(&encode_error_code("28P01", String::from("invalid password")))
                .await
                .map_err(|e| RymeError::Io(e.to_string()))?;
            return Err(error);
        }
    };
    let tenant = match authenticator(&startup.user, &password) {
        Ok(tenant) if !tenant.is_empty() => tenant,
        Ok(_) | Err(_) => {
            socket
                .write_all(&encode_error_code("28P01", String::from("authentication failed")))
                .await
                .map_err(|e| RymeError::Io(e.to_string()))?;
            return Err(RymeError::Unauthorized);
        }
    };
    let executor = Arc::new((*executor).clone().with_tenant(tenant.clone()));
    Ok((executor, limits.with_tenant(tenant)))
}

async fn read_client_message<S>(socket: &mut S) -> Result<(u8, Vec<u8>)>
where
    S: AsyncRead + Unpin,
{
    let tag = socket.read_u8().await.map_err(|e| RymeError::Io(e.to_string()))?;
    let length = socket.read_u32().await.map_err(|e| RymeError::Io(e.to_string()))? as usize;
    if !(4..=64 * 1024).contains(&length) {
        return Err(RymeError::InvalidArgument(String::from("message")));
    }
    let mut payload = vec![0u8; length - 4];
    socket.read_exact(&mut payload).await.map_err(|e| RymeError::Io(e.to_string()))?;
    Ok((tag, payload))
}

async fn serve_loop<B, S>(
    mut socket: S,
    executor: Arc<Executor<B>>,
    limits: ConnLimits,
    cancellation: Arc<AtomicBool>,
) -> std::result::Result<(), RymeError>
where
    B: TxnBackend,
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut statements: HashMap<String, String> = HashMap::new();
    let mut portals: HashMap<String, Portal> = HashMap::new();
    let mut prepared_cache: HashMap<String, (String, Statement)> = HashMap::new();
    let mut session: HashMap<String, String> = HashMap::new();
    let mut copy_in: Option<CopyInState> = None;
    let mut active_transaction: Option<SessionTransaction> = None;
    loop {
        let tag = match socket.read_u8().await {
            Ok(tag) => tag,
            Err(_) => return Ok(()),
        };
        let length = socket.read_u32().await.map_err(|e| RymeError::Io(e.to_string()))? as usize;
        if length < 4 {
            return Err(RymeError::InvalidArgument(String::from("frame")));
        }
        let mut payload = vec![0u8; length - 4];
        socket.read_exact(&mut payload).await.map_err(|e| RymeError::Io(e.to_string()))?;

        if cancellation.swap(false, Ordering::Acquire) && copy_in.is_none() {
            let response =
                encode_error_code("57014", String::from("canceling statement due to user request"));
            socket.write_all(&response).await.map_err(|e| RymeError::Io(e.to_string()))?;
            send_ready(&mut socket).await?;
            continue;
        }

        if copy_in.is_some() {
            match (copy_in.take(), tag) {
                (Some(CopyInState::Receiving { table, columns, mut data }), b'd') => {
                    const MAX_COPY_BYTES: usize = 64 * 1024 * 1024;
                    if data.len().saturating_add(payload.len()) > MAX_COPY_BYTES {
                        copy_in = Some(CopyInState::Failed {
                            message: String::from("COPY data exceeds the 64 MiB limit"),
                        });
                    } else {
                        data.extend_from_slice(&payload);
                        copy_in = Some(CopyInState::Receiving { table, columns, data });
                    }
                    continue;
                }
                (Some(CopyInState::Receiving { table, columns, data }), b'c') => {
                    let response = match decode_copy_rows(
                        &data,
                        &columns,
                        &executor
                            .catalog_columns(&table)
                            .into_iter()
                            .map(|column| column.name)
                            .collect::<Vec<_>>(),
                    ) {
                        Ok(decoded) => {
                            let count = match &decoded {
                                DecodedCopy::KeyValue(rows) => rows.len(),
                                DecodedCopy::Columns { rows, .. } => rows.len(),
                            };
                            let statement = match decoded {
                                DecodedCopy::KeyValue(rows) => Statement::CopyFrom {
                                    table: table.clone(),
                                    columns: Vec::new(),
                                    rows,
                                },
                                DecodedCopy::Columns { columns, rows } => Statement::InsertRows {
                                    table: table.clone(),
                                    columns,
                                    rows,
                                    upsert: false,
                                    on_conflict_do_nothing: false,
                                    conflict_target: Vec::new(),
                                    conflict_update: Vec::new(),
                                    conflict_filter: Vec::new(),
                                },
                            };
                            if let Some(denied) =
                                admit_statement(&limits, &statement, data.len() as u64)
                            {
                                denied
                            } else {
                                let start = std::time::Instant::now();
                                if let Some(transaction) = active_transaction.as_mut() {
                                    match executor
                                        .execute_in_transaction(&mut transaction.txn, statement)
                                        .await
                                    {
                                        Ok((_result, changes)) => {
                                            note_range_changes(
                                                &limits.range_hook,
                                                &table,
                                                &changes,
                                                count.max(1) as u64,
                                            );
                                            transaction.changes.extend(changes);
                                            observe_statement(&limits, true);
                                            let fingerprint = limits.slow_log.as_ref().map(|_| {
                                                query_fingerprint(&format!(
                                                    "COPY {table} FROM STDIN"
                                                ))
                                            });
                                            record_timing(
                                                &limits,
                                                fingerprint,
                                                table,
                                                start.elapsed().as_micros() as u64,
                                            );
                                            copy_complete(count)
                                        }
                                        Err(error) => {
                                            transaction.failed = true;
                                            encode_error_code(error_code(&error), error.to_string())
                                        }
                                    }
                                } else {
                                    let range_keys = if limits.range_hook.is_armed() {
                                        statement_range_keys(&statement)
                                    } else {
                                        None
                                    };
                                    let result = match statement {
                                        Statement::CopyFrom { table, rows, .. } => {
                                            executor.bulk_upsert(table, rows).await.map(|_| ())
                                        }
                                        statement => executor.execute(statement).await.map(|_| ()),
                                    };
                                    match result {
                                        Ok(()) => {
                                            observe_statement(&limits, true);
                                            note_statement_range(
                                                &limits.range_hook,
                                                &table,
                                                range_keys.as_deref(),
                                                count as u64,
                                            );
                                            let fingerprint = limits.slow_log.as_ref().map(|_| {
                                                query_fingerprint(&format!(
                                                    "COPY {table} FROM STDIN"
                                                ))
                                            });
                                            record_timing(
                                                &limits,
                                                fingerprint,
                                                table,
                                                start.elapsed().as_micros() as u64,
                                            );
                                            copy_complete(count)
                                        }
                                        Err(error) => {
                                            encode_error_code(error_code(&error), error.to_string())
                                        }
                                    }
                                }
                            }
                        }
                        Err(message) => encode_error_code("22P04", message),
                    };
                    let response = match admit_response(&limits, response.len() as u64) {
                        Some(denied) => denied,
                        None => response,
                    };
                    socket.write_all(&response).await.map_err(|e| RymeError::Io(e.to_string()))?;
                    send_ready(&mut socket).await?;
                    continue;
                }
                (Some(CopyInState::Receiving { .. }), b'f') => {
                    let response = encode_error_code("57014", copy_fail_message(&payload));
                    socket.write_all(&response).await.map_err(|e| RymeError::Io(e.to_string()))?;
                    send_ready(&mut socket).await?;
                    continue;
                }
                (Some(CopyInState::Failed { message }), b'd') => {
                    copy_in = Some(CopyInState::Failed { message });
                    continue;
                }
                (Some(CopyInState::Failed { message }), b'c' | b'f') => {
                    let response = encode_error_code("22P04", message);
                    socket.write_all(&response).await.map_err(|e| RymeError::Io(e.to_string()))?;
                    send_ready(&mut socket).await?;
                    continue;
                }
                (Some(CopyInState::Receiving { .. }), _)
                | (Some(CopyInState::Failed { .. }), _) => {
                    let response =
                        encode_error_code("08P01", String::from("unexpected message during COPY"));
                    socket.write_all(&response).await.map_err(|e| RymeError::Io(e.to_string()))?;
                    send_ready(&mut socket).await?;
                    continue;
                }
                (None, _) => unreachable!(),
            }
        }

        match tag {
            b'X' => return Ok(()),
            b'Q' => {
                let query = String::from_utf8_lossy(&payload);
                let mut out = Vec::new();
                let mut failed = false;
                let mut awaiting_copy = false;
                for statement in split_statements(&query) {
                    let trimmed = statement.trim_matches(|c| c == '\0' || c == ';' || c == ' ');
                    if trimmed.is_empty() {
                        continue;
                    }
                    if failed {
                        break;
                    }
                    if let Some(response) = catalog_query(trimmed, &executor) {
                        out.extend_from_slice(&response);
                    } else if let Some(control) = transaction_control(trimmed, &session) {
                        out.extend_from_slice(
                            &handle_transaction_control(
                                control,
                                &executor,
                                &mut active_transaction,
                            )
                            .await,
                        );
                    } else if let Some(response) = session_command(trimmed, &mut session) {
                        out.extend_from_slice(&response);
                    } else if let Some(response) = prepared_command(
                        trimmed,
                        &mut statements,
                        &mut prepared_cache,
                        &executor,
                        &session,
                        &limits,
                        &mut active_transaction,
                    )
                    .await
                    {
                        out.extend_from_slice(&response);
                    } else {
                        match parse(trimmed) {
                            Ok(Statement::CopyFrom { table, columns, rows }) if rows.is_empty() => {
                                out.extend_from_slice(&copy_in_response());
                                copy_in = Some(CopyInState::Receiving {
                                    table,
                                    columns,
                                    data: Vec::new(),
                                });
                                awaiting_copy = true;
                                break;
                            }
                            Ok(Statement::CopyTo { table, columns }) => {
                                let statement = Statement::CopyTo { table, columns };
                                if let Some(denied) =
                                    admit_statement(&limits, &statement, trimmed.len() as u64)
                                {
                                    if let Some(transaction) = active_transaction.as_mut() {
                                        transaction.failed = true;
                                    }
                                    out.extend_from_slice(&denied);
                                    break;
                                }
                                let start = std::time::Instant::now();
                                match execute_copy_to_for_session(
                                    &executor,
                                    statement,
                                    &session,
                                    &mut active_transaction,
                                )
                                .await
                                {
                                    Ok(QueryResult::Table { columns, rows }) => {
                                        observe_statement(&limits, false);
                                        record_timing(
                                            &limits,
                                            limits
                                                .slow_log
                                                .as_ref()
                                                .map(|_| query_fingerprint(trimmed)),
                                            String::from("COPY"),
                                            start.elapsed().as_micros() as u64,
                                        );
                                        out.extend_from_slice(&copy_out_response(&columns));
                                        for row in &rows {
                                            out.extend_from_slice(&copy_data(
                                                &encode_copy_text_row(row),
                                            ));
                                        }
                                        out.extend_from_slice(&copy_done());
                                        out.extend_from_slice(&command_complete(&format!(
                                            "COPY {}",
                                            rows.len()
                                        )));
                                    }
                                    Ok(_) => {
                                        out.extend_from_slice(&encode_error_code(
                                            "42601",
                                            String::from("COPY TO requires tabular data"),
                                        ));
                                    }
                                    Err(error) => {
                                        out.extend_from_slice(&encode_error_code(
                                            error_code(&error),
                                            error.to_string(),
                                        ));
                                    }
                                }
                                break;
                            }
                            Ok(statement) => {
                                let response = execute_statement_for_session(
                                    &executor,
                                    &limits,
                                    statement,
                                    trimmed.len() as u64,
                                    &session,
                                    &mut active_transaction,
                                )
                                .await;
                                if response.first() == Some(&b'E') {
                                    failed = true;
                                }
                                out.extend_from_slice(&response);
                            }
                            Err(_) => match session_select(trimmed, &session) {
                                Some(rows) => out.extend_from_slice(&encode_session_rows(rows)),
                                None => {
                                    out.extend_from_slice(&encode_error_code(
                                        "42601",
                                        format!("unsupported statement: {trimmed}"),
                                    ));
                                    failed = true;
                                    if let Some(transaction) = active_transaction.as_mut() {
                                        transaction.failed = true;
                                    }
                                }
                            },
                        }
                    }
                }
                if out.is_empty() {
                    out.extend_from_slice(&frame(b'I', b""));
                }
                if cancellation.swap(false, Ordering::Acquire) {
                    out = encode_error_code(
                        "57014",
                        String::from("canceling statement due to user request"),
                    );
                }
                let out = match admit_response(&limits, out.len() as u64) {
                    Some(denied) => denied,
                    None => out,
                };
                socket.write_all(&out).await.map_err(|e| RymeError::Io(e.to_string()))?;
                if !awaiting_copy {
                    send_ready(&mut socket).await?;
                }
            }
            b'P' => match parse_prepare(&payload) {
                Ok((name, query)) => {
                    prepared_cache.remove(&name);
                    statements.insert(name, query);
                    socket
                        .write_all(&frame(b'1', b""))
                        .await
                        .map_err(|e| RymeError::Io(e.to_string()))?;
                }
                Err(e) => {
                    let response = encode_error(e.to_string());
                    socket.write_all(&response).await.map_err(|e| RymeError::Io(e.to_string()))?;
                }
            },
            b'B' => match parse_bind(&payload) {
                Ok((portal, statement, params)) => {
                    portals.insert(portal, Portal { statement, params, cached: None });
                    socket
                        .write_all(&frame(b'2', b""))
                        .await
                        .map_err(|e| RymeError::Io(e.to_string()))?;
                }
                Err(e) => {
                    let response = encode_error(e.to_string());
                    socket.write_all(&response).await.map_err(|e| RymeError::Io(e.to_string()))?;
                }
            },
            b'D' => {
                let kind = payload.first().copied().unwrap_or(0);
                let mut offset = 1;
                let name = read_cstring(&payload, &mut offset).unwrap_or_default();
                let query = if kind == b'P' {
                    portals.get(&name).and_then(|portal| statements.get(&portal.statement)).cloned()
                } else {
                    statements.get(&name).cloned()
                };
                let response = match query {
                    Some(query) => {
                        let mut response = Vec::new();
                        if kind == b'S' {
                            response.extend(parameter_description(&query));
                        }
                        response.extend(describe_query(&query));
                        response
                    }
                    None => row_description(),
                };
                socket.write_all(&response).await.map_err(|e| RymeError::Io(e.to_string()))?;
            }
            b'E' => {
                let portal = read_cstring(&payload, &mut 0).unwrap_or_default();
                let response = match portals.get_mut(&portal) {
                    Some(entry) => match statements.get(&entry.statement) {
                        Some(query) => {
                            let bound = bind(query, &entry.params);
                            let trimmed = bound.trim_matches(|c| c == '\0' || c == ';' || c == ' ');
                            let cached = entry
                                .cached
                                .as_ref()
                                .filter(|(text, _)| text.as_str() == trimmed)
                                .map(|(_, statement)| statement.clone());
                            let parsed = match cached {
                                Some(statement) => Ok(statement),
                                None => match parse(trimmed) {
                                    Ok(statement) => {
                                        entry.cached =
                                            Some((trimmed.to_string(), statement.clone()));
                                        Ok(statement)
                                    }
                                    Err(e) => Err(e),
                                },
                            };
                            if let Some(response) = catalog_query(trimmed, &executor) {
                                response
                            } else if let Some(control) = transaction_control(trimmed, &session) {
                                handle_transaction_control(
                                    control,
                                    &executor,
                                    &mut active_transaction,
                                )
                                .await
                            } else {
                                match parsed {
                                    Ok(statement) => {
                                        execute_statement_for_session(
                                            &executor,
                                            &limits,
                                            statement,
                                            bound.len() as u64,
                                            &session,
                                            &mut active_transaction,
                                        )
                                        .await
                                    }
                                    Err(_) => match session_select(
                                        bound.trim_matches(|c| c == '\0' || c == ';' || c == ' '),
                                        &session,
                                    ) {
                                        Some(rows) => encode_session_rows(rows),
                                        None => encode_error_code("42601", String::from("syntax")),
                                    },
                                }
                            }
                        }
                        None => encode_error(String::from("unknown statement")),
                    },
                    None => encode_error(String::from("unknown portal")),
                };
                let response = match admit_response(&limits, response.len() as u64) {
                    Some(denied) => denied,
                    None => response,
                };
                let response = if cancellation.swap(false, Ordering::Acquire) {
                    encode_error_code(
                        "57014",
                        String::from("canceling statement due to user request"),
                    )
                } else {
                    response
                };
                socket.write_all(&response).await.map_err(|e| RymeError::Io(e.to_string()))?;
            }
            b'C' => {
                if payload.first() == Some(&b'S') {
                    if let Some(name) = read_cstring(&payload, &mut 1) {
                        statements.remove(&name);
                        prepared_cache.remove(&name);
                    }
                } else if let Some(name) = read_cstring(&payload, &mut 1) {
                    portals.remove(&name);
                }
                socket
                    .write_all(&frame(b'3', b""))
                    .await
                    .map_err(|e| RymeError::Io(e.to_string()))?;
            }
            b'S' | b'H' => {
                send_ready(&mut socket).await?;
            }
            _ => {
                let response = encode_error(String::from("unsupported"));
                socket.write_all(&response).await.map_err(|e| RymeError::Io(e.to_string()))?;
                send_ready(&mut socket).await?;
            }
        }
    }
}

fn parameter_description(query: &str) -> Vec<u8> {
    let count = parameter_count(query).min(i16::MAX as usize) as i16;
    let mut body = Vec::with_capacity(2 + count as usize * 4);
    body.extend_from_slice(&count.to_be_bytes());
    for _ in 0..count {
        body.extend_from_slice(&0u32.to_be_bytes());
    }
    frame(b't', &body)
}

fn parameter_count(query: &str) -> usize {
    let chars = query.chars().collect::<Vec<_>>();
    let mut index = 0;
    let mut maximum = 0usize;
    let mut quote = None;
    let mut line_comment = false;
    let mut block_comment = false;
    while index < chars.len() {
        let current = chars[index];
        if line_comment {
            if current == '\n' {
                line_comment = false;
            }
            index += 1;
            continue;
        }
        if block_comment {
            if current == '*' && chars.get(index + 1) == Some(&'/') {
                block_comment = false;
                index += 2;
            } else {
                index += 1;
            }
            continue;
        }
        if let Some(delimiter) = quote {
            if current == '\\' && delimiter == '\'' {
                index += 2.min(chars.len().saturating_sub(index));
                continue;
            }
            if current == delimiter {
                if chars.get(index + 1) == Some(&delimiter) {
                    index += 2;
                    continue;
                }
                quote = None;
            }
            index += 1;
            continue;
        }
        if current == '-' && chars.get(index + 1) == Some(&'-') {
            line_comment = true;
            index += 2;
            continue;
        }
        if current == '/' && chars.get(index + 1) == Some(&'*') {
            block_comment = true;
            index += 2;
            continue;
        }
        if current == '\'' || current == '"' {
            quote = Some(current);
            index += 1;
            continue;
        }
        if current == '$' {
            let mut end = index + 1;
            while chars.get(end).is_some_and(char::is_ascii_digit) {
                end += 1;
            }
            if end > index + 1 {
                if let Ok(number) = chars[index + 1..end].iter().collect::<String>().parse() {
                    maximum = maximum.max(number);
                }
                index = end;
                continue;
            }
        }
        index += 1;
    }
    maximum
}

fn describe_query(query: &str) -> Vec<u8> {
    let trimmed = query.trim_matches(|c| c == '\0' || c == ';' || c == ' ');
    if let Some(columns) = catalog_query_columns(trimmed) {
        return multi_row_description(&columns);
    }
    if let Ok(statement) = parse(trimmed) {
        if let Statement::Returning { fields, .. } = &statement {
            return returning_description(fields);
        }
        if let Statement::SelectColumns { columns, aliases, .. } = &statement {
            let names = columns
                .iter()
                .enumerate()
                .map(|(index, column)| {
                    aliases
                        .get(index)
                        .and_then(|alias| alias.clone())
                        .unwrap_or_else(|| column.clone())
                })
                .collect::<Vec<_>>();
            return multi_row_description(&names);
        }
        if let Statement::SelectValues { columns, .. } = &statement {
            return multi_row_description(columns);
        }
        if let Statement::SequenceValue { operation, .. } = &statement {
            let name = match operation {
                ryme_sql::SequenceOperation::Nextval => "nextval",
                ryme_sql::SequenceOperation::Currval => "currval",
                ryme_sql::SequenceOperation::Setval => "setval",
            };
            return multi_row_description(&[String::from(name)]);
        }
        if matches!(
            statement,
            ryme_sql::Statement::SelectByKey { .. } | ryme_sql::Statement::SelectScan { .. }
        ) {
            return row_description();
        }
        return frame(b'n', b"");
    }
    if let Some(columns) = session_select(trimmed, &HashMap::new()) {
        let names: Vec<String> = columns.into_iter().map(|(name, _)| name).collect();
        return multi_row_description(&names);
    }
    frame(b'n', b"")
}

fn catalog_query_columns(query: &str) -> Option<Vec<String>> {
    let upper = query.to_ascii_uppercase();
    let kind = if upper.contains("INFORMATION_SCHEMA.TABLE_PRIVILEGES")
        || upper.contains("INFORMATION_SCHEMA.ROLE_TABLE_GRANTS")
    {
        "privileges"
    } else if upper.contains("PG_CATALOG.PG_ROLES") || upper.contains("PG_ROLES") {
        "roles"
    } else if upper.contains("INFORMATION_SCHEMA.TABLES") || upper.contains("PG_CATALOG.PG_TABLES")
    {
        "tables"
    } else if upper.contains("INFORMATION_SCHEMA.COLUMNS") {
        "columns"
    } else if upper.contains("PG_CATALOG.PG_ATTRIBUTE") {
        "attributes"
    } else if upper.contains("PG_CATALOG.PG_INDEXES") {
        "indexes"
    } else if upper.contains("PG_CATALOG.PG_POLICIES") || upper.contains("PG_POLICIES") {
        "policies"
    } else if upper.contains("PG_CATALOG.PG_PROC") || upper.contains("PG_PROC") {
        "functions"
    } else if upper.contains("PG_CATALOG.PG_TRIGGER") || upper.contains("PG_TRIGGER") {
        "triggers"
    } else if upper.contains("PG_CATALOG.PG_SEQUENCES") || upper.contains("PG_SEQUENCES") {
        "sequences"
    } else if upper.contains("PG_CATALOG.PG_NAMESPACE") {
        "namespaces"
    } else if upper.contains("PG_CATALOG.PG_CLASS") {
        "classes"
    } else if upper.contains("PG_CATALOG.PG_TYPE") {
        "types"
    } else if upper.contains("PG_CATALOG.PG_CONSTRAINT") {
        "constraints"
    } else if upper.contains("PG_CATALOG.PG_DEFAULT_ACL") || upper.contains("PG_DEFAULT_ACL") {
        "default_acls"
    } else if upper.contains("PG_CATALOG.PG_INDEX") {
        "index"
    } else {
        return None;
    };
    let from = upper.find(" FROM ")?;
    let selected = split_select_list(query[6..from].trim());
    let defaults = match kind {
        "tables" => vec![String::from("table_name")],
        "privileges" => vec![String::from("table_name")],
        "roles" => vec![String::from("rolname")],
        "indexes" => vec![String::from("indexname")],
        "policies" => vec![String::from("policyname")],
        "functions" => vec![String::from("proname")],
        "triggers" => vec![String::from("tgname")],
        "sequences" => vec![String::from("sequencename")],
        "namespaces" => vec![String::from("nspname")],
        "classes" => vec![String::from("relname")],
        "types" => vec![String::from("typname")],
        "constraints" => vec![String::from("conname")],
        "default_acls" => vec![String::from("defaclrole")],
        "index" => vec![String::from("indexrelid")],
        "attributes" => vec![String::from("attname")],
        _ => vec![String::from("column_name")],
    };
    if selected.is_empty() || selected.iter().any(|item| item == "*") {
        return Some(match kind {
            "privileges" => vec![
                String::from("grantor"),
                String::from("grantee"),
                String::from("table_catalog"),
                String::from("table_schema"),
                String::from("table_name"),
                String::from("privilege_type"),
                String::from("is_grantable"),
            ],
            "roles" => vec![
                String::from("oid"),
                String::from("rolname"),
                String::from("rolsuper"),
                String::from("rolinherit"),
                String::from("rolcreaterole"),
                String::from("rolcreatedb"),
                String::from("rolcanlogin"),
                String::from("rolreplication"),
                String::from("rolbypassrls"),
                String::from("rolconnlimit"),
                String::from("rolpassword"),
                String::from("rolvaliduntil"),
            ],
            "tables" => vec![
                String::from("table_schema"),
                String::from("table_name"),
                String::from("table_type"),
            ],
            "indexes" => vec![
                String::from("schemaname"),
                String::from("tablename"),
                String::from("indexname"),
                String::from("tablespace"),
                String::from("indexdef"),
            ],
            "policies" => vec![
                String::from("schemaname"),
                String::from("tablename"),
                String::from("policyname"),
                String::from("permissive"),
                String::from("roles"),
                String::from("cmd"),
                String::from("qual"),
                String::from("with_check"),
            ],
            "functions" => vec![
                String::from("oid"),
                String::from("proname"),
                String::from("pronamespace"),
                String::from("proowner"),
                String::from("prolang"),
                String::from("prokind"),
                String::from("prorettype"),
                String::from("pronargs"),
                String::from("prosrc"),
                String::from("probin"),
            ],
            "triggers" => vec![
                String::from("oid"),
                String::from("tgrelid"),
                String::from("tgname"),
                String::from("tgfoid"),
                String::from("tgtype"),
                String::from("tgenabled"),
                String::from("tgisinternal"),
            ],
            "sequences" => vec![
                String::from("schemaname"),
                String::from("sequencename"),
                String::from("sequenceowner"),
                String::from("data_type"),
                String::from("start_value"),
                String::from("min_value"),
                String::from("max_value"),
                String::from("increment_by"),
                String::from("cycle"),
                String::from("cache_size"),
                String::from("last_value"),
            ],
            "namespaces" => vec![
                String::from("oid"),
                String::from("nspname"),
                String::from("nspowner"),
                String::from("nspacl"),
            ],
            "classes" => vec![
                String::from("oid"),
                String::from("relname"),
                String::from("relnamespace"),
                String::from("relkind"),
                String::from("relpersistence"),
                String::from("relhasindex"),
            ],
            "types" => vec![
                String::from("oid"),
                String::from("typname"),
                String::from("typnamespace"),
                String::from("typtype"),
                String::from("typrelid"),
                String::from("typlen"),
            ],
            "constraints" => vec![
                String::from("oid"),
                String::from("conname"),
                String::from("connamespace"),
                String::from("contype"),
                String::from("conrelid"),
                String::from("conindid"),
                String::from("confrelid"),
                String::from("conkey"),
                String::from("confkey"),
                String::from("confdeltype"),
                String::from("confupdtype"),
                String::from("convalidated"),
            ],
            "default_acls" => vec![
                String::from("oid"),
                String::from("defaclrole"),
                String::from("defaclnamespace"),
                String::from("defaclobjtype"),
                String::from("defaclacl"),
            ],
            "index" => vec![
                String::from("indexrelid"),
                String::from("indrelid"),
                String::from("indisunique"),
                String::from("indisprimary"),
                String::from("indnatts"),
                String::from("indnkeyatts"),
                String::from("indkey"),
            ],
            "attributes" => vec![
                String::from("attrelid"),
                String::from("attname"),
                String::from("atttypid"),
                String::from("attlen"),
                String::from("attnum"),
                String::from("attnotnull"),
                String::from("atthasdef"),
                String::from("attisdropped"),
            ],
            _ => vec![
                String::from("column_name"),
                String::from("data_type"),
                String::from("is_nullable"),
                String::from("ordinal_position"),
                String::from("column_default"),
            ],
        });
    }
    let fields: Vec<String> = selected
        .into_iter()
        .map(|item| {
            let item = item.trim();
            let item = item.split_ascii_whitespace().next().unwrap_or(item);
            item.rsplit('.').next().unwrap_or(item).trim_matches('"').to_ascii_lowercase()
        })
        .collect();
    if fields.is_empty() {
        Some(defaults)
    } else {
        Some(fields)
    }
}

fn sql_literal_after(query: &str, keyword: &str) -> Option<String> {
    let upper = query.to_ascii_uppercase();
    let start = upper.find(keyword)? + keyword.len();
    let rest = &query[start..];
    let open = rest.find('\'')? + 1;
    let end = rest[open..].find('\'')? + open;
    Some(rest[open..end].replace("''", "'"))
}

fn catalog_table_parts(table: &str) -> (&str, &str) {
    table.rsplit_once('.').map_or(("public", table), |(schema, name)| (schema, name))
}

fn catalog_oid(name: &str) -> u32 {
    let mut hash = 2_166_136_261u32;
    for byte in name.as_bytes() {
        hash ^= u32::from(*byte);
        hash = hash.wrapping_mul(16_777_619);
    }
    hash.max(1)
}

fn catalog_namespace_oid(schema: &str) -> u32 {
    if schema.eq_ignore_ascii_case("public") {
        2200
    } else {
        catalog_oid(&format!("namespace:{schema}"))
    }
}

fn catalog_relation_oid(table: &str) -> u32 {
    catalog_oid(&format!("relation:{table}"))
}

fn catalog_index_oid(name: &str) -> u32 {
    catalog_oid(&format!("index:{name}"))
}

fn postgres_type_oid(data_type: &str) -> u32 {
    match data_type.to_ascii_lowercase().as_str() {
        "bool" | "boolean" => 16,
        "int2" | "smallint" => 21,
        "int4" | "integer" => 23,
        "int8" | "bigint" => 20,
        "float4" | "real" => 700,
        "float8" | "double" | "double precision" => 701,
        "numeric" | "decimal" => 1700,
        "varchar" | "character varying" => 1043,
        "text" => 25,
        "date" => 1082,
        "timestamp" => 1114,
        "timestamptz" | "timestamp with time zone" => 1184,
        "uuid" => 2950,
        "json" => 114,
        "jsonb" => 3802,
        other => catalog_oid(&format!("type:{other}")),
    }
}

fn postgres_type_length(data_type: &str) -> i16 {
    match data_type.to_ascii_lowercase().as_str() {
        "bool" | "boolean" => 1,
        "int2" | "smallint" => 2,
        "int4" | "integer" | "float4" | "real" => 4,
        "int8" | "bigint" | "float8" | "double" | "double precision" => 8,
        "uuid" => 16,
        _ => -1,
    }
}

fn postgres_type_rows() -> Vec<(u32, &'static str, i16, &'static str)> {
    vec![
        (16, "bool", 1, "b"),
        (20, "int8", 8, "b"),
        (21, "int2", 2, "b"),
        (23, "int4", 4, "b"),
        (25, "text", -1, "b"),
        (700, "float4", 4, "b"),
        (701, "float8", 8, "b"),
        (1043, "varchar", -1, "b"),
        (1082, "date", 4, "b"),
        (1114, "timestamp", 8, "b"),
        (1184, "timestamptz", 8, "b"),
        (1700, "numeric", -1, "b"),
        (2950, "uuid", 16, "b"),
        (114, "json", -1, "b"),
        (3802, "jsonb", -1, "b"),
    ]
}

fn information_schema_type(data_type: &str) -> &str {
    match data_type {
        "timestamptz" => "timestamp with time zone",
        "timestamp" => "timestamp without time zone",
        "varchar" => "character varying",
        "int2" => "smallint",
        "int4" | "integer" => "integer",
        "int8" | "bigint" => "bigint",
        "bool" => "boolean",
        other => other,
    }
}

fn default_privilege_acl_code(privilege: &str) -> &'static str {
    match privilege.to_ascii_uppercase().as_str() {
        "SELECT" => "r",
        "INSERT" => "a",
        "UPDATE" => "w",
        "DELETE" => "d",
        "TRUNCATE" => "D",
        "REFERENCES" => "x",
        "TRIGGER" => "t",
        "USAGE" => "U",
        "EXECUTE" => "X",
        "CREATE" => "C",
        "CONNECT" => "c",
        "TEMPORARY" | "TEMP" => "T",
        _ => "r",
    }
}

fn catalog_query<B>(query: &str, executor: &Arc<Executor<B>>) -> Option<Vec<u8>>
where
    B: TxnBackend,
{
    let columns = catalog_query_columns(query)?;
    let upper = query.to_ascii_uppercase();
    if upper.contains("PG_CATALOG.PG_DEFAULT_ACL") || upper.contains("PG_DEFAULT_ACL") {
        let snapshot = executor.schema_snapshot();
        let rows = snapshot
            .default_privileges
            .into_iter()
            .map(|default| {
                let owner = default.owner.as_deref().unwrap_or("postgres");
                let schema_oid = default.schema.as_deref().map(catalog_namespace_oid).unwrap_or(0);
                let object_type = match default.object_type.to_ascii_uppercase().as_str() {
                    "SEQUENCE" => "S",
                    "FUNCTION" => "f",
                    "TYPE" => "T",
                    "SCHEMA" => "n",
                    _ => "r",
                };
                let privilege = default_privilege_acl_code(&default.privilege);
                let grantor = catalog_oid(&format!("role:{owner}"));
                let acl = [
                    String::from("{"),
                    default.grantee.clone(),
                    String::from("="),
                    String::from(privilege),
                    default.grant_option.then_some("*").unwrap_or("").to_string(),
                    String::from("/"),
                    owner.to_string(),
                    String::from("}"),
                ]
                .concat();
                columns
                    .iter()
                    .map(|column| match column.as_str() {
                        "oid" => catalog_oid(&format!(
                            "default-acl:{}:{}:{}:{}:{}",
                            owner,
                            default.schema.as_deref().unwrap_or(""),
                            default.object_type,
                            default.grantee,
                            default.privilege
                        ))
                        .to_string()
                        .into_bytes(),
                        "defaclrole" => grantor.to_string().into_bytes(),
                        "defaclnamespace" => schema_oid.to_string().into_bytes(),
                        "defaclobjtype" => object_type.as_bytes().to_vec(),
                        "defaclacl" => acl.as_bytes().to_vec(),
                        _ => Vec::new(),
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        return Some(encode_catalog_rows(&columns, rows));
    }
    if upper.contains("PG_CATALOG.PG_ROLES") || upper.contains("PG_ROLES") {
        let filter = sql_literal_after(query, "ROLNAME");
        let mut roles = vec![
            RoleDefinition {
                name: String::from("postgres"),
                login: true,
                superuser: true,
                createdb: true,
                createrole: true,
                inherit: true,
            },
            RoleDefinition {
                name: String::from("anon"),
                login: false,
                superuser: false,
                createdb: false,
                createrole: false,
                inherit: true,
            },
            RoleDefinition {
                name: String::from("authenticated"),
                login: false,
                superuser: false,
                createdb: false,
                createrole: false,
                inherit: true,
            },
            RoleDefinition {
                name: String::from("service_role"),
                login: false,
                superuser: false,
                createdb: false,
                createrole: false,
                inherit: true,
            },
        ];
        for role in executor.catalog_roles() {
            if let Some(existing) = roles.iter_mut().find(|entry| entry.name == role.name) {
                *existing = role;
            } else {
                roles.push(role);
            }
        }
        let rows: Vec<Vec<Vec<u8>>> = roles
            .into_iter()
            .filter(|role| filter.as_ref().is_none_or(|want| want == &role.name))
            .map(|role| {
                let oid = catalog_oid(&format!("role:{}", role.name));
                columns
                    .iter()
                    .map(|column| match column.as_str() {
                        "oid" => oid.to_string().into_bytes(),
                        "rolname" => role.name.as_bytes().to_vec(),
                        "rolsuper" => {
                            if role.superuser {
                                b"t".to_vec()
                            } else {
                                b"f".to_vec()
                            }
                        }
                        "rolinherit" => {
                            if role.inherit {
                                b"t".to_vec()
                            } else {
                                b"f".to_vec()
                            }
                        }
                        "rolcreaterole" => {
                            if role.createrole {
                                b"t".to_vec()
                            } else {
                                b"f".to_vec()
                            }
                        }
                        "rolcreatedb" => {
                            if role.createdb {
                                b"t".to_vec()
                            } else {
                                b"f".to_vec()
                            }
                        }
                        "rolcanlogin" => {
                            if role.login {
                                b"t".to_vec()
                            } else {
                                b"f".to_vec()
                            }
                        }
                        "rolreplication" | "rolbypassrls" => b"f".to_vec(),
                        "rolconnlimit" => b"-1".to_vec(),
                        "rolpassword" | "rolvaliduntil" => SQL_NULL_SENTINEL.to_vec(),
                        _ => Vec::new(),
                    })
                    .collect()
            })
            .collect();
        return Some(encode_catalog_rows(&columns, rows));
    }
    if upper.contains("INFORMATION_SCHEMA.TABLE_PRIVILEGES")
        || upper.contains("INFORMATION_SCHEMA.ROLE_TABLE_GRANTS")
    {
        let table_filter = sql_literal_after(query, "TABLE_NAME");
        let mut rows = Vec::new();
        for privilege in executor.catalog_privileges() {
            if !privilege.object_type.eq_ignore_ascii_case("TABLE") {
                continue;
            }
            let mut tables = vec![privilege.object.clone()];
            if privilege.object.ends_with(".*") {
                let schema = privilege.object.trim_end_matches(".*");
                tables = executor
                    .catalog_tables()
                    .into_iter()
                    .filter(|table| catalog_table_parts(table).0 == schema)
                    .collect();
            }
            for table in tables {
                let (schema, name) = catalog_table_parts(&table);
                if table_filter.as_ref().is_some_and(|want| want != &table && want != name) {
                    continue;
                }
                rows.push(
                    columns
                        .iter()
                        .map(|column| match column.as_str() {
                            "grantor" => b"postgres".to_vec(),
                            "grantee" => privilege.grantee.as_bytes().to_vec(),
                            "table_catalog" => b"default".to_vec(),
                            "table_schema" => schema.as_bytes().to_vec(),
                            "table_name" => name.as_bytes().to_vec(),
                            "privilege_type" => privilege.privilege.as_bytes().to_vec(),
                            "is_grantable" => {
                                if privilege.grant_option {
                                    b"YES".to_vec()
                                } else {
                                    b"NO".to_vec()
                                }
                            }
                            _ => Vec::new(),
                        })
                        .collect(),
                );
            }
        }
        return Some(encode_catalog_rows(&columns, rows));
    }
    if upper.contains("INFORMATION_SCHEMA.TABLES") || upper.contains("PG_CATALOG.PG_TABLES") {
        let filter = sql_literal_after(query, "TABLE_NAME")
            .or_else(|| sql_literal_after(query, "TABLENAME"));
        let include_views = upper.contains("INFORMATION_SCHEMA.TABLES");
        let mut relations: Vec<(String, bool)> =
            executor.catalog_tables().into_iter().map(|table| (table, false)).collect();
        if include_views {
            relations.extend(executor.catalog_views().into_iter().map(|view| (view, true)));
        }
        let rows: Vec<Vec<Vec<u8>>> = relations
            .into_iter()
            .filter(|table| {
                let (_, table_name) = catalog_table_parts(&table.0);
                filter.as_ref().is_none_or(|want| want == &table.0 || want == table_name)
            })
            .map(|(table, is_view)| {
                let (schema, table_name) = catalog_table_parts(&table);
                columns
                    .iter()
                    .map(|column| match column.as_str() {
                        "table_schema" | "schemaname" => schema.as_bytes().to_vec(),
                        "table_name" | "tablename" => table_name.as_bytes().to_vec(),
                        "table_type" => {
                            if is_view {
                                b"VIEW".to_vec()
                            } else {
                                b"BASE TABLE".to_vec()
                            }
                        }
                        _ => Vec::new(),
                    })
                    .collect()
            })
            .collect();
        return Some(encode_catalog_rows(&columns, rows));
    }

    if upper.contains("PG_CATALOG.PG_NAMESPACE") {
        let filter = sql_literal_after(query, "NSPNAME");
        let rows: Vec<Vec<Vec<u8>>> = vec![("public", 2200u32)]
            .into_iter()
            .filter(|(schema, _)| filter.as_ref().is_none_or(|want| want == schema))
            .map(|(schema, oid)| {
                columns
                    .iter()
                    .map(|column| match column.as_str() {
                        "oid" => oid.to_string().into_bytes(),
                        "nspname" => schema.as_bytes().to_vec(),
                        "nspowner" => b"10".to_vec(),
                        "nspacl" => Vec::new(),
                        _ => Vec::new(),
                    })
                    .collect()
            })
            .collect();
        return Some(encode_catalog_rows(&columns, rows));
    }

    if upper.contains("PG_CATALOG.PG_CLASS") {
        let filter = sql_literal_after(query, "RELNAME");
        let mut relations: Vec<(String, u8)> =
            executor.catalog_tables().into_iter().map(|table| (table, b'r')).collect();
        relations.extend(executor.catalog_views().into_iter().map(|view| (view, b'v')));
        relations
            .extend(executor.catalog_sequences().into_iter().map(|sequence| (sequence.name, b'S')));
        let rows: Vec<Vec<Vec<u8>>> = relations
            .into_iter()
            .filter(|table| {
                let (_, table_name) = catalog_table_parts(&table.0);
                filter.as_ref().is_none_or(|want| want == &table.0 || want == table_name)
            })
            .map(|(table, relkind)| {
                let (schema, table_name) = catalog_table_parts(&table);
                let oid = catalog_relation_oid(&table);
                let has_index = !executor.catalog_indexes(&table).is_empty();
                columns
                    .iter()
                    .map(|column| match column.as_str() {
                        "oid" => oid.to_string().into_bytes(),
                        "relname" => table_name.as_bytes().to_vec(),
                        "relnamespace" => catalog_namespace_oid(schema).to_string().into_bytes(),
                        "relkind" => vec![relkind],
                        "relpersistence" => b"p".to_vec(),
                        "relhasindex" => {
                            if has_index {
                                b"t".to_vec()
                            } else {
                                b"f".to_vec()
                            }
                        }
                        "relowner" => b"10".to_vec(),
                        "reltuples" => b"-1".to_vec(),
                        _ => Vec::new(),
                    })
                    .collect()
            })
            .collect();
        return Some(encode_catalog_rows(&columns, rows));
    }

    if upper.contains("PG_CATALOG.PG_TYPE") {
        let filter = sql_literal_after(query, "TYPNAME");
        let rows: Vec<Vec<Vec<u8>>> = postgres_type_rows()
            .into_iter()
            .filter(|(_, name, _, _)| filter.as_ref().is_none_or(|want| want == name))
            .map(|(oid, name, length, kind)| {
                columns
                    .iter()
                    .map(|column| match column.as_str() {
                        "oid" => oid.to_string().into_bytes(),
                        "typname" => name.as_bytes().to_vec(),
                        "typnamespace" => b"11".to_vec(),
                        "typtype" => kind.as_bytes().to_vec(),
                        "typrelid" => b"0".to_vec(),
                        "typlen" => length.to_string().into_bytes(),
                        "typbyval" => {
                            if length > 0 {
                                b"t".to_vec()
                            } else {
                                b"f".to_vec()
                            }
                        }
                        _ => Vec::new(),
                    })
                    .collect()
            })
            .collect();
        return Some(encode_catalog_rows(&columns, rows));
    }

    if upper.contains("PG_CATALOG.PG_INDEXES") {
        let table = sql_literal_after(query, "TABLENAME");
        let indexes =
            table.as_deref().map(|name| executor.catalog_indexes(name)).unwrap_or_default();
        let rows: Vec<Vec<Vec<u8>>> = indexes
            .into_iter()
            .map(|index| {
                let (schema, table_name) = catalog_table_parts(&index.table);
                let fields = if index.columns.is_empty() {
                    match index.field {
                        Field::Key => String::from("id"),
                        Field::Value => String::from("value"),
                    }
                } else {
                    index.columns.join(", ")
                };
                let unique = if index.unique { "UNIQUE " } else { "" };
                columns
                    .iter()
                    .map(|column| match column.as_str() {
                        "schemaname" => schema.as_bytes().to_vec(),
                        "tablename" => table_name.as_bytes().to_vec(),
                        "indexname" => index.name.as_bytes().to_vec(),
                        "tablespace" => Vec::new(),
                        "indexdef" => format!(
                            "CREATE {unique}INDEX {} ON {}.{} ({fields})",
                            index.name, schema, table_name,
                        )
                        .into_bytes(),
                        _ => Vec::new(),
                    })
                    .collect()
            })
            .collect();
        return Some(encode_catalog_rows(&columns, rows));
    }

    if upper.contains("PG_CATALOG.PG_SEQUENCES") || upper.contains("PG_SEQUENCES") {
        let filter = sql_literal_after(query, "SEQUENCENAME");
        let rows: Vec<Vec<Vec<u8>>> = executor
            .catalog_sequences()
            .into_iter()
            .filter(|sequence| {
                let (_, name) = catalog_table_parts(&sequence.name);
                filter.as_ref().is_none_or(|want| want == &sequence.name || want == name)
            })
            .map(|sequence| {
                let (schema, name) = catalog_table_parts(&sequence.name);
                columns
                    .iter()
                    .map(|column| match column.as_str() {
                        "schemaname" => schema.as_bytes().to_vec(),
                        "sequencename" => name.as_bytes().to_vec(),
                        "sequenceowner" => b"ryme".to_vec(),
                        "data_type" => sequence.data_type.as_bytes().to_vec(),
                        "start_value" => sequence.start.to_string().into_bytes(),
                        "min_value" => sequence.min_value.to_string().into_bytes(),
                        "max_value" => sequence.max_value.to_string().into_bytes(),
                        "increment_by" => sequence.increment.to_string().into_bytes(),
                        "cycle" => {
                            if sequence.cycle {
                                b"t".to_vec()
                            } else {
                                b"f".to_vec()
                            }
                        }
                        "cache_size" => sequence.cache.to_string().into_bytes(),
                        "last_value" => sequence.last_value.map_or_else(
                            || SQL_NULL_SENTINEL.to_vec(),
                            |value| value.to_string().into_bytes(),
                        ),
                        _ => Vec::new(),
                    })
                    .collect()
            })
            .collect();
        return Some(encode_catalog_rows(&columns, rows));
    }

    if upper.contains("PG_CATALOG.PG_POLICIES") || upper.contains("PG_POLICIES") {
        let predicates =
            upper.find(" WHERE ").map(|index| &query[index + " WHERE ".len()..]).unwrap_or(query);
        let table_filter = sql_literal_after(predicates, "TABLENAME");
        let policy_filter = sql_literal_after(predicates, "POLICYNAME");
        let rows: Vec<Vec<Vec<u8>>> = executor
            .schema_snapshot()
            .rls_policies
            .into_values()
            .filter(|policy| !policy.name.starts_with("__config__"))
            .filter(|policy| {
                table_filter.as_ref().is_none_or(|want| {
                    let (_, table_name) = catalog_table_parts(&policy.table);
                    want == &policy.table || want == table_name
                })
            })
            .filter(|policy| policy_filter.as_ref().is_none_or(|want| want == &policy.name))
            .map(|policy| {
                let (schema, table) = catalog_table_parts(&policy.table);
                columns
                    .iter()
                    .map(|column| match column.as_str() {
                        "schemaname" => schema.as_bytes().to_vec(),
                        "tablename" => table.as_bytes().to_vec(),
                        "policyname" => policy.name.as_bytes().to_vec(),
                        "permissive" => b"t".to_vec(),
                        "roles" => b"{public}".to_vec(),
                        "cmd" => policy.command.as_bytes().to_vec(),
                        "qual" => policy.using.as_deref().map_or_else(
                            || SQL_NULL_SENTINEL.to_vec(),
                            |value| value.as_bytes().to_vec(),
                        ),
                        "with_check" => policy.check.as_deref().map_or_else(
                            || SQL_NULL_SENTINEL.to_vec(),
                            |value| value.as_bytes().to_vec(),
                        ),
                        _ => Vec::new(),
                    })
                    .collect()
            })
            .collect();
        return Some(encode_catalog_rows(&columns, rows));
    }

    if upper.contains("PG_CATALOG.PG_PROC") || upper.contains("PG_PROC") {
        let predicates =
            upper.find(" WHERE ").map(|index| &query[index + " WHERE ".len()..]).unwrap_or(query);
        let name_filter = sql_literal_after(predicates, "PRONAME");
        let rows: Vec<Vec<Vec<u8>>> = executor
            .schema_snapshot()
            .functions
            .into_values()
            .filter(|function| name_filter.as_ref().is_none_or(|want| want == &function.name))
            .map(|function| {
                let oid = catalog_oid(&format!("function:{}", function.name));
                let language = catalog_oid(&format!("language:{}", function.language));
                columns
                    .iter()
                    .map(|column| match column.as_str() {
                        "oid" => oid.to_string().into_bytes(),
                        "proname" => function.name.as_bytes().to_vec(),
                        "pronamespace" => b"2200".to_vec(),
                        "proowner" => b"10".to_vec(),
                        "prolang" => language.to_string().into_bytes(),
                        "prokind" => b"f".to_vec(),
                        "prorettype" => b"2279".to_vec(),
                        "pronargs" => b"0".to_vec(),
                        "prosrc" => function.body.as_bytes().to_vec(),
                        "probin" => Vec::new(),
                        _ => Vec::new(),
                    })
                    .collect()
            })
            .collect();
        return Some(encode_catalog_rows(&columns, rows));
    }

    if upper.contains("PG_CATALOG.PG_TRIGGER") || upper.contains("PG_TRIGGER") {
        let predicates =
            upper.find(" WHERE ").map(|index| &query[index + " WHERE ".len()..]).unwrap_or(query);
        let name_filter = sql_literal_after(predicates, "TGNAME");
        let table_filter = sql_literal_after(predicates, "TGRELID");
        let rows: Vec<Vec<Vec<u8>>> = executor
            .schema_snapshot()
            .triggers
            .into_values()
            .filter(|trigger| name_filter.as_ref().is_none_or(|want| want == &trigger.name))
            .filter(|trigger| {
                table_filter.as_ref().is_none_or(|want| {
                    let relation = catalog_relation_oid(&trigger.table).to_string();
                    want == &trigger.table || want == &relation
                })
            })
            .map(|trigger| {
                let oid = catalog_oid(&format!("trigger:{}\0{}", trigger.table, trigger.name));
                let relation_oid = catalog_relation_oid(&trigger.table);
                let function_oid = catalog_oid(&format!("function:{}", trigger.function));
                let mut trigger_type = 1u32;
                if trigger.timing.eq_ignore_ascii_case("BEFORE") {
                    trigger_type |= 2;
                }
                for event in &trigger.events {
                    trigger_type |= match event.as_str() {
                        "INSERT" => 4,
                        "DELETE" => 8,
                        "UPDATE" => 16,
                        "TRUNCATE" => 32,
                        _ => 0,
                    };
                }
                columns
                    .iter()
                    .map(|column| match column.as_str() {
                        "oid" => oid.to_string().into_bytes(),
                        "tgrelid" => relation_oid.to_string().into_bytes(),
                        "tgname" => trigger.name.as_bytes().to_vec(),
                        "tgfoid" => function_oid.to_string().into_bytes(),
                        "tgtype" => trigger_type.to_string().into_bytes(),
                        "tgenabled" => b"O".to_vec(),
                        "tgisinternal" => b"f".to_vec(),
                        _ => Vec::new(),
                    })
                    .collect()
            })
            .collect();
        return Some(encode_catalog_rows(&columns, rows));
    }

    if upper.contains("PG_CATALOG.PG_CONSTRAINT") {
        let filter = sql_literal_after(query, "RELNAME");
        let mut constraints = Vec::new();
        for table in executor.catalog_tables() {
            let (_, table_name) = catalog_table_parts(&table);
            if filter
                .as_ref()
                .is_some_and(|want| want.as_str() != table.as_str() && want != table_name)
            {
                continue;
            }
            let relation_oid = catalog_relation_oid(&table);
            let definitions = executor.catalog_columns(&table);
            let primary_ordinals = definitions
                .iter()
                .enumerate()
                .filter_map(|(ordinal, definition)| definition.primary_key.then_some(ordinal + 1))
                .collect::<Vec<_>>();
            if !primary_ordinals.is_empty() {
                constraints.push((
                    format!("{table_name}_pkey"),
                    String::from("p"),
                    relation_oid,
                    catalog_index_oid(&format!("constraint:{table}")),
                    format!(
                        "{{{}}}",
                        primary_ordinals.iter().map(usize::to_string).collect::<Vec<_>>().join(",")
                    ),
                    0,
                    String::new(),
                    String::new(),
                    String::new(),
                ));
            }
            for index in executor.catalog_indexes(&table).into_iter().filter(|index| index.unique) {
                let ordinals = if index.columns.is_empty() {
                    index
                        .column
                        .as_deref()
                        .and_then(|column| {
                            definitions
                                .iter()
                                .position(|definition| definition.name.eq_ignore_ascii_case(column))
                        })
                        .map(|ordinal| vec![ordinal + 1])
                        .unwrap_or_else(|| vec![1])
                } else {
                    index
                        .columns
                        .iter()
                        .filter_map(|column| {
                            definitions
                                .iter()
                                .position(|definition| definition.name.eq_ignore_ascii_case(column))
                                .map(|ordinal| ordinal + 1)
                        })
                        .collect::<Vec<_>>()
                };
                constraints.push((
                    index.name.clone(),
                    String::from("u"),
                    relation_oid,
                    catalog_index_oid(&index.name),
                    format!(
                        "{{{}}}",
                        ordinals.iter().map(usize::to_string).collect::<Vec<_>>().join(",")
                    ),
                    0,
                    String::new(),
                    String::new(),
                    String::new(),
                ));
            }
            for (foreign_index, foreign_key) in
                executor.catalog_foreign_keys(&table).into_iter().enumerate()
            {
                let conkey = foreign_key
                    .columns
                    .iter()
                    .filter_map(|column| {
                        definitions
                            .iter()
                            .position(|definition| definition.name.eq_ignore_ascii_case(column))
                            .map(|ordinal| ordinal + 1)
                    })
                    .collect::<Vec<_>>();
                let referenced_definitions =
                    executor.catalog_columns(&foreign_key.referenced_table);
                let confkey = foreign_key
                    .referenced_columns
                    .iter()
                    .filter_map(|column| {
                        referenced_definitions
                            .iter()
                            .position(|definition| definition.name.eq_ignore_ascii_case(column))
                            .map(|ordinal| ordinal + 1)
                    })
                    .collect::<Vec<_>>();
                let confdeltype = match foreign_key.on_delete {
                    ForeignKeyAction::Restrict => b"r".to_vec(),
                    ForeignKeyAction::Cascade => b"c".to_vec(),
                    ForeignKeyAction::SetNull => b"n".to_vec(),
                    ForeignKeyAction::SetDefault => b"d".to_vec(),
                };
                let confupdtype = match foreign_key.on_update {
                    ForeignKeyAction::Restrict => b"r".to_vec(),
                    ForeignKeyAction::Cascade => b"c".to_vec(),
                    ForeignKeyAction::SetNull => b"n".to_vec(),
                    ForeignKeyAction::SetDefault => b"d".to_vec(),
                };
                constraints.push((
                    format!("{table_name}_fkey_{foreign_index}"),
                    String::from("f"),
                    relation_oid,
                    0,
                    format!(
                        "{{{}}}",
                        conkey.iter().map(usize::to_string).collect::<Vec<_>>().join(",")
                    ),
                    catalog_relation_oid(&foreign_key.referenced_table),
                    format!(
                        "{{{}}}",
                        confkey.iter().map(usize::to_string).collect::<Vec<_>>().join(",")
                    ),
                    String::from_utf8_lossy(&confdeltype).into_owned(),
                    String::from_utf8_lossy(&confupdtype).into_owned(),
                ));
            }
        }
        let rows: Vec<Vec<Vec<u8>>> = constraints
            .into_iter()
            .map(
                |(
                    name,
                    kind,
                    relation_oid,
                    index_oid,
                    conkey,
                    referenced_relation_oid,
                    confkey,
                    confdeltype,
                    confupdtype,
                )| {
                    columns
                        .iter()
                        .map(|column| match column.as_str() {
                            "oid" => {
                                catalog_oid(&format!("constraint:{name}")).to_string().into_bytes()
                            }
                            "conname" => name.as_bytes().to_vec(),
                            "connamespace" => b"2200".to_vec(),
                            "contype" => kind.as_bytes().to_vec(),
                            "conrelid" => relation_oid.to_string().into_bytes(),
                            "conindid" => index_oid.to_string().into_bytes(),
                            "confrelid" => referenced_relation_oid.to_string().into_bytes(),
                            "conkey" => conkey.as_bytes().to_vec(),
                            "confkey" => confkey.as_bytes().to_vec(),
                            "confdeltype" => confdeltype.as_bytes().to_vec(),
                            "confupdtype" => confupdtype.as_bytes().to_vec(),
                            "convalidated" => b"t".to_vec(),
                            _ => Vec::new(),
                        })
                        .collect()
                },
            )
            .collect();
        return Some(encode_catalog_rows(&columns, rows));
    }

    if upper.contains("PG_CATALOG.PG_INDEX") {
        let filter = sql_literal_after(query, "RELNAME");
        let rows: Vec<Vec<Vec<u8>>> = executor
            .catalog_tables()
            .into_iter()
            .flat_map(|table| {
                let (_, table_name) = catalog_table_parts(&table);
                if filter
                    .as_ref()
                    .is_some_and(|want| want.as_str() != table.as_str() && want != table_name)
                {
                    return Vec::new();
                }
                let relation_oid = catalog_relation_oid(&table);
                executor
                    .catalog_indexes(&table)
                    .into_iter()
                    .map(|index| {
                        let definitions = executor.catalog_columns(&table);
                        let ordinals = if index.columns.is_empty() {
                            index
                                .column
                                .as_deref()
                                .and_then(|column| {
                                    definitions.iter().position(|definition| {
                                        definition.name.eq_ignore_ascii_case(column)
                                    })
                                })
                                .map(|ordinal| vec![ordinal + 1])
                                .unwrap_or_else(|| vec![1])
                        } else {
                            index
                                .columns
                                .iter()
                                .filter_map(|column| {
                                    definitions.iter().position(|definition| {
                                        definition.name.eq_ignore_ascii_case(column)
                                    })
                                })
                                .map(|ordinal| ordinal + 1)
                                .collect::<Vec<_>>()
                        };
                        let index_oid = catalog_index_oid(&index.name);
                        columns
                            .iter()
                            .map(|column| match column.as_str() {
                                "indexrelid" => index_oid.to_string().into_bytes(),
                                "indrelid" => relation_oid.to_string().into_bytes(),
                                "indisunique" => {
                                    if index.unique {
                                        b"t".to_vec()
                                    } else {
                                        b"f".to_vec()
                                    }
                                }
                                "indisprimary" => b"f".to_vec(),
                                "indnatts" | "indnkeyatts" => {
                                    ordinals.len().to_string().into_bytes()
                                }
                                "indkey" => ordinals
                                    .iter()
                                    .map(usize::to_string)
                                    .collect::<Vec<_>>()
                                    .join(" ")
                                    .into_bytes(),
                                _ => Vec::new(),
                            })
                            .collect()
                    })
                    .collect::<Vec<_>>()
            })
            .collect();
        return Some(encode_catalog_rows(&columns, rows));
    }

    let table =
        sql_literal_after(query, "TABLE_NAME").or_else(|| sql_literal_after(query, "RELNAME"))?;
    let definitions = executor.catalog_columns(&table);
    let rows: Vec<Vec<Vec<u8>>> = definitions
        .iter()
        .enumerate()
        .map(|(index, column)| {
            columns
                .iter()
                .map(|field| match field.as_str() {
                    "attrelid" => catalog_relation_oid(&table).to_string().into_bytes(),
                    "column_name" | "attname" => column.name.as_bytes().to_vec(),
                    "data_type" => information_schema_type(&column.data_type).as_bytes().to_vec(),
                    "atttypid" => postgres_type_oid(&column.data_type).to_string().into_bytes(),
                    "is_nullable" => {
                        if column.nullable {
                            b"YES".to_vec()
                        } else {
                            b"NO".to_vec()
                        }
                    }
                    "column_default" => {
                        column.column_default.as_deref().unwrap_or("").as_bytes().to_vec()
                    }
                    "ordinal_position" | "attnum" => (index + 1).to_string().into_bytes(),
                    "attlen" => postgres_type_length(&column.data_type).to_string().into_bytes(),
                    "attnotnull" => {
                        if column.nullable {
                            b"f".to_vec()
                        } else {
                            b"t".to_vec()
                        }
                    }
                    "atthasdef" => {
                        if column.column_default.is_some() {
                            b"t".to_vec()
                        } else {
                            b"f".to_vec()
                        }
                    }
                    "attisdropped" => b"f".to_vec(),
                    "attidentity" | "attgenerated" => Vec::new(),
                    _ => Vec::new(),
                })
                .collect()
        })
        .collect();
    Some(encode_catalog_rows(&columns, rows))
}

fn encode_catalog_rows(columns: &[String], rows: Vec<Vec<Vec<u8>>>) -> Vec<u8> {
    let mut out = multi_row_description(columns);
    for row in rows {
        out.extend(data_row_values(&row));
    }
    out.extend(command_complete("SELECT"));
    out
}

fn split_statements(query: &str) -> Vec<&str> {
    let bytes = query.as_bytes();
    let mut parts = Vec::new();
    let mut start = 0;
    let mut quote: Option<u8> = None;
    let mut dollar_quote: Option<Vec<u8>> = None;
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        if let Some(tag) = dollar_quote.as_ref() {
            if bytes[index..].starts_with(tag) {
                index += tag.len();
                dollar_quote = None;
            } else {
                index += 1;
            }
            continue;
        }
        if let Some(open) = quote {
            if byte == open {
                if index + 1 < bytes.len() && bytes[index + 1] == open {
                    index += 1;
                } else {
                    quote = None;
                }
            }
        } else if byte == b'\'' || byte == b'"' {
            quote = Some(byte);
        } else if byte == b'$' {
            if let Some(end) = dollar_quote_end(bytes, index) {
                dollar_quote = Some(bytes[index..end].to_vec());
                index = end;
                continue;
            }
        } else if byte == b';' {
            parts.push(&query[start..index]);
            start = index + 1;
        } else if byte == b'-' && index + 1 < bytes.len() && bytes[index + 1] == b'-' {
            parts.push(&query[start..index]);
            while index < bytes.len() && bytes[index] != b'\n' {
                index += 1;
            }
            start = index;
            continue;
        }
        index += 1;
    }
    parts.push(&query[start..]);
    parts
}

fn dollar_quote_end(bytes: &[u8], start: usize) -> Option<usize> {
    if bytes.get(start) != Some(&b'$') {
        return None;
    }
    let mut index = start + 1;
    if bytes.get(index) == Some(&b'$') {
        return Some(index + 1);
    }
    if !bytes.get(index).is_some_and(|byte| byte.is_ascii_alphabetic() || *byte == b'_') {
        return None;
    }
    index += 1;
    while bytes.get(index).is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_') {
        index += 1;
    }
    (bytes.get(index) == Some(&b'$')).then_some(index + 1)
}

fn session_default(name: &str) -> Option<&'static str> {
    match name.to_ascii_lowercase().as_str() {
        "server_version" => Some("16.0"),
        "server_version_num" => Some("160000"),
        "server_encoding" => Some("UTF8"),
        "client_encoding" => Some("UTF8"),
        "datestyle" => Some("ISO, MDY"),
        "timezone" => Some("UTC"),
        "time zone" => Some("UTC"),
        "integer_datetimes" => Some("on"),
        "standard_conforming_strings" => Some("on"),
        "transaction_isolation" => Some("serializable"),
        "default_transaction_isolation" => Some("serializable"),
        "application_name" => Some(""),
        "session_authorization" => Some("ryme"),
        "is_superuser" => Some("off"),
        "max_identifier_length" => Some("63"),
        "search_path" => Some("public"),
        _ => None,
    }
}

fn session_isolation(session: &HashMap<String, String>) -> Isolation {
    match session.get("transaction_isolation").map(String::as_str) {
        Some("snapshot") => Isolation::Snapshot,
        _ => Isolation::Serializable,
    }
}

fn normalize_isolation_level(words: &[&str]) -> Option<&'static str> {
    match words.iter().map(|word| word.to_ascii_lowercase()).collect::<Vec<_>>().as_slice() {
        [level] if level == "serializable" => Some("serializable"),
        [level] if level == "snapshot" => Some("snapshot"),
        [first, second] if first == "read" && second == "committed" => Some("snapshot"),
        [first, second] if first == "repeatable" && second == "read" => Some("snapshot"),
        _ => None,
    }
}

fn transaction_control(
    query: &str,
    session: &HashMap<String, String>,
) -> Option<TransactionControl> {
    let normalized = query.trim().trim_end_matches(';').trim();
    let upper = normalized.to_ascii_uppercase();
    let words = normalized.split_whitespace().collect::<Vec<_>>();
    if upper.starts_with("SAVEPOINT ") && words.len() == 2 {
        return normalize_savepoint_name(words[1]).map(TransactionControl::Savepoint);
    }
    if upper.starts_with("RELEASE ") {
        let name = if words.len() == 2 {
            words[1]
        } else if words.len() == 3 && words[1].eq_ignore_ascii_case("SAVEPOINT") {
            words[2]
        } else {
            ""
        };
        if !name.is_empty() {
            return normalize_savepoint_name(name).map(TransactionControl::ReleaseSavepoint);
        }
    }
    if upper.starts_with("ROLLBACK ") {
        let explicit_to = upper.starts_with("ROLLBACK TO ")
            || upper.starts_with("ROLLBACK SAVEPOINT ")
            || upper.starts_with("ROLLBACK TRANSACTION TO ")
            || upper.starts_with("ROLLBACK TRANSACTION SAVEPOINT ");
        if explicit_to {
            return words
                .last()
                .and_then(|name| normalize_savepoint_name(name))
                .map(TransactionControl::RollbackToSavepoint);
        }
    }
    if upper == "BEGIN"
        || upper == "BEGIN TRANSACTION"
        || upper == "START TRANSACTION"
        || upper.starts_with("BEGIN ISOLATION LEVEL ")
        || upper.starts_with("START TRANSACTION ISOLATION LEVEL ")
    {
        let isolation = if upper.contains("ISOLATION LEVEL SNAPSHOT") {
            Isolation::Snapshot
        } else if upper.contains("ISOLATION LEVEL READ COMMITTED") {
            Isolation::Snapshot
        } else {
            session_isolation(session)
        };
        return Some(TransactionControl::Begin(isolation));
    }
    if upper == "COMMIT" || upper == "COMMIT TRANSACTION" || upper == "END" {
        return Some(TransactionControl::Commit);
    }
    if upper == "ROLLBACK" || upper == "ROLLBACK TRANSACTION" {
        return Some(TransactionControl::Rollback);
    }
    None
}

fn normalize_savepoint_name(value: &str) -> Option<String> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    let name = unquote_literal(value);
    if name.is_empty() {
        None
    } else if value.starts_with('"') && value.ends_with('"') {
        Some(name)
    } else {
        Some(name.to_ascii_lowercase())
    }
}

async fn handle_transaction_control<B>(
    control: TransactionControl,
    executor: &Arc<Executor<B>>,
    active: &mut Option<SessionTransaction>,
) -> Vec<u8>
where
    B: TxnBackend,
{
    match control {
        TransactionControl::Begin(isolation) => {
            if active.is_some() {
                encode_error_code("25001", String::from("transaction already in progress"))
            } else {
                *active = Some(SessionTransaction {
                    txn: executor.begin_transaction(isolation),
                    changes: Vec::new(),
                    failed: false,
                    savepoints: Vec::new(),
                });
                command_complete("BEGIN")
            }
        }
        TransactionControl::Commit => {
            let Some(state) = active.take() else {
                return command_complete("COMMIT");
            };
            if state.failed {
                return encode_error_code(
                    "25P02",
                    String::from("current transaction is aborted, commands ignored until end of transaction block"),
                );
            }
            match executor.commit_transaction(state.txn, state.changes).await {
                Ok(_) => command_complete("COMMIT"),
                Err(error) => encode_error_code(error_code(&error), error.to_string()),
            }
        }
        TransactionControl::Rollback => {
            active.take();
            command_complete("ROLLBACK")
        }
        TransactionControl::Savepoint(name) => {
            let Some(state) = active.as_mut() else {
                return encode_error_code(
                    "25P01",
                    String::from("SAVEPOINT can only be used in transaction blocks"),
                );
            };
            if state.failed {
                return encode_error_code(
                    "25P02",
                    String::from(
                        "current transaction is aborted, commands ignored until end of transaction block",
                    ),
                );
            }
            if let Some(position) =
                state.savepoints.iter().position(|savepoint| savepoint.name == name)
            {
                state.savepoints.truncate(position);
            }
            state.savepoints.push(Savepoint {
                name,
                checkpoint: state.txn.checkpoint(),
                changes_len: state.changes.len(),
            });
            command_complete("SAVEPOINT")
        }
        TransactionControl::ReleaseSavepoint(name) => {
            let Some(state) = active.as_mut() else {
                return encode_error_code(
                    "25P01",
                    String::from("RELEASE SAVEPOINT can only be used in transaction blocks"),
                );
            };
            if state.failed {
                return encode_error_code(
                    "25P02",
                    String::from(
                        "current transaction is aborted, commands ignored until end of transaction block",
                    ),
                );
            }
            let Some(position) =
                state.savepoints.iter().rposition(|savepoint| savepoint.name == name)
            else {
                return encode_error_code("3B001", format!("savepoint \"{name}\" does not exist"));
            };
            state.savepoints.truncate(position);
            command_complete("RELEASE")
        }
        TransactionControl::RollbackToSavepoint(name) => {
            let Some(state) = active.as_mut() else {
                return encode_error_code(
                    "25P01",
                    String::from("ROLLBACK TO SAVEPOINT can only be used in transaction blocks"),
                );
            };
            let Some(position) =
                state.savepoints.iter().rposition(|savepoint| savepoint.name == name)
            else {
                return encode_error_code("3B001", format!("savepoint \"{name}\" does not exist"));
            };
            let savepoint = &state.savepoints[position];
            state.txn.restore_checkpoint(&savepoint.checkpoint);
            state.changes.truncate(savepoint.changes_len);
            state.savepoints.truncate(position + 1);
            state.failed = false;
            command_complete("ROLLBACK")
        }
    }
}

async fn execute_statement_for_session<B>(
    executor: &Arc<Executor<B>>,
    limits: &ConnLimits,
    statement: Statement,
    bytes: u64,
    session: &HashMap<String, String>,
    active: &mut Option<SessionTransaction>,
) -> Vec<u8>
where
    B: TxnBackend,
{
    if active.as_ref().is_some_and(|transaction| transaction.failed) {
        return encode_error_code(
            "25P02",
            String::from(
                "current transaction is aborted, commands ignored until end of transaction block",
            ),
        );
    }
    if let Some(denied) = admit_statement(limits, &statement, bytes) {
        if let Some(transaction) = active.as_mut() {
            transaction.failed = true;
        }
        return denied;
    }
    let write = statement.is_write();
    let table = statement.table().to_string();
    let range_keys =
        if limits.range_hook.is_armed() { statement_range_keys(&statement) } else { None };
    let fingerprint = limits.slow_log.as_ref().map(|_| query_fingerprint(&table));
    let start = std::time::Instant::now();
    if let Some(transaction) = active.as_mut() {
        match executor.execute_in_transaction(&mut transaction.txn, statement).await {
            Ok((result, changes)) => {
                if write {
                    note_range_changes(
                        &limits.range_hook,
                        &table,
                        &changes,
                        changes.len().max(1) as u64,
                    );
                }
                transaction.changes.extend(changes);
                observe_statement(limits, write);
                record_timing(limits, fingerprint, table, start.elapsed().as_micros() as u64);
                encode_result(result)
            }
            Err(error) => {
                transaction.failed = true;
                encode_error_code(error_code(&error), error.to_string())
            }
        }
    } else {
        match executor.execute_with(statement, session_isolation(session)).await {
            Ok(result) => {
                observe_statement(limits, write);
                if write {
                    note_statement_range(&limits.range_hook, &table, range_keys.as_deref(), 1);
                }
                record_timing(limits, fingerprint, table, start.elapsed().as_micros() as u64);
                encode_result(result)
            }
            Err(error) => encode_error_code(error_code(&error), error.to_string()),
        }
    }
}

async fn execute_copy_to_for_session<B>(
    executor: &Arc<Executor<B>>,
    statement: Statement,
    session: &HashMap<String, String>,
    active: &mut Option<SessionTransaction>,
) -> Result<QueryResult>
where
    B: TxnBackend,
{
    if let Some(transaction) = active.as_mut() {
        if transaction.failed {
            return Err(RymeError::InvalidArgument(String::from("current transaction is aborted")));
        }
        match executor.execute_in_transaction(&mut transaction.txn, statement).await {
            Ok((result, changes)) => {
                transaction.changes.extend(changes);
                Ok(result)
            }
            Err(error) => {
                transaction.failed = true;
                Err(error)
            }
        }
    } else {
        executor.execute_with(statement, session_isolation(session)).await
    }
}

async fn prepared_command<B>(
    query: &str,
    statements: &mut HashMap<String, String>,
    prepared_cache: &mut HashMap<String, (String, Statement)>,
    executor: &Arc<Executor<B>>,
    session: &HashMap<String, String>,
    limits: &ConnLimits,
    active: &mut Option<SessionTransaction>,
) -> Option<Vec<u8>>
where
    B: TxnBackend,
{
    if let Some(control) = transaction_control(query, session) {
        return Some(handle_transaction_control(control, executor, active).await);
    }
    let head = query.split_whitespace().next().unwrap_or("").to_ascii_uppercase();
    match head.as_str() {
        "PREPARE" => {
            let rest = query[7..].trim();
            let mut words = rest.split_whitespace();
            let name = words.next().unwrap_or("").to_string();
            let keyword = words.next().unwrap_or("");
            if name.is_empty() || !keyword.eq_ignore_ascii_case("AS") || name.contains('(') {
                return None;
            }
            let body_at = rest.to_ascii_uppercase().find(" AS ").map(|at| at + 4)?;
            let body = rest[body_at..].trim();
            if body.is_empty() || (parse(body).is_err() && session_select(body, session).is_none())
            {
                return Some(encode_error_code("42601", format!("unsupported statement: {query}")));
            }
            prepared_cache.remove(&name);
            statements.insert(name, body.to_string());
            Some(command_complete("PREPARE"))
        }
        "EXECUTE" => {
            let rest = query[7..].trim();
            let (name, params) = match rest.find('(') {
                Some(open) => {
                    let params = rest[open + 1..].trim_end_matches([')', ';', ' ']);
                    (rest[..open].trim(), params)
                }
                None => (rest.trim_end_matches([';', ' ']), ""),
            };
            let stored = match statements.get(name) {
                Some(stored) => stored.clone(),
                None => {
                    return Some(encode_error_code(
                        "26000",
                        format!("prepared statement \"{name}\" does not exist"),
                    ));
                }
            };
            let mut values = Vec::new();
            if !params.trim().is_empty() {
                let mut current = String::new();
                let mut quoted = false;
                for ch in params.chars().chain(std::iter::once(',')) {
                    if ch == '\'' {
                        quoted = !quoted;
                        current.push(ch);
                    } else if ch == ',' && !quoted {
                        values.push(execute_parameter(current.trim()));
                        current = String::new();
                    } else {
                        current.push(ch);
                    }
                }
            }
            let bound = bind(&stored, &values);
            let trimmed = bound.trim_matches(|c| c == '\0' || c == ';' || c == ' ');
            let cached = prepared_cache
                .get(name)
                .filter(|(text, _)| text.as_str() == trimmed)
                .map(|(_, statement)| statement.clone());
            let parsed = match cached {
                Some(statement) => Ok(statement),
                None => match parse(trimmed) {
                    Ok(statement) => {
                        prepared_cache
                            .insert(name.to_string(), (trimmed.to_string(), statement.clone()));
                        Ok(statement)
                    }
                    Err(e) => Err(e),
                },
            };
            match parsed {
                Ok(statement) => Some(
                    execute_statement_for_session(
                        executor,
                        limits,
                        statement,
                        bound.len() as u64,
                        session,
                        active,
                    )
                    .await,
                ),
                Err(_) => session_select(trimmed, session).map(encode_session_rows).or_else(|| {
                    Some(encode_error_code("42601", format!("unsupported statement: {query}")))
                }),
            }
        }
        "DEALLOCATE" => {
            let rest = query[10..].trim().trim_end_matches([';', ' ']);
            if rest.eq_ignore_ascii_case("ALL") {
                statements.clear();
                prepared_cache.clear();
                return Some(command_complete("DEALLOCATE"));
            }
            if statements.remove(rest).is_none() {
                return Some(encode_error_code(
                    "26000",
                    format!("prepared statement \"{rest}\" does not exist"),
                ));
            }
            prepared_cache.remove(rest);
            Some(command_complete("DEALLOCATE"))
        }
        _ => None,
    }
}

fn session_command(query: &str, session: &mut HashMap<String, String>) -> Option<Vec<u8>> {
    let head = query.split_whitespace().next().unwrap_or("").to_ascii_uppercase();
    match head.as_str() {
        "SET" => {
            let rest = query[3..].trim().trim_end_matches(';').trim();
            let words = rest.split_whitespace().collect::<Vec<_>>();
            if words.first().is_some_and(|w| w.eq_ignore_ascii_case("SESSION"))
                && !words.get(1).is_some_and(|w| w.eq_ignore_ascii_case("CHARACTERISTICS"))
            {
                return session_command(
                    &format!("SET {}", rest["SESSION".len()..].trim()),
                    session,
                );
            }
            let isolation_words = if words.len() >= 4
                && words[0].eq_ignore_ascii_case("TRANSACTION")
                && words[1].eq_ignore_ascii_case("ISOLATION")
                && words[2].eq_ignore_ascii_case("LEVEL")
            {
                Some(&words[3..])
            } else if words.len() >= 7
                && words[0].eq_ignore_ascii_case("SESSION")
                && words[1].eq_ignore_ascii_case("CHARACTERISTICS")
                && words[2].eq_ignore_ascii_case("AS")
                && words[3].eq_ignore_ascii_case("TRANSACTION")
                && words[4].eq_ignore_ascii_case("ISOLATION")
                && words[5].eq_ignore_ascii_case("LEVEL")
            {
                Some(&words[6..])
            } else {
                None
            };
            if let Some(isolation_words) = isolation_words {
                let normalized = normalize_isolation_level(isolation_words)?;
                session.insert(String::from("transaction_isolation"), String::from(normalized));
                return Some(command_complete("SET"));
            }
            if words.len() >= 3
                && words[0].eq_ignore_ascii_case("TIME")
                && words[1].eq_ignore_ascii_case("ZONE")
            {
                let value = words[2..].join(" ");
                if value.is_empty() {
                    return None;
                }
                session.insert(
                    String::from("timezone"),
                    unquote_literal(value.trim_start_matches('=').trim()),
                );
                return Some(command_complete("SET"));
            }
            if words.len() >= 2 && words[0].eq_ignore_ascii_case("NAMES") {
                let value = words[1..].join(" ");
                if value.is_empty() {
                    return None;
                }
                session.insert(
                    String::from("client_encoding"),
                    unquote_literal(value.trim_start_matches('=').trim()),
                );
                return Some(command_complete("SET"));
            }
            let (name, value) = rest
                .split_once('=')
                .map(|(name, value)| (name.trim(), value.trim()))
                .or_else(|| {
                    let mut parts = rest.splitn(3, char::is_whitespace);
                    let name = parts.next()?.trim();
                    let keyword = parts.next()?.trim();
                    let value = parts.next()?.trim();
                    if keyword.eq_ignore_ascii_case("TO") {
                        Some((name, value))
                    } else {
                        None
                    }
                })?;
            if name.is_empty() || value.is_empty() {
                return None;
            }
            session.insert(name.to_ascii_lowercase(), unquote_literal(value));
            Some(command_complete("SET"))
        }
        "SHOW" => {
            let name = query[4..].trim().trim_end_matches(';').trim();
            if name.is_empty() {
                return None;
            }
            if name.eq_ignore_ascii_case("ALL") {
                let names = [
                    "application_name",
                    "client_encoding",
                    "datestyle",
                    "integer_datetimes",
                    "search_path",
                    "server_encoding",
                    "server_version",
                    "server_version_num",
                    "standard_conforming_strings",
                    "default_transaction_isolation",
                    "session_authorization",
                    "is_superuser",
                    "max_identifier_length",
                    "timezone",
                    "transaction_isolation",
                ];
                let mut out =
                    multi_row_description(&[String::from("name"), String::from("setting")]);
                for name in names {
                    let value = session
                        .get(name)
                        .cloned()
                        .or_else(|| session_default(name).map(String::from))
                        .unwrap_or_default();
                    out.extend(data_row_values(&[name.as_bytes().to_vec(), value.into_bytes()]));
                }
                out.extend(command_complete("SHOW"));
                return Some(out);
            }
            let value = session
                .get(&name.to_ascii_lowercase())
                .cloned()
                .or_else(|| session_default(name).map(String::from))?;
            Some(session_select_rows(vec![(name.to_string(), value)]))
        }
        "RESET" => {
            let name = query[5..].trim().trim_end_matches(';').trim();
            if name.eq_ignore_ascii_case("ALL") {
                session.clear();
                return Some(command_complete("RESET"));
            }
            if name.is_empty() {
                return None;
            }
            session.remove(&name.to_ascii_lowercase());
            Some(command_complete("RESET"))
        }
        _ => None,
    }
}

fn unquote_literal(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.len() >= 2
        && ((trimmed.starts_with('\'') && trimmed.ends_with('\''))
            || (trimmed.starts_with('"') && trimmed.ends_with('"')))
    {
        return trimmed[1..trimmed.len() - 1].to_string();
    }
    trimmed.to_string()
}

fn execute_parameter(value: &str) -> String {
    if value.eq_ignore_ascii_case("NULL") {
        String::from("\0")
    } else {
        unquote_literal(value)
    }
}

fn split_select_list(input: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut depth: u32 = 0;
    let mut quoted: Option<char> = None;
    let mut current = String::new();
    for ch in input.chars() {
        if let Some(quote) = quoted {
            current.push(ch);
            if ch == quote {
                quoted = None;
            }
            continue;
        }
        match ch {
            '\'' | '"' => {
                quoted = Some(ch);
                current.push(ch);
            }
            '(' => {
                depth += 1;
                current.push(ch);
            }
            ')' => {
                depth = depth.saturating_sub(1);
                current.push(ch);
            }
            ',' if depth == 0 => {
                out.push(current.trim().to_string());
                current.clear();
            }
            _ => current.push(ch),
        }
    }
    if !current.trim().is_empty() {
        out.push(current.trim().to_string());
    }
    out
}

fn strip_alias(item: &str) -> &str {
    let trimmed = item.trim();
    let upper = trimmed.to_ascii_uppercase();
    if let Some(position) = upper.rfind(" AS ") {
        return trimmed[..position].trim();
    }
    let bytes = trimmed.as_bytes();
    let mut depth: u32 = 0;
    let mut quoted: Option<u8> = None;
    let mut index = bytes.len();
    while index > 0 {
        index -= 1;
        let byte = bytes[index];
        if let Some(quote) = quoted {
            if byte == quote {
                quoted = None;
            }
            continue;
        }
        match byte {
            b'\'' | b'"' => quoted = Some(byte),
            b')' => depth += 1,
            b'(' => depth = depth.saturating_sub(1),
            b' ' | b'\t' if depth == 0 => return trimmed[..index].trim(),
            _ => {}
        }
    }
    trimmed
}

fn column_name(item: &str) -> String {
    let trimmed = item.trim();
    let upper = trimmed.to_ascii_uppercase();
    if let Some(position) = upper.rfind(" AS ") {
        return unquote_literal(trimmed[position + 4..].trim());
    }
    strip_alias(item).to_string()
}

fn strip_casts(expression: &str) -> &str {
    let mut current = expression.trim();
    loop {
        let Some(position) = current.rfind("::") else {
            return current;
        };
        let suffix = current[position + 2..].trim();
        if suffix.is_empty()
            || !suffix.chars().all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == ' ')
        {
            return current;
        }
        current = current[..position].trim();
    }
}

fn eval_select_item(item: &str, session: &HashMap<String, String>) -> Option<String> {
    let expression = strip_casts(strip_alias(item));
    if expression.starts_with('\'') && expression.ends_with('\'') && expression.len() >= 2 {
        return Some(expression[1..expression.len() - 1].to_string());
    }
    if expression.parse::<i64>().is_ok() {
        return Some(expression.to_string());
    }
    match expression.to_ascii_lowercase().as_str() {
        "version()" | "pg_catalog.version()" => Some(String::from("PostgreSQL 16.0 on rymeDB")),
        "current_database()" => Some(String::from("default")),
        "current_user" | "current_user()" => Some(String::from("ryme")),
        "current_schema()" => Some(String::from("public")),
        "current_catalog" => Some(String::from("default")),
        expression if expression.starts_with("current_setting(") && expression.ends_with(')') => {
            let setting = expression[16..expression.len() - 1].trim();
            if setting.contains(',') {
                return None;
            }
            let name = unquote_literal(setting).to_ascii_lowercase();
            session.get(&name).cloned().or_else(|| session_default(&name).map(String::from))
        }
        _ => None,
    }
}

fn session_select(query: &str, session: &HashMap<String, String>) -> Option<Vec<(String, String)>> {
    let trimmed = query.trim();
    if !trimmed.to_ascii_uppercase().starts_with("SELECT ") {
        return None;
    }
    let mut list = trimmed[6..].trim().to_string();
    for suffix in [" FOR UPDATE", " FOR SHARE", " LIMIT 1", " OFFSET 0"] {
        if list.to_ascii_uppercase().ends_with(suffix) {
            list.truncate(list.len() - suffix.len());
        }
    }
    if list.to_ascii_uppercase().contains(" FROM ") {
        return None;
    }
    let items = split_select_list(&list);
    if items.is_empty() {
        return None;
    }
    let mut out = Vec::new();
    for item in items {
        out.push((column_name(&item), eval_select_item(&item, session)?));
    }
    Some(out)
}

fn session_select_rows(columns: Vec<(String, String)>) -> Vec<u8> {
    let names: Vec<String> = columns.iter().map(|(name, _)| name.clone()).collect();
    let values: Vec<String> = columns.into_iter().map(|(_, value)| value).collect();
    let mut out = multi_row_description(&names);
    out.extend(session_data_row(&values));
    out.extend(command_complete("SELECT 1"));
    out
}

fn encode_session_rows(columns: Vec<(String, String)>) -> Vec<u8> {
    session_select_rows(columns)
}

fn multi_row_description(names: &[String]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&(names.len() as u16).to_be_bytes());
    for name in names {
        body.extend_from_slice(name.as_bytes());
        body.push(0);
        body.extend_from_slice(&0u32.to_be_bytes());
        body.extend_from_slice(&0u16.to_be_bytes());
        body.extend_from_slice(&25u32.to_be_bytes());
        body.extend_from_slice(&(-1i16).to_be_bytes());
        body.extend_from_slice(&(-1i32).to_be_bytes());
        body.extend_from_slice(&0i16.to_be_bytes());
    }
    frame(b'T', &body)
}

fn returning_description(fields: &[ReturningField]) -> Vec<u8> {
    let names: Vec<String> = fields
        .iter()
        .map(|field| match field {
            ReturningField::Key => String::from("id"),
            ReturningField::Value => String::from("value"),
            ReturningField::Column(column) => column.clone(),
        })
        .collect();
    multi_row_description(&names)
}

fn session_data_row(values: &[String]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&(values.len() as u16).to_be_bytes());
    for value in values {
        body.extend_from_slice(&(value.len() as u32).to_be_bytes());
        body.extend_from_slice(value.as_bytes());
    }
    frame(b'D', &body)
}

fn read_cstring(payload: &[u8], offset: &mut usize) -> Option<String> {
    let end = payload[*offset..].iter().position(|b| *b == 0)?;
    let value = String::from_utf8_lossy(&payload[*offset..*offset + end]).into_owned();
    *offset = *offset + end + 1;
    Some(value)
}

fn read_i16(payload: &[u8], offset: &mut usize) -> Option<i16> {
    if payload.len() < *offset + 2 {
        return None;
    }
    let value = i16::from_be_bytes([payload[*offset], payload[*offset + 1]]);
    *offset += 2;
    Some(value)
}

fn read_i32(payload: &[u8], offset: &mut usize) -> Option<i32> {
    if payload.len() < *offset + 4 {
        return None;
    }
    let value = i32::from_be_bytes([
        payload[*offset],
        payload[*offset + 1],
        payload[*offset + 2],
        payload[*offset + 3],
    ]);
    *offset += 4;
    Some(value)
}

fn parse_prepare(payload: &[u8]) -> std::result::Result<(String, String), RymeError> {
    let mut offset = 0;
    let name = read_cstring(payload, &mut offset)
        .ok_or_else(|| RymeError::InvalidArgument(String::from("parse name")))?;
    let query = read_cstring(payload, &mut offset)
        .ok_or_else(|| RymeError::InvalidArgument(String::from("parse query")))?;
    Ok((name, query))
}

fn parse_bind(payload: &[u8]) -> std::result::Result<(String, String, Vec<String>), RymeError> {
    let mut offset = 0;
    let portal = read_cstring(payload, &mut offset)
        .ok_or_else(|| RymeError::InvalidArgument(String::from("bind portal")))?;
    let statement = read_cstring(payload, &mut offset)
        .ok_or_else(|| RymeError::InvalidArgument(String::from("bind statement")))?;
    let formats = read_i16(payload, &mut offset)
        .ok_or_else(|| RymeError::InvalidArgument(String::from("bind formats")))?;
    for _ in 0..formats.max(0) {
        read_i16(payload, &mut offset)
            .ok_or_else(|| RymeError::InvalidArgument(String::from("bind format")))?;
    }
    let count = read_i16(payload, &mut offset)
        .ok_or_else(|| RymeError::InvalidArgument(String::from("bind count")))?;
    let mut params = Vec::new();
    for _ in 0..count.max(0) {
        let length = read_i32(payload, &mut offset)
            .ok_or_else(|| RymeError::InvalidArgument(String::from("bind length")))?;
        if length < 0 {
            params.push(String::from("\0"));
            continue;
        }
        let length = length as usize;
        if payload.len() < offset + length {
            return Err(RymeError::InvalidArgument(String::from("bind value")));
        }
        params.push(String::from_utf8_lossy(&payload[offset..offset + length]).into_owned());
        offset += length;
    }
    Ok((portal, statement, params))
}

fn encode_result(result: QueryResult) -> Vec<u8> {
    match result {
        QueryResult::Ok => command_complete("OK"),
        QueryResult::Row { pk: _, value } => {
            let text = String::from_utf8_lossy(&value).into_owned();
            let mut out = row_description();
            out.extend(data_row(text));
            out.extend(command_complete("SELECT 1"));
            out
        }
        QueryResult::Scalar { label, value } => {
            let mut out = multi_row_description(&[label]);
            out.extend(data_row_values(&[value]));
            out.extend(command_complete("SELECT 1"));
            out
        }
        QueryResult::Rows { rows } => {
            let mut out = row_description();
            for (_, value) in rows {
                let text = String::from_utf8_lossy(&value).into_owned();
                out.extend(data_row(text));
            }
            out.extend(command_complete("SELECT"));
            out
        }
        QueryResult::Table { columns, rows } => {
            let mut out = multi_row_description(&columns);
            for row in rows {
                out.extend(data_row_values(&row));
            }
            out.extend(command_complete("SELECT"));
            out
        }
        QueryResult::Returning { columns, rows } => {
            let mut out = multi_row_description(&columns);
            for row in rows {
                out.extend(data_row_values(&row));
            }
            out.extend(command_complete("SELECT"));
            out
        }
    }
}

fn copy_in_response() -> Vec<u8> {
    let mut body = Vec::with_capacity(9);
    body.push(0); // text format
    body.extend_from_slice(&2u16.to_be_bytes());
    body.extend_from_slice(&0i16.to_be_bytes());
    body.extend_from_slice(&0i16.to_be_bytes());
    frame(b'G', &body)
}

fn copy_out_response(columns: &[String]) -> Vec<u8> {
    let mut body = Vec::with_capacity(3 + columns.len() * 2);
    body.push(0); // text format
    body.extend_from_slice(&(columns.len() as u16).to_be_bytes());
    for _ in columns {
        body.extend_from_slice(&0i16.to_be_bytes());
    }
    frame(b'H', &body)
}

fn copy_data(data: &[u8]) -> Vec<u8> {
    frame(b'd', data)
}

fn copy_done() -> Vec<u8> {
    frame(b'c', b"")
}

fn encode_copy_text_row(row: &[Vec<u8>]) -> Vec<u8> {
    let mut encoded = Vec::new();
    for (index, field) in row.iter().enumerate() {
        if index > 0 {
            encoded.push(b'\t');
        }
        if field == SQL_NULL_SENTINEL {
            encoded.extend_from_slice(b"\\N");
            continue;
        }
        for byte in field {
            match byte {
                b'\\' => encoded.extend_from_slice(b"\\\\"),
                b'\t' => encoded.extend_from_slice(b"\\t"),
                b'\n' => encoded.extend_from_slice(b"\\n"),
                b'\r' => encoded.extend_from_slice(b"\\r"),
                byte => encoded.push(*byte),
            }
        }
    }
    encoded.push(b'\n');
    encoded
}

fn copy_complete(rows: usize) -> Vec<u8> {
    command_complete(&format!("COPY {rows}"))
}

fn copy_fail_message(payload: &[u8]) -> String {
    let message = payload.split(|byte| *byte == 0).next().unwrap_or(payload);
    let message = String::from_utf8_lossy(message).trim().to_string();
    if message.is_empty() {
        String::from("COPY failed")
    } else {
        message
    }
}

fn decode_copy_rows(
    data: &[u8],
    requested_columns: &[String],
    table_columns: &[String],
) -> std::result::Result<DecodedCopy, String> {
    const MAX_COPY_ROWS: usize = 10_000;
    let columns = if requested_columns.is_empty() {
        table_columns.to_vec()
    } else {
        requested_columns.to_vec()
    };
    if !columns.is_empty() {
        let mut rows = Vec::new();
        for raw_line in data.split(|byte| *byte == b'\n') {
            let line = raw_line.strip_suffix(&[b'\r'][..]).unwrap_or(raw_line);
            if line.is_empty() || line == b"\\." {
                continue;
            }
            if rows.len() >= MAX_COPY_ROWS {
                return Err(String::from("COPY exceeds the 10000 row limit"));
            }
            let fields = line.split(|byte| *byte == b'\t').collect::<Vec<_>>();
            if fields.len() != columns.len() {
                return Err(format!(
                    "COPY row has {} fields but table expects {}",
                    fields.len(),
                    columns.len()
                ));
            }
            let mut values = Vec::with_capacity(fields.len());
            for field in fields {
                let value = decode_copy_field(field)?;
                if value.as_ref().is_some_and(|value| value.len() > 4 * 1024 * 1024) {
                    return Err(String::from("COPY field exceeds the 4 MiB limit"));
                }
                values.push(value.map_or(InsertValue::Null, InsertValue::Value));
            }
            rows.push(values);
        }
        return Ok(DecodedCopy::Columns { columns, rows });
    }

    let mut rows = Vec::new();
    for raw_line in data.split(|byte| *byte == b'\n') {
        let line = raw_line.strip_suffix(&[b'\r'][..]).unwrap_or(raw_line);
        if line.is_empty() || line == b"\\." {
            continue;
        }
        if rows.len() >= MAX_COPY_ROWS {
            return Err(String::from("COPY exceeds the 10000 row limit"));
        }
        let Some(separator) = line.iter().position(|byte| *byte == b'\t') else {
            return Err(String::from("COPY text rows require key and value columns"));
        };
        if line[separator + 1..].contains(&b'\t') {
            return Err(String::from("COPY rows must contain exactly two columns"));
        }
        let key = decode_copy_field(&line[..separator])?
            .ok_or_else(|| String::from("COPY key cannot be NULL"))?;
        let value = decode_copy_field(&line[separator + 1..])?.unwrap_or_default();
        if key.is_empty() {
            return Err(String::from("COPY key cannot be empty"));
        }
        if key.len() > 1024 {
            return Err(String::from("COPY key exceeds the 1024 byte limit"));
        }
        if value.len() > 4 * 1024 * 1024 {
            return Err(String::from("COPY value exceeds the 4 MiB limit"));
        }
        rows.push((key, value));
    }
    Ok(DecodedCopy::KeyValue(rows))
}

fn decode_copy_field(field: &[u8]) -> std::result::Result<Option<Vec<u8>>, String> {
    if field == b"\\N" {
        return Ok(None);
    }
    let mut decoded = Vec::with_capacity(field.len());
    let mut index = 0;
    while index < field.len() {
        if field[index] != b'\\' {
            decoded.push(field[index]);
            index += 1;
            continue;
        }
        index += 1;
        let Some(escaped) = field.get(index).copied() else {
            return Err(String::from("COPY field ends with an incomplete escape"));
        };
        decoded.push(match escaped {
            b't' => b'\t',
            b'n' => b'\n',
            b'r' => b'\r',
            b'\\' => b'\\',
            other => other,
        });
        index += 1;
    }
    Ok(Some(decoded))
}

fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + body.len());
    out.push(tag);
    out.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
    out.extend_from_slice(body);
    out
}

fn row_description() -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&1u16.to_be_bytes());
    body.extend_from_slice(b"value\0");
    body.extend_from_slice(&0u32.to_be_bytes());
    body.extend_from_slice(&0u16.to_be_bytes());
    body.extend_from_slice(&25u32.to_be_bytes());
    body.extend_from_slice(&(-1i16).to_be_bytes());
    body.extend_from_slice(&(-1i32).to_be_bytes());
    body.extend_from_slice(&0i16.to_be_bytes());
    frame(b'T', &body)
}

fn data_row(text: String) -> Vec<u8> {
    data_row_values(&[text.into_bytes()])
}

fn data_row_values(values: &[Vec<u8>]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&(values.len() as u16).to_be_bytes());
    for value in values {
        if value.as_slice() == SQL_NULL_SENTINEL {
            body.extend_from_slice(&(-1i32).to_be_bytes());
            continue;
        }
        body.extend_from_slice(&(value.len() as u32).to_be_bytes());
        body.extend_from_slice(value);
    }
    frame(b'D', &body)
}

fn command_complete(tag: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(tag.as_bytes());
    body.push(0);
    frame(b'C', &body)
}

fn encode_error(message: String) -> Vec<u8> {
    encode_error_code("XX000", message)
}

fn encode_error_code(code: &str, message: String) -> Vec<u8> {
    let mut body = Vec::new();
    body.push(b'S');
    body.extend_from_slice(b"ERROR\0");
    body.push(b'V');
    body.extend_from_slice(b"ERROR\0");
    body.push(b'C');
    body.extend_from_slice(code.as_bytes());
    body.push(0);
    body.push(b'M');
    body.extend_from_slice(message.as_bytes());
    body.push(0);
    body.push(0);
    frame(b'E', &body)
}

fn error_code(error: &RymeError) -> &'static str {
    match error {
        RymeError::InvalidArgument(_) => "42601",
        RymeError::NotFound(_) => "02000",
        RymeError::Conflict(_) => "40001",
        RymeError::Unauthorized => "28000",
        RymeError::Forbidden => "42501",
        RymeError::Overload(_) => "53200",
        RymeError::Timeout => "57014",
        RymeError::ReadOnly(_) => "25006",
        _ => "XX000",
    }
}

fn parameter_status(name: &str, value: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(name.as_bytes());
    body.push(0);
    body.extend_from_slice(value.as_bytes());
    body.push(0);
    frame(b'S', &body)
}

async fn send_greeting<S>(socket: &mut S) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    for (name, value) in [
        ("server_version", "16.0"),
        ("server_encoding", "UTF8"),
        ("client_encoding", "UTF8"),
        ("DateStyle", "ISO, MDY"),
        ("TimeZone", "UTC"),
        ("integer_datetimes", "on"),
        ("standard_conforming_strings", "on"),
        ("application_name", ""),
        ("transaction_isolation", "serializable"),
    ] {
        let packet = parameter_status(name, value);
        socket.write_all(&packet).await.map_err(|e| RymeError::Io(e.to_string()))?;
    }
    Ok(())
}

async fn send_auth_ok<S>(socket: &mut S) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut body = Vec::new();
    body.extend_from_slice(&0u32.to_be_bytes());
    let packet = frame(b'R', &body);
    socket.write_all(&packet).await.map_err(|e| RymeError::Io(e.to_string()))?;
    Ok(())
}

async fn send_auth_cleartext_password<S>(socket: &mut S) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let packet = frame(b'R', &3u32.to_be_bytes());
    socket.write_all(&packet).await.map_err(|e| RymeError::Io(e.to_string()))?;
    Ok(())
}

async fn send_backend_key_data<S>(socket: &mut S, pid: u32, secret: u32) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut body = Vec::with_capacity(8);
    body.extend_from_slice(&pid.to_be_bytes());
    body.extend_from_slice(&secret.to_be_bytes());
    socket.write_all(&frame(b'K', &body)).await.map_err(|e| RymeError::Io(e.to_string()))?;
    Ok(())
}

async fn send_ready<S>(socket: &mut S) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let packet = frame(b'Z', b"I");
    socket.write_all(&packet).await.map_err(|e| RymeError::Io(e.to_string()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ryme_sql::{
        DefaultPrivilegeGrant, FunctionDefinition, PrivilegeGrant, RlsPolicy, RoleDefinition,
        SchemaSnapshot, SequenceDefinition, TriggerDefinition, ViewDefinition,
    };

    #[test]
    fn split_statements_respects_quotes() {
        assert_eq!(split_statements("SELECT 1"), vec!["SELECT 1"]);
        assert_eq!(split_statements("SELECT 1; SELECT 2;"), vec!["SELECT 1", " SELECT 2", ""]);
        assert_eq!(
            split_statements("INSERT INTO t KEY 'a;b' VALUE 'c'; SELECT 1"),
            vec!["INSERT INTO t KEY 'a;b' VALUE 'c'", " SELECT 1"]
        );
        assert_eq!(
            split_statements("SELECT 'it''s'; SELECT 2"),
            vec!["SELECT 'it''s'", " SELECT 2"]
        );
        assert_eq!(
            split_statements("-- leading\nSELECT 1; -- trailing"),
            vec!["", "\nSELECT 1", " ", ""]
        );
        assert_eq!(
            split_statements("CREATE FUNCTION f() RETURNS trigger AS $$ BEGIN PERFORM 1; RETURN NEW; END; $$ LANGUAGE plpgsql; SELECT 1"),
            vec!["CREATE FUNCTION f() RETURNS trigger AS $$ BEGIN PERFORM 1; RETURN NEW; END; $$ LANGUAGE plpgsql", " SELECT 1"]
        );
        assert_eq!(
            split_statements("SELECT $tag$inside; body$tag$; SELECT 2"),
            vec!["SELECT $tag$inside; body$tag$", " SELECT 2"]
        );
    }

    #[test]
    fn set_show_reset_roundtrip() {
        let mut session = HashMap::new();
        assert!(session_command("SET application_name TO 'x'", &mut session).is_some());
        assert_eq!(session.get("application_name"), Some(&String::from("x")));
        assert!(session_command("SET TimeZone = UTC", &mut session).is_some());
        assert!(session_command("SHOW server_version", &mut session).is_some());
        assert!(session_command("SHOW application_name", &mut session).is_some());
        assert!(session_command("RESET application_name", &mut session).is_some());
        assert!(!session.contains_key("application_name"));
        assert!(session_command("RESET ALL", &mut session).is_some());
        assert!(session.is_empty());
        assert!(session_command("SET", &mut session).is_none());
        assert!(session_command("SHOW", &mut session).is_none());
    }

    #[test]
    fn set_transaction_isolation() {
        let mut session = HashMap::new();
        assert_eq!(session_isolation(&session), Isolation::Serializable);
        assert!(session_command("SET TRANSACTION ISOLATION LEVEL SNAPSHOT", &mut session).is_some());
        assert_eq!(session.get("transaction_isolation"), Some(&String::from("snapshot")));
        assert_eq!(session_isolation(&session), Isolation::Snapshot);
        assert!(
            session_command("set transaction isolation level serializable", &mut session).is_some()
        );
        assert_eq!(session_isolation(&session), Isolation::Serializable);
        assert!(session_command("SET TRANSACTION ISOLATION LEVEL READ COMMITTED", &mut session)
            .is_some());
        assert_eq!(session.get("transaction_isolation"), Some(&String::from("snapshot")));
        assert_eq!(session_isolation(&session), Isolation::Snapshot);
        assert!(session_command(
            "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL REPEATABLE READ",
            &mut session,
        )
        .is_some());
        assert_eq!(session.get("transaction_isolation"), Some(&String::from("snapshot")));
        assert!(session_command("SET TRANSACTION ISOLATION LEVEL SNAPSHOT EXTRA", &mut session)
            .is_none());
        assert!(session_command("SHOW transaction_isolation", &mut session).is_some());
    }

    #[test]
    fn savepoint_controls_parse_and_normalize_names() {
        let session = HashMap::new();
        assert!(matches!(
            transaction_control("SAVEPOINT Work;", &session),
            Some(TransactionControl::Savepoint(name)) if name == "work"
        ));
        assert!(matches!(
            transaction_control("RELEASE SAVEPOINT Work", &session),
            Some(TransactionControl::ReleaseSavepoint(name)) if name == "work"
        ));
        assert!(matches!(
            transaction_control("ROLLBACK TRANSACTION TO SAVEPOINT Work", &session),
            Some(TransactionControl::RollbackToSavepoint(name)) if name == "work"
        ));
        assert!(matches!(
            transaction_control("SAVEPOINT \"CaseSensitive\"", &session),
            Some(TransactionControl::Savepoint(name)) if name == "CaseSensitive"
        ));
    }

    #[tokio::test]
    async fn savepoint_rollback_discards_only_later_changes() {
        let executor = Arc::new(Executor::new(String::from("tenant"), String::from("db")));
        executor
            .execute(parse("CREATE TABLE messages (id TEXT PRIMARY KEY, body TEXT)").unwrap())
            .await
            .unwrap();
        let mut active = Some(SessionTransaction {
            txn: executor.begin_transaction(Isolation::Serializable),
            changes: Vec::new(),
            failed: false,
            savepoints: Vec::new(),
        });

        {
            let state = active.as_mut().unwrap();
            let (_, changes) = executor
                .execute_in_transaction(
                    &mut state.txn,
                    parse("INSERT INTO messages (id, body) VALUES ('first', 'kept')").unwrap(),
                )
                .await
                .unwrap();
            state.changes.extend(changes);
        }
        handle_transaction_control(
            TransactionControl::Savepoint(String::from("after_first")),
            &executor,
            &mut active,
        )
        .await;
        {
            let state = active.as_mut().unwrap();
            let (_, changes) = executor
                .execute_in_transaction(
                    &mut state.txn,
                    parse("INSERT INTO messages (id, body) VALUES ('second', 'discarded')")
                        .unwrap(),
                )
                .await
                .unwrap();
            state.changes.extend(changes);
        }
        handle_transaction_control(
            TransactionControl::RollbackToSavepoint(String::from("after_first")),
            &executor,
            &mut active,
        )
        .await;
        handle_transaction_control(TransactionControl::Commit, &executor, &mut active).await;

        assert!(matches!(
            executor.execute(parse("SELECT id, body FROM messages").unwrap()).await.unwrap(),
            QueryResult::Table { rows, .. }
                if rows == vec![vec![b"first".to_vec(), b"kept".to_vec()]]
        ));
    }

    #[test]
    fn select_expressions() {
        let session = HashMap::new();
        let rows = session_select("SELECT 1", &session).unwrap();
        assert_eq!(rows, vec![(String::from("1"), String::from("1"))]);
        let rows = session_select("SELECT $1::text AS echo", &session).unwrap_or_else(|| {
            session_select(&bind("SELECT $1::text AS echo", &[String::from("hi")]), &session)
                .unwrap()
        });
        assert_eq!(rows, vec![(String::from("echo"), String::from("hi"))]);
        let rows =
            session_select("SELECT version(), current_database(), 42 AS n", &session).unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[2], (String::from("n"), String::from("42")));
        assert!(session_select("SELECT * FROM users", &session).is_none());
        let rows = session_select("SELECT pg_catalog.version()", &session).unwrap();
        assert_eq!(rows[0].1, "PostgreSQL 16.0 on rymeDB");
    }

    #[test]
    fn session_settings_match_common_driver_queries() {
        let mut session = HashMap::new();
        assert!(session_command("SET TIME ZONE 'America/New_York'", &mut session).is_some());
        assert_eq!(session.get("timezone"), Some(&String::from("America/New_York")));
        assert!(session_command("SET NAMES 'UTF8'", &mut session).is_some());
        assert_eq!(session.get("client_encoding"), Some(&String::from("UTF8")));
        assert!(session_command("SET SESSION application_name = 'worker'", &mut session).is_some());
        let rows = session_select("SELECT current_setting('application_name')", &session).unwrap();
        assert_eq!(rows[0].1, "worker");
        let rows =
            session_select("SELECT current_setting('server_version_num')", &session).unwrap();
        assert_eq!(rows[0].1, "160000");
        assert!(session_command("SHOW default_transaction_isolation", &mut session).is_some());
        assert!(session_command("SHOW ALL", &mut session).is_some());
    }

    #[test]
    fn bind_decodes_null_parameters_as_sql_null() {
        let mut payload = Vec::new();
        payload.extend_from_slice(b"portal\0statement\0");
        payload.extend_from_slice(&0i16.to_be_bytes());
        payload.extend_from_slice(&1i16.to_be_bytes());
        payload.extend_from_slice(&(-1i32).to_be_bytes());
        let (_, _, params) = parse_bind(&payload).unwrap();
        assert_eq!(params, vec![String::from("\0")]);
        assert_eq!(execute_parameter("NULL"), "\0");
        assert_eq!(execute_parameter("'NULL'"), "NULL");
    }

    #[test]
    fn parameter_description_ignores_quoted_and_commented_placeholders() {
        assert_eq!(parameter_count("SELECT '$12', \"$13\", $2, $10 -- $20\n/* $30 */"), 10);
        let description = parameter_description("SELECT $2");
        assert_eq!(description[0], b't');
        assert_eq!(i16::from_be_bytes([description[5], description[6]]), 2);
    }

    #[test]
    fn error_codes_map() {
        assert_eq!(error_code(&RymeError::Conflict(String::from("x"))), "40001");
        assert_eq!(error_code(&RymeError::Unauthorized), "28000");
        assert_eq!(error_code(&RymeError::NotFound(String::from("x"))), "02000");
        assert_eq!(error_code(&RymeError::ReadOnly(String::from("x"))), "25006");
    }

    #[test]
    fn describe_shapes() {
        assert_eq!(describe_query("SELECT 1")[0], b'T');
        assert!(parse("SELECT 1, 2 AS n").is_ok());
        assert_eq!(describe_query("SELECT 1, 2 AS n")[0], b'T');
        let aliased = describe_query("SELECT payload AS body, count AS total FROM events");
        assert!(aliased.windows(b"body\0".len()).any(|window| window == b"body\0"));
        assert!(aliased.windows(b"total\0".len()).any(|window| window == b"total\0"));
        assert_eq!(describe_query("INSERT INTO t KEY '1' VALUE 'v'")[0], b'n');
        assert_eq!(describe_query("SELECT nonsense()")[0], b'n');
    }

    #[test]
    fn catalog_policies_exposes_persisted_rls_metadata() {
        let executor = Arc::new(Executor::new(String::from("tenant"), String::from("db")));
        let mut snapshot = SchemaSnapshot::default();
        snapshot.rls_policies.insert(
            String::from("messages\0tenant_policy"),
            RlsPolicy {
                name: String::from("tenant_policy"),
                table: String::from("messages"),
                column: String::from("tenant_id"),
                command: String::from("SELECT"),
                using: Some(String::from("tenant_id = auth.uid()")),
                check: None,
                read: true,
                write: false,
            },
        );
        executor.restore_schema_snapshot(snapshot).unwrap();

        let response = catalog_query(
            "SELECT policyname, cmd, qual, with_check FROM pg_catalog.pg_policies WHERE tablename = 'messages'",
            &executor,
        )
        .unwrap();
        assert!(response.windows(b"tenant_policy".len()).any(|window| window == b"tenant_policy"));
        assert!(response
            .windows(b"tenant_id = auth.uid()".len())
            .any(|window| window == b"tenant_id = auth.uid()"));
        assert!(response.windows(b"SELECT".len()).any(|window| window == b"SELECT"));
        let null = (-1i32).to_be_bytes();
        assert!(response.windows(null.len()).any(|window| window == null));
    }

    #[test]
    fn catalogs_expose_persisted_functions_and_triggers() {
        let executor = Arc::new(Executor::new(String::from("tenant"), String::from("db")));
        let mut snapshot = SchemaSnapshot::default();
        snapshot.functions.insert(
            String::from("set_updated_at"),
            FunctionDefinition {
                name: String::from("set_updated_at"),
                body: String::from("BEGIN NEW.updated_at = now(); RETURN NEW; END;"),
                language: String::from("plpgsql"),
            },
        );
        snapshot.triggers.insert(
            String::from("profiles\0set_updated_at"),
            TriggerDefinition {
                name: String::from("set_updated_at"),
                table: String::from("profiles"),
                timing: String::from("BEFORE"),
                events: vec![String::from("UPDATE")],
                function: String::from("set_updated_at"),
            },
        );
        executor.restore_schema_snapshot(snapshot).unwrap();

        let functions = catalog_query(
            "SELECT proname, prosrc FROM pg_catalog.pg_proc WHERE proname = 'set_updated_at'",
            &executor,
        )
        .unwrap();
        assert!(functions
            .windows(b"set_updated_at".len())
            .any(|window| window == b"set_updated_at"));
        assert!(functions
            .windows(b"NEW.updated_at = now()".len())
            .any(|window| window == b"NEW.updated_at = now()"));

        let triggers = catalog_query(
            "SELECT tgname, tgtype FROM pg_catalog.pg_trigger WHERE tgname = 'set_updated_at'",
            &executor,
        )
        .unwrap();
        assert!(triggers
            .windows(b"set_updated_at".len())
            .any(|window| window == b"set_updated_at"));
        assert!(triggers.windows(b"19".len()).any(|window| window == b"19"));
    }

    #[test]
    fn catalogs_expose_views_as_relations() {
        let executor = Arc::new(Executor::new(String::from("tenant"), String::from("db")));
        let mut snapshot = SchemaSnapshot::default();
        snapshot.views.insert(
            String::from("active_messages"),
            ViewDefinition {
                name: String::from("active_messages"),
                query: parse("SELECT * FROM messages").unwrap(),
            },
        );
        executor.restore_schema_snapshot(snapshot).unwrap();

        let tables = catalog_query(
            "SELECT table_name, table_type FROM information_schema.tables WHERE table_name = 'active_messages'",
            &executor,
        )
        .unwrap();
        assert!(tables
            .windows(b"active_messages".len())
            .any(|window| window == b"active_messages"));
        assert!(tables.windows(b"VIEW".len()).any(|window| window == b"VIEW"));

        let relations = catalog_query(
            "SELECT relname, relkind FROM pg_catalog.pg_class WHERE relname = 'active_messages'",
            &executor,
        )
        .unwrap();
        assert!(relations
            .windows(b"active_messages".len())
            .any(|window| window == b"active_messages"));
        assert!(relations.windows(b"v".len()).any(|window| window == b"v"));
    }

    #[test]
    fn catalogs_expose_sequences() {
        let executor = Arc::new(Executor::new(String::from("tenant"), String::from("db")));
        let mut snapshot = SchemaSnapshot::default();
        snapshot.sequences.insert(
            String::from("public.events_id_seq"),
            SequenceDefinition {
                name: String::from("public.events_id_seq"),
                data_type: String::from("bigint"),
                start: 10,
                increment: 2,
                min_value: 1,
                max_value: i64::MAX,
                cache: 1,
                cycle: false,
                last_value: Some(10),
            },
        );
        executor.restore_schema_snapshot(snapshot).unwrap();

        let response = catalog_query(
            "SELECT sequencename, increment_by, last_value FROM pg_catalog.pg_sequences WHERE sequencename = 'events_id_seq'",
            &executor,
        )
        .unwrap();
        assert!(response.windows(b"events_id_seq".len()).any(|window| window == b"events_id_seq"));
        assert!(response.windows(b"2".len()).any(|window| window == b"2"));
        assert!(response.windows(b"10".len()).any(|window| window == b"10"));

        let relations = catalog_query(
            "SELECT relname, relkind FROM pg_catalog.pg_class WHERE relname = 'events_id_seq'",
            &executor,
        )
        .unwrap();
        assert!(relations.windows(b"events_id_seq".len()).any(|window| window == b"events_id_seq"));
        assert!(relations.windows(b"S".len()).any(|window| window == b"S"));
    }

    #[test]
    fn catalogs_expose_roles_and_table_privileges() {
        let executor = Arc::new(Executor::new(String::from("tenant"), String::from("db")));
        let mut snapshot = SchemaSnapshot::default();
        snapshot.roles.insert(
            String::from("app_reader"),
            RoleDefinition {
                name: String::from("app_reader"),
                login: true,
                superuser: false,
                createdb: false,
                createrole: false,
                inherit: true,
            },
        );
        snapshot.privileges.push(PrivilegeGrant {
            object_type: String::from("TABLE"),
            object: String::from("public.messages"),
            privilege: String::from("SELECT"),
            grantee: String::from("app_reader"),
            grant_option: true,
        });
        executor.restore_schema_snapshot(snapshot).unwrap();

        let roles = catalog_query(
            "SELECT rolname, rolcanlogin FROM pg_catalog.pg_roles WHERE rolname = 'app_reader'",
            &executor,
        )
        .unwrap();
        assert!(roles.windows(b"app_reader".len()).any(|window| window == b"app_reader"));
        assert!(roles.windows(b"t".len()).any(|window| window == b"t"));

        let privileges = catalog_query(
            "SELECT grantee, table_name, privilege_type, is_grantable FROM information_schema.table_privileges WHERE table_name = 'messages'",
            &executor,
        )
        .unwrap();
        assert!(privileges.windows(b"app_reader".len()).any(|window| window == b"app_reader"));
        assert!(privileges.windows(b"SELECT".len()).any(|window| window == b"SELECT"));
        assert!(privileges.windows(b"YES".len()).any(|window| window == b"YES"));
    }

    #[test]
    fn catalog_exposes_default_privileges() {
        let executor = Arc::new(Executor::new(String::from("tenant"), String::from("db")));
        let mut snapshot = SchemaSnapshot::default();
        snapshot.default_privileges.push(DefaultPrivilegeGrant {
            object_type: String::from("TABLE"),
            schema: Some(String::from("public")),
            owner: Some(String::from("postgres")),
            privilege: String::from("SELECT"),
            grantee: String::from("authenticated"),
            grant_option: true,
        });
        executor.restore_schema_snapshot(snapshot).unwrap();

        let response = catalog_query(
            "SELECT defaclrole, defaclnamespace, defaclobjtype, defaclacl FROM pg_catalog.pg_default_acl WHERE defaclobjtype = 'r'",
            &executor,
        )
        .unwrap();
        assert!(response
            .windows(b"authenticated=r*/postgres".len())
            .any(|window| { window == b"authenticated=r*/postgres" }));
        assert!(response.windows(b"r".len()).any(|window| window == b"r"));
        assert!(response
            .windows(catalog_namespace_oid("public").to_string().len())
            .any(|window| window == catalog_namespace_oid("public").to_string().as_bytes()));
    }

    #[test]
    fn null_cells_use_postgres_null_lengths() {
        let packet = data_row_values(&[SQL_NULL_SENTINEL.to_vec(), b"value".to_vec()]);
        assert_eq!(packet[0], b'D');
        assert_eq!(u16::from_be_bytes([packet[5], packet[6]]), 2);
        assert_eq!(i32::from_be_bytes(packet[7..11].try_into().unwrap()), -1);
        assert_eq!(i32::from_be_bytes(packet[11..15].try_into().unwrap()), 5);
    }

    #[test]
    fn scalar_results_preserve_labels_and_nulls() {
        let packet = encode_result(QueryResult::Scalar {
            label: String::from("sum"),
            value: SQL_NULL_SENTINEL.to_vec(),
        });
        assert!(packet.windows(b"sum\0".len()).any(|window| window == b"sum\0"));
        assert!(packet.windows(4).any(|window| window == (-1i32).to_be_bytes()));
    }
}
