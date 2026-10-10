use ryme_auth::{ApiKeyStore, Principal};
use ryme_error::{Result, RymeError};
use ryme_gateway::Gateway;
use ryme_metering::{MeterRegistry, Metric, UsageEvent};
use ryme_observe::{
    parse_traceparent, query_fingerprint, Histogram, LatencyWindow, SlowEntry, SlowLog,
    TraceCollector, TraceSpan, SLOW_THRESHOLD_MICROS,
};
use ryme_qos::QosRegistry;
use ryme_router::RangeLoadHook;
use std::sync::{Arc, Mutex};
use tonic::{Request, Response, Status};

#[allow(clippy::result_large_err)]
pub mod proto {
    tonic::include_proto!("rymedb.v1");
}

pub const VERSION: &str = "ryme-grpc/1";

#[derive(Debug, Clone)]
pub struct GrpcGateway<B = ryme_txn::TxnManager> {
    gateway: Gateway<B>,
    keys: ApiKeyStore,
    qos: Arc<Mutex<QosRegistry>>,
    metering: Option<Arc<Mutex<MeterRegistry>>>,
    latency: Option<LatencyWindow>,
    histogram: Option<Histogram>,
    slow_log: Option<SlowLog>,
    traces: Option<Arc<Mutex<TraceCollector>>>,
    range_hook: RangeLoadHook,
}

impl<B> GrpcGateway<B>
where
    B: ryme_txn::TxnBackend + Clone + Send + Sync + 'static,
{
    pub fn with_gateway(gateway: Gateway<B>, keys: ApiKeyStore) -> Self {
        Self {
            gateway,
            keys,
            qos: Arc::new(Mutex::new(QosRegistry::new())),
            metering: None,
            latency: None,
            histogram: None,
            slow_log: None,
            traces: None,
            range_hook: RangeLoadHook::default(),
        }
    }

    pub fn with_qos(gateway: Gateway<B>, keys: ApiKeyStore, qos: Arc<Mutex<QosRegistry>>) -> Self {
        Self {
            gateway,
            keys,
            qos,
            metering: None,
            latency: None,
            histogram: None,
            slow_log: None,
            traces: None,
            range_hook: RangeLoadHook::default(),
        }
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

    fn table_key(table: &str, pk: &[u8]) -> Vec<u8> {
        let mut routing = Vec::with_capacity(table.len() + pk.len() + 1);
        routing.extend_from_slice(table.as_bytes());
        routing.push(0);
        routing.extend_from_slice(pk);
        routing
    }

    fn sql_range_keys(statement: &ryme_sql::Statement) -> Option<Vec<Vec<u8>>> {
        match statement {
            ryme_sql::Statement::Insert { pk, .. }
            | ryme_sql::Statement::Upsert { pk, .. }
            | ryme_sql::Statement::InsertIgnore { pk, .. }
            | ryme_sql::Statement::InsertConflict { pk, .. }
            | ryme_sql::Statement::Update { pk, .. }
            | ryme_sql::Statement::UpdateRow { pk, .. }
            | ryme_sql::Statement::Delete { pk, .. } => Some(vec![pk.clone()]),
            ryme_sql::Statement::CopyFrom { rows, .. } => {
                Some(rows.iter().map(|(pk, _)| pk.clone()).collect())
            }
            ryme_sql::Statement::Returning { statement, .. } => Self::sql_range_keys(statement),
            _ => None,
        }
    }

    pub async fn serve(self, addr: std::net::SocketAddr) -> Result<()> {
        tonic::transport::Server::builder()
            .add_service(proto::ryme_server::RymeServer::new(self))
            .serve(addr)
            .await
            .map_err(|e| RymeError::Io(e.to_string()))
    }

    pub async fn serve_with_incoming(self, listener: tokio::net::TcpListener) -> Result<()> {
        self.serve_with_incoming_limited(listener, 1024).await
    }

    pub async fn serve_with_incoming_limited(
        self,
        listener: tokio::net::TcpListener,
        max_connections: usize,
    ) -> Result<()> {
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        tonic::transport::Server::builder()
            .concurrency_limit_per_connection(max_connections.max(1))
            .add_service(proto::ryme_server::RymeServer::new(self))
            .serve_with_incoming(incoming)
            .await
            .map_err(|e| RymeError::Io(e.to_string()))
    }

    pub async fn serve_tls(
        self,
        listener: tokio::net::TcpListener,
        acceptor: ryme_tls::TlsAcceptor,
    ) -> Result<()> {
        self.serve_tls_limited(listener, acceptor, 1024).await
    }

    pub async fn serve_tls_limited(
        self,
        listener: tokio::net::TcpListener,
        acceptor: ryme_tls::TlsAcceptor,
        max_connections: usize,
    ) -> Result<()> {
        let (cert, key) = acceptor.identity();
        let identity = tonic::transport::Identity::from_pem(cert, key);
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        tonic::transport::Server::builder()
            .concurrency_limit_per_connection(max_connections.max(1))
            .tls_config(tonic::transport::ServerTlsConfig::new().identity(identity))
            .map_err(|e| RymeError::Io(e.to_string()))?
            .add_service(proto::ryme_server::RymeServer::new(self))
            .serve_with_incoming(incoming)
            .await
            .map_err(|e| RymeError::Io(e.to_string()))
    }

    fn principal(&self, request: &Request<impl prost::Message>) -> Result<Principal> {
        self.keys.authenticate(bearer_key(request))
    }

    fn admit_read(&self, principal: &Principal) -> Result<()> {
        let now = qos_now();
        match self.qos.lock() {
            Ok(mut qos) => qos.admit_read(&principal.tenant, now),
            Err(_) => Err(RymeError::Internal(String::from("qos lock"))),
        }
    }

    fn admit_write(&self, principal: &Principal, bytes: u64) -> Result<()> {
        let now = qos_now();
        match self.qos.lock() {
            Ok(mut qos) => qos.admit_write(&principal.tenant, bytes, now),
            Err(_) => Err(RymeError::Internal(String::from("qos lock"))),
        }
    }

    #[allow(clippy::result_large_err)]
    fn admit_response<T: prost::Message>(
        &self,
        principal: &Principal,
        reply: T,
    ) -> std::result::Result<Response<T>, Status> {
        let now = qos_now();
        match self.qos.lock() {
            Ok(mut qos) => qos.admit_egress(&principal.tenant, reply.encoded_len() as u64, now),
            Err(_) => Err(RymeError::Internal(String::from("qos lock"))),
        }
        .map_err(status_of)?;
        Ok(Response::new(reply))
    }

    fn observe(&self, principal: &Principal, write: bool) {
        let Some(metering) = self.metering.as_ref() else { return };
        let metric = if write { Metric::WriteUnit } else { Metric::ReadUnit };
        let event = UsageEvent::new(
            principal.tenant.clone(),
            self.gateway.database_name().to_string(),
            metric,
            1,
            String::new(),
        );
        if let Ok(mut registry) = metering.lock() {
            registry.ingest(event);
        }
    }

    fn record_timing(
        &self,
        fingerprint: &str,
        table: &str,
        micros: u64,
        parent: Option<(String, String)>,
    ) {
        if let Some(latency) = self.latency.as_ref() {
            latency.observe_micros(micros);
        }
        if let Some(histogram) = self.histogram.as_ref() {
            histogram.record(micros);
        }
        if micros > SLOW_THRESHOLD_MICROS {
            if let Some(slow_log) = self.slow_log.as_ref() {
                slow_log.record(SlowEntry {
                    kind: String::from("grpc"),
                    fingerprint: fingerprint.to_string(),
                    table: table.to_string(),
                    micros,
                    at_unix: slow_now_secs(),
                });
            }
        }
        if let Some(traces) = self.traces.as_ref() {
            let mut span = match parent {
                Some((trace_id, parent_id)) => {
                    TraceSpan::linked(trace_id, parent_id, String::from("grpc"), slow_now_secs())
                }
                None => TraceSpan::root(String::from("grpc"), slow_now_secs()),
            };
            span.attr(String::from("op"), fingerprint.to_string());
            span.attr(String::from("table"), table.to_string());
            span.finish(micros);
            if let Ok(mut collector) = traces.lock() {
                collector.push(span);
            }
        }
    }
}

fn bearer_key(request: &Request<impl prost::Message>) -> &str {
    let presented =
        request.metadata().get("authorization").and_then(|value| value.to_str().ok()).unwrap_or("");
    presented.strip_prefix("Bearer ").unwrap_or(presented)
}

fn status_of(error: RymeError) -> Status {
    match error {
        RymeError::Overload(message) => Status::resource_exhausted(message),
        RymeError::Unauthorized => Status::unauthenticated("unauthorized"),
        RymeError::InvalidArgument(message) => Status::invalid_argument(message),
        RymeError::NotFound(message) => Status::not_found(message),
        RymeError::Unavailable(message) => Status::unavailable(message),
        RymeError::ReadOnly(message) => Status::unavailable(message),
        _ => Status::internal(error.to_string()),
    }
}

fn qos_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

fn trace_parent(request: &Request<impl prost::Message>) -> Option<(String, String)> {
    request
        .metadata()
        .get("traceparent")
        .and_then(|value| value.to_str().ok())
        .and_then(parse_traceparent)
}

fn slow_now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn masked_row(
    gateway: &Gateway<impl ryme_txn::TxnBackend>,
    table: &str,
    pk: Vec<u8>,
    value: Vec<u8>,
) -> proto::Row {
    let masked = gateway.masked(table, value);
    proto::Row {
        pk: String::from_utf8_lossy(&pk).into_owned().into_bytes(),
        value: String::from_utf8_lossy(&masked).into_owned().into_bytes(),
    }
}

#[tonic::async_trait]
impl<B> proto::ryme_server::Ryme for GrpcGateway<B>
where
    B: ryme_txn::TxnBackend + Clone + Send + Sync + 'static,
{
    async fn health(
        &self,
        _request: Request<proto::HealthRequest>,
    ) -> std::result::Result<Response<proto::HealthReply>, Status> {
        Ok(Response::new(proto::HealthReply { ok: true, version: VERSION.to_string() }))
    }

    async fn kv_get(
        &self,
        request: Request<proto::KvGetRequest>,
    ) -> std::result::Result<Response<proto::KvGetReply>, Status> {
        let principal = self.principal(&request).map_err(status_of)?;
        let parent = trace_parent(&request);
        let start = std::time::Instant::now();
        let inner = request.into_inner();
        if inner.table.is_empty() || inner.pk.is_empty() {
            return Err(Status::invalid_argument("table/pk required"));
        }
        self.admit_read(&principal).map_err(status_of)?;
        let reply = match self.gateway.get_async(&principal, &inner.table, &inner.pk).await {
            Ok(Some(value)) => {
                let masked = self.gateway.masked(&inner.table, value);
                proto::KvGetReply { ok: true, error: String::new(), value: masked, found: true }
            }
            Ok(None) => proto::KvGetReply {
                ok: false,
                error: String::from("not found"),
                value: Vec::new(),
                found: false,
            },
            Err(e) => proto::KvGetReply {
                ok: false,
                error: e.to_string(),
                value: Vec::new(),
                found: false,
            },
        };
        self.observe(&principal, false);
        self.record_timing("kv_get", &inner.table, start.elapsed().as_micros() as u64, parent);
        self.admit_response(&principal, reply)
    }

    async fn kv_put(
        &self,
        request: Request<proto::KvPutRequest>,
    ) -> std::result::Result<Response<proto::CommitReply>, Status> {
        let principal = self.principal(&request).map_err(status_of)?;
        let parent = trace_parent(&request);
        let start = std::time::Instant::now();
        let inner = request.into_inner();
        if inner.table.is_empty() || inner.pk.is_empty() {
            return Err(Status::invalid_argument("table/pk required"));
        }
        if inner.value.len() > 4 * 1024 * 1024 {
            return Err(Status::invalid_argument("value too large"));
        }
        self.admit_write(&principal, (inner.pk.len() + inner.value.len()) as u64)
            .map_err(status_of)?;
        let expires_at = match inner.ttl_secs {
            0 => None,
            secs if secs > 315_360_000 => {
                return Err(Status::invalid_argument("ttl too large"));
            }
            secs => Some(ryme_txn::now_unix().saturating_add(secs)),
        };
        let routing = if self.range_hook.is_armed() {
            Some(Self::table_key(&inner.table, &inner.pk))
        } else {
            None
        };
        let reply = match self
            .gateway
            .put_with_ttl(&principal, &inner.table, inner.pk, inner.value, expires_at)
            .await
        {
            Ok(commit) => proto::CommitReply { ok: true, error: String::new(), commit },
            Err(e) => proto::CommitReply { ok: false, error: e.to_string(), commit: 0 },
        };
        if reply.ok {
            if let Some(key) = routing {
                self.range_hook.note(&key, 1);
            }
        }
        self.observe(&principal, true);
        self.record_timing("kv_put", &inner.table, start.elapsed().as_micros() as u64, parent);
        self.admit_response(&principal, reply)
    }

    async fn kv_delete(
        &self,
        request: Request<proto::KvDeleteRequest>,
    ) -> std::result::Result<Response<proto::CommitReply>, Status> {
        let principal = self.principal(&request).map_err(status_of)?;
        let parent = trace_parent(&request);
        let start = std::time::Instant::now();
        let inner = request.into_inner();
        if inner.table.is_empty() || inner.pk.is_empty() {
            return Err(Status::invalid_argument("table/pk required"));
        }
        self.admit_write(&principal, inner.pk.len() as u64).map_err(status_of)?;
        let routing = if self.range_hook.is_armed() {
            Some(Self::table_key(&inner.table, &inner.pk))
        } else {
            None
        };
        let reply = match self.gateway.delete(&principal, &inner.table, inner.pk).await {
            Ok(commit) => proto::CommitReply { ok: true, error: String::new(), commit },
            Err(e) => proto::CommitReply { ok: false, error: e.to_string(), commit: 0 },
        };
        if reply.ok {
            if let Some(key) = routing {
                self.range_hook.note(&key, 1);
            }
        }
        self.observe(&principal, true);
        self.record_timing("kv_delete", &inner.table, start.elapsed().as_micros() as u64, parent);
        self.admit_response(&principal, reply)
    }

    async fn scan(
        &self,
        request: Request<proto::ScanRequest>,
    ) -> std::result::Result<Response<proto::ScanReply>, Status> {
        let principal = self.principal(&request).map_err(status_of)?;
        let parent = trace_parent(&request);
        let start = std::time::Instant::now();
        let inner = request.into_inner();
        if inner.table.is_empty() {
            return Err(Status::invalid_argument("table required"));
        }
        self.admit_read(&principal).map_err(status_of)?;
        let limit = (inner.limit as usize).clamp(1, 1000).max(1);
        let reply = match self.gateway.scan_async(&principal, &inner.table, limit).await {
            Ok(rows) => proto::ScanReply {
                ok: true,
                error: String::new(),
                rows: rows
                    .into_iter()
                    .map(|(pk, value)| masked_row(&self.gateway, &inner.table, pk, value))
                    .collect(),
            },
            Err(e) => proto::ScanReply { ok: false, error: e.to_string(), rows: Vec::new() },
        };
        self.observe(&principal, false);
        self.record_timing("scan", &inner.table, start.elapsed().as_micros() as u64, parent);
        self.admit_response(&principal, reply)
    }

    async fn sql(
        &self,
        request: Request<proto::SqlRequest>,
    ) -> std::result::Result<Response<proto::SqlReply>, Status> {
        let principal = self.principal(&request).map_err(status_of)?;
        let parent = trace_parent(&request);
        let start = std::time::Instant::now();
        let inner = request.into_inner();
        if inner.sql.is_empty() {
            return Err(Status::invalid_argument("sql required"));
        }
        let statement = match ryme_sql::parse(&inner.sql) {
            Ok(statement) => statement,
            Err(e) => {
                self.record_timing("sql", "", start.elapsed().as_micros() as u64, parent.clone());
                return Err(Status::invalid_argument(e.to_string()));
            }
        };
        let write = statement.is_write();
        let denied = match write {
            true => self.admit_write(&principal, inner.sql.len() as u64).err(),
            false => self.admit_read(&principal).err(),
        };
        if let Some(e) = denied {
            self.record_timing("sql", "", start.elapsed().as_micros() as u64, parent);
            return Err(status_of(e));
        }
        let table = statement.table().to_string();
        let range_keys = if write && self.range_hook.is_armed() {
            Self::sql_range_keys(&statement)
        } else {
            None
        };
        let fingerprint = self.slow_log.as_ref().map(|_| query_fingerprint(&inner.sql));
        let mut executor = ryme_sql::Executor::with_backend(
            principal.tenant.clone(),
            String::from("default"),
            self.gateway.manager_clone(),
        )
        .with_realtime(self.gateway.realtime());
        if self.gateway.is_read_only() {
            executor.set_read_only(true);
        }
        let response = match executor.execute(statement).await {
            Ok(ryme_sql::QueryResult::Ok) => {
                if write && self.range_hook.is_armed() {
                    if let Some(keys) = range_keys.as_deref() {
                        for key in keys {
                            self.range_hook.note(&Self::table_key(&table, key), 1);
                        }
                    } else {
                        self.range_hook.note(table.as_bytes(), 1);
                    }
                }
                proto::SqlReply { ok: true, error: String::new(), rows: Vec::new() }
            }
            Ok(ryme_sql::QueryResult::Row { pk, value }) => proto::SqlReply {
                ok: true,
                error: String::new(),
                rows: vec![masked_row(&self.gateway, &table, pk, value)],
            },
            Ok(ryme_sql::QueryResult::Scalar { label, value }) => proto::SqlReply {
                ok: true,
                error: String::new(),
                rows: vec![proto::Row {
                    pk: label.into_bytes(),
                    value: String::from_utf8_lossy(&value).into_owned().into_bytes(),
                }],
            },
            Ok(ryme_sql::QueryResult::Rows { rows }) => proto::SqlReply {
                ok: true,
                error: String::new(),
                rows: rows
                    .into_iter()
                    .map(|(pk, value)| masked_row(&self.gateway, &table, pk, value))
                    .collect(),
            },
            Ok(ryme_sql::QueryResult::Table { columns, rows }) => proto::SqlReply {
                ok: true,
                error: String::new(),
                rows: rows
                    .into_iter()
                    .map(|row| {
                        let pk = row.first().cloned().unwrap_or_default();
                        let values = row
                            .into_iter()
                            .map(|value| {
                                if value.as_slice() == ryme_sql::SQL_NULL_SENTINEL {
                                    serde_json::Value::Null
                                } else {
                                    serde_json::json!(value)
                                }
                            })
                            .collect::<Vec<_>>();
                        let value = serde_json::json!({
                            "columns": columns.clone(),
                            "values": values,
                        })
                        .to_string()
                        .into_bytes();
                        masked_row(&self.gateway, &table, pk, value)
                    })
                    .collect(),
            },
            Ok(ryme_sql::QueryResult::Returning { rows, .. }) => proto::SqlReply {
                ok: true,
                error: String::new(),
                rows: rows
                    .into_iter()
                    .map(|row| {
                        let pk = row.first().cloned().unwrap_or_default();
                        let value = row.get(1).cloned().unwrap_or_else(|| pk.clone());
                        masked_row(&self.gateway, &table, pk, value)
                    })
                    .collect(),
            },
            Err(e) => proto::SqlReply { ok: false, error: e.to_string(), rows: Vec::new() },
        };
        self.observe(&principal, write);
        self.record_timing(
            fingerprint.as_deref().unwrap_or("sql"),
            &table,
            start.elapsed().as_micros() as u64,
            parent,
        );
        self.admit_response(&principal, response)
    }
}
