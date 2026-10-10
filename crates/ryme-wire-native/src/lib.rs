use ryme_auth::{ApiKeyStore, Principal};
use ryme_error::{Result, RymeError};
use ryme_gateway::Gateway;
use ryme_metering::{MeterRegistry, Metric, UsageEvent};
use ryme_observe::{
    query_fingerprint, Histogram, LatencyWindow, SlowEntry, SlowLog, TraceCollector, TraceSpan,
    SLOW_THRESHOLD_MICROS,
};
use ryme_qos::QosRegistry;
use ryme_router::RangeLoadHook;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

pub const MAX_FRAME: usize = 4 * 1024 * 1024 + 1024;
pub const VERSION: &str = "ryme-native/1";

pub use ryme_tls::TlsAcceptor;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Request {
    pub key: String,
    pub op: String,
    #[serde(default)]
    pub table: String,
    #[serde(default)]
    pub pk: String,
    #[serde(default)]
    pub value: String,
    #[serde(default)]
    pub sql: String,
    #[serde(default)]
    pub limit: usize,
    #[serde(default)]
    pub ttl_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Response {
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rows: Option<Vec<Row>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default)]
    pub version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Row {
    pub pk: String,
    pub value: String,
}

impl Response {
    pub fn ok() -> Self {
        Self {
            ok: true,
            value: None,
            rows: None,
            commit: None,
            error: None,
            version: VERSION.to_string(),
        }
    }

    pub fn err(message: String) -> Self {
        Self {
            ok: false,
            value: None,
            rows: None,
            commit: None,
            error: Some(message),
            version: VERSION.to_string(),
        }
    }
}

fn response_bytes(response: &Response) -> u64 {
    let mut bytes = response.error.as_ref().map(|e| e.len()).unwrap_or(0) as u64;
    bytes += response.value.as_ref().map(|v| v.len()).unwrap_or(0) as u64;
    if let Some(rows) = response.rows.as_ref() {
        for row in rows {
            bytes += (row.pk.len() + row.value.len()) as u64;
        }
    }
    bytes
}

pub fn encode_frame(body: &[u8]) -> Result<Vec<u8>> {
    if body.len() > MAX_FRAME {
        return Err(RymeError::Overload(String::from("frame")));
    }
    let mut out = Vec::with_capacity(4 + body.len());
    out.extend_from_slice(&(body.len() as u32).to_be_bytes());
    out.extend_from_slice(body);
    Ok(out)
}

pub fn decode_frame(buffer: &[u8]) -> Result<Option<(Vec<u8>, usize)>> {
    if buffer.len() < 4 {
        return Ok(None);
    }
    let mut len_bytes = [0u8; 4];
    len_bytes.copy_from_slice(&buffer[..4]);
    let len = u32::from_be_bytes(len_bytes) as usize;
    if len > MAX_FRAME {
        return Err(RymeError::Overload(String::from("frame")));
    }
    if buffer.len() < 4 + len {
        return Ok(None);
    }
    Ok(Some((buffer[4..4 + len].to_vec(), 4 + len)))
}

#[derive(Debug, Clone)]
pub struct NativeGateway<B = ryme_txn::TxnManager> {
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

impl<B> NativeGateway<B>
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

    fn note_routing_key(&self, table: &str, pk: &[u8], count: u64) {
        if !self.range_hook.is_armed() {
            return;
        }
        let mut routing = Vec::with_capacity(table.len() + pk.len() + 1);
        routing.extend_from_slice(table.as_bytes());
        routing.push(0);
        routing.extend_from_slice(pk);
        self.range_hook.note(&routing, count);
    }

    fn note_table_key(&self, table: &str, pk: &str, count: u64) {
        self.note_routing_key(table, pk.as_bytes(), count);
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

    pub async fn serve(&self, listener: TcpListener) -> Result<()> {
        loop {
            let (socket, _) = listener.accept().await.map_err(|e| RymeError::Io(e.to_string()))?;
            socket.set_nodelay(true).map_err(|e| RymeError::Io(e.to_string()))?;
            let service = self.clone();
            tokio::spawn(async move {
                let _ = service.handle(socket).await;
            });
        }
    }

    pub async fn serve_limited(&self, listener: TcpListener, max_connections: usize) -> Result<()> {
        let limit = Arc::new(tokio::sync::Semaphore::new(max_connections.max(1)));
        loop {
            let (socket, _) = listener.accept().await.map_err(|e| RymeError::Io(e.to_string()))?;
            let Ok(permit) = limit.clone().try_acquire_owned() else {
                let mut socket = socket;
                let body = serde_json::to_vec(&Response::err(String::from("overloaded")))
                    .unwrap_or_default();
                if let Ok(frame) = encode_frame(&body) {
                    let _ = socket.write_all(&frame).await;
                }
                continue;
            };
            socket.set_nodelay(true).map_err(|e| RymeError::Io(e.to_string()))?;
            let service = self.clone();
            tokio::spawn(async move {
                let _permit = permit;
                let _ = service.handle(socket).await;
            });
        }
    }

    pub async fn serve_tls(
        &self,
        listener: TcpListener,
        max_connections: usize,
        acceptor: TlsAcceptor,
    ) -> Result<()> {
        let limit = Arc::new(tokio::sync::Semaphore::new(max_connections.max(1)));
        let acceptor = acceptor.acceptor();
        loop {
            let (socket, _) = listener.accept().await.map_err(|e| RymeError::Io(e.to_string()))?;
            let Ok(permit) = limit.clone().try_acquire_owned() else {
                continue;
            };
            socket.set_nodelay(true).map_err(|e| RymeError::Io(e.to_string()))?;
            let service = self.clone();
            let acceptor = acceptor.clone();
            tokio::spawn(async move {
                let _permit = permit;
                let Ok(tls) = acceptor.accept(socket).await else {
                    return;
                };
                let _ = service.handle_stream(tls).await;
            });
        }
    }

    async fn handle(&self, socket: TcpStream) -> Result<()> {
        self.handle_stream(socket).await
    }

    async fn handle_stream<S>(&self, mut socket: S) -> Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        // Batch ordinary pipelined replies to reduce syscall overhead while
        // keeping the response buffer bounded for connection-level fairness.
        const WRITE_BATCH_BYTES: usize = 64 * 1024;
        let mut pending: Vec<u8> = Vec::new();
        let mut chunk = vec![0u8; 65536];
        loop {
            let read = socket.read(&mut chunk).await.map_err(|e| RymeError::Io(e.to_string()))?;
            if read == 0 {
                return Ok(());
            }
            pending.extend_from_slice(&chunk[..read]);
            let mut consumed = 0usize;
            let mut replies = Vec::with_capacity(WRITE_BATCH_BYTES);
            while let Some((body, frame_bytes)) = decode_frame(&pending[consumed..])? {
                consumed += frame_bytes;
                let response = self.dispatch(&body).await;
                let raw = serde_json::to_vec(&response)
                    .map_err(|e| RymeError::Internal(e.to_string()))?;
                let frame = encode_frame(&raw)?;
                replies.extend_from_slice(&frame);
                if replies.len() >= WRITE_BATCH_BYTES {
                    socket.write_all(&replies).await.map_err(|e| RymeError::Io(e.to_string()))?;
                    replies.clear();
                }
            }
            if consumed > 0 {
                pending.drain(..consumed);
            }
            if !replies.is_empty() {
                socket.write_all(&replies).await.map_err(|e| RymeError::Io(e.to_string()))?;
            }
            if pending.len() > MAX_FRAME + 4 {
                return Err(RymeError::Overload(String::from("request")));
            }
        }
    }

    async fn dispatch(&self, body: &[u8]) -> Response {
        let request: Request = match serde_json::from_slice(body) {
            Ok(request) => request,
            Err(e) => return Response::err(e.to_string()),
        };
        let principal = match self.keys.authenticate(&request.key) {
            Ok(principal) => principal,
            Err(_) => return Response::err(String::from("unauthorized")),
        };
        let response = match request.op.as_str() {
            "ping" => {
                let start = std::time::Instant::now();
                let response = Response::ok();
                self.observe(&principal, false);
                self.record_timing("ping", "", start.elapsed().as_micros() as u64);
                response
            }
            "get" => self.op_get(&principal, &request).await,
            "put" => self.op_put(&principal, &request).await,
            "delete" => self.op_delete(&principal, &request).await,
            "scan" => self.op_scan(&principal, &request).await,
            "sql" => self.op_sql(&principal, &request).await,
            _ => Response::err(String::from("unknown op")),
        };
        if self.admit_egress(&principal, response_bytes(&response)).is_err() {
            return Response::err(String::from("overload: egress quota"));
        }
        response
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

    fn admit_egress(&self, principal: &Principal, bytes: u64) -> Result<()> {
        let now = qos_now();
        match self.qos.lock() {
            Ok(mut qos) => qos.admit_egress(&principal.tenant, bytes, now),
            Err(_) => Err(RymeError::Internal(String::from("qos lock"))),
        }
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

    fn record_timing(&self, fingerprint: &str, table: &str, micros: u64) {
        if let Some(latency) = self.latency.as_ref() {
            latency.observe_micros(micros);
        }
        if let Some(histogram) = self.histogram.as_ref() {
            histogram.record(micros);
        }
        if micros > SLOW_THRESHOLD_MICROS {
            if let Some(slow_log) = self.slow_log.as_ref() {
                slow_log.record(SlowEntry {
                    kind: String::from("native"),
                    fingerprint: fingerprint.to_string(),
                    table: table.to_string(),
                    micros,
                    at_unix: slow_now_secs(),
                });
            }
        }
        if let Some(traces) = self.traces.as_ref() {
            let mut span = TraceSpan::root(String::from("native"), slow_now_secs());
            span.attr(String::from("op"), fingerprint.to_string());
            span.attr(String::from("table"), table.to_string());
            span.finish(micros);
            if let Ok(mut collector) = traces.lock() {
                collector.push(span);
            }
        }
    }

    async fn op_get(&self, principal: &Principal, request: &Request) -> Response {
        if request.table.is_empty() || request.pk.is_empty() {
            return Response::err(String::from("table/pk required"));
        }
        let start = std::time::Instant::now();
        if let Err(e) = self.admit_read(principal) {
            let response = Response::err(e.to_string());
            self.record_timing("get", &request.table, start.elapsed().as_micros() as u64);
            return response;
        }
        let response =
            match self.gateway.get_async(principal, &request.table, request.pk.as_bytes()).await {
                Ok(Some(value)) => {
                    let masked = self.gateway.masked(&request.table, value);
                    let mut response = Response::ok();
                    response.value = Some(String::from_utf8_lossy(&masked).to_string());
                    response
                }
                Ok(None) => Response::err(String::from("not found")),
                Err(e) => Response::err(e.to_string()),
            };
        self.observe(principal, false);
        self.record_timing("get", &request.table, start.elapsed().as_micros() as u64);
        response
    }

    async fn op_put(&self, principal: &Principal, request: &Request) -> Response {
        if request.table.is_empty() || request.pk.is_empty() {
            return Response::err(String::from("table/pk required"));
        }
        if request.value.len() > 4 * 1024 * 1024 {
            return Response::err(String::from("value too large"));
        }
        let start = std::time::Instant::now();
        if let Err(e) = self.admit_write(principal, (request.pk.len() + request.value.len()) as u64)
        {
            let response = Response::err(e.to_string());
            self.record_timing("put", &request.table, start.elapsed().as_micros() as u64);
            return response;
        }
        let expires_at = match request.ttl_secs {
            0 => None,
            secs if secs > 315_360_000 => return Response::err(String::from("ttl too large")),
            secs => Some(ryme_txn::now_unix().saturating_add(secs)),
        };
        let response = match self
            .gateway
            .put_with_ttl(
                principal,
                &request.table,
                request.pk.clone().into_bytes(),
                request.value.clone().into_bytes(),
                expires_at,
            )
            .await
        {
            Ok(commit) => {
                self.note_table_key(&request.table, &request.pk, 1);
                let mut response = Response::ok();
                response.commit = Some(commit);
                response
            }
            Err(e) => Response::err(e.to_string()),
        };
        self.observe(principal, true);
        self.record_timing("put", &request.table, start.elapsed().as_micros() as u64);
        response
    }

    async fn op_delete(&self, principal: &Principal, request: &Request) -> Response {
        if request.table.is_empty() || request.pk.is_empty() {
            return Response::err(String::from("table/pk required"));
        }
        let start = std::time::Instant::now();
        if let Err(e) = self.admit_write(principal, request.pk.len() as u64) {
            let response = Response::err(e.to_string());
            self.record_timing("delete", &request.table, start.elapsed().as_micros() as u64);
            return response;
        }
        let response = match self
            .gateway
            .delete(principal, &request.table, request.pk.clone().into_bytes())
            .await
        {
            Ok(commit) => {
                self.note_table_key(&request.table, &request.pk, 1);
                let mut response = Response::ok();
                response.commit = Some(commit);
                response
            }
            Err(e) => Response::err(e.to_string()),
        };
        self.observe(principal, true);
        self.record_timing("delete", &request.table, start.elapsed().as_micros() as u64);
        response
    }

    async fn op_scan(&self, principal: &Principal, request: &Request) -> Response {
        if request.table.is_empty() {
            return Response::err(String::from("table required"));
        }
        let start = std::time::Instant::now();
        if let Err(e) = self.admit_read(principal) {
            let response = Response::err(e.to_string());
            self.record_timing("scan", &request.table, start.elapsed().as_micros() as u64);
            return response;
        }
        let limit = request.limit.clamp(1, 1000).max(1);
        let response = match self.gateway.scan_async(principal, &request.table, limit).await {
            Ok(rows) => {
                let mut response = Response::ok();
                response.rows = Some(
                    rows.into_iter()
                        .map(|(pk, value)| {
                            let masked = self.gateway.masked(&request.table, value);
                            Row {
                                pk: String::from_utf8_lossy(&pk).to_string(),
                                value: String::from_utf8_lossy(&masked).to_string(),
                            }
                        })
                        .collect(),
                );
                response
            }
            Err(e) => Response::err(e.to_string()),
        };
        self.observe(principal, false);
        self.record_timing("scan", &request.table, start.elapsed().as_micros() as u64);
        response
    }

    async fn op_sql(&self, principal: &Principal, request: &Request) -> Response {
        if request.sql.is_empty() {
            return Response::err(String::from("sql required"));
        }
        let start = std::time::Instant::now();
        let statement = match ryme_sql::parse(&request.sql) {
            Ok(statement) => statement,
            Err(e) => {
                let response = Response::err(e.to_string());
                self.record_timing("sql", "", start.elapsed().as_micros() as u64);
                return response;
            }
        };
        let write = statement.is_write();
        let bytes = request.sql.len() as u64;
        let denied = match write {
            true => self.admit_write(principal, bytes).err(),
            false => self.admit_read(principal).err(),
        };
        if let Some(e) = denied {
            let response = Response::err(e.to_string());
            self.record_timing("sql", "", start.elapsed().as_micros() as u64);
            return response;
        }
        let table = statement.table().to_string();
        let range_keys = if write && self.range_hook.is_armed() {
            Self::sql_range_keys(&statement)
        } else {
            None
        };
        let fingerprint = self.slow_log.as_ref().map(|_| query_fingerprint(&request.sql));
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
                if write {
                    if let Some(keys) = range_keys.as_deref() {
                        for key in keys {
                            self.note_routing_key(&table, key, 1);
                        }
                    } else {
                        self.range_hook.note(table.as_bytes(), 1);
                    }
                }
                Response::ok()
            }
            Ok(ryme_sql::QueryResult::Row { pk, value }) => {
                let masked = self.gateway.masked(&table, value);
                let mut response = Response::ok();
                response.rows = Some(vec![Row {
                    pk: String::from_utf8_lossy(&pk).to_string(),
                    value: String::from_utf8_lossy(&masked).to_string(),
                }]);
                response
            }
            Ok(ryme_sql::QueryResult::Scalar { label, value }) => {
                let mut response = Response::ok();
                response.rows = Some(vec![Row {
                    pk: label,
                    value: String::from_utf8_lossy(&value).to_string(),
                }]);
                response
            }
            Ok(ryme_sql::QueryResult::Rows { rows }) => {
                let mut response = Response::ok();
                response.rows = Some(
                    rows.into_iter()
                        .map(|(pk, value)| {
                            let masked = self.gateway.masked(&table, value);
                            Row {
                                pk: String::from_utf8_lossy(&pk).to_string(),
                                value: String::from_utf8_lossy(&masked).to_string(),
                            }
                        })
                        .collect(),
                );
                response
            }
            Ok(ryme_sql::QueryResult::Table { columns, rows }) => {
                let mut response = Response::ok();
                response.rows = Some(
                    rows.into_iter()
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
                            .to_string();
                            let masked = self.gateway.masked(&table, value.into_bytes());
                            Row {
                                pk: String::from_utf8_lossy(&pk).to_string(),
                                value: String::from_utf8_lossy(&masked).to_string(),
                            }
                        })
                        .collect(),
                );
                response
            }
            Ok(ryme_sql::QueryResult::Returning { rows, .. }) => {
                let mut response = Response::ok();
                response.rows = Some(
                    rows.into_iter()
                        .map(|row| {
                            let pk = row.first().cloned().unwrap_or_default();
                            let value = row.get(1).cloned().unwrap_or_else(|| pk.clone());
                            let masked = self.gateway.masked(&table, value);
                            Row {
                                pk: String::from_utf8_lossy(&pk).to_string(),
                                value: String::from_utf8_lossy(&masked).to_string(),
                            }
                        })
                        .collect(),
                );
                response
            }
            Err(e) => Response::err(e.to_string()),
        };
        self.observe(principal, write);
        self.record_timing(
            fingerprint.as_deref().unwrap_or("sql"),
            &table,
            start.elapsed().as_micros() as u64,
        );
        response
    }
}
fn qos_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

fn slow_now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_roundtrip() {
        let body = b"{\"op\":\"ping\"}".to_vec();
        let frame = encode_frame(&body).unwrap();
        assert_eq!(&frame[..4], &(body.len() as u32).to_be_bytes());
        let decoded = decode_frame(&frame).unwrap().unwrap();
        assert_eq!(decoded.0, body);
        assert_eq!(decoded.1, 4 + body.len());
    }

    #[test]
    fn frame_partial() {
        let body = b"hello".to_vec();
        let frame = encode_frame(&body).unwrap();
        assert!(decode_frame(&frame[..5]).unwrap().is_none());
        assert!(decode_frame(&[]).unwrap().is_none());
    }

    #[test]
    fn frame_rejects_huge() {
        assert!(encode_frame(&vec![0u8; MAX_FRAME + 1]).is_err());
        let mut header = (MAX_FRAME as u32 + 1).to_be_bytes().to_vec();
        header.extend_from_slice(b"x");
        assert!(decode_frame(&header).is_err());
    }

    #[tokio::test]
    async fn native_ping_unauthorized() {
        let gateway = Gateway::new(
            String::from("t"),
            String::from("d"),
            String::from("main"),
            ryme_auth::PolicyEngine::new(),
            ryme_realtime::Realtime::new(16),
        );
        let native = NativeGateway::with_gateway(gateway, ApiKeyStore::new());
        let body = serde_json::to_vec(&Request {
            key: String::from("bad"),
            op: String::from("ping"),
            table: String::new(),
            pk: String::new(),
            value: String::new(),
            sql: String::new(),
            limit: 0,
            ttl_secs: 0,
        })
        .unwrap();
        let response = native.dispatch(&body).await;
        assert!(!response.ok);
    }

    #[tokio::test]
    async fn native_put_get_roundtrip() {
        use std::collections::HashSet;
        let gateway = Gateway::new(
            String::from("t"),
            String::from("d"),
            String::from("main"),
            ryme_auth::PolicyEngine::new(),
            ryme_realtime::Realtime::new(16),
        );
        let keys = ApiKeyStore::new();
        let mut roles = HashSet::new();
        roles.insert(ryme_auth::Role::Owner);
        keys.insert(
            String::from("k"),
            Principal { id: String::from("u"), tenant: String::from("t"), roles },
        );
        let native = NativeGateway::with_gateway(gateway, keys);
        let put = serde_json::to_vec(&Request {
            key: String::from("k"),
            op: String::from("put"),
            table: String::from("docs"),
            pk: String::from("a"),
            value: String::from("1"),
            sql: String::new(),
            limit: 0,
            ttl_secs: 0,
        })
        .unwrap();
        let put_response = native.dispatch(&put).await;
        assert!(put_response.ok);
        let get = serde_json::to_vec(&Request {
            key: String::from("k"),
            op: String::from("get"),
            table: String::from("docs"),
            pk: String::from("a"),
            value: String::new(),
            sql: String::new(),
            limit: 0,
            ttl_secs: 0,
        })
        .unwrap();
        let get_response = native.dispatch(&get).await;
        assert!(get_response.ok);
        assert_eq!(get_response.value, Some(String::from("1")));
    }

    #[tokio::test]
    async fn native_egress_throttles_responses() {
        use std::collections::HashSet;
        let gateway = Gateway::new(
            String::from("t"),
            String::from("d"),
            String::from("main"),
            ryme_auth::PolicyEngine::new(),
            ryme_realtime::Realtime::new(16),
        );
        let keys = ApiKeyStore::new();
        let mut roles = HashSet::new();
        roles.insert(ryme_auth::Role::Owner);
        keys.insert(
            String::from("k"),
            Principal { id: String::from("u"), tenant: String::from("t"), roles },
        );
        let qos = Arc::new(Mutex::new(ryme_qos::QosRegistry::new()));
        {
            let mut registry = qos.lock().unwrap();
            registry.set_quota(
                "t",
                ryme_qos::Quota {
                    egress_bytes_per_sec: 512,
                    ..ryme_qos::Quota::for_tier(ryme_qos::Tier::Shared)
                },
                0,
            );
        }
        let native = NativeGateway::with_qos(gateway, keys, qos);
        let put = serde_json::to_vec(&Request {
            key: String::from("k"),
            op: String::from("put"),
            table: String::from("docs"),
            pk: String::from("a"),
            value: "v".repeat(200),
            sql: String::new(),
            limit: 0,
            ttl_secs: 0,
        })
        .unwrap();
        assert!(native.dispatch(&put).await.ok);
        let get = serde_json::to_vec(&Request {
            key: String::from("k"),
            op: String::from("get"),
            table: String::from("docs"),
            pk: String::from("a"),
            value: String::new(),
            sql: String::new(),
            limit: 0,
            ttl_secs: 0,
        })
        .unwrap();
        assert!(native.dispatch(&get).await.ok);
        let denied = native.dispatch(&get).await;
        assert!(!denied.ok);
        assert!(denied.error.unwrap_or_default().contains("egress"));
    }
}
