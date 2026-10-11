use clap::{Parser, Subcommand};
use futures_util::{SinkExt, StreamExt};
use ryme_storage::RecordKey;
use ryme_txn::TxnManager;
use serde::Deserialize;
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_tungstenite::tungstenite::Message;

#[derive(Debug, Parser)]
struct Args {
    #[command(subcommand)]
    command: Option<Command>,
    #[arg(long, default_value_t = 200000)]
    ops: usize,
    #[arg(long, default_value_t = 8)]
    concurrency: usize,
    #[arg(long, default_value_t = 128)]
    value_bytes: usize,
}

#[derive(Debug, Subcommand)]
enum Command {
    Local {
        #[arg(long, default_value_t = 200000)]
        ops: usize,
        #[arg(long, default_value_t = 128)]
        value_bytes: usize,
    },
    Resp {
        #[arg(long, default_value = "127.0.0.1:6380")]
        addr: String,
        #[arg(long, default_value_t = 100000)]
        ops: usize,
        #[arg(long, default_value_t = 8)]
        concurrency: usize,
        #[arg(long, default_value_t = 128)]
        value_bytes: usize,
        #[arg(long, default_value_t = 10000)]
        keyspace: usize,
        #[arg(long, default_value_t = 0)]
        hotspot_percent: u64,
        #[arg(long, default_value = "mixed")]
        workload: String,
        #[arg(long, default_value_t = 1)]
        pipeline: usize,
        #[arg(long)]
        json: bool,
    },
    Sql {
        #[arg(long, default_value = "127.0.0.1:5433")]
        addr: String,
        #[arg(long, default_value = "postgres")]
        user: String,
        #[arg(long, default_value = "postgres")]
        database: String,
        #[arg(long)]
        password: Option<String>,
        #[arg(long, default_value_t = 10000)]
        ops: usize,
        #[arg(long, default_value_t = 8)]
        concurrency: usize,
        #[arg(long, default_value = "mixed")]
        workload: String,
        #[arg(long)]
        prepared: bool,
        #[arg(long)]
        json: bool,
    },
    Realtime {
        #[arg(long, default_value = "127.0.0.1:3000")]
        addr: String,
        #[arg(long, default_value = "ryme-dev-key")]
        api_key: String,
        #[arg(long, default_value = "bench")]
        channel: String,
        #[arg(long, default_value_t = 100)]
        connections: usize,
        #[arg(long, default_value_t = 10000)]
        messages: usize,
        #[arg(long, default_value_t = 128)]
        payload_bytes: usize,
        #[arg(long, default_value_t = 32)]
        publish_concurrency: usize,
        #[arg(long, default_value_t = 256)]
        connection_concurrency: usize,
        #[arg(long, default_value_t = 100)]
        warmup_ms: u64,
        #[arg(long)]
        json: bool,
    },
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    match args.command {
        Some(Command::Local { ops, value_bytes }) => {
            local_bench(ops, value_bytes);
        }
        Some(Command::Resp {
            addr,
            ops,
            concurrency,
            value_bytes,
            keyspace,
            hotspot_percent,
            workload,
            pipeline,
            json,
        }) => {
            resp_bench(RespBench {
                addr,
                ops,
                concurrency,
                value_bytes,
                keyspace,
                hotspot_percent,
                workload,
                pipeline,
                json,
            })
            .await;
        }
        Some(Command::Sql {
            addr,
            user,
            database,
            password,
            ops,
            concurrency,
            workload,
            prepared,
            json,
        }) => {
            sql_bench(SqlBench {
                addr,
                user,
                database,
                password: password.or_else(|| std::env::var("PGPASSWORD").ok()),
                ops,
                concurrency,
                workload,
                prepared,
                json,
            })
            .await;
        }
        Some(Command::Realtime {
            addr,
            api_key,
            channel,
            connections,
            messages,
            payload_bytes,
            publish_concurrency,
            connection_concurrency,
            warmup_ms,
            json,
        }) => {
            realtime_bench(RealtimeBench {
                addr,
                api_key,
                channel,
                connections,
                messages,
                payload_bytes,
                publish_concurrency,
                connection_concurrency,
                warmup_ms,
                json,
            })
            .await;
        }
        None => {
            local_bench(args.ops, args.value_bytes);
        }
    }
}

struct RealtimeBench {
    addr: String,
    api_key: String,
    channel: String,
    connections: usize,
    messages: usize,
    payload_bytes: usize,
    publish_concurrency: usize,
    connection_concurrency: usize,
    warmup_ms: u64,
    json: bool,
}

struct RealtimeResult {
    published: usize,
    publish_errors: usize,
    received: usize,
    receive_errors: usize,
    connections: usize,
    messages: usize,
    publish_seconds: f64,
    total_seconds: f64,
}

#[derive(Deserialize)]
struct RealtimeEventSample {
    payload: RealtimePayloadSample,
}

#[derive(Deserialize)]
struct RealtimePayloadSample {
    sent_ns: u64,
}

async fn realtime_bench(config: RealtimeBench) {
    if config.connections == 0 || config.messages == 0 {
        eprintln!("connections and messages must be greater than zero");
        return;
    }
    let benchmark_start = Instant::now();
    let ws_url =
        format!("ws://{}/v1/broadcast/{}?api_key={}", config.addr, config.channel, config.api_key);
    let connection_results = futures_util::stream::iter(0..config.connections)
        .map(|_| {
            let ws_url = ws_url.clone();
            async move {
                tokio::time::timeout(
                    std::time::Duration::from_secs(15),
                    tokio_tungstenite::connect_async(ws_url),
                )
                .await
            }
        })
        .buffer_unordered(config.connection_concurrency.max(1))
        .collect::<Vec<_>>()
        .await;
    let mut streams = Vec::with_capacity(config.connections);
    for result in connection_results {
        match result {
            Ok(Ok((stream, _))) => streams.push(stream),
            Ok(Err(error)) => {
                eprintln!(
                    "realtime connection failed after {} connections: {error}",
                    streams.len()
                );
                return;
            }
            Err(_) => {
                eprintln!("realtime connection timed out after {} connections", streams.len());
                return;
            }
        }
    }
    tokio::time::sleep(std::time::Duration::from_millis(config.warmup_ms)).await;

    let (published_target, published_target_rx) = tokio::sync::watch::channel(None::<usize>);
    let (readers_start, reader_start_rx) = tokio::sync::watch::channel(false);
    let mut readers = Vec::with_capacity(streams.len());
    let latency_sample_limit = (1_000_000 / config.connections.max(1)).max(1);
    for mut stream in streams {
        let mut published_target_rx = published_target_rx.clone();
        let mut reader_start_rx = reader_start_rx.clone();
        readers.push(tokio::spawn(async move {
            let _ = reader_start_rx.wait_for(|started| *started).await;
            let reader_start = Instant::now();
            let mut received = 0usize;
            let mut errors = 0usize;
            let mut latency_micros = Vec::with_capacity(latency_sample_limit);
            loop {
                if published_target_rx.borrow().is_some_and(|target| received >= target) {
                    break;
                }
                let next_message = tokio::select! {
                    _ = published_target_rx.changed() => continue,
                    message = tokio::time::timeout(
                        std::time::Duration::from_secs(60),
                        stream.next(),
                    ) => message,
                };
                match next_message {
                    Ok(Some(Ok(Message::Text(text)))) => {
                        received += 1;
                        if latency_micros.len() < latency_sample_limit {
                            if let Ok(event) =
                                serde_json::from_str::<RealtimeEventSample>(text.as_ref())
                            {
                                let now_ns =
                                    benchmark_start.elapsed().as_nanos().min(u64::MAX as u128)
                                        as u64;
                                if now_ns >= event.payload.sent_ns {
                                    latency_micros.push((now_ns - event.payload.sent_ns) / 1_000);
                                }
                            }
                        }
                    }
                    Ok(Some(Ok(Message::Ping(payload)))) => {
                        if stream.send(Message::Pong(payload)).await.is_err() {
                            errors += 1;
                            break;
                        }
                    }
                    Ok(Some(Ok(_))) => {}
                    Ok(Some(Err(_))) | Ok(None) | Err(_) => {
                        errors += 1;
                        break;
                    }
                }
            }
            (received, errors, latency_micros, reader_start.elapsed().as_micros())
        }));
    }

    readers_start.send_replace(true);
    let payload = serde_json::json!({"data": "x".repeat(config.payload_bytes)});
    let client = reqwest::Client::new();
    let publish_url = format!("http://{}/v1/broadcast", config.addr);
    let publish_start = Instant::now();
    let publish_results = futures_util::stream::iter(0..config.messages)
        .map(|index| {
            let client = client.clone();
            let publish_url = publish_url.clone();
            let api_key = config.api_key.clone();
            let channel = config.channel.clone();
            let payload = payload.clone();
            async move {
                client
                    .post(publish_url)
                    .bearer_auth(api_key)
                    .json(&serde_json::json!({
                        "channel": channel,
                        "payload": {
                            "index": index,
                            "sent_ns": benchmark_start.elapsed().as_nanos().min(u64::MAX as u128),
                            "data": payload["data"]
                        }
                    }))
                    .send()
                    .await
                    .map(|response| response.status().is_success())
                    .unwrap_or(false)
            }
        })
        .buffer_unordered(config.publish_concurrency.max(1))
        .collect::<Vec<_>>()
        .await;
    let publish_seconds = publish_start.elapsed().as_secs_f64().max(0.000001);
    let published = publish_results.iter().filter(|ok| **ok).count();
    let publish_errors = config.messages - published;
    published_target.send_replace(Some(published));

    let mut received = 0usize;
    let mut receive_errors = 0usize;
    let mut latency_micros = Vec::new();
    let mut slowest_reader_micros = 0u128;
    let mut min_reader_received = usize::MAX;
    let mut max_reader_received = 0usize;
    for reader in readers {
        match reader.await {
            Ok((count, errors, mut samples, reader_micros)) => {
                received += count;
                receive_errors += errors;
                latency_micros.append(&mut samples);
                slowest_reader_micros = slowest_reader_micros.max(reader_micros);
                min_reader_received = min_reader_received.min(count);
                max_reader_received = max_reader_received.max(count);
            }
            Err(_) => receive_errors += 1,
        }
    }
    let total_seconds = publish_start.elapsed().as_secs_f64().max(0.000001);
    latency_micros.sort_unstable();
    let latency_percentile = |percentile: f64| -> u64 {
        if latency_micros.is_empty() {
            return 0;
        }
        let index = ((percentile / 100.0) * latency_micros.len() as f64).floor() as usize;
        latency_micros[index.min(latency_micros.len() - 1)]
    };
    let latency_samples = latency_micros.len();
    let latency_p50 = latency_percentile(50.0);
    let latency_p95 = latency_percentile(95.0);
    let latency_p99 = latency_percentile(99.0);
    let latency_max = latency_micros.last().copied().unwrap_or(0);
    let result = RealtimeResult {
        published,
        publish_errors,
        received,
        receive_errors,
        connections: config.connections,
        messages: config.messages,
        publish_seconds,
        total_seconds,
    };
    let expected = result.connections * result.published;
    let delivery_percent =
        if expected == 0 { 0.0 } else { result.received as f64 * 100.0 / expected as f64 };
    let output = serde_json::json!({
        "target": config.addr,
        "channel": config.channel,
        "machine": machine_metadata(),
        "connections": result.connections,
        "messages": result.messages,
        "published": result.published,
        "publish_errors": result.publish_errors,
        "received": result.received,
        "receive_errors": result.receive_errors,
        "slowest_reader_ms": (slowest_reader_micros as f64 / 1000.0).round() / 1000.0,
        "min_reader_messages": min_reader_received,
        "max_reader_messages": max_reader_received,
        "delivery_percent": (delivery_percent * 100.0).round() / 100.0,
        "publish_qps": (result.published as f64 / result.publish_seconds).round() as u64,
        "fanout_deliveries_per_second":
            (result.received as f64 / result.total_seconds).round() as u64,
        "latency_samples": latency_samples,
        "publish_to_client_p50_us": latency_p50,
        "publish_to_client_p95_us": latency_p95,
        "publish_to_client_p99_us": latency_p99,
        "publish_to_client_max_us": latency_max,
        "publish_to_client_latency_histogram_us": latency_histogram(&latency_micros),
        "total_seconds": (result.total_seconds * 1000.0).round() / 1000.0,
    });
    if config.json {
        println!("{output}");
    } else {
        println!(
            "target={} connections={} messages={} published={} publish_errors={} received={} receive_errors={} delivery={delivery_percent:.2}% publish_qps={} fanout_deliveries_per_second={} latency_samples={} publish_to_client_p50={}us publish_to_client_p95={}us publish_to_client_p99={}us publish_to_client_max={}us total_seconds={:.3}",
            config.addr,
            result.connections,
            result.messages,
            result.published,
            result.publish_errors,
            result.received,
            result.receive_errors,
            (result.published as f64 / result.publish_seconds).round() as u64,
            (result.received as f64 / result.total_seconds).round() as u64,
            latency_samples,
            latency_p50,
            latency_p95,
            latency_p99,
            latency_max,
            result.total_seconds,
        );
    }
}

fn local_bench(ops: usize, value_bytes: usize) {
    let manager = TxnManager::new();
    let value = vec![7u8; value_bytes];
    let start = Instant::now();
    let mut applied = 0usize;
    for index in 0..ops {
        let key = RecordKey::new("bench", "bench", "kv", format!("k-{index}").as_bytes());
        let mut txn = manager.begin();
        manager.put(&mut txn, key, value.clone());
        if manager.commit(txn).is_ok() {
            applied += 1;
        }
    }
    let elapsed = start.elapsed();
    let seconds = elapsed.as_secs_f64().max(0.000001);
    let throughput = applied as f64 / seconds;
    println!("ops={applied} seconds={seconds:.3} qps={throughput:.0}");
    let probe_ops = 10000.min(ops);
    if probe_ops > 0 {
        let probe_start = Instant::now();
        let mut hits = 0usize;
        for index in 0..probe_ops {
            let key = RecordKey::new("bench", "bench", "kv", format!("k-{index}").as_bytes());
            let mut txn = manager.begin();
            if manager.get(&mut txn, &key).unwrap_or(None).is_some() {
                hits += 1;
            }
        }
        let probe_micros = probe_start.elapsed().as_micros().max(1);
        println!("probe_hits={hits} probe_mean_micros={}", probe_micros / probe_ops as u128);
    }
}

fn machine_metadata() -> serde_json::Value {
    let logical_cpus = std::thread::available_parallelism().map(|count| count.get()).unwrap_or(1);
    let mut metadata = serde_json::json!({
        "os": std::env::consts::OS,
        "arch": std::env::consts::ARCH,
        "logical_cpus": logical_cpus,
    });
    if let Ok(host) = std::env::var("RYME_BENCH_HOST") {
        metadata["host"] = serde_json::Value::String(host);
    }
    if let Ok(model) = std::fs::read_to_string("/proc/cpuinfo") {
        if let Some(model) = model.lines().find_map(|line| {
            line.strip_prefix("model name")
                .and_then(|value| value.strip_prefix(':'))
                .map(|value| value.trim().to_string())
        }) {
            metadata["cpu_model"] = serde_json::Value::String(model);
        }
    }
    metadata
}

fn latency_histogram(samples: &[u64]) -> Vec<serde_json::Value> {
    const UPPER_BOUNDS: [u64; 14] =
        [10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, u64::MAX];
    let mut counts = vec![0u64; UPPER_BOUNDS.len()];
    for sample in samples {
        let bucket = UPPER_BOUNDS
            .iter()
            .position(|upper| *sample <= *upper)
            .unwrap_or(UPPER_BOUNDS.len() - 1);
        counts[bucket] = counts[bucket].saturating_add(1);
    }
    UPPER_BOUNDS
        .into_iter()
        .zip(counts)
        .map(|(upper, count)| {
            serde_json::json!({
                "le_us": if upper == u64::MAX {
                    serde_json::Value::String(String::from("+Inf"))
                } else {
                    serde_json::Value::from(upper)
                },
                "count": count,
            })
        })
        .collect()
}

fn pick_key(counter: u64, keyspace: usize, hotspot_percent: u64, worker: usize) -> String {
    let hot_keys = keyspace.max(1) / 100;
    let hot = counter % 100 < hotspot_percent && hot_keys > 0;
    if hot {
        format!("k-{}", (counter * 2654435761u64 + worker as u64) % hot_keys.max(1) as u64)
    } else {
        format!("k-{}", (counter * 2654435761u64 + worker as u64 * 97) % keyspace.max(1) as u64)
    }
}

fn encode(parts: &[&[u8]]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", parts.len()).into_bytes();
    for part in parts {
        out.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
        out.extend_from_slice(part);
        out.extend_from_slice(b"\r\n");
    }
    out
}

async fn read_reply(socket: &mut tokio::net::TcpStream, scratch: &mut Vec<u8>) -> Option<bool> {
    let mut chunk = vec![0u8; 65536];
    loop {
        if let Some(end) = find_frame_end(scratch) {
            let app_error = scratch.first() == Some(&b'-');
            scratch.drain(..end);
            return Some(app_error);
        }
        match socket.read(&mut chunk).await {
            Ok(0) => return None,
            Ok(read) => scratch.extend_from_slice(&chunk[..read]),
            Err(_) => return None,
        }
    }
}

fn worker_ops(total: usize, workers: usize, worker: usize) -> usize {
    let workers = workers.max(1);
    let base = total / workers;
    base + usize::from(worker < total % workers)
}

fn find_frame_end(buffer: &[u8]) -> Option<usize> {
    frame_len(buffer)
}

fn frame_len(buffer: &[u8]) -> Option<usize> {
    if buffer.is_empty() {
        return None;
    }
    match buffer[0] {
        b'+' | b'-' | b':' => buffer.windows(2).position(|w| w == b"\r\n").map(|index| index + 2),
        b'$' => {
            let end = buffer.windows(2).position(|w| w == b"\r\n")?;
            let length: i64 = String::from_utf8_lossy(&buffer[1..end]).parse().ok()?;
            if length < 0 {
                return Some(end + 2);
            }
            let total = end + 2 + length as usize + 2;
            if buffer.len() >= total { Some(total) } else { None }
        }
        b'*' => {
            let end = buffer.windows(2).position(|w| w == b"\r\n")?;
            let count: i64 = String::from_utf8_lossy(&buffer[1..end]).parse().ok()?;
            if count < 0 {
                return Some(end + 2);
            }
            let mut offset = end + 2;
            for _ in 0..count {
                offset += frame_len(&buffer[offset..])?;
            }
            Some(offset)
        }
        _ => None,
    }
}

struct RespBench {
    addr: String,
    ops: usize,
    concurrency: usize,
    value_bytes: usize,
    keyspace: usize,
    hotspot_percent: u64,
    workload: String,
    pipeline: usize,
    json: bool,
}

struct SqlBench {
    addr: String,
    user: String,
    database: String,
    password: Option<String>,
    ops: usize,
    concurrency: usize,
    workload: String,
    prepared: bool,
    json: bool,
}

struct SqlTask {
    addr: String,
    user: String,
    database: String,
    password: Option<String>,
    ops: usize,
    worker: usize,
    workload: String,
    prepared: bool,
    ready_gate: Arc<tokio::sync::Barrier>,
    start_gate: Arc<tokio::sync::Barrier>,
}

async fn pg_message(stream: &mut tokio::net::TcpStream) -> Result<(u8, Vec<u8>), String> {
    let mut kind = [0u8; 1];
    stream.read_exact(&mut kind).await.map_err(|error| error.to_string())?;
    let mut length = [0u8; 4];
    stream.read_exact(&mut length).await.map_err(|error| error.to_string())?;
    let length = u32::from_be_bytes(length) as usize;
    if !(4..=16 * 1024 * 1024).contains(&length) {
        return Err(String::from("invalid PostgreSQL message length"));
    }
    let mut body = vec![0u8; length - 4];
    stream.read_exact(&mut body).await.map_err(|error| error.to_string())?;
    Ok((kind[0], body))
}

async fn pg_connect(
    addr: &str,
    user: &str,
    database: &str,
    password: Option<&str>,
) -> Result<tokio::net::TcpStream, String> {
    let mut stream =
        tokio::net::TcpStream::connect(addr).await.map_err(|error| error.to_string())?;
    stream.set_nodelay(true).map_err(|error| error.to_string())?;
    let mut startup = Vec::new();
    startup.extend_from_slice(&196_608u32.to_be_bytes());
    startup.extend_from_slice(b"user\0");
    startup.extend_from_slice(user.as_bytes());
    startup.push(0);
    startup.extend_from_slice(b"database\0");
    startup.extend_from_slice(database.as_bytes());
    startup.push(0);
    startup.extend_from_slice(b"application_name\0ryme-bench\0\0");
    let length = u32::try_from(startup.len() + 4).map_err(|_| String::from("startup too large"))?;
    let mut frame = length.to_be_bytes().to_vec();
    frame.extend_from_slice(&startup);
    stream.write_all(&frame).await.map_err(|error| error.to_string())?;
    loop {
        let (kind, body) = pg_message(&mut stream).await?;
        match kind {
            b'R' if body.len() >= 4 && u32::from_be_bytes(body[..4].try_into().unwrap()) == 0 => {}
            b'R' if body.len() >= 4 && u32::from_be_bytes(body[..4].try_into().unwrap()) == 3 => {
                let password =
                    password.ok_or_else(|| String::from("PostgreSQL password required"))?;
                let length = u32::try_from(password.len() + 5)
                    .map_err(|_| String::from("password too large"))?;
                stream.write_all(b"p").await.map_err(|error| error.to_string())?;
                stream.write_all(&length.to_be_bytes()).await.map_err(|error| error.to_string())?;
                stream.write_all(password.as_bytes()).await.map_err(|error| error.to_string())?;
                stream.write_all(&[0]).await.map_err(|error| error.to_string())?;
            }
            b'R' => return Err(String::from("PostgreSQL server requested authentication")),
            b'E' => return Err(String::from("PostgreSQL startup failed")),
            b'Z' => return Ok(stream),
            _ => {}
        }
    }
}

async fn pg_query(stream: &mut tokio::net::TcpStream, query: &str) -> Result<(), String> {
    let query = query.as_bytes();
    let length = u32::try_from(query.len() + 5).map_err(|_| String::from("query too large"))?;
    stream.write_all(b"Q").await.map_err(|error| error.to_string())?;
    stream.write_all(&length.to_be_bytes()).await.map_err(|error| error.to_string())?;
    stream.write_all(query).await.map_err(|error| error.to_string())?;
    stream.write_all(&[0]).await.map_err(|error| error.to_string())?;
    let mut failed = None;
    loop {
        let (kind, _) = pg_message(stream).await?;
        match kind {
            b'E' => failed = Some(String::from("PostgreSQL query failed")),
            b'Z' => return failed.map_or(Ok(()), Err),
            _ => {}
        }
    }
}

fn pg_frame(kind: u8, body: &[u8]) -> Result<Vec<u8>, String> {
    let length = u32::try_from(body.len() + 4).map_err(|_| String::from("message too large"))?;
    let mut frame = Vec::with_capacity(body.len() + 5);
    frame.push(kind);
    frame.extend_from_slice(&length.to_be_bytes());
    frame.extend_from_slice(body);
    Ok(frame)
}

fn pg_cstring(body: &mut Vec<u8>, value: &str) {
    body.extend_from_slice(value.as_bytes());
    body.push(0);
}

async fn pg_prepare(
    stream: &mut tokio::net::TcpStream,
    name: &str,
    query: &str,
) -> Result<(), String> {
    let mut parse = Vec::new();
    pg_cstring(&mut parse, name);
    pg_cstring(&mut parse, query);
    parse.extend_from_slice(&0u16.to_be_bytes());
    let mut frame = pg_frame(b'P', &parse)?;
    frame.extend_from_slice(&pg_frame(b'S', &[])?);
    stream.write_all(&frame).await.map_err(|error| error.to_string())?;
    let mut failed = None;
    loop {
        let (kind, _) = pg_message(stream).await?;
        match kind {
            b'E' => failed = Some(String::from("PostgreSQL prepare failed")),
            b'Z' => return failed.map_or(Ok(()), Err),
            _ => {}
        }
    }
}

async fn pg_execute_prepared(
    stream: &mut tokio::net::TcpStream,
    name: &str,
    parameter: &str,
) -> Result<(), String> {
    let mut bind = Vec::new();
    pg_cstring(&mut bind, "");
    pg_cstring(&mut bind, name);
    bind.extend_from_slice(&1u16.to_be_bytes());
    bind.extend_from_slice(&0u16.to_be_bytes());
    bind.extend_from_slice(&1u16.to_be_bytes());
    let parameter = parameter.as_bytes();
    let length = i32::try_from(parameter.len()).map_err(|_| String::from("parameter too large"))?;
    bind.extend_from_slice(&length.to_be_bytes());
    bind.extend_from_slice(parameter);
    bind.extend_from_slice(&0u16.to_be_bytes());

    let mut execute = Vec::new();
    pg_cstring(&mut execute, "");
    execute.extend_from_slice(&0u32.to_be_bytes());
    let mut frame = pg_frame(b'B', &bind)?;
    frame.extend_from_slice(&pg_frame(b'E', &execute)?);
    frame.extend_from_slice(&pg_frame(b'S', &[])?);
    stream.write_all(&frame).await.map_err(|error| error.to_string())?;
    let mut failed = None;
    loop {
        let (kind, _) = pg_message(stream).await?;
        match kind {
            b'E' => failed = Some(String::from("PostgreSQL prepared query failed")),
            b'Z' => return failed.map_or(Ok(()), Err),
            _ => {}
        }
    }
}

async fn run_sql_worker(task: SqlTask) -> (Vec<u64>, u64) {
    let mut stream =
        (pg_connect(&task.addr, &task.user, &task.database, task.password.as_deref()).await).ok();
    let setup_ok = if let Some(stream) = stream.as_mut() {
        pg_query(
            stream,
            "CREATE TABLE IF NOT EXISTS ryme_bench_sql (id TEXT PRIMARY KEY, payload TEXT)",
        )
        .await
        .is_ok()
    } else {
        false
    };
    task.ready_gate.wait().await;
    task.start_gate.wait().await;
    let Some(mut stream) = stream else {
        return (Vec::new(), task.ops as u64);
    };
    if !setup_ok {
        return (Vec::new(), task.ops as u64);
    }
    let prepared_queries = if task.prepared {
        for (name, query) in [
            ("ryme_bench_select", "SELECT payload FROM ryme_bench_sql WHERE id = $1"),
            (
                "ryme_bench_insert",
                "INSERT INTO ryme_bench_sql (id, payload) VALUES ($1, 'payload') ON CONFLICT (id) DO UPDATE SET payload = EXCLUDED.payload",
            ),
        ] {
            if pg_prepare(&mut stream, name, query).await.is_err() {
                return (Vec::new(), task.ops as u64);
            }
        }
        true
    } else {
        false
    };
    let mut latencies = Vec::with_capacity(task.ops);
    let mut errors = 0u64;
    for index in 0..task.ops {
        let key = format!("k-{}", index as u64 * 2_654_435_761 + task.worker as u64);
        let query = match task.workload.as_str() {
            "select" => format!("SELECT payload FROM ryme_bench_sql WHERE id = '{key}'"),
            "insert" => format!(
                "INSERT INTO ryme_bench_sql (id, payload) VALUES ('{key}', 'payload') ON CONFLICT (id) DO UPDATE SET payload = EXCLUDED.payload"
            ),
            _ if index.is_multiple_of(2) => {
                format!("SELECT payload FROM ryme_bench_sql WHERE id = '{key}'")
            }
            _ => format!(
                "INSERT INTO ryme_bench_sql (id, payload) VALUES ('{key}', 'payload') ON CONFLICT (id) DO UPDATE SET payload = EXCLUDED.payload"
            ),
        };
        let started = Instant::now();
        let query_result = if prepared_queries {
            let name = if task.workload == "select" || index.is_multiple_of(2) {
                "ryme_bench_select"
            } else {
                "ryme_bench_insert"
            };
            pg_execute_prepared(&mut stream, name, &key).await
        } else {
            pg_query(&mut stream, &query).await
        };
        if query_result.is_ok() {
            latencies.push(started.elapsed().as_micros() as u64);
        } else {
            errors = errors.saturating_add(1);
        }
    }
    (latencies, errors)
}

async fn sql_bench(config: SqlBench) {
    let workers = config.concurrency.max(1);
    let ready_gate = Arc::new(tokio::sync::Barrier::new(workers + 1));
    let start_gate = Arc::new(tokio::sync::Barrier::new(workers + 1));
    let mut handles = Vec::with_capacity(workers);
    for worker in 0..workers {
        handles.push(tokio::spawn(run_sql_worker(SqlTask {
            addr: config.addr.clone(),
            user: config.user.clone(),
            database: config.database.clone(),
            password: config.password.clone(),
            ops: worker_ops(config.ops, workers, worker),
            worker,
            workload: config.workload.clone(),
            prepared: config.prepared,
            ready_gate: ready_gate.clone(),
            start_gate: start_gate.clone(),
        })));
    }
    ready_gate.wait().await;
    let started = Instant::now();
    start_gate.wait().await;
    let mut samples = Vec::new();
    let mut errors = 0u64;
    for handle in handles {
        let (mut worker_samples, worker_errors) = handle.await.unwrap();
        samples.append(&mut worker_samples);
        errors = errors.saturating_add(worker_errors);
    }
    let seconds = started.elapsed().as_secs_f64().max(0.000001);
    samples.sort_unstable();
    let count = samples.len();
    let percentile = |p: f64| -> u64 {
        if count == 0 {
            return 0;
        }
        samples[((p / 100.0) * count as f64).floor() as usize % count]
    };
    let output = serde_json::json!({
        "target": config.addr,
        "protocol": if config.prepared {
            "postgres-extended-prepared"
        } else {
            "postgres-simple-query"
        },
        "database": config.database,
        "workload": config.workload,
        "prepared": config.prepared,
        "machine": machine_metadata(),
        "requested_ops": config.ops,
        "successful_ops": count,
        "failed_ops": errors,
        "seconds": (seconds * 1000.0).round() / 1000.0,
        "qps": (count as f64 / seconds).round() as u64,
        "p50_us": percentile(50.0),
        "p90_us": percentile(90.0),
        "p95_us": percentile(95.0),
        "p99_us": percentile(99.0),
        "p999_us": percentile(99.9),
        "max_us": samples.last().copied().unwrap_or(0),
        "latency_histogram_us": latency_histogram(&samples),
    });
    if config.json {
        println!("{output}");
    } else {
        println!(
            "target={} protocol={} workload={} prepared={} requested_ops={} successful_ops={} failed_ops={} qps={:.0} p50={}us p99={}us max={}us",
            output["target"].as_str().unwrap_or(""),
            output["protocol"].as_str().unwrap_or(""),
            output["workload"].as_str().unwrap_or(""),
            config.prepared,
            config.ops,
            count,
            errors,
            count as f64 / seconds,
            percentile(50.0),
            percentile(99.0),
            samples.last().copied().unwrap_or(0),
        );
    }
}

struct TaskConfig {
    addr: String,
    ops: usize,
    worker: usize,
    value: Vec<u8>,
    keyspace: usize,
    hotspot_percent: u64,
    workload: String,
    pipeline: usize,
    ready_gate: Arc<tokio::sync::Barrier>,
    start_gate: Arc<tokio::sync::Barrier>,
}

async fn run_worker(task: TaskConfig) -> (Vec<u64>, u64) {
    let socket = match tokio::net::TcpStream::connect(&task.addr).await {
        Ok(socket) if socket.set_nodelay(true).is_ok() => Some(socket),
        _ => None,
    };
    task.ready_gate.wait().await;
    task.start_gate.wait().await;
    let mut socket = match socket {
        Some(socket) => socket,
        None => return (Vec::new(), task.ops as u64),
    };
    let mut scratch = Vec::new();
    let mut latencies: Vec<u64> = Vec::with_capacity(task.ops);
    let mut errors = 0u64;
    let mut index = 0usize;
    while index < task.ops {
        if task.workload == "tx" {
            let counter = index as u64;
            let key = pick_key(counter, task.keyspace, task.hotspot_percent, task.worker);
            let key_bytes = key.into_bytes();
            let counter_key = format!("txc-{}-{}", task.worker, counter % 128);
            let start = Instant::now();
            let frames = [
                encode(&[b"MULTI"]),
                encode(&[b"SET", &key_bytes, &task.value]),
                encode(&[b"INCRBY", counter_key.as_bytes(), b"1"]),
                encode(&[b"GET", &key_bytes]),
                encode(&[b"EXEC"]),
            ];
            let mut write_failed = false;
            for frame in frames {
                if socket.write_all(&frame).await.is_err() {
                    write_failed = true;
                    break;
                }
            }
            if write_failed {
                errors += (task.ops - index) as u64;
                break;
            }
            let mut replies = 0u8;
            let mut failed = false;
            let mut connection_lost = false;
            for _ in 0..5 {
                match read_reply(&mut socket, &mut scratch).await {
                    Some(false) => replies += 1,
                    Some(true) => failed = true,
                    None => {
                        failed = true;
                        connection_lost = true;
                        break;
                    }
                }
            }
            let micros = start.elapsed().as_micros() as u64;
            if !failed && replies == 5 {
                latencies.push(micros);
            } else if connection_lost {
                errors += (task.ops - index) as u64;
                break;
            } else {
                errors += 1;
            }
            index += 1;
            continue;
        }
        let batch = task.pipeline.max(1).min(task.ops - index);
        let start = Instant::now();
        let mut write_failed = false;
        for offset in 0..batch {
            let counter = (index + offset) as u64;
            let key = pick_key(counter, task.keyspace, task.hotspot_percent, task.worker);
            let write = match task.workload.as_str() {
                "get" => false,
                "set" => true,
                _ => counter.is_multiple_of(2),
            };
            if write {
                let key_bytes = key.into_bytes();
                let frame = encode(&[b"SET", &key_bytes, &task.value]);
                if socket.write_all(&frame).await.is_err() {
                    write_failed = true;
                    break;
                }
            } else {
                let key_bytes = key.into_bytes();
                let frame = encode(&[b"GET", &key_bytes]);
                if socket.write_all(&frame).await.is_err() {
                    write_failed = true;
                    break;
                }
            }
        }
        if write_failed {
            errors += (task.ops - index) as u64;
            break;
        }
        let mut ok = true;
        let mut connection_lost = false;
        let mut batch_errors = 0u64;
        for _ in 0..batch {
            match read_reply(&mut socket, &mut scratch).await {
                Some(false) => {}
                Some(true) => batch_errors += 1,
                None => {
                    ok = false;
                    connection_lost = true;
                    break;
                }
            }
        }
        let micros = start.elapsed().as_micros() as u64;
        if ok {
            errors += batch_errors;
            for _ in 0..batch.saturating_sub(batch_errors as usize) {
                latencies.push(micros / batch as u64);
            }
        } else if connection_lost {
            errors += (task.ops - index) as u64;
            break;
        } else {
            errors += batch as u64;
        }
        index += batch;
    }
    (latencies, errors)
}

async fn resp_bench(config: RespBench) {
    let value = vec![7u8; config.value_bytes];
    let workers = config.concurrency.max(1);
    let ready_gate = Arc::new(tokio::sync::Barrier::new(workers + 1));
    let start_gate = Arc::new(tokio::sync::Barrier::new(workers + 1));
    let mut handles = Vec::new();
    for worker in 0..workers {
        let task = TaskConfig {
            addr: config.addr.clone(),
            ops: worker_ops(config.ops, workers, worker),
            worker,
            value: value.clone(),
            keyspace: config.keyspace,
            hotspot_percent: config.hotspot_percent,
            workload: config.workload.clone(),
            pipeline: config.pipeline,
            ready_gate: ready_gate.clone(),
            start_gate: start_gate.clone(),
        };
        handles.push(tokio::spawn(async move { run_worker(task).await }));
    }
    ready_gate.wait().await;
    let start = Instant::now();
    start_gate.wait().await;
    let mut all: Vec<u64> = Vec::new();
    let mut errors = 0u64;
    for handle in handles {
        let (mut latencies, task_errors) = handle.await.unwrap();
        errors += task_errors;
        all.append(&mut latencies);
    }
    let elapsed = start.elapsed().as_secs_f64().max(0.000001);
    all.sort_unstable();
    let count = all.len();
    let percentile = |p: f64| -> u64 {
        if count == 0 {
            return 0;
        }
        all[((p / 100.0) * count as f64).floor() as usize % count]
    };
    let mean = if count == 0 { 0 } else { all.iter().sum::<u64>() / count as u64 };
    let result = serde_json::json!({
        "target": config.addr,
        "machine": machine_metadata(),
        "workload": config.workload,
        "pipeline": config.pipeline,
        "ops": count,
        "requested_ops": config.ops,
        "successful_ops": count,
        "failed_ops": errors,
        "errors": errors,
        "seconds": (elapsed * 1000.0).round() / 1000.0,
        "qps": (count as f64 / elapsed).round() as u64,
        "attempted_qps": (config.ops as f64 / elapsed).round() as u64,
        "p50_us": percentile(50.0),
        "p90_us": percentile(90.0),
        "p95_us": percentile(95.0),
        "p99_us": percentile(99.0),
        "p999_us": percentile(99.9),
        "max_us": all.last().copied().unwrap_or(0),
        "mean_us": mean,
        "latency_histogram_us": latency_histogram(&all),
    });
    if config.json {
        println!("{result}");
    } else {
        println!(
            "target={} workload={} pipeline={} requested_ops={} successful_ops={count} failed_ops={errors} qps={:.0} attempted_qps={:.0} p50={}us p90={}us p95={}us p99={}us p999={}us max={}us",
            config.addr,
            config.workload,
            config.pipeline,
            config.ops,
            count as f64 / elapsed,
            config.ops as f64 / elapsed,
            percentile(50.0),
            percentile(90.0),
            percentile(95.0),
            percentile(99.0),
            percentile(99.9),
            all.last().copied().unwrap_or(0),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::{latency_histogram, worker_ops};

    #[test]
    fn worker_split_preserves_requested_operations() {
        for total in 0..32 {
            for workers in 1..8 {
                let assigned: usize =
                    (0..workers).map(|worker| worker_ops(total, workers, worker)).sum();
                assert_eq!(assigned, total);
                let counts: Vec<_> =
                    (0..workers).map(|worker| worker_ops(total, workers, worker)).collect();
                assert!(counts.windows(2).all(|pair| pair[0].abs_diff(pair[1]) <= 1));
            }
        }
    }

    #[test]
    fn latency_histogram_covers_each_sample_once() {
        let histogram = latency_histogram(&[1, 10, 11, 100_000, 100_001]);
        let total: u64 = histogram.iter().map(|bucket| bucket["count"].as_u64().unwrap()).sum();
        assert_eq!(total, 5);
        assert_eq!(histogram.last().unwrap()["count"], 1);
    }
}
