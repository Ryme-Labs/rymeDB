use clap::{Parser, Subcommand};
use futures_util::{SinkExt, StreamExt};
use ryme_storage::RecordKey;
use ryme_txn::TxnManager;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
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
        Some(Command::Realtime {
            addr,
            api_key,
            channel,
            connections,
            messages,
            payload_bytes,
            publish_concurrency,
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

async fn realtime_bench(config: RealtimeBench) {
    if config.connections == 0 || config.messages == 0 {
        eprintln!("connections and messages must be greater than zero");
        return;
    }
    let ws_url =
        format!("ws://{}/v1/broadcast/{}?api_key={}", config.addr, config.channel, config.api_key);
    let mut streams = Vec::with_capacity(config.connections);
    for _ in 0..config.connections {
        match tokio::time::timeout(
            std::time::Duration::from_secs(15),
            tokio_tungstenite::connect_async(&ws_url),
        )
        .await
        {
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

    let published_target = Arc::new(AtomicUsize::new(usize::MAX));
    let mut readers = Vec::with_capacity(streams.len());
    for mut stream in streams {
        let published_target = published_target.clone();
        readers.push(tokio::spawn(async move {
            let mut received = 0usize;
            let mut errors = 0usize;
            loop {
                let target = published_target.load(Ordering::Acquire);
                if target == usize::MAX {
                    tokio::task::yield_now().await;
                    continue;
                }
                if received >= target {
                    break;
                }
                match tokio::time::timeout(std::time::Duration::from_secs(60), stream.next()).await
                {
                    Ok(Some(Ok(Message::Text(_)))) => received += 1,
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
            let _ = stream.close(None).await;
            (received, errors)
        }));
    }

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
                        "payload": {"index": index, "data": payload["data"]}
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
    published_target.store(published, Ordering::Release);

    let receive_start = Instant::now();
    let mut received = 0usize;
    let mut receive_errors = 0usize;
    for reader in readers {
        match reader.await {
            Ok((count, errors)) => {
                received += count;
                receive_errors += errors;
            }
            Err(_) => receive_errors += 1,
        }
    }
    let total_seconds = publish_start.elapsed().as_secs_f64().max(0.000001);
    let receive_seconds = receive_start.elapsed().as_secs_f64().max(0.000001);
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
        "connections": result.connections,
        "messages": result.messages,
        "published": result.published,
        "publish_errors": result.publish_errors,
        "received": result.received,
        "receive_errors": result.receive_errors,
        "delivery_percent": (delivery_percent * 100.0).round() / 100.0,
        "publish_qps": (result.published as f64 / result.publish_seconds).round() as u64,
        "fanout_deliveries_per_second":
            (result.received as f64 / receive_seconds).round() as u64,
        "total_seconds": (result.total_seconds * 1000.0).round() / 1000.0,
    });
    if config.json {
        println!("{output}");
    } else {
        println!(
            "target={} connections={} messages={} published={} publish_errors={} received={} receive_errors={} delivery={delivery_percent:.2}% publish_qps={} fanout_deliveries_per_second={} total_seconds={:.3}",
            config.addr,
            result.connections,
            result.messages,
            result.published,
            result.publish_errors,
            result.received,
            result.receive_errors,
            (result.published as f64 / result.publish_seconds).round() as u64,
            (result.received as f64 / receive_seconds).round() as u64,
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
    let probe_start = Instant::now();
    let mut hits = 0usize;
    for index in 0..10000.min(ops) {
        let key = RecordKey::new("bench", "bench", "kv", format!("k-{index}").as_bytes());
        let mut txn = manager.begin();
        if manager.get(&mut txn, &key).unwrap_or(None).is_some() {
            hits += 1;
        }
    }
    let probe_micros = probe_start.elapsed().as_micros().max(1);
    println!("probe_hits={hits} probe_mean_micros={}", probe_micros / 10000);
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
            if buffer.len() >= total {
                Some(total)
            } else {
                None
            }
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

struct TaskConfig {
    addr: String,
    ops: usize,
    worker: usize,
    value: Vec<u8>,
    keyspace: usize,
    hotspot_percent: u64,
    workload: String,
    pipeline: usize,
}

async fn run_worker(task: TaskConfig) -> (Vec<u64>, u64) {
    let mut socket = tokio::net::TcpStream::connect(&task.addr).await.unwrap();
    socket.set_nodelay(true).unwrap();
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
            socket.write_all(&encode(&[b"MULTI"])).await.unwrap();
            socket.write_all(&encode(&[b"SET", &key_bytes, &task.value])).await.unwrap();
            socket.write_all(&encode(&[b"INCRBY", counter_key.as_bytes(), b"1"])).await.unwrap();
            socket.write_all(&encode(&[b"GET", &key_bytes])).await.unwrap();
            socket.write_all(&encode(&[b"EXEC"])).await.unwrap();
            let mut replies = 0u8;
            let mut failed = false;
            for _ in 0..5 {
                match read_reply(&mut socket, &mut scratch).await {
                    Some(false) => replies += 1,
                    _ => {
                        failed = true;
                        break;
                    }
                }
            }
            let micros = start.elapsed().as_micros() as u64;
            if !failed && replies == 5 {
                latencies.push(micros);
            } else {
                errors += 1;
            }
            index += 1;
            continue;
        }
        let batch = task.pipeline.max(1).min(task.ops - index);
        let start = Instant::now();
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
                socket.write_all(&frame).await.unwrap();
            } else {
                let key_bytes = key.into_bytes();
                let frame = encode(&[b"GET", &key_bytes]);
                socket.write_all(&frame).await.unwrap();
            }
        }
        let mut ok = true;
        let mut batch_errors = 0u64;
        for _ in 0..batch {
            match read_reply(&mut socket, &mut scratch).await {
                Some(false) => {}
                Some(true) => batch_errors += 1,
                None => {
                    ok = false;
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
        } else {
            errors += batch as u64;
        }
        index += batch;
    }
    (latencies, errors)
}

async fn resp_bench(config: RespBench) {
    let value = vec![7u8; config.value_bytes];
    let per_worker = config.ops / config.concurrency.max(1);
    let mut handles = Vec::new();
    for worker in 0..config.concurrency.max(1) {
        let task = TaskConfig {
            addr: config.addr.clone(),
            ops: per_worker,
            worker,
            value: value.clone(),
            keyspace: config.keyspace,
            hotspot_percent: config.hotspot_percent,
            workload: config.workload.clone(),
            pipeline: config.pipeline,
        };
        handles.push(tokio::spawn(async move { run_worker(task).await }));
    }
    let mut all: Vec<u64> = Vec::new();
    let mut errors = 0u64;
    let start = Instant::now();
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
        "workload": config.workload,
        "pipeline": config.pipeline,
        "ops": count,
        "errors": errors,
        "seconds": (elapsed * 1000.0).round() / 1000.0,
        "qps": (count as f64 / elapsed).round() as u64,
        "p50_us": percentile(50.0),
        "p90_us": percentile(90.0),
        "p95_us": percentile(95.0),
        "p99_us": percentile(99.0),
        "p999_us": percentile(99.9),
        "max_us": all.last().copied().unwrap_or(0),
        "mean_us": mean,
    });
    if config.json {
        println!("{result}");
    } else {
        println!(
            "target={} workload={} pipeline={} ops={count} errors={errors} qps={:.0} p50={}us p90={}us p95={}us p99={}us p999={}us max={}us",
            config.addr,
            config.workload,
            config.pipeline,
            count as f64 / elapsed,
            percentile(50.0),
            percentile(90.0),
            percentile(95.0),
            percentile(99.0),
            percentile(99.9),
            all.last().copied().unwrap_or(0),
        );
    }
}
