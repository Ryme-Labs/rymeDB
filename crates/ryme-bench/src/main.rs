use clap::{Parser, Subcommand};
use futures_util::{SinkExt, StreamExt};
use ryme_index::PartitionedIndex;
use ryme_shard::{ShardSet, TableRef};
use ryme_storage::{Engine, ImmutableSegment, RecordKey, SegmentEntry, SegmentStore, StorageMode};
use ryme_txn::{DurableManager, SyncPolicy, Transaction, TxnBackend, TxnManager};
use serde::Deserialize;
use std::sync::{Arc, Barrier};
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
        #[arg(long, default_value_t = 8)]
        concurrency: usize,
        #[arg(long, default_value_t = 128)]
        value_bytes: usize,
        #[arg(long, help = "Number of distinct keys (defaults to ops)")]
        keyspace: Option<usize>,
    },
    ShardedLocal {
        #[arg(long, default_value_t = 20000)]
        ops: usize,
        #[arg(long, default_value_t = 4)]
        concurrency: usize,
        #[arg(long, default_value_t = 128)]
        value_bytes: usize,
        #[arg(long, default_value_t = 4)]
        shards: usize,
        #[arg(long, help = "Distinct keys per shard; defaults to ops")]
        keyspace: Option<usize>,
        #[arg(long, help = "Sync each write before acknowledging it")]
        sync_each_write: bool,
        #[arg(long, help = "Use segment-backed Standard mode instead of Hot mode")]
        standard_mode: bool,
        #[arg(long, default_value_t = 1, help = "Writes per transaction")]
        batch_size: usize,
    },
    SegmentBuild {
        #[arg(long, default_value_t = 1024)]
        entries: usize,
        #[arg(long, default_value_t = 128)]
        value_bytes: usize,
        #[arg(long, default_value_t = 30)]
        iterations: usize,
    },
    SegmentScan {
        #[arg(long, default_value_t = 10_000)]
        rows: usize,
        #[arg(long, default_value_t = 100)]
        deltas: usize,
        #[arg(long, default_value_t = 100)]
        limit: usize,
        #[arg(long, default_value_t = 30)]
        iterations: usize,
        #[arg(long, default_value_t = 1)]
        concurrency: usize,
        #[arg(long, default_value_t = 67_108_864)]
        cache_bytes: u64,
        #[arg(long, help = "Compact generated segments before scanning")]
        compact: bool,
    },
    SegmentStartup {
        #[arg(long, default_value_t = 10_000)]
        rows: usize,
        #[arg(long, default_value_t = 500)]
        superseded_deltas: usize,
        #[arg(long, default_value_t = 500)]
        deltas: usize,
        #[arg(long, default_value_t = 10)]
        iterations: usize,
    },
    SegmentPrune {
        #[arg(long, default_value_t = 10_000)]
        rows: usize,
        #[arg(long, default_value_t = 100)]
        deltas: usize,
        #[arg(long, default_value_t = 10)]
        iterations: usize,
    },
    VectorIndex {
        #[arg(long, default_value_t = 2_000)]
        vectors: usize,
        #[arg(long, default_value_t = 64)]
        dimensions: usize,
        #[arg(long, default_value_t = 200)]
        queries: usize,
        #[arg(long, default_value_t = 4)]
        partitions: usize,
        #[arg(long, default_value_t = 10)]
        top_k: usize,
        #[arg(long, default_value_t = 64)]
        ann_ef: usize,
        #[arg(long)]
        json: bool,
    },
    TextIndex {
        #[arg(long, default_value_t = 10_000)]
        documents: usize,
        #[arg(long, default_value_t = 300)]
        queries: usize,
        #[arg(long, default_value_t = 4)]
        partitions: usize,
        #[arg(long, default_value_t = 10)]
        top_k: usize,
        #[arg(long, default_value_t = 1, help = "Common query terms (1 or 2)")]
        query_terms: usize,
        #[arg(long)]
        json: bool,
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
    TxnReadBatch {
        #[arg(long, default_value_t = 10000)]
        records: usize,
        #[arg(long, default_value_t = 1024)]
        candidates: usize,
        #[arg(long, default_value_t = 100)]
        iterations: usize,
        #[arg(long, default_value_t = 256)]
        batch_size: usize,
        #[arg(long, default_value_t = 128)]
        value_bytes: usize,
        #[arg(long, help = "Benchmark disk-backed Standard storage mode")]
        standard_mode: bool,
        #[arg(long)]
        json: bool,
    },
    Realtime {
        #[arg(long, default_value = "127.0.0.1:3000")]
        addr: String,
        #[arg(long)]
        api_key: Option<String>,
        #[arg(long, default_value = "bench")]
        channel: String,
        #[arg(long, default_value_t = 1)]
        channel_count: usize,
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
        Some(Command::Local { ops, concurrency, value_bytes, keyspace }) => {
            local_bench(ops, concurrency, value_bytes, keyspace);
        }
        Some(Command::ShardedLocal {
            ops,
            concurrency,
            value_bytes,
            shards,
            keyspace,
            sync_each_write,
            standard_mode,
            batch_size,
        }) => {
            let mode = if standard_mode { StorageMode::Standard } else { StorageMode::Hot };
            sharded_local_bench(
                ops,
                concurrency,
                value_bytes,
                shards,
                keyspace,
                sync_each_write,
                mode,
                batch_size,
            );
        }
        Some(Command::SegmentBuild { entries, value_bytes, iterations }) => {
            segment_build_bench(entries, value_bytes, iterations);
        }
        Some(Command::SegmentScan {
            rows,
            deltas,
            limit,
            iterations,
            concurrency,
            cache_bytes,
            compact,
        }) => {
            segment_scan_bench(rows, deltas, limit, iterations, concurrency, cache_bytes, compact);
        }
        Some(Command::SegmentStartup { rows, superseded_deltas, deltas, iterations }) => {
            segment_startup_bench(rows, superseded_deltas, deltas, iterations);
        }
        Some(Command::SegmentPrune { rows, deltas, iterations }) => {
            segment_prune_bench(rows, deltas, iterations);
        }
        Some(Command::VectorIndex {
            vectors,
            dimensions,
            queries,
            partitions,
            top_k,
            ann_ef,
            json,
        }) => {
            vector_index_bench(vectors, dimensions, queries, partitions, top_k, ann_ef, json);
        }
        Some(Command::TextIndex { documents, queries, partitions, top_k, query_terms, json }) => {
            text_index_bench(documents, queries, partitions, top_k, query_terms, json);
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
        Some(Command::TxnReadBatch {
            records,
            candidates,
            iterations,
            batch_size,
            value_bytes,
            standard_mode,
            json,
        }) => txn_read_batch_bench(
            records,
            candidates,
            iterations,
            batch_size,
            value_bytes,
            standard_mode,
            json,
        ),
        Some(Command::Realtime {
            addr,
            api_key,
            channel,
            channel_count,
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
                api_key: api_key.or_else(|| std::env::var("RYME_API_KEY").ok()).unwrap_or_default(),
                channel,
                channel_count,
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
            local_bench(args.ops, args.concurrency, args.value_bytes, None);
        }
    }
}

struct RealtimeBench {
    addr: String,
    api_key: String,
    channel: String,
    channel_count: usize,
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
    if config.connections == 0 || config.messages == 0 || config.channel_count == 0 {
        eprintln!("connections, messages, and channel-count must be greater than zero");
        return;
    }
    if config.api_key.is_empty() {
        eprintln!("realtime benchmark requires --api-key or RYME_API_KEY");
        return;
    }
    let benchmark_start = Instant::now();
    let connection_results = futures_util::stream::iter(0..config.connections)
        .map(|connection| {
            let topic = connection % config.channel_count;
            let ws_url = format!(
                "ws://{}/v1/broadcast/{}?api_key={}",
                config.addr,
                realtime_channel(&config.channel, config.channel_count, topic),
                config.api_key
            );
            async move {
                tokio::time::timeout(
                    std::time::Duration::from_secs(15),
                    tokio_tungstenite::connect_async(ws_url),
                )
                .await
                .map(|result| (topic, result))
            }
        })
        .buffer_unordered(config.connection_concurrency.max(1))
        .collect::<Vec<_>>()
        .await;
    let mut streams = Vec::with_capacity(config.connections);
    let mut subscribers_per_channel = vec![0usize; config.channel_count];
    for result in connection_results {
        match result {
            Ok((topic, Ok((stream, _)))) => {
                subscribers_per_channel[topic] += 1;
                streams.push((topic, stream));
            }
            Ok((_, Err(error))) => {
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

    let (published_target, published_target_rx) = tokio::sync::watch::channel(None::<Vec<usize>>);
    let (readers_start, reader_start_rx) = tokio::sync::watch::channel(false);
    let mut readers = Vec::with_capacity(streams.len());
    let latency_sample_limit = (1_000_000 / config.connections.max(1)).max(1);
    for (topic, mut stream) in streams {
        let mut published_target_rx = published_target_rx.clone();
        let mut reader_start_rx = reader_start_rx.clone();
        readers.push(tokio::spawn(async move {
            let _ = reader_start_rx.wait_for(|started| *started).await;
            let reader_start = Instant::now();
            let mut received = 0usize;
            let mut errors = 0usize;
            let mut latency_micros = Vec::with_capacity(latency_sample_limit);
            loop {
                if published_target_rx
                    .borrow()
                    .as_ref()
                    .is_some_and(|targets| received >= targets[topic])
                {
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
            let topic = index % config.channel_count;
            let client = client.clone();
            let publish_url = publish_url.clone();
            let api_key = config.api_key.clone();
            let channel = realtime_channel(&config.channel, config.channel_count, topic);
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
                    .then_some(topic)
            }
        })
        .buffer_unordered(config.publish_concurrency.max(1))
        .collect::<Vec<_>>()
        .await;
    let publish_seconds = publish_start.elapsed().as_secs_f64().max(0.000001);
    let mut published_per_channel = vec![0usize; config.channel_count];
    for topic in publish_results.iter().flatten() {
        published_per_channel[*topic] += 1;
    }
    let published = publish_results.iter().flatten().count();
    let publish_errors = config.messages - published;
    published_target.send_replace(Some(published_per_channel.clone()));

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
    let expected = expected_fanout(&subscribers_per_channel, &published_per_channel);
    let delivery_percent =
        if expected == 0 { 0.0 } else { result.received as f64 * 100.0 / expected as f64 };
    let output = serde_json::json!({
        "target": config.addr,
        "channel": config.channel,
        "channel_count": config.channel_count,
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
            "target={} connections={} channels={} messages={} published={} publish_errors={} received={} receive_errors={} delivery={delivery_percent:.2}% publish_qps={} fanout_deliveries_per_second={} latency_samples={} publish_to_client_p50={}us publish_to_client_p95={}us publish_to_client_p99={}us publish_to_client_max={}us total_seconds={:.3}",
            config.addr,
            result.connections,
            config.channel_count,
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

fn local_bench(ops: usize, concurrency: usize, value_bytes: usize, keyspace: Option<usize>) {
    let manager = Arc::new(TxnManager::new());
    let value = vec![7u8; value_bytes];
    let workers = concurrency.max(1).min(ops.max(1));
    let keyspace = keyspace.unwrap_or(ops).max(1);
    let warmup_ops = if keyspace < ops { keyspace } else { 0 };
    for index in 0..warmup_ops {
        let key = RecordKey::new("bench", "bench", "kv", format!("k-{index}").as_bytes());
        let mut txn = manager.begin();
        manager.put(&mut txn, key, value.clone());
        manager.commit(txn).expect("local benchmark warmup commit failed");
    }
    println!(
        "benchmark=txn-local value_bytes={value_bytes} keyspace={keyspace} warmup_ops={warmup_ops} metadata={}",
        machine_metadata()
    );
    let start_gate = Arc::new(Barrier::new(workers + 1));
    let mut handles = Vec::with_capacity(workers);
    for worker in 0..workers {
        let manager = Arc::clone(&manager);
        let start_gate = Arc::clone(&start_gate);
        let value = value.clone();
        let base = ops / workers;
        let extra = ops % workers;
        let first = worker * base + worker.min(extra);
        let end = first + base + usize::from(worker < extra);
        handles.push(std::thread::spawn(move || {
            start_gate.wait();
            let mut applied = 0usize;
            let mut failed = 0usize;
            for index in first..end {
                let key = RecordKey::new(
                    "bench",
                    "bench",
                    "kv",
                    format!("k-{}", index % keyspace).as_bytes(),
                );
                let mut txn = manager.begin();
                manager.put(&mut txn, key, value.clone());
                if manager.commit(txn).is_ok() {
                    applied += 1;
                } else {
                    failed += 1;
                }
            }
            (applied, failed)
        }));
    }
    let start = Instant::now();
    start_gate.wait();
    let (applied, failed) = handles
        .into_iter()
        .map(|handle| handle.join().expect("local benchmark worker panicked"))
        .fold((0usize, 0usize), |(applied, failed), (worker_applied, worker_failed)| {
            (applied + worker_applied, failed + worker_failed)
        });
    let elapsed = start.elapsed();
    let seconds = elapsed.as_secs_f64().max(0.000001);
    let throughput = applied as f64 / seconds;
    println!("successful_ops={applied} failed_ops={failed} requested_ops={ops} workers={workers} seconds={seconds:.3} qps={throughput:.0}");
    let probe_ops = 10000.min(ops);
    if probe_ops > 0 {
        let probe_workers = concurrency.max(1).min(probe_ops);
        let probe_start = Instant::now();
        let mut handles = Vec::with_capacity(probe_workers);
        for worker in 0..probe_workers {
            let manager = Arc::clone(&manager);
            let base = probe_ops / probe_workers;
            let extra = probe_ops % probe_workers;
            let first = worker * base + worker.min(extra);
            let end = first + base + usize::from(worker < extra);
            handles.push(std::thread::spawn(move || {
                let mut hits = 0usize;
                for index in first..end {
                    let key = RecordKey::new(
                        "bench",
                        "bench",
                        "kv",
                        format!("k-{}", index % keyspace).as_bytes(),
                    );
                    let mut txn = manager.begin();
                    if manager.get(&mut txn, &key).unwrap_or(None).is_some() {
                        hits += 1;
                    }
                }
                hits
            }));
        }
        let hits = handles
            .into_iter()
            .map(|handle| handle.join().expect("local benchmark probe worker panicked"))
            .sum::<usize>();
        let probe_nanos = probe_start.elapsed().as_nanos().max(1);
        let probe_mean_micros = probe_nanos as f64 / probe_ops as f64 / 1_000.0;
        let probe_qps = probe_ops as f64 / (probe_nanos as f64 / 1_000_000_000.0);
        println!(
            "probe_hits={hits} probe_workers={probe_workers} probe_mean_micros={probe_mean_micros:.3} probe_qps={probe_qps:.0}"
        );
    }
}

fn sharded_local_bench(
    ops: usize,
    concurrency: usize,
    value_bytes: usize,
    shards: usize,
    keyspace: Option<usize>,
    sync_each_write: bool,
    mode: StorageMode,
    batch_size: usize,
) {
    const MAX_BATCH_SIZE: usize = 20_000;
    const MAX_BATCH_VALUE_BYTES: usize = 8 * 1024 * 1024;
    let batch_value_bytes = batch_size.checked_mul(value_bytes);
    if !(1..=256).contains(&shards)
        || batch_size == 0
        || batch_size > MAX_BATCH_SIZE
        || batch_value_bytes.is_none_or(|bytes| bytes > MAX_BATCH_VALUE_BYTES)
    {
        eprintln!("shards or batch size are outside benchmark bounds");
        std::process::exit(2);
    }
    let requested_writes = match ops.checked_mul(batch_size) {
        Some(writes) => writes,
        None => {
            eprintln!("requested write count overflows");
            std::process::exit(2);
        }
    };
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let data_dir =
        std::env::temp_dir().join(format!("ryme-bench-sharded-{}-{nanos}", std::process::id()));
    let policy = if sync_each_write { SyncPolicy::Always } else { SyncPolicy::Never };
    let backend = Arc::new(
        ShardSet::open_with_mode(&data_dir, shards, policy, 0, mode)
            .expect("could not open temporary sharded benchmark database"),
    );
    let tables = benchmark_tables(&backend, shards);
    let value = vec![7u8; value_bytes];
    let workers = concurrency.max(1).min(ops.max(1));
    let keyspace = keyspace.unwrap_or(requested_writes).max(1);
    let total_keyspace = keyspace.saturating_mul(shards);
    let warmup_ops =
        if total_keyspace < requested_writes { total_keyspace.div_ceil(batch_size) } else { 0 };
    for index in 0..warmup_ops {
        let mut txn = backend.begin();
        for offset in 0..batch_size {
            let key = benchmark_record_key(
                &tables,
                shards,
                index.saturating_mul(batch_size).saturating_add(offset),
                keyspace,
            );
            backend.put(&mut txn, key, value.clone());
        }
        block_on_ready(backend.commit(txn)).expect("sharded benchmark warmup commit failed");
    }

    let start = Instant::now();
    let mut handles = Vec::with_capacity(workers);
    for worker in 0..workers {
        let backend = Arc::clone(&backend);
        let tables = tables.clone();
        let value = value.clone();
        let base = ops / workers;
        let extra = ops % workers;
        let first = worker * base + worker.min(extra);
        let end = first + base + usize::from(worker < extra);
        handles.push(std::thread::spawn(move || {
            const SAMPLE_EVERY: usize = 64;
            let mut applied = 0usize;
            let mut failed = 0usize;
            let mut samples = Vec::new();
            for index in first..end {
                let sample = index % SAMPLE_EVERY == 0;
                let operation_start = sample.then(Instant::now);
                let mut txn = backend.begin();
                for offset in 0..batch_size {
                    let key = benchmark_record_key(
                        &tables,
                        shards,
                        index.saturating_mul(batch_size).saturating_add(offset),
                        keyspace,
                    );
                    backend.put(&mut txn, key, value.clone());
                }
                if block_on_ready(backend.commit(txn)).is_ok() {
                    applied += 1;
                } else {
                    failed += 1;
                }
                if let Some(operation_start) = operation_start {
                    samples.push(operation_start.elapsed().as_micros() as u64);
                }
            }
            (applied, failed, samples)
        }));
    }
    let mut samples = Vec::new();
    let (applied, failed) = handles
        .into_iter()
        .map(|handle| handle.join().expect("sharded benchmark worker panicked"))
        .fold(
            (0usize, 0usize),
            |(applied, failed), (worker_applied, worker_failed, worker_samples)| {
                samples.extend(worker_samples);
                (applied + worker_applied, failed + worker_failed)
            },
        );
    let seconds = start.elapsed().as_secs_f64().max(0.000001);
    samples.sort_unstable();
    let percentile = |value: f64| -> u64 {
        if samples.is_empty() {
            return 0;
        }
        let index = ((value / 100.0) * samples.len() as f64).floor() as usize;
        samples[index.min(samples.len() - 1)]
    };
    let output = serde_json::json!({
        "benchmark": "sharded-local",
        "machine": machine_metadata(),
        "storage_mode": match mode {
            StorageMode::Hot => "hot",
            StorageMode::Standard => "standard",
        },
        "writes_per_transaction": batch_size,
        "shards": shards,
        "workers": workers,
        "sync_each_write": sync_each_write,
        "value_bytes": value_bytes,
        "keyspace_per_shard": keyspace,
        "warmup_ops": warmup_ops,
        "requested_transactions": ops,
        "successful_transactions": applied,
        "failed_transactions": failed,
        "requested_ops": requested_writes,
        "successful_ops": applied.saturating_mul(batch_size),
        "failed_ops": failed.saturating_mul(batch_size),
        "seconds": seconds,
        "qps": applied.saturating_mul(batch_size) as f64 / seconds,
        "transaction_qps": applied as f64 / seconds,
        "latency_sample_stride": 64,
        "latency_samples": samples.len(),
        "sampled_p50_us": percentile(50.0),
        "sampled_p95_us": percentile(95.0),
        "sampled_p99_us": percentile(99.0),
    });
    println!("{output}");
    drop(backend);
    if let Err(error) = std::fs::remove_dir_all(&data_dir) {
        eprintln!("could not remove temporary benchmark data {}: {error}", data_dir.display());
    }
}

fn segment_build_bench(entries: usize, value_bytes: usize, iterations: usize) {
    const MAX_ENTRIES: usize = 100_000;
    const MAX_INPUT_BYTES: usize = 16 * 1024 * 1024;
    let input_bytes = entries.checked_mul(value_bytes);
    if entries == 0
        || entries > MAX_ENTRIES
        || iterations == 0
        || input_bytes.is_none_or(|bytes| bytes > MAX_INPUT_BYTES)
    {
        eprintln!("entries, value bytes, or iterations exceed benchmark bounds");
        std::process::exit(2);
    }

    let template = (0..entries)
        .map(|index| SegmentEntry {
            key: RecordKey::new(
                "bench",
                "bench",
                "segment-build",
                format!("k-{index:08}").as_bytes(),
            ),
            commit_ts: 1,
            value: Some(vec![7; value_bytes]),
            expires_at: 0,
        })
        .collect::<Vec<_>>();

    // Warm both paths before collecting samples.
    drop(ImmutableSegment::from_entries(0, &template).expect("borrowed segment warmup failed"));
    drop(
        ImmutableSegment::from_entries_owned(0, template.clone())
            .expect("owned segment warmup failed"),
    );

    let lookup_segment = ImmutableSegment::from_entries(0, &template)
        .expect("decode benchmark segment build failed");
    let encoded_segment = lookup_segment.encode().expect("decode benchmark segment encode failed");
    drop(ImmutableSegment::decode(&encoded_segment).expect("segment decode warmup failed"));

    let lookup_count = iterations.saturating_mul(256);
    let mut lookup_state = 0x9e37_79b9_7f4a_7c15u64;
    for _ in 0..lookup_count.min(256) {
        lookup_state ^= lookup_state << 13;
        lookup_state ^= lookup_state >> 7;
        lookup_state ^= lookup_state << 17;
        let key = &template[lookup_state as usize % entries].key;
        std::hint::black_box(lookup_segment.get(key, 1, 0).expect("lookup warmup failed"));
    }
    let lookup_start = Instant::now();
    for _ in 0..lookup_count {
        lookup_state ^= lookup_state << 13;
        lookup_state ^= lookup_state >> 7;
        lookup_state ^= lookup_state << 17;
        let key = &template[lookup_state as usize % entries].key;
        let value = lookup_segment.get(key, 1, 0).expect("segment lookup failed");
        assert!(value.is_some(), "segment lookup missed a generated key");
        std::hint::black_box(value);
    }
    let lookup_elapsed_ns = lookup_start.elapsed().as_nanos();

    let mut borrowed_ns = Vec::with_capacity(iterations);
    let mut owned_ns = Vec::with_capacity(iterations);
    let mut decoded_ns = Vec::with_capacity(iterations);
    for sample in 0..iterations {
        let start = Instant::now();
        let segment = ImmutableSegment::from_entries(sample as u64, &template)
            .expect("borrowed segment build failed");
        borrowed_ns.push(start.elapsed().as_nanos());
        std::hint::black_box(&segment);
        drop(segment);

        let start = Instant::now();
        let segment =
            ImmutableSegment::decode(&encoded_segment).expect("segment decode benchmark failed");
        decoded_ns.push(start.elapsed().as_nanos());
        std::hint::black_box(&segment);
        drop(segment);

        // In production the caller already owns this vector; keep its setup
        // outside the timed interval to isolate the builder's extra clone.
        let input = template.clone();
        let start = Instant::now();
        let segment = ImmutableSegment::from_entries_owned(sample as u64, input)
            .expect("owned segment build failed");
        owned_ns.push(start.elapsed().as_nanos());
        std::hint::black_box(&segment);
        drop(segment);
    }

    borrowed_ns.sort_unstable();
    owned_ns.sort_unstable();
    decoded_ns.sort_unstable();
    let percentile = |samples: &[u128], percentile: usize| -> u128 {
        samples[((samples.len() - 1) * percentile / 100).min(samples.len() - 1)]
    };
    let borrowed_median = percentile(&borrowed_ns, 50).max(1);
    let owned_median = percentile(&owned_ns, 50).max(1);
    println!(
        "{}",
        serde_json::json!({
            "benchmark": "segment-build-ownership",
            "machine": machine_metadata(),
            "entries": entries,
            "value_bytes": value_bytes,
            "iterations": iterations,
            "encoded_bytes": encoded_segment.len(),
            "borrowed_clone_p50_ns": borrowed_median,
            "borrowed_clone_p95_ns": percentile(&borrowed_ns, 95),
            "owned_p50_ns": owned_median,
            "owned_p95_ns": percentile(&owned_ns, 95),
            "owned_p50_speedup": borrowed_median as f64 / owned_median as f64,
            "decode_p50_ns": percentile(&decoded_ns, 50),
            "decode_p95_ns": percentile(&decoded_ns, 95),
            "lookup_ops": lookup_count,
            "lookup_mean_ns": lookup_elapsed_ns / lookup_count.max(1) as u128,
        })
    );
}

fn segment_scan_bench(
    rows: usize,
    deltas: usize,
    limit: usize,
    iterations: usize,
    concurrency: usize,
    cache_bytes: u64,
    compact: bool,
) {
    const MAX_ROWS: usize = 100_000;
    const MAX_DELTAS: usize = 1_024;
    const MAX_ITERATIONS: usize = 100_000;
    let total_keys = rows.checked_add(deltas);
    if rows == 0
        || rows > MAX_ROWS
        || deltas > MAX_DELTAS
        || limit == 0
        || total_keys.is_none_or(|keys| keys > MAX_ROWS + MAX_DELTAS)
        || iterations == 0
        || iterations > MAX_ITERATIONS
        || concurrency == 0
        || concurrency > 64
    {
        eprintln!("rows, deltas, limit, iterations, or concurrency exceed benchmark bounds");
        std::process::exit(2);
    }
    let total_keys = total_keys.expect("validated scan key count");
    let data_dir = std::env::temp_dir().join(format!(
        "ryme-bench-segment-scan-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0)
    ));
    let store = SegmentStore::open_with_cache(&data_dir, cache_bytes)
        .expect("could not open temporary segment scan benchmark store");
    let mut engine = Engine::new();
    for index in 0..rows {
        let key = RecordKey::new("bench", "bench", "scan", format!("k-{index:08}").as_bytes());
        engine.apply(key, 1, Some(vec![7; 64])).expect("could not build scan snapshot");
    }
    store.write(1, &engine).expect("could not write scan snapshot");
    drop(engine);
    for index in 0..deltas {
        let entry = SegmentEntry {
            key: RecordKey::new(
                "bench",
                "bench",
                "scan",
                format!("k-{:08}", rows + index).as_bytes(),
            ),
            commit_ts: index as u64 + 2,
            value: Some(vec![7; 64]),
            expires_at: 0,
        };
        store
            .write_delta_owned_with_sync(index as u64 + 2, vec![entry], false)
            .expect("could not write scan delta");
    }
    let compaction_ns = if compact {
        let start = Instant::now();
        store.compact().expect("could not compact benchmark segments");
        Some(start.elapsed().as_nanos())
    } else {
        None
    };

    let read_ts = deltas as u64 + 1;
    let expected_rows = limit.min(total_keys);
    let warmup = store
        .scan("bench", "bench", "scan", read_ts, 0, limit)
        .expect("scan benchmark warmup failed");
    assert_eq!(warmup.len(), expected_rows, "scan warmup returned wrong row count");
    drop(warmup);

    let mut samples_ns = Vec::with_capacity(iterations.saturating_mul(concurrency));
    let benchmark_start = Instant::now();
    let barrier = Barrier::new(concurrency);
    let worker_samples: Vec<Vec<u128>> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..concurrency)
            .map(|_| {
                scope.spawn(|| {
                    barrier.wait();
                    let mut samples = Vec::with_capacity(iterations);
                    for _ in 0..iterations {
                        let start = Instant::now();
                        let result = store
                            .scan("bench", "bench", "scan", read_ts, 0, limit)
                            .expect("segment scan benchmark failed");
                        samples.push(start.elapsed().as_nanos());
                        assert_eq!(result.len(), expected_rows, "scan returned wrong row count");
                        std::hint::black_box(&result);
                    }
                    samples
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|handle| handle.join().expect("scan benchmark worker failed"))
            .collect()
    });
    samples_ns.extend(worker_samples.into_iter().flatten());
    let scan_count = iterations.saturating_mul(concurrency);
    let elapsed = benchmark_start.elapsed().as_secs_f64().max(0.000001);
    samples_ns.sort_unstable();
    let percentile = |percentile: usize| -> u128 {
        samples_ns[((samples_ns.len() - 1) * percentile / 100).min(samples_ns.len() - 1)]
    };
    let cache = store.cache_stats();
    println!(
        "{}",
        serde_json::json!({
            "benchmark": "segment-scan-page",
            "machine": machine_metadata(),
            "snapshot_rows": rows,
            "delta_segments": deltas,
            "total_keys": total_keys,
            "limit": limit,
            "iterations": iterations,
            "concurrency": concurrency,
            "cache_bytes": cache_bytes,
            "compacted": compact,
            "compaction_ns": compaction_ns,
            "returned_rows_per_scan": expected_rows,
            "scan_p50_ns": percentile(50),
            "scan_p95_ns": percentile(95),
            "scans_per_second": scan_count as f64 / elapsed,
            "rows_per_second": scan_count as f64 * expected_rows as f64 / elapsed,
            "segment_cache_hits": cache.hits,
            "segment_cache_misses": cache.misses,
        })
    );
    drop(store);
    if let Err(error) = std::fs::remove_dir_all(&data_dir) {
        eprintln!("could not remove temporary benchmark data {}: {error}", data_dir.display());
    }
}

fn segment_startup_bench(rows: usize, superseded_deltas: usize, deltas: usize, iterations: usize) {
    if rows == 0
        || rows > 100_000
        || superseded_deltas > 1_024
        || deltas > 1_024
        || superseded_deltas.saturating_add(deltas) > 1_024
        || iterations == 0
        || iterations > 100
    {
        eprintln!("rows, deltas, or iterations exceed segment startup benchmark bounds");
        std::process::exit(2);
    }

    let data_dir = std::env::temp_dir().join(format!(
        "ryme-bench-segment-startup-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0)
    ));
    let store = SegmentStore::open_with_cache(&data_dir, 0)
        .expect("could not open temporary segment startup benchmark store");
    let mut engine = Engine::new();
    for index in 0..rows {
        let key = RecordKey::new("bench", "bench", "startup", format!("k-{index:08}").as_bytes());
        engine.apply(key, 1, Some(vec![7; 64])).expect("could not build startup snapshot");
    }
    store.write(1, &engine).expect("could not write startup snapshot");
    for index in 0..superseded_deltas {
        let commit_ts = index as u64 + 2;
        let key = RecordKey::new(
            "bench",
            "bench",
            "startup",
            format!("k-{:08}", index % rows).as_bytes(),
        );
        let value = vec![9; 64];
        engine
            .apply(key.clone(), commit_ts, Some(value.clone()))
            .expect("could not extend superseded startup history");
        store
            .write_delta_owned_with_sync(
                commit_ts,
                vec![SegmentEntry { key, commit_ts, value: Some(value), expires_at: 0 }],
                false,
            )
            .expect("could not write superseded startup delta");
    }
    let snapshot_commit = superseded_deltas as u64 + 1;
    store.write(snapshot_commit, &engine).expect("could not write latest startup snapshot");
    for index in 0..deltas {
        let commit_ts = snapshot_commit + index as u64 + 1;
        let key = RecordKey::new(
            "bench",
            "bench",
            "startup",
            format!("k-{:08}", index % rows).as_bytes(),
        );
        store
            .write_delta_owned_with_sync(
                commit_ts,
                vec![SegmentEntry { key, commit_ts, value: Some(vec![9; 64]), expires_at: 0 }],
                false,
            )
            .expect("could not write startup delta");
    }

    let expected_max = snapshot_commit + deltas as u64;
    let mut full_decode_ns = Vec::with_capacity(iterations);
    let mut header_scan_ns = Vec::with_capacity(iterations);
    for iteration in 0..iterations {
        if iteration % 2 == 0 {
            let start = Instant::now();
            let decoded_max = store
                .segment_paths()
                .expect("could not list startup segments")
                .iter()
                .map(|path| {
                    let bytes = std::fs::read(path).expect("could not read startup segment");
                    ImmutableSegment::decode(&bytes)
                        .expect("could not decode startup segment")
                        .meta()
                        .max_commit_ts
                })
                .max()
                .unwrap_or(0);
            full_decode_ns.push(start.elapsed().as_nanos());
            assert_eq!(decoded_max, expected_max, "full decode found wrong commit timestamp");

            let start = Instant::now();
            let header_max = store.max_commit_ts().expect("could not read segment headers");
            header_scan_ns.push(start.elapsed().as_nanos());
            assert_eq!(header_max, expected_max, "header scan found wrong commit timestamp");
        } else {
            let start = Instant::now();
            let header_max = store.max_commit_ts().expect("could not read segment headers");
            header_scan_ns.push(start.elapsed().as_nanos());
            assert_eq!(header_max, expected_max, "header scan found wrong commit timestamp");

            let start = Instant::now();
            let decoded_max = store
                .segment_paths()
                .expect("could not list startup segments")
                .iter()
                .map(|path| {
                    let bytes = std::fs::read(path).expect("could not read startup segment");
                    ImmutableSegment::decode(&bytes)
                        .expect("could not decode startup segment")
                        .meta()
                        .max_commit_ts
                })
                .max()
                .unwrap_or(0);
            full_decode_ns.push(start.elapsed().as_nanos());
            assert_eq!(decoded_max, expected_max, "full decode found wrong commit timestamp");
        }
    }
    full_decode_ns.sort_unstable();
    header_scan_ns.sort_unstable();
    let percentile = |samples: &[u128], percent: usize| {
        samples[((samples.len() - 1) * percent / 100).min(samples.len() - 1)]
    };
    let full_decode_p50 = percentile(&full_decode_ns, 50);
    let header_scan_p50 = percentile(&header_scan_ns, 50);
    println!(
        "{}",
        serde_json::json!({
            "benchmark": "segment-startup-max-commit",
            "machine": machine_metadata(),
            "snapshot_rows": rows,
            "superseded_delta_segments": superseded_deltas,
            "delta_segments": deltas,
            "iterations": iterations,
            "full_decode_p50_ns": full_decode_p50,
            "full_decode_p95_ns": percentile(&full_decode_ns, 95),
            "header_scan_p50_ns": header_scan_p50,
            "header_scan_p95_ns": percentile(&header_scan_ns, 95),
            "p50_speedup": full_decode_p50 as f64 / header_scan_p50.max(1) as f64,
        })
    );

    drop(store);
    if let Err(error) = std::fs::remove_dir_all(&data_dir) {
        eprintln!("could not remove temporary benchmark data {}: {error}", data_dir.display());
    }
}

fn segment_prune_bench(rows: usize, deltas: usize, iterations: usize) {
    if rows == 0 || rows > 100_000 || deltas > 1_024 || iterations == 0 || iterations > 100 {
        eprintln!("rows, deltas, or iterations exceed segment prune benchmark bounds");
        std::process::exit(2);
    }

    let data_dir = std::env::temp_dir().join(format!(
        "ryme-bench-segment-prune-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0)
    ));
    let store = SegmentStore::open_with_cache(&data_dir, 0)
        .expect("could not open temporary segment prune benchmark store");
    let mut engine = Engine::new();
    for index in 0..rows {
        let key = RecordKey::new("bench", "bench", "prune", format!("k-{index:08}").as_bytes());
        engine.apply(key, 1, Some(vec![7; 64])).expect("could not build prune snapshot");
    }
    store.write(1, &engine).expect("could not write prune snapshot");
    drop(engine);
    for index in 0..deltas {
        let commit_ts = index as u64 + 2;
        let key =
            RecordKey::new("bench", "bench", "prune", format!("k-{:08}", index % rows).as_bytes());
        store
            .write_delta_owned_with_sync(
                commit_ts,
                vec![SegmentEntry { key, commit_ts, value: Some(vec![9; 64]), expires_at: 0 }],
                false,
            )
            .expect("could not write prune delta");
    }

    let full_decode_scan = || {
        let mut segments = Vec::new();
        for path in store.segment_paths().expect("could not list prune segments") {
            let bytes = std::fs::read(&path).expect("could not read prune segment");
            let segment = ImmutableSegment::decode(&bytes).expect("could not decode prune segment");
            segments.push((segment.meta().id, path, segment.is_snapshot()));
        }
        segments.sort_by_key(|(id, _, _)| *id);
        let latest_full =
            segments.iter().filter_map(|(id, _, is_snapshot)| is_snapshot.then_some(*id)).max();
        let keep_paths = segments
            .iter()
            .filter(|(id, _, is_snapshot)| {
                Some(*id) == latest_full
                    || latest_full.is_some_and(|latest| *id > latest && !is_snapshot)
            })
            .map(|(_, path, _)| path)
            .collect::<Vec<_>>();
        std::hint::black_box(&keep_paths);
    };

    // Warm the file cache and both code paths before collecting paired samples.
    full_decode_scan();
    assert_eq!(store.prune(1).expect("prune benchmark warmup failed"), 0);
    let mut full_decode_ns = Vec::with_capacity(iterations);
    let mut metadata_prune_ns = Vec::with_capacity(iterations);
    for iteration in 0..iterations {
        if iteration % 2 == 0 {
            let start = Instant::now();
            full_decode_scan();
            full_decode_ns.push(start.elapsed().as_nanos());

            let start = Instant::now();
            assert_eq!(store.prune(1).expect("metadata prune benchmark failed"), 0);
            metadata_prune_ns.push(start.elapsed().as_nanos());
        } else {
            let start = Instant::now();
            assert_eq!(store.prune(1).expect("metadata prune benchmark failed"), 0);
            metadata_prune_ns.push(start.elapsed().as_nanos());

            let start = Instant::now();
            full_decode_scan();
            full_decode_ns.push(start.elapsed().as_nanos());
        }
    }
    full_decode_ns.sort_unstable();
    metadata_prune_ns.sort_unstable();
    let percentile = |samples: &[u128], percent: usize| {
        samples[((samples.len() - 1) * percent / 100).min(samples.len() - 1)]
    };
    let full_decode_p50 = percentile(&full_decode_ns, 50);
    let metadata_prune_p50 = percentile(&metadata_prune_ns, 50);
    println!(
        "{}",
        serde_json::json!({
            "benchmark": "segment-prune-metadata",
            "machine": machine_metadata(),
            "snapshot_rows": rows,
            "delta_segments": deltas,
            "iterations": iterations,
            "full_decode_p50_ns": full_decode_p50,
            "full_decode_p95_ns": percentile(&full_decode_ns, 95),
            "metadata_prune_p50_ns": metadata_prune_p50,
            "metadata_prune_p95_ns": percentile(&metadata_prune_ns, 95),
            "p50_speedup": full_decode_p50 as f64 / metadata_prune_p50.max(1) as f64,
        })
    );

    drop(store);
    if let Err(error) = std::fs::remove_dir_all(&data_dir) {
        eprintln!("could not remove temporary benchmark data {}: {error}", data_dir.display());
    }
}

fn deterministic_bench_vector(index: usize, dimensions: usize) -> Vec<f32> {
    (0..dimensions)
        .map(|axis| {
            let mut value = (index as u64)
                .wrapping_mul(0x9e3779b97f4a7c15)
                .wrapping_add((axis as u64).wrapping_mul(0xbf58476d1ce4e5b9));
            value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
            value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
            value ^= value >> 31;
            let unit = (value >> 40) as f32 / ((1u32 << 24) - 1) as f32;
            let component = unit * 2.0 - 1.0;
            if component.abs() < f32::EPSILON {
                if value & 1 == 0 {
                    0.001
                } else {
                    -0.001
                }
            } else {
                component
            }
        })
        .collect()
}

fn vector_index_bench(
    vectors: usize,
    dimensions: usize,
    queries: usize,
    partitions: usize,
    top_k: usize,
    ann_ef: usize,
    json: bool,
) {
    const MAX_VECTOR_BENCH_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;
    if vectors == 0
        || vectors > ryme_index::MAX_VECTORS_PER_SPACE
        || dimensions == 0
        || dimensions > ryme_index::MAX_DIM
        || queries == 0
        || queries > 100_000
        || !(1..=ryme_index::MAX_PARTITIONS).contains(&partitions)
        || !(1..=ryme_index::MAX_TOP_K).contains(&top_k)
        || !(1..=256).contains(&ann_ef)
    {
        eprintln!(
            "vector-index benchmark arguments are outside supported limits (queries max: 100000, ann-ef max: 256)"
        );
        std::process::exit(2);
    }
    let vector_payload_bytes = match vectors.checked_mul(dimensions).and_then(|n| n.checked_mul(4))
    {
        Some(bytes) if bytes <= MAX_VECTOR_BENCH_PAYLOAD_BYTES => bytes,
        _ => {
            eprintln!("vector-index benchmark dataset exceeds the 16 MiB vector-payload limit");
            std::process::exit(2);
        }
    };
    let make_vector = |index: usize| deterministic_bench_vector(index, dimensions);

    let generation_start = Instant::now();
    let dataset = (0..vectors).map(make_vector).collect::<Vec<_>>();
    let dataset_generation_ns = generation_start.elapsed().as_nanos();
    let mut index = PartitionedIndex::new(partitions);
    let build_start = Instant::now();
    for (vector_id, vector) in dataset.into_iter().enumerate() {
        index
            .vector_upsert("vector-index-bench", format!("v{vector_id:08}"), vector)
            .expect("vector-index benchmark insert failed");
    }
    let build_ns = build_start.elapsed().as_nanos();

    let top_k = top_k.min(vectors);
    let mut exact_ns = Vec::with_capacity(queries);
    let mut ann_ns = Vec::with_capacity(queries);
    let mut recall_sum = 0.0f64;
    for query_number in 0..queries {
        let query = make_vector(vectors.wrapping_add(query_number));
        let (exact, approximate) = if query_number % 2 == 0 {
            let start = Instant::now();
            let exact = index
                .vector_search("vector-index-bench", &query, top_k)
                .expect("exact vector benchmark search failed");
            exact_ns.push(start.elapsed().as_nanos());

            let start = Instant::now();
            let approximate = index
                .ann_search("vector-index-bench", &query, top_k, ann_ef)
                .expect("ANN vector benchmark search failed");
            ann_ns.push(start.elapsed().as_nanos());
            (exact, approximate)
        } else {
            let start = Instant::now();
            let approximate = index
                .ann_search("vector-index-bench", &query, top_k, ann_ef)
                .expect("ANN vector benchmark search failed");
            ann_ns.push(start.elapsed().as_nanos());

            let start = Instant::now();
            let exact = index
                .vector_search("vector-index-bench", &query, top_k)
                .expect("exact vector benchmark search failed");
            exact_ns.push(start.elapsed().as_nanos());
            (exact, approximate)
        };

        let exact_ids =
            exact.into_iter().map(|hit| hit.id).collect::<std::collections::HashSet<_>>();
        let matching = approximate.iter().filter(|hit| exact_ids.contains(&hit.id)).count();
        recall_sum += matching as f64 / top_k.max(1) as f64;
    }
    exact_ns.sort_unstable();
    ann_ns.sort_unstable();
    let percentile = |samples: &[u128], percent: usize| {
        samples[((samples.len() - 1) * percent / 100).min(samples.len() - 1)]
    };
    let exact_total_ns = exact_ns.iter().copied().sum::<u128>().max(1);
    let ann_total_ns = ann_ns.iter().copied().sum::<u128>().max(1);
    let output = serde_json::json!({
        "benchmark": "vector-index-exact-vs-adaptive-ann",
        "machine": machine_metadata(),
        "vectors": vectors,
        "dimensions": dimensions,
        "partitions": partitions,
        "queries": queries,
        "top_k": top_k,
        "ann_ef": ann_ef,
        "vector_payload_bytes": vector_payload_bytes,
        "dataset_generation_ns": dataset_generation_ns,
        "index_build_ns": build_ns,
        "index_build_vectors_per_second": vectors as f64 * 1_000_000_000.0 / build_ns.max(1) as f64,
        "exact_qps": queries as f64 * 1_000_000_000.0 / exact_total_ns as f64,
        "exact_p50_ns": percentile(&exact_ns, 50),
        "exact_p95_ns": percentile(&exact_ns, 95),
        "exact_p99_ns": percentile(&exact_ns, 99),
        "adaptive_ann_qps": queries as f64 * 1_000_000_000.0 / ann_total_ns as f64,
        "adaptive_ann_p50_ns": percentile(&ann_ns, 50),
        "adaptive_ann_p95_ns": percentile(&ann_ns, 95),
        "adaptive_ann_p99_ns": percentile(&ann_ns, 99),
        "mean_recall_at_k": recall_sum / queries as f64,
    });
    if json {
        println!("{output}");
    } else {
        println!(
            "vectors={vectors} dimensions={dimensions} partitions={partitions} top_k={top_k} payload_bytes={vector_payload_bytes} build_vectors_per_second={:.0} exact_qps={:.0} exact_p50={}ns exact_p99={}ns adaptive_ann_qps={:.0} adaptive_ann_p50={}ns adaptive_ann_p99={}ns mean_recall_at_k={:.3}",
            output["index_build_vectors_per_second"].as_f64().unwrap_or(0.0),
            output["exact_qps"].as_f64().unwrap_or(0.0),
            output["exact_p50_ns"].as_u64().unwrap_or(0),
            output["exact_p99_ns"].as_u64().unwrap_or(0),
            output["adaptive_ann_qps"].as_f64().unwrap_or(0.0),
            output["adaptive_ann_p50_ns"].as_u64().unwrap_or(0),
            output["adaptive_ann_p99_ns"].as_u64().unwrap_or(0),
            output["mean_recall_at_k"].as_f64().unwrap_or(0.0),
        );
    }
}

fn text_index_bench(
    documents: usize,
    queries: usize,
    partitions: usize,
    top_k: usize,
    query_terms: usize,
    json: bool,
) {
    if documents == 0
        || documents > ryme_index::MAX_DOCS_PER_SPACE
        || queries == 0
        || queries > 100_000
        || !(1..=ryme_index::MAX_PARTITIONS).contains(&partitions)
        || !(1..=ryme_index::MAX_TOP_K).contains(&top_k)
        || !(1..=2).contains(&query_terms)
    {
        eprintln!(
            "text-index benchmark arguments are outside supported limits (documents max: {}, queries max: 100000)",
            ryme_index::MAX_DOCS_PER_SPACE
        );
        std::process::exit(2);
    }

    let mut index = PartitionedIndex::new(partitions);
    let build_start = Instant::now();
    for document in 0..documents {
        let id = format!("d{document:08}");
        let text = format!("global shared bucket{:03}", document % 100);
        index
            .text_index("text-index-bench", id, &text)
            .expect("text index benchmark insert failed");
    }
    let index_build_ns = build_start.elapsed().as_nanos();
    let query = if query_terms == 1 { "global" } else { "global shared" };
    index
        .text_search("text-index-bench", query, top_k)
        .expect("text index benchmark warmup failed");
    let mut query_ns = Vec::with_capacity(queries);
    for _ in 0..queries {
        let start = Instant::now();
        index
            .text_search("text-index-bench", query, top_k)
            .expect("text index benchmark search failed");
        query_ns.push(start.elapsed().as_nanos());
    }
    query_ns.sort_unstable();
    let percentile = |samples: &[u128], percent: usize| {
        samples[((samples.len() - 1) * percent / 100).min(samples.len() - 1)]
    };
    let total_query_ns = query_ns.iter().copied().sum::<u128>().max(1);
    let output = serde_json::json!({
        "benchmark": "text-index-common-term-top-k",
        "machine": machine_metadata(),
        "documents": documents,
        "partitions": partitions,
        "queries": queries,
        "top_k": top_k,
        "query_terms": query_terms,
        "matched_documents_per_query": documents,
        "index_build_ns": index_build_ns,
        "query_qps": queries as f64 * 1_000_000_000.0 / total_query_ns as f64,
        "query_p50_ns": percentile(&query_ns, 50),
        "query_p95_ns": percentile(&query_ns, 95),
        "query_p99_ns": percentile(&query_ns, 99),
    });
    if json {
        println!("{output}");
    } else {
        println!(
            "documents={documents} partitions={partitions} top_k={top_k} query_terms={query_terms} index_build={}ns query_qps={:.0} query_p50={}ns query_p99={}ns",
            output["index_build_ns"].as_u64().unwrap_or(0),
            output["query_qps"].as_f64().unwrap_or(0.0),
            output["query_p50_ns"].as_u64().unwrap_or(0),
            output["query_p99_ns"].as_u64().unwrap_or(0),
        );
    }
}

fn benchmark_tables(backend: &ShardSet, shards: usize) -> Vec<String> {
    let mut tables = vec![None; shards];
    for index in 0..shards.saturating_mul(256) {
        let table = format!("kv-{index}");
        let shard = backend.route(&TableRef::new("bench", "bench", &table));
        tables[shard].get_or_insert(table);
        if tables.iter().all(Option::is_some) {
            break;
        }
    }
    tables.into_iter().map(|table| table.expect("could not find a table for every shard")).collect()
}

fn benchmark_record_key(
    tables: &[String],
    shards: usize,
    index: usize,
    keyspace: usize,
) -> RecordKey {
    let table_index = index % shards;
    let key_index = index / shards % keyspace;
    RecordKey::new("bench", "bench", &tables[table_index], format!("k-{key_index}").as_bytes())
}

fn block_on_ready<F: std::future::Future>(future: F) -> F::Output {
    use std::task::{Context, Poll, Waker};
    let mut future = std::pin::pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

fn realtime_channel(base: &str, channel_count: usize, topic: usize) -> String {
    if channel_count == 1 {
        base.to_owned()
    } else {
        format!("{base}-{topic}")
    }
}

fn expected_fanout(subscribers: &[usize], published: &[usize]) -> usize {
    subscribers
        .iter()
        .zip(published)
        .map(|(subscribers, published)| subscribers.saturating_mul(*published))
        .fold(0usize, usize::saturating_add)
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
        if let Some(model) = cpu_model_from_proc(&model) {
            metadata["cpu_model"] = serde_json::Value::String(model);
        }
    }
    if let Ok(meminfo) = std::fs::read_to_string("/proc/meminfo") {
        if let Some(bytes) = host_memory_bytes_from_proc(&meminfo) {
            metadata["host_memory_bytes"] = serde_json::Value::from(bytes);
        }
    }
    metadata
}

fn cpu_model_from_proc(cpuinfo: &str) -> Option<String> {
    cpuinfo.lines().find_map(|line| {
        let (name, value) = line.trim().split_once(':')?;
        (name.trim() == "model name").then(|| value.trim().to_string())
    })
}

fn host_memory_bytes_from_proc(meminfo: &str) -> Option<u64> {
    let value = meminfo.lines().find_map(|line| {
        let (name, value) = line.trim().split_once(':')?;
        (name.trim() == "MemTotal").then_some(value)
    })?;
    value.split_whitespace().next()?.parse::<u64>().ok()?.checked_mul(1024)
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

enum BatchReadStore {
    Hot(TxnManager),
    Standard(DurableManager),
}

impl BatchReadStore {
    fn begin(&self) -> ryme_txn::Transaction {
        match self {
            Self::Hot(manager) => manager.begin(),
            Self::Standard(manager) => manager.begin(),
        }
    }

    fn put(&self, txn: &mut Transaction, key: RecordKey, value: Vec<u8>) {
        match self {
            Self::Hot(manager) => manager.put(txn, key, value),
            Self::Standard(manager) => manager.put(txn, key, value),
        }
    }

    fn commit(&self, txn: Transaction) -> ryme_error::Result<u64> {
        match self {
            Self::Hot(manager) => manager.commit(txn),
            Self::Standard(manager) => manager.commit(txn),
        }
    }

    fn get(&self, txn: &mut Transaction, key: &RecordKey) -> ryme_error::Result<Option<Vec<u8>>> {
        match self {
            Self::Hot(manager) => manager.get(txn, key),
            Self::Standard(manager) => manager.get(txn, key),
        }
    }

    fn get_many(
        &self,
        txn: &mut Transaction,
        keys: &[RecordKey],
    ) -> ryme_error::Result<Vec<Option<Vec<u8>>>> {
        match self {
            Self::Hot(manager) => manager.get_many(txn, keys),
            Self::Standard(manager) => manager.get_many(txn, keys),
        }
    }
}

struct BenchTempDir(std::path::PathBuf);

impl BenchTempDir {
    fn create() -> std::io::Result<Self> {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir()
            .join(format!("ryme-bench-txn-batch-{}-{nonce}", std::process::id()));
        std::fs::create_dir(&path)?;
        Ok(Self(path))
    }
}

impl Drop for BenchTempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn txn_read_batch_bench(
    records: usize,
    candidates: usize,
    iterations: usize,
    batch_size: usize,
    value_bytes: usize,
    standard_mode: bool,
    json: bool,
) {
    if records == 0 || candidates == 0 || candidates > records || iterations == 0 || batch_size == 0
    {
        eprintln!("records, candidates, iterations, and batch-size must be positive; candidates must not exceed records");
        return;
    }

    let temp_dir = if standard_mode {
        match BenchTempDir::create() {
            Ok(temp_dir) => Some(temp_dir),
            Err(error) => {
                eprintln!("failed to create temporary benchmark directory: {error}");
                return;
            }
        }
    } else {
        None
    };
    let manager = if let Some(temp_dir) = temp_dir.as_ref() {
        match DurableManager::open_with_mode(
            &temp_dir.0,
            64 * 1024 * 1024,
            SyncPolicy::Never,
            64 * 1024 * 1024,
            StorageMode::Standard,
        ) {
            Ok(manager) => BatchReadStore::Standard(manager),
            Err(error) => {
                eprintln!("failed to initialize Standard-mode benchmark: {error}");
                return;
            }
        }
    } else {
        BatchReadStore::Hot(TxnManager::new())
    };
    let value = vec![b'x'; value_bytes];
    let mut keys = Vec::with_capacity(records);
    let mut seed = manager.begin();
    for id in 0..records {
        let pk = (id as u64).to_be_bytes();
        let key = RecordKey::new("bench", "default", "batch-read", &pk);
        manager.put(&mut seed, key.clone(), value.clone());
        keys.push(key);
    }
    if let Err(error) = manager.commit(seed) {
        eprintln!("failed to seed batch-read benchmark: {error}");
        return;
    }
    let keys = &keys[..candidates];

    for _ in 0..5 {
        std::hint::black_box(measure_single_reads(&manager, keys, false));
        std::hint::black_box(measure_single_reads(&manager, keys, true));
        std::hint::black_box(measure_batched_reads(&manager, keys, batch_size));
    }

    let mut single_samples = Vec::with_capacity(iterations);
    let mut read_only_samples = Vec::with_capacity(iterations);
    let mut batch_samples = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        single_samples.push(measure_single_reads(&manager, keys, false));
        read_only_samples.push(measure_single_reads(&manager, keys, true));
        batch_samples.push(measure_batched_reads(&manager, keys, batch_size));
    }
    single_samples.sort_unstable();
    read_only_samples.sort_unstable();
    batch_samples.sort_unstable();
    let percentile =
        |samples: &[u64], percent: usize| samples[((samples.len() - 1) * percent) / 100];
    let summarize = |samples: &[u64]| {
        let total_ns = samples.iter().map(|sample| u128::from(*sample)).sum::<u128>().max(1);
        serde_json::json!({
            "p50_ns": percentile(samples, 50),
            "p95_ns": percentile(samples, 95),
            "p99_ns": percentile(samples, 99),
            "rows_per_second": (candidates as f64 * samples.len() as f64 * 1_000_000_000.0
                / total_ns as f64).round() as u64,
        })
    };
    let single = summarize(&single_samples);
    let read_only = summarize(&read_only_samples);
    let batched = summarize(&batch_samples);
    let single_p50 = single["p50_ns"].as_u64().unwrap_or(0);
    let read_only_p50 = read_only["p50_ns"].as_u64().unwrap_or(0).max(1);
    let batch_p50 = batched["p50_ns"].as_u64().unwrap_or(0).max(1);
    let output = serde_json::json!({
        "benchmark": "transaction-batch-read",
        "machine": machine_metadata(),
        "records": records,
        "candidates_per_iteration": candidates,
        "iterations": iterations,
        "batch_size": batch_size,
        "value_bytes": value_bytes,
        "storage_mode": if standard_mode { "standard" } else { "hot" },
        "single_read": single,
        "read_only_single_read": read_only,
        "batched_read": batched,
        "p50_speedup": (single_p50 as f64 / batch_p50 as f64 * 100.0).round() / 100.0,
        "read_only_p50_speedup": (single_p50 as f64 / read_only_p50 as f64 * 100.0).round() / 100.0,
    });
    if json {
        println!("{output}");
    } else {
        println!(
            "records={records} candidates={} iterations={iterations} batch_size={batch_size} tracked_p50={}ns read_only_p50={}ns read_only_speedup={}x batched_p50={}ns batch_speedup={}x",
            candidates,
            output["single_read"]["p50_ns"].as_u64().unwrap_or(0),
            output["read_only_single_read"]["p50_ns"].as_u64().unwrap_or(0),
            output["read_only_p50_speedup"].as_f64().unwrap_or(0.0),
            output["batched_read"]["p50_ns"].as_u64().unwrap_or(0),
            output["p50_speedup"].as_f64().unwrap_or(0.0),
        );
    }
}

fn measure_single_reads(manager: &BatchReadStore, keys: &[RecordKey], read_only: bool) -> u64 {
    let mut txn = manager.begin();
    if read_only {
        txn.set_read_only().expect("benchmark read-only mode");
    }
    let started = Instant::now();
    let mut found = 0usize;
    for key in keys {
        found += usize::from(manager.get(&mut txn, key).expect("benchmark read").is_some());
    }
    std::hint::black_box(found);
    started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64
}

fn measure_batched_reads(manager: &BatchReadStore, keys: &[RecordKey], batch_size: usize) -> u64 {
    let mut txn = manager.begin();
    let started = Instant::now();
    let mut found = 0usize;
    for batch in keys.chunks(batch_size) {
        found += manager
            .get_many(&mut txn, batch)
            .expect("benchmark batch read")
            .into_iter()
            .filter(Option::is_some)
            .count();
    }
    std::hint::black_box(found);
    started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64
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
    use super::{
        cpu_model_from_proc, expected_fanout, host_memory_bytes_from_proc, latency_histogram,
        realtime_channel, worker_ops,
    };

    #[test]
    fn realtime_channel_names_preserve_single_channel_compatibility() {
        assert_eq!(realtime_channel("bench", 1, 0), "bench");
        assert_eq!(realtime_channel("bench", 3, 0), "bench-0");
        assert_eq!(realtime_channel("bench", 3, 2), "bench-2");
    }

    #[test]
    fn realtime_expected_delivery_accounts_for_channel_distribution() {
        assert_eq!(expected_fanout(&[2, 1, 0], &[3, 4, 5]), 10);
        assert_eq!(expected_fanout(&[0], &[100]), 0);
    }

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

    #[test]
    fn machine_metadata_parses_linux_cpu_and_memory_fields() {
        let cpuinfo = "processor : 0\nmodel name\t: Test CPU 3.2GHz\n";
        assert_eq!(cpu_model_from_proc(cpuinfo).as_deref(), Some("Test CPU 3.2GHz"));
        assert_eq!(host_memory_bytes_from_proc("MemTotal: 16384 kB\n"), Some(16_777_216));
        assert_eq!(host_memory_bytes_from_proc("MemFree: 1024 kB\n"), None);
    }
}
