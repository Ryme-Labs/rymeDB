use ryme_raft::net::Node;
use ryme_storage::RecordKey;
use std::collections::BTreeMap;
use std::time::Duration;

fn writes(pairs: Vec<(&str, &str)>) -> BTreeMap<RecordKey, ryme_txn::WriteOp> {
    let mut out = BTreeMap::new();
    for (key, value) in pairs {
        out.insert(
            RecordKey::new("t", "d", "s", key.as_bytes()),
            ryme_txn::WriteOp::put(value.as_bytes().to_vec()),
        );
    }
    out
}

fn key(name: &str) -> RecordKey {
    RecordKey::new("t", "d", "s", name.as_bytes())
}

#[tokio::test]
async fn peers_resolve_dns_names() {
    let root = std::env::temp_dir().join(format!("ryme-dns-{}-{}", std::process::id(), now_ms()));
    let _ = std::fs::remove_dir_all(&root);
    let mut listeners = Vec::new();
    let mut ports = Vec::new();
    for _ in 0..2 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        ports.push(listener.local_addr().unwrap().port());
        listeners.push(listener);
    }
    let names: Vec<String> = ports.iter().map(|port| format!("localhost:{port}")).collect();
    let mut nodes = Vec::new();
    let mut tasks = Vec::new();
    for id in 0..2 {
        let peers: Vec<String> =
            names.iter().enumerate().filter(|(i, _)| *i != id).map(|(_, n)| n.clone()).collect();
        let peer_ids: Vec<usize> = (0..2).filter(|i| *i != id).collect();
        let node = Node::open(id, peers, peer_ids, &root.join(format!("n{id}"))).unwrap();
        tasks.push(node.spawn(listeners.remove(0)));
        nodes.push(node);
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    let leader = loop {
        let mut found = None;
        for (index, node) in nodes.iter().enumerate() {
            if node.is_leader().await {
                found = Some(index);
                break;
            }
        }
        if let Some(index) = found {
            break index;
        }
        assert!(tokio::time::Instant::now() < deadline, "no leader elected");
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    nodes[leader].propose_write(writes(vec![("k", "v")])).await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let mut done = true;
        for node in &nodes {
            if node.read_latest(&key("k")).await.unwrap() != Some(b"v".to_vec()) {
                done = false;
            }
        }
        if done {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "replication lag");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    for (node, node_tasks) in nodes.iter().zip(tasks) {
        node.shutdown(node_tasks);
    }
    let _ = std::fs::remove_dir_all(&root);
}

fn now_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}
