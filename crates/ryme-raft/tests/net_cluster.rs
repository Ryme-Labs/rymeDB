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

async fn wait_leader(nodes: &[std::sync::Arc<Node>], skip: Option<usize>) -> usize {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        for (index, node) in nodes.iter().enumerate() {
            if Some(index) == skip {
                continue;
            }
            if node.is_leader().await {
                return index;
            }
        }
        assert!(tokio::time::Instant::now() < deadline, "no leader elected");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_value(nodes: &[std::sync::Arc<Node>], name: &str, value: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let mut done = true;
        for node in nodes {
            if node.read_latest(&key(name)).await.unwrap() != Some(value.as_bytes().to_vec()) {
                done = false;
            }
        }
        if done {
            return;
        }
        assert!(tokio::time::Instant::now() < deadline, "replication lag for {name}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn tcp_elect_replicate_failover_restart() {
    let root = std::env::temp_dir().join(format!("ryme-net-{}-{}", std::process::id(), now_ms()));
    let _ = std::fs::remove_dir_all(&root);
    let mut listeners = Vec::new();
    let mut addrs = Vec::new();
    for _ in 0..3 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        addrs.push(listener.local_addr().unwrap());
        listeners.push(listener);
    }
    let mut nodes = Vec::new();
    let mut node_tasks = Vec::new();
    for id in 0..3 {
        let peers: Vec<String> = addrs
            .iter()
            .copied()
            .enumerate()
            .filter(|(i, _)| *i != id)
            .map(|(_, a)| a.to_string())
            .collect();
        let peer_ids: Vec<usize> = (0..3).filter(|i| *i != id).collect();
        let dir = root.join(format!("n{id}"));
        let node = Node::open(id, peers, peer_ids, &dir).unwrap();
        let listener = listeners.remove(0);
        node_tasks.push(node.spawn(listener));
        nodes.push(node);
    }
    let first = wait_leader(&nodes, None).await;
    nodes[first].propose_write(writes(vec![("k", "v1")])).await.unwrap();
    wait_value(&nodes, "k", "v1").await;
    nodes[first].shutdown(std::mem::take(&mut node_tasks[first]));
    tokio::time::sleep(Duration::from_millis(300)).await;
    let second = wait_leader(&nodes, Some(first)).await;
    assert_ne!(first, second);
    nodes[second].propose_write(writes(vec![("k", "v2")])).await.unwrap();
    let live: Vec<std::sync::Arc<Node>> =
        nodes.iter().enumerate().filter(|(i, _)| *i != first).map(|(_, n)| n.clone()).collect();
    wait_value(&live, "k", "v2").await;
    let peers: Vec<String> = addrs
        .iter()
        .copied()
        .enumerate()
        .filter(|(i, _)| *i != first)
        .map(|(_, a)| a.to_string())
        .collect();
    let peer_ids: Vec<usize> = (0..3).filter(|i| *i != first).collect();
    let dir = root.join(format!("n{first}"));
    let revived = Node::open(first, peers, peer_ids, &dir).unwrap();
    let listener = tokio::net::TcpListener::bind(addrs[first]).await.unwrap();
    let revived_task = revived.spawn(listener);
    let mut with_revived = live.clone();
    with_revived.push(revived.clone());
    wait_value(&with_revived, "k", "v2").await;
    assert_eq!(revived.read_latest(&key("k")).await.unwrap(), Some(b"v2".to_vec()));
    revived.shutdown(revived_task);
    abort_all(&nodes, node_tasks);
    let _ = std::fs::remove_dir_all(&root);
}

fn now_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

async fn boot_cluster(
    root: &std::path::Path,
) -> (Vec<std::sync::Arc<Node>>, Vec<Vec<tokio::task::JoinHandle<()>>>) {
    let mut listeners = Vec::new();
    let mut addrs = Vec::new();
    for _ in 0..3 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        addrs.push(listener.local_addr().unwrap());
        listeners.push(listener);
    }
    let mut nodes = Vec::new();
    let mut tasks = Vec::new();
    for id in 0..3 {
        let peers: Vec<String> = addrs
            .iter()
            .copied()
            .enumerate()
            .filter(|(i, _)| *i != id)
            .map(|(_, a)| a.to_string())
            .collect();
        let peer_ids: Vec<usize> = (0..3).filter(|i| *i != id).collect();
        let node = Node::open(id, peers, peer_ids, &root.join(format!("n{id}"))).unwrap();
        tasks.push(node.spawn(listeners.remove(0)));
        nodes.push(node);
    }
    (nodes, tasks)
}

fn abort_all(nodes: &[std::sync::Arc<Node>], tasks: Vec<Vec<tokio::task::JoinHandle<()>>>) {
    for (node, node_tasks) in nodes.iter().zip(tasks) {
        node.shutdown(node_tasks);
    }
}

#[tokio::test]
async fn tcp_transfer_moves_leadership() {
    let root = std::env::temp_dir().join(format!("ryme-xfer-{}-{}", std::process::id(), now_ms()));
    let _ = std::fs::remove_dir_all(&root);
    let (nodes, tasks) = boot_cluster(&root).await;
    let first = wait_leader(&nodes, None).await;
    let target = (0..3).find(|i| *i != first).unwrap();
    nodes[first].transfer(target).await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if nodes[target].is_leader().await && !nodes[first].is_leader().await {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "no transfer");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    nodes[target].propose_write(writes(vec![("k", "v")])).await.unwrap();
    wait_value(&nodes, "k", "v").await;
    let dials: u64 = nodes[target]
        .pool((0..3).find(|i| *i != target).unwrap())
        .map(|pool| pool.dials())
        .unwrap_or(u64::MAX);
    assert!(dials <= 6, "pool dials: {dials}");
    abort_all(&nodes, tasks);
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn tcp_isolated_follower_holds_term() {
    let root = std::env::temp_dir().join(format!("ryme-iso-{}-{}", std::process::id(), now_ms()));
    let _ = std::fs::remove_dir_all(&root);
    let (nodes, tasks) = boot_cluster(&root).await;
    let leader = wait_leader(&nodes, None).await;
    let term_before = nodes[leader].term().await;
    let loner = (0..3).find(|i| *i != leader).unwrap();
    let loner_term = nodes[loner].term().await;
    nodes[loner].set_isolated(true);
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert_eq!(nodes[loner].term().await, loner_term);
    nodes[loner].set_isolated(false);
    let healed = wait_leader(&nodes, None).await;
    assert!(nodes[healed].term().await >= term_before);
    nodes[healed].propose_write(writes(vec![("k", "v")])).await.unwrap();
    wait_value(&nodes, "k", "v").await;
    abort_all(&nodes, tasks);
    let _ = std::fs::remove_dir_all(&root);
}
