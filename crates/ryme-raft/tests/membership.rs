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

async fn wait_leader(nodes: &[std::sync::Arc<Node>], skip: &[usize]) -> usize {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        for (index, node) in nodes.iter().enumerate() {
            if skip.contains(&index) {
                continue;
            }
            if node.is_leader().await {
                return index;
            }
        }
        assert!(tokio::time::Instant::now() < deadline, "no leader");
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
        assert!(tokio::time::Instant::now() < deadline, "no convergence for {name}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn membership_join_catchup_remove() {
    let root =
        std::env::temp_dir().join(format!("ryme-membership-{}-{}", std::process::id(), now_ms()));
    let _ = std::fs::remove_dir_all(&root);
    let mut listeners = Vec::new();
    let mut addrs = Vec::new();
    for _ in 0..4 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        addrs.push(listener.local_addr().unwrap());
        listeners.push(listener);
    }
    let mut nodes = Vec::new();
    let mut tasks = Vec::new();
    for id in 0..3 {
        let peers: Vec<String> = addrs[..3]
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
    let first = wait_leader(&nodes, &[]).await;
    nodes[first].propose_write(writes(vec![("k", "v1")])).await.unwrap();
    wait_value(&nodes, "k", "v1").await;
    let join_peers: Vec<String> = addrs[..3].iter().map(|a| a.to_string()).collect();
    let join_ids: Vec<usize> = vec![0, 1, 2];
    let joiner = Node::open_learner(3, join_peers, join_ids, &root.join("n3")).unwrap();
    let join_tasks = joiner.spawn(listeners.remove(0));
    nodes[first].add_member(3, addrs[3].to_string()).await.unwrap();
    let mut with_joiner = nodes.clone();
    with_joiner.push(joiner.clone());
    wait_value(&with_joiner, "k", "v1").await;
    nodes[first].propose_write(writes(vec![("k", "v2")])).await.unwrap();
    wait_value(&with_joiner, "k", "v2").await;
    let mut leader = wait_leader(&with_joiner, &[]).await;
    if leader == 3 {
        let target = (0..3).find(|i| *i != first).unwrap_or(0);
        with_joiner[leader].transfer(target).await.unwrap();
        leader = wait_leader(&with_joiner, &[3]).await;
    }
    with_joiner[leader].remove_member(3).await.unwrap();
    tokio::time::sleep(Duration::from_millis(600)).await;
    let term = with_joiner[leader].term().await;
    let writer = wait_leader(&nodes, &[]).await;
    nodes[writer].propose_write(writes(vec![("k", "v3")])).await.unwrap();
    wait_value(&nodes, "k", "v3").await;
    assert!(with_joiner[leader].term().await >= term);
    joiner.set_isolated(true);
    joiner.shutdown(join_tasks);
    for (node, node_tasks) in nodes.iter().zip(tasks.into_iter()) {
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
