use ryme_raft::net::{Member, Node};
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

fn member(id: usize, addrs: &[std::net::SocketAddr]) -> Member {
    Member { id, addr: addrs[id].to_string() }
}

async fn wait_leader(nodes: &[std::sync::Arc<Node>], skip: &[usize]) -> usize {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        for (index, node) in nodes.iter().enumerate() {
            if skip.contains(&index) {
                continue;
            }
            if node.is_leader().await {
                return index;
            }
        }
        if std::env::var("RYME_DEBUG_ELECT").is_ok() {
            for (index, node) in nodes.iter().enumerate() {
                let current: Vec<usize> =
                    node.current_config().await.into_iter().map(|m| m.id).collect();
                let joint: Vec<usize> = node
                    .joint_config()
                    .await
                    .unwrap_or_default()
                    .into_iter()
                    .map(|m| m.id)
                    .collect();
                eprintln!(
                    "dbg n{index} leader={} term={} cur={current:?} joint={joint:?}",
                    node.is_leader().await,
                    node.term().await
                );
            }
        }
        assert!(tokio::time::Instant::now() < deadline, "no leader");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_value(nodes: &[std::sync::Arc<Node>], name: &str, value: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
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

async fn boot_set(
    root: &std::path::Path,
    ids: &[usize],
    addrs: &[std::net::SocketAddr],
    listeners: &mut Vec<tokio::net::TcpListener>,
    learners: bool,
) -> (Vec<std::sync::Arc<Node>>, Vec<Vec<tokio::task::JoinHandle<()>>>) {
    let mut nodes = Vec::new();
    let mut tasks = Vec::new();
    for id in ids {
        let peers: Vec<String> = addrs
            .iter()
            .copied()
            .enumerate()
            .filter(|(i, _)| *i != *id)
            .map(|(_, a)| a.to_string())
            .collect();
        let peer_ids: Vec<usize> = (0..addrs.len()).filter(|i| *i != *id).collect();
        eprintln!("boot id={id} peers={peers:?} peer_ids={peer_ids:?}");
        let node = if learners {
            Node::open_learner(*id, peers, peer_ids, &root.join(format!("n{id}"))).unwrap()
        } else {
            Node::open(*id, peers, peer_ids, &root.join(format!("n{id}"))).unwrap()
        };
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
async fn joint_replaces_disjoint_sets() {
    let root = std::env::temp_dir().join(format!("ryme-joint-{}-{}", std::process::id(), now_ms()));
    let _ = std::fs::remove_dir_all(&root);
    let mut listeners = Vec::new();
    let mut addrs = Vec::new();
    for _ in 0..5 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        addrs.push(listener.local_addr().unwrap());
        listeners.push(listener);
    }
    let (mut nodes, mut tasks) =
        boot_set(&root, &[0, 1, 2], &addrs[..3], &mut listeners, false).await;
    let first = wait_leader(&nodes, &[]).await;
    nodes[first].propose_write(writes(vec![("k", "v1")])).await.unwrap();
    wait_value(&nodes, "k", "v1").await;
    let (join_nodes, join_tasks) = boot_set(&root, &[3, 4], &addrs, &mut listeners, true).await;
    nodes.extend(join_nodes);
    tasks.extend(join_tasks);
    let target = vec![member(2, &addrs), member(3, &addrs), member(4, &addrs)];
    if first != 2 {
        nodes[first].transfer(2).await.unwrap();
        let second = wait_leader(&nodes, &[first]).await;
        assert_eq!(second, 2);
    }
    nodes[2].replace_members(target).await.unwrap();
    nodes[2].propose_write(writes(vec![("k", "v2")])).await.unwrap();
    wait_value(&nodes[2..], "k", "v2").await;
    for index in [0usize, 1] {
        let killed: Vec<tokio::task::JoinHandle<()>> = std::mem::take(&mut tasks[index]);
        nodes[index].shutdown(killed);
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    let writer = wait_leader(&nodes[2..], &[]).await + 2;
    nodes[writer].propose_write(writes(vec![("k", "v3")])).await.unwrap();
    wait_value(&nodes[2..], "k", "v3").await;
    abort_all(&nodes, tasks);
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn joint_survives_leader_loss() {
    let root =
        std::env::temp_dir().join(format!("ryme-joint-fail-{}-{}", std::process::id(), now_ms()));
    let _ = std::fs::remove_dir_all(&root);
    let mut listeners = Vec::new();
    let mut addrs = Vec::new();
    for _ in 0..5 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        addrs.push(listener.local_addr().unwrap());
        listeners.push(listener);
    }
    let (mut nodes, mut tasks) =
        boot_set(&root, &[0, 1, 2], &addrs[..3], &mut listeners, false).await;
    let first = wait_leader(&nodes, &[]).await;
    let (join_nodes, join_tasks) = boot_set(&root, &[3, 4], &addrs, &mut listeners, true).await;
    nodes.extend(join_nodes);
    tasks.extend(join_tasks);
    let target = vec![member(2, &addrs), member(3, &addrs), member(4, &addrs)];
    nodes[first].propose_joint(target).await.unwrap();
    let killed: Vec<tokio::task::JoinHandle<()>> = std::mem::take(&mut tasks[first]);
    nodes[first].shutdown(killed);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let second = wait_leader(&nodes, &[first]).await;
    nodes[second].finalize().await.unwrap();
    let live: Vec<std::sync::Arc<Node>> =
        [2usize, 3, 4].into_iter().filter(|id| *id != first).map(|id| nodes[id].clone()).collect();
    let writer = live[wait_leader(&live, &[]).await].clone();
    writer.propose_write(writes(vec![("k", "v")])).await.unwrap();
    wait_value(&live, "k", "v").await;
    abort_all(&nodes, tasks);
    let _ = std::fs::remove_dir_all(&root);
}

fn now_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}
