use ryme_raft::net::{ClusterBackend, Node, RangeOwner};
use ryme_storage::RecordKey;
use ryme_txn::TxnBackend;
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
        assert!(tokio::time::Instant::now() < deadline, "no leader");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_value(backends: &[ClusterBackend], name: &str, value: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let mut done = true;
        for backend in backends {
            let mut txn = backend.begin();
            if backend.get(&mut txn, &key(name)).unwrap() != Some(value.as_bytes().to_vec()) {
                done = false;
            }
        }
        if done {
            return;
        }
        assert!(tokio::time::Instant::now() < deadline, "no convergence");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn backend_replicates_and_fails_over() {
    let root = std::env::temp_dir().join(format!("ryme-be-{}-{}", std::process::id(), now_ms()));
    let _ = std::fs::remove_dir_all(&root);
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
    let backends: Vec<ClusterBackend> = nodes.iter().cloned().map(ClusterBackend::new).collect();
    let first = wait_leader(&nodes, None).await;
    let mut txn = backends[first].begin();
    for (key, op) in writes(vec![("k", "v1")]) {
        backends[first].put(&mut txn, key, op.value.unwrap());
    }
    backends[first].commit(txn).await.unwrap();
    wait_value(&backends, "k", "v1").await;
    let target = (first + 1) % nodes.len();
    let snapshot =
        nodes[first].fetch_range_snapshot(target, Vec::new(), Vec::new(), 0, 16).await.unwrap();
    assert!(!snapshot.truncated);
    assert_eq!(snapshot.rows.len(), 1);
    assert_eq!(snapshot.rows[0].table, "s");
    assert_eq!(snapshot.rows[0].pk, b"k".to_vec());
    assert_eq!(snapshot.rows[0].value, b"v1".to_vec());
    assert_eq!(snapshot.rows[0].versions.len(), 1);
    assert_eq!(nodes[first].install_range_snapshot_on(target, snapshot).await.unwrap(), 1);
    nodes[first].set_range_owners(vec![RangeOwner {
        start: b"s\0".to_vec(),
        end: Vec::new(),
        owner: target,
        epoch: 1,
    }]);
    assert_eq!(nodes[first].range_owner(b"s\0k"), Some((target, 1)));
    let (owner_value, owner_expiry) =
        nodes[first].fetch_range_value(target, key("k"), 0).await.unwrap();
    assert_eq!(owner_value, Some(b"v1".to_vec()));
    assert_eq!(owner_expiry, Some(0));
    nodes[first].shutdown(std::mem::take(&mut tasks[first]));
    tokio::time::sleep(Duration::from_millis(300)).await;
    let second = wait_leader(&nodes, Some(first)).await;
    assert_ne!(first, second);
    let mut txn = backends[second].begin();
    for (key, op) in writes(vec![("k", "v2")]) {
        backends[second].put(&mut txn, key, op.value.unwrap());
    }
    backends[second].commit(txn).await.unwrap();
    let live: Vec<ClusterBackend> =
        backends.into_iter().enumerate().filter(|(i, _)| *i != first).map(|(_, b)| b).collect();
    wait_value(&live, "k", "v2").await;
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

#[test]
fn range_snapshot_copies_mvcc_history_and_tombstones() {
    let root = std::env::temp_dir().join(format!(
        "ryme-range-mvcc-{}-{}",
        std::process::id(),
        now_ms()
    ));
    let _ = std::fs::remove_dir_all(&root);
    let source = Node::open(0, Vec::new(), Vec::new(), &root.join("source")).unwrap();
    let target = Node::open(1, Vec::new(), Vec::new(), &root.join("target")).unwrap();
    let key = key("history");
    for value in [b"v0".to_vec(), b"v1".to_vec()] {
        let mut txn = source.manager().begin();
        source.manager().put(&mut txn, key.clone(), value);
        source.manager().commit(txn).unwrap();
    }
    let mut txn = source.manager().begin();
    source.manager().delete(&mut txn, key.clone());
    source.manager().commit(txn).unwrap();

    let snapshot = source.snapshot_range(&[], &[], 0, 16).unwrap();
    assert_eq!(snapshot.rows.len(), 1);
    assert!(snapshot.rows[0].value.is_empty());
    assert_eq!(snapshot.rows[0].versions.len(), 3);
    target.install_range_snapshot(&snapshot).unwrap();
    assert_eq!(target.snapshot_range(&[], &[], 0, 16).unwrap(), snapshot);

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn range_owner_respects_half_open_bounds() {
    let root = std::env::temp_dir().join(format!("ryme-range-owner-{}-{}", std::process::id(), now_ms()));
    let _ = std::fs::remove_dir_all(&root);
    let node = Node::open(0, Vec::new(), Vec::new(), &root).unwrap();
    node.set_range_owners(vec![
        RangeOwner { start: b"a".to_vec(), end: b"m".to_vec(), owner: 1, epoch: 4 },
        RangeOwner { start: b"m".to_vec(), end: Vec::new(), owner: 2, epoch: 5 },
    ]);
    assert_eq!(node.range_owner(b"a"), Some((1, 4)));
    assert_eq!(node.range_owner(b"l"), Some((1, 4)));
    assert_eq!(node.range_owner(b"m"), Some((2, 5)));
    assert_eq!(node.range_owner(b"z"), Some((2, 5)));
    assert_eq!(node.range_owner(b"0"), None);
    let _ = std::fs::remove_dir_all(&root);
}
