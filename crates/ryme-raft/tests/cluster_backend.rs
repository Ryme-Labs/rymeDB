use ryme_raft::net::{ClusterBackend, Node, RangeOwner};
use ryme_storage::RecordKey;
use ryme_txn::{Isolation, TxnBackend};
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
    let filtered_snapshot = nodes[first]
        .fetch_range_snapshot_for(
            target,
            Vec::new(),
            Vec::new(),
            0,
            16,
            String::from("t"),
            String::from("d"),
        )
        .await
        .unwrap();
    assert_eq!(filtered_snapshot.rows.len(), 1);
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
    let owner_values =
        nodes[first].fetch_range_values(target, vec![key("k"), key("missing")], 0).await.unwrap();
    assert_eq!(owner_values, vec![(Some(b"v1".to_vec()), Some(0)), (None, None)]);
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

#[tokio::test]
async fn configured_range_owner_limits_follower_materialization() {
    let root = std::env::temp_dir().join(format!("ryme-owner-{}-{}", std::process::id(), now_ms()));
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
            .map(|(_, address)| address.to_string())
            .collect();
        let peer_ids: Vec<usize> = (0..3).filter(|i| *i != id).collect();
        let node = Node::open(id, peers, peer_ids, &root.join(format!("n{id}"))).unwrap();
        tasks.push(node.spawn(listeners.remove(0)));
        nodes.push(node);
    }
    let backends: Vec<ClusterBackend> = nodes.iter().cloned().map(ClusterBackend::new).collect();
    let leader = wait_leader(&nodes, None).await;
    let owner = (leader + 1) % nodes.len();
    let mut start = b"s".to_vec();
    start.push(0);
    let placement = RangeOwner { start, end: Vec::new(), owner, epoch: 1 };
    for node in &nodes {
        node.set_range_owners(vec![placement.clone()]);
    }

    let mut txn = backends[leader].begin();
    backends[leader].put(&mut txn, key("owned"), b"value".to_vec());
    backends[leader].commit(txn).await.unwrap();

    let non_owner = (0..nodes.len()).find(|id| *id != leader && *id != owner).unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let owner_value = nodes[owner].read_latest(&key("owned")).await.unwrap();
        let non_owner_value = nodes[non_owner].read_latest(&key("owned")).await.unwrap();
        if owner_value == Some(b"value".to_vec()) && non_owner_value.is_none() {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "owner materialization did not converge");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(nodes[leader].read_latest(&key("owned")).await.unwrap(), Some(b"value".to_vec()));

    let mut routed_read = backends[non_owner].begin();
    assert_eq!(
        backends[non_owner].get_async(&mut routed_read, &key("owned")).await.unwrap(),
        Some(b"value".to_vec())
    );
    assert!(routed_read.read_keys().contains(&key("owned")));

    for (node, task) in nodes.iter().zip(tasks) {
        node.shutdown(task);
    }
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn single_owner_write_from_follower_routes_through_mesh() {
    let root =
        std::env::temp_dir().join(format!("ryme-write-route-{}-{}", std::process::id(), now_ms()));
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
            .map(|(_, address)| address.to_string())
            .collect();
        let peer_ids: Vec<usize> = (0..3).filter(|i| *i != id).collect();
        let node = Node::open(id, peers, peer_ids, &root.join(format!("n{id}"))).unwrap();
        tasks.push(node.spawn(listeners.remove(0)));
        nodes.push(node);
    }
    let backends: Vec<ClusterBackend> = nodes.iter().cloned().map(ClusterBackend::new).collect();
    let leader = wait_leader(&nodes, None).await;
    let owner = (leader + 1) % nodes.len();
    let client = (leader + 2) % nodes.len();
    let mut start = b"s".to_vec();
    start.push(0);
    let placement = RangeOwner { start, end: Vec::new(), owner, epoch: 1 };
    for node in &nodes {
        node.set_range_owners(vec![placement.clone()]);
    }

    let mut seed = backends[leader].begin();
    backends[leader].put(&mut seed, key("seed"), b"seed".to_vec());
    backends[leader].commit(seed).await.unwrap();

    let mut txn = backends[client].begin();
    backends[client].put(&mut txn, key("routed"), b"from-follower".to_vec());
    backends[client].commit(txn).await.unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let owner_value = nodes[owner].read_latest(&key("routed")).await.unwrap();
        if owner_value == Some(b"from-follower".to_vec()) {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "routed write did not reach owner");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        nodes[leader].read_latest(&key("routed")).await.unwrap(),
        Some(b"from-follower".to_vec())
    );
    assert_eq!(nodes[client].read_latest(&key("routed")).await.unwrap(), None);

    for (node, task) in nodes.iter().zip(tasks) {
        node.shutdown(task);
    }
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn multi_range_write_only_transaction_forwards_to_leader() {
    let root =
        std::env::temp_dir().join(format!("ryme-multi-route-{}-{}", std::process::id(), now_ms()));
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
            .map(|(_, address)| address.to_string())
            .collect();
        let peer_ids: Vec<usize> = (0..3).filter(|i| *i != id).collect();
        let node = Node::open(id, peers, peer_ids, &root.join(format!("n{id}"))).unwrap();
        tasks.push(node.spawn(listeners.remove(0)));
        nodes.push(node);
    }
    let backends: Vec<ClusterBackend> = nodes.iter().cloned().map(ClusterBackend::new).collect();
    let leader = wait_leader(&nodes, None).await;
    let upper_owner = (leader + 1) % nodes.len();
    let client = (leader + 2) % nodes.len();
    let mut split = b"s".to_vec();
    split.push(0);
    split.extend_from_slice(b"m");
    let mut start = b"s".to_vec();
    start.push(0);
    let upper_start = split.clone();
    let placement = vec![
        RangeOwner { start, end: split, owner: leader, epoch: 1 },
        RangeOwner { start: upper_start, end: Vec::new(), owner: upper_owner, epoch: 1 },
    ];
    for node in &nodes {
        node.set_range_owners(placement.clone());
    }

    let mut seed = backends[leader].begin();
    backends[leader].put(&mut seed, key("seed"), b"seed".to_vec());
    backends[leader].commit(seed).await.unwrap();

    let mut txn = backends[client].begin();
    backends[client].put(&mut txn, key("a"), b"left".to_vec());
    backends[client].put(&mut txn, key("z"), b"right".to_vec());
    backends[client].commit(txn).await.unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let left = nodes[leader].read_latest(&key("a")).await.unwrap();
        let right = nodes[upper_owner].read_latest(&key("z")).await.unwrap();
        if left == Some(b"left".to_vec()) && right == Some(b"right".to_vec()) {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "multi-range write did not converge");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(nodes[client].read_latest(&key("a")).await.unwrap(), None);
    assert_eq!(nodes[client].read_latest(&key("z")).await.unwrap(), None);

    let mut routed_scan = backends[client].begin();
    let first_page = backends[client].scan_async(&mut routed_scan, "t", "d", "s", 2).await.unwrap();
    assert_eq!(
        first_page,
        vec![(b"a".to_vec(), b"left".to_vec()), (b"seed".to_vec(), b"seed".to_vec())]
    );
    let second_page = backends[client]
        .scan_after_async(&mut routed_scan, "t", "d", "s", b"seed", 2)
        .await
        .unwrap();
    assert_eq!(second_page, vec![(b"z".to_vec(), b"right".to_vec())]);
    assert!(routed_scan.scanned_tables().contains(&(
        String::from("t"),
        String::from("d"),
        String::from("s")
    )));

    let executor = ryme_sql::Executor::with_backend(
        String::from("t"),
        String::from("d"),
        backends[client].clone(),
    );
    let mut sql_txn = executor.begin_transaction(Isolation::Serializable);
    let (result, _) = executor
        .execute_in_transaction(
            &mut sql_txn,
            ryme_sql::parse("SELECT * FROM s ORDER BY id").unwrap(),
        )
        .await
        .unwrap();
    assert!(matches!(result, ryme_sql::QueryResult::Rows { rows } if rows == vec![
        (b"a".to_vec(), b"left".to_vec()),
        (b"seed".to_vec(), b"seed".to_vec()),
        (b"z".to_vec(), b"right".to_vec()),
    ]));

    let mut update_txn = executor.begin_transaction(Isolation::Serializable);
    let (_, changes) = executor
        .execute_in_transaction(
            &mut update_txn,
            ryme_sql::parse("UPDATE s SET value = 'changed' WHERE id = 'z'").unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(changes.len(), 1);
    executor.commit_transaction(update_txn, changes).await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if nodes[upper_owner].read_latest(&key("z")).await.unwrap() == Some(b"changed".to_vec()) {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "remote update did not converge");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let mut stale = backends[client].begin();
    backends[client].put(&mut stale, key("stale"), b"must-not-commit".to_vec());
    let moved = placement
        .iter()
        .cloned()
        .map(|mut range| {
            range.epoch = 2;
            range
        })
        .collect::<Vec<_>>();
    for node in &nodes {
        node.set_range_owners(moved.clone());
    }
    let error = backends[client].commit(stale).await.unwrap_err();
    assert_eq!(error, ryme_error::RymeError::Conflict(String::from("range topology changed")));

    for (node, task) in nodes.iter().zip(tasks) {
        node.shutdown(task);
    }
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn follower_transaction_forwarding_preserves_read_conflicts() {
    let root =
        std::env::temp_dir().join(format!("ryme-txn-route-{}-{}", std::process::id(), now_ms()));
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
            .map(|(_, address)| address.to_string())
            .collect();
        let peer_ids: Vec<usize> = (0..3).filter(|i| *i != id).collect();
        let node = Node::open(id, peers, peer_ids, &root.join(format!("n{id}"))).unwrap();
        tasks.push(node.spawn(listeners.remove(0)));
        nodes.push(node);
    }
    let backends: Vec<ClusterBackend> = nodes.iter().cloned().map(ClusterBackend::new).collect();
    let leader = wait_leader(&nodes, None).await;
    let client = (leader + 1) % nodes.len();

    let mut seed = backends[leader].begin();
    backends[leader].put(&mut seed, key("watched"), b"v1".to_vec());
    backends[leader].commit(seed).await.unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let mut probe = backends[client].begin();
        if backends[client].get(&mut probe, &key("watched")).unwrap() == Some(b"v1".to_vec()) {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "seed did not reach follower");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let mut stale = backends[client].begin();
    assert_eq!(backends[client].get(&mut stale, &key("watched")).unwrap(), Some(b"v1".to_vec()));

    let mut update = backends[leader].begin();
    backends[leader].put(&mut update, key("watched"), b"v2".to_vec());
    backends[leader].commit(update).await.unwrap();

    backends[client].put(&mut stale, key("other"), b"value".to_vec());
    let result = backends[client].commit(stale).await;
    assert!(matches!(result, Err(ryme_error::RymeError::Conflict(_))));

    for (node, task) in nodes.iter().zip(tasks) {
        node.shutdown(task);
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
    let root =
        std::env::temp_dir().join(format!("ryme-range-mvcc-{}-{}", std::process::id(), now_ms()));
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
    let root =
        std::env::temp_dir().join(format!("ryme-range-owner-{}-{}", std::process::id(), now_ms()));
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
