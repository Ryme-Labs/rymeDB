use ryme_raft::net::{MeshTransport, Node};
use ryme_storage::RecordKey;
use std::collections::BTreeMap;
use std::time::Duration;

fn fixture(name: &str) -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../ryme-wire-native/tests/fixtures")
        .join(name)
}

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

fn transport() -> MeshTransport {
    let acceptor = ryme_tls::TlsAcceptor::from_pem_files_mutual(
        fixture("ryme-test-mesh-cert.pem").as_path(),
        fixture("ryme-test-mesh-key.pem").as_path(),
        fixture("ryme-test-mesh-ca-cert.pem").as_path(),
    )
    .unwrap();
    let connector = ryme_tls::MeshConnector::from_pem_files(
        fixture("ryme-test-mesh-ca-cert.pem").as_path(),
        Some(fixture("ryme-test-mesh-client-cert.pem").as_path()),
        Some(fixture("ryme-test-mesh-client-key.pem").as_path()),
    )
    .unwrap();
    MeshTransport { acceptor, connector }
}

async fn wait_leader(nodes: &[std::sync::Arc<Node>]) -> usize {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        for (index, node) in nodes.iter().enumerate() {
            if node.is_leader().await {
                return index;
            }
        }
        assert!(tokio::time::Instant::now() < deadline, "no leader elected");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_value(nodes: &[std::sync::Arc<Node>], name: &str, value: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let key = RecordKey::new("t", "d", "s", name.as_bytes());
    loop {
        let mut done = true;
        for node in nodes {
            if node.read_latest(&key).await.unwrap() != Some(value.as_bytes().to_vec()) {
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
async fn mesh_tls_elect_and_replicate() {
    let root = std::env::temp_dir().join(format!(
        "ryme-mesh-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0)
    ));
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
        node.set_mesh_tls(transport());
        assert!(node.mesh_enabled());
        let listener = listeners.remove(0);
        node_tasks.push(node.spawn(listener));
        nodes.push(node);
    }
    let first = wait_leader(&nodes).await;
    nodes[first].propose_write(writes(vec![("mk", "mv1")])).await.unwrap();
    wait_value(&nodes, "mk", "mv1").await;
    for (node, tasks) in nodes.iter().zip(node_tasks) {
        node.shutdown(tasks);
    }
    let _ = std::fs::remove_dir_all(&root);
}
