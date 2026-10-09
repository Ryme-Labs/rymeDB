#[tokio::test]
async fn sweep_removes_expired() {
    let dir = std::env::temp_dir().join(format!("ryme-sweep-{}-{}", std::process::id(), now_ms()));
    let _ = std::fs::remove_dir_all(&dir);
    let durable =
        ryme_txn::DurableManager::open(&dir, 1024 * 1024, ryme_txn::SyncPolicy::Never).unwrap();
    let backend = ryme_server::Backend::Single(durable);
    let tenant = "t";
    let database = "d";
    let table = "_kv";
    let key = |name: &str| ryme_storage::RecordKey::new(tenant, database, table, name.as_bytes());
    {
        let manager = match &backend {
            ryme_server::Backend::Single(manager) => manager.inner(),
            _ => panic!("single"),
        };
        let mut txn = manager.begin();
        manager.put_with_ttl(&mut txn, key("gone"), b"v".to_vec(), 1);
        manager.put(&mut txn, key("kept"), b"v".to_vec());
        manager.commit(txn).unwrap();
    }
    assert_eq!(backend.sweep_table(tenant, database, table, 100, None).await.unwrap(), 1);
    {
        let manager = match &backend {
            ryme_server::Backend::Single(manager) => manager.inner(),
            _ => panic!("single"),
        };
        let mut txn = manager.begin();
        assert_eq!(manager.get(&mut txn, &key("gone")).unwrap(), None);
        assert_eq!(manager.get(&mut txn, &key("kept")).unwrap(), Some(b"v".to_vec()));
    }
    assert_eq!(backend.sweep_once(None).await, 0);
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn sweep_emits_cdc_delete() {
    let dir =
        std::env::temp_dir().join(format!("ryme-sweep-cdc-{}-{}", std::process::id(), now_ms()));
    let _ = std::fs::remove_dir_all(&dir);
    let durable =
        ryme_txn::DurableManager::open(&dir, 1024 * 1024, ryme_txn::SyncPolicy::Never).unwrap();
    let backend = ryme_server::Backend::Single(durable);
    let tenant = "t";
    let database = "d";
    let table = "_kv";
    let key = |name: &str| ryme_storage::RecordKey::new(tenant, database, table, name.as_bytes());
    {
        let manager = match &backend {
            ryme_server::Backend::Single(manager) => manager.inner(),
            _ => panic!("single"),
        };
        let mut txn = manager.begin();
        manager.put_with_ttl(&mut txn, key("gone"), b"v".to_vec(), 1);
        manager.commit(txn).unwrap();
    }
    let realtime = ryme_realtime::Realtime::new(64);
    let mut rx = realtime.subscribe(tenant, database, table);
    assert_eq!(
        backend.sweep_table(tenant, database, table, 100, Some(&realtime)).await.unwrap(),
        1
    );
    let record = rx.try_recv().unwrap();
    assert_eq!(record.op, ryme_realtime::Operation::Delete);
    assert_eq!(record.pk, b"gone".to_vec());
    assert_eq!(record.before, None);
    assert_eq!(record.after, None);
    let _ = std::fs::remove_dir_all(&dir);
}

fn now_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}
