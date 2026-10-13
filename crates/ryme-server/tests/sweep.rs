use ryme_txn::TxnBackend;

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

#[tokio::test]
async fn sharded_expiry_sweep_uses_global_commit_timestamp() {
    let dir = std::env::temp_dir().join(format!(
        "ryme-sweep-sharded-{}-{}",
        std::process::id(),
        now_ms()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let shards = ryme_shard::ShardSet::open(&dir, 4, ryme_txn::SyncPolicy::Never).unwrap();
    let mut tables_by_shard: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();
    let (expired_table, busy_table) = (0..64)
        .map(|index| {
            let table = format!("table-{index}");
            (table.clone(), shards.route_table("t", "d", &table))
        })
        .find_map(|(table, shard)| {
            let previous =
                tables_by_shard.iter().find(|(_, previous_shard)| **previous_shard != shard);
            match previous {
                Some((previous_table, _)) => Some((previous_table.clone(), table)),
                None => {
                    tables_by_shard.insert(table, shard);
                    None
                }
            }
        })
        .expect("tables should route to distinct shards");
    let backend = ryme_server::Backend::Sharded(shards);
    let expired_key = ryme_storage::RecordKey::new("t", "d", &expired_table, b"expired");
    let mut seed = backend.begin();
    backend.put_with_ttl(&mut seed, expired_key, b"value".to_vec(), 1);
    backend.commit(seed).await.unwrap();

    let mut latest_before_sweep = 0;
    for index in 0..5 {
        let mut txn = backend.begin();
        backend.put(
            &mut txn,
            ryme_storage::RecordKey::new("t", "d", &busy_table, format!("key-{index}").as_bytes()),
            b"value".to_vec(),
        );
        latest_before_sweep = backend.commit(txn).await.unwrap();
    }

    let realtime = ryme_realtime::Realtime::new(16);
    let mut receiver = realtime.subscribe("t", "d", &expired_table);
    assert_eq!(
        backend.sweep_table("t", "d", &expired_table, 10, Some(&realtime)).await.unwrap(),
        1
    );
    let deletion = receiver.try_recv().unwrap();
    assert!(deletion.commit_ts > latest_before_sweep);

    let _ = std::fs::remove_dir_all(&dir);
}

fn now_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}
