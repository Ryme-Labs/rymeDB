use std::sync::{Arc, Mutex};

fn now_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

#[test]
fn reconcile_resets_stored_bytes_to_actual() {
    let dir =
        std::env::temp_dir().join(format!("ryme-qosbytes-{}-{}", std::process::id(), now_ms()));
    let _ = std::fs::remove_dir_all(&dir);
    let durable =
        ryme_txn::DurableManager::open(&dir, 1024 * 1024, ryme_txn::SyncPolicy::Never).unwrap();
    let backend = ryme_server::Backend::Single(durable);
    let tenant = "t";
    let database = "d";
    let table = "docs";
    let key = |name: &str| ryme_storage::RecordKey::new(tenant, database, table, name.as_bytes());
    let manager = match &backend {
        ryme_server::Backend::Single(manager) => manager.inner(),
        _ => panic!("single"),
    };
    let mut txn = manager.begin();
    manager.put(&mut txn, key("k1"), vec![7u8; 64]);
    manager.put(&mut txn, key("k2"), vec![7u8; 64]);
    manager.commit(txn).unwrap();
    let qos = Arc::new(Mutex::new(ryme_qos::QosRegistry::new()));
    {
        let mut registry = qos.lock().unwrap();
        registry.admit_write(tenant, 100, 0).unwrap();
        registry.admit_write(tenant, 100, 0).unwrap();
        registry.admit_write(tenant, 100, 0).unwrap();
        assert!(registry.view(tenant).map(|v| v.stored_bytes).unwrap_or(0) >= 300);
    }
    backend.reconcile_qos(&qos);
    let actual = backend.stored_bytes_by_tenant().get(tenant).copied().unwrap_or(0);
    assert!(actual > 0 && actual < 300, "{actual}");
    assert_eq!(qos.lock().unwrap().view(tenant).map(|v| v.stored_bytes), Some(actual));
    let mut txn = manager.begin();
    manager.delete(&mut txn, key("k2"));
    manager.commit(txn).unwrap();
    backend.reconcile_qos(&qos);
    let retained = backend.stored_bytes_by_tenant().get(tenant).copied().unwrap_or(0);
    assert_eq!(qos.lock().unwrap().view(tenant).map(|v| v.stored_bytes), Some(retained));
    assert!(retained < 300, "{retained}");
    let _ = std::fs::remove_dir_all(&dir);
}
