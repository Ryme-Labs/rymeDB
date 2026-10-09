async fn serve() -> std::net::SocketAddr {
    let gateway = ryme_wire_pg::PgGateway::new(String::from("t"), String::from("d"));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = gateway.serve(listener).await;
    });
    addr
}

fn psql_available() -> bool {
    std::process::Command::new("psql")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

async fn psql(addr: std::net::SocketAddr, sql: &str) -> (bool, String) {
    let output = tokio::process::Command::new("psql")
        .arg("-h")
        .arg("127.0.0.1")
        .arg("-p")
        .arg(addr.port().to_string())
        .arg("-U")
        .arg("ryme")
        .arg("-d")
        .arg("postgres")
        .arg("-tAX")
        .arg("-v")
        .arg("ON_ERROR_STOP=1")
        .arg("-c")
        .arg(sql)
        .output()
        .await
        .unwrap();
    let mut text = String::from_utf8_lossy(&output.stdout).into_owned();
    text.push_str(&String::from_utf8_lossy(&output.stderr));
    (output.status.success(), text.trim().to_string())
}

#[tokio::test]
async fn psql_smoke() {
    if !psql_available() {
        return;
    }
    let addr = serve().await;
    let (ok, out) = psql(addr, "SHOW server_version;").await;
    assert!(ok, "{out}");
    assert!(out.contains("16.0"), "{out}");
    let (ok, _) = psql(addr, "INSERT INTO docs KEY 'k1' VALUE 'v1';").await;
    assert!(ok);
    let (ok, out) = psql(addr, "SELECT * FROM docs KEY 'k1';").await;
    assert!(ok, "{out}");
    assert!(out.contains("v1"), "{out}");
    let (ok, _) = psql(addr, "INSERT INTO docs (id, value) VALUES ('k2', 'v2');").await;
    assert!(ok);
    let (ok, _) = psql(
        addr,
        "INSERT INTO docs (id, value) VALUES ('k2', 'v3') ON CONFLICT (id) DO UPDATE SET value = EXCLUDED.value;",
    )
    .await;
    assert!(ok);
    let (ok, out) = psql(addr, "SELECT * FROM docs KEY 'k2';").await;
    assert!(ok, "{out}");
    assert!(out.contains("v3"), "{out}");
    let (ok, out) = psql(
        addr,
        "INSERT INTO docs (id, payload) VALUES ('std', 'before'); SELECT * FROM docs WHERE id = 'std'; UPDATE docs SET payload = 'after' WHERE id = 'std'; SELECT * FROM docs WHERE id = 'std'; DELETE FROM docs WHERE id = 'std'; SELECT * FROM docs WHERE id = 'std';",
    )
    .await;
    assert!(ok, "{out}");
    assert!(out.contains("before"), "{out}");
    assert!(out.contains("after"), "{out}");
    let (ok, out) = psql(
        addr,
        "INSERT INTO docs (id, value) VALUES ('returning', 'one') RETURNING id, value; UPDATE docs SET value = 'two' WHERE id = 'returning' RETURNING *; DELETE FROM docs WHERE id = 'returning' RETURNING id, value;",
    )
    .await;
    assert!(ok, "{out}");
    assert!(out.contains("returning|one"), "{out}");
    assert!(out.contains("returning|two"), "{out}");
    let (ok, out) = psql(
        addr,
        "CREATE TABLE IF NOT EXISTS public.messages (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), payload JSONB NOT NULL, created_at TIMESTAMPTZ DEFAULT now()); SELECT table_schema, table_name FROM information_schema.tables WHERE table_name = 'messages'; SELECT column_name, data_type, is_nullable, column_default FROM information_schema.columns WHERE table_name = 'messages' ORDER BY ordinal_position;",
    )
    .await;
    assert!(ok, "{out}");
    assert!(out.contains("public|messages"), "{out}");
    assert!(out.contains("payload|jsonb|NO"), "{out}");
    assert!(out.contains("id|uuid|NO|gen_random_uuid()"), "{out}");
    assert!(out.contains("created_at|timestamp with time zone|YES|now()"), "{out}");
    let (ok, out) = psql(
        addr,
        "CREATE TABLE public.events (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), payload TEXT NOT NULL, created_at TIMESTAMPTZ DEFAULT now()); INSERT INTO public.events (payload) VALUES ('hello') RETURNING *;",
    )
    .await;
    assert!(ok, "{out}");
    assert!(out.contains("hello"), "{out}");
    let (ok, out) = psql(
        addr,
        "CREATE TABLE public.fk_users (id TEXT PRIMARY KEY); CREATE TABLE public.fk_profiles (id TEXT PRIMARY KEY, user_id TEXT REFERENCES public.fk_users (id) ON DELETE SET NULL); SELECT nspname, oid FROM pg_catalog.pg_namespace WHERE nspname = 'public'; SELECT relname, relkind FROM pg_catalog.pg_class WHERE relname = 'messages'; SELECT attname, atttypid, attnotnull FROM pg_catalog.pg_attribute WHERE relname = 'messages' ORDER BY attnum; SELECT typname, oid FROM pg_catalog.pg_type WHERE typname = 'uuid'; SELECT conname, contype FROM pg_catalog.pg_constraint WHERE relname = 'messages'; SELECT conname, confdeltype, confupdtype FROM pg_catalog.pg_constraint WHERE relname = 'fk_profiles'; SELECT indexrelid, indrelid, indisunique FROM pg_catalog.pg_index WHERE relname = 'messages';",
    )
    .await;
    assert!(ok, "{out}");
    assert!(out.contains("public|2200"), "{out}");
    assert!(out.contains("messages|r"), "{out}");
    assert!(out.contains("id|2950|t"), "{out}");
    assert!(out.contains("uuid|2950"), "{out}");
    assert!(out.contains("messages_id_pkey|p"), "{out}");
    assert!(out.contains("fk_profiles_fkey_0|n|r"), "{out}");
    let (ok, out) = psql(
        addr,
        "CREATE TABLE public.projection (id TEXT PRIMARY KEY, payload TEXT, count INTEGER); INSERT INTO public.projection (id, payload, count) VALUES ('p1', 'hello', 3); SELECT payload, count FROM public.projection WHERE id = 'p1'; UPDATE public.projection SET payload = 'changed', count = 4 WHERE id = 'p1' RETURNING id, payload, count;",
    )
    .await;
    assert!(ok, "{out}");
    assert!(out.contains("hello|3"), "{out}");
    assert!(out.contains("p1|changed|4"), "{out}");
    let (ok, out) = psql(
        addr,
        "CREATE INDEX messages_payload_idx ON public.messages (payload); SELECT indexname, tablename FROM pg_catalog.pg_indexes WHERE tablename = 'messages';",
    )
    .await;
    assert!(ok, "{out}");
    assert!(out.contains("messages_payload_idx|messages"), "{out}");
    let (ok, out) = psql(
        addr,
        "BEGIN; INSERT INTO docs (id, value) VALUES ('tx', 'inside'); SELECT * FROM docs KEY 'tx'; COMMIT;",
    )
    .await;
    assert!(ok, "{out}");
    assert!(out.contains("inside"), "{out}");
    let (ok, out) = psql(
        addr,
        "BEGIN; INSERT INTO docs (id, value) VALUES ('rolled', 'gone'); ROLLBACK; SELECT * FROM docs KEY 'rolled';",
    )
    .await;
    assert!(ok, "{out}");
    assert!(!out.contains("gone"), "{out}");
    let (ok, out) = psql(addr, "SET application_name TO 'smoke'; SHOW application_name;").await;
    assert!(ok, "{out}");
    assert!(out.contains("smoke"), "{out}");
    let (ok, out) =
        psql(addr, "PREPARE qq AS SELECT * FROM docs KEY 'k1'; EXECUTE qq; DEALLOCATE qq;").await;
    assert!(ok, "{out}");
    assert!(out.contains("v1"), "{out}");
    let (ok, _) = psql(addr, "EXECUTE qq;").await;
    assert!(!ok);
    let (ok, out) = psql(addr, "SELECT * FROM nosuchtable KEY 'x';").await;
    assert!(ok, "{out}");
    assert_eq!(out, "");
    let (ok, _) = psql(addr, "SELEC bogus syntax here;").await;
    assert!(!ok);
}
