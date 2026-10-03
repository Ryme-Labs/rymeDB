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
