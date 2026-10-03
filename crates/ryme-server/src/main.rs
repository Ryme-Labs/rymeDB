use clap::Parser;
use ryme_config::Config;
use ryme_error::Result;
use std::net::SocketAddr;
use std::path::PathBuf;

#[derive(Debug, Parser)]
struct Args {
    #[arg(long)]
    pg_listen: Option<SocketAddr>,
    #[arg(long)]
    resp_listen: Option<SocketAddr>,
    #[arg(long)]
    native_listen: Option<SocketAddr>,
    #[arg(long)]
    grpc_listen: Option<SocketAddr>,
    #[arg(long)]
    grpc_tls_listen: Option<SocketAddr>,
    #[arg(long)]
    native_tls_listen: Option<SocketAddr>,
    #[arg(long)]
    tls_cert_pem: Option<PathBuf>,
    #[arg(long)]
    tls_key_pem: Option<PathBuf>,
    #[arg(long)]
    tls_client_ca_pem: Option<PathBuf>,
    #[arg(long)]
    https_listen: Option<SocketAddr>,
    #[arg(long)]
    resp_tls_listen: Option<SocketAddr>,
    #[arg(long)]
    raft_tls: bool,
    #[arg(long)]
    region: Option<String>,
    #[arg(long)]
    read_only: bool,
    #[arg(long)]
    http_listen: Option<SocketAddr>,
    #[arg(long)]
    raft_listen: Option<SocketAddr>,
    #[arg(long)]
    advertise_addr: Option<String>,
    #[arg(long)]
    data_dir: Option<PathBuf>,
    #[arg(long)]
    autosplit_writes: Option<u64>,
    #[arg(long)]
    autosplit_interval_secs: Option<u64>,
    #[arg(long)]
    verify_interval_secs: Option<u64>,
    #[arg(long)]
    config: Option<PathBuf>,
}

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("ryme-server: {e}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let args = Args::parse();
    let mut config = match args.config.as_ref() {
        Some(path) => Config::from_file(path)?,
        None => Config::default(),
    };
    if let Some(addr) = args.pg_listen {
        config.pg_listen = addr;
    }
    if let Some(addr) = args.resp_listen {
        config.resp_listen = addr;
    }
    if let Some(addr) = args.native_listen {
        config.native_listen = Some(addr);
    }
    if let Some(addr) = args.grpc_listen {
        config.grpc_listen = Some(addr);
    }
    if let Some(addr) = args.grpc_tls_listen {
        config.grpc_tls_listen = Some(addr);
    }
    if let Some(addr) = args.native_tls_listen {
        config.native_tls_listen = Some(addr);
    }
    if let Some(path) = args.tls_cert_pem {
        config.tls_cert_pem = Some(path);
    }
    if let Some(path) = args.tls_key_pem {
        config.tls_key_pem = Some(path);
    }
    if let Some(path) = args.tls_client_ca_pem {
        config.tls_client_ca_pem = Some(path);
    }
    if let Some(addr) = args.https_listen {
        config.https_listen = Some(addr);
    }
    if let Some(addr) = args.resp_tls_listen {
        config.resp_tls_listen = Some(addr);
    }
    if args.raft_tls {
        config.raft_tls = true;
    }
    if let Some(region) = args.region {
        config.region = region;
    }
    if args.read_only {
        config.read_only = true;
    }
    if let Some(addr) = args.http_listen {
        config.http_listen = addr;
    }
    if let Some(addr) = args.raft_listen {
        config.cluster.raft_listen = Some(addr);
    }
    if let Some(addr) = args.advertise_addr {
        config.cluster.advertise_addr = Some(addr);
    }
    if let Some(dir) = args.data_dir {
        config.data_dir = dir;
    }
    if let Some(writes) = args.autosplit_writes {
        config.autosplit_writes = writes;
    }
    if let Some(interval) = args.autosplit_interval_secs {
        config.autosplit_interval_secs = interval;
    }
    if let Some(interval) = args.verify_interval_secs {
        config.archive.verify_interval_secs = interval;
    }
    config.validate()?;
    let pg_listener = ryme_server::bind_retry(config.pg_listen).await?;
    let resp_listener = ryme_server::bind_retry(config.resp_listen).await?;
    let http_listener = ryme_server::bind_retry(config.http_listen).await?;
    eprintln!(
        "ryme-server {} pg={} resp={} native={:?} native_tls={:?} http={} raft={:?} dir={} region={} read_only={}",
        config.node_id,
        config.pg_listen,
        config.resp_listen,
        config.native_listen,
        config.native_tls_listen,
        config.http_listen,
        config.cluster.raft_listen,
        config.data_dir.display(),
        config.region,
        config.read_only
    );
    if let Some(raft_addr) = config.cluster.raft_listen {
        let raft_listener = ryme_server::bind_retry(raft_addr).await?;
        let handle = ryme_server::serve_cluster(
            config,
            pg_listener,
            resp_listener,
            http_listener,
            raft_listener,
        )
        .await?;
        let _ = tokio::signal::ctrl_c().await;
        handle.shutdown();
        return Ok(());
    }
    ryme_server::serve(config, pg_listener, resp_listener, http_listener).await
}
