use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use parking_lot::RwLock;
use tokio::runtime::Handle;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

use tinycfs::cluster::Cluster;
use tinycfs::config::Config;
use tinycfs::consensus::Consensus;
use tinycfs::fs::store::FileStore;
use tinycfs::fs::TinyCfs;

#[derive(Parser, Debug)]
#[command(
    name = "tinycfs",
    version,
    about = "Tiny Cluster File System — distributed shared filesystem for small files"
)]
struct Args {
    /// Directory to mount the shared filesystem on.
    mountpoint: PathBuf,

    /// Path to the JSON configuration file.
    #[arg(
        short,
        long,
        default_value = "/etc/tinycfs/tinycfs.conf",
        env = "TINYCFS_CONF"
    )]
    config: PathBuf,

    /// Allow other users to access the mounted filesystem.
    #[arg(long)]
    allow_other: bool,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    // ── Logging ───────────────────────────────────────────────────────────
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();

    // ── Configuration ─────────────────────────────────────────────────────
    let config = match Config::load(&args.config) {
        Ok(c) => c,
        Err(e) => {
            error!("Failed to load config {:?}: {}", args.config, e);
            std::process::exit(1);
        }
    };
    info!(
        "Starting tinycfs node '{}' in cluster '{}'",
        config.local_node, config.cluster_name
    );

    // ── Cluster networking ────────────────────────────────────────────────
    let (cluster_handle, _cluster) = Cluster::start(config.clone()).await;

    // ── Consensus engine ──────────────────────────────────────────────────
    // `msg_tx`: pipe all incoming cluster messages here.
    // `apply_rx`: committed log entries in order, for the state machine.
    let (consensus, mut apply_rx, msg_tx) = Consensus::new(cluster_handle.clone());

    // ── Forward incoming cluster messages to the Raft actor ───────────────
    let rx_in = cluster_handle.rx_in.clone();
    tokio::spawn(async move {
        loop {
            let env = rx_in.lock().await.recv().await;
            match env {
                Some(envelope) => {
                    if msg_tx.send(envelope).await.is_err() {
                        break; // Raft actor shut down
                    }
                }
                None => break,
            }
        }
    });

    // ── Filesystem state machine ──────────────────────────────────────────
    let store: Arc<RwLock<FileStore>> = Arc::new(RwLock::new(FileStore::new()));

    // Apply committed Raft entries to the local in-memory inode tree.
    let store2 = store.clone();
    tokio::spawn(async move {
        while let Some(entry) = apply_rx.recv().await {
            let mut st = store2.write();
            if let Err(e) = st.apply(&entry.op) {
                warn!("State machine apply error: {}", e);
            }
        }
    });

    // ── FUSE mount ────────────────────────────────────────────────────────
    let mountpoint = args.mountpoint.clone();
    if !mountpoint.exists() {
        error!("Mount point {:?} does not exist", mountpoint);
        std::process::exit(1);
    }

    let rt_handle = Handle::current();
    let filesystem = TinyCfs::new(rt_handle, consensus, store);

    let mut fuse_opts: Vec<fuser::MountOption> = vec![
        fuser::MountOption::FSName("tinycfs".to_string()),
        fuser::MountOption::AutoUnmount,
        fuser::MountOption::DefaultPermissions,
    ];
    if args.allow_other {
        fuse_opts.push(fuser::MountOption::AllowOther);
    }

    info!("Mounting filesystem at {:?}", mountpoint);

    // Run FUSE in a dedicated blocking thread; the tokio runtime continues on
    // other threads serving the cluster and consensus tasks.
    tokio::task::spawn_blocking(move || {
        if let Err(e) = fuser::mount2(filesystem, &mountpoint, &fuse_opts) {
            error!("FUSE mount error: {}", e);
        }
    })
    .await
    .unwrap_or_else(|e| error!("FUSE thread panicked: {}", e));

    info!("Filesystem unmounted, shutting down");
}
