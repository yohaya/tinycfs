mod cluster;
mod config;
mod consensus;
mod error;
mod fs;

use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use parking_lot::RwLock;
use tokio::runtime::Handle;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use crate::config::Config;
use crate::consensus::Consensus;
use crate::fs::store::FileStore;
use crate::fs::TinyCfs;

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

#[tokio::main]
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
    let (cluster_handle, _cluster) = cluster::Cluster::start(config.clone()).await;

    // ── Consensus engine ──────────────────────────────────────────────────
    let (consensus, mut apply_rx) = Consensus::new(cluster_handle.clone());
    consensus.start();

    // ── Filesystem state machine ──────────────────────────────────────────
    let store: Arc<RwLock<FileStore>> = Arc::new(RwLock::new(FileStore::new()));

    // Apply committed Raft entries to the local filesystem state.
    let store2 = store.clone();
    tokio::spawn(async move {
        while let Some(entry) = apply_rx.recv().await {
            let mut st = store2.write();
            if let Err(e) = st.apply(&entry.op) {
                // Log but don't crash; idempotent operations may produce benign errors.
                tracing::warn!("State machine apply error: {}", e);
            }
        }
    });

    // ── Message dispatch ──────────────────────────────────────────────────
    let consensus2 = consensus.clone();
    let rx_in = cluster_handle.rx_in.clone();
    tokio::spawn(async move {
        loop {
            let env = rx_in.lock().await.recv().await;
            match env {
                Some(envelope) => consensus2.handle_message(envelope.from, envelope.msg),
                None => break,
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

    // Mount FUSE in a blocking thread so the tokio runtime keeps running.
    tokio::task::spawn_blocking(move || {
        if let Err(e) = fuser::mount2(filesystem, &mountpoint, &fuse_opts) {
            error!("FUSE mount error: {}", e);
        }
    })
    .await
    .unwrap_or_else(|e| error!("FUSE thread panicked: {}", e));

    info!("Filesystem unmounted, shutting down");
}
