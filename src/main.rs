use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use parking_lot::RwLock;
use tokio::runtime::Handle;
use tracing::{error, info};
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;

use tinycfs::cluster::Cluster;
use tinycfs::config::Config;
use tinycfs::consensus::Consensus;
use tinycfs::fs::store::FileStore;
use tinycfs::fs::TinyCfs;
use tinycfs::persistence::RaftDb;
use tinycfs::syslog_layer::SyslogLayer;

// ─── CLI ─────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(
    name = "tinycfs",
    version,
    about = "Tiny Cluster File System — distributed shared filesystem for small files",
    long_about = "Mount a POSIX-compliant shared filesystem on each cluster node.\n\
                  All nodes see the same files; writes are replicated via Raft consensus.\n\
                  Default mount flags: noatime, nodiratime, noexec (safe for config files)."
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

    // ── Access control ────────────────────────────────────────────────────

    #[arg(long)]
    allow_other: bool,

    #[arg(long)]
    allow_root: bool,

    // ── Execution control ─────────────────────────────────────────────────

    #[arg(long, default_value_t = true)]
    noexec: bool,

    #[arg(long, conflicts_with = "noexec", default_value_t = false)]
    exec: bool,

    // ── Performance / atime ───────────────────────────────────────────────

    #[arg(long, default_value_t = true)]
    noatime: bool,

    #[arg(long, default_value_t = true)]
    nodiratime: bool,

    #[arg(long, default_value_t = false)]
    relatime: bool,

    // ── Write protection ──────────────────────────────────────────────────

    #[arg(long, short = 'r')]
    read_only: bool,

    // ── SUID / device files ───────────────────────────────────────────────

    #[arg(long, default_value_t = true)]
    nosuid: bool,

    #[arg(long, default_value_t = true)]
    nodev: bool,

    // ── Kernel caching ────────────────────────────────────────────────────

    #[arg(long, default_value_t = false)]
    direct_io: bool,

    // ── Auto-unmount ──────────────────────────────────────────────────────

    #[arg(long, default_value_t = false)]
    no_auto_unmount: bool,
}

// ─── Main ─────────────────────────────────────────────────────────────────────

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    // ── Logging setup ──────────────────────────────────────────────────────
    // Two layers:
    //   1. fmt  — structured stdout/stderr output controlled by RUST_LOG
    //   2. syslog — WARN/ERROR events forwarded to the system syslog daemon
    //              (LOG_DAEMON facility), visible in journalctl / /var/log/syslog
    SyslogLayer::init();
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer().with_filter(
                EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
            ),
        )
        .with(SyslogLayer)
        .init();

    let args = Args::parse();
    let exec_enabled = args.exec || !args.noexec;

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

    // Write a structured startup notice to syslog so operators can confirm
    // the node started with the expected configuration without tailing stdout.
    let peer_list = config
        .nodes
        .iter()
        .filter(|n| n.name != config.local_node)
        .map(|n| format!("{}={}:{}", n.name, n.ip, n.port))
        .collect::<Vec<_>>()
        .join(", ");
    SyslogLayer::startup(&format!(
        "starting node='{}' cluster='{}' mountpoint='{}' peers=[{}]",
        config.local_node,
        config.cluster_name,
        args.mountpoint.display(),
        peer_list,
    ));

    // ── Persistence (optional) ────────────────────────────────────────────
    let db = if let Some(ref data_dir) = config.data_dir {
        let db_path = std::path::Path::new(data_dir).join("raft.db");
        if let Err(e) = std::fs::create_dir_all(data_dir) {
            error!("Cannot create data_dir {:?}: {}", data_dir, e);
            std::process::exit(1);
        }
        match RaftDb::open(&db_path) {
            Ok(d) => {
                info!("Opened Raft DB at {:?}", db_path);
                Some(d)
            }
            Err(e) => {
                error!("Failed to open Raft DB {:?}: {}", db_path, e);
                std::process::exit(1);
            }
        }
    } else {
        info!("No data_dir configured — running with in-memory state (no persistence)");
        None
    };

    // ── Filesystem store ──────────────────────────────────────────────────
    let store: Arc<RwLock<FileStore>> = Arc::new(RwLock::new(FileStore::new()));

    // ── Cluster networking ────────────────────────────────────────────────
    let (cluster_handle, _cluster) = Cluster::start(config.clone()).await;

    // ── Consensus engine ──────────────────────────────────────────────────
    // Consensus::new loads snapshot + log from DB (if any) into `store`.
    info!("Consensus algorithm: {:?}", config.algorithm);
    let (consensus, msg_tx) =
        Consensus::new(config.algorithm.clone(), cluster_handle.clone(), db, store.clone(), config.snapshot_every as u64);

    // Forward inbound cluster messages to the Raft actor.
    let rx_in = cluster_handle.rx_in.clone();
    tokio::spawn(async move {
        loop {
            let env = rx_in.lock().await.recv().await;
            match env {
                Some(envelope) => {
                    if msg_tx.send(envelope).await.is_err() {
                        break;
                    }
                }
                None => break,
            }
        }
    });

    // ── FUSE mount options ────────────────────────────────────────────────
    let mountpoint = args.mountpoint.clone();
    if !mountpoint.exists() {
        error!("Mount point {:?} does not exist", mountpoint);
        std::process::exit(1);
    }

    let mut fuse_opts: Vec<fuser::MountOption> = vec![
        fuser::MountOption::FSName("tinycfs".to_string()),
        fuser::MountOption::Subtype("tinycfs".to_string()),
        fuser::MountOption::DefaultPermissions,
    ];

    if !args.no_auto_unmount {
        fuse_opts.push(fuser::MountOption::AutoUnmount);
    }
    if args.allow_other {
        fuse_opts.push(fuser::MountOption::AllowOther);
    }
    if args.allow_root {
        fuse_opts.push(fuser::MountOption::AllowRoot);
    }
    if args.read_only {
        fuse_opts.push(fuser::MountOption::RO);
        info!("Mounted read-only (--read-only)");
    }
    if exec_enabled {
        fuse_opts.push(fuser::MountOption::Exec);
    } else {
        fuse_opts.push(fuser::MountOption::NoExec);
    }
    if args.nosuid {
        fuse_opts.push(fuser::MountOption::NoSuid);
    }
    if args.nodev {
        fuse_opts.push(fuser::MountOption::NoDev);
    }
    if args.noatime {
        fuse_opts.push(fuser::MountOption::NoAtime);
    }
    if args.relatime && !args.noatime {
        fuse_opts.push(fuser::MountOption::CUSTOM("relatime".to_string()));
    }
    if args.direct_io {
        fuse_opts.push(fuser::MountOption::CUSTOM("direct_io".to_string()));
    }

    info!(
        "Mounting at {:?} [noexec={}, noatime={}, nodiratime={}, nosuid={}, \
         nodev={}, ro={}, direct_io={}]",
        mountpoint,
        !exec_enabled,
        args.noatime,
        args.nodiratime,
        args.nosuid,
        args.nodev,
        args.read_only,
        args.direct_io,
    );

    let rt_handle = Handle::current();
    let filesystem = TinyCfs::new(
        rt_handle,
        consensus,
        store,
        config.local_node.clone(),
        config.max_file_size_bytes,
        config.max_fs_size_bytes,
    );

    tokio::task::spawn_blocking(move || {
        if let Err(e) = fuser::mount2(filesystem, &mountpoint, &fuse_opts) {
            error!("FUSE mount error: {}", e);
        }
    })
    .await
    .unwrap_or_else(|e| error!("FUSE thread panicked: {}", e));

    info!("Filesystem unmounted, shutting down");
}
