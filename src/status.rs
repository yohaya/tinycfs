//! Runtime cluster status: Unix socket server, periodic file writer, and
//! query client used by `tinycfs --status`.
//!
//! The running instance binds a Unix domain socket at
//! `/run/tinycfs/<node>.status.sock` and also refreshes
//! `/run/tinycfs/<node>.status` every 5 seconds.
//!
//! `tinycfs --status` connects to the socket, reads the plain-text snapshot,
//! and exits — always showing live data without file staleness.

use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixListener;
use tracing::warn;

use crate::cluster::ClusterHandle;
use crate::config::Config;
use crate::consensus::PubState;
use crate::fs::store::FileStore;

const STATUS_DIR: &str = "/run/tinycfs";
const FILE_REFRESH_SECS: u64 = 5;

/// Unix socket path for a given node name.
pub fn socket_path(local_node: &str) -> String {
    format!("{}/{}.status.sock", STATUS_DIR, local_node)
}

/// Status file path (refreshed every FILE_REFRESH_SECS seconds).
pub fn status_file_path(local_node: &str) -> String {
    format!("{}/{}.status", STATUS_DIR, local_node)
}

/// Spawn the status socket server and periodic file writer as background tasks.
pub fn spawn(
    config: Config,
    cluster: ClusterHandle,
    pub_state: Arc<RwLock<PubState>>,
    store: Arc<RwLock<FileStore>>,
    started_at: Instant,
) {
    // Socket server — answers live queries from `tinycfs --status`.
    {
        let config = config.clone();
        let cluster = cluster.clone();
        let pub_state = pub_state.clone();
        let store = store.clone();
        tokio::spawn(async move {
            serve_socket(config, cluster, pub_state, store, started_at).await;
        });
    }

    // Periodic file writer — lets operators `cat /run/tinycfs/<node>.status`.
    {
        tokio::spawn(async move {
            write_file_loop(config, cluster, pub_state, store, started_at).await;
        });
    }
}

/// Query a running instance via its Unix socket and return the status text.
/// Called from `main()` when `--status` is passed.
pub async fn query(local_node: &str) -> std::io::Result<String> {
    let path = socket_path(local_node);
    let mut stream = tokio::net::UnixStream::connect(&path).await?;
    let mut buf = String::new();
    stream.read_to_string(&mut buf).await?;
    Ok(buf)
}

// ── Internal helpers ──────────────────────────────────────────────────────────

async fn serve_socket(
    config: Config,
    cluster: ClusterHandle,
    pub_state: Arc<RwLock<PubState>>,
    store: Arc<RwLock<FileStore>>,
    started_at: Instant,
) {
    if std::fs::create_dir_all(STATUS_DIR).is_err() {
        warn!("Cannot create status dir {}; status socket unavailable", STATUS_DIR);
        return;
    }
    let path = socket_path(&config.local_node);
    let _ = std::fs::remove_file(&path); // remove stale socket from previous run

    let listener = match UnixListener::bind(&path) {
        Ok(l) => l,
        Err(e) => {
            warn!("Cannot bind status socket {}: {}", path, e);
            return;
        }
    };

    loop {
        match listener.accept().await {
            Ok((mut stream, _)) => {
                let text = format_status(&config, &cluster, &pub_state, &store, started_at);
                let _ = stream.write_all(text.as_bytes()).await;
                let _ = stream.shutdown().await;
            }
            Err(e) => {
                warn!("Status socket accept error: {}", e);
                break;
            }
        }
    }
}

async fn write_file_loop(
    config: Config,
    cluster: ClusterHandle,
    pub_state: Arc<RwLock<PubState>>,
    store: Arc<RwLock<FileStore>>,
    started_at: Instant,
) {
    if std::fs::create_dir_all(STATUS_DIR).is_err() {
        return; // silently skip if dir unavailable
    }
    let path = status_file_path(&config.local_node);
    loop {
        let text = format_status(&config, &cluster, &pub_state, &store, started_at);
        let _ = tokio::fs::write(&path, text.as_bytes()).await;
        tokio::time::sleep(Duration::from_secs(FILE_REFRESH_SECS)).await;
    }
}

// ── Formatting ────────────────────────────────────────────────────────────────

fn fmt_bytes(b: u64) -> String {
    const K: u64 = 1024;
    const M: u64 = K * 1024;
    const G: u64 = M * 1024;
    if b >= G      { format!("{:.2} GiB", b as f64 / G as f64) }
    else if b >= M { format!("{:.2} MiB", b as f64 / M as f64) }
    else if b >= K { format!("{:.1} KiB", b as f64 / K as f64) }
    else           { format!("{} B", b) }
}

fn fmt_uptime(d: Duration) -> String {
    let s = d.as_secs();
    let (days, h, m, sec) = (s / 86400, (s % 86400) / 3600, (s % 3600) / 60, s % 60);
    if days > 0        { format!("{}d {:02}h {:02}m {:02}s", days, h, m, sec) }
    else if h > 0      { format!("{}h {:02}m {:02}s", h, m, sec) }
    else if m > 0      { format!("{}m {:02}s", m, sec) }
    else               { format!("{}s", sec) }
}

/// Build the full status text from live in-memory data.
pub fn format_status(
    config: &Config,
    cluster: &ClusterHandle,
    pub_state: &Arc<RwLock<PubState>>,
    store: &Arc<RwLock<FileStore>>,
    started_at: Instant,
) -> String {
    let ps = pub_state.read().clone();
    let (inode_count, lock_count, total_bytes) = {
        let s = store.read();
        (s.inode_count(), s.lock_count(), s.total_data_bytes())
    };

    let peer_names: Vec<String> = cluster.peers.read().values().cloned().collect();
    let leader_name = ps.leader_id
        .and_then(|lid| cluster.peers.read().get(&lid).cloned())
        .unwrap_or_else(|| {
            if ps.is_leader {
                config.local_node.clone()
            } else {
                "(unknown)".to_string()
            }
        });

    let role = if ps.role_name.is_empty() { "Unknown" } else { ps.role_name };
    let algo = match &config.algorithm {
        crate::config::ConsensusAlgorithm::Raft  => "raft",
        crate::config::ConsensusAlgorithm::Totem => "totem",
    };

    format!(
        "TinyCFS Status\n\
         ══════════════════════════════════════════════════\n\
         Node:               {node}\n\
         Cluster:            {cluster}\n\
         Algorithm:          {algo}\n\
         PID:                {pid}\n\
         Uptime:             {uptime}\n\
         \n\
         ── Raft / Consensus ───────────────────────────────\n\
         Role:               {role}\n\
         Term:               {term}\n\
         Leader:             {leader}\n\
         Has Quorum:         {quorum}\n\
         Commit Index:       {commit}\n\
         Applied Index:      {applied}\n\
         In-Memory Entries:  {entries}\n\
         Snapshot At:        {snap}\n\
         \n\
         ── Peers ──────────────────────────────────────────\n\
         Connected:          {peer_count}\n\
         Peers:              {peers}\n\
         \n\
         ── Filesystem ─────────────────────────────────────\n\
         Inodes:             {inodes}\n\
         Total Data:         {size}\n\
         Active Locks:       {locks}\n\
         \n\
         ── Performance Counters ───────────────────────────\n\
         Replication Rounds: {hb}\n\
         Entries Applied:    {committed}\n\
         Elections Started:  {elections}\n\
         ══════════════════════════════════════════════════\n",
        node      = config.local_node,
        cluster   = config.cluster_name,
        algo      = algo,
        pid       = std::process::id(),
        uptime    = fmt_uptime(started_at.elapsed()),
        role      = role,
        term      = ps.current_term,
        leader    = leader_name,
        quorum    = if ps.has_quorum { "yes" } else { "no" },
        commit    = ps.commit_index,
        applied   = ps.last_applied,
        entries   = ps.log_entries,
        snap      = ps.snapshot_index,
        peer_count = cluster.peer_count(),
        peers     = if peer_names.is_empty() { "(none)".to_string() } else { peer_names.join(", ") },
        inodes    = inode_count,
        size      = fmt_bytes(total_bytes),
        locks     = lock_count,
        hb        = ps.heartbeats_sent,
        committed = ps.proposals_committed,
        elections = ps.elections_started,
    )
}
