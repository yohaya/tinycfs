use serde::{Deserialize, Serialize};
use std::path::Path;
use crate::error::{Result, TinyCfsError};

/// Consensus algorithm to use for replication.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum ConsensusAlgorithm {
    /// Raft — leader-based, strong consistency, battle-tested default.
    #[default]
    Raft,
    /// Totem SRTP — token-ring total-order multicast as designed in Corosync.
    /// Any node may propose when it holds the token; no fixed leader.
    Totem,
}

/// Configuration for a single cluster node.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct NodeConfig {
    /// Unique name for this node (e.g. "node1", "storage-01").
    pub name: String,
    /// IP address this node listens on.
    pub ip: String,
    /// TCP port for cluster communication (default: 7788).
    #[serde(default = "default_port")]
    pub port: u16,
    /// If true, this node receives log replication but does NOT vote in
    /// elections and does NOT count toward quorum. Useful for read-scale
    /// (observers can serve reads locally with zero write overhead on leader).
    #[serde(default)]
    pub observer: bool,
}

fn default_port() -> u16 {
    7788
}

impl NodeConfig {
    pub fn addr(&self) -> String {
        format!("{}:{}", self.ip, self.port)
    }
}

fn default_max_file_size() -> u64 {
    // 8 MiB default
    8 * 1024 * 1024
}

fn default_max_fs_size() -> u64 {
    // 1 GiB default
    1 * 1024 * 1024 * 1024
}

fn default_snapshot_every() -> usize {
    // Take a snapshot every 10 000 applied entries; compact the log.
    10_000
}

fn default_true() -> bool { true }

/// Top-level tinycfs.conf configuration.
///
/// Example:
/// ```json
/// {
///   "cluster_name": "mycluster",
///   "local_node": "node1",
///   "data_dir": "/var/lib/tinycfs",
///   "algorithm": "raft",
///   "max_file_size_bytes": 1048576,
///   "nodes": [
///     { "name": "node1", "ip": "192.168.1.10", "port": 7788 },
///     { "name": "node2", "ip": "192.168.1.11", "port": 7788 },
///     { "name": "node3", "ip": "192.168.1.12", "port": 7788 }
///   ]
/// }
/// ```
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Config {
    /// Human-readable cluster identifier.
    pub cluster_name: String,
    /// The name of *this* node — must match one entry in `nodes`.
    pub local_node: String,
    /// Directory for persistent state (raft.db). If absent, state is in-memory only.
    #[serde(default)]
    pub data_dir: Option<String>,
    /// Consensus algorithm to use for replication. Default: raft.
    #[serde(default)]
    pub algorithm: ConsensusAlgorithm,
    /// Maximum file size in bytes. Writes exceeding this return EFBIG.
    #[serde(default = "default_max_file_size")]
    pub max_file_size_bytes: u64,
    /// Maximum total filesystem size in bytes (sum of all file data). Writes
    /// that would exceed this return ENOSPC. Default: 4 GiB.
    #[serde(default = "default_max_fs_size")]
    pub max_fs_size_bytes: u64,
    /// Take a FileStore snapshot (and compact the Raft log) every N applied entries.
    #[serde(default = "default_snapshot_every")]
    pub snapshot_every: usize,
    /// Default FUSE mount point. Used when no mountpoint is passed on the command line.
    #[serde(default)]
    pub mountpoint: Option<String>,
    // ── Default FUSE mount options ─────────────────────────────────────────
    /// Disallow execution of files on the mount. Default: true.
    #[serde(default = "default_true")]
    pub noexec: bool,
    /// Skip updating access times on reads. Default: true (improves performance).
    #[serde(default = "default_true")]
    pub noatime: bool,
    /// Skip updating directory access times on reads. Default: true.
    #[serde(default = "default_true")]
    pub nodiratime: bool,
    /// Disallow setuid/setgid execution. Default: true (security hardening).
    #[serde(default = "default_true")]
    pub nosuid: bool,
    /// Disallow interpretation of block/character device files. Default: true.
    #[serde(default = "default_true")]
    pub nodev: bool,
    /// List of all nodes in the cluster (including this one).
    pub nodes: Vec<NodeConfig>,
}

impl Config {
    /// Load configuration from a JSON5 or plain JSON file.
    ///
    /// JSON5 is a superset of JSON: plain `.conf` and `.json` files are
    /// accepted automatically.  The parser handles `//` line comments,
    /// `/* */` block comments, trailing commas, and unquoted keys — all
    /// of which are used in the shipped `tinycfs.conf.example`.
    pub fn load(path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| TinyCfsError::Config(format!("cannot read {:?}: {}", path, e)))?;
        let cfg: Config = json5::from_str(&content)
            .map_err(|e| TinyCfsError::Config(format!("parse error in {:?}: {}", path, e)))?;
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> Result<()> {
        if self.cluster_name.is_empty() {
            return Err(TinyCfsError::Config("cluster_name is empty".into()));
        }
        if self.local_node.is_empty() {
            return Err(TinyCfsError::Config("local_node is empty".into()));
        }
        if self.nodes.is_empty() {
            return Err(TinyCfsError::Config("nodes list is empty".into()));
        }
        if self.local_node_config().is_none() {
            return Err(TinyCfsError::Config(format!(
                "local_node '{}' not found in nodes list",
                self.local_node
            )));
        }
        Ok(())
    }

    pub fn local_node_config(&self) -> Option<&NodeConfig> {
        self.nodes.iter().find(|n| n.name == self.local_node)
    }

    pub fn peer_nodes(&self) -> impl Iterator<Item = &NodeConfig> {
        let local = &self.local_node;
        self.nodes.iter().filter(move |n| &n.name != local)
    }

    /// Total number of voting nodes (excludes observers).
    pub fn total_voting_nodes(&self) -> usize {
        self.nodes.iter().filter(|n| !n.observer).count()
    }

    /// Whether this node is configured as an observer.
    pub fn is_observer(&self) -> bool {
        self.local_node_config().map(|n| n.observer).unwrap_or(false)
    }

    /// Derive a stable 64-bit node ID from the node name using SHA-256.
    pub fn node_id(name: &str) -> u64 {
        use sha2::{Sha256, Digest};
        let hash = Sha256::digest(name.as_bytes());
        u64::from_le_bytes(hash[..8].try_into().unwrap())
    }
}
