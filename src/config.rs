use serde::{Deserialize, Serialize};
use std::path::Path;
use crate::error::{Result, TinyCfsError};

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
}

fn default_port() -> u16 {
    7788
}

impl NodeConfig {
    pub fn addr(&self) -> String {
        format!("{}:{}", self.ip, self.port)
    }
}

/// Top-level tinycfs.conf configuration.
///
/// Example:
/// ```json
/// {
///   "cluster_name": "mycluster",
///   "local_node": "node1",
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
    /// Optional directory for persistent state (raft log, snapshots).
    #[serde(default)]
    pub data_dir: Option<String>,
    /// List of all nodes in the cluster (including this one).
    pub nodes: Vec<NodeConfig>,
}

impl Config {
    /// Load configuration from a JSON file.
    pub fn load(path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| TinyCfsError::Config(format!("cannot read {:?}: {}", path, e)))?;
        let cfg: Config = serde_json::from_str(&content)
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

    /// Derive a stable 64-bit node ID from the node name using SHA-256.
    pub fn node_id(name: &str) -> u64 {
        use sha2::{Sha256, Digest};
        let hash = Sha256::digest(name.as_bytes());
        u64::from_le_bytes(hash[..8].try_into().unwrap())
    }
}
