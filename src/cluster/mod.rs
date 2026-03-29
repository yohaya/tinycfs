pub mod message;
pub mod transport;

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::Instant;

use parking_lot::RwLock;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::time;
use tracing::{debug, error, info, warn};

use crate::config::{Config, NodeConfig};
use crate::cluster::message::{Message, NodeId};
use crate::cluster::transport::{recv_msg, send_msg};

/// A message received from a peer, tagged with the sender's node ID.
#[derive(Debug)]
pub struct Envelope {
    pub from: NodeId,
    pub msg: Message,
}

/// Handle returned to higher-level code so it can send messages and receive
/// inbound envelopes.
#[derive(Clone)]
pub struct ClusterHandle {
    pub local_id: NodeId,
    pub local_name: String,
    /// Sends a message to a specific peer (NodeId).
    pub tx_out: mpsc::UnboundedSender<(NodeId, Message)>,
    /// Receives messages from any peer.
    pub rx_in: Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<Envelope>>>,
    /// Current set of connected peers (node_id → name).
    pub peers: Arc<RwLock<HashMap<NodeId, String>>>,
    /// Node IDs that are configured as non-voting observers.
    pub observer_ids: Arc<HashSet<NodeId>>,
    /// Total number of voting nodes in the cluster (from config; includes self).
    pub total_voting_nodes: usize,
    /// Whether this node itself is an observer.
    pub is_observer: bool,
}

impl ClusterHandle {
    /// Send a message to one peer (best-effort).
    pub fn send(&self, to: NodeId, msg: Message) {
        let _ = self.tx_out.send((to, msg));
    }

    /// Broadcast a message to all known peers.
    pub fn broadcast(&self, msg: Message) {
        let ids: Vec<NodeId> = self.peers.read().keys().copied().collect();
        for id in ids {
            self.send(id, msg.clone());
        }
    }

    /// Number of currently connected peers (all, including observers).
    pub fn peer_count(&self) -> usize {
        self.peers.read().len()
    }

    /// IDs of currently connected peers (all, including observers).
    pub fn peer_ids(&self) -> Vec<NodeId> {
        self.peers.read().keys().copied().collect()
    }

    /// IDs of currently connected voting peers (excludes observers).
    pub fn voting_peer_ids(&self) -> Vec<NodeId> {
        self.peers
            .read()
            .keys()
            .copied()
            .filter(|id| !self.observer_ids.contains(id))
            .collect()
    }

    /// Whether a given peer is an observer.
    pub fn is_peer_observer(&self, id: NodeId) -> bool {
        self.observer_ids.contains(&id)
    }
}

/// Per-connection state kept by the cluster manager.
struct Conn {
    /// Carries (send_time, message) so the writer task can compute the correct
    /// simulated delivery time relative to when the message was actually sent,
    /// not relative to when the writer task happens to wake up.
    tx: mpsc::UnboundedSender<(Instant, Message)>,
}

/// Optional cluster configuration that affects runtime behaviour.
///
/// Used primarily by the simulation to inject artificial network latency.
#[derive(Clone, Default)]
pub struct ClusterOptions {
    /// If set, each outbound message is delayed by a random duration drawn
    /// uniformly from [min, max] before being written to the socket.
    /// Simulates real-world WAN/LAN propagation delay.
    pub network_delay: Option<(Duration, Duration)>,
}

/// The cluster manager. Owns all TCP connections.
pub struct Cluster {
    config: Config,
    local_id: NodeId,
    /// node_id -> per-connection send channel
    conns: Arc<RwLock<HashMap<NodeId, Conn>>>,
    /// Inbound message channel (written by per-connection tasks)
    env_tx: mpsc::UnboundedSender<Envelope>,
}

impl Cluster {
    /// Start the cluster: bind listener, spawn connector tasks, return handle.
    pub async fn start(config: Config) -> (ClusterHandle, Self) {
        Self::start_with_opts(config, ClusterOptions::default()).await
    }

    /// Start with explicit options (used by the simulation for delay injection).
    pub async fn start_with_opts(config: Config, opts: ClusterOptions) -> (ClusterHandle, Self) {
        let local_id = Config::node_id(&config.local_node);
        let local_cfg = config.local_node_config().expect("local node not in config");

        let conns: Arc<RwLock<HashMap<NodeId, Conn>>> = Arc::new(RwLock::new(HashMap::new()));
        let peers: Arc<RwLock<HashMap<NodeId, String>>> = Arc::new(RwLock::new(HashMap::new()));

        // Build observer set and total voting count from config.
        let observer_ids: Arc<HashSet<NodeId>> = Arc::new(
            config
                .nodes
                .iter()
                .filter(|n| n.observer)
                .map(|n| Config::node_id(&n.name))
                .collect(),
        );
        let total_voting_nodes = config.total_voting_nodes();
        let is_observer = config.is_observer();

        let (env_tx, env_rx) = mpsc::unbounded_channel::<Envelope>();
        let (out_tx, mut out_rx) = mpsc::unbounded_channel::<(NodeId, Message)>();

        // ── Listener task ──────────────────────────────────────────────────
        let listen_addr: SocketAddr = local_cfg.addr().parse().expect("invalid listen address");
        let listener = TcpListener::bind(listen_addr).await.expect("failed to bind");
        info!("Listening on {}", listen_addr);

        let conns2 = conns.clone();
        let peers2 = peers.clone();
        let env_tx2 = env_tx.clone();
        let cfg2 = config.clone();
        let lid = local_id;
        let delay2 = opts.network_delay;
        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, addr)) => {
                        debug!("Accepted connection from {}", addr);
                        let conns = conns2.clone();
                        let peers = peers2.clone();
                        let env_tx = env_tx2.clone();
                        let cfg = cfg2.clone();
                        tokio::spawn(handle_incoming(stream, lid, cfg, conns, peers, env_tx, delay2));
                    }
                    Err(e) => error!("Accept error: {}", e),
                }
            }
        });

        // ── Outbound dispatcher ────────────────────────────────────────────
        let conns3 = conns.clone();
        tokio::spawn(async move {
            while let Some((to, msg)) = out_rx.recv().await {
                let tx = conns3.read().get(&to).map(|c| c.tx.clone());
                if let Some(tx) = tx {
                    // Stamp with the time the message reached the outbound queue
                    // so the writer task computes delay relative to the true
                    // send instant, not relative to when it wakes up.
                    let _ = tx.send((Instant::now(), msg));
                } else {
                    debug!("No connection to node {:x}, dropping message", to);
                }
            }
        });

        // ── Connector tasks for each peer ──────────────────────────────────
        // Only connect to peers with a higher node ID than ours.  This ensures
        // exactly one TCP connection per pair (the lower-ID side initiates).
        // The higher-ID side accepts via handle_incoming, which registers the
        // peer in its own `conns` so it can send back over the same socket.
        let cluster = Cluster {
            config: config.clone(),
            local_id,
            conns: conns.clone(),
            env_tx: env_tx.clone(),
        };

        for peer in config.peer_nodes() {
            let peer_id = Config::node_id(&peer.name);
            if peer_id <= local_id {
                continue; // higher-ID peer will connect to us
            }
            let peer = peer.clone();
            let conns = conns.clone();
            let peers = peers.clone();
            let env_tx = env_tx.clone();
            let cfg = config.clone();
            let delay = opts.network_delay;
            tokio::spawn(connect_to_peer(peer, lid, cfg, conns, peers, env_tx, delay));
        }

        let handle = ClusterHandle {
            local_id,
            local_name: config.local_node.clone(),
            tx_out: out_tx,
            rx_in: Arc::new(tokio::sync::Mutex::new(env_rx)),
            peers,
            observer_ids,
            total_voting_nodes,
            is_observer,
        };

        (handle, cluster)
    }
}

/// Attempt to connect to a peer with exponential back-off, then maintain the
/// connection indefinitely. If it drops, retry.
async fn connect_to_peer(
    peer: NodeConfig,
    local_id: NodeId,
    config: Config,
    conns: Arc<RwLock<HashMap<NodeId, Conn>>>,
    peers: Arc<RwLock<HashMap<NodeId, String>>>,
    env_tx: mpsc::UnboundedSender<Envelope>,
    delay: Option<(Duration, Duration)>,
) {
    let peer_id = Config::node_id(&peer.name);
    let mut backoff = Duration::from_millis(500);

    loop {
        match TcpStream::connect(&peer.addr()).await {
            Ok(mut stream) => {
                info!("Connected to {} ({})", peer.name, peer.addr());

                // Send Hello
                let hello = Message::Hello {
                    node_id: local_id,
                    name: config.local_node.clone(),
                    cluster_name: config.cluster_name.clone(),
                };
                if let Err(e) = send_msg(&mut stream, &hello).await {
                    warn!("Failed to send Hello to {}: {}", peer.name, e);
                    time::sleep(backoff).await;
                    continue;
                }

                // Expect HelloAck
                match recv_msg(&mut stream).await {
                    Ok(Message::HelloAck { node_id, name }) => {
                        info!("Handshake complete with {} (id={:x})", name, node_id);
                    }
                    Ok(other) => {
                        warn!("Unexpected handshake reply from {}: {:?}", peer.name, other);
                        time::sleep(backoff).await;
                        continue;
                    }
                    Err(e) => {
                        warn!("Handshake recv error from {}: {}", peer.name, e);
                        time::sleep(backoff).await;
                        continue;
                    }
                }

                backoff = Duration::from_millis(500); // reset

                // Split into reader + writer
                let (read_half, write_half) = stream.into_split();
                let (tx, rx) = mpsc::unbounded_channel::<(Instant, Message)>();

                conns.write().insert(peer_id, Conn { tx });
                peers.write().insert(peer_id, peer.name.clone());

                // Writer task
                let peer_name2 = peer.name.clone();
                tokio::spawn(writer_task(write_half, rx, peer_name2, delay));

                // Reader task (blocks until connection drops)
                reader_task(read_half, peer_id, env_tx.clone()).await;

                warn!("Connection to {} dropped, reconnecting…", peer.name);
                conns.write().remove(&peer_id);
                peers.write().remove(&peer_id);
            }
            Err(e) => {
                debug!("Cannot connect to {} ({}): {}", peer.name, peer.addr(), e);
            }
        }

        time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(30));
    }
}

/// Handle an incoming TCP connection through the full lifecycle.
async fn handle_incoming(
    mut stream: TcpStream,
    local_id: NodeId,
    config: Config,
    conns: Arc<RwLock<HashMap<NodeId, Conn>>>,
    peers: Arc<RwLock<HashMap<NodeId, String>>>,
    env_tx: mpsc::UnboundedSender<Envelope>,
    delay: Option<(Duration, Duration)>,
) {
    // Expect Hello
    let (peer_id, peer_name) = match recv_msg(&mut stream).await {
        Ok(Message::Hello { node_id, name, cluster_name }) => {
            if cluster_name != config.cluster_name {
                warn!("Rejecting connection: cluster name mismatch ('{}' != '{}')", cluster_name, config.cluster_name);
                return;
            }
            (node_id, name)
        }
        Ok(other) => {
            warn!("Expected Hello, got {:?}", other);
            return;
        }
        Err(e) => {
            warn!("Error reading Hello: {}", e);
            return;
        }
    };

    // Send HelloAck
    let ack = Message::HelloAck { node_id: local_id, name: config.local_node.clone() };
    if let Err(e) = send_msg(&mut stream, &ack).await {
        warn!("Failed to send HelloAck to {}: {}", peer_name, e);
        return;
    }

    info!("Accepted peer {} (id={:x})", peer_name, peer_id);

    let (read_half, write_half) = stream.into_split();
    let (tx, rx) = mpsc::unbounded_channel::<(Instant, Message)>();

    conns.write().insert(peer_id, Conn { tx });
    peers.write().insert(peer_id, peer_name.clone());

    // Writer task
    tokio::spawn(writer_task(write_half, rx, peer_name.clone(), delay));

    // Reader task
    reader_task(read_half, peer_id, env_tx).await;

    info!("Peer {} disconnected", peer_name);
    conns.write().remove(&peer_id);
    peers.write().remove(&peer_id);
}

/// Reads messages from a TCP read-half and forwards them to env_tx.
async fn reader_task(
    mut read_half: tokio::net::tcp::OwnedReadHalf,
    peer_id: NodeId,
    env_tx: mpsc::UnboundedSender<Envelope>,
) {
    // We need a full TcpStream for recv_msg, so wrap with a helper that works
    // on the read half.
    use tokio::io::AsyncReadExt;

    loop {
        let mut len_buf = [0u8; 4];
        if let Err(e) = read_half.read_exact(&mut len_buf).await {
            if e.kind() != std::io::ErrorKind::UnexpectedEof {
                warn!("Read error from peer {:x}: {}", peer_id, e);
            }
            break;
        }
        let len = u32::from_be_bytes(len_buf);
        if len > 16 * 1024 * 1024 {
            warn!("Message too large ({} bytes) from peer {:x}", len, peer_id);
            break;
        }
        let mut buf = vec![0u8; len as usize];
        if let Err(e) = read_half.read_exact(&mut buf).await {
            warn!("Read error body from peer {:x}: {}", peer_id, e);
            break;
        }
        match bincode::deserialize::<Message>(&buf) {
            Ok(msg) => {
                let _ = env_tx.send(Envelope { from: peer_id, msg });
            }
            Err(e) => {
                warn!("Deserialization error from peer {:x}: {}", peer_id, e);
                break;
            }
        }
    }
}

/// Drains a channel and writes messages to the TCP write-half.
///
/// Network delay model (simulation only):
/// Each message m_i is assigned an independent random delay D_i and is
/// delivered at max(send_time_i + D_i, last_deliver_time).  This means
/// that if the previous message is still "in flight", the new message
/// piggybacks on it and is delivered immediately after — which is correct
/// for an ordered TCP channel.  Crucially, delays are NOT cumulative: a
/// heartbeat sent while many data messages are queued still arrives within
/// D_i ms of being sent (modulo the time to finish writing the previous
/// message), preventing heartbeat starvation.
///
/// Each message carries the `Instant` at which it was placed in this task's
/// channel by the outbound dispatcher.  The delivery time is computed as
/// `sent_at + rand(min..max)`, clamped to `last_deliver` for ordering.
/// Using `sent_at` (rather than `Instant::now()` at dequeue time) prevents
/// `last_deliver` from accumulating indefinitely when the channel fills up
/// faster than the simulated link can drain it.
async fn writer_task(
    mut write_half: tokio::net::tcp::OwnedWriteHalf,
    mut rx: mpsc::UnboundedReceiver<(Instant, Message)>,
    peer_name: String,
    delay: Option<(Duration, Duration)>,
) {
    use rand::Rng;
    use tokio::io::AsyncWriteExt;

    // Tracks the earliest time the write half is "free" (previous write done).
    let mut last_deliver = Instant::now();

    while let Some((sent_at, msg)) = rx.recv().await {
        // Only sleep when simulated network delay is enabled.  In production
        // (delay == None) skipping the sleep avoids a spurious 1-ms timer
        // wakeup per message (sleep_until(Instant::now()) is never a true
        // no-op — Tokio schedules it for the next timer wheel tick).
        if let Some((min, max)) = delay {
            let ms = rand::thread_rng()
                .gen_range(min.as_millis() as u64..=max.as_millis() as u64);
            let intended = sent_at + Duration::from_millis(ms);
            let deliver_at = intended.max(last_deliver);
            last_deliver = deliver_at;
            time::sleep_until(deliver_at).await;
        }

        let payload = match bincode::serialize(&msg) {
            Ok(p) => p,
            Err(e) => {
                error!("Serialization error for {}: {}", peer_name, e);
                continue;
            }
        };
        let len = (payload.len() as u32).to_be_bytes();
        if write_half.write_all(&len).await.is_err()
            || write_half.write_all(&payload).await.is_err()
        {
            break;
        }
    }
    debug!("Writer task for {} exiting", peer_name);
}
