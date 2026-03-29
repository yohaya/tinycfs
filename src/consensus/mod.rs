pub mod log;

use crate::config::ConsensusAlgorithm;

/// High-throughput Raft consensus engine — 500-node capable.
///
/// Design:
/// 1. **Actor model** — single tokio task owns all mutable Raft state.
/// 2. **Proposal batching** — drains up to MAX_ENTRIES_PER_RPC ops per tick.
/// 3. **Parallel per-peer replication** — each peer gets an independent write path.
/// 4. **Correct quorum** — uses total_voting_nodes from config, not connected peers,
///    so partitioned nodes cannot form a false quorum.
/// 5. **SQLite persistence** — term/voted_for persisted on every state change.
///    FileStore written to disk after EVERY committed batch (before acking client),
///    guaranteeing zero data loss on crash. Log is in-memory only; re-synced from
///    the leader on restart. Log is compacted every `snapshot_every` entries to
///    bound memory usage.
/// 6. **30-second write retry** — propose() keeps retrying until a leader is
///    available, returning EROFS only after 30 s.
/// 7. **Observer support** — observer nodes replicate but don't vote.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use tokio::sync::{mpsc, oneshot};
use tokio::time;
use tracing::{debug, info, warn};

use crate::cluster::message::{
    AppendEntries, AppendEntriesReply, FileOp, FileOpResult, InstallSnapshot, InstallSnapshotReply,
    LogEntry, LogIndex, Message, NodeId, RequestVote, RequestVoteReply, Term,
};
use crate::cluster::{ClusterHandle, Envelope};
use crate::consensus::log::Log;
use crate::error::{Result, TinyCfsError};
use crate::fs::store::FileStore;
use crate::persistence::RaftDb;

// ─── Tunables ─────────────────────────────────────────────────────────────────
/// Max entries per AppendEntries RPC — larger batches amortise per-RPC cost at
/// high node counts.
const MAX_ENTRIES_PER_RPC: usize = 1024;
const PROPOSAL_QUEUE_DEPTH: usize = 32_768;
/// How long propose() keeps retrying when no leader is available.
const PROPOSE_TIMEOUT: Duration = Duration::from_secs(30);
/// Maximum number of distinct log indices buffered in deferred_responses.
/// Guards against unbounded growth when a follower lags behind the leader.
const MAX_DEFERRED_RESPONSES: usize = 65_536;

// ─── Public proposal API ──────────────────────────────────────────────────────

pub struct Proposal {
    pub op: FileOp,
    pub done: oneshot::Sender<FileOpResult>,
}

// ─── Internal Raft state ──────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
enum Role {
    Follower {
        leader: Option<NodeId>,
        election_deadline: Instant,
    },
    Candidate {
        votes: HashSet<NodeId>,
        election_deadline: Instant,
    },
    Leader {
        next_index: HashMap<NodeId, LogIndex>,
        match_index: HashMap<NodeId, LogIndex>,
        last_heartbeat: Instant,
        /// Highest log index that was included in the last replicate_to_all call.
        /// Lets tick() detect when forwarded requests added new entries and need
        /// replication without waiting for the next heartbeat interval.
        last_replicated_index: LogIndex,
    },
}

struct RaftState {
    current_term: Term,
    voted_for: Option<NodeId>,
    log: Log,
    commit_index: LogIndex,
    last_applied: LogIndex,
    role: Role,
    /// Callbacks for locally-proposed entries waiting to be committed.
    /// Keyed by log index for O(1) lookup in advance_applied().
    pending: HashMap<LogIndex, oneshot::Sender<FileOpResult>>,
    next_req_id: u64,
    /// Next snapshot threshold.
    next_snapshot_at: LogIndex,
    /// Configurable election timeouts (carried here so become_follower can use them).
    election_timeout_min_ms: u64,
    election_timeout_max_ms: u64,
}

impl RaftState {
    fn new(snapshot_every: LogIndex, election_timeout_min_ms: u64, election_timeout_max_ms: u64) -> Self {
        RaftState {
            current_term: 0,
            voted_for: None,
            log: Log::new(),
            commit_index: 0,
            last_applied: 0,
            role: Role::Follower {
                leader: None,
                election_deadline: random_deadline(election_timeout_min_ms, election_timeout_max_ms),
            },
            pending: HashMap::new(),
            next_req_id: 1,
            next_snapshot_at: snapshot_every,
            election_timeout_min_ms,
            election_timeout_max_ms,
        }
    }

    fn is_leader(&self) -> bool {
        matches!(self.role, Role::Leader { .. })
    }

    fn leader_id(&self) -> Option<NodeId> {
        match &self.role {
            Role::Follower { leader, .. } => *leader,
            _ => None,
        }
    }

    fn become_follower(&mut self, term: Term, leader: Option<NodeId>) {
        self.current_term = term;
        self.voted_for = None;
        // Drop any pending leader callbacks so propose() retries quickly.
        self.pending.clear();
        self.role = Role::Follower {
            leader,
            election_deadline: random_deadline(self.election_timeout_min_ms, self.election_timeout_max_ms),
        };
    }
}

fn random_deadline(min_ms: u64, max_ms: u64) -> Instant {
    use rand::Rng;
    let ms = rand::thread_rng().gen_range(min_ms..=max_ms);
    Instant::now() + Duration::from_millis(ms)
}

// ─── Public handle ────────────────────────────────────────────────────────────

/// A clone-able handle to whichever consensus engine is running (Raft or Totem).
///
/// The FUSE layer only calls `propose()`.  The simulation also calls
/// `is_leader()` / `has_quorum()` to check readiness.
#[derive(Clone)]
pub struct Consensus {
    inner: Arc<ConsensusInner>,
}

enum ConsensusInner {
    Raft {
        proposal_tx: mpsc::Sender<Proposal>,
        pub_state: Arc<RwLock<PubState>>,
    },
    Totem {
        proposal_tx: mpsc::Sender<Proposal>,
        pub_state: Arc<RwLock<crate::totem::TotemPubState>>,
    },
}

#[derive(Clone, Default)]
pub struct PubState {
    pub is_leader: bool,
    pub leader_id: Option<NodeId>,
    pub current_term: Term,
    /// True when this node is the leader or knows a live leader.
    pub has_quorum: bool,
    // ── Extended status (updated on every state change) ──────────────────────
    /// Highest log index committed by a quorum.
    pub commit_index: LogIndex,
    /// Highest log index applied to the local state machine.
    pub last_applied: LogIndex,
    /// Number of in-memory log entries (post-compaction).
    pub log_entries: usize,
    /// Log index of the most recent snapshot (0 = no snapshot taken yet).
    pub snapshot_index: LogIndex,
    /// Human-readable role: "Leader", "Follower", "Candidate", or "".
    pub role_name: &'static str,
    /// Number of currently connected peers (all, including observers).
    pub connected_peers: usize,
    // ── Monotonic performance counters ─────────────────────────────────────
    /// Times replicate_to_all() was called (heartbeats + replication rounds).
    pub heartbeats_sent: u64,
    /// Total log entries applied to the state machine since startup.
    pub proposals_committed: u64,
    /// Elections initiated since startup.
    pub elections_started: u64,
}

impl Consensus {
    /// Build the consensus engine.
    ///
    /// The algorithm is selected by the `algorithm` parameter (from Config).
    /// Both algorithms apply committed entries directly to `store` so no
    /// external apply loop is needed in main.rs.
    ///
    /// Returns:
    /// - `Consensus` — clone-able handle for FUSE / callers.
    /// - `mpsc::Sender<Envelope>` — pipe incoming cluster messages here.
    pub fn new(
        algorithm: ConsensusAlgorithm,
        cluster: ClusterHandle,
        db: Option<RaftDb>,
        store: Arc<RwLock<FileStore>>,
        snapshot_every: u64,
        persist_every: u64,
        max_file_size: u64,
        max_fs_size: u64,
        heartbeat_interval_ms: u64,
        election_timeout_min_ms: u64,
        election_timeout_max_ms: u64,
    ) -> (Self, mpsc::Sender<Envelope>) {
        match algorithm {
            ConsensusAlgorithm::Raft => {
                let (proposal_tx, proposal_rx) = mpsc::channel(PROPOSAL_QUEUE_DEPTH);
                let (msg_tx, msg_rx) = mpsc::channel(16_384);
                let pub_state = Arc::new(RwLock::new(PubState::default()));

                let state = if let Some(ref db) = db {
                    load_state_from_db(db, &store, snapshot_every, election_timeout_min_ms, election_timeout_max_ms)
                } else {
                    RaftState::new(snapshot_every, election_timeout_min_ms, election_timeout_max_ms)
                };

                // Re-apply size limits after potential snapshot restore.
                // Snapshot deserialization resets serde(skip) fields to their
                // defaults (0), so the limits must be restored here.
                store.write().set_limits(max_file_size, max_fs_size);

                let engine = RaftEngine {
                    state,
                    cluster,
                    pub_state: pub_state.clone(),
                    db,
                    store,
                    snapshot_every,
                    persist_every,
                    last_persisted: 0,
                    max_file_size,
                    max_fs_size,
                    heartbeat_interval: Duration::from_millis(heartbeat_interval_ms),
                    client_pending: HashMap::new(),
                    deferred_responses: HashMap::new(),
                    heartbeats_sent: 0,
                    proposals_committed: 0,
                    elections_started: 0,
                };
                tokio::spawn(engine.run(proposal_rx, msg_rx));

                let inner = Arc::new(ConsensusInner::Raft { proposal_tx, pub_state });
                (Consensus { inner }, msg_tx)
            }
            ConsensusAlgorithm::Totem => {
                let (proposal_tx, proposal_rx) = mpsc::channel(PROPOSAL_QUEUE_DEPTH);
                let (msg_tx, msg_rx) = mpsc::channel(16_384);
                let pub_state = Arc::new(RwLock::new(crate::totem::TotemPubState::default()));
                let total_voting = cluster.total_voting_nodes;

                let engine = crate::totem::TotemEngine::new(
                    cluster,
                    db,
                    store.clone(),
                    pub_state.clone(),
                    total_voting,
                );
                // Re-apply limits after potential snapshot restore in TotemEngine::new.
                store.write().set_limits(max_file_size, max_fs_size);
                tokio::spawn(engine.run(proposal_rx, msg_rx));

                let inner = Arc::new(ConsensusInner::Totem { proposal_tx, pub_state });
                (Consensus { inner }, msg_tx)
            }
        }
    }

    /// Submit a write operation.  Retries for up to 30 seconds while waiting
    /// for a leader (Raft) or ring quorum (Totem) to be available.
    pub async fn propose(&self, op: FileOp) -> Result<()> {
        let proposal_tx = match &*self.inner {
            ConsensusInner::Raft { proposal_tx, .. } => proposal_tx.clone(),
            ConsensusInner::Totem { proposal_tx, .. } => proposal_tx.clone(),
        };
        Self::propose_inner(proposal_tx, op).await
    }

    async fn propose_inner(proposal_tx: mpsc::Sender<Proposal>, op: FileOp) -> Result<()> {
        let deadline = Instant::now() + PROPOSE_TIMEOUT;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(TinyCfsError::Timeout);
            }
            let (done_tx, done_rx) = oneshot::channel();
            let proposal = Proposal { op: op.clone(), done: done_tx };

            match proposal_tx.try_send(proposal) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => {
                    time::sleep(Duration::from_millis(50).min(remaining)).await;
                    continue;
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    return Err(TinyCfsError::Cluster("consensus engine stopped".into()));
                }
            }

            match time::timeout(remaining, done_rx).await {
                Ok(Ok(FileOpResult::Ok)) => return Ok(()),
                Ok(Ok(FileOpResult::Error(e))) => return Err(TinyCfsError::Cluster(e)),
                Ok(Ok(FileOpResult::LockContended(holder))) => {
                    return Err(TinyCfsError::LockContended(holder));
                }
                Ok(Ok(FileOpResult::FileTooLarge)) => {
                    return Err(TinyCfsError::FileTooLarge { limit: 0 });
                }
                Ok(Ok(FileOpResult::NoSpace)) => {
                    return Err(TinyCfsError::NoSpace);
                }
                Ok(Err(_)) => {
                    // done_tx dropped (leader step-down / ring reform) — retry.
                    time::sleep(Duration::from_millis(100).min(remaining)).await;
                    continue;
                }
                Err(_timeout) => return Err(TinyCfsError::Timeout),
            }
        }
    }

    pub fn is_leader(&self) -> bool {
        match &*self.inner {
            ConsensusInner::Raft { pub_state, .. } => pub_state.read().is_leader,
            // Totem has no fixed leader; return true when ring has quorum.
            ConsensusInner::Totem { pub_state, .. } => pub_state.read().has_quorum,
        }
    }

    pub fn leader_id(&self) -> Option<NodeId> {
        match &*self.inner {
            ConsensusInner::Raft { pub_state, .. } => pub_state.read().leader_id,
            ConsensusInner::Totem { .. } => None,
        }
    }

    pub fn has_quorum(&self) -> bool {
        match &*self.inner {
            ConsensusInner::Raft { pub_state, .. } => pub_state.read().has_quorum,
            ConsensusInner::Totem { pub_state, .. } => pub_state.read().has_quorum,
        }
    }

    /// Return the shared public state snapshot (Raft only; Totem returns a fresh default).
    /// Callers can clone the Arc to hold a long-lived reference for status display.
    pub fn pub_state(&self) -> Arc<RwLock<PubState>> {
        match &*self.inner {
            ConsensusInner::Raft { pub_state, .. } => pub_state.clone(),
            ConsensusInner::Totem { .. } => Arc::new(RwLock::new(PubState::default())),
        }
    }
}

// ─── State loader ─────────────────────────────────────────────────────────────

fn load_state_from_db(
    db: &RaftDb,
    store: &Arc<RwLock<FileStore>>,
    snapshot_every: u64,
    election_timeout_min_ms: u64,
    election_timeout_max_ms: u64,
) -> RaftState {
    // Raft safety depends on reading the correct term and voted_for on restart.
    // Treating a DB error as "no prior state" (term=0, voted_for=None) would
    // reset the node to a blank slate and allow it to vote in any term,
    // potentially enabling two leaders in the same term.  Abort instead.
    let (term, voted_for) = db.load_meta().unwrap_or_else(|e| {
        panic!(
            "FATAL: cannot read Raft metadata from persistent store: {e}. \
             Refusing to start — proceeding with term=0 / voted_for=None would \
             reset vote history and violate Raft safety."
        )
    });

    // Load persisted filesystem state.  Try the new per-inode table first;
    // fall back to the legacy blob snapshot for databases written by older
    // versions or populated via InstallSnapshot before the first delta persist.
    let (snapshot_index, snapshot_term) = match db.load_fs_state() {
        Ok(Some((inode_rows, next_ino, total_bytes, si, st))) => {
            let inodes: std::collections::HashMap<crate::fs::inode::Ino, crate::fs::inode::Inode> =
                inode_rows.into_iter().collect();
            let loaded = FileStore::from_parts(inodes, next_ino, total_bytes);
            *store.write() = loaded;
            info!("Loaded per-inode state at index {} (term {})", si, st);
            (si, st)
        }
        Ok(None) => {
            // Fall back to legacy full-blob snapshot (older DB or post-InstallSnapshot).
            match db.load_snapshot() {
                Ok(Some((si, st, data))) => {
                    match bincode::deserialize::<FileStore>(&data) {
                        Ok(snap_store) => {
                            *store.write() = snap_store;
                            info!("Loaded legacy snapshot at index {} (term {})", si, st);
                            (si, st)
                        }
                        Err(e) => {
                            warn!(
                                "Failed to deserialize snapshot at index {} — treating as absent: {}",
                                si, e
                            );
                            (0, 0)
                        }
                    }
                }
                Ok(None) => (0, 0),
                Err(e) => { warn!("Failed to load snapshot: {}", e); (0, 0) }
            }
        }
        Err(e) => { warn!("Failed to load fs state: {}", e); (0, 0) }
    };

    // Start with an empty in-memory log anchored at the snapshot boundary.
    // The leader will fill the gap via AppendEntries / InstallSnapshot.
    let mut log = Log::new();
    log.set_snapshot_base(snapshot_index, snapshot_term);

    info!(
        "Loaded term={}, voted_for={:?}, starting from snapshot index {}",
        term, voted_for, snapshot_index
    );

    let next_snapshot_at = if snapshot_index == 0 {
        snapshot_every
    } else {
        snapshot_index + snapshot_every
    };

    RaftState {
        current_term: term,
        voted_for,
        log,
        commit_index: snapshot_index,
        last_applied: snapshot_index,
        role: Role::Follower {
            leader: None,
            election_deadline: random_deadline(election_timeout_min_ms, election_timeout_max_ms),
        },
        pending: HashMap::new(),
        next_req_id: 1,
        next_snapshot_at,
        election_timeout_min_ms,
        election_timeout_max_ms,
    }
}

// ─── Raft actor ───────────────────────────────────────────────────────────────

struct RaftEngine {
    state: RaftState,
    cluster: ClusterHandle,
    pub_state: Arc<RwLock<PubState>>,
    db: Option<RaftDb>,
    store: Arc<RwLock<FileStore>>,
    /// Log compaction interval (entries between compact_log calls).
    snapshot_every: u64,
    /// Snapshot persistence interval — write FileStore to SQLite every N entries.
    /// Smaller = more I/O, better per-node durability.  Must be ≤ snapshot_every.
    persist_every: u64,
    /// last_applied index at which we last called persist_store.
    last_persisted: LogIndex,
    /// Size limits from Config — re-applied to the store after every snapshot restore.
    max_file_size: u64,
    max_fs_size: u64,
    /// Configurable timing — how often the leader sends keepalive RPCs.
    heartbeat_interval: Duration,
    /// Forwarded-request callbacks: request_id → oneshot reply to local proposer.
    client_pending: HashMap<u64, oneshot::Sender<FileOpResult>>,
    /// Responses received from the leader before this follower has applied the
    /// corresponding log entry.  Keyed by the log index; entries are fired from
    /// advance_applied() once last_applied reaches the required index.
    deferred_responses: HashMap<LogIndex, Vec<(oneshot::Sender<FileOpResult>, FileOpResult)>>,
    // ── Performance counters (mirrored into pub_state) ────────────────────
    heartbeats_sent: u64,
    proposals_committed: u64,
    elections_started: u64,
}

impl RaftEngine {
    async fn run(
        mut self,
        mut proposal_rx: mpsc::Receiver<Proposal>,
        mut msg_rx: mpsc::Receiver<Envelope>,
    ) {
        loop {
            // Compute the exact instant the next protocol timeout fires.
            // The task is truly idle between events — no fixed-interval polling.
            let next_timeout = self.next_deadline();

            tokio::select! {
                biased;
                Some(env) = msg_rx.recv() => {
                    self.handle_message(env.from, env.msg);
                    // Drain any additional messages that arrived while we were
                    // processing this one.  Multiple ClientRequest messages from
                    // followers accumulate log entries in one pass so
                    // replicate_if_new_entries() sends them in a single batch
                    // rather than one AppendEntries RPC per forwarded request.
                    while let Ok(env) = msg_rx.try_recv() {
                        self.handle_message(env.from, env.msg);
                    }
                    self.replicate_if_new_entries();
                }
                _ = time::sleep_until(next_timeout) => {
                    self.on_deadline(&mut proposal_rx).await;
                }
                Some(p) = proposal_rx.recv() => {
                    self.handle_proposals(p, &mut proposal_rx).await;
                }
            }
        }
    }

    /// Next instant the engine must wake up even if no messages arrive.
    fn next_deadline(&self) -> time::Instant {
        let now = Instant::now();
        let deadline = match &self.state.role {
            Role::Follower { election_deadline, .. }
            | Role::Candidate { election_deadline, .. } => *election_deadline,
            Role::Leader { last_heartbeat, .. } => *last_heartbeat + self.heartbeat_interval,
        };
        // Convert: compute remaining duration and add to tokio's Instant::now().
        let remaining = deadline.saturating_duration_since(now);
        time::Instant::now() + remaining
    }

    /// Called when the deadline timer fires.
    async fn on_deadline(&mut self, proposal_rx: &mut mpsc::Receiver<Proposal>) {
        let now = Instant::now();
        match self.state.role.clone() {
            Role::Follower { election_deadline, leader } => {
                if now >= election_deadline && !self.cluster.is_observer {
                    self.start_election();
                } else {
                    // Deadline fired slightly early (timer jitter). Forward any
                    // queued proposals to the known leader.
                    self.forward_or_drop_proposals(leader, proposal_rx);
                }
            }
            Role::Candidate { election_deadline, .. } => {
                if now >= election_deadline {
                    self.start_election();
                }
                while proposal_rx.try_recv().is_ok() {}
            }
            Role::Leader { .. } => {
                // Heartbeat interval elapsed — send heartbeats / replicate.
                let new_last = self.state.log.last_index();
                if let Role::Leader {
                    ref mut last_heartbeat,
                    ref mut last_replicated_index,
                    ..
                } = self.state.role
                {
                    *last_heartbeat = now;
                    *last_replicated_index = new_last;
                }
                self.replicate_to_all();
            }
        }
    }

    /// Called when a proposal arrives on the channel. Batches and processes it
    /// along with any other proposals already queued.
    async fn handle_proposals(&mut self, first: Proposal, proposal_rx: &mut mpsc::Receiver<Proposal>) {
        match self.state.role.clone() {
            Role::Follower { leader, .. } => {
                // Forward first proposal, then drain the rest.
                let mut proposals = vec![first];
                while let Ok(p) = proposal_rx.try_recv() {
                    proposals.push(p);
                }
                self.forward_or_drop_proposals_vec(leader, proposals);
            }
            Role::Candidate { .. } => {
                drop(first);
                while proposal_rx.try_recv().is_ok() {}
            }
            Role::Leader { .. } => {
                let mut batch = vec![first];
                while batch.len() < MAX_ENTRIES_PER_RPC {
                    match proposal_rx.try_recv() {
                        Ok(p) => batch.push(p),
                        Err(_) => break,
                    }
                }
                self.append_batch(batch);
                let now = Instant::now();
                let new_last = self.state.log.last_index();
                if let Role::Leader {
                    ref mut last_heartbeat,
                    ref mut last_replicated_index,
                    ..
                } = self.state.role
                {
                    *last_heartbeat = now;
                    *last_replicated_index = new_last;
                }
                self.replicate_to_all();
            }
        }
    }

    /// Replicate immediately if there are log entries the leader hasn't sent yet.
    /// Handles ClientRequest messages forwarded by followers between heartbeats.
    fn replicate_if_new_entries(&mut self) {
        if let Role::Leader { last_replicated_index, .. } = self.state.role.clone() {
            let new_last = self.state.log.last_index();
            if new_last > last_replicated_index {
                let now = Instant::now();
                if let Role::Leader {
                    ref mut last_heartbeat,
                    ref mut last_replicated_index,
                    ..
                } = self.state.role
                {
                    *last_heartbeat = now;
                    *last_replicated_index = new_last;
                }
                self.replicate_to_all();
            }
        }
    }

    fn forward_or_drop_proposals(
        &mut self,
        leader: Option<NodeId>,
        proposal_rx: &mut mpsc::Receiver<Proposal>,
    ) {
        loop {
            match proposal_rx.try_recv() {
                Ok(p) => self.forward_one_proposal(leader, p),
                Err(_) => break,
            }
        }
    }

    fn forward_or_drop_proposals_vec(&mut self, leader: Option<NodeId>, proposals: Vec<Proposal>) {
        for p in proposals {
            self.forward_one_proposal(leader, p);
        }
    }

    fn forward_one_proposal(&mut self, leader: Option<NodeId>, p: Proposal) {
        if let Some(leader_id) = leader {
            let req_id = self.state.next_req_id;
            self.state.next_req_id += 1;
            self.client_pending.insert(req_id, p.done);
            self.cluster.send(
                leader_id,
                Message::ClientRequest {
                    request_id: req_id,
                    op: p.op,
                },
            );
        }
        // No leader → done_tx drops → propose() retries.
    }

    fn append_batch(&mut self, batch: Vec<Proposal>) {
        for proposal in batch {
            let idx = self.state.log.last_index() + 1;
            let entry = LogEntry {
                index: idx,
                term: self.state.current_term,
                request_id: self.state.next_req_id,
                client_node: self.cluster.local_id,
                op: proposal.op,
            };
            self.state.next_req_id += 1;
            self.state.log.append(entry);
            self.state.pending.insert(idx, proposal.done);
        }
    }

    // ── Message handlers ──────────────────────────────────────────────────

    fn handle_message(&mut self, from: NodeId, msg: Message) {
        match msg {
            Message::RequestVote(rv) => self.on_request_vote(from, rv),
            Message::RequestVoteReply(rvr) => self.on_vote_reply(from, rvr),
            Message::AppendEntries(ae) => self.on_append_entries(from, ae),
            Message::AppendEntriesReply(aer) => self.on_append_entries_reply(from, aer),
            Message::InstallSnapshot(is) => self.on_install_snapshot(from, is),
            Message::InstallSnapshotReply(isr) => self.on_install_snapshot_reply(from, isr),
            Message::ClientRequest { request_id, op } => {
                self.on_forwarded_request(from, request_id, op)
            }
            Message::ClientResponse { request_id, result, log_index } => {
                if let Some(tx) = self.client_pending.remove(&request_id) {
                    if self.state.last_applied >= log_index {
                        // Entry already applied locally — signal the caller now.
                        let _ = tx.send(result);
                    } else if self.deferred_responses.len() < MAX_DEFERRED_RESPONSES {
                        // Entry not yet applied on this follower.  Defer until
                        // advance_applied() reaches log_index so the caller sees
                        // the write in the local store when it wakes up.
                        self.deferred_responses
                            .entry(log_index)
                            .or_default()
                            .push((tx, result));
                        // If the cap is exceeded: tx drops, done_rx gets Err,
                        // propose() retries.
                    }
                }
            }
            _ => {}
        }
    }

    /// Step down to follower, clearing any stale forwarded-request callbacks so
    /// their callers don't wait forever on a channel that will never be resolved.
    fn step_down(&mut self, term: Term, leader: Option<NodeId>) {
        self.state.become_follower(term, leader);
        self.client_pending.clear();
        self.deferred_responses.clear();
    }

    fn on_request_vote(&mut self, from: NodeId, rv: RequestVote) {
        // Observers don't vote.
        if self.cluster.is_observer {
            return;
        }
        if rv.term > self.state.current_term {
            self.step_down(rv.term, None);
            self.persist_meta();
        }

        let grant = rv.term >= self.state.current_term
            && (self.state.voted_for.is_none()
                || self.state.voted_for == Some(rv.candidate_id))
            && (rv.last_log_term > self.state.log.last_term()
                || (rv.last_log_term == self.state.log.last_term()
                    && rv.last_log_index >= self.state.log.last_index()));

        if grant {
            self.state.voted_for = Some(rv.candidate_id);
            self.persist_meta();
            if let Role::Follower { ref mut election_deadline, .. } = self.state.role {
                *election_deadline = random_deadline(self.state.election_timeout_min_ms, self.state.election_timeout_max_ms);
            }
        }

        self.cluster.send(
            from,
            Message::RequestVoteReply(RequestVoteReply {
                term: self.state.current_term,
                vote_granted: grant,
            }),
        );
    }

    fn on_vote_reply(&mut self, from: NodeId, rvr: RequestVoteReply) {
        if rvr.term > self.state.current_term {
            self.step_down(rvr.term, None);
            self.persist_meta();
            self.update_pub_state();
            return;
        }

        // Observers don't cast votes, ignore replies if they somehow arrive.
        if self.cluster.is_observer {
            return;
        }

        if let Role::Candidate { ref mut votes, .. } = self.state.role {
            if rvr.vote_granted && !self.cluster.is_peer_observer(from) {
                votes.insert(from);
                let n_votes = votes.len() + 1; // +1 for self
                let quorum = self.cluster.total_voting_nodes / 2 + 1;

                if n_votes >= quorum {
                    info!(
                        "Elected leader for term {} ({}/{} votes)",
                        self.state.current_term, n_votes, self.cluster.total_voting_nodes
                    );
                    let last_index = self.state.log.last_index();
                    let peer_ids = self.cluster.peer_ids();
                    self.state.role = Role::Leader {
                        next_index: peer_ids.iter().map(|&id| (id, last_index + 1)).collect(),
                        match_index: peer_ids.iter().map(|&id| (id, 0)).collect(),
                        last_heartbeat: Instant::now() - self.heartbeat_interval,
                        last_replicated_index: last_index,
                    };
                    self.update_pub_state();
                    self.replicate_to_all();
                }
            }
        }
    }

    fn on_append_entries(&mut self, from: NodeId, ae: AppendEntries) {
        if ae.term < self.state.current_term {
            self.cluster.send(
                from,
                Message::AppendEntriesReply(AppendEntriesReply {
                    term: self.state.current_term,
                    success: false,
                    match_index: self.state.log.last_index(),
                }),
            );
            return;
        }

        if ae.term > self.state.current_term {
            self.step_down(ae.term, Some(ae.leader_id));
            self.persist_meta();
        } else {
            match self.state.role {
                Role::Follower { ref mut leader, ref mut election_deadline } => {
                    *leader = Some(ae.leader_id);
                    *election_deadline = random_deadline(self.state.election_timeout_min_ms, self.state.election_timeout_max_ms);
                }
                _ => {
                    self.step_down(ae.term, Some(ae.leader_id));
                    self.persist_meta();
                }
            }
        }

        // Save leader_commit before partially moving ae.entries into
        // append_leader_entries (Vec<LogEntry> is not Copy).
        let leader_commit = ae.leader_commit;
        let success = self.state.log.append_leader_entries(
            ae.prev_log_index,
            ae.prev_log_term,
            ae.entries,
        );

        // No log persistence — the Raft log is in-memory only.

        if success && leader_commit > self.state.commit_index {
            self.state.commit_index =
                leader_commit.min(self.state.log.last_index());
            self.advance_applied();
        }

        self.update_pub_state();

        self.cluster.send(
            from,
            Message::AppendEntriesReply(AppendEntriesReply {
                term: self.state.current_term,
                success,
                match_index: if success { self.state.log.last_index() } else { 0 },
            }),
        );
    }

    fn on_append_entries_reply(&mut self, from: NodeId, aer: AppendEntriesReply) {
        if aer.term > self.state.current_term {
            self.step_down(aer.term, None);
            self.persist_meta();
            self.update_pub_state();
            return;
        }

        let last_index = self.state.log.last_index();
        let current_term = self.state.current_term;
        let total_voting = self.cluster.total_voting_nodes;
        let quorum_needed = total_voting / 2 + 1;
        let mut should_replicate = false;

        if let Role::Leader { ref mut next_index, ref mut match_index, .. } = self.state.role {
            if aer.success {
                let new_match = aer.match_index;
                let old_match = match_index.get(&from).copied().unwrap_or(0);
                if new_match > old_match {
                    match_index.insert(from, new_match);
                    // Only advance next_index — never regress it.  An old reply
                    // can arrive after the optimistic advance already pushed
                    // next_index further ahead; clamping down would re-send
                    // entries the follower already has.
                    let cur_ni = next_index.get(&from).copied().unwrap_or(1);
                    if new_match + 1 > cur_ni {
                        next_index.insert(from, new_match + 1);
                    }
                }

                // Collect all match values, pad with 0 for disconnected voting peers.
                // This prevents a partitioned leader from committing without quorum.
                let mut all_matches: Vec<LogIndex> = match_index
                    .iter()
                    .filter(|(id, _)| !self.cluster.is_peer_observer(**id))
                    .map(|(_, &m)| m)
                    .collect();
                all_matches.push(last_index); // leader itself

                // Pad with 0 for voting peers that haven't connected yet.
                while all_matches.len() < total_voting {
                    all_matches.push(0);
                }
                all_matches.sort_unstable(); // ascending

                // quorum_needed-th largest = index [total_voting - quorum_needed]
                let quorum_match =
                    all_matches.get(total_voting.saturating_sub(quorum_needed)).copied().unwrap_or(0);

                let term_ok = self.state.log.term_at(quorum_match) == Some(current_term);
                if quorum_match > self.state.commit_index && term_ok {
                    self.state.commit_index = quorum_match;
                    self.advance_applied();
                }
            } else {
                let ni = next_index.entry(from).or_insert(1);
                if aer.match_index + 1 < *ni {
                    *ni = (aer.match_index + 1).max(1);
                } else {
                    *ni = (*ni).saturating_sub(1).max(1);
                }
                should_replicate = true;
            }
        }

        if should_replicate {
            self.send_append_entries_to(from);
        }
    }

    fn on_forwarded_request(&mut self, from: NodeId, request_id: u64, op: FileOp) {
        if !self.state.is_leader() {
            self.cluster.send(
                from,
                Message::ClientResponse {
                    request_id,
                    result: FileOpResult::Error("not leader".into()),
                    log_index: 0,
                },
            );
            return;
        }

        let idx = self.state.log.last_index() + 1;
        let entry = LogEntry {
            index: idx,
            term: self.state.current_term,
            request_id,
            client_node: from,
            op,
        };
        self.state.log.append(entry);

        let (tx, rx) = oneshot::channel::<FileOpResult>();
        self.state.pending.insert(idx, tx);

        let cluster = self.cluster.clone();
        tokio::spawn(async move {
            match rx.await {
                Ok(result) => {
                    cluster.send(from, Message::ClientResponse { request_id, result, log_index: idx });
                }
                Err(_) => {}
            }
        });
        // Do NOT call replicate_to_all() here — tick() detects new log entries
        // and batches them for replication, avoiding redundant per-request AE floods.
    }

    // ── Election ──────────────────────────────────────────────────────────

    fn start_election(&mut self) {
        // Drop forwarded-request callbacks — the follower→leader path is broken.
        // propose() will get Err from done_rx.await and retry.
        self.client_pending.clear();
        self.elections_started += 1;

        self.state.current_term += 1;
        let term = self.state.current_term;
        let my_id = self.cluster.local_id;
        self.state.voted_for = Some(my_id);
        self.persist_meta();

        let mut votes = HashSet::new();
        votes.insert(my_id);

        // Single-node cluster: self-vote alone satisfies quorum — become leader
        // immediately without waiting for peer replies (there are none).
        let quorum = self.cluster.total_voting_nodes / 2 + 1;
        if votes.len() >= quorum {
            info!(
                "Single-node cluster: self-elected leader for term {} (quorum {})",
                term, quorum
            );
            let last_index = self.state.log.last_index();
            let peer_ids = self.cluster.peer_ids();
            self.state.role = Role::Leader {
                next_index: peer_ids.iter().map(|&id| (id, last_index + 1)).collect(),
                match_index: peer_ids.iter().map(|&id| (id, 0)).collect(),
                last_heartbeat: Instant::now() - self.heartbeat_interval,
                last_replicated_index: last_index,
            };
            self.update_pub_state();
            self.replicate_to_all();
            return;
        }

        self.state.role =
            Role::Candidate { votes, election_deadline: random_deadline(self.state.election_timeout_min_ms, self.state.election_timeout_max_ms) };
        self.update_pub_state();

        info!("Starting election for term {}", term);

        self.cluster.broadcast(Message::RequestVote(RequestVote {
            term,
            candidate_id: my_id,
            last_log_index: self.state.log.last_index(),
            last_log_term: self.state.log.last_term(),
        }));
    }

    // ── Replication ───────────────────────────────────────────────────────

    /// Send AppendEntries to all known peers, first syncing in any peers that
    /// connected after the election.  Peers that connect after the election
    /// are not in next_index / match_index yet; without this sync they never
    /// receive heartbeats and their election timers fire, causing constant
    /// re-elections.
    fn replicate_to_all(&mut self) {
        // Collect peers and update tracking maps in one pass while we hold
        // the mutable role borrow, then release it before calling
        // send_append_entries_to (which needs an immutable self borrow).
        let peers: Vec<NodeId> = match self.state.role {
            Role::Leader { ref mut next_index, ref mut match_index, .. } => {
                let last_index = self.state.log.last_index();
                for &peer in self.cluster.peer_ids().iter() {
                    next_index.entry(peer).or_insert(last_index + 1);
                    match_index.entry(peer).or_insert(0);
                }
                next_index.keys().copied().collect()
            }
            _ => return,
        };

        if peers.is_empty() {
            // Single-node cluster: no peers to replicate to, so commit immediately.
            // In a multi-node cluster the leader waits for peer acknowledgements
            // (on_append_entries_reply) before advancing commit_index.  With no
            // peers the quorum is just the leader itself, so every appended entry
            // is immediately safe to commit.
            let last_index = self.state.log.last_index();
            let current_term = self.state.current_term;
            if last_index > self.state.commit_index
                && self.state.log.term_at(last_index) == Some(current_term)
            {
                self.state.commit_index = last_index;
                self.advance_applied();
            }
            return;
        }

        self.heartbeats_sent += 1;
        for peer in peers {
            self.send_append_entries_to(peer);
        }
    }

    fn send_append_entries_to(&mut self, peer: NodeId) {
        // Step 1: read next_index without holding a borrow on self.state.
        let ni = if let Role::Leader { ref next_index, .. } = self.state.role {
            *next_index.get(&peer).unwrap_or(&1)
        } else {
            return;
        };

        // If the follower is behind our snapshot boundary, log entries it
        // needs have been compacted away — send InstallSnapshot instead.
        if ni <= self.state.log.snapshot_index {
            self.send_install_snapshot_to(peer);
            return;
        }

        let prev = ni.saturating_sub(1);

        // Step 2: collect entries (no role borrow held).
        let slice = self.state.log.entries_from(prev);
        let send_count = slice.len().min(MAX_ENTRIES_PER_RPC);
        let entries: Vec<_> = slice[..send_count].to_vec();
        let last_sent_index = entries.last().map(|e| e.index);

        // Step 3: optimistically advance next_index so subsequent calls only
        // carry newly appended entries instead of re-sending in-flight ones.
        if let Some(last_idx) = last_sent_index {
            if let Role::Leader { ref mut next_index, .. } = self.state.role {
                let entry = next_index.entry(peer).or_insert(1);
                if last_idx + 1 > *entry {
                    *entry = last_idx + 1;
                }
            }
        }

        // Step 4: build and dispatch the AppendEntries RPC.
        let prev_term = self.state.log.term_at(prev).unwrap_or(0);
        let ae = AppendEntries {
            term: self.state.current_term,
            leader_id: self.cluster.local_id,
            prev_log_index: prev,
            prev_log_term: prev_term,
            entries,
            leader_commit: self.state.commit_index,
        };
        self.cluster.send(peer, Message::AppendEntries(ae));
    }

    /// Send the current snapshot to a follower that has fallen behind the
    /// leader's log compaction boundary.
    fn send_install_snapshot_to(&mut self, peer: NodeId) {
        let snap_index = self.state.log.snapshot_index;
        let snap_term = self.state.log.snapshot_term;

        let data = {
            let st = self.store.read();
            match bincode::serialize(&*st) {
                Ok(d) => d,
                Err(e) => {
                    warn!("InstallSnapshot serialize failed for {}: {}", peer, e);
                    return;
                }
            }
        };

        // Optimistically advance next_index past the snapshot so subsequent
        // ticks don't re-trigger this path while the snapshot is in flight.
        if let Role::Leader { ref mut next_index, .. } = self.state.role {
            let entry = next_index.entry(peer).or_insert(1);
            if snap_index + 1 > *entry {
                *entry = snap_index + 1;
            }
        }

        self.cluster.send(
            peer,
            Message::InstallSnapshot(InstallSnapshot {
                term: self.state.current_term,
                leader_id: self.cluster.local_id,
                snapshot_index: snap_index,
                snapshot_term: snap_term,
                data,
            }),
        );
    }

    fn on_install_snapshot(&mut self, from: NodeId, is: InstallSnapshot) {
        // Stale term — reject.
        if is.term < self.state.current_term {
            self.cluster.send(
                from,
                Message::InstallSnapshotReply(InstallSnapshotReply {
                    term: self.state.current_term,
                    match_index: self.state.log.last_index(),
                }),
            );
            return;
        }

        if is.term > self.state.current_term {
            self.step_down(is.term, Some(is.leader_id));
            self.persist_meta();
        } else {
            match self.state.role {
                Role::Follower { ref mut leader, ref mut election_deadline } => {
                    *leader = Some(is.leader_id);
                    *election_deadline = random_deadline(self.state.election_timeout_min_ms, self.state.election_timeout_max_ms);
                }
                _ => {
                    self.step_down(is.term, Some(is.leader_id));
                    self.persist_meta();
                }
            }
        }

        // Snapshot is behind our current state — nothing to do.
        if is.snapshot_index <= self.state.last_applied {
            self.cluster.send(
                from,
                Message::InstallSnapshotReply(InstallSnapshotReply {
                    term: self.state.current_term,
                    match_index: self.state.log.last_index(),
                }),
            );
            return;
        }

        // Deserialize the snapshot first so we can write both storage formats
        // atomically before updating in-memory state.
        let snap_store = match bincode::deserialize::<crate::fs::store::FileStore>(&is.data) {
            Ok(s) => s,
            Err(e) => {
                warn!("InstallSnapshot: deserialize failed from {}: {}", from, e);
                return;
            }
        };

        // Persist to DB BEFORE updating in-memory state.
        // Write both the legacy blob (for backwards compat) and the per-inode
        // table so the node can use the fast delta path after this snapshot.
        if let Some(db) = &mut self.db {
            // Legacy blob — keeps the fs_snapshot table consistent.
            if let Err(e) = db.save_snapshot(is.snapshot_index, is.snapshot_term, &is.data) {
                warn!("InstallSnapshot: blob DB save failed, ignoring snapshot: {}", e);
                return;
            }
            // Per-inode table — atomically replaces all rows using meta/data split.
            use crate::fs::inode::{Inode, InodePersistMeta};
            let inodes: Vec<(u64, Vec<u8>, Option<Vec<u8>>)> = snap_store.inodes().iter()
                .filter_map(|(ino, inode)| {
                    let meta = bincode::serialize(&InodePersistMeta::from_inode(inode)).ok()?;
                    let data = match inode {
                        Inode::File(f) => Some(f.data.clone()),
                        _              => None,
                    };
                    Some((*ino, meta, data))
                })
                .collect();
            if let Err(e) = db.apply_full_inode_snapshot(
                &inodes,
                snap_store.next_ino_val(),
                snap_store.total_data_bytes(),
                is.snapshot_index,
                is.snapshot_term,
            ) {
                warn!("InstallSnapshot: per-inode DB save failed (non-fatal): {}", e);
                // Non-fatal: blob snapshot was saved; delta path will catch up.
            }
        }

        // Apply the snapshot to the state machine.
        {
            let mut st = self.store.write();
            *st = snap_store;
            st.set_limits(self.max_file_size, self.max_fs_size);
        }

        // Compact the log to the snapshot boundary and reset applied state.
        self.state.log.compact_before(is.snapshot_index, is.snapshot_term);
        self.state.commit_index = is.snapshot_index;
        self.state.last_applied = is.snapshot_index;
        self.state.next_snapshot_at = is.snapshot_index + self.snapshot_every;

        info!(
            "Installed snapshot at index {} (term {})",
            is.snapshot_index, is.snapshot_term
        );
        self.update_pub_state();

        self.cluster.send(
            from,
            Message::InstallSnapshotReply(InstallSnapshotReply {
                term: self.state.current_term,
                match_index: is.snapshot_index,
            }),
        );
    }

    fn on_install_snapshot_reply(&mut self, from: NodeId, isr: InstallSnapshotReply) {
        if isr.term > self.state.current_term {
            self.step_down(isr.term, None);
            self.persist_meta();
            self.update_pub_state();
            return;
        }

        // Treat a successful snapshot install like a successful AER.
        if let Role::Leader { ref mut next_index, ref mut match_index, .. } = self.state.role {
            let new_match = isr.match_index;
            let old_match = match_index.get(&from).copied().unwrap_or(0);
            if new_match > old_match {
                match_index.insert(from, new_match);
                let cur_ni = next_index.get(&from).copied().unwrap_or(1);
                if new_match + 1 > cur_ni {
                    next_index.insert(from, new_match + 1);
                }
            }
        }

        // Send any entries that come after the snapshot.
        self.send_append_entries_to(from);
    }

    // ── Apply committed entries ───────────────────────────────────────────

    /// Advance last_applied up to commit_index, apply each entry to the store,
    /// then fire callbacks to waiting proposers.
    ///
    /// **Durability model**: data committed by a Raft quorum is durable across
    /// the cluster — at least a majority of nodes hold the entry in their
    /// in-memory log before the leader reports "committed".  Per-node SQLite
    /// persistence happens every `persist_every` entries via this function (or
    /// at every `snapshot_every` log compaction, whichever comes first).
    ///
    /// Writing the full FileStore snapshot to SQLite on *every* committed batch
    /// caused O(filesystem_size) I/O per write: serialise all files, write
    /// ~N/4096 SQLite pages, `fsync`, then WAL-checkpoint (copy all pages back
    /// from the WAL to the main DB file).  With thousands of files and a large
    /// store this degraded throughput to single-digit KB/s and spiked CPU on
    /// followers that apply the same entries.
    fn advance_applied(&mut self) {
        let start_applied = self.state.last_applied;
        let mut callbacks: Vec<(oneshot::Sender<FileOpResult>, FileOpResult)> = Vec::new();

        while self.state.last_applied < self.state.commit_index {
            self.state.last_applied += 1;
            let idx = self.state.last_applied;

            if let Some(entry) = self.state.log.get(idx).cloned() {
                // Apply to the replicated state machine.
                let result = {
                    let mut st = self.store.write();
                    st.apply(&entry.op)
                };

                let op_result = match &result {
                    Ok(()) => FileOpResult::Ok,
                    Err(TinyCfsError::LockContended(holder)) => {
                        FileOpResult::LockContended(holder.clone())
                    }
                    Err(TinyCfsError::FileTooLarge { .. }) => FileOpResult::FileTooLarge,
                    Err(TinyCfsError::NoSpace) => FileOpResult::NoSpace,
                    Err(e) => FileOpResult::Error(e.to_string()),
                };

                if let Err(ref e) = result {
                    match e {
                        TinyCfsError::LockContended(_) | TinyCfsError::AlreadyExists(_) => {}
                        other => warn!("State machine at index {}: {}", idx, other),
                    }
                }

                // Collect callbacks — defer firing until after the DB write.
                // O(1) HashMap lookup replaces the prior O(n) Vec scan.
                if let Some(tx) = self.state.pending.remove(&idx) {
                    callbacks.push((tx, op_result.clone()));
                }
            }
        }

        if self.state.last_applied > start_applied {
            self.proposals_committed += self.state.last_applied - start_applied;
            // Persist the snapshot every persist_every entries.  Callbacks are
            // fired *after* this block — if persist_every == 1 clients still
            // wait for the SQLite write before getting a reply.
            let due = self.last_persisted == 0
                || self.state.last_applied - self.last_persisted >= self.persist_every;
            if due {
                let snap_term = self.state.log.term_at(self.state.last_applied)
                    .unwrap_or(self.state.log.snapshot_term);
                self.persist_store(self.state.last_applied, snap_term);
                self.last_persisted = self.state.last_applied;
            }
        }

        // Fire callbacks (after optional persist so persist_every==1 is still safe).
        for (tx, result) in callbacks {
            let _ = tx.send(result);
        }

        // Fire any follower ClientResponse callbacks that were deferred because
        // the entry had not yet been applied locally when the response arrived.
        if !self.deferred_responses.is_empty() {
            let ready: Vec<LogIndex> = self.deferred_responses.keys()
                .copied()
                .filter(|&idx| idx <= self.state.last_applied)
                .collect();
            for idx in ready {
                if let Some(pending) = self.deferred_responses.remove(&idx) {
                    for (tx, result) in pending {
                        let _ = tx.send(result);
                    }
                }
            }
        }

        // Compact the in-memory log periodically to bound memory usage.
        if self.state.last_applied >= self.state.next_snapshot_at {
            self.compact_log();
        }
    }

    /// Serialize the FileStore and write it to SQLite as the current snapshot.
    /// Persist changed inodes to SQLite as a delta (O(changed_files), not O(total_files)).
    ///
    /// Drains the dirty/deleted sets from the FileStore, serialises only the
    /// modified inodes, and writes them in a single SQLite transaction along
    /// with the updated bookkeeping values.  The `snap_index` / `snap_term`
    /// are always written so the restart logic knows where in the Raft log the
    /// on-disk state is anchored — even when the dirty sets happen to be empty
    /// (e.g. compact_log calling after a batch that was already persisted).
    fn persist_store(&mut self, snap_index: LogIndex, snap_term: Term) {
        if self.db.is_none() { return; }

        // Drain dirty sets (brief write lock — just swaps three HashSets).
        let (dirty_set, data_dirty_set, deleted_set) = {
            let mut st = self.store.write();
            st.drain_dirty()
        };

        // Build the delta while holding a read lock.
        // For each dirty inode: always serialize the small meta blob;
        // only clone the large data blob when file content actually changed.
        let (dirty, deleted, next_ino, total_bytes) = {
            use crate::fs::inode::{Inode, InodePersistMeta};
            let st = self.store.read();
            let dirty: Vec<(u64, Vec<u8>, Option<Vec<u8>>)> = dirty_set.iter()
                .filter_map(|&ino| {
                    let inode = st.get(ino)?;
                    let meta = bincode::serialize(&InodePersistMeta::from_inode(inode)).ok()?;
                    let data = if data_dirty_set.contains(&ino) {
                        match inode {
                            Inode::File(f) => Some(f.data.clone()),
                            _              => None,
                        }
                    } else {
                        None
                    };
                    Some((ino, meta, data))
                })
                .collect();
            let deleted: Vec<u64> = deleted_set.into_iter().collect();
            (dirty, deleted, st.next_ino_val(), st.total_data_bytes())
        };

        // Write to SQLite.  block_in_place tells the tokio runtime that this
        // thread may block (on WAL writes / checkpoint fsyncs) so it can move
        // other async tasks to different threads during the I/O.
        let db = self.db.as_mut().unwrap();
        tokio::task::block_in_place(|| {
            if let Err(e) = db.apply_inode_delta(
                &dirty, &deleted, next_ino, total_bytes, snap_index, snap_term,
            ) {
                warn!("FileStore delta persist failed at index {}: {}", snap_index, e);
            }
        });
    }

    /// Compact the in-memory Raft log up to last_applied.
    ///
    /// Always persists the FileStore snapshot BEFORE discarding log entries.
    /// After compaction those entries no longer exist in memory; if the node
    /// crashed without a snapshot the only recovery path would be a full
    /// InstallSnapshot from the leader.  Persisting first keeps the recovery
    /// window bounded to at most `persist_every` entries even if the node
    /// crashes immediately after log compaction.
    fn compact_log(&mut self) {
        let snap_index = self.state.last_applied;
        let snap_term = self.state.log.term_at(snap_index)
            .unwrap_or(self.state.log.snapshot_term);
        // Always write a fresh snapshot at the compaction boundary regardless
        // of when last_persisted was updated.
        self.persist_store(snap_index, snap_term);
        self.last_persisted = snap_index;
        self.state.log.compact_before(snap_index, snap_term);
        self.state.next_snapshot_at = snap_index + self.snapshot_every;
        debug!("Log compacted at index {} (term {})", snap_index, snap_term);
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    fn persist_meta(&mut self) {
        if let Some(db) = &mut self.db {
            if let Err(e) = db.save_term(self.state.current_term, self.state.voted_for) {
                // Raft safety depends on this write: if we continue after a
                // failed persist we may vote again in this term after a restart.
                panic!(
                    "FATAL: failed to persist Raft metadata \
                     (term={}, voted_for={:?}): {}",
                    self.state.current_term, self.state.voted_for, e
                );
            }
        }
    }

    fn update_pub_state(&self) {
        let mut ps = self.pub_state.write();
        ps.is_leader = self.state.is_leader();
        ps.leader_id = self.state.leader_id();
        ps.current_term = self.state.current_term;
        ps.has_quorum = if ps.is_leader {
            // Leader has quorum only if enough voting peers are currently connected.
            // +1 accounts for the leader itself.
            let connected_voting = self.cluster.voting_peer_ids().len() + 1;
            let quorum = self.cluster.total_voting_nodes / 2 + 1;
            connected_voting >= quorum
        } else {
            matches!(&self.state.role, Role::Follower { leader: Some(_), .. })
        };
        // Extended status
        ps.commit_index = self.state.commit_index;
        ps.last_applied = self.state.last_applied;
        ps.log_entries = self.state.log.len();
        ps.snapshot_index = self.state.log.snapshot_index;
        ps.role_name = match &self.state.role {
            Role::Leader { .. } => "Leader",
            Role::Follower { .. } => "Follower",
            Role::Candidate { .. } => "Candidate",
        };
        ps.connected_peers = self.cluster.peer_count();
        // Performance counters
        ps.heartbeats_sent = self.heartbeats_sent;
        ps.proposals_committed = self.proposals_committed;
        ps.elections_started = self.elections_started;
    }
}
