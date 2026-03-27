pub mod log;

/// High-throughput Raft consensus engine.
///
/// Design goals for hundreds of nodes / thousands of writes per second:
///
/// 1. **Actor model** — a single dedicated tokio task owns all mutable Raft
///    state; no Mutex contention between concurrent proposals.
/// 2. **Proposal batching** — `propose()` queues work into a bounded channel;
///    the Raft task drains the full channel each tick and appends all ops in
///    one `AppendEntries` RPC instead of one RPC per write.
/// 3. **Parallel per-peer replication** — each peer gets an independent
///    async write path; a slow peer does not stall others.
/// 4. **Pipelined AppendEntries** — the leader does not wait for an
///    acknowledgement before sending the next batch; in-flight entries are
///    tracked per peer via `next_index` / `match_index`.
///
/// Throughput model (rough):
///   - Batch window: 1 ms → at 10 000 writes/s each batch has ~10 entries.
///   - Leader sends one RPC to each of N peers per batch: O(N) fan-out.
///   - For N ≤ ~200 this is fine on a 1 Gbit LAN.
///   - For N > 200 add a hierarchical relay layer (not yet implemented):
///     split peers into relay groups; each relay replicates its sub-tree.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use tokio::sync::{mpsc, oneshot};
use tokio::time;
use tracing::{debug, info, warn};

use crate::cluster::message::{
    AppendEntries, AppendEntriesReply, FileOp, FileOpResult, LogEntry, LogIndex, Message, NodeId,
    RequestVote, RequestVoteReply, Term,
};
use crate::cluster::{ClusterHandle, Envelope};
use crate::consensus::log::Log;
use crate::error::{Result, TinyCfsError};

// ─── Tunables ─────────────────────────────────────────────────────────────────

/// How often the leader sends heartbeats (and flushes pending proposals).
const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(5);
/// Randomised election timeout range.
const ELECTION_TIMEOUT_MIN: Duration = Duration::from_millis(150);
const ELECTION_TIMEOUT_MAX: Duration = Duration::from_millis(300);
/// Maximum entries per AppendEntries RPC (limits per-RPC payload size).
const MAX_ENTRIES_PER_RPC: usize = 256;
/// Proposal queue depth — limits memory when the cluster falls behind.
const PROPOSAL_QUEUE_DEPTH: usize = 16_384;

// ─── Public proposal API ──────────────────────────────────────────────────────

/// A single write proposal enqueued by a FUSE handler.
pub struct Proposal {
    pub op: FileOp,
    /// Fires with Ok/Err once the entry is committed by a quorum.
    pub done: oneshot::Sender<FileOpResult>,
}

// ─── Internal Raft state (owned by the Raft actor task) ──────────────────────

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
        /// Per-peer: next log index to send.
        next_index: HashMap<NodeId, LogIndex>,
        /// Per-peer: highest index known to be replicated on that peer.
        match_index: HashMap<NodeId, LogIndex>,
        last_heartbeat: Instant,
    },
}

impl Role {
    fn name(&self) -> &'static str {
        match self {
            Role::Follower { .. } => "follower",
            Role::Candidate { .. } => "candidate",
            Role::Leader { .. } => "leader",
        }
    }
}

struct RaftState {
    // Persistent fields (kept in-memory; snapshot/WAL is a future concern)
    current_term: Term,
    voted_for: Option<NodeId>,
    log: Log,

    // Volatile
    commit_index: LogIndex,
    last_applied: LogIndex,
    role: Role,

    // Pending proposals — resolved when their log index commits.
    pending: Vec<(LogIndex, oneshot::Sender<FileOpResult>)>,

    // Monotonic request counter (used to tag forwarded client requests)
    next_req_id: u64,
}

impl RaftState {
    fn new() -> Self {
        RaftState {
            current_term: 0,
            voted_for: None,
            log: Log::new(),
            commit_index: 0,
            last_applied: 0,
            role: Role::Follower {
                leader: None,
                election_deadline: random_deadline(),
            },
            pending: Vec::new(),
            next_req_id: 1,
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
        self.role = Role::Follower {
            leader,
            election_deadline: random_deadline(),
        };
    }

    /// Advance `last_applied` up to `commit_index` and fire callbacks.
    /// Returns committed entries (for the apply channel).
    fn advance_applied(&mut self) -> Vec<LogEntry> {
        let mut applied = Vec::new();
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            if let Some(entry) = self.log.get(self.last_applied).cloned() {
                applied.push(entry.clone());

                // Resolve any pending proposals at this index
                let idx = self.last_applied;
                let mut i = 0;
                while i < self.pending.len() {
                    if self.pending[i].0 == idx {
                        let (_, tx) = self.pending.remove(i);
                        let _ = tx.send(FileOpResult::Ok);
                    } else {
                        i += 1;
                    }
                }
            }
        }
        applied
    }
}

fn random_deadline() -> Instant {
    use rand::Rng;
    let ms = rand::thread_rng().gen_range(
        ELECTION_TIMEOUT_MIN.as_millis() as u64..=ELECTION_TIMEOUT_MAX.as_millis() as u64,
    );
    Instant::now() + Duration::from_millis(ms)
}

// ─── Shared public handle (clone-able, Send) ──────────────────────────────────

/// Lightweight handle to the Raft engine.  Clone freely — it is just a few
/// cheap Arc/channel clones.
#[derive(Clone)]
pub struct Consensus {
    proposal_tx: mpsc::Sender<Proposal>,
    /// Read-only snapshot of role + leader for routing purposes.
    pub_state: Arc<RwLock<PubState>>,
    local_id: NodeId,
}

/// Read-only public state — updated by the Raft actor after every role change.
#[derive(Clone, Default)]
pub struct PubState {
    pub is_leader: bool,
    pub leader_id: Option<NodeId>,
    pub current_term: Term,
}

impl Consensus {
    /// Build the consensus engine.
    ///
    /// Returns:
    /// - `Consensus` handle for FUSE and the main task to use.
    /// - `mpsc::UnboundedReceiver<LogEntry>` — committed entries delivered
    ///   in order; pipe these into the filesystem state machine.
    /// - `mpsc::Sender<Envelope>` — pipe incoming cluster messages here.
    pub fn new(
        cluster: ClusterHandle,
    ) -> (
        Self,
        mpsc::UnboundedReceiver<LogEntry>,
        mpsc::Sender<Envelope>,
    ) {
        let (proposal_tx, proposal_rx) = mpsc::channel(PROPOSAL_QUEUE_DEPTH);
        let (apply_tx, apply_rx) = mpsc::unbounded_channel();
        let (msg_tx, msg_rx) = mpsc::channel(4096);
        let pub_state = Arc::new(RwLock::new(PubState::default()));

        let local_id = cluster.local_id;
        let engine = RaftEngine {
            state: RaftState::new(),
            cluster,
            apply_tx,
            pub_state: pub_state.clone(),
        };

        tokio::spawn(engine.run(proposal_rx, msg_rx));

        let handle = Consensus { proposal_tx, pub_state, local_id };
        (handle, apply_rx, msg_tx)
    }

    /// Submit a write operation. Awaits until committed by a quorum (or timeout).
    ///
    /// This call is **non-blocking on the proposal queue**: it returns quickly
    /// when the queue is full (back-pressure) by returning `NoQuorum`.
    pub async fn propose(&self, op: FileOp) -> Result<()> {
        let (done_tx, done_rx) = oneshot::channel();
        let proposal = Proposal { op, done: done_tx };

        // Enqueue — non-blocking; fails with NoQuorum under back-pressure.
        self.proposal_tx
            .try_send(proposal)
            .map_err(|_| TinyCfsError::NoQuorum)?;

        // Wait for the commit notification (5 s timeout)
        match time::timeout(Duration::from_secs(5), done_rx).await {
            Ok(Ok(FileOpResult::Ok)) => Ok(()),
            Ok(Ok(FileOpResult::Error(e))) => Err(TinyCfsError::Cluster(e)),
            Ok(Err(_)) => Err(TinyCfsError::Cluster("proposal channel dropped".into())),
            Err(_) => Err(TinyCfsError::Timeout),
        }
    }

    /// Whether this node is currently the Raft leader.
    pub fn is_leader(&self) -> bool {
        self.pub_state.read().is_leader
    }

    /// Node ID of the current leader, if known.
    pub fn leader_id(&self) -> Option<NodeId> {
        self.pub_state.read().leader_id
    }
}

// ─── The Raft actor (single tokio task, owns all mutable state) ───────────────

struct RaftEngine {
    state: RaftState,
    cluster: ClusterHandle,
    apply_tx: mpsc::UnboundedSender<LogEntry>,
    pub_state: Arc<RwLock<PubState>>,
}

impl RaftEngine {
    async fn run(
        mut self,
        mut proposal_rx: mpsc::Receiver<Proposal>,
        mut msg_rx: mpsc::Receiver<Envelope>,
    ) {
        let mut ticker = time::interval(Duration::from_millis(1));
        ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                biased;

                // ── Incoming cluster messages ────────────────────────────
                Some(env) = msg_rx.recv() => {
                    self.handle_message(env.from, env.msg);
                }

                // ── Tick: heartbeat, election timeout, proposal flush ────
                _ = ticker.tick() => {
                    self.tick(&mut proposal_rx).await;
                }
            }
        }
    }

    // ── Tick ──────────────────────────────────────────────────────────────

    async fn tick(&mut self, proposal_rx: &mut mpsc::Receiver<Proposal>) {
        let now = Instant::now();

        match self.state.role.clone() {
            Role::Follower { election_deadline, .. } => {
                if now >= election_deadline {
                    self.start_election();
                }
            }
            Role::Candidate { election_deadline, .. } => {
                if now >= election_deadline {
                    self.start_election();
                }
            }
            Role::Leader { last_heartbeat, .. } => {
                // ── Batch all pending proposals ──────────────────────────
                let mut batch: Vec<Proposal> = Vec::new();
                // Non-blocking drain (bounded by MAX_ENTRIES_PER_RPC)
                while batch.len() < MAX_ENTRIES_PER_RPC {
                    match proposal_rx.try_recv() {
                        Ok(p) => batch.push(p),
                        Err(_) => break,
                    }
                }

                let had_proposals = !batch.is_empty();
                if had_proposals {
                    self.append_batch(batch);
                }

                if now.duration_since(last_heartbeat) >= HEARTBEAT_INTERVAL || had_proposals {
                    if let Role::Leader { ref mut last_heartbeat, .. } = self.state.role {
                        *last_heartbeat = now;
                    }
                    self.replicate_to_all();
                }
            }
        }
    }

    /// Append a batch of proposals to the local log as a contiguous range.
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
            self.state.pending.push((idx, proposal.done));
        }
    }

    // ── Raft message handlers ─────────────────────────────────────────────

    fn handle_message(&mut self, from: NodeId, msg: Message) {
        match msg {
            Message::RequestVote(rv) => self.on_request_vote(from, rv),
            Message::RequestVoteReply(rvr) => self.on_vote_reply(from, rvr),
            Message::AppendEntries(ae) => self.on_append_entries(from, ae),
            Message::AppendEntriesReply(aer) => self.on_append_entries_reply(from, aer),
            Message::ClientRequest { request_id, op } => {
                self.on_forwarded_request(from, request_id, op)
            }
            _ => {}
        }
    }

    fn on_request_vote(&mut self, from: NodeId, rv: RequestVote) {
        if rv.term > self.state.current_term {
            self.state.become_follower(rv.term, None);
        }

        let grant = rv.term >= self.state.current_term
            && (self.state.voted_for.is_none()
                || self.state.voted_for == Some(rv.candidate_id))
            && (rv.last_log_term > self.state.log.last_term()
                || (rv.last_log_term == self.state.log.last_term()
                    && rv.last_log_index >= self.state.log.last_index()));

        if grant {
            self.state.voted_for = Some(rv.candidate_id);
            if let Role::Follower { ref mut election_deadline, .. } = self.state.role {
                *election_deadline = random_deadline();
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
            self.state.become_follower(rvr.term, None);
            return;
        }

        if let Role::Candidate { ref mut votes, .. } = self.state.role {
            if rvr.vote_granted {
                votes.insert(from);
                let n_votes = votes.len() + 1; // +1 for self
                let total_nodes = self.cluster.peer_count() + 1;
                let quorum = total_nodes / 2 + 1;

                if n_votes >= quorum {
                    info!(
                        "Elected leader for term {} ({}/{} votes)",
                        self.state.current_term, n_votes, total_nodes
                    );
                    let last_index = self.state.log.last_index();
                    let peer_ids = self.cluster.peer_ids();
                    self.state.role = Role::Leader {
                        next_index: peer_ids.iter().map(|&id| (id, last_index + 1)).collect(),
                        match_index: peer_ids.iter().map(|&id| (id, 0)).collect(),
                        last_heartbeat: Instant::now()
                            - HEARTBEAT_INTERVAL, // force immediate heartbeat
                    };
                    self.update_pub_state();
                    // Immediately send heartbeats to assert leadership
                    self.replicate_to_all();
                }
            }
        }
    }

    fn on_append_entries(&mut self, from: NodeId, ae: AppendEntries) {
        if ae.term < self.state.current_term {
            let reply = AppendEntriesReply {
                term: self.state.current_term,
                success: false,
                match_index: self.state.log.last_index(),
            };
            self.cluster.send(from, Message::AppendEntriesReply(reply));
            return;
        }

        if ae.term > self.state.current_term {
            self.state.become_follower(ae.term, Some(ae.leader_id));
        } else {
            // Reset election timer and record leader
            if let Role::Follower { ref mut leader, ref mut election_deadline } = self.state.role {
                *leader = Some(ae.leader_id);
                *election_deadline = random_deadline();
            } else {
                // We were a candidate; step down
                self.state.become_follower(ae.term, Some(ae.leader_id));
            }
        }

        let entry_count = ae.entries.len();
        let success = self.state.log.append_leader_entries(
            ae.prev_log_index,
            ae.prev_log_term,
            ae.entries,
        );

        if success && ae.leader_commit > self.state.commit_index {
            self.state.commit_index = ae.leader_commit.min(self.state.log.last_index());
            let applied = self.state.advance_applied();
            for e in applied {
                let _ = self.apply_tx.send(e);
            }
        }

        self.update_pub_state();

        let reply = AppendEntriesReply {
            term: self.state.current_term,
            success,
            match_index: if success { self.state.log.last_index() } else { 0 },
        };
        self.cluster.send(from, Message::AppendEntriesReply(reply));
    }

    fn on_append_entries_reply(&mut self, from: NodeId, aer: AppendEntriesReply) {
        if aer.term > self.state.current_term {
            self.state.become_follower(aer.term, None);
            self.update_pub_state();
            return;
        }

        // Snapshot values we'll need after releasing the role borrow
        let last_index = self.state.log.last_index();
        let current_term = self.state.current_term;
        let mut should_replicate = false;

        if let Role::Leader { ref mut next_index, ref mut match_index, .. } = self.state.role {
            if aer.success {
                let new_match = aer.match_index;
                let old_match = match_index.get(&from).copied().unwrap_or(0);

                if new_match > old_match {
                    match_index.insert(from, new_match);
                    next_index.insert(from, new_match + 1);
                }

                // Advance commit_index if a new majority point is reached
                let mut mi_vals: Vec<LogIndex> = match_index.values().copied().collect();
                mi_vals.push(last_index); // leader itself has all entries
                mi_vals.sort_unstable();
                let quorum_idx = mi_vals[mi_vals.len() / 2]; // upper half of sorted = majority

                if quorum_idx > self.state.commit_index
                    && self.state.log.term_at(quorum_idx) == Some(current_term)
                {
                    self.state.commit_index = quorum_idx;
                    let applied = self.state.advance_applied();
                    for e in applied {
                        let _ = self.apply_tx.send(e);
                    }
                }
            } else {
                // Peer rejected — back up next_index and retry immediately
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
            let result = FileOpResult::Error("not leader".into());
            self.cluster.send(from, Message::ClientResponse { request_id, result });
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

        // Track this so we can respond when it commits
        let (tx, rx) = oneshot::channel::<FileOpResult>();
        self.state.pending.push((idx, tx));

        // Respond to the originating node when the entry commits
        let cluster = self.cluster.clone();
        tokio::spawn(async move {
            if let Ok(result) = rx.await {
                cluster.send(from, Message::ClientResponse { request_id, result });
            }
        });

        self.replicate_to_all();
    }

    // ── Election ──────────────────────────────────────────────────────────

    fn start_election(&mut self) {
        self.state.current_term += 1;
        let term = self.state.current_term;
        let my_id = self.cluster.local_id;

        let mut votes = HashSet::new();
        votes.insert(my_id);
        self.state.voted_for = Some(my_id);
        self.state.role = Role::Candidate {
            votes,
            election_deadline: random_deadline(),
        };
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

    /// Send AppendEntries (or heartbeat) to all peers in parallel.
    fn replicate_to_all(&self) {
        if let Role::Leader { ref next_index, .. } = self.state.role {
            let peers: Vec<NodeId> = next_index.keys().copied().collect();
            for peer in peers {
                self.send_append_entries_to(peer);
            }
        }
    }

    fn send_append_entries_to(&self, peer: NodeId) {
        let (prev_index, entries) = if let Role::Leader { ref next_index, .. } = self.state.role {
            let ni = *next_index.get(&peer).unwrap_or(&1);
            let prev = ni.saturating_sub(1);
            let entries = self.state.log.entries_from(prev);
            // Cap at MAX_ENTRIES_PER_RPC to limit per-message size
            let entries = if entries.len() > MAX_ENTRIES_PER_RPC {
                entries[..MAX_ENTRIES_PER_RPC].to_vec()
            } else {
                entries
            };
            (prev, entries)
        } else {
            return;
        };

        let prev_term = self.state.log.term_at(prev_index).unwrap_or(0);
        let ae = AppendEntries {
            term: self.state.current_term,
            leader_id: self.cluster.local_id,
            prev_log_index: prev_index,
            prev_log_term: prev_term,
            entries,
            leader_commit: self.state.commit_index,
        };
        self.cluster.send(peer, Message::AppendEntries(ae));
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    fn update_pub_state(&self) {
        let mut ps = self.pub_state.write();
        ps.is_leader = self.state.is_leader();
        ps.leader_id = self.state.leader_id();
        ps.current_term = self.state.current_term;
    }
}
