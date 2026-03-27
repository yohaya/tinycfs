pub mod log;

/// High-throughput Raft consensus engine — 500-node capable.
///
/// Design:
/// 1. **Actor model** — single tokio task owns all mutable Raft state.
/// 2. **Proposal batching** — drains up to MAX_ENTRIES_PER_RPC ops per tick.
/// 3. **Parallel per-peer replication** — each peer gets an independent write path.
/// 4. **Correct quorum** — uses total_voting_nodes from config, not connected peers,
///    so partitioned nodes cannot form a false quorum.
/// 5. **SQLite persistence** — term/vote/log persisted before ACK; snapshotted
///    every SNAPSHOT_EVERY applied entries for log compaction.
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

const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(5);
/// Election timeouts are set wide enough that no follower fires before the
/// newly elected leader's first heartbeat arrives.  Worst-case path:
///   ELECTION_TIMEOUT_MAX (winning node waits this long)
///   + vote RTT (~20 ms for 10 ms one-way delay)
///   + heartbeat one-way delay (~10 ms)
///   + forwarding-task scheduling jitter (~10 ms)
/// ≈ 340 ms.  Min = 2× that to be safe.
const ELECTION_TIMEOUT_MIN: Duration = Duration::from_millis(700);
const ELECTION_TIMEOUT_MAX: Duration = Duration::from_millis(1400);
/// Max entries per AppendEntries RPC — larger batches amortise per-RPC cost at
/// high node counts.
const MAX_ENTRIES_PER_RPC: usize = 1024;
const PROPOSAL_QUEUE_DEPTH: usize = 32_768;
/// Default snapshot interval when none is provided via config.
const SNAPSHOT_EVERY_DEFAULT: LogIndex = 10_000;
/// How long propose() keeps retrying when no leader is available.
const PROPOSE_TIMEOUT: Duration = Duration::from_secs(30);

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
    pending: Vec<(LogIndex, oneshot::Sender<FileOpResult>)>,
    next_req_id: u64,
    /// Next snapshot threshold.
    next_snapshot_at: LogIndex,
}

impl RaftState {
    fn new(snapshot_every: LogIndex) -> Self {
        RaftState {
            current_term: 0,
            voted_for: None,
            log: Log::new(),
            commit_index: 0,
            last_applied: 0,
            role: Role::Follower { leader: None, election_deadline: random_deadline() },
            pending: Vec::new(),
            next_req_id: 1,
            next_snapshot_at: snapshot_every,
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
        self.role = Role::Follower { leader, election_deadline: random_deadline() };
    }
}

fn random_deadline() -> Instant {
    use rand::Rng;
    let ms = rand::thread_rng().gen_range(
        ELECTION_TIMEOUT_MIN.as_millis() as u64..=ELECTION_TIMEOUT_MAX.as_millis() as u64,
    );
    Instant::now() + Duration::from_millis(ms)
}

// ─── Public handle ────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct Consensus {
    proposal_tx: mpsc::Sender<Proposal>,
    pub_state: Arc<RwLock<PubState>>,
    local_id: NodeId,
}

#[derive(Clone, Default)]
pub struct PubState {
    pub is_leader: bool,
    pub leader_id: Option<NodeId>,
    pub current_term: Term,
    /// True when this node is the leader or knows a live leader.
    pub has_quorum: bool,
}

impl Consensus {
    /// Build the consensus engine.
    ///
    /// The engine applies committed log entries directly to `store`, so
    /// no external apply loop is needed in main.rs.
    ///
    /// Returns:
    /// - `Consensus` — clone-able handle for FUSE / callers.
    /// - `mpsc::Sender<Envelope>` — pipe incoming cluster messages here.
    pub fn new(
        cluster: ClusterHandle,
        db: Option<RaftDb>,
        store: Arc<RwLock<FileStore>>,
        snapshot_every: u64,
    ) -> (Self, mpsc::Sender<Envelope>) {
        let (proposal_tx, proposal_rx) = mpsc::channel(PROPOSAL_QUEUE_DEPTH);
        let (msg_tx, msg_rx) = mpsc::channel(16_384);
        let pub_state = Arc::new(RwLock::new(PubState::default()));

        let local_id = cluster.local_id;

        // ── Load persisted state ──────────────────────────────────────────
        let state = if let Some(ref db) = db {
            load_state_from_db(db, &store, snapshot_every)
        } else {
            RaftState::new(snapshot_every)
        };

        let engine = RaftEngine {
            state,
            cluster,
            pub_state: pub_state.clone(),
            db,
            store,
            snapshot_every,
            client_pending: HashMap::new(),
        };

        tokio::spawn(engine.run(proposal_rx, msg_rx));

        let handle = Consensus { proposal_tx, pub_state, local_id };
        (handle, msg_tx)
    }

    /// Submit a write operation.
    ///
    /// Retries internally for up to 30 seconds while waiting for a leader to be
    /// elected (leader loss, partition heal).  Returns `Timeout` (→ EROFS) only
    /// after the 30 s window is exhausted.
    pub async fn propose(&self, op: FileOp) -> Result<()> {
        let deadline = Instant::now() + PROPOSE_TIMEOUT;

        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(TinyCfsError::Timeout);
            }

            let (done_tx, done_rx) = oneshot::channel();
            let proposal = Proposal { op: op.clone(), done: done_tx };

            match self.proposal_tx.try_send(proposal) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => {
                    // Back-pressure: queue full — wait a bit and retry.
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
                Ok(Err(_)) => {
                    // done_tx dropped (leader stepped down) — retry.
                    time::sleep(Duration::from_millis(100).min(remaining)).await;
                    continue;
                }
                Err(_timeout) => return Err(TinyCfsError::Timeout),
            }
        }
    }

    pub fn is_leader(&self) -> bool {
        self.pub_state.read().is_leader
    }

    pub fn leader_id(&self) -> Option<NodeId> {
        self.pub_state.read().leader_id
    }

    pub fn has_quorum(&self) -> bool {
        self.pub_state.read().has_quorum
    }
}

// ─── State loader ─────────────────────────────────────────────────────────────

fn load_state_from_db(db: &RaftDb, store: &Arc<RwLock<FileStore>>, snapshot_every: u64) -> RaftState {
    let (term, voted_for) = db.load_meta().unwrap_or((0, None));

    let (snapshot_index, snapshot_term) = match db.load_snapshot() {
        Ok(Some((si, st, data))) => {
            match bincode::deserialize::<FileStore>(&data) {
                Ok(snap_store) => {
                    *store.write() = snap_store;
                    info!("Loaded snapshot at index {}", si);
                }
                Err(e) => warn!("Failed to deserialize snapshot: {}", e),
            }
            (si, st)
        }
        Ok(None) => (0, 0),
        Err(e) => {
            warn!("Failed to load snapshot: {}", e);
            (0, 0)
        }
    };

    let log_entries = db.load_entries_after(snapshot_index).unwrap_or_default();
    let mut log = Log::new();
    log.set_snapshot_base(snapshot_index, snapshot_term);
    for entry in log_entries {
        log.append(entry);
    }
    info!(
        "Loaded term={}, voted_for={:?}, log entries after snapshot {}={} entries",
        term, voted_for, snapshot_index, log.len()
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
        role: Role::Follower { leader: None, election_deadline: random_deadline() },
        pending: Vec::new(),
        next_req_id: 1,
        next_snapshot_at,
    }
}

// ─── Raft actor ───────────────────────────────────────────────────────────────

struct RaftEngine {
    state: RaftState,
    cluster: ClusterHandle,
    pub_state: Arc<RwLock<PubState>>,
    db: Option<RaftDb>,
    store: Arc<RwLock<FileStore>>,
    /// Snapshot interval from config (entries between snapshots).
    snapshot_every: u64,
    /// Forwarded-request callbacks: request_id → oneshot reply to local proposer.
    client_pending: HashMap<u64, oneshot::Sender<FileOpResult>>,
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
                Some(env) = msg_rx.recv() => {
                    self.handle_message(env.from, env.msg);
                }
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
            Role::Follower { election_deadline, leader } => {
                if now >= election_deadline && !self.cluster.is_observer {
                    self.start_election();
                } else {
                    // Forward proposals to the known leader, or drop them so
                    // propose() retries (it treats a dropped done_tx as signal
                    // to retry).
                    loop {
                        match proposal_rx.try_recv() {
                            Ok(p) => {
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
                                // If no leader yet, drop done_tx → propose() retries.
                            }
                            Err(_) => break,
                        }
                    }
                }
            }
            Role::Candidate { election_deadline, .. } => {
                if now >= election_deadline {
                    self.start_election();
                }
                // While a candidate, drain and drop proposals so propose() retries.
                while proposal_rx.try_recv().is_ok() {}
            }
            Role::Leader { last_heartbeat, last_replicated_index, .. } => {
                let mut batch: Vec<Proposal> = Vec::new();
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
                // Replicate if the HB interval elapsed OR if new entries exist
                // (e.g. from forwarded ClientRequest messages handled between ticks).
                let new_last = self.state.log.last_index();
                let has_new_entries = new_last > last_replicated_index;
                if now.duration_since(last_heartbeat) >= HEARTBEAT_INTERVAL || has_new_entries {
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
            // Persist before appending to in-memory log.
            if let Some(db) = &self.db {
                if let Err(e) = db.append_entry(&entry) {
                    warn!("DB: failed to persist log entry {}: {}", idx, e);
                }
            }
            self.state.log.append(entry);
            self.state.pending.push((idx, proposal.done));
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
            Message::ClientResponse { request_id, result } => {
                if let Some(tx) = self.client_pending.remove(&request_id) {
                    let _ = tx.send(result);
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
                        last_heartbeat: Instant::now() - HEARTBEAT_INTERVAL,
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
                    *election_deadline = random_deadline();
                }
                _ => {
                    self.step_down(ae.term, Some(ae.leader_id));
                    self.persist_meta();
                }
            }
        }

        let success = self.state.log.append_leader_entries(
            ae.prev_log_index,
            ae.prev_log_term,
            ae.entries.clone(),
        );

        // Persist new entries.
        if success && !ae.entries.is_empty() {
            if let Some(db) = &self.db {
                for entry in &ae.entries {
                    if let Err(e) = db.append_entry(entry) {
                        warn!("DB: persist follower entry {}: {}", entry.index, e);
                    }
                }
            }
        }

        if success && ae.leader_commit > self.state.commit_index {
            self.state.commit_index =
                ae.leader_commit.min(self.state.log.last_index());
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
        if let Some(db) = &self.db {
            let _ = db.append_entry(&entry);
        }
        self.state.log.append(entry);

        let (tx, rx) = oneshot::channel::<FileOpResult>();
        self.state.pending.push((idx, tx));

        let cluster = self.cluster.clone();
        tokio::spawn(async move {
            match rx.await {
                Ok(result) => {
                    cluster.send(from, Message::ClientResponse { request_id, result });
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

        self.state.current_term += 1;
        let term = self.state.current_term;
        let my_id = self.cluster.local_id;
        self.state.voted_for = Some(my_id);
        self.persist_meta();

        let mut votes = HashSet::new();
        votes.insert(my_id);
        self.state.role =
            Role::Candidate { votes, election_deadline: random_deadline() };
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
                    *election_deadline = random_deadline();
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

        // Apply the snapshot to the state machine.
        match bincode::deserialize::<crate::fs::store::FileStore>(&is.data) {
            Ok(snap_store) => {
                *self.store.write() = snap_store;
            }
            Err(e) => {
                warn!("InstallSnapshot deserialize failed: {}", e);
                return;
            }
        }

        // Compact the log to the snapshot boundary and reset applied state.
        self.state.log.compact_before(is.snapshot_index, is.snapshot_term);
        self.state.commit_index = is.snapshot_index;
        self.state.last_applied = is.snapshot_index;
        self.state.next_snapshot_at = is.snapshot_index + self.snapshot_every;

        // Persist snapshot + compact the on-disk log.
        if let Some(db) = &self.db {
            if let Err(e) = db.save_snapshot(is.snapshot_index, is.snapshot_term, &is.data) {
                warn!("InstallSnapshot: DB save failed: {}", e);
            } else if let Err(e) = db.compact_log_before(is.snapshot_index) {
                warn!("InstallSnapshot: DB compact failed: {}", e);
            }
        }

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
    /// and fire callbacks to waiting proposers.
    fn advance_applied(&mut self) {
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
                    Err(e) => FileOpResult::Error(e.to_string()),
                };

                if let Err(ref e) = result {
                    // Log warnings for unexpected errors (not LockContended or AlreadyExists).
                    match e {
                        TinyCfsError::LockContended(_) | TinyCfsError::AlreadyExists(_) => {}
                        other => warn!("State machine at index {}: {}", idx, other),
                    }
                }

                // Resolve pending proposals at this index.
                let mut i = 0;
                while i < self.state.pending.len() {
                    if self.state.pending[i].0 == idx {
                        let (_, tx) = self.state.pending.remove(i);
                        let _ = tx.send(op_result.clone());
                    } else {
                        i += 1;
                    }
                }
            }
        }

        // Snapshot check.
        if self.state.last_applied >= self.state.next_snapshot_at {
            self.take_snapshot();
        }
    }

    fn take_snapshot(&mut self) {
        let snap_index = self.state.last_applied;
        let snap_term = self.state.log.term_at(snap_index).unwrap_or(0);

        let data = {
            let st = self.store.read();
            match bincode::serialize(&*st) {
                Ok(d) => d,
                Err(e) => {
                    warn!("Snapshot serialization failed: {}", e);
                    return;
                }
            }
        };

        if let Some(db) = &self.db {
            if let Err(e) = db.save_snapshot(snap_index, snap_term, &data) {
                warn!("Snapshot save failed: {}", e);
                return;
            }
            if let Err(e) = db.compact_log_before(snap_index) {
                warn!("Log compaction failed: {}", e);
            }
        }

        self.state.log.compact_before(snap_index, snap_term);
        self.state.next_snapshot_at = snap_index + self.snapshot_every;
        info!("Snapshot at index {} (term {}), log compacted", snap_index, snap_term);
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    fn persist_meta(&self) {
        if let Some(db) = &self.db {
            if let Err(e) = db.save_term(self.state.current_term, self.state.voted_for) {
                warn!("DB: failed to persist term: {}", e);
            }
        }
    }

    fn update_pub_state(&self) {
        let mut ps = self.pub_state.write();
        ps.is_leader = self.state.is_leader();
        ps.leader_id = self.state.leader_id();
        ps.current_term = self.state.current_term;
        ps.has_quorum = ps.is_leader
            || matches!(&self.state.role, Role::Follower { leader: Some(_), .. });
    }
}
