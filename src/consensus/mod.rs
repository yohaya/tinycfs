pub mod log;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::sync::{mpsc, oneshot};
use tokio::time;
use tracing::{debug, info, warn};

use crate::cluster::message::{
    AppendEntries, AppendEntriesReply, FileOp, FileOpResult, LogEntry, LogIndex, Message, NodeId,
    RequestVote, RequestVoteReply, Term,
};
use crate::cluster::ClusterHandle;
use crate::consensus::log::Log;
use crate::error::{Result, TinyCfsError};

/// Timing constants (inspired by Raft paper recommendations).
const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(50);
const ELECTION_TIMEOUT_MIN: Duration = Duration::from_millis(150);
const ELECTION_TIMEOUT_MAX: Duration = Duration::from_millis(300);

/// A pending client request waiting for its log entry to commit.
struct Pending {
    index: LogIndex,
    tx: oneshot::Sender<FileOpResult>,
}

// ─── Raft role ────────────────────────────────────────────────────────────────

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

// ─── Raft state ───────────────────────────────────────────────────────────────

struct RaftState {
    // Persistent (conceptual — kept in memory for now)
    current_term: Term,
    voted_for: Option<NodeId>,
    log: Log,

    // Volatile
    commit_index: LogIndex,
    last_applied: LogIndex,
    role: Role,

    // Pending client proposals
    pending: Vec<Pending>,

    // Request ID counter
    next_request_id: u64,
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
                election_deadline: random_election_deadline(),
            },
            pending: Vec::new(),
            next_request_id: 1,
        }
    }

    fn become_follower(&mut self, term: Term, leader: Option<NodeId>) {
        self.current_term = term;
        self.voted_for = None;
        self.role = Role::Follower {
            leader,
            election_deadline: random_election_deadline(),
        };
    }

    fn is_leader(&self) -> bool {
        matches!(self.role, Role::Leader { .. })
    }

    fn leader_id(&self) -> Option<NodeId> {
        match &self.role {
            Role::Follower { leader, .. } => *leader,
            Role::Leader { .. } => None, // We are the leader
            Role::Candidate { .. } => None,
        }
    }
}

fn random_election_deadline() -> Instant {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let ms = rng.gen_range(
        ELECTION_TIMEOUT_MIN.as_millis()..=ELECTION_TIMEOUT_MAX.as_millis(),
    ) as u64;
    Instant::now() + Duration::from_millis(ms)
}

// ─── Consensus engine ─────────────────────────────────────────────────────────

/// Shared handle to the Raft consensus engine. Clone-able and Send.
#[derive(Clone)]
pub struct Consensus {
    inner: Arc<ConsensusInner>,
}

struct ConsensusInner {
    state: Mutex<RaftState>,
    /// Committed log entries are sent here so the FUSE state machine can apply them.
    apply_tx: mpsc::UnboundedSender<LogEntry>,
    /// Cluster handle for sending messages.
    cluster: ClusterHandle,
}

impl Consensus {
    /// Create the consensus engine. Returns (Consensus, apply_rx) where apply_rx
    /// delivers committed log entries to the state machine in order.
    pub fn new(
        cluster: ClusterHandle,
    ) -> (Self, mpsc::UnboundedReceiver<LogEntry>) {
        let (apply_tx, apply_rx) = mpsc::unbounded_channel();
        let inner = Arc::new(ConsensusInner {
            state: Mutex::new(RaftState::new()),
            apply_tx,
            cluster,
        });
        (Consensus { inner }, apply_rx)
    }

    /// Start the Raft background ticker.
    pub fn start(&self) {
        let c = self.clone();
        tokio::spawn(async move { c.run_ticker().await });
    }

    /// Submit a write operation. Blocks until committed by a quorum.
    /// Can be called from any thread (including blocking FUSE threads).
    pub async fn propose(&self, op: FileOp) -> Result<()> {
        // If we are the leader, append to log directly.
        // If we are a follower, forward to the leader and wait.
        let (tx, rx) = oneshot::channel();

        {
            let mut st = self.inner.state.lock();
            if st.is_leader() {
                let index = st.log.last_index() + 1;
                let request_id = st.next_request_id;
                st.next_request_id += 1;
                let entry = LogEntry {
                    index,
                    term: st.current_term,
                    request_id,
                    client_node: self.inner.cluster.local_id,
                    op,
                };
                st.log.append(entry);
                st.pending.push(Pending { index, tx });
                // Heartbeat will replicate it immediately
            } else {
                // Forward to leader
                match st.leader_id() {
                    Some(leader_id) => {
                        let request_id = st.next_request_id;
                        st.next_request_id += 1;
                        drop(st);
                        let msg = Message::ClientRequest { request_id, op };
                        self.inner.cluster.send(leader_id, msg);
                        // Store pending indexed by request_id so we can match response
                        // For simplicity, we use a oneshot and match by request_id
                        // (see handle_client_response)
                        drop(tx); // The forwarding path returns error for now
                        return Err(TinyCfsError::NotLeader { leader_hint: Some(leader_id) });
                    }
                    None => {
                        return Err(TinyCfsError::NoQuorum);
                    }
                }
            }
        }

        // Trigger immediate replication
        self.replicate_to_all();

        // Wait for commit
        match time::timeout(Duration::from_secs(5), rx).await {
            Ok(Ok(FileOpResult::Ok)) => Ok(()),
            Ok(Ok(FileOpResult::Error(e))) => Err(TinyCfsError::Cluster(e)),
            Ok(Err(_)) => Err(TinyCfsError::Cluster("proposal channel dropped".into())),
            Err(_) => Err(TinyCfsError::Timeout),
        }
    }

    // ── Message processing ────────────────────────────────────────────────

    /// Process a message from a peer.
    pub fn handle_message(&self, from: NodeId, msg: Message) {
        match msg {
            Message::RequestVote(rv) => self.handle_request_vote(from, rv),
            Message::RequestVoteReply(rvr) => self.handle_vote_reply(from, rvr),
            Message::AppendEntries(ae) => self.handle_append_entries(from, ae),
            Message::AppendEntriesReply(aer) => self.handle_append_entries_reply(from, aer),
            Message::ClientRequest { request_id, op } => {
                self.handle_forwarded_request(from, request_id, op)
            }
            Message::ClientResponse { request_id, result } => {
                self.handle_client_response(request_id, result)
            }
            _ => {}
        }
    }

    fn handle_request_vote(&self, from: NodeId, rv: RequestVote) {
        let mut st = self.inner.state.lock();

        if rv.term > st.current_term {
            st.become_follower(rv.term, None);
        }

        let grant = rv.term >= st.current_term
            && (st.voted_for.is_none() || st.voted_for == Some(rv.candidate_id))
            && (rv.last_log_term > st.log.last_term()
                || (rv.last_log_term == st.log.last_term()
                    && rv.last_log_index >= st.log.last_index()));

        if grant {
            st.voted_for = Some(rv.candidate_id);
            // Reset election timer so we don't start our own election
            if let Role::Follower { election_deadline, .. } = &mut st.role {
                *election_deadline = random_election_deadline();
            }
            debug!(
                "Granting vote to {:x} for term {}",
                rv.candidate_id, rv.term
            );
        }

        let reply = Message::RequestVoteReply(RequestVoteReply {
            term: st.current_term,
            vote_granted: grant,
        });
        drop(st);
        self.inner.cluster.send(from, reply);
    }

    fn handle_vote_reply(&self, from: NodeId, rvr: RequestVoteReply) {
        let mut st = self.inner.state.lock();

        if rvr.term > st.current_term {
            st.become_follower(rvr.term, None);
            return;
        }

        if let Role::Candidate { ref mut votes, .. } = st.role {
            if rvr.vote_granted {
                votes.insert(from);
                let vote_count = votes.len() + 1; // +1 for self
                let peers = self.inner.cluster.peer_count();
                let quorum = (peers + 2) / 2; // majority of total nodes

                if vote_count >= quorum {
                    info!("Elected as leader for term {}", st.current_term);
                    // Transition to leader
                    let last_index = st.log.last_index();
                    let peer_ids = self.inner.cluster.peer_ids();
                    let next_index: HashMap<NodeId, LogIndex> =
                        peer_ids.iter().map(|&id| (id, last_index + 1)).collect();
                    let match_index: HashMap<NodeId, LogIndex> =
                        peer_ids.iter().map(|&id| (id, 0)).collect();
                    st.role = Role::Leader {
                        next_index,
                        match_index,
                        last_heartbeat: Instant::now(),
                    };
                }
            }
        }
    }

    fn handle_append_entries(&self, from: NodeId, ae: AppendEntries) {
        let mut st = self.inner.state.lock();

        if ae.term < st.current_term {
            let reply = Message::AppendEntriesReply(AppendEntriesReply {
                term: st.current_term,
                success: false,
                match_index: st.log.last_index(),
            });
            drop(st);
            self.inner.cluster.send(from, reply);
            return;
        }

        if ae.term > st.current_term {
            st.become_follower(ae.term, Some(ae.leader_id));
        } else if let Role::Follower { leader, election_deadline, .. } = &mut st.role {
            *leader = Some(ae.leader_id);
            *election_deadline = random_election_deadline();
        } else {
            // We were a candidate and received AE from a valid leader
            st.become_follower(ae.term, Some(ae.leader_id));
        }

        let success = st.log.append_leader_entries(
            ae.prev_log_index,
            ae.prev_log_term,
            ae.entries,
        );

        let match_idx = if success { st.log.last_index() } else { 0 };

        if success && ae.leader_commit > st.commit_index {
            st.commit_index = ae.leader_commit.min(st.log.last_index());
            self.advance_applied(&mut st);
        }

        let reply = Message::AppendEntriesReply(AppendEntriesReply {
            term: st.current_term,
            success,
            match_index: match_idx,
        });
        drop(st);
        self.inner.cluster.send(from, reply);
    }

    fn handle_append_entries_reply(&self, from: NodeId, aer: AppendEntriesReply) {
        let mut st = self.inner.state.lock();

        if aer.term > st.current_term {
            st.become_follower(aer.term, None);
            return;
        }

        // Snapshot log info before entering mutable role borrow
        let last_log_index = st.log.last_index();
        let current_term = st.current_term;

        if let Role::Leader { ref mut next_index, ref mut match_index, .. } = st.role {
            if aer.success {
                match_index.insert(from, aer.match_index);
                next_index.insert(from, aer.match_index + 1);

                // Clone match_index to release partial borrow before calling helpers
                let match_index_snapshot: HashMap<NodeId, LogIndex> = match_index.clone();
                drop(st); // release lock before re-acquiring in quorum_match_index

                // Re-acquire to update commit_index
                let mut st = self.inner.state.lock();
                let new_commit = self.quorum_match_index(&match_index_snapshot, last_log_index);
                if new_commit > st.commit_index
                    && st.log.term_at(new_commit) == Some(current_term)
                {
                    st.commit_index = new_commit;
                    self.advance_applied(&mut st);
                }
            } else {
                // Follower rejected — back up next_index
                let ni = next_index.entry(from).or_insert(1);
                *ni = (*ni).saturating_sub(1).max(1);
                if aer.match_index + 1 < *ni {
                    *ni = aer.match_index + 1;
                }
            }
        }
    }

    fn handle_forwarded_request(&self, from: NodeId, request_id: u64, op: FileOp) {
        let mut st = self.inner.state.lock();
        if !st.is_leader() {
            let result = FileOpResult::Error("not leader".into());
            let reply = Message::ClientResponse { request_id, result };
            drop(st);
            self.inner.cluster.send(from, reply);
            return;
        }

        let index = st.log.last_index() + 1;
        let entry = LogEntry {
            index,
            term: st.current_term,
            request_id,
            client_node: from,
            op,
        };
        st.log.append(entry);

        // We need to send the response when this entry commits.
        // Store a callback keyed by log index.
        // For forwarded requests, we respond via the network.
        // We use a local oneshot and a spawned task that awaits it.
        let (tx, rx) = oneshot::channel::<FileOpResult>();
        st.pending.push(Pending { index, tx });
        drop(st);

        let cluster = self.inner.cluster.clone();
        tokio::spawn(async move {
            if let Ok(result) = rx.await {
                let msg = Message::ClientResponse { request_id, result };
                cluster.send(from, msg);
            }
        });

        self.replicate_to_all();
    }

    fn handle_client_response(&self, _request_id: u64, _result: FileOpResult) {
        // Responses to forwarded requests are handled by dedicated tasks.
        // This path is unused in the current design but kept for extensibility.
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    /// Find the highest log index replicated on a quorum of nodes.
    fn quorum_match_index(
        &self,
        match_index: &HashMap<NodeId, LogIndex>,
        leader_last: LogIndex,
    ) -> LogIndex {
        let mut indices: Vec<LogIndex> = match_index.values().copied().collect();
        indices.push(leader_last); // leader has all entries
        indices.sort_unstable();
        let quorum = (indices.len() + 1) / 2;
        indices[indices.len() - quorum]
    }

    /// Advance last_applied up to commit_index and fire pending callbacks.
    fn advance_applied(&self, st: &mut RaftState) {
        while st.last_applied < st.commit_index {
            st.last_applied += 1;
            let entry = st.log.get(st.last_applied).cloned();
            if let Some(entry) = entry {
                let _ = self.inner.apply_tx.send(entry.clone());

                // Resolve any pending proposal for this index (drain to consume tx)
                let mut remaining = Vec::new();
                for p in st.pending.drain(..) {
                    if p.index == entry.index {
                        let _ = p.tx.send(FileOpResult::Ok);
                    } else {
                        remaining.push(p);
                    }
                }
                st.pending = remaining;
            }
        }
    }

    /// Send AppendEntries (or heartbeat) to all peers.
    fn replicate_to_all(&self) {
        let st = self.inner.state.lock();
        if let Role::Leader { ref next_index, .. } = st.role {
            let peers: Vec<NodeId> = next_index.keys().copied().collect();
            for peer_id in peers {
                self.send_append_entries_to(&st, peer_id);
            }
        }
    }

    fn send_append_entries_to(&self, st: &RaftState, peer_id: NodeId) {
        let ni = match &st.role {
            Role::Leader { next_index, .. } => *next_index.get(&peer_id).unwrap_or(&1),
            _ => return,
        };

        let prev_index = ni.saturating_sub(1);
        let prev_term = st.log.term_at(prev_index).unwrap_or(0);
        let entries = st.log.entries_from(prev_index);

        let ae = AppendEntries {
            term: st.current_term,
            leader_id: self.inner.cluster.local_id,
            prev_log_index: prev_index,
            prev_log_term: prev_term,
            entries,
            leader_commit: st.commit_index,
        };
        self.inner.cluster.send(peer_id, Message::AppendEntries(ae));
    }

    // ── Ticker ────────────────────────────────────────────────────────────

    async fn run_ticker(&self) {
        let mut interval = time::interval(Duration::from_millis(10));
        loop {
            interval.tick().await;
            self.tick();
        }
    }

    fn tick(&self) {
        let now = Instant::now();
        let mut st = self.inner.state.lock();

        match &st.role.clone() {
            Role::Follower { election_deadline, .. } => {
                if now >= *election_deadline {
                    self.start_election(&mut st);
                }
            }
            Role::Candidate { election_deadline, .. } => {
                if now >= *election_deadline {
                    // Restart election
                    self.start_election(&mut st);
                }
            }
            Role::Leader { last_heartbeat, .. } => {
                if now.duration_since(*last_heartbeat) >= HEARTBEAT_INTERVAL {
                    if let Role::Leader { ref mut last_heartbeat, ref next_index, .. } = st.role {
                        *last_heartbeat = now;
                        let peers: Vec<NodeId> = next_index.keys().copied().collect();
                        drop(st); // unlock before sending
                        let st2 = self.inner.state.lock();
                        for peer_id in peers {
                            self.send_append_entries_to(&st2, peer_id);
                        }
                        return;
                    }
                }
            }
        }
    }

    fn start_election(&self, st: &mut RaftState) {
        st.current_term += 1;
        let term = st.current_term;
        let my_id = self.inner.cluster.local_id;
        let last_log_index = st.log.last_index();
        let last_log_term = st.log.last_term();

        let mut votes = HashSet::new();
        votes.insert(my_id); // vote for self

        st.voted_for = Some(my_id);
        st.role = Role::Candidate {
            votes,
            election_deadline: random_election_deadline(),
        };

        info!("Starting election for term {}", term);

        let rv = RequestVote {
            term,
            candidate_id: my_id,
            last_log_index,
            last_log_term,
        };
        drop(st);
        self.inner.cluster.broadcast(Message::RequestVote(rv));
    }
}
