//! Totem Single Ring Protocol (SRTP) consensus engine.
//!
//! Faithfully implements the Totem protocol as designed in Corosync:
//! token-based total-order atomic multicast over a logical ring.
//!
//! Key protocol properties (per Amir, Cristian et al. 1995):
//!
//! - **Total order** — all nodes deliver messages in the same sequence.
//! - **No fixed leader** — the token rotates, giving each node a periodic
//!   send window (up to `TOKEN_SEND_LIMIT` messages per hold).
//! - **Fault tolerance** — quorum required; token timeout triggers ring reform.
//! - **Retransmission** — missing messages requested via the token's `rtr_list`.
//! - **Flow control** — token's `backlog` field allows adaptive pacing.
//!
//! # Ring lifecycle
//!
//! ```text
//! Startup:  broadcast TotemJoin → collect acks → coordinator sends TotemSync + token
//! Steady:   token rotates node0→node1→…→nodeN→node0, each holder multicasts up to
//!           TOKEN_SEND_LIMIT messages, updates ARU, builds rtr_list, passes token
//! Failure:  token timeout → start_join → finalize_join → new ring_id → sync + token
//! ```

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use tokio::sync::{mpsc, oneshot};
use tokio::time;
use tracing::{debug, info, warn};

use crate::cluster::message::{
    FileOp, FileOpResult, Message, NodeId, Seq, TotemJoin, TotemJoinAck, TotemMcast, TotemSync,
    TotemToken,
};
use crate::cluster::{ClusterHandle, Envelope};
use crate::consensus::Proposal;
use crate::error::{Result, TinyCfsError};
use crate::fs::store::FileStore;
use crate::persistence::RaftDb;

// ── Tunables ──────────────────────────────────────────────────────────────────

/// How long to wait for the token before declaring it lost.
const TOKEN_TIMEOUT: Duration = Duration::from_millis(500);
/// Duration to collect Join/JoinAck messages when forming a new ring.
const JOIN_TIMEOUT: Duration = Duration::from_millis(1000);
/// After startup, wait this long before expecting the ring to be operational.
const STARTUP_GRACE: Duration = Duration::from_millis(3000);
/// Max messages a node may send per token hold (Totem flow control ceiling).
const TOKEN_SEND_LIMIT: u32 = 64;
/// Max rtr_list entries per token (cap to avoid huge token messages).
const MAX_RTR_LIST: usize = 64;
/// How long propose() retries while waiting for ring quorum.
const PROPOSE_TIMEOUT: Duration = Duration::from_secs(30);
const PROPOSAL_QUEUE_DEPTH: usize = 32_768;

// ── Public state visible to the FUSE layer ────────────────────────────────────

#[derive(Clone, Default)]
pub struct TotemPubState {
    /// True when the ring has a quorum of voting nodes and is in steady state.
    pub has_quorum: bool,
    /// Number of nodes in the current ring.
    pub ring_size: usize,
    /// Current ring identifier (monotonically increasing).
    pub ring_id: u64,
    /// Whether this node currently holds the token.
    pub has_token: bool,
}

// ── Public handle (clone-able, used by FUSE / callers) ────────────────────────

#[derive(Clone)]
pub struct TotemHandle {
    pub proposal_tx: mpsc::Sender<Proposal>,
    pub pub_state: Arc<RwLock<TotemPubState>>,
}

impl TotemHandle {
    /// Start the Totem engine and return a handle + message injection channel.
    pub fn start(
        cluster: ClusterHandle,
        db: Option<RaftDb>,
        store: Arc<RwLock<FileStore>>,
    ) -> (Self, mpsc::Sender<Envelope>) {
        let (proposal_tx, proposal_rx) = mpsc::channel(PROPOSAL_QUEUE_DEPTH);
        let (msg_tx, msg_rx) = mpsc::channel(16_384);
        let pub_state = Arc::new(RwLock::new(TotemPubState::default()));
        let total_voting = cluster.total_voting_nodes;

        let engine =
            TotemEngine::new(cluster, db, store, pub_state.clone(), total_voting);
        tokio::spawn(engine.run(proposal_rx, msg_rx));

        (TotemHandle { proposal_tx, pub_state }, msg_tx)
    }

    pub fn has_quorum(&self) -> bool {
        self.pub_state.read().has_quorum
    }

    /// In Totem there is no fixed leader; any node with quorum can accept writes.
    /// Return `true` when the ring is operational so that the sim's "leader?"
    /// readiness check works correctly.
    pub fn is_leader(&self) -> bool {
        self.pub_state.read().has_quorum
    }

    pub fn leader_id(&self) -> Option<NodeId> {
        None
    }
}

// ── Internal engine ───────────────────────────────────────────────────────────

pub struct TotemEngine {
    // ── Ring state
    ring: Vec<NodeId>,
    ring_id: u64,
    my_pos: usize,

    // ── Sequence tracking
    /// Highest sequence number ever assigned (by me or via the token).
    mcast_seq: Seq,
    /// Highest sequence number delivered to the state machine.
    deliver_seq: Seq,
    /// All-received-up-to: highest seq such that all seqs ≤ this are in held_msgs.
    local_aru: Seq,

    // ── Message store
    /// Received messages awaiting in-order delivery.
    held_msgs: BTreeMap<Seq, TotemMcast>,
    /// Messages I sent, kept for retransmission.
    sent_msgs: BTreeMap<Seq, TotemMcast>,

    // ── Token state
    has_token: bool,
    last_token_at: Instant,
    startup_grace_until: Instant,

    // ── Callbacks: seq → oneshot
    seq_callbacks: HashMap<Seq, oneshot::Sender<FileOpResult>>,

    // ── Proposal queue
    send_queue: VecDeque<Proposal>,
    next_request_id: u64,

    // ── Join / ring-reform state
    join_ring_id: u64,
    /// Nodes that have participated in the current join phase.
    join_responders: HashMap<NodeId, ()>,
    join_deadline: Option<Instant>,
    /// Exponential backoff for repeated failed joins (cluster down).
    join_retry_backoff: Duration,

    // ── Infrastructure
    cluster: ClusterHandle,
    db: Option<RaftDb>,
    store: Arc<RwLock<FileStore>>,
    pub_state: Arc<RwLock<TotemPubState>>,
    total_voting_nodes: usize,
}

impl TotemEngine {
    pub fn new(
        cluster: ClusterHandle,
        db: Option<RaftDb>,
        store: Arc<RwLock<FileStore>>,
        pub_state: Arc<RwLock<TotemPubState>>,
        total_voting_nodes: usize,
    ) -> Self {
        let my_id = cluster.local_id;
        TotemEngine {
            ring: vec![my_id],
            ring_id: 0,
            my_pos: 0,
            mcast_seq: 0,
            deliver_seq: 0,
            local_aru: 0,
            held_msgs: BTreeMap::new(),
            sent_msgs: BTreeMap::new(),
            has_token: false,
            last_token_at: Instant::now(),
            startup_grace_until: Instant::now() + STARTUP_GRACE,
            seq_callbacks: HashMap::new(),
            send_queue: VecDeque::new(),
            next_request_id: 1,
            join_ring_id: 0,
            join_responders: HashMap::new(),
            join_deadline: None,
            join_retry_backoff: JOIN_TIMEOUT,
            cluster,
            db,
            store,
            pub_state,
            total_voting_nodes,
        }
    }

    pub async fn run(
        mut self,
        mut proposal_rx: mpsc::Receiver<Proposal>,
        mut msg_rx: mpsc::Receiver<Envelope>,
    ) {
        let mut ticker = time::interval(Duration::from_millis(10));
        ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                biased;
                Some(env) = msg_rx.recv() => {
                    self.handle_message(env.from, env.msg);
                }
                _ = ticker.tick() => {
                    while let Ok(p) = proposal_rx.try_recv() {
                        self.send_queue.push_back(p);
                    }
                    self.tick();
                }
            }
        }
    }

    fn handle_message(&mut self, from: NodeId, msg: Message) {
        match msg {
            Message::TotemToken(t) => self.on_token(from, t),
            Message::TotemMcast(m) => self.on_mcast(m),
            Message::TotemJoin(j) => self.on_join(from, j),
            Message::TotemJoinAck(a) => self.on_join_ack(from, a),
            Message::TotemSync(s) => self.on_sync(from, s),
            _ => {}
        }
    }

    // ── Periodic tick ─────────────────────────────────────────────────────────

    fn tick(&mut self) {
        let now = Instant::now();

        // While in the join phase, wait for the deadline.
        if let Some(deadline) = self.join_deadline {
            if now >= deadline {
                self.finalize_join(now);
            }
            return;
        }

        // Start initial join once TCP peers are connected (avoids sending to no one).
        if self.ring_id == 0 && !self.cluster.peer_ids().is_empty() {
            self.start_join(now);
            return;
        }

        // Check for token timeout (only after startup grace period).
        if !self.has_token && now >= self.startup_grace_until {
            if now.duration_since(self.last_token_at) > TOKEN_TIMEOUT {
                info!("Totem: token timeout — starting ring reform");
                self.start_join(now);
                return;
            }
        }

        // Single-node ring: "pass" the token back to ourselves on each tick.
        if self.has_token && self.ring.len() == 1 {
            let aru = self.local_aru;
            let seq = self.mcast_seq;
            self.process_token_hold(aru, seq, self.local_aru, vec![]);
        }

        self.update_pub_state();
    }

    // ── Join / ring-reform ────────────────────────────────────────────────────

    fn start_join(&mut self, now: Instant) {
        self.join_retry_backoff = JOIN_TIMEOUT; // reset on organic ring reform
        let new_ring_id = self.join_ring_id.max(self.ring_id) + 1;
        self.join_ring_id = new_ring_id;
        self.join_responders.clear();
        // Count self as a responder.
        self.join_responders.insert(self.cluster.local_id, ());
        self.join_deadline = Some(now + JOIN_TIMEOUT);
        self.has_token = false;

        let members = self.current_members();
        debug!(
            "Totem: broadcasting Join ring_id={} members={:?}",
            new_ring_id, members
        );
        self.cluster.broadcast(Message::TotemJoin(TotemJoin {
            ring_id: new_ring_id,
            members,
            sender: self.cluster.local_id,
        }));
    }

    fn current_members(&self) -> Vec<NodeId> {
        let mut m: Vec<NodeId> = self.cluster.peer_ids();
        m.push(self.cluster.local_id);
        m.sort_unstable();
        m
    }

    fn on_join(&mut self, from: NodeId, join: TotemJoin) {
        if join.ring_id < self.join_ring_id {
            return; // stale
        }
        if join.ring_id > self.ring_id {
            // Higher ring proposed — restart our own join to catch up.
            if self.join_deadline.is_none() || join.ring_id > self.join_ring_id {
                self.join_ring_id = join.ring_id;
                self.join_responders.clear();
                self.join_responders.insert(self.cluster.local_id, ());
                self.join_deadline = Some(Instant::now() + JOIN_TIMEOUT);
                self.has_token = false;
                // Broadcast our own Join so all ring members know we're participating.
                let members = self.current_members();
                debug!(
                    "Totem: adopting ring_id={}, re-broadcasting Join members={:?}",
                    join.ring_id, members
                );
                self.cluster.broadcast(Message::TotemJoin(TotemJoin {
                    ring_id: join.ring_id,
                    members,
                    sender: self.cluster.local_id,
                }));
            }
        }
        if join.ring_id == self.join_ring_id {
            self.join_responders.insert(from, ());
            // Ack back with our own member list.
            self.cluster.send(
                from,
                Message::TotemJoinAck(TotemJoinAck {
                    ring_id: join.ring_id,
                    members: self.current_members(),
                    sender: self.cluster.local_id,
                }),
            );
        }
    }

    fn on_join_ack(&mut self, from: NodeId, ack: TotemJoinAck) {
        if ack.ring_id == self.join_ring_id {
            self.join_responders.insert(from, ());
        }
    }

    fn finalize_join(&mut self, now: Instant) {
        self.join_deadline = None;

        // Agreed ring = all nodes that responded during this join phase.
        let mut agreed: Vec<NodeId> =
            self.join_responders.keys().copied().collect();
        agreed.sort_unstable();

        let voting_count = agreed
            .iter()
            .filter(|&&id| !self.cluster.is_peer_observer(id))
            .count();
        let quorum = self.total_voting_nodes / 2 + 1;

        if voting_count < quorum {
            let backoff = self.join_retry_backoff;
            self.join_retry_backoff = (backoff * 2).min(Duration::from_secs(30));
            warn!(
                "Totem: join failed — {} voting members agreed, need {}; retrying in {:?}",
                voting_count, quorum, backoff
            );
            // Use start_join to broadcast and set up state, then override the
            // deadline with the backoff duration so we don't hammer the network.
            self.start_join(now); // also resets join_retry_backoff, but we saved it above
            self.join_retry_backoff = (backoff * 2).min(Duration::from_secs(30)); // restore
            self.join_deadline = Some(now + backoff);
            return;
        }

        let new_ring_id = self.join_ring_id;
        info!(
            "Totem: ring {} formed — {} members: {:?}",
            new_ring_id,
            agreed.len(),
            agreed
        );

        self.ring = agreed.clone();
        self.ring_id = new_ring_id;
        self.my_pos = self
            .ring
            .iter()
            .position(|&id| id == self.cluster.local_id)
            .unwrap_or(0);

        // Drop in-flight callbacks so their callers retry.
        self.seq_callbacks.clear();
        self.held_msgs.clear();
        self.sent_msgs.clear();

        // Coordinator = lowest-ID voting node.
        let coordinator = agreed
            .iter()
            .find(|&&id| !self.cluster.is_peer_observer(id))
            .copied()
            .unwrap_or(agreed[0]);

        if coordinator == self.cluster.local_id {
            info!("Totem: I am coordinator for ring {}", new_ring_id);
            self.send_sync_and_token();
        }

        self.update_pub_state();
    }

    fn send_sync_and_token(&mut self) {
        let data = {
            let st = self.store.read();
            match bincode::serialize(&*st) {
                Ok(d) => d,
                Err(e) => {
                    warn!("Totem: sync serialize failed: {}", e);
                    return;
                }
            }
        };

        self.cluster.broadcast(Message::TotemSync(TotemSync {
            ring_id: self.ring_id,
            members: self.ring.clone(),
            deliver_seq: self.deliver_seq,
            data,
        }));

        // Reset sequence counters for the new ring.
        self.mcast_seq = self.deliver_seq;
        self.local_aru = self.deliver_seq;
        self.last_token_at = Instant::now();

        // Take the token and immediately process a round.
        // confirmed_aru starts at deliver_seq: the coordinator's already-committed
        // state is the baseline for all nodes.  New messages won't be deliverable
        // until the first full rotation confirms their global minimum.
        self.has_token = true;
        let aru = self.local_aru;
        let seq = self.mcast_seq;
        let confirmed = self.deliver_seq;
        self.process_token_hold(aru, seq, confirmed, vec![]);
    }

    fn on_sync(&mut self, from: NodeId, sync: TotemSync) {
        if sync.ring_id < self.ring_id {
            return; // stale sync
        }
        // Adopt ring membership from the coordinator's sync message.
        if !sync.members.is_empty() {
            self.ring = sync.members.clone();
            self.ring_id = sync.ring_id;
            self.my_pos = self
                .ring
                .iter()
                .position(|&id| id == self.cluster.local_id)
                .unwrap_or(0);
            self.join_deadline = None;
        }
        match bincode::deserialize::<FileStore>(&sync.data) {
            Ok(snap_store) => {
                *self.store.write() = snap_store;
                self.deliver_seq = sync.deliver_seq;
                self.local_aru = sync.deliver_seq;
                self.mcast_seq = sync.deliver_seq;
                self.last_token_at = Instant::now(); // reset token timer

                // Clear ALL in-flight state from the previous ring.  When a
                // non-coordinator node receives TotemSync it may have skipped
                // finalize_join (which is where these would normally be cleared).
                // Stale held_msgs with old ring_ids would corrupt local_aru
                // accounting in the new ring, and stale seq_callbacks would
                // never fire (causing proposal timeouts).
                self.held_msgs.clear();
                self.sent_msgs.clear();
                self.seq_callbacks.clear();

                info!(
                    "Totem: applied sync from {} at ring {} seq {} members={:?}",
                    from, sync.ring_id, sync.deliver_seq, self.ring
                );
                self.update_pub_state();
            }
            Err(e) => warn!("Totem: sync deserialize failed: {}", e),
        }
    }

    // ── Token protocol ────────────────────────────────────────────────────────

    fn on_token(&mut self, _from: NodeId, token: TotemToken) {
        if token.ring_id != self.ring_id {
            debug!(
                "Totem: ignoring token from ring {} (current: {})",
                token.ring_id, self.ring_id
            );
            return;
        }
        self.has_token = true;
        self.last_token_at = Instant::now();

        // Adopt highest seq seen in token (another node may have assigned it).
        if token.seq > self.mcast_seq {
            self.mcast_seq = token.seq;
        }

        self.process_token_hold(token.aru, token.seq, token.confirmed_aru, token.rtr_list);
    }

    /// Core token processing: retransmit, send new messages, deliver, pass token.
    ///
    /// `token_aru`     — running minimum ARU from the incoming token (current rotation).
    /// `token_seq`     — highest sequence number in the incoming token.
    /// `confirmed_aru` — globally confirmed delivery point from the PREVIOUS rotation
    ///                   (set by the coordinator, passed unchanged by all others).
    fn process_token_hold(
        &mut self,
        token_aru: Seq,
        token_seq: Seq,
        confirmed_aru: Seq,
        rtr_list: Vec<Seq>,
    ) {
        // 1. Retransmit any messages the incoming token requested.
        for seq in &rtr_list {
            if let Some(mcast) = self.sent_msgs.get(seq).cloned() {
                debug!("Totem: retransmitting seq {}", seq);
                self.cluster.broadcast(Message::TotemMcast(mcast));
            }
        }

        // 2. Check quorum before sending.
        let quorum_ok = {
            let voting = self
                .ring
                .iter()
                .filter(|&&id| !self.cluster.is_peer_observer(id))
                .count();
            voting >= self.total_voting_nodes / 2 + 1
        };

        // 3. Send up to TOKEN_SEND_LIMIT pending proposals.
        if quorum_ok {
            let mut sent = 0u32;
            while sent < TOKEN_SEND_LIMIT {
                let Some(proposal) = self.send_queue.pop_front() else {
                    break;
                };
                let seq = self.mcast_seq + 1;
                self.mcast_seq = seq;

                let request_id = self.next_request_id;
                self.next_request_id += 1;

                let mcast = TotemMcast {
                    ring_id: self.ring_id,
                    seq,
                    sender: self.cluster.local_id,
                    request_id,
                    op: proposal.op,
                };

                self.seq_callbacks.insert(seq, proposal.done);
                self.cluster.broadcast(Message::TotemMcast(mcast.clone()));
                // Add to our own held_msgs (broadcast() doesn't loop back to self).
                self.held_msgs.insert(seq, mcast.clone());
                self.sent_msgs.insert(seq, mcast);
                // Advance local ARU if consecutive.
                while self.held_msgs.contains_key(&(self.local_aru + 1)) {
                    self.local_aru += 1;
                }
                sent += 1;
            }
        }

        // 4. Deliver messages confirmed safe by confirmed_aru.
        //
        // `confirmed_aru` is the globally confirmed delivery point set by the
        // coordinator after a complete ring rotation.  It equals the final `aru`
        // from the previous rotation — i.e., the minimum local_aru across ALL
        // nodes in the ring.  By using the same value on every node throughout a
        // rotation we guarantee all nodes deliver the exact same set of messages.
        //
        // For a single-node ring there is no rotation: use local_aru directly.
        let safe_up_to = if self.ring.len() == 1 { self.local_aru } else { confirmed_aru };
        self.try_deliver(safe_up_to);

        // 5. Build rtr_list for the next token holder.
        let high = self.mcast_seq.max(token_seq);
        let mut new_rtr: Vec<Seq> = Vec::new();
        for seq in (self.local_aru + 1)..=high {
            if !self.held_msgs.contains_key(&seq) {
                new_rtr.push(seq);
                if new_rtr.len() >= MAX_RTR_LIST {
                    break;
                }
            }
        }

        // 6. New ARU for the outgoing token.
        // The ring coordinator (my_pos == 0) resets token.aru to its own
        // local_aru at the start of each rotation (reference point). Other
        // nodes take min(local_aru, token_aru) to bring the running minimum
        // down to the bottleneck. After a full rotation the coordinator
        // delivers up to this confirmed minimum.
        let new_aru = if self.ring.len() == 1 {
            self.local_aru // single-node: I have everything
        } else if self.my_pos == 0 {
            self.local_aru // coordinator resets aru to own value each rotation
        } else {
            self.local_aru.min(token_aru)
        };

        // 7. Outgoing confirmed_aru.
        // Coordinator: the incoming token_aru is now fully confirmed (all nodes
        // have reported their local_aru for the completed rotation). Announce it
        // as the new confirmed_aru so every node can deliver up to it next round.
        // Non-coordinator: pass confirmed_aru through unchanged.
        let new_confirmed = if self.my_pos == 0 || self.ring.len() == 1 {
            token_aru // coordinator: token_aru = global minimum just confirmed
        } else {
            confirmed_aru // others: relay unchanged
        };

        // 8. GC sent_msgs confirmed by all (seq ≤ confirmed_aru).
        // Use the incoming confirmed_aru (not new_aru) so we only discard messages
        // that ALL nodes are known to have received, preventing retransmit failures.
        self.sent_msgs.retain(|&s, _| s > confirmed_aru);

        // 9. Pass token to next node in ring.
        self.has_token = false;
        let next_pos = (self.my_pos + 1) % self.ring.len();
        let next = self.ring[next_pos];

        if next == self.cluster.local_id {
            // Single-node ring: keep the token for the next tick.
            self.has_token = true;
        } else {
            self.cluster.send(
                next,
                Message::TotemToken(TotemToken {
                    ring_id: self.ring_id,
                    seq: self.mcast_seq,
                    aru: new_aru,
                    confirmed_aru: new_confirmed,
                    rtr_list: new_rtr,
                    backlog: self.send_queue.len() as u32,
                    aru_addr: self.cluster.local_id,
                }),
            );
        }

        self.update_pub_state();
    }

    fn on_mcast(&mut self, mcast: TotemMcast) {
        if mcast.ring_id != self.ring_id {
            return;
        }
        let seq = mcast.seq;
        self.held_msgs.insert(seq, mcast);
        // Advance local ARU (tracks what this node has received).
        // Do NOT deliver here — delivery only happens during token hold once
        // the token ARU confirms all ring members have received the message.
        while self.held_msgs.contains_key(&(self.local_aru + 1)) {
            self.local_aru += 1;
        }
    }

    // ── Delivery ──────────────────────────────────────────────────────────────

    /// Deliver all consecutive in-order messages up to `safe_up_to` (the global
    /// ARU from the token), persist the store, then fire callbacks.
    /// Persistence happens before callbacks for zero data loss.
    fn try_deliver(&mut self, safe_up_to: Seq) {
        let mut callbacks: Vec<(oneshot::Sender<FileOpResult>, FileOpResult)> = Vec::new();
        let mut delivered = false;

        while self.deliver_seq < safe_up_to {
            let next_seq = self.deliver_seq + 1;
            let Some(mcast) = self.held_msgs.remove(&next_seq) else {
                break; // gap — retransmit will fill it later
            };
            self.deliver_seq = next_seq;
            let seq = self.deliver_seq;

            let result = {
                let mut st = self.store.write();
                st.apply(&mcast.op)
            };

            let op_result = match result {
                Ok(()) => FileOpResult::Ok,
                Err(TinyCfsError::LockContended(h)) => FileOpResult::LockContended(h),
                Err(ref e @ TinyCfsError::AlreadyExists(_)) => {
                    FileOpResult::Error(e.to_string())
                }
                Err(ref e) => {
                    warn!("Totem: state machine error at seq {}: {}", seq, e);
                    FileOpResult::Error(e.to_string())
                }
            };

            if let Some(tx) = self.seq_callbacks.remove(&seq) {
                callbacks.push((tx, op_result));
            }
            delivered = true;
        }

        if delivered {
            // Persist BEFORE firing any callback (zero data loss guarantee).
            self.persist_store(self.deliver_seq);
        }

        for (tx, result) in callbacks {
            let _ = tx.send(result);
        }
    }

    fn persist_store(&self, seq: Seq) {
        if let Some(db) = &self.db {
            let data = {
                let st = self.store.read();
                match bincode::serialize(&*st) {
                    Ok(d) => d,
                    Err(e) => {
                        warn!("Totem: serialize failed at seq {}: {}", seq, e);
                        return;
                    }
                }
            };
            if let Err(e) = db.save_snapshot(seq, self.ring_id, &data) {
                warn!("Totem: persist failed at seq {}: {}", seq, e);
            }
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    fn update_pub_state(&self) {
        let voting = self
            .ring
            .iter()
            .filter(|&&id| !self.cluster.is_peer_observer(id))
            .count();
        let quorum_ok =
            voting >= self.total_voting_nodes / 2 + 1 && self.join_deadline.is_none();
        let mut ps = self.pub_state.write();
        ps.has_quorum = quorum_ok;
        ps.ring_size = self.ring.len();
        ps.ring_id = self.ring_id;
        ps.has_token = self.has_token;
    }
}

// ── propose() helper (used by Consensus wrapper) ──────────────────────────────

/// Submit a proposal to the Totem engine, retrying up to 30 s while the ring
/// is forming or recovering from a partition.
pub async fn propose(tx: mpsc::Sender<Proposal>, op: FileOp) -> Result<()> {
    let deadline = Instant::now() + PROPOSE_TIMEOUT;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(TinyCfsError::Timeout);
        }
        let (done_tx, done_rx) = oneshot::channel();
        let proposal = Proposal { op: op.clone(), done: done_tx };
        match tx.try_send(proposal) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                time::sleep(Duration::from_millis(50).min(remaining)).await;
                continue;
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                return Err(TinyCfsError::Cluster("totem engine stopped".into()));
            }
        }
        match time::timeout(remaining, done_rx).await {
            Ok(Ok(FileOpResult::Ok)) => return Ok(()),
            Ok(Ok(FileOpResult::Error(e))) => return Err(TinyCfsError::Cluster(e)),
            Ok(Ok(FileOpResult::LockContended(h))) => {
                return Err(TinyCfsError::LockContended(h));
            }
            Ok(Err(_)) => {
                // done_tx dropped (ring reform) — retry
                time::sleep(Duration::from_millis(100).min(remaining)).await;
                continue;
            }
            Err(_) => return Err(TinyCfsError::Timeout),
        }
    }
}
