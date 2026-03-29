use serde::{Deserialize, Serialize};

/// Stable 64-bit node identity derived from the node name.
pub type NodeId = u64;
pub type Term = u64;
pub type LogIndex = u64;

// ─── Raft log entry ──────────────────────────────────────────────────────────

/// A single entry in the replicated Raft log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub index: LogIndex,
    pub term: Term,
    /// Monotonic request ID issued by the proposing node.
    pub request_id: u64,
    /// The originating node (used to route the response back).
    pub client_node: NodeId,
    pub op: FileOp,
}

// ─── Filesystem operations ───────────────────────────────────────────────────

/// All mutating filesystem operations that go through Raft consensus.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FileOp {
    CreateFile {
        path: String,
        mode: u32,
        uid: u32,
        gid: u32,
    },
    CreateDir {
        path: String,
        mode: u32,
        uid: u32,
        gid: u32,
    },
    CreateSymlink {
        path: String,
        target: String,
        uid: u32,
        gid: u32,
    },
    Write {
        path: String,
        offset: u64,
        data: Vec<u8>,
    },
    Truncate {
        path: String,
        size: u64,
    },
    Unlink {
        path: String,
    },
    Rmdir {
        path: String,
    },
    Rename {
        from: String,
        to: String,
    },
    SetAttr {
        path: String,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        mtime: Option<u64>,
        atime: Option<u64>,
        size: Option<u64>,
    },

    // ── Distributed file locking ─────────────────────────────────────────────

    /// Try to acquire an exclusive lock on `path`.
    /// Fails (LockContended) if a different holder already holds a non-expired lock.
    AcquireLock {
        path: String,
        /// Unique string identifying the lock owner, e.g. "node1:12345".
        holder_id: String,
        /// Lock TTL in seconds. The lock is automatically released if the holder
        /// does not renew within this period (prevents dead-lock from crashed nodes).
        ttl_secs: u64,
    },
    /// Release a lock.  Idempotent if the lock is already gone.
    ReleaseLock {
        path: String,
        holder_id: String,
    },
    /// Extend the TTL of an existing lock held by `holder_id`.
    RenewLock {
        path: String,
        holder_id: String,
        ttl_secs: u64,
    },
}

/// Result of a filesystem operation sent back to the waiting client.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FileOpResult {
    Ok,
    Error(String),
    /// Lock is held by the named holder — used to propagate LockContended back
    /// to the proposer so it can return EWOULDBLOCK or retry (setlkw).
    LockContended(String),
}

// ─── Raft messages ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVote {
    pub term: Term,
    pub candidate_id: NodeId,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteReply {
    pub term: Term,
    pub vote_granted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntries {
    pub term: Term,
    pub leader_id: NodeId,
    pub prev_log_index: LogIndex,
    pub prev_log_term: Term,
    pub entries: Vec<LogEntry>,
    pub leader_commit: LogIndex,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesReply {
    pub term: Term,
    pub success: bool,
    /// When success==true: highest log index replicated on this follower.
    /// When success==false: the follower's last log index (helps leader back up).
    pub match_index: LogIndex,
}

// ─── InstallSnapshot ─────────────────────────────────────────────────────────

/// Sent by the leader when a follower's next_index has fallen behind the
/// leader's snapshot boundary (log compaction trimmed the entries it needs).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallSnapshot {
    pub term: Term,
    pub leader_id: NodeId,
    pub snapshot_index: LogIndex,
    pub snapshot_term: Term,
    /// Serialized `FileStore` state at `snapshot_index`.
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallSnapshotReply {
    pub term: Term,
    /// Highest log index now confirmed on this follower (= snapshot_index on success).
    pub match_index: LogIndex,
}

// ─── Totem messages ──────────────────────────────────────────────────────────

/// Sequence number type used by the Totem protocol.
pub type Seq = u64;

/// The Totem token — rotates around the ring granting permission to multicast.
///
/// Designed after the Corosync Totem SRTP token structure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TotemToken {
    /// Monotonically increasing ring ID.  Nodes ignore tokens from old rings.
    pub ring_id: u64,
    /// Highest sequence number assigned so far in this ring.
    pub seq: Seq,
    /// All-received-up-to: the lowest per-node ARU seen so far on this token
    /// rotation (minimum across all holders).  Messages with seq ≤ aru are
    /// guaranteed to have been received by every ring member.
    pub aru: Seq,
    /// Globally confirmed delivery point from the PREVIOUS rotation.
    /// The coordinator sets this after each full ring pass (= the final `aru`
    /// of the completed rotation, which is the true global minimum).  All other
    /// nodes pass this value through unchanged.  Every node delivers up to
    /// `confirmed_aru` — guaranteeing all nodes deliver the same set of messages
    /// at the same point in the protocol.
    pub confirmed_aru: Seq,
    /// Sequence numbers that the last token holder had not yet received and
    /// is requesting retransmission of.
    pub rtr_list: Vec<Seq>,
    /// Number of messages the token sender has waiting to send (backlog).
    pub backlog: u32,
    /// Identity of the node that reported the lowest ARU.
    pub aru_addr: NodeId,
}

/// An atomic multicast message in the Totem protocol.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TotemMcast {
    /// Ring this message belongs to.
    pub ring_id: u64,
    /// Total-order sequence number assigned by the token holder.
    pub seq: Seq,
    /// The node that originated this message.
    pub sender: NodeId,
    /// Opaque request ID used to match the delivery callback on the
    /// originating node.
    pub request_id: u64,
    /// The filesystem operation to replicate.
    pub op: FileOp,
}

/// Broadcast by any node that wants to form or rejoin a ring.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TotemJoin {
    /// Proposed ring ID (must be strictly higher than any known ring ID).
    pub ring_id: u64,
    /// The set of node IDs the sender proposes for the new ring.
    pub members: Vec<NodeId>,
    pub sender: NodeId,
}

/// Acknowledgement of a TotemJoin proposal.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TotemJoinAck {
    pub ring_id: u64,
    /// The set of nodes the acknowledging node can reach.
    pub members: Vec<NodeId>,
    pub sender: NodeId,
}

/// Sent by the ring coordinator after forming a new ring to distribute the
/// current filesystem state and ring membership.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TotemSync {
    pub ring_id: u64,
    /// Ordered list of all nodes in the new ring.
    pub members: Vec<NodeId>,
    /// Highest sequence number already delivered before this sync point.
    pub deliver_seq: Seq,
    /// Serialized `FileStore` snapshot.
    pub data: Vec<u8>,
}

// ─── Top-level network message ───────────────────────────────────────────────

/// Every message exchanged between cluster nodes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Message {
    // ── Cluster membership ──────────────────────────────────────────────────
    /// Sent when a new TCP connection is established.
    Hello {
        node_id: NodeId,
        name: String,
        cluster_name: String,
    },
    /// Acknowledgement of Hello.
    HelloAck {
        node_id: NodeId,
        name: String,
    },

    // ── Raft consensus ──────────────────────────────────────────────────────
    RequestVote(RequestVote),
    RequestVoteReply(RequestVoteReply),
    AppendEntries(AppendEntries),
    AppendEntriesReply(AppendEntriesReply),
    InstallSnapshot(InstallSnapshot),
    InstallSnapshotReply(InstallSnapshotReply),

    // ── Totem consensus ─────────────────────────────────────────────────────
    TotemToken(TotemToken),
    TotemMcast(TotemMcast),
    TotemJoin(TotemJoin),
    TotemJoinAck(TotemJoinAck),
    TotemSync(TotemSync),

    // ── Client request forwarding ───────────────────────────────────────────
    /// A follower forwards a write request to the leader.
    ClientRequest {
        request_id: u64,
        op: FileOp,
    },
    /// The leader sends the result back to the originating follower.
    ClientResponse {
        request_id: u64,
        result: FileOpResult,
        /// The Raft log index at which this request was committed.  The
        /// follower must not signal the local proposer until it has applied
        /// at least this index, otherwise the file/op is not yet visible in
        /// the follower's local store and the FUSE handler sees a stale view.
        log_index: LogIndex,
    },
}
