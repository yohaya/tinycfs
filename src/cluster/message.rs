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
}

/// Result of a filesystem operation sent back to the waiting client.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FileOpResult {
    Ok,
    Error(String),
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
    },
}
