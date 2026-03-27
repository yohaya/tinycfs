//! SQLite-backed persistence for tinycfs.
//!
//! Only two things are written to disk:
//!
//!   1. **Raft metadata** (`current_term`, `voted_for`) — the only Raft state
//!      that MUST survive a crash for safety.  Without it a node could vote
//!      twice in the same term (voted_for) or accept a stale leader (term).
//!
//!   2. **FileStore snapshot** — a serialized copy of the in-memory filesystem
//!      taken every `snapshot_every` applied entries.  On restart the node
//!      loads this snapshot and re-syncs any entries committed since the
//!      snapshot from the Raft leader, just like pmxcfs syncs from peers.
//!
//! The Raft log is intentionally kept in memory only.  After a crash the
//! node starts from the last snapshot (or empty if none) and the leader
//! re-sends any missing entries via AppendEntries / InstallSnapshot.
//! Safety holds because committed entries always exist on a majority of nodes.

use rusqlite::{params, Connection};
use std::path::Path;

use crate::cluster::message::{LogIndex, NodeId, Term};
use crate::error::{Result, TinyCfsError};

pub struct RaftDb {
    conn: Connection,
}

// Connection is Send (not Sync). RaftDb is used exclusively from the single
// Raft actor task, so Send is all we need.
unsafe impl Send for RaftDb {}

impl RaftDb {
    pub fn open(path: &Path) -> Result<Self> {
        let conn = Connection::open(path)
            .map_err(|e| TinyCfsError::Io(e.to_string()))?;

        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous   = NORMAL;
             PRAGMA cache_size    = -4000;

             CREATE TABLE IF NOT EXISTS raft_meta (
                 key   TEXT PRIMARY KEY,
                 value INTEGER NOT NULL
             ) WITHOUT ROWID;

             CREATE TABLE IF NOT EXISTS fs_snapshot (
                 id             INTEGER PRIMARY KEY CHECK (id = 1),
                 snapshot_index INTEGER NOT NULL,
                 snapshot_term  INTEGER NOT NULL,
                 data           BLOB    NOT NULL
             );",
        )
        .map_err(|e| TinyCfsError::Io(e.to_string()))?;

        Ok(RaftDb { conn })
    }

    // ── Raft metadata ─────────────────────────────────────────────────────────

    /// Persist current_term and voted_for atomically.
    pub fn save_term(&self, term: Term, voted_for: Option<NodeId>) -> Result<()> {
        self.conn
            .execute(
                "INSERT OR REPLACE INTO raft_meta (key, value) VALUES ('term', ?1)",
                params![term as i64],
            )
            .map_err(|e| TinyCfsError::Io(e.to_string()))?;

        match voted_for {
            Some(v) => {
                self.conn
                    .execute(
                        "INSERT OR REPLACE INTO raft_meta (key, value) VALUES ('voted_for', ?1)",
                        params![v as i64],
                    )
                    .map_err(|e| TinyCfsError::Io(e.to_string()))?;
            }
            None => {
                self.conn
                    .execute("DELETE FROM raft_meta WHERE key = 'voted_for'", [])
                    .map_err(|e| TinyCfsError::Io(e.to_string()))?;
            }
        }
        Ok(())
    }

    /// Load term + voted_for from disk (returns 0 / None if no data).
    pub fn load_meta(&self) -> Result<(Term, Option<NodeId>)> {
        let term = self
            .conn
            .query_row("SELECT value FROM raft_meta WHERE key = 'term'", [], |r| {
                r.get::<_, i64>(0)
            })
            .map(|v| v as u64)
            .unwrap_or(0);

        let voted_for = self
            .conn
            .query_row(
                "SELECT value FROM raft_meta WHERE key = 'voted_for'",
                [],
                |r| r.get::<_, i64>(0),
            )
            .ok()
            .map(|v| v as u64);

        Ok((term, voted_for))
    }

    // ── FileStore snapshot ────────────────────────────────────────────────────

    /// Persist a FileStore snapshot (only the latest is kept).
    pub fn save_snapshot(&self, index: LogIndex, term: Term, data: &[u8]) -> Result<()> {
        self.conn
            .execute(
                "INSERT OR REPLACE INTO fs_snapshot \
                 (id, snapshot_index, snapshot_term, data) VALUES (1, ?1, ?2, ?3)",
                params![index as i64, term as i64, data],
            )
            .map_err(|e| TinyCfsError::Io(e.to_string()))?;
        Ok(())
    }

    /// Load the most recent snapshot, if any.
    pub fn load_snapshot(&self) -> Result<Option<(LogIndex, Term, Vec<u8>)>> {
        let result = self.conn.query_row(
            "SELECT snapshot_index, snapshot_term, data FROM fs_snapshot WHERE id = 1",
            [],
            |row| {
                Ok((
                    row.get::<_, i64>(0)? as u64,
                    row.get::<_, i64>(1)? as u64,
                    row.get::<_, Vec<u8>>(2)?,
                ))
            },
        );
        match result {
            Ok(v) => Ok(Some(v)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(TinyCfsError::Io(e.to_string())),
        }
    }
}
