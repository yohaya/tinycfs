//! SQLite-backed Raft persistence layer.
//!
//! Stores current_term, voted_for, Raft log entries, and FileStore snapshots.
//! Uses WAL mode + NORMAL synchronous for fast, durable writes.
//! SQLite is compiled in (bundled feature) — no system library required.

use rusqlite::{params, Connection};
use std::path::Path;

use crate::cluster::message::{FileOp, LogEntry, LogIndex, NodeId, Term};
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
             PRAGMA wal_autocheckpoint = 1000;
             PRAGMA cache_size    = -8000;

             CREATE TABLE IF NOT EXISTS raft_meta (
                 key   TEXT PRIMARY KEY,
                 value INTEGER NOT NULL
             ) WITHOUT ROWID;

             CREATE TABLE IF NOT EXISTS raft_log (
                 log_index   INTEGER PRIMARY KEY,
                 term        INTEGER NOT NULL,
                 request_id  INTEGER NOT NULL,
                 client_node INTEGER NOT NULL,
                 op          BLOB    NOT NULL
             );

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

    /// Persist current term and voted_for atomically.
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

    /// Load term + voted_for from disk (returns 0/None if no data).
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

    // ── Log entries ───────────────────────────────────────────────────────────

    pub fn append_entry(&self, entry: &LogEntry) -> Result<()> {
        let op_bytes = bincode::serialize(&entry.op)
            .map_err(|e| TinyCfsError::Serialization(e.to_string()))?;
        self.conn
            .execute(
                "INSERT OR REPLACE INTO raft_log \
                 (log_index, term, request_id, client_node, op) \
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    entry.index as i64,
                    entry.term as i64,
                    entry.request_id as i64,
                    entry.client_node as i64,
                    op_bytes,
                ],
            )
            .map_err(|e| TinyCfsError::Io(e.to_string()))?;
        Ok(())
    }

    /// Load all entries with `log_index > after_index`, ordered ascending.
    pub fn load_entries_after(&self, after_index: LogIndex) -> Result<Vec<LogEntry>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT log_index, term, request_id, client_node, op \
                 FROM raft_log WHERE log_index > ?1 ORDER BY log_index",
            )
            .map_err(|e| TinyCfsError::Io(e.to_string()))?;

        let rows = stmt
            .query_map(params![after_index as i64], |row| {
                Ok((
                    row.get::<_, i64>(0)? as u64,
                    row.get::<_, i64>(1)? as u64,
                    row.get::<_, i64>(2)? as u64,
                    row.get::<_, i64>(3)? as u64,
                    row.get::<_, Vec<u8>>(4)?,
                ))
            })
            .map_err(|e| TinyCfsError::Io(e.to_string()))?;

        let mut entries = Vec::new();
        for row in rows {
            let (index, term, request_id, client_node, op_bytes) =
                row.map_err(|e| TinyCfsError::Io(e.to_string()))?;
            let op: FileOp = bincode::deserialize(&op_bytes)
                .map_err(|e| TinyCfsError::Serialization(e.to_string()))?;
            entries.push(LogEntry { index, term, request_id, client_node, op });
        }
        Ok(entries)
    }

    // ── Filesystem snapshot ───────────────────────────────────────────────────

    /// Persist a FileStore snapshot (only the latest snapshot is kept).
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

    /// Remove log entries with index ≤ `before_index` (log compaction).
    pub fn compact_log_before(&self, before_index: LogIndex) -> Result<()> {
        self.conn
            .execute(
                "DELETE FROM raft_log WHERE log_index <= ?1",
                params![before_index as i64],
            )
            .map_err(|e| TinyCfsError::Io(e.to_string()))?;
        Ok(())
    }
}
