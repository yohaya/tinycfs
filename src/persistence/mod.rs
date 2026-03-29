//! SQLite-backed persistence for tinycfs.
//!
//! Two things are written to disk:
//!
//!   1. **Raft metadata** (`current_term`, `voted_for`) — must survive a crash
//!      for Raft safety (prevents double-voting / accepting a stale leader).
//!
//!   2. **FileStore state** — stored as *one row per inode* in `fs_inodes`,
//!      not as a single serialised blob.  On each committed batch only the
//!      inodes that actually changed are written: O(changed_files_in_batch)
//!      instead of O(total_filesystem_size).  With `persist_every=1` (the
//!      default) every write is immediately durable at O(1) cost regardless
//!      of how many other files exist.
//!
//!      The old `fs_snapshot` blob table is retained for two purposes:
//!        - receiving `InstallSnapshot` from a leader (full state transfer)
//!        - reading databases written by an older version of tinycfs
//!
//! On restart:
//!   1. Load all rows from `fs_inodes` → rebuild the in-memory FileStore.
//!   2. Load `snapshot_index` from `fs_meta` → tell the Raft log where the
//!      persisted state is anchored.
//!   3. Ask the leader for any entries committed after `snapshot_index`.

use rusqlite::{params, Connection};
use std::path::Path;

use crate::cluster::message::{LogIndex, NodeId, Term};
use crate::error::{Result, TinyCfsError};

pub struct RaftDb {
    conn: Connection,
}

// SAFETY: Connection is Send but not Sync. RaftDb is only ever accessed from
// the single Raft actor task — no concurrent access is possible.
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

             -- Per-inode table: one row per inode, bincode-encoded Inode struct.
             -- Replaces the old single-blob fs_snapshot for normal write path.
             CREATE TABLE IF NOT EXISTS fs_inodes (
                 ino  INTEGER PRIMARY KEY,
                 data BLOB NOT NULL
             );

             -- FileStore bookkeeping: next_ino, total_bytes, snapshot_index, snapshot_term.
             CREATE TABLE IF NOT EXISTS fs_meta (
                 key   TEXT PRIMARY KEY,
                 value INTEGER NOT NULL
             ) WITHOUT ROWID;

             -- Legacy blob snapshot: used for InstallSnapshot and backwards-compat reads.
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
    pub fn save_term(&mut self, term: Term, voted_for: Option<NodeId>) -> Result<()> {
        let tx = self.conn
            .transaction()
            .map_err(|e| TinyCfsError::Io(e.to_string()))?;

        tx.execute(
            "INSERT OR REPLACE INTO raft_meta (key, value) VALUES ('term', ?1)",
            params![term as i64],
        ).map_err(|e| TinyCfsError::Io(e.to_string()))?;

        match voted_for {
            Some(v) => {
                tx.execute(
                    "INSERT OR REPLACE INTO raft_meta (key, value) VALUES ('voted_for', ?1)",
                    params![v as i64],
                ).map_err(|e| TinyCfsError::Io(e.to_string()))?;
            }
            None => {
                tx.execute("DELETE FROM raft_meta WHERE key = 'voted_for'", [])
                    .map_err(|e| TinyCfsError::Io(e.to_string()))?;
            }
        }

        tx.commit().map_err(|e| TinyCfsError::Io(e.to_string()))?;
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

    // ── Per-inode delta persistence ───────────────────────────────────────────

    /// Write the delta from a committed Raft batch to SQLite in one transaction.
    ///
    /// `dirty` — (ino, bincode-serialized Inode) for every inode created or
    ///           modified in the batch.
    /// `deleted` — inode numbers that were removed from the filesystem.
    ///
    /// `snap_index` / `snap_term` are always written to `fs_meta` so that on
    /// restart the engine knows where in the Raft log the on-disk state is
    /// anchored (even when dirty and deleted are both empty, e.g. after a log
    /// compaction whose data was already persisted by a prior call).
    pub fn apply_inode_delta(
        &mut self,
        dirty:       &[(u64, Vec<u8>)],  // (ino, bincode-encoded Inode)
        deleted:     &[u64],             // ino numbers to remove
        next_ino:    u64,
        total_bytes: u64,
        snap_index:  LogIndex,
        snap_term:   Term,
    ) -> Result<()> {
        let tx = self.conn.transaction()
            .map_err(|e| TinyCfsError::Io(e.to_string()))?;

        for (ino, data) in dirty {
            tx.execute(
                "INSERT OR REPLACE INTO fs_inodes (ino, data) VALUES (?1, ?2)",
                params![*ino as i64, data],
            ).map_err(|e| TinyCfsError::Io(e.to_string()))?;
        }
        for ino in deleted {
            tx.execute(
                "DELETE FROM fs_inodes WHERE ino = ?1",
                params![*ino as i64],
            ).map_err(|e| TinyCfsError::Io(e.to_string()))?;
        }

        for (key, val) in &[
            ("next_ino",       next_ino),
            ("total_bytes",    total_bytes),
            ("snapshot_index", snap_index),
            ("snapshot_term",  snap_term),
        ] {
            tx.execute(
                "INSERT OR REPLACE INTO fs_meta (key, value) VALUES (?1, ?2)",
                params![key, *val as i64],
            ).map_err(|e| TinyCfsError::Io(e.to_string()))?;
        }

        tx.commit().map_err(|e| TinyCfsError::Io(e.to_string()))?;
        Ok(())
    }

    /// Atomically replace the entire `fs_inodes` table with the given set.
    ///
    /// Called when the node receives an InstallSnapshot from the leader.
    /// Clears the old table and writes the full snapshot in one transaction
    /// so the on-disk state is never partially applied.
    pub fn apply_full_inode_snapshot(
        &mut self,
        inodes:      &[(u64, Vec<u8>)],  // (ino, bincode-encoded Inode)
        next_ino:    u64,
        total_bytes: u64,
        snap_index:  LogIndex,
        snap_term:   Term,
    ) -> Result<()> {
        let tx = self.conn.transaction()
            .map_err(|e| TinyCfsError::Io(e.to_string()))?;

        tx.execute("DELETE FROM fs_inodes", [])
            .map_err(|e| TinyCfsError::Io(e.to_string()))?;

        for (ino, data) in inodes {
            tx.execute(
                "INSERT INTO fs_inodes (ino, data) VALUES (?1, ?2)",
                params![*ino as i64, data],
            ).map_err(|e| TinyCfsError::Io(e.to_string()))?;
        }

        for (key, val) in &[
            ("next_ino",       next_ino),
            ("total_bytes",    total_bytes),
            ("snapshot_index", snap_index),
            ("snapshot_term",  snap_term),
        ] {
            tx.execute(
                "INSERT OR REPLACE INTO fs_meta (key, value) VALUES (?1, ?2)",
                params![key, *val as i64],
            ).map_err(|e| TinyCfsError::Io(e.to_string()))?;
        }

        tx.commit().map_err(|e| TinyCfsError::Io(e.to_string()))?;
        Ok(())
    }

    /// Load persisted filesystem state from the per-inode table.
    ///
    /// Returns `None` when `fs_inodes` is empty — either first boot or a node
    /// that only has the legacy blob snapshot (see `load_snapshot`).
    pub fn load_fs_state(
        &self,
    ) -> Result<Option<(Vec<(u64, Vec<u8>)>, u64, u64, LogIndex, Term)>> {
        let count: i64 = self.conn
            .query_row("SELECT COUNT(*) FROM fs_inodes", [], |r| r.get(0))
            .map_err(|e| TinyCfsError::Io(e.to_string()))?;
        if count == 0 {
            return Ok(None);
        }

        let mut stmt = self.conn
            .prepare("SELECT ino, data FROM fs_inodes")
            .map_err(|e| TinyCfsError::Io(e.to_string()))?;

        let inodes: Vec<(u64, Vec<u8>)> = stmt
            .query_map([], |row| {
                Ok((row.get::<_, i64>(0)? as u64, row.get::<_, Vec<u8>>(1)?))
            })
            .map_err(|e| TinyCfsError::Io(e.to_string()))?
            .filter_map(|r| r.ok())
            .collect();

        let get_meta = |key: &str| -> u64 {
            self.conn
                .query_row(
                    "SELECT value FROM fs_meta WHERE key = ?1",
                    params![key],
                    |r| r.get::<_, i64>(0),
                )
                .map(|v| v as u64)
                .unwrap_or(0)
        };

        Ok(Some((
            inodes,
            get_meta("next_ino"),
            get_meta("total_bytes"),
            get_meta("snapshot_index"),
            get_meta("snapshot_term"),
        )))
    }

    // ── Legacy blob snapshot (InstallSnapshot + backwards compat) ─────────────

    /// Persist a full FileStore snapshot blob (used for InstallSnapshot).
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

    /// Load the most recent blob snapshot, if any.
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
