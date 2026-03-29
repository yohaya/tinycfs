//! SQLite-backed persistence for tinycfs.
//!
//! Two things are written to disk:
//!
//!   1. **Raft metadata** (`current_term`, `voted_for`) — must survive a crash
//!      for Raft safety (prevents double-voting / accepting a stale leader).
//!
//!   2. **FileStore state** — stored as *one row per inode* in `fs_inodes`,
//!      split across two columns:
//!
//!      - `meta`  BLOB — `bincode(InodePersistMeta)` (~60 bytes): always
//!                       rewritten on any mutation.
//!      - `data`  BLOB — raw file bytes: only rewritten when file content
//!                       changes (write/truncate/create).
//!
//!      This split means a `chmod`, `rename`, or `setattr` on a 256 KiB file
//!      writes only the tiny metadata blob, not the full file content.
//!
//! SQLite tuning applied in `open()`:
//!   - WAL journal mode — writers never block readers.
//!   - `synchronous = NORMAL` — WAL appends without fsync; checkpoint fsyncs.
//!   - `wal_autocheckpoint = 10 000` pages (40 MB) — 10× fewer checkpoint
//!     fsyncs than the default 1 000-page threshold, which fired every ~14
//!     commits at 256 KiB blob size.
//!   - `cache_size = -32 000` (32 MB) — reduces page reads during upserts.
//!   - `mmap_size = 268 435 456` (256 MB) — memory-mapped reads.
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
use crate::fs::inode::{Inode, InodePersistMeta};

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

        // Performance pragmas (must precede table creation).
        conn.execute_batch(
            "PRAGMA journal_mode       = WAL;
             PRAGMA synchronous        = NORMAL;
             PRAGMA cache_size         = -32000;
             PRAGMA mmap_size          = 268435456;
             PRAGMA wal_autocheckpoint = 10000;",
        )
        .map_err(|e| TinyCfsError::Io(e.to_string()))?;

        // Create all supporting tables.
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS raft_meta (
                 key   TEXT PRIMARY KEY,
                 value INTEGER NOT NULL
             ) WITHOUT ROWID;

             CREATE TABLE IF NOT EXISTS fs_meta (
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

        // Detect old fs_inodes schema (single 'data' column, no 'meta' column).
        // If found, drop and recreate — the node will replay state from the leader.
        let table_exists: bool = conn.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='fs_inodes'",
            [],
            |r| r.get::<_, i64>(0),
        ).unwrap_or(0) > 0;

        if table_exists {
            let meta_col_count: i64 = conn.query_row(
                "SELECT COUNT(*) FROM pragma_table_info('fs_inodes') WHERE name='meta'",
                [],
                |r| r.get(0),
            ).unwrap_or(0);
            if meta_col_count == 0 {
                // Old schema — drop so the new CREATE TABLE below runs cleanly.
                conn.execute_batch("DROP TABLE fs_inodes; DELETE FROM fs_meta;")
                    .map_err(|e| TinyCfsError::Io(e.to_string()))?;
            }
        }

        // Per-inode table with meta/data split.
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS fs_inodes (
                 ino  INTEGER PRIMARY KEY,
                 meta BLOB NOT NULL,
                 data BLOB
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
    /// `dirty` — per-inode tuples `(ino, meta_blob, data_blob)`:
    ///   - `meta_blob` — `bincode(InodePersistMeta)` — always written (~60 B)
    ///   - `data_blob` — `Some(bytes)` when file content changed (write/truncate);
    ///                   `None` for metadata-only mutations — the existing data
    ///                   column value is left untouched.
    /// `deleted` — inode numbers that were removed from the filesystem.
    pub fn apply_inode_delta(
        &mut self,
        dirty:       &[(u64, Vec<u8>, Option<Vec<u8>>)],
        deleted:     &[u64],
        next_ino:    u64,
        total_bytes: u64,
        snap_index:  LogIndex,
        snap_term:   Term,
    ) -> Result<()> {
        let tx = self.conn.transaction()
            .map_err(|e| TinyCfsError::Io(e.to_string()))?;

        for (ino, meta, data) in dirty {
            match data {
                Some(d) => {
                    // Data changed — upsert both meta and data.
                    tx.execute(
                        "INSERT INTO fs_inodes (ino, meta, data) VALUES (?1, ?2, ?3)
                         ON CONFLICT(ino) DO UPDATE SET meta = excluded.meta,
                                                        data = excluded.data",
                        params![*ino as i64, meta, d],
                    ).map_err(|e| TinyCfsError::Io(e.to_string()))?;
                }
                None => {
                    // Metadata only — upsert meta, leave data column intact.
                    tx.execute(
                        "INSERT INTO fs_inodes (ino, meta) VALUES (?1, ?2)
                         ON CONFLICT(ino) DO UPDATE SET meta = excluded.meta",
                        params![*ino as i64, meta],
                    ).map_err(|e| TinyCfsError::Io(e.to_string()))?;
                }
            }
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
    pub fn apply_full_inode_snapshot(
        &mut self,
        inodes:      &[(u64, Vec<u8>, Option<Vec<u8>>)],  // (ino, meta_blob, data_blob?)
        next_ino:    u64,
        total_bytes: u64,
        snap_index:  LogIndex,
        snap_term:   Term,
    ) -> Result<()> {
        let tx = self.conn.transaction()
            .map_err(|e| TinyCfsError::Io(e.to_string()))?;

        tx.execute("DELETE FROM fs_inodes", [])
            .map_err(|e| TinyCfsError::Io(e.to_string()))?;

        for (ino, meta, data) in inodes {
            tx.execute(
                "INSERT INTO fs_inodes (ino, meta, data) VALUES (?1, ?2, ?3)",
                params![*ino as i64, meta, data],
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
    /// Decodes each row's `meta` + `data` columns into a live `Inode`.
    /// Returns `None` when `fs_inodes` is empty — either first boot or an
    /// old-schema node that was migrated (see `load_snapshot` for fallback).
    pub fn load_fs_state(
        &self,
    ) -> Result<Option<(Vec<(u64, Inode)>, u64, u64, LogIndex, Term)>> {
        let count: i64 = self.conn
            .query_row("SELECT COUNT(*) FROM fs_inodes", [], |r| r.get(0))
            .map_err(|e| TinyCfsError::Io(e.to_string()))?;
        if count == 0 {
            return Ok(None);
        }

        let mut stmt = self.conn
            .prepare("SELECT ino, meta, data FROM fs_inodes")
            .map_err(|e| TinyCfsError::Io(e.to_string()))?;

        let inodes: Vec<(u64, Inode)> = stmt
            .query_map([], |row| {
                let ino  = row.get::<_, i64>(0)? as u64;
                let meta = row.get::<_, Vec<u8>>(1)?;
                let data = row.get::<_, Option<Vec<u8>>>(2)?;
                Ok((ino, meta, data))
            })
            .map_err(|e| TinyCfsError::Io(e.to_string()))?
            .filter_map(|r| r.ok())
            .filter_map(|(ino, meta, data)| {
                let pm: InodePersistMeta = bincode::deserialize(&meta).ok()?;
                Some((ino, pm.into_inode(data)))
            })
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
