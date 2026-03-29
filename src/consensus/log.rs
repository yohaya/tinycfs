use crate::cluster::message::{LogEntry, LogIndex, Term};

/// The replicated Raft log.
///
/// Log indices start at 1.  After a snapshot is taken, entries up to
/// `snapshot_index` are discarded.  `get(i)` for i ≤ snapshot_index
/// returns None (those entries are captured in the snapshot).
#[derive(Debug, Default)]
pub struct Log {
    entries: Vec<LogEntry>,
    /// Highest index included in the most recent snapshot (0 = no snapshot).
    pub snapshot_index: LogIndex,
    /// Term of the `snapshot_index` entry.
    pub snapshot_term: Term,
}

impl Log {
    pub fn new() -> Self {
        Self { entries: Vec::new(), snapshot_index: 0, snapshot_term: 0 }
    }

    /// Set the snapshot base (called on startup when loading a snapshot).
    pub fn set_snapshot_base(&mut self, index: LogIndex, term: Term) {
        self.snapshot_index = index;
        self.snapshot_term = term;
    }

    /// Returns the index of the last entry, or `snapshot_index` if empty.
    pub fn last_index(&self) -> LogIndex {
        self.entries.last().map(|e| e.index).unwrap_or(self.snapshot_index)
    }

    /// Returns the term of the last entry, or `snapshot_term` if empty.
    pub fn last_term(&self) -> Term {
        self.entries.last().map(|e| e.term).unwrap_or(self.snapshot_term)
    }

    /// Term of the entry at `index`.
    /// - Returns `Some(0)` for index 0 (sentinel before log start).
    /// - Returns `Some(snapshot_term)` for `index == snapshot_index`.
    /// - Returns `None` for indices that are neither in log nor in snapshot.
    pub fn term_at(&self, index: LogIndex) -> Option<Term> {
        if index == 0 {
            return Some(0);
        }
        if index == self.snapshot_index {
            return Some(self.snapshot_term);
        }
        self.get(index).map(|e| e.term)
    }

    /// Retrieve entry by 1-based index. Returns None if out of range.
    pub fn get(&self, index: LogIndex) -> Option<&LogEntry> {
        if index == 0 || index <= self.snapshot_index {
            return None;
        }
        let offset = (index - self.snapshot_index - 1) as usize;
        self.entries.get(offset)
    }

    /// Append a new entry (caller sets entry.index = last_index + 1).
    pub fn append(&mut self, entry: LogEntry) {
        debug_assert_eq!(entry.index, self.last_index() + 1);
        self.entries.push(entry);
    }

    /// Truncate the log, removing all entries with index > `last_valid`.
    pub fn truncate_after(&mut self, last_valid: LogIndex) {
        // Entries are monotonically ordered by index; binary search is O(log n)
        // vs the O(n) scan that retain() would perform.
        let keep = self.entries.partition_point(|e| e.index <= last_valid);
        self.entries.truncate(keep);
    }

    /// Append entries from a leader, possibly truncating conflicting entries.
    /// Returns `true` if prev_log_index/prev_log_term are consistent.
    pub fn append_leader_entries(
        &mut self,
        prev_log_index: LogIndex,
        prev_log_term: Term,
        new_entries: Vec<LogEntry>,
    ) -> bool {
        // Consistency check
        if prev_log_index > 0 {
            if prev_log_index < self.snapshot_index {
                // prev is before our snapshot — the leader's view is older; reject
                // (leader should send from snapshot or higher)
                return false;
            }
            match self.term_at(prev_log_index) {
                None => return false,
                Some(t) if t != prev_log_term => return false,
                _ => {}
            }
        }

        // Discard conflicting entries
        for entry in &new_entries {
            if let Some(existing_term) = self.term_at(entry.index) {
                if existing_term != entry.term {
                    self.truncate_after(entry.index - 1);
                    break;
                }
            }
        }

        // Append missing entries
        for entry in new_entries {
            if entry.index > self.last_index() {
                self.entries.push(entry);
            }
        }

        true
    }

    /// Return entries with index > `after_index` as a slice.
    ///
    /// Uses binary search for O(log N) instead of a linear scan.
    pub fn entries_from(&self, after_index: LogIndex) -> &[LogEntry] {
        let start = self.entries.partition_point(|e| e.index <= after_index);
        &self.entries[start..]
    }

    /// Discard all entries with index ≤ `index` (log compaction after snapshot).
    /// Updates snapshot_index and snapshot_term.
    pub fn compact_before(&mut self, index: LogIndex, term: Term) {
        self.entries.retain(|e| e.index > index);
        self.snapshot_index = index;
        self.snapshot_term = term;
    }

    /// Total number of in-memory entries (after snapshot compaction).
    pub fn len(&self) -> usize {
        self.entries.len()
    }
}
