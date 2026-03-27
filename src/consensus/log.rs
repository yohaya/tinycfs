use crate::cluster::message::{LogEntry, LogIndex, Term};

/// The replicated Raft log.
///
/// Log indices start at 1. Index 0 is a sentinel "nothing" entry (term=0).
#[derive(Debug, Default)]
pub struct Log {
    entries: Vec<LogEntry>,
}

impl Log {
    pub fn new() -> Self {
        Self { entries: Vec::new() }
    }

    /// Returns the index of the last entry, or 0 if empty.
    pub fn last_index(&self) -> LogIndex {
        self.entries.last().map(|e| e.index).unwrap_or(0)
    }

    /// Returns the term of the last entry, or 0 if empty.
    pub fn last_term(&self) -> Term {
        self.entries.last().map(|e| e.term).unwrap_or(0)
    }

    /// Term of the entry at `index`, or 0 if `index == 0` (sentinel).
    pub fn term_at(&self, index: LogIndex) -> Option<Term> {
        if index == 0 {
            return Some(0);
        }
        self.get(index).map(|e| e.term)
    }

    /// Retrieve entry by index (1-based). Returns None if out of range.
    pub fn get(&self, index: LogIndex) -> Option<&LogEntry> {
        if index == 0 {
            return None;
        }
        self.entries.get((index - 1) as usize)
    }

    /// Append a new entry (the caller sets entry.index = last_index + 1).
    pub fn append(&mut self, entry: LogEntry) {
        debug_assert_eq!(entry.index, self.last_index() + 1);
        self.entries.push(entry);
    }

    /// Truncate the log, removing all entries with index > `last_valid`.
    /// Used by followers when the leader sends conflicting entries.
    pub fn truncate_after(&mut self, last_valid: LogIndex) {
        self.entries.retain(|e| e.index <= last_valid);
    }

    /// Append entries from a leader, possibly truncating first.
    ///
    /// Returns `true` if `prev_log_index`/`prev_log_term` match (consistency
    /// check), `false` otherwise (caller should reply failure to leader).
    pub fn append_leader_entries(
        &mut self,
        prev_log_index: LogIndex,
        prev_log_term: Term,
        new_entries: Vec<LogEntry>,
    ) -> bool {
        // Consistency check
        if prev_log_index > 0 {
            match self.term_at(prev_log_index) {
                None => return false,
                Some(t) if t != prev_log_term => return false,
                _ => {}
            }
        }

        // Find and discard conflicting entries
        for entry in &new_entries {
            if let Some(existing_term) = self.term_at(entry.index) {
                if existing_term != entry.term {
                    self.truncate_after(entry.index - 1);
                    break;
                }
            }
        }

        // Append any entries not already present
        for entry in new_entries {
            if entry.index > self.last_index() {
                self.entries.push(entry);
            }
        }

        true
    }

    /// Entries in range `(after_index, up_to_index]`, for leader→follower
    /// replication. Returns an empty vec if there are no new entries.
    pub fn entries_from(&self, after_index: LogIndex) -> Vec<LogEntry> {
        self.entries
            .iter()
            .filter(|e| e.index > after_index)
            .cloned()
            .collect()
    }

    /// Total number of entries stored.
    pub fn len(&self) -> usize {
        self.entries.len()
    }
}
