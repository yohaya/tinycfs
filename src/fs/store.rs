/// In-memory filesystem state machine.
///
/// This is the replicated state kept on every node. Updated by applying
/// committed Raft log entries in order. Reads go directly here (no network).
/// Writes are proposed to Raft and applied here once committed.
///
/// Serializable via serde/bincode for Raft snapshots.
use std::collections::{HashMap, HashSet};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::cluster::message::FileOp;
use crate::error::{Result, TinyCfsError};
use crate::fs::inode::{DirInode, FileInode, Ino, InoKind, Inode, InodeMeta, SymlinkInode, InodePersistMeta};

/// Root inode is always 1 (FUSE convention).
pub const ROOT_INO: Ino = 1;

// ── Distributed lock entry ────────────────────────────────────────────────────

mod serde_systemtime {
    use serde::{Deserialize, Deserializer, Serializer};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    pub fn serialize<S: Serializer>(t: &SystemTime, ser: S) -> Result<S::Ok, S::Error> {
        let nanos = t.duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as u64;
        ser.serialize_u64(nanos)
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(de: D) -> Result<SystemTime, D::Error> {
        let nanos = u64::deserialize(de)?;
        Ok(UNIX_EPOCH + Duration::from_nanos(nanos))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LockEntry {
    /// Opaque string identifying the lock holder, e.g. "node1:12345".
    pub holder_id: String,
    /// Wall-clock expiry.  Locks past this time are treated as released.
    #[serde(with = "serde_systemtime")]
    pub expires_at: SystemTime,
}

// ── FileStore ─────────────────────────────────────────────────────────────────

#[derive(Serialize, Deserialize)]
pub struct FileStore {
    inodes: HashMap<Ino, Inode>,
    next_ino: Ino,
    /// Distributed lock table: path → lock entry.
    locks: HashMap<String, LockEntry>,
    /// Running total of all file data bytes across the filesystem.
    /// Maintained incrementally; `#[serde(default)]` ensures old snapshots
    /// without this field deserialize to 0 and self-correct on next write.
    #[serde(default)]
    total_data_bytes: u64,
    /// Per-file and total-filesystem size limits sourced from Config.
    /// Not serialized — must be re-applied via `set_limits` after every
    /// snapshot restore.  Default 0 means "no limit enforced".
    #[serde(skip, default)]
    max_file_size_bytes: u64,
    #[serde(skip, default)]
    max_fs_size_bytes: u64,
    // ── Dirty tracking for per-inode delta persistence ────────────────────
    // Populated by every mutator; drained by persist_store in the Raft actor.
    // Not serialized — rebuilt from applied ops, never snapshotted.
    #[serde(skip, default)]
    dirty_inodes: HashSet<Ino>,
    /// Subset of dirty_inodes where file *data* changed (write/truncate/create).
    /// Used to skip re-writing the large data blob for metadata-only mutations.
    #[serde(skip, default)]
    data_dirty_inodes: HashSet<Ino>,
    #[serde(skip, default)]
    deleted_inodes: HashSet<Ino>,
}

impl FileStore {
    /// Create a new, empty filesystem with only the root directory.
    pub fn new() -> Self {
        let mut inodes = HashMap::new();
        let root_meta = InodeMeta {
            ino: ROOT_INO,
            kind: InoKind::Directory,
            mode: 0o755,
            uid: 0,
            gid: 0,
            nlink: 2,
            atime: SystemTime::now(),
            mtime: SystemTime::now(),
            ctime: SystemTime::now(),
        };
        inodes.insert(ROOT_INO, Inode::Dir(DirInode { meta: root_meta, parent_ino: ROOT_INO, entries: HashMap::new() }));
        // Mark the root inode dirty so it is persisted on the first commit.
        let mut dirty_inodes = HashSet::new();
        dirty_inodes.insert(ROOT_INO);
        FileStore {
            inodes,
            next_ino: 2,
            locks: HashMap::new(),
            total_data_bytes: 0,
            max_file_size_bytes: 0,
            max_fs_size_bytes: 0,
            dirty_inodes,
            data_dirty_inodes: HashSet::new(),
            deleted_inodes: HashSet::new(),
        }
    }

    /// Reconstruct a FileStore from persisted inode data (loaded from SQLite).
    /// `dirty_inodes` starts empty — nothing needs to be re-persisted.
    pub fn from_parts(inodes: HashMap<Ino, Inode>, next_ino: Ino, total_data_bytes: u64) -> Self {
        FileStore {
            inodes,
            next_ino,
            locks: HashMap::new(),
            total_data_bytes,
            max_file_size_bytes: 0,
            max_fs_size_bytes: 0,
            dirty_inodes: HashSet::new(),
            data_dirty_inodes: HashSet::new(),
            deleted_inodes: HashSet::new(),
        }
    }

    fn alloc_ino(&mut self) -> Ino {
        let ino = self.next_ino;
        self.next_ino += 1;
        ino
    }

    // ── Public read accessors ─────────────────────────────────────────────

    pub fn get(&self, ino: Ino) -> Option<&Inode> {
        self.inodes.get(&ino)
    }

    pub fn lookup(&self, parent: Ino, name: &str) -> Option<Ino> {
        if let Some(Inode::Dir(dir)) = self.inodes.get(&parent) {
            dir.entries.get(name).copied()
        } else {
            None
        }
    }

    /// Total bytes of file data currently held across all files.
    /// Total number of inodes currently in the filesystem (root + all entries).
    pub fn inode_count(&self) -> usize {
        self.inodes.len()
    }

    /// Number of currently held distributed locks.
    pub fn lock_count(&self) -> usize {
        self.locks.len()
    }

    pub fn total_data_bytes(&self) -> u64 {
        self.total_data_bytes
    }

    /// Set per-file and total-filesystem size limits.  Must be called after
    /// store creation and after every snapshot restore because these fields are
    /// not persisted (`serde::skip`).
    pub fn set_limits(&mut self, max_file_size: u64, max_fs_size: u64) {
        self.max_file_size_bytes = max_file_size;
        self.max_fs_size_bytes = max_fs_size;
    }

    /// Current value of the inode allocator (needed by the persist layer).
    pub fn next_ino_val(&self) -> Ino { self.next_ino }

    /// All inodes — used when serialising a full snapshot for InstallSnapshot.
    pub fn inodes(&self) -> &HashMap<Ino, Inode> { &self.inodes }

    /// Drain the dirty-inode sets accumulated since the last persist call.
    ///
    /// Returns `(dirty_set, data_dirty_set, deleted_set)` where:
    /// - `dirty_set`      — every inode that was created or mutated (always persists metadata)
    /// - `data_dirty_set` — subset of dirty_set where file *data* changed (write/truncate)
    /// - `deleted_set`    — inodes removed from the filesystem
    pub fn drain_dirty(&mut self) -> (HashSet<Ino>, HashSet<Ino>, HashSet<Ino>) {
        (
            std::mem::take(&mut self.dirty_inodes),
            std::mem::take(&mut self.data_dirty_inodes),
            std::mem::take(&mut self.deleted_inodes),
        )
    }

    /// Returns a reference to the `InodePersistMeta` accessor (used by persistence).
    pub fn persist_meta_for(&self, ino: Ino) -> Option<InodePersistMeta> {
        self.inodes.get(&ino).map(InodePersistMeta::from_inode)
    }

    /// Returns the raw file data bytes for a file inode (used by persistence).
    pub fn file_data_for(&self, ino: Ino) -> Option<&[u8]> {
        match self.inodes.get(&ino) {
            Some(Inode::File(f)) => Some(&f.data),
            _ => None,
        }
    }

    pub fn readdir(&self, ino: Ino) -> Result<Vec<(String, Ino, InoKind)>> {
        match self.inodes.get(&ino) {
            Some(Inode::Dir(dir)) => {
                let mut entries = vec![
                    (".".into(), ino, InoKind::Directory),
                    ("..".into(), dir.parent_ino, InoKind::Directory),
                ];
                for (name, &child_ino) in &dir.entries {
                    if let Some(child) = self.inodes.get(&child_ino) {
                        entries.push((name.clone(), child_ino, child.kind()));
                    }
                }
                Ok(entries)
            }
            Some(_) => Err(TinyCfsError::NotDirectory),
            None => Err(TinyCfsError::NotFound(format!("inode {}", ino))),
        }
    }

    // ── Lock table ────────────────────────────────────────────────────────

    /// Try to acquire an exclusive lock.  Returns `LockContended` if another
    /// non-expired holder holds the lock.
    pub fn try_acquire_lock(
        &mut self,
        path: &str,
        holder_id: &str,
        ttl_secs: u64,
    ) -> Result<()> {
        let now = SystemTime::now();
        // GC expired locks before checking.
        self.locks.retain(|_, v| v.expires_at > now);

        if let Some(existing) = self.locks.get(path) {
            if existing.expires_at > now && existing.holder_id != holder_id {
                return Err(TinyCfsError::LockContended(existing.holder_id.clone()));
            }
        }
        let expires_at = now + Duration::from_secs(ttl_secs.max(1));
        self.locks.insert(
            path.to_string(),
            LockEntry { holder_id: holder_id.to_string(), expires_at },
        );
        Ok(())
    }

    /// Release a lock.  Idempotent — OK if lock is already gone.
    pub fn release_lock(&mut self, path: &str, holder_id: &str) -> Result<()> {
        match self.locks.get(path) {
            Some(e) if e.holder_id == holder_id || e.expires_at <= SystemTime::now() => {
                self.locks.remove(path);
                Ok(())
            }
            Some(e) => Err(TinyCfsError::LockContended(e.holder_id.clone())),
            None => Ok(()),
        }
    }

    /// Extend the TTL of an existing lock.
    pub fn renew_lock(
        &mut self,
        path: &str,
        holder_id: &str,
        ttl_secs: u64,
    ) -> Result<()> {
        let now = SystemTime::now();
        match self.locks.get_mut(path) {
            Some(e) if e.holder_id == holder_id => {
                e.expires_at = now + Duration::from_secs(ttl_secs.max(1));
                Ok(())
            }
            Some(e) if e.expires_at <= now => {
                // Expired — any node may take over.
                *e = LockEntry {
                    holder_id: holder_id.to_string(),
                    expires_at: now + Duration::from_secs(ttl_secs.max(1)),
                };
                Ok(())
            }
            Some(e) => Err(TinyCfsError::LockContended(e.holder_id.clone())),
            None => Err(TinyCfsError::NotFound(format!("no lock for {}", path))),
        }
    }

    /// Returns the active (non-expired) lock on `path`, if any.
    pub fn check_lock(&self, path: &str) -> Option<&LockEntry> {
        self.locks.get(path).filter(|e| e.expires_at > SystemTime::now())
    }

    // ── Apply a committed FileOp ──────────────────────────────────────────

    pub fn apply(&mut self, op: &FileOp) -> Result<()> {
        match op {
            FileOp::CreateFile { path, mode, uid, gid } => {
                self.create_file(path, *mode, *uid, *gid)
            }
            FileOp::CreateDir { path, mode, uid, gid } => {
                self.create_dir(path, *mode, *uid, *gid)
            }
            FileOp::CreateSymlink { path, target, uid, gid } => {
                self.create_symlink(path, target, *uid, *gid)
            }
            FileOp::Write { path, offset, data } => self.write_file(path, *offset, data),
            FileOp::Truncate { path, size } => self.truncate(path, *size),
            FileOp::Unlink { path } => self.unlink(path),
            FileOp::Rmdir { path } => self.rmdir(path),
            FileOp::Rename { from, to } => self.rename(from, to),
            FileOp::SetAttr { path, mode, uid, gid, mtime, atime, size } => {
                self.setattr(path, *mode, *uid, *gid, *mtime, *atime, *size)
            }
            FileOp::AcquireLock { path, holder_id, ttl_secs } => {
                self.try_acquire_lock(path, holder_id, *ttl_secs)
            }
            FileOp::ReleaseLock { path, holder_id } => self.release_lock(path, holder_id),
            FileOp::RenewLock { path, holder_id, ttl_secs } => {
                self.renew_lock(path, holder_id, *ttl_secs)
            }
        }
    }

    // ── Internal mutators ─────────────────────────────────────────────────

    fn resolve_parent<'a>(&self, path: &'a str) -> Result<(Ino, &'a str)> {
        let path = path.trim_start_matches('/');
        let (parent_path, name) = match path.rfind('/') {
            Some(pos) => (&path[..pos], &path[pos + 1..]),
            None => ("", path),
        };
        let parent_ino =
            if parent_path.is_empty() { ROOT_INO } else { self.resolve_path(parent_path)? };
        Ok((parent_ino, name))
    }

    fn resolve_path(&self, path: &str) -> Result<Ino> {
        let mut ino = ROOT_INO;
        for part in path.split('/').filter(|s| !s.is_empty()) {
            ino = self
                .lookup(ino, part)
                .ok_or_else(|| TinyCfsError::NotFound(part.to_string()))?;
        }
        Ok(ino)
    }

    fn create_file(&mut self, path: &str, mode: u32, uid: u32, gid: u32) -> Result<()> {
        let (parent_ino, name) = self.resolve_parent(path)?;
        match self.inodes.get(&parent_ino) {
            Some(Inode::Dir(dir)) => {
                if dir.entries.contains_key(name) {
                    return Err(TinyCfsError::AlreadyExists(name.to_string()));
                }
            }
            Some(_) => return Err(TinyCfsError::NotDirectory),
            None => return Err(TinyCfsError::NotFound(format!("inode {}", parent_ino))),
        }
        let ino = self.alloc_ino();
        let meta = InodeMeta::new(ino, InoKind::RegularFile, mode, uid, gid);
        self.inodes.insert(ino, Inode::File(FileInode { meta, data: Vec::new() }));
        if let Some(Inode::Dir(dir)) = self.inodes.get_mut(&parent_ino) {
            dir.entries.insert(name.to_string(), ino);
            dir.meta.mtime = SystemTime::now();
        }
        self.dirty_inodes.insert(ino);
        self.data_dirty_inodes.insert(ino); // new file — data column must be created
        self.dirty_inodes.insert(parent_ino);
        Ok(())
    }

    fn create_dir(&mut self, path: &str, mode: u32, uid: u32, gid: u32) -> Result<()> {
        let (parent_ino, name) = self.resolve_parent(path)?;
        match self.inodes.get(&parent_ino) {
            Some(Inode::Dir(dir)) => {
                if dir.entries.contains_key(name) {
                    return Err(TinyCfsError::AlreadyExists(name.to_string()));
                }
            }
            Some(_) => return Err(TinyCfsError::NotDirectory),
            None => return Err(TinyCfsError::NotFound(format!("inode {}", parent_ino))),
        }
        let ino = self.alloc_ino();
        let mut meta = InodeMeta::new(ino, InoKind::Directory, mode, uid, gid);
        meta.nlink = 2;
        self.inodes.insert(ino, Inode::Dir(DirInode { meta, parent_ino, entries: HashMap::new() }));
        if let Some(Inode::Dir(dir)) = self.inodes.get_mut(&parent_ino) {
            dir.entries.insert(name.to_string(), ino);
            dir.meta.mtime = SystemTime::now();
            dir.meta.nlink += 1;
        }
        self.dirty_inodes.insert(ino);
        self.dirty_inodes.insert(parent_ino);
        Ok(())
    }

    fn create_symlink(&mut self, path: &str, target: &str, uid: u32, gid: u32) -> Result<()> {
        let (parent_ino, name) = self.resolve_parent(path)?;
        let ino = self.alloc_ino();
        let meta = InodeMeta::new(ino, InoKind::Symlink, 0o777, uid, gid);
        self.inodes.insert(
            ino,
            Inode::Symlink(SymlinkInode { meta, target: target.to_string() }),
        );
        if let Some(Inode::Dir(dir)) = self.inodes.get_mut(&parent_ino) {
            dir.entries.insert(name.to_string(), ino);
            dir.meta.mtime = SystemTime::now();
        }
        self.dirty_inodes.insert(ino);
        self.dirty_inodes.insert(parent_ino);
        Ok(())
    }

    fn write_file(&mut self, path: &str, offset: u64, data: &[u8]) -> Result<()> {
        // Enforce per-file size limit in the state machine so the check is
        // atomic with the write and cannot be raced by concurrent proposals
        // from other nodes (TOCTOU).  The FUSE layer also pre-checks this as
        // an advisory fast-path, but this check is the authoritative one.
        let end = offset + data.len() as u64;
        if self.max_file_size_bytes > 0 && end > self.max_file_size_bytes {
            return Err(TinyCfsError::FileTooLarge { limit: self.max_file_size_bytes });
        }

        // Auto-create the file if it does not exist (upsert semantics).
        if self.resolve_path(path).is_err() {
            self.create_file(path, 0o644, 0, 0)?;
        }
        let ino = self.resolve_path(path)?;
        match self.inodes.get_mut(&ino) {
            Some(Inode::File(f)) => {
                let old_size = f.data.len() as u64;
                // Enforce total filesystem size limit.
                let growth = end.saturating_sub(old_size);
                if self.max_fs_size_bytes > 0
                    && self.total_data_bytes.saturating_add(growth) > self.max_fs_size_bytes
                {
                    return Err(TinyCfsError::NoSpace);
                }
                let end_usize = end as usize;
                if end_usize > f.data.len() {
                    f.data.resize(end_usize, 0);
                }
                f.data[offset as usize..end_usize].copy_from_slice(data);
                f.meta.mtime = SystemTime::now();
                let new_size = f.data.len() as u64;
                self.total_data_bytes =
                    self.total_data_bytes.saturating_add(new_size).saturating_sub(old_size);
                self.dirty_inodes.insert(ino);
                self.data_dirty_inodes.insert(ino);
                Ok(())
            }
            Some(_) => Err(TinyCfsError::IsDirectory),
            None => Err(TinyCfsError::NotFound(path.to_string())),
        }
    }

    fn truncate(&mut self, path: &str, size: u64) -> Result<()> {
        let ino = self.resolve_path(path)?;
        match self.inodes.get_mut(&ino) {
            Some(Inode::File(f)) => {
                let old_size = f.data.len() as u64;
                if size > old_size {
                    // Truncate-up: enforce per-file and total size limits.
                    if self.max_file_size_bytes > 0 && size > self.max_file_size_bytes {
                        return Err(TinyCfsError::FileTooLarge { limit: self.max_file_size_bytes });
                    }
                    let growth = size - old_size;
                    if self.max_fs_size_bytes > 0
                        && self.total_data_bytes.saturating_add(growth) > self.max_fs_size_bytes
                    {
                        return Err(TinyCfsError::NoSpace);
                    }
                }
                f.data.resize(size as usize, 0);
                f.meta.mtime = SystemTime::now();
                self.total_data_bytes =
                    self.total_data_bytes.saturating_add(size).saturating_sub(old_size);
                self.dirty_inodes.insert(ino);
                self.data_dirty_inodes.insert(ino);
                Ok(())
            }
            Some(_) => Err(TinyCfsError::IsDirectory),
            None => Err(TinyCfsError::NotFound(path.to_string())),
        }
    }

    fn unlink(&mut self, path: &str) -> Result<()> {
        let (parent_ino, name) = self.resolve_parent(path)?;
        let child_ino = self
            .lookup(parent_ino, name)
            .ok_or_else(|| TinyCfsError::NotFound(name.to_string()))?;
        if matches!(self.inodes.get(&child_ino), Some(Inode::Dir(_))) {
            return Err(TinyCfsError::IsDirectory);
        }
        if let Some(Inode::File(f)) = self.inodes.get(&child_ino) {
            self.total_data_bytes = self.total_data_bytes.saturating_sub(f.data.len() as u64);
        }
        if let Some(Inode::Dir(dir)) = self.inodes.get_mut(&parent_ino) {
            dir.entries.remove(name);
            dir.meta.mtime = SystemTime::now();
        }
        self.inodes.remove(&child_ino);
        self.dirty_inodes.remove(&child_ino);   // no longer exists
        self.deleted_inodes.insert(child_ino);
        self.dirty_inodes.insert(parent_ino);
        Ok(())
    }

    fn rmdir(&mut self, path: &str) -> Result<()> {
        let (parent_ino, name) = self.resolve_parent(path)?;
        let child_ino = self
            .lookup(parent_ino, name)
            .ok_or_else(|| TinyCfsError::NotFound(name.to_string()))?;
        match self.inodes.get(&child_ino) {
            Some(Inode::Dir(d)) if !d.entries.is_empty() => return Err(TinyCfsError::NotEmpty),
            Some(Inode::Dir(_)) => {}
            Some(_) => return Err(TinyCfsError::NotDirectory),
            None => return Err(TinyCfsError::NotFound(name.to_string())),
        }
        if let Some(Inode::Dir(dir)) = self.inodes.get_mut(&parent_ino) {
            dir.entries.remove(name);
            dir.meta.mtime = SystemTime::now();
            dir.meta.nlink -= 1;
        }
        self.inodes.remove(&child_ino);
        self.dirty_inodes.remove(&child_ino);
        self.deleted_inodes.insert(child_ino);
        self.dirty_inodes.insert(parent_ino);
        Ok(())
    }

    fn rename(&mut self, from: &str, to: &str) -> Result<()> {
        let (from_parent, from_name) = self.resolve_parent(from)?;
        let (to_parent, to_name) = self.resolve_parent(to)?;
        let child_ino = self
            .lookup(from_parent, from_name)
            .ok_or_else(|| TinyCfsError::NotFound(from_name.to_string()))?;
        if let Some(old_ino) = self.lookup(to_parent, to_name) {
            self.inodes.remove(&old_ino);
            self.dirty_inodes.remove(&old_ino);
            self.deleted_inodes.insert(old_ino);
        }
        if let Some(Inode::Dir(dir)) = self.inodes.get_mut(&from_parent) {
            dir.entries.remove(from_name);
            dir.meta.mtime = SystemTime::now();
        }
        if let Some(Inode::Dir(dir)) = self.inodes.get_mut(&to_parent) {
            dir.entries.insert(to_name.to_string(), child_ino);
            dir.meta.mtime = SystemTime::now();
        }
        // Update parent_ino in the moved inode if it's a directory.
        if from_parent != to_parent {
            if let Some(Inode::Dir(child_dir)) = self.inodes.get_mut(&child_ino) {
                child_dir.parent_ino = to_parent;
            }
        }
        self.dirty_inodes.insert(child_ino);
        self.dirty_inodes.insert(from_parent);
        self.dirty_inodes.insert(to_parent);
        Ok(())
    }

    fn setattr(
        &mut self,
        path: &str,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        mtime_secs: Option<u64>,
        atime_secs: Option<u64>,
        size: Option<u64>,
    ) -> Result<()> {
        let ino = self.resolve_path(path)?;
        let now = SystemTime::now();
        if let Some(size) = size {
            if let Some(Inode::File(f)) = self.inodes.get_mut(&ino) {
                let old_size = f.data.len() as u64;
                f.data.resize(size as usize, 0);
                f.meta.mtime = now;
                self.total_data_bytes =
                    self.total_data_bytes.saturating_add(size).saturating_sub(old_size);
                self.data_dirty_inodes.insert(ino);
            }
        }
        if let Some(inode) = self.inodes.get_mut(&ino) {
            let m = inode.meta_mut();
            if let Some(v) = mode { m.mode = v; }
            if let Some(v) = uid { m.uid = v; }
            if let Some(v) = gid { m.gid = v; }
            if let Some(v) = mtime_secs {
                m.mtime = UNIX_EPOCH + Duration::from_secs(v);
            }
            if let Some(v) = atime_secs {
                m.atime = UNIX_EPOCH + Duration::from_secs(v);
            }
            m.ctime = now;
            self.dirty_inodes.insert(ino);
        }
        Ok(())
    }
}
