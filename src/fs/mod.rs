pub mod inode;
pub mod store;

use std::ffi::OsStr;
use std::time::{Duration, Instant, UNIX_EPOCH};

use fuser::{
    Filesystem, KernelConfig, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty,
    ReplyEntry, ReplyOpen, ReplyWrite, Request,
};
use parking_lot::RwLock;
use std::sync::Arc;
use tokio::runtime::Handle;
use tracing::warn;

use crate::cluster::message::FileOp;
use crate::consensus::Consensus;
use crate::error::{to_errno, TinyCfsError};
use crate::fs::inode::{to_fuser_attr, to_fuser_type, Inode};
use crate::fs::store::{FileStore, ROOT_INO};

/// FUSE filesystem for TinyCFS.
///
/// Read operations go directly to the local in-memory store.
/// Write operations are proposed to Raft and applied once committed.
pub struct TinyCfs {
    handle: Handle,
    consensus: Consensus,
    store: Arc<RwLock<FileStore>>,
    /// Node name used as part of the lock holder ID.
    node_name: String,
    /// Maximum file size in bytes (configurable via Config).
    max_file_size: u64,
    /// Maximum total filesystem size in bytes across all files.
    max_fs_size: u64,
}

impl TinyCfs {
    pub fn new(
        handle: Handle,
        consensus: Consensus,
        store: Arc<RwLock<FileStore>>,
        node_name: String,
        max_file_size: u64,
        max_fs_size: u64,
    ) -> Self {
        TinyCfs { handle, consensus, store, node_name, max_file_size, max_fs_size }
    }

    fn path_for(&self, parent: u64, name: &OsStr) -> Option<String> {
        let name_str = name.to_str()?;
        Some(self.reconstruct_path(parent, name_str))
    }

    fn reconstruct_path(&self, parent_ino: u64, child_name: &str) -> String {
        let store = self.store.read();
        let parent_path = self.ino_to_path(&store, parent_ino);
        if parent_path == "/" {
            format!("/{}", child_name)
        } else {
            format!("{}/{}", parent_path, child_name)
        }
    }

    fn ino_to_path(&self, store: &FileStore, ino: u64) -> String {
        if ino == ROOT_INO {
            return "/".to_string();
        }
        self.search_path(store, ROOT_INO, ino, "/").unwrap_or_else(|| "/".to_string())
    }

    fn search_path(
        &self,
        store: &FileStore,
        dir_ino: u64,
        target: u64,
        prefix: &str,
    ) -> Option<String> {
        if let Some(Inode::Dir(dir)) = store.get(dir_ino) {
            for (name, &child_ino) in &dir.entries {
                let path = if prefix == "/" {
                    format!("/{}", name)
                } else {
                    format!("{}/{}", prefix, name)
                };
                if child_ino == target {
                    return Some(path);
                }
                if let Some(Inode::Dir(_)) = store.get(child_ino) {
                    if let Some(found) = self.search_path(store, child_ino, target, &path) {
                        return Some(found);
                    }
                }
            }
        }
        None
    }

    fn ino_path(&self, ino: u64) -> String {
        let store = self.store.read();
        self.ino_to_path(&store, ino)
    }

    /// Returns `true` when the cluster is operational and can commit writes.
    ///
    /// For Raft: true when this node is the leader, or is a follower that
    ///           knows the current leader (writes are forwarded).
    /// For Totem: true when the token ring has a voting quorum.
    ///
    /// When false the filesystem is effectively read-only: every mutating
    /// FUSE operation immediately returns EROFS so that the caller does not
    /// block for the full 30-second propose() timeout.
    #[inline]
    fn cluster_writeable(&self) -> bool {
        self.consensus.has_quorum()
    }
}

const TTL: Duration = Duration::from_secs(1);

impl Filesystem for TinyCfs {
    fn init(&mut self, _req: &Request<'_>, config: &mut KernelConfig) -> Result<(), libc::c_int> {
        // Allow the kernel to send writes up to 4 MiB in a single FUSE call.
        // Fuser defaults to its MAX_WRITE_SIZE (16 MiB) but being explicit
        // here documents the choice and aligns with blksize below.
        let _ = config.set_max_write(4 * 1024 * 1024);
        Ok(())
    }

    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let name_str = match name.to_str() {
            Some(s) => s,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };
        let store = self.store.read();
        match store.lookup(parent, name_str) {
            Some(ino) => match store.get(ino) {
                Some(inode) => reply.entry(&TTL, &to_fuser_attr(inode), 0),
                None => reply.error(libc::ENOENT),
            },
            None => reply.error(libc::ENOENT),
        }
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyAttr) {
        let store = self.store.read();
        match store.get(ino) {
            Some(inode) => reply.attr(&TTL, &to_fuser_attr(inode)),
            None => reply.error(libc::ENOENT),
        }
    }

    fn readlink(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyData) {
        let store = self.store.read();
        match store.get(ino) {
            Some(Inode::Symlink(s)) => reply.data(s.target.as_bytes()),
            _ => reply.error(libc::EINVAL),
        }
    }

    fn open(&mut self, _req: &Request<'_>, _ino: u64, _flags: i32, reply: ReplyOpen) {
        reply.opened(0, 0);
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        // Reads are served from the local in-memory store without a network
        // round-trip (linearizable reads would require a leader heartbeat check
        // or a read-only log entry — a future improvement). A partitioned old
        // leader may therefore serve stale data until it rejoins the cluster.
        let store = self.store.read();
        match store.get(ino) {
            Some(Inode::File(f)) => {
                let offset = offset as usize;
                if offset >= f.data.len() {
                    reply.data(&[]);
                } else {
                    let end = (offset + size as usize).min(f.data.len());
                    reply.data(&f.data[offset..end]);
                }
            }
            Some(_) => reply.error(libc::EISDIR),
            None => reply.error(libc::ENOENT),
        }
    }

    fn write(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        if !self.cluster_writeable() {
            reply.error(libc::EROFS);
            return;
        }
        // Enforce per-file size limit before proposing.
        let end = offset as u64 + data.len() as u64;
        if end > self.max_file_size {
            reply.error(libc::EFBIG);
            return;
        }

        // Enforce total filesystem size limit before proposing.
        let (path, would_exceed) = {
            let store = self.store.read();
            let path = self.ino_to_path(&store, ino);
            let current_file_size = match store.get(ino) {
                Some(crate::fs::inode::Inode::File(f)) => f.data.len() as u64,
                _ => 0,
            };
            let growth = end.saturating_sub(current_file_size);
            let would_exceed =
                store.total_data_bytes().saturating_add(growth) > self.max_fs_size;
            (path, would_exceed)
        };
        if would_exceed {
            reply.error(libc::ENOSPC);
            return;
        }
        let op = FileOp::Write { path: path.clone(), offset: offset as u64, data: data.to_vec() };
        let consensus = self.consensus.clone();
        let data_len = data.len();
        match self.handle.block_on(consensus.propose(op)) {
            Ok(()) => reply.written(data_len as u32),
            Err(e) => {
                warn!("FUSE write failed for {:?} at offset {}: {}", path, offset, e);
                reply.error(to_errno(&e));
            }
        }
    }

    fn setattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<fuser::TimeOrNow>,
        mtime: Option<fuser::TimeOrNow>,
        _ctime: Option<std::time::SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<std::time::SystemTime>,
        _chgtime: Option<std::time::SystemTime>,
        _bkuptime: Option<std::time::SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        if !self.cluster_writeable() {
            reply.error(libc::EROFS);
            return;
        }
        let path = self.ino_path(ino);

        let to_secs = |t: fuser::TimeOrNow| match t {
            fuser::TimeOrNow::SpecificTime(st) => {
                st.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
            }
            fuser::TimeOrNow::Now => std::time::SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        };

        let op = FileOp::SetAttr {
            path,
            mode,
            uid,
            gid,
            mtime: mtime.map(to_secs),
            atime: atime.map(to_secs),
            size,
        };

        let consensus = self.consensus.clone();
        match self.handle.block_on(consensus.propose(op)) {
            Ok(()) => {
                let store = self.store.read();
                match store.get(ino) {
                    Some(inode) => reply.attr(&TTL, &to_fuser_attr(inode)),
                    None => reply.error(libc::ENOENT),
                }
            }
            Err(e) => {
                warn!("FUSE setattr failed for ino {}: {}", ino, e);
                reply.error(to_errno(&e));
            }
        }
    }

    fn create(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        if !self.cluster_writeable() {
            reply.error(libc::EROFS);
            return;
        }
        let name_str = match name.to_str() {
            Some(s) => s,
            None => { reply.error(libc::EINVAL); return; }
        };
        let path = match self.path_for(parent, name) {
            Some(p) => p,
            None => {
                reply.error(libc::EINVAL);
                return;
            }
        };

        // If the file already exists in our store, handle O_EXCL / open-existing
        // without a Raft round-trip (creating a new log entry for an already-existing
        // file would fail with AlreadyExists once applied, returning EIO to the caller).
        {
            let store = self.store.read();
            if let Some(ino) = store.lookup(parent, name_str) {
                if flags & libc::O_EXCL != 0 {
                    reply.error(libc::EEXIST);
                    return;
                }
                // File exists and O_EXCL not set: open it (truncation handled below).
                if let Some(inode) = store.get(ino) {
                    // If O_TRUNC is set, propose a truncate through consensus so all nodes
                    // see the truncation; otherwise just return the existing attrs.
                    if flags & libc::O_TRUNC != 0 {
                        let truncate_path = path.clone();
                        drop(store);
                        let consensus = self.consensus.clone();
                        let op = FileOp::Truncate { path: truncate_path, size: 0 };
                        if let Err(e) = self.handle.block_on(consensus.propose(op)) {
                            warn!("FUSE create O_TRUNC failed for {:?}: {}", path, e);
                            reply.error(to_errno(&e));
                            return;
                        }
                        let store = self.store.read();
                        match store.get(ino) {
                            Some(inode) => { reply.created(&TTL, &to_fuser_attr(inode), 0, 0, 0); }
                            None => { reply.error(libc::ENOENT); }
                        }
                    } else {
                        reply.created(&TTL, &to_fuser_attr(inode), 0, 0, 0);
                    }
                    return;
                }
            }
        }

        let op = FileOp::CreateFile { path: path.clone(), mode: mode & 0o7777, uid: req.uid(), gid: req.gid() };
        let consensus = self.consensus.clone();
        match self.handle.block_on(consensus.propose(op)) {
            Ok(()) => {
                let store = self.store.read();
                match store.lookup(parent, name_str).and_then(|ino| store.get(ino)) {
                    Some(inode) => reply.created(&TTL, &to_fuser_attr(inode), 0, 0, 0),
                    None => {
                        warn!("FUSE create: file {:?} not in store after successful propose", path);
                        reply.error(libc::ENOENT);
                    }
                }
            }
            Err(e) => {
                warn!("FUSE create failed for {:?}: {}", path, e);
                reply.error(to_errno(&e));
            }
        }
    }

    fn mkdir(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        if !self.cluster_writeable() {
            reply.error(libc::EROFS);
            return;
        }
        let path = match self.path_for(parent, name) {
            Some(p) => p,
            None => {
                reply.error(libc::EINVAL);
                return;
            }
        };
        let op =
            FileOp::CreateDir { path, mode: mode & 0o7777, uid: req.uid(), gid: req.gid() };
        let consensus = self.consensus.clone();
        match self.handle.block_on(consensus.propose(op)) {
            Ok(()) => {
                let name_str = name.to_str().unwrap_or("");
                let store = self.store.read();
                match store.lookup(parent, name_str).and_then(|ino| store.get(ino)) {
                    Some(inode) => reply.entry(&TTL, &to_fuser_attr(inode), 0),
                    None => reply.error(libc::ENOENT),
                }
            }
            Err(e) => reply.error(to_errno(&e)),
        }
    }

    fn symlink(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        link_name: &OsStr,
        target: &std::path::Path,
        reply: ReplyEntry,
    ) {
        if !self.cluster_writeable() {
            reply.error(libc::EROFS);
            return;
        }
        let path = match self.path_for(parent, link_name) {
            Some(p) => p,
            None => {
                reply.error(libc::EINVAL);
                return;
            }
        };
        let target_str = match target.to_str() {
            Some(s) => s.to_string(),
            None => {
                reply.error(libc::EINVAL);
                return;
            }
        };
        let op = FileOp::CreateSymlink { path, target: target_str, uid: req.uid(), gid: req.gid() };
        let consensus = self.consensus.clone();
        match self.handle.block_on(consensus.propose(op)) {
            Ok(()) => {
                let name_str = link_name.to_str().unwrap_or("");
                let store = self.store.read();
                match store.lookup(parent, name_str).and_then(|ino| store.get(ino)) {
                    Some(inode) => reply.entry(&TTL, &to_fuser_attr(inode), 0),
                    None => reply.error(libc::ENOENT),
                }
            }
            Err(e) => reply.error(to_errno(&e)),
        }
    }

    fn unlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        if !self.cluster_writeable() {
            reply.error(libc::EROFS);
            return;
        }
        let path = match self.path_for(parent, name) {
            Some(p) => p,
            None => {
                reply.error(libc::EINVAL);
                return;
            }
        };
        let consensus = self.consensus.clone();
        match self.handle.block_on(consensus.propose(FileOp::Unlink { path })) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(to_errno(&e)),
        }
    }

    fn rmdir(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        if !self.cluster_writeable() {
            reply.error(libc::EROFS);
            return;
        }
        let path = match self.path_for(parent, name) {
            Some(p) => p,
            None => {
                reply.error(libc::EINVAL);
                return;
            }
        };
        let consensus = self.consensus.clone();
        match self.handle.block_on(consensus.propose(FileOp::Rmdir { path })) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(to_errno(&e)),
        }
    }

    fn rename(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        new_parent: u64,
        new_name: &OsStr,
        _flags: u32,
        reply: ReplyEmpty,
    ) {
        if !self.cluster_writeable() {
            reply.error(libc::EROFS);
            return;
        }
        let from = match self.path_for(parent, name) {
            Some(p) => p,
            None => {
                reply.error(libc::EINVAL);
                return;
            }
        };
        let to = match self.path_for(new_parent, new_name) {
            Some(p) => p,
            None => {
                reply.error(libc::EINVAL);
                return;
            }
        };
        let consensus = self.consensus.clone();
        match self.handle.block_on(consensus.propose(FileOp::Rename { from, to })) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(to_errno(&e)),
        }
    }

    fn opendir(&mut self, _req: &Request<'_>, _ino: u64, _flags: i32, reply: ReplyOpen) {
        reply.opened(0, 0);
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let store = self.store.read();
        match store.readdir(ino) {
            Ok(entries) => {
                for (i, (name, child_ino, kind)) in
                    entries.iter().enumerate().skip(offset as usize)
                {
                    let ftype = to_fuser_type(kind);
                    if reply.add(*child_ino, (i + 1) as i64, ftype, name) {
                        break;
                    }
                }
                reply.ok();
            }
            Err(e) => reply.error(to_errno(&e)),
        }
    }

    fn statfs(&mut self, _req: &Request<'_>, _ino: u64, reply: fuser::ReplyStatfs) {
        // bsize / frsize = 128 KiB: matches blksize in FileAttr so that df,
        // cp and other tools that inspect f_bsize use the same I/O unit.
        reply.statfs(
            1_000_000, 900_000, 900_000, 1_000_000, 999_000, 128 * 1024, 255, 128 * 1024,
        );
    }

    // ── Distributed file locking ──────────────────────────────────────────

    /// Handles both setlk (non-blocking, sleep=false) and setlkw (blocking, sleep=true).
    ///
    /// Whole-file exclusive locking — byte ranges are ignored.
    /// Lock holder ID = "<node_name>:<lock_owner>" for cluster-uniqueness.
    fn setlk(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        lock_owner: u64,
        _start: u64,
        _end: u64,
        typ: i32,
        _pid: u32,
        sleep: bool,
        reply: ReplyEmpty,
    ) {
        if !self.cluster_writeable() {
            reply.error(libc::EROFS);
            return;
        }
        let path = self.ino_path(ino);
        let holder_id = format!("{}:{}", self.node_name, lock_owner);

        let op = match typ as i32 {
            x if x == libc::F_UNLCK as i32 => FileOp::ReleaseLock { path, holder_id },
            x if x == libc::F_RDLCK as i32 || x == libc::F_WRLCK as i32 => {
                FileOp::AcquireLock { path, holder_id, ttl_secs: 60 }
            }
            _ => {
                reply.error(libc::EINVAL);
                return;
            }
        };

        if sleep {
            // setlkw: block until lock is available or 30 s elapsed.
            // Use the Tokio timer (via block_on) rather than std::thread::sleep
            // so the runtime's timer infrastructure is used and the OS can
            // schedule other work on this thread during the wait interval.
            let deadline = Instant::now() + Duration::from_secs(30);
            let consensus = self.consensus.clone();
            loop {
                match self.handle.block_on(consensus.propose(op.clone())) {
                    Ok(()) => {
                        reply.ok();
                        return;
                    }
                    Err(TinyCfsError::LockContended(_)) if Instant::now() < deadline => {
                        self.handle.block_on(tokio::time::sleep(Duration::from_millis(100)));
                    }
                    Err(e) => {
                        reply.error(to_errno(&e));
                        return;
                    }
                }
            }
        } else {
            // setlk: non-blocking.
            let consensus = self.consensus.clone();
            match self.handle.block_on(consensus.propose(op)) {
                Ok(()) => reply.ok(),
                Err(e) => reply.error(to_errno(&e)),
            }
        }
    }
}
