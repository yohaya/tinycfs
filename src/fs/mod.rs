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
}

impl TinyCfs {
    pub fn new(
        handle: Handle,
        consensus: Consensus,
        store: Arc<RwLock<FileStore>>,
        node_name: String,
        max_file_size: u64,
    ) -> Self {
        TinyCfs { handle, consensus, store, node_name, max_file_size }
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
}

const TTL: Duration = Duration::from_secs(1);

impl Filesystem for TinyCfs {
    fn init(&mut self, _req: &Request<'_>, _config: &mut KernelConfig) -> Result<(), libc::c_int> {
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
        // Enforce per-file size limit before proposing.
        let end = offset as u64 + data.len() as u64;
        if end > self.max_file_size {
            reply.error(libc::EFBIG);
            return;
        }

        let path = self.ino_path(ino);
        let op = FileOp::Write { path, offset: offset as u64, data: data.to_vec() };
        let consensus = self.consensus.clone();
        let data_len = data.len();
        match self.handle.block_on(consensus.propose(op)) {
            Ok(()) => reply.written(data_len as u32),
            Err(e) => reply.error(to_errno(&e)),
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
            Err(e) => reply.error(to_errno(&e)),
        }
    }

    fn create(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        let path = match self.path_for(parent, name) {
            Some(p) => p,
            None => {
                reply.error(libc::EINVAL);
                return;
            }
        };
        let op = FileOp::CreateFile { path, mode: mode & 0o7777, uid: req.uid(), gid: req.gid() };
        let consensus = self.consensus.clone();
        match self.handle.block_on(consensus.propose(op)) {
            Ok(()) => {
                let name_str = name.to_str().unwrap_or("");
                let store = self.store.read();
                match store.lookup(parent, name_str).and_then(|ino| store.get(ino)) {
                    Some(inode) => reply.created(&TTL, &to_fuser_attr(inode), 0, 0, 0),
                    None => reply.error(libc::ENOENT),
                }
            }
            Err(e) => reply.error(to_errno(&e)),
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
        reply.statfs(
            1_000_000, 900_000, 900_000, 1_000_000, 999_000, 4096, 255, 4096,
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
            let deadline = Instant::now() + Duration::from_secs(30);
            let consensus = self.consensus.clone();
            loop {
                match self.handle.block_on(consensus.propose(op.clone())) {
                    Ok(()) => {
                        reply.ok();
                        return;
                    }
                    Err(TinyCfsError::LockContended(_)) if Instant::now() < deadline => {
                        std::thread::sleep(Duration::from_millis(100));
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
