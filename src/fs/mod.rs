pub mod inode;
pub mod store;

use std::ffi::OsStr;
use std::time::{Duration, UNIX_EPOCH};

use fuser::{
    FileAttr, FileType, Filesystem, KernelConfig, ReplyAttr, ReplyCreate, ReplyData,
    ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyWrite, Request,
};
use parking_lot::RwLock;
use std::sync::Arc;
use tokio::runtime::Handle;
use tracing::{debug, warn};

use crate::cluster::message::FileOp;
use crate::consensus::Consensus;
use crate::error::to_errno;
use crate::fs::inode::{to_fuser_attr, to_fuser_type, Inode, InoKind};
use crate::fs::store::{FileStore, ROOT_INO};

/// FUSE filesystem for TinyCFS.
///
/// Read operations go directly to the local in-memory store.
/// Write operations are proposed to Raft and applied once committed.
pub struct TinyCfs {
    handle: Handle,
    consensus: Consensus,
    store: Arc<RwLock<FileStore>>,
}

impl TinyCfs {
    pub fn new(handle: Handle, consensus: Consensus, store: Arc<RwLock<FileStore>>) -> Self {
        TinyCfs { handle, consensus, store }
    }

    /// Derive the full path string for a child of `parent` with `name`.
    fn path_for(&self, parent: u64, name: &OsStr) -> Option<String> {
        let name_str = name.to_str()?;
        let path = self.inode_path(parent, name_str)?;
        Some(path)
    }

    /// Walk upward from inode to construct an absolute path. Simplified O(n)
    /// implementation — performance is fine for small filesystems.
    fn inode_path(&self, ino: u64, child_name: &str) -> Option<String> {
        // Build path component by component
        let store = self.store.read();
        let mut segments: Vec<String> = vec![child_name.to_string()];
        // For a simple implementation we track inode → parent via a reverse map.
        // Since we don't maintain that here, we derive paths by searching.
        // A proper implementation would maintain a parent pointer.
        // For now, return /child_name for root children, or use a BFS search.
        Some(self.reconstruct_path(&store, ino, child_name))
    }

    fn reconstruct_path(&self, store: &FileStore, parent_ino: u64, child_name: &str) -> String {
        // Walk up the tree to build an absolute path
        let parent_path = self.ino_to_path(store, parent_ino);
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
        // Search all directories for this inode (simplified)
        // A proper implementation maintains a reverse map
        self.search_path(store, ROOT_INO, ino, "/")
            .unwrap_or_else(|| "/".to_string())
    }

    fn search_path(&self, store: &FileStore, dir_ino: u64, target: u64, prefix: &str) -> Option<String> {
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
            None => { reply.error(libc::ENOENT); return; }
        };

        let store = self.store.read();
        match store.lookup(parent, name_str) {
            Some(ino) => {
                if let Some(inode) = store.get(ino) {
                    reply.entry(&TTL, &to_fuser_attr(inode), 0);
                } else {
                    reply.error(libc::ENOENT);
                }
            }
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
        // Stateless open: no file handles tracked
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
                let end = (offset + size as usize).min(f.data.len());
                if offset >= f.data.len() {
                    reply.data(&[]);
                } else {
                    reply.data(&f.data[offset..end]);
                }
            }
            Some(_) => reply.error(libc::EISDIR),
            None => reply.error(libc::ENOENT),
        }
    }

    fn write(
        &mut self,
        req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        let path = self.ino_path(ino);
        let op = FileOp::Write {
            path,
            offset: offset as u64,
            data: data.to_vec(),
        };
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

        let atime_secs = atime.map(|t| match t {
            fuser::TimeOrNow::SpecificTime(st) => {
                st.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
            }
            fuser::TimeOrNow::Now => {
                std::time::SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs()
            }
        });

        let mtime_secs = mtime.map(|t| match t {
            fuser::TimeOrNow::SpecificTime(st) => {
                st.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
            }
            fuser::TimeOrNow::Now => {
                std::time::SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs()
            }
        });

        let op = FileOp::SetAttr {
            path,
            mode,
            uid,
            gid,
            mtime: mtime_secs,
            atime: atime_secs,
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
            None => { reply.error(libc::EINVAL); return; }
        };
        let op = FileOp::CreateFile {
            path,
            mode: mode & 0o7777,
            uid: req.uid(),
            gid: req.gid(),
        };
        let consensus = self.consensus.clone();
        match self.handle.block_on(consensus.propose(op)) {
            Ok(()) => {
                // Find the newly created inode
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
            None => { reply.error(libc::EINVAL); return; }
        };
        let op = FileOp::CreateDir {
            path,
            mode: mode & 0o7777,
            uid: req.uid(),
            gid: req.gid(),
        };
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
            None => { reply.error(libc::EINVAL); return; }
        };
        let target_str = match target.to_str() {
            Some(s) => s.to_string(),
            None => { reply.error(libc::EINVAL); return; }
        };
        let op = FileOp::CreateSymlink {
            path,
            target: target_str,
            uid: req.uid(),
            gid: req.gid(),
        };
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
            None => { reply.error(libc::EINVAL); return; }
        };
        let op = FileOp::Unlink { path };
        let consensus = self.consensus.clone();
        match self.handle.block_on(consensus.propose(op)) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(to_errno(&e)),
        }
    }

    fn rmdir(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let path = match self.path_for(parent, name) {
            Some(p) => p,
            None => { reply.error(libc::EINVAL); return; }
        };
        let op = FileOp::Rmdir { path };
        let consensus = self.consensus.clone();
        match self.handle.block_on(consensus.propose(op)) {
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
            None => { reply.error(libc::EINVAL); return; }
        };
        let to = match self.path_for(new_parent, new_name) {
            Some(p) => p,
            None => { reply.error(libc::EINVAL); return; }
        };
        let op = FileOp::Rename { from, to };
        let consensus = self.consensus.clone();
        match self.handle.block_on(consensus.propose(op)) {
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
                for (i, (name, child_ino, kind)) in entries.iter().enumerate().skip(offset as usize) {
                    let ftype = to_fuser_type(&kind);
                    if reply.add(*child_ino, (i + 1) as i64, ftype, &name) {
                        break; // buffer full
                    }
                }
                reply.ok();
            }
            Err(e) => reply.error(to_errno(&e)),
        }
    }

    fn statfs(&mut self, _req: &Request<'_>, _ino: u64, reply: fuser::ReplyStatfs) {
        reply.statfs(
            /* blocks */ 1_000_000,
            /* bfree  */ 900_000,
            /* bavail */ 900_000,
            /* files  */ 1_000_000,
            /* ffree  */ 999_000,
            /* bsize  */ 4096,
            /* namelen */ 255,
            /* frsize */ 4096,
        );
    }
}
