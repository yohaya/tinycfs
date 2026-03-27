/// In-memory filesystem state machine.
///
/// This is the read replica kept on every node. It is updated by applying
/// committed Raft log entries in order. Reads go directly here (no network).
/// Writes are proposed to Raft and applied here once committed.
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH, Duration};

use crate::cluster::message::FileOp;
use crate::error::{Result, TinyCfsError};
use crate::fs::inode::{DirInode, FileInode, Ino, InoKind, Inode, InodeMeta, SymlinkInode};

/// Root inode is always 1 (FUSE convention).
pub const ROOT_INO: Ino = 1;

pub struct FileStore {
    inodes: HashMap<Ino, Inode>,
    next_ino: Ino,
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
        let root = Inode::Dir(DirInode {
            meta: root_meta,
            entries: HashMap::new(),
        });
        inodes.insert(ROOT_INO, root);

        FileStore {
            inodes,
            next_ino: 2,
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

    pub fn readdir(&self, ino: Ino) -> Result<Vec<(String, Ino, InoKind)>> {
        match self.inodes.get(&ino) {
            Some(Inode::Dir(dir)) => {
                let mut entries = vec![
                    (".".into(), ino, InoKind::Directory),
                    ("..".into(), ino, InoKind::Directory), // simplified
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
            FileOp::Write { path, offset, data } => {
                self.write_file(path, *offset, data)
            }
            FileOp::Truncate { path, size } => self.truncate(path, *size),
            FileOp::Unlink { path } => self.unlink(path),
            FileOp::Rmdir { path } => self.rmdir(path),
            FileOp::Rename { from, to } => self.rename(from, to),
            FileOp::SetAttr { path, mode, uid, gid, mtime, atime, size } => {
                self.setattr(path, *mode, *uid, *gid, *mtime, *atime, *size)
            }
        }
    }

    // ── Internal mutators ─────────────────────────────────────────────────

    /// Resolve a path to (parent_ino, filename, Option<child_ino>).
    fn resolve_parent<'a>(&self, path: &'a str) -> Result<(Ino, &'a str)> {
        let path = path.trim_start_matches('/');
        let (parent_path, name) = match path.rfind('/') {
            Some(pos) => (&path[..pos], &path[pos + 1..]),
            None => ("", path),
        };

        let parent_ino = if parent_path.is_empty() {
            ROOT_INO
        } else {
            self.resolve_path(parent_path)?
        };

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

        if let Some(Inode::Dir(dir)) = self.inodes.get(&parent_ino) {
            if dir.entries.contains_key(name) {
                return Err(TinyCfsError::AlreadyExists(name.to_string()));
            }
        }

        let ino = self.alloc_ino();
        let meta = InodeMeta::new(ino, InoKind::RegularFile, mode, uid, gid);
        self.inodes.insert(ino, Inode::File(FileInode { meta, data: Vec::new() }));

        if let Some(Inode::Dir(dir)) = self.inodes.get_mut(&parent_ino) {
            dir.entries.insert(name.to_string(), ino);
            dir.meta.mtime = SystemTime::now();
        }
        Ok(())
    }

    fn create_dir(&mut self, path: &str, mode: u32, uid: u32, gid: u32) -> Result<()> {
        let (parent_ino, name) = self.resolve_parent(path)?;

        if let Some(Inode::Dir(dir)) = self.inodes.get(&parent_ino) {
            if dir.entries.contains_key(name) {
                return Err(TinyCfsError::AlreadyExists(name.to_string()));
            }
        }

        let ino = self.alloc_ino();
        let mut meta = InodeMeta::new(ino, InoKind::Directory, mode, uid, gid);
        meta.nlink = 2;
        self.inodes.insert(ino, Inode::Dir(DirInode { meta, entries: HashMap::new() }));

        if let Some(Inode::Dir(dir)) = self.inodes.get_mut(&parent_ino) {
            dir.entries.insert(name.to_string(), ino);
            dir.meta.mtime = SystemTime::now();
            dir.meta.nlink += 1;
        }
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
        Ok(())
    }

    fn write_file(&mut self, path: &str, offset: u64, data: &[u8]) -> Result<()> {
        let ino = self.resolve_path(path)?;
        match self.inodes.get_mut(&ino) {
            Some(Inode::File(f)) => {
                let end = (offset as usize) + data.len();
                if end > f.data.len() {
                    f.data.resize(end, 0);
                }
                f.data[offset as usize..end].copy_from_slice(data);
                f.meta.mtime = SystemTime::now();
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
                f.data.resize(size as usize, 0);
                f.meta.mtime = SystemTime::now();
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

        // Must not be a directory
        if matches!(self.inodes.get(&child_ino), Some(Inode::Dir(_))) {
            return Err(TinyCfsError::IsDirectory);
        }

        if let Some(Inode::Dir(dir)) = self.inodes.get_mut(&parent_ino) {
            dir.entries.remove(name);
            dir.meta.mtime = SystemTime::now();
        }
        self.inodes.remove(&child_ino);
        Ok(())
    }

    fn rmdir(&mut self, path: &str) -> Result<()> {
        let (parent_ino, name) = self.resolve_parent(path)?;
        let child_ino = self
            .lookup(parent_ino, name)
            .ok_or_else(|| TinyCfsError::NotFound(name.to_string()))?;

        match self.inodes.get(&child_ino) {
            Some(Inode::Dir(d)) if !d.entries.is_empty() => {
                return Err(TinyCfsError::NotEmpty);
            }
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
        Ok(())
    }

    fn rename(&mut self, from: &str, to: &str) -> Result<()> {
        let (from_parent, from_name) = self.resolve_parent(from)?;
        let (to_parent, to_name) = self.resolve_parent(to)?;

        let child_ino = self
            .lookup(from_parent, from_name)
            .ok_or_else(|| TinyCfsError::NotFound(from_name.to_string()))?;

        // Remove existing target if present (overwrite)
        if let Some(old_ino) = self.lookup(to_parent, to_name) {
            self.inodes.remove(&old_ino);
        }

        // Move in source directory
        if let Some(Inode::Dir(dir)) = self.inodes.get_mut(&from_parent) {
            dir.entries.remove(from_name);
            dir.meta.mtime = SystemTime::now();
        }

        // Insert in dest directory
        if let Some(Inode::Dir(dir)) = self.inodes.get_mut(&to_parent) {
            dir.entries.insert(to_name.to_string(), child_ino);
            dir.meta.mtime = SystemTime::now();
        }
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
                f.data.resize(size as usize, 0);
                f.meta.mtime = now;
            }
        }

        if let Some(inode) = self.inodes.get_mut(&ino) {
            let m = inode.meta_mut();
            if let Some(v) = mode { m.mode = v; }
            if let Some(v) = uid  { m.uid = v; }
            if let Some(v) = gid  { m.gid = v; }
            if let Some(v) = mtime_secs {
                m.mtime = UNIX_EPOCH + Duration::from_secs(v);
            }
            if let Some(v) = atime_secs {
                m.atime = UNIX_EPOCH + Duration::from_secs(v);
            }
            m.ctime = now;
        }
        Ok(())
    }
}
