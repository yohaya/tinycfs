use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

/// Inode number (1-based; 1 is always the root directory).
pub type Ino = u64;

/// File type mirroring `fuser::FileType`.
#[derive(Debug, Clone, PartialEq)]
pub enum InoKind {
    RegularFile,
    Directory,
    Symlink,
}

/// Metadata common to all inode types.
#[derive(Debug, Clone)]
pub struct InodeMeta {
    pub ino: Ino,
    pub kind: InoKind,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub nlink: u32,
    pub atime: SystemTime,
    pub mtime: SystemTime,
    pub ctime: SystemTime,
}

impl InodeMeta {
    pub fn new(ino: Ino, kind: InoKind, mode: u32, uid: u32, gid: u32) -> Self {
        let now = SystemTime::now();
        InodeMeta {
            ino,
            kind,
            mode,
            uid,
            gid,
            nlink: 1,
            atime: now,
            mtime: now,
            ctime: now,
        }
    }
}

/// A directory inode.
#[derive(Debug, Clone)]
pub struct DirInode {
    pub meta: InodeMeta,
    /// name → child inode number
    pub entries: HashMap<String, Ino>,
}

/// A regular file inode.
#[derive(Debug, Clone)]
pub struct FileInode {
    pub meta: InodeMeta,
    pub data: Vec<u8>,
}

impl FileInode {
    pub fn size(&self) -> u64 {
        self.data.len() as u64
    }
}

/// A symbolic link inode.
#[derive(Debug, Clone)]
pub struct SymlinkInode {
    pub meta: InodeMeta,
    pub target: String,
}

/// Unified inode container.
#[derive(Debug, Clone)]
pub enum Inode {
    File(FileInode),
    Dir(DirInode),
    Symlink(SymlinkInode),
}

impl Inode {
    pub fn meta(&self) -> &InodeMeta {
        match self {
            Inode::File(f) => &f.meta,
            Inode::Dir(d) => &d.meta,
            Inode::Symlink(s) => &s.meta,
        }
    }

    pub fn meta_mut(&mut self) -> &mut InodeMeta {
        match self {
            Inode::File(f) => &mut f.meta,
            Inode::Dir(d) => &mut d.meta,
            Inode::Symlink(s) => &mut s.meta,
        }
    }

    pub fn kind(&self) -> InoKind {
        self.meta().kind.clone()
    }

    pub fn size(&self) -> u64 {
        match self {
            Inode::File(f) => f.size(),
            Inode::Dir(_) => 4096,
            Inode::Symlink(s) => s.target.len() as u64,
        }
    }
}

/// Convert our InoKind to fuser's FileType.
pub fn to_fuser_type(kind: &InoKind) -> fuser::FileType {
    match kind {
        InoKind::RegularFile => fuser::FileType::RegularFile,
        InoKind::Directory => fuser::FileType::Directory,
        InoKind::Symlink => fuser::FileType::Symlink,
    }
}

/// Build a fuser::FileAttr from an Inode.
pub fn to_fuser_attr(inode: &Inode) -> fuser::FileAttr {
    let m = inode.meta();
    fuser::FileAttr {
        ino: m.ino,
        size: inode.size(),
        blocks: (inode.size() + 511) / 512,
        atime: m.atime,
        mtime: m.mtime,
        ctime: m.ctime,
        crtime: m.ctime,
        kind: to_fuser_type(&m.kind),
        perm: m.mode as u16,
        nlink: m.nlink,
        uid: m.uid,
        gid: m.gid,
        rdev: 0,
        blksize: 4096,
        flags: 0,
    }
}
