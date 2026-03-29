use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// Inode number (1-based; 1 is always the root directory).
pub type Ino = u64;

/// File type mirroring `fuser::FileType`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum InoKind {
    RegularFile,
    Directory,
    Symlink,
}

// ── SystemTime serde helper ───────────────────────────────────────────────────
// Serialize as u64 nanoseconds since UNIX_EPOCH for snapshot portability.
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

/// Metadata common to all inode types.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InodeMeta {
    pub ino: Ino,
    pub kind: InoKind,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub nlink: u32,
    #[serde(with = "serde_systemtime")]
    pub atime: SystemTime,
    #[serde(with = "serde_systemtime")]
    pub mtime: SystemTime,
    #[serde(with = "serde_systemtime")]
    pub ctime: SystemTime,
}

impl InodeMeta {
    pub fn new(ino: Ino, kind: InoKind, mode: u32, uid: u32, gid: u32) -> Self {
        let now = SystemTime::now();
        InodeMeta { ino, kind, mode, uid, gid, nlink: 1, atime: now, mtime: now, ctime: now }
    }
}

/// A directory inode.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirInode {
    pub meta: InodeMeta,
    /// Inode number of the parent directory (used for ".." in readdir).
    /// The root directory points to itself.
    pub parent_ino: Ino,
    /// name → child inode number
    pub entries: HashMap<String, Ino>,
}

/// A regular file inode.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SymlinkInode {
    pub meta: InodeMeta,
    pub target: String,
}

/// Unified inode container.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Inode {
    File(FileInode),
    Dir(DirInode),
    Symlink(SymlinkInode),
}

/// Split representation for SQLite persistence: inode metadata without the
/// embedded file data blob.  Stored in the `meta` column of `fs_inodes`.
///
/// Separating meta from data means metadata-only mutations (rename, chmod,
/// setattr) only rewrite the tiny meta blob (~60 bytes), not the full file
/// content, regardless of file size.  File data lives in the `data` column
/// and is only rewritten when the content actually changes (write/truncate).
#[derive(Debug, Serialize, Deserialize)]
pub enum InodePersistMeta {
    File(InodeMeta),
    Dir(DirInode),
    Symlink(SymlinkInode),
}

impl InodePersistMeta {
    pub fn from_inode(inode: &Inode) -> Self {
        match inode {
            Inode::File(f)    => InodePersistMeta::File(f.meta.clone()),
            Inode::Dir(d)     => InodePersistMeta::Dir(d.clone()),
            Inode::Symlink(s) => InodePersistMeta::Symlink(s.clone()),
        }
    }

    pub fn into_inode(self, data: Option<Vec<u8>>) -> Inode {
        match self {
            InodePersistMeta::File(meta) =>
                Inode::File(FileInode { meta, data: data.unwrap_or_default() }),
            InodePersistMeta::Dir(d)     => Inode::Dir(d),
            InodePersistMeta::Symlink(s) => Inode::Symlink(s),
        }
    }
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
        // 128 KiB preferred I/O size: tools like cp/rsync use st_blksize to
        // size their read/write buffers.  At 4 KiB each write was a separate
        // Raft round-trip (~400 KB/s at 5 ms RTT); at 128 KiB we get 32x
        // fewer proposals per MB → ~12.8 MB/s on the same link.
        blksize: 128 * 1024,
        flags: 0,
    }
}
