//! Defines [`MountError`], returned by the engine's operations.

use crate::item::ItemId;
use loonfs_client::ClientError;
use loonfs_types::{ContentId, NamespaceId};
use thiserror::Error;

/// Error returned by a mount's engine.
///
/// The adapter maps each variant to the `errno` that the kernel returns, and
/// `loonfs mount` prints the admission variants as the reason a mount was
/// refused.
#[derive(Debug, Clone, Error)]
#[non_exhaustive]
pub enum MountError {
    /// The mount is read-only, so it accepts no change to the tree.
    #[error("mount is read-only: `{operation}` changes the tree")]
    ReadOnly {
        /// The engine operation that was refused.
        operation: &'static str,
    },
    /// No item in the mount has this ID.
    #[error("item {item} is not in the mount")]
    ItemNotFound {
        /// The item that was asked for.
        item: ItemId,
    },
    /// The operation reads file content, and the item isn't a file.
    #[error("item {item} is not a file")]
    NotAFile {
        /// The item that was asked for.
        item: ItemId,
    },
    /// The host can't serve the mount.
    #[error("host can't serve the mount: {reason}")]
    HostUnsupported {
        /// Why the host can't serve the mount.
        reason: String,
    },
    /// The state directory's filesystem has less free space than the mount
    /// keeps free.
    #[error(
        "state directory `{path}` has {available_bytes} bytes free, and the mount keeps at least \
         {min_free_bytes} bytes free"
    )]
    InsufficientFreeSpace {
        /// The directory whose filesystem was checked.
        path: String,
        /// The free space that the filesystem reported.
        available_bytes: u64,
        /// The free space that the mount keeps (`--cache-min-free`).
        min_free_bytes: u64,
    },
    /// A writable mount needs a namespace that compares names exactly.
    #[error(
        "namespace `{namespace_id}` uses `case_insensitive` naming, and a writable mount requires \
         `case_sensitive` naming"
    )]
    CaseInsensitiveNaming {
        /// The namespace that was refused.
        namespace_id: NamespaceId,
    },
    /// The mount's path isn't a directory.
    #[error("mount path `{path}` is not a directory")]
    NotADirectory {
        /// The mount's path in the namespace.
        path: String,
    },
    /// The mount's root has a child that the mount's control directory would
    /// hide.
    #[error("mount path `{path}` has a child named `.loonfs`, which the mount reserves")]
    ReservedName {
        /// The mount's path in the namespace.
        path: String,
    },
    /// The mounted tree has more entries than the entry budget.
    #[error(
        "mount path `{path}` has more than {max_entries} entries (the scan stopped after \
         {listed_entries}), so it doesn't fit the entry budget"
    )]
    EntryBudgetExceeded {
        /// The mount's path in the namespace.
        path: String,
        /// The entries that the scan listed before it stopped.
        listed_entries: u64,
        /// The entry budget (`--max-entries`).
        max_entries: u64,
    },
    /// A mount of an ACL namespace needs an administrator subject.
    #[error("mounting an ACL namespace requires an administrator subject: {message}")]
    AdministratorRequired {
        /// The server's reason.
        message: String,
    },
    /// The mount's state directory belongs to another mount.
    #[error(
        "state directory `{path}` belongs to a mount of `{namespace_id}` at `{mount_path}`, not \
         this one"
    )]
    StateMismatch {
        /// The state directory.
        path: String,
        /// The namespace that the state belongs to.
        namespace_id: NamespaceId,
        /// The path that the state belongs to.
        mount_path: String,
    },
    /// A request to the server failed. The client's error is boxed, because
    /// it's much larger than the engine's other errors.
    #[error(transparent)]
    Client(Box<ClientError>),
    /// The mount's state database failed.
    #[error("mount state database failed: {message}")]
    Database {
        /// The database's error.
        message: String,
    },
    /// A file in the mount's state directory couldn't be read or written.
    #[error("local i/o on `{path}` failed: {message}")]
    Io {
        /// The file or directory.
        path: String,
        /// The operating system's error.
        message: String,
    },
    /// A cached revision didn't match its checksum, twice.
    #[error("content `{content_id}` didn't match its checksum after it was fetched again")]
    ContentMismatch {
        /// The content whose bytes didn't match.
        content_id: ContentId,
    },
    /// The cache can't store more bytes, so it stopped filling.
    #[error("the content cache is full: {reason}")]
    NoSpace {
        /// Which limit was reached.
        reason: String,
    },
    /// The engine's control task stopped, so the mount serves nothing more.
    #[error("the mount's control task stopped")]
    Stopped,
}

/// Result type returned by the engine.
pub type Result<T> = std::result::Result<T, MountError>;

impl MountError {
    pub(crate) fn io(path: &std::path::Path, error: &std::io::Error) -> Self {
        Self::Io {
            path: path.display().to_string(),
            message: error.to_string(),
        }
    }
}

impl From<ClientError> for MountError {
    fn from(error: ClientError) -> Self {
        Self::Client(Box::new(error))
    }
}

impl From<rusqlite::Error> for MountError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Database {
            message: error.to_string(),
        }
    }
}
