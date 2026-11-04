//! Files the CLI locks for one command at a time and replaces durably: the
//! upload journals, the merge records, and the config file.

use crate::config::absolute_env_path;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

const XDG_STATE_SUBDIR: &str = "loonfs";

/// Returns where the CLI keeps one kind of record:
/// `$XDG_STATE_HOME/loonfs/<kind>` when that variable names an absolute
/// directory, and `$HOME/.local/state/loonfs/<kind>` otherwise, in the same
/// order, and for the same reasons, as the config file's location.
pub(crate) fn state_directory(kind: &str) -> Option<PathBuf> {
    if let Some(state_home) = absolute_env_path("XDG_STATE_HOME") {
        return Some(state_home.join(XDG_STATE_SUBDIR).join(kind));
    }
    let home = absolute_env_path("HOME")?;
    Some(home.join(".local/state").join(XDG_STATE_SUBDIR).join(kind))
}

/// Whether [`lock_sidecar`] waits for a lock another command holds or fails
/// at once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LockContention {
    Wait,
    /// Fails with [`io::ErrorKind::WouldBlock`].
    Fail,
}

/// Locks the sidecar `<path>.lock` for this command and returns the open
/// sidecar, which keeps the lock until it is dropped.
///
/// The sidecar's inode stays the same when the file at `path` is replaced or
/// removed, so neither can bypass the lock.
pub(crate) fn lock_sidecar(path: &Path, contention: LockContention) -> io::Result<File> {
    let sidecar = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path.with_extension("lock"))?;
    match contention {
        LockContention::Wait => fs4::fs_std::FileExt::lock_exclusive(&sidecar)?,
        LockContention::Fail => {
            if !fs4::fs_std::FileExt::try_lock_exclusive(&sidecar)? {
                return Err(io::ErrorKind::WouldBlock.into());
            }
        }
    }
    Ok(sidecar)
}

/// Replaces the file at `path` with `bytes` durably: it writes an owner-only
/// temporary file in the same directory, syncs it, renames it over the file,
/// and syncs the directory.
pub(crate) fn replace_durably(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let directory = parent(path);
    let mut temporary = tempfile::NamedTempFile::new_in(directory)?;
    temporary.write_all(bytes)?;
    temporary.as_file().sync_all()?;
    temporary.persist(path).map_err(|error| error.error)?;
    sync_directory(directory)
}

/// A JSON state file that one command owns until this value is dropped.
#[derive(Debug)]
pub(crate) struct OwnedStateFile {
    path: PathBuf,
    /// The lock from [`lock_sidecar`].
    _ownership: File,
}

impl OwnedStateFile {
    /// Creates the file's directory and locks the file for this command, or
    /// fails when another command owns `what` the file records.
    pub(crate) fn open(path: PathBuf, what: &str) -> io::Result<Self> {
        std::fs::create_dir_all(parent(&path))?;
        let ownership =
            lock_sidecar(&path, LockContention::Fail).map_err(|error| match error.kind() {
                io::ErrorKind::WouldBlock => {
                    io::Error::other(format!("another command owns this {what}"))
                }
                _ => error,
            })?;
        Ok(Self {
            path,
            _ownership: ownership,
        })
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    /// Reads and decodes the file, or returns `None` when it doesn't exist.
    pub(crate) fn read<T: DeserializeOwned>(&self) -> io::Result<Option<T>> {
        match std::fs::read(&self.path) {
            Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Replaces the file with `value` through [`replace_durably`].
    pub(crate) fn write<T: Serialize>(&self, value: &T) -> io::Result<()> {
        replace_durably(&self.path, &serde_json::to_vec(value)?)
    }

    /// Removes the file durably. A file that doesn't exist is already
    /// removed.
    pub(crate) fn remove(&self) -> io::Result<()> {
        match std::fs::remove_file(&self.path) {
            Ok(()) => sync_directory(parent(&self.path)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        }
    }
}

fn sync_directory(directory: &Path) -> io::Result<()> {
    #[cfg(unix)]
    File::open(directory)?.sync_all()?;
    Ok(())
}

/// Returns the directory that holds `path`, which is the working directory
/// for a bare file name.
fn parent(path: &Path) -> &Path {
    match path
        .parent()
        .expect("a state file path should have a parent directory")
    {
        directory if directory.as_os_str().is_empty() => Path::new("."),
        directory => directory,
    }
}
