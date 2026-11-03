//! Files under the CLI's state directory that one command owns at a time and
//! replaces atomically: the upload journals and the merge records.

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

/// A JSON state file that one command owns until this value is dropped.
#[derive(Debug)]
pub(crate) struct OwnedStateFile {
    path: PathBuf,
    /// A lock on a sidecar file, whose inode stays the same when the state
    /// file is replaced or removed, so neither can bypass the lock.
    _ownership: File,
}

impl OwnedStateFile {
    /// Creates the file's directory and locks the file for this command, or
    /// fails when another command owns `what` the file records.
    pub(crate) fn open(path: PathBuf, what: &str) -> io::Result<Self> {
        std::fs::create_dir_all(parent(&path))?;
        let ownership = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path.with_extension("lock"))?;
        if !fs4::fs_std::FileExt::try_lock_exclusive(&ownership)? {
            return Err(io::Error::other(format!(
                "another command owns this {what}"
            )));
        }
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

    /// Replaces the file with `value` durably: it writes a temporary file in
    /// the same directory, syncs it, renames it over the file, and syncs the
    /// directory.
    pub(crate) fn write<T: Serialize>(&self, value: &T) -> io::Result<()> {
        let mut temporary = tempfile::NamedTempFile::new_in(parent(&self.path))?;
        serde_json::to_writer(&mut temporary, value)?;
        temporary.flush()?;
        temporary.as_file().sync_all()?;
        temporary.persist(&self.path).map_err(|error| error.error)?;
        self.sync_directory()
    }

    /// Removes the file durably. A file that doesn't exist is already
    /// removed.
    pub(crate) fn remove(&self) -> io::Result<()> {
        match std::fs::remove_file(&self.path) {
            Ok(()) => self.sync_directory(),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        }
    }

    fn sync_directory(&self) -> io::Result<()> {
        #[cfg(unix)]
        File::open(parent(&self.path))?.sync_all()?;
        Ok(())
    }
}

fn parent(path: &Path) -> &Path {
    path.parent()
        .expect("a state file path should have a parent directory")
}
