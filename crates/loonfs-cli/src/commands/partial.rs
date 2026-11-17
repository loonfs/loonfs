//! The partial file a download writes before it is installed at its
//! destination.

use loonfs_types::DestinationBehavior;
use std::io::Write;
use std::path::{Path, PathBuf};

/// Suffix for partial download files.
const PARTIAL_SUFFIX: &str = ".loonfs-partial";

/// Stores a download until it completes.
pub(super) struct PartialDownload {
    file: tempfile::NamedTempFile,
}

impl PartialDownload {
    /// Locks the partial file for this command and empties it.
    ///
    /// A partial file another command holds fails the open before the file
    /// changes. Bytes an earlier, interrupted run left are discarded.
    pub(super) fn open(destination: &Path) -> std::io::Result<Self> {
        let Some(path) = sibling(destination, PARTIAL_SUFFIX) else {
            return Err(std::io::Error::other("destination has no file name"));
        };
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;
        if !fs4::fs_std::FileExt::try_lock_exclusive(&file)? || !names_file(&path, &file)? {
            return Err(std::io::Error::other(
                "another command is downloading to this destination",
            ));
        }
        file.set_len(0)?;
        Ok(Self {
            file: tempfile::NamedTempFile::from_parts(
                file,
                tempfile::TempPath::try_from_path(&path)?,
            ),
        })
    }

    pub(super) fn write_all(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.file.write_all(bytes)
    }

    /// Installs the completed download with an atomic rename.
    pub(super) fn install(
        mut self,
        destination: &Path,
        behavior: DestinationBehavior,
    ) -> std::io::Result<()> {
        self.file.flush()?;
        let persisted = match behavior {
            DestinationBehavior::Replace => self.file.persist(destination),
            DestinationBehavior::NoReplace => self.file.persist_noclobber(destination),
        };
        persisted.map(|_| ()).map_err(|error| error.error)
    }
}

/// Returns whether `path` still names the open `file`.
///
/// The lock is on the partial file's inode, and a command that installs or
/// deletes its partial file moves or unlinks that inode before it unlocks.
/// A second command that opened the path just before then holds a lock on
/// an inode that is no longer the partial file, possibly the installed
/// destination itself.
#[cfg(unix)]
fn names_file(path: &Path, file: &std::fs::File) -> std::io::Result<bool> {
    use std::os::unix::fs::MetadataExt;
    let opened = file.metadata()?;
    match std::fs::metadata(path) {
        Ok(named) => Ok(named.dev() == opened.dev() && named.ino() == opened.ino()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

#[cfg(not(unix))]
fn names_file(_path: &Path, _file: &std::fs::File) -> std::io::Result<bool> {
    Ok(true)
}

/// Returns a hidden sibling of `destination` with the given suffix.
fn sibling(destination: &Path, suffix: &str) -> Option<PathBuf> {
    let file_name = destination.file_name()?;
    let mut name = std::ffi::OsString::from(".");
    name.push(file_name);
    name.push(suffix);
    Some(parent_of(destination).join(name))
}

/// Returns the destination directory, or the working directory for a bare name.
pub(super) fn parent_of(destination: &Path) -> &Path {
    destination
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_second_download_to_one_destination_fails_before_it_truncates() {
        let dir = tempfile::tempdir().expect("tempdir");
        let destination = dir.path().join("file.bin");
        let partial_path = sibling(&destination, PARTIAL_SUFFIX).expect("partial path");

        let mut first = PartialDownload::open(&destination).expect("open the first");
        first.write_all(b"0123").expect("write");

        let error = PartialDownload::open(&destination)
            .err()
            .expect("the second download must fail");
        assert_eq!(
            error.to_string(),
            "another command is downloading to this destination"
        );
        assert_eq!(std::fs::read(&partial_path).expect("read partial"), b"0123");

        first.write_all(b"456789").expect("write");
        first
            .install(&destination, DestinationBehavior::Replace)
            .expect("install");
        assert_eq!(
            std::fs::read(&destination).expect("read destination"),
            b"0123456789"
        );
    }
}
