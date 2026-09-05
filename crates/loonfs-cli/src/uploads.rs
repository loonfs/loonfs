//! Local journals used to resume interrupted multipart uploads.

use crate::config::absolute_env_path;
use loonfs_api::v0::CompletedUploadPart;
use loonfs_api::{Checksum, ChecksumAlgorithm, UploadId};
use loonfs_client::{MultipartUploadJournal, MultipartUploadResume};
use serde::{Deserialize, Serialize};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// Directory under `$XDG_STATE_HOME`.
const XDG_STATE_SUBDIR: &str = "loonfs";
/// Per-upload journal directory.
const UPLOADS_SUBDIR: &str = "uploads";

/// State required to resume one upload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct UploadState {
    upload_id: UploadId,
    part_size_bytes: u64,
    checksum_algorithm: ChecksumAlgorithm,
    parts: Vec<StatePart>,
    source: SourceIdentity,
}

/// A completed multipart upload part.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StatePart {
    part_number: u32,
    etag: String,
    checksum: Checksum,
}

/// File properties used to detect changes before resuming an upload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SourceIdentity {
    size_bytes: u64,
    modified_ns: i128,
}

impl SourceIdentity {
    /// Reads a file's length and full-precision modification time.
    pub(crate) fn of(path: &Path) -> std::io::Result<Self> {
        let metadata = std::fs::metadata(path)?;
        let modified = metadata.modified()?;
        let modified_ns = match modified.duration_since(std::time::UNIX_EPOCH) {
            Ok(since) => since.as_nanos() as i128,
            Err(before) => -(before.duration().as_nanos() as i128),
        };
        Ok(Self {
            size_bytes: metadata.len(),
            modified_ns,
        })
    }
}

/// Local journal for one upload.
#[derive(Debug)]
pub(crate) struct UploadJournal {
    path: PathBuf,
    source: SourceIdentity,
    /// Latest state, flushed after every change.
    state: Mutex<Option<UploadState>>,
}

impl UploadJournal {
    /// Resolves the journal path. Missing state-directory configuration is an error.
    pub(crate) fn for_upload(
        profile: &str,
        namespace: &str,
        remote_path: &str,
        local_path: &Path,
        source: SourceIdentity,
    ) -> io::Result<Self> {
        let local_path = local_path.canonicalize()?;
        let key = Checksum::sha256(&serde_json::to_vec(&(
            profile,
            namespace,
            remote_path,
            local_path.as_os_str().as_encoded_bytes(),
        ))?)
        .value;
        Ok(Self {
            path: uploads_dir()
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::NotFound,
                        "upload journals require an absolute XDG_STATE_HOME or HOME",
                    )
                })?
                .join(format!("{key}.json")),
            source,
            state: Mutex::new(None),
        })
    }

    /// Loads an existing record. Only a missing file means there is no upload.
    pub(crate) fn resume(&self) -> io::Result<Option<MultipartUploadResume>> {
        let bytes = match std::fs::read(&self.path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(self.error(error)),
        };
        let recorded: UploadState =
            serde_json::from_slice(&bytes).map_err(|error| self.error(error))?;
        if recorded.source != self.source {
            return Err(
                self.error("source file changed; remove this journal to start a new upload")
            );
        }
        let resume = MultipartUploadResume {
            upload_id: recorded.upload_id.clone(),
            part_size_bytes: recorded.part_size_bytes,
            checksum_algorithm: recorded.checksum_algorithm,
            parts: recorded
                .parts
                .iter()
                .map(|part| CompletedUploadPart {
                    part_number: part.part_number,
                    etag: part.etag.clone(),
                    checksum: part.checksum.clone(),
                })
                .collect(),
        };
        *self.lock() = Some(recorded);
        Ok(Some(resume))
    }

    /// Removes an acknowledged upload's record. Removal errors remain visible.
    pub(crate) fn forget(&self) -> io::Result<()> {
        match std::fs::remove_file(&self.path) {
            Ok(()) => self.sync_directory().map_err(|error| self.error(error))?,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(self.error(error)),
        }
        *self.lock() = None;
        Ok(())
    }

    /// Replace the record only after the complete new file reaches disk.
    fn flush(&self, state: &UploadState) -> io::Result<()> {
        let write = || -> io::Result<()> {
            let parent = self.path.parent().expect("journal has a directory");
            std::fs::create_dir_all(parent)?;
            let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
            serde_json::to_writer(&mut temporary, state)?;
            temporary.flush()?;
            temporary.as_file().sync_all()?;
            temporary.persist(&self.path).map_err(|error| error.error)?;
            self.sync_directory()
        };
        write().map_err(|error| self.error(error))
    }

    fn sync_directory(&self) -> io::Result<()> {
        #[cfg(unix)]
        std::fs::File::open(self.path.parent().expect("journal has a directory"))?.sync_all()?;
        Ok(())
    }

    fn error(&self, error: impl std::fmt::Display) -> io::Error {
        io::Error::other(format!("upload journal `{}`: {error}", self.path.display()))
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Option<UploadState>> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl MultipartUploadJournal for UploadJournal {
    fn began(
        &self,
        upload_id: &UploadId,
        part_size_bytes: u64,
        checksum_algorithm: ChecksumAlgorithm,
    ) -> io::Result<()> {
        let state = UploadState {
            upload_id: upload_id.clone(),
            part_size_bytes,
            checksum_algorithm,
            parts: Vec::new(),
            source: self.source.clone(),
        };
        self.flush(&state)?;
        *self.lock() = Some(state);
        Ok(())
    }

    fn part_completed(&self, part: &CompletedUploadPart) -> io::Result<()> {
        let mut held = self.lock();
        let mut next = held
            .as_ref()
            .ok_or_else(|| self.error("no upload session was recorded"))?
            .clone();
        next.parts.push(StatePart {
            part_number: part.part_number,
            etag: part.etag.clone(),
            checksum: part.checksum.clone(),
        });
        self.flush(&next)?;
        *held = Some(next);
        Ok(())
    }
}

/// Where per-upload records live: `$XDG_STATE_HOME/loonfs/uploads` when that
/// variable names an absolute directory, and `$HOME/.local/state/loonfs/uploads`
/// otherwise — the same order, and the same reasoning, as the config file's.
fn uploads_dir() -> Option<PathBuf> {
    if let Some(state_home) = absolute_env_path("XDG_STATE_HOME") {
        return Some(state_home.join(XDG_STATE_SUBDIR).join(UPLOADS_SUBDIR));
    }
    let home = absolute_env_path("HOME")?;
    Some(
        home.join(".local/state")
            .join(XDG_STATE_SUBDIR)
            .join(UPLOADS_SUBDIR),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source(size_bytes: u64) -> SourceIdentity {
        SourceIdentity {
            size_bytes,
            modified_ns: 7,
        }
    }

    fn journal_at(path: &Path) -> UploadJournal {
        UploadJournal {
            path: path.to_owned(),
            source: source(1024),
            state: Mutex::new(None),
        }
    }

    fn begin(journal: &UploadJournal) {
        journal
            .began(
                &UploadId::parse("upl_00000000000000000000000000000001").expect("id"),
                1024 * 1024,
                ChecksumAlgorithm::Crc64nvme,
            )
            .expect("record session");
    }

    fn part(number: u32) -> CompletedUploadPart {
        CompletedUploadPart {
            part_number: number,
            etag: format!("etag-{number}"),
            checksum: Checksum::crc64nvme(format!("part-{number}").as_bytes()),
        }
    }

    #[test]
    fn a_record_survives_repeated_interruptions_and_is_removed_after_acknowledgement() {
        let dir = tempfile::tempdir().expect("directory");
        let path = dir.path().join("upload.json");
        let first = journal_at(&path);
        assert!(first.resume().expect("read").is_none());
        begin(&first);
        first.part_completed(&part(1)).expect("first part");
        let next = journal_at(&path);
        let resumed = next.resume().expect("read").expect("session");
        assert_eq!(resumed.parts, vec![part(1)]);
        assert_eq!(resumed.part_size_bytes, 1024 * 1024);
        assert_eq!(resumed.checksum_algorithm, ChecksumAlgorithm::Crc64nvme);
        next.part_completed(&part(2)).expect("second part");
        assert_eq!(
            journal_at(&path)
                .resume()
                .expect("read")
                .expect("session")
                .parts,
            vec![part(1), part(2)]
        );
        next.forget().expect("remove acknowledged record");
        assert!(next.resume().expect("read").is_none());
        next.forget().expect("already absent");
    }

    #[test]
    fn malformed_and_unreadable_records_are_errors_and_are_preserved() {
        let dir = tempfile::tempdir().expect("directory");
        let path = dir.path().join("upload.json");
        let journal = journal_at(&path);
        std::fs::write(&path, b"{\"upload_id\":").expect("torn record");
        assert!(journal
            .resume()
            .expect_err("invalid record")
            .to_string()
            .contains("upload.json"));
        assert_eq!(
            std::fs::read(&path).expect("preserved record"),
            b"{\"upload_id\":"
        );
        std::fs::remove_file(&path).expect("remove fixture");
        std::fs::create_dir(&path).expect("unreadable record");
        assert!(journal.resume().is_err());
        assert!(journal.forget().is_err());
    }

    #[test]
    fn a_changed_source_requires_an_explicit_new_upload() {
        let dir = tempfile::tempdir().expect("directory");
        let path = dir.path().join("upload.json");
        let journal = journal_at(&path);
        begin(&journal);
        let mut changed = journal_at(&path);
        changed.source = source(2048);
        assert!(changed
            .resume()
            .expect_err("changed source")
            .to_string()
            .contains("source file changed"));
        assert!(path.exists());
    }

    #[test]
    fn a_failed_update_preserves_the_last_durable_parts() {
        let dir = tempfile::tempdir().expect("directory");
        let parent = dir.path().join("uploads");
        let saved = dir.path().join("saved");
        let journal = journal_at(&parent.join("upload.json"));
        begin(&journal);
        journal.part_completed(&part(1)).expect("first part");
        std::fs::rename(&parent, &saved).expect("move directory");
        std::fs::write(&parent, b"not a directory").expect("block journal writes");
        assert!(journal.part_completed(&part(2)).is_err());
        assert_eq!(journal.lock().as_ref().expect("state").parts.len(), 1);
        std::fs::remove_file(&parent).expect("remove blocker");
        std::fs::rename(&saved, &parent).expect("restore directory");
        assert_eq!(
            journal_at(&journal.path)
                .resume()
                .expect("read")
                .expect("session")
                .parts,
            vec![part(1)]
        );
    }

    #[test]
    fn missing_session_state_cannot_silently_discard_a_part() {
        let dir = tempfile::tempdir().expect("directory");
        assert!(journal_at(&dir.path().join("upload.json"))
            .part_completed(&part(1))
            .is_err());
    }

    #[test]
    fn the_journal_key_includes_both_endpoints_and_the_source_path() {
        let dir = tempfile::tempdir().expect("directory");
        let first = dir.path().join("file");
        let other = dir.path().join("other");
        std::fs::write(&first, b"data").expect("source");
        std::fs::write(&other, b"data").expect("source");
        let path = |profile, namespace, remote, local: &Path| {
            UploadJournal::for_upload(profile, namespace, remote, local, source(1024))
                .expect("journal path")
                .path
        };
        let original = path("default", "demo", "/file", &first);
        assert_eq!(original, path("default", "demo", "/file", &first));
        for candidate in [
            path("other", "demo", "/file", &first),
            path("default", "other", "/file", &first),
            path("default", "demo", "/other", &first),
            path("default", "demo", "/file", &other),
        ] {
            assert_ne!(original, candidate);
        }
    }
}
