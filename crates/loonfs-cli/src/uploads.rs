//! Where an interrupted upload's bookkeeping lives between runs.
//!
//! An upload session records the geometry it was opened with and nothing
//! per part — parts are the uploading client's own account, deliberately —
//! so the only record of what landed is the one this CLI keeps. It keeps it
//! under the user's state directory, keyed by the upload it describes, and
//! deletes it the moment the upload commits or is abandoned.
//!
//! Only a remote profile's direct multipart upload has anything to record.
//! An embedded profile stages through its own runtime with no session to
//! rejoin, and a payload small enough to travel in one request has no parts
//! to have half-finished.

use loonfs_api::v0::CompletedUploadPart;
use loonfs_api::{StorageChecksum, UploadId};
use loonfs_client::{MultipartUploadJournal, MultipartUploadResume};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// Directory under `$XDG_STATE_HOME`.
const XDG_STATE_SUBDIR: &str = "loonfs";
/// Directory under `$HOME`, matching where a config file lives when XDG
/// says nothing.
const LEGACY_STATE_SUBDIR: &str = ".loonfs/state";
/// Directory both roots hold the per-upload files in.
const UPLOADS_SUBDIR: &str = "uploads";

/// What one interrupted upload got through.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct UploadState {
    upload_id: String,
    part_size_bytes: u64,
    parts: Vec<StatePart>,
    source: SourceIdentity,
}

/// One part already in object storage, as the provider acknowledged it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StatePart {
    part_number: u32,
    etag: String,
    crc64nvme: String,
}

/// Enough of the local file to notice it is not the same file any more.
///
/// A payload that changed under a half-finished upload cannot be resumed
/// into: the parts already in object storage came from bytes that no longer
/// exist, and completing the assembly would produce an object matching
/// neither version. Length and modification time are what a filesystem
/// offers cheaply, and disagreeing on either is enough to start over.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SourceIdentity {
    size_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    modified_ms: Option<u64>,
}

impl SourceIdentity {
    /// Reads a local file's identity. A modification time the filesystem
    /// will not state leaves the length as the whole check, which is weaker
    /// and still better than nothing.
    pub(crate) fn of(path: &Path) -> std::io::Result<Self> {
        let metadata = std::fs::metadata(path)?;
        let modified_ms = metadata.modified().ok().and_then(|modified| {
            modified
                .duration_since(std::time::UNIX_EPOCH)
                .ok()
                .map(|since| since.as_millis() as u64)
        });
        Ok(Self {
            size_bytes: metadata.len(),
            modified_ms,
        })
    }
}

/// The record of one upload, and where it is kept.
///
/// Named by a digest of everything that decides which upload this is: two
/// puts of different files, to different paths, in different namespaces, or
/// through different profiles are different uploads and never share a
/// record.
#[derive(Debug)]
pub(crate) struct UploadJournal {
    path: PathBuf,
    source: SourceIdentity,
    /// What this run has been told, held until it is written down. Every
    /// change is flushed immediately: a record that lags the object store
    /// makes a rerun send parts that already landed, which is wasteful, and
    /// a record that leads it makes a rerun claim parts that did not, which
    /// fails the assembly.
    state: Mutex<Option<UploadState>>,
}

impl UploadJournal {
    /// The journal for one upload, or nothing when there is nowhere to keep
    /// it. A resume nobody can record is not a failure; it is an upload that
    /// starts over if it is interrupted.
    pub(crate) fn for_upload(
        profile: &str,
        namespace: &str,
        remote_path: &str,
        local_path: &Path,
        source: SourceIdentity,
    ) -> Option<Self> {
        let key = StorageChecksum::sha256(
            format!(
                "{profile}\u{0}{namespace}\u{0}{remote_path}\u{0}{}",
                local_path.display()
            )
            .as_bytes(),
        )
        .value;
        Some(Self {
            path: uploads_dir()?.join(format!("{key}.json")),
            source,
            state: Mutex::new(None),
        })
    }

    /// What an earlier run of this upload got through, when its record is
    /// still about the same bytes.
    ///
    /// Every disagreement answers nothing and takes the record away with
    /// it: a stale record is not a failure, it is an upload that starts
    /// over.
    pub(crate) fn resume(&self) -> Option<MultipartUploadResume> {
        let recorded = std::fs::read(&self.path).ok()?;
        let recorded: UploadState = serde_json::from_slice(&recorded).ok().or_else(|| {
            self.forget();
            None
        })?;
        if recorded.source != self.source {
            self.forget();
            return None;
        }
        let upload_id = UploadId::parse(&recorded.upload_id).ok().or_else(|| {
            self.forget();
            None
        })?;
        let resume = MultipartUploadResume {
            upload_id,
            part_size_bytes: recorded.part_size_bytes,
            parts: recorded
                .parts
                .iter()
                .map(|part| CompletedUploadPart {
                    part_number: part.part_number,
                    etag: part.etag.clone(),
                    crc64nvme: part.crc64nvme.clone(),
                })
                .collect(),
        };
        // The run that picks this up appends to it: a resumed upload that is
        // itself interrupted must leave a record of everything that landed,
        // not only of what landed before it started.
        *self.lock() = Some(recorded);
        Some(resume)
    }

    /// Removes the record. Called when the upload commits, and whenever
    /// what is on disk turns out to describe something else.
    pub(crate) fn forget(&self) {
        let _ = std::fs::remove_file(&self.path);
        *self.lock() = None;
    }

    /// Writes the record down. Nothing surfaces a failure: an upload that
    /// cannot be recorded still uploads, it just cannot be resumed.
    fn flush(&self, state: &UploadState) {
        let Some(parent) = self.path.parent() else {
            return;
        };
        if std::fs::create_dir_all(parent).is_err() {
            return;
        }
        if let Ok(encoded) = serde_json::to_vec(state) {
            let _ = std::fs::write(&self.path, encoded);
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Option<UploadState>> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl MultipartUploadJournal for UploadJournal {
    fn began(&self, upload_id: &UploadId, part_size_bytes: u64) {
        let state = UploadState {
            upload_id: upload_id.to_string(),
            part_size_bytes,
            parts: Vec::new(),
            source: self.source.clone(),
        };
        self.flush(&state);
        *self.lock() = Some(state);
    }

    fn part_completed(&self, part: &CompletedUploadPart) {
        let mut held = self.lock();
        let Some(state) = held.as_mut() else {
            // No session was opened and no record was picked up, so this
            // part belongs to an upload nobody is keeping track of.
            return;
        };
        state.parts.push(StatePart {
            part_number: part.part_number,
            etag: part.etag.clone(),
            crc64nvme: part.crc64nvme.clone(),
        });
        self.flush(state);
    }
}

/// Where per-upload records live: `$XDG_STATE_HOME/loonfs/uploads` when that
/// variable names an absolute directory, and `~/.loonfs/state/uploads`
/// otherwise — the same order, and the same reasoning, as the config file's.
fn uploads_dir() -> Option<PathBuf> {
    if let Some(state_home) = absolute_env_path("XDG_STATE_HOME") {
        return Some(state_home.join(XDG_STATE_SUBDIR).join(UPLOADS_SUBDIR));
    }
    let home = absolute_env_path("HOME")?;
    Some(home.join(LEGACY_STATE_SUBDIR).join(UPLOADS_SUBDIR))
}

/// An environment variable naming an absolute directory. The XDG spec calls
/// an empty or relative value invalid, and ignoring it keeps a stray
/// relative value from making state follow the working directory.
fn absolute_env_path(name: &str) -> Option<PathBuf> {
    let path = PathBuf::from(std::env::var_os(name)?);
    path.is_absolute().then_some(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity(size_bytes: u64, modified_ms: u64) -> SourceIdentity {
        SourceIdentity {
            size_bytes,
            modified_ms: Some(modified_ms),
        }
    }

    fn journal_at(dir: &Path, remote_path: &str, source: SourceIdentity) -> UploadJournal {
        UploadJournal {
            path: dir.join(format!("{}.json", remote_path.replace('/', "_"))),
            source,
            state: Mutex::new(None),
        }
    }

    fn part(part_number: u32) -> CompletedUploadPart {
        CompletedUploadPart {
            part_number,
            etag: format!("\"etag-{part_number}\""),
            crc64nvme: "00000000000000".to_owned(),
        }
    }

    fn upload_id() -> UploadId {
        UploadId::parse("upl_00000000000000000000000000000001").expect("valid upload id")
    }

    /// What a run records is exactly what the next one picks up, part by
    /// part, and committing takes the record away.
    #[test]
    fn a_record_hands_the_next_run_the_parts_that_landed() {
        let dir = tempfile::tempdir().expect("tempdir");
        let source = identity(1024, 7);
        let journal = journal_at(dir.path(), "/big.bin", source.clone());
        assert_eq!(journal.resume(), None, "nothing recorded resumes nothing");

        journal.began(&upload_id(), 1024 * 1024);
        journal.part_completed(&part(1));
        journal.part_completed(&part(2));

        let resumed = journal_at(dir.path(), "/big.bin", source.clone())
            .resume()
            .expect("a record of the same upload");
        assert_eq!(resumed.upload_id, upload_id());
        assert_eq!(resumed.part_size_bytes, 1024 * 1024);
        assert_eq!(
            resumed
                .parts
                .iter()
                .map(|p| p.part_number)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );

        // A run that resumed and was interrupted again leaves a record of
        // everything that landed, its own parts included.
        let second = journal_at(dir.path(), "/big.bin", source.clone());
        second.resume().expect("a record to pick up");
        second.part_completed(&part(3));
        assert_eq!(
            journal_at(dir.path(), "/big.bin", source)
                .resume()
                .expect("a record")
                .parts
                .iter()
                .map(|p| p.part_number)
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );

        journal.forget();
        assert_eq!(journal.resume(), None, "a committed upload keeps nothing");
    }

    /// A payload that changed under a half-finished upload cannot be
    /// resumed into: the parts already stored came from bytes that are gone.
    #[test]
    fn a_source_that_changed_invalidates_its_record() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal = journal_at(dir.path(), "/big.bin", identity(1024, 7));
        journal.began(&upload_id(), 1024 * 1024);
        journal.part_completed(&part(1));

        let rewritten = journal_at(dir.path(), "/big.bin", identity(1024, 8));
        assert_eq!(rewritten.resume(), None, "a newer file is a different file");
        assert!(
            !rewritten.path.exists(),
            "an invalidated record is removed rather than left to mislead"
        );

        let resized = journal_at(dir.path(), "/big.bin", identity(2048, 7));
        assert_eq!(resized.resume(), None, "a longer file is a different file");
    }

    /// A record this build cannot read is no record at all, and does not
    /// survive to confuse the next run either.
    #[test]
    fn an_unreadable_record_is_discarded() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal = journal_at(dir.path(), "/big.bin", identity(1024, 7));
        std::fs::write(&journal.path, b"{\"upload_id\":").expect("write torn record");
        assert_eq!(journal.resume(), None);
        assert!(!journal.path.exists());
    }

    /// Two uploads are the same upload only when everything that names one
    /// agrees.
    #[test]
    fn the_record_is_named_by_what_decides_which_upload_it_is() {
        let source = identity(1024, 7);
        let path = |profile, namespace, remote, local| {
            UploadJournal::for_upload(profile, namespace, remote, Path::new(local), source.clone())
                .map(|journal| journal.path)
        };
        let baseline = path("default", "demo", "/big.bin", "/tmp/big.bin");
        assert!(baseline.is_some(), "a home directory names a state file");
        assert_eq!(
            baseline,
            path("default", "demo", "/big.bin", "/tmp/big.bin")
        );
        assert_ne!(baseline, path("other", "demo", "/big.bin", "/tmp/big.bin"));
        assert_ne!(
            baseline,
            path("default", "prod", "/big.bin", "/tmp/big.bin")
        );
        assert_ne!(
            baseline,
            path("default", "demo", "/other.bin", "/tmp/big.bin")
        );
        assert_ne!(
            baseline,
            path("default", "demo", "/big.bin", "/tmp/other.bin")
        );
    }
}
