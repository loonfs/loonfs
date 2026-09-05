//! Durable recovery records for file-backed remote PUT attempts.

use crate::config::absolute_env_path;
use loonfs_api::v0::{CommitRequest, CompletedUploadPart, FilesystemOperation};
use loonfs_api::{Checksum, ChecksumAlgorithm, CommitId, UploadId};
use loonfs_client::{MultipartUploadResume, NamespacePath, PutFileJournal, PutFileOptions};
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

const XDG_STATE_SUBDIR: &str = "loonfs";
const UPLOADS_SUBDIR: &str = "uploads";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct UploadState {
    source: SourceIdentity,
    options: PutFileOptions,
    progress: UploadProgress,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
enum UploadProgress {
    Uploading {
        multipart: Option<MultipartUploadResume>,
    },
    Prepared {
        request: Box<CommitRequest>,
    },
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

/// One PUT attempt, exclusively owned until this value is dropped.
#[derive(Debug)]
pub(crate) struct UploadJournal {
    path: PathBuf,
    state: Mutex<UploadState>,
    /// Explicit commit IDs retain their requests for subsequent invocations.
    keep_after_ack: bool,
    /// A stable sidecar inode prevents rename/removal from bypassing the lock.
    _ownership: File,
}

impl UploadJournal {
    pub(crate) fn for_upload(
        profile: &str,
        server_url: &str,
        spec: &NamespacePath,
        local_path: &Path,
        source: SourceIdentity,
        options: &PutFileOptions,
    ) -> io::Result<Self> {
        let key = journal_key(
            profile,
            server_url,
            spec,
            local_path,
            options.commit.commit_id.as_ref(),
        )?;
        let dir = uploads_dir().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "upload journals require an absolute XDG_STATE_HOME or HOME",
            )
        })?;
        Self::open_at(dir.join(format!("{key}.json")), source, options)
    }

    fn open_at(
        path: PathBuf,
        source: SourceIdentity,
        options: &PutFileOptions,
    ) -> io::Result<Self> {
        let parent = path.parent().expect("journal has a directory");
        std::fs::create_dir_all(parent).map_err(|error| journal_error(&path, error))?;
        let ownership = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path.with_extension("lock"))
            .map_err(|error| journal_error(&path, error))?;
        if !fs4::fs_std::FileExt::try_lock_exclusive(&ownership)
            .map_err(|error| journal_error(&path, error))?
        {
            return Err(journal_error(
                &path,
                "another command owns this PUT attempt",
            ));
        }
        let recorded = match std::fs::read(&path) {
            Ok(bytes) => Some(
                serde_json::from_slice::<UploadState>(&bytes)
                    .map_err(|error| journal_error(&path, error))?,
            ),
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => return Err(journal_error(&path, error)),
        };
        let is_new = recorded.is_none();
        let state = match recorded {
            Some(recorded) => {
                if recorded.source != source {
                    return Err(journal_error(
                        &path,
                        "source file changed; use a new --commit-id or remove this journal to start another attempt",
                    ));
                }
                let commit_id = recorded
                    .options
                    .commit
                    .commit_id
                    .clone()
                    .ok_or_else(|| journal_error(&path, "record has no commit ID"))?;
                let mut requested = options.clone();
                requested.commit.commit_id.get_or_insert(commit_id);
                if requested != recorded.options {
                    return Err(journal_error(&path, "PUT options changed; resume with the original options or use a new --commit-id for another attempt"));
                }
                if let UploadProgress::Prepared { request } = &recorded.progress {
                    if !request_matches_options(request, &recorded.options) {
                        return Err(journal_error(
                            &path,
                            "prepared request does not match its PUT options",
                        ));
                    }
                }
                recorded
            }
            None => {
                let mut options = options.clone();
                options
                    .commit
                    .commit_id
                    .get_or_insert_with(CommitId::generate);
                UploadState {
                    source,
                    options,
                    progress: UploadProgress::Uploading { multipart: None },
                }
            }
        };
        let journal = Self {
            path,
            state: Mutex::new(state),
            keep_after_ack: options.commit.commit_id.is_some(),
            _ownership: ownership,
        };
        if is_new {
            journal.flush(&journal.lock())?;
        }
        Ok(journal)
    }

    pub(crate) fn options(&self) -> PutFileOptions {
        self.lock().options.clone()
    }

    pub(crate) fn resume(&self) -> Option<MultipartUploadResume> {
        match &self.lock().progress {
            UploadProgress::Uploading { multipart } => multipart.clone(),
            UploadProgress::Prepared { .. } => None,
        }
    }

    /// A saved request is replayed directly, even after its upload session expires.
    pub(crate) fn prepared_request(&self) -> Option<CommitRequest> {
        match &self.lock().progress {
            UploadProgress::Prepared { request } => Some((**request).clone()),
            UploadProgress::Uploading { .. } => None,
        }
    }

    /// Removes ordinary attempts after acknowledgement. Explicit IDs keep their
    /// exact request so repeating the same command can replay it without uploading.
    pub(crate) fn acknowledge(&self) -> io::Result<()> {
        if self.keep_after_ack {
            return Ok(());
        }
        match std::fs::remove_file(&self.path) {
            Ok(()) => self.sync_directory().map_err(|error| self.error(error)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(self.error(error)),
        }
    }

    fn flush(&self, state: &UploadState) -> io::Result<()> {
        let write = || -> io::Result<()> {
            let parent = self.path.parent().expect("journal has a directory");
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
        File::open(self.path.parent().expect("journal has a directory"))?.sync_all()?;
        Ok(())
    }

    fn error(&self, error: impl std::fmt::Display) -> io::Error {
        journal_error(&self.path, error)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, UploadState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn update(&self, change: impl FnOnce(&mut UploadProgress) -> io::Result<()>) -> io::Result<()> {
        let mut held = self.lock();
        let mut next = held.clone();
        change(&mut next.progress)?;
        self.flush(&next)?;
        *held = next;
        Ok(())
    }
}

impl PutFileJournal for UploadJournal {
    fn began(
        &self,
        upload_id: &UploadId,
        part_size_bytes: u64,
        checksum_algorithm: ChecksumAlgorithm,
    ) -> io::Result<()> {
        self.update(|progress| match progress {
            UploadProgress::Uploading { multipart } if multipart.is_none() => {
                *multipart = Some(MultipartUploadResume {
                    upload_id: upload_id.clone(),
                    part_size_bytes,
                    checksum_algorithm,
                    parts: Vec::new(),
                });
                Ok(())
            }
            _ => {
                Err(self.error("this PUT attempt already has an upload session or commit request"))
            }
        })
    }

    fn part_completed(&self, part: &CompletedUploadPart) -> io::Result<()> {
        self.update(|progress| match progress {
            UploadProgress::Uploading {
                multipart: Some(resume),
            } => {
                resume.parts.push(part.clone());
                Ok(())
            }
            _ => Err(self.error("no active multipart session was recorded")),
        })
    }

    fn commit_prepared(&self, request: &CommitRequest) -> io::Result<()> {
        if !request_matches_options(request, &self.options()) {
            return Err(self.error("prepared request does not match its PUT options"));
        }
        self.update(|progress| match progress {
            UploadProgress::Uploading { .. } => {
                *progress = UploadProgress::Prepared {
                    request: Box::new(request.clone()),
                };
                Ok(())
            }
            UploadProgress::Prepared { request: saved } if **saved == *request => Ok(()),
            UploadProgress::Prepared { .. } => {
                Err(self.error("the prepared commit request cannot change"))
            }
        })
    }
}

fn request_matches_options(request: &CommitRequest, options: &PutFileOptions) -> bool {
    options.commit.commit_id.as_ref() == Some(&request.commit_id)
        && options.commit.actor == request.actor
        && options.commit.message == request.message
        && matches!(request.operations.as_slice(), [FilesystemOperation::PutFile {
            behavior, expected_inode_id, expected_revision_no, ..
        }] if *behavior == options.behavior
            && *expected_inode_id == options.expected_inode_id
            && *expected_revision_no == options.expected_revision_no)
}

fn journal_error(path: &Path, error: impl std::fmt::Display) -> io::Error {
    io::Error::other(format!("upload journal `{}`: {error}", path.display()))
}

fn journal_key(
    profile: &str,
    server_url: &str,
    spec: &NamespacePath,
    local_path: &Path,
    commit_id: Option<&CommitId>,
) -> io::Result<String> {
    let local_path = local_path.canonicalize()?;
    Ok(Checksum::sha256(&serde_json::to_vec(&(
        profile,
        server_url,
        spec.namespace(),
        spec.absolute_path(),
        local_path.as_os_str().as_encoded_bytes(),
        commit_id,
    ))?)
    .value)
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
    fn options() -> PutFileOptions {
        PutFileOptions::new(loonfs_test_support::test_actor())
    }
    fn open(path: &Path) -> UploadJournal {
        UploadJournal::open_at(path.to_owned(), source(1024), &options()).expect("open journal")
    }
    fn begin(journal: &UploadJournal) {
        journal
            .began(
                &UploadId::parse("upl_00000000000000000000000000000001").expect("id"),
                1024,
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
    fn request(journal: &UploadJournal) -> CommitRequest {
        let options = journal.options();
        CommitRequest::single(
            options.commit.commit_id.expect("chosen ID"),
            options.commit.actor,
            options.commit.message,
            FilesystemOperation::PutFile {
                path: loonfs_api::AbsolutePath::parse("/file").expect("path"),
                content_ref: loonfs_test_support::ids::content_ref(b"data"),
                behavior: options.behavior,
                expected_inode_id: options.expected_inode_id,
                expected_revision_no: options.expected_revision_no,
            },
        )
    }

    #[test]
    fn interruptions_preserve_the_commit_id_parts_and_exact_request() {
        let dir = tempfile::tempdir().expect("directory");
        let path = dir.path().join("upload.json");
        let first = open(&path);
        let chosen = first.options();
        assert!(chosen.commit.commit_id.is_some());
        assert!(first.resume().is_none());
        begin(&first);
        first.part_completed(&part(1)).expect("first part");
        drop(first);
        let next = open(&path);
        assert_eq!(next.options(), chosen);
        assert_eq!(next.resume().expect("session").parts, vec![part(1)]);
        next.part_completed(&part(2)).expect("second part");
        drop(next);
        let next = open(&path);
        assert_eq!(
            next.resume().expect("session").parts,
            vec![part(1), part(2)]
        );
        let expected = request(&next);
        let mut wrong_options = expected.clone();
        wrong_options.message = Some("different intent".to_owned());
        assert!(next.commit_prepared(&wrong_options).is_err());
        assert!(next.prepared_request().is_none());
        next.commit_prepared(&expected).expect("save commit");
        drop(next);
        let replay = open(&path);
        assert!(replay.resume().is_none());
        assert_eq!(replay.prepared_request(), Some(expected.clone()));
        assert!(replay.part_completed(&part(3)).is_err());
        let mut changed = expected;
        changed.commit_id = CommitId::generate();
        assert!(replay.commit_prepared(&changed).is_err());
        replay.acknowledge().expect("acknowledged");
        assert!(!path.exists());
        replay.acknowledge().expect("already removed");
    }

    #[test]
    fn explicit_commit_ids_keep_the_request_after_acknowledgement() {
        let dir = tempfile::tempdir().expect("directory");
        let path = dir.path().join("upload.json");
        let mut options = options();
        options.commit.commit_id = Some(CommitId::generate());
        let journal =
            UploadJournal::open_at(path.clone(), source(1024), &options).expect("journal");
        let expected = request(&journal);
        journal.commit_prepared(&expected).expect("record");
        journal.acknowledge().expect("acknowledged");
        drop(journal);
        assert!(path.exists());
        assert_eq!(
            UploadJournal::open_at(path, source(1024), &options)
                .expect("reopen")
                .prepared_request(),
            Some(expected)
        );
    }

    #[test]
    fn only_one_command_can_own_an_attempt_even_after_its_record_is_removed() {
        let dir = tempfile::tempdir().expect("directory");
        let path = dir.path().join("upload.json");
        let journal = open(&path);
        for removed in [false, true] {
            if removed {
                journal.acknowledge().expect("remove record");
            }
            assert!(
                UploadJournal::open_at(path.clone(), source(1024), &options())
                    .expect_err("owned")
                    .to_string()
                    .contains("another command")
            );
        }
        drop(journal);
        let reopened = open(&path);
        assert!(reopened.resume().is_none());
    }

    #[test]
    fn corrupt_unreadable_and_changed_records_are_preserved() {
        let dir = tempfile::tempdir().expect("directory");
        let path = dir.path().join("upload.json");
        std::fs::write(&path, b"{").expect("corrupt record");
        assert!(UploadJournal::open_at(path.clone(), source(1024), &options()).is_err());
        assert_eq!(std::fs::read(&path).expect("preserved"), b"{");
        std::fs::remove_file(&path).expect("remove fixture");
        std::fs::create_dir(&path).expect("unreadable record");
        assert!(UploadJournal::open_at(path.clone(), source(1024), &options()).is_err());
        std::fs::remove_dir(&path).expect("remove fixture");
        let original = open(&path).options();
        assert!(
            UploadJournal::open_at(path.clone(), source(2048), &options())
                .expect_err("changed file")
                .to_string()
                .contains("source file changed")
        );
        let mut changed = options();
        changed.commit.message = Some("different".to_owned());
        assert!(UploadJournal::open_at(path.clone(), source(1024), &changed)
            .expect_err("changed options")
            .to_string()
            .contains("PUT options changed"));
        changed = options();
        changed.commit.commit_id = Some(CommitId::generate());
        assert!(UploadJournal::open_at(path.clone(), source(1024), &changed).is_err());
        assert_eq!(open(&path).options(), original);
    }

    #[test]
    fn failed_updates_preserve_the_last_durable_state() {
        let dir = tempfile::tempdir().expect("directory");
        let parent = dir.path().join("uploads");
        let saved = dir.path().join("saved");
        let path = parent.join("upload.json");
        let journal = open(&path);
        assert!(journal.part_completed(&part(1)).is_err());
        begin(&journal);
        journal.part_completed(&part(1)).expect("first part");
        std::fs::rename(&parent, &saved).expect("move directory");
        std::fs::write(&parent, b"blocked").expect("block writes");
        assert!(journal.part_completed(&part(2)).is_err());
        assert!(journal.commit_prepared(&request(&journal)).is_err());
        assert_eq!(
            journal.resume().expect("original state").parts,
            vec![part(1)]
        );
        std::fs::remove_file(&parent).expect("unblock");
        std::fs::rename(&saved, &parent).expect("restore");
        drop(journal);
        assert_eq!(
            open(&path).resume().expect("durable state").parts,
            vec![part(1)]
        );
    }

    #[test]
    fn keys_include_the_server_paths_profile_and_explicit_commit_id() {
        let dir = tempfile::tempdir().expect("directory");
        let first = dir.path().join("file");
        let other = dir.path().join("other");
        std::fs::write(&first, b"data").expect("source");
        std::fs::write(&other, b"data").expect("source");
        let spec = NamespacePath::parse("demo", "/file").expect("path");
        let key = |profile, url, spec: &NamespacePath, file: &Path, id| {
            journal_key(profile, url, spec, file, id).expect("key")
        };
        let original = key("default", "https://one", &spec, &first, None);
        assert_eq!(original, key("default", "https://one", &spec, &first, None));
        for candidate in [
            key("other", "https://one", &spec, &first, None),
            key("default", "https://two", &spec, &first, None),
            key(
                "default",
                "https://one",
                &NamespacePath::parse("other", "/file").expect("path"),
                &first,
                None,
            ),
            key(
                "default",
                "https://one",
                &NamespacePath::parse("demo", "/other").expect("path"),
                &first,
                None,
            ),
            key("default", "https://one", &spec, &other, None),
            key(
                "default",
                "https://one",
                &spec,
                &first,
                Some(&CommitId::generate()),
            ),
        ] {
            assert_ne!(original, candidate);
        }
    }
}
