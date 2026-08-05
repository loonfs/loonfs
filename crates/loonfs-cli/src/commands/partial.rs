//! The file a download writes into before it is installed, and the note
//! beside it saying which content it holds.
//!
//! Both are named from the destination rather than randomly, because a
//! rerun has to find them: that is the whole of how an interrupted download
//! picks up where it stopped. Both are deleted once the download reaches a
//! verdict — installed at the destination, or failed and reported — so the
//! only run that leaves bytes behind is one that never got to clean up,
//! which is exactly the run worth resuming.

use crate::backend::FileDownload;
use crate::error::CliError;
use loonfs_api::{ContentRef, RevisionNo, StorageChecksum};
use serde::{Deserialize, Serialize};
use std::io::{Read, Seek, Write};
use std::path::{Path, PathBuf};

/// Suffix of the file a download's bytes land in.
const PARTIAL_SUFFIX: &str = ".loonfs-partial";
/// Suffix of the note beside it.
const META_SUFFIX: &str = ".loonfs-partial.meta";
/// How much of a partial file is read at a time when its bytes are folded
/// back into a resumed download's verification. Bounded for the same reason
/// the download itself is: a resumed 50 GiB file must not cost 50 GiB.
const FOLD_CHUNK_BYTES: usize = 1024 * 1024;

/// What the bytes in a partial file are, so a rerun can tell whether they
/// are still the bytes it wants.
///
/// The content id settles it on its own: content objects are immutable and
/// randomly identified, so the same id is the same bytes and a different id
/// is a different file. The rest is recorded because a partial disagreeing
/// with any of it is not worth resuming either, and because a note this
/// build cannot read at all is the same answer — start over.
///
/// The storage checksum is the reference's own evidence whatever produced
/// it, which is the only thing an object nobody hashed for us carries. The
/// whole-file SHA-256 is recorded beside it when there is one rather than
/// instead of it, because the two are different claims about the same
/// bytes and a note that dropped either would be a weaker match.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PartialMeta {
    content_id: String,
    size_bytes: u64,
    storage_checksum: StorageChecksum,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    whole_file_sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    revision_no: Option<u64>,
}

impl PartialMeta {
    /// Describes the content a download is about to fetch.
    pub(super) fn describe(content_ref: &ContentRef, revision_no: Option<RevisionNo>) -> Self {
        Self {
            content_id: content_ref.content_id.to_string(),
            size_bytes: content_ref.size_bytes,
            storage_checksum: content_ref.storage_checksum.clone(),
            whole_file_sha256: content_ref.whole_file_sha256.clone(),
            revision_no: revision_no.map(|revision_no| revision_no.0),
        }
    }
}

/// How much of a destination's partial file a fresh download of `meta` may
/// pick up, which is none of it unless everything agrees.
///
/// Every disagreement answers zero and says nothing: a stale partial is not
/// a failure, it is a download that starts over.
pub(super) fn resumable_bytes(destination: &Path, meta: &PartialMeta) -> u64 {
    let (Some(partial_path), Some(meta_path)) = (
        sibling(destination, PARTIAL_SUFFIX),
        sibling(destination, META_SUFFIX),
    ) else {
        return 0;
    };
    let Ok(recorded) = std::fs::read(&meta_path) else {
        return 0;
    };
    if serde_json::from_slice::<PartialMeta>(&recorded)
        .ok()
        .as_ref()
        != Some(meta)
    {
        return 0;
    }
    let Ok(metadata) = std::fs::metadata(&partial_path) else {
        return 0;
    };
    // A partial longer than the content is not a prefix of it, whatever the
    // note says.
    if metadata.len() > meta.size_bytes {
        return 0;
    }
    metadata.len()
}

/// A download's bytes on their way to a destination.
pub(super) struct PartialDownload {
    file: tempfile::NamedTempFile,
    /// The note, held as a temporary path so it is removed on exactly the
    /// occasions the bytes are.
    _meta: tempfile::TempPath,
    path: PathBuf,
    resumed_from: u64,
}

impl PartialDownload {
    /// Opens the partial file for a download that starts at `resume_from`,
    /// and lays the note down beside it.
    ///
    /// The note is written before the first byte: bytes on disk with
    /// nothing saying what they are cannot be resumed from, and a rerun
    /// that guessed would be worse than one that started over. Content this
    /// build cannot identify gets no note at all, which is the same answer
    /// spelled by its absence.
    pub(super) fn open(
        destination: &Path,
        meta: Option<&PartialMeta>,
        resume_from: u64,
    ) -> std::io::Result<Self> {
        let (Some(path), Some(meta_path)) = (
            sibling(destination, PARTIAL_SUFFIX),
            sibling(destination, META_SUFFIX),
        ) else {
            return Err(std::io::Error::other("destination has no file name"));
        };
        match meta {
            Some(meta) => {
                let encoded = serde_json::to_vec(meta).map_err(std::io::Error::other)?;
                std::fs::write(&meta_path, encoded)?;
            }
            // A stale note from an earlier run must not outlive the bytes it
            // described.
            None => drop(std::fs::remove_file(&meta_path)),
        }
        let meta = tempfile::TempPath::try_from_path(&meta_path)?;
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;
        // One call covers both cases: it drops a longer partial's tail, and
        // it empties one this download is not resuming at all.
        file.set_len(resume_from)?;
        file.seek(std::io::SeekFrom::Start(resume_from))?;
        Ok(Self {
            file: tempfile::NamedTempFile::from_parts(
                file,
                tempfile::TempPath::try_from_path(&path)?,
            ),
            _meta: meta,
            path,
            resumed_from: resume_from,
        })
    }

    /// Hands the download the bytes already on disk, so its verification
    /// still covers the whole file and not merely the part this run
    /// fetched.
    ///
    /// `local_error` shapes a failure to read those bytes back. A download
    /// that refuses them — a partial longer than the content it claims to
    /// resume — reports its own reason instead, because that is the answer
    /// worth printing.
    pub(super) fn fold_into(
        &self,
        download: &mut FileDownload,
        local_error: impl Fn(std::io::Error) -> CliError,
    ) -> Result<(), CliError> {
        if self.resumed_from == 0 {
            return Ok(());
        }
        let mut reader = std::fs::File::open(&self.path).map_err(&local_error)?;
        let mut buffer = vec![0u8; FOLD_CHUNK_BYTES];
        let mut remaining = self.resumed_from;
        while remaining > 0 {
            let wanted = buffer.len().min(remaining as usize);
            reader
                .read_exact(&mut buffer[..wanted])
                .map_err(&local_error)?;
            download.fold_resumed_prefix(&buffer[..wanted])?;
            remaining -= wanted as u64;
        }
        Ok(())
    }

    pub(super) fn write_all(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.file.write_all(bytes)
    }

    /// Installs the completed download at its destination with one rename,
    /// and takes the note away with it.
    ///
    /// Only a download that finished — and, when it was streamed, verified
    /// — reaches this, so the file at the destination is never a truncated
    /// or unverified one.
    pub(super) fn install(mut self, destination: &Path, force: bool) -> std::io::Result<()> {
        self.file.flush()?;
        let persisted = if force {
            self.file.persist(destination)
        } else {
            self.file.persist_noclobber(destination)
        };
        persisted.map(|_| ()).map_err(|error| error.error)
    }
}

/// The sibling of `destination` carrying `suffix`, as a hidden file in the
/// destination's own directory so the rename that installs it never crosses
/// a filesystem.
fn sibling(destination: &Path, suffix: &str) -> Option<PathBuf> {
    let file_name = destination.file_name()?;
    let mut name = std::ffi::OsString::from(".");
    name.push(file_name);
    name.push(suffix);
    Some(parent_of(destination).join(name))
}

/// The directory a destination's partial file is created in — its own, and
/// the working directory for a bare file name.
pub(super) fn parent_of(destination: &Path) -> &Path {
    destination
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

/// Shapes a note this build wrote and cannot now read. Nothing surfaces it:
/// it is the reason a resume did not happen, not a failure of the download.
#[allow(dead_code)]
fn unreadable_note(error: serde_json::Error) -> CliError {
    CliError::invalid_input(format!("unreadable partial-download note: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use loonfs_api::{ContentId, ContentRef, ContentRefKind};

    fn meta_for(bytes: &[u8]) -> PartialMeta {
        PartialMeta::describe(&ContentRef::blob_v1(ContentId::generate(), bytes), None)
    }

    /// A reference whose only full-object evidence is a CRC-32C, as a direct
    /// transfer to Google Cloud Storage leaves behind.
    fn crc32c_content_ref(bytes: &[u8]) -> ContentRef {
        ContentRef {
            kind: ContentRefKind::BlobV1,
            content_id: ContentId::generate(),
            size_bytes: bytes.len() as u64,
            storage_checksum: StorageChecksum::crc32c(bytes),
            whole_file_sha256: None,
        }
    }

    /// A partial whose note matches the content being fetched is picked up
    /// at exactly the length on disk.
    #[test]
    fn a_matching_note_resumes_at_what_is_on_disk() {
        let dir = tempfile::tempdir().expect("tempdir");
        let destination = dir.path().join("file.bin");
        let meta = meta_for(b"0123456789");

        assert_eq!(
            resumable_bytes(&destination, &meta),
            0,
            "nothing on disk resumes nothing"
        );
        let mut partial =
            PartialDownload::open(&destination, Some(&meta), 0).expect("open partial");
        partial.write_all(b"0123").expect("write");
        drop(partial);
        assert_eq!(
            resumable_bytes(&destination, &meta),
            0,
            "a partial whose download reached a verdict is gone"
        );
    }

    /// The note is what makes bytes resumable, and it has to describe the
    /// content this run resolved: a different file, a different revision, or
    /// a note this build cannot read all start over.
    #[test]
    fn a_note_that_does_not_match_starts_over() {
        let dir = tempfile::tempdir().expect("tempdir");
        let destination = dir.path().join("file.bin");
        let meta = meta_for(b"0123456789");
        let partial_path = sibling(&destination, PARTIAL_SUFFIX).expect("partial path");
        let meta_path = sibling(&destination, META_SUFFIX).expect("meta path");

        std::fs::write(&partial_path, b"0123").expect("write partial");
        std::fs::write(
            &meta_path,
            serde_json::to_vec(&meta).expect("encode the note"),
        )
        .expect("write note");
        assert_eq!(resumable_bytes(&destination, &meta), 4);

        // A different content id is a different file at the same path.
        assert_eq!(resumable_bytes(&destination, &meta_for(b"9876543210")), 0);

        // A partial longer than the content is not a prefix of it.
        std::fs::write(&partial_path, vec![0u8; 11]).expect("overlong partial");
        assert_eq!(resumable_bytes(&destination, &meta), 0);

        // A note this build cannot read is no note at all.
        std::fs::write(&partial_path, b"0123").expect("write partial");
        std::fs::write(&meta_path, b"{\"content_id\":").expect("write torn note");
        assert_eq!(resumable_bytes(&destination, &meta), 0);

        // Bytes with no note beside them say nothing about themselves.
        std::fs::remove_file(&meta_path).expect("remove note");
        assert_eq!(resumable_bytes(&destination, &meta), 0);
    }

    /// A file whose only evidence is a CRC-32C is described and resumed like
    /// any other: the note carries the reference's own checksum, so content
    /// nobody hashed for us is still identified on disk rather than being
    /// content a rerun has to start over on.
    #[test]
    fn a_crc32c_only_note_describes_and_resumes_its_partial() {
        let dir = tempfile::tempdir().expect("tempdir");
        let destination = dir.path().join("file.bin");
        let content_ref = crc32c_content_ref(b"0123456789");
        let meta = PartialMeta::describe(&content_ref, None);

        let encoded = serde_json::to_vec(&meta).expect("encode the note");
        let document: serde_json::Value =
            serde_json::from_slice(&encoded).expect("decode the note as a document");
        assert_eq!(document["storage_checksum"]["algorithm"], "crc32c");
        assert!(
            document.get("whole_file_sha256").is_none(),
            "a digest nobody computed is absent, not null: {document}"
        );

        std::fs::write(
            sibling(&destination, PARTIAL_SUFFIX).expect("partial path"),
            b"0123",
        )
        .expect("write partial");
        std::fs::write(
            sibling(&destination, META_SUFFIX).expect("meta path"),
            &encoded,
        )
        .expect("write note");
        assert_eq!(resumable_bytes(&destination, &meta), 4);

        // The checksum is part of the match: the same id and length under a
        // different digest is not the content this run resolved.
        let other = PartialMeta::describe(
            &ContentRef {
                storage_checksum: StorageChecksum::crc32c(b"9876543210"),
                ..content_ref
            },
            None,
        );
        assert_eq!(resumable_bytes(&destination, &other), 0);
    }
}
