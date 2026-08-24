//! Partial files and metadata used to resume interrupted downloads.

use crate::backend::FileDownload;
use crate::error::CliError;
use loonfs_api::{Checksum, ContentRef, RevisionNo};
use serde::{Deserialize, Deserializer, Serialize};
use std::io::{Read, Seek, Write};
use std::path::{Path, PathBuf};

/// Suffix for partial download files.
const PARTIAL_SUFFIX: &str = ".loonfs-partial";
/// Suffix for partial download metadata.
const META_SUFFIX: &str = ".loonfs-partial.meta";
/// Chunk size used to checksum an existing partial file.
const FOLD_CHUNK_BYTES: usize = 1024 * 1024;

/// Identifies the content stored in a partial download.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PartialMeta {
    content_id: String,
    size_bytes: u64,
    checksum: Checksum,
    #[serde(deserialize_with = "required_option")]
    revision_no: Option<u64>,
}

fn required_option<'de, T, D>(deserializer: D) -> Result<Option<T>, D::Error>
where
    T: Deserialize<'de>,
    D: Deserializer<'de>,
{
    Option::<T>::deserialize(deserializer)
}

impl PartialMeta {
    /// Describes the content a download is about to fetch.
    pub(super) fn describe(content_ref: &ContentRef, revision_no: Option<RevisionNo>) -> Self {
        Self {
            content_id: content_ref.content_id.to_string(),
            size_bytes: content_ref.size_bytes,
            checksum: content_ref.checksum.clone(),
            revision_no: revision_no.map(|revision_no| revision_no.0),
        }
    }
}

/// Returns the number of reusable bytes when the partial file matches `meta`.
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
    // A partial file cannot be reused if it is longer than the content.
    if metadata.len() > meta.size_bytes {
        return 0;
    }
    metadata.len()
}

/// Stores a partial download and its metadata until completion.
pub(super) struct PartialDownload {
    file: tempfile::NamedTempFile,
    /// Metadata stored as a temporary path so it is removed with the partial
    /// file.
    _meta: tempfile::TempPath,
    path: PathBuf,
    resumed_from: u64,
}

impl PartialDownload {
    /// Opens the partial file at `resume_from` and writes its metadata file.
    ///
    /// Metadata is written before content so a later run can verify that the
    /// existing bytes belong to the same download. Content without a stable
    /// identity is not resumed.
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
            None => drop(std::fs::remove_file(&meta_path)),
        }
        let meta = tempfile::TempPath::try_from_path(&meta_path)?;
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;
        // Remove stale trailing bytes, or clear the file when starting over.
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

    /// Adds the existing bytes to the verifier so it checks the complete file.
    ///
    /// `local_error` maps failures while reading the partial file. Validation
    /// errors from the download are returned unchanged.
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

    /// Installs the completed download with an atomic rename.
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
    use loonfs_api::{ContentId, ContentRef, ContentRefKind};

    fn meta_for(bytes: &[u8]) -> PartialMeta {
        PartialMeta::describe(&ContentRef::blob_v1(ContentId::generate(), bytes), None)
    }

    fn crc32c_content_ref(bytes: &[u8]) -> ContentRef {
        ContentRef {
            kind: ContentRefKind::BlobV1,
            content_id: ContentId::generate(),
            size_bytes: bytes.len() as u64,
            checksum: Checksum::crc32c(bytes),
        }
    }

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

    #[test]
    fn a_crc32c_note_describes_and_resumes_its_partial() {
        let dir = tempfile::tempdir().expect("tempdir");
        let destination = dir.path().join("file.bin");
        let content_ref = crc32c_content_ref(b"0123456789");
        let meta = PartialMeta::describe(&content_ref, None);

        let encoded = serde_json::to_vec(&meta).expect("encode the note");
        let document: serde_json::Value =
            serde_json::from_slice(&encoded).expect("decode the note as a document");
        assert_eq!(document["checksum"]["algorithm"], "crc32c");
        assert!(document.get("revision_no").is_some());

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
                checksum: Checksum::crc32c(b"9876543210"),
                ..content_ref
            },
            None,
        );
        assert_eq!(resumable_bytes(&destination, &other), 0);
    }

    #[test]
    fn an_old_partial_sidecar_cleanly_restarts_from_byte_zero() {
        let dir = tempfile::tempdir().expect("tempdir");
        let destination = dir.path().join("file.bin");
        let partial_path = sibling(&destination, PARTIAL_SUFFIX).expect("partial path");
        let meta_path = sibling(&destination, META_SUFFIX).expect("meta path");
        let expected = meta_for(b"0123456789");

        std::fs::write(&partial_path, b"0123").expect("write partial");
        std::fs::write(
            &meta_path,
            serde_json::json!({
                "content_id": expected.content_id,
                "size_bytes": expected.size_bytes,
                "checksum": expected.checksum
            })
            .to_string(),
        )
        .expect("write old sidecar");

        assert_eq!(resumable_bytes(&destination, &expected), 0);
        let restarted = PartialDownload::open(&destination, Some(&expected), 0)
            .expect("restart partial download");
        assert_eq!(restarted.resumed_from, 0);
        assert_eq!(
            std::fs::metadata(partial_path)
                .expect("partial metadata")
                .len(),
            0
        );
    }
}
