//! File reads over a loaded metadata view: whole-file bytes, content
//! resolution, and direct download targets.

use crate::authorize::{Absence, ReadAccess};
use crate::error::{CoreError, Result};
use crate::path::read::loaded_view::LoadedMetadataView;
use crate::storage::content_location::extent_object_key;
use crate::storage::content_location::ContentLocation;
use crate::storage::tail_content::{assemble_tail_content, write_tail_content, ContentWrites};
use crate::PermitPool;
use loonfs_objectstore::ObjectStore;
use loonfs_objectstore::ObjectStoreErrorKind;
use loonfs_types::options::AttributeInclusion;
use loonfs_types::{
    AbsolutePath, AccessRight, ContentExtent, ContentRef, InodeId, InodeKind, PathEntry,
    PathEntryKind, RevisionNo,
};

/// File bytes plus the metadata entry they came from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileBytes {
    /// Authoritative metadata for the file that was read.
    pub entry: PathEntry,
    /// Validated file bytes.
    pub bytes: Vec<u8>,
}

/// Immutable object ranges for a selected revision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectDownloadTarget {
    /// Absolute path as rendered from stored display names.
    pub absolute_path: AbsolutePath,
    /// Revision this target reads, resolved from the request.
    pub revision_no: RevisionNo,
    /// Identity, byte length, and checksum evidence for those bytes.
    pub content_ref: ContentRef,
    /// The first byte the download reads: it reads
    /// `[start_offset, content_ref.size_bytes)`.
    pub start_offset: u64,
    /// Consecutive object ranges starting at `start_offset`.
    pub ranges: Vec<GrantedRange>,
}

/// Content information needed to authorize an inode download.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectDownloadByInodeTarget {
    /// File inode that owns this revision.
    pub inode_id: InodeId,
    /// Revision being read.
    pub revision_no: RevisionNo,
    /// Content identity, size, and checksum.
    pub content_ref: ContentRef,
    /// The first byte the download reads: it reads
    /// `[start_offset, content_ref.size_bytes)`.
    pub start_offset: u64,
    /// Consecutive object ranges starting at `start_offset`.
    pub ranges: Vec<GrantedRange>,
}

/// Object bytes for a host to sign without proxying them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrantedRange {
    /// Logical unscoped key passed to the object-store signer.
    pub object_key: String,
    /// Offset within the object, which may differ from the revision offset.
    pub object_start: u64,
    /// Number of object bytes authorized.
    pub length: u64,
}

impl<'a, S: ObjectStore + ?Sized> LoadedMetadataView<'a, S> {
    pub(crate) async fn read_file_bytes(
        &self,
        absolute_path: &str,
        max_content_bytes: Option<u64>,
        access: &ReadAccess<'_, S>,
    ) -> Result<FileBytes> {
        let entry = self
            .resolve_file_content(absolute_path, None, access)
            .await?;
        let content_ref = file_content_ref(&entry)?;
        self.file_bytes(entry, content_ref, max_content_bytes).await
    }

    /// Reads the current content of a visible file inode, wherever it is
    /// bound, with its entry.
    pub(crate) async fn read_file_bytes_by_inode(
        &self,
        inode_id: InodeId,
        max_content_bytes: Option<u64>,
        access: &ReadAccess<'_, S>,
    ) -> Result<FileBytes> {
        let entry = self
            .stat_inode(inode_id, AttributeInclusion::Omit, access)
            .await?;
        let content_ref = file_content_ref(&entry)?;
        self.file_bytes(entry, content_ref, max_content_bytes).await
    }

    async fn file_bytes(
        &self,
        entry: PathEntry,
        content_ref: ContentRef,
        max_content_bytes: Option<u64>,
    ) -> Result<FileBytes> {
        ensure_within_read_limit(content_ref.size_bytes, max_content_bytes)?;
        let bytes = self
            .resolve_content_location(&content_ref)
            .await?
            .read_bytes(self.segments.store(), Some(self.object_cache), &content_ref)
            .await?;
        Ok(FileBytes { entry, bytes })
    }

    /// Resolves a path to the entry of the file it names, with the selected
    /// revision in the entry's kind: the metadata half both the buffered read
    /// and the streaming one run, so neither can resolve a path the other
    /// would refuse.
    pub(crate) async fn resolve_file_content(
        &self,
        absolute_path: &str,
        revision_no: Option<RevisionNo>,
        access: &ReadAccess<'_, S>,
    ) -> Result<PathEntry> {
        let mut entry = self
            .resolve_path(absolute_path, AttributeInclusion::Omit, access)
            .await?;
        // A directory fails here, before the history check.
        file_content_ref(&entry)?;
        if let Some(revision_no) = revision_no {
            if !matches!(entry.kind, PathEntryKind::File { revision_no: current, .. } if current == revision_no)
            {
                access
                    .require(
                        &mut self.metadata_view().session(),
                        entry.inode_id,
                        AccessRight::History.into(),
                        Absence::Path(absolute_path),
                    )
                    .await?;
            }
            let revision = self.revision_by_inode(entry.inode_id, revision_no).await?;
            entry.kind = PathEntryKind::File {
                revision_no: revision.revision_no,
                size_bytes: revision.content_ref.size_bytes,
                content_ref: Some(revision.content_ref),
                revision_committed_by: revision.committed_by,
                revision_committed_at_ms: revision.committed_at_ms,
            };
        }
        Ok(entry)
    }

    pub(crate) async fn resolve_content_location(
        &self,
        content_ref: &ContentRef,
    ) -> Result<ContentLocation> {
        let content = self.wal_tail.content(content_ref);
        ContentLocation::resolve(&self.metadata_view(), content.as_ref(), content_ref).await
    }

    async fn download_ranges(
        &self,
        content_ref: &ContentRef,
        start_offset: u64,
        content_writes: &PermitPool,
    ) -> Result<Vec<GrantedRange>> {
        let store = self.segments.store();
        if start_offset != 0 && start_offset >= content_ref.size_bytes {
            return Err(CoreError::ResumeOffsetOutOfRange {
                start_offset,
                size_bytes: content_ref.size_bytes,
            });
        }
        let cannot_write = |error| match error {
            CoreError::Store {
                kind: ObjectStoreErrorKind::PermissionDenied,
                ..
            } => CoreError::ContentNotMaterialized {
                content_id: content_ref.content_id.clone(),
            },
            error => error,
        };
        let location = self.resolve_content_location(content_ref).await?;
        if content_ref.size_bytes == 0 {
            let key = location.object_key();
            if store
                .head(key)
                .await
                .map_err(|error| CoreError::store(key, &error))?
                .is_none()
            {
                let _permit = content_writes.acquire().await;
                store
                    .put_immutable_verified(key, bytes::Bytes::new())
                    .await
                    .map_err(|error| cannot_write(CoreError::store(key, &error)))?;
            }
            return Ok(vec![GrantedRange {
                object_key: key.to_owned(),
                object_start: 0,
                length: 0,
            }]);
        }
        if location.is_resident() && location.has_pieces() {
            let key = location.object_key();
            if store
                .head(key)
                .await
                .map_err(|error| CoreError::store(key, &error))?
                .is_some_and(|metadata| metadata.size_bytes >= content_ref.size_bytes)
            {
                return Ok(vec![GrantedRange {
                    object_key: key.to_owned(),
                    object_start: start_offset,
                    length: content_ref.size_bytes - start_offset,
                }]);
            }
        }
        let extents = if location.has_pieces() {
            let content = self
                .wal_tail
                .content(content_ref)
                .expect("resident pieces should name a projected chain");
            write_tail_content(
                ContentWrites::new(store, content_writes),
                &self.metadata_view(),
                &content,
                assemble_tail_content(&content)?,
            )
            .await
            .map_err(cannot_write)?
            .layout
            .extents
        } else {
            location
                .extents
                .into_iter()
                .map(|located| located.extent)
                .collect()
        };
        Ok(granted_ranges(
            extents,
            start_offset,
            content_ref.size_bytes,
        ))
    }

    pub(crate) async fn direct_download_target(
        &self,
        absolute_path: &str,
        revision_no: Option<RevisionNo>,
        start_offset: u64,
        access: &ReadAccess<'_, S>,
        content_writes: &PermitPool,
    ) -> Result<DirectDownloadTarget> {
        let entry = self
            .resolve_file_content(absolute_path, revision_no, access)
            .await?;
        let content_ref = file_content_ref(&entry)?;
        let PathEntryKind::File { revision_no, .. } = entry.kind else {
            return Err(CoreError::ExpectedFile {
                target: entry.path.to_string(),
                kind: InodeKind::Directory,
            });
        };
        let ranges = self
            .download_ranges(&content_ref, start_offset, content_writes)
            .await?;

        Ok(DirectDownloadTarget {
            absolute_path: entry.path,
            revision_no,
            content_ref,
            start_offset,
            ranges,
        })
    }

    /// Resolves inode content for a direct download: a retained revision
    /// without requiring a current path, or the current revision of a
    /// visible file.
    pub(crate) async fn direct_download_target_by_inode(
        &self,
        inode_id: InodeId,
        revision_no: Option<RevisionNo>,
        start_offset: u64,
        access: &ReadAccess<'_, S>,
        content_writes: &PermitPool,
    ) -> Result<DirectDownloadByInodeTarget> {
        let revision = match revision_no {
            Some(revision_no) => {
                self.authorized_revision_by_inode(inode_id, revision_no, access)
                    .await?
            }
            None => self.current_revision_by_inode(inode_id, access).await?,
        };
        let ranges = self
            .download_ranges(&revision.content_ref, start_offset, content_writes)
            .await?;
        Ok(DirectDownloadByInodeTarget {
            inode_id,
            revision_no: revision.revision_no,
            content_ref: revision.content_ref,
            start_offset,
            ranges,
        })
    }
}

/// Refuses a buffered content read whose resolved size exceeds the caller's
/// budget. The check runs on metadata, before any content fetch, so an
/// over-limit read costs no object-store traffic and allocates nothing.
pub(crate) fn ensure_within_read_limit(
    size_bytes: u64,
    max_content_bytes: Option<u64>,
) -> Result<()> {
    match max_content_bytes {
        Some(max_bytes) if size_bytes > max_bytes => Err(CoreError::ContentTooLarge {
            size_bytes,
            max_bytes,
        }),
        _ => Ok(()),
    }
}

/// Returns the content reference of a file entry that a stat built, which
/// always includes it.
pub(crate) fn file_content_ref(entry: &PathEntry) -> Result<ContentRef> {
    match &entry.kind {
        PathEntryKind::File { content_ref, .. } => Ok(content_ref
            .clone()
            .expect("a stat entry should include its content reference")),
        // Core builds entries from stored kinds, so an unknown kind never reaches here.
        PathEntryKind::Directory { .. } | PathEntryKind::Unknown => Err(CoreError::ExpectedFile {
            target: entry.path.to_string(),
            kind: InodeKind::Directory,
        }),
    }
}

/// Returns the ranges of `extents` that store the bytes of a
/// `size_bytes`-byte file from `start_offset` to its end.
fn granted_ranges(
    extents: Vec<ContentExtent>,
    start_offset: u64,
    size_bytes: u64,
) -> Vec<GrantedRange> {
    let mut ranges = Vec::new();
    let mut offset = 0;
    for extent in extents {
        let end = offset + extent.length;
        let first = offset.max(start_offset);
        let last = end.min(size_bytes);
        if first < last {
            ranges.push(GrantedRange {
                object_key: extent_object_key(&extent),
                object_start: extent.offset + (first - offset),
                length: last - first,
            });
        }
        offset = end;
        if offset >= size_bytes {
            break;
        }
    }
    ranges
}
