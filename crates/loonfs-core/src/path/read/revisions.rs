//! File revisions over a loaded metadata view: revision lookups, revision
//! history pages, and reads of one revision's bytes.

use super::current_files::resolve_visible_inode;
use super::listing::{invalid_cursor, validate_cursor_head};
use crate::authorize::{Absence, ReadAccess};
use crate::error::{CoreError, Result};
use crate::metadata::{ancestry_allows_read, MetadataViewSession, RevisionRecord};
use crate::path::read::file_reads::ensure_within_read_limit;
use crate::path::read::file_reads::file_content_ref;
use crate::path::read::file_reads::FileBytes;
use crate::path::read::loaded_view::LoadedMetadataView;
use loonfs_objectstore::ObjectStore;
use loonfs_types::options::AttributeInclusion;
use loonfs_types::{
    AccessRight, ChangeSeq, FileRevision, FileRevisionsPageCursor, InodeId, InodeKind, Page,
    PageRequest, PathEntryKind, RevisionNo,
};
use std::collections::HashMap;

/// One page of a file's retained revisions, with the file's inode ID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileRevisionsPage {
    /// The file inode that the path resolved to.
    pub inode_id: InodeId,
    /// Retained revisions, newest first.
    pub page: Page<FileRevision, FileRevisionsPageCursor>,
}

impl<'a, S: ObjectStore + ?Sized> LoadedMetadataView<'a, S> {
    /// Resolves one retained revision after the access checks a direct read
    /// of it needs: `read` on the inode, and `history` when the revision is
    /// not the current one.
    pub(crate) async fn authorized_revision_by_inode(
        &self,
        inode_id: InodeId,
        revision_no: RevisionNo,
        access: &ReadAccess<'_, S>,
    ) -> Result<RevisionRecord> {
        let mut session = self.metadata_view().session();
        ensure_ancestry_allows_read(&mut session, inode_id).await?;
        access
            .require(
                &mut session,
                inode_id,
                AccessRight::Read.into(),
                Absence::Inode,
            )
            .await?;
        if !access.is_unrestricted()
            && self
                .metadata_view()
                .latest_revision_head(inode_id)
                .await?
                .is_none_or(|current| current.revision_no != revision_no)
        {
            access
                .require(
                    &mut session,
                    inode_id,
                    AccessRight::History.into(),
                    Absence::Inode,
                )
                .await?;
        }
        self.revision_by_inode(inode_id, revision_no).await
    }

    /// Resolves the current revision of a visible file inode after the `read`
    /// check a current read needs.
    pub(crate) async fn current_revision_by_inode(
        &self,
        inode_id: InodeId,
        access: &ReadAccess<'_, S>,
    ) -> Result<RevisionRecord> {
        let mut session = self.metadata_view().session();
        let mut ancestor_paths = HashMap::new();
        let resolved = resolve_visible_inode(&mut session, &mut ancestor_paths, inode_id)
            .await?
            .ok_or(CoreError::InodeNotFound(inode_id))?;
        access
            .require(
                &mut session,
                inode_id,
                AccessRight::Read.into(),
                Absence::Inode,
            )
            .await?;
        if resolved.inode_kind != InodeKind::File {
            return Err(CoreError::ExpectedFile {
                target: resolved.absolute_path.to_string(),
                kind: resolved.inode_kind,
            });
        }
        self.metadata_view()
            .latest_revision_head(inode_id)
            .await?
            .ok_or(CoreError::InodeNotFound(inode_id))
    }

    pub(crate) async fn list_file_revisions_page(
        &self,
        absolute_path: &str,
        request: PageRequest<FileRevisionsPageCursor>,
        access: &ReadAccess<'_, S>,
    ) -> Result<FileRevisionsPage> {
        let entry = self
            .resolve_path(absolute_path, AttributeInclusion::Omit, access)
            .await?;
        if matches!(entry.kind, PathEntryKind::Directory {}) {
            return Err(CoreError::ExpectedFile {
                target: entry.path.to_string(),
                kind: InodeKind::Directory,
            });
        }
        access
            .require(
                &mut self.metadata_view().session(),
                entry.inode_id,
                AccessRight::History.into(),
                Absence::Path(absolute_path),
            )
            .await?;
        let page = self.file_revisions_page(entry.inode_id, request).await?;
        Ok(FileRevisionsPage {
            inode_id: entry.inode_id,
            page,
        })
    }

    pub(crate) async fn list_file_revisions_by_inode_page(
        &self,
        inode_id: InodeId,
        request: PageRequest<FileRevisionsPageCursor>,
        access: &ReadAccess<'_, S>,
    ) -> Result<Page<FileRevision, FileRevisionsPageCursor>> {
        let mut session = self.metadata_view().session();
        ensure_ancestry_allows_read(&mut session, inode_id).await?;
        access
            .require(
                &mut session,
                inode_id,
                AccessRight::Read.into(),
                Absence::Inode,
            )
            .await?;
        access
            .require(
                &mut session,
                inode_id,
                AccessRight::History.into(),
                Absence::Inode,
            )
            .await?;
        self.file_revisions_page(inode_id, request).await
    }

    /// One page of a file inode's retained revisions, newest first, for a
    /// caller that has already authorized the listing.
    async fn file_revisions_page(
        &self,
        inode_id: InodeId,
        request: PageRequest<FileRevisionsPageCursor>,
    ) -> Result<Page<FileRevision, FileRevisionsPageCursor>> {
        let inode = self
            .metadata_view()
            .inode_at_seq(inode_id)
            .await?
            .ok_or(CoreError::InodeNotFound(inode_id))?;
        if inode.inode_kind != InodeKind::File {
            return Err(CoreError::ExpectedFile {
                target: inode_id.to_string(),
                kind: inode.inode_kind,
            });
        }
        if let Some(cursor) = request.cursor.as_ref() {
            validate_file_revisions_cursor(cursor, self.head.seq, inode_id)?;
        }

        let start_after = request.cursor.as_ref().map(|cursor| {
            crate::metadata::manifest_index::RevisionPagePosition::after(
                cursor.last_revision_no,
                cursor.last_committed_seq,
                cursor.last_revision_delta_index,
            )
        });
        let mut revision_records = self
            .metadata_view()
            .session()
            .revisions_by_inode_page_desc(inode_id, start_after, request.limit.limit_plus_one())
            .await?;
        let next_cursor =
            request
                .limit
                .finish_page(&mut revision_records, |last| FileRevisionsPageCursor {
                    head_seq: self.head.seq,
                    inode_id,
                    last_revision_no: last.revision_no,
                    last_committed_seq: last.committed_seq,
                    last_revision_delta_index: last.delta_index,
                });
        let revisions = revision_records
            .into_iter()
            .map(|revision| FileRevision {
                inode_id: revision.inode_id,
                revision_no: revision.revision_no,
                committed_seq: revision.committed_seq,
                commit_id: revision.commit_id,
                committed_at_ms: revision.committed_at_ms,
                committed_by: revision.committed_by,
                content_ref: revision.content_ref,
            })
            .collect();

        Ok(Page {
            items: revisions,
            next_cursor,
        })
    }

    pub(crate) async fn read_file_revision_bytes(
        &self,
        absolute_path: &str,
        revision_no: RevisionNo,
        max_content_bytes: Option<u64>,
        access: &ReadAccess<'_, S>,
    ) -> Result<FileBytes> {
        let entry = self
            .resolve_file_content(absolute_path, Some(revision_no), access)
            .await?;
        let content_ref = file_content_ref(&entry)?;
        ensure_within_read_limit(content_ref.size_bytes, max_content_bytes)?;
        let bytes = self
            .resolve_content_location(&content_ref)
            .await?
            .read_bytes(self.segments.store(), Some(self.object_cache), &content_ref)
            .await?;
        Ok(FileBytes { entry, bytes })
    }

    pub(crate) async fn read_file_revision_bytes_by_inode(
        &self,
        inode_id: InodeId,
        revision_no: RevisionNo,
        max_content_bytes: Option<u64>,
        access: &ReadAccess<'_, S>,
    ) -> Result<Vec<u8>> {
        let revision = self
            .authorized_revision_by_inode(inode_id, revision_no, access)
            .await?;
        ensure_within_read_limit(revision.content_ref.size_bytes, max_content_bytes)?;
        Ok(self
            .resolve_content_location(&revision.content_ref)
            .await?
            .read_bytes(
                self.segments.store(),
                Some(self.object_cache),
                &revision.content_ref,
            )
            .await?)
    }

    pub(crate) async fn revision_by_inode(
        &self,
        inode_id: InodeId,
        revision_no: RevisionNo,
    ) -> Result<RevisionRecord> {
        self.metadata_view()
            .revision_by_inode(inode_id, revision_no)
            .await
    }
}

/// Refuses a read by inode ID of an inode that an expired deletion hides (a
/// deletion at or below the view's retention floor), or whose ancestry is
/// disconnected, with the error of an unknown inode and before any access
/// decision. See [`ancestry_allows_read`] for the walk.
///
/// A read that requires a visible inode doesn't need this check, because its
/// path walk already requires bound edges up to the root.
async fn ensure_ancestry_allows_read<S: ObjectStore + ?Sized>(
    session: &mut MetadataViewSession<'_, '_, S>,
    inode_id: InodeId,
) -> Result<()> {
    if !ancestry_allows_read(session, inode_id).await? {
        return Err(CoreError::InodeNotFound(inode_id));
    }
    Ok(())
}

fn validate_file_revisions_cursor(
    cursor: &FileRevisionsPageCursor,
    head_seq: ChangeSeq,
    inode_id: InodeId,
) -> Result<()> {
    validate_cursor_head(head_seq, Some(cursor.head_seq))?;
    if cursor.inode_id != inode_id {
        return Err(invalid_cursor(
            "file revisions cursor inode does not match the requested file",
        ));
    }
    Ok(())
}
