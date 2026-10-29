//! Publish plans that add bytes to the end of a file.

use super::planner::OperationPlanningSession;
use super::publish_path_planning::{CompiledFilesystemOperation, Target};
use crate::commit::{AppendedContent, CommitOperation, CommitValidationError};
use crate::error::{CoreError, Result};
use crate::metadata::RevisionRecord;
use crate::storage::tail_content::materialize_content_layout;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::wal::WalInlineContent;
use loonfs_types::{AccessRight, Checksum, ContentId, ContentRef, ContentRefKind, RevisionNo};

impl<S: ObjectStore + ?Sized> OperationPlanningSession<'_, S> {
    /// Plans the next revision of a file as its current bytes followed by
    /// `bytes`. The guard is checked first so that a stale caller hears about
    /// the revision, not about the content it would have extended.
    pub(super) async fn plan_append(
        &self,
        target: Target<'_>,
        expected_revision_no: Option<RevisionNo>,
        bytes: &[u8],
    ) -> Result<CompiledFilesystemOperation> {
        let target = self
            .view
            .resolve_authorized_target(target, AccessRight::Write.into())
            .await?;
        target.ensure_file()?;
        let revision = self.view.latest_file_revision(&target).await?;
        if let Some(expected) =
            expected_revision_no.filter(|expected| *expected != revision.revision_no)
        {
            return Err(CommitValidationError::BaseRevisionMismatch {
                inode_id: revision.inode_id,
                expected,
                actual: Some(revision.revision_no),
            }
            .into());
        }
        let appended = self.append_to(&revision, bytes).await?;
        Ok(CompiledFilesystemOperation {
            ops: vec![CommitOperation::ReplaceFile {
                inode_id: revision.inode_id,
                base_revision_no: revision.revision_no,
                content_ref: appended.content_ref.clone(),
                append_offset: Some(revision.content_ref.size_bytes),
            }],
            appended: Some(appended),
        })
    }

    async fn append_to(&self, base_row: &RevisionRecord, bytes: &[u8]) -> Result<AppendedContent> {
        let namespace_id = self.view.namespace_id;
        let base = &base_row.content_ref;
        let head_size = self
            .view
            .metadata
            .content_head(&base.content_id)
            .await?
            .unwrap_or(base.size_bytes);
        let extends_base = base.owner_namespace_id == *namespace_id
            && head_size == base.size_bytes
            && base.size_bytes > 0;
        let added = bytes.len() as u64;
        let size_bytes = base.size_bytes.checked_add(added).ok_or_else(|| {
            CoreError::NamespaceCorrupt(format!(
                "content `{}` records a size of {} bytes, too large to append {added} bytes to",
                base.content_id, base.size_bytes
            ))
        })?;
        let algorithm = self.content_writes.store.checksum_algorithm();
        if base.checksum.algorithm() != algorithm {
            return Err(CoreError::NamespaceCorrupt(format!(
                "content `{}` uses `{}`; the store requires `{algorithm}`",
                base.content_id,
                base.checksum.algorithm()
            )));
        }
        let checksum = base
            .checksum
            .crc_combine(&Checksum::compute(algorithm, bytes), added)
            .ok_or_else(|| {
                CoreError::NamespaceCorrupt(format!(
                    "content `{}` has an invalid checksum",
                    base.content_id
                ))
            })?;
        let content_id = if extends_base {
            base.content_id.clone()
        } else {
            ContentId::generate()
        };
        let layout = if extends_base || base.size_bytes == 0 {
            None
        } else {
            let content = self.pieces.content(base);
            Some(
                materialize_content_layout(
                    self.content_writes,
                    &self.view.metadata,
                    content.as_ref(),
                    base,
                )
                .await?,
            )
        };
        Ok(AppendedContent {
            content_ref: ContentRef {
                kind: ContentRefKind::BlobV1,
                owner_namespace_id: namespace_id.clone(),
                content_id: content_id.clone(),
                size_bytes,
                checksum,
            },
            layout,
            pieces: vec![WalInlineContent {
                content_id,
                offset: base.size_bytes,
                bytes: bytes.to_vec(),
            }],
        })
    }
}
