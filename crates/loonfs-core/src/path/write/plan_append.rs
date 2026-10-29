//! Publish plans that add bytes to the end of a file.

use super::planned_pieces::CommitPieces;
use super::preconditions::ensure_expected_inode;
use super::publish_path_planning::{
    resolve_visible_inode, resolve_visible_path_for_authorization, CompiledFilesystemOperation,
    PublishPathPlanningView,
};
use crate::authorize::Absence;
use crate::commit::{AppendedContent, CommitOperation, CommitValidationError};
use crate::error::{CoreError, Result};
use crate::metadata::RevisionRecord;
use crate::path::mutation_path::{ensure_mutation_path, final_component};
use crate::storage::tail_content::materialize_content_layout;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::wal::WalInlineContent;
use loonfs_types::{
    AbsolutePath, AccessRight, AccessRights, Checksum, ContentId, ContentRef, ContentRefKind,
    DestinationBehavior, DestinationPrecondition, InodeId, InodeKind, PreconditionFields,
    RevisionNo,
};

#[allow(
    clippy::too_many_arguments,
    reason = "an append carries the commit's pieces, the store, and content write permits"
)]
pub(super) async fn plan_append_file<S: ObjectStore + ?Sized>(
    absolute_path: &AbsolutePath,
    bytes: &[u8],
    expected_inode_id: Option<InodeId>,
    expected_revision_no: Option<RevisionNo>,
    store: &S,
    content_writes: &crate::PermitPool,
    view: &PublishPathPlanningView<'_, '_, '_, S>,
    pieces: &CommitPieces<'_>,
) -> Result<CompiledFilesystemOperation> {
    ensure_mutation_path(absolute_path)?;
    let expected = DestinationPrecondition {
        behavior: DestinationBehavior::Replace,
        expected_inode_id,
        expected_revision_no,
    }
    .resolve(PreconditionFields::Put)?;
    let write = AccessRights::from(AccessRight::Write);
    let absence = Absence::Path(absolute_path.as_str());
    let target =
        resolve_visible_path_for_authorization(view, absolute_path, write, absence).await?;
    view.authorize(target.inode_id, write, absence).await?;
    ensure_expected_inode(
        &target,
        expected.map(|expected| expected.inode_id),
        &final_component(absolute_path)?,
    )?;
    if target.inode_kind != InodeKind::File {
        return Err(CoreError::ExpectedFile {
            target: absolute_path.as_str().to_owned(),
            kind: target.inode_kind,
        });
    }
    let revision = view
        .view
        .latest_revision_head(target.inode_id)
        .await?
        .ok_or_else(|| CoreError::PathNotFound(absolute_path.as_str().to_owned()))?;
    plan_append(
        store,
        content_writes,
        view,
        pieces,
        revision,
        expected.and_then(|expected| expected.revision_no),
        bytes,
    )
    .await
}

pub(super) async fn plan_append_file_by_inode<S: ObjectStore + ?Sized>(
    inode_id: InodeId,
    bytes: &[u8],
    expected_revision_no: Option<RevisionNo>,
    store: &S,
    content_writes: &crate::PermitPool,
    view: &PublishPathPlanningView<'_, '_, '_, S>,
    pieces: &CommitPieces<'_>,
) -> Result<CompiledFilesystemOperation> {
    let target = resolve_visible_inode(view, inode_id).await?;
    view.authorize(inode_id, AccessRight::Write.into(), Absence::Inode)
        .await?;
    if target.inode_kind != InodeKind::File {
        return Err(CoreError::ExpectedFile {
            target: target.absolute_path.to_string(),
            kind: target.inode_kind,
        });
    }
    let revision = view
        .view
        .latest_revision_head(inode_id)
        .await?
        .ok_or(CoreError::InodeNotFound(inode_id))?;
    plan_append(
        store,
        content_writes,
        view,
        pieces,
        revision,
        expected_revision_no,
        bytes,
    )
    .await
}

/// Plans the next revision of a file as its current bytes followed by
/// `bytes`. The guard is checked first so that a stale caller hears about
/// the revision, not about the content it would have extended.
async fn plan_append<S: ObjectStore + ?Sized>(
    store: &S,
    content_writes: &crate::PermitPool,
    view: &PublishPathPlanningView<'_, '_, '_, S>,
    pieces: &CommitPieces<'_>,
    revision: RevisionRecord,
    expected_revision_no: Option<RevisionNo>,
    bytes: &[u8],
) -> Result<CompiledFilesystemOperation> {
    if let Some(expected) =
        expected_revision_no.filter(|expected| *expected != revision.revision_no)
    {
        return Err(CommitValidationError::BaseRevisionMismatch {
            inode_id: revision.inode_id,
            expected,
            actual: Some(revision.revision_no),
            precondition_index: None,
        }
        .into());
    }
    let appended = append_to(store, content_writes, view, pieces, &revision, bytes).await?;
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

async fn append_to<S: ObjectStore + ?Sized>(
    store: &S,
    content_writes: &crate::PermitPool,
    view: &PublishPathPlanningView<'_, '_, '_, S>,
    pieces: &CommitPieces<'_>,
    base_row: &RevisionRecord,
    bytes: &[u8],
) -> Result<AppendedContent> {
    let base = &base_row.content_ref;
    let head_size = view
        .view
        .content_head(&base.content_id)
        .await?
        .unwrap_or(base.size_bytes);
    let extends_base = base.owner_namespace_id == *view.namespace_id
        && head_size == base.size_bytes
        && base.size_bytes > 0;
    let added = bytes.len() as u64;
    let size_bytes = base.size_bytes.checked_add(added).ok_or_else(|| {
        CoreError::NamespaceCorrupt(format!(
            "content `{}` records a size of {} bytes, too large to append {added} bytes to",
            base.content_id, base.size_bytes
        ))
    })?;
    let algorithm = store.checksum_algorithm();
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
        let content = pieces.content(base);
        Some(
            materialize_content_layout(store, view.view, content.as_ref(), base, content_writes)
                .await?,
        )
    };
    Ok(AppendedContent {
        content_ref: ContentRef {
            kind: ContentRefKind::BlobV1,
            owner_namespace_id: view.namespace_id.clone(),
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
