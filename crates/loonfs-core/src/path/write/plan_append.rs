//! Publish plans that add bytes to the end of a file.

use super::ensure_expected_inode;
use super::publish_path_planning::{
    resolve_visible_inode, resolve_visible_path_for_authorization, CompiledFilesystemOperation,
    PublishPathPlanningView,
};
use crate::authorize::Absence;
use crate::commit::{AppendedContent, CommitOp, CommitValidationError};
use crate::error::{CoreError, Result};
use crate::metadata::RevisionRecord;
use crate::path::mutation_path::{ensure_mutation_path, final_component};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::wal::{ContentBase, WalInlineContent};
use loonfs_types::{
    AbsolutePath, AccessRight, AccessRights, Checksum, ContentId, ContentRef, ContentRefKind,
    DestinationBehavior, DestinationPrecondition, InodeId, InodeKind, PreconditionFields,
    RevisionNo,
};

pub(super) async fn plan_append_file<S: ObjectStore + ?Sized>(
    absolute_path: &AbsolutePath,
    bytes: &[u8],
    expected_inode_id: Option<InodeId>,
    expected_revision_no: Option<RevisionNo>,
    view: &PublishPathPlanningView<'_, '_, '_, S>,
) -> Result<CompiledFilesystemOperation> {
    ensure_mutation_path(absolute_path)?;
    let expected = DestinationPrecondition {
        behavior: DestinationBehavior::Replace,
        expected_inode_id,
        expected_revision_no,
    }
    .resolve(PreconditionFields::Put)?;
    let write = AccessRights::from_iter([AccessRight::Write]);
    let absence = Absence::Path(absolute_path.as_str());
    let target =
        resolve_visible_path_for_authorization(view, absolute_path, write, absence).await?;
    view.authorize(target.inode_id, write, absence).await?;
    ensure_expected_inode(
        view.view.naming(),
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
        view,
        absolute_path.as_str(),
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
    view: &PublishPathPlanningView<'_, '_, '_, S>,
) -> Result<CompiledFilesystemOperation> {
    let target = resolve_visible_inode(view, inode_id).await?;
    view.authorize(
        inode_id,
        AccessRights::from_iter([AccessRight::Write]),
        Absence::Inode,
    )
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
        view,
        target.absolute_path.as_str(),
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
    view: &PublishPathPlanningView<'_, '_, '_, S>,
    target: &str,
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
    let appended = append_to(view, target, &revision.content_ref, bytes).await?;
    Ok(CompiledFilesystemOperation {
        ops: vec![CommitOp::ReplaceFile {
            inode_id: revision.inode_id,
            base_revision_no: revision.revision_no,
            content_ref: appended.content_ref.clone(),
        }],
        appended: Some(appended),
    })
}

/// Builds the reference to `base`'s bytes followed by `bytes`, and the
/// piece that carries them.
///
/// The bytes extend `base`'s content object only when this namespace owns
/// it and `base` names all of it. Otherwise they start a new object whose
/// first bytes are `base`'s. Appending to no bytes also starts a new object,
/// so its piece never shares offset 0 with an empty value in this commit.
async fn append_to<S: ObjectStore + ?Sized>(
    view: &PublishPathPlanningView<'_, '_, '_, S>,
    target: &str,
    base: &ContentRef,
    bytes: &[u8],
) -> Result<AppendedContent> {
    let missing_row = || {
        CoreError::NamespaceCorrupt(format!(
            "`{target}` names {} bytes of content `{}`, which no publication row records",
            base.size_bytes, base.content_id
        ))
    };
    let head = view
        .view
        .content_head(&base.content_id)
        .await?
        .ok_or_else(missing_row)?;
    let extends_base = base.owner_namespace_id == *view.namespace_id
        && head.size_bytes == base.size_bytes
        && base.size_bytes > 0;
    let base_row = if head.size_bytes == base.size_bytes {
        head
    } else {
        view.view
            .content_publication(&base.content_id, base.size_bytes)
            .await?
            .ok_or_else(missing_row)?
    };
    let added = bytes.len() as u64;
    let size_bytes = base.size_bytes.checked_add(added).ok_or_else(|| {
        CoreError::Internal(format!(
            "appending {added} bytes to `{target}` overflows its {} bytes",
            base.size_bytes
        ))
    })?;
    let crc64nvme = base_row
        .crc64nvme
        .map(|crc| {
            crc.crc64nvme_combine(&Checksum::crc64nvme(bytes), added)
                .ok_or_else(|| {
                    CoreError::NamespaceCorrupt(format!(
                        "the publication row of content `{}` records a malformed `crc64nvme`",
                        base.content_id
                    ))
                })
        })
        .transpose()?;
    let (hash_state, checksum) = match (base_row.hash_state, &crc64nvme) {
        (Some(mut state), _) => {
            state.update(bytes);
            let checksum = state.finish();
            (Some(state), checksum)
        }
        (None, Some(crc)) => (None, crc.clone()),
        (None, None) => {
            return Err(CoreError::AppendNotSupported {
                target: target.to_owned(),
            })
        }
    };
    let content_id = if extends_base {
        base.content_id.clone()
    } else {
        ContentId::generate()
    };
    Ok(AppendedContent {
        content_ref: ContentRef {
            kind: ContentRefKind::BlobV1,
            owner_namespace_id: view.namespace_id.clone(),
            content_id: content_id.clone(),
            size_bytes,
            checksum,
        },
        hash_state,
        crc64nvme,
        piece: WalInlineContent {
            content_id,
            offset: base.size_bytes,
            bytes: bytes.to_vec(),
            base: (!extends_base && base.size_bytes > 0).then(|| ContentBase {
                owner_namespace_id: base.owner_namespace_id.clone(),
                content_id: base.content_id.clone(),
            }),
        },
    })
}
