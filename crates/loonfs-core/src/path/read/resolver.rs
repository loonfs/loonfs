use crate::error::CoreError;
use crate::metadata::{MetadataState, ResolvedVisiblePath};
use crate::namespace::full_materialization::FullNamespaceMaterialization;
use crate::path::helpers::parse_absolute_path_for_core;
use loonfs_api::{AuthoritativePathEntry, ChangeSeq, NamespaceId};

#[tracing::instrument(
    level = "info",
    name = "loon.phase",
    err,
    skip_all,
    fields(phase = "walk_path")
)]
pub(crate) fn resolve_path_from_full_materialization(
    materialization: &FullNamespaceMaterialization,
    absolute_path: &str,
) -> Result<AuthoritativePathEntry, CoreError> {
    let absolute_path = parse_absolute_path_for_core(absolute_path)?;
    let resolved = materialization.metadata_state.resolve_visible_path(
        &absolute_path,
        materialization.head.name_policy,
        materialization.head.seq,
    )?;
    build_authoritative_path_entry(
        &materialization.head.namespace_id,
        materialization.head.seq,
        &materialization.metadata_state,
        &resolved,
    )
}

pub(super) fn build_authoritative_path_entry(
    namespace_id: &NamespaceId,
    head_seq: ChangeSeq,
    metadata_state: &MetadataState,
    resolved: &ResolvedVisiblePath,
) -> Result<AuthoritativePathEntry, CoreError> {
    let revision = metadata_state.latest_revision_head_at_seq(resolved.inode_id, head_seq);
    let content_ref = revision
        .as_ref()
        .map(|revision| revision.content_ref.clone());
    let size_bytes = content_ref
        .as_ref()
        .map(|content_ref| content_ref.size_bytes);

    Ok(AuthoritativePathEntry {
        namespace_id: namespace_id.clone(),
        absolute_path: resolved.absolute_path.clone(),
        inode_id: resolved.inode_id,
        inode_kind: resolved.inode_kind.clone(),
        head_seq,
        parent_inode_id: resolved.parent_inode_id,
        display_name: resolved.display_name.clone(),
        revision_no: revision.as_ref().map(|revision| revision.revision_no),
        size_bytes,
        content_ref,
    })
}
