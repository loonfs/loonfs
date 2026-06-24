use super::resolver::build_authoritative_path_entry;
use crate::error::CoreError;
use crate::metadata::{DirentryBindRecord, ResolvedVisiblePath};
use crate::namespace::basis::VerifiedNamespaceBasis;
use crate::path::helpers::{map_path_error_to_core, parse_absolute_path_for_core};
use loonfs_api::{
    AbsolutePath, AuthoritativePathEntry, ChangeSeq, DirectoryPageCursor, DisplayName, InodeKind,
    NameKey, Page, PageRequest,
};

#[tracing::instrument(
    level = "info",
    name = "loon.phase",
    err,
    skip_all,
    fields(phase = "walk_path")
)]
pub(crate) fn list_path_from_basis(
    basis: &VerifiedNamespaceBasis,
    absolute_path: &str,
) -> Result<Vec<AuthoritativePathEntry>, CoreError> {
    let absolute_path = parse_absolute_path_for_core(absolute_path)?;
    let resolved = basis.metadata_state.resolve_visible_path(
        &absolute_path,
        basis.head.name_policy,
        basis.head.seq,
    )?;
    if resolved.inode_kind == InodeKind::File {
        return Ok(vec![build_authoritative_path_entry(
            &basis.head.namespace_id,
            basis.head.seq,
            &basis.metadata_state,
            &resolved,
        )?]);
    }
    if resolved.inode_kind != InodeKind::Dir {
        return Err(CoreError::ExpectedDirectory {
            path: resolved.absolute_path,
            kind: resolved.inode_kind,
        });
    }

    basis
        .metadata_state
        .visible_children(resolved.inode_id, basis.head.seq)
        .into_iter()
        .map(|direntry| {
            let child = basis
                .metadata_state
                .visible_inode(direntry.child_inode_id, basis.head.seq)
                .expect("visible child listing should resolve inode");
            let child_path = AbsolutePath::parse(&resolved.absolute_path)
                .map_err(map_path_error_to_core)?
                .join(&DisplayName::parse(&direntry.display_name).map_err(map_path_error_to_core)?);
            build_authoritative_path_entry(
                &basis.head.namespace_id,
                basis.head.seq,
                &basis.metadata_state,
                &ResolvedVisiblePath {
                    absolute_path: child_path.as_str().to_owned(),
                    inode_id: direntry.child_inode_id,
                    inode_kind: child.inode_kind,
                    parent_inode_id: Some(direntry.parent_inode_id),
                    display_name: direntry.display_name,
                },
            )
        })
        .collect()
}

#[tracing::instrument(
    level = "info",
    name = "loon.phase",
    err,
    skip_all,
    fields(phase = "walk_path")
)]
pub(crate) fn list_path_page_from_basis(
    basis: &VerifiedNamespaceBasis,
    absolute_path: &str,
    request: PageRequest<DirectoryPageCursor>,
) -> Result<Page<AuthoritativePathEntry, DirectoryPageCursor>, CoreError> {
    let page_head_seq = page_head_seq(
        basis.head.seq,
        basis.snapshot_floor_seq.max(basis.head.retention_floor_seq),
        request.cursor.as_ref(),
    )?;
    let absolute_path = parse_absolute_path_for_core(absolute_path)?;
    let resolved = basis.metadata_state.resolve_visible_path(
        &absolute_path,
        basis.head.name_policy,
        page_head_seq,
    )?;
    if let Some(cursor) = request.cursor.as_ref() {
        validate_directory_cursor(cursor, &resolved)?;
    }

    if resolved.inode_kind == InodeKind::File {
        if request.cursor.is_some() {
            return Err(invalid_cursor(
                "directory cursor cannot resume a file listing",
            ));
        }
        return Ok(Page {
            items: vec![build_authoritative_path_entry(
                &basis.head.namespace_id,
                page_head_seq,
                &basis.metadata_state,
                &resolved,
            )?],
            next_cursor: None,
        });
    }
    if resolved.inode_kind != InodeKind::Dir {
        return Err(CoreError::ExpectedDirectory {
            path: resolved.absolute_path,
            kind: resolved.inode_kind,
        });
    }

    let start_after = request
        .cursor
        .as_ref()
        .map(|cursor| cursor.last_name_key.as_str());
    let children = basis.metadata_state.visible_children_page_by_name_key(
        resolved.inode_id,
        page_head_seq,
        start_after,
        request.limit.limit_plus_one(),
    );
    page_directory_children(
        &basis.head.namespace_id,
        page_head_seq,
        &basis.metadata_state,
        &resolved,
        children,
        request,
    )
}

pub(super) fn page_head_seq(
    current_head_seq: ChangeSeq,
    snapshot_floor_seq: ChangeSeq,
    cursor: Option<&DirectoryPageCursor>,
) -> Result<ChangeSeq, CoreError> {
    let Some(cursor) = cursor else {
        return Ok(current_head_seq);
    };
    if cursor.head_seq > current_head_seq {
        return Err(invalid_cursor(format!(
            "cursor snapshot `{}` is ahead of current head `{}`",
            cursor.head_seq.0, current_head_seq.0
        )));
    }
    if cursor.head_seq < snapshot_floor_seq {
        return Err(invalid_cursor(format!(
            "cursor snapshot `{}` is older than available snapshot floor `{}`",
            cursor.head_seq.0, snapshot_floor_seq.0
        )));
    }
    Ok(cursor.head_seq)
}

pub(super) fn validate_directory_cursor(
    cursor: &DirectoryPageCursor,
    resolved: &ResolvedVisiblePath,
) -> Result<(), CoreError> {
    if resolved.inode_kind != InodeKind::Dir {
        return Err(invalid_cursor(
            "directory cursor resolved to a non-directory path",
        ));
    }
    if resolved.inode_id != cursor.dir_inode_id {
        return Err(invalid_cursor(format!(
            "cursor directory inode `{}` does not match requested path inode `{}`",
            cursor.dir_inode_id.0, resolved.inode_id.0
        )));
    }
    Ok(())
}

pub(super) fn invalid_cursor(message: impl Into<String>) -> CoreError {
    CoreError::InvalidCursor(message.into())
}

fn page_directory_children(
    namespace_id: &loonfs_api::NamespaceId,
    page_head_seq: ChangeSeq,
    metadata_state: &crate::metadata::MetadataState,
    resolved: &ResolvedVisiblePath,
    children: Vec<DirentryBindRecord>,
    request: PageRequest<DirectoryPageCursor>,
) -> Result<Page<AuthoritativePathEntry, DirectoryPageCursor>, CoreError> {
    let mut children = children;
    let has_more = children.len() > request.limit.as_usize();
    if has_more {
        children.truncate(request.limit.as_usize());
    }

    let next_cursor = if has_more {
        let last = children
            .last()
            .expect("non-zero page limit with more children must return an item");
        Some(DirectoryPageCursor {
            head_seq: page_head_seq,
            dir_inode_id: resolved.inode_id,
            last_name_key: NameKey::try_new(last.name_key.clone()).map_err(|error| {
                CoreError::NamespaceCorrupt(format!("invalid stored name_key: {error}"))
            })?,
        })
    } else {
        None
    };

    let items = children
        .into_iter()
        .map(|direntry| {
            let child = metadata_state
                .visible_inode(direntry.child_inode_id, page_head_seq)
                .expect("visible child listing should resolve inode");
            let child_path = AbsolutePath::parse(&resolved.absolute_path)
                .map_err(map_path_error_to_core)?
                .join(&DisplayName::parse(&direntry.display_name).map_err(map_path_error_to_core)?);
            build_authoritative_path_entry(
                namespace_id,
                page_head_seq,
                metadata_state,
                &ResolvedVisiblePath {
                    absolute_path: child_path.as_str().to_owned(),
                    inode_id: direntry.child_inode_id,
                    inode_kind: child.inode_kind,
                    parent_inode_id: Some(direntry.parent_inode_id),
                    display_name: direntry.display_name,
                },
            )
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(Page { items, next_cursor })
}
