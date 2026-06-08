use super::resolver::build_authoritative_path_entry;
use crate::error::CoreError;
use crate::metadata::ResolvedVisiblePath;
use crate::namespace::basis::VerifiedNamespaceBasis;
use crate::path::helpers::{map_path_error_to_core, parse_absolute_path_for_core};
use loon_api::{AbsolutePath, AuthoritativePathEntry, DisplayName, InodeKind};

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
