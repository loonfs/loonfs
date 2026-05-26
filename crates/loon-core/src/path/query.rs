use super::helpers::{map_path_error_to_core, parse_absolute_path_for_core};
use crate::basis::load_verified_namespace_basis;
use crate::content::{read_durable_content_bytes, validate_durable_content_reference};
use crate::error::CoreError;
use crate::metadata::{MetadataState, ResolvedVisiblePath};
use loon_api::{
    AbsolutePath, AuthoritativeFileBytes, AuthoritativePathEntry, ChangeSeq, ContentStoreId,
    DisplayName, InodeKind, NamespaceId,
};
use loon_objectstore::ObjectStore;

pub fn resolve_path<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
) -> Result<AuthoritativePathEntry, CoreError> {
    let basis = load_verified_namespace_basis(store, namespace_id)?;
    resolve_path_from_basis(store, &basis, absolute_path)
}

pub fn resolve_path_from_basis<S: ObjectStore + ?Sized>(
    store: &S,
    basis: &crate::VerifiedNamespaceBasis,
    absolute_path: &str,
) -> Result<AuthoritativePathEntry, CoreError> {
    let absolute_path = parse_absolute_path_for_core(absolute_path)?;
    let resolved = basis.metadata_state.resolve_visible_path(
        &absolute_path,
        basis.head.name_policy,
        basis.head.seq,
    )?;
    build_authoritative_path_entry(
        store,
        &basis.head.namespace_id,
        &basis.content_store_id,
        basis.head.seq,
        &basis.metadata_state,
        &resolved,
    )
}

pub fn list_path<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
) -> Result<Vec<AuthoritativePathEntry>, CoreError> {
    let basis = load_verified_namespace_basis(store, namespace_id)?;
    list_path_from_basis(store, &basis, absolute_path)
}

pub fn list_path_from_basis<S: ObjectStore + ?Sized>(
    store: &S,
    basis: &crate::VerifiedNamespaceBasis,
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
            store,
            &basis.head.namespace_id,
            &basis.content_store_id,
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
                store,
                &basis.head.namespace_id,
                &basis.content_store_id,
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

pub fn read_file_bytes<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
) -> Result<AuthoritativeFileBytes, CoreError> {
    let basis = load_verified_namespace_basis(store, namespace_id)?;
    read_file_bytes_from_basis(store, &basis, absolute_path)
}

pub fn read_file_bytes_from_basis<S: ObjectStore + ?Sized>(
    store: &S,
    basis: &crate::VerifiedNamespaceBasis,
    absolute_path: &str,
) -> Result<AuthoritativeFileBytes, CoreError> {
    let entry = resolve_path_from_basis(store, basis, absolute_path)?;
    if entry.inode_kind != InodeKind::File {
        return Err(CoreError::ExpectedFile {
            path: entry.absolute_path,
            kind: entry.inode_kind,
        });
    }
    let content_ref = entry
        .content_ref
        .clone()
        .ok_or_else(|| CoreError::MissingPath(absolute_path.to_owned()))?;
    let read = read_durable_content_bytes(store, &basis.content_store_id, &content_ref)?;
    Ok(AuthoritativeFileBytes {
        entry,
        bytes: read.bytes,
    })
}

fn build_authoritative_path_entry<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    content_store_id: &ContentStoreId,
    head_seq: ChangeSeq,
    metadata_state: &MetadataState,
    resolved: &ResolvedVisiblePath,
) -> Result<AuthoritativePathEntry, CoreError> {
    let revision = metadata_state.latest_revision_head_at_seq(resolved.inode_id, head_seq);
    let content_ref = revision
        .as_ref()
        .map(|revision| revision.content_ref.clone());
    let size_bytes = match content_ref.as_ref() {
        Some(content_ref) => {
            let validated =
                validate_durable_content_reference(store, content_store_id, content_ref)?;
            Some(validated.file_size_bytes)
        }
        None => None,
    };

    Ok(AuthoritativePathEntry {
        namespace_id: namespace_id.clone(),
        absolute_path: resolved.absolute_path.clone(),
        inode_id: resolved.inode_id,
        inode_kind: resolved.inode_kind.clone(),
        authoritative_head_seq: head_seq,
        parent_inode_id: resolved.parent_inode_id,
        display_name: resolved.display_name.clone(),
        revision_no: revision.as_ref().map(|revision| revision.revision_no),
        size_bytes,
        content_ref,
    })
}
