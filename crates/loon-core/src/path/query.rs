use super::helpers::{map_path_error_to_core, parse_absolute_path_for_core};
use crate::basis::load_verified_namespace_basis;
use crate::content::read_durable_content_bytes;
use crate::error::CoreError;
use crate::metadata::{MetadataState, ResolvedVisiblePath};
use loon_api::{
    AbsolutePath, AuthoritativeFileBytes, AuthoritativePathEntry, ChangeSeq, DisplayName,
    FileRevision, InodeId, InodeKind, ListFileRevisionsResponse, NamespaceId, RevisionNo,
};
use loon_objectstore::ObjectStore;

pub fn resolve_path<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
) -> Result<AuthoritativePathEntry, CoreError> {
    let basis = load_verified_namespace_basis(store, namespace_id)?;
    resolve_path_from_basis(&basis, absolute_path)
}

pub fn resolve_path_from_basis(
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
        &basis.head.namespace_id,
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
    list_path_from_basis(&basis, absolute_path)
}

pub fn list_path_from_basis(
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
    let entry = resolve_path_from_basis(basis, absolute_path)?;
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

pub fn list_file_revisions<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
) -> Result<ListFileRevisionsResponse, CoreError> {
    let basis = load_verified_namespace_basis(store, namespace_id)?;
    list_file_revisions_from_basis(&basis, absolute_path)
}

pub fn list_file_revisions_from_basis(
    basis: &crate::VerifiedNamespaceBasis,
    absolute_path: &str,
) -> Result<ListFileRevisionsResponse, CoreError> {
    let entry = resolve_path_from_basis(basis, absolute_path)?;
    if entry.inode_kind != InodeKind::File {
        return Err(CoreError::ExpectedFile {
            path: entry.absolute_path,
            kind: entry.inode_kind,
        });
    }
    list_file_revisions_for_inode_from_basis(basis, entry.inode_id)
}

pub fn list_file_revisions_for_inode<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
) -> Result<ListFileRevisionsResponse, CoreError> {
    let basis = load_verified_namespace_basis(store, namespace_id)?;
    list_file_revisions_for_inode_from_basis(&basis, inode_id)
}

pub fn list_file_revisions_for_inode_from_basis(
    basis: &crate::VerifiedNamespaceBasis,
    inode_id: InodeId,
) -> Result<ListFileRevisionsResponse, CoreError> {
    let inode = basis
        .metadata_state
        .inode_at_seq(inode_id, basis.head.seq)
        .ok_or_else(|| CoreError::MissingPath(inode_id.to_string()))?;
    if inode.inode_kind != InodeKind::File {
        return Err(CoreError::ExpectedFile {
            path: inode_id.to_string(),
            kind: inode.inode_kind,
        });
    }

    let mut revisions = basis
        .metadata_state
        .revisions()
        .iter()
        .filter(|revision| {
            revision.inode_id == inode_id && revision.committed_seq <= basis.head.seq
        })
        .map(|revision| FileRevision {
            inode_id: revision.inode_id,
            revision_no: revision.revision_no,
            committed_seq: revision.committed_seq,
            content_ref: revision.content_ref.clone(),
        })
        .collect::<Vec<_>>();
    revisions.sort_by_key(|revision| revision.revision_no);

    Ok(ListFileRevisionsResponse {
        namespace_id: basis.head.namespace_id.clone(),
        inode_id,
        head_seq: basis.head.seq,
        revisions,
    })
}

pub fn read_file_revision_bytes<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    revision_no: RevisionNo,
) -> Result<AuthoritativeFileBytes, CoreError> {
    let basis = load_verified_namespace_basis(store, namespace_id)?;
    read_file_revision_bytes_from_basis(store, &basis, absolute_path, revision_no)
}

pub fn read_file_revision_bytes_from_basis<S: ObjectStore + ?Sized>(
    store: &S,
    basis: &crate::VerifiedNamespaceBasis,
    absolute_path: &str,
    revision_no: RevisionNo,
) -> Result<AuthoritativeFileBytes, CoreError> {
    let mut entry = resolve_path_from_basis(basis, absolute_path)?;
    if entry.inode_kind != InodeKind::File {
        return Err(CoreError::ExpectedFile {
            path: entry.absolute_path,
            kind: entry.inode_kind,
        });
    }
    let revision = revision_for_inode(basis, entry.inode_id, revision_no)?;
    entry.revision_no = Some(revision.revision_no);
    entry.size_bytes = Some(revision.content_ref.size_bytes);
    entry.content_ref = Some(revision.content_ref.clone());
    let read = read_durable_content_bytes(store, &basis.content_store_id, &revision.content_ref)?;
    Ok(AuthoritativeFileBytes {
        entry,
        bytes: read.bytes,
    })
}

pub fn read_file_revision_bytes_for_inode<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    revision_no: RevisionNo,
) -> Result<Vec<u8>, CoreError> {
    let basis = load_verified_namespace_basis(store, namespace_id)?;
    read_file_revision_bytes_for_inode_from_basis(store, &basis, inode_id, revision_no)
}

pub fn read_file_revision_bytes_for_inode_from_basis<S: ObjectStore + ?Sized>(
    store: &S,
    basis: &crate::VerifiedNamespaceBasis,
    inode_id: InodeId,
    revision_no: RevisionNo,
) -> Result<Vec<u8>, CoreError> {
    let revision = revision_for_inode(basis, inode_id, revision_no)?;
    let read = read_durable_content_bytes(store, &basis.content_store_id, &revision.content_ref)?;
    Ok(read.bytes)
}

fn revision_for_inode(
    basis: &crate::VerifiedNamespaceBasis,
    inode_id: InodeId,
    revision_no: RevisionNo,
) -> Result<crate::metadata::RevisionRecord, CoreError> {
    let inode = basis
        .metadata_state
        .inode_at_seq(inode_id, basis.head.seq)
        .ok_or_else(|| CoreError::MissingPath(inode_id.to_string()))?;
    if inode.inode_kind != InodeKind::File {
        return Err(CoreError::ExpectedFile {
            path: inode_id.to_string(),
            kind: inode.inode_kind,
        });
    }
    basis
        .metadata_state
        .revision_at_seq(inode_id, revision_no, basis.head.seq)
        .ok_or(CoreError::MissingRevision {
            inode_id,
            revision_no,
        })
}

fn build_authoritative_path_entry(
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
        authoritative_head_seq: head_seq,
        parent_inode_id: resolved.parent_inode_id,
        display_name: resolved.display_name.clone(),
        revision_no: revision.as_ref().map(|revision| revision.revision_no),
        size_bytes,
        content_ref,
    })
}
