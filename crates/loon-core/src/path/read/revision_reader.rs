use super::resolver::resolve_path_from_basis;
use crate::basis::VerifiedNamespaceBasis;
use crate::content::read_durable_content_bytes;
use crate::error::CoreError;
use crate::metadata::RevisionRecord;
use loon_api::{
    AuthoritativeFileBytes, FileRevision, InodeId, InodeKind, ListFileRevisionsResponse, RevisionNo,
};
use loon_objectstore::ObjectStore;

pub(crate) fn read_file_bytes_from_basis<S: ObjectStore + ?Sized>(
    store: &S,
    basis: &VerifiedNamespaceBasis,
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

pub(crate) fn list_file_revisions_from_basis(
    basis: &VerifiedNamespaceBasis,
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

pub(crate) fn list_file_revisions_for_inode_from_basis(
    basis: &VerifiedNamespaceBasis,
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

pub(crate) fn read_file_revision_bytes_from_basis<S: ObjectStore + ?Sized>(
    store: &S,
    basis: &VerifiedNamespaceBasis,
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

pub(crate) fn read_file_revision_bytes_for_inode_from_basis<S: ObjectStore + ?Sized>(
    store: &S,
    basis: &VerifiedNamespaceBasis,
    inode_id: InodeId,
    revision_no: RevisionNo,
) -> Result<Vec<u8>, CoreError> {
    let revision = revision_for_inode(basis, inode_id, revision_no)?;
    let read = read_durable_content_bytes(store, &basis.content_store_id, &revision.content_ref)?;
    Ok(read.bytes)
}

fn revision_for_inode(
    basis: &VerifiedNamespaceBasis,
    inode_id: InodeId,
    revision_no: RevisionNo,
) -> Result<RevisionRecord, CoreError> {
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
