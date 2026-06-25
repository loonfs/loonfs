use super::resolver::resolve_path_from_full_materialization;
use crate::error::CoreError;
use crate::metadata::RevisionRecord;
use crate::namespace::full_materialization::FullNamespaceMaterialization;
use crate::storage::content::read_durable_content_bytes;
use loonfs_api::{
    AuthoritativeFileBytes, FileRevision, InodeId, InodeKind, ListFileRevisionsResponse, RevisionNo,
};
use loonfs_objectstore::ObjectStore;

pub(crate) async fn read_file_bytes_from_full_materialization<S: ObjectStore + ?Sized>(
    store: &S,
    materialization: &FullNamespaceMaterialization,
    absolute_path: &str,
) -> Result<AuthoritativeFileBytes, CoreError> {
    let entry = resolve_path_from_full_materialization(materialization, absolute_path)?;
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
    let read =
        read_durable_content_bytes(store, &materialization.content_store_id, &content_ref).await?;
    Ok(AuthoritativeFileBytes {
        entry,
        bytes: read.bytes,
    })
}

pub(crate) fn list_file_revisions_from_full_materialization(
    materialization: &FullNamespaceMaterialization,
    absolute_path: &str,
) -> Result<ListFileRevisionsResponse, CoreError> {
    let entry = resolve_path_from_full_materialization(materialization, absolute_path)?;
    if entry.inode_kind != InodeKind::File {
        return Err(CoreError::ExpectedFile {
            path: entry.absolute_path,
            kind: entry.inode_kind,
        });
    }
    list_file_revisions_for_inode_from_full_materialization(materialization, entry.inode_id)
}

pub(crate) fn list_file_revisions_for_inode_from_full_materialization(
    materialization: &FullNamespaceMaterialization,
    inode_id: InodeId,
) -> Result<ListFileRevisionsResponse, CoreError> {
    let inode = materialization
        .metadata_state
        .inode_at_seq(inode_id, materialization.head.seq)
        .ok_or_else(|| CoreError::MissingPath(inode_id.to_string()))?;
    if inode.inode_kind != InodeKind::File {
        return Err(CoreError::ExpectedFile {
            path: inode_id.to_string(),
            kind: inode.inode_kind,
        });
    }

    let mut revisions = materialization
        .metadata_state
        .revisions()
        .iter()
        .filter(|revision| {
            revision.inode_id == inode_id && revision.committed_seq <= materialization.head.seq
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
        namespace_id: materialization.head.namespace_id.clone(),
        inode_id,
        head_seq: materialization.head.seq,
        revisions,
    })
}

pub(crate) async fn read_file_revision_bytes_from_full_materialization<S: ObjectStore + ?Sized>(
    store: &S,
    materialization: &FullNamespaceMaterialization,
    absolute_path: &str,
    revision_no: RevisionNo,
) -> Result<AuthoritativeFileBytes, CoreError> {
    let mut entry = resolve_path_from_full_materialization(materialization, absolute_path)?;
    if entry.inode_kind != InodeKind::File {
        return Err(CoreError::ExpectedFile {
            path: entry.absolute_path,
            kind: entry.inode_kind,
        });
    }
    let revision = revision_for_inode(materialization, entry.inode_id, revision_no)?;
    entry.revision_no = Some(revision.revision_no);
    entry.size_bytes = Some(revision.content_ref.size_bytes);
    entry.content_ref = Some(revision.content_ref.clone());
    let read = read_durable_content_bytes(
        store,
        &materialization.content_store_id,
        &revision.content_ref,
    )
    .await?;
    Ok(AuthoritativeFileBytes {
        entry,
        bytes: read.bytes,
    })
}

pub(crate) async fn read_file_revision_bytes_for_inode_from_full_materialization<
    S: ObjectStore + ?Sized,
>(
    store: &S,
    materialization: &FullNamespaceMaterialization,
    inode_id: InodeId,
    revision_no: RevisionNo,
) -> Result<Vec<u8>, CoreError> {
    let revision = revision_for_inode(materialization, inode_id, revision_no)?;
    let read = read_durable_content_bytes(
        store,
        &materialization.content_store_id,
        &revision.content_ref,
    )
    .await?;
    Ok(read.bytes)
}

fn revision_for_inode(
    materialization: &FullNamespaceMaterialization,
    inode_id: InodeId,
    revision_no: RevisionNo,
) -> Result<RevisionRecord, CoreError> {
    let inode = materialization
        .metadata_state
        .inode_at_seq(inode_id, materialization.head.seq)
        .ok_or_else(|| CoreError::MissingPath(inode_id.to_string()))?;
    if inode.inode_kind != InodeKind::File {
        return Err(CoreError::ExpectedFile {
            path: inode_id.to_string(),
            kind: inode.inode_kind,
        });
    }
    materialization
        .metadata_state
        .revision_at_seq(inode_id, revision_no, materialization.head.seq)
        .ok_or(CoreError::MissingRevision {
            inode_id,
            revision_no,
        })
}
