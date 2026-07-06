//! The change feed: committed changes after a sequence number, with durable
//! WAL deltas converted to API deltas.

use crate::error::{CoreError, MetadataProjectionLoadError, Result};
use crate::namespace::catalog::load_namespace_catalog_entry;
use crate::namespace::control::{load_namespace_head_control, read_wal_floor_seq_or_zero};
use crate::wal::{load_validated_wal_chain, WalChainLoadRequest};
use loonfs_api::v0::{ChangesResponse, CommitDelta, CommittedChange};
use loonfs_api::wire::control::NamespaceState;
use loonfs_api::wire::wal::{WalCommitDelta, WalDelta};
use loonfs_api::{ChangeSeq, EffectiveLimit, NameKey, NamespaceId};
use loonfs_objectstore::ObjectStore;

pub(crate) async fn list_changes_after<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    after_seq: ChangeSeq,
    limit: EffectiveLimit,
) -> Result<ChangesResponse> {
    load_namespace_catalog_entry(store, namespace_id).await?;
    let head = load_namespace_head_control(store, namespace_id)
        .await
        .map_err(|error| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::LoadHead(error))
        })?
        .state;
    if head.state == NamespaceState::Deleted {
        return Err(CoreError::NamespaceDeleted {
            namespace_id: namespace_id.clone(),
        });
    }

    let retention_floor_seq = read_wal_floor_seq_or_zero(store, namespace_id)
        .await
        .map_err(|error| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::LoadHead(error))
        })?;
    if after_seq < retention_floor_seq {
        return Err(CoreError::RebootstrapRequired {
            after_seq,
            retention_floor_seq,
        });
    }
    if after_seq >= head.seq {
        return Ok(ChangesResponse {
            namespace_id: namespace_id.clone(),
            after_seq,
            through_seq: head.seq,
            next_after_seq: None,
            changes: Vec::new(),
        });
    }

    let wal_chain = load_validated_wal_chain(
        store,
        WalChainLoadRequest {
            namespace_id,
            chain_base_seq: retention_floor_seq,
            head_seq: head.seq,
            visible_tip: head.visible_wal_tip.clone(),
            stop_after_seq: Some(after_seq),
            recent_segments: &head.recent_segments,
        },
    )
    .await
    .map_err(|error| {
        CoreError::MetadataProjection(MetadataProjectionLoadError::WalChainLoad(error))
    })?;
    let mut changes = Vec::with_capacity(limit.as_usize());
    let mut through_seq = head.seq;
    let mut next_after_seq = None;
    'segments: for segment in wal_chain.segments() {
        for record in segment.records() {
            if record.seq > after_seq {
                let seq = record.seq;
                changes.push(CommittedChange {
                    seq,
                    commit_id: record.commit_id.clone(),
                    message: record.message.clone(),
                    deltas: record
                        .deltas
                        .iter()
                        .map(commit_delta_from_wal)
                        .collect::<std::result::Result<Vec<_>, _>>()?,
                });
                if changes.len() == limit.as_usize() {
                    through_seq = seq;
                    if seq < head.seq {
                        next_after_seq = Some(seq);
                    }
                    break 'segments;
                }
            }
        }
    }

    Ok(ChangesResponse {
        namespace_id: namespace_id.clone(),
        after_seq,
        through_seq,
        next_after_seq,
        changes,
    })
}

fn commit_delta_from_wal(delta: &WalCommitDelta) -> Result<CommitDelta> {
    let semantic_op_index = delta.semantic_op_index;
    Ok(match &delta.delta {
        WalDelta::CreateInode {
            delta_index,
            inode_id,
            inode_kind,
        } => CommitDelta::CreateInode {
            semantic_op_index,
            delta_index: *delta_index,
            inode_id: *inode_id,
            inode_kind: *inode_kind,
        },
        WalDelta::BindDirentry {
            delta_index,
            parent_inode_id,
            name_key,
            display_name,
            child_inode_id,
        } => CommitDelta::BindDirentry {
            semantic_op_index,
            delta_index: *delta_index,
            parent_inode_id: *parent_inode_id,
            name_key: NameKey::parse(name_key).map_err(|err| {
                CoreError::NamespaceCorrupt(format!("invalid WAL name_key: {err}"))
            })?,
            display_name: display_name.clone(),
            child_inode_id: *child_inode_id,
        },
        WalDelta::UnbindDirentry {
            delta_index,
            parent_inode_id,
            name_key,
            child_inode_id,
            bind_seq,
            bind_delta_index,
        } => CommitDelta::UnbindDirentry {
            semantic_op_index,
            delta_index: *delta_index,
            parent_inode_id: *parent_inode_id,
            name_key: NameKey::parse(name_key).map_err(|err| {
                CoreError::NamespaceCorrupt(format!("invalid WAL name_key: {err}"))
            })?,
            child_inode_id: *child_inode_id,
            bind_seq: *bind_seq,
            bind_delta_index: *bind_delta_index,
        },
        WalDelta::AppendFileRevision {
            delta_index,
            inode_id,
            revision_no,
            content_ref,
        } => CommitDelta::AppendFileRevision {
            semantic_op_index,
            delta_index: *delta_index,
            inode_id: *inode_id,
            revision_no: *revision_no,
            content_ref: content_ref.clone(),
        },
        WalDelta::TombstoneSubtree {
            delta_index,
            root_inode_id,
        } => CommitDelta::TombstoneSubtree {
            semantic_op_index,
            delta_index: *delta_index,
            root_inode_id: *root_inode_id,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;
    use loonfs_api::{ChangeSeq, InodeId};

    #[test]
    fn invalid_wal_delta_name_key_is_namespace_corrupt() {
        let delta = WalCommitDelta {
            semantic_op_index: 0,
            delta: WalDelta::BindDirentry {
                delta_index: 0,
                parent_inode_id: InodeId(1),
                name_key: "bad/key".to_owned(),
                display_name: "file.txt".to_owned(),
                child_inode_id: InodeId(2),
            },
        };

        let error = commit_delta_from_wal(&delta).expect_err("invalid durable WAL name key");

        assert_eq!(error.code(), ErrorCode::NamespaceCorrupt);
    }

    #[test]
    fn invalid_wal_unbind_name_key_is_namespace_corrupt() {
        let delta = WalCommitDelta {
            semantic_op_index: 0,
            delta: WalDelta::UnbindDirentry {
                delta_index: 0,
                parent_inode_id: InodeId(1),
                name_key: "bad/key".to_owned(),
                child_inode_id: InodeId(2),
                bind_seq: ChangeSeq(1),
                bind_delta_index: 0,
            },
        };

        let error = commit_delta_from_wal(&delta).expect_err("invalid durable WAL name key");

        assert_eq!(error.code(), ErrorCode::NamespaceCorrupt);
    }
}
