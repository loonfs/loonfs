//! The change feed: committed changes after a sequence number, with each
//! commit's durable WAL deltas mapped to semantic filesystem events.

use crate::error::{CoreError, MetadataProjectionLoadError, Result};
use crate::namespace::catalog::load_namespace_catalog_entry;
use crate::namespace::control::{load_namespace_head_control, read_wal_floor_seq_or_zero};
use crate::wal::{load_validated_wal_chain, WalChainLoadRequest};
use loonfs_api::v0::{ChangesResponse, CommittedChange, FilesystemChange};
use loonfs_api::wire::control::NamespaceState;
use loonfs_api::wire::wal::{WalCommitDelta, WalDelta};
use loonfs_api::{ChangeSeq, EffectiveLimit, NamespaceId};
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
                    committed_at_ms: record.committed_at_ms,
                    message: record.message.clone(),
                    writer_id: record.writer_id.clone(),
                    writer_session_id: record.writer_session_id.clone(),
                    events: events_from_wal_deltas(&record.deltas)?,
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

/// Maps one commit's ordered WAL deltas to semantic filesystem events, one
/// per request operation.
///
/// The reducer materializes every operation as one fixed delta pattern
/// (`materialize_validated_op`), so this match is total over well-formed
/// commits; an unmatched pattern means the feed mapper and the reducer have
/// drifted and is reported as a server error rather than guessed at.
pub(crate) fn events_from_wal_deltas(
    deltas: &[WalCommitDelta],
) -> Result<Vec<FilesystemChange>> {
    let mut events = Vec::new();
    let mut group: Vec<&WalDelta> = Vec::new();
    let mut group_op_index = None;
    for delta in deltas {
        if group_op_index != Some(delta.semantic_op_index) {
            if group_op_index.is_some() {
                events.push(event_from_op_deltas(&group)?);
                group.clear();
            }
            group_op_index = Some(delta.semantic_op_index);
        }
        group.push(&delta.delta);
    }
    if group_op_index.is_some() {
        events.push(event_from_op_deltas(&group)?);
    }
    Ok(events)
}

fn event_from_op_deltas(deltas: &[&WalDelta]) -> Result<FilesystemChange> {
    Ok(match deltas {
        // CreateDirectory: allocate + bind.
        [WalDelta::CreateInode {
            inode_id,
            inode_kind,
            ..
        }, WalDelta::BindDirentry {
            parent_inode_id,
            display_name,
            child_inode_id,
            ..
        }] if child_inode_id == inode_id => FilesystemChange::Created {
            inode_id: *inode_id,
            inode_kind: *inode_kind,
            parent_inode_id: *parent_inode_id,
            name: display_name.clone(),
            revision_no: None,
            content_ref: None,
        },
        // CreateFile (and copy-file): allocate + bind + first revision.
        [WalDelta::CreateInode {
            inode_id,
            inode_kind,
            ..
        }, WalDelta::BindDirentry {
            parent_inode_id,
            display_name,
            child_inode_id,
            ..
        }, WalDelta::AppendFileRevision {
            inode_id: revision_inode_id,
            revision_no,
            content_ref,
            ..
        }] if child_inode_id == inode_id && revision_inode_id == inode_id => {
            FilesystemChange::Created {
                inode_id: *inode_id,
                inode_kind: *inode_kind,
                parent_inode_id: *parent_inode_id,
                name: display_name.clone(),
                revision_no: Some(*revision_no),
                content_ref: Some(content_ref.clone()),
            }
        }
        // ReplaceFile or RestoreRevision: one durable fact for both.
        [WalDelta::AppendFileRevision {
            inode_id,
            revision_no,
            content_ref,
            ..
        }] => FilesystemChange::ContentChanged {
            inode_id: *inode_id,
            revision_no: *revision_no,
            content_ref: content_ref.clone(),
        },
        // Rename: retire the old binding, publish the new one.
        [WalDelta::UnbindDirentry {
            parent_inode_id: from_parent_inode_id,
            display_name: from_name,
            child_inode_id,
            ..
        }, WalDelta::BindDirentry {
            parent_inode_id: to_parent_inode_id,
            display_name: to_name,
            child_inode_id: bound_inode_id,
            ..
        }] if child_inode_id == bound_inode_id => FilesystemChange::Moved {
            inode_id: *child_inode_id,
            from_parent_inode_id: *from_parent_inode_id,
            from_name: from_name.clone(),
            to_parent_inode_id: *to_parent_inode_id,
            to_name: to_name.clone(),
        },
        // DeleteFile / DeleteSubtree: retire the binding, hide the subtree.
        [WalDelta::UnbindDirentry {
            child_inode_id, ..
        }, WalDelta::TombstoneSubtree {
            root_inode_id,
            parent_inode_id,
            display_name,
            ..
        }] if child_inode_id == root_inode_id => FilesystemChange::Deleted {
            inode_id: *root_inode_id,
            parent_inode_id: *parent_inode_id,
            name: display_name.clone(),
        },
        // Undelete: revoke the exact deletion generation, re-bind the root.
        [WalDelta::RevokeSubtreeTombstone { root_inode_id, .. }, WalDelta::BindDirentry {
            parent_inode_id,
            display_name,
            child_inode_id,
            ..
        }] if root_inode_id == child_inode_id => FilesystemChange::Undeleted {
            inode_id: *root_inode_id,
            parent_inode_id: *parent_inode_id,
            name: display_name.clone(),
        },
        other => {
            return Err(CoreError::Internal(format!(
                "change feed cannot map a committed operation's delta \
                 pattern ({} deltas); the feed mapper and the commit \
                 reducer have drifted",
                other.len()
            )))
        }
    })
}
