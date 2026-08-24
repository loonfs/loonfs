//! The change feed: committed changes after a sequence number, with each
//! commit's durable WAL deltas mapped to semantic filesystem events.

use crate::error::{CoreError, MetadataProjectionLoadError, Result};
use crate::namespace::basis::resolve_retention_floor_seq;
use crate::namespace::control::load_namespace_head_control;
use crate::wal::{load_validated_wal_chain, WalChainLoadRequest};
use loonfs_api::v0::{CommittedChange, FilesystemChange, ListChangesResponse};
use loonfs_api::wire::control::NamespaceStatus;
use loonfs_api::wire::wal::{WalCommitDelta, WalCommitPayload, WalDelta};
use loonfs_api::{ChangeSeq, EffectiveLimit, NamespaceId};
use loonfs_objectstore::ObjectStore;
use std::num::NonZeroU32;

pub(crate) async fn list_changes_after<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    after_seq: ChangeSeq,
    limit: EffectiveLimit,
) -> Result<ListChangesResponse> {
    let head = load_namespace_head_control(store, namespace_id)
        .await
        .map_err(CoreError::ControlObjectLoad)?
        .state;
    if head.status == (NamespaceStatus::Deleted {}) {
        return Err(CoreError::NamespaceDeleted {
            namespace_id: namespace_id.clone(),
        });
    }

    let retention_floor_seq = resolve_retention_floor_seq(store, &head)
        .await
        .map_err(CoreError::ControlObjectLoad)?;
    if after_seq < retention_floor_seq {
        return Err(CoreError::RebootstrapRequired {
            after_seq,
            retention_floor_seq,
        });
    }
    if after_seq >= head.seq {
        return Ok(ListChangesResponse {
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
                let committed_seq = record.seq;
                changes.push(committed_change_from_wal_record(record)?);
                if changes.len() == limit.as_usize() {
                    through_seq = committed_seq;
                    if committed_seq < head.seq {
                        next_after_seq = Some(committed_seq);
                    }
                    break 'segments;
                }
            }
        }
    }

    Ok(ListChangesResponse {
        namespace_id: namespace_id.clone(),
        after_seq,
        through_seq,
        next_after_seq,
        changes,
    })
}

/// Reads the committed change at `committed_seq` through the normal change
/// feed path. Returns `None` when no commit exists at that sequence and
/// `RebootstrapRequired` when its WAL history is no longer retained.
pub(super) async fn find_committed_change_at<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    committed_seq: ChangeSeq,
) -> Result<Option<CommittedChange>> {
    let page = list_changes_after(
        store,
        namespace_id,
        ChangeSeq(committed_seq.0.saturating_sub(1)),
        EffectiveLimit::new(NonZeroU32::MIN),
    )
    .await?;
    Ok(page
        .changes
        .into_iter()
        .find(|change| change.committed_seq == committed_seq))
}

/// Converts one WAL commit record into the shared API change shape.
pub(super) fn committed_change_from_wal_record(
    record: &WalCommitPayload,
) -> Result<CommittedChange> {
    Ok(CommittedChange {
        committed_seq: record.seq,
        commit_id: record.commit_id.clone(),
        committed_by: record.committed_by.clone(),
        committed_at_ms: record.committed_at_ms,
        message: record.message.clone(),
        events: events_from_wal_deltas(&record.deltas)?,
    })
}

/// Maps one commit's ordered WAL deltas to semantic filesystem events, one
/// per internal operation.
///
/// One request operation can compile into several internal operations —
/// creating missing parent directories, replacing a file by moving over it,
/// copying attributes onto a new inode — and each of those gets its own
/// event. The deltas carry the request-operation index they came from, so the
/// events stay in request order whatever their count.
///
/// The reducer materializes every internal operation as one fixed delta
/// pattern (`materialize_validated_op`), so this match is total over
/// well-formed commits; an unmatched pattern means the feed mapper and the
/// reducer have drifted and is reported as a server error rather than
/// guessed at.
pub(crate) fn events_from_wal_deltas(deltas: &[WalCommitDelta]) -> Result<Vec<FilesystemChange>> {
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
            inode_kind: loonfs_api::InodeKind::Directory,
            ..
        }, WalDelta::BindDirentry {
            parent_inode_id,
            display_name,
            child_inode_id,
            ..
        }] if child_inode_id == inode_id => FilesystemChange::DirectoryCreated {
            inode_id: *inode_id,
            parent_inode_id: *parent_inode_id,
            display_name: display_name.clone(),
        },
        // CreateFile (and copy-file): allocate + bind + first revision.
        [WalDelta::CreateInode {
            inode_id,
            inode_kind: loonfs_api::InodeKind::File,
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
            FilesystemChange::FileCreated {
                inode_id: *inode_id,
                parent_inode_id: *parent_inode_id,
                display_name: display_name.clone(),
                revision_no: *revision_no,
                content_ref: content_ref.clone(),
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
            from_display_name: from_name.clone(),
            to_parent_inode_id: *to_parent_inode_id,
            to_display_name: to_name.clone(),
        },
        // DeleteFile / DeleteSubtree: retire the binding, hide the subtree.
        [WalDelta::UnbindDirentry { child_inode_id, .. }, WalDelta::TombstoneSubtree {
            root_inode_id,
            deleted_direntry,
            ..
        }] if child_inode_id == root_inode_id => FilesystemChange::Deleted {
            inode_id: *root_inode_id,
            deleted_binding: deleted_direntry.as_ref().map(|direntry| {
                loonfs_api::v0::DirectoryBinding {
                    parent_inode_id: direntry.parent_inode_id,
                    name_key: direntry.name_key.clone(),
                    display_name: direntry.display_name.clone(),
                }
            }),
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
            display_name: display_name.clone(),
        },
        // UpdateAttributes, including the copy that carries a source's
        // attributes onto the inode it just created. The delta already holds
        // the complete resulting map, so the event does too.
        [WalDelta::AppendAttributesRevision {
            inode_id,
            attributes_revision_no,
            attributes,
            ..
        }] => FilesystemChange::AttributesChanged {
            inode_id: *inode_id,
            attributes_revision_no: *attributes_revision_no,
            attributes: attributes.clone(),
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

#[cfg(test)]
mod tests {
    use super::event_from_op_deltas;
    use loonfs_api::v0::FilesystemChange;
    use loonfs_api::wire::wal::WalDelta;
    use loonfs_api::{AttributeKey, AttributeRevisionNo, AttributeValue, Attributes, InodeId};

    fn attributes() -> Attributes {
        Attributes::new(std::collections::BTreeMap::from([(
            AttributeKey::parse("owner").expect("valid attribute key"),
            AttributeValue::parse("ada").expect("valid attribute value"),
        )]))
        .expect("valid attribute map")
    }

    fn append_attributes(delta_index: u32) -> WalDelta {
        WalDelta::AppendAttributesRevision {
            delta_index,
            inode_id: InodeId(7),
            attributes_revision_no: AttributeRevisionNo(3),
            attributes: attributes(),
        }
    }

    #[test]
    fn one_attribute_delta_maps_to_one_event_carrying_the_whole_map() {
        let delta = append_attributes(0);

        assert_eq!(
            event_from_op_deltas(&[&delta]).expect("map the operation"),
            FilesystemChange::AttributesChanged {
                inode_id: InodeId(7),
                attributes_revision_no: AttributeRevisionNo(3),
                attributes: attributes(),
            }
        );
    }

    #[test]
    fn a_delta_pattern_the_reducer_never_produces_is_rejected() {
        let first = append_attributes(0);
        let second = append_attributes(1);

        let error = event_from_op_deltas(&[&first, &second])
            .expect_err("two attribute deltas are not one operation");
        assert!(error.to_string().contains("drifted"), "{error}");
    }
}
