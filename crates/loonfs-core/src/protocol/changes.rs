//! The change feed: committed changes after a sequence number, with each
//! commit's durable WAL deltas mapped to semantic filesystem events.

use crate::binding_generation::BindingGeneration;
use crate::error::{CoreError, Result};
use crate::metadata::MetadataView;
use crate::path::read::LoadedMetadataView;
use loonfs_api::v0::{Commit, FilesystemChange, ListChangesResponse};
use loonfs_api::wire::wal::{WalCommitDelta, WalCommitPayload, WalDelta};
use loonfs_api::{ChangeSeq, EffectiveLimit, NamespaceId};
use loonfs_objectstore::ObjectStore;

pub(crate) async fn list_changes_after<S: ObjectStore + ?Sized>(
    view: &LoadedMetadataView<'_, S>,
    after_seq: ChangeSeq,
    limit: EffectiveLimit,
) -> Result<ListChangesResponse> {
    let head = view.head();
    let retention_floor_seq = view.retention_floor_seq();
    let namespace_id = &head.namespace_id;
    crate::namespace::control::ensure_namespace_live(head)?;

    if after_seq < retention_floor_seq {
        return Err(CoreError::RebootstrapRequired {
            after_seq,
            retention_floor_seq,
        });
    }
    if after_seq > head.seq {
        return Err(CoreError::InvalidCursor(format!(
            "change feed sequence `{after_seq}` is ahead of namespace head `{}`",
            head.seq
        )));
    }
    if after_seq == head.seq {
        return Ok(ListChangesResponse {
            namespace_id: namespace_id.clone(),
            after_seq,
            through_seq: head.seq,
            next_after_seq: None,
            changes: Vec::new(),
        });
    }

    let records = view
        .metadata_view()
        .commits_after(after_seq, limit.as_usize())
        .await?;
    let changes = records
        .iter()
        .map(|record| committed_change_from_wal_record(namespace_id, record))
        .collect::<Result<Vec<_>>>()?;
    let (through_seq, next_after_seq) = if changes.len() == limit.as_usize() {
        let committed_seq = changes
            .last()
            .expect("a full change page should contain a change")
            .committed_seq;
        (
            committed_seq,
            (committed_seq < head.seq).then_some(committed_seq),
        )
    } else {
        (head.seq, None)
    };

    Ok(ListChangesResponse {
        namespace_id: namespace_id.clone(),
        after_seq,
        through_seq,
        next_after_seq,
        changes,
    })
}

pub(super) async fn find_committed_change_at<S: ObjectStore + ?Sized>(
    view: &MetadataView<'_, '_, S>,
    namespace_id: &NamespaceId,
    committed_seq: ChangeSeq,
) -> Result<Option<Commit>> {
    view.commit_at_seq(committed_seq)
        .await?
        .as_ref()
        .map(|record| committed_change_from_wal_record(namespace_id, record))
        .transpose()
}

/// Converts one WAL commit record into the shared API change shape.
pub(super) fn committed_change_from_wal_record(
    namespace_id: &NamespaceId,
    record: &WalCommitPayload,
) -> Result<Commit> {
    Ok(Commit {
        namespace_id: namespace_id.clone(),
        committed_seq: record.seq,
        commit_id: record.commit_id.clone(),
        committed_by: record.committed_by.clone(),
        committed_at_ms: record.committed_at_ms,
        message: record.message.clone(),
        events: Some(events_from_wal_deltas(
            namespace_id,
            record.seq,
            &record.deltas,
        )?),
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
///
/// `committed_seq` is also the bind sequence for bindings created by this
/// commit.
pub(crate) fn events_from_wal_deltas(
    namespace_id: &NamespaceId,
    committed_seq: ChangeSeq,
    deltas: &[WalCommitDelta],
) -> Result<Vec<FilesystemChange>> {
    let mut events = Vec::new();
    let mut group: Vec<&WalDelta> = Vec::new();
    let mut group_op_index = None;
    for delta in deltas {
        if group_op_index != Some(delta.semantic_op_index) {
            if group_op_index.is_some() {
                events.push(event_from_op_deltas(namespace_id, committed_seq, &group)?);
                group.clear();
            }
            group_op_index = Some(delta.semantic_op_index);
        }
        group.push(&delta.delta);
    }
    if group_op_index.is_some() {
        events.push(event_from_op_deltas(namespace_id, committed_seq, &group)?);
    }
    Ok(events)
}

fn event_from_op_deltas(
    namespace_id: &NamespaceId,
    committed_seq: ChangeSeq,
    deltas: &[&WalDelta],
) -> Result<FilesystemChange> {
    Ok(match deltas {
        // CreateDirectory: allocate + bind.
        [WalDelta::CreateInode {
            inode_id,
            inode_kind: loonfs_api::InodeKind::Directory,
            ..
        }, WalDelta::BindDirentry {
            delta_index,
            parent_inode_id,
            display_name,
            child_inode_id,
            ..
        }] if child_inode_id == inode_id => FilesystemChange::DirectoryCreated {
            inode_id: *inode_id,
            parent_inode_id: *parent_inode_id,
            display_name: display_name.clone(),
            binding_generation: binding_generation(namespace_id, committed_seq, *delta_index)?,
        },
        // CreateFile (and copy-file): allocate + bind + first revision.
        [WalDelta::CreateInode {
            inode_id,
            inode_kind: loonfs_api::InodeKind::File,
            ..
        }, WalDelta::BindDirentry {
            delta_index,
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
                binding_generation: binding_generation(namespace_id, committed_seq, *delta_index)?,
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
            parent_inode_id: source_parent_inode_id,
            display_name: from_name,
            child_inode_id,
            ..
        }, WalDelta::BindDirentry {
            delta_index,
            parent_inode_id: destination_parent_inode_id,
            display_name: to_name,
            child_inode_id: bound_inode_id,
            ..
        }] if child_inode_id == bound_inode_id => FilesystemChange::Moved {
            inode_id: *child_inode_id,
            source_parent_inode_id: *source_parent_inode_id,
            source_display_name: from_name.clone(),
            destination_parent_inode_id: *destination_parent_inode_id,
            destination_display_name: to_name.clone(),
            binding_generation: binding_generation(namespace_id, committed_seq, *delta_index)?,
        },
        // DeleteFile / DeleteSubtree: retire the binding, hide the subtree.
        [WalDelta::UnbindDirentry { child_inode_id, .. }, WalDelta::TombstoneSubtree {
            root_inode_id,
            deleted_direntry,
            ..
        }] if child_inode_id == root_inode_id => FilesystemChange::Deleted {
            inode_id: *root_inode_id,
            deleted_binding: loonfs_api::v0::DirectoryBinding {
                parent_inode_id: deleted_direntry.parent_inode_id,
                name_key: deleted_direntry.name_key.clone(),
                display_name: deleted_direntry.display_name.clone(),
            },
        },
        // Undelete: revoke the exact deletion generation, re-bind the root.
        [WalDelta::RevokeSubtreeTombstone { root_inode_id, .. }, WalDelta::BindDirentry {
            delta_index,
            parent_inode_id,
            display_name,
            child_inode_id,
            ..
        }] if root_inode_id == child_inode_id => FilesystemChange::Undeleted {
            inode_id: *root_inode_id,
            parent_inode_id: *parent_inode_id,
            display_name: display_name.clone(),
            binding_generation: binding_generation(namespace_id, committed_seq, *delta_index)?,
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
        [WalDelta::AppendAccessRevision {
            inode_id,
            access_revision_no,
            boundary,
            grants,
            ..
        }] => FilesystemChange::AccessChanged {
            inode_id: *inode_id,
            access_revision_no: *access_revision_no,
            boundary: *boundary,
            grants: grants.clone(),
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

fn binding_generation(
    namespace_id: &NamespaceId,
    bind_seq: ChangeSeq,
    bind_delta_index: u32,
) -> Result<loonfs_api::BindingGeneration> {
    BindingGeneration {
        bind_seq,
        bind_delta_index,
    }
    .encode(namespace_id)
    .map_err(|error| CoreError::Internal(format!("failed to encode a binding generation: {error}")))
}

#[cfg(test)]
mod tests {
    use super::event_from_op_deltas;
    use crate::checkpoint::{
        MetadataSegmentCache, WalTailProjectionCache, WalTailProjectionCacheConfig,
    };
    use crate::context::MutationContext;
    use crate::error::CoreError;
    use crate::namespace::bootstrap::bootstrap_namespace;
    use crate::namespace::read_anchor::load_head_and_metadata_basis;
    use crate::{NamespaceEngine, RuntimeReadContext};
    use loonfs_api::v0::FilesystemChange;
    use loonfs_api::wire::wal::WalDelta;
    use loonfs_api::{
        AttributeKey, AttributeRevisionNo, AttributeValue, Attributes, ChangeSeq, EffectiveLimit,
        InodeId, NamespaceId,
    };
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use std::num::NonZeroU32;
    use std::sync::Arc;
    use tempfile::tempdir;

    fn namespace_id() -> NamespaceId {
        NamespaceId::parse("demo").expect("valid namespace id")
    }

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

    #[tokio::test]
    async fn change_feed_accepts_the_head_but_rejects_a_future_cursor() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = namespace_id();
        bootstrap_namespace(
            &store,
            &namespace_id,
            &MutationContext {
                writer_id: loonfs_api::WriterId::parse("writer").expect("writer id"),
                now_ms: 1,
            },
            &loonfs_test_support::test_actor(),
            &loonfs_api::NamespaceAccess::Unrestricted {},
            false,
        )
        .await
        .expect("bootstrap");
        let limit = EffectiveLimit::new(NonZeroU32::MIN);
        let loaded = load_head_and_metadata_basis(&store, &namespace_id)
            .await
            .expect("load read basis");
        let context = RuntimeReadContext {
            head: loaded.head,
            basis: loaded.basis,
            segment_cache: Arc::new(MetadataSegmentCache::new(Default::default())),
            tail_cache: Arc::new(WalTailProjectionCache::new(
                WalTailProjectionCacheConfig {
                    max_entries: 1,
                    max_rows: usize::MAX,
                    max_decoded_bytes: usize::MAX,
                },
                None,
            )),
        };
        let engine = NamespaceEngine::reader(&store, namespace_id);

        let caught_up = engine
            .list_changes_after(ChangeSeq(0), limit, &context)
            .await
            .expect("the head is a valid cursor");
        assert!(caught_up.changes.is_empty());
        assert_eq!(caught_up.through_seq, ChangeSeq(0));

        let error = engine
            .list_changes_after(ChangeSeq(1), limit, &context)
            .await
            .expect_err("an unpublished sequence cannot be a valid cursor");
        assert!(matches!(error, CoreError::InvalidCursor(_)), "{error}");
    }

    #[test]
    fn one_attribute_delta_maps_to_one_event_carrying_the_whole_map() {
        let delta = append_attributes(0);

        assert_eq!(
            event_from_op_deltas(&namespace_id(), ChangeSeq(7), &[&delta])
                .expect("map the operation"),
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

        let error = event_from_op_deltas(&namespace_id(), ChangeSeq(7), &[&first, &second])
            .expect_err("two attribute deltas are not one operation");
        assert!(error.to_string().contains("drifted"), "{error}");
    }
}
