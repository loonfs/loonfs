//! The change feed: committed changes after a sequence number, with each
//! commit's durable WAL deltas mapped to semantic filesystem events.

use crate::error::{CoreError, Result};
use crate::heap_bytes::HeapBytes;
use crate::limits::CHANGE_FEED_PAGE_BYTES;
use crate::path::read::LoadedMetadataView;
use loonfs_objectstore::ObjectStore;
use loonfs_types::api::v0::{Commit, FilesystemChange, ListChangesResponse};
use loonfs_types::format::manifest::DeltaPosition;
use loonfs_types::format::wal::{WalCommitDelta, WalCommitPayload, WalDelta};
use loonfs_types::{ChangeSeq, EffectiveLimit, NamespaceId};

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

    let mut changes = Vec::new();
    let mut event_bytes = 0_usize;
    let mut through_seq = after_seq;
    while changes.len() < limit.as_usize() && through_seq < head.seq {
        let records = view.metadata_view().commits_after(through_seq, 1).await?;
        let Some(record) = records.first() else {
            through_seq = head.seq;
            break;
        };
        let change = committed_change_from_wal_record(namespace_id, record)?;
        let next_event_bytes = event_bytes.saturating_add(change.events.heap_bytes());
        if !changes.is_empty() && next_event_bytes > CHANGE_FEED_PAGE_BYTES {
            break;
        }
        event_bytes = next_event_bytes;
        through_seq = change.committed_seq;
        changes.push(change);
        if event_bytes >= CHANGE_FEED_PAGE_BYTES {
            break;
        }
    }
    let next_after_seq = (through_seq < head.seq).then_some(through_seq);

    Ok(ListChangesResponse {
        namespace_id: namespace_id.clone(),
        after_seq,
        through_seq,
        next_after_seq,
        changes,
    })
}

/// Converts one WAL commit record into the shared API change shape.
pub(super) fn committed_change_from_wal_record(
    namespace_id: &NamespaceId,
    record: &WalCommitPayload,
) -> Result<Commit> {
    Ok(Commit {
        namespace_id: namespace_id.clone(),
        committed_seq: record.committed_seq,
        commit_id: record.commit_id.clone(),
        committed_by: record.committed_by.clone(),
        committed_at_ms: record.committed_at_ms,
        message: record.message.clone(),
        events: events_from_wal_deltas(namespace_id, record.committed_seq, &record.deltas)?,
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
/// Validation emits one fixed delta pattern per internal operation. An
/// unmatched pattern means validation and the feed mapper disagree.
///
/// `committed_seq` is also the bind sequence for bindings created by this
/// commit.
pub(crate) fn events_from_wal_deltas(
    namespace_id: &NamespaceId,
    committed_seq: ChangeSeq,
    deltas: &[WalCommitDelta],
) -> Result<Vec<FilesystemChange>> {
    let mut events = Vec::new();
    let mut group = Vec::new();
    for deltas in loonfs_types::format::wal::semantic_operation_groups(deltas) {
        group.clear();
        group.extend(deltas.iter().map(|delta| &delta.delta));
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
            inode_kind: loonfs_types::InodeKind::Directory,
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
            binding_version: binding_version(namespace_id, committed_seq, *delta_index),
        },
        // CreateFile (and copy-file): allocate + bind + first revision.
        [WalDelta::CreateInode {
            inode_id,
            inode_kind: loonfs_types::InodeKind::File,
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
                binding_version: binding_version(namespace_id, committed_seq, *delta_index),
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
            binding_version: binding_version(namespace_id, committed_seq, *delta_index),
        },
        // DeleteFile / DeleteSubtree: retire the binding, hide the subtree.
        [WalDelta::UnbindDirentry { child_inode_id, .. }, WalDelta::TombstoneSubtree {
            root_inode_id,
            deleted_binding,
            ..
        }] if child_inode_id == root_inode_id => FilesystemChange::Deleted {
            inode_id: *root_inode_id,
            deleted_binding: loonfs_types::api::v0::DirectoryBinding {
                parent_inode_id: deleted_binding.parent_inode_id,
                name_key: deleted_binding.name_key.clone(),
                display_name: deleted_binding.display_name.clone(),
            },
        },
        // Undelete: revoke the exact deletion position, re-bind the root.
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
            binding_version: binding_version(namespace_id, committed_seq, *delta_index),
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

fn binding_version(
    namespace_id: &NamespaceId,
    committed_seq: ChangeSeq,
    delta_index: u32,
) -> loonfs_types::BindingVersion {
    crate::binding_version::encode(
        DeltaPosition {
            seq: committed_seq,
            delta_index,
        },
        namespace_id,
    )
}

#[cfg(test)]
mod tests {
    use super::event_from_op_deltas;
    use crate::context::MutationContext;
    use crate::error::CoreError;
    use crate::manifest::{HeadStateCache, MetadataSegmentCache};
    use crate::namespace::read_anchor::load_read_anchor;
    use crate::test_support::ops::create;
    use crate::{NamespaceEngine, RuntimeReadContext};
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use loonfs_types::api::v0::FilesystemChange;
    use loonfs_types::format::wal::WalDelta;
    use loonfs_types::{
        AttributeKey, AttributeValue, Attributes, AttributesRevisionNo, ChangeSeq, EffectiveLimit,
        InodeId, NamespaceId,
    };
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
            attributes_revision_no: AttributesRevisionNo(3),
            attributes: attributes(),
        }
    }

    #[tokio::test]
    async fn change_feed_accepts_the_head_but_rejects_a_future_cursor() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = namespace_id();
        create(
            &store,
            &namespace_id,
            &MutationContext {
                writer_id: loonfs_types::WriterId::parse("writer").expect("writer id"),
                now_ms: 1,
            },
        )
        .await
        .expect("bootstrap");
        let limit = EffectiveLimit::new(NonZeroU32::MIN);
        let loaded = load_read_anchor(&store, &namespace_id)
            .await
            .expect("load read basis");
        let context = RuntimeReadContext {
            basis: loaded.basis(),
            head: loaded.read_state,
            segment_cache: Arc::new(MetadataSegmentCache::unshared(usize::MAX)),
            head_state: Arc::new(HeadStateCache::unshared(usize::MAX)),
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

    #[tokio::test]
    async fn large_commits_page_by_event_bytes_without_splitting_or_skipping() {
        use crate::commit_engine::{publish_namespace_commits_batch, CommitCandidate};
        use crate::heap_bytes::HeapBytes;
        use crate::limits::{CHANGE_FEED_PAGE_BYTES, MAX_COMMIT_OPERATIONS};
        use crate::path::read::load_current_metadata_view;
        use crate::publish::{CommitRequest, FilesystemOperation};
        use loonfs_types::{AbsolutePath, CommitId};

        let directory = tempdir().expect("tempdir");
        let store = LocalFsStore::new(directory.path()).expect("store");
        let namespace_id = namespace_id();
        let context = MutationContext {
            writer_id: loonfs_types::WriterId::parse("writer").expect("writer id"),
            now_ms: 1,
        };
        create(&store, &namespace_id, &context)
            .await
            .expect("bootstrap");
        let mut expected = Vec::new();
        for number in 1..=5 {
            let depth = if number == 4 { 4 } else { 2 };
            let request = CommitRequest {
                preconditions: Vec::new(),
                commit_id: CommitId::parse(format!("commit-{number}")).expect("commit id"),
                actor_id: loonfs_test_support::test_actor(),
                subject: None,
                message: None,
                operations: (0..MAX_COMMIT_OPERATIONS)
                    .map(|operation| FilesystemOperation::CreateDirectory {
                        path: AbsolutePath::parse(format!(
                            "/directory-{number}-{operation}{}",
                            "/child".repeat(depth - 1)
                        ))
                        .expect("path"),
                        parents: true,
                    })
                    .collect(),
            };
            let commit = publish_namespace_commits_batch(
                &store,
                &namespace_id,
                vec![CommitCandidate::new(request)],
                &context,
            )
            .await
            .pop()
            .expect("one result")
            .expect("publish commit");
            assert_eq!(commit.events.len(), MAX_COMMIT_OPERATIONS * depth);
            assert_eq!(
                commit.events.heap_bytes() > CHANGE_FEED_PAGE_BYTES,
                number == 4
            );
            expected.push((commit.committed_seq, commit.commit_id, commit.events.len()));
            if number == 2 || number == 4 {
                crate::manifest::fold_wal(&store, &namespace_id)
                    .await
                    .expect("fold commits");
            }
        }

        for fully_folded in [false, true] {
            if fully_folded {
                crate::manifest::fold_wal(&store, &namespace_id)
                    .await
                    .expect("fold tail");
            }
            let view = load_current_metadata_view(&store, &namespace_id)
                .await
                .expect("read view");
            for commit_limit in [1000, 1] {
                let limit = EffectiveLimit::new(NonZeroU32::new(commit_limit).expect("limit"));
                let mut after_seq = ChangeSeq(0);
                let mut actual = Vec::new();
                loop {
                    let page = super::list_changes_after(&view, after_seq, limit)
                        .await
                        .expect("page");
                    assert!(!page.changes.is_empty());
                    assert!(page.changes.len() <= limit.as_usize());
                    let bytes: usize = page
                        .changes
                        .iter()
                        .map(|commit| commit.events.heap_bytes())
                        .sum();
                    if bytes > CHANGE_FEED_PAGE_BYTES {
                        assert_eq!(page.changes.len(), 1);
                        assert_eq!(page.changes[0].committed_seq, ChangeSeq(4));
                    }
                    if after_seq == ChangeSeq(0) && commit_limit == 1000 {
                        assert!(page.changes.len() < limit.as_usize());
                        assert!(bytes <= CHANGE_FEED_PAGE_BYTES);
                        assert!(page.next_after_seq.is_some());
                    }
                    let last = page.changes.last().expect("nonempty page").committed_seq;
                    assert_eq!(page.through_seq, last);
                    actual.extend(page.changes.into_iter().map(|commit| {
                        (commit.committed_seq, commit.commit_id, commit.events.len())
                    }));
                    let Some(next) = page.next_after_seq else {
                        break;
                    };
                    assert_eq!(next, last);
                    assert!(next > after_seq);
                    after_seq = next;
                    assert!(actual.len() < expected.len());
                }
                assert_eq!(actual, expected);
            }
        }
        let manifest = crate::namespace::control::load_current_manifest(&store, &namespace_id)
            .await
            .expect("manifest");
        let basis = crate::manifest::metadata_basis_from_manifest(&store, None, &manifest);
        let rows = basis
            .segments
            .scan_range_page(
                loonfs_types::format::manifest::MetadataRowFamily::Commits,
                &loonfs_types::format::manifest::lookup_keys::commit_row_key(ChangeSeq(2)),
                None,
                2,
            )
            .await
            .expect("commit row page");
        assert_eq!(rows.len(), 2);
        assert_eq!(basis.segments.peak_page_rows(), 2);
        assert_eq!(
            rows.iter()
                .map(|row| {
                    row.row_key_for_family(
                        loonfs_types::format::manifest::MetadataRowFamily::Commits,
                    )
                })
                .collect::<Vec<_>>(),
            [2, 3].map(|seq| {
                loonfs_types::format::manifest::lookup_keys::commit_row_key(ChangeSeq(seq))
            })
        );
    }

    #[test]
    fn one_attribute_delta_maps_to_one_event_carrying_the_whole_map() {
        let delta = append_attributes(0);

        assert_eq!(
            event_from_op_deltas(&namespace_id(), ChangeSeq(7), &[&delta])
                .expect("map the operation"),
            FilesystemChange::AttributesChanged {
                inode_id: InodeId(7),
                attributes_revision_no: AttributesRevisionNo(3),
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
