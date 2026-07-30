//! Behavior tests for metadata materialization: WAL-delta application,
//! index maintenance, and seq-gated queries.

use super::*;
use loonfs_api::wire::wal::WalDelta;
use loonfs_api::ContentId;
use loonfs_api::{
    AbsolutePath, ChangeSeq, CommitId, ContentRef, InodeId, InodeKind, NameKey, RevisionNo,
};

fn name_key(value: &str) -> NameKey {
    NameKey::parse(value).expect("valid name key")
}

#[test]
fn bind_direntry_replay_uses_persisted_name_key() {
    let applied = MetadataState::default().apply_committed_wal_deltas(
        ChangeSeq(1),
        4_200,
        &[WalDelta::BindDirentry {
            delta_index: 7,
            parent_inode_id: InodeId(1),
            name_key: NameKey::parse("persisted-key").expect("valid name key"),
            display_name: loonfs_api::DisplayName::parse("Report.TXT").expect("valid display name"),
            child_inode_id: InodeId(2),
        }],
    );

    assert_eq!(applied.direntry_binds().len(), 1);
    let bind = &applied.direntry_binds()[0];
    assert_eq!(bind.name_key.as_str(), "persisted-key");
    assert_eq!(bind.display_name.as_str(), "Report.TXT");
    assert_eq!(bind.bind_delta_index, 7);
}

#[test]
fn child_lookup_uses_persisted_name_key_without_recanonicalizing() {
    let metadata_state = MetadataState::from_rows(
        vec![
            InodeRecord {
                inode_id: InodeId(1),
                inode_kind: InodeKind::Directory,
                created_seq: ChangeSeq(1),
            },
            InodeRecord {
                inode_id: InodeId(2),
                inode_kind: InodeKind::File,
                created_seq: ChangeSeq(1),
            },
        ],
        vec![DirentryBindRecord {
            parent_inode_id: InodeId(1),
            name_key: NameKey::parse("persisted-key").expect("valid name key"),
            display_name: loonfs_api::DisplayName::parse("Report.TXT").expect("valid display name"),
            child_inode_id: InodeId(2),
            bind_seq: ChangeSeq(1),
            bind_delta_index: 0,
        }],
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );

    assert!(metadata_state
        .visible_child(InodeId(1), &name_key("persisted-key"), ChangeSeq(1))
        .is_some());
    assert!(metadata_state
        .visible_child(InodeId(1), &name_key("Report.TXT"), ChangeSeq(1))
        .is_none());
}

#[test]
fn maintained_indexes_track_bind_unbind_rename_and_tombstone() {
    let metadata_state = MetadataState::from_rows(
        vec![
            InodeRecord {
                inode_id: InodeId(1),
                inode_kind: InodeKind::Directory,
                created_seq: ChangeSeq(0),
            },
            InodeRecord {
                inode_id: InodeId(2),
                inode_kind: InodeKind::Directory,
                created_seq: ChangeSeq(1),
            },
            InodeRecord {
                inode_id: InodeId(3),
                inode_kind: InodeKind::File,
                created_seq: ChangeSeq(2),
            },
        ],
        vec![
            DirentryBindRecord {
                parent_inode_id: InodeId(1),
                name_key: NameKey::parse("docs").expect("valid name key"),
                display_name: loonfs_api::DisplayName::parse("docs").expect("valid display name"),
                child_inode_id: InodeId(2),
                bind_seq: ChangeSeq(1),
                bind_delta_index: 0,
            },
            DirentryBindRecord {
                parent_inode_id: InodeId(2),
                name_key: NameKey::parse("report.txt").expect("valid name key"),
                display_name: loonfs_api::DisplayName::parse("report.txt")
                    .expect("valid display name"),
                child_inode_id: InodeId(3),
                bind_seq: ChangeSeq(2),
                bind_delta_index: 0,
            },
        ],
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );

    assert_eq!(metadata_state.indexed_seq(), ChangeSeq(2));
    assert!(metadata_state.inode_at_head(InodeId(2)).is_some());
    assert_eq!(
        metadata_state
            .visible_child_at_head(InodeId(1), &name_key("docs"))
            .expect("docs visible")
            .child_inode_id,
        InodeId(2)
    );
    assert_eq!(
        metadata_state
            .current_parent_binding_for_child_at_head(InodeId(2))
            .expect("parent binding")
            .parent_inode_id,
        InodeId(1)
    );

    let metadata_state = metadata_state.apply_committed_wal_deltas(
        ChangeSeq(3),
        4_200,
        &[WalDelta::UnbindDirentry {
            delta_index: 0,
            parent_inode_id: InodeId(1),
            name_key: NameKey::parse("docs").expect("valid name key"),
            display_name: loonfs_api::DisplayName::parse("docs").expect("valid display name"),
            child_inode_id: InodeId(2),
            bind_seq: ChangeSeq(1),
            bind_delta_index: 0,
        }],
    );
    assert!(metadata_state
        .visible_child_at_head(InodeId(1), &name_key("docs"))
        .is_none());
    assert!(metadata_state
        .current_parent_binding_for_child_at_head(InodeId(2))
        .is_none());

    let metadata_state = metadata_state.apply_committed_wal_deltas(
        ChangeSeq(4),
        4_200,
        &[WalDelta::BindDirentry {
            delta_index: 0,
            parent_inode_id: InodeId(1),
            name_key: NameKey::parse("renamed").expect("valid name key"),
            display_name: loonfs_api::DisplayName::parse("renamed").expect("valid display name"),
            child_inode_id: InodeId(2),
        }],
    );
    assert!(metadata_state
        .visible_child_at_head(InodeId(1), &name_key("docs"))
        .is_none());
    assert_eq!(
        metadata_state
            .visible_child_at_head(InodeId(1), &name_key("renamed"))
            .expect("renamed visible")
            .child_inode_id,
        InodeId(2)
    );

    let metadata_state = metadata_state.apply_committed_wal_deltas(
        ChangeSeq(5),
        4_200,
        &[WalDelta::TombstoneSubtree {
            delta_index: 0,
            root_inode_id: InodeId(2),
            parent_inode_id: None,
            name_key: None,
            display_name: None,
        }],
    );
    assert!(metadata_state
        .visible_child_at_head(InodeId(1), &name_key("renamed"))
        .is_none());
    assert_eq!(
        metadata_state
            .covering_subtree_tombstone_at_head(InodeId(3))
            .expect("descendant tombstone")
            .root_inode_id,
        InodeId(2)
    );
}

#[test]
fn rebuilt_indexes_answer_current_head_queries_after_deserialize() {
    let metadata_state = MetadataState::from_rows(
        vec![
            InodeRecord {
                inode_id: InodeId(1),
                inode_kind: InodeKind::Directory,
                created_seq: ChangeSeq(0),
            },
            InodeRecord {
                inode_id: InodeId(2),
                inode_kind: InodeKind::File,
                created_seq: ChangeSeq(1),
            },
        ],
        vec![DirentryBindRecord {
            parent_inode_id: InodeId(1),
            name_key: NameKey::parse("file.txt").expect("valid name key"),
            display_name: loonfs_api::DisplayName::parse("file.txt").expect("valid display name"),
            child_inode_id: InodeId(2),
            bind_seq: ChangeSeq(1),
            bind_delta_index: 0,
        }],
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );

    let encoded = serde_json::to_string(&metadata_state).expect("encode metadata");
    assert!(!encoded.contains("indexes"));
    assert!(!encoded.contains("row_count"));
    assert!(!encoded.contains("decoded_bytes"));
    let decoded: MetadataState = serde_json::from_str(&encoded).expect("decode metadata");

    assert_eq!(decoded.row_count(), metadata_state.row_count());
    assert_eq!(decoded.decoded_bytes(), metadata_state.decoded_bytes());
    assert_eq!(decoded.indexed_seq(), ChangeSeq(1));
    assert_eq!(
        decoded
            .visible_child_at_head(InodeId(1), &name_key("file.txt"))
            .expect("indexed child")
            .child_inode_id,
        InodeId(2)
    );
}

#[test]
fn stale_binding_is_not_active_after_newer_bind_claims_same_name() {
    let metadata_state = MetadataState::from_rows(
        vec![
            InodeRecord {
                inode_id: InodeId(1),
                inode_kind: InodeKind::Directory,
                created_seq: ChangeSeq(0),
            },
            InodeRecord {
                inode_id: InodeId(2),
                inode_kind: InodeKind::File,
                created_seq: ChangeSeq(1),
            },
            InodeRecord {
                inode_id: InodeId(3),
                inode_kind: InodeKind::File,
                created_seq: ChangeSeq(2),
            },
        ],
        vec![
            DirentryBindRecord {
                parent_inode_id: InodeId(1),
                name_key: NameKey::parse("report").expect("valid name key"),
                display_name: loonfs_api::DisplayName::parse("report").expect("valid display name"),
                child_inode_id: InodeId(2),
                bind_seq: ChangeSeq(1),
                bind_delta_index: 0,
            },
            DirentryBindRecord {
                parent_inode_id: InodeId(1),
                name_key: NameKey::parse("report").expect("valid name key"),
                display_name: loonfs_api::DisplayName::parse("report").expect("valid display name"),
                child_inode_id: InodeId(3),
                bind_seq: ChangeSeq(2),
                bind_delta_index: 0,
            },
        ],
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );

    assert_eq!(
        metadata_state
            .visible_child_at_head(InodeId(1), &name_key("report"))
            .expect("latest child")
            .child_inode_id,
        InodeId(3)
    );
    assert!(metadata_state
        .current_parent_binding_for_child_at_head(InodeId(2))
        .is_none());
}

#[test]
fn resolve_visible_path_folds_names_and_uses_stored_display_name() {
    let metadata_state = MetadataState::from_rows(
        vec![
            InodeRecord {
                inode_id: InodeId(1),
                inode_kind: InodeKind::Directory,
                created_seq: ChangeSeq(1),
            },
            InodeRecord {
                inode_id: InodeId(2),
                inode_kind: InodeKind::File,
                created_seq: ChangeSeq(1),
            },
        ],
        vec![DirentryBindRecord {
            parent_inode_id: InodeId(1),
            name_key: NameKey::parse("report.txt").expect("valid name key"),
            display_name: loonfs_api::DisplayName::parse("Report.TXT").expect("valid display name"),
            child_inode_id: InodeId(2),
            bind_seq: ChangeSeq(1),
            bind_delta_index: 0,
        }],
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );

    let resolved = metadata_state
        .resolve_visible_path(
            &AbsolutePath::parse("/REPORT.txt").expect("path"),
            ChangeSeq(1),
        )
        .expect("resolve path");

    assert_eq!(resolved.inode_id, InodeId(2));
    assert_eq!(resolved.absolute_path, "/Report.TXT");
    assert_eq!(resolved.display_name.as_str(), "Report.TXT");
}

#[test]
fn metadata_state_serialized_shape_preserves_row_field_names() {
    let metadata_state = MetadataState::from_rows(
        vec![InodeRecord {
            inode_id: InodeId(1),
            inode_kind: InodeKind::Directory,
            created_seq: ChangeSeq(0),
        }],
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );

    let encoded = serde_json::to_value(&metadata_state).expect("encode metadata state");
    assert_eq!(
        encoded,
        serde_json::json!({
            "inodes": [{
                "inode_id": 1,
                "inode_kind": "dir",
                "created_seq": 0
            }],
            "direntry_binds": [],
            "direntry_unbinds": [],
            "revisions": [],
            "subtree_tombstones": [],
            "commit_receipts": []
        })
    );
}

#[test]
fn metadata_state_accessors_expose_rows_read_only() {
    let metadata_state = MetadataState::from_rows(
        vec![InodeRecord {
            inode_id: InodeId(1),
            inode_kind: InodeKind::Directory,
            created_seq: ChangeSeq(0),
        }],
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );

    assert_eq!(metadata_state.inodes().len(), 1);
    assert!(metadata_state.direntry_binds().is_empty());
    assert!(metadata_state.direntry_unbinds().is_empty());
    assert!(metadata_state.revisions().is_empty());
    assert!(metadata_state.subtree_tombstones().is_empty());
    assert!(metadata_state.commit_receipts().is_empty());
}

#[test]
fn find_commit_receipt_returns_latest_matching_receipt() {
    let commit_id = CommitId::parse("same-commit").expect("valid commit id");
    let metadata_state = MetadataState::from_rows(
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        vec![
            CommitReceiptRecord {
                commit_id: commit_id.clone(),
                semantic_commit_fingerprint: "old".to_owned(),
                committed_seq: ChangeSeq(1),
                committed_at_ms: 4_200,
                message: Some("old message".to_owned()),
            },
            CommitReceiptRecord {
                commit_id: CommitId::parse("other-commit").expect("valid commit id"),
                semantic_commit_fingerprint: "other".to_owned(),
                committed_seq: ChangeSeq(3),
                committed_at_ms: 4_200,
                message: None,
            },
            CommitReceiptRecord {
                commit_id: commit_id.clone(),
                semantic_commit_fingerprint: "new".to_owned(),
                committed_seq: ChangeSeq(2),
                committed_at_ms: 4_200,
                message: Some("new message".to_owned()),
            },
        ],
    );

    let receipt = metadata_state
        .find_commit_receipt(&commit_id)
        .expect("receipt");
    assert_eq!(receipt.committed_seq, ChangeSeq(2));
    assert_eq!(receipt.semantic_commit_fingerprint, "new");
}

/// Revision rows keep no in-memory index — reads scan the tail-sized rows —
/// but they must still advance the indexed-seq watermark that gates at-head
/// query routing, and the receipt index must survive a serde round trip.
#[test]
fn revisions_advance_watermark_and_receipt_index_round_trips() {
    let commit_id = CommitId::parse("indexed-commit").expect("valid commit id");
    let content_ref = ContentRef::blob_v1(ContentId::generate(), b"first revision bytes");
    let replacement_ref = ContentRef::blob_v1(ContentId::generate(), b"second revision bytes");

    let mut builder = MetadataStateBuilder::default();
    builder.push_inode(InodeRecord {
        inode_id: InodeId(7),
        inode_kind: InodeKind::File,
        created_seq: ChangeSeq(1),
    });
    builder.push_revision(RevisionRecord {
        inode_id: InodeId(7),
        revision_no: RevisionNo(1),
        committed_seq: ChangeSeq(2),
        committed_at_ms: 4_200,
        revision_delta_index: 0,
        content_ref: content_ref.clone(),
    });
    builder.push_revision(RevisionRecord {
        inode_id: InodeId(7),
        revision_no: RevisionNo(2),
        committed_seq: ChangeSeq(3),
        committed_at_ms: 4_200,
        revision_delta_index: 0,
        content_ref: replacement_ref.clone(),
    });
    builder.push_commit_receipt(CommitReceiptRecord {
        commit_id: commit_id.clone(),
        semantic_commit_fingerprint: "fingerprint".to_owned(),
        committed_seq: ChangeSeq(3),
        committed_at_ms: 4_200,
        message: Some("replace indexed file".to_owned()),
    });
    let metadata_state = builder.finish();

    assert_eq!(metadata_state.row_count(), 4);
    assert!(metadata_state.decoded_bytes() >= metadata_state.row_count());
    assert_eq!(metadata_state.indexed_seq(), ChangeSeq(3));
    assert_eq!(
        metadata_state
            .revisions()
            .last()
            .expect("revision rows")
            .content_ref,
        replacement_ref
    );
    assert_eq!(
        metadata_state
            .find_commit_receipt(&commit_id)
            .expect("commit receipt")
            .committed_seq,
        ChangeSeq(3)
    );

    let decoded: MetadataState =
        serde_json::from_value(serde_json::to_value(&metadata_state).expect("encode"))
            .expect("decode");
    assert_eq!(decoded.indexed_seq(), ChangeSeq(3));
    assert_eq!(
        decoded
            .find_commit_receipt(&commit_id)
            .expect("receipt after decode")
            .semantic_commit_fingerprint,
        "fingerprint"
    );
}

/// Binding churn under one parent:
/// - seq 1: child 2 bound at `contested`, child 4 bound at `deleted`
/// - seq 2: child 2 renamed to `renamed-away`, child 4 unbound for good
/// - seq 3: child 3 takes over `contested`
///
/// At head this leaves `contested` rebound, `renamed-away` active, and
/// `deleted` with only a dead binding.
fn churned_binding_state() -> MetadataState {
    let mut state = MetadataState::default();
    state.apply_committed_wal_deltas_mut(
        ChangeSeq(0),
        4_200,
        &[WalDelta::CreateInode {
            delta_index: 0,
            inode_id: InodeId(1),
            inode_kind: InodeKind::Directory,
        }],
    );
    state.apply_committed_wal_deltas_mut(
        ChangeSeq(1),
        4_200,
        &[
            WalDelta::CreateInode {
                delta_index: 0,
                inode_id: InodeId(2),
                inode_kind: InodeKind::Directory,
            },
            WalDelta::BindDirentry {
                delta_index: 1,
                parent_inode_id: InodeId(1),
                name_key: NameKey::parse("contested").expect("valid name key"),
                display_name: loonfs_api::DisplayName::parse("contested")
                    .expect("valid display name"),
                child_inode_id: InodeId(2),
            },
            WalDelta::CreateInode {
                delta_index: 2,
                inode_id: InodeId(4),
                inode_kind: InodeKind::Directory,
            },
            WalDelta::BindDirentry {
                delta_index: 3,
                parent_inode_id: InodeId(1),
                name_key: NameKey::parse("deleted").expect("valid name key"),
                display_name: loonfs_api::DisplayName::parse("deleted")
                    .expect("valid display name"),
                child_inode_id: InodeId(4),
            },
        ],
    );
    state.apply_committed_wal_deltas_mut(
        ChangeSeq(2),
        4_200,
        &[
            WalDelta::UnbindDirentry {
                delta_index: 0,
                parent_inode_id: InodeId(1),
                name_key: NameKey::parse("contested").expect("valid name key"),
                display_name: loonfs_api::DisplayName::parse("contested")
                    .expect("valid display name"),
                child_inode_id: InodeId(2),
                bind_seq: ChangeSeq(1),
                bind_delta_index: 1,
            },
            WalDelta::BindDirentry {
                delta_index: 1,
                parent_inode_id: InodeId(1),
                name_key: NameKey::parse("renamed-away").expect("valid name key"),
                display_name: loonfs_api::DisplayName::parse("renamed-away")
                    .expect("valid display name"),
                child_inode_id: InodeId(2),
            },
            WalDelta::UnbindDirentry {
                delta_index: 2,
                parent_inode_id: InodeId(1),
                name_key: NameKey::parse("deleted").expect("valid name key"),
                display_name: loonfs_api::DisplayName::parse("deleted")
                    .expect("valid display name"),
                child_inode_id: InodeId(4),
                bind_seq: ChangeSeq(1),
                bind_delta_index: 3,
            },
        ],
    );
    state.apply_committed_wal_deltas_mut(
        ChangeSeq(3),
        4_200,
        &[
            WalDelta::CreateInode {
                delta_index: 0,
                inode_id: InodeId(3),
                inode_kind: InodeKind::Directory,
            },
            WalDelta::BindDirentry {
                delta_index: 1,
                parent_inode_id: InodeId(1),
                name_key: NameKey::parse("contested").expect("valid name key"),
                display_name: loonfs_api::DisplayName::parse("contested")
                    .expect("valid display name"),
                child_inode_id: InodeId(3),
            },
        ],
    );
    state
}

/// Rebuilds the churned state from its rows, so the `from_rows` index
/// construction path is pinned against the incremental one.
fn churned_binding_state_rebuilt() -> MetadataState {
    let incremental = churned_binding_state();
    MetadataState::from_rows(
        incremental.inodes().to_vec(),
        incremental.direntry_binds().to_vec(),
        incremental.direntry_unbinds().to_vec(),
        incremental.revisions().to_vec(),
        incremental.subtree_tombstones().to_vec(),
        incremental.commit_receipts().to_vec(),
    )
}

#[test]
fn bound_child_at_head_sees_latest_bind_including_dead_bindings() {
    let state = churned_binding_state();
    let head = state.indexed_seq();
    assert_eq!(head, ChangeSeq(3));

    // The latest bind at the contested name is child 3.
    let head_bind = state
        .bound_child_at_seq(InodeId(1), &name_key("contested"), head)
        .expect("bind at head");
    assert_eq!(head_bind.child_inode_id, InodeId(3));
    assert_eq!(head_bind.bind_seq, ChangeSeq(3));

    // The deleted name still answers with its dead binding: the bind is
    // unbound but tombstone-ancestry walks must see it.
    let dead_bind = state
        .bound_child_at_seq(InodeId(1), &name_key("deleted"), head)
        .expect("dead binding visible at head");
    assert_eq!(dead_bind.child_inode_id, InodeId(4));
    assert!(state.is_direntry_unbound_at_seq(&dead_bind, head));
    assert!(state
        .visible_child(InodeId(1), &name_key("deleted"), head)
        .is_none());
}

#[test]
fn bound_child_below_indexed_seq_still_scans_history() {
    let state = churned_binding_state();

    // At seq 2 the contested name's latest bind is still child 2's
    // (unbound) binding; the rebind at seq 3 is not visible yet.
    let historical = state
        .bound_child_at_seq(InodeId(1), &name_key("contested"), ChangeSeq(2))
        .expect("historical bind");
    assert_eq!(historical.child_inode_id, InodeId(2));
    assert_eq!(historical.bind_seq, ChangeSeq(1));
}

#[test]
fn incremental_and_rebuilt_indexes_agree_on_latest_binds() {
    let incremental = churned_binding_state();
    let rebuilt = churned_binding_state_rebuilt();

    for name_key_value in ["contested", "renamed-away", "deleted", "never-bound"] {
        assert_eq!(
            rebuilt.bound_child_at_seq(InodeId(1), &name_key(name_key_value), ChangeSeq(3)),
            incremental.bound_child_at_seq(InodeId(1), &name_key(name_key_value), ChangeSeq(3),),
            "latest bind for `{name_key_value}` diverges between construction paths"
        );
    }
}

/// Queries above `indexed_seq()` are at-head queries: commit validation
/// probes the materialization at the next assigned seq and must hit the indexes.
#[test]
fn queries_above_indexed_seq_match_at_head_results() {
    let state = churned_binding_state();
    let beyond_head = ChangeSeq(state.indexed_seq().0 + 1);

    assert_eq!(
        state.bound_child_at_seq(InodeId(1), &name_key("contested"), beyond_head),
        state
            .indexes
            .latest_bind(InodeId(1), &name_key("contested")),
    );
    assert_eq!(
        state.visible_child(InodeId(1), &name_key("contested"), beyond_head),
        state.visible_child_at_head(InodeId(1), &name_key("contested")),
    );
    assert_eq!(
        state.current_parent_binding_for_child(InodeId(2), beyond_head),
        state.current_parent_binding_for_child_at_head(InodeId(2)),
    );
    assert_eq!(
        state.visible_inode(InodeId(3), beyond_head),
        state.visible_inode_at_head(InodeId(3)),
    );
}

/// The directory-empty checks probe existence through the bounded
/// [`MetadataView::has_visible_children`]; a directory whose every child
/// binding was unbound must read as empty again.
#[test]
fn has_visible_children_sees_through_unbinds() {
    let dir = InodeId(1);
    let state = MetadataState::from_rows(
        vec![
            InodeRecord {
                inode_id: dir,
                inode_kind: InodeKind::Directory,
                created_seq: ChangeSeq(1),
            },
            InodeRecord {
                inode_id: InodeId(2),
                inode_kind: InodeKind::File,
                created_seq: ChangeSeq(2),
            },
        ],
        vec![DirentryBindRecord {
            parent_inode_id: dir,
            name_key: NameKey::parse("doc.txt").expect("valid name key"),
            display_name: loonfs_api::DisplayName::parse("doc.txt").expect("valid display name"),
            child_inode_id: InodeId(2),
            bind_seq: ChangeSeq(2),
            bind_delta_index: 0,
        }],
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );
    let view = InMemoryMetadataView::in_memory(&state, None, ChangeSeq(2));
    assert!(
        visibility::resolve_in_memory_read(view.has_visible_children(dir))
            .expect("probe bound child")
    );
    // The file inode is not a directory: probing it reports no children.
    assert!(
        !visibility::resolve_in_memory_read(view.has_visible_children(InodeId(2)))
            .expect("probe non-directory")
    );

    let emptied = MetadataState::from_rows(
        state.inodes().to_vec(),
        state.direntry_binds().to_vec(),
        vec![DirentryUnbindRecord {
            parent_inode_id: dir,
            name_key: NameKey::parse("doc.txt").expect("valid name key"),
            display_name: loonfs_api::DisplayName::parse("doc.txt").expect("valid display name"),
            child_inode_id: InodeId(2),
            bind_seq: ChangeSeq(2),
            bind_delta_index: 0,
            unbind_seq: ChangeSeq(3),
            unbind_delta_index: 0,
        }],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );
    let view = InMemoryMetadataView::in_memory(&emptied, None, ChangeSeq(3));
    assert!(
        !visibility::resolve_in_memory_read(view.has_visible_children(dir))
            .expect("probe emptied directory")
    );
}
