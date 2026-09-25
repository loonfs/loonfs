//! Tests for commit validation overlay rows.

use super::*;
use crate::metadata::{InMemoryMetadataView, MetadataState};
use loonfs_api::wire::manifest::{DeletedBinding, DeltaPosition};
use loonfs_api::wire::wal::WalDelta;
use loonfs_api::ContentId;
use loonfs_api::{
    AttributeKey, AttributeValue, Attributes, ContentRef, InodeId, InodeKind, NameKey, RevisionNo,
};
use loonfs_api::{ChangeSeq, CommitId};

fn commit_id(committed_seq: ChangeSeq) -> CommitId {
    CommitId::parse(format!("c_overlay_{}", committed_seq.0)).expect("commit id")
}

fn assert_overlay_matches_replay(committed_seq: ChangeSeq, deltas: &[WalDelta]) {
    let overlay = overlay_rows(committed_seq, deltas);
    let mut replayed = MetadataState::default();
    replayed.apply_committed_wal_deltas_mut(
        committed_seq,
        &commit_id(committed_seq),
        &loonfs_api::ActorId::loonfs(),
        4_200,
        deltas,
    );

    assert!(
        replayed.row_count() > 0,
        "scenario must exercise at least one row"
    );
    assert_row_categories_equal(&overlay, &replayed);
}

fn overlay_rows(committed_seq: ChangeSeq, deltas: &[WalDelta]) -> MetadataState {
    let base = MetadataState::default();
    let accepted = MetadataState::default();
    let mut view = PublishValidationView::new(
        InMemoryMetadataView::in_memory(&base, None, ChangeSeq(0)),
        &accepted,
        committed_seq,
    );
    view.apply_deltas_mut(
        &commit_id(committed_seq),
        &loonfs_api::ActorId::loonfs(),
        4_200,
        deltas,
    );
    view.overlay
}

fn assert_row_categories_equal(overlay: &MetadataState, replayed: &MetadataState) {
    assert_eq!(overlay.inodes(), replayed.inodes(), "inode rows diverged");
    assert_eq!(
        overlay.direntry_binds(),
        replayed.direntry_binds(),
        "direntry bind rows diverged"
    );

    assert_eq!(
        overlay.revisions(),
        replayed.revisions(),
        "revision rows diverged"
    );
    assert_eq!(
        overlay.subtree_tombstones(),
        replayed.subtree_tombstones(),
        "subtree tombstone rows diverged"
    );
    assert_eq!(
        overlay.commit_receipts(),
        replayed.commit_receipts(),
        "commit receipt rows diverged"
    );
    assert_eq!(
        overlay.attributes_revisions(),
        replayed.attributes_revisions(),
        "attribute revision rows diverged"
    );
}

fn content_ref(seed: u8) -> ContentRef {
    ContentRef::blob_v1(
        loonfs_api::NamespaceId::parse("demo").expect("namespace id"),
        ContentId::generate(),
        &[seed; 12],
    )
}

fn attributes(entries: impl IntoIterator<Item = (&'static str, &'static str)>) -> Attributes {
    Attributes::new(
        entries
            .into_iter()
            .map(|(key, value)| {
                (
                    AttributeKey::parse(key).expect("valid attribute key"),
                    AttributeValue::parse(value).expect("valid attribute value"),
                )
            })
            .collect(),
    )
    .expect("valid attribute map")
}

#[test]
fn create_dir_overlay_rows_match_replayed_wal_deltas() {
    assert_overlay_matches_replay(
        ChangeSeq(7),
        &[
            WalDelta::CreateInode {
                delta_index: 0,
                inode_id: InodeId(2),
                inode_kind: InodeKind::Directory,
            },
            WalDelta::BindDirentry {
                delta_index: 1,
                parent_inode_id: InodeId(1),
                name_key: NameKey::parse("docs").expect("valid name key"),
                display_name: loonfs_api::DisplayName::parse("Docs").expect("valid display name"),
                child_inode_id: InodeId(2),
            },
        ],
    );
}

#[test]
fn create_file_overlay_rows_match_replayed_wal_deltas() {
    assert_overlay_matches_replay(
        ChangeSeq(3),
        &[
            WalDelta::CreateInode {
                delta_index: 0,
                inode_id: InodeId(2),
                inode_kind: InodeKind::File,
            },
            WalDelta::BindDirentry {
                delta_index: 1,
                parent_inode_id: InodeId(1),
                name_key: NameKey::parse("note.txt").expect("valid name key"),
                display_name: loonfs_api::DisplayName::parse("Note.TXT")
                    .expect("valid display name"),
                child_inode_id: InodeId(2),
            },
            WalDelta::AppendFileRevision {
                delta_index: 2,
                inode_id: InodeId(2),
                revision_no: RevisionNo(1),
                content_ref: content_ref(1),
            },
        ],
    );
}

#[test]
fn replace_file_overlay_rows_match_replayed_wal_deltas() {
    assert_overlay_matches_replay(
        ChangeSeq(9),
        &[WalDelta::AppendFileRevision {
            delta_index: 0,
            inode_id: InodeId(4),
            revision_no: RevisionNo(5),
            content_ref: content_ref(2),
        }],
    );
}

#[test]
fn restore_revision_overlay_rows_match_replayed_wal_deltas() {
    assert_overlay_matches_replay(
        ChangeSeq(12),
        &[WalDelta::AppendFileRevision {
            delta_index: 0,
            inode_id: InodeId(4),
            revision_no: RevisionNo(6),
            content_ref: content_ref(3),
        }],
    );
}

#[test]
fn delete_file_overlay_rows_match_replayed_wal_deltas() {
    assert_overlay_matches_replay(
        ChangeSeq(15),
        &[
            WalDelta::UnbindDirentry {
                delta_index: 0,
                parent_inode_id: InodeId(2),
                name_key: NameKey::parse("note.txt").expect("name key"),
                display_name: loonfs_api::DisplayName::parse("Note.TXT").expect("display name"),
                child_inode_id: InodeId(4),
                target: DeltaPosition {
                    seq: ChangeSeq(8),
                    delta_index: 3,
                },
            },
            WalDelta::TombstoneSubtree {
                delta_index: 1,
                root_inode_id: InodeId(4),
                deleted_binding: DeletedBinding {
                    parent_inode_id: InodeId(2),
                    name_key: NameKey::parse("note.txt").expect("name key"),
                    display_name: loonfs_api::DisplayName::parse("Note.TXT").expect("display name"),
                },
            },
        ],
    );
}

#[test]
fn rename_of_preexisting_binding_overlay_rows_match_replayed_wal_deltas() {
    assert_overlay_matches_replay(
        ChangeSeq(20),
        &[
            WalDelta::UnbindDirentry {
                delta_index: 0,
                parent_inode_id: InodeId(2),
                name_key: NameKey::parse("old.txt").expect("name key"),
                display_name: loonfs_api::DisplayName::parse("Old.TXT").expect("display name"),
                child_inode_id: InodeId(4),
                target: DeltaPosition {
                    seq: ChangeSeq(11),
                    delta_index: 2,
                },
            },
            WalDelta::BindDirentry {
                delta_index: 1,
                parent_inode_id: InodeId(3),
                name_key: NameKey::parse("new.txt").expect("valid name key"),
                display_name: loonfs_api::DisplayName::parse("New.TXT")
                    .expect("valid display name"),
                child_inode_id: InodeId(4),
            },
        ],
    );
}

#[test]
fn rename_of_same_commit_binding_overlay_rows_match_replayed_wal_deltas() {
    let committed_seq = ChangeSeq(21);
    assert_overlay_matches_replay(
        committed_seq,
        &[
            WalDelta::CreateInode {
                delta_index: 0,
                inode_id: InodeId(2),
                inode_kind: InodeKind::File,
            },
            WalDelta::BindDirentry {
                delta_index: 1,
                parent_inode_id: InodeId(1),
                name_key: NameKey::parse("draft.md").expect("valid name key"),
                display_name: loonfs_api::DisplayName::parse("Draft.md")
                    .expect("valid display name"),
                child_inode_id: InodeId(2),
            },
            WalDelta::AppendFileRevision {
                delta_index: 2,
                inode_id: InodeId(2),
                revision_no: RevisionNo(1),
                content_ref: content_ref(4),
            },
            WalDelta::UnbindDirentry {
                delta_index: 3,
                parent_inode_id: InodeId(1),
                name_key: NameKey::parse("draft.md").expect("name key"),
                display_name: loonfs_api::DisplayName::parse("Draft.md").expect("display name"),
                child_inode_id: InodeId(2),
                target: DeltaPosition {
                    seq: committed_seq,
                    delta_index: 1,
                },
            },
            WalDelta::BindDirentry {
                delta_index: 4,
                parent_inode_id: InodeId(1),
                name_key: NameKey::parse("final.md").expect("valid name key"),
                display_name: loonfs_api::DisplayName::parse("Final.md")
                    .expect("valid display name"),
                child_inode_id: InodeId(2),
            },
        ],
    );
}

#[test]
fn update_attributes_overlay_rows_match_replayed_wal_deltas() {
    assert_overlay_matches_replay(
        ChangeSeq(25),
        &[WalDelta::AppendAttributesRevision {
            delta_index: 0,
            inode_id: InodeId(4),
            attributes_revision_no: loonfs_api::AttributesRevisionNo(1),
            attributes: attributes([("owner", "ada")]),
        }],
    );
}

#[test]
fn cleared_attributes_overlay_rows_match_replayed_wal_deltas() {
    assert_overlay_matches_replay(
        ChangeSeq(26),
        &[
            WalDelta::AppendAttributesRevision {
                delta_index: 0,
                inode_id: InodeId(4),
                attributes_revision_no: loonfs_api::AttributesRevisionNo(1),
                attributes: attributes([("owner", "ada")]),
            },
            WalDelta::AppendAttributesRevision {
                delta_index: 1,
                inode_id: InodeId(4),
                attributes_revision_no: loonfs_api::AttributesRevisionNo(2),
                attributes: loonfs_api::Attributes::default(),
            },
        ],
    );
}

#[test]
fn delete_subtree_overlay_rows_match_replayed_wal_deltas() {
    assert_overlay_matches_replay(
        ChangeSeq(30),
        &[
            WalDelta::UnbindDirentry {
                delta_index: 0,
                parent_inode_id: InodeId(1),
                name_key: NameKey::parse("attic").expect("name key"),
                display_name: loonfs_api::DisplayName::parse("Attic").expect("display name"),
                child_inode_id: InodeId(5),
                target: DeltaPosition {
                    seq: ChangeSeq(22),
                    delta_index: 6,
                },
            },
            WalDelta::TombstoneSubtree {
                delta_index: 1,
                root_inode_id: InodeId(5),
                deleted_binding: DeletedBinding {
                    parent_inode_id: InodeId(1),
                    name_key: NameKey::parse("attic").expect("name key"),
                    display_name: loonfs_api::DisplayName::parse("Attic").expect("display name"),
                },
            },
        ],
    );
}

#[test]
fn chained_multi_op_commit_overlay_rows_match_replayed_wal_deltas() {
    let committed_seq = ChangeSeq(40);
    assert_overlay_matches_replay(
        committed_seq,
        &[
            WalDelta::CreateInode {
                delta_index: 0,
                inode_id: InodeId(2),
                inode_kind: InodeKind::Directory,
            },
            WalDelta::BindDirentry {
                delta_index: 1,
                parent_inode_id: InodeId(1),
                name_key: NameKey::parse("docs").expect("valid name key"),
                display_name: loonfs_api::DisplayName::parse("Docs").expect("valid display name"),
                child_inode_id: InodeId(2),
            },
            WalDelta::CreateInode {
                delta_index: 2,
                inode_id: InodeId(3),
                inode_kind: InodeKind::File,
            },
            WalDelta::BindDirentry {
                delta_index: 3,
                parent_inode_id: InodeId(2),
                name_key: NameKey::parse("note.txt").expect("valid name key"),
                display_name: loonfs_api::DisplayName::parse("Note.txt")
                    .expect("valid display name"),
                child_inode_id: InodeId(3),
            },
            WalDelta::AppendFileRevision {
                delta_index: 4,
                inode_id: InodeId(3),
                revision_no: RevisionNo(1),
                content_ref: content_ref(5),
            },
            WalDelta::AppendFileRevision {
                delta_index: 5,
                inode_id: InodeId(3),
                revision_no: RevisionNo(2),
                content_ref: content_ref(6),
            },
            WalDelta::UnbindDirentry {
                delta_index: 6,
                parent_inode_id: InodeId(2),
                name_key: NameKey::parse("note.txt").expect("name key"),
                display_name: loonfs_api::DisplayName::parse("Note.txt").expect("display name"),
                child_inode_id: InodeId(3),
                target: DeltaPosition {
                    seq: committed_seq,
                    delta_index: 3,
                },
            },
            WalDelta::BindDirentry {
                delta_index: 7,
                parent_inode_id: InodeId(2),
                name_key: NameKey::parse("renamed.txt").expect("valid name key"),
                display_name: loonfs_api::DisplayName::parse("Renamed.txt")
                    .expect("valid display name"),
                child_inode_id: InodeId(3),
            },
            WalDelta::AppendFileRevision {
                delta_index: 8,
                inode_id: InodeId(3),
                revision_no: RevisionNo(3),
                content_ref: content_ref(5),
            },
            WalDelta::UnbindDirentry {
                delta_index: 9,
                parent_inode_id: InodeId(2),
                name_key: NameKey::parse("renamed.txt").expect("name key"),
                display_name: loonfs_api::DisplayName::parse("Renamed.txt").expect("display name"),
                child_inode_id: InodeId(3),
                target: DeltaPosition {
                    seq: committed_seq,
                    delta_index: 7,
                },
            },
            WalDelta::TombstoneSubtree {
                delta_index: 10,
                root_inode_id: InodeId(3),
                deleted_binding: DeletedBinding {
                    parent_inode_id: InodeId(2),
                    name_key: NameKey::parse("renamed.txt").expect("name key"),
                    display_name: loonfs_api::DisplayName::parse("Renamed.txt")
                        .expect("display name"),
                },
            },
            WalDelta::UnbindDirentry {
                delta_index: 11,
                parent_inode_id: InodeId(1),
                name_key: NameKey::parse("docs").expect("name key"),
                display_name: loonfs_api::DisplayName::parse("Docs").expect("display name"),
                child_inode_id: InodeId(2),
                target: DeltaPosition {
                    seq: committed_seq,
                    delta_index: 1,
                },
            },
            WalDelta::TombstoneSubtree {
                delta_index: 12,
                root_inode_id: InodeId(2),
                deleted_binding: DeletedBinding {
                    parent_inode_id: InodeId(1),
                    name_key: NameKey::parse("docs").expect("name key"),
                    display_name: loonfs_api::DisplayName::parse("Docs").expect("display name"),
                },
            },
        ],
    );
}

#[test]
fn overlays_across_commits_match_accumulated_wal_replay() {
    let first_seq = ChangeSeq(4);
    let second_seq = ChangeSeq(9);
    let first_deltas = [
        WalDelta::CreateInode {
            delta_index: 0,
            inode_id: InodeId(2),
            inode_kind: InodeKind::Directory,
        },
        WalDelta::BindDirentry {
            delta_index: 1,
            parent_inode_id: InodeId(1),
            name_key: NameKey::parse("a").expect("valid name key"),
            display_name: loonfs_api::DisplayName::parse("a").expect("valid display name"),
            child_inode_id: InodeId(2),
        },
        WalDelta::CreateInode {
            delta_index: 2,
            inode_id: InodeId(3),
            inode_kind: InodeKind::File,
        },
        WalDelta::BindDirentry {
            delta_index: 3,
            parent_inode_id: InodeId(2),
            name_key: NameKey::parse("f").expect("valid name key"),
            display_name: loonfs_api::DisplayName::parse("f").expect("valid display name"),
            child_inode_id: InodeId(3),
        },
        WalDelta::AppendFileRevision {
            delta_index: 4,
            inode_id: InodeId(3),
            revision_no: RevisionNo(1),
            content_ref: content_ref(7),
        },
    ];
    let second_deltas = [
        WalDelta::AppendFileRevision {
            delta_index: 0,
            inode_id: InodeId(3),
            revision_no: RevisionNo(2),
            content_ref: content_ref(8),
        },
        WalDelta::UnbindDirentry {
            delta_index: 1,
            parent_inode_id: InodeId(2),
            name_key: NameKey::parse("f").expect("name key"),
            display_name: loonfs_api::DisplayName::parse("f").expect("display name"),
            child_inode_id: InodeId(3),
            target: DeltaPosition {
                seq: first_seq,
                delta_index: 3,
            },
        },
        WalDelta::BindDirentry {
            delta_index: 2,
            parent_inode_id: InodeId(1),
            name_key: NameKey::parse("f2").expect("valid name key"),
            display_name: loonfs_api::DisplayName::parse("f2").expect("valid display name"),
            child_inode_id: InodeId(3),
        },
    ];

    let first_overlay = overlay_rows(first_seq, &first_deltas);
    let second_overlay = overlay_rows(second_seq, &second_deltas);

    let mut replayed = MetadataState::default();
    replayed.apply_committed_wal_deltas_mut(
        first_seq,
        &commit_id(first_seq),
        &loonfs_api::ActorId::loonfs(),
        4_200,
        &first_deltas,
    );
    replayed.apply_committed_wal_deltas_mut(
        second_seq,
        &commit_id(second_seq),
        &loonfs_api::ActorId::loonfs(),
        4_200,
        &second_deltas,
    );

    assert_eq!(
        concat(first_overlay.inodes(), second_overlay.inodes()),
        replayed.inodes(),
        "inode rows diverged"
    );
    assert_eq!(
        concat(
            first_overlay.direntry_binds(),
            second_overlay.direntry_binds()
        ),
        replayed.direntry_binds(),
        "direntry bind rows diverged"
    );

    assert_eq!(
        concat(first_overlay.revisions(), second_overlay.revisions()),
        replayed.revisions(),
        "revision rows diverged"
    );
    assert_eq!(
        concat(
            first_overlay.subtree_tombstones(),
            second_overlay.subtree_tombstones()
        ),
        replayed.subtree_tombstones(),
        "subtree tombstone rows diverged"
    );
}

fn concat<T: Clone>(first: &[T], second: &[T]) -> Vec<T> {
    first.iter().chain(second).cloned().collect()
}
