//! The accumulating in-batch overlay: rows earlier ops would persist,
//! visible to the validation of later ops in the same batch.

use super::materialize::materialize_validated_op;
use super::ValidatedOp;
use crate::metadata::MetadataState;
use loonfs_api::ChangeSeq;

pub(super) struct CommitOverlayRows {
    rows: MetadataState,
}

impl CommitOverlayRows {
    pub(super) fn new() -> Self {
        Self {
            rows: MetadataState::default(),
        }
    }

    pub(super) fn from_rows(rows: &MetadataState) -> Self {
        Self { rows: rows.clone() }
    }

    pub(super) fn rows(&self) -> &MetadataState {
        &self.rows
    }

    /// Applies one validated op's effects to the overlay rows by
    /// materializing its WAL deltas and appending each through the same row
    /// mapping durable replay uses
    /// ([`MetadataState::apply_committed_wal_delta_mut`]).
    ///
    /// The overlay is derived from the WAL encoding instead of mapping
    /// [`ValidatedOp`] to rows directly, so the state later ops in a batch
    /// validate against cannot disagree with what replay persists for the
    /// earlier ops. The plan is materialized once more by
    /// `materialize_commit` after validation; reusing these deltas there
    /// would mean carrying them inside the serialized [`ValidatedOp`]s,
    /// which is not worth the wire churn for a few small per-op clones.
    pub(super) fn apply_validated_op_mut(
        &mut self,
        committed_seq: ChangeSeq,
        committed_at_ms: u64,
        op: &ValidatedOp,
    ) {
        let (deltas, _result) = materialize_validated_op(op);
        for delta in &deltas {
            self.rows.apply_committed_wal_delta_mut(
                committed_seq,
                committed_at_ms,
                &delta.wal_delta,
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::ResolvedBinding;
    use loonfs_api::wire::wal::WalDelta;
    use loonfs_api::{ContentRef, ContentRefKind, InodeId, RevisionNo};

    /// The commit overlay derives its rows from the WAL encoding
    /// (`materialize_validated_op` + `apply_committed_wal_delta_mut`), the
    /// same pair durable replay is built on. These tests pin the boundary
    /// that keeps validation honest: for the same ops and committed seq, the
    /// overlay rows must equal the rows batch WAL replay appends, row for
    /// row — so any future shortcut that encodes an op's effects for the
    /// overlay directly (or a replay change the overlay misses) fails here.
    fn assert_overlay_matches_replay(committed_seq: ChangeSeq, ops: &[ValidatedOp]) {
        let overlay = overlay_rows(committed_seq, ops);
        let mut replayed = MetadataState::default();
        replayed.apply_committed_wal_deltas_mut(
            committed_seq,
            4_200,
            &materialized_wal_deltas(ops),
        );

        assert!(
            replayed.row_count() > 0,
            "scenario must exercise at least one row"
        );
        assert_row_categories_equal(&overlay, &replayed);
    }

    fn overlay_rows(committed_seq: ChangeSeq, ops: &[ValidatedOp]) -> MetadataState {
        let mut overlay = CommitOverlayRows::new();
        for op in ops {
            overlay.apply_validated_op_mut(committed_seq, 4_200, op);
        }
        overlay.rows
    }

    fn materialized_wal_deltas(ops: &[ValidatedOp]) -> Vec<WalDelta> {
        ops.iter()
            .flat_map(|op| materialize_validated_op(op).0)
            .map(|delta| delta.wal_delta)
            .collect()
    }

    fn assert_row_categories_equal(overlay: &MetadataState, replayed: &MetadataState) {
        assert_eq!(overlay.inodes(), replayed.inodes(), "inode rows diverged");
        assert_eq!(
            overlay.direntry_binds(),
            replayed.direntry_binds(),
            "direntry bind rows diverged"
        );
        assert_eq!(
            overlay.direntry_unbinds(),
            replayed.direntry_unbinds(),
            "direntry unbind rows diverged"
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
    }

    fn content_ref(seed: u8) -> ContentRef {
        ContentRef {
            kind: ContentRefKind::WholeFileV0,
            digest: format!("sha256:{}", format!("{seed:02x}").repeat(32)),
            size_bytes: u64::from(seed) + 10,
        }
    }

    fn binding(
        parent: u64,
        name_key: &str,
        display_name: &str,
        child: u64,
        bind_seq: u64,
        bind_delta_index: u32,
    ) -> ResolvedBinding {
        ResolvedBinding {
            parent_inode_id: InodeId(parent),
            name_key: name_key.to_owned(),
            display_name: display_name.to_owned(),
            child_inode_id: InodeId(child),
            bind_seq: ChangeSeq(bind_seq),
            bind_delta_index,
        }
    }

    #[test]
    fn create_dir_overlay_rows_match_replayed_wal_deltas() {
        assert_overlay_matches_replay(
            ChangeSeq(7),
            &[ValidatedOp::CreateDir {
                op_index: 0,
                parent_inode_id: InodeId(1),
                display_name: "Docs".to_owned(),
                name_key: "docs".to_owned(),
                child_inode_id: InodeId(2),
                create_inode_delta_index: 0,
                bind_delta_index: 1,
            }],
        );
    }

    #[test]
    fn create_file_overlay_rows_match_replayed_wal_deltas() {
        assert_overlay_matches_replay(
            ChangeSeq(3),
            &[ValidatedOp::CreateFile {
                op_index: 0,
                parent_inode_id: InodeId(1),
                display_name: "Note.TXT".to_owned(),
                name_key: "note.txt".to_owned(),
                child_inode_id: InodeId(2),
                content_ref: content_ref(1),
                create_inode_delta_index: 0,
                bind_delta_index: 1,
                revision_delta_index: 2,
            }],
        );
    }

    #[test]
    fn replace_file_overlay_rows_match_replayed_wal_deltas() {
        assert_overlay_matches_replay(
            ChangeSeq(9),
            &[ValidatedOp::ReplaceFile {
                op_index: 0,
                inode_id: InodeId(4),
                revision_no: RevisionNo(5),
                content_ref: content_ref(2),
                revision_delta_index: 0,
            }],
        );
    }

    #[test]
    fn restore_revision_overlay_rows_match_replayed_wal_deltas() {
        assert_overlay_matches_replay(
            ChangeSeq(12),
            &[ValidatedOp::RestoreRevision {
                op_index: 0,
                inode_id: InodeId(4),
                source_revision_no: RevisionNo(2),
                revision_no: RevisionNo(6),
                content_ref: content_ref(3),
                revision_delta_index: 0,
            }],
        );
    }

    #[test]
    fn delete_file_overlay_rows_match_replayed_wal_deltas() {
        assert_overlay_matches_replay(
            ChangeSeq(15),
            &[ValidatedOp::DeleteFile {
                op_index: 0,
                inode_id: InodeId(4),
                source_binding: binding(2, "note.txt", "Note.TXT", 4, 8, 3),
                unbind_delta_index: 0,
                tombstone_delta_index: 1,
            }],
        );
    }

    #[test]
    fn rename_of_preexisting_binding_overlay_rows_match_replayed_wal_deltas() {
        assert_overlay_matches_replay(
            ChangeSeq(20),
            &[ValidatedOp::Rename {
                op_index: 0,
                inode_id: InodeId(4),
                source_binding: binding(2, "old.txt", "Old.TXT", 4, 11, 2),
                new_parent_inode_id: InodeId(3),
                new_display_name: "New.TXT".to_owned(),
                new_name_key: "new.txt".to_owned(),
                unbind_delta_index: 0,
                bind_delta_index: 1,
            }],
        );
    }

    #[test]
    fn rename_of_same_commit_binding_overlay_rows_match_replayed_wal_deltas() {
        // The rename unbinds the binding created by the CreateFile op earlier
        // in the same commit, so its source binding carries this commit's seq
        // and the earlier op's bind delta index.
        let committed_seq = ChangeSeq(21);
        assert_overlay_matches_replay(
            committed_seq,
            &[
                ValidatedOp::CreateFile {
                    op_index: 0,
                    parent_inode_id: InodeId(1),
                    display_name: "Draft.md".to_owned(),
                    name_key: "draft.md".to_owned(),
                    child_inode_id: InodeId(2),
                    content_ref: content_ref(4),
                    create_inode_delta_index: 0,
                    bind_delta_index: 1,
                    revision_delta_index: 2,
                },
                ValidatedOp::Rename {
                    op_index: 1,
                    inode_id: InodeId(2),
                    source_binding: binding(1, "draft.md", "Draft.md", 2, committed_seq.0, 1),
                    new_parent_inode_id: InodeId(1),
                    new_display_name: "Final.md".to_owned(),
                    new_name_key: "final.md".to_owned(),
                    unbind_delta_index: 3,
                    bind_delta_index: 4,
                },
            ],
        );
    }

    #[test]
    fn delete_subtree_overlay_rows_match_replayed_wal_deltas() {
        assert_overlay_matches_replay(
            ChangeSeq(30),
            &[ValidatedOp::DeleteSubtree {
                op_index: 0,
                root_inode_id: InodeId(5),
                source_binding: binding(1, "attic", "Attic", 5, 22, 6),
                unbind_delta_index: 0,
                tombstone_delta_index: 1,
            }],
        );
    }

    #[test]
    fn chained_multi_op_commit_overlay_rows_match_replayed_wal_deltas() {
        // A single commit whose later ops mutate what earlier ops created:
        // create /Docs, create /Docs/Note.txt, replace it, rename it, restore
        // it, delete it, then delete the /Docs subtree. Source bindings point
        // at binds made earlier in this commit (same seq, earlier delta
        // index), mirroring how validation resolves them from the overlay.
        let committed_seq = ChangeSeq(40);
        assert_overlay_matches_replay(
            committed_seq,
            &[
                ValidatedOp::CreateDir {
                    op_index: 0,
                    parent_inode_id: InodeId(1),
                    display_name: "Docs".to_owned(),
                    name_key: "docs".to_owned(),
                    child_inode_id: InodeId(2),
                    create_inode_delta_index: 0,
                    bind_delta_index: 1,
                },
                ValidatedOp::CreateFile {
                    op_index: 1,
                    parent_inode_id: InodeId(2),
                    display_name: "Note.txt".to_owned(),
                    name_key: "note.txt".to_owned(),
                    child_inode_id: InodeId(3),
                    content_ref: content_ref(5),
                    create_inode_delta_index: 2,
                    bind_delta_index: 3,
                    revision_delta_index: 4,
                },
                ValidatedOp::ReplaceFile {
                    op_index: 2,
                    inode_id: InodeId(3),
                    revision_no: RevisionNo(2),
                    content_ref: content_ref(6),
                    revision_delta_index: 5,
                },
                ValidatedOp::Rename {
                    op_index: 3,
                    inode_id: InodeId(3),
                    source_binding: binding(2, "note.txt", "Note.txt", 3, committed_seq.0, 3),
                    new_parent_inode_id: InodeId(2),
                    new_display_name: "Renamed.txt".to_owned(),
                    new_name_key: "renamed.txt".to_owned(),
                    unbind_delta_index: 6,
                    bind_delta_index: 7,
                },
                ValidatedOp::RestoreRevision {
                    op_index: 4,
                    inode_id: InodeId(3),
                    source_revision_no: RevisionNo(1),
                    revision_no: RevisionNo(3),
                    content_ref: content_ref(5),
                    revision_delta_index: 8,
                },
                ValidatedOp::DeleteFile {
                    op_index: 5,
                    inode_id: InodeId(3),
                    source_binding: binding(2, "renamed.txt", "Renamed.txt", 3, committed_seq.0, 7),
                    unbind_delta_index: 9,
                    tombstone_delta_index: 10,
                },
                ValidatedOp::DeleteSubtree {
                    op_index: 6,
                    root_inode_id: InodeId(2),
                    source_binding: binding(1, "docs", "Docs", 2, committed_seq.0, 1),
                    unbind_delta_index: 11,
                    tombstone_delta_index: 12,
                },
            ],
        );
    }

    #[test]
    fn overlays_across_commits_match_accumulated_wal_replay() {
        // Two commits at distinct seqs: replaying both delta batches into one
        // state must append exactly the rows each commit's overlay produced,
        // in commit order, for every row category.
        let first_seq = ChangeSeq(4);
        let second_seq = ChangeSeq(9);
        let first_ops = [
            ValidatedOp::CreateDir {
                op_index: 0,
                parent_inode_id: InodeId(1),
                display_name: "a".to_owned(),
                name_key: "a".to_owned(),
                child_inode_id: InodeId(2),
                create_inode_delta_index: 0,
                bind_delta_index: 1,
            },
            ValidatedOp::CreateFile {
                op_index: 1,
                parent_inode_id: InodeId(2),
                display_name: "f".to_owned(),
                name_key: "f".to_owned(),
                child_inode_id: InodeId(3),
                content_ref: content_ref(7),
                create_inode_delta_index: 2,
                bind_delta_index: 3,
                revision_delta_index: 4,
            },
        ];
        let second_ops = [
            ValidatedOp::ReplaceFile {
                op_index: 0,
                inode_id: InodeId(3),
                revision_no: RevisionNo(2),
                content_ref: content_ref(8),
                revision_delta_index: 0,
            },
            ValidatedOp::Rename {
                op_index: 1,
                inode_id: InodeId(3),
                source_binding: binding(2, "f", "f", 3, first_seq.0, 3),
                new_parent_inode_id: InodeId(1),
                new_display_name: "f2".to_owned(),
                new_name_key: "f2".to_owned(),
                unbind_delta_index: 1,
                bind_delta_index: 2,
            },
        ];

        let first_overlay = overlay_rows(first_seq, &first_ops);
        let second_overlay = overlay_rows(second_seq, &second_ops);

        let mut replayed = MetadataState::default();
        replayed.apply_committed_wal_deltas_mut(
            first_seq,
            4_200,
            &materialized_wal_deltas(&first_ops),
        );
        replayed.apply_committed_wal_deltas_mut(
            second_seq,
            4_200,
            &materialized_wal_deltas(&second_ops),
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
            concat(
                first_overlay.direntry_unbinds(),
                second_overlay.direntry_unbinds()
            ),
            replayed.direntry_unbinds(),
            "direntry unbind rows diverged"
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
}
