use loon_api::{
    sha256_digest, ChangeSeq, ContentRef, ContentRefKind, InodeId, InodeKind, RevisionNo, WalOp,
};
use loon_core::metadata::{InodeRecord, MetadataState as CoreMetadataState};
use loon_model::metadata::MetadataState as ModelMetadataState;

type NormalizedInodes = Vec<(u64, &'static str, u64)>;
type NormalizedDirentries = Vec<(u64, String, u64, u64, u32)>;
type NormalizedRevisions = Vec<(u64, u64, u64, u32, String)>;
type NormalizedTombstones = Vec<(u64, u64, u32)>;
type NormalizedMetadata = (
    NormalizedInodes,
    NormalizedDirentries,
    NormalizedRevisions,
    NormalizedTombstones,
);

fn content_ref(seed: &str) -> ContentRef {
    ContentRef {
        kind: ContentRefKind::WholeFileV0,
        digest: sha256_digest(seed.as_bytes()),
        size_bytes: seed.len() as u64,
    }
}

#[test]
fn metadata_apply_matches_model_for_basic_commit_sequence() {
    let core_state = core_bootstrap_state();
    let model_state = model_bootstrap_state();

    let create_dir = vec![WalOp::CreateDir {
        op_index: 0,
        inode_id: InodeId(2),
        parent_inode: InodeId(1),
        display_name: "docs".to_owned(),
    }];
    let create_file = vec![WalOp::CreateFile {
        op_index: 0,
        inode_id: InodeId(3),
        parent_inode: InodeId(2),
        display_name: "readme.txt".to_owned(),
        content_ref: content_ref("content-1"),
    }];
    let replace_file = vec![WalOp::ReplaceFile {
        op_index: 0,
        inode_id: InodeId(3),
        base_revision_no: RevisionNo(1),
        content_ref: content_ref("content-2"),
    }];

    let core_state = core_state
        .apply_committed_wal_ops(ChangeSeq(1), &create_dir)
        .unwrap()
        .metadata_state
        .apply_committed_wal_ops(ChangeSeq(2), &create_file)
        .unwrap()
        .metadata_state
        .apply_committed_wal_ops(ChangeSeq(3), &replace_file)
        .unwrap()
        .metadata_state;

    let model_state = model_state
        .apply_committed_wal_ops(ChangeSeq(1), &create_dir)
        .unwrap()
        .metadata_state
        .apply_committed_wal_ops(ChangeSeq(2), &create_file)
        .unwrap()
        .metadata_state
        .apply_committed_wal_ops(ChangeSeq(3), &replace_file)
        .unwrap()
        .metadata_state;

    assert_eq!(normalize_core(&core_state), normalize_model(&model_state));
}

#[test]
fn metadata_apply_matches_model_for_rename() {
    assert_states_match(&[
        vec![WalOp::CreateDir {
            op_index: 0,
            inode_id: InodeId(2),
            parent_inode: InodeId(1),
            display_name: "docs".to_owned(),
        }],
        vec![WalOp::CreateFile {
            op_index: 0,
            inode_id: InodeId(3),
            parent_inode: InodeId(2),
            display_name: "readme.txt".to_owned(),
            content_ref: content_ref("content-1"),
        }],
        vec![WalOp::Rename {
            op_index: 0,
            inode_id: InodeId(3),
            new_parent_inode: InodeId(1),
            new_display_name: "README.txt".to_owned(),
        }],
    ]);
}

#[test]
fn metadata_apply_matches_model_for_restore_revision() {
    assert_states_match(&[
        vec![WalOp::CreateDir {
            op_index: 0,
            inode_id: InodeId(2),
            parent_inode: InodeId(1),
            display_name: "docs".to_owned(),
        }],
        vec![WalOp::CreateFile {
            op_index: 0,
            inode_id: InodeId(3),
            parent_inode: InodeId(2),
            display_name: "readme.txt".to_owned(),
            content_ref: content_ref("content-1"),
        }],
        vec![WalOp::ReplaceFile {
            op_index: 0,
            inode_id: InodeId(3),
            base_revision_no: RevisionNo(1),
            content_ref: content_ref("content-2"),
        }],
        vec![WalOp::RestoreRevision {
            op_index: 0,
            inode_id: InodeId(3),
            source_revision_no: RevisionNo(1),
            base_revision_no: RevisionNo(2),
            content_ref: content_ref("content-1"),
        }],
    ]);
}

#[test]
fn metadata_apply_matches_model_for_restore_revision_of_current_head() {
    assert_states_match(&[
        vec![WalOp::CreateDir {
            op_index: 0,
            inode_id: InodeId(2),
            parent_inode: InodeId(1),
            display_name: "docs".to_owned(),
        }],
        vec![WalOp::CreateFile {
            op_index: 0,
            inode_id: InodeId(3),
            parent_inode: InodeId(2),
            display_name: "readme.txt".to_owned(),
            content_ref: content_ref("content-1"),
        }],
        vec![WalOp::RestoreRevision {
            op_index: 0,
            inode_id: InodeId(3),
            source_revision_no: RevisionNo(1),
            base_revision_no: RevisionNo(1),
            content_ref: content_ref("content-1"),
        }],
    ]);
}

#[test]
fn metadata_apply_matches_model_for_delete_file() {
    assert_states_match(&[
        vec![WalOp::CreateDir {
            op_index: 0,
            inode_id: InodeId(2),
            parent_inode: InodeId(1),
            display_name: "docs".to_owned(),
        }],
        vec![WalOp::CreateFile {
            op_index: 0,
            inode_id: InodeId(3),
            parent_inode: InodeId(2),
            display_name: "readme.txt".to_owned(),
            content_ref: content_ref("content-1"),
        }],
        vec![WalOp::DeleteFile {
            op_index: 0,
            inode_id: InodeId(3),
        }],
    ]);
}

#[test]
fn metadata_apply_matches_model_for_delete_subtree() {
    assert_states_match(&[
        vec![WalOp::CreateDir {
            op_index: 0,
            inode_id: InodeId(2),
            parent_inode: InodeId(1),
            display_name: "docs".to_owned(),
        }],
        vec![WalOp::CreateDir {
            op_index: 0,
            inode_id: InodeId(3),
            parent_inode: InodeId(2),
            display_name: "nested".to_owned(),
        }],
        vec![WalOp::DeleteSubtree {
            op_index: 0,
            root_inode: InodeId(2),
        }],
    ]);
}

fn core_bootstrap_state() -> CoreMetadataState {
    CoreMetadataState {
        inodes: vec![InodeRecord {
            inode_id: InodeId(1),
            inode_kind: InodeKind::Dir,
            created_seq: ChangeSeq(0),
        }],
        direntries: Vec::new(),
        revisions: Vec::new(),
        subtree_tombstones: Vec::new(),
        commit_receipts: Vec::new(),
    }
}

fn model_bootstrap_state() -> ModelMetadataState {
    loon_model::bootstrap_basis_metadata_state()
}

fn assert_states_match(sequences: &[Vec<WalOp>]) {
    let mut core_state = core_bootstrap_state();
    let mut model_state = model_bootstrap_state();

    for (index, ops) in sequences.iter().enumerate() {
        let seq = ChangeSeq(u64::try_from(index + 1).expect("seq"));
        core_state = core_state
            .apply_committed_wal_ops(seq, ops)
            .expect("core apply")
            .metadata_state;
        model_state = model_state
            .apply_committed_wal_ops(seq, ops)
            .expect("model apply")
            .metadata_state;
    }

    assert_eq!(normalize_core(&core_state), normalize_model(&model_state));
}

fn normalize_core(state: &CoreMetadataState) -> NormalizedMetadata {
    (
        state
            .inodes
            .iter()
            .map(|inode| {
                normalize_inode(
                    inode.inode_id.0,
                    inode.inode_kind.clone(),
                    inode.created_seq.0,
                )
            })
            .collect(),
        state
            .direntries
            .iter()
            .map(|direntry| {
                (
                    direntry.parent_inode_id.0,
                    direntry.display_name.clone(),
                    direntry.child_inode_id.0,
                    direntry.bind_seq.0,
                    direntry.bind_op_index,
                )
            })
            .collect(),
        state
            .revisions
            .iter()
            .map(|revision| {
                (
                    revision.inode_id.0,
                    revision.revision_no.0,
                    revision.committed_seq.0,
                    revision.revision_op_index,
                    revision.content_ref.digest.clone(),
                )
            })
            .collect(),
        state
            .subtree_tombstones
            .iter()
            .map(|tombstone| {
                (
                    tombstone.root_inode_id.0,
                    tombstone.tombstone_seq.0,
                    tombstone.tombstone_op_index,
                )
            })
            .collect(),
    )
}

fn normalize_model(state: &ModelMetadataState) -> NormalizedMetadata {
    (
        state
            .inodes
            .iter()
            .map(|inode| {
                normalize_inode(
                    inode.inode_id.0,
                    inode.inode_kind.clone(),
                    inode.created_seq.0,
                )
            })
            .collect(),
        state
            .direntries
            .iter()
            .map(|direntry| {
                (
                    direntry.parent_inode_id.0,
                    direntry.display_name.clone(),
                    direntry.child_inode_id.0,
                    direntry.bind_seq.0,
                    direntry.bind_op_index,
                )
            })
            .collect(),
        state
            .revisions
            .iter()
            .map(|revision| {
                (
                    revision.inode_id.0,
                    revision.revision_no.0,
                    revision.committed_seq.0,
                    revision.revision_op_index,
                    revision.content_ref.digest.clone(),
                )
            })
            .collect(),
        state
            .subtree_tombstones
            .iter()
            .map(|tombstone| {
                (
                    tombstone.root_inode_id.0,
                    tombstone.tombstone_seq.0,
                    tombstone.tombstone_op_index,
                )
            })
            .collect(),
    )
}

fn normalize_inode(
    inode_id: u64,
    inode_kind: InodeKind,
    created_seq: u64,
) -> (u64, &'static str, u64) {
    (
        inode_id,
        match inode_kind {
            InodeKind::Dir => "dir",
            InodeKind::File => "file",
            InodeKind::Mount => "mount",
        },
        created_seq,
    )
}
