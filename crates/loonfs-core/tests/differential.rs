use loonfs_api::wire::wal::WalDelta;
use loonfs_api::{
    sha256_digest, ChangeSeq, ContentRef, ContentRefKind, InodeId, InodeKind, RevisionNo,
};
use loonfs_core::metadata::MetadataState as CoreMetadataState;
use loonfs_model::metadata::MetadataState as ModelMetadataState;

type NormalizedInodes = Vec<(u64, &'static str, u64)>;
type NormalizedDirentryBinds = Vec<(u64, String, u64, u64, u32)>;
type NormalizedRevisions = Vec<(u64, u64, u64, u32, String)>;
type NormalizedTombstones = Vec<(u64, u64, u32)>;
type NormalizedMetadata = (
    NormalizedInodes,
    NormalizedDirentryBinds,
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

fn create_dir(
    delta_index: u32,
    inode_id: InodeId,
    parent_inode: InodeId,
    display_name: &str,
) -> Vec<WalDelta> {
    vec![
        WalDelta::CreateInode {
            delta_index,
            inode_id,
            inode_kind: InodeKind::Dir,
        },
        WalDelta::BindDirentry {
            delta_index: delta_index.saturating_add(1),
            parent_inode,
            name_key: loonfs_api::name_key_for_display_name(
                loonfs_api::NamePolicy::default(),
                display_name,
            ),
            display_name: display_name.to_owned(),
            child_inode: inode_id,
        },
    ]
}

fn create_file(
    delta_index: u32,
    inode_id: InodeId,
    parent_inode: InodeId,
    display_name: &str,
    content_ref: ContentRef,
) -> Vec<WalDelta> {
    vec![
        WalDelta::CreateInode {
            delta_index,
            inode_id,
            inode_kind: InodeKind::File,
        },
        WalDelta::BindDirentry {
            delta_index: delta_index.saturating_add(1),
            parent_inode,
            name_key: loonfs_api::name_key_for_display_name(
                loonfs_api::NamePolicy::default(),
                display_name,
            ),
            display_name: display_name.to_owned(),
            child_inode: inode_id,
        },
        WalDelta::AppendFileRevision {
            delta_index: delta_index.saturating_add(2),
            inode_id,
            revision_no: RevisionNo(1),
            content_ref,
        },
    ]
}

fn append_revision(
    delta_index: u32,
    inode_id: InodeId,
    revision_no: RevisionNo,
    content_ref: ContentRef,
) -> Vec<WalDelta> {
    vec![WalDelta::AppendFileRevision {
        delta_index,
        inode_id,
        revision_no,
        content_ref,
    }]
}

fn bind(
    delta_index: u32,
    inode_id: InodeId,
    parent_inode: InodeId,
    display_name: &str,
) -> Vec<WalDelta> {
    vec![WalDelta::BindDirentry {
        delta_index,
        parent_inode,
        name_key: loonfs_api::name_key_for_display_name(
            loonfs_api::NamePolicy::default(),
            display_name,
        ),
        display_name: display_name.to_owned(),
        child_inode: inode_id,
    }]
}

fn tombstone(delta_index: u32, root_inode: InodeId) -> Vec<WalDelta> {
    vec![WalDelta::TombstoneSubtree {
        delta_index,
        root_inode,
    }]
}

#[test]
fn metadata_apply_matches_model_for_basic_commit_sequence() {
    let core_state = core_bootstrap_state();
    let model_state = model_bootstrap_state();

    let create_dir = create_dir(0, InodeId(2), InodeId(1), "docs");
    let create_file = create_file(
        0,
        InodeId(3),
        InodeId(2),
        "readme.txt",
        content_ref("content-1"),
    );
    let replace_file = append_revision(0, InodeId(3), RevisionNo(2), content_ref("content-2"));

    let core_state = core_state
        .apply_committed_wal_deltas(ChangeSeq(1), &create_dir)
        .expect("core applies create-dir delta")
        .metadata_state
        .apply_committed_wal_deltas(ChangeSeq(2), &create_file)
        .expect("core applies create-file deltas")
        .metadata_state
        .apply_committed_wal_deltas(ChangeSeq(3), &replace_file)
        .expect("core applies replace-file delta")
        .metadata_state;

    let model_state = model_state
        .apply_committed_wal_deltas(ChangeSeq(1), &create_dir)
        .expect("model applies create-dir delta")
        .metadata_state
        .apply_committed_wal_deltas(ChangeSeq(2), &create_file)
        .expect("model applies create-file deltas")
        .metadata_state
        .apply_committed_wal_deltas(ChangeSeq(3), &replace_file)
        .expect("model applies replace-file delta")
        .metadata_state;

    assert_eq!(normalize_core(&core_state), normalize_model(&model_state));
}

#[test]
fn metadata_apply_matches_model_for_rename() {
    assert_states_match(&[
        create_dir(0, InodeId(2), InodeId(1), "docs"),
        create_file(
            0,
            InodeId(3),
            InodeId(2),
            "readme.txt",
            content_ref("content-1"),
        ),
        bind(0, InodeId(3), InodeId(1), "README.txt"),
    ]);
}

#[test]
fn metadata_apply_matches_model_for_restore_revision() {
    assert_states_match(&[
        create_dir(0, InodeId(2), InodeId(1), "docs"),
        create_file(
            0,
            InodeId(3),
            InodeId(2),
            "readme.txt",
            content_ref("content-1"),
        ),
        append_revision(0, InodeId(3), RevisionNo(2), content_ref("content-2")),
        append_revision(0, InodeId(3), RevisionNo(3), content_ref("content-1")),
    ]);
}

#[test]
fn metadata_apply_matches_model_for_restore_revision_of_current_head() {
    assert_states_match(&[
        create_dir(0, InodeId(2), InodeId(1), "docs"),
        create_file(
            0,
            InodeId(3),
            InodeId(2),
            "readme.txt",
            content_ref("content-1"),
        ),
        append_revision(0, InodeId(3), RevisionNo(2), content_ref("content-1")),
    ]);
}

#[test]
fn metadata_apply_matches_model_for_delete_file() {
    assert_states_match(&[
        create_dir(0, InodeId(2), InodeId(1), "docs"),
        create_file(
            0,
            InodeId(3),
            InodeId(2),
            "readme.txt",
            content_ref("content-1"),
        ),
        tombstone(0, InodeId(3)),
    ]);
}

#[test]
fn metadata_apply_matches_model_for_delete_subtree() {
    assert_states_match(&[
        create_dir(0, InodeId(2), InodeId(1), "docs"),
        create_dir(0, InodeId(3), InodeId(2), "nested"),
        tombstone(0, InodeId(2)),
    ]);
}

fn core_bootstrap_state() -> CoreMetadataState {
    CoreMetadataState::default()
        .apply_committed_wal_deltas(
            ChangeSeq(0),
            &[WalDelta::CreateInode {
                delta_index: 0,
                inode_id: InodeId(1),
                inode_kind: InodeKind::Dir,
            }],
        )
        .expect("bootstrap core root")
        .metadata_state
}

fn model_bootstrap_state() -> ModelMetadataState {
    loonfs_model::bootstrap_basis_metadata_state()
}

fn assert_states_match(sequences: &[Vec<WalDelta>]) {
    let mut core_state = core_bootstrap_state();
    let mut model_state = model_bootstrap_state();

    for (index, deltas) in sequences.iter().enumerate() {
        let seq = ChangeSeq(u64::try_from(index + 1).expect("seq"));
        core_state = core_state
            .apply_committed_wal_deltas(seq, deltas)
            .expect("core apply")
            .metadata_state;
        model_state = model_state
            .apply_committed_wal_deltas(seq, deltas)
            .expect("model apply")
            .metadata_state;
    }

    assert_eq!(normalize_core(&core_state), normalize_model(&model_state));
}

fn normalize_core(state: &CoreMetadataState) -> NormalizedMetadata {
    (
        state
            .inodes()
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
            .direntry_binds()
            .iter()
            .map(|direntry| {
                (
                    direntry.parent_inode_id.0,
                    direntry.display_name.clone(),
                    direntry.child_inode_id.0,
                    direntry.bind_seq.0,
                    direntry.bind_delta_index,
                )
            })
            .collect(),
        state
            .revisions()
            .iter()
            .map(|revision| {
                (
                    revision.inode_id.0,
                    revision.revision_no.0,
                    revision.committed_seq.0,
                    revision.revision_delta_index,
                    revision.content_ref.digest.clone(),
                )
            })
            .collect(),
        state
            .subtree_tombstones()
            .iter()
            .map(|tombstone| {
                (
                    tombstone.root_inode_id.0,
                    tombstone.tombstone_seq.0,
                    tombstone.tombstone_delta_index,
                )
            })
            .collect(),
    )
}

fn normalize_model(state: &ModelMetadataState) -> NormalizedMetadata {
    let ModelMetadataState {
        inodes,
        direntry_binds,
        revisions,
        subtree_tombstones,
        ..
    } = state;
    (
        inodes
            .iter()
            .map(|inode| {
                normalize_inode(
                    inode.inode_id.0,
                    inode.inode_kind.clone(),
                    inode.created_seq.0,
                )
            })
            .collect(),
        direntry_binds
            .iter()
            .map(|direntry| {
                (
                    direntry.parent_inode_id.0,
                    direntry.display_name.clone(),
                    direntry.child_inode_id.0,
                    direntry.bind_seq.0,
                    direntry.bind_delta_index,
                )
            })
            .collect(),
        revisions
            .iter()
            .map(|revision| {
                (
                    revision.inode_id.0,
                    revision.revision_no.0,
                    revision.committed_seq.0,
                    revision.revision_delta_index,
                    revision.content_ref.digest.clone(),
                )
            })
            .collect(),
        subtree_tombstones
            .iter()
            .map(|tombstone| {
                (
                    tombstone.root_inode_id.0,
                    tombstone.tombstone_seq.0,
                    tombstone.tombstone_delta_index,
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
        },
        created_seq,
    )
}
