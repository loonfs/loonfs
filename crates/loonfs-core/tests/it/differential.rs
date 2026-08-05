//! Differential checks between core metadata and the `loonfs-model` oracle.

use loonfs_api::wire::manifest::{DeletedDirentry, TombstoneGeneration};
use loonfs_api::wire::wal::WalDelta;
use loonfs_api::{
    ChangeSeq, ContentId, ContentRef, DisplayName, InodeId, InodeKind, NameKey, RevisionNo,
};
use loonfs_core::metadata::{
    MetadataState as CoreMetadataState, SubtreeTombstoneAction as CoreTombstoneAction,
};
use loonfs_model::metadata::{
    MetadataState as ModelMetadataState, SubtreeTombstoneAction as ModelTombstoneAction,
};

type NormalizedInodes = Vec<(u64, &'static str, u64)>;
type NormalizedDirentryBinds = Vec<(u64, String, u64, u64, u32)>;
type NormalizedRevisions = Vec<(u64, u64, u64, u32, ContentId)>;
type NormalizedTombstones = Vec<NormalizedTombstone>;
type NormalizedMetadata = (
    NormalizedInodes,
    NormalizedDirentryBinds,
    NormalizedRevisions,
    NormalizedTombstones,
);

/// One tombstone event, whole: the generation, what the event did, and the
/// binding a delete recorded. Comparing only the generation would let a
/// dropped or mangled deleted binding through, so both sides reduce to
/// every field; the fields are named rather than positional so a divergence
/// prints which one differs.
#[derive(Debug, PartialEq, Eq)]
struct NormalizedTombstone {
    root_inode_id: u64,
    tombstone_seq: u64,
    tombstone_delta_index: u32,
    action: NormalizedTombstoneAction,
}

#[derive(Debug, PartialEq, Eq)]
enum NormalizedTombstoneAction {
    Set {
        deleted_binding: Option<NormalizedBinding>,
    },
    Revoke {
        target_seq: u64,
        target_delta_index: u32,
    },
}

#[derive(Debug, PartialEq, Eq)]
struct NormalizedBinding {
    parent_inode_id: u64,
    name_key: String,
    display_name: String,
}

fn content_ref(seed: &str) -> ContentRef {
    ContentRef::blob_v1(ContentId::generate(), seed.as_bytes())
}

fn create_directory(
    delta_index: u32,
    inode_id: InodeId,
    parent_inode_id: InodeId,
    display_name: &str,
) -> Vec<WalDelta> {
    vec![
        WalDelta::CreateInode {
            delta_index,
            inode_id,
            inode_kind: InodeKind::Directory,
        },
        WalDelta::BindDirentry {
            delta_index: delta_index.saturating_add(1),
            parent_inode_id,
            name_key: NameKey::parse(loonfs_api::name_key_for_display_name(display_name))
                .expect("derived name key"),
            display_name: DisplayName::parse(display_name).expect("valid display name"),
            child_inode_id: inode_id,
        },
    ]
}

fn create_file(
    delta_index: u32,
    inode_id: InodeId,
    parent_inode_id: InodeId,
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
            parent_inode_id,
            name_key: NameKey::parse(loonfs_api::name_key_for_display_name(display_name))
                .expect("derived name key"),
            display_name: DisplayName::parse(display_name).expect("valid display name"),
            child_inode_id: inode_id,
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
    parent_inode_id: InodeId,
    display_name: &str,
) -> Vec<WalDelta> {
    vec![WalDelta::BindDirentry {
        delta_index,
        parent_inode_id,
        name_key: NameKey::parse(loonfs_api::name_key_for_display_name(display_name))
            .expect("derived name key"),
        display_name: DisplayName::parse(display_name).expect("valid display name"),
        child_inode_id: inode_id,
    }]
}

/// A delete by path, which records the binding it removed.
fn tombstone(
    delta_index: u32,
    root_inode_id: InodeId,
    parent_inode_id: InodeId,
    display_name: &str,
) -> Vec<WalDelta> {
    vec![WalDelta::TombstoneSubtree {
        delta_index,
        root_inode_id,
        deleted_direntry: Some(DeletedDirentry {
            parent_inode_id,
            name_key: NameKey::parse(loonfs_api::name_key_for_display_name(display_name))
                .expect("derived name key"),
            display_name: DisplayName::parse(display_name).expect("valid display name"),
        }),
    }]
}

/// A delete addressed by inode, which had no name to record.
fn tombstone_without_binding(delta_index: u32, root_inode_id: InodeId) -> Vec<WalDelta> {
    vec![WalDelta::TombstoneSubtree {
        delta_index,
        root_inode_id,
        deleted_direntry: None,
    }]
}

/// Undelete as the commit path materializes it: revoke the exact deletion
/// generation, then re-bind the recovered inode.
fn undelete(
    delta_index: u32,
    inode_id: InodeId,
    parent_inode_id: InodeId,
    display_name: &str,
    target: TombstoneGeneration,
) -> Vec<WalDelta> {
    let mut deltas = vec![WalDelta::RevokeSubtreeTombstone {
        delta_index,
        root_inode_id: inode_id,
        target,
    }];
    deltas.extend(bind(
        delta_index.saturating_add(1),
        inode_id,
        parent_inode_id,
        display_name,
    ));
    deltas
}

#[test]
fn metadata_apply_matches_model_for_basic_commit_sequence() {
    assert_states_match(&[
        create_directory(0, InodeId(2), InodeId(1), "docs"),
        create_file(
            0,
            InodeId(3),
            InodeId(2),
            "readme.txt",
            content_ref("content-1"),
        ),
        append_revision(0, InodeId(3), RevisionNo(2), content_ref("content-2")),
    ]);
}

#[test]
fn metadata_apply_matches_model_for_rename() {
    assert_states_match(&[
        create_directory(0, InodeId(2), InodeId(1), "docs"),
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
        create_directory(0, InodeId(2), InodeId(1), "docs"),
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
        create_directory(0, InodeId(2), InodeId(1), "docs"),
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

// The deleted names below are spelled with capitals on purpose: their
// derived key case-folds, so a tombstone that lost the user-facing spelling
// and fell back to the key would differ here.

#[test]
fn metadata_apply_matches_model_for_delete_file() {
    assert_states_match(&[
        create_directory(0, InodeId(2), InodeId(1), "docs"),
        create_file(
            0,
            InodeId(3),
            InodeId(2),
            "Readme.TXT",
            content_ref("content-1"),
        ),
        tombstone(0, InodeId(3), InodeId(2), "Readme.TXT"),
    ]);
}

#[test]
fn metadata_apply_matches_model_for_delete_subtree() {
    assert_states_match(&[
        create_directory(0, InodeId(2), InodeId(1), "Docs"),
        create_directory(0, InodeId(3), InodeId(2), "nested"),
        tombstone(0, InodeId(2), InodeId(1), "Docs"),
    ]);
}

#[test]
fn metadata_apply_matches_model_for_delete_addressed_by_inode() {
    assert_states_match(&[
        create_directory(0, InodeId(2), InodeId(1), "docs"),
        create_file(
            0,
            InodeId(3),
            InodeId(2),
            "readme.txt",
            content_ref("content-1"),
        ),
        tombstone_without_binding(0, InodeId(3)),
    ]);
}

#[test]
fn metadata_apply_matches_model_for_undelete() {
    assert_states_match(&[
        create_directory(0, InodeId(2), InodeId(1), "docs"),
        create_file(
            0,
            InodeId(3),
            InodeId(2),
            "Readme.TXT",
            content_ref("content-1"),
        ),
        tombstone(1, InodeId(3), InodeId(2), "Readme.TXT"),
        // The revoke names the delete's own generation — the third commit,
        // second delta — which differs from where the revoke itself lands.
        undelete(
            0,
            InodeId(3),
            InodeId(2),
            "Readme.TXT",
            TombstoneGeneration {
                seq: ChangeSeq(3),
                delta_index: 1,
            },
        ),
    ]);
}

fn core_bootstrap_state() -> CoreMetadataState {
    CoreMetadataState::default().apply_committed_wal_deltas(
        ChangeSeq(0),
        4_200,
        &[WalDelta::CreateInode {
            delta_index: 0,
            inode_id: InodeId(1),
            inode_kind: InodeKind::Directory,
        }],
    )
}

fn model_bootstrap_state() -> ModelMetadataState {
    loonfs_model::bootstrap_metadata_state()
}

fn assert_states_match(sequences: &[Vec<WalDelta>]) {
    let mut core_state = core_bootstrap_state();
    let mut model_state = model_bootstrap_state();

    for (index, deltas) in sequences.iter().enumerate() {
        let seq = ChangeSeq(u64::try_from(index + 1).expect("seq"));
        core_state = core_state.apply_committed_wal_deltas(seq, 4_200, deltas);
        model_state = model_state.apply_committed_wal_deltas(seq, 4_200, deltas);
    }

    assert_eq!(normalize_core(&core_state), normalize_model(&model_state));
}

fn normalize_core(state: &CoreMetadataState) -> NormalizedMetadata {
    (
        state
            .inodes()
            .iter()
            .map(|inode| normalize_inode(inode.inode_id.0, inode.inode_kind, inode.created_seq.0))
            .collect(),
        state
            .direntry_binds()
            .iter()
            .map(|direntry| {
                (
                    direntry.parent_inode_id.0,
                    direntry.display_name.as_str().to_owned(),
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
                    revision.content_ref.content_id.clone(),
                )
            })
            .collect(),
        state
            .subtree_tombstones()
            .iter()
            .map(|tombstone| NormalizedTombstone {
                root_inode_id: tombstone.root_inode_id.0,
                tombstone_seq: tombstone.generation.seq.0,
                tombstone_delta_index: tombstone.generation.delta_index,
                action: match &tombstone.action {
                    CoreTombstoneAction::Set { deleted_direntry } => {
                        NormalizedTombstoneAction::Set {
                            deleted_binding: deleted_direntry.as_ref().map(|direntry| {
                                NormalizedBinding {
                                    parent_inode_id: direntry.parent_inode_id.0,
                                    name_key: direntry.name_key.as_str().to_owned(),
                                    display_name: direntry.display_name.as_str().to_owned(),
                                }
                            }),
                        }
                    }
                    CoreTombstoneAction::Revoke { target } => NormalizedTombstoneAction::Revoke {
                        target_seq: target.seq.0,
                        target_delta_index: target.delta_index,
                    },
                },
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
            .map(|inode| normalize_inode(inode.inode_id.0, inode.inode_kind, inode.created_seq.0))
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
                    revision.content_ref.content_id.clone(),
                )
            })
            .collect(),
        subtree_tombstones
            .iter()
            .map(|tombstone| NormalizedTombstone {
                root_inode_id: tombstone.root_inode_id.0,
                tombstone_seq: tombstone.tombstone_seq.0,
                tombstone_delta_index: tombstone.tombstone_delta_index,
                action: match &tombstone.action {
                    ModelTombstoneAction::Set { deleted_binding } => {
                        NormalizedTombstoneAction::Set {
                            deleted_binding: deleted_binding.as_ref().map(|binding| {
                                NormalizedBinding {
                                    parent_inode_id: binding.parent_inode_id.0,
                                    name_key: binding.name_key.clone(),
                                    display_name: binding.display_name.clone(),
                                }
                            }),
                        }
                    }
                    ModelTombstoneAction::Revoke { target } => NormalizedTombstoneAction::Revoke {
                        target_seq: target.seq.0,
                        target_delta_index: target.delta_index,
                    },
                },
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
            InodeKind::Directory => "dir",
            InodeKind::File => "file",
        },
        created_seq,
    )
}
