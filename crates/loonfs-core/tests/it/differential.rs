//! Differential checks between core metadata and the `loonfs-model` oracle.

use loonfs_api::wire::manifest::{DeletedDirentry, TombstoneGeneration};
use loonfs_api::wire::wal::WalDelta;
use loonfs_api::{
    ActorId, ActorRef, AttributeKey, AttributeRevisionNo, AttributeValue, Attributes, ChangeSeq,
    CommitId, ContentId, ContentRef, DisplayName, InodeId, InodeKind, NameKey, RevisionNo,
};
use loonfs_core::metadata::{
    MetadataState as CoreMetadataState, SubtreeTombstoneAction as CoreTombstoneAction,
};
use loonfs_model::metadata::{
    MetadataState as ModelMetadataState, SubtreeTombstoneAction as ModelTombstoneAction,
};

type NormalizedInodes = Vec<(u64, &'static str, u64, CommitId, ActorRef, u64)>;
type NormalizedDirentryBinds = Vec<(u64, String, u64, u64, u32)>;
type NormalizedRevisions = Vec<(u64, u64, u64, CommitId, u64, ActorRef, u32, ContentId)>;
type NormalizedTombstones = Vec<NormalizedTombstone>;
type NormalizedAttributes = Vec<NormalizedAttributeRevision>;
type NormalizedMetadata = (
    NormalizedInodes,
    NormalizedDirentryBinds,
    NormalizedRevisions,
    NormalizedTombstones,
    NormalizedAttributes,
);

/// One attribute revision, whole: the position, the counter, and every entry
/// of the map. Comparing only the counter would let a dropped or mangled
/// entry through, so both sides reduce every field, named rather than
/// positional so a divergence prints which one differs.
#[derive(Debug, PartialEq, Eq)]
struct NormalizedAttributeRevision {
    inode_id: u64,
    revision: u64,
    committed_seq: u64,
    commit_id: CommitId,
    delta_index: u32,
    updated_by: ActorRef,
    updated_at_ms: u64,
    entries: Vec<(String, String)>,
}

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
    commit_id: CommitId,
    deleted_at_ms: u64,
    deleted_by: ActorRef,
    action: NormalizedTombstoneAction,
}

#[derive(Debug, PartialEq, Eq)]
enum NormalizedTombstoneAction {
    Set {
        deleted_binding: NormalizedBinding,
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
    loonfs_test_support::ids::content_ref(seed.as_bytes())
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
        deleted_direntry: DeletedDirentry {
            parent_inode_id,
            name_key: NameKey::parse(loonfs_api::name_key_for_display_name(display_name))
                .expect("derived name key"),
            display_name: DisplayName::parse(display_name).expect("valid display name"),
        },
    }]
}

fn attribute_map(entries: &[(&str, &str)]) -> Attributes {
    Attributes::new(
        entries
            .iter()
            .map(|(key, value)| {
                let key = AttributeKey::parse(key).expect("valid attribute key");
                let value = AttributeValue::parse(value).expect("valid attribute value");
                (key, value)
            })
            .collect(),
    )
    .expect("valid attribute map")
}

fn update_attributes(
    delta_index: u32,
    inode_id: InodeId,
    revision: u64,
    attributes: Attributes,
) -> Vec<WalDelta> {
    vec![WalDelta::AppendAttributesRevision {
        delta_index,
        inode_id,
        attributes_revision_no: AttributeRevisionNo(revision),
        attributes,
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

#[test]
fn metadata_apply_matches_model_for_attribute_writes_and_removals() {
    assert_states_match(&[
        create_directory(0, InodeId(2), InodeId(1), "docs"),
        create_file(
            0,
            InodeId(3),
            InodeId(2),
            "readme.txt",
            content_ref("content-1"),
        ),
        // Set, then overwrite one key while adding another, then remove one:
        // each delta carries the whole resulting map.
        update_attributes(
            0,
            InodeId(3),
            1,
            attribute_map(&[("owner", "ada"), ("tags", "draft,review")]),
        ),
        update_attributes(
            0,
            InodeId(3),
            2,
            attribute_map(&[
                ("owner", "grace"),
                ("tags", "draft,review"),
                ("stage", "final"),
            ]),
        ),
        update_attributes(
            0,
            InodeId(3),
            3,
            attribute_map(&[("owner", "grace"), ("stage", "final")]),
        ),
        // A directory carries attributes too.
        update_attributes(0, InodeId(2), 1, attribute_map(&[("owner", "hopper")])),
    ]);
}

#[test]
fn metadata_apply_matches_model_for_a_cleared_attribute_map() {
    assert_states_match(&[
        create_directory(0, InodeId(2), InodeId(1), "docs"),
        update_attributes(0, InodeId(2), 1, attribute_map(&[("owner", "ada")])),
        update_attributes(0, InodeId(2), 2, Attributes::default()),
    ]);
}

#[test]
fn metadata_apply_matches_model_for_copy_attribute_inheritance() {
    assert_states_match(&[
        create_file(
            0,
            InodeId(2),
            InodeId(1),
            "source.txt",
            content_ref("content-1"),
        ),
        update_attributes(0, InodeId(2), 1, attribute_map(&[("owner", "ada")])),
        {
            let mut deltas = create_file(
                0,
                InodeId(3),
                InodeId(1),
                "copy.txt",
                content_ref("content-1"),
            );
            deltas.extend(update_attributes(
                3,
                InodeId(3),
                1,
                attribute_map(&[("owner", "ada")]),
            ));
            deltas
        },
    ]);
}

#[test]
fn metadata_apply_matches_model_for_delete_then_undelete_with_attributes() {
    assert_states_match(&[
        create_file(
            0,
            InodeId(2),
            InodeId(1),
            "Readme.TXT",
            content_ref("content-1"),
        ),
        update_attributes(0, InodeId(2), 1, attribute_map(&[("owner", "ada")])),
        tombstone(1, InodeId(2), InodeId(1), "Readme.TXT"),
        undelete(
            0,
            InodeId(2),
            InodeId(1),
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
        &loonfs_api::wire::control::genesis_commit_id(),
        &ActorRef::loonfs_system(),
        4_000,
        &[WalDelta::CreateInode {
            delta_index: 0,
            inode_id: InodeId(1),
            inode_kind: InodeKind::Directory,
        }],
    )
}

fn model_bootstrap_state() -> ModelMetadataState {
    loonfs_model::bootstrap_metadata_state(4_000)
}

fn assert_states_match(sequences: &[Vec<WalDelta>]) {
    let mut core_state = core_bootstrap_state();
    let mut model_state = model_bootstrap_state();

    for (index, deltas) in sequences.iter().enumerate() {
        let seq = ChangeSeq(u64::try_from(index + 1).expect("seq"));
        let actor = ActorRef::user(
            ActorId::parse(format!("scenario-actor-{index}")).expect("valid actor id"),
        );
        let committed_at_ms = 4_200 + u64::try_from(index).expect("timestamp offset");
        let commit_id =
            CommitId::parse(format!("c_differential_{index}")).expect("valid commit id");
        core_state =
            core_state.apply_committed_wal_deltas(seq, &commit_id, &actor, committed_at_ms, deltas);
        model_state = model_state.apply_committed_wal_deltas(
            seq,
            &commit_id,
            &actor,
            committed_at_ms,
            deltas,
        );
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
                    inode.inode_kind,
                    inode.created_seq.0,
                    inode.commit_id.clone(),
                    inode.created_by.clone(),
                    inode.created_at_ms,
                )
            })
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
                    revision.commit_id.clone(),
                    revision.committed_at_ms,
                    revision.committed_by.clone(),
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
                commit_id: tombstone.commit_id.clone(),
                deleted_at_ms: tombstone.deleted_at_ms,
                deleted_by: tombstone.deleted_by.clone(),
                action: match &tombstone.action {
                    CoreTombstoneAction::Set { deleted_direntry } => {
                        NormalizedTombstoneAction::Set {
                            deleted_binding: NormalizedBinding {
                                parent_inode_id: deleted_direntry.parent_inode_id.0,
                                name_key: deleted_direntry.name_key.as_str().to_owned(),
                                display_name: deleted_direntry.display_name.as_str().to_owned(),
                            },
                        }
                    }
                    CoreTombstoneAction::Revoke { target } => NormalizedTombstoneAction::Revoke {
                        target_seq: target.seq.0,
                        target_delta_index: target.delta_index,
                    },
                },
            })
            .collect(),
        state
            .attributes_revisions()
            .iter()
            .map(|record| NormalizedAttributeRevision {
                inode_id: record.inode_id.0,
                revision: record.attributes_revision_no.0,
                committed_seq: record.committed_seq.0,
                commit_id: record.commit_id.clone(),
                delta_index: record.delta_index,
                updated_by: record.updated_by.clone(),
                updated_at_ms: record.updated_at_ms,
                entries: record
                    .attributes
                    .iter()
                    .map(|(key, value)| (key.as_str().to_owned(), value.as_str().to_owned()))
                    .collect(),
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
        attribute_revisions,
        ..
    } = state;
    (
        inodes
            .iter()
            .map(|inode| {
                normalize_inode(
                    inode.inode_id.0,
                    inode.inode_kind,
                    inode.created_seq.0,
                    inode.commit_id.clone(),
                    inode.created_by.clone(),
                    inode.created_at_ms,
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
                    revision.commit_id.clone(),
                    revision.committed_at_ms,
                    revision.committed_by.clone(),
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
                commit_id: tombstone.commit_id.clone(),
                deleted_at_ms: tombstone.deleted_at_ms,
                deleted_by: tombstone.deleted_by.clone(),
                action: match &tombstone.action {
                    ModelTombstoneAction::Set { deleted_binding } => {
                        NormalizedTombstoneAction::Set {
                            deleted_binding: NormalizedBinding {
                                parent_inode_id: deleted_binding.parent_inode_id.0,
                                name_key: deleted_binding.name_key.clone(),
                                display_name: deleted_binding.display_name.clone(),
                            },
                        }
                    }
                    ModelTombstoneAction::Revoke { target } => NormalizedTombstoneAction::Revoke {
                        target_seq: target.seq.0,
                        target_delta_index: target.delta_index,
                    },
                },
            })
            .collect(),
        attribute_revisions
            .iter()
            .map(|record| NormalizedAttributeRevision {
                inode_id: record.inode_id.0,
                revision: record.revision_no,
                committed_seq: record.committed_seq.0,
                commit_id: record.commit_id.clone(),
                delta_index: record.delta_index,
                updated_by: record.updated_by.clone(),
                updated_at_ms: record.updated_at_ms,
                entries: record
                    .entries
                    .iter()
                    .map(|entry| (entry.key.clone(), entry.value.clone()))
                    .collect(),
            })
            .collect(),
    )
}

fn normalize_inode(
    inode_id: u64,
    inode_kind: InodeKind,
    created_seq: u64,
    commit_id: CommitId,
    created_by: ActorRef,
    created_at_ms: u64,
) -> (u64, &'static str, u64, CommitId, ActorRef, u64) {
    (
        inode_id,
        match inode_kind {
            InodeKind::Directory => "dir",
            InodeKind::File => "file",
        },
        created_seq,
        commit_id,
        created_by,
        created_at_ms,
    )
}
