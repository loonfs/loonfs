//! Differential checks between core metadata and the `loonfs-model` oracle.
//!
//! Each scenario replays the same WAL deltas through both and compares their
//! rows. It also asks both the same visibility questions: after every commit
//! at the new head, where core reads its indexes, and at every earlier
//! sequence once the scenario ends, where core scans its rows.

use loonfs_core::metadata::{
    DirentryBindingRecord as CoreBindingRecord, InodeRecord as CoreInodeRecord,
    MetadataState as CoreMetadataState, ResolvedVisiblePath,
    TombstoneRowAction as CoreTombstoneAction, VisiblePathError,
};
use loonfs_core::publish::{
    CommitCandidate, CommitRequest, FilesystemOperation, InlineContent, NamespaceCommitEngine,
};
use loonfs_core::time::Deadline;
use loonfs_model::metadata::{
    DirentryBindingRecord as ModelBindingRecord, DirentryBindingState as ModelBindingState,
    InodeRecord as ModelInodeRecord, MetadataState as ModelMetadataState,
    SubtreeTombstoneAction as ModelTombstoneAction,
};
use loonfs_model::visibility::PathLookup;
use loonfs_objectstore::keys::wal_prefix;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::timing::StdMonotonicTimer;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::{DeletedBinding, DeltaPosition, MetadataRow};
use loonfs_types::format::wal::decode_wal_object_envelope_zstd;
use loonfs_types::format::wal::WalDelta;
use loonfs_types::{
    AbsolutePath, AccessGrants, AccessRevisionNo, ActorId, AttributeKey, AttributeValue,
    Attributes, AttributesRevisionNo, ChangeSeq, CommitId, ContentId, ContentRef, DisplayName,
    InodeId, InodeKind, NameKey, NamespaceNaming, RevisionNo, ROOT_INODE_ID,
};
use std::collections::BTreeSet;
use std::sync::Arc;
use NamespaceNaming::{CaseInsensitive, CaseSensitive};

type NormalizedInode = (u64, &'static str, u64, CommitId, ActorId, u64);
type NormalizedInodes = Vec<NormalizedInode>;
type NormalizedDirentryBinds = Vec<NormalizedDirectoryBinding>;

#[derive(Debug, PartialEq, Eq)]
struct NormalizedDirectoryBinding {
    parent_inode_id: InodeId,
    name_key: NameKey,
    child_inode_id: InodeId,
    position: DeltaPosition,
    display_name: Option<DisplayName>,
}
type NormalizedRevisions = Vec<(u64, u64, u64, CommitId, u64, ActorId, u32, ContentId)>;
type NormalizedTombstones = Vec<NormalizedTombstone>;
type NormalizedAttributes = Vec<NormalizedAttributeRevision>;
type NormalizedMetadata = (
    NormalizedInodes,
    NormalizedDirentryBinds,
    NormalizedRevisions,
    NormalizedTombstones,
    NormalizedAttributes,
    Vec<NormalizedAccessRevision>,
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
    committed_by: ActorId,
    committed_at_ms: u64,
    entries: Vec<(String, String)>,
}

#[derive(Debug, PartialEq, Eq)]
struct NormalizedAccessRevision {
    inode_id: u64,
    revision: u64,
    committed_seq: u64,
    commit_id: CommitId,
    delta_index: u32,
    committed_by: ActorId,
    committed_at_ms: u64,
    boundary: bool,
    grants: Vec<(String, Vec<String>)>,
}

/// One tombstone event, whole: the position, what the event did, and the
/// binding a delete recorded. Comparing only the position would let a
/// dropped or mangled deleted binding through, so both sides reduce to
/// every field; the fields are named rather than positional so a divergence
/// prints which one differs.
#[derive(Debug, PartialEq, Eq)]
struct NormalizedTombstone {
    root_inode_id: u64,
    tombstone_seq: u64,
    tombstone_delta_index: u32,
    commit_id: CommitId,
    committed_at_ms: u64,
    committed_by: ActorId,
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

#[derive(Debug, PartialEq, Eq)]
struct ListingEntry {
    binding: NormalizedDirectoryBinding,
    child_kind: InodeKind,
}

/// A path lookup outcome in the shape of core's answer.
#[derive(Debug, PartialEq, Eq)]
enum PathAnswer {
    Found {
        absolute_path: String,
        inode_id: InodeId,
        inode_kind: InodeKind,
        created_by: ActorId,
        created_at_ms: u64,
        parent_inode_id: Option<InodeId>,
        display_name: String,
        binding_version: Option<DeltaPosition>,
    },
    NotFound {
        absolute_path: String,
    },
    NotADirectory {
        absolute_path: String,
        inode_id: InodeId,
        inode_kind: InodeKind,
    },
    RootMissing,
}

/// What both sides answer at each compared sequence: every inode and every
/// directory the scenario creates, and the root plus every path it names.
struct Questions {
    inodes: Vec<InodeId>,
    directories: Vec<InodeId>,
    paths: Vec<AbsolutePath>,
}

impl Questions {
    fn new(paths: &[&str], commits: &[Vec<WalDelta>]) -> Self {
        let mut questions = Self {
            inodes: vec![ROOT_INODE_ID],
            directories: vec![ROOT_INODE_ID],
            paths: std::iter::once("/")
                .chain(paths.iter().copied())
                .map(|path| AbsolutePath::parse(path).expect("valid path"))
                .collect(),
        };
        for delta in commits.iter().flatten() {
            if let WalDelta::CreateInode {
                inode_id,
                inode_kind,
                ..
            } = delta
            {
                questions.inodes.push(*inode_id);
                if *inode_kind == InodeKind::Directory {
                    questions.directories.push(*inode_id);
                }
            }
        }
        questions
    }
}

fn content_ref(seed: &str) -> ContentRef {
    loonfs_test_support::ids::content_ref(seed.as_bytes())
}

fn position(seq: u64, delta_index: u32) -> DeltaPosition {
    DeltaPosition {
        seq: ChangeSeq(seq),
        delta_index,
    }
}

fn name_key(naming: NamespaceNaming, display_name: &str) -> NameKey {
    NameKey::parse(loonfs_types::name_key_for_display_name(
        naming,
        display_name,
    ))
    .expect("derived name key")
}

fn create_directory(
    naming: NamespaceNaming,
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
        bind(
            naming,
            delta_index.saturating_add(1),
            inode_id,
            InodeKind::Directory,
            parent_inode_id,
            display_name,
        ),
    ]
}

fn create_file(
    naming: NamespaceNaming,
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
        bind(
            naming,
            delta_index.saturating_add(1),
            inode_id,
            InodeKind::File,
            parent_inode_id,
            display_name,
        ),
        WalDelta::AppendFileRevision {
            delta_index: delta_index.saturating_add(2),
            inode_id,
            revision_no: RevisionNo(1),
            content_ref,
            hash_state: None,
            crc64nvme: None,
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
        hash_state: None,
        crc64nvme: None,
    }]
}

fn bind(
    naming: NamespaceNaming,
    delta_index: u32,
    inode_id: InodeId,
    kind: InodeKind,
    parent_inode_id: InodeId,
    display_name: &str,
) -> WalDelta {
    WalDelta::BindDirentry {
        delta_index,
        parent_inode_id,
        name_key: name_key(naming, display_name),
        display_name: DisplayName::parse(display_name).expect("valid display name"),
        child_inode_id: inode_id,
        child_kind: kind,
        child_created_by: ActorId::loonfs(),
        child_created_at_ms: 4_200,
    }
}

/// Retires the bind recorded at `bound_at`, the first delta of a rename or a
/// delete.
fn unbind(
    naming: NamespaceNaming,
    delta_index: u32,
    inode_id: InodeId,
    kind: InodeKind,
    parent_inode_id: InodeId,
    display_name: &str,
    bound_at: DeltaPosition,
) -> WalDelta {
    WalDelta::UnbindDirentry {
        delta_index,
        parent_inode_id,
        name_key: name_key(naming, display_name),
        display_name: DisplayName::parse(display_name).expect("valid display name"),
        child_inode_id: inode_id,
        child_kind: kind,
        child_created_by: ActorId::loonfs(),
        child_created_at_ms: 4_200,
        target: bound_at,
    }
}

/// A delete by path as the commit path materializes it: retire the binding,
/// then record a tombstone that keeps the binding it removed.
fn delete(
    naming: NamespaceNaming,
    delta_index: u32,
    inode_id: InodeId,
    kind: InodeKind,
    parent_inode_id: InodeId,
    display_name: &str,
    bound_at: DeltaPosition,
) -> Vec<WalDelta> {
    vec![
        unbind(
            naming,
            delta_index,
            inode_id,
            kind,
            parent_inode_id,
            display_name,
            bound_at,
        ),
        WalDelta::TombstoneSubtree {
            delta_index: delta_index.saturating_add(1),
            root_inode_id: inode_id,
            deleted_binding: DeletedBinding {
                parent_inode_id,
                name_key: name_key(naming, display_name),
                display_name: DisplayName::parse(display_name).expect("valid display name"),
            },
        },
    ]
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
        attributes_revision_no: AttributesRevisionNo(revision),
        attributes,
    }]
}

fn update_access(
    delta_index: u32,
    inode_id: InodeId,
    revision: u64,
    boundary: bool,
    grants: AccessGrants,
) -> Vec<WalDelta> {
    vec![WalDelta::AppendAccessRevision {
        delta_index,
        inode_id,
        access_revision_no: AccessRevisionNo(revision),
        boundary,
        grants,
    }]
}

/// Undelete as the commit path materializes it: revoke the exact deletion
/// position, then re-bind the recovered inode.
fn undelete(
    naming: NamespaceNaming,
    delta_index: u32,
    inode_id: InodeId,
    kind: InodeKind,
    parent_inode_id: InodeId,
    display_name: &str,
    target: DeltaPosition,
) -> Vec<WalDelta> {
    vec![
        WalDelta::RevokeSubtreeTombstone {
            delta_index,
            root_inode_id: inode_id,
            target,
        },
        bind(
            naming,
            delta_index.saturating_add(1),
            inode_id,
            kind,
            parent_inode_id,
            display_name,
        ),
    ]
}

#[test]
fn metadata_apply_matches_model_for_basic_commit_sequence() {
    assert_core_matches_model(
        CaseInsensitive,
        // The last two paths fold to the stored name and look inside a file.
        &[
            "/docs",
            "/docs/readme.txt",
            "/DOCS/README.TXT",
            "/docs/readme.txt/notes",
        ],
        &[
            create_directory(CaseInsensitive, 0, InodeId(2), InodeId(1), "docs"),
            create_file(
                CaseInsensitive,
                0,
                InodeId(3),
                InodeId(2),
                "readme.txt",
                content_ref("content-1"),
            ),
            append_revision(0, InodeId(3), RevisionNo(2), content_ref("content-2")),
        ],
    );
}

#[test]
fn metadata_apply_matches_model_for_rename() {
    assert_core_matches_model(
        CaseInsensitive,
        &["/docs", "/docs/readme.txt", "/README.txt", "/readme.txt"],
        &[
            create_directory(CaseInsensitive, 0, InodeId(2), InodeId(1), "docs"),
            create_file(
                CaseInsensitive,
                0,
                InodeId(3),
                InodeId(2),
                "readme.txt",
                content_ref("content-1"),
            ),
            vec![
                unbind(
                    CaseInsensitive,
                    0,
                    InodeId(3),
                    InodeKind::File,
                    InodeId(2),
                    "readme.txt",
                    position(2, 1),
                ),
                bind(
                    CaseInsensitive,
                    1,
                    InodeId(3),
                    InodeKind::File,
                    InodeId(1),
                    "README.txt",
                ),
            ],
        ],
    );
}

#[test]
fn metadata_apply_matches_model_for_case_sensitive_siblings_and_a_case_only_rename() {
    let file = InodeKind::File;
    let docs = InodeId(2);
    assert_core_matches_model(
        CaseSensitive,
        &[
            "/docs",
            "/DOCS",
            "/docs/Report.txt",
            "/docs/report.txt",
            "/docs/REPORT.txt",
        ],
        &[
            create_directory(CaseSensitive, 0, docs, InodeId(1), "docs"),
            create_file(
                CaseSensitive,
                0,
                InodeId(3),
                docs,
                "Report.txt",
                content_ref("content-1"),
            ),
            create_file(
                CaseSensitive,
                0,
                InodeId(4),
                docs,
                "report.txt",
                content_ref("content-2"),
            ),
            // Changing only the case moves the file to a free slot.
            vec![
                unbind(
                    CaseSensitive,
                    0,
                    InodeId(3),
                    file,
                    docs,
                    "Report.txt",
                    position(2, 1),
                ),
                bind(CaseSensitive, 1, InodeId(3), file, docs, "REPORT.txt"),
            ],
        ],
    );
}

#[test]
fn metadata_apply_matches_model_for_restore_revision() {
    assert_core_matches_model(
        CaseInsensitive,
        &["/docs", "/docs/readme.txt"],
        &[
            create_directory(CaseInsensitive, 0, InodeId(2), InodeId(1), "docs"),
            create_file(
                CaseInsensitive,
                0,
                InodeId(3),
                InodeId(2),
                "readme.txt",
                content_ref("content-1"),
            ),
            append_revision(0, InodeId(3), RevisionNo(2), content_ref("content-2")),
            append_revision(0, InodeId(3), RevisionNo(3), content_ref("content-1")),
        ],
    );
}

#[test]
fn metadata_apply_matches_model_for_restore_revision_of_current_head() {
    assert_core_matches_model(
        CaseInsensitive,
        &["/docs", "/docs/readme.txt"],
        &[
            create_directory(CaseInsensitive, 0, InodeId(2), InodeId(1), "docs"),
            create_file(
                CaseInsensitive,
                0,
                InodeId(3),
                InodeId(2),
                "readme.txt",
                content_ref("content-1"),
            ),
            append_revision(0, InodeId(3), RevisionNo(2), content_ref("content-1")),
        ],
    );
}

// The deleted names below are spelled with capitals on purpose: their
// derived key case-folds, so a tombstone that lost the user-facing spelling
// and fell back to the key would differ here.

#[test]
fn metadata_apply_matches_model_for_delete_file() {
    assert_core_matches_model(
        CaseInsensitive,
        &["/docs", "/docs/Readme.TXT"],
        &[
            create_directory(CaseInsensitive, 0, InodeId(2), InodeId(1), "docs"),
            create_file(
                CaseInsensitive,
                0,
                InodeId(3),
                InodeId(2),
                "Readme.TXT",
                content_ref("content-1"),
            ),
            delete(
                CaseInsensitive,
                0,
                InodeId(3),
                InodeKind::File,
                InodeId(2),
                "Readme.TXT",
                position(2, 1),
            ),
        ],
    );
}

#[test]
fn metadata_apply_matches_model_for_delete_subtree() {
    assert_core_matches_model(
        CaseInsensitive,
        &["/Docs", "/Docs/nested"],
        &[
            create_directory(CaseInsensitive, 0, InodeId(2), InodeId(1), "Docs"),
            create_directory(CaseInsensitive, 0, InodeId(3), InodeId(2), "nested"),
            delete(
                CaseInsensitive,
                0,
                InodeId(2),
                InodeKind::Directory,
                InodeId(1),
                "Docs",
                position(1, 1),
            ),
        ],
    );
}

#[test]
fn metadata_apply_matches_model_for_undelete() {
    assert_core_matches_model(
        CaseInsensitive,
        &["/docs", "/docs/Readme.TXT"],
        &[
            create_directory(CaseInsensitive, 0, InodeId(2), InodeId(1), "docs"),
            create_file(
                CaseInsensitive,
                0,
                InodeId(3),
                InodeId(2),
                "Readme.TXT",
                content_ref("content-1"),
            ),
            delete(
                CaseInsensitive,
                0,
                InodeId(3),
                InodeKind::File,
                InodeId(2),
                "Readme.TXT",
                position(2, 1),
            ),
            // The revoke names the delete's own position — the third commit,
            // second delta — which differs from where the revoke itself lands.
            undelete(
                CaseInsensitive,
                0,
                InodeId(3),
                InodeKind::File,
                InodeId(2),
                "Readme.TXT",
                position(3, 1),
            ),
        ],
    );
}

#[test]
fn metadata_apply_matches_model_for_attribute_writes_and_removals() {
    assert_core_matches_model(
        CaseInsensitive,
        &["/docs", "/docs/readme.txt"],
        &[
            create_directory(CaseInsensitive, 0, InodeId(2), InodeId(1), "docs"),
            create_file(
                CaseInsensitive,
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
        ],
    );
}

#[test]
fn metadata_apply_matches_model_for_access_updates() {
    let first =
        serde_json::from_value(serde_json::json!({"prn_ada": ["read", "write"]})).expect("grants");
    let second = serde_json::from_value(serde_json::json!({"prn_team": ["read", "history"]}))
        .expect("grants");
    assert_core_matches_model(
        CaseInsensitive,
        &["/docs", "/other"],
        &[
            create_directory(CaseInsensitive, 0, InodeId(2), InodeId(1), "docs"),
            create_directory(CaseInsensitive, 0, InodeId(3), InodeId(1), "other"),
            update_access(0, InodeId(2), 1, true, first),
            update_access(0, InodeId(2), 2, false, second),
            update_access(0, InodeId(3), 1, false, AccessGrants::default()),
        ],
    );
}

#[test]
fn metadata_apply_matches_model_for_a_cleared_attribute_map() {
    assert_core_matches_model(
        CaseInsensitive,
        &["/docs"],
        &[
            create_directory(CaseInsensitive, 0, InodeId(2), InodeId(1), "docs"),
            update_attributes(0, InodeId(2), 1, attribute_map(&[("owner", "ada")])),
            update_attributes(0, InodeId(2), 2, Attributes::default()),
        ],
    );
}

#[test]
fn metadata_apply_matches_model_for_copy_attribute_inheritance() {
    assert_core_matches_model(
        CaseInsensitive,
        &["/source.txt", "/copy.txt"],
        &[
            create_file(
                CaseInsensitive,
                0,
                InodeId(2),
                InodeId(1),
                "source.txt",
                content_ref("content-1"),
            ),
            update_attributes(0, InodeId(2), 1, attribute_map(&[("owner", "ada")])),
            {
                let mut deltas = create_file(
                    CaseInsensitive,
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
        ],
    );
}

#[test]
fn metadata_apply_matches_model_for_delete_then_undelete_with_attributes() {
    assert_core_matches_model(
        CaseInsensitive,
        &["/Readme.TXT"],
        &[
            create_file(
                CaseInsensitive,
                0,
                InodeId(2),
                InodeId(1),
                "Readme.TXT",
                content_ref("content-1"),
            ),
            update_attributes(0, InodeId(2), 1, attribute_map(&[("owner", "ada")])),
            delete(
                CaseInsensitive,
                0,
                InodeId(2),
                InodeKind::File,
                InodeId(1),
                "Readme.TXT",
                position(1, 1),
            ),
            undelete(
                CaseInsensitive,
                0,
                InodeId(2),
                InodeKind::File,
                InodeId(1),
                "Readme.TXT",
                position(3, 1),
            ),
        ],
    );
}

#[test]
fn metadata_apply_matches_model_for_slot_reuse_across_renames_delete_and_undelete() {
    let file = InodeKind::File;
    let docs = InodeId(2);
    let model = assert_core_matches_model(
        CaseInsensitive,
        &[
            "/docs",
            "/docs/report.txt",
            "/docs/draft.txt",
            "/docs/final.txt",
            "/docs/FINAL.TXT",
            "/docs/old.txt",
        ],
        &[
            create_directory(CaseInsensitive, 0, docs, InodeId(1), "docs"),
            create_file(
                CaseInsensitive,
                0,
                InodeId(3),
                docs,
                "report.txt",
                content_ref("content-1"),
            ),
            vec![
                unbind(
                    CaseInsensitive,
                    0,
                    InodeId(3),
                    file,
                    docs,
                    "report.txt",
                    position(2, 1),
                ),
                bind(CaseInsensitive, 1, InodeId(3), file, docs, "draft.txt"),
            ],
            vec![
                unbind(
                    CaseInsensitive,
                    0,
                    InodeId(3),
                    file,
                    docs,
                    "draft.txt",
                    position(3, 1),
                ),
                bind(CaseInsensitive, 1, InodeId(3), file, docs, "final.txt"),
            ],
            delete(
                CaseInsensitive,
                0,
                InodeId(3),
                file,
                docs,
                "final.txt",
                position(4, 1),
            ),
            // A new file takes the freed slot under a spelling with the same
            // name key.
            create_file(
                CaseInsensitive,
                0,
                InodeId(4),
                docs,
                "Final.TXT",
                content_ref("content-2"),
            ),
            // The undelete takes the slot the first rename freed.
            undelete(
                CaseInsensitive,
                0,
                InodeId(3),
                file,
                docs,
                "report.txt",
                position(5, 1),
            ),
            // One commit unbinds `final.txt` and binds it again; the later
            // delta wins.
            vec![
                unbind(
                    CaseInsensitive,
                    0,
                    InodeId(4),
                    file,
                    docs,
                    "Final.TXT",
                    position(6, 1),
                ),
                bind(CaseInsensitive, 1, InodeId(4), file, docs, "old.txt"),
                unbind(
                    CaseInsensitive,
                    2,
                    InodeId(3),
                    file,
                    docs,
                    "report.txt",
                    position(7, 1),
                ),
                bind(CaseInsensitive, 3, InodeId(3), file, docs, "final.txt"),
            ],
        ],
    );

    let resolved = |path: &str, seq: u64| {
        let path = AbsolutePath::parse(path).expect("valid path");
        match model
            .resolve_path(CaseInsensitive, &path, ChangeSeq(seq))
            .expect("the model should answer for a state it accepted")
        {
            PathLookup::Found { inode, .. } => Some(inode.inode_id),
            PathLookup::NotFound { .. } | PathLookup::NotADirectory { .. } => None,
        }
    };
    assert_eq!(resolved("/docs/final.txt", 4), Some(InodeId(3)));
    assert_eq!(resolved("/docs/final.txt", 5), None);
    assert_eq!(resolved("/docs/final.txt", 6), Some(InodeId(4)));
    assert_eq!(resolved("/docs/report.txt", 7), Some(InodeId(3)));
    assert_eq!(resolved("/docs/final.txt", 8), Some(InodeId(3)));
    assert_eq!(resolved("/docs/old.txt", 8), Some(InodeId(4)));
}

#[test]
fn metadata_apply_matches_model_for_a_hidden_descendant_addressed_by_inode() {
    let directory = InodeKind::Directory;
    let notes = InodeId(4);
    let model = assert_core_matches_model(
        CaseInsensitive,
        &[
            "/projects",
            "/projects/alpha",
            "/projects/alpha/notes.txt",
            "/archive",
            "/archive/alpha",
            "/archive/alpha/notes.txt",
        ],
        &[
            create_directory(CaseInsensitive, 0, InodeId(2), InodeId(1), "projects"),
            create_directory(CaseInsensitive, 0, InodeId(3), InodeId(2), "alpha"),
            create_file(
                CaseInsensitive,
                0,
                notes,
                InodeId(3),
                "notes.txt",
                content_ref("content-1"),
            ),
            delete(
                CaseInsensitive,
                0,
                InodeId(2),
                directory,
                InodeId(1),
                "projects",
                position(1, 1),
            ),
            create_directory(CaseInsensitive, 0, InodeId(5), InodeId(1), "projects"),
            undelete(
                CaseInsensitive,
                0,
                InodeId(2),
                directory,
                InodeId(1),
                "archive",
                position(4, 1),
            ),
        ],
    );

    // The descendant keeps its binding under the deleted root, so only the
    // tombstone on its ancestor hides it.
    for seq in [ChangeSeq(4), ChangeSeq(5)] {
        let hidden = model
            .visible_inode(notes, seq)
            .expect("the model should answer for a state it accepted");
        assert_eq!(hidden, None, "inode `{notes}` at seq {seq}");
        assert_eq!(
            model
                .parent_binding(notes, seq)
                .map(|binding| binding.parent_inode_id),
            Some(InodeId(3)),
            "parent of inode `{notes}` at seq {seq}"
        );
        let listing = model
            .list_directory(InodeId(3), seq)
            .expect("the model should answer for a state it accepted");
        assert!(listing.is_empty(), "listing of inode `3` at seq {seq}");
    }
    let restored = model
        .visible_inode(notes, ChangeSeq(6))
        .expect("the model should answer for a state it accepted");
    assert_eq!(restored.map(|inode| inode.inode_id), Some(notes));
}

#[test]
fn repeated_content_in_one_commit_emits_one_publication() {
    let content = content_ref("shared content");
    let mut deltas = create_file(
        CaseInsensitive,
        0,
        InodeId(2),
        InodeId(1),
        "one",
        content.clone(),
    );
    deltas.extend(create_file(
        CaseInsensitive,
        3,
        InodeId(3),
        InodeId(1),
        "two",
        content,
    ));
    assert_core_matches_model(CaseInsensitive, &["/one", "/two"], &[deltas]);
}

#[test]
fn appends_publish_one_row_per_reference_with_the_longest_first() {
    let content_id = ContentId::generate();
    let namespace_id = content_ref("unused").owner_namespace_id;
    let appended = |delta_index, revision_no, bytes: &[u8]| {
        let mut hash_state = loonfs_types::Sha256State::new();
        hash_state.update(bytes);
        WalDelta::AppendFileRevision {
            delta_index,
            inode_id: InodeId(2),
            revision_no: RevisionNo(revision_no),
            content_ref: ContentRef::blob_v1(namespace_id.clone(), content_id.clone(), bytes),
            hash_state: Some(hash_state),
            crc64nvme: Some(loonfs_types::Checksum::crc64nvme(bytes)),
        }
    };
    let mut first = create_file(
        CaseInsensitive,
        0,
        InodeId(2),
        InodeId(1),
        "log",
        ContentRef::blob_v1(namespace_id.clone(), content_id.clone(), b"one"),
    );
    first.push(appended(3, 2, b"one two"));
    assert_core_matches_model(
        CaseInsensitive,
        &["/log"],
        &[
            first,
            vec![
                appended(0, 3, b"one two three"),
                appended(1, 4, b"one two three four"),
                appended(2, 5, b"one two three four"),
            ],
        ],
    );
}

/// Plans appends through the commit engine and replays the deltas it
/// published: one commit that puts and appends twice, a restore, an append
/// that starts a new chain from the restored revision, a copy appended to
/// in its own commit, and an append to the original after the copy's
/// append took its chain.
#[tokio::test]
async fn planned_appends_match_the_model() {
    let directory = tempfile::tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let namespace_id = loonfs_types::NamespaceId::parse("appends").expect("namespace");
    let context = crate::common::mutation_context("differential", 1_000);
    crate::common::commit_split_support::bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("namespace");
    let path = |path: &str| AbsolutePath::parse(path).expect("path");
    let append = |target: &str, bytes: &[u8]| FilesystemOperation::AppendFile {
        path: path(target),
        inline_content: bytes.to_vec(),
        expected_inode_id: None,
        expected_revision_no: None,
    };
    let value = InlineContent::new(
        namespace_id.clone(),
        ContentId::generate(),
        bytes::Bytes::from_static(b"one"),
    );
    let requests = [
        vec![
            FilesystemOperation::PutFile {
                path: path("/log"),
                content_ref: Some(value.content_ref().clone()),
                inline_content: None,
                behavior: loonfs_types::DestinationBehavior::NoReplace,
                expected_inode_id: None,
                expected_revision_no: None,
            },
            append("/log", b"two"),
            append("/log", b"three"),
        ],
        vec![FilesystemOperation::RestoreRevision {
            path: path("/log"),
            source_revision_no: RevisionNo(1),
        }],
        vec![append("/log", b"four")],
        vec![FilesystemOperation::CopyPath {
            source_path: path("/log"),
            destination_path: path("/copy"),
            precondition: loonfs_types::DestinationPrecondition::default(),
        }],
        vec![append("/copy", b"five")],
        vec![append("/log", b"six")],
    ];
    let mut engine = NamespaceCommitEngine::new(namespace_id.clone());
    for (index, operations) in requests.into_iter().enumerate() {
        let request = CommitRequest {
            commit_id: CommitId::generate(),
            actor_id: loonfs_test_support::test_actor(),
            subject: None,
            message: None,
            preconditions: Vec::new(),
            operations,
        };
        let inline_content = if index == 0 {
            vec![value.clone()]
        } else {
            Vec::new()
        };
        engine
            .publish_batch(
                &store,
                [CommitCandidate::with_inline_content(
                    request,
                    Vec::new(),
                    inline_content,
                )],
                &context,
                &Deadline::start(Arc::new(StdMonotonicTimer::default())),
            )
            .await
            .results
            .pop()
            .expect("one result")
            .expect("commit");
    }

    let mut commits = Vec::new();
    for key in store
        .list_prefix(&wal_prefix(&namespace_id))
        .await
        .expect("list WAL")
    {
        let bytes = store.get(&key, None).await.expect("get").expect("WAL");
        let wal = decode_wal_object_envelope_zstd(&bytes).expect("decode WAL");
        commits.extend(wal.payload().records.iter().map(|record| {
            record
                .deltas
                .iter()
                .map(|delta| delta.delta.clone())
                .collect::<Vec<_>>()
        }));
    }
    assert_eq!(commits.len(), 6);
    let model = assert_core_matches_model(CaseInsensitive, &["/log", "/copy"], &commits);
    let chains: BTreeSet<_> = model
        .content_publications
        .iter()
        .map(|row| &row.content_id)
        .collect();
    assert_eq!(chains.len(), 3);
}

fn core_bootstrap_state() -> CoreMetadataState {
    CoreMetadataState::default().apply_committed_wal_deltas(
        ChangeSeq(0),
        &loonfs_types::format::control::genesis_commit_id(),
        &ActorId::loonfs(),
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

/// Replays `commits` through core and the model, compares their visibility
/// answers and their rows, and returns the model's final state.
fn assert_core_matches_model(
    naming: NamespaceNaming,
    paths: &[&str],
    commits: &[Vec<WalDelta>],
) -> ModelMetadataState {
    let questions = Questions::new(paths, commits);
    let mut core_state = core_bootstrap_state();
    let mut model_state = model_bootstrap_state();

    for (index, deltas) in commits.iter().enumerate() {
        let seq = ChangeSeq(u64::try_from(index + 1).expect("seq"));
        let actor = ActorId::parse(format!("scenario-actor-{index}")).expect("valid actor id");
        let committed_at_ms = 4_200 + u64::try_from(index).expect("timestamp offset");
        let commit_id =
            CommitId::parse(format!("c_differential_{index}")).expect("valid commit id");
        core_state =
            core_state.apply_committed_wal_deltas(seq, &commit_id, &actor, committed_at_ms, deltas);
        model_state = model_state
            .apply_committed_wal_deltas(seq, &commit_id, &actor, committed_at_ms, deltas)
            .expect("the scenario should keep the model's binding rules");
        assert_answers_match(naming, &core_state, &model_state, &questions, seq);
    }
    for seq in 0..u64::try_from(commits.len()).expect("seq") {
        assert_answers_match(
            naming,
            &core_state,
            &model_state,
            &questions,
            ChangeSeq(seq),
        );
    }

    let published: BTreeSet<_> = core_state
        .revisions()
        .iter()
        .map(|row| {
            (
                &row.content_ref.content_id,
                row.content_ref.size_bytes,
                row.committed_seq,
            )
        })
        .collect();
    assert_eq!(core_state.content_publications().len(), published.len());
    assert_eq!(normalize_core(&core_state), normalize_model(&model_state));
    let mut core_publications: Vec<_> = core_state
        .content_publications()
        .iter()
        .map(|row| {
            (
                MetadataRow::ContentPublication(row.clone()).row_key(),
                (
                    &row.content_id,
                    row.committed_seq,
                    row.delta_index,
                    row.size_bytes,
                    &row.hash_state,
                    &row.crc64nvme,
                ),
            )
        })
        .collect();
    core_publications.sort_by(|left, right| left.0.cmp(&right.0));
    assert_eq!(
        core_publications
            .into_iter()
            .map(|(_, row)| row)
            .collect::<Vec<_>>(),
        model_state
            .content_publications_in_key_order()
            .into_iter()
            .map(|row| (
                &row.content_id,
                row.committed_seq,
                row.delta_index,
                row.size_bytes,
                &row.hash_state,
                &row.crc64nvme,
            ))
            .collect::<Vec<_>>()
    );
    for (content_id, _, _) in &published {
        assert_eq!(
            core_state
                .content_head(content_id)
                .map(|head| (head.size_bytes, head.committed_seq)),
            model_state
                .content_head(content_id)
                .map(|head| (head.size_bytes, head.committed_seq))
        );
    }
    model_state
}

/// Asks core and the model every question at `seq` and requires the same
/// answers.
fn assert_answers_match(
    naming: NamespaceNaming,
    core: &CoreMetadataState,
    model: &ModelMetadataState,
    questions: &Questions,
    seq: ChangeSeq,
) {
    const ACCEPTED: &str = "the model should answer for a state it accepted";
    for &inode_id in &questions.inodes {
        assert_eq!(
            core.visible_inode(inode_id, seq).as_ref().map(core_inode),
            model
                .visible_inode(inode_id, seq)
                .expect(ACCEPTED)
                .map(model_inode),
            "visible inode `{inode_id}` at seq {seq}"
        );
        assert_eq!(
            core.current_parent_binding_for_child(inode_id, seq)
                .as_ref()
                .map(core_binding),
            model.parent_binding(inode_id, seq).map(model_binding),
            "parent binding of inode `{inode_id}` at seq {seq}"
        );
    }
    for path in &questions.paths {
        assert_eq!(
            core_path_answer(core.resolve_visible_path(naming, path, seq)),
            model_path_answer(model.resolve_path(naming, path, seq).expect(ACCEPTED)),
            "path `{path}` at seq {seq}"
        );
    }
    for &directory in &questions.directories {
        assert_eq!(
            core_listing(core, directory, seq),
            model_listing(model, directory, seq),
            "listing of inode `{directory}` at seq {seq}"
        );
        for &new_parent in &questions.directories {
            assert_eq!(
                core.would_create_directory_cycle(directory, new_parent, seq),
                model
                    .would_create_cycle(directory, new_parent, seq)
                    .expect(ACCEPTED),
                "moving inode `{directory}` under inode `{new_parent}` at seq {seq}"
            );
        }
    }
}

/// Core's in-memory state has no listing call, so its listing is the visible
/// child of every name ever bound under the parent, in name key order.
fn core_listing(
    core: &CoreMetadataState,
    parent_inode_id: InodeId,
    seq: ChangeSeq,
) -> Vec<ListingEntry> {
    let name_keys: BTreeSet<&NameKey> = core
        .direntry_binds()
        .iter()
        .filter(|row| row.parent_inode_id == parent_inode_id)
        .map(|row| &row.name_key)
        .collect();
    name_keys
        .into_iter()
        .filter_map(|name_key| core.visible_child(parent_inode_id, name_key, seq))
        .map(|binding| ListingEntry {
            binding: core_binding(&binding),
            child_kind: binding.child_kind,
        })
        .collect()
}

fn model_listing(
    model: &ModelMetadataState,
    parent_inode_id: InodeId,
    seq: ChangeSeq,
) -> Vec<ListingEntry> {
    model
        .list_directory(parent_inode_id, seq)
        .expect("the model should answer for a state it accepted")
        .into_iter()
        .map(|entry| ListingEntry {
            binding: model_binding(entry.binding),
            child_kind: entry.child.inode_kind,
        })
        .collect()
}

fn core_path_answer(answer: Result<ResolvedVisiblePath, VisiblePathError>) -> PathAnswer {
    match answer {
        Ok(resolved) => PathAnswer::Found {
            absolute_path: resolved.absolute_path.to_string(),
            inode_id: resolved.inode_id,
            inode_kind: resolved.inode_kind,
            created_by: resolved.created_by,
            created_at_ms: resolved.created_at_ms,
            parent_inode_id: resolved.parent_inode_id,
            display_name: resolved.display_name,
            binding_version: resolved.binding_version,
        },
        Err(VisiblePathError::PathNotFound { absolute_path }) => {
            PathAnswer::NotFound { absolute_path }
        }
        Err(VisiblePathError::PathComponentNotDirectory {
            absolute_path,
            inode_id,
            inode_kind,
        }) => PathAnswer::NotADirectory {
            absolute_path,
            inode_id,
            inode_kind,
        },
        Err(VisiblePathError::RootMissing) => PathAnswer::RootMissing,
    }
}

fn model_path_answer(lookup: PathLookup<'_>) -> PathAnswer {
    match lookup {
        PathLookup::Found {
            absolute_path,
            inode,
            binding,
        } => {
            let binding = binding.map(model_binding);
            PathAnswer::Found {
                absolute_path,
                inode_id: inode.inode_id,
                inode_kind: inode.inode_kind,
                created_by: inode.committed_by.clone(),
                created_at_ms: inode.committed_at_ms,
                parent_inode_id: binding.as_ref().map(|binding| binding.parent_inode_id),
                display_name: binding
                    .as_ref()
                    .and_then(|binding| binding.display_name.as_ref())
                    .map(ToString::to_string)
                    .unwrap_or_default(),
                binding_version: binding.map(|binding| binding.position),
            }
        }
        PathLookup::NotFound { absolute_path } => PathAnswer::NotFound { absolute_path },
        PathLookup::NotADirectory {
            absolute_path,
            inode,
        } => PathAnswer::NotADirectory {
            absolute_path,
            inode_id: inode.inode_id,
            inode_kind: inode.inode_kind,
        },
    }
}

fn core_inode(inode: &CoreInodeRecord) -> NormalizedInode {
    normalize_inode(
        inode.inode_id.0,
        inode.inode_kind,
        inode.committed_seq.0,
        inode.commit_id.clone(),
        inode.committed_by.clone(),
        inode.committed_at_ms,
    )
}

fn model_inode(inode: &ModelInodeRecord) -> NormalizedInode {
    normalize_inode(
        inode.inode_id.0,
        inode.inode_kind,
        inode.committed_seq.0,
        inode.commit_id.clone(),
        inode.committed_by.clone(),
        inode.committed_at_ms,
    )
}

fn core_binding(direntry: &CoreBindingRecord) -> NormalizedDirectoryBinding {
    NormalizedDirectoryBinding {
        parent_inode_id: direntry.parent_inode_id,
        name_key: direntry.name_key.clone(),
        child_inode_id: direntry.child_inode_id,
        position: direntry.position(),
        display_name: direntry.display_name().cloned(),
    }
}

fn model_binding(direntry: &ModelBindingRecord) -> NormalizedDirectoryBinding {
    NormalizedDirectoryBinding {
        parent_inode_id: direntry.parent_inode_id,
        name_key: direntry.name_key.clone(),
        child_inode_id: direntry.child_inode_id,
        position: DeltaPosition {
            seq: direntry.committed_seq,
            delta_index: direntry.delta_index,
        },
        display_name: match &direntry.state {
            ModelBindingState::Bound { display_name } => Some(display_name.clone()),
            ModelBindingState::Unbound => None,
        },
    }
}

fn normalize_core(state: &CoreMetadataState) -> NormalizedMetadata {
    (
        state.inodes().iter().map(core_inode).collect(),
        state.direntry_binds().iter().map(core_binding).collect(),
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
                    revision.delta_index,
                    revision.content_ref.content_id.clone(),
                )
            })
            .collect(),
        state
            .subtree_tombstones()
            .iter()
            .map(|tombstone| NormalizedTombstone {
                root_inode_id: tombstone.root_inode_id.0,
                tombstone_seq: tombstone.committed_seq.0,
                tombstone_delta_index: tombstone.delta_index,
                commit_id: tombstone.commit_id.clone(),
                committed_at_ms: tombstone.committed_at_ms,
                committed_by: tombstone.committed_by.clone(),
                action: match &tombstone.action {
                    CoreTombstoneAction::Set { deleted_binding } => {
                        NormalizedTombstoneAction::Set {
                            deleted_binding: NormalizedBinding {
                                parent_inode_id: deleted_binding.parent_inode_id.0,
                                name_key: deleted_binding.name_key.as_str().to_owned(),
                                display_name: deleted_binding.display_name.as_str().to_owned(),
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
                committed_by: record.committed_by.clone(),
                committed_at_ms: record.committed_at_ms,
                entries: record
                    .attributes
                    .iter()
                    .map(|(key, value)| (key.as_str().to_owned(), value.as_str().to_owned()))
                    .collect(),
            })
            .collect(),
        state
            .access_revisions()
            .iter()
            .map(|record| NormalizedAccessRevision {
                inode_id: record.inode_id.0,
                revision: record.access_revision_no.0,
                committed_seq: record.committed_seq.0,
                commit_id: record.commit_id.clone(),
                delta_index: record.delta_index,
                committed_by: record.committed_by.clone(),
                committed_at_ms: record.committed_at_ms,
                boundary: record.boundary,
                grants: record
                    .grants
                    .iter()
                    .map(|(principal, rights)| {
                        (
                            principal.as_str().to_owned(),
                            rights
                                .iter()
                                .map(|right| right.as_str().to_owned())
                                .collect(),
                        )
                    })
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
        inodes.iter().map(model_inode).collect(),
        direntry_binds.iter().map(model_binding).collect(),
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
                tombstone_seq: tombstone.committed_seq.0,
                tombstone_delta_index: tombstone.delta_index,
                commit_id: tombstone.commit_id.clone(),
                committed_at_ms: tombstone.committed_at_ms,
                committed_by: tombstone.committed_by.clone(),
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
                committed_by: record.committed_by.clone(),
                committed_at_ms: record.committed_at_ms,
                entries: record
                    .entries
                    .iter()
                    .map(|entry| (entry.key.clone(), entry.value.clone()))
                    .collect(),
            })
            .collect(),
        state
            .access_revisions
            .iter()
            .map(|record| NormalizedAccessRevision {
                inode_id: record.inode_id.0,
                revision: record.revision_no,
                committed_seq: record.committed_seq.0,
                commit_id: record.commit_id.clone(),
                delta_index: record.delta_index,
                committed_by: record.committed_by.clone(),
                committed_at_ms: record.committed_at_ms,
                boundary: record.boundary,
                grants: record
                    .grants
                    .iter()
                    .map(|entry| (entry.principal_id.clone(), entry.rights.clone()))
                    .collect(),
            })
            .collect(),
    )
}

fn normalize_inode(
    inode_id: u64,
    inode_kind: InodeKind,
    committed_seq: u64,
    commit_id: CommitId,
    committed_by: ActorId,
    committed_at_ms: u64,
) -> NormalizedInode {
    (
        inode_id,
        match inode_kind {
            InodeKind::Directory => "dir",
            InodeKind::File => "file",
        },
        committed_seq,
        commit_id,
        committed_by,
        committed_at_ms,
    )
}
