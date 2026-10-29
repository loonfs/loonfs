//! A base compaction removes the rows of the inodes that an expired deletion
//! hides, and keeps every row that a view at a retained sequence reads.

use super::active_deletions::{expire, inode_id_of};
use super::*;
use crate::manifest::kept_inodes::{load_kept_inode_filter, KeptInodeFilter};
use crate::namespace::control::LoadedManifest;
use crate::storage::content_admission::PreparedContent;
use compaction_step::MergePlacement;
use loonfs_types::{
    AccessGrants, AccessRight, AccessRights, AttributeKey, AttributeValue, DeleteDirectoryBehavior,
    InodeKind, NamespaceAccess, PrincipalId, PrincipalScope, PrincipalSet, Subject, SubjectId,
    ROOT_INODE_ID,
};
use std::assert_matches;

const PRINCIPAL_SCOPE: &str = "org_demo";

/// A subject with every right, so the namespace's commits can update access
/// rows.
fn administrator() -> Subject {
    Subject {
        principal_scope: PrincipalScope::parse(PRINCIPAL_SCOPE).expect("principal scope"),
        subject_id: SubjectId::parse("root").expect("subject id"),
        principals: PrincipalSet::new(BTreeSet::from([
            PrincipalId::parse("prn_root").expect("principal id")
        ]))
        .expect("principals"),
    }
}

/// Creates a namespace with access control, whose access group has rows.
async fn bootstrap_restricted_namespace<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) {
    crate::namespace::bootstrap::bootstrap_namespace(
        store,
        namespace_id,
        context,
        &loonfs_test_support::test_actor(),
        &crate::options::CreateNamespaceOptions {
            access: NamespaceAccess::Acl {
                principal_scope: PrincipalScope::parse(PRINCIPAL_SCOPE).expect("principal scope"),
                root_grants: AccessGrants::new(BTreeMap::from([(
                    PrincipalId::parse("prn_root").expect("principal id"),
                    AccessRights::ADMIN,
                )]))
                .expect("root grants"),
            },
            ..Default::default()
        },
        crate::test_support::std_timer(),
    )
    .await
    .expect("bootstrap a restricted namespace");
    acquire_writer_epoch(store, namespace_id, context)
        .await
        .expect("acquire the writer");
    fold::fold_wal(store, namespace_id)
        .await
        .expect("publish the first manifest");
}

/// Commits one operation as the administrator.
async fn commit<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    operation: FilesystemOperation,
    prepared: Vec<PreparedContent>,
    context: &MutationContext,
) -> crate::error::Result<loonfs_types::Commit> {
    let intent = CommitIntent::single(
        CommitId::generate(),
        loonfs_test_support::test_actor(),
        None,
        operation,
    )
    .with_subject(administrator());
    let candidate = if prepared.is_empty() {
        CommitCandidate::new(intent)
    } else {
        CommitCandidate::prepared(intent, prepared)
    };
    NamespaceCommitEngine::new(
        namespace_id.clone(),
        Arc::new(crate::test_support::content_writes()),
    )
    .publish_batch(
        store,
        vec![candidate],
        context,
        &Deadline::start(Arc::new(StdMonotonicTimer::default())),
    )
    .await
    .results
    .pop()
    .expect("one result")
}

/// Writes `bytes` at `path` as the administrator, replacing any file there.
async fn put<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    path: &str,
    bytes: &[u8],
    context: &MutationContext,
) {
    let stored = store_bytes_as_content(store, namespace_id, bytes)
        .await
        .expect("store content");
    let content_ref = stored.content_ref().clone();
    let catalog = load_namespace_catalog_entry(store, namespace_id)
        .await
        .expect("load namespace catalog");
    let operation = FilesystemOperation::PutFile {
        path: AbsolutePath::parse(path).expect("path"),
        content_ref: Some(content_ref),
        inline_content: None,
        behavior: DestinationBehavior::Replace,
        expected_inode_id: None,
        expected_revision_no: None,
    };
    let prepared = vec![prepare_stored_content(&catalog, stored)];
    commit(store, namespace_id, operation, prepared, context)
        .await
        .expect("write the file");
}

async fn set_attribute<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    path: &str,
    context: &MutationContext,
) {
    let operation = FilesystemOperation::UpdateAttributes {
        path: AbsolutePath::parse(path).expect("path"),
        set: BTreeMap::from([(
            AttributeKey::parse("owner").expect("attribute key"),
            AttributeValue::parse("ada").expect("attribute value"),
        )]),
        remove: Vec::new(),
        expected_inode_id: None,
        expected_attributes_revision_no: None,
    };
    commit(store, namespace_id, operation, Vec::new(), context)
        .await
        .expect("set an attribute");
}

/// Grants `principal` `read` and `history` on `path`, which a read of a
/// deleted file's revision needs, and sets the path's boundary flag to
/// `boundary`.
async fn grant_read<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    path: &str,
    principal: &str,
    boundary: bool,
    context: &MutationContext,
) {
    let operation = FilesystemOperation::UpdateAccess {
        path: AbsolutePath::parse(path).expect("path"),
        boundary,
        grants: AccessGrants::new(BTreeMap::from([(
            PrincipalId::parse(principal).expect("principal id"),
            AccessRights::from_iter([AccessRight::Read, AccessRight::History]),
        )]))
        .expect("grants"),
        expected_inode_id: None,
        expected_access_revision_no: None,
    };
    commit(store, namespace_id, operation, Vec::new(), context)
        .await
        .expect("grant read access");
}

async fn delete_recursively<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    path: &str,
    context: &MutationContext,
) -> ChangeSeq {
    let operation = FilesystemOperation::DeletePath {
        path: AbsolutePath::parse(path).expect("path"),
        behavior: DeleteDirectoryBehavior::Recursive,
        expected_inode_id: None,
    };
    commit(store, namespace_id, operation, Vec::new(), context)
        .await
        .expect("delete the path")
        .committed_seq
}

/// A namespace in which the expired deletion of `/docs` hides four inodes
/// with revisions, attributes, and access rows, the deletion of `/trash` is
/// recoverable, and `/keep` stays visible.
struct ExpiredDocs {
    docs: InodeId,
    /// `/docs`, `/docs/notes.txt`, `/docs/sub`, and `/docs/sub/deep.txt`.
    hidden: Vec<InodeId>,
    /// `/trash` and `/trash/old.txt`.
    recoverable: Vec<InodeId>,
    /// The root, `/keep`, and `/keep/kept.txt`.
    visible: Vec<InodeId>,
}

impl ExpiredDocs {
    fn every_inode(&self) -> Vec<InodeId> {
        [&self.hidden, &self.recoverable, &self.visible]
            .into_iter()
            .flatten()
            .copied()
            .collect()
    }
}

async fn expire_docs_beside_recoverable_trash<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) -> ExpiredDocs {
    bootstrap_restricted_namespace(store, namespace_id, context).await;
    put(store, namespace_id, "/docs/notes.txt", b"one", context).await;
    put(store, namespace_id, "/docs/notes.txt", b"two", context).await;
    put(store, namespace_id, "/docs/sub/deep.txt", b"deep", context).await;
    put(store, namespace_id, "/keep/kept.txt", b"kept", context).await;
    put(store, namespace_id, "/trash/old.txt", b"old", context).await;
    for path in ["/docs/notes.txt", "/keep/kept.txt", "/trash/old.txt"] {
        set_attribute(store, namespace_id, path, context).await;
    }
    for path in ["/docs", "/docs/sub", "/keep", "/trash"] {
        grant_read(store, namespace_id, path, "prn_team", true, context).await;
    }
    let mut ids = BTreeMap::new();
    for path in [
        "/docs",
        "/docs/notes.txt",
        "/docs/sub",
        "/docs/sub/deep.txt",
        "/trash",
        "/trash/old.txt",
        "/keep",
        "/keep/kept.txt",
    ] {
        ids.insert(path, inode_id_of(store, namespace_id, path).await);
    }
    let deletion_seq = delete_recursively(store, namespace_id, "/docs", context).await;
    delete_recursively(store, namespace_id, "/trash", context).await;
    fold::fold_wal(store, namespace_id)
        .await
        .expect("fold the deletions");
    // The floor stops at the deletion, so the views that the compaction has
    // to preserve include the ones before `/trash` was deleted.
    advance_retention_floor(
        store,
        None,
        namespace_id,
        RetentionTarget::Seq(deletion_seq),
        crate::test_support::grace_elapsed_after(context.now_ms),
        crate::test_support::std_timer(),
    )
    .await
    .expect("move the floor to the deletion");
    expire(store, namespace_id, ids["/docs"], deletion_seq, context)
        .await
        .expect("expire the deletion of /docs");
    fold::fold_wal(store, namespace_id)
        .await
        .expect("fold the expiry");
    ExpiredDocs {
        docs: ids["/docs"],
        hidden: [
            "/docs",
            "/docs/notes.txt",
            "/docs/sub",
            "/docs/sub/deep.txt",
        ]
        .map(|path| ids[path])
        .to_vec(),
        recoverable: ["/trash", "/trash/old.txt"].map(|path| ids[path]).to_vec(),
        visible: vec![ROOT_INODE_ID, ids["/keep"], ids["/keep/kept.txt"]],
    }
}

/// Every row of `family` in the current manifest.
async fn family_rows<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    family: ApiMetadataRowFamily,
) -> Vec<MetadataRow> {
    let current = load_current_manifest(store, namespace_id)
        .await
        .expect("load the current manifest");
    crate::manifest::metadata_basis_from_manifest(store, None, &current)
        .segments
        .scan_prefix(family, "")
        .await
        .expect("scan the family")
}

/// The inodes that a row stores state for, or the edge it records.
fn row_inodes(row: &MetadataRow) -> Vec<InodeId> {
    match row {
        MetadataRow::Inode(record) => vec![record.inode_id],
        MetadataRow::FileRevision(record) => vec![record.inode_id],
        MetadataRow::AttributesRevision(record) => vec![record.inode_id],
        MetadataRow::AccessRevision(record) => vec![record.inode_id],
        MetadataRow::DirentryBinding(record) => {
            vec![record.parent_inode_id, record.child_inode_id]
        }
        MetadataRow::Tombstone(record) => vec![record.root_inode_id],
        MetadataRow::ActiveDeletion(record) => vec![record.root_inode_id],
        MetadataRow::CommitReceipt(_) | MetadataRow::Commit(_) | MetadataRow::ContentLayout(_) => {
            Vec::new()
        }
    }
}

/// The rows of each family that refer to one of `inode_ids`.
async fn rows_of<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    inode_ids: &[InodeId],
) -> BTreeMap<ApiMetadataRowFamily, Vec<MetadataRow>> {
    let mut rows = BTreeMap::new();
    for family in MANIFEST_ROW_FAMILIES {
        rows.insert(
            family,
            family_rows(store, namespace_id, family)
                .await
                .into_iter()
                .filter(|row| {
                    row_inodes(row)
                        .iter()
                        .any(|inode_id| inode_ids.contains(inode_id))
                })
                .collect(),
        );
    }
    rows
}

/// The families that store rows by inode, and the bindings.
const INODE_FAMILIES: [ApiMetadataRowFamily; 6] = [
    ApiMetadataRowFamily::Inodes,
    ApiMetadataRowFamily::Revisions,
    ApiMetadataRowFamily::Attributes,
    ApiMetadataRowFamily::Access,
    ApiMetadataRowFamily::DirentryBinds,
    ApiMetadataRowFamily::DirentryChildBinds,
];

/// One inode's state at `seq`: its inode row, and its newest revision,
/// attribute row, and access row.
fn inode_state(state: &MetadataState, inode_id: InodeId, seq: ChangeSeq) -> String {
    format!(
        "{:?} {:?} {:?} {:?}",
        state.inode_at_seq(inode_id, seq),
        state
            .revisions()
            .iter()
            .filter(|row| row.inode_id == inode_id && row.committed_seq <= seq)
            .max_by_key(|row| row.revision_no),
        state
            .attributes_revisions()
            .iter()
            .filter(|row| row.inode_id == inode_id && row.committed_seq <= seq)
            .max_by_key(|row| row.attributes_revision_no),
        state
            .access_revisions()
            .iter()
            .filter(|row| row.inode_id == inode_id && row.committed_seq <= seq)
            .max_by_key(|row| row.access_revision_no),
    )
}

/// What a view at `seq` shows: every visible path with its inode's state,
/// and whether each of `inode_ids` is visible. The questions are the
/// differential suite's, asked of one namespace before and after a
/// compaction instead of core and the model.
fn visible_metadata(state: &MetadataState, inode_ids: &[InodeId], seq: ChangeSeq) -> Vec<String> {
    let mut lines = Vec::new();
    let mut directories = vec![(AbsolutePath::root(), ROOT_INODE_ID)];
    while let Some((path, directory)) = directories.pop() {
        lines.push(format!("{path} {}", inode_state(state, directory, seq)));
        let name_keys = state
            .direntry_binds()
            .iter()
            .filter(|row| row.parent_inode_id == directory)
            .map(|row| &row.name_key)
            .collect::<BTreeSet<_>>();
        for name_key in name_keys {
            let Some(binding) = state.visible_child(directory, name_key, seq) else {
                continue;
            };
            let child = path.join(binding.display_name().expect("a visible child is bound"));
            match binding.child_kind {
                InodeKind::Directory => directories.push((child, binding.child_inode_id)),
                InodeKind::File => lines.push(format!(
                    "{child} {}",
                    inode_state(state, binding.child_inode_id, seq)
                )),
            }
        }
    }
    for inode_id in inode_ids {
        lines.push(format!(
            "inode {inode_id} {:?}",
            state.visible_inode(*inode_id, seq)
        ));
    }
    lines.sort();
    lines
}

/// The visible metadata at every retained sequence, from the floor to the
/// head.
async fn visible_metadata_at_retained_sequences<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    inode_ids: &[InodeId],
) -> Vec<Vec<String>> {
    let floor_seq = read_floor_seq(store, namespace_id).await;
    let (head, state) = load_manifest_projection_metadata_state(store, namespace_id)
        .await
        .expect("load the namespace's rows");
    (floor_seq.0..=head.seq.0)
        .map(|seq| visible_metadata(&state, inode_ids, ChangeSeq(seq)))
        .collect()
}

fn every_group_compacts() -> MetadataRunPolicy {
    MetadataRunPolicy {
        max_delta_runs: NonZeroUsize::MIN,
        ..MetadataRunPolicy::default()
    }
}

/// Checks that once a deletion expires, a base compaction of each group
/// removes every row of the inodes it hides, with every binding under them
/// and the trash entry, keeps the tombstone events, and leaves the visible
/// metadata at every retained sequence as it was.
#[tokio::test]
async fn a_base_compaction_of_each_group_removes_every_row_an_expired_deletion_hides() {
    let temp = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp.path()).expect("store");
    let namespace_id = NamespaceId::parse("expired-rows").expect("namespace id");
    let context = mutation_context("writer-1", 5_000);
    let docs = expire_docs_beside_recoverable_trash(&store, &namespace_id, &context).await;
    let before = rows_of(&store, &namespace_id, &docs.hidden).await;
    for family in INODE_FAMILIES {
        assert!(!before[&family].is_empty(), "{family:?} has hidden rows");
    }
    let views_before =
        visible_metadata_at_retained_sequences(&store, &namespace_id, &docs.every_inode()).await;

    let (_, published) = drain_compaction(&store, &namespace_id, every_group_compacts()).await;
    assert!(published >= MetadataFamilyGroup::ALL.len());

    let after = rows_of(&store, &namespace_id, &docs.hidden).await;
    for family in INODE_FAMILIES {
        assert_eq!(after[&family], Vec::new(), "{family:?} keeps no hidden row");
    }
    assert_eq!(
        after[&ApiMetadataRowFamily::ActiveDeletions],
        Vec::new(),
        "the listed and removed pair goes"
    );
    assert_eq!(
        after[&ApiMetadataRowFamily::Tombstones]
            .iter()
            .map(|row| match row {
                MetadataRow::Tombstone(record) =>
                    (record.root_inode_id, record.action.is_recoverable()),
                other => panic!("unexpected tombstone row {other:?}"),
            })
            .collect::<Vec<_>>(),
        vec![(docs.docs, true), (docs.docs, false)],
        "the set and expire events stay"
    );
    assert_eq!(
        visible_metadata_at_retained_sequences(&store, &namespace_id, &docs.every_inode()).await,
        views_before,
        "the visible metadata at every retained sequence is unchanged"
    );
    let undelete = commit(
        &store,
        &namespace_id,
        FilesystemOperation::undelete(
            docs.docs,
            before[&ApiMetadataRowFamily::ActiveDeletions]
                .iter()
                .find_map(|row| match row {
                    MetadataRow::ActiveDeletion(record) => Some(record.deletion_seq),
                    _ => None,
                })
                .expect("the deletion's trash rows"),
            &loonfs_types::options::UndeleteDestination::Recorded,
        ),
        Vec::new(),
        &context,
    )
    .await
    .expect_err("an expired deletion can't be undeleted");
    assert_eq!(undelete.code(), ErrorCode::NotDeleted);
}

/// Checks that the base compactions that remove an expired deletion's rows
/// keep every row of a recoverable deletion and of the visible tree.
#[tokio::test]
async fn a_recoverable_deletion_keeps_every_row_while_an_expired_one_loses_its_rows() {
    let temp = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp.path()).expect("store");
    let namespace_id = NamespaceId::parse("recoverable-rows").expect("namespace id");
    let context = mutation_context("writer-1", 5_000);
    let docs = expire_docs_beside_recoverable_trash(&store, &namespace_id, &context).await;
    let kept = [docs.recoverable.as_slice(), docs.visible.as_slice()].concat();
    let before = rows_of(&store, &namespace_id, &kept).await;

    drain_compaction(&store, &namespace_id, every_group_compacts()).await;

    let after = rows_of(&store, &namespace_id, &kept).await;
    for family in [
        ApiMetadataRowFamily::Inodes,
        ApiMetadataRowFamily::Revisions,
        ApiMetadataRowFamily::Attributes,
        ApiMetadataRowFamily::Access,
        ApiMetadataRowFamily::Tombstones,
    ] {
        assert_eq!(
            after[&family], before[&family],
            "{family:?} keeps every row"
        );
    }
    assert_matches!(
        after[&ApiMetadataRowFamily::ActiveDeletions].as_slice(),
        [MetadataRow::ActiveDeletion(crate::metadata::ActiveDeletionRecord {
            action: loonfs_types::format::manifest::ActiveDeletionRowAction::Listed { .. },
            root_inode_id,
            ..
        })] if *root_inode_id == docs.recoverable[0],
        "the recoverable deletion stays in the trash"
    );
    assert_eq!(
        rows_of(&store, &namespace_id, &docs.hidden).await[&ApiMetadataRowFamily::Inodes],
        Vec::new(),
        "the expired deletion's inodes are gone"
    );
}

/// Reads the first revision of `inode_id` as a subject with `principal`.
async fn read_as<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    principal: &str,
) -> crate::error::Result<Vec<u8>> {
    let principals = PrincipalSet::new(BTreeSet::from([
        PrincipalId::parse(principal).expect("principal id")
    ]))
    .expect("principals");
    load_current_metadata_view(store, namespace_id)
        .await
        .expect("load the current view")
        .read_file_revision_bytes_by_inode(
            store,
            inode_id,
            RevisionNo(1),
            None,
            &crate::authorize::ReadAccess::live(crate::authorize::Authorizer::Subject {
                principals: &principals,
            }),
        )
        .await
}

/// Checks that when a file is deleted on its own under a directory whose
/// deletion then expires, base compactions of every group keep the access
/// boundary between the two, so the deleted file stays readable by the
/// person the boundary grants and not by the team that the directory above
/// it grants.
#[tokio::test]
async fn a_recoverable_deletion_inside_an_expired_one_keeps_the_boundary_above_it() {
    let temp = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp.path()).expect("store");
    let namespace_id = NamespaceId::parse("inner-deletion").expect("namespace id");
    let context = mutation_context("writer-1", 5_000);
    bootstrap_restricted_namespace(&store, &namespace_id, &context).await;
    put(
        &store,
        &namespace_id,
        "/team/hr/salaries.csv",
        b"salaries",
        &context,
    )
    .await;
    grant_read(&store, &namespace_id, "/team", "prn_team", false, &context).await;
    grant_read(&store, &namespace_id, "/team/hr", "prn_ada", true, &context).await;
    let team = inode_id_of(&store, &namespace_id, "/team").await;
    let hr = inode_id_of(&store, &namespace_id, "/team/hr").await;
    let salaries = inode_id_of(&store, &namespace_id, "/team/hr/salaries.csv").await;
    delete_recursively(&store, &namespace_id, "/team/hr/salaries.csv", &context).await;
    let deletion_seq = delete_recursively(&store, &namespace_id, "/team", &context).await;
    fold::fold_wal(&store, &namespace_id)
        .await
        .expect("fold the deletions");
    advance_retention_floor(
        &store,
        None,
        &namespace_id,
        RetentionTarget::Seq(deletion_seq),
        crate::test_support::grace_elapsed_after(context.now_ms),
        crate::test_support::std_timer(),
    )
    .await
    .expect("move the floor to the deletion of /team");
    expire(&store, &namespace_id, team, deletion_seq, &context)
        .await
        .expect("expire the deletion of /team");
    fold::fold_wal(&store, &namespace_id)
        .await
        .expect("fold the expiry");

    drain_compaction(&store, &namespace_id, every_group_compacts()).await;

    assert_matches!(
        rows_of(&store, &namespace_id, &[hr]).await[&ApiMetadataRowFamily::Access].as_slice(),
        [MetadataRow::AccessRevision(row)] if row.boundary,
        "the boundary on /team/hr is still stored"
    );
    assert_eq!(
        read_as(&store, &namespace_id, salaries, "prn_ada")
            .await
            .expect("the person /team/hr grants reads the deleted file"),
        b"salaries"
    );
    let error = read_as(&store, &namespace_id, salaries, "prn_team")
        .await
        .expect_err("the boundary excludes the team");
    assert_eq!(error.code(), ErrorCode::InodeNotFound);
}

/// The runs a delta-only window of `group` merges: every run of the group
/// after its oldest.
fn delta_window(
    manifest: &NamespaceManifestEnvelope,
    group: MetadataFamilyGroup,
) -> Vec<MetadataRunManifest> {
    let mut runs = runs_newest_first(manifest.payload())
        .into_iter()
        .filter(|run| {
            compaction_step::group_run_descriptors(run, group)
                .next()
                .is_some()
        })
        .collect::<Vec<_>>();
    runs.sort_by_key(|run| (run.tier != RunTier::Base, run.run_seq));
    runs.remove(0);
    runs
}

/// A namespace with an open access mode in which the expired deletion of
/// `/old` hides `/old`, `/old/a.txt`, and `/old/b.txt`, and the visible
/// `/keep-{index}.txt` files share a run with `/old/b.txt`. Returns the
/// inodes of `/old` and `/old/a.txt`, which an earlier run created.
async fn expire_old<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) -> (InodeId, InodeId) {
    bootstrap_namespace(store, namespace_id, context)
        .await
        .expect("bootstrap");
    write_test_file(store, namespace_id, "/old/a.txt", "c_old_a", context).await;
    fold::fold_wal(store, namespace_id).await.expect("fold");
    write_test_file(store, namespace_id, "/old/b.txt", "c_old_b", context).await;
    let keep = (0..8)
        .map(|index| format!("/keep-{index}.txt"))
        .collect::<Vec<_>>();
    write_files_bytes(store, namespace_id, &keep, b"keep", context)
        .await
        .expect("write visible files");
    fold::fold_wal(store, namespace_id).await.expect("fold");
    let old = inode_id_of(store, namespace_id, "/old").await;
    let a = inode_id_of(store, namespace_id, "/old/a.txt").await;
    let deletion_seq = delete_path(store, namespace_id, "/old", context, None)
        .await
        .expect("delete /old")
        .committed_seq;
    fold::fold_wal(store, namespace_id).await.expect("fold");
    advance_retention_floor(
        store,
        None,
        namespace_id,
        RetentionTarget::Head,
        crate::test_support::grace_elapsed_after(context.now_ms),
        crate::test_support::std_timer(),
    )
    .await
    .expect("move the floor past the deletion");
    expire(store, namespace_id, old, deletion_seq, context)
        .await
        .expect("expire the deletion");
    fold::fold_wal(store, namespace_id).await.expect("fold");
    (old, a)
}

/// Checks that a compaction whose window skips a group's oldest run keeps
/// every row of an expired deletion's inodes, and a base compaction of the
/// same groups then removes them.
#[tokio::test]
async fn a_delta_only_compaction_keeps_an_expired_deletions_rows() {
    let temp = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp.path()).expect("store");
    let namespace_id = NamespaceId::parse("delta-only").expect("namespace id");
    let context = mutation_context("writer-1", 5_000);
    let (old, a) = expire_old(&store, &namespace_id, &context).await;
    let hidden = [old, a];
    let before = rows_of(&store, &namespace_id, &hidden).await;
    let floor_seq = read_floor_seq(&store, &namespace_id).await;

    for group in [MetadataFamilyGroup::Inodes, MetadataFamilyGroup::Bindings] {
        let current = load_current_manifest(&store, &namespace_id)
            .await
            .expect("load the current manifest");
        let window = delta_window(&current.state.envelope, group);
        assert!(window.len() >= 2, "{group:?} has a delta-only window");
        let spec = MetadataCompactionSpec::new(
            group,
            window.iter().map(|run| run.run_no).collect(),
            window
                .iter()
                .flat_map(|run| compaction_step::group_run_descriptors(run, group))
                .map(|descriptor| descriptor.row_count)
                .sum(),
            MergePlacement::Delta {
                output_seq: window.iter().map(|run| run.run_seq).max().expect("runs"),
            },
            floor_seq,
        );
        let outcome = run_planned_compaction(
            &store,
            &namespace_id,
            &context,
            MetadataRunPolicy::default(),
            &spec,
        )
        .await;
        assert_matches!(outcome, CompactionUnitOutcome::Published { .. });
    }
    assert_eq!(
        rows_of(&store, &namespace_id, &hidden).await,
        before,
        "a delta-only compaction removes no row"
    );

    drain_compaction(&store, &namespace_id, every_group_compacts()).await;
    let after = rows_of(&store, &namespace_id, &hidden).await;
    for family in [
        ApiMetadataRowFamily::Inodes,
        ApiMetadataRowFamily::Revisions,
        ApiMetadataRowFamily::DirentryBinds,
        ApiMetadataRowFamily::DirentryChildBinds,
    ] {
        assert!(!before[&family].is_empty(), "{family:?} had hidden rows");
        assert_eq!(after[&family], Vec::new(), "{family:?} keeps no hidden row");
    }
}

/// The rows of the segments a merge wrote.
async fn merged_rows<S: ObjectStore + ?Sized>(
    store: &S,
    output_segments: &[MetadataSegmentRef],
) -> Vec<MetadataRow> {
    let mut rows = Vec::new();
    for descriptor in output_segments {
        let index =
            index_block_load::load_segment_index_for_compaction(store, None, None, descriptor)
                .await
                .expect("load the output index");
        for entry in index.iter() {
            let block = data_block_load::load_segment_data_block_with_readahead(
                store,
                None,
                None,
                descriptor,
                std::slice::from_ref(entry),
            )
            .await
            .expect("load an output block");
            rows.extend(block.rows.iter().cloned());
        }
    }
    rows
}

/// The object keys of the segments the walk reads: the basis's slot index
/// and `active_deletions`.
fn walked_segment_keys(runs: &[MetadataRunManifest]) -> BTreeSet<String> {
    runs.iter()
        .flat_map(|run| &run.segments)
        .filter(|family| {
            matches!(
                family.family,
                ApiMetadataRowFamily::DirentryBinds | ApiMetadataRowFamily::ActiveDeletions
            )
        })
        .flat_map(|family| &family.segments)
        .map(metadata_segment_object_key)
        .collect()
}

/// Checks that a base compaction removes an expired deletion's rows when its
/// basis has more `inodes` rows than fit in `MAX_BLOOM_FILTER_BYTES` at ten
/// bits each, as it does under that limit, and that the walk reads each small
/// segment once either way.
#[tokio::test]
async fn a_kept_inode_filter_over_its_byte_limit_still_removes_hidden_rows() {
    let temp = tempdir().expect("tempdir");
    let store = RecordingStore::metadata_segments(LocalFsStore::new(temp.path()).expect("store"));
    let namespace_id = NamespaceId::parse("filter-limit").expect("namespace id");
    let context = mutation_context("writer-1", 5_000);
    let (_, a) = expire_old(&store, &namespace_id, &context).await;
    assert_eq!(
        rows_of(&store, &namespace_id, &[a]).await[&ApiMetadataRowFamily::Revisions].len(),
        1,
        "the basis has the hidden revision"
    );
    let current = load_current_manifest(&store, &namespace_id)
        .await
        .expect("load the current manifest");
    let policy = MetadataRunPolicy::default();
    let group = MetadataFamilyGroup::Revisions;
    // 200 million rows take the whole 64 MiB at about 2.7 bits per row.
    for inode_rows in [None, Some(200_000_000)] {
        let mut basis =
            crate::manifest::metadata_basis_from_manifest(&store, None, &current).segments;
        let mut runs = (*basis.scan_runs).clone();
        if let Some(inode_rows) = inode_rows {
            let inodes = runs
                .iter_mut()
                .flat_map(|run| &mut run.segments)
                .find(|family| {
                    family.family == ApiMetadataRowFamily::Inodes && !family.segments.is_empty()
                })
                .expect("an inodes segment");
            inodes.segments[0].row_count = inode_rows;
        }
        basis.scan_runs = Arc::new(runs.clone());
        let spec = compaction_step::plan_group(
            current.state.envelope.payload(),
            &runs,
            group,
            policy,
            MetadataCompactionPolicy::CompactImmediately,
        )
        .expect("plan the group");
        assert_matches!(spec.placement(), MergePlacement::Base { .. });
        store.reset();
        let merged = run_metadata_compaction(
            &basis,
            None,
            &namespace_id,
            &spec,
            policy,
            &MetadataCompactionCancellation::default(),
        )
        .await
        .expect("merge")
        .expect("not cancelled");
        let walked = walked_segment_keys(&runs);
        let walk_reads = store
            .take_gets()
            .iter()
            .filter(|(key, _)| walked.contains(key))
            .count();
        let hidden_rows = merged_rows(&store, &merged.output_segments)
            .await
            .iter()
            .filter(|row| matches!(row, MetadataRow::FileRevision(record) if record.inode_id == a))
            .count();
        assert_eq!(hidden_rows, 0, "{inode_rows:?}: the hidden revision goes");
        assert_eq!(
            walk_reads,
            walked.len(),
            "{inode_rows:?}: the walk reads each small segment once, whole"
        );
    }
}

/// The current manifest with `next_run_no` set to `seed`, which seeds the
/// kept inode filter of a compaction over it.
fn seeded_manifest(current: &LoadedManifest, seed: RunNo) -> Arc<NamespaceManifestEnvelope> {
    let mut payload = current.state.envelope.payload().clone();
    payload.next_run_no = seed;
    Arc::new(encode_manifest(payload).into_envelope())
}

/// Merges `spec` over the current manifest with its kept inode filter seeded
/// by `seed`, and publishes the output.
async fn compact_with_seed<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    spec: &MetadataCompactionSpec,
    seed: RunNo,
) {
    let current = load_current_manifest(store, namespace_id)
        .await
        .expect("load the current manifest");
    let mut basis = crate::manifest::metadata_basis_from_manifest(store, None, &current).segments;
    basis.manifest = Some(seeded_manifest(&current, seed));
    let input_keys = input_segment_keys(&basis.scan_runs, spec).expect("the spec's runs");
    let cancellation = MetadataCompactionCancellation::default();
    let merged = run_metadata_compaction(
        &basis,
        None,
        namespace_id,
        spec,
        MetadataRunPolicy::default(),
        &cancellation,
    )
    .await
    .expect("merge")
    .expect("not cancelled");
    drop(basis);
    let timer = crate::test_support::std_timer();
    let outcome = finalize_metadata_compaction(
        store,
        namespace_id,
        spec,
        &input_keys,
        merged,
        &cancellation,
        &CompactionPublication {
            compactor_epoch: loonfs_types::CompactorEpoch(0),
            compaction: Deadline::start(Arc::clone(&timer)),
            publication: Deadline::start(timer),
        },
    )
    .await
    .expect("publish the merge");
    assert_matches!(outcome, CompactionUnitOutcome::Published { .. });
}

/// Whether the kept inode filter that `seed` seeds over the current manifest
/// keeps `row`.
async fn seed_keeps<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    seed: RunNo,
    row: &MetadataRow,
) -> bool {
    seeded_kept_inode_filter(store, namespace_id, seed)
        .await
        .keeps(row)
}

/// The kept inode filter of a base compaction of the inodes group over the
/// current manifest, seeded by `seed`.
async fn seeded_kept_inode_filter<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    seed: RunNo,
) -> KeptInodeFilter {
    let current = load_current_manifest(store, namespace_id)
        .await
        .expect("load the current manifest");
    let mut basis = crate::manifest::metadata_basis_from_manifest(store, None, &current).segments;
    basis.manifest = Some(seeded_manifest(&current, seed));
    load_kept_inode_filter(
        &basis,
        namespace_id,
        MetadataFamilyGroup::Inodes,
        MergePlacement::Base {
            output_seq: current.state.envelope.payload().head_seq,
        },
        &MetadataCompactionCancellation::default(),
    )
    .await
    .expect("build the filter")
    .expect("not cancelled")
}

/// Checks that a false positive in the kept inode filter keeps a hidden
/// inode's row for one compaction, and that a later compaction, whose output
/// run number seeds another filter, removes it.
#[tokio::test]
async fn a_false_positive_keeps_a_hidden_row_until_a_compaction_with_another_seed() {
    let temp = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp.path()).expect("store");
    let namespace_id = NamespaceId::parse("false-positive").expect("namespace id");
    let context = mutation_context("writer-1", 5_000);
    let (_, a) = expire_old(&store, &namespace_id, &context).await;
    let hidden_row = family_rows(&store, &namespace_id, ApiMetadataRowFamily::Inodes)
        .await
        .into_iter()
        .find(|row| matches!(row, MetadataRow::Inode(record) if record.inode_id == a))
        .expect("the hidden inode's row");
    let mut seed = RunNo(0);
    while !seed_keeps(&store, &namespace_id, seed, &hidden_row).await {
        assert!(seed.0 < 100_000, "some seed matches the hidden inode");
        seed = RunNo(seed.0 + 1);
    }

    let current = load_current_manifest(&store, &namespace_id)
        .await
        .expect("load the current manifest");
    let payload = current.state.envelope.payload();
    let spec = compaction_step::plan_group(
        payload,
        &runs_newest_first(payload),
        MetadataFamilyGroup::Inodes,
        MetadataRunPolicy::default(),
        MetadataCompactionPolicy::CompactImmediately,
    )
    .expect("plan the inodes group");
    assert_matches!(spec.placement(), MergePlacement::Base { .. });
    compact_with_seed(&store, &namespace_id, &spec, seed).await;
    let inodes = family_rows(&store, &namespace_id, ApiMetadataRowFamily::Inodes).await;
    assert!(
        inodes.contains(&hidden_row),
        "the false positive keeps the row"
    );

    // The next compaction's filter is seeded by the run number it writes.
    let current = load_current_manifest(&store, &namespace_id)
        .await
        .expect("load the current manifest");
    let payload = current.state.envelope.payload();
    let base = runs_newest_first(payload)
        .into_iter()
        .find(|run| {
            compaction_step::group_run_descriptors(run, MetadataFamilyGroup::Inodes)
                .next()
                .is_some()
        })
        .expect("the inodes base run");
    assert_eq!(base.tier, RunTier::Base);
    assert!(!seed_keeps(&store, &namespace_id, payload.next_run_no, &hidden_row).await);
    let later = MetadataCompactionSpec::new(
        MetadataFamilyGroup::Inodes,
        vec![base.run_no],
        inodes.len() as u64,
        MergePlacement::Base {
            output_seq: payload.head_seq,
        },
        payload.retention_floor_seq,
    );
    compact_with_seed(&store, &namespace_id, &later, payload.next_run_no).await;
    assert!(
        !family_rows(&store, &namespace_id, ApiMetadataRowFamily::Inodes)
            .await
            .contains(&hidden_row),
        "a compaction with another seed removes the row"
    );
}

/// Checks that a base compaction over a namespace whose floor has never
/// advanced keeps every inode without a walk, even with a recoverable
/// deletion in the trash.
#[tokio::test]
async fn a_kept_inode_filter_over_a_floor_of_zero_keeps_every_inode() {
    let temp = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp.path()).expect("store");
    let namespace_id = NamespaceId::parse("zero-floor").expect("namespace id");
    let context = mutation_context("writer-1", 5_000);
    bootstrap_restricted_namespace(&store, &namespace_id, &context).await;
    put(&store, &namespace_id, "/trash/old.txt", b"old", &context).await;
    delete_recursively(&store, &namespace_id, "/trash", &context).await;
    fold::fold_wal(&store, &namespace_id)
        .await
        .expect("fold the deletion");
    assert_eq!(read_floor_seq(&store, &namespace_id).await, ChangeSeq(0));

    assert!(matches!(
        seeded_kept_inode_filter(&store, &namespace_id, RunNo(0)).await,
        KeptInodeFilter::EveryInode
    ));
}

/// Checks that once the floor of the same namespace advances and the
/// deletion expires, a base compaction walks the tree and builds a filter of
/// the inodes that the walk reached.
#[tokio::test]
async fn a_kept_inode_filter_after_a_deletion_expires_holds_the_reached_inodes() {
    let temp = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp.path()).expect("store");
    let namespace_id = NamespaceId::parse("expired-floor").expect("namespace id");
    let context = mutation_context("writer-1", 5_000);
    bootstrap_restricted_namespace(&store, &namespace_id, &context).await;
    put(&store, &namespace_id, "/trash/old.txt", b"old", &context).await;
    let trash = inode_id_of(&store, &namespace_id, "/trash").await;
    let deletion_seq = delete_recursively(&store, &namespace_id, "/trash", &context).await;
    fold::fold_wal(&store, &namespace_id)
        .await
        .expect("fold the deletion");
    advance_retention_floor(
        &store,
        None,
        &namespace_id,
        RetentionTarget::Seq(deletion_seq),
        crate::test_support::grace_elapsed_after(context.now_ms),
        crate::test_support::std_timer(),
    )
    .await
    .expect("move the floor to the deletion");
    expire(&store, &namespace_id, trash, deletion_seq, &context)
        .await
        .expect("expire the deletion of /trash");
    fold::fold_wal(&store, &namespace_id)
        .await
        .expect("fold the expiry");

    assert!(matches!(
        seeded_kept_inode_filter(&store, &namespace_id, RunNo(0)).await,
        KeptInodeFilter::Reached(_)
    ));
}
