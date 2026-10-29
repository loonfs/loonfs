//! A base compaction removes the rows of the inodes that an expired deletion
//! hides, and keeps every row that a view at a retained sequence reads.

use super::active_deletions::inode_id_of;
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
use std::sync::atomic::{AtomicUsize, Ordering};

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
/// recoverable, and `/keep` stays visible. The current manifest acknowledges
/// the floor at the deletion of `/docs`.
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
    advance_floor_through(store, namespace_id, deletion_seq, context).await;
    acknowledge_floor(store, namespace_id, context).await;
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
        "the listed row goes"
    );
    assert_eq!(
        after[&ApiMetadataRowFamily::Tombstones]
            .iter()
            .map(|row| match row {
                MetadataRow::Tombstone(record) =>
                    (record.root_inode_id, record.action.hides_subtree()),
                other => panic!("unexpected tombstone row {other:?}"),
            })
            .collect::<Vec<_>>(),
        vec![(docs.docs, true)],
        "the set event stays"
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

/// The inodes of a namespace in which `/outer/inner/file.txt` and
/// `/outer/inner` are kept, `/outer/inner/deep/gone.txt` is deleted on its
/// own (the inner deletion), and `/outer` is deleted after it (the outer
/// deletion).
struct NestedDeletions {
    outer: InodeId,
    inner_parent: InodeId,
    kept_file: InodeId,
    inner: InodeId,
    gone: InodeId,
    inner_seq: ChangeSeq,
    outer_seq: ChangeSeq,
}

async fn delete_nested<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) -> NestedDeletions {
    bootstrap_restricted_namespace(store, namespace_id, context).await;
    put(
        store,
        namespace_id,
        "/outer/inner/file.txt",
        b"kept",
        context,
    )
    .await;
    put(
        store,
        namespace_id,
        "/outer/inner/deep/gone.txt",
        b"gone",
        context,
    )
    .await;
    let mut ids = BTreeMap::new();
    for path in [
        "/outer",
        "/outer/inner",
        "/outer/inner/file.txt",
        "/outer/inner/deep",
        "/outer/inner/deep/gone.txt",
    ] {
        ids.insert(path, inode_id_of(store, namespace_id, path).await);
    }
    let inner_seq = delete_recursively(store, namespace_id, "/outer/inner/deep", context).await;
    let outer_seq = delete_recursively(store, namespace_id, "/outer", context).await;
    fold::fold_wal(store, namespace_id)
        .await
        .expect("fold the deletions");
    NestedDeletions {
        outer: ids["/outer"],
        inner_parent: ids["/outer/inner"],
        kept_file: ids["/outer/inner/file.txt"],
        inner: ids["/outer/inner/deep"],
        gone: ids["/outer/inner/deep/gone.txt"],
        inner_seq,
        outer_seq,
    }
}

/// The inode that a row of an inode family stores state for, or the child
/// of a binding row.
fn row_owner(row: &MetadataRow) -> InodeId {
    match row {
        MetadataRow::DirentryBinding(record) => record.child_inode_id,
        other => row_inodes(other)[0],
    }
}

/// The rows of the inode families in the current manifest, as
/// [`owned_rows_of`] selects them for `inode_ids`.
async fn owned_rows<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    inode_ids: &[InodeId],
) -> BTreeMap<ApiMetadataRowFamily, Vec<MetadataRow>> {
    let mut rows = BTreeMap::new();
    for family in INODE_FAMILIES {
        rows.insert(family, family_rows(store, namespace_id, family).await);
    }
    owned_rows_of(&rows, inode_ids)
}

/// The rows in `rows` that store state for one of `inode_ids`, and the
/// binding rows whose child is one of them.
fn owned_rows_of(
    rows: &BTreeMap<ApiMetadataRowFamily, Vec<MetadataRow>>,
    inode_ids: &[InodeId],
) -> BTreeMap<ApiMetadataRowFamily, Vec<MetadataRow>> {
    rows.iter()
        .map(|(family, rows)| {
            let owned = rows
                .iter()
                .filter(|row| inode_ids.contains(&row_owner(row)))
                .cloned()
                .collect();
            (*family, owned)
        })
        .collect()
}

/// Checks nested deletions under a floor between them and a floor above
/// both. With the floor between, the inner deletion has expired and the
/// outer one is recoverable: a read by inode refuses the inner subtree and
/// allows the outer one, and base compactions remove the inner subtree's
/// rows and keep every row under the outer deletion, including the inner
/// deletion's saved parent. With the floor above both, base compactions
/// remove every row of both subtrees.
#[tokio::test]
async fn nested_deletions_expire_in_order_and_lose_their_rows_in_order() {
    let temp = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp.path()).expect("store");
    let namespace_id = NamespaceId::parse("nested-deletions").expect("namespace id");
    let context = mutation_context("writer-1", 5_000);
    let nested = delete_nested(&store, &namespace_id, &context).await;
    assert!(nested.inner_seq < nested.outer_seq);
    let outer = [nested.outer, nested.inner_parent, nested.kept_file];
    let inner = [nested.inner, nested.gone];
    let both = [outer.as_slice(), inner.as_slice()].concat();

    advance_floor_through(&store, &namespace_id, nested.inner_seq, &context).await;
    acknowledge_floor(&store, &namespace_id, &context).await;
    assert_eq!(
        read_as(&store, &namespace_id, nested.gone, "prn_root")
            .await
            .expect_err("a read under the expired deletion")
            .code(),
        ErrorCode::InodeNotFound
    );
    let before = owned_rows(&store, &namespace_id, &both).await;
    drain_compaction(&store, &namespace_id, every_group_compacts()).await;
    assert_eq!(
        owned_rows(&store, &namespace_id, &outer).await,
        owned_rows_of(&before, &outer),
        "every row under the recoverable deletion stays"
    );
    let inner_before = owned_rows_of(&before, &inner);
    for family in [
        ApiMetadataRowFamily::Inodes,
        ApiMetadataRowFamily::Revisions,
        ApiMetadataRowFamily::DirentryBinds,
        ApiMetadataRowFamily::DirentryChildBinds,
    ] {
        assert!(
            !inner_before[&family].is_empty(),
            "{family:?} had rows of the inner subtree"
        );
    }
    for (family, rows) in owned_rows(&store, &namespace_id, &inner).await {
        assert_eq!(
            rows,
            Vec::new(),
            "{family:?} keeps no row of the inner subtree"
        );
    }
    assert_eq!(
        read_as(&store, &namespace_id, nested.kept_file, "prn_root")
            .await
            .expect("a read under the recoverable deletion"),
        b"kept"
    );

    advance_floor_through(&store, &namespace_id, nested.outer_seq, &context).await;
    acknowledge_floor(&store, &namespace_id, &context).await;
    // A later write gives the inode families delta runs, so the next
    // compactions rewrite their bases.
    put(&store, &namespace_id, "/later.txt", b"later", &context).await;
    fold::fold_wal(&store, &namespace_id)
        .await
        .expect("fold the later write");
    drain_compaction(&store, &namespace_id, every_group_compacts()).await;
    for (family, rows) in owned_rows(&store, &namespace_id, &both).await {
        assert_eq!(
            rows,
            Vec::new(),
            "{family:?} keeps no row of either subtree"
        );
    }
    assert_eq!(
        read_as(&store, &namespace_id, nested.kept_file, "prn_root")
            .await
            .expect_err("both deletions have expired")
            .code(),
        ErrorCode::InodeNotFound
    );
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
/// `/keep-{index}.txt` files share a run with `/old/b.txt`. The current
/// manifest acknowledges the floor at the deletion. Returns the inodes of
/// `/old` and `/old/a.txt`, which an earlier run created.
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
    advance_floor_through(store, namespace_id, deletion_seq, context).await;
    acknowledge_floor(store, namespace_id, context).await;
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

/// Counts the events with one message on the thread that installs it.
struct MessageCounter {
    message: &'static str,
    count: Arc<AtomicUsize>,
}

impl tracing::Subscriber for MessageCounter {
    fn register_callsite(
        &self,
        _: &'static tracing::Metadata<'static>,
    ) -> tracing::subscriber::Interest {
        tracing::subscriber::Interest::sometimes()
    }

    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }

    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}

    fn event(&self, event: &tracing::Event<'_>) {
        struct Message(Option<String>);
        impl tracing::field::Visit for Message {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                if field.name() == "message" {
                    self.0 = Some(format!("{value:?}"));
                }
            }
        }
        let mut message = Message(None);
        event.record(&mut message);
        if message.0.as_deref() == Some(self.message) {
            self.count.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn enter(&self, _: &tracing::span::Id) {}

    fn exit(&self, _: &tracing::span::Id) {}
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

/// The object keys of the basis's segments of `family`.
fn segment_keys(runs: &[MetadataRunManifest], family: ApiMetadataRowFamily) -> BTreeSet<String> {
    runs.iter()
        .flat_map(|run| &run.segments)
        .filter(|segments| segments.family == family)
        .flat_map(|segments| &segments.segments)
        .map(metadata_segment_object_key)
        .collect()
}

/// The number of recorded reads of the segments in `keys`.
fn reads_of(gets: &[loonfs_test_support::stores::RecordedGet], keys: &BTreeSet<String>) -> usize {
    gets.iter().filter(|(key, _)| keys.contains(key)).count()
}

/// Checks that a base compaction whose kept inode filter would be over its
/// byte limit keeps every row it would have tested, reads none of the slot
/// index for the walk, and logs that once, and that the same compaction
/// under the limit removes the expired deletion's rows.
#[tokio::test]
async fn a_kept_inode_filter_over_its_byte_limit_keeps_every_row_and_logs_once() {
    let temp = tempdir().expect("tempdir");
    let store = RecordingStore::metadata_segments(LocalFsStore::new(temp.path()).expect("store"));
    let namespace_id = NamespaceId::parse("filter-limit").expect("namespace id");
    let context = mutation_context("writer-1", 5_000);
    let (_, a) = expire_old(&store, &namespace_id, &context).await;
    let current = load_current_manifest(&store, &namespace_id)
        .await
        .expect("load the current manifest");
    let policy = MetadataRunPolicy::default();
    let group = MetadataFamilyGroup::Revisions;
    for over_limit in [true, false] {
        let mut basis =
            crate::manifest::metadata_basis_from_manifest(&store, None, &current).segments;
        let mut runs = (*basis.scan_runs).clone();
        if over_limit {
            let inodes = runs
                .iter_mut()
                .flat_map(|run| &mut run.segments)
                .find(|family| {
                    family.family == ApiMetadataRowFamily::Inodes && !family.segments.is_empty()
                })
                .expect("an inodes segment");
            inodes.segments[0].row_count =
                crate::limits::MAX_BLOOM_FILTER_BYTES as u64 * 8 / 10 + 1;
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
        let logged = Arc::new(AtomicUsize::new(0));
        let guard = tracing::subscriber::set_default(MessageCounter {
            message: "kept inode filter exceeded its byte limit; kept every row",
            count: Arc::clone(&logged),
        });
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
        drop(guard);
        let gets = store.take_gets();
        let slot_index = segment_keys(&runs, ApiMetadataRowFamily::DirentryBinds);
        let trash = segment_keys(&runs, ApiMetadataRowFamily::ActiveDeletions);
        let rows = merged_rows(&store, &merged.output_segments).await;
        let hidden_rows = rows
            .iter()
            .filter(|row| matches!(row, MetadataRow::FileRevision(record) if record.inode_id == a))
            .count();
        assert_eq!(
            reads_of(&gets, &trash),
            trash.len(),
            "the scan for an expired deletion and the walk read each small trash segment once, \
             whole"
        );
        if over_limit {
            assert_eq!(hidden_rows, 1, "the hidden revision stays");
            assert_eq!(
                reads_of(&gets, &slot_index),
                0,
                "an over-limit filter walks nothing"
            );
            assert_eq!(logged.load(Ordering::SeqCst), 1, "the compaction logs once");
        } else {
            assert_eq!(hidden_rows, 0, "the hidden revision goes");
            assert_eq!(
                reads_of(&gets, &slot_index),
                slot_index.len(),
                "the walk reads each small segment once, whole"
            );
            assert_eq!(logged.load(Ordering::SeqCst), 0);
        }
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
    .keeps(row)
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

/// Builds the kept inode filter of a base compaction of `revisions` over the
/// current manifest, and returns the filter with the number of the basis's
/// slot-index reads that building it took.
async fn kept_inode_filter_and_walk_reads(
    store: &RecordingStore<LocalFsStore>,
    namespace_id: &NamespaceId,
) -> (KeptInodeFilter, usize) {
    let current = load_current_manifest(store, namespace_id)
        .await
        .expect("load the current manifest");
    let basis = crate::manifest::metadata_basis_from_manifest(store, None, &current).segments;
    let slot_index = segment_keys(&basis.scan_runs, ApiMetadataRowFamily::DirentryBinds);
    store.reset();
    let filter = load_kept_inode_filter(
        &basis,
        namespace_id,
        MetadataFamilyGroup::Revisions,
        MergePlacement::Base {
            output_seq: current.state.envelope.payload().head_seq,
        },
        &MetadataCompactionCancellation::default(),
    )
    .await
    .expect("build the filter")
    .expect("not cancelled");
    (filter, reads_of(&store.take_gets(), &slot_index))
}

/// Checks that a base compaction builds no kept inode filter, and walks
/// nothing, while no deletion at or below the acknowledged floor is listed
/// without a removal: with only an undeleted deletion below the floor, and
/// with a deletion that a retention advance expired before a fold
/// acknowledges the advance. Once a fold acknowledges it, the compaction
/// walks the tree and builds the filter, which leaves out the hidden inode.
#[tokio::test]
async fn a_base_compaction_walks_the_tree_only_when_a_deletion_has_expired() {
    let temp = tempdir().expect("tempdir");
    let store = RecordingStore::metadata_segments(LocalFsStore::new(temp.path()).expect("store"));
    let namespace_id = NamespaceId::parse("expiry-scan").expect("namespace id");
    let context = mutation_context("writer-1", 5_000);
    bootstrap_restricted_namespace(&store, &namespace_id, &context).await;
    put(&store, &namespace_id, "/old/a.txt", b"old", &context).await;
    put(
        &store,
        &namespace_id,
        "/restored.txt",
        b"restored",
        &context,
    )
    .await;
    let restored = inode_id_of(&store, &namespace_id, "/restored.txt").await;
    let a = inode_id_of(&store, &namespace_id, "/old/a.txt").await;
    let restored_seq = delete_recursively(&store, &namespace_id, "/restored.txt", &context).await;
    commit(
        &store,
        &namespace_id,
        FilesystemOperation::undelete(
            restored,
            restored_seq,
            &loonfs_types::options::UndeleteDestination::Recorded,
        ),
        Vec::new(),
        &context,
    )
    .await
    .expect("undelete");
    let head_seq = delete_recursively(&store, &namespace_id, "/old", &context).await;
    advance_floor_through(&store, &namespace_id, ChangeSeq(head_seq.0 - 1), &context).await;
    acknowledge_floor(&store, &namespace_id, &context).await;

    let (filter, walk_reads) = kept_inode_filter_and_walk_reads(&store, &namespace_id).await;
    assert!(
        matches!(filter, KeptInodeFilter::EveryInode),
        "an undeleted deletion"
    );
    assert_eq!(walk_reads, 0);

    advance_floor_through(&store, &namespace_id, head_seq, &context).await;
    let (filter, walk_reads) = kept_inode_filter_and_walk_reads(&store, &namespace_id).await;
    assert!(
        matches!(filter, KeptInodeFilter::EveryInode),
        "a deletion that the acknowledged floor hasn't reached"
    );
    assert_eq!(walk_reads, 0);

    acknowledge_floor(&store, &namespace_id, &context).await;
    let (filter, walk_reads) = kept_inode_filter_and_walk_reads(&store, &namespace_id).await;
    assert!(walk_reads > 0, "the walk reads the slot index");
    let revision = |inode_id| {
        MetadataRow::FileRevision(crate::metadata::RevisionRecord {
            inode_id,
            revision_no: RevisionNo(1),
            committed_seq: ChangeSeq(1),
            commit_id: CommitId::parse("c_probe").expect("commit id"),
            committed_by: loonfs_types::ActorId::loonfs(),
            committed_at_ms: 1_000,
            delta_index: 0,
            content_ref: loonfs_types::ContentRef::blob_v1(
                namespace_id.clone(),
                loonfs_types::ContentId::parse("con_00000000000000000000000000000000")
                    .expect("content id"),
                b"probe",
                loonfs_types::ChecksumAlgorithm::Crc64nvme,
            ),
        })
    };
    assert!(matches!(filter, KeptInodeFilter::Reached(_)));
    assert!(
        filter.keeps(&revision(restored)),
        "a visible file keeps its rows"
    );
    assert!(!filter.keeps(&revision(a)), "a hidden file loses its rows");
}
