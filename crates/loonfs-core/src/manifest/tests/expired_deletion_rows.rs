//! A base compaction removes the rows of the inodes that an expired deletion
//! hides, and keeps every row that a view at a retained sequence reads.

use super::trash::inode_id_of;
use super::*;
use crate::compaction::kept_inodes::{load_kept_inode_filter, KeptInodeFilter};
use crate::storage::content_admission::PreparedContent;
use crate::test_support::kept_inodes::{
    compact_groups_without_false_positives, compact_with_seed, seeded_kept_inode_filter,
    seeded_manifest, whole_group_spec,
};
use loonfs_types::{
    AccessGrants, AccessRight, AccessRights, AttributeKey, AttributeValue, DeleteDirectoryBehavior,
    InodeKind, NamespaceAccess, PrincipalId, PrincipalScope, PrincipalSet, Subject, SubjectId,
    ROOT_INODE_ID,
};
use plan::CompactionPlacement;
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
    context: &WriterAt,
) {
    crate::test_support::NamespaceFixture::new(store, namespace_id, context)
        .engine()
        .bootstrap_namespace(
            &loonfs_test_support::test_actor(),
            &crate::options::CreateNamespaceOptions {
                access: NamespaceAccess::Acl {
                    principal_scope: PrincipalScope::parse(PRINCIPAL_SCOPE)
                        .expect("principal scope"),
                    root_grants: AccessGrants::new(BTreeMap::from([(
                        PrincipalId::parse("prn_root").expect("principal id"),
                        AccessRights::ADMIN,
                    )]))
                    .expect("root grants"),
                },
                ..Default::default()
            },
        )
        .await
        .expect("bootstrap a restricted namespace");
    crate::test_support::namespace_store(store, namespace_id)
        .acquire_writer_epoch(&context.writer_id)
        .await
        .expect("acquire the writer");
    crate::test_support::namespace_store(store, namespace_id)
        .fold_wal()
        .await
        .expect("publish the first manifest");
}

/// Commits one operation as the administrator.
async fn commit<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    operation: FilesystemOperation,
    prepared: Vec<PreparedContent>,
    context: &WriterAt,
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
        crate::test_support::namespace_store(store, namespace_id),
        context.writer_id.clone(),
    )
    .publish_batch(vec![candidate], &context.clock())
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
    context: &WriterAt,
) {
    let stored = crate::test_support::namespace_store(store, namespace_id)
        .store_bytes_as_content(bytes)
        .await
        .expect("store content");
    let content_ref = stored.content_ref().clone();
    let catalog = crate::test_support::namespace_store(store, namespace_id)
        .load_catalog_entry()
        .await
        .expect("load namespace catalog");
    let operation = FilesystemOperation::PutFile {
        path: AbsolutePath::parse(path).expect("path"),
        content: loonfs_types::FileContent::Ref(content_ref),
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
    context: &WriterAt,
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
    context: &WriterAt,
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
    context: &WriterAt,
) -> CommitSeq {
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
/// with revisions, attributes, and access rows, `/archive` is in the trash,
/// and `/keep` stays visible. The current manifest's retention
/// floor is at the deletion of `/docs`.
struct ExpiredDocs {
    docs: InodeId,
    /// `/docs`, `/docs/notes.txt`, `/docs/sub`, and `/docs/sub/deep.txt`.
    hidden: Vec<InodeId>,
    /// `/archive` and `/archive/old.txt`.
    in_trash: Vec<InodeId>,
    /// The root, `/keep`, and `/keep/kept.txt`.
    visible: Vec<InodeId>,
}

impl ExpiredDocs {
    fn every_inode(&self) -> Vec<InodeId> {
        [&self.hidden, &self.in_trash, &self.visible]
            .into_iter()
            .flatten()
            .copied()
            .collect()
    }
}

async fn expire_docs_beside_an_archive_in_the_trash<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &WriterAt,
) -> ExpiredDocs {
    bootstrap_restricted_namespace(store, namespace_id, context).await;
    put(store, namespace_id, "/docs/notes.txt", b"one", context).await;
    put(store, namespace_id, "/docs/notes.txt", b"two", context).await;
    put(store, namespace_id, "/docs/sub/deep.txt", b"deep", context).await;
    put(store, namespace_id, "/keep/kept.txt", b"kept", context).await;
    put(store, namespace_id, "/archive/old.txt", b"old", context).await;
    for path in ["/docs/notes.txt", "/keep/kept.txt", "/archive/old.txt"] {
        set_attribute(store, namespace_id, path, context).await;
    }
    for path in ["/docs", "/docs/sub", "/keep", "/archive"] {
        grant_read(store, namespace_id, path, "prn_team", true, context).await;
    }
    let mut ids = BTreeMap::new();
    for path in [
        "/docs",
        "/docs/notes.txt",
        "/docs/sub",
        "/docs/sub/deep.txt",
        "/archive",
        "/archive/old.txt",
        "/keep",
        "/keep/kept.txt",
    ] {
        ids.insert(path, inode_id_of(store, namespace_id, path).await);
    }
    let deletion_seq = delete_recursively(store, namespace_id, "/docs", context).await;
    delete_recursively(store, namespace_id, "/archive", context).await;
    crate::test_support::namespace_store(store, namespace_id)
        .fold_wal()
        .await
        .expect("fold the deletions");
    // The floor stops at the deletion, so the views that the compaction has
    // to preserve include the ones before `/archive` was deleted.
    advance_floor_through(store, namespace_id, deletion_seq, context).await;
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
        in_trash: ["/archive", "/archive/old.txt"]
            .map(|path| ids[path])
            .to_vec(),
        visible: vec![ROOT_INODE_ID, ids["/keep"], ids["/keep/kept.txt"]],
    }
}

/// Every row of `family` in the current manifest.
pub(super) async fn family_rows<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    family: ApiMetadataRowFamily,
) -> Vec<MetadataRow> {
    let current = crate::test_support::namespace_store(store, namespace_id)
        .load_current_manifest()
        .await
        .expect("load the current manifest");
    super::super::metadata_basis_from_manifest(store, None, &current)
        .segments
        .scan_range(
            family,
            "",
            None,
            usize::MAX,
            None,
            scan::BlocksPerRefill::One,
        )
        .await
        .expect("scan the family")
        .into_iter()
        .map(|(_, row)| row)
        .collect::<Vec<_>>()
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
        MetadataRow::Trash(record) => vec![record.root_inode_id],
        MetadataRow::CommitReceipt(_)
        | MetadataRow::Commit(_)
        | MetadataRow::Merge(_)
        | MetadataRow::ContentLayout(_) => Vec::new(),
    }
}

/// The rows of each family that refer to one of `inode_ids`.
async fn rows_of<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    inode_ids: &[InodeId],
) -> BTreeMap<ApiMetadataRowFamily, Vec<MetadataRow>> {
    let mut rows = BTreeMap::new();
    for family in ApiMetadataRowFamily::ALL {
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
fn inode_state(state: &MetadataState, inode_id: InodeId, seq: CommitSeq) -> String {
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
fn visible_metadata(state: &MetadataState, inode_ids: &[InodeId], seq: CommitSeq) -> Vec<String> {
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
        .map(|seq| visible_metadata(&state, inode_ids, CommitSeq(seq)))
        .collect()
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
    let context = writer_at("writer-1", 5_000);
    let docs = expire_docs_beside_an_archive_in_the_trash(&store, &namespace_id, &context).await;
    let before = rows_of(&store, &namespace_id, &docs.hidden).await;
    for family in INODE_FAMILIES {
        assert!(!before[&family].is_empty(), "{family:?} has hidden rows");
    }
    let views_before =
        visible_metadata_at_retained_sequences(&store, &namespace_id, &docs.every_inode()).await;

    compact_groups_without_false_positives(
        &store,
        &namespace_id,
        &MetadataFamilyGroup::ALL,
        &docs.hidden,
    )
    .await;

    let after = rows_of(&store, &namespace_id, &docs.hidden).await;
    for family in INODE_FAMILIES {
        assert_eq!(after[&family], Vec::new(), "{family:?} keeps no hidden row");
    }
    assert_eq!(
        after[&ApiMetadataRowFamily::Trash],
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
    let restore_deleted = commit(
        &store,
        &namespace_id,
        FilesystemOperation::restore_deleted(
            docs.docs,
            before[&ApiMetadataRowFamily::Trash]
                .iter()
                .find_map(|row| match row {
                    MetadataRow::Trash(record) => Some(record.deletion_seq),
                    _ => None,
                })
                .expect("the deletion's trash rows"),
            &loonfs_types::options::RestoreDeletedDestination::Recorded,
        ),
        Vec::new(),
        &context,
    )
    .await
    .expect_err("an expired deletion can't be restored");
    assert_eq!(restore_deleted.code(), ErrorCode::NotDeleted);
}

/// Checks that the base compactions that remove an expired deletion's rows
/// keep every row of a deletion in the trash and of the visible tree.
#[tokio::test]
async fn a_deletion_in_the_trash_keeps_every_row_while_an_expired_one_loses_its_rows() {
    let temp = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp.path()).expect("store");
    let namespace_id = NamespaceId::parse("trash-rows").expect("namespace id");
    let context = writer_at("writer-1", 5_000);
    let docs = expire_docs_beside_an_archive_in_the_trash(&store, &namespace_id, &context).await;
    let kept = [docs.in_trash.as_slice(), docs.visible.as_slice()].concat();
    let before = rows_of(&store, &namespace_id, &kept).await;

    compact_groups_without_false_positives(
        &store,
        &namespace_id,
        &MetadataFamilyGroup::ALL,
        &docs.hidden,
    )
    .await;

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
        after[&ApiMetadataRowFamily::Trash].as_slice(),
        [MetadataRow::Trash(crate::metadata::TrashRecord {
            action: loonfs_types::format::manifest::TrashRowAction::Listed { .. },
            root_inode_id,
            ..
        })] if *root_inode_id == docs.in_trash[0],
        "the deletion stays in the trash"
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
    crate::test_support::namespace_store(store, namespace_id)
        .load_current_metadata_view()
        .await
        .expect("load the current view")
        .read_file_revision_bytes_by_inode(
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
    inner_seq: CommitSeq,
    outer_seq: CommitSeq,
}

async fn delete_nested<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &WriterAt,
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
    crate::test_support::namespace_store(store, namespace_id)
        .fold_wal()
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
/// outer one is in the trash: a read by inode refuses the inner subtree and
/// allows the outer one, and base compactions remove the inner subtree's
/// rows and keep every row under the outer deletion, including the inner
/// deletion's saved parent. With the floor above both, base compactions
/// remove every row of both subtrees.
#[tokio::test]
async fn nested_deletions_expire_in_order_and_lose_their_rows_in_order() {
    let temp = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp.path()).expect("store");
    let namespace_id = NamespaceId::parse("nested-deletions").expect("namespace id");
    let context = writer_at("writer-1", 5_000);
    let nested = delete_nested(&store, &namespace_id, &context).await;
    assert!(nested.inner_seq < nested.outer_seq);
    let outer = [nested.outer, nested.inner_parent, nested.kept_file];
    let inner = [nested.inner, nested.gone];
    let both = [outer.as_slice(), inner.as_slice()].concat();

    advance_floor_through(&store, &namespace_id, nested.inner_seq, &context).await;
    assert_eq!(
        read_as(&store, &namespace_id, nested.gone, "prn_root")
            .await
            .expect_err("a read under the expired deletion")
            .code(),
        ErrorCode::InodeNotFound
    );
    let before = owned_rows(&store, &namespace_id, &both).await;
    compact_groups_without_false_positives(
        &store,
        &namespace_id,
        &MetadataFamilyGroup::ALL,
        &inner,
    )
    .await;
    assert_eq!(
        owned_rows(&store, &namespace_id, &outer).await,
        owned_rows_of(&before, &outer),
        "every row under the deletion in the trash stays"
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
            .expect("a read under the deletion in the trash"),
        b"kept"
    );

    advance_floor_through(&store, &namespace_id, nested.outer_seq, &context).await;
    // A later write gives the inode families delta runs, so the next
    // compactions rewrite their bases.
    put(&store, &namespace_id, "/later.txt", b"later", &context).await;
    crate::test_support::namespace_store(&store, &namespace_id)
        .fold_wal()
        .await
        .expect("fold the later write");
    compact_groups_without_false_positives(&store, &namespace_id, &MetadataFamilyGroup::ALL, &both)
        .await;
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

/// The runs a delta-only window of `group` compacts: every run of the group
/// after its oldest.
fn delta_window(
    manifest: &NamespaceManifestEnvelope,
    group: MetadataFamilyGroup,
) -> Vec<MetadataRunManifest> {
    let mut runs = runs_newest_first(manifest.payload())
        .into_iter()
        .filter(|run| plan::group_run_descriptors(run, group).next().is_some())
        .collect::<Vec<_>>();
    runs.sort_by_key(|run| (run.tier != RunTier::Base, run.run_seq));
    runs.remove(0);
    runs
}

/// A namespace with an open access mode in which the expired deletion of
/// `/old` hides `/old`, `/old/a.txt`, and `/old/b.txt`, and the visible
/// `/keep-{index}.txt` files share a run with `/old/b.txt`. The current
/// manifest's retention floor is at the deletion. Returns the inodes of
/// `/old` and `/old/a.txt`, which an earlier run created.
async fn expire_old<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &WriterAt,
) -> (InodeId, InodeId) {
    bootstrap_namespace(store, namespace_id, context)
        .await
        .expect("bootstrap");
    write_test_file(store, namespace_id, "/old/a.txt", "c_old_a", context).await;
    crate::test_support::namespace_store(store, namespace_id)
        .fold_wal()
        .await
        .expect("fold");
    write_test_file(store, namespace_id, "/old/b.txt", "c_old_b", context).await;
    let keep = (0..8)
        .map(|index| format!("/keep-{index}.txt"))
        .collect::<Vec<_>>();
    crate::test_support::NamespaceFixture::new(store, namespace_id, context)
        .write_files_bytes(&keep, b"keep")
        .await
        .expect("write visible files");
    crate::test_support::namespace_store(store, namespace_id)
        .fold_wal()
        .await
        .expect("fold");
    let old = inode_id_of(store, namespace_id, "/old").await;
    let a = inode_id_of(store, namespace_id, "/old/a.txt").await;
    let deletion_seq = crate::test_support::NamespaceFixture::new(store, namespace_id, context)
        .delete_path("/old", None)
        .await
        .expect("delete /old")
        .committed_seq;
    advance_floor_through(store, namespace_id, deletion_seq, context).await;
    (old, a)
}

/// Creates `count` nested directories `/c{count-1}/d/d/...` in a few large
/// commits. Each directory contains an empty directory `f`, and every
/// directory except the deepest also contains the next directory down as `d`,
/// which sorts before `f`. The commits build the chain from the bottom with
/// moves, because a request path has at most 128 components.
async fn create_directory_chain<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &WriterAt,
    count: usize,
) {
    let path = |path: String| AbsolutePath::parse(path).expect("path");
    let fixture = crate::test_support::NamespaceFixture::new(store, namespace_id, context);
    let mut operations = Vec::new();
    for level in 0..count {
        operations.push(FilesystemOperation::CreateDirectory {
            path: path(format!("/c{level}/f")),
            parents: true,
        });
        if level > 0 {
            operations.push(FilesystemOperation::MovePath {
                source_path: path(format!("/c{}", level - 1)),
                destination_path: path(format!("/c{level}/d")),
                precondition: loonfs_types::DestinationPrecondition {
                    behavior: DestinationBehavior::NoReplace,
                    ..Default::default()
                },
            });
        }
        if operations.len() >= 1_024 || level + 1 == count {
            let intent = CommitIntent {
                commit_id: CommitId::generate(),
                actor_id: loonfs_test_support::test_actor(),
                subject: None,
                message: None,
                operations: std::mem::take(&mut operations),
                preconditions: Vec::new(),
            };
            fixture
                .publish_candidates(vec![CommitCandidate::new(intent)])
                .await
                .pop()
                .expect("one result")
                .expect("commit a part of the chain");
        }
    }
}

/// Checks that a base compaction whose kept inode walk reaches its limit
/// publishes, keeps a revision that an expired deletion hides, and reports
/// that it skipped the removal of hidden rows.
///
/// Once the walk has read a directory's last row, it drops the directory's
/// cursor before it descends into the directory's last child, so a chain of
/// small directories uses one cursor at any depth. In the chain that this test builds, every directory has a second
/// entry after the child it descends into (`f` after `d`, and the root's
/// `keep.txt` after the chain's top), so every directory keeps its cursor. The
/// walk reaches the limit when it reads the directory at depth 4,095, with
/// the root's cursor and 4,095 others on the stack, and finds a child
/// directory.
#[tokio::test]
async fn a_compaction_whose_kept_inode_walk_reaches_its_limit_skips_hidden_row_removal() {
    let temp = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp.path()).expect("store");
    let namespace_id = NamespaceId::parse("walk-limit").expect("namespace id");
    let context = writer_at("writer-1", 5_000);
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");
    write_test_file(&store, &namespace_id, "/old/a.txt", "c_old_a", &context).await;
    write_test_file(&store, &namespace_id, "/keep.txt", "c_keep", &context).await;
    create_directory_chain(
        &store,
        &namespace_id,
        &context,
        crate::limits::MAX_KEPT_INODE_WALK_DEPTH,
    )
    .await;
    let a = inode_id_of(&store, &namespace_id, "/old/a.txt").await;
    let deletion_seq = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .delete_path("/old", None)
        .await
        .expect("delete /old")
        .committed_seq;
    advance_floor_through(&store, &namespace_id, deletion_seq, &context).await;
    let before =
        rows_of(&store, &namespace_id, &[a]).await[&ApiMetadataRowFamily::Revisions].clone();
    assert_eq!(before.len(), 1, "the basis has the hidden revision");

    let current = crate::test_support::namespace_store(&store, &namespace_id)
        .load_current_manifest()
        .await
        .expect("load the current manifest");
    let outcome = run_planned_compaction(
        &store,
        &namespace_id,
        &context,
        MetadataRunPolicy::default(),
        &whole_group_spec(&current, MetadataFamilyGroup::Revisions),
    )
    .await;
    assert_matches!(
        outcome,
        CompactionUnitOutcome::Published { skipped, .. }
            if skipped == [loonfs_types::MetadataSkippedWork::ExpiredRowRemovalWalkLimit]
    );
    assert_eq!(
        rows_of(&store, &namespace_id, &[a]).await[&ApiMetadataRowFamily::Revisions],
        before,
        "the compaction keeps the hidden revision"
    );
}

/// Checks that a compaction whose window skips a group's oldest run keeps
/// every row of an expired deletion's inodes, and a base compaction of the
/// same groups then removes them.
#[tokio::test]
async fn a_delta_only_compaction_keeps_an_expired_deletions_rows() {
    let temp = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp.path()).expect("store");
    let namespace_id = NamespaceId::parse("delta-only").expect("namespace id");
    let context = writer_at("writer-1", 5_000);
    let (old, a) = expire_old(&store, &namespace_id, &context).await;
    let hidden = [old, a];
    let before = rows_of(&store, &namespace_id, &hidden).await;
    let floor_seq = read_floor_seq(&store, &namespace_id).await;

    for group in [MetadataFamilyGroup::Inodes, MetadataFamilyGroup::Bindings] {
        let current = crate::test_support::namespace_store(&store, &namespace_id)
            .load_current_manifest()
            .await
            .expect("load the current manifest");
        let window = delta_window(&current.envelope, group);
        assert!(window.len() >= 2, "{group:?} has a delta-only window");
        let spec = MetadataCompactionSpec::new(
            group,
            window.iter().map(|run| run.run_no).collect(),
            window
                .iter()
                .flat_map(|run| plan::group_run_descriptors(run, group))
                .map(|descriptor| descriptor.row_count)
                .sum(),
            CompactionPlacement::Delta {
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

    compact_groups_without_false_positives(
        &store,
        &namespace_id,
        &MetadataFamilyGroup::ALL,
        &hidden,
    )
    .await;
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

/// The rows of the segments a compaction wrote.
async fn compacted_rows<S: ObjectStore + ?Sized>(
    store: &S,
    output_segments: &[MetadataSegmentRef],
) -> Vec<MetadataRow> {
    let mut rows = Vec::new();
    for descriptor in output_segments {
        let index = index_block_load::load_segment_index_for_compaction(store, descriptor)
            .await
            .expect("load the output index");
        let blocks =
            data_block_load::load_segment_data_block_span(store, None, None, descriptor, &index)
                .await
                .expect("load the output blocks");
        for block in blocks {
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

/// Checks that a capped filter still removes hidden rows and walks each
/// small slot-index segment once.
#[tokio::test]
async fn a_kept_inode_filter_over_its_byte_limit_still_removes_hidden_rows() {
    let temp = tempdir().expect("tempdir");
    let store = RecordingStore::metadata_segments(LocalFsStore::new(temp.path()).expect("store"));
    let namespace_id = NamespaceId::parse("filter-limit").expect("namespace id");
    let context = writer_at("writer-1", 5_000);
    let (_, a) = expire_old(&store, &namespace_id, &context).await;
    assert_eq!(
        rows_of(&store, &namespace_id, &[a]).await[&ApiMetadataRowFamily::Revisions].len(),
        1,
        "the basis has the hidden revision"
    );
    let current = crate::test_support::namespace_store(&store, &namespace_id)
        .load_current_manifest()
        .await
        .expect("load the current manifest");
    let hidden_revision = family_rows(&store, &namespace_id, ApiMetadataRowFamily::Revisions)
        .await
        .into_iter()
        .find(|row| matches!(row, MetadataRow::FileRevision(record) if record.inode_id == a))
        .expect("the hidden revision");
    let policy = MetadataRunPolicy::default();
    let group = MetadataFamilyGroup::Revisions;
    // 200 million rows take the whole 64 MiB at about 2.7 bits per row.
    for inode_rows in [None, Some(200_000_000)] {
        let mut basis = super::super::metadata_basis_from_manifest(&store, None, &current).segments;
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
        let spec = whole_group_spec(&current, group);
        // The low bits of an inode ID are random, so the test seeds this
        // variant's filter with a run number whose filter has no false
        // positive for the hidden revision, and the compaction then removes the
        // revision in every run.
        let mut seeded = false;
        for offset in 0..64 {
            basis.manifest =
                seeded_manifest(&current, RunNo(current.payload().next_run_no.0 + offset));
            let filter = load_kept_inode_filter(
                &basis,
                &namespace_id,
                &spec,
                &MetadataCompactionCancellation::default(),
            )
            .await
            .expect("build the filter")
            .expect("not cancelled");
            if !filter.keeps(&hidden_revision) {
                seeded = true;
                break;
            }
        }
        assert!(seeded, "{inode_rows:?}: a seed with no false positive");
        // The compaction walks through an empty block cache of its own, as a
        // runtime's compaction does.
        let cache = crate::test_support::segment_cache();
        let mut walked_basis =
            super::super::metadata_basis_from_manifest(&store, Some(&cache), &current).segments;
        walked_basis.scan_runs = Arc::clone(&basis.scan_runs);
        walked_basis.manifest = Arc::clone(&basis.manifest);
        store.reset();
        let compacted = run_metadata_compaction(
            &walked_basis,
            &crate::cache::MetadataSegmentCache::unshared(0),
            &namespace_id,
            &spec,
            policy,
            &MetadataCompactionCancellation::default(),
        )
        .await
        .expect("compact")
        .expect("not cancelled");
        let walked = segment_keys(&runs, ApiMetadataRowFamily::DirentryBinds);
        let walk_reads = store
            .take_gets()
            .iter()
            .filter(|(key, _)| walked.contains(key))
            .count();
        let hidden_rows = compacted_rows(&store, &compacted.output_segments)
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

/// Checks that a false positive in the kept inode filter keeps a hidden
/// inode's row for one compaction, and that a later compaction, whose output
/// run number seeds another filter, removes it.
#[tokio::test]
async fn a_false_positive_keeps_a_hidden_row_until_a_compaction_with_another_seed() {
    let temp = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp.path()).expect("store");
    let namespace_id = NamespaceId::parse("false-positive").expect("namespace id");
    let context = writer_at("writer-1", 5_000);
    let (_, a) = expire_old(&store, &namespace_id, &context).await;
    // A filter of the nine inodes that `expire_old` leaves visible has the
    // minimum 4,096 bits, and it matches the hidden inode in about one seed
    // in 200,000. With these files the filter has ten bits for each inode,
    // and it matches the hidden inode in about one seed in 120.
    let visible = (0..400)
        .map(|index| format!("/visible-{index}.txt"))
        .collect::<Vec<_>>();
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .write_files_bytes(&visible, b"visible")
        .await
        .expect("write visible files");
    crate::test_support::namespace_store(&store, &namespace_id)
        .fold_wal()
        .await
        .expect("fold");
    let hidden_row = family_rows(&store, &namespace_id, ApiMetadataRowFamily::Inodes)
        .await
        .into_iter()
        .find(|row| matches!(row, MetadataRow::Inode(record) if record.inode_id == a))
        .expect("the hidden inode's row");
    let current = crate::test_support::namespace_store(&store, &namespace_id)
        .load_current_manifest()
        .await
        .expect("load the current manifest");
    let spec = whole_group_spec(&current, MetadataFamilyGroup::Inodes);
    // The seeds' walks read one basis through a segment cache, so each walk
    // after the first reads the rows of the 400 files from memory.
    let cache = crate::test_support::segment_cache();
    let mut basis =
        super::super::metadata_basis_from_manifest(&store, Some(&cache), &current).segments;
    let mut seed = RunNo(0);
    loop {
        basis.manifest = seeded_manifest(&current, seed);
        let filter = load_kept_inode_filter(
            &basis,
            &namespace_id,
            &spec,
            &MetadataCompactionCancellation::default(),
        )
        .await
        .expect("build the filter")
        .expect("not cancelled");
        if filter.keeps(&hidden_row) {
            break;
        }
        assert!(seed.0 < 100_000, "some seed matches the hidden inode");
        seed = RunNo(seed.0 + 1);
    }

    compact_with_seed(&store, &namespace_id, &spec, seed).await;
    let inodes = family_rows(&store, &namespace_id, ApiMetadataRowFamily::Inodes).await;
    assert!(
        inodes.contains(&hidden_row),
        "the false positive keeps the row"
    );

    // The next compaction's filter is seeded by the run number it writes.
    let current = crate::test_support::namespace_store(&store, &namespace_id)
        .load_current_manifest()
        .await
        .expect("load the current manifest");
    let payload = current.payload();
    let base = runs_newest_first(payload)
        .into_iter()
        .find(|run| {
            plan::group_run_descriptors(run, MetadataFamilyGroup::Inodes)
                .next()
                .is_some()
        })
        .expect("the inodes base run");
    assert_eq!(base.tier, RunTier::Base);
    // The low bits of an inode ID are random, so the filter that the next run
    // number seeds can also keep the row. The test takes the first later seed
    // whose filter doesn't.
    let mut later_seed = None;
    for offset in 0..64 {
        let candidate = RunNo(payload.next_run_no.0 + offset);
        if !seed_keeps(&store, &namespace_id, candidate, &hidden_row).await {
            later_seed = Some(candidate);
            break;
        }
    }
    let later_seed = later_seed.expect("a later seed whose filter removes the row");
    let later = MetadataCompactionSpec::new(
        MetadataFamilyGroup::Inodes,
        vec![base.run_no],
        inodes.len() as u64,
        CompactionPlacement::Base {
            output_seq: payload.head_seq,
        },
        payload.retention_floor_seq,
    );
    compact_with_seed(&store, &namespace_id, &later, later_seed).await;
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
    let current = crate::test_support::namespace_store(store, namespace_id)
        .load_current_manifest()
        .await
        .expect("load the current manifest");
    let basis = super::super::metadata_basis_from_manifest(store, None, &current).segments;
    let slot_index = segment_keys(&basis.scan_runs, ApiMetadataRowFamily::DirentryBinds);
    store.reset();
    let filter = load_kept_inode_filter(
        &basis,
        namespace_id,
        &MetadataCompactionSpec::new(
            MetadataFamilyGroup::Revisions,
            vec![RunNo(0)],
            0,
            CompactionPlacement::Base {
                output_seq: current.payload().head_seq,
            },
            CommitSeq(0),
        ),
        &MetadataCompactionCancellation::default(),
    )
    .await
    .expect("build the filter")
    .expect("not cancelled");
    (filter, reads_of(&store.take_gets(), &slot_index))
}

/// Base-compacts `group` over the current manifest and publishes the output.
pub(super) async fn compact_group<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &WriterAt,
    group: MetadataFamilyGroup,
) {
    let current = crate::test_support::namespace_store(store, namespace_id)
        .load_current_manifest()
        .await
        .expect("load the current manifest");
    let spec = whole_group_spec(&current, group);
    let outcome = run_planned_compaction(
        store,
        namespace_id,
        context,
        MetadataRunPolicy::default(),
        &spec,
    )
    .await;
    assert_matches!(
        outcome,
        CompactionUnitOutcome::Published { .. },
        "{group:?}"
    );
}

/// The deletion sequences of the `listed` rows in the current manifest's
/// `trash` family.
async fn listed_deletions<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Vec<CommitSeq> {
    family_rows(store, namespace_id, ApiMetadataRowFamily::Trash)
        .await
        .into_iter()
        .filter_map(|row| match row {
            MetadataRow::Trash(crate::metadata::TrashRecord {
                deletion_seq,
                action: loonfs_types::format::manifest::TrashRowAction::Listed { .. },
                ..
            }) => Some(deletion_seq),
            _ => None,
        })
        .collect()
}

/// Checks that a base compaction builds no kept inode filter, and reads none
/// of the slot index, while the basis manifest's retention floor is 0, even
/// with two deletions in the trash. Once a retention advance expires one of
/// them, the compaction builds the filter and walks the tree, even after a
/// base compaction of the `trash` family removed the expired deletion's
/// `listed` row, and the filter leaves out the hidden file and keeps the
/// file in the trash.
#[tokio::test]
async fn a_base_compaction_walks_the_tree_once_the_floor_is_above_zero() {
    let temp = tempdir().expect("tempdir");
    let store = RecordingStore::metadata_segments(LocalFsStore::new(temp.path()).expect("store"));
    let namespace_id = NamespaceId::parse("floor-walk").expect("namespace id");
    let context = writer_at("writer-1", 5_000);
    bootstrap_restricted_namespace(&store, &namespace_id, &context).await;
    put(&store, &namespace_id, "/old/a.txt", b"old", &context).await;
    put(&store, &namespace_id, "/trashed.txt", b"kept", &context).await;
    let a = inode_id_of(&store, &namespace_id, "/old/a.txt").await;
    let in_trash = inode_id_of(&store, &namespace_id, "/trashed.txt").await;
    let expired_seq = delete_recursively(&store, &namespace_id, "/old", &context).await;
    let trash_seq = delete_recursively(&store, &namespace_id, "/trashed.txt", &context).await;
    crate::test_support::namespace_store(&store, &namespace_id)
        .fold_wal()
        .await
        .expect("fold the deletions");
    assert_eq!(read_floor_seq(&store, &namespace_id).await, CommitSeq(0));

    let (filter, walk_reads) = kept_inode_filter_and_walk_reads(&store, &namespace_id).await;
    assert!(
        matches!(filter, KeptInodeFilter::EveryInode),
        "a retention floor of 0"
    );
    assert_eq!(walk_reads, 0);

    advance_floor_through(&store, &namespace_id, expired_seq, &context).await;
    compact_group(&store, &namespace_id, &context, MetadataFamilyGroup::Trash).await;
    assert_eq!(
        listed_deletions(&store, &namespace_id).await,
        vec![trash_seq],
        "the expired deletion's listed row is gone"
    );
    let (filter, walk_reads) = kept_inode_filter_and_walk_reads(&store, &namespace_id).await;
    assert!(walk_reads > 0, "the walk reads the slot index");
    let revision = |inode_id| {
        MetadataRow::FileRevision(crate::metadata::RevisionRecord {
            inode_id,
            revision_no: RevisionNo(1),
            committed_seq: CommitSeq(1),
            commit_id: CommitId::parse("c_probe").expect("commit id"),
            committed_by: loonfs_types::ActorId::loonfs(),
            committed_at_ms: 1_000,
            delta_index: 0,
            content_ref: loonfs_types::ContentRef::blob_v1(
                loonfs_types::ContentId::parse("con_00000000000000000000000000")
                    .expect("content id"),
                b"probe",
                loonfs_types::ChecksumAlgorithm::Crc64nvme,
            ),
        })
    };
    assert!(matches!(filter, KeptInodeFilter::Reached(_)));
    // A Bloom filter keeps every inode that the walk inserted, whatever its
    // seed, so the file in the trash is kept by every filter. The hidden file
    // isn't inserted, so some seed's filter rejects it. (The low bits of an
    // inode ID are random, so one filter can keep it by a false positive.)
    assert!(
        filter.keeps(&revision(in_trash)),
        "a file in the trash keeps its rows"
    );
    let mut rejected = false;
    for seed in 0..64 {
        let filter = seeded_kept_inode_filter(&store, &namespace_id, RunNo(seed)).await;
        assert!(filter.keeps(&revision(in_trash)), "seed {seed}");
        if !filter.keeps(&revision(a)) {
            rejected = true;
            break;
        }
    }
    assert!(rejected, "a hidden file loses its rows");
}

/// Checks that a base compaction of a group that stores rows by inode still
/// removes the rows that an expired deletion hides when a base compaction of
/// the `trash` family has already removed the deletion's `listed` row.
#[tokio::test]
async fn a_group_compacted_after_the_trash_still_removes_an_expired_deletions_rows() {
    let temp = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp.path()).expect("store");
    let namespace_id = NamespaceId::parse("trash-first").expect("namespace id");
    let context = writer_at("writer-1", 5_000);
    let (old, a) = expire_old(&store, &namespace_id, &context).await;
    let hidden = [old, a];
    let before = rows_of(&store, &namespace_id, &hidden).await;

    compact_group(&store, &namespace_id, &context, MetadataFamilyGroup::Trash).await;
    assert_eq!(
        rows_of(&store, &namespace_id, &hidden).await[&ApiMetadataRowFamily::Trash],
        Vec::new(),
        "the base compaction of trash removes the listed row"
    );

    compact_groups_without_false_positives(
        &store,
        &namespace_id,
        &[
            MetadataFamilyGroup::Revisions,
            MetadataFamilyGroup::Inodes,
            MetadataFamilyGroup::Bindings,
        ],
        &hidden,
    )
    .await;
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

/// Checks that a base compaction over the manifest of the retention
/// advance's fold removes every row that an expired deletion hides, with no
/// commit after the advance. At or below the floor, a deletion whose latest
/// event is `set` has expired, and nothing else has to happen before a
/// compaction removes its rows.
#[tokio::test]
async fn a_base_compaction_over_the_advance_removes_an_expired_deletions_rows() {
    let temp = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp.path()).expect("store");
    let namespace_id = NamespaceId::parse("advance-basis").expect("namespace id");
    let context = writer_at("writer-1", 5_000);
    let (old, a) = expire_old(&store, &namespace_id, &context).await;
    let hidden = [old, a];
    let before = rows_of(&store, &namespace_id, &hidden).await;

    // The advance's manifest is the current one, and no WAL object follows
    // it.
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let advanced = namespace
        .load_current_manifest()
        .await
        .expect("load the advance's manifest");
    let head = namespace.load_read_state().await.expect("read the head");
    assert_eq!(head.wal_no, advanced.folded_wal_no());
    assert!(advanced.retention_floor_seq() > CommitSeq(0));

    compact_groups_without_false_positives(
        &store,
        &namespace_id,
        &MetadataFamilyGroup::ALL,
        &hidden,
    )
    .await;
    let after = rows_of(&store, &namespace_id, &hidden).await;
    for family in [
        ApiMetadataRowFamily::Inodes,
        ApiMetadataRowFamily::Revisions,
        ApiMetadataRowFamily::DirentryBinds,
        ApiMetadataRowFamily::DirentryChildBinds,
        ApiMetadataRowFamily::Trash,
    ] {
        assert!(!before[&family].is_empty(), "{family:?} had hidden rows");
        assert_eq!(after[&family], Vec::new(), "{family:?} keeps no hidden row");
    }
    assert_eq!(
        namespace
            .load_read_state()
            .await
            .expect("read the head")
            .wal_no,
        head.wal_no,
        "the compactions wrote no WAL object"
    );
}
