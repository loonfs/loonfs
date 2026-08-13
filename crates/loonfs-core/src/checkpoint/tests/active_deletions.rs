//! The derived active-deletions family: its reducer, the fold that collapses
//! cancelled deletions, and the bounded trash listing it backs.
//!
//! The family is materialized by checkpoint publication, so it lives with the
//! other manifest-family tests; the listing is its only consumer and is
//! exercised here against real manifests rather than in isolation.

use super::*;
use crate::metadata::{
    active_tombstone_from_records, MetadataStateBuilder, SubtreeTombstoneAction,
    SubtreeTombstoneRecord,
};
use crate::path::read::{load_metadata_view, AttributeProjection, ReadLoadContext};
use loonfs_api::wire::manifest::{ActiveDeletionRowAction, DeletedDirentry, TombstoneGeneration};
use loonfs_api::{DisplayName, Page, PageRequest, TrashEntry, TrashPageCursor};

fn generation(seq: u64) -> TombstoneGeneration {
    TombstoneGeneration {
        seq: ChangeSeq(seq),
        delta_index: 0,
    }
}

fn tombstone_set(root_inode_id: InodeId, seq: u64, name: &str) -> SubtreeTombstoneRecord {
    SubtreeTombstoneRecord {
        root_inode_id,
        generation: generation(seq),
        deleted_at_ms: 1_000 + seq,
        action: SubtreeTombstoneAction::Set {
            deleted_direntry: Some(DeletedDirentry {
                parent_inode_id: InodeId(1),
                name_key: NameKey::parse(name).expect("name key"),
                display_name: DisplayName::parse(name).expect("display name"),
            }),
        },
    }
}

fn tombstone_revoke(root_inode_id: InodeId, seq: u64, target_seq: u64) -> SubtreeTombstoneRecord {
    SubtreeTombstoneRecord {
        root_inode_id,
        generation: generation(seq),
        deleted_at_ms: 1_000 + seq,
        action: SubtreeTombstoneAction::Revoke {
            target: generation(target_seq),
        },
    }
}

fn state_from_tombstones(tombstones: Vec<SubtreeTombstoneRecord>) -> MetadataState {
    let mut builder = MetadataStateBuilder::default();
    for tombstone in tombstones {
        builder.push_subtree_tombstone(tombstone);
    }
    builder.finish()
}

/// The family's rows for a state, as `(row key, listed or removed)` pairs in
/// scan order.
fn active_deletion_rows(state: &MetadataState) -> Vec<(String, &'static str)> {
    manifest_rows_for_family(state, ApiMetadataTableFamily::ActiveDeletions)
        .into_iter()
        .map(|row| {
            let row_key = row.row_key_for_family(ApiMetadataTableFamily::ActiveDeletions);
            let kind = match row {
                MetadataRow::ActiveDeletion {
                    action: ActiveDeletionRowAction::Listed { .. },
                    ..
                } => "listed",
                MetadataRow::ActiveDeletion {
                    action: ActiveDeletionRowAction::Removed { .. },
                    ..
                } => "removed",
                other => panic!("unexpected row in the active-deletions family: {other:?}"),
            };
            (row_key, kind)
        })
        .collect()
}

#[test]
fn a_delete_adds_a_row_an_undelete_removes_it_and_a_redelete_adds_a_new_one() {
    let deleted = state_from_tombstones(vec![tombstone_set(InodeId(7), 5, "notes.txt")]);
    assert_eq!(
        active_deletion_rows(&deleted),
        vec![(
            "active-deletion-00000000000000000005-00000000000000000007-1".to_owned(),
            "listed"
        )],
        "a delete adds exactly one listed row, keyed by its own generation"
    );

    let undeleted = state_from_tombstones(vec![
        tombstone_set(InodeId(7), 5, "notes.txt"),
        tombstone_revoke(InodeId(7), 9, 5),
    ]);
    assert_eq!(
        active_deletion_rows(&undeleted),
        vec![
            (
                "active-deletion-00000000000000000005-00000000000000000007-0".to_owned(),
                "removed"
            ),
            (
                "active-deletion-00000000000000000005-00000000000000000007-1".to_owned(),
                "listed"
            ),
        ],
        "an undelete adds a removal that sorts ahead of the row it removes"
    );

    let redeleted = state_from_tombstones(vec![
        tombstone_set(InodeId(7), 5, "notes.txt"),
        tombstone_revoke(InodeId(7), 9, 5),
        tombstone_set(InodeId(7), 12, "notes.txt"),
    ]);
    assert_eq!(
        active_deletion_rows(&redeleted)
            .into_iter()
            .filter(|(_, kind)| *kind == "listed")
            .map(|(row_key, _)| row_key)
            .collect::<Vec<_>>(),
        vec![
            "active-deletion-00000000000000000005-00000000000000000007-1".to_owned(),
            "active-deletion-00000000000000000012-00000000000000000007-1".to_owned(),
        ],
        "a re-delete lands at a new sequence and adds a new row"
    );
}

#[test]
fn a_removal_carries_the_undeletes_sequence_so_it_lands_in_that_commits_run() {
    let state = state_from_tombstones(vec![
        tombstone_set(InodeId(7), 5, "notes.txt"),
        tombstone_revoke(InodeId(7), 9, 5),
    ]);
    let delta_rows = super::super::row::manifest_rows_for_family_after_seq(
        &state,
        ApiMetadataTableFamily::ActiveDeletions,
        ChangeSeq(5),
    );
    assert_eq!(
        delta_rows.len(),
        1,
        "only the undelete's removal belongs to the run above sequence 5"
    );
    assert!(
        matches!(
            &delta_rows[0],
            MetadataRow::ActiveDeletion {
                action: ActiveDeletionRowAction::Removed { revoked_at_seq },
                ..
            } if *revoked_at_seq == ChangeSeq(9)
        ),
        "unexpected delta row: {:?}",
        delta_rows[0]
    );
}

#[test]
fn the_fold_drops_cancelled_pairs_and_keeps_every_live_deletion() {
    let state = state_from_tombstones(vec![
        tombstone_set(InodeId(7), 5, "notes.txt"),
        tombstone_revoke(InodeId(7), 9, 5),
        tombstone_set(InodeId(8), 11, "report.txt"),
        tombstone_set(InodeId(7), 12, "notes.txt"),
    ]);
    let mut rows_by_family = std::collections::BTreeMap::from([(
        ApiMetadataTableFamily::ActiveDeletions,
        manifest_rows_for_family(&state, ApiMetadataTableFamily::ActiveDeletions),
    )]);
    // Far above every row: the floor must not touch this family.
    fold_rows_with_retention(
        MetadataFamilyGroup::ActiveDeletions,
        &mut rows_by_family,
        ChangeSeq(10_000),
    )
    .expect("fold active deletions");

    let kept = rows_by_family
        .remove(&ApiMetadataTableFamily::ActiveDeletions)
        .expect("family rows")
        .into_iter()
        .map(|row| row.row_key_for_family(ApiMetadataTableFamily::ActiveDeletions))
        .collect::<Vec<_>>();
    assert_eq!(
        kept,
        vec![
            "active-deletion-00000000000000000011-00000000000000000008-1".to_owned(),
            "active-deletion-00000000000000000012-00000000000000000007-1".to_owned(),
        ],
        "the cancelled pair goes and both live deletions stay, however far the floor advanced"
    );
}

// ---------------------------------------------------------------------------
// The listing over real manifests
// ---------------------------------------------------------------------------

async fn submit_operation_for_test<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    commit_id: &str,
    operation: FilesystemOperation,
    context: &MutationContext,
) -> loonfs_api::CommitResponse {
    NamespaceCommitEngine::new(namespace_id.clone())
        .publish_batch(
            store,
            vec![CommitCandidate::prepared(
                CommitRequest::single(
                    CommitId::parse(commit_id).expect("commit id"),
                    loonfs_test_support::test_actor(),
                    None,
                    operation,
                ),
                Vec::new(),
            )],
            context,
            &PublishTailOptions::default(),
        )
        .await
        .results
        .pop()
        .expect("one result")
        .expect("commit accepted")
}

async fn undelete<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    commit_id: &str,
    inode_id: InodeId,
    deleted_at_seq: ChangeSeq,
    absolute_path: &str,
    context: &MutationContext,
) -> loonfs_api::CommitResponse {
    submit_operation_for_test(
        store,
        namespace_id,
        commit_id,
        FilesystemOperation::Undelete {
            inode_id,
            deleted_at_seq,
            path: Some(AbsolutePath::parse(absolute_path).expect("path")),
        },
        context,
    )
    .await
}

async fn trash_page<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    limit: u32,
    cursor: Option<TrashPageCursor>,
) -> Page<TrashEntry, TrashPageCursor> {
    let view = load_metadata_view(store, namespace_id, ReadLoadContext::latest())
        .await
        .expect("load read view");
    view.list_trash_page(PageRequest {
        cursor,
        limit: EffectiveLimit::new(NonZeroU32::new(limit).expect("non-zero limit")),
    })
    .await
    .expect("list trash page")
}

/// The inode a visible path resolves to, read before a delete hides it.
async fn inode_id_of<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
) -> InodeId {
    load_metadata_view(store, namespace_id, ReadLoadContext::latest())
        .await
        .expect("load read view")
        .resolve_path(absolute_path, AttributeProjection::Omit)
        .await
        .expect("resolve path")
        .inode_id
}

/// Every trash entry, paged to exhaustion.
async fn trash_entries<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    limit: u32,
) -> Vec<TrashEntry> {
    let mut entries = Vec::new();
    let mut cursor = None;
    loop {
        let page = trash_page(store, namespace_id, limit, cursor).await;
        entries.extend(page.items);
        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    entries
}

/// The listing algorithm the active-deletions family replaced: load every
/// tombstone event the namespace ever recorded, group by root, and reduce
/// newest-event-wins per root.
///
/// This lives only here, as the differential test's model. Production has one
/// implementation, and it is the range scan.
fn trash_by_walking_every_tombstone(state: &MetadataState, head_seq: ChangeSeq) -> Vec<TrashEntry> {
    let mut per_root: std::collections::BTreeMap<InodeId, Vec<SubtreeTombstoneRecord>> =
        std::collections::BTreeMap::new();
    for record in state.subtree_tombstones() {
        per_root
            .entry(record.root_inode_id)
            .or_default()
            .push(record.clone());
    }
    per_root
        .into_iter()
        .filter_map(|(root_inode_id, records)| {
            let active = active_tombstone_from_records(records, head_seq)?;
            let deleted_direntry = match active.action {
                SubtreeTombstoneAction::Set { deleted_direntry } => deleted_direntry,
                SubtreeTombstoneAction::Revoke { .. } => {
                    unreachable!("the active tombstone is a set by construction")
                }
            };
            Some(TrashEntry {
                root_inode_id,
                deleted_at_seq: active.generation.seq,
                deleted_at_ms: active.deleted_at_ms,
                parent_inode_id: deleted_direntry
                    .as_ref()
                    .map(|direntry| direntry.parent_inode_id),
                name_key: deleted_direntry
                    .as_ref()
                    .map(|direntry| direntry.name_key.clone()),
                display_name: deleted_direntry.map(|direntry| direntry.display_name),
            })
        })
        .collect()
}

/// Compares the two listings as sets. The family orders entries oldest
/// deletion first where the old walk ordered them by root inode, so order is
/// normalized away and pinned separately by
/// `the_listing_is_ordered_oldest_deletion_first`.
fn sorted_by_generation(mut entries: Vec<TrashEntry>) -> Vec<TrashEntry> {
    entries.sort_by_key(|entry| (entry.deleted_at_seq, entry.root_inode_id));
    entries
}

async fn assert_listing_matches_the_old_walk<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    step: usize,
) {
    let (head, state) = load_checkpoint_projection_metadata_state(store, namespace_id)
        .await
        .expect("load projection");
    let expected = sorted_by_generation(trash_by_walking_every_tombstone(&state, head.seq));
    // A limit of 2 forces the page machinery — cursors, removal markers
    // straddling page boundaries — on every step.
    let listed = sorted_by_generation(trash_entries(store, namespace_id, 2).await);
    assert_eq!(
        listed, expected,
        "step {step}: the family-backed listing must equal the old walk"
    );
}

/// Deterministic history driver. `rand` is not a dependency here (and ambient
/// randomness is banned), so the schedule is an explicit script that walks
/// every reducer transition and both nested cases.
#[derive(Debug, Clone, Copy)]
enum HistoryStep {
    Create(&'static str),
    Delete(&'static str),
    /// Undelete the deletion the named path produced, re-binding at the
    /// second path.
    Undelete(&'static str, &'static str),
}

#[tokio::test]
async fn the_family_backed_listing_equals_the_old_tombstone_walk_on_every_step() {
    let temp = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp.path()).expect("create local-fs store");
    let namespace_id = NamespaceId::parse("differential-trash").expect("namespace id");
    let mut context = mutation_context("writer-1", 5_000);
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap namespace");

    let script = [
        HistoryStep::Create("/a.txt"),
        HistoryStep::Create("/b.txt"),
        HistoryStep::Create("/dir/inner.txt"),
        // A plain delete, then its undelete, then a re-delete: the three
        // reducer transitions in order.
        HistoryStep::Delete("/a.txt"),
        HistoryStep::Undelete("/a.txt", "/a-restored.txt"),
        HistoryStep::Delete("/a-restored.txt"),
        // A deletion inside an already-deleted subtree: delete the child
        // first, then its parent, so two live deletions nest.
        HistoryStep::Delete("/dir/inner.txt"),
        HistoryStep::Delete("/dir"),
        // An undelete whose old parent is still deleted: the recovered
        // subtree re-binds elsewhere and the nested child stays deleted.
        HistoryStep::Undelete("/dir", "/dir-restored"),
        HistoryStep::Delete("/b.txt"),
        // Re-delete the recovered subtree, then recover it once more.
        HistoryStep::Delete("/dir-restored"),
        HistoryStep::Undelete("/dir-restored", "/dir-again"),
        HistoryStep::Undelete("/b.txt", "/b-back.txt"),
    ];

    let mut deletions: std::collections::HashMap<String, (InodeId, ChangeSeq)> =
        std::collections::HashMap::new();
    for (step, action) in script.into_iter().enumerate() {
        context = mutation_context("writer-1", 5_000 + step as u64);
        let commit_id = format!("com_step{step:028}");
        match action {
            HistoryStep::Create(path) => {
                write_test_file(&store, &namespace_id, path, &commit_id, &context).await;
            }
            HistoryStep::Delete(path) => {
                let deleted_inode_id = inode_id_of(&store, &namespace_id, path).await;
                let response = delete_path(&store, &namespace_id, path, &context, None)
                    .await
                    .expect("delete path");
                deletions.insert(path.to_owned(), (deleted_inode_id, response.committed_seq));
            }
            HistoryStep::Undelete(deleted_path, restored_path) => {
                let (inode_id, deleted_at_seq) = deletions
                    .remove(deleted_path)
                    .expect("undelete follows a recorded delete");
                undelete(
                    &store,
                    &namespace_id,
                    &commit_id,
                    inode_id,
                    deleted_at_seq,
                    restored_path,
                    &context,
                )
                .await;
            }
        }
        assert_listing_matches_the_old_walk(&store, &namespace_id, step).await;
        // Half the steps materialize into a manifest, so the comparison sees
        // both a WAL-tail-only listing and a durable one.
        if step % 2 == 1 {
            create_checkpoint(&store, &namespace_id, &context)
                .await
                .expect("create checkpoint");
            assert_listing_matches_the_old_walk(&store, &namespace_id, step).await;
        }
    }

    // Fold everything, then compare once more: the cancelled pairs are gone
    // from the family and the answer is unchanged.
    drain_reorganization(
        &store,
        &namespace_id,
        &context,
        MetadataLsmPolicy::default(),
    )
    .await;
    assert_listing_matches_the_old_walk(&store, &namespace_id, usize::MAX).await;
    assert!(
        !trash_entries(&store, &namespace_id, 2).await.is_empty(),
        "the script must leave live deletions, or the comparison proves nothing"
    );
}

#[tokio::test]
async fn the_listing_is_ordered_oldest_deletion_first() {
    let temp = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp.path()).expect("create local-fs store");
    let namespace_id = NamespaceId::parse("trash-order").expect("namespace id");
    let context = mutation_context("writer-1", 5_000);
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap namespace");

    for (index, name) in ["/a.txt", "/b.txt", "/c.txt"].into_iter().enumerate() {
        write_test_file(
            &store,
            &namespace_id,
            name,
            &format!("com_make{index:026}"),
            &context,
        )
        .await;
    }
    // Delete newest-inode first so root-inode order and deletion order
    // disagree.
    for name in ["/c.txt", "/a.txt", "/b.txt"] {
        delete_path(&store, &namespace_id, name, &context, None)
            .await
            .expect("delete path");
    }

    let entries = trash_entries(&store, &namespace_id, 10).await;
    let names = entries
        .iter()
        .map(|entry| {
            entry
                .display_name
                .as_ref()
                .expect("a path delete records the deleted name")
                .to_string()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        names,
        vec!["c.txt".to_owned(), "a.txt".to_owned(), "b.txt".to_owned()],
        "the trash lists deletions oldest first, not by root inode"
    );
    assert!(
        entries
            .windows(2)
            .all(|pair| (pair[0].deleted_at_seq, pair[0].root_inode_id)
                < (pair[1].deleted_at_seq, pair[1].root_inode_id)),
        "entries must ascend by (deleted_at_seq, root_inode_id): {entries:?}"
    );
}

#[tokio::test]
async fn trash_pages_resume_after_the_generation_the_cursor_names() {
    let temp = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp.path()).expect("create local-fs store");
    let namespace_id = NamespaceId::parse("trash-paging").expect("namespace id");
    let context = mutation_context("writer-1", 5_000);
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap namespace");
    for index in 0..5u32 {
        write_test_file(
            &store,
            &namespace_id,
            &format!("/file-{index}.txt"),
            &format!("com_page{index:028}"),
            &context,
        )
        .await;
        delete_path(
            &store,
            &namespace_id,
            &format!("/file-{index}.txt"),
            &context,
            None,
        )
        .await
        .expect("delete path");
    }
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("create checkpoint");

    let first = trash_page(&store, &namespace_id, 2, None).await;
    assert_eq!(
        first.items.len(),
        2,
        "a full page returns exactly the limit"
    );
    let cursor = first
        .next_cursor
        .clone()
        .expect("five deletions do not fit one page of two");
    assert_eq!(
        (cursor.last_deleted_at_seq, cursor.last_root_inode_id),
        (first.items[1].deleted_at_seq, first.items[1].root_inode_id),
        "the cursor names the generation the page ended on"
    );

    let encoded = loonfs_api::encode_cursor(&cursor).expect("encode cursor");
    let decoded: TrashPageCursor = loonfs_api::decode_cursor(&encoded).expect("decode cursor");
    assert_eq!(decoded, cursor, "the trash cursor round-trips on the wire");

    let second = trash_page(&store, &namespace_id, 2, Some(decoded)).await;
    assert_eq!(second.items.len(), 2);
    let third = trash_page(
        &store,
        &namespace_id,
        2,
        Some(second.next_cursor.clone().expect("a fifth entry remains")),
    )
    .await;
    assert_eq!(third.items.len(), 1, "the last page is short");
    assert!(
        third.next_cursor.is_none(),
        "a short page ends the listing without a cursor"
    );

    let all = trash_entries(&store, &namespace_id, 2).await;
    assert_eq!(all.len(), 5);
    assert_eq!(
        trash_entries(&store, &namespace_id, 100).await,
        all,
        "the paged walk and a single large page agree"
    );

    let wrong_kind = loonfs_api::encode_cursor(&loonfs_api::DirectoryPageCursor {
        head_seq: ChangeSeq(1),
        directory_inode_id: InodeId(1),
        last_name_key: NameKey::parse("x").expect("name key"),
    })
    .expect("encode cursor");
    assert!(
        loonfs_api::decode_cursor::<TrashPageCursor>(&wrong_kind).is_err(),
        "another endpoint's cursor must not resume a trash listing"
    );
}

#[tokio::test]
async fn a_deletion_far_below_the_retention_floor_still_lists_and_still_undeletes() {
    let temp = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp.path()).expect("create local-fs store");
    let namespace_id = NamespaceId::parse("trash-below-floor").expect("namespace id");
    let context = mutation_context("writer-1", 5_000);
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap namespace");

    write_test_file(
        &store,
        &namespace_id,
        "/old.txt",
        "com_floorseed0000000000000001",
        &context,
    )
    .await;
    let deleted_inode_id = inode_id_of(&store, &namespace_id, "/old.txt").await;
    let deleted = delete_path(&store, &namespace_id, "/old.txt", &context, None)
        .await
        .expect("delete path");

    // Push history far past the deletion, then move the floor up over it and
    // fold every run.
    for index in 0..6u32 {
        write_test_file(
            &store,
            &namespace_id,
            &format!("/later-{index}.txt"),
            &format!("com_floorfill{index:022}"),
            &context,
        )
        .await;
    }
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("create checkpoint");
    advance_retention_floor(&store, &namespace_id, &context)
        .await
        .expect("advance retention floor");
    drain_reorganization(
        &store,
        &namespace_id,
        &context,
        MetadataLsmPolicy::default(),
    )
    .await;
    let floor_seq = read_floor_seq(&store, &namespace_id).await;
    assert!(
        floor_seq > deleted.committed_seq,
        "the floor must have passed the deletion for this test to mean anything: \
         floor {floor_seq}, deletion {}",
        deleted.committed_seq
    );

    let entries = trash_entries(&store, &namespace_id, 10).await;
    assert_eq!(
        entries.len(),
        1,
        "a deletion below the floor stays listed: {entries:?}"
    );
    assert_eq!(entries[0].root_inode_id, deleted_inode_id);
    assert_eq!(entries[0].deleted_at_seq, deleted.committed_seq);

    undelete(
        &store,
        &namespace_id,
        "com_floorrecover000000000001",
        deleted_inode_id,
        deleted.committed_seq,
        "/recovered.txt",
        &context,
    )
    .await;
    assert!(
        trash_entries(&store, &namespace_id, 10).await.is_empty(),
        "recovery below the floor still works, and empties the trash"
    );
}

#[tokio::test]
async fn a_trash_page_costs_the_page_not_the_namespaces_deletion_history() {
    /// One page's reads over a namespace with `deletions` recoverable
    /// deletions, measured after the family is folded into its base.
    async fn page_reads(deletions: u32) -> usize {
        let temp = tempdir().expect("tempdir");
        let store = CountingStore::metadata_tables(
            LocalFsStore::new(temp.path()).expect("create local-fs store"),
        );
        let namespace_id = NamespaceId::parse("trash-bounded").expect("namespace id");
        let context = mutation_context("writer-1", 5_000);
        bootstrap_namespace(&store, &namespace_id, &context, false)
            .await
            .expect("bootstrap namespace");
        for index in 0..deletions {
            let path = format!("/doomed-{index}.txt");
            write_test_file(
                &store,
                &namespace_id,
                &path,
                &format!("com_bounded{index:022}"),
                &context,
            )
            .await;
            delete_path(&store, &namespace_id, &path, &context, None)
                .await
                .expect("delete path");
        }
        create_checkpoint(&store, &namespace_id, &context)
            .await
            .expect("create checkpoint");
        drain_reorganization(
            &store,
            &namespace_id,
            &context,
            MetadataLsmPolicy::default(),
        )
        .await;

        store.reset();
        let page = trash_page(&store, &namespace_id, 4, None).await;
        assert_eq!(
            page.items.len(),
            4,
            "the page must be full to be comparable"
        );
        store.count(OperationClass::Read)
    }

    let small = page_reads(8).await;
    let large = page_reads(32).await;
    assert_eq!(
        small, large,
        "a page of four costs the same over 8 and 32 deletions; \
         it read {small} then {large} metadata objects"
    );
    assert!(
        large < 32,
        "a bounded page must not approach one read per deletion: {large}"
    );
}

/// Pins what nesting means for the trash, because it is the case the family
/// most easily gets wrong. Deletions are per root, not per subtree: a child
/// deleted before its parent keeps its own entry while the parent's deletion
/// covers it, and recovering the parent leaves the child's own deletion
/// listed. The differential test is the authority that this equals the old
/// walk; this names the behavior.
#[tokio::test]
async fn nested_deletions_each_keep_their_own_entry() {
    let temp = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp.path()).expect("create local-fs store");
    let namespace_id = NamespaceId::parse("trash-nested").expect("namespace id");
    let context = mutation_context("writer-1", 5_000);
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap namespace");
    write_test_file(
        &store,
        &namespace_id,
        "/dir/inner.txt",
        "com_nested0000000000000000001",
        &context,
    )
    .await;

    let child_inode_id = inode_id_of(&store, &namespace_id, "/dir/inner.txt").await;
    delete_path(&store, &namespace_id, "/dir/inner.txt", &context, None)
        .await
        .expect("delete the child");
    let parent_inode_id = inode_id_of(&store, &namespace_id, "/dir").await;
    let parent_deleted = delete_path(&store, &namespace_id, "/dir", &context, None)
        .await
        .expect("delete the parent");

    let nested = trash_entries(&store, &namespace_id, 10).await;
    assert_eq!(
        nested
            .iter()
            .map(|entry| entry.root_inode_id)
            .collect::<Vec<_>>(),
        vec![child_inode_id, parent_inode_id],
        "a deletion inside an already-deleted subtree keeps its own entry"
    );

    undelete(
        &store,
        &namespace_id,
        "com_nested0000000000000000002",
        parent_inode_id,
        parent_deleted.committed_seq,
        "/dir-restored",
        &context,
    )
    .await;
    let after = trash_entries(&store, &namespace_id, 10).await;
    assert_eq!(
        after
            .iter()
            .map(|entry| entry.root_inode_id)
            .collect::<Vec<_>>(),
        vec![child_inode_id],
        "recovering the parent leaves the child's own deletion listed"
    );
}

#[tokio::test]
async fn a_deletion_committed_after_the_last_manifest_lists_immediately() {
    let temp = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp.path()).expect("create local-fs store");
    let namespace_id = NamespaceId::parse("trash-wal-tail").expect("namespace id");
    let context = mutation_context("writer-1", 5_000);
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap namespace");
    write_test_file(
        &store,
        &namespace_id,
        "/durable.txt",
        "com_tail000000000000000000001",
        &context,
    )
    .await;
    delete_path(&store, &namespace_id, "/durable.txt", &context, None)
        .await
        .expect("delete path");
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("create checkpoint");

    // This pair lands after the manifest and is only in the WAL tail.
    write_test_file(
        &store,
        &namespace_id,
        "/fresh.txt",
        "com_tail000000000000000000002",
        &context,
    )
    .await;
    let fresh = delete_path(&store, &namespace_id, "/fresh.txt", &context, None)
        .await
        .expect("delete path");

    let entries = trash_entries(&store, &namespace_id, 10).await;
    assert_eq!(entries.len(), 2, "{entries:?}");
    assert_eq!(
        entries[1].deleted_at_seq, fresh.committed_seq,
        "the unflushed deletion lists last, in deletion order"
    );

    // An undelete in the tail hides a deletion the manifest still lists.
    let durable_inode_id = entries[0].root_inode_id;
    let durable_seq = entries[0].deleted_at_seq;
    undelete(
        &store,
        &namespace_id,
        "com_tail000000000000000000003",
        durable_inode_id,
        durable_seq,
        "/durable-back.txt",
        &context,
    )
    .await;
    let entries = trash_entries(&store, &namespace_id, 10).await;
    assert_eq!(
        entries.len(),
        1,
        "an unflushed undelete must hide the durable row it cancels: {entries:?}"
    );
    assert_eq!(entries[0].deleted_at_seq, fresh.committed_seq);
}
