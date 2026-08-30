//! Path-intent planning, query visibility, and mutation semantics.

#![allow(clippy::panic)]
// These integration tests use panic in unexpected match arms for precise diagnostics.

use crate::common::commit_split_support::*;
use crate::common::{namespace_engine, read_context};
use loonfs_api::{
    v0::FilesystemChange,
    wire::wal::{decode_wal_segment_envelope_zstd, WalDelta},
    AbsolutePath, ChangeSeq, CommitId, DeleteDirectoryBehavior, DestinationBehavior,
    DirectoryPageCursor, InodeId, InodeKind, NameKey, NamespaceId, Page, PageRequest, PathEntry,
    RevisionNo,
};
use loonfs_core::commit::CommitValidationError;
use loonfs_core::content::store_bytes_as_content;
use loonfs_core::publish::{CommitCandidate, CommitRequest, FilesystemOperation};
use loonfs_core::{Error as CoreError, ErrorCode, MutationContext};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::ObjectStore;
use loonfs_test_support::ids::{namespace_id, page_limit};
use loonfs_test_support::stores::OperationClass;
use tempfile::tempdir;

fn named_entry(entry: &PathEntry) -> &str {
    entry
        .display_name
        .as_ref()
        .expect("non-root entry should carry a display name")
        .as_str()
}

async fn delete_path<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    context: &MutationContext,
    commit_id: Option<&str>,
) -> Result<loonfs_api::CommitResponse, CoreError> {
    submit_operation(
        store,
        namespace_id,
        test_commit_id(commit_id),
        FilesystemOperation::DeletePath {
            path: AbsolutePath::parse(absolute_path).expect("path"),
            behavior: DeleteDirectoryBehavior::Recursive,
            expected_inode_id: None,
        },
        context,
    )
    .await
}

async fn delete_path_non_recursive<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    context: &MutationContext,
    commit_id: Option<&str>,
) -> Result<loonfs_api::CommitResponse, CoreError> {
    submit_operation(
        store,
        namespace_id,
        test_commit_id(commit_id),
        FilesystemOperation::DeletePath {
            path: AbsolutePath::parse(absolute_path).expect("path"),
            behavior: DeleteDirectoryBehavior::NonRecursive,
            expected_inode_id: None,
        },
        context,
    )
    .await
}

async fn move_path<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    from_path: &str,
    to_path: &str,
    context: &MutationContext,
    commit_id: Option<&str>,
) -> Result<loonfs_api::CommitResponse, CoreError> {
    submit_operation(
        store,
        namespace_id,
        test_commit_id(commit_id),
        FilesystemOperation::MovePath {
            from_path: AbsolutePath::parse(from_path).expect("path"),
            to_path: AbsolutePath::parse(to_path).expect("path"),
            behavior: DestinationBehavior::NoReplace,
            destination_expected_inode_id: None,
            destination_expected_revision_no: None,
        },
        context,
    )
    .await
}

async fn copy_file_path<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    from_path: &str,
    to_path: &str,
    context: &MutationContext,
    commit_id: Option<&str>,
) -> Result<loonfs_api::CommitResponse, CoreError> {
    submit_operation(
        store,
        namespace_id,
        test_commit_id(commit_id),
        FilesystemOperation::CopyPath {
            from_path: AbsolutePath::parse(from_path).expect("path"),
            to_path: AbsolutePath::parse(to_path).expect("path"),
            behavior: DestinationBehavior::NoReplace,
            destination_expected_inode_id: None,
            destination_expected_revision_no: None,
        },
        context,
    )
    .await
}

async fn put_file_with_guards<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    bytes: &[u8],
    behavior: DestinationBehavior,
    expected_inode_id: Option<InodeId>,
    expected_revision_no: Option<RevisionNo>,
    context: &MutationContext,
    commit_id: &str,
) -> Result<loonfs_api::CommitResponse, CoreError> {
    let content = store_bytes_as_content(store, namespace_id, bytes).await?;
    submit_operation(
        store,
        namespace_id,
        CommitId::parse(commit_id).expect("valid commit id"),
        FilesystemOperation::PutFile {
            path: AbsolutePath::parse(absolute_path).expect("path"),
            content_ref: content.into_content_ref(),
            behavior,
            expected_inode_id,
            expected_revision_no,
        },
        context,
    )
    .await
}

async fn restore_file_revision<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    source_revision_no: RevisionNo,
    context: &MutationContext,
    commit_id: Option<&str>,
) -> Result<loonfs_api::CommitResponse, CoreError> {
    submit_operation(
        store,
        namespace_id,
        test_commit_id(commit_id),
        FilesystemOperation::RestoreRevision {
            path: AbsolutePath::parse(absolute_path).expect("path"),
            source_revision_no,
        },
        context,
    )
    .await
}

async fn list_path<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
) -> Result<Vec<loonfs_api::PathEntry>, CoreError> {
    let context = read_context(store, namespace_id).await;
    let engine = namespace_engine(store, namespace_id, &mutation_context());
    let mut entries = Vec::new();
    let mut cursor = None;
    loop {
        let page = engine
            .list_path_page(
                absolute_path,
                PageRequest {
                    limit: page_limit(1_000),
                    cursor,
                },
                loonfs_api::options::ListPathEntriesOptions::default(),
                &context,
            )
            .await?;
        entries.extend(page.items);
        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => return Ok(entries),
        }
    }
}

async fn list_path_page<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    limit: u32,
    cursor: Option<DirectoryPageCursor>,
) -> Result<Page<PathEntry, DirectoryPageCursor>, CoreError> {
    let context = read_context(store, namespace_id).await;
    namespace_engine(store, namespace_id, &mutation_context())
        .list_path_page(
            absolute_path,
            PageRequest {
                limit: page_limit(limit),
                cursor,
            },
            loonfs_api::options::ListPathEntriesOptions::default(),
            &context,
        )
        .await
}

/// Drains the paged revision listing — the only revision-listing operation
/// the stack exposes — so guards can assert over a file's full history.
async fn list_file_revisions<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
) -> Result<loonfs_api::ListFileRevisionsResponse, CoreError> {
    let entry = resolve_path(store, namespace_id, absolute_path).await?;
    list_file_revisions_for_inode(store, namespace_id, absolute_path, entry.inode_id).await
}

async fn list_file_revisions_for_inode<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    _absolute_path: &str,
    inode_id: InodeId,
) -> Result<loonfs_api::ListFileRevisionsResponse, CoreError> {
    let context = read_context(store, namespace_id).await;
    let engine = namespace_engine(store, namespace_id, &mutation_context());
    let mut revisions = Vec::new();
    let mut cursor = None;
    loop {
        let page = engine
            .list_file_revisions_for_inode_page(
                inode_id,
                PageRequest {
                    limit: page_limit(2),
                    cursor: cursor.clone(),
                },
                &context,
            )
            .await?;
        revisions.extend(page.items);
        cursor = page.next_cursor;
        if cursor.is_none() {
            return Ok(loonfs_api::ListFileRevisionsResponse {
                namespace_id: namespace_id.clone(),
                inode_id,
                head_seq: context.head.seq,
                revisions,
                next_cursor: None,
            });
        }
    }
}

async fn read_file_revision_bytes<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    revision_no: RevisionNo,
) -> Result<loonfs_api::FileBytes, CoreError> {
    let context = read_context(store, namespace_id).await;
    namespace_engine(store, namespace_id, &mutation_context())
        .get_file_revision(absolute_path, revision_no, &context, None)
        .await
}

async fn read_file_revision_bytes_for_inode<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    revision_no: RevisionNo,
) -> Result<Vec<u8>, CoreError> {
    let context = read_context(store, namespace_id).await;
    namespace_engine(store, namespace_id, &mutation_context())
        .get_file_revision_for_inode(inode_id, revision_no, &context, None)
        .await
}

async fn resolve_path_latest<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
) -> Result<loonfs_api::PathEntry, CoreError> {
    resolve_path(store, namespace_id, absolute_path).await
}

async fn list_path_latest<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
) -> Result<Vec<loonfs_api::PathEntry>, CoreError> {
    list_path(store, namespace_id, absolute_path).await
}

#[tokio::test]
async fn metadata_queries_do_not_get_content_blobs_but_file_reads_do_once() {
    let temp_dir = tempdir().expect("tempdir");
    let store = content_blob_counting_store(temp_dir.path());
    let context = mutation_context();
    let namespace_id = namespace_id("demo");

    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    create_directory_path(&store, &namespace_id, "/docs", &context, Some("mkdir-docs"))
        .await
        .expect("create docs");

    for index in 0..3 {
        let path = format!("/docs/file-{index}.txt");
        let bytes = format!("file-{index}-bytes");
        let commit_id = format!("put-file-{index}");
        put_file_bytes(
            &store,
            &namespace_id,
            &path,
            bytes.as_bytes(),
            DestinationBehavior::NoReplace,
            &context,
            Some(&commit_id),
        )
        .await
        .expect("put file");
    }

    store.reset();
    let stat = resolve_path(&store, &namespace_id, "/docs/file-1.txt")
        .await
        .expect("stat file");
    assert_eq!(stat.inode_kind(), InodeKind::File);
    assert_eq!(stat.size_bytes(), Some("file-1-bytes".len() as u64));
    assert!(stat.content_ref().is_some());
    assert_eq!(store.count(OperationClass::Read), 0);

    store.reset();
    let entries = list_path(&store, &namespace_id, "/docs")
        .await
        .expect("list docs");
    assert_eq!(entries.len(), 3);
    for entry in entries {
        assert_eq!(entry.inode_kind(), InodeKind::File);
        assert_eq!(entry.size_bytes(), Some("file-0-bytes".len() as u64));
        assert!(entry.content_ref().is_some());
    }
    assert_eq!(store.count(OperationClass::Read), 0);

    store.reset();
    let read = read_file_bytes(&store, &namespace_id, "/docs/file-1.txt")
        .await
        .expect("read file");
    assert_eq!(read.bytes, b"file-1-bytes");
    assert_eq!(store.count(OperationClass::Read), 1);
}

#[tokio::test]
async fn query_driven_stat_and_list_use_metadata_view_with_delta_run_and_wal_overlay() {
    let temp_dir = tempdir().expect("tempdir");
    let store = content_blob_counting_store(temp_dir.path());
    let context = mutation_context();
    let namespace_id = namespace_id("demo");

    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/a.txt",
        b"alpha",
        DestinationBehavior::NoReplace,
        &context,
        Some("put-alpha"),
    )
    .await
    .expect("put alpha");
    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/b.txt",
        b"bravo",
        DestinationBehavior::NoReplace,
        &context,
        Some("put-bravo"),
    )
    .await
    .expect("put bravo");
    put_file_bytes(
        &store,
        &namespace_id,
        "/dead/leaf.txt",
        b"dead",
        DestinationBehavior::NoReplace,
        &context,
        Some("put-dead"),
    )
    .await
    .expect("put dead");
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("base checkpoint");

    move_path(
        &store,
        &namespace_id,
        "/docs/a.txt",
        "/docs/moved.txt",
        &context,
        Some("move-alpha"),
    )
    .await
    .expect("move alpha");
    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/b.txt",
        b"bravo two",
        DestinationBehavior::Replace,
        &context,
        Some("replace-bravo"),
    )
    .await
    .expect("replace bravo");
    delete_path(
        &store,
        &namespace_id,
        "/dead",
        &context,
        Some("delete-dead"),
    )
    .await
    .expect("delete dead");
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("delta checkpoint");

    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/wal.txt",
        b"wal",
        DestinationBehavior::NoReplace,
        &context,
        Some("put-wal"),
    )
    .await
    .expect("put wal tail");

    let expected_stat = resolve_path(&store, &namespace_id, "/docs/moved.txt")
        .await
        .expect("stat");
    let expected_list = list_path(&store, &namespace_id, "/docs")
        .await
        .expect("list");
    let expected_file_list = list_path(&store, &namespace_id, "/docs/moved.txt")
        .await
        .expect("file list");

    store.reset();
    let actual_stat = resolve_path_latest(&store, &namespace_id, "/docs/moved.txt")
        .await
        .expect("materialized stat");
    let actual_list = list_path_latest(&store, &namespace_id, "/docs")
        .await
        .expect("materialized list");
    let actual_file_list = list_path_latest(&store, &namespace_id, "/docs/moved.txt")
        .await
        .expect("materialized file list");
    let actual_casefold_stat = resolve_path_latest(&store, &namespace_id, "/DOCS/MOVED.TXT")
        .await
        .expect("materialized casefold stat");

    assert_eq!(actual_stat, expected_stat);
    assert_eq!(actual_casefold_stat, expected_stat);
    assert_eq!(actual_list, expected_list);
    assert_eq!(actual_file_list, expected_file_list);
    assert_eq!(store.count(OperationClass::Read), 0);

    assert!(resolve_path_latest(&store, &namespace_id, "/docs/a.txt")
        .await
        .is_err());
    assert!(resolve_path_latest(&store, &namespace_id, "/dead/leaf.txt")
        .await
        .is_err());
}

#[tokio::test]
async fn query_driven_stat_uses_exact_name_key_for_dash_containing_siblings() {
    let temp_dir = tempdir().expect("tempdir");
    let store = content_blob_counting_store(temp_dir.path());
    let context = mutation_context();
    let namespace_id = namespace_id("demo");

    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/report",
        b"short",
        DestinationBehavior::NoReplace,
        &context,
        Some("put-report"),
    )
    .await
    .expect("put report");
    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/report-2024",
        b"newer-longer",
        DestinationBehavior::NoReplace,
        &context,
        Some("put-report-2024"),
    )
    .await
    .expect("put report-2024");
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("checkpoint");

    let expected = resolve_path(&store, &namespace_id, "/docs/report")
        .await
        .expect("stat");
    let actual = resolve_path_latest(&store, &namespace_id, "/docs/report")
        .await
        .expect("materialized stat");

    assert_eq!(actual, expected);
    assert_eq!(actual.path.as_str(), "/docs/report");
    assert_eq!(actual.size_bytes(), Some(5));
}

#[tokio::test]
async fn wide_directory_listing_resolves_tail_unbinds_cross_directory_renames_and_rebinds() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    let namespace_id = namespace_id("demo");

    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    create_directory_path(&store, &namespace_id, "/wide", &context, Some("mkdir-wide"))
        .await
        .expect("create wide");
    create_directory_path(
        &store,
        &namespace_id,
        "/other",
        &context,
        Some("mkdir-other"),
    )
    .await
    .expect("create other");

    // More names than one raw scan chunk, so group boundaries cross
    // manifest pages; a few names rebound several times, so groups carry
    // more than one row.
    for index in 0..70u32 {
        let path = format!("/wide/file-{index:03}.txt");
        put_file_bytes(
            &store,
            &namespace_id,
            &path,
            b"first",
            DestinationBehavior::NoReplace,
            &context,
            Some(&format!("put-{index:03}")),
        )
        .await
        .expect("put file");
    }
    for index in [3u32, 35, 68] {
        let path = format!("/wide/file-{index:03}.txt");
        put_file_bytes(
            &store,
            &namespace_id,
            &path,
            b"rebound",
            DestinationBehavior::Replace,
            &context,
            Some(&format!("rebind-{index:03}")),
        )
        .await
        .expect("rebind file");
    }
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("checkpoint wide dir");

    // Tail-only mutations over checkpointed state: a delete whose unbind
    // lives only in the WAL tail, and a rename into another directory whose
    // current binding lives outside the listed parent.
    delete_path(
        &store,
        &namespace_id,
        "/wide/file-010.txt",
        &context,
        Some("delete-tail-unbind"),
    )
    .await
    .expect("delete over checkpointed bind");
    move_path(
        &store,
        &namespace_id,
        "/wide/file-020.txt",
        "/other/moved-away.txt",
        &context,
        Some("move-cross-directory"),
    )
    .await
    .expect("move into other directory");
    put_file_bytes(
        &store,
        &namespace_id,
        "/wide/file-005.txt",
        b"tail-rebound",
        DestinationBehavior::Replace,
        &context,
        Some("tail-rebind-005"),
    )
    .await
    .expect("tail rebind over checkpointed bind");

    let mut listed = Vec::new();
    let mut cursor = None;
    loop {
        let page = list_path_page(&store, &namespace_id, "/wide", 16, cursor.clone())
            .await
            .expect("list page");
        listed.extend(page.items.iter().map(|entry| named_entry(entry).to_owned()));
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }

    let expected: Vec<String> = (0..70u32)
        .filter(|index| *index != 10 && *index != 20)
        .map(|index| format!("file-{index:03}.txt"))
        .collect();
    assert_eq!(listed, expected);

    // The tail rebind is the visible revision, not the checkpointed one.
    let rebound = read_file_bytes(&store, &namespace_id, "/wide/file-005.txt")
        .await
        .expect("read tail-rebound file");
    assert_eq!(rebound.bytes, b"tail-rebound");
    // The moved child is visible under its new parent only.
    let moved = read_file_bytes(&store, &namespace_id, "/other/moved-away.txt")
        .await
        .expect("read moved file");
    assert_eq!(moved.bytes, b"first");
}

#[tokio::test]
async fn query_driven_directory_page_merges_manifest_and_tail_visible_children() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    let namespace_id = namespace_id("demo");

    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    create_directory_path(&store, &namespace_id, "/docs", &context, Some("mkdir-docs"))
        .await
        .expect("create docs");
    create_directory_path(
        &store,
        &namespace_id,
        "/docs/a-dir",
        &context,
        Some("mkdir-a-dir"),
    )
    .await
    .expect("create a-dir");
    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/c-file.txt",
        b"charlie",
        DestinationBehavior::NoReplace,
        &context,
        Some("put-c-file"),
    )
    .await
    .expect("put c-file");
    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/stale.txt",
        b"stale",
        DestinationBehavior::NoReplace,
        &context,
        Some("put-stale"),
    )
    .await
    .expect("put stale");
    move_path(
        &store,
        &namespace_id,
        "/docs/stale.txt",
        "/docs/b-renamed.txt",
        &context,
        Some("move-stale"),
    )
    .await
    .expect("move stale");
    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/dead/leaf.txt",
        b"dead",
        DestinationBehavior::NoReplace,
        &context,
        Some("put-dead"),
    )
    .await
    .expect("put dead");
    delete_path(
        &store,
        &namespace_id,
        "/docs/dead",
        &context,
        Some("delete-dead"),
    )
    .await
    .expect("delete dead");
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("checkpoint manifest children");

    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/d-tail.txt",
        b"delta",
        DestinationBehavior::NoReplace,
        &context,
        Some("put-d-tail"),
    )
    .await
    .expect("put d-tail");
    create_directory_path(
        &store,
        &namespace_id,
        "/docs/e-tail-dir",
        &context,
        Some("mkdir-e-tail-dir"),
    )
    .await
    .expect("create tail dir");
    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/tail-dead/leaf.txt",
        b"tail-dead",
        DestinationBehavior::NoReplace,
        &context,
        Some("put-tail-dead"),
    )
    .await
    .expect("put tail dead");
    delete_path(
        &store,
        &namespace_id,
        "/docs/tail-dead",
        &context,
        Some("delete-tail-dead"),
    )
    .await
    .expect("delete tail dead");

    let first = list_path_page(&store, &namespace_id, "/docs", 2, None)
        .await
        .expect("first page");
    assert_eq!(
        first.items.iter().map(named_entry).collect::<Vec<_>>(),
        vec!["a-dir", "b-renamed.txt"]
    );
    let second = list_path_page(&store, &namespace_id, "/docs", 2, first.next_cursor.clone())
        .await
        .expect("second page");
    assert_eq!(
        second.items.iter().map(named_entry).collect::<Vec<_>>(),
        vec!["c-file.txt", "d-tail.txt"]
    );
    let third = list_path_page(
        &store,
        &namespace_id,
        "/docs",
        2,
        second.next_cursor.clone(),
    )
    .await
    .expect("third page");
    assert!(third.next_cursor.is_none());
    assert_eq!(
        third.items.iter().map(named_entry).collect::<Vec<_>>(),
        vec!["e-tail-dir"]
    );

    let entries = first
        .items
        .into_iter()
        .chain(second.items)
        .chain(third.items)
        .collect::<Vec<_>>();
    assert_eq!(
        entries.iter().map(named_entry).collect::<Vec<_>>(),
        vec![
            "a-dir",
            "b-renamed.txt",
            "c-file.txt",
            "d-tail.txt",
            "e-tail-dir"
        ]
    );
    assert!(entries
        .iter()
        .all(|entry| !matches!(named_entry(entry), "stale.txt" | "dead" | "tail-dead")));

    for directory_name in ["a-dir", "e-tail-dir"] {
        let entry = entries
            .iter()
            .find(|entry| named_entry(entry) == directory_name)
            .expect("directory entry");
        assert_eq!(entry.inode_kind(), InodeKind::Directory);
        assert_eq!(entry.revision_no(), None);
        assert_eq!(entry.size_bytes(), None);
        assert!(entry.content_ref().is_none());
    }

    for (file_name, size) in [
        ("b-renamed.txt", b"stale".len() as u64),
        ("c-file.txt", b"charlie".len() as u64),
        ("d-tail.txt", b"delta".len() as u64),
    ] {
        let entry = entries
            .iter()
            .find(|entry| named_entry(entry) == file_name)
            .expect("file entry");
        assert_eq!(entry.inode_kind(), InodeKind::File);
        assert_eq!(entry.revision_no(), Some(RevisionNo(1)));
        assert_eq!(entry.size_bytes(), Some(size));
        assert!(entry.content_ref().is_some());
    }
}

#[tokio::test]
async fn revision_history_and_change_feed_name_the_same_supplied_commit() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    let supplied_commit_id = CommitId::parse("c_cross_projection").expect("commit id");

    let response = write_file_bytes(
        &store,
        &namespace_id,
        "/docs/provenance.txt",
        b"provenance",
        &context,
        Some(supplied_commit_id.as_str()),
    )
    .await
    .expect("commit file mutation");
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("checkpoint committed row");

    let revisions = list_file_revisions(&store, &namespace_id, "/docs/provenance.txt")
        .await
        .expect("read revision history");
    let revision = revisions
        .revisions
        .iter()
        .find(|revision| revision.committed_seq == response.committed_seq)
        .expect("committed revision");
    let changes = list_changes_after(&store, &namespace_id, ChangeSeq(0))
        .await
        .expect("read change feed");
    let change = changes
        .changes
        .iter()
        .find(|change| change.committed_seq == response.committed_seq)
        .expect("committed change");

    assert_eq!(revision.commit_id, change.commit_id);
    assert_eq!(revision.commit_id, supplied_commit_id);
}

#[tokio::test]
async fn revision_queries_read_historical_bytes_and_path_restore_appends_revision() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");

    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/rev.txt",
        b"one",
        DestinationBehavior::NoReplace,
        &context,
        Some("rev-create"),
    )
    .await
    .expect("create file");
    let replacement_context = MutationContext {
        writer_id: context.writer_id.clone(),
        now_ms: 2_000,
    };
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/rev.txt",
        b"two",
        &replacement_context,
        Some("rev-replace"),
    )
    .await
    .expect("replace file");

    let entry = resolve_path(&store, &namespace_id, "/docs/rev.txt")
        .await
        .expect("stat file");
    let inode_id = entry.inode_id;
    let path_revisions = list_file_revisions(&store, &namespace_id, "/docs/rev.txt")
        .await
        .expect("path revisions");
    assert_eq!(path_revisions.inode_id, inode_id);
    assert_eq!(
        path_revisions
            .revisions
            .iter()
            .map(|revision| revision.revision_no)
            .collect::<Vec<_>>(),
        vec![RevisionNo(2), RevisionNo(1)]
    );

    let historical =
        read_file_revision_bytes(&store, &namespace_id, "/docs/rev.txt", RevisionNo(1))
            .await
            .expect("read first revision");
    assert_eq!(historical.bytes, b"one");
    assert_eq!(
        historical.entry.revision_committed_at_ms(),
        Some(context.now_ms)
    );
    assert_eq!(
        entry.revision_committed_at_ms(),
        Some(replacement_context.now_ms)
    );
    let inode_historical =
        read_file_revision_bytes_for_inode(&store, &namespace_id, inode_id, RevisionNo(2))
            .await
            .expect("read inode revision");
    assert_eq!(inode_historical, b"two");

    move_path(
        &store,
        &namespace_id,
        "/docs/rev.txt",
        "/docs/moved.txt",
        &context,
        Some("rev-move"),
    )
    .await
    .expect("move file");
    assert_eq!(
        list_file_revisions(&store, &namespace_id, "/docs/rev.txt")
            .await
            .expect_err("old path no longer resolves")
            .code(),
        ErrorCode::PathNotFound
    );
    let inode_revisions =
        list_file_revisions_for_inode(&store, &namespace_id, "/docs/moved.txt", inode_id)
            .await
            .expect("inode revisions");
    assert_eq!(inode_revisions.revisions.len(), 2);

    restore_file_revision(
        &store,
        &namespace_id,
        "/docs/moved.txt",
        RevisionNo(1),
        &context,
        Some("rev-restore"),
    )
    .await
    .expect("restore first revision");
    let restored = read_file_bytes(&store, &namespace_id, "/docs/moved.txt")
        .await
        .expect("read restored");
    assert_eq!(restored.bytes, b"one");
    let restored_revisions =
        list_file_revisions_for_inode(&store, &namespace_id, "/docs/moved.txt", inode_id)
            .await
            .expect("restored revisions");
    assert_eq!(
        restored_revisions
            .revisions
            .iter()
            .map(|revision| revision.revision_no)
            .collect::<Vec<_>>(),
        vec![RevisionNo(3), RevisionNo(2), RevisionNo(1)]
    );
}

#[tokio::test]
async fn name_key_stays_typed_through_planning_and_fingerprint() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    // A spelling whose canonical lookup key differs from it: planning must
    // derive the key from the path component and carry it typed all the way
    // into the durable binding.
    let request = CommitRequest::single(
        CommitId::parse("typed-name-key").expect("valid commit id"),
        loonfs_test_support::test_actor(),
        Some("typed boundary".to_owned()),
        FilesystemOperation::CreateDirectory {
            path: AbsolutePath::parse("/Caf\u{e9}").expect("path"),
            parents: false,
        },
    );
    let expected_name_key = NameKey::parse("caf\u{e9}").expect("valid name key");
    // Identity is computed from the request alone, before anything is
    // published, so the committed record must carry exactly this value.
    let expected_fingerprint = CommitCandidate::new(request.clone())
        .semantic_identity(&namespace_id)
        .expect("fingerprint mutation request");

    submit_commit(&store, &namespace_id, request, &context)
        .await
        .expect("commit typed name key");

    let wal_keys = store
        .list_prefix("namespaces/demo/wal/segments/")
        .await
        .expect("list wal");
    assert_eq!(wal_keys.len(), 1);
    let wal_bytes = store
        .get(&wal_keys[0], None)
        .await
        .expect("read wal")
        .expect("wal exists");
    let segment = decode_wal_segment_envelope_zstd(&wal_bytes).expect("decode segment");
    assert_eq!(segment.payload.records.len(), 1);
    assert_eq!(
        segment.payload.records[0].semantic_commit_fingerprint,
        expected_fingerprint.as_str()
    );
    assert!(segment.payload.records[0]
        .deltas
        .iter()
        .any(|delta| matches!(
            &delta.delta,
            WalDelta::BindDirentry { name_key, .. } if name_key == &expected_name_key
        )));
}

#[tokio::test]
async fn path_intents_in_one_batch_see_tentative_state_and_continue_the_seq_ladder() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    let content = store_bytes_as_content(&store, &namespace_id, b"hello")
        .await
        .expect("stage content");

    let responses = publish_namespace_commits_batch(
        &store,
        &namespace_id,
        vec![
            prepared_candidate(
                &store,
                &namespace_id,
                CommitRequest::single(
                    CommitId::parse("put-batched-path").expect("valid commit id"),
                    loonfs_test_support::test_actor(),
                    None,
                    FilesystemOperation::PutFile {
                        path: AbsolutePath::parse("/docs/a.txt").expect("path"),
                        content_ref: content.into_content_ref(),
                        behavior: DestinationBehavior::NoReplace,
                        expected_inode_id: None,
                        expected_revision_no: None,
                    },
                ),
            )
            .await,
            CommitCandidate::new(CommitRequest::single(
                CommitId::parse("move-batched-path").expect("valid commit id"),
                loonfs_test_support::test_actor(),
                None,
                FilesystemOperation::MovePath {
                    from_path: AbsolutePath::parse("/docs/a.txt").expect("path"),
                    to_path: AbsolutePath::parse("/docs/b.txt").expect("path"),
                    behavior: DestinationBehavior::NoReplace,
                    destination_expected_inode_id: None,
                    destination_expected_revision_no: None,
                },
            )),
        ],
        &context,
    )
    .await;

    assert_eq!(
        responses[0].as_ref().expect("put").committed_seq,
        ChangeSeq(1)
    );
    assert_eq!(
        responses[1].as_ref().expect("move").committed_seq,
        ChangeSeq(2)
    );
    let moved_bytes = read_file_bytes(&store, &namespace_id, "/docs/b.txt")
        .await
        .expect("read moved file");
    assert_eq!(moved_bytes.bytes, b"hello");

    let wal_keys = store
        .list_prefix("namespaces/demo/wal/segments/")
        .await
        .expect("list wal");
    assert_eq!(wal_keys.len(), 1);
    let wal_bytes = store
        .get(&wal_keys[0], None)
        .await
        .expect("read wal")
        .expect("wal exists");
    let segment = decode_wal_segment_envelope_zstd(&wal_bytes).expect("decode segment");
    assert_eq!(segment.payload.records.len(), 2);

    let copied = copy_file_path(
        &store,
        &namespace_id,
        "/docs/b.txt",
        "/docs/c.txt",
        &context,
        Some("copy-path"),
    )
    .await
    .expect("copy path");
    assert_eq!(copied.committed_seq, ChangeSeq(3));

    let deleted = delete_path_non_recursive(
        &store,
        &namespace_id,
        "/docs/b.txt",
        &context,
        Some("delete-path"),
    )
    .await
    .expect("delete path");
    assert_eq!(deleted.committed_seq, ChangeSeq(4));

    let copied_bytes = read_file_bytes(&store, &namespace_id, "/docs/c.txt")
        .await
        .expect("read copied file");
    assert_eq!(copied_bytes.bytes, b"hello");
}

#[tokio::test]
async fn move_path_respells_the_same_slot_without_a_conflict() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id("demo"), &context, false)
        .await
        .expect("bootstrap namespace");
    write_file_bytes(
        &store,
        &namespace_id("demo"),
        "/docs/report.pdf",
        b"alpha",
        &context,
        Some("seed-respell"),
    )
    .await
    .expect("seed respell");
    let before = resolve_path_latest(&store, &namespace_id("demo"), "/docs/report.pdf")
        .await
        .expect("resolve before respell");

    // Case-only rename: same name key under the same parent, new spelling.
    move_path(
        &store,
        &namespace_id("demo"),
        "/docs/report.pdf",
        "/docs/REPORT.PDF",
        &context,
        Some("respell"),
    )
    .await
    .expect("case-only respelling lands");

    let after = resolve_path_latest(&store, &namespace_id("demo"), "/docs/report.pdf")
        .await
        .expect("resolve after respell");
    assert_eq!(named_entry(&after), "REPORT.PDF");
    assert_eq!(after.inode_id, before.inode_id);

    // Repeating the identical spelling changes nothing and stays a conflict.
    let error = move_path(
        &store,
        &namespace_id("demo"),
        "/docs/REPORT.PDF",
        "/docs/REPORT.PDF",
        &context,
        Some("respell-noop"),
    )
    .await
    .expect_err("unchanged spelling");
    assert_eq!(error.code(), ErrorCode::PathConflict);
}

#[tokio::test]
async fn write_and_move_under_deleted_ancestor_start_fresh_subtrees() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id("demo"), &context, false)
        .await
        .expect("bootstrap namespace");
    write_file_bytes(
        &store,
        &namespace_id("demo"),
        "/docs/old.txt",
        b"old",
        &context,
        Some("seed-docs"),
    )
    .await
    .expect("seed docs");
    write_file_bytes(
        &store,
        &namespace_id("demo"),
        "/tmp/source.txt",
        b"source",
        &context,
        Some("seed-source"),
    )
    .await
    .expect("seed source");
    delete_path(
        &store,
        &namespace_id("demo"),
        "/docs",
        &context,
        Some("delete-docs"),
    )
    .await
    .expect("delete docs");

    // Deleting `/docs` unbinds the name: it is reusable immediately, and
    // writes and moves under it create a fresh subtree rather than
    // conflicting with the dead one. The old subtree stays dead — its
    // previous child is not resurrected by the recreation.
    write_file_bytes(
        &store,
        &namespace_id("demo"),
        "/docs/new.txt",
        b"new",
        &context,
        Some("put-under-recreated"),
    )
    .await
    .expect("put recreates the subtree");
    let old_child = resolve_path(&store, &namespace_id("demo"), "/docs/old.txt").await;
    assert!(old_child.is_err(), "dead subtree child must stay invisible");

    move_path(
        &store,
        &namespace_id("demo"),
        "/tmp/source.txt",
        "/docs/source.txt",
        &context,
        Some("move-under-recreated"),
    )
    .await
    .expect("move lands in the recreated subtree");
}

#[tokio::test]
async fn create_directory_path_creates_directory_without_auto_parents() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id("demo"), &context, false)
        .await
        .expect("bootstrap namespace");

    let created = create_directory_path(
        &store,
        &namespace_id("demo"),
        "/docs",
        &context,
        Some("mkdir-docs"),
    )
    .await
    .expect("create dir");
    assert_eq!(created.committed_seq, ChangeSeq(1));
    let docs = resolve_path(&store, &namespace_id("demo"), "/docs")
        .await
        .expect("resolve docs");
    assert_eq!(docs.inode_kind(), InodeKind::Directory);

    let missing_parent = create_directory_path(
        &store,
        &namespace_id("demo"),
        "/missing/nested",
        &context,
        Some("mkdir-missing-parent"),
    )
    .await
    .expect_err("mkdir does not auto-create parents");
    assert_eq!(missing_parent.code(), ErrorCode::PathNotFound);
}

#[tokio::test]
async fn path_move_writes_unbind_and_old_binding_stops_resolving() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id("demo"), &context, false)
        .await
        .expect("bootstrap namespace");
    write_file_bytes(
        &store,
        &namespace_id("demo"),
        "/docs/a.txt",
        b"hello",
        &context,
        Some("seed-a"),
    )
    .await
    .expect("seed file");
    let file = resolve_path(&store, &namespace_id("demo"), "/docs/a.txt")
        .await
        .expect("resolve file");

    move_path(
        &store,
        &namespace_id("demo"),
        "/docs/a.txt",
        "/docs/b.txt",
        &context,
        Some("move-a-to-b"),
    )
    .await
    .expect("move file");
    let move_count = list_changes_after(&store, &namespace_id("demo"), ChangeSeq(0))
        .await
        .expect("change feed")
        .changes
        .iter()
        .flat_map(|change| &change.events)
        .filter(|event| matches!(event, FilesystemChange::Moved { .. }))
        .count();
    assert_eq!(move_count, 1);
    assert!(resolve_path(&store, &namespace_id("demo"), "/docs/a.txt")
        .await
        .is_err());
    resolve_path(&store, &namespace_id("demo"), "/docs/b.txt")
        .await
        .expect("new path visible");

    // The unbind is what makes the old binding dead: an operation aimed at
    // the source path finds nothing, while the same inode is still reachable
    // — and still deletable under a guard naming it — at the destination.
    let stale_binding = submit_operation(
        &store,
        &namespace_id("demo"),
        CommitId::parse("delete-old-binding").expect("valid commit id"),
        FilesystemOperation::DeletePath {
            path: AbsolutePath::parse("/docs/a.txt").expect("path"),
            behavior: DeleteDirectoryBehavior::NonRecursive,
            expected_inode_id: Some(file.inode_id),
        },
        &context,
    )
    .await
    .expect_err("the pre-move binding should no longer resolve");
    assert_eq!(stale_binding.code(), ErrorCode::PathNotFound);

    submit_operation(
        &store,
        &namespace_id("demo"),
        CommitId::parse("delete-new-binding").expect("valid commit id"),
        FilesystemOperation::DeletePath {
            path: AbsolutePath::parse("/docs/b.txt").expect("path"),
            behavior: DeleteDirectoryBehavior::NonRecursive,
            expected_inode_id: Some(file.inode_id),
        },
        &context,
    )
    .await
    .expect("the moved inode keeps its identity under the new binding");
}

#[tokio::test]
async fn no_replace_put_rejects_an_existing_name_and_an_equivalent_spelling() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id("demo"), &context, false)
        .await
        .expect("bootstrap namespace");
    write_file_bytes(
        &store,
        &namespace_id("demo"),
        "/docs/hello.txt",
        b"hello",
        &context,
        Some("seed-hello"),
    )
    .await
    .expect("seed file");
    write_file_bytes(
        &store,
        &namespace_id("demo"),
        "/Cafe\u{0301}.txt",
        b"hello",
        &context,
        Some("seed-unicode-name"),
    )
    .await
    .expect("seed unicode name");

    let error = put_file_bytes(
        &store,
        &namespace_id("demo"),
        "/docs/hello.txt",
        b"new-bytes",
        DestinationBehavior::NoReplace,
        &context,
        Some("put-no-force"),
    )
    .await
    .expect_err("put without force");
    assert_eq!(error.code(), ErrorCode::PathConflict);

    let error = put_file_bytes(
        &store,
        &namespace_id("demo"),
        "/CAF\u{00c9}.TXT",
        b"new-bytes",
        DestinationBehavior::NoReplace,
        &context,
        Some("create-only-conflict"),
    )
    .await
    .expect_err("create-only conflict");
    assert_eq!(error.code(), ErrorCode::PathConflict);
}

#[tokio::test]
async fn put_replace_guards_cover_identity_revision_and_delete_recreate() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    let namespace_id = namespace_id("demo");
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");

    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/guarded.txt",
        b"first",
        &context,
        Some("seed-guarded-put"),
    )
    .await
    .expect("seed guarded file");
    let observed = resolve_path(&store, &namespace_id, "/docs/guarded.txt")
        .await
        .expect("observe guarded file");
    let wrong_inode = InodeId(999_999);
    let error = put_file_with_guards(
        &store,
        &namespace_id,
        "/docs/guarded.txt",
        b"wrong inode",
        DestinationBehavior::Replace,
        Some(wrong_inode),
        Some(RevisionNo(1)),
        &context,
        "put-wrong-inode",
    )
    .await
    .expect_err("a mismatching inode guard must fail");
    assert!(matches!(
        error,
        CoreError::CommitValidation(CommitValidationError::BindingPreconditionMismatch {
            expected_child_inode_id,
            actual_child_inode_id,
            ..
        }) if expected_child_inode_id == wrong_inode && actual_child_inode_id == observed.inode_id
    ));
    assert_eq!(
        read_file_bytes(&store, &namespace_id, "/docs/guarded.txt")
            .await
            .expect("read unchanged file")
            .bytes,
        b"first"
    );

    put_file_with_guards(
        &store,
        &namespace_id,
        "/docs/guarded.txt",
        b"second",
        DestinationBehavior::Replace,
        Some(observed.inode_id),
        Some(RevisionNo(1)),
        &context,
        "put-correct-guards",
    )
    .await
    .expect("the observed inode and revision must replace");
    assert_eq!(
        read_file_bytes(&store, &namespace_id, "/docs/guarded.txt")
            .await
            .expect("read replaced file")
            .bytes,
        b"second"
    );

    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/aba.txt",
        b"old inode",
        &context,
        Some("seed-put-aba"),
    )
    .await
    .expect("seed ABA file");
    let old = resolve_path(&store, &namespace_id, "/docs/aba.txt")
        .await
        .expect("observe ABA file");
    delete_path(
        &store,
        &namespace_id,
        "/docs/aba.txt",
        &context,
        Some("delete-put-aba"),
    )
    .await
    .expect("delete observed file");
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/aba.txt",
        b"new inode",
        &context,
        Some("recreate-put-aba"),
    )
    .await
    .expect("recreate ABA path");
    let recreated = resolve_path(&store, &namespace_id, "/docs/aba.txt")
        .await
        .expect("observe recreated file");
    assert_ne!(old.inode_id, recreated.inode_id);
    assert_eq!(old.revision_no(), Some(RevisionNo(1)));
    assert_eq!(recreated.revision_no(), Some(RevisionNo(1)));

    let error = put_file_with_guards(
        &store,
        &namespace_id,
        "/docs/aba.txt",
        b"must not land",
        DestinationBehavior::Replace,
        Some(old.inode_id),
        Some(RevisionNo(1)),
        &context,
        "guarded-put-aba",
    )
    .await
    .expect_err("the old identity must reject the recreated file");
    match error {
        CoreError::CommitValidation(CommitValidationError::BindingPreconditionMismatch {
            expected_child_inode_id,
            actual_child_inode_id,
            ..
        }) => {
            assert_eq!(expected_child_inode_id, old.inode_id);
            assert_eq!(actual_child_inode_id, recreated.inode_id);
        }
        other => panic!("expected binding mismatch, got {other:?}"),
    }
    put_file_with_guards(
        &store,
        &namespace_id,
        "/docs/aba.txt",
        b"revision-only landed",
        DestinationBehavior::Replace,
        None,
        Some(RevisionNo(1)),
        &context,
        "revision-only-put-aba",
    )
    .await
    .expect("the revision-only ABA shape still matches the new inode");

    for (commit_id, expected_inode_id, expected_revision_no) in [
        ("put-inode-guard-no-replace", Some(observed.inode_id), None),
        ("put-revision-guard-no-replace", None, Some(RevisionNo(2))),
    ] {
        let error = put_file_with_guards(
            &store,
            &namespace_id,
            "/docs/guarded.txt",
            b"invalid",
            DestinationBehavior::NoReplace,
            expected_inode_id,
            expected_revision_no,
            &context,
            commit_id,
        )
        .await
        .expect_err("a put guard contradicts no-replace");
        assert_eq!(error.code(), ErrorCode::InvalidRequest);
    }
    let error = put_file_with_guards(
        &store,
        &namespace_id,
        "/docs/missing.txt",
        b"missing",
        DestinationBehavior::Replace,
        Some(observed.inode_id),
        Some(RevisionNo(2)),
        &context,
        "guarded-put-missing",
    )
    .await
    .expect_err("a guard asserts an existing path");
    assert_eq!(error.code(), ErrorCode::PathNotFound);
}

#[tokio::test]
async fn delete_path_non_recursive_rejects_non_empty_directory() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id("demo"), &context, false)
        .await
        .expect("bootstrap namespace");
    write_file_bytes(
        &store,
        &namespace_id("demo"),
        "/docs/hello.txt",
        b"hello",
        &context,
        Some("seed-docs"),
    )
    .await
    .expect("seed file");

    let error = delete_path_non_recursive(
        &store,
        &namespace_id("demo"),
        "/docs",
        &context,
        Some("delete-docs"),
    )
    .await
    .expect_err("non-recursive delete should reject non-empty dir");
    assert!(matches!(error, CoreError::DirectoryNotEmpty(path) if path == "/docs"));
}

#[tokio::test]
async fn copy_file_path_creates_new_inode_and_reuses_content_blob() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id("demo"), &context, false)
        .await
        .expect("bootstrap namespace");
    write_file_bytes(
        &store,
        &namespace_id("demo"),
        "/docs/source.txt",
        b"hello",
        &context,
        Some("seed-source"),
    )
    .await
    .expect("seed source");

    copy_file_path(
        &store,
        &namespace_id("demo"),
        "/docs/source.txt",
        "/docs/copy.txt",
        &context,
        Some("copy-file"),
    )
    .await
    .expect("copy file");

    let source = resolve_path(&store, &namespace_id("demo"), "/docs/source.txt")
        .await
        .expect("source stat");
    let copy = resolve_path(&store, &namespace_id("demo"), "/docs/copy.txt")
        .await
        .expect("copy stat");
    assert_ne!(source.inode_id, copy.inode_id);
    assert_eq!(
        source.content_ref(),
        copy.content_ref(),
        "copy should reuse stored content ref"
    );
}

#[tokio::test]
async fn resolve_path_uses_nfc_casefold_folding() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    let stored_path = "/Cafe\u{0301}.txt";
    let lookup_path = "/CAF\u{00c9}.TXT";
    bootstrap_namespace(&store, &namespace_id("demo"), &context, false)
        .await
        .expect("bootstrap namespace");
    write_file_bytes(
        &store,
        &namespace_id("demo"),
        stored_path,
        b"hello",
        &context,
        Some("seed-unicode-name"),
    )
    .await
    .expect("seed unicode name");

    let resolved = resolve_path(&store, &namespace_id("demo"), lookup_path)
        .await
        .expect("resolve path");
    assert_eq!(resolved.path.as_str(), stored_path);
    assert_eq!(named_entry(&resolved), "Cafe\u{0301}.txt");
}

#[tokio::test]
async fn tombstoned_children_stay_unlisted_and_live_entries_keep_revision_data() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id("demo"), &context, false)
        .await
        .expect("bootstrap namespace");

    create_directory_path(&store, &namespace_id("demo"), "/live", &context, None)
        .await
        .expect("create live dir");
    create_directory_path(&store, &namespace_id("demo"), "/dead", &context, None)
        .await
        .expect("create dead dir");
    put_file_bytes(
        &store,
        &namespace_id("demo"),
        "/live/kept.txt",
        b"alive",
        DestinationBehavior::NoReplace,
        &context,
        None,
    )
    .await
    .expect("put live file");
    put_file_bytes(
        &store,
        &namespace_id("demo"),
        "/dead/gone.txt",
        b"doomed",
        DestinationBehavior::NoReplace,
        &context,
        None,
    )
    .await
    .expect("put doomed file");
    delete_path(&store, &namespace_id("demo"), "/dead", &context, None)
        .await
        .expect("delete dead subtree");

    let root_entries = list_path(&store, &namespace_id("demo"), "/")
        .await
        .expect("list root");
    let root_names: Vec<&str> = root_entries
        .iter()
        .map(|entry| entry.path.as_str())
        .collect();
    assert!(
        root_names.contains(&"/live"),
        "live directory must list, got {root_names:?}"
    );
    assert!(
        !root_names.iter().any(|path| path.starts_with("/dead")),
        "tombstoned directory must not list, got {root_names:?}"
    );

    let live_entries = list_path(&store, &namespace_id("demo"), "/live")
        .await
        .expect("list live dir");
    assert_eq!(live_entries.len(), 1, "one live child");
    let kept = &live_entries[0];
    assert_eq!(kept.path.as_str(), "/live/kept.txt");
    assert_eq!(
        kept.size_bytes(),
        Some(b"alive".len() as u64),
        "listed entry carries its revision's size"
    );
    assert!(
        kept.revision_no().is_some(),
        "listed entry carries its revision number"
    );
    assert!(
        kept.content_ref().is_some(),
        "listed entry carries its content ref"
    );

    resolve_path(&store, &namespace_id("demo"), "/dead/gone.txt")
        .await
        .expect_err("file under tombstoned subtree must not resolve");
    resolve_path(&store, &namespace_id("demo"), "/dead")
        .await
        .expect_err("tombstoned directory must not resolve");
}

#[tokio::test]
async fn move_replace_atomically_replaces_a_file_destination() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    let namespace_id = namespace_id("demo");

    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/a.txt",
        b"alpha",
        DestinationBehavior::NoReplace,
        &context,
        Some("put-a"),
    )
    .await
    .expect("put a");
    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/b.txt",
        b"beta",
        DestinationBehavior::NoReplace,
        &context,
        Some("put-b"),
    )
    .await
    .expect("put b");

    // The default stays create-only: an occupied destination is a conflict.
    let error = submit_operation(
        &store,
        &namespace_id,
        CommitId::parse("move-no-replace").expect("valid commit id"),
        FilesystemOperation::MovePath {
            from_path: AbsolutePath::parse("/docs/a.txt").expect("path"),
            to_path: AbsolutePath::parse("/docs/b.txt").expect("path"),
            behavior: DestinationBehavior::NoReplace,
            destination_expected_inode_id: None,
            destination_expected_revision_no: None,
        },
        &context,
    )
    .await
    .expect_err("no-replace move onto an occupied name fails");
    assert!(matches!(error, CoreError::DestinationExists { .. }));

    // Replace compiles to one commit: the destination file's delete and the
    // source's rebind land atomically.
    submit_operation(
        &store,
        &namespace_id,
        CommitId::parse("move-replace").expect("valid commit id"),
        FilesystemOperation::MovePath {
            from_path: AbsolutePath::parse("/docs/a.txt").expect("path"),
            to_path: AbsolutePath::parse("/docs/b.txt").expect("path"),
            behavior: DestinationBehavior::Replace,
            destination_expected_inode_id: None,
            destination_expected_revision_no: None,
        },
        &context,
    )
    .await
    .expect("replace move");

    let read = read_file_bytes(&store, &namespace_id, "/docs/b.txt")
        .await
        .expect("read destination");
    assert_eq!(read.bytes, b"alpha");
    read_file_bytes(&store, &namespace_id, "/docs/a.txt")
        .await
        .expect_err("source path is gone");
}

#[tokio::test]
async fn move_replace_rejects_directory_destinations_and_self_moves() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    let namespace_id = namespace_id("demo");

    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/a.txt",
        b"alpha",
        DestinationBehavior::NoReplace,
        &context,
        Some("put-a"),
    )
    .await
    .expect("put a");
    create_directory_path(&store, &namespace_id, "/docs/dir", &context, Some("mkdir"))
        .await
        .expect("mkdir");

    // Mirrors put: only a file destination can be replaced.
    let error = submit_operation(
        &store,
        &namespace_id,
        CommitId::parse("move-onto-dir").expect("valid commit id"),
        FilesystemOperation::MovePath {
            from_path: AbsolutePath::parse("/docs/a.txt").expect("path"),
            to_path: AbsolutePath::parse("/docs/dir").expect("path"),
            behavior: DestinationBehavior::Replace,
            destination_expected_inode_id: None,
            destination_expected_revision_no: None,
        },
        &context,
    )
    .await
    .expect_err("replace move onto a directory fails");
    assert!(matches!(error, CoreError::ExpectedFile { .. }));

    // A path never replaces itself, force or not.
    let error = submit_operation(
        &store,
        &namespace_id,
        CommitId::parse("move-onto-self").expect("valid commit id"),
        FilesystemOperation::MovePath {
            from_path: AbsolutePath::parse("/docs/a.txt").expect("path"),
            to_path: AbsolutePath::parse("/docs/a.txt").expect("path"),
            behavior: DestinationBehavior::Replace,
            destination_expected_inode_id: None,
            destination_expected_revision_no: None,
        },
        &context,
    )
    .await
    .expect_err("replace move onto itself fails");
    assert!(matches!(error, CoreError::DestinationExists { .. }));
}

#[tokio::test]
async fn copy_replace_appends_a_revision_to_the_destination_inode() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    let namespace_id = namespace_id("demo");

    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/a.txt",
        b"alpha",
        DestinationBehavior::NoReplace,
        &context,
        Some("put-a"),
    )
    .await
    .expect("put a");
    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/b.txt",
        b"beta",
        DestinationBehavior::NoReplace,
        &context,
        Some("put-b"),
    )
    .await
    .expect("put b");
    let before = list_file_revisions(&store, &namespace_id, "/docs/b.txt")
        .await
        .expect("destination revisions before");

    submit_operation(
        &store,
        &namespace_id,
        CommitId::parse("copy-replace").expect("valid commit id"),
        FilesystemOperation::CopyPath {
            from_path: AbsolutePath::parse("/docs/a.txt").expect("path"),
            to_path: AbsolutePath::parse("/docs/b.txt").expect("path"),
            behavior: DestinationBehavior::Replace,
            destination_expected_inode_id: None,
            destination_expected_revision_no: None,
        },
        &context,
    )
    .await
    .expect("replace copy");

    // Mirrors put onto an existing file: the destination keeps its inode and
    // revision history, gaining one revision with the source's content.
    let read = read_file_bytes(&store, &namespace_id, "/docs/b.txt")
        .await
        .expect("read destination");
    assert_eq!(read.bytes, b"alpha");
    let after = list_file_revisions(&store, &namespace_id, "/docs/b.txt")
        .await
        .expect("destination revisions after");
    assert_eq!(after.inode_id, before.inode_id);
    assert_eq!(after.revisions.len(), before.revisions.len() + 1);
    let source = read_file_bytes(&store, &namespace_id, "/docs/a.txt")
        .await
        .expect("source still read");
    assert_eq!(source.bytes, b"alpha");
}

#[tokio::test]
async fn move_replace_guards_validate_the_destination() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    let namespace_id = namespace_id("demo");
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    for (path, bytes, commit_id) in [
        ("/docs/source.txt", b"source".as_slice(), "seed-move-source"),
        (
            "/docs/destination.txt",
            b"destination".as_slice(),
            "seed-move-destination",
        ),
        (
            "/docs/vacant-source.txt",
            b"vacant source".as_slice(),
            "seed-vacant-move-source",
        ),
    ] {
        write_file_bytes(
            &store,
            &namespace_id,
            path,
            bytes,
            &context,
            Some(commit_id),
        )
        .await
        .expect("seed move fixture");
    }
    let source = resolve_path(&store, &namespace_id, "/docs/source.txt")
        .await
        .expect("observe source");
    let destination = resolve_path(&store, &namespace_id, "/docs/destination.txt")
        .await
        .expect("observe destination");

    let error = submit_operation(
        &store,
        &namespace_id,
        test_commit_id(Some("move-wrong-destination-inode")),
        FilesystemOperation::MovePath {
            from_path: AbsolutePath::parse("/docs/source.txt").expect("path"),
            to_path: AbsolutePath::parse("/docs/destination.txt").expect("path"),
            behavior: DestinationBehavior::Replace,
            destination_expected_inode_id: Some(source.inode_id),
            destination_expected_revision_no: Some(RevisionNo(1)),
        },
        &context,
    )
    .await
    .expect_err("a wrong destination inode must fail");
    assert!(matches!(
        error,
        CoreError::CommitValidation(CommitValidationError::BindingPreconditionMismatch {
            expected_child_inode_id,
            actual_child_inode_id,
            ..
        }) if expected_child_inode_id == source.inode_id
            && actual_child_inode_id == destination.inode_id
    ));
    assert_eq!(
        read_file_bytes(&store, &namespace_id, "/docs/destination.txt")
            .await
            .expect("destination survives wrong inode")
            .bytes,
        b"destination"
    );

    let error = submit_operation(
        &store,
        &namespace_id,
        test_commit_id(Some("move-stale-destination-revision")),
        FilesystemOperation::MovePath {
            from_path: AbsolutePath::parse("/docs/source.txt").expect("path"),
            to_path: AbsolutePath::parse("/docs/destination.txt").expect("path"),
            behavior: DestinationBehavior::Replace,
            destination_expected_inode_id: Some(destination.inode_id),
            destination_expected_revision_no: Some(RevisionNo(2)),
        },
        &context,
    )
    .await
    .expect_err("a stale destination revision must fail");
    assert_eq!(error.code(), ErrorCode::StaleRevision);

    submit_operation(
        &store,
        &namespace_id,
        test_commit_id(Some("move-correct-destination-guards")),
        FilesystemOperation::MovePath {
            from_path: AbsolutePath::parse("/docs/source.txt").expect("path"),
            to_path: AbsolutePath::parse("/docs/destination.txt").expect("path"),
            behavior: DestinationBehavior::Replace,
            destination_expected_inode_id: Some(destination.inode_id),
            destination_expected_revision_no: Some(RevisionNo(1)),
        },
        &context,
    )
    .await
    .expect("the observed destination guards must allow the move");
    assert_eq!(
        read_file_bytes(&store, &namespace_id, "/docs/destination.txt")
            .await
            .expect("read moved source")
            .bytes,
        b"source"
    );

    let error = submit_operation(
        &store,
        &namespace_id,
        test_commit_id(Some("move-guarded-vacant-destination")),
        FilesystemOperation::MovePath {
            from_path: AbsolutePath::parse("/docs/vacant-source.txt").expect("path"),
            to_path: AbsolutePath::parse("/docs/vacant.txt").expect("path"),
            behavior: DestinationBehavior::Replace,
            destination_expected_inode_id: Some(destination.inode_id),
            destination_expected_revision_no: Some(RevisionNo(1)),
        },
        &context,
    )
    .await
    .expect_err("destination guards assert an occupied destination");
    assert_eq!(error.code(), ErrorCode::PathNotFound);
}

#[tokio::test]
async fn copy_replace_guards_validate_the_destination_and_close_aba() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    let namespace_id = namespace_id("demo");
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    for (path, bytes, commit_id) in [
        ("/docs/source.txt", b"source".as_slice(), "seed-copy-source"),
        (
            "/docs/destination.txt",
            b"destination".as_slice(),
            "seed-copy-destination",
        ),
        (
            "/docs/aba-destination.txt",
            b"old destination".as_slice(),
            "seed-copy-aba-destination",
        ),
    ] {
        write_file_bytes(
            &store,
            &namespace_id,
            path,
            bytes,
            &context,
            Some(commit_id),
        )
        .await
        .expect("seed copy fixture");
    }
    let source = resolve_path(&store, &namespace_id, "/docs/source.txt")
        .await
        .expect("observe copy source");
    let destination = resolve_path(&store, &namespace_id, "/docs/destination.txt")
        .await
        .expect("observe copy destination");

    let error = submit_operation(
        &store,
        &namespace_id,
        test_commit_id(Some("copy-wrong-destination-inode")),
        FilesystemOperation::CopyPath {
            from_path: AbsolutePath::parse("/docs/source.txt").expect("path"),
            to_path: AbsolutePath::parse("/docs/destination.txt").expect("path"),
            behavior: DestinationBehavior::Replace,
            destination_expected_inode_id: Some(source.inode_id),
            destination_expected_revision_no: Some(RevisionNo(1)),
        },
        &context,
    )
    .await
    .expect_err("a wrong copy destination inode must fail");
    assert!(matches!(
        error,
        CoreError::CommitValidation(CommitValidationError::BindingPreconditionMismatch {
            expected_child_inode_id,
            actual_child_inode_id,
            ..
        }) if expected_child_inode_id == source.inode_id
            && actual_child_inode_id == destination.inode_id
    ));
    assert_eq!(
        read_file_bytes(&store, &namespace_id, "/docs/destination.txt")
            .await
            .expect("destination survives wrong copy inode")
            .bytes,
        b"destination"
    );

    let error = submit_operation(
        &store,
        &namespace_id,
        test_commit_id(Some("copy-stale-destination-revision")),
        FilesystemOperation::CopyPath {
            from_path: AbsolutePath::parse("/docs/source.txt").expect("path"),
            to_path: AbsolutePath::parse("/docs/destination.txt").expect("path"),
            behavior: DestinationBehavior::Replace,
            destination_expected_inode_id: Some(destination.inode_id),
            destination_expected_revision_no: Some(RevisionNo(2)),
        },
        &context,
    )
    .await
    .expect_err("a stale copy destination revision must fail");
    assert_eq!(error.code(), ErrorCode::StaleRevision);

    submit_operation(
        &store,
        &namespace_id,
        test_commit_id(Some("copy-correct-destination-guards")),
        FilesystemOperation::CopyPath {
            from_path: AbsolutePath::parse("/docs/source.txt").expect("path"),
            to_path: AbsolutePath::parse("/docs/destination.txt").expect("path"),
            behavior: DestinationBehavior::Replace,
            destination_expected_inode_id: Some(destination.inode_id),
            destination_expected_revision_no: Some(RevisionNo(1)),
        },
        &context,
    )
    .await
    .expect("the observed copy destination guards must succeed");
    let copied = resolve_path(&store, &namespace_id, "/docs/destination.txt")
        .await
        .expect("observe copied destination");
    assert_eq!(copied.inode_id, destination.inode_id);
    assert_eq!(copied.revision_no(), Some(RevisionNo(2)));
    assert_eq!(
        read_file_bytes(&store, &namespace_id, "/docs/destination.txt")
            .await
            .expect("read copied content")
            .bytes,
        b"source"
    );

    let error = submit_operation(
        &store,
        &namespace_id,
        test_commit_id(Some("copy-guarded-vacant-destination")),
        FilesystemOperation::CopyPath {
            from_path: AbsolutePath::parse("/docs/source.txt").expect("path"),
            to_path: AbsolutePath::parse("/docs/vacant.txt").expect("path"),
            behavior: DestinationBehavior::Replace,
            destination_expected_inode_id: Some(destination.inode_id),
            destination_expected_revision_no: Some(RevisionNo(2)),
        },
        &context,
    )
    .await
    .expect_err("copy destination guards assert an occupied path");
    assert_eq!(error.code(), ErrorCode::PathNotFound);

    let old = resolve_path(&store, &namespace_id, "/docs/aba-destination.txt")
        .await
        .expect("observe ABA destination");
    delete_path(
        &store,
        &namespace_id,
        "/docs/aba-destination.txt",
        &context,
        Some("delete-copy-aba-destination"),
    )
    .await
    .expect("delete ABA destination");
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/aba-destination.txt",
        b"new destination",
        &context,
        Some("recreate-copy-aba-destination"),
    )
    .await
    .expect("recreate ABA destination");
    let recreated = resolve_path(&store, &namespace_id, "/docs/aba-destination.txt")
        .await
        .expect("observe recreated destination");
    assert_ne!(old.inode_id, recreated.inode_id);
    assert_eq!(old.revision_no(), recreated.revision_no());

    let error = submit_operation(
        &store,
        &namespace_id,
        test_commit_id(Some("guarded-copy-aba")),
        FilesystemOperation::CopyPath {
            from_path: AbsolutePath::parse("/docs/source.txt").expect("path"),
            to_path: AbsolutePath::parse("/docs/aba-destination.txt").expect("path"),
            behavior: DestinationBehavior::Replace,
            destination_expected_inode_id: Some(old.inode_id),
            destination_expected_revision_no: Some(RevisionNo(1)),
        },
        &context,
    )
    .await
    .expect_err("the old copy destination identity must reject the recreated file");
    assert!(matches!(
        error,
        CoreError::CommitValidation(CommitValidationError::BindingPreconditionMismatch {
            expected_child_inode_id,
            actual_child_inode_id,
            ..
        }) if expected_child_inode_id == old.inode_id
            && actual_child_inode_id == recreated.inode_id
    ));
}

#[tokio::test]
async fn move_by_inode_threads_destination_guards() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    let namespace_id = namespace_id("demo");
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/source.txt",
        b"source",
        &context,
        Some("seed-inode-move-source"),
    )
    .await
    .expect("seed source");
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/destination.txt",
        b"destination",
        &context,
        Some("seed-inode-move-destination"),
    )
    .await
    .expect("seed destination");
    let docs = resolve_path(&store, &namespace_id, "/docs")
        .await
        .expect("resolve destination parent");
    let source = resolve_path(&store, &namespace_id, "/docs/source.txt")
        .await
        .expect("resolve source");
    let destination = resolve_path(&store, &namespace_id, "/docs/destination.txt")
        .await
        .expect("resolve destination");
    let binding_generation = source
        .binding_generation
        .clone()
        .expect("named source has a binding generation");

    let error = submit_operation(
        &store,
        &namespace_id,
        test_commit_id(Some("inode-move-wrong-destination")),
        FilesystemOperation::MoveByInode {
            inode_id: source.inode_id,
            expected_binding_generation: binding_generation.clone(),
            to_parent_inode_id: docs.inode_id,
            to_display_name: loonfs_api::DisplayName::parse("destination.txt")
                .expect("display name"),
            behavior: DestinationBehavior::Replace,
            destination_expected_inode_id: Some(source.inode_id),
            destination_expected_revision_no: Some(RevisionNo(1)),
        },
        &context,
    )
    .await
    .expect_err("a wrong inode guard must fail an inode-addressed move");
    assert_eq!(error.code(), ErrorCode::PathConflict);

    submit_operation(
        &store,
        &namespace_id,
        test_commit_id(Some("inode-move-correct-destination")),
        FilesystemOperation::MoveByInode {
            inode_id: source.inode_id,
            expected_binding_generation: binding_generation,
            to_parent_inode_id: docs.inode_id,
            to_display_name: loonfs_api::DisplayName::parse("destination.txt")
                .expect("display name"),
            behavior: DestinationBehavior::Replace,
            destination_expected_inode_id: Some(destination.inode_id),
            destination_expected_revision_no: Some(RevisionNo(1)),
        },
        &context,
    )
    .await
    .expect("correct destination guards must allow the inode-addressed move");
    let moved = resolve_path(&store, &namespace_id, "/docs/destination.txt")
        .await
        .expect("resolve moved source");
    assert_eq!(moved.inode_id, source.inode_id);
}
