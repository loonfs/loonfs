//! Path-intent planning, query visibility, and mutation semantics.

use crate::common::*;
use loonfs_core::commit::CommitValidationError;
use loonfs_core::publish::{CommitCandidate, CommitIntent, FilesystemOperation};
use loonfs_core::test_support::WriterAt;
use loonfs_core::test_support::{namespace_store, NamespaceFixture};
use loonfs_core::{CoreError, ErrorCode};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::ObjectStore;
use loonfs_objectstore::ObjectStoreExtension;
use loonfs_test_support::ids::{commit_id, namespace_id, page_limit};
use loonfs_test_support::stores::OperationSelector;
use loonfs_types::{
    format::wal::{decode_wal_object_envelope_zstd, WalDelta},
    AbsolutePath, ChangeSeq, CommitId, DeleteDirectoryBehavior, DestinationBehavior,
    EntryInodeKind, FilesystemChange, InodeId, ListPageCursor, NameKey, NamespaceId, Page,
    PageRequest, PathEntry, RevisionNo,
};
use std::assert_matches;
use tempfile::tempdir;

fn named_entry(entry: &PathEntry) -> &str {
    entry
        .display_name
        .as_ref()
        .expect("non-root entry should carry a display name")
        .as_str()
}

#[derive(Clone, Copy)]
struct WritePreconditions {
    inode_id: Option<InodeId>,
    revision_no: Option<RevisionNo>,
}

async fn delete_path_non_recursive<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    context: &WriterAt,
    commit_id: Option<&CommitId>,
) -> Result<loonfs_types::Commit, CoreError> {
    submit_operation(
        store,
        namespace_id,
        commit_id.cloned().unwrap_or_else(CommitId::generate),
        FilesystemOperation::DeletePath {
            path: AbsolutePath::parse(absolute_path).expect("path"),
            behavior: DeleteDirectoryBehavior::NonRecursive,
            expected_inode_id: None,
        },
        context,
    )
    .await
}

async fn copy_file_path<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    source_path: &str,
    destination_path: &str,
    context: &WriterAt,
    commit_id: Option<&CommitId>,
) -> Result<loonfs_types::Commit, CoreError> {
    submit_operation(
        store,
        namespace_id,
        commit_id.cloned().unwrap_or_else(CommitId::generate),
        FilesystemOperation::CopyPath {
            source_path: AbsolutePath::parse(source_path).expect("path"),
            destination_path: AbsolutePath::parse(destination_path).expect("path"),
            precondition: loonfs_types::DestinationPrecondition {
                behavior: DestinationBehavior::NoReplace,
                expected_inode_id: None,
                expected_revision_no: None,
            },
        },
        context,
    )
    .await
}

async fn put_file_with_preconditions<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    bytes: &[u8],
    preconditions: WritePreconditions,
    context: &WriterAt,
    commit_id: &str,
) -> Result<loonfs_types::Commit, CoreError> {
    let content = namespace_store(store, namespace_id)
        .store_bytes_as_content(bytes)
        .await?;
    submit_operation(
        store,
        namespace_id,
        CommitId::parse(commit_id).expect("valid commit id"),
        FilesystemOperation::PutFile {
            path: AbsolutePath::parse(absolute_path).expect("path"),
            content: loonfs_types::FileContent::Ref(content.into_content_ref()),
            behavior: DestinationBehavior::Replace,
            expected_inode_id: preconditions.inode_id,
            expected_revision_no: preconditions.revision_no,
        },
        context,
    )
    .await
}

async fn list_path<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
) -> Result<Vec<loonfs_types::PathEntry>, CoreError> {
    let context = read_context(store, namespace_id).await;
    let engine = namespace_engine(store, namespace_id, &default_writer());
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
                loonfs_types::options::ListOptions::default(),
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
    cursor: Option<ListPageCursor>,
) -> Result<Page<PathEntry, ListPageCursor>, CoreError> {
    let context = read_context(store, namespace_id).await;
    namespace_engine(store, namespace_id, &default_writer())
        .list_path_page(
            absolute_path,
            PageRequest {
                limit: page_limit(limit),
                cursor,
            },
            loonfs_types::options::ListOptions::default(),
            &context,
        )
        .await
}

/// Drains the paged revision listing — the only revision-listing operation
/// the stack exposes — so tests can check a file's full history.
async fn list_file_revisions<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
) -> Result<loonfs_types::ListFileRevisionsResponse, CoreError> {
    let entry = resolve_path(store, namespace_id, absolute_path).await?;
    list_file_revisions_by_inode(store, namespace_id, absolute_path, entry.inode_id).await
}

async fn list_file_revisions_by_inode<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    _absolute_path: &str,
    inode_id: InodeId,
) -> Result<loonfs_types::ListFileRevisionsResponse, CoreError> {
    let context = read_context(store, namespace_id).await;
    let engine = namespace_engine(store, namespace_id, &default_writer());
    let mut revisions = Vec::new();
    let mut cursor = None;
    loop {
        let page = engine
            .list_file_revisions_by_inode_page(
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
            return Ok(loonfs_types::ListFileRevisionsResponse {
                namespace_id: namespace_id.clone(),
                inode_id,
                head_seq: context.read_basis.head.seq,
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
) -> Result<loonfs_core::FileBytes, CoreError> {
    let context = read_context(store, namespace_id).await;
    namespace_engine(store, namespace_id, &default_writer())
        .read_file_revision(absolute_path, revision_no, &context, None)
        .await
}

async fn read_file_revision_bytes_by_inode<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    revision_no: RevisionNo,
) -> Result<Vec<u8>, CoreError> {
    let context = read_context(store, namespace_id).await;
    namespace_engine(store, namespace_id, &default_writer())
        .read_file_revision_by_inode(inode_id, revision_no, &context, None)
        .await
}

async fn resolve_path_latest<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
) -> Result<loonfs_types::PathEntry, CoreError> {
    resolve_path(store, namespace_id, absolute_path).await
}

async fn list_path_latest<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
) -> Result<Vec<loonfs_types::PathEntry>, CoreError> {
    list_path(store, namespace_id, absolute_path).await
}

#[tokio::test]
async fn metadata_queries_do_not_get_content_blobs_but_file_reads_do_once() {
    let temp_dir = tempdir().expect("tempdir");
    let store = content_blob_counting_store(temp_dir.path());
    let context = default_writer();
    let namespace_id = namespace_id("demo");

    NamespaceFixture::new(&store, &namespace_id, &context)
        .create()
        .await
        .expect("bootstrap");
    create_directory_path(
        &store,
        &namespace_id,
        "/docs",
        &context,
        Some(&commit_id("mkdir-docs")),
    )
    .await
    .expect("create docs");

    for index in 0..3 {
        let path = format!("/docs/file-{index}.txt");
        let bytes = format!("file-{index}-bytes");
        let commit = commit_id(&format!("put-file-{index}"));
        NamespaceFixture::new(&store, &namespace_id, &context)
            .put_file(
                &path,
                bytes.as_bytes(),
                DestinationBehavior::NoReplace,
                Some(&commit),
            )
            .await
            .expect("put file");
    }

    store.reset();
    let stat = resolve_path(&store, &namespace_id, "/docs/file-1.txt")
        .await
        .expect("stat file");
    assert_eq!(stat.inode_kind(), EntryInodeKind::File);
    assert_eq!(stat.size_bytes(), Some("file-1-bytes".len() as u64));
    assert!(stat.content_ref().is_some());
    assert_eq!(store.count(OperationSelector::Read), 0);

    store.reset();
    let entries = list_path(&store, &namespace_id, "/docs")
        .await
        .expect("list docs");
    assert_eq!(entries.len(), 3);
    for entry in entries {
        assert_eq!(entry.inode_kind(), EntryInodeKind::File);
        assert_eq!(entry.size_bytes(), Some("file-0-bytes".len() as u64));
        assert!(
            entry.content_ref().is_none(),
            "a listing reports no reference"
        );
    }
    assert_eq!(store.count(OperationSelector::Read), 0);

    store.reset();
    let read = read_file_bytes(&store, &namespace_id, "/docs/file-1.txt")
        .await
        .expect("read file");
    assert_eq!(read.bytes, b"file-1-bytes");
    assert_eq!(store.count(OperationSelector::Read), 1);
}

#[tokio::test]
async fn query_driven_stat_and_list_use_metadata_view_with_delta_run_and_wal_overlay() {
    let temp_dir = tempdir().expect("tempdir");
    let store = content_blob_counting_store(temp_dir.path());
    let context = default_writer();
    let namespace_id = namespace_id("demo");

    NamespaceFixture::new(&store, &namespace_id, &context)
        .create()
        .await
        .expect("bootstrap");
    NamespaceFixture::new(&store, &namespace_id, &context)
        .put_file(
            "/docs/a.txt",
            b"alpha",
            DestinationBehavior::NoReplace,
            Some(&commit_id("put-alpha")),
        )
        .await
        .expect("put alpha");
    NamespaceFixture::new(&store, &namespace_id, &context)
        .put_file(
            "/docs/b.txt",
            b"bravo",
            DestinationBehavior::NoReplace,
            Some(&commit_id("put-bravo")),
        )
        .await
        .expect("put bravo");
    NamespaceFixture::new(&store, &namespace_id, &context)
        .put_file(
            "/dead/leaf.txt",
            b"dead",
            DestinationBehavior::NoReplace,
            Some(&commit_id("put-dead")),
        )
        .await
        .expect("put dead");
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("base checkpoint");

    NamespaceFixture::new(&store, &namespace_id, &context)
        .move_path(
            "/docs/a.txt",
            "/docs/moved.txt",
            Some(&commit_id("move-alpha")),
        )
        .await
        .expect("move alpha");
    NamespaceFixture::new(&store, &namespace_id, &context)
        .put_file(
            "/docs/b.txt",
            b"bravo two",
            DestinationBehavior::Replace,
            Some(&commit_id("replace-bravo")),
        )
        .await
        .expect("replace bravo");
    NamespaceFixture::new(&store, &namespace_id, &context)
        .delete_path("/dead", Some(&commit_id("delete-dead")))
        .await
        .expect("delete dead");
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("delta checkpoint");

    NamespaceFixture::new(&store, &namespace_id, &context)
        .put_file(
            "/docs/wal.txt",
            b"wal",
            DestinationBehavior::NoReplace,
            Some(&commit_id("put-wal")),
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
    assert_eq!(store.count(OperationSelector::Read), 0);

    assert_eq!(
        resolve_path_latest(&store, &namespace_id, "/docs/a.txt")
            .await
            .expect_err("the moved path should not resolve")
            .code(),
        loonfs_types::ErrorCode::PathNotFound
    );
    assert_eq!(
        resolve_path_latest(&store, &namespace_id, "/dead/leaf.txt")
            .await
            .expect_err("a path under the deleted directory should not resolve")
            .code(),
        loonfs_types::ErrorCode::PathNotFound
    );
}

#[tokio::test]
async fn query_driven_stat_uses_exact_name_key_for_dash_containing_siblings() {
    let temp_dir = tempdir().expect("tempdir");
    let store = content_blob_counting_store(temp_dir.path());
    let context = default_writer();
    let namespace_id = namespace_id("demo");

    NamespaceFixture::new(&store, &namespace_id, &context)
        .create()
        .await
        .expect("bootstrap");
    NamespaceFixture::new(&store, &namespace_id, &context)
        .put_file(
            "/docs/report",
            b"short",
            DestinationBehavior::NoReplace,
            Some(&commit_id("put-report")),
        )
        .await
        .expect("put report");
    NamespaceFixture::new(&store, &namespace_id, &context)
        .put_file(
            "/docs/report-2024",
            b"newer-longer",
            DestinationBehavior::NoReplace,
            Some(&commit_id("put-report-2024")),
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
    let context = default_writer();
    let namespace_id = namespace_id("demo");

    NamespaceFixture::new(&store, &namespace_id, &context)
        .create()
        .await
        .expect("bootstrap");
    create_directory_path(
        &store,
        &namespace_id,
        "/wide",
        &context,
        Some(&commit_id("mkdir-wide")),
    )
    .await
    .expect("create wide");
    create_directory_path(
        &store,
        &namespace_id,
        "/other",
        &context,
        Some(&commit_id("mkdir-other")),
    )
    .await
    .expect("create other");

    // More names than one raw scan chunk, so group boundaries cross
    // manifest pages; a few names rebound several times, so groups carry
    // more than one row.
    for index in 0..70u32 {
        if index % 32 == 0 {
            namespace_engine(&store, &namespace_id, &context)
                .fold_wal()
                .await
                .expect("fold seed writes");
        }
        let path = format!("/wide/file-{index:03}.txt");
        NamespaceFixture::new(&store, &namespace_id, &context)
            .put_file(
                &path,
                b"first",
                DestinationBehavior::NoReplace,
                Some(&commit_id(&format!("put-{index:03}"))),
            )
            .await
            .expect("put file");
    }
    for index in [3u32, 35, 68] {
        let path = format!("/wide/file-{index:03}.txt");
        NamespaceFixture::new(&store, &namespace_id, &context)
            .put_file(
                &path,
                b"rebound",
                DestinationBehavior::Replace,
                Some(&commit_id(&format!("rebind-{index:03}"))),
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
    NamespaceFixture::new(&store, &namespace_id, &context)
        .delete_path("/wide/file-010.txt", Some(&commit_id("delete-tail-unbind")))
        .await
        .expect("delete over checkpointed bind");
    NamespaceFixture::new(&store, &namespace_id, &context)
        .move_path(
            "/wide/file-020.txt",
            "/other/moved-away.txt",
            Some(&commit_id("move-cross-directory")),
        )
        .await
        .expect("move into other directory");
    NamespaceFixture::new(&store, &namespace_id, &context)
        .put_file(
            "/wide/file-005.txt",
            b"tail-rebound",
            DestinationBehavior::Replace,
            Some(&commit_id("tail-rebind-005")),
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
    let context = default_writer();
    let namespace_id = namespace_id("demo");

    NamespaceFixture::new(&store, &namespace_id, &context)
        .create()
        .await
        .expect("bootstrap");
    create_directory_path(
        &store,
        &namespace_id,
        "/docs",
        &context,
        Some(&commit_id("mkdir-docs")),
    )
    .await
    .expect("create docs");
    create_directory_path(
        &store,
        &namespace_id,
        "/docs/a-dir",
        &context,
        Some(&commit_id("mkdir-a-dir")),
    )
    .await
    .expect("create a-dir");
    NamespaceFixture::new(&store, &namespace_id, &context)
        .put_file(
            "/docs/c-file.txt",
            b"charlie",
            DestinationBehavior::NoReplace,
            Some(&commit_id("put-c-file")),
        )
        .await
        .expect("put c-file");
    NamespaceFixture::new(&store, &namespace_id, &context)
        .put_file(
            "/docs/stale.txt",
            b"stale",
            DestinationBehavior::NoReplace,
            Some(&commit_id("put-stale")),
        )
        .await
        .expect("put stale");
    NamespaceFixture::new(&store, &namespace_id, &context)
        .move_path(
            "/docs/stale.txt",
            "/docs/b-renamed.txt",
            Some(&commit_id("move-stale")),
        )
        .await
        .expect("move stale");
    NamespaceFixture::new(&store, &namespace_id, &context)
        .put_file(
            "/docs/dead/leaf.txt",
            b"dead",
            DestinationBehavior::NoReplace,
            Some(&commit_id("put-dead")),
        )
        .await
        .expect("put dead");
    NamespaceFixture::new(&store, &namespace_id, &context)
        .delete_path("/docs/dead", Some(&commit_id("delete-dead")))
        .await
        .expect("delete dead");
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("checkpoint manifest children");

    NamespaceFixture::new(&store, &namespace_id, &context)
        .put_file(
            "/docs/d-tail.txt",
            b"delta",
            DestinationBehavior::NoReplace,
            Some(&commit_id("put-d-tail")),
        )
        .await
        .expect("put d-tail");
    create_directory_path(
        &store,
        &namespace_id,
        "/docs/e-tail-dir",
        &context,
        Some(&commit_id("mkdir-e-tail-dir")),
    )
    .await
    .expect("create tail dir");
    NamespaceFixture::new(&store, &namespace_id, &context)
        .put_file(
            "/docs/tail-dead/leaf.txt",
            b"tail-dead",
            DestinationBehavior::NoReplace,
            Some(&commit_id("put-tail-dead")),
        )
        .await
        .expect("put tail dead");
    NamespaceFixture::new(&store, &namespace_id, &context)
        .delete_path("/docs/tail-dead", Some(&commit_id("delete-tail-dead")))
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
        assert_eq!(entry.inode_kind(), EntryInodeKind::Directory);
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
        assert_eq!(entry.inode_kind(), EntryInodeKind::File);
        assert_eq!(entry.revision_no(), Some(RevisionNo(1)));
        assert_eq!(entry.size_bytes(), Some(size));
        assert!(
            entry.content_ref().is_none(),
            "a listing reports no reference"
        );
    }
}

#[tokio::test]
async fn revision_history_and_change_feed_name_the_same_supplied_commit() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = default_writer();
    NamespaceFixture::new(&store, &namespace_id, &context)
        .create()
        .await
        .expect("bootstrap");
    let supplied_commit_id = CommitId::parse("c_cross_projection").expect("commit id");

    let response = NamespaceFixture::new(&store, &namespace_id, &context)
        .write_file_bytes(
            "/docs/provenance.txt",
            b"provenance",
            Some(&supplied_commit_id),
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
    let context = default_writer();
    NamespaceFixture::new(&store, &namespace_id, &context)
        .create()
        .await
        .expect("bootstrap");

    NamespaceFixture::new(&store, &namespace_id, &context)
        .put_file(
            "/docs/rev.txt",
            b"one",
            DestinationBehavior::NoReplace,
            Some(&commit_id("rev-create")),
        )
        .await
        .expect("create file");
    let replacement_context = WriterAt {
        writer_id: context.writer_id.clone(),
        now_ms: 2_000,
    };
    NamespaceFixture::new(&store, &namespace_id, &replacement_context)
        .write_file_bytes("/docs/rev.txt", b"two", Some(&commit_id("rev-replace")))
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
        read_file_revision_bytes_by_inode(&store, &namespace_id, inode_id, RevisionNo(2))
            .await
            .expect("read inode revision");
    assert_eq!(inode_historical, b"two");

    NamespaceFixture::new(&store, &namespace_id, &context)
        .move_path(
            "/docs/rev.txt",
            "/docs/moved.txt",
            Some(&commit_id("rev-move")),
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
        list_file_revisions_by_inode(&store, &namespace_id, "/docs/moved.txt", inode_id)
            .await
            .expect("inode revisions");
    assert_eq!(inode_revisions.revisions.len(), 2);

    NamespaceFixture::new(&store, &namespace_id, &context)
        .restore_file_revision(
            "/docs/moved.txt",
            RevisionNo(1),
            Some(&commit_id("rev-restore")),
        )
        .await
        .expect("restore first revision");
    let restored = read_file_bytes(&store, &namespace_id, "/docs/moved.txt")
        .await
        .expect("read restored");
    assert_eq!(restored.bytes, b"one");
    let restored_revisions =
        list_file_revisions_by_inode(&store, &namespace_id, "/docs/moved.txt", inode_id)
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
    let context = default_writer();
    NamespaceFixture::new(&store, &namespace_id, &context)
        .create()
        .await
        .expect("bootstrap");
    // A spelling whose canonical lookup key differs from it: planning must
    // derive the key from the path component and carry it typed all the way
    // into the durable binding.
    let request = CommitIntent::single(
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
    let expected_fingerprint =
        CommitCandidate::new(request.clone()).semantic_identity(&namespace_id);

    submit_commit(&store, &namespace_id, request, &context)
        .await
        .expect("commit typed name key");

    let wal_keys = store
        .list_prefix("namespaces/demo/wal/")
        .await
        .expect("list wal");
    assert_eq!(wal_keys.len(), 2);
    let wal_bytes = store
        .get(&wal_keys[1], None)
        .await
        .expect("read wal")
        .expect("wal exists");
    let wal_object = decode_wal_object_envelope_zstd(&wal_bytes).expect("decode WAL object");
    assert_eq!(wal_object.payload().records.len(), 1);
    assert_eq!(
        wal_object.payload().records[0].semantic_commit_fingerprint,
        expected_fingerprint
    );
    assert!(wal_object.payload().records[0]
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
    let context = default_writer();
    NamespaceFixture::new(&store, &namespace_id, &context)
        .create()
        .await
        .expect("bootstrap");
    let content = namespace_store(&store, &namespace_id)
        .store_bytes_as_content(b"hello")
        .await
        .expect("stage content");

    let responses = NamespaceFixture::new(&store, &namespace_id, &context)
        .publish_candidates(vec![
            prepared_candidate(
                &store,
                &namespace_id,
                CommitIntent::single(
                    CommitId::parse("put-batched-path").expect("valid commit id"),
                    loonfs_test_support::test_actor(),
                    None,
                    FilesystemOperation::PutFile {
                        path: AbsolutePath::parse("/docs/a.txt").expect("path"),
                        content: loonfs_types::FileContent::Ref(content.into_content_ref()),
                        behavior: DestinationBehavior::NoReplace,
                        expected_inode_id: None,
                        expected_revision_no: None,
                    },
                ),
            )
            .await,
            CommitCandidate::new(CommitIntent::single(
                CommitId::parse("move-batched-path").expect("valid commit id"),
                loonfs_test_support::test_actor(),
                None,
                FilesystemOperation::MovePath {
                    source_path: AbsolutePath::parse("/docs/a.txt").expect("path"),
                    destination_path: AbsolutePath::parse("/docs/b.txt").expect("path"),
                    precondition: loonfs_types::DestinationPrecondition {
                        behavior: DestinationBehavior::NoReplace,
                        expected_inode_id: None,
                        expected_revision_no: None,
                    },
                },
            )),
        ])
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
        .list_prefix("namespaces/demo/wal/")
        .await
        .expect("list wal");
    assert_eq!(wal_keys.len(), 2);
    let wal_bytes = store
        .get(&wal_keys[1], None)
        .await
        .expect("read wal")
        .expect("wal exists");
    let wal_object = decode_wal_object_envelope_zstd(&wal_bytes).expect("decode WAL object");
    assert_eq!(wal_object.payload().records.len(), 2);

    let copied = copy_file_path(
        &store,
        &namespace_id,
        "/docs/b.txt",
        "/docs/c.txt",
        &context,
        Some(&commit_id("copy-path")),
    )
    .await
    .expect("copy path");
    assert_eq!(copied.committed_seq, ChangeSeq(3));

    let deleted = delete_path_non_recursive(
        &store,
        &namespace_id,
        "/docs/b.txt",
        &context,
        Some(&commit_id("delete-path")),
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
    let context = default_writer();
    NamespaceFixture::new(&store, &namespace_id("demo"), &context)
        .create()
        .await
        .expect("bootstrap namespace");
    NamespaceFixture::new(&store, &namespace_id("demo"), &context)
        .write_file_bytes(
            "/docs/report.pdf",
            b"alpha",
            Some(&commit_id("seed-respell")),
        )
        .await
        .expect("seed respell");
    let before = resolve_path_latest(&store, &namespace_id("demo"), "/docs/report.pdf")
        .await
        .expect("resolve before respell");

    // Case-only rename: same name key under the same parent, new spelling.
    NamespaceFixture::new(&store, &namespace_id("demo"), &context)
        .move_path(
            "/docs/report.pdf",
            "/docs/REPORT.PDF",
            Some(&commit_id("respell")),
        )
        .await
        .expect("case-only respelling lands");

    let after = resolve_path_latest(&store, &namespace_id("demo"), "/docs/report.pdf")
        .await
        .expect("resolve after respell");
    assert_eq!(named_entry(&after), "REPORT.PDF");
    assert_eq!(after.inode_id, before.inode_id);

    // Repeating the identical spelling changes nothing and stays a conflict.
    let error = NamespaceFixture::new(&store, &namespace_id("demo"), &context)
        .move_path(
            "/docs/REPORT.PDF",
            "/docs/REPORT.PDF",
            Some(&commit_id("respell-noop")),
        )
        .await
        .expect_err("unchanged spelling");
    assert_eq!(error.code(), ErrorCode::PathConflict);
}

#[tokio::test]
async fn write_and_move_under_deleted_ancestor_start_fresh_subtrees() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = default_writer();
    NamespaceFixture::new(&store, &namespace_id("demo"), &context)
        .create()
        .await
        .expect("bootstrap namespace");
    NamespaceFixture::new(&store, &namespace_id("demo"), &context)
        .write_file_bytes("/docs/old.txt", b"old", Some(&commit_id("seed-docs")))
        .await
        .expect("seed docs");
    NamespaceFixture::new(&store, &namespace_id("demo"), &context)
        .write_file_bytes(
            "/tmp/source.txt",
            b"source",
            Some(&commit_id("seed-source")),
        )
        .await
        .expect("seed source");
    NamespaceFixture::new(&store, &namespace_id("demo"), &context)
        .delete_path("/docs", Some(&commit_id("delete-docs")))
        .await
        .expect("delete docs");

    // Deleting `/docs` unbinds the name: it is reusable immediately, and
    // writes and moves under it create a fresh subtree rather than
    // conflicting with the dead one. The old subtree stays dead — its
    // previous child is not resurrected by the recreation.
    NamespaceFixture::new(&store, &namespace_id("demo"), &context)
        .write_file_bytes(
            "/docs/new.txt",
            b"new",
            Some(&commit_id("put-under-recreated")),
        )
        .await
        .expect("put recreates the subtree");
    let old_child = resolve_path(&store, &namespace_id("demo"), "/docs/old.txt").await;
    assert_eq!(
        old_child
            .expect_err("dead subtree child must stay invisible")
            .code(),
        loonfs_types::ErrorCode::PathNotFound
    );

    NamespaceFixture::new(&store, &namespace_id("demo"), &context)
        .move_path(
            "/tmp/source.txt",
            "/docs/source.txt",
            Some(&commit_id("move-under-recreated")),
        )
        .await
        .expect("move lands in the recreated subtree");
}

#[tokio::test]
async fn create_directory_path_creates_directory_without_auto_parents() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = default_writer();
    NamespaceFixture::new(&store, &namespace_id("demo"), &context)
        .create()
        .await
        .expect("bootstrap namespace");

    let created = create_directory_path(
        &store,
        &namespace_id("demo"),
        "/docs",
        &context,
        Some(&commit_id("mkdir-docs")),
    )
    .await
    .expect("create dir");
    assert_eq!(created.committed_seq, ChangeSeq(1));
    let docs = resolve_path(&store, &namespace_id("demo"), "/docs")
        .await
        .expect("resolve docs");
    assert_eq!(docs.inode_kind(), EntryInodeKind::Directory);

    let missing_parent = create_directory_path(
        &store,
        &namespace_id("demo"),
        "/missing/nested",
        &context,
        Some(&commit_id("mkdir-missing-parent")),
    )
    .await
    .expect_err("mkdir does not auto-create parents");
    assert_eq!(missing_parent.code(), ErrorCode::PathNotFound);
}

#[tokio::test]
async fn path_move_writes_unbind_and_old_binding_stops_resolving() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = default_writer();
    NamespaceFixture::new(&store, &namespace_id("demo"), &context)
        .create()
        .await
        .expect("bootstrap namespace");
    NamespaceFixture::new(&store, &namespace_id("demo"), &context)
        .write_file_bytes("/docs/a.txt", b"hello", Some(&commit_id("seed-a")))
        .await
        .expect("seed file");
    let file = resolve_path(&store, &namespace_id("demo"), "/docs/a.txt")
        .await
        .expect("resolve file");

    NamespaceFixture::new(&store, &namespace_id("demo"), &context)
        .move_path(
            "/docs/a.txt",
            "/docs/b.txt",
            Some(&commit_id("move-a-to-b")),
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
    assert_eq!(
        resolve_path(&store, &namespace_id("demo"), "/docs/a.txt")
            .await
            .expect_err("the old path should not resolve after the move")
            .code(),
        loonfs_types::ErrorCode::PathNotFound
    );
    resolve_path(&store, &namespace_id("demo"), "/docs/b.txt")
        .await
        .expect("new path visible");

    // The unbind is what makes the old binding dead: an operation aimed at
    // the source path finds nothing, while the same inode is still reachable
    // — and still deletable under a precondition naming it — at the destination.
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
    let context = default_writer();
    NamespaceFixture::new(&store, &namespace_id("demo"), &context)
        .create()
        .await
        .expect("bootstrap namespace");
    NamespaceFixture::new(&store, &namespace_id("demo"), &context)
        .write_file_bytes("/docs/hello.txt", b"hello", Some(&commit_id("seed-hello")))
        .await
        .expect("seed file");
    NamespaceFixture::new(&store, &namespace_id("demo"), &context)
        .write_file_bytes(
            "/Cafe\u{0301}.txt",
            b"hello",
            Some(&commit_id("seed-unicode-name")),
        )
        .await
        .expect("seed unicode name");

    let error = NamespaceFixture::new(&store, &namespace_id("demo"), &context)
        .put_file(
            "/docs/hello.txt",
            b"new-bytes",
            DestinationBehavior::NoReplace,
            Some(&commit_id("put-no-force")),
        )
        .await
        .expect_err("put without force");
    assert_eq!(error.code(), ErrorCode::PathConflict);

    let error = NamespaceFixture::new(&store, &namespace_id("demo"), &context)
        .put_file(
            "/CAF\u{00c9}.TXT",
            b"new-bytes",
            DestinationBehavior::NoReplace,
            Some(&commit_id("create-only-conflict")),
        )
        .await
        .expect_err("create-only conflict");
    assert_eq!(error.code(), ErrorCode::PathConflict);
}

#[tokio::test]
async fn put_precondition_matrix_covers_identity_aba_and_valid_combinations() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = default_writer();
    let namespace_id = namespace_id("demo");
    NamespaceFixture::new(&store, &namespace_id, &context)
        .create()
        .await
        .expect("bootstrap");

    NamespaceFixture::new(&store, &namespace_id, &context)
        .write_file_bytes(
            "/docs/aba.txt",
            b"old inode",
            Some(&commit_id("seed-put-aba")),
        )
        .await
        .expect("seed ABA file");
    let old = resolve_path(&store, &namespace_id, "/docs/aba.txt")
        .await
        .expect("observe ABA file");
    NamespaceFixture::new(&store, &namespace_id, &context)
        .delete_path("/docs/aba.txt", Some(&commit_id("delete-put-aba")))
        .await
        .expect("delete observed file");
    NamespaceFixture::new(&store, &namespace_id, &context)
        .write_file_bytes(
            "/docs/aba.txt",
            b"new inode",
            Some(&commit_id("recreate-put-aba")),
        )
        .await
        .expect("recreate ABA path");
    let recreated = resolve_path(&store, &namespace_id, "/docs/aba.txt")
        .await
        .expect("observe recreated file");
    assert_ne!(old.inode_id, recreated.inode_id);
    assert_eq!(old.revision_no(), Some(RevisionNo(1)));
    assert_eq!(recreated.revision_no(), Some(RevisionNo(1)));

    let error = put_file_with_preconditions(
        &store,
        &namespace_id,
        "/docs/aba.txt",
        b"must not land",
        WritePreconditions {
            inode_id: Some(old.inode_id),
            revision_no: Some(RevisionNo(1)),
        },
        &context,
        "with_preconditions-put-aba",
    )
    .await
    .expect_err("the old identity must reject the recreated file");
    let CoreError::FailedOperation {
        operation_index: 0,
        error: source,
    } = error
    else {
        panic!("expected failed operation, got {error:?}");
    };
    match *source {
        CoreError::CommitValidation(CommitValidationError::BindingPreconditionMismatch {
            expected_inode_id: Some(expected_child_inode_id),
            actual_inode_id: Some(actual_child_inode_id),
            ..
        }) => {
            assert_eq!(expected_child_inode_id, old.inode_id);
            assert_eq!(actual_child_inode_id, recreated.inode_id);
        }
        other => panic!("expected binding mismatch, got {other:?}"),
    }
    let error = put_file_with_preconditions(
        &store,
        &namespace_id,
        "/docs/aba.txt",
        b"revision-only must not land",
        WritePreconditions {
            inode_id: None,
            revision_no: Some(RevisionNo(1)),
        },
        &context,
        "revision-only-put-aba",
    )
    .await
    .expect_err("a revision cannot identify the inode occupying a path");
    assert_eq!(error.code(), ErrorCode::InvalidRequest);
    assert_eq!(
        read_file_bytes(&store, &namespace_id, "/docs/aba.txt")
            .await
            .expect("recreated file remains visible")
            .bytes,
        b"new inode"
    );

    put_file_with_preconditions(
        &store,
        &namespace_id,
        "/docs/aba.txt",
        b"inode only",
        WritePreconditions {
            inode_id: Some(recreated.inode_id),
            revision_no: None,
        },
        &context,
        "inode-only-put",
    )
    .await
    .expect("an inode-only precondition allows replacement");
    put_file_with_preconditions(
        &store,
        &namespace_id,
        "/docs/aba.txt",
        b"inode and revision",
        WritePreconditions {
            inode_id: Some(recreated.inode_id),
            revision_no: Some(RevisionNo(2)),
        },
        &context,
        "inode-revision-put",
    )
    .await
    .expect("matching inode and revision preconditions allow replacement");

    let rejected_content = namespace_store(&store, &namespace_id)
        .store_bytes_as_content(b"must not land")
        .await
        .expect("stage rejected content");
    let error = submit_operation(
        &store,
        &namespace_id,
        commit_id("no-replace-with_preconditions-put"),
        FilesystemOperation::PutFile {
            path: AbsolutePath::parse("/docs/aba.txt").expect("path"),
            content: loonfs_types::FileContent::Ref(rejected_content.into_content_ref()),
            behavior: DestinationBehavior::NoReplace,
            expected_inode_id: Some(recreated.inode_id),
            expected_revision_no: None,
        },
        &context,
    )
    .await
    .expect_err("no-replace rejects every put precondition");
    assert_eq!(error.code(), ErrorCode::InvalidRequest);
    assert_eq!(
        read_file_bytes(&store, &namespace_id, "/docs/aba.txt")
            .await
            .expect("file with preconditions remains visible")
            .bytes,
        b"inode and revision"
    );
}

#[tokio::test]
async fn delete_path_non_recursive_rejects_non_empty_directory() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = default_writer();
    NamespaceFixture::new(&store, &namespace_id("demo"), &context)
        .create()
        .await
        .expect("bootstrap namespace");
    NamespaceFixture::new(&store, &namespace_id("demo"), &context)
        .write_file_bytes("/docs/hello.txt", b"hello", Some(&commit_id("seed-docs")))
        .await
        .expect("seed file");

    let docs = resolve_path(&store, &namespace_id("demo"), "/docs")
        .await
        .expect("resolve the directory")
        .inode_id;
    let error = delete_path_non_recursive(
        &store,
        &namespace_id("demo"),
        "/docs",
        &context,
        Some(&commit_id("delete-docs")),
    )
    .await
    .expect_err("non-recursive delete should reject non-empty dir");
    assert_matches!(
        &error,
        CoreError::FailedOperation { operation_index: 0, error: source }
            if matches!(source.as_ref(), CoreError::CommitValidation(
                CommitValidationError::DirectoryNotEmpty { inode_id }
            ) if *inode_id == docs)
    );
    assert_eq!(error.code(), ErrorCode::DirectoryNotEmpty);
}

#[tokio::test]
async fn copy_file_path_creates_new_inode_and_reuses_content_blob() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = default_writer();
    NamespaceFixture::new(&store, &namespace_id("demo"), &context)
        .create()
        .await
        .expect("bootstrap namespace");
    NamespaceFixture::new(&store, &namespace_id("demo"), &context)
        .write_file_bytes(
            "/docs/source.txt",
            b"hello",
            Some(&commit_id("seed-source")),
        )
        .await
        .expect("seed source");

    copy_file_path(
        &store,
        &namespace_id("demo"),
        "/docs/source.txt",
        "/docs/copy.txt",
        &context,
        Some(&commit_id("copy-file")),
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

/// Creates each display name of the folding corpus and its name key as root
/// directories: a spelling whose key is already taken is refused before any
/// write, and both spellings resolve to the first entry created.
#[tokio::test]
async fn the_folding_corpus_pins_directory_admission_collisions_and_lookup() {
    #[derive(serde::Deserialize)]
    struct NameVector {
        display_name: loonfs_types::DisplayName,
        name_key: NameKey,
    }

    let corpus: Vec<NameVector> = serde_json::from_str(include_str!(
        "../../../loonfs-types/tests/golden/name_folding.v1.json"
    ))
    .expect("folding corpus");
    let temp_dir = tempdir().expect("tempdir");
    let store = loonfs_test_support::stores::RecordingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        loonfs_test_support::stores::KeyPredicate::any(),
    );
    let context = default_writer();
    let namespace_id = namespace_id("demo");
    NamespaceFixture::new(&store, &namespace_id, &context)
        .create()
        .await
        .expect("bootstrap namespace");
    let mut engine = loonfs_core::publish::NamespaceCommitEngine::new(
        namespace_store(&store, &namespace_id),
        context.writer_id.clone(),
    );
    let mut entries = std::collections::BTreeMap::<NameKey, PathEntry>::new();
    for NameVector {
        display_name,
        name_key,
    } in &corpus
    {
        let stored_path = format!("/{display_name}");
        let lookup_path = format!("/{name_key}");
        for path in [&stored_path, &lookup_path] {
            let candidate = CommitCandidate::new(CommitIntent::single(
                CommitId::generate(),
                loonfs_test_support::test_actor(),
                None,
                FilesystemOperation::CreateDirectory {
                    path: AbsolutePath::parse(path).expect("corpus path"),
                    parents: false,
                },
            ));
            store.take();
            let result = engine
                .publish_batch(vec![candidate], &context.clock())
                .await
                .results
                .into_iter()
                .next()
                .expect("one admission result");
            if entries.contains_key(name_key) {
                let error = result.expect_err("folded sibling collision");
                assert_eq!(error.code(), ErrorCode::PathConflict);
                assert_eq!(store.counts().puts, 0);
                assert_eq!(store.counts().compare_and_swaps, 0);
                assert_eq!(store.counts().deletes, 0);
            } else {
                result.expect("admit corpus display name");
            }
            let resolved = resolve_path(&store, &namespace_id, path)
                .await
                .expect("resolve corpus spelling");
            if let Some(entry) = entries.get(name_key) {
                assert_eq!(&resolved, entry);
            } else {
                assert_eq!(named_entry(&resolved), display_name.as_str());
                assert!(entries
                    .values()
                    .all(|other| other.inode_id != resolved.inode_id));
                entries.insert(name_key.clone(), resolved);
            }
        }
    }
}

#[tokio::test]
async fn tombstoned_children_stay_unlisted_and_live_entries_keep_revision_data() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = default_writer();
    NamespaceFixture::new(&store, &namespace_id("demo"), &context)
        .create()
        .await
        .expect("bootstrap namespace");

    create_directory_path(&store, &namespace_id("demo"), "/live", &context, None)
        .await
        .expect("create live dir");
    create_directory_path(&store, &namespace_id("demo"), "/dead", &context, None)
        .await
        .expect("create dead dir");
    NamespaceFixture::new(&store, &namespace_id("demo"), &context)
        .put_file(
            "/live/kept.txt",
            b"alive",
            DestinationBehavior::NoReplace,
            None,
        )
        .await
        .expect("put live file");
    NamespaceFixture::new(&store, &namespace_id("demo"), &context)
        .put_file(
            "/dead/gone.txt",
            b"doomed",
            DestinationBehavior::NoReplace,
            None,
        )
        .await
        .expect("put doomed file");
    NamespaceFixture::new(&store, &namespace_id("demo"), &context)
        .delete_path("/dead", None)
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
        kept.content_ref().is_none(),
        "a listed entry reports no content reference"
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
    let context = default_writer();
    let namespace_id = namespace_id("demo");

    NamespaceFixture::new(&store, &namespace_id, &context)
        .create()
        .await
        .expect("bootstrap");
    NamespaceFixture::new(&store, &namespace_id, &context)
        .put_file(
            "/docs/a.txt",
            b"alpha",
            DestinationBehavior::NoReplace,
            Some(&commit_id("put-a")),
        )
        .await
        .expect("put a");
    NamespaceFixture::new(&store, &namespace_id, &context)
        .put_file(
            "/docs/b.txt",
            b"beta",
            DestinationBehavior::NoReplace,
            Some(&commit_id("put-b")),
        )
        .await
        .expect("put b");

    // The default stays create-only: an occupied destination is a conflict.
    let error = submit_operation(
        &store,
        &namespace_id,
        CommitId::parse("move-no-replace").expect("valid commit id"),
        FilesystemOperation::MovePath {
            source_path: AbsolutePath::parse("/docs/a.txt").expect("path"),
            destination_path: AbsolutePath::parse("/docs/b.txt").expect("path"),
            precondition: loonfs_types::DestinationPrecondition {
                behavior: DestinationBehavior::NoReplace,
                expected_inode_id: None,
                expected_revision_no: None,
            },
        },
        &context,
    )
    .await
    .expect_err("no-replace move onto an occupied name fails");
    assert_matches!(
        error,
        CoreError::FailedOperation { operation_index: 0, error: source }
        if matches!(*source, CoreError::DestinationExists { .. })
    );

    // Replace compiles to one commit: the destination file's delete and the
    // source's rebind land atomically.
    submit_operation(
        &store,
        &namespace_id,
        CommitId::parse("move-replace").expect("valid commit id"),
        FilesystemOperation::MovePath {
            source_path: AbsolutePath::parse("/docs/a.txt").expect("path"),
            destination_path: AbsolutePath::parse("/docs/b.txt").expect("path"),
            precondition: loonfs_types::DestinationPrecondition {
                behavior: DestinationBehavior::Replace,
                expected_inode_id: None,
                expected_revision_no: None,
            },
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
    let context = default_writer();
    let namespace_id = namespace_id("demo");

    NamespaceFixture::new(&store, &namespace_id, &context)
        .create()
        .await
        .expect("bootstrap");
    NamespaceFixture::new(&store, &namespace_id, &context)
        .put_file(
            "/docs/a.txt",
            b"alpha",
            DestinationBehavior::NoReplace,
            Some(&commit_id("put-a")),
        )
        .await
        .expect("put a");
    create_directory_path(
        &store,
        &namespace_id,
        "/docs/dir",
        &context,
        Some(&commit_id("mkdir")),
    )
    .await
    .expect("mkdir");

    // Mirrors put: only a file destination can be replaced.
    let error = submit_operation(
        &store,
        &namespace_id,
        CommitId::parse("move-onto-dir").expect("valid commit id"),
        FilesystemOperation::MovePath {
            source_path: AbsolutePath::parse("/docs/a.txt").expect("path"),
            destination_path: AbsolutePath::parse("/docs/dir").expect("path"),
            precondition: loonfs_types::DestinationPrecondition {
                behavior: DestinationBehavior::Replace,
                expected_inode_id: None,
                expected_revision_no: None,
            },
        },
        &context,
    )
    .await
    .expect_err("replace move onto a directory fails");
    assert_matches!(
        error,
        CoreError::FailedOperation { operation_index: 0, error: source }
        if matches!(*source, CoreError::ExpectedFile { .. })
    );

    // A path never replaces itself, force or not.
    let error = submit_operation(
        &store,
        &namespace_id,
        CommitId::parse("move-onto-self").expect("valid commit id"),
        FilesystemOperation::MovePath {
            source_path: AbsolutePath::parse("/docs/a.txt").expect("path"),
            destination_path: AbsolutePath::parse("/docs/a.txt").expect("path"),
            precondition: loonfs_types::DestinationPrecondition {
                behavior: DestinationBehavior::Replace,
                expected_inode_id: None,
                expected_revision_no: None,
            },
        },
        &context,
    )
    .await
    .expect_err("replace move onto itself fails");
    assert_matches!(
        error,
        CoreError::FailedOperation { operation_index: 0, error: source }
        if matches!(*source, CoreError::DestinationExists { .. })
    );
}

#[tokio::test]
async fn copy_replace_appends_a_revision_to_the_destination_inode() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = default_writer();
    let namespace_id = namespace_id("demo");

    NamespaceFixture::new(&store, &namespace_id, &context)
        .create()
        .await
        .expect("bootstrap");
    NamespaceFixture::new(&store, &namespace_id, &context)
        .put_file(
            "/docs/a.txt",
            b"alpha",
            DestinationBehavior::NoReplace,
            Some(&commit_id("put-a")),
        )
        .await
        .expect("put a");
    NamespaceFixture::new(&store, &namespace_id, &context)
        .put_file(
            "/docs/b.txt",
            b"beta",
            DestinationBehavior::NoReplace,
            Some(&commit_id("put-b")),
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
            source_path: AbsolutePath::parse("/docs/a.txt").expect("path"),
            destination_path: AbsolutePath::parse("/docs/b.txt").expect("path"),
            precondition: loonfs_types::DestinationPrecondition {
                behavior: DestinationBehavior::Replace,
                expected_inode_id: None,
                expected_revision_no: None,
            },
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
async fn move_path_precondition_matrix_checks_the_destination_state() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = default_writer();
    let namespace_id = namespace_id("demo");
    NamespaceFixture::new(&store, &namespace_id, &context)
        .create()
        .await
        .expect("bootstrap");
    for (path, bytes, commit) in [
        ("/docs/source.txt", b"source".as_slice(), "seed-move-source"),
        (
            "/docs/destination.txt",
            b"destination".as_slice(),
            "seed-move-destination",
        ),
        (
            "/docs/inode-source.txt",
            b"inode source".as_slice(),
            "seed-inode-move-source",
        ),
        (
            "/docs/inode-destination.txt",
            b"inode destination".as_slice(),
            "seed-inode-move-destination",
        ),
    ] {
        NamespaceFixture::new(&store, &namespace_id, &context)
            .write_file_bytes(path, bytes, Some(&commit_id(commit)))
            .await
            .expect("seed move fixture");
    }
    let destination = resolve_path(&store, &namespace_id, "/docs/destination.txt")
        .await
        .expect("observe destination");
    let inode_destination = resolve_path(&store, &namespace_id, "/docs/inode-destination.txt")
        .await
        .expect("observe inode-only destination");

    submit_operation(
        &store,
        &namespace_id,
        commit_id("move-inode-only-destination-precondition"),
        FilesystemOperation::MovePath {
            source_path: AbsolutePath::parse("/docs/inode-source.txt").expect("path"),
            destination_path: AbsolutePath::parse("/docs/inode-destination.txt").expect("path"),
            precondition: loonfs_types::DestinationPrecondition {
                behavior: DestinationBehavior::Replace,
                expected_inode_id: Some(inode_destination.inode_id),
                expected_revision_no: None,
            },
        },
        &context,
    )
    .await
    .expect("an inode-only destination precondition allows a move");

    let error = submit_operation(
        &store,
        &namespace_id,
        commit_id("move-revision-only-destination-precondition"),
        FilesystemOperation::MovePath {
            source_path: AbsolutePath::parse("/docs/source.txt").expect("path"),
            destination_path: AbsolutePath::parse("/docs/destination.txt").expect("path"),
            precondition: loonfs_types::DestinationPrecondition {
                behavior: DestinationBehavior::Replace,
                expected_inode_id: None,
                expected_revision_no: Some(RevisionNo(1)),
            },
        },
        &context,
    )
    .await
    .expect_err("a destination revision requires its inode");
    assert_eq!(error.code(), ErrorCode::InvalidRequest);

    let error = submit_operation(
        &store,
        &namespace_id,
        commit_id("move-no-replace-destination-precondition"),
        FilesystemOperation::MovePath {
            source_path: AbsolutePath::parse("/docs/source.txt").expect("path"),
            destination_path: AbsolutePath::parse("/docs/destination.txt").expect("path"),
            precondition: loonfs_types::DestinationPrecondition {
                behavior: DestinationBehavior::NoReplace,
                expected_inode_id: Some(destination.inode_id),
                expected_revision_no: None,
            },
        },
        &context,
    )
    .await
    .expect_err("no-replace rejects every destination precondition");
    assert_eq!(error.code(), ErrorCode::InvalidRequest);
    assert_eq!(
        read_file_bytes(&store, &namespace_id, "/docs/source.txt")
            .await
            .expect("source remains after rejected moves")
            .bytes,
        b"source"
    );
    assert_eq!(
        read_file_bytes(&store, &namespace_id, "/docs/destination.txt")
            .await
            .expect("destination remains after rejected moves")
            .bytes,
        b"destination"
    );

    let error = submit_operation(
        &store,
        &namespace_id,
        commit_id("move-stale-destination-revision"),
        FilesystemOperation::MovePath {
            source_path: AbsolutePath::parse("/docs/source.txt").expect("path"),
            destination_path: AbsolutePath::parse("/docs/destination.txt").expect("path"),
            precondition: loonfs_types::DestinationPrecondition {
                behavior: DestinationBehavior::Replace,
                expected_inode_id: Some(destination.inode_id),
                expected_revision_no: Some(RevisionNo(2)),
            },
        },
        &context,
    )
    .await
    .expect_err("a stale destination revision must fail");
    assert_eq!(error.code(), ErrorCode::StaleRevision);

    submit_operation(
        &store,
        &namespace_id,
        commit_id("move-correct-destination-preconditions"),
        FilesystemOperation::MovePath {
            source_path: AbsolutePath::parse("/docs/source.txt").expect("path"),
            destination_path: AbsolutePath::parse("/docs/destination.txt").expect("path"),
            precondition: loonfs_types::DestinationPrecondition {
                behavior: DestinationBehavior::Replace,
                expected_inode_id: Some(destination.inode_id),
                expected_revision_no: Some(RevisionNo(1)),
            },
        },
        &context,
    )
    .await
    .expect("the observed destination preconditions must allow the move");
    assert_eq!(
        read_file_bytes(&store, &namespace_id, "/docs/destination.txt")
            .await
            .expect("read moved source")
            .bytes,
        b"source"
    );
}

#[tokio::test]
async fn copy_path_precondition_matrix_covers_identity_aba_and_valid_combinations() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = default_writer();
    let namespace_id = namespace_id("demo");
    NamespaceFixture::new(&store, &namespace_id, &context)
        .create()
        .await
        .expect("bootstrap");
    for (path, bytes, commit) in [
        ("/docs/source.txt", b"source".as_slice(), "seed-copy-source"),
        (
            "/docs/aba-destination.txt",
            b"old destination".as_slice(),
            "seed-copy-aba-destination",
        ),
    ] {
        NamespaceFixture::new(&store, &namespace_id, &context)
            .write_file_bytes(path, bytes, Some(&commit_id(commit)))
            .await
            .expect("seed copy fixture");
    }
    let old = resolve_path(&store, &namespace_id, "/docs/aba-destination.txt")
        .await
        .expect("observe ABA destination");
    NamespaceFixture::new(&store, &namespace_id, &context)
        .delete_path(
            "/docs/aba-destination.txt",
            Some(&commit_id("delete-copy-aba-destination")),
        )
        .await
        .expect("delete ABA destination");
    NamespaceFixture::new(&store, &namespace_id, &context)
        .write_file_bytes(
            "/docs/aba-destination.txt",
            b"new destination",
            Some(&commit_id("recreate-copy-aba-destination")),
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
        commit_id("with_preconditions-copy-aba"),
        FilesystemOperation::CopyPath {
            source_path: AbsolutePath::parse("/docs/source.txt").expect("path"),
            destination_path: AbsolutePath::parse("/docs/aba-destination.txt").expect("path"),
            precondition: loonfs_types::DestinationPrecondition {
                behavior: DestinationBehavior::Replace,
                expected_inode_id: Some(old.inode_id),
                expected_revision_no: Some(RevisionNo(1)),
            },
        },
        &context,
    )
    .await
    .expect_err("the old copy destination identity must reject the recreated file");
    let CoreError::FailedOperation {
        operation_index: 0,
        error: source,
    } = error
    else {
        panic!("expected failed operation, got {error:?}");
    };
    assert_matches!(
        *source,
        CoreError::CommitValidation(CommitValidationError::BindingPreconditionMismatch {
            expected_inode_id: Some(expected_child_inode_id),
            actual_inode_id: Some(actual_child_inode_id),
            ..
        }) if expected_child_inode_id == old.inode_id
            && actual_child_inode_id == recreated.inode_id
    );

    let error = submit_operation(
        &store,
        &namespace_id,
        commit_id("copy-revision-only-destination-precondition"),
        FilesystemOperation::CopyPath {
            source_path: AbsolutePath::parse("/docs/source.txt").expect("path"),
            destination_path: AbsolutePath::parse("/docs/aba-destination.txt").expect("path"),
            precondition: loonfs_types::DestinationPrecondition {
                behavior: DestinationBehavior::Replace,
                expected_inode_id: None,
                expected_revision_no: Some(RevisionNo(1)),
            },
        },
        &context,
    )
    .await
    .expect_err("a destination revision requires its inode");
    assert_eq!(error.code(), ErrorCode::InvalidRequest);

    let error = submit_operation(
        &store,
        &namespace_id,
        commit_id("copy-no-replace-destination-precondition"),
        FilesystemOperation::CopyPath {
            source_path: AbsolutePath::parse("/docs/source.txt").expect("path"),
            destination_path: AbsolutePath::parse("/docs/aba-destination.txt").expect("path"),
            precondition: loonfs_types::DestinationPrecondition {
                behavior: DestinationBehavior::NoReplace,
                expected_inode_id: Some(recreated.inode_id),
                expected_revision_no: None,
            },
        },
        &context,
    )
    .await
    .expect_err("no-replace rejects every destination precondition");
    assert_eq!(error.code(), ErrorCode::InvalidRequest);
    assert_eq!(
        read_file_bytes(&store, &namespace_id, "/docs/aba-destination.txt")
            .await
            .expect("destination remains after rejected copies")
            .bytes,
        b"new destination"
    );

    submit_operation(
        &store,
        &namespace_id,
        commit_id("copy-inode-only-destination-precondition"),
        FilesystemOperation::CopyPath {
            source_path: AbsolutePath::parse("/docs/source.txt").expect("path"),
            destination_path: AbsolutePath::parse("/docs/aba-destination.txt").expect("path"),
            precondition: loonfs_types::DestinationPrecondition {
                behavior: DestinationBehavior::Replace,
                expected_inode_id: Some(recreated.inode_id),
                expected_revision_no: None,
            },
        },
        &context,
    )
    .await
    .expect("an inode-only destination precondition allows a copy");
    submit_operation(
        &store,
        &namespace_id,
        commit_id("copy-inode-revision-destination-precondition"),
        FilesystemOperation::CopyPath {
            source_path: AbsolutePath::parse("/docs/source.txt").expect("path"),
            destination_path: AbsolutePath::parse("/docs/aba-destination.txt").expect("path"),
            precondition: loonfs_types::DestinationPrecondition {
                behavior: DestinationBehavior::Replace,
                expected_inode_id: Some(recreated.inode_id),
                expected_revision_no: Some(RevisionNo(2)),
            },
        },
        &context,
    )
    .await
    .expect("matching inode and revision preconditions allow a copy");
}

#[tokio::test]
async fn move_by_inode_precondition_matrix_checks_the_destination_state() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = default_writer();
    let namespace_id = namespace_id("demo");
    NamespaceFixture::new(&store, &namespace_id, &context)
        .create()
        .await
        .expect("bootstrap");
    NamespaceFixture::new(&store, &namespace_id, &context)
        .write_file_bytes(
            "/docs/source.txt",
            b"source",
            Some(&commit_id("seed-inode-move-source")),
        )
        .await
        .expect("seed source");
    NamespaceFixture::new(&store, &namespace_id, &context)
        .write_file_bytes(
            "/docs/destination.txt",
            b"destination",
            Some(&commit_id("seed-inode-move-destination")),
        )
        .await
        .expect("seed destination");
    NamespaceFixture::new(&store, &namespace_id, &context)
        .write_file_bytes(
            "/docs/source-two.txt",
            b"source two",
            Some(&commit_id("seed-inode-move-source-two")),
        )
        .await
        .expect("seed second source");
    NamespaceFixture::new(&store, &namespace_id, &context)
        .write_file_bytes(
            "/docs/destination-two.txt",
            b"destination two",
            Some(&commit_id("seed-inode-move-destination-two")),
        )
        .await
        .expect("seed second destination");
    let docs = resolve_path(&store, &namespace_id, "/docs")
        .await
        .expect("resolve destination parent");
    let source = resolve_path(&store, &namespace_id, "/docs/source.txt")
        .await
        .expect("resolve source");
    let destination = resolve_path(&store, &namespace_id, "/docs/destination.txt")
        .await
        .expect("resolve destination");
    let source_two = resolve_path(&store, &namespace_id, "/docs/source-two.txt")
        .await
        .expect("resolve second source");
    let destination_two = resolve_path(&store, &namespace_id, "/docs/destination-two.txt")
        .await
        .expect("resolve second destination");
    let binding_version = source
        .binding_version
        .clone()
        .expect("named source has a binding version");
    let binding_version_two = source_two
        .binding_version
        .clone()
        .expect("second source has a binding version");

    let error = submit_operation(
        &store,
        &namespace_id,
        commit_id("inode-move-wrong-destination"),
        FilesystemOperation::MoveByInode {
            inode_id: source.inode_id,
            expected_binding_version: binding_version.clone(),
            destination_parent_inode_id: docs.inode_id,
            destination_display_name: loonfs_types::DisplayName::parse("destination.txt")
                .expect("display name"),
            precondition: loonfs_types::DestinationPrecondition {
                behavior: DestinationBehavior::Replace,
                expected_inode_id: Some(source.inode_id),
                expected_revision_no: Some(RevisionNo(1)),
            },
        },
        &context,
    )
    .await
    .expect_err("a wrong inode precondition must fail an inode-addressed move");
    assert_eq!(error.code(), ErrorCode::PathConflict);

    let error = submit_operation(
        &store,
        &namespace_id,
        commit_id("inode-move-revision-only-destination"),
        FilesystemOperation::MoveByInode {
            inode_id: source.inode_id,
            expected_binding_version: binding_version.clone(),
            destination_parent_inode_id: docs.inode_id,
            destination_display_name: loonfs_types::DisplayName::parse("destination.txt")
                .expect("display name"),
            precondition: loonfs_types::DestinationPrecondition {
                behavior: DestinationBehavior::Replace,
                expected_inode_id: None,
                expected_revision_no: Some(RevisionNo(1)),
            },
        },
        &context,
    )
    .await
    .expect_err("a destination revision requires its inode");
    assert_eq!(error.code(), ErrorCode::InvalidRequest);

    let error = submit_operation(
        &store,
        &namespace_id,
        commit_id("inode-move-no-replace-destination-precondition"),
        FilesystemOperation::MoveByInode {
            inode_id: source.inode_id,
            expected_binding_version: binding_version.clone(),
            destination_parent_inode_id: docs.inode_id,
            destination_display_name: loonfs_types::DisplayName::parse("destination.txt")
                .expect("display name"),
            precondition: loonfs_types::DestinationPrecondition {
                behavior: DestinationBehavior::NoReplace,
                expected_inode_id: Some(destination.inode_id),
                expected_revision_no: None,
            },
        },
        &context,
    )
    .await
    .expect_err("no-replace rejects every destination precondition");
    assert_eq!(error.code(), ErrorCode::InvalidRequest);
    assert_eq!(
        read_file_bytes(&store, &namespace_id, "/docs/source.txt")
            .await
            .expect("source remains after rejected inode moves")
            .bytes,
        b"source"
    );
    assert_eq!(
        read_file_bytes(&store, &namespace_id, "/docs/destination.txt")
            .await
            .expect("destination remains after rejected inode moves")
            .bytes,
        b"destination"
    );

    submit_operation(
        &store,
        &namespace_id,
        commit_id("inode-move-inode-only-destination-precondition"),
        FilesystemOperation::MoveByInode {
            inode_id: source.inode_id,
            expected_binding_version: binding_version,
            destination_parent_inode_id: docs.inode_id,
            destination_display_name: loonfs_types::DisplayName::parse("destination.txt")
                .expect("display name"),
            precondition: loonfs_types::DestinationPrecondition {
                behavior: DestinationBehavior::Replace,
                expected_inode_id: Some(destination.inode_id),
                expected_revision_no: None,
            },
        },
        &context,
    )
    .await
    .expect("an inode-only destination precondition allows an inode move");
    submit_operation(
        &store,
        &namespace_id,
        commit_id("inode-move-inode-revision-destination-precondition"),
        FilesystemOperation::MoveByInode {
            inode_id: source_two.inode_id,
            expected_binding_version: binding_version_two,
            destination_parent_inode_id: docs.inode_id,
            destination_display_name: loonfs_types::DisplayName::parse("destination-two.txt")
                .expect("display name"),
            precondition: loonfs_types::DestinationPrecondition {
                behavior: DestinationBehavior::Replace,
                expected_inode_id: Some(destination_two.inode_id),
                expected_revision_no: Some(RevisionNo(1)),
            },
        },
        &context,
    )
    .await
    .expect("matching inode and revision preconditions allow an inode move");
}

/// Requires every file entry of a listing of `directory` to report what a
/// stat of the entry's path reports, apart from the content reference, which
/// only the stat reports, and returns the listing.
async fn listing_matching_stats<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    directory: &str,
) -> Vec<PathEntry> {
    let entries = list_path(store, namespace_id, directory)
        .await
        .expect("list directory");
    for entry in &entries {
        let mut stat = resolve_path(store, namespace_id, entry.path.as_str())
            .await
            .expect("stat listed entry");
        if let loonfs_types::PathEntryKind::File { content_ref, .. } = &mut stat.kind {
            assert!(content_ref.take().is_some(), "a stat reports the reference");
        }
        assert_eq!(entry.kind, stat.kind, "{}", entry.path);
        assert_eq!(
            entry.binding_version, stat.binding_version,
            "{}",
            entry.path
        );
    }
    entries
}

/// Checks that a listing reports each file's current revision after a write,
/// a move, a write after the move, a delete, and an undelete, that a fold
/// leaves every listing unchanged, and that a listing of the folded tree
/// reads no segment of the `revisions` family.
#[tokio::test]
async fn listed_revisions_follow_moves_writes_and_undeletes_across_a_fold() {
    let temp_dir = tempdir().expect("tempdir");
    let store = loonfs_test_support::stores::RecordingStore::metadata_segments(
        LocalFsStore::new(temp_dir.path()).expect("store"),
    );
    let context = default_writer();
    let namespace_id = namespace_id("listed-revisions");
    let fixture = NamespaceFixture::new(&store, &namespace_id, &context);
    fixture.create().await.expect("bootstrap");
    for directory in ["/a", "/b"] {
        create_directory_path(&store, &namespace_id, directory, &context, None)
            .await
            .expect("create directory");
    }
    for bytes in [&b"one"[..], b"two"] {
        fixture
            .write_file_bytes("/a/report.txt", bytes, None)
            .await
            .expect("write file");
    }
    fixture
        .move_path("/a/report.txt", "/b/final.txt", None)
        .await
        .expect("move file");
    assert!(listing_matching_stats(&store, &namespace_id, "/a")
        .await
        .is_empty());
    let moved = listing_matching_stats(&store, &namespace_id, "/b").await;
    assert_eq!(
        moved
            .iter()
            .map(|entry| (entry.path.as_str(), entry.revision_no(), entry.size_bytes()))
            .collect::<Vec<_>>(),
        [("/b/final.txt", Some(RevisionNo(2)), Some(3))]
    );

    fixture
        .write_file_bytes("/b/final.txt", b"three!", None)
        .await
        .expect("write moved file");
    let deletion = fixture
        .delete_path("/b/final.txt", None)
        .await
        .expect("delete file");
    assert!(listing_matching_stats(&store, &namespace_id, "/b")
        .await
        .is_empty());
    submit_operation(
        &store,
        &namespace_id,
        CommitId::generate(),
        FilesystemOperation::Undelete {
            inode_id: moved[0].inode_id,
            deletion_seq: deletion.committed_seq,
            destination_path: None,
            destination_parent_inode_id: None,
            destination_display_name: None,
        },
        &context,
    )
    .await
    .expect("undelete file");
    let unfolded = listing_matching_stats(&store, &namespace_id, "/b").await;
    assert_eq!(
        unfolded
            .iter()
            .map(|entry| (entry.path.as_str(), entry.revision_no(), entry.size_bytes()))
            .collect::<Vec<_>>(),
        [("/b/final.txt", Some(RevisionNo(3)), Some(6))]
    );
    assert!(unfolded[0].content_ref().is_none());

    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("fold the tail");
    let revision_keys: std::collections::BTreeSet<String> = namespace_store(&store, &namespace_id)
        .load_current_manifest()
        .await
        .expect("manifest")
        .payload()
        .runs
        .iter()
        .flat_map(|run| &run.segments)
        .filter(|segment| {
            segment.family == loonfs_types::format::manifest::MetadataRowFamily::Revisions
        })
        .map(loonfs_objectstore::keys::metadata_segment_object_key)
        .collect();
    assert!(!revision_keys.is_empty());
    store.reset();
    let folded = list_path(&store, &namespace_id, "/b")
        .await
        .expect("list folded directory");
    let reads = store.take_get_keys();
    assert!(
        reads.iter().all(|key| !revision_keys.contains(key)),
        "a folded listing read a revision segment: {reads:?}"
    );
    assert_eq!(folded, unfolded);
    assert!(listing_matching_stats(&store, &namespace_id, "/a")
        .await
        .is_empty());
    assert_eq!(
        listing_matching_stats(&store, &namespace_id, "/b").await,
        folded
    );
}
