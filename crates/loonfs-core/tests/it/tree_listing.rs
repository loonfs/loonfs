//! Recursive directory listings: the walk's order, paging with the stack
//! cursor, which cursors resume which listing, and the store requests a page
//! costs.

use crate::common::commit_split_support::{
    bootstrap_namespace, create_checkpoint, mutation_context, submit_commit, submit_operation,
};
use crate::common::{namespace_engine, read_context};
use loonfs_core::content::store_bytes_as_content;
use loonfs_core::publish::{CommitIntent, FilesystemOperation};
use loonfs_core::CoreError;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::ObjectStore;
use loonfs_test_support::ids::{namespace_id, page_limit};
use loonfs_test_support::stores::{KeyPredicate, RecordingStore};
use loonfs_types::options::ListOptions;
use loonfs_types::{
    AbsolutePath, CommitId, DestinationBehavior, DestinationPrecondition, ListPageCursor,
    NamespaceId, Page, PageRequest, PathEntry,
};
use std::assert_matches;
use std::collections::HashMap;
use tempfile::tempdir;

/// The small tree's paths, a directory for each path that ends in `/`.
const SMALL_TREE: &[&str] = &[
    "/docs/",
    "/docs/a/",
    "/docs/a/m/",
    "/docs/c/",
    "/docs/e/",
    "/docs/a/m/deep.txt",
    "/docs/a/z.txt",
    "/docs/B.txt",
    "/docs/c/x.txt",
    "/top.txt",
];

/// The recursive listing of `/docs` in the small tree. `B.txt` sorts after
/// `a` because name keys ignore case.
const SMALL_TREE_LISTING: &[&str] = &[
    "/docs/a",
    "/docs/a/m",
    "/docs/a/m/deep.txt",
    "/docs/a/z.txt",
    "/docs/B.txt",
    "/docs/c",
    "/docs/c/x.txt",
    "/docs/e",
];

/// Creates a namespace and commits `paths` in one commit, a directory for
/// each path that ends in `/` and a file for every other path.
async fn seed<S: ObjectStore + ?Sized>(store: &S, namespace_id: &NamespaceId, paths: &[&str]) {
    let context = mutation_context();
    bootstrap_namespace(store, namespace_id, &context)
        .await
        .expect("bootstrap");
    let content_ref = store_bytes_as_content(store, namespace_id, b"body")
        .await
        .expect("content")
        .into_content_ref();
    let operations = paths
        .iter()
        .map(|path| match path.strip_suffix('/') {
            Some(directory) => FilesystemOperation::CreateDirectory {
                path: AbsolutePath::parse(directory).expect("path"),
                parents: false,
            },
            None => FilesystemOperation::PutFile {
                path: AbsolutePath::parse(*path).expect("path"),
                content_ref: Some(content_ref.clone()),
                inline_content: None,
                behavior: DestinationBehavior::NoReplace,
                expected_inode_id: None,
                expected_revision_no: None,
            },
        })
        .collect();
    let intent = CommitIntent {
        commit_id: CommitId::generate(),
        actor_id: loonfs_test_support::test_actor(),
        subject: None,
        message: None,
        operations,
        preconditions: Vec::new(),
    };
    submit_commit(store, namespace_id, intent, &context)
        .await
        .expect("seed commit");
}

fn listing(recursive: bool) -> ListOptions {
    ListOptions {
        recursive,
        ..Default::default()
    }
}

/// Lists one page of `path` with `options`.
async fn page<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    path: &str,
    limit: u32,
    cursor: Option<ListPageCursor>,
    options: ListOptions,
) -> Result<Page<PathEntry, ListPageCursor>, CoreError> {
    let context = read_context(store, namespace_id).await;
    namespace_engine(store, namespace_id, &mutation_context())
        .list_path_page(
            path,
            PageRequest {
                limit: page_limit(limit),
                cursor,
            },
            options,
            &context,
        )
        .await
}

/// Lists `path` recursively in pages of `limit`, and returns each page's
/// entry paths. Every cursor goes through its wire encoding.
async fn recursive_pages<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    path: &str,
    limit: u32,
) -> Vec<Vec<String>> {
    let mut pages = Vec::new();
    let mut cursor = None;
    loop {
        let listed = page(store, namespace_id, path, limit, cursor, listing(true))
            .await
            .expect("recursive page");
        pages.push(paths(&listed.items));
        let Some(next) = listed.next_cursor else {
            return pages;
        };
        let encoded = next.encode().expect("encode cursor");
        cursor = Some(ListPageCursor::decode(&encoded, &listing(true)).expect("decode cursor"));
    }
}

fn paths(entries: &[PathEntry]) -> Vec<String> {
    entries
        .iter()
        .map(|entry| entry.path.as_str().to_owned())
        .collect()
}

/// Checks that a recursive listing returns the subtree depth first, each
/// directory before its descendants and each directory's children in
/// name-key order, with full paths, by path and by inode, while a listing
/// that isn't recursive still returns the children only.
#[tokio::test]
async fn a_recursive_listing_walks_the_subtree_depth_first_in_name_key_order() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = namespace_id("demo");
    seed(&store, &namespace_id, SMALL_TREE).await;

    let context = read_context(&store, &namespace_id).await;
    let engine = namespace_engine(&store, &namespace_id, &mutation_context());
    let docs = engine
        .resolve_path("/docs", Default::default(), &context)
        .await
        .expect("stat docs");
    let by_path = page(&store, &namespace_id, "/docs", 1_000, None, listing(true))
        .await
        .expect("recursive listing");
    assert_eq!(paths(&by_path.items), SMALL_TREE_LISTING);
    assert!(by_path.next_cursor.is_none());
    // Each entry's parent is the listed directory or an entry before it.
    let mut inode_ids = HashMap::from([("/docs".to_owned(), docs.inode_id)]);
    for entry in &by_path.items {
        let (parent, _) = entry.path.as_str().rsplit_once('/').expect("parent path");
        assert_eq!(
            entry.parent_inode_id,
            inode_ids.get(parent).copied(),
            "{}",
            entry.path
        );
        inode_ids.insert(entry.path.to_string(), entry.inode_id);
    }

    let by_inode = engine
        .list_inode_children_page(
            docs.inode_id,
            PageRequest {
                limit: page_limit(1_000),
                cursor: None,
            },
            listing(true),
            &context,
        )
        .await
        .expect("recursive listing by inode");
    assert_eq!(by_inode.items, by_path.items);

    let children = page(&store, &namespace_id, "/docs", 1_000, None, listing(false))
        .await
        .expect("listing of the children");
    assert_eq!(
        paths(&children.items),
        ["/docs/a", "/docs/B.txt", "/docs/c", "/docs/e"]
    );
    let file = page(
        &store,
        &namespace_id,
        "/top.txt",
        1_000,
        None,
        listing(true),
    )
    .await
    .expect("recursive listing of a file");
    assert_eq!(paths(&file.items), ["/top.txt"]);
}

/// Checks that recursive pages of every size resume from the stack cursor,
/// inside a subtree and across subtrees, return every entry exactly once in
/// order, and end without an empty last page.
#[tokio::test]
async fn recursive_pages_resume_from_the_stack_cursor_and_return_every_entry_once() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = namespace_id("demo");
    seed(&store, &namespace_id, SMALL_TREE).await;

    for limit in 1..=SMALL_TREE_LISTING.len() + 1 {
        let pages = recursive_pages(
            &store,
            &namespace_id,
            "/docs",
            u32::try_from(limit).expect("limit"),
        )
        .await;
        assert_eq!(
            pages.len(),
            SMALL_TREE_LISTING.len().div_ceil(limit),
            "limit {limit}: {pages:?}"
        );
        assert!(pages.iter().all(|page| page.len() <= limit), "{pages:?}");
        assert_eq!(pages.concat(), SMALL_TREE_LISTING, "limit {limit}");
    }
}

/// Checks that a page resumed after a directory on the cursor's stack moved
/// away continues in that directory's parent, after the directory's name.
#[tokio::test]
async fn a_resumed_page_continues_in_the_parent_of_a_directory_that_moved_away() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = namespace_id("demo");
    seed(&store, &namespace_id, SMALL_TREE).await;

    let first = page(&store, &namespace_id, "/docs", 3, None, listing(true))
        .await
        .expect("first page");
    assert_eq!(paths(&first.items), &SMALL_TREE_LISTING[..3]);
    submit_operation(
        &store,
        &namespace_id,
        CommitId::generate(),
        FilesystemOperation::MovePath {
            source_path: AbsolutePath::parse("/docs/a/m").expect("source"),
            destination_path: AbsolutePath::parse("/moved").expect("destination"),
            precondition: DestinationPrecondition {
                behavior: DestinationBehavior::NoReplace,
                expected_inode_id: None,
                expected_revision_no: None,
            },
        },
        &mutation_context(),
    )
    .await
    .expect("move the directory the cursor is in");

    let second = page(
        &store,
        &namespace_id,
        "/docs",
        1_000,
        first.next_cursor,
        listing(true),
    )
    .await
    .expect("resumed page");
    assert_eq!(paths(&second.items), &SMALL_TREE_LISTING[3..]);
    assert!(second.next_cursor.is_none());
}

/// Checks that a cursor from a listing of one directory's children can't
/// resume a recursive listing, and the reverse.
#[tokio::test]
async fn a_cursor_resumes_only_the_kind_of_listing_that_issued_it() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = namespace_id("demo");
    seed(&store, &namespace_id, SMALL_TREE).await;

    for recursive in [false, true] {
        let cursor = page(&store, &namespace_id, "/docs", 1, None, listing(recursive))
            .await
            .expect("first page")
            .next_cursor
            .expect("cursor");
        let error = page(
            &store,
            &namespace_id,
            "/docs",
            1,
            Some(cursor),
            listing(!recursive),
        )
        .await
        .expect_err("a cursor of the other kind");
        assert_matches!(error, CoreError::InvalidCursor(_), "recursive {recursive}");
    }
}

/// Checks that a recursive page over a tree of directories costs the same
/// store requests as a page of one directory with as many entries, because
/// every directory's rows come from the same segments.
#[tokio::test]
async fn a_recursive_page_costs_the_store_requests_of_one_directory_page() {
    let temp_dir = tempdir().expect("tempdir");
    let store = RecordingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::any(),
    );
    let namespace_id = namespace_id("demo");
    let mut paths = vec!["/tree/".to_owned(), "/flat/".to_owned()];
    for directory in 0..16 {
        paths.push(format!("/tree/d{directory:02}/"));
        for file in 0..4 {
            paths.push(format!("/tree/d{directory:02}/f{file}.txt"));
            paths.push(format!("/flat/d{directory:02}-f{file}.txt"));
        }
    }
    for directory in 0..16 {
        paths.push(format!("/flat/d{directory:02}/"));
    }
    let paths: Vec<&str> = paths.iter().map(String::as_str).collect();
    seed(&store, &namespace_id, &paths).await;
    create_checkpoint(&store, &namespace_id, &mutation_context())
        .await
        .expect("fold the tree into segments");

    let mut requests = Vec::new();
    for (path, recursive) in [("/tree", true), ("/flat", false)] {
        let context = read_context(&store, &namespace_id).await;
        store.reset();
        let listed = namespace_engine(&store, &namespace_id, &mutation_context())
            .list_path_page(
                path,
                PageRequest {
                    limit: page_limit(1_000),
                    cursor: None,
                },
                listing(recursive),
                &context,
            )
            .await
            .expect("list page");
        assert_eq!(listed.items.len(), 80, "{path}");
        assert!(listed.next_cursor.is_none());
        requests.push(store.counts());
    }
    assert_eq!(requests[0], requests[1]);
}
