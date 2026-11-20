//! The store's namespace listing.

use crate::common::*;
use bytes::Bytes;
use loonfs::{
    CommitSeq, EffectiveLimit, ErrorCode, ExecutionBudget, InodeId, ListNamespacesResponse,
    NamespaceId, NamespacesPager, SharedObjectStore,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::{ObjectStore as _, PutMode};
use loonfs_test_support::ids::namespace_id;
use loonfs_test_support::stores::{FailStore, InjectedError, KeyPredicate, OperationSelector};
use loonfs_types::{NamespaceListPageCursor, TrashPageCursor};
use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;

/// Returns every page of `pager`.
async fn pages(mut pager: NamespacesPager) -> Vec<ListNamespacesResponse> {
    let mut pages = Vec::new();
    while let Some(page) = pager.next().await {
        pages.push(page.expect("namespace page"));
    }
    pages
}

/// Returns the IDs of a page's namespaces.
fn ids(page: &ListNamespacesResponse) -> Vec<&str> {
    page.namespaces
        .iter()
        .map(|namespace| namespace.namespace_id.as_str())
        .collect()
}

async fn create(fs: &TestRuntime, names: &[&str]) -> Vec<NamespaceId> {
    let mut created = Vec::new();
    for name in names {
        let id = namespace_id(name);
        fs.create_namespace(&id, &loonfs_test_support::test_actor())
            .await
            .expect("create namespace");
        created.push(id);
    }
    created
}

fn page_limit(limit: u32) -> EffectiveLimit {
    EffectiveLimit::new(NonZeroU32::new(limit).expect("nonzero limit"))
}

/// Checks the listing's order, which is the order of the keys
/// `namespaces/{id}/`: `a-b` comes before `a`, because `-` sorts before the
/// `/` that ends `a`'s key, and `a_b` and `alpha` come after `a`.
#[tokio::test]
async fn namespaces_are_listed_in_key_order_with_their_metadata() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = open_runtime_async(store(temp_dir.path()), "namespace-list-order").await;
    create(&fs, &["alpha", "a", "a_b", "a-b"]).await;

    let pages = pages(fs.writer.list_namespaces().page_limit(page_limit(2))).await;

    let listed: Vec<_> = pages
        .iter()
        .flat_map(|page| page.namespaces.clone())
        .collect();
    assert_eq!(
        listed
            .iter()
            .map(|namespace| namespace.namespace_id.as_str())
            .collect::<Vec<_>>(),
        ["a-b", "a", "a_b", "alpha"]
    );
    for namespace in &listed {
        let metadata = fs
            .writer
            .namespace(&namespace.namespace_id)
            .metadata()
            .await
            .expect("namespace metadata");
        assert_eq!(namespace, &metadata);
    }
}

/// Checks that a deleted namespace is left out of its page but still ends
/// the page it was listed on, so the page after it resumes past it.
#[tokio::test]
async fn a_deleted_namespace_is_left_out_and_keeps_its_place_in_the_page() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = open_runtime_async(store(temp_dir.path()), "namespace-list-deleted").await;
    let [_, bravo, _] =
        <[NamespaceId; 3]>::try_from(create(&fs, &["alpha", "bravo", "charlie"]).await)
            .expect("three namespaces");
    fs.writer
        .open_namespace(&bravo)
        .expect("open namespace")
        .delete()
        .await
        .expect("delete namespace");

    let paged = pages(fs.reader.list_namespaces().page_limit(page_limit(1))).await;
    assert_eq!(
        paged
            .iter()
            .map(|page| (ids(page), page.next_cursor.is_some()))
            .collect::<Vec<_>>(),
        [
            (vec!["alpha"], true),
            (vec![], true),
            (vec!["charlie"], false),
        ]
    );

    let page = fs.reader.list_namespaces().page().await.expect("one page");
    assert_eq!(ids(&page), ["alpha", "charlie"]);
    assert_eq!(page.next_cursor, None);
}

/// Checks that a namespace whose creation stopped after its first manifest,
/// which reads as `namespace_not_found`, is left out of the listing.
#[tokio::test]
async fn an_initializing_namespace_is_left_out() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(FailStore::new(
        LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
        KeyPredicate::new(|key| key.starts_with("pools/") && key.contains("/members/")),
        OperationSelector::Put,
        InjectedError::Transport("member entry".into()),
    ));
    let object_store: SharedObjectStore = store.clone();
    let fs = open_runtime_async(object_store, "namespace-list-initializing").await;
    let half = namespace_id("half");
    store.fail_all();
    fs.create_namespace(&half, &loonfs_test_support::test_actor())
        .await
        .expect_err("the member entry write fails");
    store.clear();
    expect_code(
        fs.writer.namespace(&half).metadata().await,
        ErrorCode::NamespaceNotFound,
    );
    create(&fs, &["zulu"]).await;

    let page = fs.writer.list_namespaces().page().await.expect("one page");

    assert_eq!(ids(&page), ["zulu"]);
    assert_eq!(page.next_cursor, None);
}

/// Checks that a store page with no namespace in it, here a page of one
/// child of `namespaces/` that is not a namespace, returns no namespaces and
/// a cursor, and that the next page resumes after it, with one store listing
/// per page.
#[tokio::test]
async fn a_store_page_without_a_namespace_returns_a_cursor_and_no_namespaces() {
    let temp_dir = tempdir().expect("tempdir");
    let store = store(temp_dir.path());
    let fs = open_runtime_async(store.clone(), "namespace-list-foreign").await;
    // An uppercase name is not a namespace ID, and sorts before every ID.
    store
        .put(
            "namespaces/Foreign/object",
            Bytes::from_static(b"not a namespace"),
            PutMode::CreateIfAbsent,
        )
        .await
        .expect("put a foreign object");
    create(&fs, &["alpha"]).await;

    let paged = pages(fs.reader.list_namespaces().page_limit(page_limit(1))).await;

    assert_eq!(
        paged
            .iter()
            .map(|page| (ids(page), page.next_cursor.is_some()))
            .collect::<Vec<_>>(),
        [(vec![], true), (vec!["alpha"], false)]
    );
}

#[tokio::test]
async fn a_cursor_outside_the_namespace_listing_is_rejected() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = open_runtime_async(store(temp_dir.path()), "namespace-list-key").await;
    let cursor = loonfs_types::encode_cursor(&NamespaceListPageCursor {
        last_key: "pools/pool_01/".to_owned(),
    })
    .expect("encode cursor");

    expect_code(
        fs.reader
            .list_namespaces()
            .cursor(Some(cursor))
            .page()
            .await,
        ErrorCode::InvalidRequest,
    );
}

#[tokio::test]
async fn a_cursor_from_another_listing_is_rejected() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = open_runtime_async(store(temp_dir.path()), "namespace-list-cursor").await;
    let trash_cursor = loonfs_types::encode_cursor(&TrashPageCursor {
        head_seq: CommitSeq(1),
        last_deletion_seq: CommitSeq(1),
        last_root_inode_id: InodeId(2),
    })
    .expect("encode cursor");

    expect_code(
        fs.reader
            .list_namespaces()
            .cursor(Some(trash_cursor))
            .page()
            .await,
        ErrorCode::InvalidRequest,
    );
}

/// Checks that a page finishes on a runtime with one read permit: the store
/// listing takes the permit and releases it before the metadata reads take
/// theirs, one at a time.
#[tokio::test]
async fn a_listing_finishes_with_one_read_permit() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = open_runtime_with_async(store(temp_dir.path()), "namespace-list-permit", |builder| {
        builder.execution_budget(
            ExecutionBudget::builder()
                .max_concurrent_reads(NonZeroUsize::MIN)
                .build(),
        )
    })
    .await;
    create(&fs, &["alpha", "bravo"]).await;

    let page = loonfs_test_support::clock::bounded(
        Duration::from_secs(30),
        fs.writer.list_namespaces().page(),
    )
    .await
    .expect("a page before the timeout")
    .expect("one page");

    assert_eq!(ids(&page), ["alpha", "bravo"]);
    assert_eq!(page.next_cursor, None);
}

#[tokio::test]
async fn a_listing_without_a_page_limit_returns_a_small_store_in_one_page() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = open_runtime_async(store(temp_dir.path()), "namespace-list-default").await;
    create(&fs, &["alpha", "bravo", "charlie"]).await;

    let page = fs.writer.list_namespaces().page().await.expect("one page");

    assert_eq!(ids(&page), ["alpha", "bravo", "charlie"]);
    assert_eq!(page.next_cursor, None);
}
