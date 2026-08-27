//! Multi-call namespace read snapshots.

use crate::common::{open_runtime_async, store};
use loonfs::{
    CreateNamespaceOptions, DestinationBehavior, ErrorCode, NamespaceId, PageRequest,
    PaginationPolicy, PutFileOptions,
};
use tempfile::tempdir;

#[tokio::test]
async fn pinned_namespace_reads_keep_one_head_across_later_commits() {
    let temp_dir = tempdir().expect("tempdir");
    let runtime = open_runtime_async(store(temp_dir.path()), "snapshot-reader-test").await;
    let namespace_id = NamespaceId::parse("snapshot-reads").expect("namespace id");
    runtime
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    let created = runtime
        .put_file_bytes(
            &namespace_id,
            "/before.txt",
            b"before",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("create initial file");

    let snapshot = runtime
        .reader
        .pin_namespace(&namespace_id)
        .await
        .expect("pin namespace");
    assert_eq!(snapshot.head_seq(), created.committed_seq);
    let before = snapshot
        .get_path_entry("/before.txt", Default::default())
        .await
        .expect("resolve initial file");

    runtime
        .put_file_bytes(
            &namespace_id,
            "/before.txt",
            b"after",
            PutFileOptions {
                behavior: DestinationBehavior::Replace,
                ..PutFileOptions::new(loonfs_test_support::test_actor())
            },
        )
        .await
        .expect("replace file after pin");
    runtime
        .put_file_bytes(
            &namespace_id,
            "/later.txt",
            b"later",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("create file after pin");

    let pinned_before = snapshot
        .get_path_entry("/before.txt", Default::default())
        .await
        .expect("resolve pinned file");
    assert_eq!(pinned_before.revision_no(), before.revision_no());
    assert_eq!(
        snapshot
            .read_content_ref(
                pinned_before.content_ref().expect("file content reference"),
                64,
            )
            .await
            .expect("read pinned content"),
        b"before"
    );
    let later_error = snapshot
        .get_path_entry("/later.txt", Default::default())
        .await
        .expect_err("later file is absent from the pinned view");
    assert_eq!(later_error.code(), ErrorCode::PathNotFound);

    let page = snapshot
        .list_path_entries_page(
            "/",
            PageRequest {
                limit: PaginationPolicy::default()
                    .resolve_limit(None)
                    .expect("default page limit"),
                cursor: None,
            },
            Default::default(),
        )
        .await
        .expect("list pinned root");
    assert_eq!(page.head_seq, snapshot.head_seq());
    assert_eq!(
        page.entries
            .iter()
            .map(|entry| entry.path.as_str())
            .collect::<Vec<_>>(),
        ["/before.txt"]
    );
    let states = snapshot
        .resolve_current_files(&[before.inode_id])
        .await
        .expect("resolve pinned inode");
    assert_eq!(states[0].current_revision_no, before.revision_no());

    let latest = runtime
        .reader
        .get_file_bytes(&namespace_id, "/before.txt")
        .await
        .expect("read latest replacement");
    assert_eq!(latest.bytes, b"after");
    runtime
        .reader
        .get_path_entry(&namespace_id, "/later.txt", Default::default())
        .await
        .expect("latest view sees later file");
}
