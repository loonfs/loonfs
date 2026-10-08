//! Current metadata and revision reads by inode id.

use crate::common::{open_runtime_async, store};
use loonfs::{
    ErrorCode, InodeId, LoonFs, PageRequest, PaginationPolicy, PutFileOptions, RevisionNo,
};
use loonfs_test_support::ids::namespace_id;
use tempfile::tempdir;

fn page_request() -> PageRequest<String> {
    PageRequest {
        limit: PaginationPolicy::default()
            .resolve_limit(None)
            .expect("default page limit"),
        cursor: None,
    }
}

#[tokio::test]
async fn stat_inode_tracks_a_rename_and_retained_revisions_keep_the_same_identity() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = open_runtime_async(store(temp_dir.path()), "inode-rename-test").await;
    let namespace_id = namespace_id("demo");
    let namespace = fs.reader.namespace(&namespace_id);
    let actor = loonfs_test_support::test_actor();
    fs.writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let namespace_writer = fs
        .writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    namespace_writer
        .put_file("/before.txt", b"one", &actor)
        .await
        .expect("put first revision");
    namespace_writer
        .put_file_with_options(
            "/before.txt",
            b"two",
            &actor,
            &PutFileOptions {
                behavior: loonfs::DestinationBehavior::Replace,
                ..Default::default()
            },
        )
        .await
        .expect("put second revision");

    let path_entry = namespace
        .stat("/before.txt")
        .await
        .expect("stat path before rename");
    let inode_id = path_entry.inode_id;
    let inode_entry = namespace
        .stat_by_inode(inode_id)
        .await
        .expect("stat inode before rename");
    assert_eq!(inode_entry, path_entry);
    assert_eq!(
        namespace
            .read_file_revision_by_inode(inode_id, RevisionNo(1))
            .await
            .expect("read revision before rename"),
        b"one"
    );

    namespace_writer
        .move_path("/before.txt", "/after.txt", &actor)
        .await
        .expect("rename file");
    let after = namespace
        .stat_by_inode(inode_id)
        .await
        .expect("stat inode after rename");
    assert_eq!(after.inode_id, inode_id);
    assert_eq!(after.path.as_str(), "/after.txt");
    assert_eq!(
        after,
        namespace
            .stat("/after.txt")
            .await
            .expect("stat renamed path")
    );
    assert_eq!(
        namespace
            .read_file_by_inode(inode_id)
            .await
            .expect("read current content by inode after rename"),
        namespace
            .read_file("/after.txt")
            .await
            .expect("read renamed path")
    );
    let revisions = namespace
        .list_file_revisions_by_inode(inode_id)
        .page(page_request())
        .await
        .expect("list revisions after rename");
    assert_eq!(revisions.inode_id, inode_id);
    assert_eq!(revisions.revisions.len(), 2);
    assert_eq!(
        namespace
            .read_file_revision_by_inode(inode_id, RevisionNo(1))
            .await
            .expect("read revision after rename"),
        b"one"
    );

    namespace_writer
        .delete_path("/after.txt", &actor)
        .await
        .expect("delete file");
    let hidden = namespace
        .stat_by_inode(inode_id)
        .await
        .expect_err("deleted inode is not current");
    assert_eq!(hidden.code(), ErrorCode::InodeNotFound);
    assert_eq!(
        namespace
            .read_file_by_inode(inode_id)
            .await
            .expect_err("deleted inode has no current content")
            .code(),
        ErrorCode::InodeNotFound
    );
    assert_eq!(
        namespace
            .read_file_revision_by_inode(inode_id, RevisionNo(2))
            .await
            .expect("read retained deleted revision"),
        b"two"
    );
    assert_eq!(
        namespace
            .list_file_revisions_by_inode(inode_id)
            .page(page_request())
            .await
            .expect("list retained deleted revisions")
            .revisions
            .len(),
        2
    );
}

#[tokio::test]
async fn stat_inode_preserves_the_nameless_root_and_revision_error_conventions() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = open_runtime_async(store(temp_dir.path()), "inode-error-test").await;
    let namespace_id = namespace_id("demo");
    let namespace = fs.reader.namespace(&namespace_id);
    let actor = loonfs_test_support::test_actor();
    fs.writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let namespace_writer = fs
        .writer
        .open_namespace(&namespace_id)
        .expect("open namespace");

    let root = namespace
        .stat_by_inode(InodeId(1))
        .await
        .expect("stat root inode");
    assert_eq!(root.path.as_str(), "/");
    assert_eq!(root.parent_inode_id, None);
    assert_eq!(root.display_name, None);
    assert_eq!(root, namespace.stat("/").await.expect("stat root path"));

    namespace_writer
        .create_directory("/docs", &actor)
        .await
        .expect("create directory");
    let directory = namespace.stat("/docs").await.expect("stat directory");
    let directory_error = namespace
        .list_file_revisions_by_inode(directory.inode_id)
        .page(page_request())
        .await
        .expect_err("directory has no file revisions");
    assert_eq!(directory_error.code(), ErrorCode::PathConflict);

    for error in [
        namespace
            .stat_by_inode(InodeId(u64::MAX))
            .await
            .expect_err("unknown inode stat"),
        namespace
            .read_file_revision_by_inode(InodeId(u64::MAX), RevisionNo(1))
            .await
            .expect_err("unknown inode content"),
    ] {
        assert_eq!(error.code(), ErrorCode::InodeNotFound);
        assert_ne!(error.code(), ErrorCode::PathNotFound);
    }
}

#[tokio::test]
async fn stat_inode_and_stat_path_have_the_same_point_lookup_request_count() {
    use loonfs_test_support::stores::{KeyPredicate, RecordingStore};
    use std::sync::Arc;

    let temp_dir = tempdir().expect("tempdir");
    let recorded = Arc::new(RecordingStore::new(
        loonfs_objectstore::local_fs_store::LocalFsStore::new(temp_dir.path())
            .expect("local store"),
        KeyPredicate::any(),
    ));
    let shared: loonfs::SharedObjectStore = recorded.clone();
    let fs = open_runtime_async(shared.clone(), "inode-accounting-writer").await;
    let namespace_id = namespace_id("demo");
    let namespace = fs.reader.namespace(&namespace_id);
    fs.writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let namespace_writer = fs
        .writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    namespace_writer
        .put_file("/file.txt", b"body", &loonfs_test_support::test_actor())
        .await
        .expect("put file");
    let inode_id = namespace
        .stat("/file.txt")
        .await
        .expect("discover inode")
        .inode_id;
    fs.writer.drain().await.expect("finish hints");
    drop(fs);
    let _ = recorded.take_gets();

    let path_reader = LoonFs::builder_with_store(shared.clone())
        .read_only()
        .build()
        .await
        .expect("build path reader");
    let path_namespace = path_reader.namespace(&namespace_id);
    path_namespace
        .stat("/file.txt")
        .await
        .expect("cold path stat");
    let path_gets = recorded.take_gets();
    drop(path_reader);

    let inode_reader = LoonFs::builder_with_store(shared)
        .read_only()
        .build()
        .await
        .expect("build inode reader");
    let inode_namespace = inode_reader.namespace(&namespace_id);
    inode_namespace
        .stat_by_inode(inode_id)
        .await
        .expect("cold inode stat");
    let inode_gets = recorded.take_gets();

    assert_eq!(
        inode_gets.len(),
        path_gets.len(),
        "identity stat must remain a point lookup: path={path_gets:#?}, inode={inode_gets:#?}"
    );
}
