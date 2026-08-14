//! Inode-addressed current metadata and retained revision reads.

use crate::common::{open_runtime_async, store};
use loonfs::{
    CreateDirectoryOptions, CreateNamespaceOptions, DeleteOptions, ErrorCode, FsReader, InodeId,
    MoveOptions, PageRequest, PaginationPolicy, PutFileOptions, RevisionNo, StatPathOptions,
};
use loonfs_test_support::ids::namespace_id;
use tempfile::tempdir;

fn page_request() -> PageRequest<loonfs::FileRevisionsPageCursor> {
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
    let actor = loonfs_test_support::test_actor();
    fs.writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    fs.writer
        .put_file_bytes(
            &namespace_id,
            "/before.txt",
            b"one",
            PutFileOptions::new(actor.clone()),
        )
        .await
        .expect("put first revision");
    fs.writer
        .put_file_bytes(
            &namespace_id,
            "/before.txt",
            b"two",
            PutFileOptions {
                behavior: loonfs::DestinationBehavior::Replace,
                ..PutFileOptions::new(actor.clone())
            },
        )
        .await
        .expect("put second revision");

    let path_entry = fs
        .reader
        .stat_path(&namespace_id, "/before.txt", StatPathOptions::default())
        .await
        .expect("stat path before rename");
    let inode_id = path_entry.inode_id;
    let inode_entry = fs
        .reader
        .stat_inode(&namespace_id, inode_id, StatPathOptions::default())
        .await
        .expect("stat inode before rename");
    assert_eq!(inode_entry, path_entry);
    assert_eq!(
        fs.reader
            .get_file_revision_bytes_by_inode(&namespace_id, inode_id, RevisionNo(1))
            .await
            .expect("read revision before rename"),
        b"one"
    );

    fs.writer
        .move_path(
            &namespace_id,
            "/before.txt",
            "/after.txt",
            MoveOptions::new(actor.clone()),
        )
        .await
        .expect("rename file");
    let after = fs
        .reader
        .stat_inode(&namespace_id, inode_id, StatPathOptions::default())
        .await
        .expect("stat inode after rename");
    assert_eq!(after.inode_id, inode_id);
    assert_eq!(after.path.as_str(), "/after.txt");
    assert_eq!(
        after,
        fs.reader
            .stat_path(&namespace_id, "/after.txt", StatPathOptions::default())
            .await
            .expect("stat renamed path")
    );
    let revisions = fs
        .reader
        .list_file_revisions_by_inode_page(&namespace_id, inode_id, page_request())
        .await
        .expect("list revisions after rename");
    assert_eq!(revisions.inode_id, inode_id);
    assert_eq!(revisions.revisions.len(), 2);
    assert_eq!(
        fs.reader
            .get_file_revision_bytes_by_inode(&namespace_id, inode_id, RevisionNo(1))
            .await
            .expect("read revision after rename"),
        b"one"
    );

    fs.writer
        .delete_path(&namespace_id, "/after.txt", DeleteOptions::new(actor))
        .await
        .expect("delete file");
    let hidden = fs
        .reader
        .stat_inode(&namespace_id, inode_id, StatPathOptions::default())
        .await
        .expect_err("deleted inode is not current");
    assert_eq!(hidden.code(), ErrorCode::InodeNotFound);
    assert_eq!(
        fs.reader
            .get_file_revision_bytes_by_inode(&namespace_id, inode_id, RevisionNo(2))
            .await
            .expect("read retained deleted revision"),
        b"two"
    );
    assert_eq!(
        fs.reader
            .list_file_revisions_by_inode_page(&namespace_id, inode_id, page_request())
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
    let actor = loonfs_test_support::test_actor();
    fs.writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");

    let root = fs
        .reader
        .stat_inode(&namespace_id, InodeId(1), StatPathOptions::default())
        .await
        .expect("stat root inode");
    assert_eq!(root.path.as_str(), "/");
    assert_eq!(root.parent_inode_id, None);
    assert_eq!(root.display_name, None);
    assert_eq!(
        root,
        fs.reader
            .stat_path(&namespace_id, "/", StatPathOptions::default())
            .await
            .expect("stat root path")
    );

    fs.writer
        .create_directory(&namespace_id, "/docs", CreateDirectoryOptions::new(actor))
        .await
        .expect("create directory");
    let directory = fs
        .reader
        .stat_path(&namespace_id, "/docs", StatPathOptions::default())
        .await
        .expect("stat directory");
    let directory_error = fs
        .reader
        .list_file_revisions_by_inode_page(&namespace_id, directory.inode_id, page_request())
        .await
        .expect_err("directory has no file revisions");
    assert_eq!(directory_error.code(), ErrorCode::PathConflict);

    for error in [
        fs.reader
            .stat_inode(&namespace_id, InodeId(u64::MAX), StatPathOptions::default())
            .await
            .expect_err("unknown inode stat"),
        fs.reader
            .get_file_revision_bytes_by_inode(&namespace_id, InodeId(u64::MAX), RevisionNo(1))
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
    fs.writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    fs.writer
        .put_file_bytes(
            &namespace_id,
            "/file.txt",
            b"body",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("put file");
    let inode_id = fs
        .reader
        .stat_path(&namespace_id, "/file.txt", StatPathOptions::default())
        .await
        .expect("discover inode")
        .inode_id;
    drop(fs);
    let _ = recorded.take_gets();

    let path_reader = FsReader::builder_with_store(shared.clone())
        .build()
        .await
        .expect("build path reader");
    path_reader
        .stat_path(&namespace_id, "/file.txt", StatPathOptions::default())
        .await
        .expect("cold path stat");
    let path_gets = recorded.take_gets();
    drop(path_reader);

    let inode_reader = FsReader::builder_with_store(shared)
        .build()
        .await
        .expect("build inode reader");
    inode_reader
        .stat_inode(&namespace_id, inode_id, StatPathOptions::default())
        .await
        .expect("cold inode stat");
    let inode_gets = recorded.take_gets();

    assert_eq!(
        inode_gets.len(),
        path_gets.len(),
        "identity stat must remain a point lookup: path={path_gets:#?}, inode={inode_gets:#?}"
    );
}
