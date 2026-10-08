//! Reads and mutations addressed by inode id.

use crate::common::{open_runtime_async, store, TestRuntime};
use loonfs::{
    CommitId, CommitOptions, DeleteByInodeOptions, DeleteDirectoryBehavior, DisplayName,
    EntryInodeKind, ErrorCode, InodeId, LoonFs, Namespace, PageRequest, PaginationPolicy,
    PutFileOptions, ReadFileStreamOptions, RevisionNo, Writable,
};
use loonfs_test_support::ids::namespace_id;
use std::num::NonZeroU64;
use std::path::Path;
use tempfile::tempdir;

fn page_request() -> PageRequest<String> {
    PageRequest {
        limit: PaginationPolicy::default()
            .resolve_limit(None)
            .expect("default page limit"),
        cursor: None,
    }
}

fn display_name(value: &str) -> DisplayName {
    DisplayName::parse(value).expect("valid display name")
}

/// A runtime with one empty namespace and the writable handle a host holds
/// for it.
async fn writable_namespace(root: &Path, writer_id: &str) -> (TestRuntime, Namespace<Writable>) {
    let fs = open_runtime_async(store(root), writer_id).await;
    let namespace_id = namespace_id("demo");
    fs.writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let namespace = fs
        .writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    (fs, namespace)
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

#[tokio::test]
async fn a_resumed_inode_stream_reads_only_the_rest_in_the_asked_chunks() {
    let temp_dir = tempdir().expect("tempdir");
    let (_fs, namespace) = writable_namespace(temp_dir.path(), "inode-stream-resume-test").await;
    let bytes: Vec<u8> = (0..4 * 1024 + 17)
        .map(|offset: usize| (offset % 251) as u8)
        .collect();
    namespace
        .put_file("/data.bin", &bytes, &loonfs_test_support::test_actor())
        .await
        .expect("put file");
    let inode_id = namespace
        .stat("/data.bin")
        .await
        .expect("stat file")
        .inode_id;
    let held = 1024 + 5;

    let mut stream = namespace
        .read_file_stream_by_inode_with_options(
            inode_id,
            &ReadFileStreamOptions {
                chunk_bytes: NonZeroU64::new(1024).expect("non-zero chunk size"),
                start_offset: held as u64,
            },
        )
        .await
        .expect("open resumed stream");
    stream
        .fold_resumed_prefix(&bytes[..held])
        .expect("fold the held prefix");
    let mut chunk_lengths = Vec::new();
    let mut fetched = Vec::new();
    while let Some(chunk) = stream.next_chunk().await.expect("verified chunk") {
        chunk_lengths.push(chunk.len());
        fetched.extend_from_slice(&chunk);
    }
    assert_eq!(chunk_lengths, [1024, 1024, 1024, 12]);
    assert_eq!(fetched, bytes[held..]);
}

#[tokio::test]
async fn create_directory_by_inode_binds_a_new_name_under_the_parent() {
    let temp_dir = tempdir().expect("tempdir");
    let (_fs, namespace) = writable_namespace(temp_dir.path(), "inode-mkdir-test").await;
    let actor = loonfs_test_support::test_actor();
    namespace
        .create_directory("/docs", &actor)
        .await
        .expect("create parent");
    let docs = namespace.stat("/docs").await.expect("stat parent").inode_id;
    let commit_id = CommitId::parse("mkdir-archive").expect("valid commit id");

    let commit = namespace
        .create_directory_by_inode_with_options(
            docs,
            &display_name("archive"),
            &actor,
            &CommitOptions {
                commit_id: Some(commit_id.clone()),
                ..Default::default()
            },
        )
        .await
        .expect("create directory by inode");
    assert_eq!(commit.commit_id, commit_id);
    let created = namespace
        .stat("/docs/archive")
        .await
        .expect("stat created directory");
    assert_eq!(created.inode_kind(), EntryInodeKind::Directory);
    assert_eq!(created.parent_inode_id, Some(docs));

    let error = namespace
        .create_directory_by_inode(docs, &display_name("archive"), &actor)
        .await
        .expect_err("a bound name is refused");
    assert_eq!(error.code(), ErrorCode::PathConflict);
}

#[tokio::test]
async fn create_file_by_inode_writes_a_new_name_under_the_parent() {
    let temp_dir = tempdir().expect("tempdir");
    let (_fs, namespace) = writable_namespace(temp_dir.path(), "inode-create-file-test").await;
    let actor = loonfs_test_support::test_actor();
    namespace
        .create_directory("/docs", &actor)
        .await
        .expect("create parent");
    let docs = namespace.stat("/docs").await.expect("stat parent").inode_id;

    namespace
        .create_file_by_inode(docs, &display_name("report.txt"), b"first", &actor)
        .await
        .expect("create file by inode");
    let created = namespace
        .read_file("/docs/report.txt")
        .await
        .expect("read created file");
    assert_eq!(created.bytes, b"first");
    assert_eq!(created.entry.parent_inode_id, Some(docs));

    let error = namespace
        .create_file_by_inode(docs, &display_name("report.txt"), b"second", &actor)
        .await
        .expect_err("a bound name is refused");
    assert_eq!(error.code(), ErrorCode::PathConflict);
    assert_eq!(
        namespace
            .read_file("/docs/report.txt")
            .await
            .expect("read unchanged file")
            .bytes,
        b"first"
    );
}

#[tokio::test]
async fn put_file_by_inode_follows_a_move_and_requires_the_current_revision() {
    let temp_dir = tempdir().expect("tempdir");
    let (_fs, namespace) = writable_namespace(temp_dir.path(), "inode-put-file-test").await;
    let actor = loonfs_test_support::test_actor();
    namespace
        .put_file("/draft.txt", b"one", &actor)
        .await
        .expect("put first revision");
    let inode_id = namespace
        .stat("/draft.txt")
        .await
        .expect("stat file")
        .inode_id;
    namespace
        .move_path("/draft.txt", "/final.txt", &actor)
        .await
        .expect("rename file");

    namespace
        .put_file_by_inode(inode_id, RevisionNo(1), b"two", &actor)
        .await
        .expect("put the next revision by inode");
    let current = namespace
        .read_file("/final.txt")
        .await
        .expect("read renamed file");
    assert_eq!(current.bytes, b"two");
    assert_eq!(current.entry.inode_id, inode_id);
    assert_eq!(current.entry.revision_no(), Some(RevisionNo(2)));

    let error = namespace
        .put_file_by_inode(inode_id, RevisionNo(1), b"three", &actor)
        .await
        .expect_err("a stale revision is refused");
    assert_eq!(error.code(), ErrorCode::StaleRevision);
}

#[tokio::test]
async fn delete_by_inode_requires_the_current_binding_version() {
    let temp_dir = tempdir().expect("tempdir");
    let (_fs, namespace) = writable_namespace(temp_dir.path(), "inode-delete-test").await;
    let actor = loonfs_test_support::test_actor();
    namespace
        .put_file("/drafts/notes.txt", b"body", &actor)
        .await
        .expect("put child");
    let stale = namespace.stat("/drafts").await.expect("stat directory");
    namespace
        .move_path("/drafts", "/archive", &actor)
        .await
        .expect("rename directory");
    let current = namespace
        .stat("/archive")
        .await
        .expect("stat renamed directory");
    let current_version = current
        .binding_version
        .expect("a named entry has a binding version");

    let error = namespace
        .delete_by_inode(
            stale.inode_id,
            &stale
                .binding_version
                .expect("a named entry has a binding version"),
            &actor,
        )
        .await
        .expect_err("a stale binding version is refused");
    assert_eq!(error.code(), ErrorCode::BindingVersionMismatch);
    let error = namespace
        .delete_by_inode(current.inode_id, &current_version, &actor)
        .await
        .expect_err("a non-empty directory needs a recursive delete");
    assert_eq!(error.code(), ErrorCode::DirectoryNotEmpty);

    namespace
        .delete_by_inode_with_options(
            current.inode_id,
            &current_version,
            &actor,
            &DeleteByInodeOptions {
                behavior: DeleteDirectoryBehavior::Recursive,
                ..Default::default()
            },
        )
        .await
        .expect("delete the directory by inode");
    assert_eq!(
        namespace
            .stat_by_inode(current.inode_id)
            .await
            .expect_err("a deleted inode is not visible")
            .code(),
        ErrorCode::InodeNotFound
    );
}

#[tokio::test]
async fn move_by_inode_rebinds_under_a_parent_inode_at_the_current_binding_version() {
    let temp_dir = tempdir().expect("tempdir");
    let (_fs, namespace) = writable_namespace(temp_dir.path(), "inode-move-test").await;
    let actor = loonfs_test_support::test_actor();
    namespace
        .put_file("/inbox/report.txt", b"body", &actor)
        .await
        .expect("put file");
    namespace
        .create_directory("/archive", &actor)
        .await
        .expect("create destination");
    let archive = namespace
        .stat("/archive")
        .await
        .expect("stat destination")
        .inode_id;
    let report = namespace
        .stat("/inbox/report.txt")
        .await
        .expect("stat file");
    let version = report
        .binding_version
        .expect("a named entry has a binding version");

    namespace
        .move_by_inode(
            report.inode_id,
            &version,
            archive,
            &display_name("2026.txt"),
            &actor,
        )
        .await
        .expect("move by inode");
    let moved = namespace
        .stat("/archive/2026.txt")
        .await
        .expect("stat moved file");
    assert_eq!(moved.inode_id, report.inode_id);
    assert_ne!(moved.binding_version, Some(version.clone()));

    let error = namespace
        .move_by_inode(
            report.inode_id,
            &version,
            archive,
            &display_name("2027.txt"),
            &actor,
        )
        .await
        .expect_err("a stale binding version is refused");
    assert_eq!(error.code(), ErrorCode::BindingVersionMismatch);
}
