//! Paths derived from stored names can exceed the request path limits, and
//! inode-addressed reads and writes still serve them.

#![allow(clippy::panic)]
// Runtime integration tests use panic in helper assertions for precise diagnostics.

use crate::common::{expect_code, open_runtime_async, store};
use loonfs::publish::{CommitRequest, FilesystemOperation};
use loonfs::{
    CommitId, CreateDirectoryOptions, ErrorCode, FilesystemChange, InodeId, Namespace, PageRequest,
    PaginationPolicy, Writable,
};
use loonfs_test_support::ids::namespace_id;
use loonfs_types::{
    DestinationPrecondition, DisplayName, MAX_PATH_BYTES, MAX_PATH_DEPTH, ROOT_INODE_ID,
};
use tempfile::tempdir;

fn page_request() -> PageRequest<String> {
    PageRequest {
        limit: PaginationPolicy::default()
            .resolve_limit(None)
            .expect("default page limit"),
        cursor: None,
    }
}

async fn create_directory_by_inode(
    namespace: &Namespace<Writable>,
    parent_inode_id: InodeId,
    name: &str,
) -> InodeId {
    let commit = namespace
        .commit(CommitRequest::single(
            CommitId::generate(),
            loonfs_test_support::test_actor(),
            None,
            FilesystemOperation::CreateDirectoryByInode {
                parent_inode_id,
                display_name: DisplayName::parse(name).expect("display name"),
            },
        ))
        .await
        .expect("create directory by inode");
    match commit.events.as_slice() {
        [FilesystemChange::DirectoryCreated { inode_id, .. }] => *inode_id,
        other => panic!("expected one created directory, got {other:?}"),
    }
}

/// Moves a file into `directory` by inode and reads both back through the
/// inode-addressed readers. A request that names the directory's path is
/// still refused.
async fn assert_inode_addressing_serves(
    namespace: &Namespace<Writable>,
    directory: InodeId,
    directory_path: &str,
) {
    assert_eq!(
        namespace
            .stat_by_inode(directory)
            .await
            .expect("stat the directory by inode")
            .path
            .as_str(),
        directory_path
    );

    namespace
        .put_file("/loose.txt", b"loose", &loonfs_test_support::test_actor())
        .await
        .expect("put a file to move");
    let loose = namespace.stat("/loose.txt").await.expect("stat the file");
    namespace
        .commit(CommitRequest::single(
            CommitId::generate(),
            loonfs_test_support::test_actor(),
            None,
            FilesystemOperation::MoveByInode {
                inode_id: loose.inode_id,
                expected_binding_version: loose.binding_version.expect("binding version"),
                destination_parent_inode_id: directory,
                destination_display_name: DisplayName::parse("moved.txt").expect("name"),
                precondition: DestinationPrecondition::default(),
            },
        ))
        .await
        .expect("move the file into the directory by inode");

    let file_path = format!("{directory_path}/moved.txt");
    assert_eq!(
        namespace
            .stat_by_inode(loose.inode_id)
            .await
            .expect("stat the moved file by inode")
            .path
            .as_str(),
        file_path
    );
    let listing = namespace
        .list_by_inode(directory)
        .page(page_request())
        .await
        .expect("list the directory by inode");
    assert_eq!(
        listing
            .entries
            .iter()
            .map(|entry| entry.path.as_str())
            .collect::<Vec<_>>(),
        [file_path.as_str()]
    );
    let current = namespace
        .resolve_current_files(&[loose.inode_id])
        .await
        .expect("resolve the moved file");
    assert_eq!(
        current[0].current_path.as_ref().map(|path| path.as_str()),
        Some(file_path.as_str())
    );

    // Path-addressed requests keep the parse-time limits.
    expect_code(
        namespace.stat(directory_path).await,
        ErrorCode::InvalidRequest,
    );
}

#[tokio::test]
async fn inode_addressing_serves_a_derived_path_over_the_byte_limit() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = open_runtime_async(store(temp_dir.path()), "derived-path-bytes").await;
    let namespace_id = namespace_id("demo");
    fs.writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let namespace = fs
        .writer
        .open_namespace(&namespace_id)
        .expect("open namespace");

    let mut directory = ROOT_INODE_ID;
    let mut directory_path = String::new();
    for level in 0..20 {
        let name = format!("{level:02}{}", "n".repeat(253));
        directory = create_directory_by_inode(&namespace, directory, &name).await;
        directory_path = format!("{directory_path}/{name}");
    }
    assert!(directory_path.len() > MAX_PATH_BYTES);

    assert_inode_addressing_serves(&namespace, directory, &directory_path).await;
}

#[tokio::test]
async fn inode_addressing_serves_a_derived_path_over_the_depth_limit() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = open_runtime_async(store(temp_dir.path()), "derived-path-depth").await;
    let namespace_id = namespace_id("demo");
    let actor = loonfs_test_support::test_actor();
    fs.writer
        .create_namespace(&namespace_id, &actor)
        .await
        .expect("create namespace");
    let namespace = fs
        .writer
        .open_namespace(&namespace_id)
        .expect("open namespace");

    let chain = |root: &str| format!("/{root}{}", "/d".repeat(64));
    for root in ["a", "b"] {
        namespace
            .create_directory_with_options(
                &chain(root),
                &actor,
                &CreateDirectoryOptions {
                    parents: true,
                    ..Default::default()
                },
            )
            .await
            .expect("create a chain by path");
    }
    let directory = namespace
        .stat(&chain("b"))
        .await
        .expect("stat the deepest directory")
        .inode_id;
    // Both paths this move names are within the limits; the subtree it
    // carries ends deeper than any request may name.
    namespace
        .move_path("/b", &format!("{}/b", chain("a")), &actor)
        .await
        .expect("move a directory by path");
    let directory_path = format!("{}{}", chain("a"), chain("b"));
    assert!(directory_path.split('/').count() - 1 > MAX_PATH_DEPTH);

    assert_inode_addressing_serves(&namespace, directory, &directory_path).await;
}
