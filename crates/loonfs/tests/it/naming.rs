//! Per-namespace naming modes: how a namespace compares sibling names.

#![allow(clippy::panic)]

use crate::common::{expect_code, store, writer};
use loonfs::{CreateNamespaceOptions, ErrorCode, LoonFs, NamespaceId, Writable};
use loonfs_types::NamespaceNaming;
use tempfile::tempdir;

async fn create_case_sensitive(writer: &LoonFs<Writable>, namespace_id: &NamespaceId) {
    let created = writer
        .create_namespace_with_options(
            namespace_id,
            &loonfs_test_support::test_actor(),
            &CreateNamespaceOptions {
                naming: NamespaceNaming::CaseSensitive,
                ..Default::default()
            },
        )
        .await
        .expect("create a case-sensitive namespace");
    assert_eq!(created.naming, NamespaceNaming::CaseSensitive);
}

#[tokio::test]
async fn a_case_sensitive_namespace_keeps_names_that_differ_only_in_case_apart() {
    let temp_dir = tempdir().expect("tempdir");
    let writer = writer(store(temp_dir.path()), "case-sensitive").await;
    let namespace_id = NamespaceId::parse("case-sensitive").expect("namespace id");
    create_case_sensitive(&writer, &namespace_id).await;
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    let actor = loonfs_test_support::test_actor();
    namespace
        .put_file("/Report.txt", b"upper", &actor)
        .await
        .expect("put Report.txt");
    namespace
        .put_file("/report.txt", b"lower", &actor)
        .await
        .expect("put report.txt beside it");

    let listed = namespace
        .list("/")
        .next()
        .await
        .expect("one page")
        .expect("list the root");
    let paths: Vec<_> = listed
        .entries
        .iter()
        .map(|entry| entry.path.as_str())
        .collect();
    assert_eq!(paths, ["/Report.txt", "/report.txt"]);
    let upper = namespace
        .stat("/Report.txt")
        .await
        .expect("stat Report.txt");
    let lower = namespace
        .stat("/report.txt")
        .await
        .expect("stat report.txt");
    assert_ne!(upper.inode_id, lower.inode_id);
    expect_code(namespace.stat("/REPORT.txt").await, ErrorCode::PathNotFound);

    // A rename that only changes case moves the file to a free slot, so the
    // old spelling no longer resolves.
    namespace
        .move_path("/Report.txt", "/REPORT.txt", &actor)
        .await
        .expect("rename to the free slot");
    expect_code(namespace.stat("/Report.txt").await, ErrorCode::PathNotFound);
    let renamed = namespace
        .stat("/REPORT.txt")
        .await
        .expect("stat REPORT.txt");
    assert_eq!(renamed.inode_id, upper.inode_id);
    assert_eq!(
        namespace
            .stat("/report.txt")
            .await
            .expect("stat report.txt")
            .inode_id,
        lower.inode_id
    );
    expect_code(
        namespace
            .move_path("/REPORT.txt", "/report.txt", &actor)
            .await,
        ErrorCode::PathConflict,
    );
}

#[tokio::test]
async fn a_fork_of_a_case_sensitive_namespace_is_case_sensitive() {
    let temp_dir = tempdir().expect("tempdir");
    let writer = writer(store(temp_dir.path()), "fork-naming").await;
    let source_id = NamespaceId::parse("source").expect("namespace id");
    let fork_id = NamespaceId::parse("fork").expect("namespace id");
    create_case_sensitive(&writer, &source_id).await;
    let actor = loonfs_test_support::test_actor();
    writer
        .open_namespace(&source_id)
        .expect("open source")
        .put_file("/Report.txt", b"upper", &actor)
        .await
        .expect("put Report.txt");

    let forked = writer
        .fork_namespace(&source_id, &fork_id, &actor)
        .await
        .expect("fork");
    assert_eq!(forked.naming, NamespaceNaming::CaseSensitive);
    let fork = writer.open_namespace(&fork_id).expect("open fork");
    fork.put_file("/report.txt", b"lower", &actor)
        .await
        .expect("the fork admits the other spelling");
    assert_ne!(
        fork.stat("/Report.txt")
            .await
            .expect("stat Report.txt")
            .inode_id,
        fork.stat("/report.txt")
            .await
            .expect("stat report.txt")
            .inode_id
    );
}
