//! Directory and revision pagination cursor behavior.

#![allow(clippy::panic)]
// Runtime integration tests use panic in helper assertions for precise diagnostics.

mod common;

use common::*;
use loonfs::{
    AuthoritativePathEntry, CreateNamespaceOptions, DestinationBehavior, ErrorCode, PageRequest,
    PutFileOptions,
};
use loonfs_test_support::block_on::block_on;
use loonfs_test_support::ids::{namespace_id, page_limit};
use tempfile::tempdir;

fn display_names(entries: &[AuthoritativePathEntry]) -> Vec<&str> {
    entries
        .iter()
        .map(|entry| {
            entry
                .display_name
                .as_ref()
                .expect("listed entry should carry a display name")
                .as_str()
        })
        .collect()
}

#[test]
fn directory_pages_use_canonical_name_key_order() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "directory-page-order-test");
    let namespace_id = namespace_id("demo");
    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    for path in [
        "/docs/Zebra.txt",
        "/docs/apple.txt",
        "/docs/B.txt",
        "/docs/a.txt",
    ] {
        fs.put_file_bytes_blocking(
            &namespace_id,
            path,
            path.as_bytes(),
            PutFileOptions::default(),
        )
        .expect("put file");
    }
    fs.create_checkpoint_blocking(&namespace_id)
        .expect("checkpoint");

    let limit = page_limit(2);
    let first = block_on(fs.list_path_entries_page(
        &namespace_id,
        "/docs",
        PageRequest {
            limit,
            cursor: None,
        },
    ))
    .expect("first directory page");
    assert_eq!(display_names(&first.entries), vec!["a.txt", "apple.txt"]);

    let cursor = decode_directory_page_cursor(first.next_cursor.as_deref().expect("next cursor"));
    let second = block_on(fs.list_path_entries_page(
        &namespace_id,
        "/docs",
        PageRequest {
            limit,
            cursor: Some(cursor),
        },
    ))
    .expect("second directory page");
    assert_eq!(display_names(&second.entries), vec!["B.txt", "Zebra.txt"]);
    assert!(second.next_cursor.is_none());

    let full = block_on(fs.list_path_entries(&namespace_id, "/docs")).expect("full listing");
    assert_eq!(
        display_names(&full.entries),
        vec!["a.txt", "apple.txt", "B.txt", "Zebra.txt"]
    );
}

#[test]
fn file_revision_pages_merge_manifest_and_wal_tail_newest_first() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "file-revision-page-test");
    let namespace_id = namespace_id("demo");
    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");

    let replace = PutFileOptions {
        behavior: DestinationBehavior::Replace,
        commit_id: None,
    };
    fs.put_file_bytes_blocking(&namespace_id, "/doc.txt", b"v1", PutFileOptions::default())
        .expect("put v1");
    fs.put_file_bytes_blocking(&namespace_id, "/doc.txt", b"v2", replace.clone())
        .expect("put v2");
    fs.create_checkpoint_blocking(&namespace_id)
        .expect("checkpoint after v2");
    fs.put_file_bytes_blocking(&namespace_id, "/doc.txt", b"v3", replace.clone())
        .expect("put v3");
    fs.put_file_bytes_blocking(&namespace_id, "/doc.txt", b"v4", replace)
        .expect("put v4");

    let limit = page_limit(2);
    let first = block_on(fs.list_file_revisions_page(
        &namespace_id,
        "/doc.txt",
        PageRequest {
            limit,
            cursor: None,
        },
    ))
    .expect("first revision page");
    assert_eq!(
        first
            .revisions
            .iter()
            .map(|revision| revision.revision_no.0)
            .collect::<Vec<_>>(),
        vec![4, 3]
    );

    let cursor =
        decode_file_revisions_page_cursor(first.next_cursor.as_deref().expect("next cursor"));
    let second = block_on(fs.list_file_revisions_page(
        &namespace_id,
        "/doc.txt",
        PageRequest {
            limit,
            cursor: Some(cursor),
        },
    ))
    .expect("second revision page");
    assert_eq!(
        second
            .revisions
            .iter()
            .map(|revision| revision.revision_no.0)
            .collect::<Vec<_>>(),
        vec![2, 1]
    );
    assert!(second.next_cursor.is_none());

    let inode_page = block_on(fs.list_file_revisions_for_inode_page(
        &namespace_id,
        first.inode_id,
        PageRequest {
            limit,
            cursor: None,
        },
    ))
    .expect("inode revision page");
    assert_eq!(
        inode_page
            .revisions
            .iter()
            .map(|revision| revision.revision_no.0)
            .collect::<Vec<_>>(),
        vec![4, 3]
    );
}

#[test]
fn directory_cursor_after_later_writes_is_rejected() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "directory-page-snapshot-test");
    let namespace_id = namespace_id("demo");
    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    for path in ["/docs/a.txt", "/docs/b.txt", "/docs/c.txt"] {
        fs.put_file_bytes_blocking(
            &namespace_id,
            path,
            path.as_bytes(),
            PutFileOptions::default(),
        )
        .expect("put file");
    }

    let limit = page_limit(2);
    let first = block_on(fs.list_path_entries_page(
        &namespace_id,
        "/docs",
        PageRequest {
            limit,
            cursor: None,
        },
    ))
    .expect("first directory page");
    assert_eq!(display_names(&first.entries), vec!["a.txt", "b.txt"]);
    let cursor = decode_directory_page_cursor(first.next_cursor.as_deref().expect("next cursor"));

    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/z.txt",
        b"newer",
        PutFileOptions::default(),
    )
    .expect("put later file");

    assert_core_error_kind(
        block_on(fs.list_path_entries_page(
            &namespace_id,
            "/docs",
            PageRequest {
                limit,
                cursor: Some(cursor),
            },
        )),
        ErrorCode::RebootstrapRequired,
    );
}

#[test]
fn directory_cursor_older_than_materialized_snapshot_floor_is_rejected() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "directory-page-floor-test");
    let namespace_id = namespace_id("demo");
    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    for path in ["/docs/a.txt", "/docs/b.txt", "/docs/c.txt"] {
        fs.put_file_bytes_blocking(
            &namespace_id,
            path,
            path.as_bytes(),
            PutFileOptions::default(),
        )
        .expect("put file");
    }

    let first = block_on(fs.list_path_entries_page(
        &namespace_id,
        "/docs",
        PageRequest {
            limit: page_limit(2),
            cursor: None,
        },
    ))
    .expect("first directory page");
    let cursor = decode_directory_page_cursor(first.next_cursor.as_deref().expect("next cursor"));

    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/z.txt",
        b"newer",
        PutFileOptions::default(),
    )
    .expect("put later file");
    fs.create_checkpoint_blocking(&namespace_id)
        .expect("checkpoint newer snapshot");

    assert_core_error_kind(
        block_on(fs.list_path_entries_page(
            &namespace_id,
            "/docs",
            PageRequest {
                limit: page_limit(2),
                cursor: Some(cursor),
            },
        )),
        ErrorCode::RebootstrapRequired,
    );
}

#[test]
fn directory_cursor_rejects_path_inode_mismatch() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "directory-page-mismatch-test");
    let namespace_id = namespace_id("demo");
    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    for path in ["/docs/a.txt", "/docs/b.txt"] {
        fs.put_file_bytes_blocking(
            &namespace_id,
            path,
            path.as_bytes(),
            PutFileOptions::default(),
        )
        .expect("put file");
    }

    let first = block_on(fs.list_path_entries_page(
        &namespace_id,
        "/docs",
        PageRequest {
            limit: page_limit(1),
            cursor: None,
        },
    ))
    .expect("first directory page");
    let cursor = decode_directory_page_cursor(first.next_cursor.as_deref().expect("next cursor"));

    assert_core_error_kind(
        block_on(fs.list_path_entries_page(
            &namespace_id,
            "/",
            PageRequest {
                limit: page_limit(1),
                cursor: Some(cursor),
            },
        )),
        ErrorCode::InvalidRequest,
    );
}
