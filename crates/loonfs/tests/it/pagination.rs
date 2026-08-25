//! Directory and revision pagination cursor behavior.

#![allow(clippy::panic)]
// Runtime integration tests use panic in helper assertions for precise diagnostics.

use crate::common::*;
use loonfs::{
    CreateDirectoryOptions, CreateNamespaceOptions, DeleteDirectoryBehavior, DeleteOptions,
    DestinationBehavior, ErrorCode, InodeId, MoveOptions, PageRequest, PathEntry, PutFileOptions,
};
use loonfs_test_support::block_on::block_on;
use loonfs_test_support::ids::{namespace_id, page_limit};
use tempfile::tempdir;

fn display_names(entries: &[PathEntry]) -> Vec<&str> {
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
fn collect_up_to_keeps_unused_entries_for_the_next_call() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "path-pager-collect-test");
    let namespace_id = namespace_id("demo");
    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    for path in ["/docs/a.txt", "/docs/b.txt", "/docs/c.txt"] {
        fs.put_file_bytes_blocking(
            &namespace_id,
            path,
            path.as_bytes(),
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .expect("put file");
    }

    let mut pager = fs.reader.list_path_entries_pager(
        &namespace_id,
        "/docs",
        PageRequest {
            limit: page_limit(2),
            cursor: None,
        },
        Default::default(),
    );
    let collected = block_on(pager.collect_up_to(1)).expect("collect one entry");
    assert_eq!(display_names(&collected), vec!["a.txt"]);

    let rest_of_first = block_on(pager.next())
        .expect("buffered page remainder")
        .expect("page succeeds");
    assert_eq!(display_names(&rest_of_first.entries), vec!["b.txt"]);
    let final_page = block_on(pager.next())
        .expect("final page")
        .expect("page succeeds");
    assert_eq!(display_names(&final_page.entries), vec!["c.txt"]);
    assert!(block_on(pager.next()).is_none());
}

#[test]
fn path_entries_pager_preserves_each_page_head() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "path-pager-drift-test");
    let namespace_id = namespace_id("demo");
    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    for path in ["/docs/a.txt", "/docs/b.txt", "/docs/c.txt"] {
        fs.put_file_bytes_blocking(
            &namespace_id,
            path,
            path.as_bytes(),
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .expect("put file");
    }

    let mut pager = fs.reader.list_path_entries_pager(
        &namespace_id,
        "/docs",
        PageRequest {
            limit: page_limit(2),
            cursor: None,
        },
        Default::default(),
    );
    let first = block_on(pager.next())
        .expect("first page")
        .expect("page succeeds");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/z.txt",
        b"newer",
        PutFileOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("put later file");
    let second = block_on(pager.next())
        .expect("second page")
        .expect("page succeeds");

    assert_ne!(first.head_seq, second.head_seq);
    assert_eq!(display_names(&second.entries), vec!["c.txt", "z.txt"]);
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
            PutFileOptions::new(loonfs_test_support::test_actor()),
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
        commit: loonfs_api::options::CommitOptions {
            actor: loonfs_test_support::test_actor(),
            commit_id: None,
            message: None,
        },
        expected_revision_no: None,
    };
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/doc.txt",
        b"v1",
        PutFileOptions::new(loonfs_test_support::test_actor()),
    )
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
}

#[test]
fn directory_cursor_resumes_after_later_writes() {
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
            PutFileOptions::new(loonfs_test_support::test_actor()),
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
        PutFileOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("put later file");

    // The cursor is an ordering resume: the next page continues after the
    // last returned name key against the advanced head, so the entry
    // committed mid-listing appears in its canonical position.
    let second = block_on(fs.list_path_entries_page(
        &namespace_id,
        "/docs",
        PageRequest {
            limit,
            cursor: Some(cursor),
        },
    ))
    .expect("second directory page resumes after head drift");
    assert_eq!(display_names(&second.entries), vec!["c.txt", "z.txt"]);
    assert!(second.next_cursor.is_none());
}

#[test]
fn directory_cursor_from_the_future_is_rejected() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "directory-page-future-test");
    let namespace_id = namespace_id("demo");
    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    for path in ["/docs/a.txt", "/docs/b.txt", "/docs/c.txt"] {
        fs.put_file_bytes_blocking(
            &namespace_id,
            path,
            path.as_bytes(),
            PutFileOptions::new(loonfs_test_support::test_actor()),
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
    let mut cursor =
        decode_directory_page_cursor(first.next_cursor.as_deref().expect("next cursor"));
    cursor.head_seq = loonfs::ChangeSeq(cursor.head_seq.0 + 1000);

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
fn directory_cursor_resumes_across_a_wal_flush() {
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
            PutFileOptions::new(loonfs_test_support::test_actor()),
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
        PutFileOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("put later file");
    fs.create_checkpoint_blocking(&namespace_id)
        .expect("checkpoint newer snapshot");

    // Materializing the newer state into the manifest does not retire the
    // cursor either: retained rows answer the resume at the current head.
    let second = block_on(fs.list_path_entries_page(
        &namespace_id,
        "/docs",
        PageRequest {
            limit: page_limit(2),
            cursor: Some(cursor),
        },
    ))
    .expect("second directory page resumes across a flush");
    assert_eq!(display_names(&second.entries), vec!["c.txt", "z.txt"]);
    assert!(second.next_cursor.is_none());
}

#[test]
fn revisions_cursor_resumes_after_later_writes() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "revisions-page-drift-test");
    let namespace_id = namespace_id("demo");
    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    for body in ["one", "two", "three"] {
        fs.put_file_bytes_blocking(
            &namespace_id,
            "/docs/report.txt",
            body.as_bytes(),
            PutFileOptions {
                behavior: DestinationBehavior::Replace,
                ..PutFileOptions::new(loonfs_test_support::test_actor())
            },
        )
        .expect("put revision");
    }

    let first = block_on(fs.list_file_revisions_page(
        &namespace_id,
        "/docs/report.txt",
        PageRequest {
            limit: page_limit(2),
            cursor: None,
        },
    ))
    .expect("first revisions page");
    assert_eq!(
        first
            .revisions
            .iter()
            .map(|revision| revision.revision_no.0)
            .collect::<Vec<_>>(),
        vec![3, 2]
    );
    let cursor =
        decode_file_revisions_page_cursor(first.next_cursor.as_deref().expect("next cursor"));

    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/report.txt",
        b"four",
        PutFileOptions {
            behavior: DestinationBehavior::Replace,
            ..PutFileOptions::new(loonfs_test_support::test_actor())
        },
    )
    .expect("put fourth revision");

    // The resume continues strictly after the last returned revision, so
    // the in-flight listing completes; the revision committed mid-listing
    // is newer than the whole listing and stays out of it.
    let second = block_on(fs.list_file_revisions_page(
        &namespace_id,
        "/docs/report.txt",
        PageRequest {
            limit: page_limit(2),
            cursor: Some(cursor),
        },
    ))
    .expect("second revisions page resumes after head drift");
    assert_eq!(
        second
            .revisions
            .iter()
            .map(|revision| revision.revision_no.0)
            .collect::<Vec<_>>(),
        vec![1]
    );
    assert!(second.next_cursor.is_none());
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
            PutFileOptions::new(loonfs_test_support::test_actor()),
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

#[test]
fn inode_children_pages_stay_on_the_renamed_directory() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "inode-children-rename-test");
    let namespace_id = namespace_id("demo");
    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    for path in ["/docs/a.txt", "/docs/b.txt", "/docs/c.txt"] {
        fs.put_file_bytes_blocking(
            &namespace_id,
            path,
            path.as_bytes(),
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .expect("put file");
    }
    let docs = fs
        .stat_path_blocking(&namespace_id, "/docs")
        .expect("stat listed directory");

    let mut pager = fs.reader.list_inode_children_pager(
        &namespace_id,
        docs.inode_id,
        PageRequest {
            limit: page_limit(2),
            cursor: None,
        },
        Default::default(),
    );
    let first = block_on(pager.next())
        .expect("first page exists")
        .expect("first page succeeds");
    assert_eq!(display_names(&first.entries), vec!["a.txt", "b.txt"]);
    assert_eq!(first.parent_inode_id, docs.inode_id);

    fs.move_path_blocking(
        &namespace_id,
        "/docs",
        "/renamed",
        MoveOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("rename the listed directory between pages");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/decoy.txt",
        b"decoy",
        PutFileOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("rebind the old path to a fresh directory");

    let second = block_on(pager.next())
        .expect("second page exists")
        .expect("second page resumes after the rename");
    assert_eq!(display_names(&second.entries), vec!["c.txt"]);
    assert_eq!(second.parent_inode_id, docs.inode_id);
    assert_eq!(second.entries[0].path.as_str(), "/renamed/c.txt");
    assert!(block_on(pager.next()).is_none());
}

#[test]
fn inode_children_rejects_files_and_missing_inodes() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "inode-children-target-test");
    let namespace_id = namespace_id("demo");
    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/a.txt",
        b"a",
        PutFileOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("put file");
    let file = fs
        .stat_path_blocking(&namespace_id, "/docs/a.txt")
        .expect("stat file");

    assert_core_error_kind(
        block_on(fs.list_inode_children_page(
            &namespace_id,
            file.inode_id,
            PageRequest {
                limit: page_limit(2),
                cursor: None,
            },
        )),
        ErrorCode::PathConflict,
    );
    assert_core_error_kind(
        block_on(fs.list_inode_children_page(
            &namespace_id,
            InodeId(4096),
            PageRequest {
                limit: page_limit(2),
                cursor: None,
            },
        )),
        ErrorCode::InodeNotFound,
    );
}

#[test]
fn inode_children_of_an_empty_directory_is_an_empty_page() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "inode-children-empty-test");
    let namespace_id = namespace_id("demo");
    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.create_directory_blocking(
        &namespace_id,
        "/empty",
        CreateDirectoryOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("create empty directory");
    let empty = fs
        .stat_path_blocking(&namespace_id, "/empty")
        .expect("stat empty directory");

    let page = block_on(fs.list_inode_children_page(
        &namespace_id,
        empty.inode_id,
        PageRequest {
            limit: page_limit(2),
            cursor: None,
        },
    ))
    .expect("list empty directory");
    assert_eq!(page.parent_inode_id, empty.inode_id);
    assert!(page.entries.is_empty());
    assert!(page.next_cursor.is_none());
}

#[test]
fn inode_children_rejects_a_recursively_deleted_directory() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "inode-children-deleted-test");
    let namespace_id = namespace_id("demo");
    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/child.txt",
        b"child",
        PutFileOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("put child");
    let docs = fs
        .stat_path_blocking(&namespace_id, "/docs")
        .expect("stat directory");
    fs.delete_path_blocking(
        &namespace_id,
        "/docs",
        DeleteOptions {
            behavior: DeleteDirectoryBehavior::Recursive,
            ..DeleteOptions::new(loonfs_test_support::test_actor())
        },
    )
    .expect("recursively delete directory");

    assert_core_error_kind(
        block_on(fs.list_inode_children_page(
            &namespace_id,
            docs.inode_id,
            PageRequest {
                limit: page_limit(2),
                cursor: None,
            },
        )),
        ErrorCode::InodeNotFound,
    );
}

#[test]
fn inode_children_cursor_rejects_a_different_directory() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "inode-children-cursor-mismatch-test");
    let namespace_id = namespace_id("demo");
    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    for path in [
        "/first/a.txt",
        "/first/b.txt",
        "/second/a.txt",
        "/second/b.txt",
    ] {
        fs.put_file_bytes_blocking(
            &namespace_id,
            path,
            path.as_bytes(),
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .expect("put file");
    }
    let first_directory = fs
        .stat_path_blocking(&namespace_id, "/first")
        .expect("stat first directory");
    let second_directory = fs
        .stat_path_blocking(&namespace_id, "/second")
        .expect("stat second directory");
    let first_page = block_on(fs.list_inode_children_page(
        &namespace_id,
        first_directory.inode_id,
        PageRequest {
            limit: page_limit(1),
            cursor: None,
        },
    ))
    .expect("first directory page");
    let cursor =
        decode_directory_page_cursor(first_page.next_cursor.as_deref().expect("next cursor"));

    assert_core_error_kind(
        block_on(fs.list_inode_children_page(
            &namespace_id,
            second_directory.inode_id,
            PageRequest {
                limit: page_limit(1),
                cursor: Some(cursor),
            },
        )),
        ErrorCode::InvalidRequest,
    );
}

#[test]
fn inode_children_cursor_from_the_future_is_rejected() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "inode-children-future-test");
    let namespace_id = namespace_id("demo");
    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    for path in ["/docs/a.txt", "/docs/b.txt", "/docs/c.txt"] {
        fs.put_file_bytes_blocking(
            &namespace_id,
            path,
            path.as_bytes(),
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .expect("put file");
    }
    let docs = fs
        .stat_path_blocking(&namespace_id, "/docs")
        .expect("stat directory");
    let first = block_on(fs.list_inode_children_page(
        &namespace_id,
        docs.inode_id,
        PageRequest {
            limit: page_limit(2),
            cursor: None,
        },
    ))
    .expect("first directory page");
    let mut cursor =
        decode_directory_page_cursor(first.next_cursor.as_deref().expect("next cursor"));
    cursor.head_seq = loonfs::ChangeSeq(cursor.head_seq.0 + 1000);

    assert_core_error_kind(
        block_on(fs.list_inode_children_page(
            &namespace_id,
            docs.inode_id,
            PageRequest {
                limit: page_limit(2),
                cursor: Some(cursor),
            },
        )),
        ErrorCode::RebootstrapRequired,
    );
}

#[test]
fn inode_children_of_the_root_list_by_the_root_inode() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "inode-children-root-test");
    let namespace_id = namespace_id("demo");
    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    for path in ["/a.txt", "/b.txt"] {
        fs.put_file_bytes_blocking(
            &namespace_id,
            path,
            path.as_bytes(),
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .expect("put file");
    }
    let root = fs
        .stat_path_blocking(&namespace_id, "/")
        .expect("stat root");

    let page = block_on(fs.list_inode_children_page(
        &namespace_id,
        root.inode_id,
        PageRequest {
            limit: page_limit(10),
            cursor: None,
        },
    ))
    .expect("list root children by inode");
    assert_eq!(display_names(&page.entries), vec!["a.txt", "b.txt"]);
    assert_eq!(page.parent_inode_id, root.inode_id);
    assert!(page.next_cursor.is_none());
}

#[test]
fn inode_children_empty_resumed_page_reports_the_drifted_head() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "inode-children-drift-head-test");
    let namespace_id = namespace_id("demo");
    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    for path in ["/docs/a.txt", "/docs/b.txt", "/docs/c.txt"] {
        fs.put_file_bytes_blocking(
            &namespace_id,
            path,
            path.as_bytes(),
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .expect("put file");
    }
    let docs = fs
        .stat_path_blocking(&namespace_id, "/docs")
        .expect("stat listed directory");
    let first = block_on(fs.list_inode_children_page(
        &namespace_id,
        docs.inode_id,
        PageRequest {
            limit: page_limit(2),
            cursor: None,
        },
    ))
    .expect("first page");
    let cursor = decode_directory_page_cursor(first.next_cursor.as_deref().expect("next cursor"));

    fs.delete_path_blocking(
        &namespace_id,
        "/docs/c.txt",
        DeleteOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("delete the remaining child");

    let resumed = block_on(fs.list_inode_children_page(
        &namespace_id,
        docs.inode_id,
        PageRequest {
            limit: page_limit(2),
            cursor: Some(cursor),
        },
    ))
    .expect("empty resumed page");
    assert!(resumed.entries.is_empty());
    assert!(resumed.next_cursor.is_none());
    assert!(resumed.head_seq > first.head_seq);
}
