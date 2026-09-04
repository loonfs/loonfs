//! Whole-namespace reads for a consumer that derives its own data from the
//! filesystem: enumerating the files a checkpoint pinned, asking where those
//! files are now, and reading one content object by reference.

#![allow(clippy::panic)]
// Runtime integration tests use panic in helper assertions for precise diagnostics.

use crate::common::*;
use loonfs::{
    CheckpointFile, CheckpointFilesPageCursor, ContentRef, CreateCheckpointOptions,
    CreateNamespaceOptions, CurrentFileState, DeleteDirectoryBehavior, DeleteOptions,
    DestinationBehavior, ErrorCode, FsReader, InodeId, MoveOptions, NamespaceId, PageRequest,
    PutFileOptions, RevisionNo, RuntimeError, SharedObjectStore, StoreConfig, UndeleteOptions,
};
use loonfs_test_support::ids::{namespace_id, page_limit};
use loonfs_test_support::stores::{KeyPredicate, OperationClass, RecordingStore};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;
use tempfile::tempdir;

fn store_config(root: &Path) -> StoreConfig {
    StoreConfig::LocalFs {
        root: root.to_string_lossy().into_owned(),
        key_prefix: None,
    }
}

/// One file as the ordinary read surface sees it, for comparing an
/// enumeration against a recursive listing taken at the same moment.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ListedFile {
    absolute_path: String,
    revision_no: RevisionNo,
    size_bytes: u64,
    content_ref: ContentRef,
}

/// Every visible file in the namespace right now, by inode, found by walking
/// directories through the ordinary listing surface.
async fn listed_files(
    reader: &FsReader,
    namespace_id: &NamespaceId,
) -> BTreeMap<InodeId, ListedFile> {
    let mut found = BTreeMap::new();
    let mut directories = vec!["/".to_owned()];
    while let Some(directory) = directories.pop() {
        let entries = collect_path_entries(reader, namespace_id, &directory)
            .await
            .expect("list directory")
            .entries;
        for entry in entries {
            match entry.kind {
                loonfs::PathEntryKind::Directory {} => {
                    directories.push(entry.path.as_str().to_owned());
                }
                loonfs::PathEntryKind::File {
                    revision_no,
                    size_bytes,
                    content_ref,
                    ..
                } => {
                    found.insert(
                        entry.inode_id,
                        ListedFile {
                            absolute_path: entry.path.as_str().to_owned(),
                            revision_no,
                            size_bytes,
                            content_ref,
                        },
                    );
                }
            }
        }
    }
    found
}

/// Every file a checkpoint pins, read one page at a time so the paging path
/// carries every assertion.
async fn checkpoint_files(
    reader: &FsReader,
    namespace_id: &NamespaceId,
    checkpoint_id: &loonfs::CheckpointId,
    limit: usize,
) -> Vec<CheckpointFile> {
    let mut files = Vec::new();
    let mut cursor = None;
    let mut seen: BTreeSet<InodeId> = BTreeSet::new();
    loop {
        let page = reader
            .list_checkpoint_files_page(
                namespace_id,
                checkpoint_id,
                PageRequest {
                    limit: page_limit(limit),
                    cursor,
                },
            )
            .await
            .expect("read a checkpoint files page");
        assert!(
            page.files.len() <= limit,
            "a page returned {} files over the {limit} limit",
            page.files.len()
        );
        for file in &page.files {
            assert!(
                seen.insert(file.inode_id),
                "inode `{}` was returned twice across pages",
                file.inode_id
            );
        }
        let page_inodes: Vec<InodeId> = page.files.iter().map(|file| file.inode_id).collect();
        let mut ascending = page_inodes.clone();
        ascending.sort_unstable();
        assert_eq!(page_inodes, ascending, "a page was not in inode order");
        if let (Some(next), Some(last)) = (page.next_cursor, page.files.last()) {
            assert_eq!(
                next.after_inode_id, last.inode_id,
                "a cursor must resume after the last file it handed back"
            );
        }
        files.extend(page.files);
        cursor = page.next_cursor;
        if cursor.is_none() {
            return files;
        }
    }
}

/// A namespace with the shapes an enumeration has to get right: nested
/// directories, a replaced file, a deleted subtree, an undeleted file, and a
/// directory holding no files at all.
async fn build_mixed_namespace(fs: &TestRuntime, namespace_id: &NamespaceId) {
    fs.create_namespace(namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    for (path, bytes) in [
        ("/docs/alpha.txt", &b"alpha"[..]),
        ("/docs/deep/bravo.txt", &b"bravo"[..]),
        ("/docs/deep/charlie.txt", &b"charlie"[..]),
        ("/scratch/discarded.txt", &b"discarded"[..]),
        ("/notes/recovered.txt", &b"recovered"[..]),
    ] {
        fs.put_file_bytes(
            namespace_id,
            path,
            bytes,
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("put file");
    }
    fs.writer
        .create_directory(
            namespace_id,
            "/empty",
            loonfs::CreateDirectoryOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("create a directory holding no files");

    // A replaced file: the enumeration must report the newer revision.
    fs.put_file_bytes(
        namespace_id,
        "/docs/deep/charlie.txt",
        b"charlie again",
        PutFileOptions {
            behavior: DestinationBehavior::Replace,
            ..PutFileOptions::new(loonfs_test_support::test_actor())
        },
    )
    .await
    .expect("replace file");

    // A deleted subtree: neither the directory nor the file below it is
    // visible any more.
    fs.writer
        .delete_path(
            namespace_id,
            "/scratch",
            DeleteOptions {
                behavior: DeleteDirectoryBehavior::Recursive,
                ..DeleteOptions::new(loonfs_test_support::test_actor())
            },
        )
        .await
        .expect("delete subtree");

    // An undeleted file: visible again, at a new path.
    let recovered_inode_id = fs
        .get_path_entry(namespace_id, "/notes/recovered.txt")
        .await
        .expect("stat before delete")
        .inode_id;
    let deletion_seq = fs
        .writer
        .delete_path(
            namespace_id,
            "/notes/recovered.txt",
            DeleteOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("delete file")
        .committed_seq;
    fs.writer
        .undelete(
            namespace_id,
            recovered_inode_id,
            deletion_seq,
            Some("/notes/restored.txt"),
            UndeleteOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("undelete file");
}

#[tokio::test]
async fn checkpoint_enumeration_answers_the_state_it_pinned() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = open_runtime_async(store(temp_dir.path()), "checkpoint-files-test").await;
    let namespace_id = namespace_id("demo");
    build_mixed_namespace(&fs, &namespace_id).await;

    // Ground truth, collected through the ordinary read surface BEFORE the
    // later writes land.
    let at_checkpoint = listed_files(&fs.reader, &namespace_id).await;
    let checkpoint = fs
        .create_checkpoint(&namespace_id)
        .await
        .expect("create checkpoint");

    // Everything after the checkpoint is the change feed's job, so none of
    // it may show up in the enumeration.
    fs.put_file_bytes(
        &namespace_id,
        "/docs/added-later.txt",
        b"added later",
        PutFileOptions::new(loonfs_test_support::test_actor()),
    )
    .await
    .expect("create a file after the checkpoint");
    fs.put_file_bytes(
        &namespace_id,
        "/docs/alpha.txt",
        b"alpha again",
        PutFileOptions {
            behavior: DestinationBehavior::Replace,
            ..PutFileOptions::new(loonfs_test_support::test_actor())
        },
    )
    .await
    .expect("replace a file after the checkpoint");
    fs.writer
        .delete_path(
            &namespace_id,
            "/docs/deep/bravo.txt",
            DeleteOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("delete a file after the checkpoint");
    fs.writer
        .move_path(
            &namespace_id,
            "/docs/deep/charlie.txt",
            "/docs/charlie.txt",
            MoveOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("move a file after the checkpoint");

    let enumerated =
        checkpoint_files(&fs.reader, &namespace_id, &checkpoint.checkpoint_id, 2).await;
    let by_inode: BTreeMap<InodeId, CheckpointFile> = enumerated
        .iter()
        .map(|file| (file.inode_id, file.clone()))
        .collect();

    assert_eq!(
        by_inode.keys().copied().collect::<Vec<_>>(),
        at_checkpoint.keys().copied().collect::<Vec<_>>(),
        "the enumeration must name exactly the files visible when the checkpoint was taken"
    );
    for (inode_id, listed) in &at_checkpoint {
        let file = &by_inode[inode_id];
        assert_eq!(
            file.revision_no, listed.revision_no,
            "wrong revision for `{}`",
            listed.absolute_path
        );
        assert_eq!(file.content_ref, listed.content_ref);
        assert_eq!(file.size_bytes, listed.size_bytes);
        assert_eq!(file.size_bytes, file.content_ref.size_bytes);
    }

    let checkpoint_paths: BTreeSet<&str> = at_checkpoint
        .values()
        .map(|listed| listed.absolute_path.as_str())
        .collect();
    assert_eq!(
        checkpoint_paths,
        BTreeSet::from([
            "/docs/alpha.txt",
            "/docs/deep/bravo.txt",
            "/docs/deep/charlie.txt",
            "/notes/restored.txt",
        ]),
        "the pinned state should hold the undeleted file and neither the \
         deleted subtree nor anything written later"
    );

    // The live namespace has moved on; the checkpoint has not.
    let now = listed_files(&fs.reader, &namespace_id).await;
    assert!(
        now.values()
            .any(|listed| listed.absolute_path == "/docs/added-later.txt"),
        "the later write should be visible at head"
    );
    assert!(
        !enumerated.iter().any(|file| {
            now.get(&file.inode_id)
                .is_some_and(|listed| listed.absolute_path == "/docs/added-later.txt")
        }),
        "a file created after the checkpoint must not appear in the enumeration"
    );
    let alpha_inode_id = at_checkpoint
        .iter()
        .find(|(_, listed)| listed.absolute_path == "/docs/alpha.txt")
        .map(|(inode_id, _)| *inode_id)
        .expect("alpha was visible at checkpoint time");
    assert_eq!(
        by_inode[&alpha_inode_id].revision_no,
        RevisionNo(1),
        "the replacement landed after the checkpoint, so the pinned revision stays"
    );
    assert_eq!(
        now[&alpha_inode_id].revision_no,
        RevisionNo(2),
        "the live namespace should carry the replacement"
    );
}

#[tokio::test]
async fn checkpoint_files_page_without_gaps_or_duplicates() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = open_runtime_async(store(temp_dir.path()), "checkpoint-files-paging-test").await;
    let namespace_id = namespace_id("demo");
    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    for index in 0..5 {
        fs.put_file_bytes(
            &namespace_id,
            &format!("/docs/file-{index}.txt"),
            format!("body {index}").as_bytes(),
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("put file");
    }
    let checkpoint = fs
        .create_checkpoint(&namespace_id)
        .await
        .expect("create checkpoint");

    let whole = checkpoint_files(&fs.reader, &namespace_id, &checkpoint.checkpoint_id, 100).await;
    assert_eq!(whole.len(), 5);
    let one_at_a_time =
        checkpoint_files(&fs.reader, &namespace_id, &checkpoint.checkpoint_id, 1).await;
    assert_eq!(
        one_at_a_time, whole,
        "paging one file at a time must produce the same sequence as one page"
    );

    // Resuming from every cursor position lands exactly on the remaining
    // tail: no row is re-read, and none is skipped.
    for (index, file) in whole.iter().enumerate() {
        let resumed = fs
            .reader
            .list_checkpoint_files_page(
                &namespace_id,
                &checkpoint.checkpoint_id,
                PageRequest {
                    limit: page_limit(100),
                    cursor: Some(CheckpointFilesPageCursor {
                        after_inode_id: file.inode_id,
                    }),
                },
            )
            .await
            .expect("resume from a cursor");
        assert_eq!(resumed.files, whole[index + 1..]);
        assert!(resumed.next_cursor.is_none());
        assert_eq!(resumed.checkpoint_seq, checkpoint.checkpoint_seq);
    }
}

#[tokio::test]
async fn an_empty_namespace_answers_one_empty_page() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = open_runtime_async(store(temp_dir.path()), "checkpoint-files-empty-test").await;
    let namespace_id = namespace_id("demo");
    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    let checkpoint = fs
        .create_checkpoint(&namespace_id)
        .await
        .expect("create checkpoint");

    let page = fs
        .reader
        .list_checkpoint_files_page(
            &namespace_id,
            &checkpoint.checkpoint_id,
            PageRequest {
                limit: page_limit(10),
                cursor: None,
            },
        )
        .await
        .expect("enumerate an empty namespace");
    assert!(page.files.is_empty());
    assert!(page.next_cursor.is_none());
    assert_eq!(page.checkpoint_seq, checkpoint.checkpoint_seq);
}

#[tokio::test]
async fn a_fork_targets_checkpoint_enumerates_the_source_state() {
    let temp_dir = tempdir().expect("tempdir");
    let store = store(temp_dir.path());
    let fs = open_runtime_async(store.clone(), "checkpoint-files-fork-test").await;
    let source = namespace_id("source");
    let target = namespace_id("target");
    fs.create_namespace(&source, CreateNamespaceOptions::default())
        .await
        .expect("create source namespace");
    for (path, bytes) in [
        ("/docs/alpha.txt", &b"alpha"[..]),
        ("/docs/deep/bravo.txt", &b"bravo"[..]),
    ] {
        fs.put_file_bytes(
            &source,
            path,
            bytes,
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("put file");
    }
    let at_fork = listed_files(&fs.reader, &source).await;

    fs.writer
        .fork_namespace(&source, &target)
        .await
        .expect("fork namespace");

    // The target has published nothing of its own, so its checkpoint's
    // manifest names metadata files the source owns.
    let checkpoint = fs
        .maintenance
        .create_checkpoint(
            &target,
            CreateCheckpointOptions {
                name: "fork-pin".to_owned(),
                ttl_ms: None,
            },
        )
        .await
        .expect("checkpoint the unflushed fork target");
    assert!(
        foreign_metadata_segment_owners(&store, &target)
            .await
            .contains(&source),
        "the fork target's basis manifest should still name source-owned metadata files"
    );

    let enumerated = checkpoint_files(&fs.reader, &target, &checkpoint.checkpoint_id, 1).await;
    assert_eq!(
        enumerated
            .iter()
            .map(|file| (file.inode_id, file.revision_no, file.content_ref.clone()))
            .collect::<Vec<_>>(),
        at_fork
            .iter()
            .map(|(inode_id, listed)| (*inode_id, listed.revision_no, listed.content_ref.clone()))
            .collect::<Vec<_>>(),
        "a fork target's checkpoint should enumerate the source state at the fork point"
    );
}

/// Which namespaces own the metadata files a namespace's current manifest
/// names.
async fn foreign_metadata_segment_owners(
    store: &SharedObjectStore,
    namespace_id: &NamespaceId,
) -> BTreeSet<NamespaceId> {
    let root =
        loonfs_core::control::load_namespace_metadata_root_control(store.as_ref(), namespace_id)
            .await
            .expect("load metadata root")
            .state;
    let key = loonfs_objectstore::keys::metadata_manifest_object(
        namespace_id,
        &root.manifest.manifest_object_id,
    );
    let bytes = store
        .get(&key, None)
        .await
        .expect("read manifest")
        .expect("manifest object exists");
    loonfs_api::wire::manifest::decode_namespace_manifest_json(&bytes)
        .expect("decode manifest")
        .payload
        .runs
        .into_iter()
        .flat_map(|run| run.segments)
        .map(|descriptor| descriptor.owner_namespace_id)
        .collect()
}

#[tokio::test]
async fn a_released_checkpoint_refuses_enumeration_instead_of_answering_current_state() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = open_runtime_async(store(temp_dir.path()), "checkpoint-files-release-test").await;
    let namespace_id = namespace_id("demo");
    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    fs.put_file_bytes(
        &namespace_id,
        "/docs/alpha.txt",
        b"alpha",
        PutFileOptions::new(loonfs_test_support::test_actor()),
    )
    .await
    .expect("put file");
    let checkpoint = fs
        .create_checkpoint(&namespace_id)
        .await
        .expect("create checkpoint");
    fs.maintenance
        .release_checkpoint(&namespace_id, &checkpoint.checkpoint_id)
        .await
        .expect("release checkpoint");

    let error = fs
        .reader
        .list_checkpoint_files_page(
            &namespace_id,
            &checkpoint.checkpoint_id,
            PageRequest {
                limit: page_limit(10),
                cursor: None,
            },
        )
        .await
        .expect_err("a released checkpoint pins nothing to enumerate");
    assert_eq!(error.code(), ErrorCode::CheckpointUnavailable);

    let missing = loonfs::CheckpointId::parse("chk_0123456789abcdef0123456789abcdef")
        .expect("valid checkpoint id");
    let error = fs
        .reader
        .list_checkpoint_files_page(
            &namespace_id,
            &missing,
            PageRequest {
                limit: page_limit(10),
                cursor: None,
            },
        )
        .await
        .expect_err("a checkpoint that never existed pins nothing either");
    assert_eq!(error.code(), ErrorCode::CheckpointUnavailable);
}

#[tokio::test]
async fn resolve_current_files_answers_the_whole_matrix_in_input_order() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = open_runtime_async(store(temp_dir.path()), "resolve-current-files-test").await;
    let namespace_id = namespace_id("demo");
    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    for (path, bytes) in [
        ("/m/unchanged.txt", &b"unchanged"[..]),
        ("/m/replaced.txt", &b"first"[..]),
        ("/m/moved.txt", &b"moved"[..]),
        ("/m/carried/inside.txt", &b"inside"[..]),
        ("/m/deleted.txt", &b"deleted"[..]),
        ("/m/subtree/child.txt", &b"child"[..]),
        ("/m/recovered.txt", &b"recovered"[..]),
    ] {
        fs.put_file_bytes(
            &namespace_id,
            path,
            bytes,
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("put file");
    }
    let inode_of = |path: &'static str| {
        let fs = &fs;
        let namespace_id = &namespace_id;
        async move {
            fs.get_path_entry(namespace_id, path)
                .await
                .expect("stat path")
                .inode_id
        }
    };
    let unchanged = inode_of("/m/unchanged.txt").await;
    let replaced = inode_of("/m/replaced.txt").await;
    let moved = inode_of("/m/moved.txt").await;
    let carried = inode_of("/m/carried/inside.txt").await;
    let deleted = inode_of("/m/deleted.txt").await;
    let in_deleted_subtree = inode_of("/m/subtree/child.txt").await;
    let recovered = inode_of("/m/recovered.txt").await;
    let directory = inode_of("/m").await;

    fs.put_file_bytes(
        &namespace_id,
        "/m/replaced.txt",
        b"second",
        PutFileOptions {
            behavior: DestinationBehavior::Replace,
            ..PutFileOptions::new(loonfs_test_support::test_actor())
        },
    )
    .await
    .expect("replace file");
    fs.writer
        .move_path(
            &namespace_id,
            "/m/moved.txt",
            "/m/moved-away.txt",
            MoveOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("move file");
    fs.writer
        .move_path(
            &namespace_id,
            "/m/carried",
            "/m/carried-elsewhere",
            MoveOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("move directory");
    fs.writer
        .delete_path(
            &namespace_id,
            "/m/deleted.txt",
            DeleteOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("delete file");
    fs.writer
        .delete_path(
            &namespace_id,
            "/m/subtree",
            DeleteOptions {
                behavior: DeleteDirectoryBehavior::Recursive,
                ..DeleteOptions::new(loonfs_test_support::test_actor())
            },
        )
        .await
        .expect("delete subtree");
    let recovered_deletion_seq = fs
        .writer
        .delete_path(
            &namespace_id,
            "/m/recovered.txt",
            DeleteOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("delete file")
        .committed_seq;
    fs.writer
        .undelete(
            &namespace_id,
            recovered,
            recovered_deletion_seq,
            Some("/m/recovered-again.txt"),
            UndeleteOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("undelete file");

    let unknown = InodeId(999_999);
    let requested = vec![
        unchanged,
        replaced,
        moved,
        carried,
        deleted,
        in_deleted_subtree,
        recovered,
        unknown,
        directory,
    ];
    let states = fs
        .reader
        .resolve_current_files(&namespace_id, &requested)
        .await
        .expect("resolve current files");

    assert_eq!(
        states
            .iter()
            .map(|state| state.inode_id)
            .collect::<Vec<_>>(),
        requested,
        "answers must come back in the order they were asked for"
    );
    assert_eq!(
        states,
        vec![
            visible_file(unchanged, RevisionNo(1), "/m/unchanged.txt"),
            visible_file(replaced, RevisionNo(2), "/m/replaced.txt"),
            visible_file(moved, RevisionNo(1), "/m/moved-away.txt"),
            visible_file(carried, RevisionNo(1), "/m/carried-elsewhere/inside.txt"),
            gone(deleted),
            gone(in_deleted_subtree),
            visible_file(recovered, RevisionNo(1), "/m/recovered-again.txt"),
            gone(unknown),
            CurrentFileState {
                inode_id: directory,
                visible: true,
                current_revision_no: None,
                current_path: Some(
                    loonfs_api::AbsolutePath::parse("/m").expect("valid absolute path")
                ),
            },
        ]
    );
}

fn visible_file(inode_id: InodeId, revision_no: RevisionNo, path: &str) -> CurrentFileState {
    CurrentFileState {
        inode_id,
        visible: true,
        current_revision_no: Some(revision_no),
        current_path: Some(loonfs_api::AbsolutePath::parse(path).expect("valid absolute path")),
    }
}

fn gone(inode_id: InodeId) -> CurrentFileState {
    CurrentFileState {
        inode_id,
        visible: false,
        current_revision_no: None,
        current_path: None,
    }
}

#[tokio::test]
async fn resolve_current_files_refuses_a_batch_over_the_cap() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = open_runtime_async(store(temp_dir.path()), "resolve-current-files-cap-test").await;
    let namespace_id = namespace_id("demo");
    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    fs.put_file_bytes(
        &namespace_id,
        "/docs/alpha.txt",
        b"alpha",
        PutFileOptions::new(loonfs_test_support::test_actor()),
    )
    .await
    .expect("put file");
    let alpha = fs
        .get_path_entry(&namespace_id, "/docs/alpha.txt")
        .await
        .expect("stat file")
        .inode_id;

    let at_cap = vec![alpha; loonfs::MAX_RESOLVE_CURRENT_FILES];
    let states = fs
        .reader
        .resolve_current_files(&namespace_id, &at_cap)
        .await
        .expect("a batch at the cap is answered");
    assert_eq!(states.len(), loonfs::MAX_RESOLVE_CURRENT_FILES);
    assert!(states.iter().all(|state| state.visible));

    let mut over_cap = at_cap;
    over_cap.push(alpha);
    let error = fs
        .reader
        .resolve_current_files(&namespace_id, &over_cap)
        .await
        .expect_err("one past the cap is refused");
    assert_eq!(error.code(), ErrorCode::InvalidRequest);
    assert!(
        error
            .to_string()
            .contains(&loonfs::MAX_RESOLVE_CURRENT_FILES.to_string()),
        "the refusal should name the cap: {error}"
    );
}

#[tokio::test]
async fn read_content_ref_answers_bytes_and_refuses_over_budget_before_fetching() {
    let temp_dir = tempdir().expect("tempdir");
    let counting = Arc::new(RecordingStore::new(
        loonfs_objectstore::local_fs_store::LocalFsStore::new(temp_dir.path())
            .expect("create local-fs store"),
        KeyPredicate::content_blob(),
    ));
    let fs = open_runtime_async(counting.clone(), "read-content-ref-test").await;
    let namespace_id = namespace_id("demo");
    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    fs.put_file_bytes(
        &namespace_id,
        "/docs/alpha.txt",
        b"alpha bytes",
        PutFileOptions::new(loonfs_test_support::test_actor()),
    )
    .await
    .expect("put file");
    let content_ref = fs
        .get_path_entry(&namespace_id, "/docs/alpha.txt")
        .await
        .expect("stat file")
        .content_ref()
        .cloned()
        .expect("a file carries a content ref");

    counting.reset();
    let bytes = fs
        .reader
        .read_content_ref(&namespace_id, &content_ref, content_ref.size_bytes)
        .await
        .expect("read content by reference");
    assert_eq!(bytes, b"alpha bytes");
    assert_eq!(
        counting.count(OperationClass::Read),
        1,
        "one reference read is one content-object read"
    );

    counting.reset();
    let error = fs
        .reader
        .read_content_ref(&namespace_id, &content_ref, content_ref.size_bytes - 1)
        .await
        .expect_err("a reference larger than the budget is refused");
    assert_eq!(error.code(), ErrorCode::ContentTooLarge);
    assert_eq!(
        counting.count(OperationClass::Read),
        0,
        "the budget is checked against the reference, before any fetch"
    );
}

#[tokio::test]
async fn read_content_ref_refuses_bytes_that_do_not_match_the_reference() {
    let temp_dir = tempdir().expect("tempdir");
    let store = store(temp_dir.path());
    let fs = open_runtime_async(store.clone(), "read-content-ref-digest-test").await;
    let namespace_id = namespace_id("demo");
    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    fs.put_file_bytes(
        &namespace_id,
        "/docs/alpha.txt",
        b"alpha bytes",
        PutFileOptions::new(loonfs_test_support::test_actor()),
    )
    .await
    .expect("put file");
    let content_ref = fs
        .get_path_entry(&namespace_id, "/docs/alpha.txt")
        .await
        .expect("stat file")
        .content_ref()
        .cloned()
        .expect("a file carries a content ref");

    // Same length, different bytes: the size check passes and the digest
    // check is what has to catch it.
    let content_store_id =
        loonfs_core::control::load_namespace_catalog_entry(store.as_ref(), &namespace_id)
            .await
            .expect("load namespace catalog")
            .content_store_id()
            .clone();
    let object_key =
        loonfs_objectstore::keys::content_blob(&content_store_id, &content_ref.content_id);
    store
        .put_overwrite(&object_key, bytes::Bytes::from_static(b"other bytes"))
        .await
        .expect("corrupt the stored content object");

    let error = fs
        .reader
        .read_content_ref(&namespace_id, &content_ref, content_ref.size_bytes)
        .await
        .expect_err("bytes that do not hash to the reference are refused");
    assert_eq!(error.code(), ErrorCode::NamespaceCorrupt);
    assert!(
        matches!(&error, RuntimeError::Core(error) if error.to_string().contains("checksum mismatch")),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn a_standalone_reader_serves_every_operation() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = open_runtime_async(store(temp_dir.path()), "standalone-reader-test").await;
    let namespace_id = namespace_id("demo");
    build_mixed_namespace(&fs, &namespace_id).await;
    let checkpoint = fs
        .create_checkpoint(&namespace_id)
        .await
        .expect("create checkpoint");
    fs.writer
        .move_path(
            &namespace_id,
            "/docs/alpha.txt",
            "/docs/alpha-moved.txt",
            MoveOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("move a file after the checkpoint");

    // No writer identity anywhere on this path: the reader opens its own
    // store client from configuration.
    let reader = FsReader::builder(store_config(temp_dir.path()))
        .build()
        .await
        .expect("build a standalone reader");

    let files = checkpoint_files(&reader, &namespace_id, &checkpoint.checkpoint_id, 2).await;
    assert_eq!(files.len(), 4);

    let states = reader
        .resolve_current_files(
            &namespace_id,
            &files.iter().map(|file| file.inode_id).collect::<Vec<_>>(),
        )
        .await
        .expect("resolve current files through a standalone reader");
    assert!(states.iter().all(|state| state.visible));
    assert!(
        states.iter().any(|state| state
            .current_path
            .as_ref()
            .is_some_and(|path| path.as_str() == "/docs/alpha-moved.txt")),
        "the standalone reader should see the move that landed after the checkpoint"
    );

    for file in &files {
        let bytes = reader
            .read_content_ref(&namespace_id, &file.content_ref, file.size_bytes)
            .await
            .expect("read content through a standalone reader");
        assert_eq!(bytes.len() as u64, file.size_bytes);
    }
}
