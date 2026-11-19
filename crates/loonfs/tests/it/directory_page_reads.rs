//! Store reads and entry metadata for a directory page across folded batches.

use loonfs::test_support::writer;
use loonfs::{DestinationBehavior, LoonFs, PutFileOptions, SharedObjectStore, StatOptions};
use loonfs_objectstore::keys::metadata_segment_object_key;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::ids::{namespace_id, page_limit};
use loonfs_test_support::stores::RecordingStore;
use loonfs_types::format::manifest::MetadataRowFamily;
use std::collections::BTreeSet;
use std::sync::Arc;
use tempfile::tempdir;

async fn family_keys(
    store: &SharedObjectStore,
    namespace_id: &loonfs::NamespaceId,
    family: MetadataRowFamily,
) -> BTreeSet<String> {
    loonfs_core::test_support::namespace_store(store, namespace_id)
        .load_current_manifest()
        .await
        .expect("load manifest")
        .payload()
        .runs
        .iter()
        .flat_map(|run| &run.segments)
        .filter(|segment| segment.family == family)
        .map(metadata_segment_object_key)
        .collect()
}

/// Checks that a directory page over folded batches reads its children's
/// kinds and revision summaries from the directory's binding rows, so it
/// reads no child inode row and no revision row, and that each listed file
/// reports what a stat of it reports, apart from the content reference.
#[tokio::test]
async fn directory_pages_read_no_child_inode_or_revision_rows() {
    let temporary = tempdir().expect("temporary directory");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temporary.path()).expect("local store"));
    let namespace_id = namespace_id("directory-page-reads");
    let actor = loonfs_test_support::test_actor();
    let writer = writer(store.clone(), "directory-page-writer").await;
    let maintenance = writer.maintenance(loonfs_test_support::ids::writer_id(
        "directory-page-maintenance",
    ));
    writer
        .create_namespace(&namespace_id, &actor)
        .await
        .expect("namespace");
    let namespace_writer = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    namespace_writer
        .create_directory("/files", &actor)
        .await
        .expect("directory");
    maintenance
        .fold_wal(&namespace_id)
        .await
        .expect("fold parents");
    let parent_inode_keys = family_keys(&store, &namespace_id, MetadataRowFamily::Inodes).await;
    // The writer's own maintenance folds before the session's fold
    // threshold, so the session never folds, and so never compacts these
    // runs into fewer segments.
    for batch in 0..12 {
        for index in batch * 25..(batch + 1) * 25 {
            namespace_writer
                .put_file(
                    &format!("/files/{index:03}.txt"),
                    &vec![b'a'; index + 1],
                    &actor,
                )
                .await
                .expect("create file");
        }
        maintenance
            .fold_wal(&namespace_id)
            .await
            .expect("fold batch");
    }
    for (replaced, index) in (0..300).step_by(3).enumerate() {
        if replaced % 25 == 24 {
            maintenance
                .fold_wal(&namespace_id)
                .await
                .expect("fold replacements");
        }
        namespace_writer
            .put_file_with_options(
                &format!("/files/{index:03}.txt"),
                &vec![b'b'; index + 2],
                &loonfs_types::ActorId::parse("editor").expect("actor"),
                &PutFileOptions {
                    behavior: DestinationBehavior::Replace,
                    ..Default::default()
                },
            )
            .await
            .expect("replace file");
    }
    maintenance
        .fold_wal(&namespace_id)
        .await
        .expect("fold replacements");
    let child_inode_keys: BTreeSet<_> =
        family_keys(&store, &namespace_id, MetadataRowFamily::Inodes)
            .await
            .difference(&parent_inode_keys)
            .cloned()
            .collect();
    assert!(child_inode_keys.len() >= 6);
    let revision_keys = family_keys(&store, &namespace_id, MetadataRowFamily::Revisions).await;
    assert!(revision_keys.len() >= 6);
    // A child inode segment's key range can include the directory's own ID,
    // and a Bloom filter false positive can make the directory's own lookup
    // read that segment. A one-entry page of the directory reads the same
    // segments for the directory's lookup.
    let one_entry = Arc::new(RecordingStore::metadata_segments(store.clone()));
    LoonFs::builder_with_store(one_entry.clone(), loonfs::Context::Service)
        .read_only()
        .build()
        .await
        .expect("one-entry reader")
        .namespace(&namespace_id)
        .list("/files")
        .page_limit(page_limit(1))
        .page()
        .await
        .expect("one-entry page");
    let directory_reads: BTreeSet<String> = one_entry.take_get_keys().into_iter().collect();
    let recording = Arc::new(RecordingStore::metadata_segments(store));
    let reader = LoonFs::builder_with_store(recording.clone(), loonfs::Context::Service)
        .read_only()
        .build()
        .await
        .expect("fresh reader");
    let namespace = reader.namespace(&namespace_id);
    let page = namespace
        .list("/files")
        .page_limit(page_limit(300))
        .page()
        .await
        .expect("directory page");
    assert_eq!(page.entries.len(), 300);
    assert!(page.next_cursor.is_none());
    for key in recording.take_get_keys() {
        assert!(
            !child_inode_keys.contains(&key) || directory_reads.contains(&key),
            "read child inode object {key}"
        );
        assert!(!revision_keys.contains(&key), "read revision object {key}");
    }
    for entry in page.entries {
        let stat = namespace
            .stat_with_options(entry.path.as_str(), &StatOptions::default())
            .await
            .expect("stat listed file");
        assert_eq!(entry.created_by, actor);
        assert_eq!(entry.created_by, stat.created_by);
        assert_eq!(entry.created_at_ms, stat.created_at_ms);
        assert_eq!(entry.binding_version, stat.binding_version);
        assert!(
            stat.content_ref().is_some(),
            "a stat of a file reports its reference"
        );
        assert_eq!(entry.kind, stat.kind);
    }
}

/// The directories of a tree with `fanout` directories in each directory,
/// `depth` levels deep below `root`, in path order, and the paths of `files`
/// files spread over the deepest directories, where consecutive files go to
/// consecutive directories.
fn spread_tree(
    root: &str,
    files: usize,
    fanout: usize,
    depth: usize,
) -> (Vec<String>, Vec<String>) {
    let mut directories = BTreeSet::new();
    let mut paths = Vec::with_capacity(files);
    for index in 0..files {
        let mut directory = root.to_owned();
        for level in 0..depth {
            let stride = fanout.pow(u32::try_from(depth - 1 - level).expect("a small depth"));
            directory.push_str(&format!("/d{level}-{:02}", (index / stride) % fanout));
            directories.insert(directory.clone());
        }
        paths.push(format!("{directory}/f-{index:06}.js"));
    }
    (directories.into_iter().collect(), paths)
}

/// Checks that a cold page of a recursive listing sends at most one GET of
/// data blocks to each `direntry_binds` segment.
///
/// The tree has 4,368 directories (16 subdirectories in each directory,
/// three levels deep) and 5,000 files spread over the deepest ones, so a
/// page of 1,000 entries scans about 470 directories one after another. The tree is committed in
/// path order in batches of 500 operations, each folded, and then compacted,
/// so one `direntry_binds` segment is too large to fetch whole and the page
/// reads it in refills. If a refill stopped at the end of each directory's
/// rows, the scan of the next directory would fetch the next block alone, so
/// the page would send one GET for each block of that segment that it
/// reads.
#[tokio::test]
async fn a_cold_recursive_page_sends_at_most_one_data_get_to_each_segment() {
    use loonfs::publish::{CommitIntent, FilesystemOperation};
    use loonfs::{CommitId, ListOptions, MaintenanceCancellation, MetadataMaintenanceOutcome};
    use loonfs_types::{AbsolutePath, FileContent};

    let temporary = tempdir().expect("temporary directory");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temporary.path()).expect("local store"));
    let namespace_id = namespace_id("cold-recursive-page");
    let actor = loonfs_test_support::test_actor();
    let writer = writer(store.clone(), "cold-recursive-writer").await;
    let maintenance = writer.maintenance(loonfs_test_support::ids::writer_id(
        "cold-recursive-maintenance",
    ));
    writer
        .create_namespace(&namespace_id, &actor)
        .await
        .expect("namespace");
    let namespace_writer = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    let prepared = namespace_writer
        .prepare_content(b"x", &loonfs_test_support::test_actor())
        .await
        .expect("prepare file content");
    let (directories, files) = spread_tree("/deps", 5_000, 16, 3);
    let operations: Vec<_> = std::iter::once("/deps")
        .chain(directories.iter().map(String::as_str))
        .map(|path| FilesystemOperation::CreateDirectory {
            path: AbsolutePath::parse(path).expect("directory path"),
            parents: false,
        })
        .chain(files.iter().map(|path| FilesystemOperation::PutFile {
            path: AbsolutePath::parse(path).expect("file path"),
            content: FileContent::Ref(prepared.content_ref().clone()),
            behavior: DestinationBehavior::NoReplace,
            expected_inode_id: None,
            expected_revision_no: None,
        }))
        .collect();
    for (batch, operations) in operations.chunks(500).enumerate() {
        let writes_files = operations
            .iter()
            .any(|operation| matches!(operation, FilesystemOperation::PutFile { .. }));
        namespace_writer
            .commit_prepared(
                CommitIntent {
                    commit_id: CommitId::parse(format!("tree-{batch}")).expect("commit id"),
                    actor_id: actor.clone(),
                    subject: None,
                    message: None,
                    operations: operations.to_vec(),
                    preconditions: Vec::new(),
                },
                writes_files.then(|| prepared.clone()).into_iter().collect(),
            )
            .await
            .expect("commit tree batch");
        maintenance
            .fold_wal(&namespace_id)
            .await
            .expect("fold tree batch");
    }
    let report = maintenance
        .maintain_metadata_while_due(&namespace_id, &MaintenanceCancellation::new())
        .await
        .expect("compact");
    assert!(report.response.compaction_units > 0);
    assert_eq!(
        report.response.outcome,
        MetadataMaintenanceOutcome::CaughtUp
    );
    // Each `direntry_binds` segment's data blocks end where its filter
    // and index blocks start.
    let data_ends: std::collections::BTreeMap<String, u64> =
        loonfs_core::test_support::namespace_store(&store, &namespace_id)
            .load_current_manifest()
            .await
            .expect("load manifest")
            .payload()
            .runs
            .iter()
            .flat_map(|run| &run.segments)
            .filter(|segment| segment.family == MetadataRowFamily::DirentryBinds)
            .map(|segment| {
                (
                    metadata_segment_object_key(segment),
                    segment.index_block.offset.min(segment.filter_block.offset),
                )
            })
            .collect();
    assert!(
        data_ends.values().any(|end| *end > 256 * 1024),
        "a segment is too large to fetch whole: {data_ends:?}"
    );

    let recording = Arc::new(RecordingStore::metadata_segments(store));
    let reader = LoonFs::builder_with_store(recording.clone(), loonfs::Context::Service)
        .read_only()
        .build()
        .await
        .expect("fresh reader");
    let page = reader
        .namespace(&namespace_id)
        .list_with_options(
            "/deps",
            &ListOptions {
                recursive: true,
                ..ListOptions::default()
            },
        )
        .page_limit(page_limit(1_000))
        .page()
        .await
        .expect("recursive page");
    assert_eq!(page.entries.len(), 1_000);
    let gets = recording.take_gets();
    for (key, data_end) in &data_ends {
        let data_gets: Vec<_> = gets
            .iter()
            .filter(|(get_key, range)| {
                get_key == key && range.is_none_or(|(start, _)| start < *data_end)
            })
            .collect();
        assert!(
            data_gets.len() <= 1,
            "{} GETs of data blocks of {key}: {data_gets:?}",
            data_gets.len()
        );
    }
}
