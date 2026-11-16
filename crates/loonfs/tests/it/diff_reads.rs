//! Store reads of a diff over many commits.

use loonfs::publish::{CommitIntent, FilesystemOperation};
use loonfs::test_support::writer;
use loonfs::{
    CommitId, DestinationBehavior, DiffEntryKind, DiffOptions, LoonFs, MaintenanceCancellation,
    MetadataMaintenanceOutcome, SharedObjectStore,
};
use loonfs_objectstore::keys::metadata_segment_object_key;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::ids::namespace_id;
use loonfs_test_support::stores::RecordingStore;
use loonfs_types::format::manifest::MetadataRowFamily;
use loonfs_types::{AbsolutePath, FileContent};
use std::collections::BTreeMap;
use std::sync::Arc;
use tempfile::tempdir;

/// Checks that a cold diff reads a `revisions` segment in refills of many
/// blocks.
///
/// The namespace has 6,000 files, created in path order and then replaced
/// three times, 500 files to a commit. After compaction, the files' revision
/// rows fill one segment of about 128 blocks (380 KB stored), in the order
/// in which the commits touch the files. A diff from the files' creation
/// reads the newest revision of each file at both of its ends. Each lookup
/// that fetches a block reads ahead up to the scan's 2 MiB decoded budget,
/// about 32 of these blocks, so the diff sends about 5 GETs of the segment's
/// data blocks, and the test allows 6 for each `revisions` segment. If each
/// lookup fetched only the block with the file's rows, the diff would send
/// one GET for each block, 128 in all.
#[tokio::test]
async fn a_cold_diff_reads_a_revisions_segment_in_a_few_data_gets() {
    const FILES: usize = 6_000;
    let temporary = tempdir().expect("temporary directory");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temporary.path()).expect("local store"));
    let namespace_id = namespace_id("cold-diff-page");
    let actor = loonfs_test_support::test_actor();
    let writer = writer(store.clone(), "cold-diff-writer").await;
    let maintenance =
        writer.maintenance(loonfs_test_support::ids::writer_id("cold-diff-maintenance"));
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
    let paths: Vec<_> = (0..FILES)
        .map(|index| AbsolutePath::parse(format!("/files/f-{index:05}.txt")).expect("file path"))
        .collect();
    let mut from_seq = None;
    for round in 0..4 {
        let prepared = namespace_writer
            .prepare_content(format!("round {round}").as_bytes())
            .await
            .expect("prepare file content");
        for (batch, paths) in paths.chunks(500).enumerate() {
            let commit = namespace_writer
                .commit_prepared(
                    CommitIntent {
                        commit_id: CommitId::parse(format!("round-{round}-batch-{batch}"))
                            .expect("commit id"),
                        actor_id: actor.clone(),
                        subject: None,
                        message: None,
                        operations: paths
                            .iter()
                            .map(|path| FilesystemOperation::PutFile {
                                path: path.clone(),
                                content: FileContent::Ref(prepared.content_ref().clone()),
                                behavior: DestinationBehavior::Replace,
                                expected_inode_id: None,
                                expected_revision_no: None,
                            })
                            .collect(),
                        preconditions: Vec::new(),
                    },
                    vec![prepared.clone()],
                )
                .await
                .expect("commit batch");
            if round == 0 {
                from_seq = Some(commit.committed_seq);
            }
            maintenance
                .fold_wal(&namespace_id)
                .await
                .expect("fold batch");
        }
    }
    for pass in 0.. {
        assert!(pass < 16, "compaction catches up");
        let report = maintenance
            .maintain_metadata_while_due(&namespace_id, &MaintenanceCancellation::new())
            .await
            .expect("compact");
        if report.response.outcome == MetadataMaintenanceOutcome::CaughtUp {
            break;
        }
    }
    // Each `revisions` segment's data blocks end where its filter and index
    // blocks start.
    let data_ends: BTreeMap<String, u64> =
        loonfs_core::test_support::namespace_store(&store, &namespace_id)
            .load_current_manifest()
            .await
            .expect("load manifest")
            .payload()
            .runs
            .iter()
            .flat_map(|run| &run.segments)
            .filter(|segment| segment.family == MetadataRowFamily::Revisions)
            .map(|segment| {
                (
                    metadata_segment_object_key(segment),
                    segment.index_block.offset.min(segment.filter_block.offset),
                )
            })
            .collect();
    // A segment of at most 256 KiB is fetched whole on its first read.
    assert!(
        data_ends.values().all(|end| *end > 256 * 1024),
        "each segment is too large to fetch whole: {data_ends:?}"
    );

    let recording = Arc::new(RecordingStore::metadata_segments(store));
    let reader = LoonFs::builder_with_store(recording.clone())
        .read_only()
        .build()
        .await
        .expect("fresh reader");
    // The diff starts after the files' creation, so every file is changed,
    // and the pages read the newest revision of each file at both ends.
    let namespace = reader.namespace(&namespace_id);
    let mut options = DiffOptions {
        path: AbsolutePath::parse("/files").expect("path"),
        ..DiffOptions::default()
    };
    let mut entries = Vec::new();
    loop {
        let page = namespace
            .diff_with_options(from_seq.expect("a commit"), &options)
            .await
            .expect("diff page");
        entries.extend(page.entries);
        options.to_seq = Some(page.to_seq);
        options.after_seq = page.next_after_seq;
        if options.after_seq.is_none() {
            break;
        }
    }
    assert_eq!(entries.len(), FILES);
    assert!(entries
        .iter()
        .all(|entry| entry.kind == DiffEntryKind::Changed));
    let gets = recording.take_gets();
    for (key, data_end) in &data_ends {
        let data_gets: Vec<_> = gets
            .iter()
            .filter(|(get_key, range)| {
                get_key == key && range.is_none_or(|(start, _)| start < *data_end)
            })
            .collect();
        assert!(
            data_gets.len() <= 6,
            "{} GETs of data blocks of {key}: {data_gets:?}",
            data_gets.len()
        );
    }
}
