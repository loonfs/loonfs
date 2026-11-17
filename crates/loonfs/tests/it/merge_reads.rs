//! Store reads of a merge of many commits.

use loonfs::publish::{CommitIntent, FilesystemOperation};
use loonfs::test_support::writer;
use loonfs::{
    CommitId, MaintenanceCancellation, MergeForkOptions, MetadataMaintenanceOutcome,
    SharedObjectStore,
};
use loonfs_objectstore::keys::metadata_segment_object_key;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::ids::namespace_id;
use loonfs_test_support::stores::RecordingStore;
use loonfs_types::format::manifest::MetadataRowFamily;
use loonfs_types::AbsolutePath;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use tempfile::tempdir;

/// Checks that a cold merge reads a fork's `inodes` and
/// `direntry_child_binds` segments in refills of many blocks.
///
/// The fork creates 20,000 directories in path order, 500 directories in
/// each commit. After compaction, most of the directories' inode rows fill one
/// segment of about 48 blocks (460 KB stored), and most of their child
/// binding rows fill one segment of about 128 blocks (740 KB stored), in the
/// order of the directories' inode IDs. A merge of the fork reads the inode
/// row, the parent binding, and the subtree tombstone of each directory, in
/// inode ID order. Each lookup that fetches a block reads ahead up to the
/// scan's 2 MiB decoded budget, about 32 of these blocks, so the merge sends
/// about 2 GETs of the inode segment's data blocks and about 5 of the binding
/// segment's, and the test allows 6 for each segment. If each lookup fetched
/// only the block with the directory's rows, the merge would send one GET
/// for each block, about 48 and 128.
///
/// After compaction, the fork commits one more directory and folds it, and
/// the merge stops at the last of the 20,000 directories' commits. A merge
/// through a split hint reads the fork at a sequence below its newest run,
/// so the merge's lookups merge every run, and the test checks that those
/// lookups read ahead too.
#[tokio::test]
async fn a_cold_merge_reads_a_forks_inode_and_binding_segments_in_a_few_data_gets() {
    const DIRECTORIES: usize = 20_000;
    let temporary = tempdir().expect("temporary directory");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temporary.path()).expect("local store"));
    let [source, fork] = ["cold-merge-source", "cold-merge-fork"].map(namespace_id);
    let actor = loonfs_test_support::test_actor();
    let writer = writer(store.clone(), "cold-merge-writer").await;
    let maintenance = writer.maintenance(loonfs_test_support::ids::writer_id(
        "cold-merge-maintenance",
    ));
    writer
        .create_namespace(&source, &actor)
        .await
        .expect("source");
    writer
        .open_namespace(&source)
        .expect("open the source")
        .create_directory("/dirs", &actor)
        .await
        .expect("directory");
    writer
        .fork_namespace(&source, &fork, &actor)
        .await
        .expect("fork");
    let fork_writer = writer.open_namespace(&fork).expect("open the fork");
    let paths: Vec<_> = (0..DIRECTORIES)
        .map(|index| AbsolutePath::parse(format!("/dirs/d-{index:05}")).expect("directory path"))
        .collect();
    let mut through_seq = None;
    for (batch, paths) in paths.chunks(500).enumerate() {
        let commit = fork_writer
            .commit(CommitIntent {
                commit_id: CommitId::parse(format!("batch-{batch}")).expect("commit id"),
                actor_id: actor.clone(),
                subject: None,
                message: None,
                operations: paths
                    .iter()
                    .map(|path| FilesystemOperation::CreateDirectory {
                        path: path.clone(),
                        parents: false,
                    })
                    .collect(),
                preconditions: Vec::new(),
            })
            .await
            .expect("commit batch");
        through_seq = Some(commit.committed_seq);
        maintenance.fold_wal(&fork).await.expect("fold batch");
    }
    for pass in 0.. {
        assert!(pass < 16, "compaction catches up");
        let report = maintenance
            .maintain_metadata_while_due(&fork, &MaintenanceCancellation::new())
            .await
            .expect("compact");
        if report.response.outcome == MetadataMaintenanceOutcome::CaughtUp {
            break;
        }
    }
    fork_writer
        .create_directory("/later", &actor)
        .await
        .expect("a later directory");
    maintenance
        .fold_wal(&fork)
        .await
        .expect("fold the later directory");
    // Each segment's data blocks end where its filter and index blocks
    // start. A segment of at most 256 KiB is fetched whole on its first
    // read, so the test checks the larger ones.
    let large_segments: Vec<_> = loonfs_core::test_support::namespace_store(&store, &fork)
        .load_current_manifest()
        .await
        .expect("load manifest")
        .payload()
        .runs
        .iter()
        .flat_map(|run| &run.segments)
        .filter(|segment| {
            matches!(
                segment.family,
                MetadataRowFamily::Inodes | MetadataRowFamily::DirentryChildBinds
            )
        })
        .map(|segment| {
            (
                segment.family,
                metadata_segment_object_key(segment),
                segment.index_block.offset.min(segment.filter_block.offset),
            )
        })
        .filter(|(_, _, data_end)| *data_end > 256 * 1024)
        .collect();
    assert_eq!(
        large_segments
            .iter()
            .map(|(family, _, _)| *family)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([
            MetadataRowFamily::Inodes,
            MetadataRowFamily::DirentryChildBinds
        ]),
        "each family has a segment too large to fetch whole: {large_segments:?}"
    );
    let data_ends: BTreeMap<String, u64> = large_segments
        .into_iter()
        .map(|(_, key, data_end)| (key, data_end))
        .collect();

    let recording = Arc::new(RecordingStore::metadata_segments(store));
    let merger = loonfs::test_support::writer(recording.clone(), "cold-merge-merger").await;
    merger
        .open_namespace(&source)
        .expect("open the source")
        .merge_fork_with_options(
            &fork,
            &actor,
            &MergeForkOptions {
                through_seq,
                ..Default::default()
            },
        )
        .await
        .expect("merge");
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
