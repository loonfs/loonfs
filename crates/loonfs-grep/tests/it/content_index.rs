//! The grep index of content values: one entry for each value however many
//! files refer to it, the `texts` and `content_revisions` families, and the
//! pruning of a reorganization into the base level.

use crate::common::{request, GrepHost};
use loonfs::publish::{CommitCandidate, CommitIntent, FilesystemOperation, PreparedContent};
use loonfs::{
    Commit, CommitId, ContentRef, LoonFs, MaintainMetadataOptions, Maintenance, MetadataCache,
    Namespace, NamespaceId, PutFileOptions, SharedObjectStore, Writable,
};
use loonfs_grep::codec::{ContentRevisionRow, ContentValue, IndexRow};
use loonfs_grep::entry::{grep_extension_name, GrepEntry};
use loonfs_grep::{GramIndexBuildPolicy, GrepBuildOutcome, GrepReorganizeOutcome};
use loonfs_objectstore::keys::extension_segment_object_key;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::ObjectStore;
use loonfs_test_support::clock::ManualClock;
use loonfs_test_support::ids::{first_page, nonzero_usize};
use loonfs_test_support::stores::{KeyPredicate, RecordingStore};
use loonfs_types::format::segment_blocks::{decode_data_block_rows, decode_index_block};
use loonfs_types::{AbsolutePath, CommitSeq, FileContent, InodeId, RevisionNo, RunNo};
use std::assert_matches;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use tempfile::TempDir;

/// A namespace with a grep host, whose writes store every file's bytes in
/// content objects and whose reader keeps no content in memory, so a test
/// counts the content reads of the index's steps at the store.
pub(crate) struct ContentFixture {
    _directory: TempDir,
    pub(crate) store: SharedObjectStore,
    content_gets: Arc<RecordingStore<LocalFsStore>>,
    pub(crate) host: GrepHost,
    pub(crate) namespace_id: NamespaceId,
    pub(crate) namespace: Namespace<Writable>,
    /// Maintenance in the runtime that has `namespace`'s writer session.
    maintenance: Maintenance,
    /// The wall clock of that runtime, which stamps the fixture's commits.
    clock: Arc<ManualClock>,
}

impl ContentFixture {
    /// Creates the namespace `name` with no files.
    pub(crate) async fn new(name: &str) -> Self {
        let directory = tempfile::tempdir().expect("tempdir");
        let content_gets = Arc::new(RecordingStore::new(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::content_blob(),
        ));
        let store: SharedObjectStore = content_gets.clone();
        let clock = Arc::new(ManualClock::at_system_time());
        let writer = LoonFs::builder_with_store(store.clone())
            .writer_id(loonfs_test_support::ids::writer_id(name))
            .wall_clock(clock.clone())
            .config(loonfs::RuntimeConfig {
                inline_content: loonfs::InlineContentPolicy {
                    inline_content_threshold_bytes: None,
                    ..Default::default()
                },
                min_publish_interval_ms: 0,
                ..loonfs::RuntimeConfig::default()
            })
            .build()
            .await
            .expect("build writer");
        let host = GrepHost::with_reader(&store, name, |builder| {
            builder.metadata_cache(MetadataCache::builder().max_content_object_bytes(0).build())
        })
        .await;
        let namespace_id = NamespaceId::parse(name).expect("namespace id");
        writer
            .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
            .await
            .expect("create namespace");
        let namespace = writer
            .open_namespace(&namespace_id)
            .expect("open namespace");
        let maintenance = writer.maintenance(loonfs_test_support::ids::writer_id(name));
        Self {
            _directory: directory,
            store,
            content_gets,
            host,
            namespace_id,
            namespace,
            maintenance,
            clock,
        }
    }

    /// Writes `bytes` to `path`, replacing any file there.
    pub(crate) async fn put(&self, path: &str, bytes: &[u8]) -> Commit {
        self.namespace
            .put_file_with_options(
                path,
                bytes,
                &loonfs_test_support::test_actor(),
                &PutFileOptions {
                    behavior: loonfs::DestinationBehavior::Replace,
                    ..Default::default()
                },
            )
            .await
            .expect("write the file")
    }

    /// Writes `bytes` to `path` through an upload of its own, replacing any
    /// file there, and returns the upload beside the commit, so a later
    /// commit can refer to the same content value.
    pub(crate) async fn put_prepared(&self, path: &str, bytes: &[u8]) -> (PreparedContent, Commit) {
        let prepared = self
            .namespace
            .prepare_content(bytes)
            .await
            .expect("prepare the content");
        let commit = self
            .commit_prepared(path, &prepared, loonfs::DestinationBehavior::Replace)
            .await;
        (prepared, commit)
    }

    /// Creates the file `path` with the content value of `prepared`, an
    /// upload from an earlier write.
    pub(crate) async fn put_existing(&self, path: &str, prepared: &PreparedContent) -> Commit {
        self.commit_prepared(path, prepared, loonfs::DestinationBehavior::NoReplace)
            .await
    }

    async fn commit_prepared(
        &self,
        path: &str,
        prepared: &PreparedContent,
        behavior: loonfs::DestinationBehavior,
    ) -> Commit {
        self.namespace
            .commit_candidate(CommitCandidate::prepared(
                CommitIntent::single(
                    CommitId::generate(),
                    loonfs_test_support::test_actor(),
                    None,
                    FilesystemOperation::PutFile {
                        path: AbsolutePath::parse(path).expect("path"),
                        content: FileContent::Ref(prepared.content_ref().clone()),
                        behavior,
                        expected_inode_id: None,
                        expected_revision_no: None,
                    },
                ),
                vec![prepared.clone()],
            ))
            .await
            .expect("commit the prepared content")
    }

    /// Copies the file at `source_path` to `path`, so that both files refer
    /// to one content value.
    pub(crate) async fn copy(&self, source_path: &str, path: &str) -> Commit {
        self.namespace
            .copy_path(source_path, path, &loonfs_test_support::test_actor())
            .await
            .expect("copy the file")
    }

    pub(crate) async fn content_ref(&self, path: &str) -> ContentRef {
        self.namespace
            .stat(path)
            .await
            .expect("stat the file")
            .content_ref()
            .expect("a file has content")
            .clone()
    }

    /// Returns how many content objects the store served since the last
    /// call.
    pub(crate) fn content_reads(&self) -> usize {
        self.content_gets.take_get_keys().len()
    }

    /// Runs one build step and returns its outcome.
    pub(crate) async fn build_step(&self) -> GrepBuildOutcome {
        self.host
            .worker
            .steps(&self.namespace_id, 0)
            .build_step(GramIndexBuildPolicy::default())
            .await
            .expect("grep build step")
    }

    /// Runs one reorganize step under `policy` and returns its outcome.
    pub(crate) async fn reorganize_step(
        &self,
        policy: GramIndexBuildPolicy,
    ) -> GrepReorganizeOutcome {
        self.host
            .worker
            .steps(&self.namespace_id, 0)
            .reorganize_step(policy)
            .await
            .expect("grep reorganize step")
    }

    pub(crate) async fn manifest(&self) -> GrepEntry {
        crate::common::control::grep_entry(&self.store, &self.namespace_id)
            .await
            .expect("grep's entry exists")
    }

    /// Returns every row of the index's current segments.
    pub(crate) async fn index_rows(&self) -> Vec<IndexRow> {
        self.run_rows()
            .await
            .into_iter()
            .map(|(_, row)| row)
            .collect()
    }

    /// Returns the content values of each posting batch of the index, read
    /// through the value table of the batch's run.
    pub(crate) async fn posting_batches(&self) -> Vec<Vec<ContentValue>> {
        let rows = self.run_rows().await;
        let tables: BTreeMap<(RunNo, u64), ContentValue> = rows
            .iter()
            .filter_map(|(run_no, row)| match row {
                IndexRow::Value(row) => Some(((*run_no, row.ordinal), row.value())),
                _ => None,
            })
            .collect();
        rows.iter()
            .filter_map(|(run_no, row)| match row {
                IndexRow::GramPostings(row) => Some(
                    row.ordinals()
                        .expect("ordinals")
                        .into_iter()
                        .map(|ordinal| tables[&(*run_no, ordinal)])
                        .collect(),
                ),
                _ => None,
            })
            .collect()
    }

    /// Returns every row of the index's current segments, with its run.
    async fn run_rows(&self) -> Vec<(RunNo, IndexRow)> {
        let mut rows = Vec::new();
        let manifest = self.manifest().await;
        let segments = manifest.runs().iter().flat_map(|run| {
            run.segments
                .iter()
                .map(move |segment| (run.run_no, segment))
        });
        for (run_no, segment) in segments {
            let bytes = self
                .store
                .get(
                    &extension_segment_object_key(&grep_extension_name(), segment),
                    None,
                )
                .await
                .expect("read segment")
                .expect("segment exists");
            let start = loonfs_types::usize_from_u64(segment.index_block.offset);
            for entry in decode_index_block(&bytes[start..], &segment.index_block)
                .expect("decode segment index")
            {
                let start = loonfs_types::usize_from_u64(entry.block.offset);
                let block = decode_data_block_rows::<IndexRow>(
                    &bytes[start..start + entry.block.stored_bytes as usize],
                    &entry.block,
                )
                .expect("decode data block");
                rows.extend(block.rows.into_iter().map(|row| (run_no, row)));
            }
        }
        rows
    }

    /// Returns the `texts` rows of `value`, as their eligibility.
    pub(crate) async fn text_rows(&self, value: &ContentValue) -> Vec<bool> {
        self.index_rows()
            .await
            .into_iter()
            .filter_map(|row| match row {
                IndexRow::Text(row) if row.value() == *value => Some(row.eligible),
                _ => None,
            })
            .collect()
    }

    /// Returns the `content_revisions` rows of the index.
    pub(crate) async fn content_revision_rows(&self) -> Vec<ContentRevisionRow> {
        self.index_rows()
            .await
            .into_iter()
            .filter_map(|row| match row {
                IndexRow::ContentRevision(row) => Some(row),
                _ => None,
            })
            .collect()
    }

    /// Returns the paths whose files match `pattern`, in match order.
    pub(crate) async fn grep_paths(&self, pattern: &str) -> Vec<String> {
        self.host
            .grep(&self.namespace_id, &request(pattern), first_page())
            .await
            .expect("grep")
            .matches
            .into_iter()
            .map(|found| found.path.as_str().to_owned())
            .collect()
    }

    /// Moves the retention floor to the head and returns the new floor, so
    /// the revisions that later revisions replaced are no longer retained.
    ///
    /// The fold and the advance run in the runtime that has the fixture's
    /// writer session, so they join that session's queue and fence nothing.
    /// The advance plans its target from the folded manifest before it folds
    /// the tail itself, so the call folds the tail first and the floor can
    /// reach the head. The floor never passes a commit younger than an hour,
    /// so the call also moves the runtime's clock an hour ahead, and the
    /// floor can then pass every commit made so far. Each call moves the
    /// clock another hour, which does no harm because this clock stamps every
    /// commit the fixture makes.
    pub(crate) async fn advance_retention_floor(&self) -> CommitSeq {
        self.maintenance
            .maintain_metadata_with_options(
                &self.namespace_id,
                &MaintainMetadataOptions {
                    max_wal_tail_objects: std::num::NonZeroU64::MIN,
                },
            )
            .await
            .expect("fold wal");
        self.clock
            .advance_ms(loonfs::RETENTION_ADVANCE_MIN_COMMIT_AGE_MS);
        self.maintenance
            .advance_retention_floor(&self.namespace_id)
            .await
            .expect("advance retention")
            .retention_floor_seq
    }
}

/// The `(inode, revision)` pairs of `rows` that refer to `value`.
fn revisions_of(
    rows: &[ContentRevisionRow],
    value: &ContentValue,
) -> BTreeSet<(InodeId, RevisionNo)> {
    rows.iter()
        .filter(|row| row.value() == *value)
        .map(|row| (row.inode_id, row.revision_no))
        .collect()
}

#[tokio::test]
async fn a_value_indexed_once_for_two_files_is_found_for_both() {
    let fixture = ContentFixture::new("one-value-two-files").await;
    fixture.put("/one.txt", b"a shared needle\n").await;
    let content_ref = fixture.content_ref("/one.txt").await;
    fixture.copy("/one.txt", "/two.txt").await;
    fixture.content_reads();

    fixture
        .host
        .enable_grep_index(&fixture.namespace_id)
        .await
        .expect("enable");

    assert_eq!(
        fixture.content_reads(),
        1,
        "the backfill reads the value once"
    );
    let value = ContentValue::of(&content_ref);
    assert_eq!(fixture.text_rows(&value).await, [true]);
    let rows = fixture.content_revision_rows().await;
    assert_eq!(revisions_of(&rows, &value).len(), 2);
    for postings in fixture.posting_batches().await {
        assert_eq!(
            postings.iter().filter(|posting| **posting == value).count(),
            1
        );
    }
    assert_eq!(
        fixture
            .grep_paths("shared needle")
            .await
            .into_iter()
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["/one.txt".to_owned(), "/two.txt".to_owned()])
    );
}

#[tokio::test]
async fn a_build_step_skips_a_value_that_texts_records() {
    let fixture = ContentFixture::new("skip-recorded-value").await;
    fixture.put("/one.txt", b"a recorded needle\n").await;
    fixture
        .host
        .enable_grep_index(&fixture.namespace_id)
        .await
        .expect("enable");
    let content_ref = fixture.content_ref("/one.txt").await;
    let two = fixture.copy("/one.txt", "/two.txt").await;
    fixture.content_reads();

    assert_matches!(
        fixture.build_step().await,
        GrepBuildOutcome::Indexed {
            built_through_seq,
            indexed_revisions: 0,
            skipped_revisions: 1,
            ..
        } if built_through_seq == two.committed_seq
    );
    assert_eq!(fixture.content_reads(), 0, "the step reads no content");
    let value = ContentValue::of(&content_ref);
    assert_eq!(fixture.text_rows(&value).await, [true]);
    let two_inode = fixture
        .namespace
        .stat("/two.txt")
        .await
        .expect("stat")
        .inode_id;
    let rows = fixture.content_revision_rows().await;
    assert!(rows.iter().any(|row| row.inode_id == two_inode
        && row.value() == value
        && row.committed_seq == two.committed_seq));
    assert_eq!(fixture.grep_paths("recorded needle").await.len(), 2);
}

#[tokio::test]
async fn a_restore_and_a_copy_by_reference_are_found_without_reindexing() {
    let fixture = ContentFixture::new("restore-and-copy").await;
    fixture.put("/a.txt", b"a restored needle\n").await;
    fixture
        .host
        .enable_grep_index(&fixture.namespace_id)
        .await
        .expect("enable");
    fixture
        .namespace
        .copy_path("/a.txt", "/b.txt", &loonfs_test_support::test_actor())
        .await
        .expect("copy a");
    fixture.put("/a.txt", b"other content\n").await;
    fixture.content_reads();
    fixture.build_step().await;
    assert_eq!(fixture.content_reads(), 1, "only the new content is read");

    fixture
        .namespace
        .restore_revision("/a.txt", RevisionNo(1), &loonfs_test_support::test_actor())
        .await
        .expect("restore a");
    assert_matches!(
        fixture.build_step().await,
        GrepBuildOutcome::Indexed {
            indexed_revisions: 0,
            skipped_revisions: 1,
            ..
        }
    );
    assert_eq!(fixture.content_reads(), 0, "the restored value isn't read");
    assert_eq!(
        fixture
            .grep_paths("restored needle")
            .await
            .into_iter()
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["/a.txt".to_owned(), "/b.txt".to_owned()])
    );
}

#[tokio::test]
async fn the_backfill_writes_a_content_revisions_row_for_every_file() {
    let fixture = ContentFixture::new("backfill-content-revisions").await;
    let text = fixture.put("/text.txt", b"a backfilled needle\n").await;
    let binary = fixture.put("/binary.bin", b"\0binary needle").await;
    let deleted = fixture.put("/deleted.txt", b"a deleted needle\n").await;
    let deleted_ref = fixture.content_ref("/deleted.txt").await;
    let deleted_inode = fixture
        .namespace
        .stat("/deleted.txt")
        .await
        .expect("stat")
        .inode_id;
    fixture.put("/twice.txt", b"a first needle\n").await;
    let first_ref = fixture.content_ref("/twice.txt").await;
    let twice = fixture.put("/twice.txt", b"a second needle\n").await;
    fixture
        .namespace
        .delete_path("/deleted.txt", &loonfs_test_support::test_actor())
        .await
        .expect("delete");

    fixture
        .host
        .enable_grep_index(&fixture.namespace_id)
        .await
        .expect("enable");

    let rows = fixture.content_revision_rows().await;
    let mut expected = Vec::new();
    for (path, commit, revision_no) in [
        ("/text.txt", &text, RevisionNo(1)),
        ("/binary.bin", &binary, RevisionNo(1)),
        ("/twice.txt", &twice, RevisionNo(2)),
    ] {
        let entry = fixture.namespace.stat(path).await.expect("stat");
        expected.push((entry.inode_id, revision_no, commit.committed_seq));
        assert_eq!(
            fixture
                .text_rows(&ContentValue::of(entry.content_ref().expect("content")))
                .await,
            [path != "/binary.bin"]
        );
    }
    expected.push((deleted_inode, RevisionNo(1), deleted.committed_seq));
    expected.sort();
    let mut actual: Vec<_> = rows
        .iter()
        .map(|row| (row.inode_id, row.revision_no, row.committed_seq))
        .collect();
    actual.sort();
    assert_eq!(actual, expected, "one row for each file's current revision");
    assert_eq!(
        fixture.text_rows(&ContentValue::of(&deleted_ref)).await,
        [true]
    );
    assert!(fixture
        .text_rows(&ContentValue::of(&first_ref))
        .await
        .is_empty());
}

/// A policy that reorganizes each delta run into the mid level and each mid
/// run into the base, in small steps.
fn reorganize_into_the_base(max_rows: usize) -> GramIndexBuildPolicy {
    GramIndexBuildPolicy {
        max_delta_runs: nonzero_usize(1),
        max_mid_runs: nonzero_usize(1),
        max_decoded_input_rows_per_step: nonzero_usize(max_rows),
        ..GramIndexBuildPolicy::default()
    }
}

#[tokio::test]
async fn a_base_reorganization_drops_a_dead_value_and_keeps_a_referenced_one() {
    let fixture = ContentFixture::new("prune-dead-values").await;
    fixture.put("/a.txt", b"a dead needle\n").await;
    let dead = ContentValue::of(&fixture.content_ref("/a.txt").await);
    fixture
        .host
        .enable_grep_index(&fixture.namespace_id)
        .await
        .expect("enable");
    fixture.put("/a.txt", b"a live needle\n").await;
    let live = ContentValue::of(&fixture.content_ref("/a.txt").await);
    fixture.build_step().await;
    assert_eq!(fixture.grep_paths("dead needle").await.len(), 0);
    assert_eq!(fixture.text_rows(&dead).await, [true]);

    fixture.advance_retention_floor().await;
    for _ in 0..16 {
        if matches!(
            fixture
                .reorganize_step(reorganize_into_the_base(1_000))
                .await,
            GrepReorganizeOutcome::NotNeeded { .. }
        ) {
            break;
        }
    }

    let manifest = fixture.manifest().await;
    assert!(manifest.runs().iter().all(|run| run.level == 2));
    assert!(
        fixture.text_rows(&dead).await.is_empty(),
        "the dead value's row is dropped"
    );
    assert_eq!(fixture.text_rows(&live).await, [true]);
    for postings in fixture.posting_batches().await {
        assert!(!postings.contains(&dead));
    }
    let rows = fixture.index_rows().await;
    assert!(
        !rows
            .iter()
            .any(|row| matches!(row, IndexRow::Value(row) if row.value() == dead)),
        "the dead value leaves the value table"
    );
    assert!(
        rows.iter().any(|row| matches!(row, IndexRow::Filter(_))),
        "the base run stores the revision filter that its later steps read"
    );
    let rows = fixture.content_revision_rows().await;
    assert!(revisions_of(&rows, &dead).is_empty());
    assert_eq!(revisions_of(&rows, &live).len(), 1);
    assert_eq!(fixture.grep_paths("live needle").await, ["/a.txt"]);
}

/// Indexes `/a.txt` with `bytes` and then with a line appended, moves the
/// retention floor to the head, and runs the first step of a base
/// reorganization that drops the first value. Returns the upload of the
/// first value and the policy whose later steps finish the reorganization.
async fn start_a_pruning_base_reorganization(
    fixture: &ContentFixture,
    bytes: &[u8],
) -> (PreparedContent, GramIndexBuildPolicy) {
    let (first, _) = fixture.put_prepared("/a.txt", bytes).await;
    fixture
        .host
        .enable_grep_index(&fixture.namespace_id)
        .await
        .expect("enable");
    fixture
        .namespace
        .append_file("/a.txt", b"and more\n", &loonfs_test_support::test_actor())
        .await
        .expect("append to a");
    fixture.build_step().await;
    fixture.advance_retention_floor().await;
    // Move the delta runs into the mid level, then start a reorganization into
    // the base in steps small enough that the first one doesn't finish it.
    let to_mid = GramIndexBuildPolicy {
        max_delta_runs: nonzero_usize(1),
        ..GramIndexBuildPolicy::default()
    };
    assert_matches!(
        fixture.reorganize_step(to_mid).await,
        GrepReorganizeOutcome::UnitWritten {
            completed: true,
            ..
        }
    );
    let to_base = GramIndexBuildPolicy {
        max_mid_runs: nonzero_usize(1),
        max_decoded_input_rows_per_step: nonzero_usize(4),
        ..GramIndexBuildPolicy::default()
    };
    assert_matches!(
        fixture.reorganize_step(to_base).await,
        GrepReorganizeOutcome::UnitWritten {
            completed: false,
            ..
        }
    );
    let pruning = fixture
        .manifest()
        .await
        .reorganize()
        .and_then(|reorganize| reorganize.pruning);
    assert!(
        pruning.is_some(),
        "the first step records the revisions that its pruning keeps"
    );
    (first, to_base)
}

/// Runs reorganize steps under `policy` until one finishes or abandons the
/// reorganization, and returns that step's outcome.
async fn reorganize_until_settled(
    fixture: &ContentFixture,
    policy: GramIndexBuildPolicy,
) -> GrepReorganizeOutcome {
    let mut outcome = fixture.reorganize_step(policy).await;
    for _ in 0..64 {
        if !matches!(
            outcome,
            GrepReorganizeOutcome::UnitWritten {
                completed: false,
                ..
            }
        ) {
            break;
        }
        outcome = fixture.reorganize_step(policy).await;
    }
    outcome
}

#[tokio::test]
async fn the_re_reference_check_abandons_a_reorganization_whose_dropped_value_comes_back() {
    let fixture = ContentFixture::new("re-reference-check").await;
    let (first, to_base) =
        start_a_pruning_base_reorganization(&fixture, b"a comeback needle\n").await;

    // A new file refers to the first value, as a merge does when it brings a
    // fork's file into the source, and a build step finds the value's `texts`
    // row in the reorganization's inputs and skips it. A restore can't bring
    // the value back, because the writer refuses to restore a revision that
    // the floor pruned.
    fixture.put_existing("/b.txt", &first).await;
    assert_matches!(
        fixture.build_step().await,
        GrepBuildOutcome::Indexed {
            indexed_revisions: 0,
            skipped_revisions: 1,
            ..
        }
    );
    assert_eq!(
        reorganize_until_settled(&fixture, to_base).await,
        GrepReorganizeOutcome::Abandoned
    );
    let manifest = fixture.manifest().await;
    assert!(manifest.reorganize().is_none());
    assert!(manifest.runs().iter().all(|run| run.level != 2));
    assert_eq!(
        fixture
            .grep_paths("comeback needle")
            .await
            .into_iter()
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["/a.txt".to_owned(), "/b.txt".to_owned()])
    );
    assert_eq!(
        fixture
            .text_rows(&ContentValue::of(first.content_ref()))
            .await,
        [true]
    );
}

#[tokio::test]
async fn a_reorganization_whose_pruning_falls_below_the_retention_floor_is_abandoned() {
    let fixture = ContentFixture::new("pruning-below-floor").await;
    let (_, to_base) = start_a_pruning_base_reorganization(&fixture, b"a floored needle\n").await;
    // The floor passes the sequence that the pruning recorded, so the last
    // step can't read the commits after it to check for a re-reference.
    fixture.put("/b.txt", b"a later needle\n").await;
    fixture.advance_retention_floor().await;

    assert_eq!(
        reorganize_until_settled(&fixture, to_base).await,
        GrepReorganizeOutcome::Abandoned
    );
    let manifest = fixture.manifest().await;
    assert!(manifest.reorganize().is_none());
    assert!(manifest.runs().iter().all(|run| run.level != 2));
}
