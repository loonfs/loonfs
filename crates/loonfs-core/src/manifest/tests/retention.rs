//! Checkpoint retention, compaction, and publication budgets.

use super::super::{FoldWalOutcome, PublicationReport};
use super::*;
use crate::segment::block_memo::SessionBlockMemo;
use crate::time::OperationClock;
use loonfs_objectstore::keys::{pin_prefix, wal_prefix};
use loonfs_test_support::clock::ManualClock;
use std::assert_matches;

async fn current_manifest_no<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> ManifestNo {
    crate::test_support::namespace_store(store, namespace_id)
        .load_current_manifest()
        .await
        .expect("read metadata manifest")
        .manifest()
        .manifest_no
}

fn run_segment_object_keys(manifest: &NamespaceManifestEnvelope) -> Vec<String> {
    runs_newest_first(manifest.payload())
        .into_iter()
        .flat_map(|run| {
            run.segments.into_iter().flat_map(|family_segments| {
                family_segments
                    .segments
                    .into_iter()
                    .map(|descriptor| metadata_segment_object_key(&descriptor))
            })
        })
        .collect()
}

// Independent uploads have different content ids and commit fingerprints.
fn metadata_states_equivalent_ignoring_content_identity(
    left: &MetadataState,
    right: &MetadataState,
) -> bool {
    let normalize = |state: &MetadataState| {
        ApiMetadataRowFamily::ALL
            .into_iter()
            .map(|family| {
                let mut rows = manifest_rows_for_family(state, family)
                    .into_iter()
                    .map(|row| match row {
                        MetadataRow::FileRevision(crate::metadata::RevisionRecord {
                            inode_id,
                            revision_no,
                            committed_seq,
                            commit_id,
                            committed_at_ms,
                            committed_by,
                            delta_index,
                            content_ref,
                            ..
                        }) => MetadataRow::FileRevision(crate::metadata::RevisionRecord {
                            inode_id,
                            revision_no,
                            committed_seq,
                            commit_id,
                            committed_at_ms,
                            committed_by,
                            delta_index,
                            content_ref: loonfs_types::ContentRef {
                                content_id: loonfs_types::ContentId::parse(
                                    "con_00000000000000000000000000",
                                )
                                .expect("placeholder content id"),
                                ..content_ref
                            },
                        }),
                        MetadataRow::Commit(mut record) => {
                            record.semantic_commit_fingerprint =
                                loonfs_test_support::ids::commit_fingerprint("normalized");
                            for delta in &mut record.deltas {
                                if let loonfs_types::format::wal::WalDelta::AppendFileRevision {
                                    content_ref,
                                    layout,
                                    ..
                                } = &mut delta.delta
                                {
                                    content_ref.content_id = loonfs_types::ContentId::parse(
                                        "con_00000000000000000000000000",
                                    )
                                    .expect("placeholder content id");
                                    if let Some(layout) = layout {
                                        for extent in &mut layout.extents {
                                            extent.content_id = content_ref.content_id.clone();
                                        }
                                    }
                                }
                                if let loonfs_types::format::wal::WalDelta::BindDirentry {
                                    revision: Some(revision),
                                    ..
                                } = &mut delta.delta
                                {
                                    revision.content_ref.content_id =
                                        loonfs_types::ContentId::parse(
                                            "con_00000000000000000000000000",
                                        )
                                        .expect("placeholder content id");
                                }
                            }
                            MetadataRow::Commit(record)
                        }
                        MetadataRow::DirentryRevision(mut record) => {
                            record.content_ref.content_id =
                                loonfs_types::ContentId::parse("con_00000000000000000000000000")
                                    .expect("placeholder content id");
                            MetadataRow::DirentryRevision(record)
                        }
                        MetadataRow::ContentLayout(mut record) => {
                            record.content_id =
                                loonfs_types::ContentId::parse("con_00000000000000000000000000")
                                    .expect("placeholder content id");
                            for extent in &mut record.layout.extents {
                                extent.content_id = record.content_id.clone();
                            }
                            MetadataRow::ContentLayout(record)
                        }
                        other => other,
                    })
                    .collect::<Vec<_>>();
                rows.sort_by_cached_key(|row| row.row_key_for_family(family));
                (family, rows)
            })
            .collect::<Vec<_>>()
    };
    normalize(left) == normalize(right)
}

fn assert_manifest_rows_have_unique_keys(metadata_state: &MetadataState) {
    for family in ApiMetadataRowFamily::ALL {
        let rows = manifest_rows_for_family(metadata_state, family);
        let mut seen = BTreeSet::new();
        for row in rows {
            let row_key = row.row_key_for_family(family);
            assert!(
                seen.insert(row_key.clone()),
                "duplicate metadata row key `{row_key}` in {family:?}"
            );
        }
    }
}

#[tokio::test]
async fn retention_advancement_uses_published_manifest_and_updates_floor_only() {
    let temp_dir = tempdir().expect("tempdir");
    let store =
        RecordingStore::metadata_segments(LocalFsStore::new(temp_dir.path()).expect("store"));
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");

    store.reset();
    let unchanged = crate::test_support::NamespaceFixture::new(
        &store,
        &namespace_id,
        &crate::test_support::writer_at(
            "test-writer",
            crate::test_support::grace_elapsed_after(context.now_ms),
        ),
    )
    .engine()
    .advance_retention_floor(RetentionTarget::Head)
    .await
    .expect("initial manifest already covers floor zero")
    .response;
    assert_eq!(unchanged.retention_floor_seq, ChangeSeq(0));
    assert_eq!(
        store.count(OperationSelector::Read),
        0,
        "retention should validate manifest descriptors without loading metadata segment payloads"
    );

    // The test folds three commits with one row per segment, so the segments
    // that the advance reads show which commit rows the advance read.
    for path in ["/docs/a.txt", "/docs/b.txt", "/docs/c.txt"] {
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
            .write_file_bytes(path, b"body\n", None)
            .await
            .expect("write a file");
    }
    crate::NamespaceStore {
        run_policy: MetadataRunPolicy {
            max_rows_per_segment: NonZeroUsize::MIN,
            ..MetadataRunPolicy::default()
        },
        ..crate::test_support::namespace_store(&store, &namespace_id)
    }
    .fold_wal_tail(
        None,
        &OperationClock::start(0, Arc::new(StdMonotonicTimer::default())),
    )
    .await
    .expect("fold the commits");
    let manifest = crate::test_support::namespace_store(&store, &namespace_id)
        .load_current_manifest()
        .await
        .expect("load metadata manifest");
    let segments = load_manifest_segments_for_inspection(
        &store,
        None,
        &namespace_id,
        &manifest.manifest().manifest_no,
    )
    .await
    .expect("load current manifest");
    let head_seq = segments.manifest().payload().head_seq;
    assert_eq!(head_seq, ChangeSeq(3));
    let referenced = segments
        .manifest()
        .payload()
        .runs
        .iter()
        .flat_map(|run| &run.segments);
    let referenced_segment_count = referenced
        .clone()
        .map(metadata_segment_object_key)
        .collect::<BTreeSet<_>>()
        .len();
    let commit_segments = referenced
        .filter(|segment| segment.family == ApiMetadataRowFamily::Commits)
        .map(metadata_segment_object_key)
        .collect::<BTreeSet<_>>();
    assert_eq!(commit_segments.len(), 3, "one segment per commit row");
    let wal_objects_prefix = wal_prefix(&namespace_id);
    let wal_objects = store
        .list_prefix(&wal_objects_prefix)
        .await
        .expect("list wal");
    store.reset();
    let advanced = crate::test_support::NamespaceFixture::new(
        &store,
        &namespace_id,
        &crate::test_support::writer_at(
            "test-writer",
            crate::test_support::grace_elapsed_after(context.now_ms),
        ),
    )
    .engine()
    .advance_retention_floor(RetentionTarget::Head)
    .await
    .expect("advance retention")
    .response;
    assert_eq!(advanced.retention_floor_seq, head_seq);
    assert_eq!(
        store.count(OperationSelector::Head),
        referenced_segment_count,
        "retention should verify each distinct referenced segment"
    );
    assert_eq!(
        store.take_get_keys().into_iter().collect::<BTreeSet<_>>(),
        commit_segments,
        "every commit is a grace window old, so the advance reads the commit rows up to the head and no other rows"
    );

    assert_eq!(read_floor_seq(&store, &namespace_id).await, head_seq);
    assert_eq!(
        store
            .list_prefix(&wal_objects_prefix)
            .await
            .expect("list wal"),
        wal_objects,
        "the advance only moves the floor"
    );
    assert!(store
        .head(&current_manifest_key(&store, &namespace_id).await)
        .await
        .expect("manifest head")
        .is_some());
}

#[tokio::test]
async fn a_targeted_advance_stops_at_the_cutoff_or_the_sequence_and_never_lowers_the_floor() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");
    // Commit times come from writers' clocks: the third commit is stamped
    // before the second.
    for (path, committed_at_ms) in [
        ("/a.txt", 2_000),
        ("/b.txt", 3_000),
        ("/c.txt", 2_500),
        ("/d.txt", 5_000),
    ] {
        let context = WriterAt {
            now_ms: committed_at_ms,
            ..context.clone()
        };
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
            .write_file_bytes(path, b"body\n", None)
            .await
            .expect("commit");
    }
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("fold the commits");

    for (target, expected_floor) in [
        (
            RetentionTarget::Cutoff {
                cutoff_at_ms: 1_999,
            },
            ChangeSeq(0),
        ),
        // Commit 3 is stamped before the cutoff, but the later commit 2
        // ends the scan first.
        (
            RetentionTarget::Cutoff {
                cutoff_at_ms: 2_999,
            },
            ChangeSeq(1),
        ),
        (
            RetentionTarget::Cutoff {
                cutoff_at_ms: 3_000,
            },
            ChangeSeq(3),
        ),
        (RetentionTarget::Seq(ChangeSeq(2)), ChangeSeq(3)),
        (RetentionTarget::Seq(ChangeSeq(99)), ChangeSeq(4)),
    ] {
        let before = current_manifest_no(&store, &namespace_id).await;
        let floor = crate::test_support::NamespaceFixture::new(
            &store,
            &namespace_id,
            &crate::test_support::writer_at(
                "test-writer",
                crate::test_support::grace_elapsed_after(5_000),
            ),
        )
        .engine()
        .advance_retention_floor(target)
        .await
        .expect("advance the floor")
        .response
        .retention_floor_seq;
        assert_eq!(floor, expected_floor, "{target:?}");
        assert_eq!(read_floor_seq(&store, &namespace_id).await, expected_floor);
        if target == RetentionTarget::Seq(ChangeSeq(2)) {
            assert_eq!(
                current_manifest_no(&store, &namespace_id).await,
                before,
                "a target below the floor publishes nothing"
            );
        }
    }
}

#[tokio::test]
async fn an_advance_stops_below_the_first_commit_younger_than_the_grace_window() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");
    let grace_ms = crate::limits::GC_DEFAULT_GRACE_WINDOW_MS;
    let now_ms = 10 * grace_ms;
    // Commit 3 is stamped before commit 2, and commits 5 and 6 are stamped
    // after `now_ms`.
    for (path, committed_at_ms) in [
        ("/a.txt", now_ms - 3 * grace_ms),
        ("/b.txt", now_ms - grace_ms / 2),
        ("/c.txt", now_ms - 2 * grace_ms),
        ("/d.txt", now_ms - grace_ms / 4),
        ("/e.txt", now_ms + grace_ms / 2),
        ("/f.txt", now_ms + 3 * grace_ms),
    ] {
        let context = WriterAt {
            now_ms: committed_at_ms,
            ..context.clone()
        };
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
            .write_file_bytes(path, b"body\n", None)
            .await
            .expect("commit");
    }
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("fold the commits");

    for (target, advance_at_ms, expected_floor) in [
        // A cutoff more than a window old stops where it would without the
        // window.
        (
            RetentionTarget::Cutoff {
                cutoff_at_ms: now_ms - 3 * grace_ms,
            },
            now_ms,
            ChangeSeq(1),
        ),
        // Commit 3 is two windows old, but commit 2 is half a window old, so
        // the walk stops before commit 2 and the floor stays at 1. A commit
        // dated out of order can only stop the floor earlier.
        (RetentionTarget::Seq(ChangeSeq(3)), now_ms, ChangeSeq(1)),
        // Commit 5 is half a window old, so the floor stops at commit 4.
        (RetentionTarget::Head, now_ms + grace_ms, ChangeSeq(4)),
        // A sequence above the head targets the head, and commit 6 is still
        // dated after the advance's clock.
        (
            RetentionTarget::Seq(ChangeSeq(99)),
            now_ms + 2 * grace_ms,
            ChangeSeq(5),
        ),
        // Commit 6 is exactly one window old, so the floor reaches the head.
        (RetentionTarget::Head, now_ms + 4 * grace_ms, ChangeSeq(6)),
    ] {
        let floor = crate::test_support::NamespaceFixture::new(
            &store,
            &namespace_id,
            &crate::test_support::writer_at("test-writer", advance_at_ms),
        )
        .engine()
        .advance_retention_floor(target)
        .await
        .expect("advance the floor")
        .response
        .retention_floor_seq;
        assert_eq!(floor, expected_floor, "{target:?} at {advance_at_ms}");
        assert_eq!(
            read_floor_seq(&store, &namespace_id).await,
            floor,
            "the response reports the published floor"
        );
    }
}

/// Publishes `count` commits stamped `committed_at_ms`, each creating the
/// directory `/{prefix}-{index}`. Each commit's message is
/// `MAX_COMMIT_MESSAGE_BYTES` of pseudorandom printable characters generated
/// from `state`, so the messages compress poorly.
async fn commit_directories_with_long_messages<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    prefix: &str,
    committed_at_ms: u64,
    count: usize,
    state: &mut u64,
) {
    let candidates: Vec<_> = (0..count)
        .map(|index| {
            let message = (0..crate::limits::MAX_COMMIT_MESSAGE_BYTES)
                .map(|_| {
                    *state ^= *state << 13;
                    *state ^= *state >> 7;
                    *state ^= *state << 17;
                    char::from(
                        b'!' + u8::try_from(*state % 94).expect("a remainder below 94 fits a byte"),
                    )
                })
                .collect();
            CommitCandidate::new(CommitIntent::single(
                CommitId::generate(),
                loonfs_test_support::test_actor(),
                Some(message),
                FilesystemOperation::CreateDirectory {
                    path: AbsolutePath::parse(format!("/{prefix}-{index:02}")).expect("path"),
                    parents: false,
                },
            ))
        })
        .collect();
    let context = WriterAt {
        now_ms: committed_at_ms,
        ..test_context()
    };
    for result in NamespaceCommitEngine::new(
        crate::test_support::namespace_store(store, namespace_id),
        context.writer_id.clone(),
    )
    .publish_batch(candidates, &context.clock())
    .await
    .results
    {
        result.expect("commit a directory");
    }
}

/// Returns the current manifest's one segment of commit rows, with its index.
async fn commit_segment_with_index<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> (MetadataSegmentRef, Arc<Vec<SegmentIndexEntry>>) {
    let manifest = crate::test_support::namespace_store(store, namespace_id)
        .load_current_manifest()
        .await
        .expect("load the manifest");
    let mut segments = manifest
        .envelope
        .payload()
        .runs
        .iter()
        .flat_map(|run| &run.segments)
        .filter(|segment| segment.family == ApiMetadataRowFamily::Commits);
    let segment = segments.next().expect("a segment of commit rows").clone();
    assert!(segments.next().is_none(), "one segment of commit rows");
    let index =
        index_block_load::load_segment_index(store, None, &SessionBlockMemo::default(), &segment)
            .await
            .expect("load the index");
    (segment, index)
}

/// A namespace whose 128 commits are folded into one segment of commit rows,
/// which is too large to be fetched whole. Each commit has a long message, so
/// each data block of the segment contains 14 commit rows.
struct LongCommitHistory {
    _temp_dir: tempfile::TempDir,
    store: RecordingStore<LocalFsStore>,
    namespace_id: NamespaceId,
    segment: MetadataSegmentRef,
    index: Arc<Vec<SegmentIndexEntry>>,
}

impl LongCommitHistory {
    /// Publishes `count` commits stamped `committed_at_ms` for each pair of
    /// `groups`, in order, and folds them.
    async fn new(groups: &[(usize, u64)]) -> Self {
        let temp_dir = tempdir().expect("tempdir");
        let store =
            RecordingStore::metadata_segments(LocalFsStore::new(temp_dir.path()).expect("store"));
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        bootstrap_namespace(&store, &namespace_id, &test_context())
            .await
            .expect("bootstrap");
        let mut state = 1_u64;
        for (group, &(count, committed_at_ms)) in groups.iter().enumerate() {
            if count > 0 {
                commit_directories_with_long_messages(
                    &store,
                    &namespace_id,
                    &format!("group-{group}"),
                    committed_at_ms,
                    count,
                    &mut state,
                )
                .await;
            }
        }
        crate::test_support::namespace_store(&store, &namespace_id)
            .fold_wal()
            .await
            .expect("fold the commits");
        let (segment, index) = commit_segment_with_index(&store, &namespace_id).await;
        assert_eq!(
            index[0].last_row_key,
            lookup_keys::commit_row_key(ChangeSeq(14)),
            "each data block contains 14 commit rows"
        );
        store.reset();
        Self {
            _temp_dir: temp_dir,
            store,
            namespace_id,
            segment,
            index,
        }
    }

    /// Takes the recorded GETs, checks that each one reads a range of the
    /// segment of commit rows, and returns the ranges.
    fn take_commit_ranges(&self) -> Vec<(u64, u64)> {
        let object_key = metadata_segment_object_key(&self.segment);
        self.store
            .take_gets()
            .into_iter()
            .map(|(key, range)| {
                assert_eq!(key, object_key, "an advance reads only commit rows");
                range.expect("an advance reads ranges of the segment of commit rows")
            })
            .collect()
    }

    /// Returns the ranges of the segment's index and of each data block from
    /// the first block through the block that contains commit `seq`, which
    /// are the GETs of a walk through [`cache_reading_one_block_at_a_time`]
    /// that reads commit `seq` last.
    fn ranges_through(&self, seq: ChangeSeq) -> Vec<(u64, u64)> {
        let range =
            |block: &BlockHandle| (block.offset, block.offset + u64::from(block.stored_bytes));
        let key = lookup_keys::commit_row_key(seq);
        let last = self.index.partition_point(|entry| entry.last_row_key < key);
        std::iter::once(range(&self.segment.index_block))
            .chain(self.index[..=last].iter().map(|entry| range(&entry.block)))
            .collect()
    }
}

/// A cache whose reads fetch one data block at a time, so the GETs of a walk
/// show each block that it reached.
fn cache_reading_one_block_at_a_time() -> MetadataSegmentCache {
    MetadataSegmentCache::new(
        Arc::new(SharedSegmentBlocks::new(64 * 1024 * 1024, 64 * 1024 * 1024)),
        ObjectStoreScope::new(0),
        crate::segment::cache::unshared_read_working_memory(),
        1,
    )
}

/// Checks that an advance reads commits upward from the floor, and stops at
/// its sequence or at the first commit younger than the grace window, with
/// one ranged GET for each block of commit rows that it reaches.
#[tokio::test]
async fn an_advance_sends_one_get_for_each_commit_block_up_to_where_it_stops() {
    let grace_ms = crate::limits::GC_DEFAULT_GRACE_WINDOW_MS;
    let now_ms = 10 * grace_ms;
    for (target, expected_floor, last_read) in [
        // Commit 65 is the first commit younger than the window, so the walk
        // reads commits 1 to 65, and the floor moves to commit 64.
        (RetentionTarget::Head, ChangeSeq(64), ChangeSeq(65)),
        // The sequence ends the walk after commit 14, the last row of the
        // first block, so the walk fetches no other block.
        (
            RetentionTarget::Seq(ChangeSeq(14)),
            ChangeSeq(14),
            ChangeSeq(14),
        ),
        // Commit 65 ends the walk below the sequence.
        (
            RetentionTarget::Seq(ChangeSeq(100)),
            ChangeSeq(64),
            ChangeSeq(65),
        ),
    ] {
        // Commits 1 to 64 are two windows old, and commits 65 to 128 are half
        // a window old.
        let history =
            LongCommitHistory::new(&[(64, now_ms - 2 * grace_ms), (64, now_ms - grace_ms / 2)])
                .await;
        let floor = {
            let mut fixture = crate::test_support::NamespaceFixture::new(
                &history.store,
                &history.namespace_id,
                &crate::test_support::writer_at("test-writer", now_ms),
            );
            fixture.namespace.segment_cache = Arc::new(cache_reading_one_block_at_a_time());
            fixture
        }
        .engine()
        .advance_retention_floor(target)
        .await
        .expect("advance the floor")
        .response
        .retention_floor_seq;
        assert_eq!(floor, expected_floor, "{target:?}");
        assert_eq!(
            history.take_commit_ranges(),
            history.ranges_through(last_read),
            "{target:?}: one GET for the index, then one for each block through the block of commit {last_read}"
        );
    }
}

/// Checks that an advance reads one commit row when the first commit above
/// the floor is younger than the grace window, as every commit of a fresh
/// namespace is, and leaves the floor where it is.
#[tokio::test]
async fn an_advance_reads_one_commit_row_when_the_first_commit_above_the_floor_is_young() {
    let grace_ms = crate::limits::GC_DEFAULT_GRACE_WINDOW_MS;
    let now_ms = 10 * grace_ms;
    // With no old commits, the namespace is fresh, and the walk reads commit
    // 1. With 13 old commits, a first advance moves the floor to 13, and the
    // walk reads commit 14, the last row of the first block, so a walk that
    // read another commit row would fetch the second block.
    for old_commits in [0, 13] {
        let history = LongCommitHistory::new(&[
            (old_commits, now_ms - 2 * grace_ms),
            (128 - old_commits, now_ms - grace_ms / 2),
        ])
        .await;
        let floor = ChangeSeq(u64::try_from(old_commits).expect("a count fits a u64"));
        for advance in ["first", "second"] {
            history.store.reset();
            let report = {
                let mut fixture = crate::test_support::NamespaceFixture::new(
                    &history.store,
                    &history.namespace_id,
                    &crate::test_support::writer_at("test-writer", now_ms),
                );
                fixture.namespace.segment_cache = Arc::new(cache_reading_one_block_at_a_time());
                fixture
            }
            .engine()
            .advance_retention_floor(RetentionTarget::Head)
            .await
            .expect("advance the floor");
            assert_eq!(
                report.response.retention_floor_seq, floor,
                "{advance} advance"
            );
            assert_eq!(
                report.published.is_some(),
                advance == "first" && old_commits > 0,
                "{advance} advance: only a floor that moves publishes a manifest"
            );
        }
        assert_eq!(
            history.take_commit_ranges(),
            history.ranges_through(ChangeSeq(floor.0 + 1)),
            "{old_commits} old commits: one GET for the index and one for the first block"
        );
    }
}

/// Checks that an advance toward a cutoff, or a retention window, that's
/// older than the grace window reads the commits up to that cutoff once.
#[tokio::test]
async fn a_cutoff_or_a_window_older_than_the_grace_window_reads_the_commits_once() {
    let grace_ms = crate::limits::GC_DEFAULT_GRACE_WINDOW_MS;
    let now_ms = 10 * grace_ms;
    for target in [
        RetentionTarget::Cutoff {
            cutoff_at_ms: now_ms - 5 * grace_ms / 2,
        },
        RetentionTarget::Window {
            default_window_ms: std::num::NonZeroU64::new(5 * grace_ms / 2),
        },
    ] {
        // Commits 1 to 64 are older than the target's cutoff, and commits 65
        // to 128 are between that cutoff and the grace window's.
        let history =
            LongCommitHistory::new(&[(64, now_ms - 3 * grace_ms), (64, now_ms - 2 * grace_ms)])
                .await;
        let floor = crate::test_support::NamespaceFixture::new(
            &history.store,
            &history.namespace_id,
            &crate::test_support::writer_at("test-writer", now_ms),
        )
        .engine()
        .advance_retention_floor(target)
        .await
        .expect("advance the floor")
        .response
        .retention_floor_seq;
        assert_eq!(floor, ChangeSeq(64), "{target:?}");
        // Without a cache, the read-ahead window from the first block covers
        // every block, and a second walk would fetch the index and the blocks
        // again.
        let ranges = history.ranges_through(ChangeSeq(128));
        assert_eq!(
            history.take_commit_ranges(),
            vec![ranges[0], (ranges[1].0, ranges[ranges.len() - 1].1)],
            "{target:?}: one GET for the index and one for the blocks"
        );
    }
}

#[tokio::test]
async fn a_window_advance_prefers_the_namespace_window_and_keeps_the_floor_without_one() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");
    let hour_ms = crate::limits::GC_DEFAULT_GRACE_WINDOW_MS;
    let now_ms = 5 * hour_ms;
    for (path, committed_at_ms) in [
        ("/a.txt", 2 * hour_ms),
        ("/b.txt", 3 * hour_ms),
        ("/c.txt", 4 * hour_ms),
    ] {
        let context = WriterAt {
            now_ms: committed_at_ms,
            ..context.clone()
        };
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
            .write_file_bytes(path, b"body\n", None)
            .await
            .expect("commit");
    }
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("fold the commits");
    let window = |window_ms: u64| std::num::NonZeroU64::new(window_ms);
    let engine = crate::test_support::NamespaceFixture::new(
        &store,
        &namespace_id,
        &crate::test_support::writer_at("test-writer", 0),
    )
    .engine();
    let set_window =
        |window_ms: Option<u64>| engine.set_retention_window(window_ms.and_then(window));

    // Each step sets the namespace's window, then advances at five hours
    // with a default window. The cutoffs are five hours minus the window
    // that applies, and all three commits are at least an hour old.
    for (namespace_window_ms, default_window_ms, expected_floor) in [
        (None, None, ChangeSeq(0)),
        (None, Some(5 * hour_ms / 2), ChangeSeq(1)),
        // The namespace's window applies, so the cutoff is 3.5 hours, not 2.5.
        (Some(3 * hour_ms / 2), Some(5 * hour_ms / 2), ChangeSeq(2)),
        // Once the window is cleared, the default applies again.
        (None, Some(hour_ms / 2), ChangeSeq(3)),
    ] {
        let set = set_window(namespace_window_ms)
            .await
            .expect("set the window");
        assert_eq!(
            set.response.retention_window_ms, namespace_window_ms,
            "the response reports the namespace's window"
        );
        let before = current_manifest_no(&store, &namespace_id).await;
        assert!(
            set_window(namespace_window_ms)
                .await
                .expect("set the same window")
                .published
                .is_none(),
            "setting the window that the namespace has publishes nothing"
        );
        assert_eq!(current_manifest_no(&store, &namespace_id).await, before);
        let target = RetentionTarget::Window {
            default_window_ms: default_window_ms.and_then(window),
        };
        let floor = crate::test_support::NamespaceFixture::new(
            &store,
            &namespace_id,
            &crate::test_support::writer_at("test-writer", now_ms),
        )
        .engine()
        .advance_retention_floor(target)
        .await
        .expect("advance the floor")
        .response
        .retention_floor_seq;
        assert_eq!(floor, expected_floor, "{namespace_window_ms:?} {target:?}");
    }
}

#[tokio::test]
async fn a_cutoff_advance_reads_commits_through_the_given_cache() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Peak(AtomicUsize);
    impl crate::cache::ReadWorkingMemoryObserver for Peak {
        fn in_use(&self, bytes: usize) {
            self.0.fetch_max(bytes, Ordering::SeqCst);
        }
        fn reservation_failed(&self) {}
    }

    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");
    for path in ["/a.txt", "/b.txt"] {
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
            .write_file_bytes(path, b"body\n", None)
            .await
            .expect("commit");
    }
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("fold the commits");

    let peak = Arc::new(Peak(AtomicUsize::new(0)));
    let observer: Arc<dyn crate::cache::ReadWorkingMemoryObserver> = peak.clone();
    let cache = Arc::new(MetadataSegmentCache::new(
        Arc::new(SharedSegmentBlocks::new(0, 0)),
        ObjectStoreScope::new(0),
        Arc::new(crate::cache::ReadWorkingMemory::new(
            64 * 1024 * 1024,
            Some(observer),
        )),
        crate::limits::READAHEAD_WINDOW_BYTES,
    ));
    let floor = {
        let mut fixture = crate::test_support::NamespaceFixture::new(
            &store,
            &namespace_id,
            &crate::test_support::writer_at(
                "test-writer",
                crate::test_support::grace_elapsed_after(context.now_ms),
            ),
        );
        fixture.namespace.segment_cache = Arc::clone(&cache);
        fixture
    }
    .engine()
    .advance_retention_floor(RetentionTarget::Cutoff {
        cutoff_at_ms: u64::MAX,
    })
    .await
    .expect("advance to the newest commit")
    .response
    .retention_floor_seq;
    assert_eq!(floor, ChangeSeq(2));
    assert!(
        peak.0.load(Ordering::SeqCst) > 0,
        "the cutoff scan charges the caller's read budget"
    );
}

#[tokio::test]
async fn retention_floor_does_not_advance_past_a_missing_basis_segment() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .write_file_bytes("/docs/hello.txt", b"hello\n", None)
        .await
        .expect("write hello");
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("create checkpoint");

    let manifest = crate::test_support::namespace_store(&store, &namespace_id)
        .load_current_manifest()
        .await
        .expect("load metadata manifest");
    let segments = load_manifest_segments_for_inspection(
        &store,
        None,
        &namespace_id,
        &manifest.manifest().manifest_no,
    )
    .await
    .expect("load current manifest");
    let missing_key = segments
        .manifest()
        .payload()
        .runs
        .iter()
        .flat_map(|run| &run.segments)
        .next()
        .map(metadata_segment_object_key)
        .expect("current manifest references a segment");
    store
        .delete(&missing_key)
        .await
        .expect("delete referenced segment");

    let error = crate::test_support::NamespaceFixture::new(
        &store,
        &namespace_id,
        &crate::test_support::writer_at(
            "test-writer",
            crate::test_support::grace_elapsed_after(context.now_ms),
        ),
    )
    .engine()
    .advance_retention_floor(RetentionTarget::Head)
    .await
    .expect_err("missing basis segment blocks floor advancement");
    assert_matches!(
        error,
        CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(
            ManifestLoadError::MissingSegment { object_key }
        )) if object_key == missing_key
    );
    assert_eq!(
        read_floor_seq(&store, &namespace_id).await,
        ChangeSeq(0),
        "the missing basis remains replayable from the prior floor"
    );
}

#[tokio::test]
async fn retention_floor_does_not_advance_when_a_basis_segment_cannot_be_checked() {
    let temp_dir = tempdir().expect("tempdir");
    let setup_store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&setup_store, &namespace_id, &context)
        .await
        .expect("bootstrap");
    crate::test_support::NamespaceFixture::new(&setup_store, &namespace_id, &context)
        .write_file_bytes("/docs/hello.txt", b"hello\n", None)
        .await
        .expect("write hello");
    create_checkpoint(&setup_store, &namespace_id, &context)
        .await
        .expect("create checkpoint");

    let manifest = crate::test_support::namespace_store(&setup_store, &namespace_id)
        .load_current_manifest()
        .await
        .expect("load metadata manifest");
    let segments = load_manifest_segments_for_inspection(
        &setup_store,
        None,
        &namespace_id,
        &manifest.manifest().manifest_no,
    )
    .await
    .expect("load current manifest");
    let selected_key = segments
        .manifest()
        .payload()
        .runs
        .iter()
        .flat_map(|run| &run.segments)
        .next()
        .map(metadata_segment_object_key)
        .expect("current manifest references a segment");
    let failed_key = selected_key.clone();
    let store = FailStore::matching(
        setup_store,
        move |operation| {
            operation.key() == selected_key
                && matches!(
                    operation.kind(),
                    loonfs_test_support::stores::OperationKind::Head
                )
        },
        InjectedError::Transport("injected segment probe failure".to_owned()),
    );
    store.fail_next(1);

    let error = crate::test_support::NamespaceFixture::new(
        &store,
        &namespace_id,
        &crate::test_support::writer_at(
            "test-writer",
            crate::test_support::grace_elapsed_after(context.now_ms),
        ),
    )
    .engine()
    .advance_retention_floor(RetentionTarget::Head)
    .await
    .expect_err("failed basis probe blocks floor advancement");
    assert_eq!(error.code(), ErrorCode::ServerError);
    assert_matches!(
        error,
        CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(
            ManifestLoadError::ReadSegment { object_key, .. }
        )) if object_key == failed_key
    );
    assert_eq!(store.attempts(), 1);
    assert_eq!(
        read_floor_seq(store.inner(), &namespace_id).await,
        ChangeSeq(0),
        "the failed probe leaves the prior floor in place"
    );
}

/// Publishes `count` commits through `writer` in one batch, each creating
/// the directory `/{prefix}-{index}`.
async fn publish_directories<S: ObjectStore>(
    writer: &mut NamespaceCommitEngine<S>,
    prefix: &str,
    count: usize,
) {
    let candidates: Vec<_> = (0..count)
        .map(|index| {
            CommitCandidate::new(CommitIntent::single(
                CommitId::generate(),
                loonfs_test_support::test_actor(),
                None,
                FilesystemOperation::CreateDirectory {
                    path: AbsolutePath::parse(format!("/{prefix}-{index}")).expect("path"),
                    parents: false,
                },
            ))
        })
        .collect();
    for result in writer
        .publish_batch(candidates, &test_context().clock())
        .await
        .results
    {
        result.expect("create a directory");
    }
}

/// The current manifest's retention floor and acknowledged floor.
async fn current_floors<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> (ChangeSeq, ChangeSeq) {
    let manifest = crate::test_support::namespace_store(store, namespace_id)
        .load_current_manifest()
        .await
        .expect("load the current manifest");
    let payload = manifest.envelope.payload();
    (payload.retention_floor_seq, payload.acknowledged_floor_seq)
}

/// The basis floors of the WAL objects above the current manifest, oldest
/// first.
async fn tail_basis_floors<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Vec<ChangeSeq> {
    crate::test_support::namespace_store(store, namespace_id)
        .load_read_anchor(crate::test_support::observed_now())
        .await
        .expect("load the WAL tail")
        .tail
        .objects()
        .iter()
        .map(|object| object.envelope().payload().basis_floor_seq)
        .collect()
}

/// Advances the floor to the folded head, at a time when every commit is a
/// grace window old.
async fn advance_to_folded_head<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> PublicationReport<loonfs_types::AdvanceRetentionResponse> {
    crate::test_support::NamespaceFixture::new(
        store,
        namespace_id,
        &crate::test_support::writer_at(
            "test-writer",
            crate::test_support::grace_elapsed_after(test_context().now_ms),
        ),
    )
    .engine()
    .advance_retention_floor(RetentionTarget::Head)
    .await
    .expect("advance the floor")
}

#[tokio::test]
async fn a_fold_acknowledges_the_highest_basis_floor_that_it_folds() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    bootstrap_namespace(&store, &namespace_id, &test_context())
        .await
        .expect("bootstrap");
    let mut writer = NamespaceCommitEngine::new(
        crate::test_support::namespace_store(&store, &namespace_id),
        test_context().writer_id,
    );
    publish_directories(&mut writer, "folded", 20).await;
    crate::test_support::namespace_store(&store, &namespace_id)
        .fold_wal()
        .await
        .expect("fold");
    publish_directories(&mut writer, "first", 1).await;
    publish_directories(&mut writer, "second", 1).await;

    // An advance raises the floor and copies the acknowledged floor.
    let advance = advance_to_folded_head(&store, &namespace_id).await;
    assert_eq!(advance.response.retention_floor_seq, ChangeSeq(20));
    assert_eq!(
        current_floors(&store, &namespace_id).await,
        (ChangeSeq(20), ChangeSeq(0))
    );

    // The session's position moves onto the advance's manifest, so its next
    // WAL object records the new floor.
    writer.record_manifest_published(advance.published.expect("the advance publishes"));
    publish_directories(&mut writer, "third", 1).await;
    assert_eq!(
        tail_basis_floors(&store, &namespace_id).await,
        [ChangeSeq(0), ChangeSeq(0), ChangeSeq(20)]
    );
    crate::test_support::namespace_store(&store, &namespace_id)
        .fold_wal()
        .await
        .expect("fold");
    assert_eq!(
        current_floors(&store, &namespace_id).await,
        (ChangeSeq(20), ChangeSeq(20))
    );

    // A fold of objects that record the acknowledged floor keeps it.
    publish_directories(&mut writer, "fourth", 1).await;
    assert_eq!(
        tail_basis_floors(&store, &namespace_id).await,
        [ChangeSeq(20)]
    );
    crate::test_support::namespace_store(&store, &namespace_id)
        .fold_wal()
        .await
        .expect("fold");
    assert_eq!(
        current_floors(&store, &namespace_id).await,
        (ChangeSeq(20), ChangeSeq(20))
    );

    // A fold with no WAL object to fold publishes nothing, so the
    // acknowledged floor stays where it is.
    let unchanged = current_manifest_no(&store, &namespace_id).await;
    assert_eq!(
        crate::test_support::namespace_store(&store, &namespace_id)
            .fold_wal()
            .await
            .expect("fold")
            .outcome,
        FoldWalOutcome::AlreadyCurrent
    );
    assert_eq!(current_manifest_no(&store, &namespace_id).await, unchanged);
}

#[tokio::test]
async fn a_fence_acknowledges_the_floor_of_a_namespace_that_nobody_writes_to() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");
    for path in ["/a.txt", "/b.txt"] {
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
            .write_file_bytes(path, b"body\n", None)
            .await
            .expect("commit");
    }
    crate::test_support::namespace_store(&store, &namespace_id)
        .fold_wal()
        .await
        .expect("fold");
    advance_to_folded_head(&store, &namespace_id).await;
    assert_eq!(
        current_floors(&store, &namespace_id).await,
        (ChangeSeq(2), ChangeSeq(0))
    );
    assert_eq!(
        crate::test_support::namespace_store(&store, &namespace_id)
            .fold_wal()
            .await
            .expect("fold")
            .outcome,
        FoldWalOutcome::AlreadyCurrent,
        "without a WAL object, no fold acknowledges the new floor"
    );

    // A new session's fence records the floor of the manifest it found, and
    // a fold of the fence acknowledges that floor.
    crate::test_support::namespace_store(&store, &namespace_id)
        .acquire_writer_epoch(&context.writer_id)
        .await
        .expect("acquire the writer epoch");
    assert_eq!(
        tail_basis_floors(&store, &namespace_id).await,
        [ChangeSeq(2)]
    );
    crate::test_support::namespace_store(&store, &namespace_id)
        .fold_wal()
        .await
        .expect("fold");
    assert_eq!(
        current_floors(&store, &namespace_id).await,
        (ChangeSeq(2), ChangeSeq(2))
    );
}

#[test]
fn a_manifest_whose_acknowledged_floor_breaks_its_bounds_fails_to_load() {
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let created = NamespaceManifestPayload::initial(
        namespace_id.clone(),
        1_000,
        loonfs_test_support::test_actor(),
        loonfs_types::NamespaceAccess::Unrestricted {},
    );
    let fork_basis = loonfs_types::format::control::ForkBasis {
        manifest: loonfs_types::format::control::ManifestRef {
            owner_namespace_id: NamespaceId::parse("source").expect("valid namespace id"),
            manifest_no: ManifestNo(4),
            head_seq: ChangeSeq(5),
            payload_checksum: loonfs_types::sha256_digest(b"source"),
        },
        source_pin_id: PinId::parse("pin_00000000000000000004-0000000000000005").expect("pin id"),
    };
    for (manifest, expected) in [
        (
            NamespaceManifestPayload {
                acknowledged_floor_seq: ChangeSeq(1),
                ..created.clone()
            },
            "acknowledged floor `1` is above the retention floor `0`",
        ),
        (
            NamespaceManifestPayload {
                fork_basis: Some(fork_basis),
                head_seq: ChangeSeq(5),
                retention_floor_seq: ChangeSeq(5),
                acknowledged_floor_seq: ChangeSeq(4),
                ..created
            },
            "acknowledged floor",
        ),
    ] {
        let (_, bytes) = encode_namespace_manifest_json(manifest)
            .expect("encode the manifest")
            .into_parts();
        let error = load::decode_manifest_at(&namespace_id, ManifestNo(1), "manifest", &bytes)
            .expect_err("the manifest fails to load");
        assert_matches!(
            &error,
            ManifestLoadError::RunManifestMismatch { message, .. } if message.contains(expected),
            "{error}"
        );
    }
}

/// Reads the files one checkpoint pins at the test clock, or the error that
/// says it no longer pins anything.
async fn read_checkpoint_files<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    checkpoint_id: &PinId,
) -> crate::error::Result<Vec<InodeId>> {
    let page = crate::test_support::namespace_store(store, namespace_id)
        .list_checkpoint_files_page(
            checkpoint_id,
            loonfs_types::PageRequest {
                cursor: None,
                limit: EffectiveLimit::new(NonZeroU32::new(64).expect("nonzero")),
            },
            crate::pin::ListCheckpointFilesOptions::default(),
        )
        .await?;
    Ok(page.files.into_iter().map(|file| file.inode_id).collect())
}

#[tokio::test]
async fn deleting_a_pin_never_reuses_its_id() {
    // A deleted pin id is never reused. A caller asking for
    // a pin again — even at the same instant, over the same basis, under the
    // same owner name — gets a brand new record, so the delete can never be
    // undone by racing it.
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .write_file_bytes("/docs/file.txt", b"body\n", None)
        .await
        .expect("write");
    let first = create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("create checkpoint");
    assert!(
        !read_checkpoint_files(&store, &namespace_id, &first.checkpoint_id)
            .await
            .expect("the first pin serves")
            .is_empty()
    );

    let engine = crate::test_support::NamespaceFixture::new(
        &store,
        &namespace_id,
        &crate::test_support::writer_at("test-writer", 0),
    )
    .engine();
    let delete = engine.delete_snapshot(&first.checkpoint_id);
    let recreate = create_checkpoint(&store, &namespace_id, &context);
    let (delete, second) = tokio::join!(delete, recreate);
    assert_eq!(delete.expect("delete").snapshot_id, first.checkpoint_id);
    let second = second.expect("a concurrent create takes a fresh pin");
    assert_ne!(
        second.checkpoint_id, first.checkpoint_id,
        "a new pin never lands on a deleted record's key"
    );

    assert!(crate::test_support::namespace_store(&store, &namespace_id)
        .load_pin(&first.checkpoint_id)
        .await
        .expect("load pin")
        .is_none());
    let error = read_checkpoint_files(&store, &namespace_id, &first.checkpoint_id)
        .await
        .expect_err("a deleted record serves no read");
    assert_eq!(error.code(), ErrorCode::SnapshotNotFound);
    assert!(
        !read_checkpoint_files(&store, &namespace_id, &second.checkpoint_id)
            .await
            .expect("the new pin serves")
            .is_empty()
    );

    crate::test_support::NamespaceFixture::new(
        &store,
        &namespace_id,
        &crate::test_support::writer_at("test-writer", 0),
    )
    .engine()
    .delete_snapshot(&first.checkpoint_id)
    .await
    .expect("a second delete succeeds");
}

#[tokio::test]
async fn each_create_mints_its_own_record_and_carries_its_own_expiry() {
    // Re-creating a checkpoint is not a renewal of an earlier one. Every
    // call is its own pin with its own id, its own creation instant, and
    // exactly the expiry it asked for; earlier records are left alone.
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let owner = |expires_at_ms| loonfs_types::format::control::PinOwner::User {
        name: "test-pin".to_owned(),
        expires_at_ms,
    };
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .write_file_bytes("/docs/file.txt", b"body\n", None)
        .await
        .expect("write");

    let first = crate::test_support::namespace_store(&store, &namespace_id)
        .create_pin(owner(Some(10_000)), &context.clock())
        .await
        .map(|report| crate::pin::checkpoint_summary(report.response))
        .expect("create checkpoint");
    assert_eq!(first.expires_at_ms, Some(10_000));

    let mut later_context = test_context();
    later_context.now_ms = 2_000;
    let mut minted = BTreeSet::from([first.checkpoint_id.clone()]);
    for expiry in [Some(99_000), Some(5_000), None] {
        let next = crate::test_support::namespace_store(&store, &namespace_id)
            .create_pin(owner(expiry), &later_context.clock())
            .await
            .map(|report| crate::pin::checkpoint_summary(report.response))
            .expect("create checkpoint");
        assert!(
            minted.insert(next.checkpoint_id.clone()),
            "each pin gets an id of its own"
        );
        assert_eq!(next.expires_at_ms, expiry);
        let record = crate::test_support::namespace_store(&store, &namespace_id)
            .load_pin(&next.checkpoint_id)
            .await
            .expect("read pin")
            .expect("record exists")
            .state;
        assert_eq!(record.owner.expires_at_ms(), expiry);
        assert_eq!(record.created_at_ms, 2_000);
    }

    // The very first record is untouched by any of it: a pin taken without
    // an expiry, or with one, is held until something deletes it.
    let original = crate::test_support::namespace_store(&store, &namespace_id)
        .load_pin(&first.checkpoint_id)
        .await
        .expect("read pin")
        .expect("record exists")
        .state;
    assert_eq!(original.created_at_ms, 1_000, "creation instant is history");
    assert_eq!(original.owner.expires_at_ms(), Some(10_000));
}

#[tokio::test]
async fn an_expired_pin_serves_no_reads_but_stays_until_a_pass_deletes_it() {
    // A read refuses an expired pin, but the record is still a
    // garbage-collection root until a pass deletes it, so its state stays
    // in place.
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .write_file_bytes("/docs/file.txt", b"body\n", None)
        .await
        .expect("write");
    let already_expired = crate::test_support::namespace_store(&store, &namespace_id)
        .create_pin(
            loonfs_types::format::control::PinOwner::User {
                name: "test-pin".to_owned(),
                expires_at_ms: Some(context.now_ms),
            },
            &context.clock(),
        )
        .await
        .map(|report| crate::pin::checkpoint_summary(report.response))
        .expect("create checkpoint whose expiry has already passed");
    assert!(crate::test_support::namespace_store(&store, &namespace_id)
        .load_pin(&already_expired.checkpoint_id)
        .await
        .expect("load pin")
        .is_some());
    let error = read_checkpoint_files(&store, &namespace_id, &already_expired.checkpoint_id)
        .await
        .expect_err("an expired pin serves no read");
    assert_eq!(error.code(), ErrorCode::SnapshotNotFound);

    // The pass after the grace deletes the record.
    crate::test_support::namespace_store(&store, &namespace_id)
        .gc(
            None,
            &crate::gc::GcOptions::default(),
            &writer_at(
                "expired",
                context.now_ms + crate::gc::GcOptions::default().grace_window_ms,
            )
            .clock(),
        )
        .await
        .expect("gc pass");
    let error = read_checkpoint_files(&store, &namespace_id, &already_expired.checkpoint_id)
        .await
        .expect_err("a deleted pin serves nothing");
    assert_eq!(error.code(), ErrorCode::SnapshotNotFound);
    assert!(crate::test_support::namespace_store(&store, &namespace_id)
        .load_pin(&already_expired.checkpoint_id)
        .await
        .expect("load pin")
        .is_none());
}

#[tokio::test]
async fn a_pin_without_a_ttl_is_held_until_it_is_deleted() {
    // No expiry means no clock: the record stays a serving pin however far
    // the wall clock moves, and only an explicit delete ends it.
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .write_file_bytes("/docs/file.txt", b"body\n", None)
        .await
        .expect("write");
    let pin = create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("create checkpoint");
    assert_eq!(pin.expires_at_ms, None);

    let mut distant = test_context();
    distant.now_ms = u64::MAX / 2;
    for _ in 0..3 {
        crate::test_support::namespace_store(&store, &namespace_id)
            .gc(None, &crate::gc::GcOptions::default(), &distant.clock())
            .await
            .expect("gc pass");
    }
    crate::test_support::namespace_store(&store, &namespace_id)
        .load_pin(&pin.checkpoint_id)
        .await
        .expect("read pin")
        .expect("an unexpiring pin survives every pass");
    assert!(
        !read_checkpoint_files(&store, &namespace_id, &pin.checkpoint_id)
            .await
            .expect("the pin still serves")
            .is_empty()
    );

    crate::test_support::NamespaceFixture::new(
        &store,
        &namespace_id,
        &crate::test_support::writer_at("test-writer", 0),
    )
    .engine()
    .delete_snapshot(&pin.checkpoint_id)
    .await
    .expect("delete");
    let error = read_checkpoint_files(&store, &namespace_id, &pin.checkpoint_id)
        .await
        .expect_err("delete ends it");
    assert_eq!(error.code(), ErrorCode::SnapshotNotFound);
}

#[tokio::test]
async fn checkpoint_creation_deletes_its_pin_when_the_floor_passed_its_manifest() {
    let directory = tempdir().expect("tempdir");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");
    let initial = crate::test_support::namespace_store(&store, &namespace_id)
        .load_read_anchor(crate::test_support::observed_now())
        .await
        .expect("initial manifest");
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .write_file_bytes("/file", b"content", None)
        .await
        .expect("write");
    crate::test_support::namespace_store(&store, &namespace_id)
        .fold_wal()
        .await
        .expect("fold");
    crate::test_support::NamespaceFixture::new(
        &store,
        &namespace_id,
        &crate::test_support::writer_at(
            "test-writer",
            crate::test_support::grace_elapsed_after(context.now_ms),
        ),
    )
    .engine()
    .advance_retention_floor(RetentionTarget::Head)
    .await
    .expect("advance floor");
    let store = loonfs_test_support::stores::RecordingStore::new(
        store,
        loonfs_test_support::stores::KeyPredicate::prefix(pin_prefix(&namespace_id)),
    );
    let error = crate::test_support::namespace_store(&store, &namespace_id)
        .create_pin_at_basis(
            loonfs_types::format::control::PinOwner::User {
                name: "old".to_owned(),
                expires_at_ms: None,
            },
            initial.basis().manifest().clone(),
            &context.clock(),
        )
        .await
        .map(crate::pin::checkpoint_summary)
        .expect_err("floor passed pin");
    assert_eq!(error.code(), ErrorCode::CheckpointUnavailable);
    assert!(store
        .list_prefix(&pin_prefix(&namespace_id))
        .await
        .expect("pins")
        .is_empty());
    assert_eq!(store.counts().create_if_absent_puts, 1);
    assert_eq!(store.counts().deletes, 1);
}

#[tokio::test]
async fn pin_verification_rejects_a_deleted_namespace() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .write_file_bytes("/docs/file.txt", b"body\n", None)
        .await
        .expect("write");
    let checkpoint = create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("create checkpoint");
    let record = crate::test_support::namespace_store(&store, &namespace_id)
        .load_pin(&checkpoint.checkpoint_id)
        .await
        .expect("load checkpoint")
        .expect("checkpoint exists")
        .state;

    let acquired = crate::test_support::namespace_store(&store, &namespace_id)
        .acquire_writer_epoch(&context.writer_id)
        .await
        .expect("writer");
    crate::test_support::namespace_store(&store, &namespace_id)
        .delete_namespace(Default::default(), acquired, None, &context.clock())
        .await
        .expect("delete");

    let verified = crate::test_support::namespace_store(&store, &record.namespace_id)
        .verify_pin_basis(&record)
        .await
        .expect("verification runs");
    assert_eq!(
        verified,
        crate::pin::record::PinBasisVerification::Invalid,
        "a tombstoned namespace cannot acquire a new checkpoint dependency"
    );
}

#[tokio::test]
async fn pin_basis_verification_store_failure_deletes_the_record() {
    let directory = tempdir().expect("tempdir");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");
    let initial = crate::test_support::namespace_store(&store, &namespace_id)
        .load_read_anchor(crate::test_support::observed_now())
        .await
        .expect("initial manifest");
    let store = loonfs_test_support::stores::FailStore::new(
        store,
        loonfs_test_support::stores::KeyPredicate::exact(loonfs_objectstore::keys::hint(
            &namespace_id,
        )),
        loonfs_test_support::stores::OperationSelector::Read,
        loonfs_test_support::stores::InjectedError::Transport("verification failed".to_owned()),
    );
    store.fail_next(1);
    let error = crate::test_support::namespace_store(&store, &namespace_id)
        .create_pin_at_basis(
            loonfs_types::format::control::PinOwner::User {
                name: "failed".to_owned(),
                expires_at_ms: None,
            },
            initial.basis().manifest().clone(),
            &context.clock(),
        )
        .await
        .map(crate::pin::checkpoint_summary)
        .expect_err("verification failed");
    assert_eq!(error.code(), ErrorCode::ServerError);
    assert!(store
        .list_prefix(&pin_prefix(&namespace_id))
        .await
        .expect("pins")
        .is_empty());
}

async fn wal_object_count<S: ObjectStore>(store: &S, namespace_id: &NamespaceId) -> usize {
    store
        .list_prefix(&wal_prefix(namespace_id))
        .await
        .expect("list WAL objects")
        .len()
}

#[tokio::test]
async fn publish_backpressure_rejects_at_the_longest_tail_the_head_describes() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let hint_key = hint(&namespace_id);
    let wal_prefix = wal_prefix(&namespace_id);
    let store = RecordingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::new(move |key| key == hint_key || key.starts_with(&wal_prefix)),
    );
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");
    let boundary = usize::try_from(crate::limits::MAX_UNFOLDED_WAL_OBJECTS)
        .expect("the write-stop bound fits a WAL object count");

    let mut engine = NamespaceCommitEngine::new(
        crate::test_support::namespace_store(&store, &namespace_id),
        context.writer_id.clone(),
    );
    for round in 0..boundary - 2 {
        let request = CommitIntent::single(
            CommitId::generate(),
            loonfs_test_support::test_actor(),
            None,
            FilesystemOperation::CreateDirectory {
                path: AbsolutePath::parse(format!("/file-{round}")).expect("path"),
                parents: false,
            },
        );
        engine
            .publish_batch(vec![CommitCandidate::new(request)], &context.clock())
            .await
            .results
            .remove(0)
            .expect("write within backpressure window");
    }
    let request = CommitIntent::single(
        CommitId::parse("at-the-boundary").expect("commit id"),
        loonfs_test_support::test_actor(),
        None,
        FilesystemOperation::CreateDirectory {
            path: AbsolutePath::parse("/at-the-boundary").expect("path"),
            parents: false,
        },
    );
    let original = engine
        .publish_batch(
            vec![CommitCandidate::new(request.clone())],
            &context.clock(),
        )
        .await
        .results
        .pop()
        .expect("one result")
        .expect("the publish that lands exactly at the bound is still admitted");
    assert_eq!(wal_object_count(&store, &namespace_id).await, boundary + 1);
    store.reset();

    let replay = engine
        .publish_batch(
            vec![CommitCandidate::new(request.clone())],
            &context.clock(),
        )
        .await;
    assert_eq!(replay.results.len(), 1);
    assert_eq!(
        replay.results[0].as_ref().expect("replay at the bound"),
        &original
    );
    assert_eq!(
        replay.wal_tail.map(|tail| tail.objects),
        Some(crate::limits::MAX_UNFOLDED_WAL_OBJECTS)
    );
    assert_eq!(store.counts().puts, 0);
    assert_eq!(store.counts().compare_and_swaps, 0);

    let mut conflict = request.clone();
    conflict.operations = vec![FilesystemOperation::CreateDirectory {
        path: AbsolutePath::parse("/different").expect("path"),
        parents: false,
    }];
    let mut new_request = conflict.clone();
    new_request.commit_id = CommitId::parse("one-too-many").expect("commit id");
    for (candidate, expected_code) in [
        (conflict, ErrorCode::CommitIdReuseConflict),
        (new_request.clone(), ErrorCode::MaintenanceRequired),
    ] {
        let result = engine
            .publish_batch(vec![CommitCandidate::new(candidate)], &context.clock())
            .await;
        assert_eq!(result.results.len(), 1);
        assert_eq!(
            result.results[0]
                .as_ref()
                .expect_err("refused request")
                .code(),
            expected_code
        );
    }
    let mixed = engine
        .publish_batch(
            vec![
                CommitCandidate::new(request),
                CommitCandidate::new(new_request.clone()),
                CommitCandidate::new(new_request),
            ],
            &context.clock(),
        )
        .await;
    assert_eq!(mixed.results.len(), 3);
    assert_eq!(
        mixed.results[0].as_ref().expect("mixed batch replay"),
        &original
    );
    for result in &mixed.results[1..] {
        assert_eq!(
            result
                .as_ref()
                .expect_err("new primary and alias refused")
                .code(),
            ErrorCode::MaintenanceRequired
        );
    }
    assert_eq!(
        mixed.wal_tail.map(|tail| tail.objects),
        Some(crate::limits::MAX_UNFOLDED_WAL_OBJECTS)
    );
    assert_eq!(
        store.counts().puts,
        0,
        "no WAL put or head write at the bound"
    );
    assert_eq!(
        store.counts().compare_and_swaps,
        0,
        "no head swap at the bound"
    );

    // Reads never gate: the change feed still serves the whole tail.
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let view = namespace
        .load_current_metadata_view()
        .await
        .expect("load feed view");
    let changes = list_changes_after(
        &view,
        ChangeSeq(0),
        EffectiveLimit::new(NonZeroU32::new(200).expect("nonzero")),
    )
    .await
    .expect("list changes");
    assert_eq!(
        changes.head_seq,
        ChangeSeq(u64::try_from(boundary - 1).expect("data WAL objects"))
    );
}

#[tokio::test]
async fn checkpoints_append_past_the_threshold_and_compaction_drains() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    let policy = MetadataRunPolicy {
        max_delta_runs: NonZeroUsize::new(2).expect("test run limit should be nonzero"),
        ..MetadataRunPolicy::default()
    };
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");

    let rounds = DEFAULT_MAX_DELTA_RUNS.get() + 2;
    for index in 1..=u64::try_from(rounds).expect("round count fits") {
        write_file_and_checkpoint(&store, &namespace_id, &context, index).await;
    }

    let appended = load_manifest_materialization_for_inspection(
        &store,
        &namespace_id,
        current_manifest_no(&store, &namespace_id).await,
    )
    .await
    .expect("load appended manifest");
    assert_eq!(delta_runs(&appended.manifest).len(), rounds);
    assert!(delta_runs(&appended.manifest).len() > DEFAULT_MAX_DELTA_RUNS.get());
    assert_eq!(appended.manifest.payload().base_seq(), ChangeSeq(0));

    // Compaction merges one family group per unit, each publishing its
    // own manifest — the manifest chain is the progress record.
    let mut units = 0usize;
    let mut last_manifest_no = None;
    loop {
        let report = {
            let mut fixture = crate::test_support::NamespaceFixture::new(
                &store,
                &namespace_id,
                &crate::test_support::writer_at("test-writer", 0),
            );
            fixture.namespace.run_policy = policy;
            fixture
        }
        .engine()
        .compact_metadata_unit(
            None,
            MetadataCompactionPolicy::default(),
            crate::compaction::CompactionAdmission::AnySize,
            loonfs_types::CompactorEpoch(0),
            &crate::compaction::MetadataCompactionCancellation::default(),
        )
        .await
        .expect("compaction step");
        match report {
            super::CompactionUnitOutcome::Published { manifest, .. } => {
                let manifest_no = manifest.manifest.manifest_no;
                units += 1;
                if let Some(previous) = last_manifest_no {
                    assert!(manifest_no > previous, "units must advance the manifest");
                }
                last_manifest_no = Some(manifest_no);
            }
            super::CompactionUnitOutcome::NotNeeded => break,
            other => panic!("no concurrent publisher exists in this test, got {other:?}"),
        }
    }
    assert!(
        units >= 2,
        "several family groups should merge, got {units}"
    );

    let drained = load_manifest_materialization_for_inspection(
        &store,
        &namespace_id,
        current_manifest_no(&store, &namespace_id).await,
    )
    .await
    .expect("load drained manifest");
    let materialization_after = load_current_projection(&store, &namespace_id)
        .await
        .expect("materialization");
    assert!(delta_runs(&drained.manifest).is_empty());
    assert_eq!(
        drained.manifest.payload().base_seq(),
        ChangeSeq(u64::try_from(rounds).expect("round count fits"))
    );
    assert!(metadata_states_equivalent(
        &materialization_after.metadata_state,
        &drained.metadata_state
    ));
}

/// Checks that a unit merges at most its run budget, that a unit within the
/// row budget plans from descriptors and reads no segment when its window is
/// too large, and that it reads only the selected runs' segments when it
/// merges.
#[tokio::test]
async fn a_unit_honors_its_run_budget_and_admits_rows_by_descriptor() {
    let temp_dir = tempdir().expect("tempdir");
    let store =
        RecordingStore::metadata_segments(LocalFsStore::new(temp_dir.path()).expect("store"));
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");
    for index in 1..=5 {
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
            .write_file_bytes(&format!("/docs/file-{index}.txt"), b"body\n", None)
            .await
            .expect("write file");
        create_checkpoint(&store, &namespace_id, &context)
            .await
            .expect("create checkpoint");
    }

    let manifest_before = current_manifest_no(&store, &namespace_id).await;
    let tiny_row_policy = MetadataRunPolicy {
        max_delta_runs: NonZeroUsize::MIN,
        max_input_runs_per_step: NonZeroUsize::new(2).expect("test run budget should be nonzero"),
        max_decoded_input_rows_per_step: NonZeroUsize::MIN,
        ..MetadataRunPolicy::default()
    };
    store.reset();
    let blocked = {
        let mut fixture = crate::test_support::NamespaceFixture::new(
            &store,
            &namespace_id,
            &crate::test_support::writer_at("test-writer", 0),
        );
        fixture.namespace.run_policy = tiny_row_policy;
        fixture
    }
    .engine()
    .compact_metadata_unit(
        None,
        MetadataCompactionPolicy::default(),
        crate::compaction::CompactionAdmission::WithinRowBudget,
        loonfs_types::CompactorEpoch(0),
        &crate::compaction::MetadataCompactionCancellation::default(),
    )
    .await
    .expect("budgeted unit");
    assert_matches!(
        blocked,
        super::CompactionUnitOutcome::TooLarge { .. },
        "one row must not admit a window"
    );
    assert_eq!(
        current_manifest_no(&store, &namespace_id).await,
        manifest_before,
        "a unit whose window is too large must not publish"
    );
    assert_eq!(store.count(OperationSelector::Put), 0);
    assert_eq!(
        store.count(OperationSelector::Read),
        0,
        "planning reads only descriptors"
    );

    let policy = MetadataRunPolicy {
        max_delta_runs: NonZeroUsize::MIN,
        max_input_runs_per_step: NonZeroUsize::new(2).expect("test run budget should be nonzero"),
        max_decoded_input_rows_per_step: NonZeroUsize::new(10)
            .expect("test row budget should be nonzero"),
        ..MetadataRunPolicy::default()
    };
    store.reset();
    let published = {
        let mut fixture = crate::test_support::NamespaceFixture::new(
            &store,
            &namespace_id,
            &crate::test_support::writer_at("test-writer", 0),
        );
        fixture.namespace.run_policy = policy;
        fixture
    }
    .engine()
    .compact_metadata_unit(
        None,
        MetadataCompactionPolicy::default(),
        crate::compaction::CompactionAdmission::WithinRowBudget,
        loonfs_types::CompactorEpoch(0),
        &crate::compaction::MetadataCompactionCancellation::default(),
    )
    .await
    .expect("admitted unit");
    let super::CompactionUnitOutcome::Published {
        group,
        input_runs,
        rows_read,
        ..
    } = published
    else {
        panic!("two oldest runs should fit the test budgets");
    };
    assert_eq!(input_runs, 2);
    assert!(input_runs <= policy.max_input_runs_per_step.get());
    assert!(rows_read <= policy.max_decoded_input_rows_per_step.get() as u64);
    // The basis manifest's acknowledged floor is 0, so the unit builds no
    // kept inode filter and reads none of the basis's slot index for one.
    let reads = store.count(OperationSelector::Read);
    // The namespace's floor is still 0, so the base unit builds no kept inode
    // filter and walks nothing.
    assert!(
        reads <= input_runs * group.families().len() * 2,
        "the unit should read index and data sections only for selected-run family segments: \
         {reads} reads"
    );
}

#[tokio::test]
async fn bounded_compaction_reaches_the_unbounded_state_and_preserves_intermediate_reads() {
    let bounded_dir = tempdir().expect("bounded tempdir");
    let unbounded_dir = tempdir().expect("unbounded tempdir");
    let bounded_store = LocalFsStore::new(bounded_dir.path()).expect("bounded store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&bounded_store, &namespace_id, &context)
        .await
        .expect("bootstrap");
    for index in 1..=6 {
        let commit_id = CommitId::parse(format!("bounded-convergence-{index}")).expect("commit id");
        crate::test_support::NamespaceFixture::new(&bounded_store, &namespace_id, &context)
            .write_file_bytes(
                &format!("/docs/file-{index}.txt"),
                format!("file {index}\n").as_bytes(),
                Some(&commit_id),
            )
            .await
            .expect("write file");
        create_checkpoint(&bounded_store, &namespace_id, &context)
            .await
            .expect("create checkpoint");
    }
    // The two compactions start from the same objects, so their results have
    // the same inode IDs.
    super::copy_directory(bounded_dir.path(), unbounded_dir.path());
    let unbounded_store = LocalFsStore::new(unbounded_dir.path()).expect("unbounded store");

    let visible_before = load_current_projection(&bounded_store, &namespace_id)
        .await
        .expect("visible state before bounded compaction");
    let unlimited = NonZeroUsize::new(usize::MAX).expect("usize max should be nonzero");
    let bounded_policy = MetadataRunPolicy {
        max_delta_runs: NonZeroUsize::new(4).expect("test trigger should be nonzero"),
        max_input_runs_per_step: NonZeroUsize::new(2).expect("test run budget should be nonzero"),
        max_decoded_input_rows_per_step: unlimited,
        max_decoded_input_bytes_per_step: unlimited,
        ..MetadataRunPolicy::default()
    };
    let first = {
        let mut fixture = crate::test_support::NamespaceFixture::new(
            &bounded_store,
            &namespace_id,
            &crate::test_support::writer_at("test-writer", 0),
        );
        fixture.namespace.run_policy = bounded_policy;
        fixture
    }
    .engine()
    .compact_metadata_unit(
        None,
        MetadataCompactionPolicy::default(),
        crate::compaction::CompactionAdmission::AnySize,
        loonfs_types::CompactorEpoch(0),
        &crate::compaction::MetadataCompactionCancellation::default(),
    )
    .await
    .expect("first compaction unit");
    assert_matches!(
        first,
        super::CompactionUnitOutcome::Published { input_runs: 2, .. }
    );
    let visible_between = load_current_projection(&bounded_store, &namespace_id)
        .await
        .expect("visible state between compaction units");
    assert!(metadata_states_equivalent(
        &visible_before.metadata_state,
        &visible_between.metadata_state
    ));

    let (bounded_manifest_no, bounded_steps_after_first) =
        drain_compaction(&bounded_store, &namespace_id, bounded_policy).await;
    let unbounded_policy = MetadataRunPolicy {
        max_delta_runs: NonZeroUsize::new(4).expect("test trigger should be nonzero"),
        max_input_runs_per_step: unlimited,
        max_decoded_input_rows_per_step: unlimited,
        max_decoded_input_bytes_per_step: unlimited,
        ..MetadataRunPolicy::default()
    };
    let (unbounded_manifest_no, unbounded_steps) =
        drain_compaction(&unbounded_store, &namespace_id, unbounded_policy).await;
    assert!(
        bounded_steps_after_first < 128,
        "compaction did not converge"
    );
    assert!(unbounded_steps < 128, "compaction did not converge");
    assert!(bounded_steps_after_first + 1 > unbounded_steps);

    let bounded = load_manifest_materialization_for_inspection(
        &bounded_store,
        &namespace_id,
        bounded_manifest_no,
    )
    .await
    .expect("load bounded result");
    let unbounded = load_manifest_materialization_for_inspection(
        &unbounded_store,
        &namespace_id,
        unbounded_manifest_no,
    )
    .await
    .expect("load unbounded result");
    assert!(metadata_states_equivalent_ignoring_content_identity(
        &bounded.metadata_state,
        &unbounded.metadata_state
    ));
    // One unbounded unit merges each group's tier of six runs whole. Units of
    // two runs merge the tier's oldest runs into the base until the runs left
    // are fewer than a tier, and those wait for later folds.
    assert!(delta_runs(&unbounded.manifest).is_empty());
    assert_eq!(
        delta_runs(&bounded.manifest)
            .iter()
            .map(|run| run.run_seq)
            .collect::<Vec<_>>(),
        [ChangeSeq(5), ChangeSeq(6)]
    );

    let later_commit = CommitId::parse("bounded-convergence-later").expect("commit id");
    crate::test_support::NamespaceFixture::new(&unbounded_store, &namespace_id, &context)
        .write_file_bytes("/docs/later.txt", b"later\n", Some(&later_commit))
        .await
        .expect("write later file");
    create_checkpoint(&unbounded_store, &namespace_id, &context)
        .await
        .expect("checkpoint later file");
    let below_trigger = {
        let mut fixture = crate::test_support::NamespaceFixture::new(
            &unbounded_store,
            &namespace_id,
            &crate::test_support::writer_at("test-writer", 0),
        );
        fixture.namespace.run_policy = unbounded_policy;
        fixture
    }
    .engine()
    .compact_metadata_unit(
        None,
        MetadataCompactionPolicy::default(),
        crate::compaction::CompactionAdmission::AnySize,
        loonfs_types::CompactorEpoch(0),
        &crate::compaction::MetadataCompactionCancellation::default(),
    )
    .await
    .expect("below-trigger step");
    assert_matches!(below_trigger, super::CompactionUnitOutcome::NotNeeded);
}

#[tokio::test]
async fn whole_run_compaction_rewrites_base_segments() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    let policy = MetadataRunPolicy {
        max_delta_runs: NonZeroUsize::MIN,
        max_rows_per_segment: NonZeroUsize::new(2)
            .expect("test segment row budget should be nonzero"),
        ..MetadataRunPolicy::default()
    };
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .write_file_bytes("/hot/original.txt", b"hot\n", None)
        .await
        .expect("write hot");
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .write_file_bytes("/cold/original.txt", b"cold\n", None)
        .await
        .expect("write cold");

    let first_manifest_no = checkpoint_then_compact(&store, &namespace_id, &context, policy).await;
    let first_materialized =
        load_manifest_materialization_for_inspection(&store, &namespace_id, first_manifest_no)
            .await
            .expect("load first manifest");
    let first_run_keys = run_segment_object_keys(&first_materialized.manifest);

    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .write_file_bytes("/hot/after-delta.txt", b"hot 2\n", None)
        .await
        .expect("write hot delta");
    checkpoint_then_compact(&store, &namespace_id, &context, policy).await;
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .write_file_bytes("/hot/after-compact.txt", b"hot 3\n", None)
        .await
        .expect("write hot compact");
    let compacted = create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("compacted checkpoint");
    let compacted_manifest_no = drain_compaction(
        &store,
        &namespace_id,
        MetadataRunPolicy {
            max_delta_runs: NonZeroUsize::MIN,
            ..policy
        },
    )
    .await
    .0;
    let compacted_materialized =
        load_manifest_materialization_for_inspection(&store, &namespace_id, compacted_manifest_no)
            .await
            .expect("load compacted manifest");
    let materialization_after = load_current_projection(&store, &namespace_id)
        .await
        .expect("materialization");
    let compacted_run_keys = run_segment_object_keys(&compacted_materialized.manifest);
    let compacted_run_prefix = format!("namespaces/{}/segments/seg_", namespace_id.as_str());

    assert_eq!(
        compacted_materialized.manifest.payload().base_seq(),
        compacted.captured_seq
    );
    assert!(delta_runs(&compacted_materialized.manifest).is_empty());
    // Every family group rebuilds on its own and takes a run number of its
    // own, so a fully merged manifest holds one base run per group that has
    // rows, and nothing else.
    for (families, base_runs) in base_runs_per_family_group(&compacted_materialized.manifest) {
        assert!(
            base_runs.len() <= 1,
            "{families:?} holds base runs {base_runs:?} after a full merge"
        );
    }
    assert!(!compacted_run_keys.is_empty());
    assert!(compacted_run_keys
        .iter()
        .all(|key| key.starts_with(&compacted_run_prefix)));
    assert!(compacted_run_keys
        .iter()
        .all(|key| !first_run_keys.contains(key)));
    assert_manifest_rows_have_unique_keys(&compacted_materialized.metadata_state);
    assert!(metadata_states_equivalent(
        &materialization_after.metadata_state,
        &compacted_materialized.metadata_state
    ));
}

#[tokio::test]
async fn whole_run_compaction_resegments_row_key_range_families_with_delta_runs() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    let policy = MetadataRunPolicy {
        max_delta_runs: NonZeroUsize::MIN,
        max_rows_per_segment: NonZeroUsize::new(2)
            .expect("test segment row budget should be nonzero"),
        ..MetadataRunPolicy::default()
    };
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");
    for index in 0..6 {
        let path = format!("/docs/file-{index}.txt");
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
            .write_file_bytes(&path, b"initial\n", None)
            .await
            .expect("write initial file");
    }

    let first_manifest_no = checkpoint_then_compact(&store, &namespace_id, &context, policy).await;
    let first_materialized =
        load_manifest_materialization_for_inspection(&store, &namespace_id, first_manifest_no)
            .await
            .expect("load first manifest");
    let revision_keys_before = base_segment_object_keys_for_family(
        &first_materialized.manifest,
        ApiMetadataRowFamily::Revisions,
    );
    assert!(revision_keys_before.len() >= 3);

    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .write_file_bytes("/docs/file-0.txt", b"delta\n", None)
        .await
        .expect("write delta revision");
    checkpoint_then_compact(&store, &namespace_id, &context, policy).await;
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .write_file_bytes("/docs/file-0.txt", b"compact\n", None)
        .await
        .expect("write compaction revision");
    let _compacted = create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("compacted checkpoint");
    let compacted_manifest_no = drain_compaction(
        &store,
        &namespace_id,
        MetadataRunPolicy {
            max_delta_runs: NonZeroUsize::MIN,
            ..policy
        },
    )
    .await
    .0;
    let compacted_materialized =
        load_manifest_materialization_for_inspection(&store, &namespace_id, compacted_manifest_no)
            .await
            .expect("load compacted manifest");
    let revision_keys_after = base_segment_object_keys_for_family(
        &compacted_materialized.manifest,
        ApiMetadataRowFamily::Revisions,
    );
    let materialization_after = load_current_projection(&store, &namespace_id)
        .await
        .expect("materialization");

    assert!(delta_runs(&compacted_materialized.manifest).is_empty());
    // Groups untouched since the first fold keep their older base run, so
    // several base runs may coexist; what matters is that no delta remains.
    assert!(runs_newest_first(compacted_materialized.manifest.payload())
        .iter()
        .all(|run| run.tier == RunTier::Base));
    assert!(revision_keys_after
        .iter()
        .all(|key| !revision_keys_before.contains(key)));
    assert!(metadata_states_equivalent(
        &materialization_after.metadata_state,
        &compacted_materialized.metadata_state
    ));
    assert_manifest_rows_have_unique_keys(&compacted_materialized.metadata_state);
}

#[tokio::test]
async fn compaction_resumes_from_the_manifest_after_interruption() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    let policy = MetadataRunPolicy {
        max_delta_runs: NonZeroUsize::MIN,
        ..MetadataRunPolicy::default()
    };
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");
    for index in 1..=3 {
        write_file_and_checkpoint(&store, &namespace_id, &context, index).await;
    }

    // One unit runs, then the process "crashes": nothing is carried over
    // but the published manifest.
    let first_report = {
        let mut fixture = crate::test_support::NamespaceFixture::new(
            &store,
            &namespace_id,
            &crate::test_support::writer_at("test-writer", 0),
        );
        fixture.namespace.run_policy = policy;
        fixture
    }
    .engine()
    .compact_metadata_unit(
        None,
        MetadataCompactionPolicy::default(),
        crate::compaction::CompactionAdmission::AnySize,
        loonfs_types::CompactorEpoch(0),
        &crate::compaction::MetadataCompactionCancellation::default(),
    )
    .await
    .expect("first unit");
    let super::CompactionUnitOutcome::Published {
        group: first_group, ..
    } = first_report
    else {
        panic!("expected a published unit, got {:?}", first_report);
    };

    // A checkpoint lands in between, adding a fresh delta run.
    write_file_and_checkpoint(&store, &namespace_id, &context, 9).await;

    // A fresh sequence of steps reads the live manifest and finishes the
    // job, including the delta the interleaved checkpoint added.
    let mut merged_groups = vec![first_group];
    loop {
        let report = {
            let mut fixture = crate::test_support::NamespaceFixture::new(
                &store,
                &namespace_id,
                &crate::test_support::writer_at("test-writer", 0),
            );
            fixture.namespace.run_policy = policy;
            fixture
        }
        .engine()
        .compact_metadata_unit(
            None,
            MetadataCompactionPolicy::default(),
            crate::compaction::CompactionAdmission::AnySize,
            loonfs_types::CompactorEpoch(0),
            &crate::compaction::MetadataCompactionCancellation::default(),
        )
        .await
        .expect("resumed unit");
        match report {
            super::CompactionUnitOutcome::Published { group, .. } => {
                merged_groups.push(group);
            }
            super::CompactionUnitOutcome::NotNeeded => break,
            other => panic!("no concurrent publisher exists in this test, got {other:?}"),
        }
    }
    assert!(merged_groups.len() >= 2);

    let drained = load_manifest_materialization_for_inspection(
        &store,
        &namespace_id,
        current_manifest_no(&store, &namespace_id).await,
    )
    .await
    .expect("load drained manifest");
    let materialization_after = load_current_projection(&store, &namespace_id)
        .await
        .expect("materialization");
    assert!(delta_runs(&drained.manifest).is_empty());
    assert!(metadata_states_equivalent(
        &materialization_after.metadata_state,
        &drained.metadata_state
    ));
}

#[tokio::test]
async fn over_budget_wal_fold_aborts_without_publishing() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = WriterAt {
        writer_id: loonfs_types::WriterId::parse("budget-test").expect("writer id"),
        now_ms: 1_000,
    };
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .put_file(
            "/docs/budget.txt",
            b"body",
            DestinationBehavior::NoReplace,
            None,
        )
        .await
        .expect("seed file");
    let manifest_before = crate::test_support::namespace_store(&store, &namespace_id)
        .load_current_manifest()
        .await
        .expect("read manifest");

    // The clock advances 20 minutes, past the 15-minute budget, while the
    // fold writes its segments, so the pre-CAS check observes the
    // publication as over budget.
    let clock = Arc::new(loonfs_test_support::clock::ManualClock::new(0));
    let advancing = Arc::clone(&clock);
    let store = loonfs_test_support::stores::MetadataMapStore::new(
        store,
        KeyPredicate::prefix(loonfs_objectstore::keys::metadata_segment_prefix(
            &namespace_id,
        )),
        move |metadata| {
            advancing.advance_ms(20 * 60 * 1000);
            metadata
        },
    );
    let overrun = OperationClock::start(0, clock);
    let Err(error) = crate::test_support::namespace_store(&store, &namespace_id)
        .fold_wal_tail(None, &overrun)
        .await
    else {
        panic!("over-budget publication must abort");
    };
    assert_matches!(
        error,
        CoreError::MetadataPublicationBudgetExceeded { .. },
        "expected budget error, got {error:?}"
    );

    let manifest_after = crate::test_support::namespace_store(&store, &namespace_id)
        .load_current_manifest()
        .await
        .expect("read manifest");
    assert_eq!(
        manifest_after, manifest_before,
        "an aborted publication must not publish a manifest"
    );

    // The in-budget retry publishes normally over fresh outputs.
    let advanced = crate::test_support::namespace_store(&store, &namespace_id)
        .fold_wal()
        .await
        .expect("in-budget retry succeeds");
    assert_eq!(advanced.outcome, crate::FoldWalOutcome::Published);
    assert!(advanced.manifest_no > manifest_before.manifest().manifest_no);
}

/// Every timer reading advances past the revalidation bound, so each check
/// finds its observation stale and loads the manifest again.
#[tokio::test]
async fn a_read_anchor_that_goes_stale_on_every_check_stops_at_the_contention_limit() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    bootstrap_namespace(&store, &namespace_id, &test_context())
        .await
        .expect("bootstrap");
    let stale = crate::time::Observation::now(Arc::new(ManualClock::stepping(
        crate::limits::READ_REVALIDATION_BOUND_MS + 1,
    )));

    let error = crate::test_support::namespace_store(&store, &namespace_id)
        .load_read_anchor(stale)
        .await
        .expect_err("a check that never confirms its manifest stops");

    let CoreError::ControlObjectLoad(
        crate::control_object::ControlObjectLoadError::StoreContention { object_key },
    ) = &error
    else {
        panic!("expected StoreContention, got {error:?}");
    };
    assert!(
        object_key.starts_with(&metadata_manifest_prefix(&namespace_id)),
        "{object_key}"
    );
    assert_eq!(error.code(), ErrorCode::StoreContention);
}

#[tokio::test]
async fn over_budget_compaction_aborts_without_publishing() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = WriterAt {
        writer_id: loonfs_types::WriterId::parse("budget-test").expect("writer id"),
        now_ms: 1_000,
    };
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .put_file(
            "/docs/merge.txt",
            b"body",
            DestinationBehavior::NoReplace,
            None,
        )
        .await
        .expect("seed file");
    crate::test_support::namespace_store(&store, &namespace_id)
        .fold_wal()
        .await
        .expect("publish a delta run to merge");
    let manifest_before = crate::test_support::namespace_store(&store, &namespace_id)
        .load_current_manifest()
        .await
        .expect("read manifest");

    let merge_everything = MetadataRunPolicy {
        max_delta_runs: NonZeroUsize::MIN,
        ..Default::default()
    };
    let error = {
        let mut fixture = crate::test_support::NamespaceFixture::new(
            &store,
            &namespace_id,
            &crate::test_support::writer_at("test-writer", 0),
        );
        fixture.namespace.run_policy = merge_everything;
        fixture.namespace.timer = Arc::new(ManualClock::stepping(20 * 60 * 1000));
        fixture
    }
    .engine()
    .compact_metadata_unit(
        None,
        MetadataCompactionPolicy::default(),
        crate::compaction::CompactionAdmission::AnySize,
        loonfs_types::CompactorEpoch(0),
        &crate::compaction::MetadataCompactionCancellation::default(),
    )
    .await
    .expect_err("over-budget compaction must abort");
    assert_matches!(
        error,
        CoreError::MetadataPublicationBudgetExceeded { .. },
        "expected budget error, got {error:?}"
    );

    let manifest_after = crate::test_support::namespace_store(&store, &namespace_id)
        .load_current_manifest()
        .await
        .expect("read manifest");
    assert_eq!(manifest_after, manifest_before);

    let report = {
        let mut fixture = crate::test_support::NamespaceFixture::new(
            &store,
            &namespace_id,
            &crate::test_support::writer_at("test-writer", 0),
        );
        fixture.namespace.run_policy = merge_everything;
        fixture
    }
    .engine()
    .compact_metadata_unit(
        None,
        MetadataCompactionPolicy::default(),
        crate::compaction::CompactionAdmission::AnySize,
        loonfs_types::CompactorEpoch(0),
        &crate::compaction::MetadataCompactionCancellation::default(),
    )
    .await
    .expect("in-budget retry merges the unit");
    assert_matches!(report, super::CompactionUnitOutcome::Published { .. });
}

/// Plans the unit a compaction would run, with the same family group and
/// policy, and returns the group beside the plan.
async fn plan_window<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    policy: MetadataRunPolicy,
    requested_group: Option<MetadataFamilyGroup>,
) -> (MetadataFamilyGroup, MetadataCompactionSpec) {
    let manifest_number = current_manifest_number(store, namespace_id).await;
    let segments =
        load_manifest_segments_for_inspection(store, None, namespace_id, &manifest_number)
            .await
            .expect("load manifest segments");
    let group = requested_group.unwrap_or_else(|| {
        super::plan::select_family_group(
            segments.scan_runs.as_ref(),
            segments.manifest().payload(),
            MetadataCompactionPolicy::default(),
            policy,
        )
        .expect("a family group with delta rows to merge")
    });
    let spec = super::plan::plan_group(
        segments.manifest().payload(),
        segments.scan_runs.as_ref(),
        group,
        policy,
        MetadataCompactionPolicy::default(),
    )
    .expect("plan the group");
    (group, spec)
}

/// Runs one compaction unit with compactor epoch 0 and the given admission.
async fn compact_with<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    policy: MetadataRunPolicy,
    admission: crate::compaction::CompactionAdmission,
) -> super::CompactionUnitOutcome {
    {
        let mut fixture = crate::test_support::NamespaceFixture::new(
            store,
            namespace_id,
            &crate::test_support::writer_at("test-writer", 0),
        );
        fixture.namespace.run_policy = policy;
        fixture
    }
    .engine()
    .compact_metadata_unit(
        None,
        MetadataCompactionPolicy::default(),
        admission,
        loonfs_types::CompactorEpoch(0),
        &crate::compaction::MetadataCompactionCancellation::default(),
    )
    .await
    .expect("run a compaction unit")
}

/// Rows one run holds in one family group: the quantity the per-step row
/// budget is measured against.
fn group_run_rows(segments: &[MetadataFamilySegments], group: &[ApiMetadataRowFamily]) -> u64 {
    segments
        .iter()
        .filter(|family_segments| group.contains(&family_segments.family))
        .flat_map(|family_segments| &family_segments.segments)
        .map(|descriptor| descriptor.row_count)
        .sum()
}

fn group_segment_object_keys(
    segments: &[MetadataFamilySegments],
    group: &[ApiMetadataRowFamily],
) -> Vec<String> {
    segments
        .iter()
        .filter(|family_segments| group.contains(&family_segments.family))
        .flat_map(|family_segments| &family_segments.segments)
        .map(metadata_segment_object_key)
        .collect()
}

/// A per-step row budget one row short of what the group's base run needs,
/// with the trigger forced so a single delta run still merges.
fn policy_that_cannot_merge_the_base(base_rows: u64) -> MetadataRunPolicy {
    let row_budget = usize::try_from(base_rows).expect("test row counts are small") - 1;
    MetadataRunPolicy {
        max_delta_runs: NonZeroUsize::MIN,
        max_decoded_input_rows_per_step: NonZeroUsize::new(row_budget)
            .expect("test row budget should be nonzero"),
        ..MetadataRunPolicy::default()
    }
}

/// Checks that a group whose base is over the row budget is too large for a
/// unit within the row budget, and that a unit that admits any size rebuilds
/// the whole group, base included, into one base run.
#[tokio::test]
async fn a_base_over_the_row_budget_is_rebuilt_whole_by_a_unit_of_any_size() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");

    // A compacted base first, then delta runs stacked above it.
    for index in 1..=4 {
        write_file_and_checkpoint(&store, &namespace_id, &context, index).await;
    }
    drain_compaction(
        &store,
        &namespace_id,
        MetadataRunPolicy {
            max_delta_runs: NonZeroUsize::MIN,
            ..MetadataRunPolicy::default()
        },
    )
    .await;
    for index in 5..=8 {
        write_file_and_checkpoint(&store, &namespace_id, &context, index).await;
    }

    let before = load_manifest_materialization_for_inspection(
        &store,
        &namespace_id,
        current_manifest_no(&store, &namespace_id).await,
    )
    .await
    .expect("load the parked manifest");
    let (group, _) = plan_window(&store, &namespace_id, MetadataRunPolicy::default(), None).await;
    let base = base_tier(&before.manifest);
    let base_rows = group_run_rows(&base, group.families());
    let base_segments = group_segment_object_keys(&base, group.families());
    assert!(base_rows > 1, "the base run must hold rows in this group");
    let policy = policy_that_cannot_merge_the_base(base_rows);

    let (_, spec) = plan_window(&store, &namespace_id, policy, None).await;
    assert_eq!(
        spec.input_runs(),
        group_runs(&before.manifest, group.families()).len(),
        "comparable input sizes warrant a base rewrite"
    );
    assert_eq!(
        compact_with(
            &store,
            &namespace_id,
            policy,
            crate::compaction::CompactionAdmission::WithinRowBudget
        )
        .await,
        super::CompactionUnitOutcome::TooLarge { group }
    );
    assert_matches!(
        compact_with(
            &store,
            &namespace_id,
            policy,
            crate::compaction::CompactionAdmission::AnySize
        )
        .await,
        super::CompactionUnitOutcome::Published { group: merged, .. } if merged == group
    );

    let after = load_manifest_materialization_for_inspection(
        &store,
        &namespace_id,
        current_manifest_no(&store, &namespace_id).await,
    )
    .await
    .expect("load the drained manifest");
    // The unit rebuilt the group, so it is one base run and the frozen base's
    // segments are gone. A run nothing rewrites is a run nothing reclaims.
    assert_eq!(
        group_runs(&after.manifest, group.families()).len(),
        1,
        "the group must end in one run"
    );
    assert_eq!(group_base_runs(&after.manifest, group.families()).len(), 1);
    let remaining = run_segment_object_keys(&after.manifest);
    for segment_key in &base_segments {
        assert!(
            !remaining.contains(segment_key),
            "the frozen base's segments must be replaced by the rebuilt run"
        );
    }
    let projection = load_current_projection(&store, &namespace_id)
        .await
        .expect("materialization");
    assert!(metadata_states_equivalent(
        &projection.metadata_state,
        &after.metadata_state
    ));
}

#[tokio::test]
async fn a_merge_above_the_base_keeps_the_rows_that_shadow_it() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");

    // The base run holds both bindings.
    for name in ["keep", "gone"] {
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
            .write_file_bytes(&format!("/docs/{name}.txt"), b"body\n", None)
            .await
            .expect("seed file");
    }
    // A base far larger than the delta runs, written 32 files per commit.
    for batch in 0..16 {
        let paths: Vec<_> = (batch * 32..(batch + 1) * 32)
            .map(|index| format!("/docs/retained-{index}.txt"))
            .collect();
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
            .write_files_bytes(&paths, b"body")
            .await
            .expect("grow the base");
    }
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("checkpoint the seed");
    drain_compaction(
        &store,
        &namespace_id,
        MetadataRunPolicy {
            max_delta_runs: NonZeroUsize::MIN,
            ..MetadataRunPolicy::default()
        },
    )
    .await;

    // One delta run cancels a binding the base holds, more delta runs land
    // above it until they make a tier, and the floor moves past the
    // cancellation.
    let deleted = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .delete_path("/docs/gone.txt", None)
        .await
        .expect("delete");
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("checkpoint the delete");
    for extra in 1..MetadataRunPolicy::default().min_compaction_runs.get() {
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
            .write_file_bytes(&format!("/docs/extra-{extra}.txt"), b"body\n", None)
            .await
            .expect("write extra");
        create_checkpoint(&store, &namespace_id, &context)
            .await
            .expect("checkpoint the extra file");
    }
    crate::test_support::NamespaceFixture::new(
        &store,
        &namespace_id,
        &crate::test_support::writer_at(
            "test-writer",
            crate::test_support::grace_elapsed_after(context.now_ms),
        ),
    )
    .engine()
    .advance_retention_floor(RetentionTarget::Head)
    .await
    .expect("advance the floor");
    assert!(
        read_floor_seq(&store, &namespace_id).await >= deleted.committed_seq,
        "the floor must sit past the cancelling row for this test to mean anything"
    );

    let before = load_manifest_materialization_for_inspection(
        &store,
        &namespace_id,
        current_manifest_no(&store, &namespace_id).await,
    )
    .await
    .expect("load the manifest before the merge");
    // The base is more than four times the size of the delta runs, so their
    // tier merges without it.
    let policy = MetadataRunPolicy::default();
    let (group, spec) = plan_window(
        &store,
        &namespace_id,
        policy,
        Some(MetadataFamilyGroup::Bindings),
    )
    .await;
    assert!(
        group
            .families()
            .contains(&ApiMetadataRowFamily::DirentryBinds),
        "this test is about the binding families, got {group:?}"
    );
    let input_runs = runs_newest_first(before.manifest.payload())
        .into_iter()
        .filter(|run| spec.inputs().contains(&run.run_no))
        .collect::<Vec<_>>();
    let newest_input = input_runs
        .iter()
        .map(|run| run.run_seq)
        .max()
        .expect("the window holds runs");
    assert_eq!(
        spec.placement(),
        super::plan::MergePlacement::Delta {
            output_seq: newest_input
        },
        "a merge above the base writes a delta run at its newest input's sequence"
    );
    assert!(input_runs.len() > 1);
    let base_before = group_base_runs(&before.manifest, group.families());
    publish_planned_compaction(&store, &namespace_id, &context, policy, &spec).await;

    let after = load_manifest_materialization_for_inspection(
        &store,
        &namespace_id,
        current_manifest_no(&store, &namespace_id).await,
    )
    .await
    .expect("load the manifest after the merge");
    // The merge wrote a delta run, not a second base run: the group's base is
    // exactly the one it was, and the merged rows sit at the identity of the
    // newest run the window held, which is where that run stood.
    assert_eq!(
        group_base_runs(&after.manifest, group.families()),
        base_before,
        "a merge above the base must not mint a base run"
    );
    let merged_delta_runs = group_delta_runs(&after.manifest, group.families());
    let [merged_delta] = &merged_delta_runs[..] else {
        panic!("the merged delta runs must leave exactly one delta run");
    };
    assert_eq!(
        (merged_delta.run_seq, merged_delta.tier),
        (newest_input, RunTier::Delta),
        "the merged delta run must stand where the newest input run stood"
    );
    // A merge that skipped older runs drops nothing, so the row set is
    // exactly what it was: the cancelling row still shadows the base's
    // binding and the deleted file stays deleted.
    assert!(
        metadata_states_equivalent(&before.metadata_state, &after.metadata_state),
        "a merge above the base must be a pure rewrite"
    );
    assert!(
        after
            .metadata_state
            .direntry_binds()
            .iter()
            .any(|binding| !binding.is_bound()),
        "the cancelling row must survive the merge"
    );
    let projection = load_current_projection(&store, &namespace_id)
        .await
        .expect("materialization");
    assert!(metadata_states_equivalent(
        &projection.metadata_state,
        &after.metadata_state
    ));
}

#[tokio::test]
async fn repeated_churn_under_small_budgets_leaves_one_base_run_per_group() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");
    // Small segments so the merged base is many segments rather than one, and
    // a row budget the groups outgrow within a couple of cycles.
    let policy = MetadataRunPolicy {
        max_delta_runs: NonZeroUsize::MIN,
        max_decoded_input_rows_per_step: NonZeroUsize::new(24).expect("nonzero"),
        max_rows_per_segment: NonZeroUsize::new(4).expect("nonzero"),
        ..MetadataRunPolicy::default()
    };

    let group = group_containing(ApiMetadataRowFamily::DirentryBinds);
    let mut compacted_groups = 0usize;
    for cycle in 0..4u64 {
        for file in 0..4u64 {
            crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
                .write_file_bytes(
                    &format!("/d{cycle}/f{file}.txt"),
                    format!("body {cycle}/{file}\n").as_bytes(),
                    None,
                )
                .await
                .expect("write file");
        }
        create_checkpoint(&store, &namespace_id, &context)
            .await
            .expect("checkpoint the writes");
        for file in 0..3u64 {
            crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
                .delete_path(&format!("/d{cycle}/f{file}.txt"), None)
                .await
                .expect("delete a file");
        }
        create_checkpoint(&store, &namespace_id, &context)
            .await
            .expect("checkpoint the deletions");
        crate::test_support::NamespaceFixture::new(
            &store,
            &namespace_id,
            &crate::test_support::writer_at(
                "test-writer",
                crate::test_support::grace_elapsed_after(context.now_ms),
            ),
        )
        .engine()
        .advance_retention_floor(RetentionTarget::Head)
        .await
        .expect("advance the floor past the deletions");
        let visible = visible_namespace(&store, &namespace_id).await;

        let mut settled = false;
        for _step in 0..512 {
            let report = compact_with(
                &store,
                &namespace_id,
                policy,
                crate::compaction::CompactionAdmission::WithinRowBudget,
            )
            .await;
            let current = load_manifest_materialization_for_inspection(
                &store,
                &namespace_id,
                current_manifest_no(&store, &namespace_id).await,
            )
            .await
            .expect("load the current manifest");
            for (merged, base_runs) in base_runs_per_family_group(&current.manifest) {
                assert!(
                    base_runs.len() <= 1,
                    "cycle {cycle}: {merged:?} holds base runs {base_runs:?}"
                );
            }
            assert_eq!(
                visible_namespace(&store, &namespace_id).await,
                visible,
                "cycle {cycle}: a step changed what a read answers"
            );
            match report {
                super::CompactionUnitOutcome::NotNeeded => {
                    settled = true;
                    break;
                }
                // The group's window no longer fits the row budget, so a
                // unit that admits any size merges it, as a metadata run's
                // caller does after a window that's too large.
                super::CompactionUnitOutcome::TooLarge { .. } => {
                    assert_matches!(
                        compact_with(
                            &store,
                            &namespace_id,
                            policy,
                            crate::compaction::CompactionAdmission::AnySize,
                        )
                        .await,
                        super::CompactionUnitOutcome::Published { .. }
                    );
                    compacted_groups += 1;
                }
                super::CompactionUnitOutcome::Published { .. } => {}
                other => panic!("no concurrent publisher exists in this test, got {other:?}"),
            }
        }
        assert!(
            settled,
            "cycle {cycle}: compaction did not settle with nothing left to compact"
        );
    }

    assert!(
        compacted_groups > 0,
        "a group must have outgrown the row budget and been rebuilt by a unit of any size"
    );
    let final_manifest = load_manifest_materialization_for_inspection(
        &store,
        &namespace_id,
        current_manifest_no(&store, &namespace_id).await,
    )
    .await
    .expect("load the final manifest");
    assert_eq!(
        group_base_runs(&final_manifest.manifest, group).len(),
        1,
        "the bindings group must end in one base run"
    );
}

#[tokio::test]
async fn a_floor_past_a_pin_keeps_its_manifest_and_runs_readable_until_deletion() {
    let directory = tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let namespace_id = NamespaceId::parse("retained-pin").expect("namespace");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .write_file_bytes("/first", b"first", None)
        .await
        .expect("first write");
    let pin = create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("pin");
    let pinned_files = read_checkpoint_files(&store, &namespace_id, &pin.checkpoint_id)
        .await
        .expect("pinned files");
    let pinned_segments = super::segment_keys_of_the_current_manifest(&store, &namespace_id).await;
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .write_file_bytes("/second", b"second", None)
        .await
        .expect("second write");
    crate::test_support::namespace_store(&store, &namespace_id)
        .fold_wal()
        .await
        .expect("fold");
    let current_segments = super::compact_a_family_group(&store, &namespace_id, &context).await;
    let only_pinned: Vec<_> = pinned_segments
        .difference(&current_segments)
        .cloned()
        .collect();
    assert!(!only_pinned.is_empty());
    let floor = crate::test_support::NamespaceFixture::new(
        &store,
        &namespace_id,
        &crate::test_support::writer_at(
            "test-writer",
            crate::test_support::grace_elapsed_after(context.now_ms),
        ),
    )
    .engine()
    .advance_retention_floor(RetentionTarget::Head)
    .await
    .expect("advance floor")
    .response;
    assert!(floor.retention_floor_seq > pin.captured_seq);
    // The runtime that published the advance raises the hint.
    crate::test_support::namespace_store(&store, &namespace_id)
        .raise_hint_to_current_manifest()
        .await;
    let aged = writer_at("gc", u64::MAX / 2);
    crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &crate::gc::GcOptions::default(), &aged.clock())
        .await
        .expect("collect past the floor");
    assert_eq!(
        read_checkpoint_files(&store, &namespace_id, &pin.checkpoint_id)
            .await
            .expect("pin readable"),
        pinned_files
    );
    for key in &pinned_segments {
        assert!(store.head(key).await.expect("pinned segment").is_some());
    }
    let manifest_key = metadata_manifest_object(&namespace_id, &pin.manifest_no);
    assert!(store
        .head(&manifest_key)
        .await
        .expect("pinned manifest")
        .is_some());
    crate::test_support::NamespaceFixture::new(
        &store,
        &namespace_id,
        &crate::test_support::writer_at("test-writer", 0),
    )
    .engine()
    .delete_snapshot(&pin.checkpoint_id)
    .await
    .expect("delete pin");
    crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &crate::gc::GcOptions::default(), &aged.clock())
        .await
        .expect("collect deleted basis");
    assert!(store
        .head(&manifest_key)
        .await
        .expect("deleted manifest")
        .is_none());
    for key in &only_pinned {
        assert!(store.head(key).await.expect("deleted segment").is_none());
    }
}
