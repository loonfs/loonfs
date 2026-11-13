//! Snapshot retention, compaction, and publication budgets.

use super::*;
use crate::publish::SharedWriterSessionStatus;
use crate::segment::block_memo::SessionBlockMemo;
use crate::time::OperationClock;
use loonfs_objectstore::keys::{snapshot_prefix, wal_prefix};
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
                            }
                            MetadataRow::Commit(record)
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
    assert_eq!(unchanged.retention_floor_seq, CommitSeq(0));
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
    assert_eq!(head_seq, CommitSeq(3));
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
    let after = store
        .list_prefix(&wal_objects_prefix)
        .await
        .expect("list wal");
    assert_eq!(
        after[..wal_objects.len()],
        wal_objects[..],
        "the advance leaves the WAL objects before it"
    );
    assert_eq!(
        after.len(),
        wal_objects.len() + 1,
        "the advance writes one WAL object, its floor object"
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
    create_snapshot(&store, &namespace_id, &context)
        .await
        .expect("fold the commits");

    for (target, expected_floor) in [
        (
            RetentionTarget::Cutoff {
                cutoff_at_ms: 1_999,
            },
            CommitSeq(0),
        ),
        // Commit 3 is stamped before the cutoff, but the later commit 2
        // ends the scan first.
        (
            RetentionTarget::Cutoff {
                cutoff_at_ms: 2_999,
            },
            CommitSeq(1),
        ),
        (
            RetentionTarget::Cutoff {
                cutoff_at_ms: 3_000,
            },
            CommitSeq(3),
        ),
        (RetentionTarget::Seq(CommitSeq(2)), CommitSeq(3)),
        (RetentionTarget::Seq(CommitSeq(99)), CommitSeq(4)),
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
        if target == RetentionTarget::Seq(CommitSeq(2)) {
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
    let grace_ms = crate::limits::GC_GRACE_WINDOW_MS;
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
    create_snapshot(&store, &namespace_id, &context)
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
            CommitSeq(1),
        ),
        // Commit 3 is two windows old, but commit 2 is half a window old, so
        // the walk stops before commit 2 and the floor stays at 1. A commit
        // dated out of order can only stop the floor earlier.
        (RetentionTarget::Seq(CommitSeq(3)), now_ms, CommitSeq(1)),
        // Commit 5 is half a window old, so the floor stops at commit 4.
        (RetentionTarget::Head, now_ms + grace_ms, CommitSeq(4)),
        // A sequence above the head targets the head, and commit 6 is still
        // dated after the advance's clock.
        (
            RetentionTarget::Seq(CommitSeq(99)),
            now_ms + 2 * grace_ms,
            CommitSeq(5),
        ),
        // Commit 6 is exactly one window old, so the floor reaches the head.
        (RetentionTarget::Head, now_ms + 4 * grace_ms, CommitSeq(6)),
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
            lookup_keys::commit_row_key(CommitSeq(14)),
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

    /// Returns the ranges of the GETs of a walk through
    /// [`cache_for_a_walk`] that reads commits `first` through `last`: the
    /// segment's index, and then one GET for each refill, which reads the
    /// next blocks of the commit range up to `MAX_SCAN_REFILL_BYTES` of
    /// stored bytes, from the block of commit `first` through the refill
    /// that reads the block of commit `last`.
    fn ranges_from_through(&self, first: CommitSeq, last: CommitSeq) -> Vec<(u64, u64)> {
        let block_of = |seq| {
            let key = lookup_keys::commit_row_key(seq);
            self.index.partition_point(|entry| entry.last_row_key < key)
        };
        let end_of = |block: &BlockHandle| block.offset + u64::from(block.stored_bytes);
        let mut ranges = vec![(
            self.segment.index_block.offset,
            end_of(&self.segment.index_block),
        )];
        let (mut start, last) = (block_of(first), block_of(last));
        while start <= last {
            let mut end = start + 1;
            while end < self.index.len()
                && end_of(&self.index[end].block) - self.index[start].block.offset
                    <= crate::limits::MAX_SCAN_REFILL_BYTES
            {
                end += 1;
            }
            ranges.push((
                self.index[start].block.offset,
                end_of(&self.index[end - 1].block),
            ));
            start = end;
        }
        ranges
    }
}

/// A cache with room for every block of a walk, so the GETs of a walk show
/// each refill that it reached.
fn cache_for_a_walk() -> MetadataSegmentCache {
    MetadataSegmentCache::unshared(64 * 1024 * 1024)
}

/// Checks that an advance reads commits upward from the floor, and stops at
/// its sequence or at the first commit younger than the grace window, with
/// one ranged GET for each refill of commit blocks that it reaches.
#[tokio::test]
async fn an_advance_sends_one_get_for_each_refill_of_commit_blocks_up_to_where_it_stops() {
    let grace_ms = crate::limits::GC_GRACE_WINDOW_MS;
    let now_ms = 10 * grace_ms;
    for (target, expected_floor, last_read) in [
        // Commit 65 is the first commit younger than the window, so the walk
        // reads commits 1 to 65, and the floor moves to commit 64.
        (RetentionTarget::Head, CommitSeq(64), CommitSeq(65)),
        // The sequence ends the walk after commit 14, the last row of the
        // first block, so the walk sends no GET after its first refill.
        (
            RetentionTarget::Seq(CommitSeq(14)),
            CommitSeq(14),
            CommitSeq(14),
        ),
        // Commit 65 ends the walk below the sequence.
        (
            RetentionTarget::Seq(CommitSeq(100)),
            CommitSeq(64),
            CommitSeq(65),
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
            fixture.namespace.segment_cache = Arc::new(cache_for_a_walk());
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
            history.ranges_from_through(CommitSeq(1), last_read),
            "{target:?}: one GET for the index, then one for each refill through the block of commit {last_read}"
        );
    }
}

/// Checks that an advance reads one commit row when the first commit above
/// the floor is younger than the grace window, as every commit of a fresh
/// namespace is, and leaves the floor where it is.
#[tokio::test]
async fn an_advance_reads_one_commit_row_when_the_first_commit_above_the_floor_is_young() {
    let grace_ms = crate::limits::GC_GRACE_WINDOW_MS;
    let now_ms = 10 * grace_ms;
    // With no old commits, the namespace is fresh, and the walk reads commit
    // 1. With 13 old commits, a first advance moves the floor to 13, and the
    // walk reads commit 14, the last row of the first block, so a walk that
    // read another commit row could need a second refill.
    for old_commits in [0, 13] {
        let history = LongCommitHistory::new(&[
            (old_commits, now_ms - 2 * grace_ms),
            (128 - old_commits, now_ms - grace_ms / 2),
        ])
        .await;
        let floor = CommitSeq(u64::try_from(old_commits).expect("a count fits a u64"));
        for advance in ["first", "second"] {
            history.store.reset();
            let report = {
                let mut fixture = crate::test_support::NamespaceFixture::new(
                    &history.store,
                    &history.namespace_id,
                    &crate::test_support::writer_at("test-writer", now_ms),
                );
                fixture.namespace.segment_cache = Arc::new(cache_for_a_walk());
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
            history.ranges_from_through(CommitSeq(floor.0 + 1), CommitSeq(floor.0 + 1)),
            "{old_commits} old commits: one GET for the index and one for the first refill"
        );
    }
}

/// Checks that an advance toward a cutoff, or a retention window, that's
/// older than the grace window reads the commits up to that cutoff once.
#[tokio::test]
async fn a_cutoff_or_a_window_older_than_the_grace_window_reads_the_commits_once() {
    let grace_ms = crate::limits::GC_GRACE_WINDOW_MS;
    let now_ms = 10 * grace_ms;
    for (target, retention_window_ms) in [
        (
            RetentionTarget::Cutoff {
                cutoff_at_ms: now_ms - 5 * grace_ms / 2,
            },
            None,
        ),
        (
            RetentionTarget::Window,
            std::num::NonZeroU64::new(5 * grace_ms / 2),
        ),
    ] {
        // Commits 1 to 64 are older than the target's cutoff, and commits 65
        // to 128 are between that cutoff and the grace window's.
        let history =
            LongCommitHistory::new(&[(64, now_ms - 3 * grace_ms), (64, now_ms - 2 * grace_ms)])
                .await;
        let engine = crate::test_support::NamespaceFixture::new(
            &history.store,
            &history.namespace_id,
            &crate::test_support::writer_at("test-writer", now_ms),
        )
        .with_segment_cache()
        .engine();
        engine
            .set_retention_window(retention_window_ms)
            .await
            .expect("set the window");
        history.store.reset();
        let floor = engine
            .advance_retention_floor(target)
            .await
            .expect("advance the floor")
            .response
            .retention_floor_seq;
        assert_eq!(floor, CommitSeq(64), "{target:?}");
        // The walk's later refills find the index and the blocks that an
        // earlier one fetched in the block cache.
        assert_eq!(
            history.take_commit_ranges(),
            history.ranges_from_through(CommitSeq(1), CommitSeq(128)),
            "{target:?}: one GET for the index and one for each refill"
        );
    }
}

#[tokio::test]
async fn a_window_advance_uses_the_namespace_window_and_keeps_the_floor_without_one() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");
    let grace_ms = crate::limits::GC_GRACE_WINDOW_MS;
    let now_ms = 5 * grace_ms;
    for (path, committed_at_ms) in [
        ("/a.txt", 2 * grace_ms),
        ("/b.txt", 3 * grace_ms),
        ("/c.txt", 4 * grace_ms),
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
    create_snapshot(&store, &namespace_id, &context)
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

    // Each step sets the namespace's window, then advances at five grace
    // windows. The cutoffs are five grace windows minus the window, and all
    // three commits are at least a grace window old.
    for (namespace_window_ms, expected_floor) in [
        (None, CommitSeq(0)),
        (Some(5 * grace_ms / 2), CommitSeq(1)),
        (Some(3 * grace_ms / 2), CommitSeq(2)),
        // Once the window is cleared, the floor stays where it is.
        (None, CommitSeq(2)),
        (Some(grace_ms / 2), CommitSeq(3)),
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
        let target = RetentionTarget::Window;
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
    create_snapshot(&store, &namespace_id, &context)
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
    assert_eq!(floor, CommitSeq(2));
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
    create_snapshot(&store, &namespace_id, &context)
        .await
        .expect("create snapshot");

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
        CommitSeq(0),
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
    create_snapshot(&setup_store, &namespace_id, &context)
        .await
        .expect("create snapshot");

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
        CommitSeq(0),
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

/// Creates and deletes the directory `/{name}` through `writer`, and returns
/// the deleted inode and the deletion's sequence.
async fn create_and_delete<S: ObjectStore>(
    writer: &mut NamespaceCommitEngine<S>,
    name: &str,
) -> (InodeId, CommitSeq) {
    publish_directories(writer, name, 1).await;
    let deleted = publish_one(
        writer,
        FilesystemOperation::DeletePath {
            path: AbsolutePath::parse(format!("/{name}-0")).expect("path"),
            behavior: loonfs_types::DeleteDirectoryBehavior::Recursive,
            expected_inode_id: None,
        },
    )
    .await
    .expect("delete the directory");
    let inode_id = deleted
        .events
        .iter()
        .find_map(|event| match event {
            loonfs_types::FilesystemEvent::Deleted { inode_id, .. } => Some(*inode_id),
            _ => None,
        })
        .expect("a delete reports its deletion");
    (inode_id, deleted.committed_seq)
}

/// Publishes one commit of `operation` through `writer`.
async fn publish_one<S: ObjectStore>(
    writer: &mut NamespaceCommitEngine<S>,
    operation: FilesystemOperation,
) -> crate::error::Result<loonfs_types::Commit> {
    writer
        .publish_batch(
            vec![CommitCandidate::new(CommitIntent::single(
                CommitId::generate(),
                loonfs_test_support::test_actor(),
                None,
                operation,
            ))],
            &test_context().clock(),
        )
        .await
        .results
        .pop()
        .expect("one result")
}

/// Checks the second example of a retention advance in the WAL: a process
/// without a writer session creates a floor object at the tip, and a writer
/// session in another process then commits a restore of a deletion below
/// the new floor. The session's put collides with the floor object, the
/// session replays the floor object, and the restore returns
/// `not_deleted`. The session isn't fenced, and its next commit is written
/// above the floor object.
#[tokio::test]
async fn a_session_that_collides_with_a_floor_object_replays_it_and_isnt_fenced() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    bootstrap_namespace(&store, &namespace_id, &test_context())
        .await
        .expect("bootstrap");
    let mut writer = NamespaceCommitEngine::new(
        crate::test_support::namespace_store(&store, &namespace_id),
        test_context().writer_id,
    )
    .writer_session(SharedWriterSessionStatus::default());
    let (gone, deletion_seq) = create_and_delete(&mut writer, "gone").await;
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let planned = namespace
        .load_current_manifest()
        .await
        .expect("load the planned manifest");

    let report = namespace
        .publish_floor_object(&planned, deletion_seq)
        .await
        .expect("create the floor object");
    assert_eq!(report.retention_floor_seq, deletion_seq);
    assert_eq!(report.manifest_retention_floor_seq, CommitSeq(0));
    let floor_object = namespace.load_read_state().await.expect("read the head");
    let restore_deleted = || FilesystemOperation::RestoreDeleted {
        inode_id: gone,
        deletion_seq,
        destination: loonfs_types::options::RestoreDeletedDestination::Recorded,
    };
    let collided = publish_one(&mut writer, restore_deleted())
        .await
        .expect_err("the put collides with the floor object");
    assert_eq!(collided.code(), ErrorCode::StaleHead, "{collided:?}");
    let refused = publish_one(&mut writer, restore_deleted())
        .await
        .expect_err("the restore is below the replayed floor");
    assert_eq!(refused.code(), ErrorCode::NotDeleted, "{refused:?}");
    publish_directories(&mut writer, "next", 1).await;

    let head = namespace.load_read_state().await.expect("read the head");
    assert_eq!(head.wal_no.0, floor_object.wal_no.0 + 1);
    assert_eq!(head.writer_epoch, planned.payload().writer_epoch);
    assert_eq!(head.retention_floor_seq, deletion_seq);
    assert_eq!(
        read_floor_seq(&store, &namespace_id).await,
        CommitSeq(0),
        "no manifest records the floor before a fold"
    );
}

/// Checks that a view that replays a floor object reports the floor before
/// any manifest records it, so the change feed refuses a start below the
/// floor. The fold that covers the floor object publishes the higher of its
/// predecessor's floor and the floor object's, and a later floor object with
/// a lower floor leaves the next fold's floor where it is.
#[tokio::test]
#[expect(
    clippy::disallowed_methods,
    reason = "the test writes a floor object with a lower floor, which no process creates"
)]
async fn a_view_reports_a_floor_objects_floor_and_a_fold_publishes_the_higher_floor() {
    use loonfs_types::format::wal::{encode_wal_object_envelope_zstd, WalObjectPayload};

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
    let (_, deletion_seq) = create_and_delete(&mut writer, "gone").await;
    publish_directories(&mut writer, "after", 1).await;
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let planned = namespace.load_current_manifest().await.expect("planned");
    namespace
        .publish_floor_object(&planned, deletion_seq)
        .await
        .expect("create the floor object");

    let anchor = namespace
        .load_read_anchor(crate::test_support::observed_now())
        .await
        .expect("load the view");
    assert_eq!(anchor.retention_floor_seq(), deletion_seq);
    assert_eq!(read_floor_seq(&store, &namespace_id).await, CommitSeq(0));
    let context = crate::RuntimeReadContext {
        read_basis: crate::control::ReadBasis {
            basis: anchor.basis(),
            head: anchor.read_state.clone(),
        },
        tail: None,
        visible_seq: None,
    };
    let error =
        crate::NamespaceEngine::reader(crate::test_support::namespace_store(&store, &namespace_id))
            .list_changes_after(
                loonfs_types::ChangeFeedPosition::after(CommitSeq(0)),
                EffectiveLimit::new(std::num::NonZeroU32::new(4).expect("nonzero")),
                loonfs_types::options::CommitSplitting::Whole,
                &context,
            )
            .await
            .expect_err("the feed starts below the view's floor");
    assert_matches!(
        error,
        CoreError::BelowRetentionFloor { retention_floor_seq, .. }
            if retention_floor_seq == deletion_seq
    );

    namespace.fold_wal().await.expect("fold the floor object");
    assert_eq!(read_floor_seq(&store, &namespace_id).await, deletion_seq);
    let head = namespace.load_read_state().await.expect("read the head");
    let lower = WalObjectPayload {
        namespace_id: namespace_id.clone(),
        wal_no: head.wal_no.successor().expect("next WAL number"),
        writer_epoch: head.writer_epoch,
        head_seq: head.seq,
        records: Vec::new(),
        retention_floor_seq: Some(CommitSeq(1)),
    };
    let key = loonfs_objectstore::keys::wal_object(&namespace_id, &lower.wal_no);
    store
        .put(
            &key,
            encode_wal_object_envelope_zstd(lower)
                .expect("encode a floor object")
                .into_bytes()
                .into(),
            loonfs_objectstore::PutMode::CreateIfAbsent,
        )
        .await
        .expect("put a floor object with a lower floor");
    namespace
        .fold_wal()
        .await
        .expect("fold the lower floor object");
    assert_eq!(read_floor_seq(&store, &namespace_id).await, deletion_seq);
}

/// Checks that two processes that advance the same namespace to different
/// floors leave the higher floor, in either order. An advance whose target
/// another process's floor object already reached writes nothing, and the
/// fold after both advances publishes the higher floor.
#[tokio::test]
async fn two_advances_to_different_floors_leave_the_higher_floor() {
    for (first, second) in [(CommitSeq(2), CommitSeq(1)), (CommitSeq(1), CommitSeq(2))] {
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
        publish_directories(&mut writer, "dir", 2).await;
        let namespace = crate::test_support::namespace_store(&store, &namespace_id);
        let planned = namespace
            .load_current_manifest()
            .await
            .expect("load the planned manifest");
        let tip = namespace
            .load_read_state()
            .await
            .expect("read the head")
            .wal_no;

        namespace
            .publish_floor_object(&planned, first)
            .await
            .expect("the first advance");
        let report = namespace
            .publish_floor_object(&planned, second)
            .await
            .expect("the second advance");
        assert_eq!(report.retention_floor_seq, CommitSeq(2));
        let head = namespace.load_read_state().await.expect("read the head");
        let floor_objects = if first > second { 1 } else { 2 };
        assert_eq!(head.wal_no.0, tip.0 + floor_objects);
        namespace.fold_wal().await.expect("fold");
        assert_eq!(read_floor_seq(&store, &namespace_id).await, CommitSeq(2));
    }
}

/// Reads the files one snapshot keeps at the test clock, or the error that
/// says it no longer keeps anything.
async fn read_snapshot_files<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    snapshot_id: &SnapshotId,
) -> crate::error::Result<Vec<InodeId>> {
    let page = crate::test_support::namespace_store(store, namespace_id)
        .list_snapshot_files_page(
            snapshot_id,
            loonfs_types::PageRequest {
                cursor: None,
                limit: EffectiveLimit::new(NonZeroU32::new(64).expect("nonzero")),
            },
            crate::snapshot::ListSnapshotFilesOptions::default(),
        )
        .await?;
    Ok(page.files.into_iter().map(|file| file.inode_id).collect())
}

#[tokio::test]
async fn deleting_a_snapshot_never_reuses_its_id() {
    // A deleted snapshot id is never reused. A caller asking for
    // a snapshot again — even at the same instant, over the same basis, under the
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
    let first = create_snapshot(&store, &namespace_id, &context)
        .await
        .expect("create snapshot");
    assert!(
        !read_snapshot_files(&store, &namespace_id, &first.snapshot_id)
            .await
            .expect("the first snapshot serves")
            .is_empty()
    );

    let engine = crate::test_support::NamespaceFixture::new(
        &store,
        &namespace_id,
        &crate::test_support::writer_at("test-writer", 0),
    )
    .engine();
    let delete = engine.delete_snapshot(&first.snapshot_id);
    let recreate = create_snapshot(&store, &namespace_id, &context);
    let (delete, second) = tokio::join!(delete, recreate);
    assert_eq!(delete.expect("delete").snapshot_id, first.snapshot_id);
    let second = second.expect("a concurrent create takes a fresh snapshot");
    assert_ne!(
        second.snapshot_id, first.snapshot_id,
        "a new snapshot is never written at a deleted record's key"
    );

    assert!(crate::test_support::namespace_store(&store, &namespace_id)
        .load_snapshot_record(&first.snapshot_id)
        .await
        .expect("load snapshot")
        .expect("the deleted snapshot stays until collection")
        .state
        .owner
        .deleted_at_ms()
        .is_some());
    let error = read_snapshot_files(&store, &namespace_id, &first.snapshot_id)
        .await
        .expect_err("a deleted record serves no read");
    assert_eq!(error.code(), ErrorCode::SnapshotNotFound);
    assert!(
        !read_snapshot_files(&store, &namespace_id, &second.snapshot_id)
            .await
            .expect("the new snapshot serves")
            .is_empty()
    );

    crate::test_support::NamespaceFixture::new(
        &store,
        &namespace_id,
        &crate::test_support::writer_at("test-writer", 0),
    )
    .engine()
    .delete_snapshot(&first.snapshot_id)
    .await
    .expect("a second delete succeeds");
}

#[tokio::test]
async fn each_create_mints_its_own_record_and_carries_its_own_expiry() {
    // Re-creating a snapshot is not a renewal of an earlier one. Every
    // call is its own snapshot with its own id, its own creation instant, and
    // exactly the expiry it asked for; earlier records are left alone.
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let owner = |expires_at_ms| loonfs_types::format::control::SnapshotOwner::User {
        name: Some("test-snapshot".to_owned()),
        expires_at_ms,
        deleted_at_ms: None,
    };
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .write_file_bytes("/docs/file.txt", b"body\n", None)
        .await
        .expect("write");

    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let first = namespace
        .create_snapshot_record(
            owner(Some(10_000)),
            crate::test_support::folded_manifest(&namespace)
                .await
                .expect("fold the WAL tail"),
            &context.clock(),
        )
        .await
        .map(crate::snapshot::snapshot_record_summary)
        .expect("create snapshot");
    assert_eq!(first.expires_at_ms, Some(10_000));

    let mut later_context = test_context();
    later_context.now_ms = 2_000;
    let mut minted = BTreeSet::from([first.snapshot_id.clone()]);
    for expiry in [Some(99_000), Some(5_000), None] {
        let namespace = crate::test_support::namespace_store(&store, &namespace_id);
        let next = namespace
            .create_snapshot_record(
                owner(expiry),
                crate::test_support::folded_manifest(&namespace)
                    .await
                    .expect("fold the WAL tail"),
                &later_context.clock(),
            )
            .await
            .map(crate::snapshot::snapshot_record_summary)
            .expect("create snapshot");
        assert!(
            minted.insert(next.snapshot_id.clone()),
            "each snapshot gets an id of its own"
        );
        assert_eq!(next.expires_at_ms, expiry);
        let record = crate::test_support::namespace_store(&store, &namespace_id)
            .load_snapshot_record(&next.snapshot_id)
            .await
            .expect("read snapshot")
            .expect("record exists")
            .state;
        assert_eq!(record.owner.expires_at_ms(), expiry);
        assert_eq!(record.created_at_ms, 2_000);
    }

    // The very first record is untouched by any of it: a snapshot taken without
    // an expiry, or with one, is held until something deletes it.
    let original = crate::test_support::namespace_store(&store, &namespace_id)
        .load_snapshot_record(&first.snapshot_id)
        .await
        .expect("read snapshot")
        .expect("record exists")
        .state;
    assert_eq!(original.created_at_ms, 1_000, "creation instant is history");
    assert_eq!(original.owner.expires_at_ms(), Some(10_000));
}

#[tokio::test]
async fn an_expired_snapshot_serves_no_reads_but_stays_until_a_collection_deletes_it() {
    // A read refuses an expired snapshot, but the record is still a
    // garbage-collection root until a collection deletes it, so its state stays
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
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let already_expired = namespace
        .create_snapshot_record(
            loonfs_types::format::control::SnapshotOwner::User {
                name: Some("test-snapshot".to_owned()),
                expires_at_ms: Some(context.now_ms),
                deleted_at_ms: None,
            },
            crate::test_support::folded_manifest(&namespace)
                .await
                .expect("fold the WAL tail"),
            &context.clock(),
        )
        .await
        .map(crate::snapshot::snapshot_record_summary)
        .expect("create snapshot whose expiry has already passed");
    assert!(crate::test_support::namespace_store(&store, &namespace_id)
        .load_snapshot_record(&already_expired.snapshot_id)
        .await
        .expect("load snapshot")
        .is_some());
    let error = read_snapshot_files(&store, &namespace_id, &already_expired.snapshot_id)
        .await
        .expect_err("an expired snapshot serves no read");
    assert_eq!(error.code(), ErrorCode::SnapshotNotFound);

    // The collection after the grace deletes the record.
    crate::test_support::namespace_store(&store, &namespace_id)
        .gc(&writer_at(
            "expired",
            context.now_ms + crate::limits::GC_GRACE_WINDOW_MS,
        )
        .clock())
        .await
        .expect("collection");
    let error = read_snapshot_files(&store, &namespace_id, &already_expired.snapshot_id)
        .await
        .expect_err("a deleted snapshot serves nothing");
    assert_eq!(error.code(), ErrorCode::SnapshotNotFound);
    assert!(crate::test_support::namespace_store(&store, &namespace_id)
        .load_snapshot_record(&already_expired.snapshot_id)
        .await
        .expect("load snapshot")
        .is_none());
}

#[tokio::test]
async fn a_snapshot_without_a_ttl_is_kept_until_it_is_deleted() {
    // No expiry means no clock: the record stays a serving snapshot however far
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
    let snapshot = create_snapshot(&store, &namespace_id, &context)
        .await
        .expect("create snapshot");
    assert_eq!(snapshot.expires_at_ms, None);

    let mut distant = test_context();
    distant.now_ms = u64::MAX / 2;
    for _ in 0..3 {
        crate::test_support::namespace_store(&store, &namespace_id)
            .gc(&distant.clock())
            .await
            .expect("collection");
    }
    crate::test_support::namespace_store(&store, &namespace_id)
        .load_snapshot_record(&snapshot.snapshot_id)
        .await
        .expect("read snapshot")
        .expect("an unexpiring snapshot survives every collection");
    assert!(
        !read_snapshot_files(&store, &namespace_id, &snapshot.snapshot_id)
            .await
            .expect("the snapshot still serves")
            .is_empty()
    );

    crate::test_support::NamespaceFixture::new(
        &store,
        &namespace_id,
        &crate::test_support::writer_at("test-writer", 0),
    )
    .engine()
    .delete_snapshot(&snapshot.snapshot_id)
    .await
    .expect("delete");
    let error = read_snapshot_files(&store, &namespace_id, &snapshot.snapshot_id)
        .await
        .expect_err("delete ends it");
    assert_eq!(error.code(), ErrorCode::SnapshotNotFound);
}

#[tokio::test]
async fn snapshot_creation_deletes_its_record_when_the_floor_passed_its_manifest() {
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
        loonfs_test_support::stores::KeyPredicate::prefix(snapshot_prefix(&namespace_id)),
    );
    let error = crate::test_support::namespace_store(&store, &namespace_id)
        .create_snapshot_record(
            loonfs_types::format::control::SnapshotOwner::User {
                name: Some("old".to_owned()),
                expires_at_ms: None,
                deleted_at_ms: None,
            },
            initial.basis().manifest().clone(),
            &context.clock(),
        )
        .await
        .map(crate::snapshot::snapshot_record_summary)
        .expect_err("floor passed snapshot");
    assert_eq!(error.code(), ErrorCode::SnapshotUnavailable);
    assert!(store
        .list_prefix(&snapshot_prefix(&namespace_id))
        .await
        .expect("snapshots")
        .is_empty());
    assert_eq!(store.counts().create_if_absent_puts, 1);
    assert_eq!(store.counts().deletes, 1);
}

#[tokio::test]
async fn snapshot_verification_rejects_a_deleted_namespace() {
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
    let snapshot = create_snapshot(&store, &namespace_id, &context)
        .await
        .expect("create snapshot");
    let record = crate::test_support::namespace_store(&store, &namespace_id)
        .load_snapshot_record(&snapshot.snapshot_id)
        .await
        .expect("load snapshot")
        .expect("snapshot exists")
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
        .verify_snapshot_basis(&record)
        .await
        .expect("verification runs");
    assert_eq!(
        verified,
        crate::snapshot::record::SnapshotBasisVerification::Invalid,
        "a tombstoned namespace cannot acquire a new snapshot dependency"
    );
}

#[tokio::test]
async fn snapshot_basis_verification_store_failure_deletes_the_record() {
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
        loonfs_test_support::stores::KeyPredicate::exact(loonfs_objectstore::keys::boundary(
            &namespace_id,
        )),
        loonfs_test_support::stores::OperationSelector::Read,
        loonfs_test_support::stores::InjectedError::Transport("verification failed".to_owned()),
    );
    store.fail_next(1);
    let error = crate::test_support::namespace_store(&store, &namespace_id)
        .create_snapshot_record(
            loonfs_types::format::control::SnapshotOwner::User {
                name: Some("failed".to_owned()),
                expires_at_ms: None,
                deleted_at_ms: None,
            },
            initial.basis().manifest().clone(),
            &context.clock(),
        )
        .await
        .map(crate::snapshot::snapshot_record_summary)
        .expect_err("verification failed");
    assert_eq!(error.code(), ErrorCode::ServerError);
    assert!(store
        .list_prefix(&snapshot_prefix(&namespace_id))
        .await
        .expect("snapshots")
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
    let boundary_key = boundary(&namespace_id);
    let wal_prefix = wal_prefix(&namespace_id);
    let store = RecordingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::new(move |key| key == boundary_key || key.starts_with(&wal_prefix)),
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
        .expect("the publish that reaches exactly the bound is still admitted");
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
        loonfs_types::ChangeFeedPosition::after(CommitSeq(0)),
        EffectiveLimit::new(NonZeroU32::new(200).expect("nonzero")),
        loonfs_types::options::CommitSplitting::Whole,
    )
    .await
    .expect("list changes");
    assert_eq!(
        changes.head_seq,
        CommitSeq(u64::try_from(boundary - 1).expect("data WAL objects"))
    );
}

#[tokio::test]
async fn snapshots_append_past_the_threshold_and_compaction_drains() {
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
        write_file_and_create_snapshot(&store, &namespace_id, &context, index).await;
    }

    let appended = load_manifest_state_for_inspection(
        &store,
        &namespace_id,
        current_manifest_no(&store, &namespace_id).await,
    )
    .await
    .expect("load appended manifest");
    assert_eq!(delta_runs(&appended.manifest).len(), rounds);
    assert!(delta_runs(&appended.manifest).len() > DEFAULT_MAX_DELTA_RUNS.get());
    assert_eq!(appended.manifest.payload().base_seq(), CommitSeq(0));

    // Compaction rewrites one family group per unit, each publishing its
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
        "several family groups should be compacted, got {units}"
    );

    let drained = load_manifest_state_for_inspection(
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
        CommitSeq(u64::try_from(rounds).expect("round count fits"))
    );
    assert!(metadata_states_equivalent(
        &materialization_after.metadata_state,
        &drained.metadata_state
    ));
}

/// Checks that a unit compacts at most its run budget, and that it reads only
/// the selected runs' segments.
#[tokio::test]
async fn a_unit_honors_its_run_budget_and_reads_only_the_selected_runs() {
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
        create_snapshot(&store, &namespace_id, &context)
            .await
            .expect("create snapshot");
    }

    let policy = MetadataRunPolicy {
        max_delta_runs: NonZeroUsize::MIN,
        max_input_runs_per_step: NonZeroUsize::new(2).expect("test run budget should be nonzero"),
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
        loonfs_types::CompactorEpoch(0),
        &crate::compaction::MetadataCompactionCancellation::default(),
    )
    .await
    .expect("admitted unit");
    let super::CompactionUnitOutcome::Published {
        group, input_runs, ..
    } = published
    else {
        panic!("two oldest runs should fit the test budgets");
    };
    assert_eq!(input_runs, 2);
    assert!(input_runs <= policy.max_input_runs_per_step.get());
    // The basis manifest's retention floor is 0, so the unit builds no kept
    // inode filter and reads none of the basis's slot index for one.
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
        create_snapshot(&bounded_store, &namespace_id, &context)
            .await
            .expect("create snapshot");
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

    let bounded =
        load_manifest_state_for_inspection(&bounded_store, &namespace_id, bounded_manifest_no)
            .await
            .expect("load bounded result");
    let unbounded =
        load_manifest_state_for_inspection(&unbounded_store, &namespace_id, unbounded_manifest_no)
            .await
            .expect("load unbounded result");
    assert!(metadata_states_equivalent_ignoring_content_identity(
        &bounded.metadata_state,
        &unbounded.metadata_state
    ));
    // One unbounded unit compacts each group's tier of six runs whole. Units of
    // two runs compact the tier's oldest runs into the base until the runs left
    // are fewer than a tier, and those wait for later folds.
    assert!(delta_runs(&unbounded.manifest).is_empty());
    assert_eq!(
        delta_runs(&bounded.manifest)
            .iter()
            .map(|run| run.run_seq)
            .collect::<Vec<_>>(),
        [CommitSeq(5), CommitSeq(6)]
    );

    let later_commit = CommitId::parse("bounded-convergence-later").expect("commit id");
    crate::test_support::NamespaceFixture::new(&unbounded_store, &namespace_id, &context)
        .write_file_bytes("/docs/later.txt", b"later\n", Some(&later_commit))
        .await
        .expect("write later file");
    create_snapshot(&unbounded_store, &namespace_id, &context)
        .await
        .expect("snapshot later file");
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

    let first_manifest_no = snapshot_then_compact(&store, &namespace_id, &context, policy).await;
    let first_materialized =
        load_manifest_state_for_inspection(&store, &namespace_id, first_manifest_no)
            .await
            .expect("load first manifest");
    let first_run_keys = run_segment_object_keys(&first_materialized.manifest);

    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .write_file_bytes("/hot/after-delta.txt", b"hot 2\n", None)
        .await
        .expect("write hot delta");
    snapshot_then_compact(&store, &namespace_id, &context, policy).await;
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .write_file_bytes("/hot/after-compact.txt", b"hot 3\n", None)
        .await
        .expect("write hot compact");
    let compacted = create_snapshot(&store, &namespace_id, &context)
        .await
        .expect("compacted snapshot");
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
        load_manifest_state_for_inspection(&store, &namespace_id, compacted_manifest_no)
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
    // own, so a fully compacted manifest holds one base run per group that has
    // rows, and nothing else.
    for (families, base_runs) in base_runs_per_family_group(&compacted_materialized.manifest) {
        assert!(
            base_runs.len() <= 1,
            "{families:?} holds base runs {base_runs:?} after a full compaction"
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

    let first_manifest_no = snapshot_then_compact(&store, &namespace_id, &context, policy).await;
    let first_materialized =
        load_manifest_state_for_inspection(&store, &namespace_id, first_manifest_no)
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
    snapshot_then_compact(&store, &namespace_id, &context, policy).await;
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .write_file_bytes("/docs/file-0.txt", b"compact\n", None)
        .await
        .expect("write compaction revision");
    let _compacted = create_snapshot(&store, &namespace_id, &context)
        .await
        .expect("compacted snapshot");
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
        load_manifest_state_for_inspection(&store, &namespace_id, compacted_manifest_no)
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
        write_file_and_create_snapshot(&store, &namespace_id, &context, index).await;
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

    // A snapshot is published in between, adding a fresh delta run.
    write_file_and_create_snapshot(&store, &namespace_id, &context, 9).await;

    // A fresh sequence of steps reads the live manifest and finishes the
    // job, including the delta the interleaved snapshot added.
    let mut compacted_groups = vec![first_group];
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
            loonfs_types::CompactorEpoch(0),
            &crate::compaction::MetadataCompactionCancellation::default(),
        )
        .await
        .expect("resumed unit");
        match report {
            super::CompactionUnitOutcome::Published { group, .. } => {
                compacted_groups.push(group);
            }
            super::CompactionUnitOutcome::NotNeeded => break,
            other => panic!("no concurrent publisher exists in this test, got {other:?}"),
        }
    }
    assert!(compacted_groups.len() >= 2);

    let drained = load_manifest_state_for_inspection(
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

    let CoreError::StoreContention { object_key } = &error else {
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
            "/docs/compacted.txt",
            b"body",
            DestinationBehavior::NoReplace,
            None,
        )
        .await
        .expect("seed file");
    crate::test_support::namespace_store(&store, &namespace_id)
        .fold_wal()
        .await
        .expect("publish a delta run to compact");
    let manifest_before = crate::test_support::namespace_store(&store, &namespace_id)
        .load_current_manifest()
        .await
        .expect("read manifest");

    let compact_everything = MetadataRunPolicy {
        max_delta_runs: NonZeroUsize::MIN,
        ..Default::default()
    };
    let error = {
        let mut fixture = crate::test_support::NamespaceFixture::new(
            &store,
            &namespace_id,
            &crate::test_support::writer_at("test-writer", 0),
        );
        fixture.namespace.run_policy = compact_everything;
        fixture.namespace.timer = Arc::new(ManualClock::stepping(20 * 60 * 1000));
        fixture
    }
    .engine()
    .compact_metadata_unit(
        None,
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
        fixture.namespace.run_policy = compact_everything;
        fixture
    }
    .engine()
    .compact_metadata_unit(
        None,
        loonfs_types::CompactorEpoch(0),
        &crate::compaction::MetadataCompactionCancellation::default(),
    )
    .await
    .expect("in-budget retry compacts the unit");
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
            policy,
        )
        .expect("a family group with delta rows to compact")
    });
    let spec = super::plan::plan_group(
        segments.manifest().payload(),
        segments.scan_runs.as_ref(),
        group,
        policy,
    )
    .expect("plan the group");
    (group, spec)
}

/// Runs one size-tiered compaction unit with compactor epoch 0.
async fn compact_with<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    policy: MetadataRunPolicy,
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
        loonfs_types::CompactorEpoch(0),
        &crate::compaction::MetadataCompactionCancellation::default(),
    )
    .await
    .expect("run a compaction unit")
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

/// Checks that a size-tiered unit rebuilds a group whose base is comparable
/// in size to its delta runs whole, base included, into one base run.
#[tokio::test]
async fn a_base_comparable_to_its_deltas_is_rebuilt_whole_with_them() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");

    // A compacted base first, then delta runs stacked above it.
    for index in 1..=4 {
        write_file_and_create_snapshot(&store, &namespace_id, &context, index).await;
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
        write_file_and_create_snapshot(&store, &namespace_id, &context, index).await;
    }

    let before = load_manifest_state_for_inspection(
        &store,
        &namespace_id,
        current_manifest_no(&store, &namespace_id).await,
    )
    .await
    .expect("load the parked manifest");
    let (group, _) = plan_window(&store, &namespace_id, MetadataRunPolicy::default(), None).await;
    let base = base_tier(&before.manifest);
    let base_segments = group_segment_object_keys(&base, group.families());
    assert!(
        !base_segments.is_empty(),
        "the base run must hold segments in this group"
    );
    let policy = MetadataRunPolicy {
        max_delta_runs: NonZeroUsize::MIN,
        ..MetadataRunPolicy::default()
    };

    let (_, spec) = plan_window(&store, &namespace_id, policy, None).await;
    assert_eq!(
        spec.input_runs(),
        group_runs(&before.manifest, group.families()).len(),
        "comparable input sizes warrant a base rewrite"
    );
    assert_matches!(
        compact_with(&store, &namespace_id, policy).await,
        super::CompactionUnitOutcome::Published { group: compacted, .. } if compacted == group
    );

    let after = load_manifest_state_for_inspection(
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
async fn a_compaction_above_the_base_keeps_the_rows_that_shadow_it() {
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
    create_snapshot(&store, &namespace_id, &context)
        .await
        .expect("snapshot the seed");
    drain_compaction(
        &store,
        &namespace_id,
        MetadataRunPolicy {
            max_delta_runs: NonZeroUsize::MIN,
            ..MetadataRunPolicy::default()
        },
    )
    .await;

    // One delta run cancels a binding the base holds, more delta runs are written
    // above it until they make a tier, and the floor moves past the
    // cancellation.
    let deleted = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .delete_path("/docs/gone.txt", None)
        .await
        .expect("delete");
    create_snapshot(&store, &namespace_id, &context)
        .await
        .expect("snapshot the delete");
    for extra in 1..MetadataRunPolicy::default().min_compaction_runs.get() {
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
            .write_file_bytes(&format!("/docs/extra-{extra}.txt"), b"body\n", None)
            .await
            .expect("write extra");
        create_snapshot(&store, &namespace_id, &context)
            .await
            .expect("snapshot the extra file");
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

    let before = load_manifest_state_for_inspection(
        &store,
        &namespace_id,
        current_manifest_no(&store, &namespace_id).await,
    )
    .await
    .expect("load the manifest before the compaction");
    // The base is more than four times the size of the delta runs, so their
    // tier is compacted without it.
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
        super::plan::CompactionPlacement::Delta {
            output_seq: newest_input
        },
        "a compaction above the base writes a delta run at its newest input's sequence"
    );
    assert!(input_runs.len() > 1);
    let base_before = group_base_runs(&before.manifest, group.families());
    publish_planned_compaction(&store, &namespace_id, policy, &spec).await;

    let after = load_manifest_state_for_inspection(
        &store,
        &namespace_id,
        current_manifest_no(&store, &namespace_id).await,
    )
    .await
    .expect("load the manifest after the compaction");
    // The compaction wrote a delta run, not a second base run: the group's base is
    // exactly the one it was, and the compacted rows sit at the identity of the
    // newest run the window held, which is where that run stood.
    assert_eq!(
        group_base_runs(&after.manifest, group.families()),
        base_before,
        "a compaction above the base must not mint a base run"
    );
    let compacted_delta_runs = group_delta_runs(&after.manifest, group.families());
    let [compacted_delta] = &compacted_delta_runs[..] else {
        panic!("the compacted delta runs must leave exactly one delta run");
    };
    assert_eq!(
        (compacted_delta.run_seq, compacted_delta.tier),
        (newest_input, RunTier::Delta),
        "the compacted delta run must stand where the newest input run stood"
    );
    // A compaction that skipped older runs drops nothing, so the row set is
    // exactly what it was: the cancelling row still shadows the base's
    // binding and the deleted file stays deleted.
    assert!(
        metadata_states_equivalent(&before.metadata_state, &after.metadata_state),
        "a compaction above the base must be a pure rewrite"
    );
    assert!(
        after
            .metadata_state
            .direntry_binds()
            .iter()
            .any(|binding| !binding.is_bound()),
        "the cancelling row must survive the compaction"
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
    // Small segments so the compacted base is many segments rather than one.
    let policy = MetadataRunPolicy {
        max_delta_runs: NonZeroUsize::MIN,
        max_rows_per_segment: NonZeroUsize::new(4).expect("nonzero"),
        ..MetadataRunPolicy::default()
    };

    let group = group_containing(ApiMetadataRowFamily::DirentryBinds);
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
        create_snapshot(&store, &namespace_id, &context)
            .await
            .expect("snapshot the writes");
        for file in 0..3u64 {
            crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
                .delete_path(&format!("/d{cycle}/f{file}.txt"), None)
                .await
                .expect("delete a file");
        }
        create_snapshot(&store, &namespace_id, &context)
            .await
            .expect("snapshot the deletions");
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
            let report = compact_with(&store, &namespace_id, policy).await;
            let current = load_manifest_state_for_inspection(
                &store,
                &namespace_id,
                current_manifest_no(&store, &namespace_id).await,
            )
            .await
            .expect("load the current manifest");
            for (families, base_runs) in base_runs_per_family_group(&current.manifest) {
                assert!(
                    base_runs.len() <= 1,
                    "cycle {cycle}: {families:?} holds base runs {base_runs:?}"
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
                super::CompactionUnitOutcome::Published { .. } => {}
                other => panic!("no concurrent publisher exists in this test, got {other:?}"),
            }
        }
        assert!(
            settled,
            "cycle {cycle}: compaction did not settle with nothing left to compact"
        );
    }

    let final_manifest = load_manifest_state_for_inspection(
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
async fn a_floor_past_a_snapshot_keeps_its_manifest_and_runs_readable_until_deletion() {
    let directory = tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let namespace_id = NamespaceId::parse("retained-snapshot").expect("namespace");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .write_file_bytes("/first", b"first", None)
        .await
        .expect("first write");
    let snapshot = create_snapshot(&store, &namespace_id, &context)
        .await
        .expect("snapshot");
    let snapshot_files = read_snapshot_files(&store, &namespace_id, &snapshot.snapshot_id)
        .await
        .expect("snapshot files");
    let snapshot_segments =
        super::segment_keys_of_the_current_manifest(&store, &namespace_id).await;
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .write_file_bytes("/second", b"second", None)
        .await
        .expect("second write");
    crate::test_support::namespace_store(&store, &namespace_id)
        .fold_wal()
        .await
        .expect("fold");
    let current_segments = super::compact_a_family_group(&store, &namespace_id).await;
    let only_snapshot: Vec<_> = snapshot_segments
        .difference(&current_segments)
        .cloned()
        .collect();
    assert!(!only_snapshot.is_empty());
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
    assert!(floor.retention_floor_seq > snapshot.captured_seq);
    let aged = writer_at("gc", u64::MAX / 2);
    crate::test_support::namespace_store(&store, &namespace_id)
        .gc(&aged.clock())
        .await
        .expect("collect past the floor");
    assert_eq!(
        read_snapshot_files(&store, &namespace_id, &snapshot.snapshot_id)
            .await
            .expect("snapshot readable"),
        snapshot_files
    );
    for key in &snapshot_segments {
        assert!(store.head(key).await.expect("snapshot segment").is_some());
    }
    let manifest_key = metadata_manifest_object(&namespace_id, &snapshot.manifest_no);
    assert!(store
        .head(&manifest_key)
        .await
        .expect("snapshot's manifest")
        .is_some());
    crate::test_support::NamespaceFixture::new(
        &store,
        &namespace_id,
        &crate::test_support::writer_at("test-writer", 0),
    )
    .engine()
    .delete_snapshot(&snapshot.snapshot_id)
    .await
    .expect("delete snapshot");
    // The first collection deletes the snapshot's record, and the second
    // deletes the manifest and segments that only the snapshot kept.
    for _ in 0..2 {
        crate::test_support::namespace_store(&store, &namespace_id)
            .gc(&aged.clock())
            .await
            .expect("collect deleted basis");
    }
    assert!(store
        .head(&manifest_key)
        .await
        .expect("deleted manifest")
        .is_none());
    for key in &only_snapshot {
        assert!(store.head(key).await.expect("deleted segment").is_none());
    }
}
