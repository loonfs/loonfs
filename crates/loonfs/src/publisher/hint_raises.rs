//! The hint raises that follow this runtime's manifest publications: which
//! publications raise the hint, and how often.

use super::*;
use crate::InlineContentPolicy;
use loonfs_core::limits::HINT_RAISE_INTERVAL_MS;

/// The store under a raise fixture: it parks the requests for the hint once
/// a test arms it, records the requests for the hint, and fails the
/// compare-and-swap writes of the hint that a test asks it to fail.
type RaiseStore = BlockingStore<RecordingStore<FailStore<LocalFsStore>>>;

/// A writer whose monotonic timer moves only when a test moves it, with one
/// namespace that it created and opened.
struct RaiseFixture {
    _directory: tempfile::TempDir,
    store: Arc<RaiseStore>,
    timer: Arc<ManualClock>,
    writer: crate::LoonFs<crate::Writable>,
    namespace_id: NamespaceId,
    namespace: crate::Namespace<crate::Writable>,
    commits: usize,
}

impl RaiseFixture {
    async fn new() -> Self {
        let directory = tempdir().expect("tempdir");
        let namespace_id = NamespaceId::parse("raised").expect("namespace id");
        let store = Arc::new(BlockingStore::new(
            RecordingStore::new(
                FailStore::new(
                    LocalFsStore::new(directory.path()).expect("store"),
                    KeyPredicate::hint(&namespace_id),
                    OperationSelector::CompareAndSwap,
                    InjectedError::Transport("hint unavailable".to_owned()),
                ),
                KeyPredicate::hint(&namespace_id),
            ),
            KeyPredicate::hint(&namespace_id),
            OperationSelector::Any,
        ));
        let timer = Arc::new(ManualClock::new(0));
        let writer = crate::LoonFs::builder_with_store(store.clone() as SharedStore)
            .writer_id(loonfs_test_support::ids::writer_id("writer-a"))
            .min_publish_interval_ms(0)
            .monotonic_timer(timer.clone())
            .build()
            .await
            .expect("build writer");
        writer
            .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
            .await
            .expect("create the namespace");
        let namespace = writer
            .open_namespace(&namespace_id)
            .expect("open the namespace");
        Self {
            _directory: directory,
            store,
            timer,
            writer,
            namespace_id,
            namespace,
            commits: 0,
        }
    }

    /// Commits a new directory. The session's first commit publishes the
    /// claim of its writer epoch, and no other commit publishes a manifest.
    async fn commit(&mut self) {
        let name = format!("directory-{}", self.commits);
        self.commits += 1;
        self.namespace
            .commit_candidate(CommitCandidate::new(create_directory_request(&name, &name)))
            .await
            .expect("commit");
    }

    /// Commits, and then folds the commit with an explicit fold, which
    /// returns within ten seconds, and returns the fold's manifest number.
    async fn commit_and_fold(&mut self) -> u64 {
        self.commit().await;
        let folded = bounded(
            Duration::from_secs(10),
            self.writer
                .maintenance(loonfs_test_support::ids::writer_id("writer-a"))
                .fold_wal(&self.namespace_id),
        )
        .await
        .expect("the fold returns")
        .expect("fold");
        assert_eq!(folded.outcome, FoldWalOutcome::Published);
        folded.manifest_no.0
    }

    /// Returns the hint's manifest number, read past the gate and the
    /// request log.
    async fn hint(&self) -> u64 {
        hint_manifest_no(self.store.inner().inner().inner(), &self.namespace_id)
            .await
            .0
    }

    /// Returns how many compare-and-swap writes of the hint the store has
    /// recorded, failed ones included.
    fn hint_writes(&self) -> usize {
        self.store.inner().counts().compare_and_swaps
    }
}

/// Checks that a publication less than `HINT_RAISE_INTERVAL_MS` after the
/// runtime's last raise of the namespace's hint returns without a request
/// for the hint, so it doesn't wait for a raise, and that the first
/// publication an interval after that raise raises the hint to its own
/// manifest.
#[tokio::test]
async fn publications_less_than_an_interval_apart_raise_the_hint_once() {
    let mut fixture = RaiseFixture::new().await;
    fixture.commit().await;
    fixture.writer.drain().await.expect("drain");
    assert_eq!(fixture.hint().await, 2, "the claim raises the hint");
    assert_eq!(fixture.hint_writes(), 1);
    // The gate parks every request for the hint, so a fold that read or
    // wrote the hint wouldn't return.
    fixture.store.arm();
    assert_eq!(fixture.commit_and_fold().await, 3);
    fixture.timer.advance_ms(HINT_RAISE_INTERVAL_MS - 1);
    assert_eq!(fixture.commit_and_fold().await, 4);
    assert_eq!(fixture.hint().await, 2);
    fixture.store.release();
    fixture.timer.advance_ms(1);
    assert_eq!(fixture.commit_and_fold().await, 5);
    assert_eq!(fixture.hint().await, 5);
    assert_eq!(fixture.hint_writes(), 2);
    fixture.writer.shutdown().await.expect("shut down");
}

/// Checks that a failed raise doesn't fail the publication that started it,
/// that it counts as the interval's raise, and that the first publication
/// an interval later raises the hint.
#[tokio::test]
async fn a_failed_raise_is_ignored_and_a_publication_an_interval_later_raises_the_hint() {
    let mut fixture = RaiseFixture::new().await;
    fixture.store.inner().inner().fail_next(1);
    fixture.commit().await;
    fixture.writer.drain().await.expect("drain");
    assert_eq!(fixture.hint().await, 1, "the claim's raise failed");
    assert_eq!(fixture.commit_and_fold().await, 3);
    assert_eq!(fixture.hint().await, 1);
    assert_eq!(fixture.hint_writes(), 1);
    fixture.timer.advance_ms(HINT_RAISE_INTERVAL_MS);
    assert_eq!(fixture.commit_and_fold().await, 4);
    assert_eq!(fixture.hint().await, 4);
    assert_eq!(fixture.hint_writes(), 2);
    fixture.writer.shutdown().await.expect("shut down");
}

/// Checks that a reader that discovers the namespace past a hint that lags
/// leaves the hint where it was.
#[tokio::test]
async fn a_reader_never_writes_the_hint() {
    let mut fixture = RaiseFixture::new().await;
    fixture.commit().await;
    assert_eq!(fixture.commit_and_fold().await, 3);
    fixture.writer.drain().await.expect("drain");
    assert_eq!(fixture.hint().await, 2);
    let reader = crate::LoonFs::builder_with_store(fixture.store.clone() as SharedStore)
        .read_only()
        .build()
        .await
        .expect("build reader");
    fixture.store.inner().reset();
    reader
        .namespace(&fixture.namespace_id)
        .stat("/directory-1")
        .await
        .expect("the reader finds the folded directory");
    assert_eq!(fixture.hint_writes(), 0);
    assert_eq!(fixture.hint().await, 2);
    fixture.writer.shutdown().await.expect("shut down");
}

/// A publication that a coverage case makes after its namespace's first
/// commit.
#[derive(Debug, Clone, Copy)]
enum Publication {
    /// The session's first commit, which claims a writer epoch.
    WriterClaim,
    ExplicitFold,
    /// The session's own fold, which a commit over the fold trigger starts.
    TriggerFold,
    RetentionAdvance,
    /// A maintainer's claim, fence, and fold, which acknowledge a retention
    /// floor that the writer committed nothing against.
    FloorAcknowledgment,
    Snapshot,
    Fork,
    Deletion,
}

/// Checks that each kind of manifest publication raises the namespace's
/// hint to the manifest it published when the runtime started no raise of
/// that hint in the last `HINT_RAISE_INTERVAL_MS`.
#[tokio::test]
async fn every_publication_raises_the_hint_to_its_manifest() {
    let directory = tempdir().expect("tempdir");
    let store: SharedStore = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
    let clock = Arc::new(ManualClock::at_system_time());
    let timer = Arc::new(ManualClock::new(0));
    // A commit with file bytes is over the inline fold trigger, and a
    // directory commit isn't.
    let writer = crate::LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("writer-a"))
        .min_publish_interval_ms(0)
        .wall_clock(clock.clone())
        .monotonic_timer(timer.clone())
        .inline_content(InlineContentPolicy {
            inline_content_fold_at_bytes: 1,
            ..InlineContentPolicy::default()
        })
        .build()
        .await
        .expect("build writer");
    let maintenance = writer.maintenance(loonfs_test_support::ids::writer_id("writer-a"));
    let actor = loonfs_test_support::test_actor();
    for publication in [
        Publication::WriterClaim,
        Publication::ExplicitFold,
        Publication::TriggerFold,
        Publication::RetentionAdvance,
        Publication::FloorAcknowledgment,
        Publication::Snapshot,
        Publication::Fork,
        Publication::Deletion,
    ] {
        let name = format!("{publication:?}").to_lowercase();
        let namespace_id = NamespaceId::parse(&name).expect("namespace id");
        writer
            .create_namespace(&namespace_id, &actor)
            .await
            .expect("create the namespace");
        let namespace = writer
            .open_namespace(&namespace_id)
            .expect("open the namespace");
        let commit = || {
            namespace.commit_candidate(CommitCandidate::new(create_directory_request(
                "seed", "docs",
            )))
        };
        if !matches!(publication, Publication::WriterClaim) {
            commit().await.expect("commit");
        }
        if matches!(
            publication,
            Publication::RetentionAdvance | Publication::FloorAcknowledgment
        ) {
            maintenance.fold_wal(&namespace_id).await.expect("fold");
            clock.advance_ms(crate::GC_DEFAULT_GRACE_WINDOW_MS);
        }
        if matches!(publication, Publication::FloorAcknowledgment) {
            maintenance
                .advance_retention_floor(&namespace_id)
                .await
                .expect("advance the floor");
        }
        writer.drain().await.expect("drain");
        let before = current_manifest_no(store.as_ref(), &namespace_id).await;
        timer.advance_ms(HINT_RAISE_INTERVAL_MS);
        match publication {
            Publication::WriterClaim => {
                commit().await.expect("commit");
            }
            Publication::ExplicitFold => {
                maintenance.fold_wal(&namespace_id).await.expect("fold");
            }
            Publication::TriggerFold => {
                namespace
                    .put_file("/file", b"body", &actor)
                    .await
                    .expect("put a file");
                namespace.wait_for_fold().await.expect("fold");
                // The fold starts the session's compaction.
                writer.drain().await.expect("drain");
            }
            Publication::RetentionAdvance => {
                maintenance
                    .advance_retention_floor(&namespace_id)
                    .await
                    .expect("advance the floor");
            }
            Publication::FloorAcknowledgment => {
                let acknowledged = maintenance
                    .acknowledge_retention_floor(&namespace_id)
                    .await
                    .expect("acknowledge the floor");
                assert_eq!(
                    acknowledged.outcome,
                    crate::AcknowledgeRetentionFloorOutcome::Fenced
                );
            }
            Publication::Snapshot => {
                namespace
                    .create_snapshot("snapshot", &crate::CreateSnapshotOptions::default())
                    .await
                    .expect("create a snapshot");
            }
            Publication::Fork => {
                let target = NamespaceId::parse(format!("{name}-target")).expect("target id");
                writer
                    .fork_namespace(&namespace_id, &target, &actor)
                    .await
                    .expect("fork");
                assert_hint_at_current_manifest(&writer, store.as_ref(), &target).await;
            }
            Publication::Deletion => {
                namespace.delete().await.expect("delete");
            }
        }
        let current = current_manifest_no(store.as_ref(), &namespace_id).await;
        assert!(current > before, "{publication:?} publishes a manifest");
        // The acknowledgment's claim raises the hint, and its fold of the
        // fence, less than an interval later, skips its raise.
        let raised = match publication {
            Publication::FloorAcknowledgment => {
                before.successor().expect("the claim's manifest number")
            }
            _ => current,
        };
        writer.drain().await.expect("drain");
        assert_eq!(
            hint_manifest_no(store.as_ref(), &namespace_id).await,
            raised,
            "{publication:?} raises the hint"
        );
    }
    writer.shutdown().await.expect("shut down");
}
