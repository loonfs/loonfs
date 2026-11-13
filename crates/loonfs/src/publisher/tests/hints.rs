//! Tests for the hint raises that follow this runtime's manifest
//! publications. Every publication raises the hint once, and a raise that
//! fails or loses its compare-and-swap doesn't change the publication's
//! result.

use super::*;
use loonfs_types::ManifestNo;
use std::num::NonZeroU64;

/// The store under a raise fixture. It parks the next compare-and-swap write
/// of the hint when a test asks it to, records the requests for the hint,
/// and fails the compare-and-swap writes of the hint that a test asks it to
/// fail.
type RaiseStore = BlockingStore<RecordingStore<FailStore<LocalFsStore>>>;

/// A writer with one namespace that it created and opened.
struct RaiseFixture {
    _directory: tempfile::TempDir,
    store: Arc<RaiseStore>,
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
            OperationSelector::CompareAndSwap,
        ));
        let writer = crate::LoonFs::builder_with_store(store.clone() as SharedStore)
            .writer_id(loonfs_test_support::ids::writer_id("writer-a"))
            .config(crate::RuntimeConfig {
                min_publish_interval_ms: 0,
                ..crate::RuntimeConfig::default()
            })
            .monotonic_timer(Arc::new(ManualClock::new(0)))
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

    /// Folds the session's tail with an explicit fold, and returns the fold's
    /// manifest number. The test fails if the fold doesn't return within ten
    /// seconds.
    async fn fold(&self) -> u64 {
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

    /// Commits, and then folds the commit, and returns the fold's manifest
    /// number.
    async fn commit_and_fold(&mut self) -> u64 {
        self.commit().await;
        self.fold().await
    }

    /// Returns the local store that sits under the gate, the request log,
    /// and the injected failures.
    fn local_store(&self) -> &LocalFsStore {
        self.store.inner().inner().inner()
    }

    /// Returns the hint's manifest number, read directly from the local
    /// store so that the read bypasses the gate and the request log.
    async fn hint(&self) -> u64 {
        hint_manifest_no(self.local_store(), &self.namespace_id)
            .await
            .0
    }

    /// Returns how many compare-and-swap writes of the hint the store has
    /// recorded, failed ones included.
    fn hint_writes(&self) -> usize {
        self.store.inner().counts().compare_and_swaps
    }
}

/// Checks that publications of one namespace in quick succession each raise
/// the hint with one compare-and-swap write, so the hint records the newest
/// manifest after each publication.
#[tokio::test]
async fn publications_in_quick_succession_each_raise_the_hint() {
    let mut fixture = RaiseFixture::new().await;
    fixture.commit().await;
    fixture.writer.drain().await.expect("drain");
    assert_eq!(fixture.hint().await, 2, "the claim raises the hint");
    assert_eq!(fixture.hint_writes(), 1);
    assert_eq!(fixture.commit_and_fold().await, 3);
    assert_eq!(fixture.hint().await, 3);
    assert_eq!(fixture.hint_writes(), 2);
    assert_eq!(fixture.commit_and_fold().await, 4);
    assert_eq!(fixture.hint().await, 4);
    assert_eq!(fixture.hint_writes(), 3);
    fixture.writer.shutdown().await.expect("shut down");
}

/// Checks that a failed raise doesn't fail the publication that started it
/// and isn't retried, so the hint stays behind until the next publication
/// raises it.
#[tokio::test]
async fn a_failed_raise_is_ignored_and_the_next_publication_raises_the_hint() {
    let mut fixture = RaiseFixture::new().await;
    fixture.store.inner().inner().fail_next(1);
    fixture.commit().await;
    fixture.writer.drain().await.expect("drain");
    assert_eq!(fixture.hint().await, 1, "the claim's raise failed");
    assert_eq!(fixture.hint_writes(), 1, "the failed raise isn't retried");
    assert_eq!(fixture.commit_and_fold().await, 3);
    assert_eq!(fixture.hint().await, 3);
    assert_eq!(fixture.hint_writes(), 2);
    fixture.writer.shutdown().await.expect("shut down");
}

/// Checks that a raise whose compare-and-swap loses to another raise of the
/// same hint doesn't change the publication's result, isn't retried, and
/// leaves the hint as the other raise wrote it.
#[tokio::test]
async fn a_raise_that_loses_its_swap_leaves_the_publication_and_the_other_raise() {
    let mut fixture = RaiseFixture::new().await;
    fixture.commit().await;
    fixture.writer.drain().await.expect("drain");
    assert_eq!(fixture.hint().await, 2);
    fixture.commit().await;
    // The gate parks the fold's raise after it has read the hint, and the
    // other raise writes the hint while the fold's write waits.
    fixture.store.block_next();
    let other_raise = async {
        fixture.store.wait_until_blocked().await;
        let raised = loonfs_core::test_support::namespace_store(
            fixture.local_store(),
            &fixture.namespace_id,
        )
        .raise_hint(ManifestNo(3), None)
        .await
        .expect("the other raise");
        fixture.store.release();
        raised
    };
    let (folded, raised) = tokio::join!(fixture.fold(), other_raise);
    assert_eq!(folded, 3, "the lost raise leaves the fold's result");
    assert_eq!(fixture.hint().await, 3);
    let hint = fixture
        .local_store()
        .head(&loonfs_objectstore::keys::hint(&fixture.namespace_id))
        .await
        .expect("head the hint")
        .expect("the hint exists");
    assert_eq!(
        hint.etag.as_deref(),
        Some(raised.etag.as_str()),
        "the hint is the version that the other raise wrote"
    );
    assert_eq!(fixture.hint_writes(), 2, "the lost raise isn't retried");
    fixture.writer.shutdown().await.expect("shut down");
}

/// Checks that a reader that starts at a hint that lags, and probes forward
/// to the newer manifest, doesn't write the hint.
#[tokio::test]
async fn a_reader_never_writes_the_hint() {
    let mut fixture = RaiseFixture::new().await;
    fixture.commit().await;
    fixture.writer.drain().await.expect("drain");
    // The fold's raise fails, so the hint stays at the claim's manifest.
    fixture.store.inner().inner().fail_next(1);
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

/// A kind of manifest publication that
/// `every_publication_raises_the_hint_to_its_manifest` checks.
#[derive(Debug, Clone, Copy)]
enum Publication {
    /// The session's first commit, which claims a writer epoch.
    WriterClaim,
    ExplicitFold,
    /// The session's own fold, which a commit over the fold trigger starts.
    TriggerFold,
    RetentionAdvance,
    Snapshot,
    Fork,
    Deletion,
}

/// Checks that each kind of manifest publication raises the namespace's
/// hint to the manifest it published.
#[tokio::test]
async fn every_publication_raises_the_hint_to_its_manifest() {
    let directory = tempdir().expect("tempdir");
    let store: SharedStore = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
    let clock = Arc::new(ManualClock::at_system_time());
    // The claim's fence and the seed commit leave two WAL objects, so the
    // third object, the put's, starts the session's own fold.
    let writer = crate::LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("writer-a"))
        .session_fold_at_wal_objects(NonZeroU64::new(3).expect("three WAL objects"))
        .config(crate::RuntimeConfig {
            min_publish_interval_ms: 0,
            ..crate::RuntimeConfig::default()
        })
        .wall_clock(clock.clone())
        .monotonic_timer(Arc::new(ManualClock::new(0)))
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
        if matches!(publication, Publication::RetentionAdvance) {
            maintenance.fold_wal(&namespace_id).await.expect("fold");
            clock.advance_ms(crate::GC_GRACE_WINDOW_MS);
        }
        writer.drain().await.expect("drain");
        let before = current_manifest_no(store.as_ref(), &namespace_id).await;
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
            Publication::Snapshot => {
                namespace
                    .create_snapshot_with_options(&crate::CreateSnapshotOptions {
                        name: Some("snapshot".to_owned()),
                        ..crate::CreateSnapshotOptions::default()
                    })
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
        writer.drain().await.expect("drain");
        assert_eq!(
            hint_manifest_no(store.as_ref(), &namespace_id).await,
            current,
            "{publication:?} raises the hint"
        );
    }
    writer.shutdown().await.expect("shut down");
}
