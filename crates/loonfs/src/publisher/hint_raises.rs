//! The hint raises that follow this runtime's manifest publications: how the
//! raises of one namespace combine, and which publications are followed by a
//! raise.

use super::*;
use crate::InlineContentPolicy;
use loonfs_test_support::stores::{Intercept, InterceptStore, Interceptor, MemoryStore};
use loonfs_types::format::control::{
    decode_control_object, encode_control_state, ControlObjectKind, HintPayload,
};
use loonfs_types::{ChecksumAlgorithm, ManifestNo};
use std::collections::HashMap;

/// Paces compare-and-swap writes to each object as the configured store
/// does, on the test clock, and records when each write to a hint started
/// and the manifest number it wrote.
#[derive(Debug)]
struct PacedSwaps {
    origin: tokio::time::Instant,
    state: Mutex<PacedSwapState>,
}

#[derive(Debug, Default)]
struct PacedSwapState {
    started_at: HashMap<String, tokio::time::Instant>,
    /// The milliseconds from the origin to each write's start, and the
    /// manifest number that the write sent.
    swaps: Vec<(u64, ManifestNo)>,
}

impl PacedSwaps {
    #[allow(
        clippy::disallowed_methods,
        reason = "the test store paces writes on the paused test clock"
    )]
    fn new() -> Self {
        Self {
            origin: tokio::time::Instant::now(),
            state: Mutex::default(),
        }
    }

    /// Records a write of `key` as started and returns `None`, or returns
    /// how long until one write interval has passed since the latest start.
    #[allow(
        clippy::disallowed_methods,
        reason = "the test store paces writes on the paused test clock"
    )]
    fn try_start(&self, key: &str, manifest_no: ManifestNo) -> Option<Duration> {
        let now = tokio::time::Instant::now();
        let interval = Duration::from_millis(loonfs_objectstore::MIN_CAS_WRITE_INTERVAL_MS);
        let mut state = self.state.lock().expect("pacing state");
        if let Some(started_at) = state.started_at.get(key) {
            if now < *started_at + interval {
                return Some(*started_at + interval - now);
            }
        }
        state.started_at.insert(key.to_owned(), now);
        let elapsed_ms = u64::try_from((now - self.origin).as_millis()).expect("a short test");
        state.swaps.push((elapsed_ms, manifest_no));
        None
    }

    fn swaps(&self) -> Vec<(u64, ManifestNo)> {
        self.state.lock().expect("pacing state").swaps.clone()
    }
}

#[async_trait]
impl Interceptor for PacedSwaps {
    async fn before(&self, context: &OperationContext<'_>) -> Intercept {
        let OperationKind::CompareAndSwap { bytes, .. } = context.kind() else {
            return Intercept::Continue;
        };
        let manifest_no = decode_control_object::<HintPayload>(bytes, ControlObjectKind::Hint)
            .expect("a hint write")
            .payload()
            .manifest_no;
        while let Some(wait) = self.try_start(context.key(), manifest_no) {
            pause(wait).await;
        }
        Intercept::Continue
    }
}

#[allow(
    clippy::disallowed_methods,
    reason = "the raiser tests wait on the paused test clock"
)]
async fn pause(wait: Duration) {
    tokio::time::sleep(wait).await;
}

type PacedStore = InterceptStore<FailStore<MemoryStore>, PacedSwaps>;

/// A raiser over a store whose hint is at manifest 1 and whose hint writes
/// can be made to fail.
struct RaiserFixture {
    store: Arc<PacedStore>,
    raiser: HintRaiser,
    namespace_id: NamespaceId,
}

impl RaiserFixture {
    async fn new() -> Self {
        let namespace_id = NamespaceId::parse("raised").expect("namespace id");
        let store = Arc::new(InterceptStore::with_interceptor(
            FailStore::new(
                MemoryStore::new(ChecksumAlgorithm::Crc64nvme),
                KeyPredicate::hint(&namespace_id),
                OperationSelector::CompareAndSwap,
                InjectedError::Transport("hint unavailable".to_owned()),
            ),
            PacedSwaps::new(),
        ));
        let hint = HintPayload {
            namespace_id: namespace_id.clone(),
            manifest_no: ManifestNo(1),
        };
        store
            .put_if_absent(
                &loonfs_objectstore::keys::hint(&namespace_id),
                encode_control_state(ControlObjectKind::Hint, &hint)
                    .expect("encode the hint")
                    .into(),
            )
            .await
            .expect("create the hint");
        let raiser = HintRaiser::new(
            store.clone(),
            Arc::new(loonfs_types::StdMonotonicTimer::default()),
            tokio::runtime::Handle::current(),
        );
        Self {
            store,
            raiser,
            namespace_id,
        }
    }

    fn raise_to(&self, manifest_no: u64) {
        self.raiser
            .raise_to(&self.namespace_id, ManifestNo(manifest_no));
    }

    /// Returns the milliseconds from the fixture's creation to each hint
    /// write's start, and the manifest number that each write sent.
    fn swaps(&self) -> Vec<(u64, u64)> {
        self.store
            .interceptor()
            .swaps()
            .into_iter()
            .map(|(at_ms, manifest_no)| (at_ms, manifest_no.0))
            .collect()
    }

    async fn hint(&self) -> u64 {
        hint_manifest_no(self.store.as_ref(), &self.namespace_id)
            .await
            .0
    }
}

/// Checks that raises requested before the namespace's raise starts share
/// one write, to the newest manifest number.
#[tokio::test(start_paused = true)]
async fn raises_requested_together_share_one_write_to_the_newest_number() {
    let fixture = RaiserFixture::new().await;
    for manifest_no in [2, 4, 3] {
        fixture.raise_to(manifest_no);
    }
    fixture.raiser.wait_for_raises().await;
    assert_eq!(fixture.swaps(), [(0, 4)]);
    assert_eq!(fixture.hint().await, 4);
}

/// Checks that publications made while a raise waits for its write interval
/// move the target, so the next raise writes the newest of their manifests,
/// and that no third raise follows.
#[tokio::test(start_paused = true)]
async fn a_target_that_moves_while_a_raise_waits_takes_one_more_raise() {
    let fixture = RaiserFixture::new().await;
    fixture.raise_to(2);
    fixture.raiser.wait_for_raises().await;
    fixture.raise_to(3);
    pause(Duration::from_millis(100)).await;
    fixture.raise_to(4);
    pause(Duration::from_millis(100)).await;
    fixture.raise_to(5);
    fixture.raiser.wait_for_raises().await;
    assert_eq!(fixture.swaps(), [(0, 2), (1_000, 3), (2_000, 5)]);
    assert_eq!(fixture.hint().await, 5);
}

/// Checks that a raise whose write loses to another process's raise loads
/// the hint and writes the newest number over it.
#[tokio::test(start_paused = true)]
async fn a_lost_write_loads_the_hint_and_still_raises_it_to_the_newest_number() {
    let fixture = RaiserFixture::new().await;
    fixture.raise_to(2);
    fixture.raiser.wait_for_raises().await;
    fixture.raise_to(4);
    pause(Duration::from_millis(500)).await;
    // Another process raises the hint to 3 while the raise to 4 waits.
    loonfs_core::control::raise_namespace_hint(
        fixture.store.inner().inner(),
        &fixture.namespace_id,
        ManifestNo(3),
        None,
        loonfs_core::test_support::std_timer(),
    )
    .await
    .expect("a rival raise");
    fixture.raiser.wait_for_raises().await;
    assert_eq!(fixture.swaps(), [(0, 2), (1_000, 4), (2_000, 4)]);
    assert_eq!(fixture.hint().await, 4);
}

/// Checks that a failed raise ends its task without writing again, and that
/// the next publication starts a new raise.
#[tokio::test(start_paused = true)]
async fn a_failed_raise_ends_its_task_and_the_next_publication_raises_the_hint() {
    let fixture = RaiserFixture::new().await;
    fixture.store.inner().fail_next(1);
    fixture.raise_to(2);
    fixture.raiser.wait_for_raises().await;
    assert_eq!(fixture.swaps(), [(0, 2)]);
    assert_eq!(fixture.hint().await, 1);
    fixture.raise_to(3);
    fixture.raiser.wait_for_raises().await;
    assert_eq!(fixture.swaps(), [(0, 2), (1_000, 3)]);
    assert_eq!(fixture.hint().await, 3);
}

/// Checks that a drain returns while a raise waits for its write interval,
/// and leaves the raise to finish on its own.
#[tokio::test(start_paused = true)]
async fn a_drain_returns_while_a_hint_raise_is_pending() {
    let fixture = RaiserFixture::new().await;
    let writer = test_writer(fixture.store.clone()).await;
    let raiser = &writer.mode.publisher.shared.hint_raiser;
    raiser.raise_to(&fixture.namespace_id, ManifestNo(2));
    raiser.wait_for_raises().await;
    raiser.raise_to(&fixture.namespace_id, ManifestNo(3));
    pause(Duration::from_millis(100)).await;
    writer.drain().await.expect("drain");
    assert_eq!(fixture.swaps(), [(0, 2)]);
    assert_eq!(fixture.hint().await, 2);
    raiser.wait_for_raises().await;
    assert_eq!(fixture.swaps(), [(0, 2), (1_000, 3)]);
    assert_eq!(fixture.hint().await, 3);
}

/// Checks that a shutdown waits for a raise that waits for its write
/// interval.
#[tokio::test(start_paused = true)]
async fn a_shutdown_waits_for_a_pending_hint_raise() {
    let fixture = RaiserFixture::new().await;
    let writer = test_writer(fixture.store.clone()).await;
    let raiser = &writer.mode.publisher.shared.hint_raiser;
    raiser.raise_to(&fixture.namespace_id, ManifestNo(2));
    raiser.wait_for_raises().await;
    raiser.raise_to(&fixture.namespace_id, ManifestNo(3));
    let mut shutdown = std::pin::pin!(writer.shutdown());
    assert!(futures::poll!(shutdown.as_mut()).is_pending());
    shutdown.await.expect("shutdown");
    assert_eq!(fixture.swaps(), [(0, 2), (1_000, 3)]);
    assert_eq!(fixture.hint().await, 3);
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
    Snapshot,
    Fork,
    Deletion,
}

/// Checks that each kind of manifest publication leaves the namespace's hint
/// at the manifest it published, once the runtime's hint raises finish.
#[tokio::test]
async fn every_publication_raises_the_hint_to_its_manifest() {
    let directory = tempdir().expect("tempdir");
    let store: SharedStore = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
    let clock = Arc::new(ManualClock::at_system_time());
    // A commit with file bytes is over the inline fold trigger, and a
    // directory commit isn't.
    let writer = crate::LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("writer-a"))
        .min_publish_interval_ms(0)
        .wall_clock(clock.clone())
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
            clock.advance_ms(crate::GC_DEFAULT_GRACE_WINDOW_MS);
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
        assert!(
            current_manifest_no(store.as_ref(), &namespace_id).await > before,
            "{publication:?} publishes a manifest"
        );
        assert_hint_at_current_manifest(&writer, store.as_ref(), &namespace_id).await;
    }
    writer.shutdown().await.expect("shut down");
}

/// Checks that an explicit fold returns while its hint raise is parked, and
/// that the fold's own requests include no write to the hint.
#[tokio::test]
async fn a_fold_returns_before_its_hint_raise() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("unraised").expect("namespace id");
    let store = Arc::new(BlockingStore::new(
        RecordingStore::new(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::hint(&namespace_id),
        ),
        KeyPredicate::hint(&namespace_id),
        OperationSelector::CompareAndSwap,
    ));
    let writer = test_writer_with_interval(store.clone(), 0).await;
    let _namespace = publish_once_into_each(&writer, std::slice::from_ref(&namespace_id)).await;
    writer.drain().await.expect("drain");
    writer.mode.publisher.wait_for_hint_raises().await;
    let raised = hint_manifest_no(store.as_ref(), &namespace_id).await;
    store.inner().reset();
    store.arm();
    let folded = bounded(
        Duration::from_secs(10),
        writer
            .maintenance(loonfs_test_support::ids::writer_id("writer-a"))
            .fold_wal(&namespace_id),
    )
    .await
    .expect("the fold doesn't wait for its hint raise")
    .expect("fold");
    assert_eq!(folded.outcome, FoldWalOutcome::Published);
    store.wait_until_blocked().await;
    assert_eq!(store.inner().counts().compare_and_swaps, 0);
    assert_eq!(
        hint_manifest_no(store.as_ref(), &namespace_id).await,
        raised
    );
    store.release();
    assert_hint_at_current_manifest(&writer, store.as_ref(), &namespace_id).await;
    assert_eq!(store.inner().counts().compare_and_swaps, 1);
    writer.shutdown().await.expect("shut down");
}
