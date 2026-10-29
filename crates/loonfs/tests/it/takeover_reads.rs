//! Reads in a former writer's runtime after another writer takes over the
//! namespace while the former writer's folds and publishes are in flight.

use loonfs::{
    ErrorCode, FoldWalOutcome, InlineContentPolicy, LoonFs, Namespace, NamespaceSessionStatus,
    ReadOnly, SharedObjectStore, Writable,
};
use loonfs_objectstore::keys::{metadata_manifest_prefix, wal_prefix};
use loonfs_objectstore::PutMode;
use loonfs_test_support::clock::bounded;
use loonfs_test_support::ids::{namespace_id, writer_id};
use loonfs_test_support::stores::{
    BlockingStore, KeyPredicate, MemoryStore, OperationContext, OperationKind, RecordedOperation,
    RecordingStore,
};
use loonfs_types::format::manifest::decode_namespace_manifest_json;
use loonfs_types::format::wal::decode_wal_object_envelope_zstd;
use loonfs_types::{ChecksumAlgorithm, ManifestNo, NamespaceId};
use std::sync::Arc;
use std::time::Duration;

/// How long a step that the test unblocks gets before the test fails.
const STEP: Duration = Duration::from_secs(10);

type Gate = Arc<BlockingStore<SharedObjectStore>>;

/// Selects the requests that a gate parks.
type Selector = Box<dyn Fn(&OperationContext<'_>) -> bool + Send + Sync>;

/// Selects a put-if-absent of a manifest that folds WAL objects.
fn fold_manifest_put(namespace_id: &NamespaceId) -> impl Fn(&OperationContext<'_>) -> bool {
    let prefix = metadata_manifest_prefix(namespace_id);
    move |operation| {
        operation.key().starts_with(&prefix)
            && matches!(
                operation.kind(),
                OperationKind::Put { bytes, mode: PutMode::CreateIfAbsent }
                    if decode_namespace_manifest_json(bytes)
                        .is_ok_and(|manifest| manifest.payload().folded_wal_no.0 > 0)
            )
    }
}

/// Selects a put-if-absent of a WAL object with commits, which leaves out
/// a writer's fence.
fn data_wal_put(namespace_id: &NamespaceId) -> impl Fn(&OperationContext<'_>) -> bool {
    let prefix = wal_prefix(namespace_id);
    move |operation| {
        operation.key().starts_with(&prefix)
            && matches!(
                operation.kind(),
                OperationKind::Put { bytes, mode: PutMode::CreateIfAbsent }
                    if decode_wal_object_envelope_zstd(bytes)
                        .is_ok_and(|wal| !wal.payload().records.is_empty())
            )
    }
}

/// The former writer A, whose runtime reads through a gate, over a store
/// that a later writer B shares.
struct Takeover {
    namespace_id: NamespaceId,
    store: SharedObjectStore,
    gate: Gate,
    /// Records the requests of A's runtime.
    recording: Arc<RecordingStore<Gate>>,
    writer: LoonFs<Writable>,
    namespace: Namespace<Writable>,
    reader: Namespace<ReadOnly>,
}

/// Creates writer A with a manifest check interval and a maximum staleness of
/// 0, so each of A's reads checks for a newer manifest, and a namespace with
/// one commit, so A's session has a position.
///
/// A's gate parks the next request that `parked` selects once a test calls
/// `block_next`. `inline_content` sets A's fold limits.
async fn takeover(
    name: &str,
    parked: fn(&NamespaceId) -> Selector,
    inline_content: InlineContentPolicy,
) -> Takeover {
    let namespace_id = namespace_id(name);
    let store: SharedObjectStore = Arc::new(MemoryStore::new(ChecksumAlgorithm::Crc64nvme));
    let gate = Arc::new(BlockingStore::matching(
        store.clone(),
        parked(&namespace_id),
    ));
    let recording = Arc::new(RecordingStore::new(gate.clone(), KeyPredicate::any()));
    let recorded: SharedObjectStore = recording.clone();
    let writer = LoonFs::builder_with_store(recorded)
        .writer_id(writer_id(name))
        .config(loonfs::RuntimeConfig {
            min_publish_interval_ms: 0,
            manifest_revalidation_interval_ms: 0,
            max_staleness_ms: 0,
            inline_content,
            ..loonfs::RuntimeConfig::default()
        })
        .build()
        .await
        .expect("writer A");
    let actor = loonfs_test_support::test_actor();
    writer
        .create_namespace(&namespace_id, &actor)
        .await
        .expect("namespace");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    namespace
        .create_directory("/a", &actor)
        .await
        .expect("A's first commit");
    Takeover {
        reader: writer.read_only().namespace(&namespace_id),
        namespace_id,
        store,
        gate,
        recording,
        writer,
        namespace,
    }
}

impl Takeover {
    /// Opens writer B over the shared store, or over `gate` when given.
    async fn peer(&self, gate: Option<Gate>) -> LoonFs<Writable> {
        let store: SharedObjectStore = match gate {
            Some(gate) => gate,
            None => self.store.clone(),
        };
        LoonFs::builder_with_store(store)
            .writer_id(writer_id("takeover-peer"))
            .config(loonfs::RuntimeConfig {
                min_publish_interval_ms: 0,
                ..loonfs::RuntimeConfig::default()
            })
            .build()
            .await
            .expect("writer B")
    }

    async fn current_manifest_no(&self) -> ManifestNo {
        loonfs_core::test_support::namespace_store(&self.store, &self.namespace_id)
            .load_current_manifest()
            .await
            .expect("current manifest")
            .manifest()
            .manifest_no
    }

    /// Checks that a read in A's runtime sees B's commit at `/b`, and that
    /// A's next commit fails with `writer_fenced`.
    async fn assert_sees_the_takeover(&self) {
        self.reader
            .stat("/b")
            .await
            .expect("a read in A's runtime sees B's commit");
        let fenced = self
            .namespace
            .create_directory("/after", &loonfs_test_support::test_actor())
            .await
            .expect_err("A's session is fenced");
        assert_eq!(fenced.code(), ErrorCode::WriterFenced);
    }
}

fn parked_folds(
    namespace_id: &NamespaceId,
) -> Box<dyn Fn(&OperationContext<'_>) -> bool + Send + Sync> {
    Box::new(fold_manifest_put(namespace_id))
}

fn parked_commits(namespace_id: &NamespaceId) -> Selector {
    Box::new(data_wal_put(namespace_id))
}

/// An inline policy under which every commit with inline bytes starts the
/// session's own fold.
fn fold_every_inline_commit() -> InlineContentPolicy {
    InlineContentPolicy {
        inline_content_fold_at_bytes: 1,
        ..InlineContentPolicy::default()
    }
}

/// Checks that when A's own fold loses its manifest number to B's claim,
/// the fold folds B's log over the claim, that A's session finds that it's
/// fenced at its next commit and not from the fold, and that a read in A's
/// runtime sees B's commit.
///
/// A's fold waits at its manifest put while B claims the namespace, and B's
/// commit waits at its WAL put until A's fold has ended, so the claim and
/// B's fence are all that the fold could load. The fold's manifest has B's
/// writer epoch, so A's position doesn't move onto it.
#[tokio::test]
async fn a_session_fold_that_loses_to_a_claim_folds_the_new_writers_log() {
    let fixture = takeover("fold-over-claim", parked_folds, fold_every_inline_commit()).await;
    let peer_gate: Gate = Arc::new(BlockingStore::matching(
        fixture.store.clone(),
        data_wal_put(&fixture.namespace_id),
    ));
    let peer = fixture.peer(Some(peer_gate.clone())).await;
    let peer_namespace = peer
        .open_namespace(&fixture.namespace_id)
        .expect("open namespace");
    let actor = loonfs_test_support::test_actor();

    fixture.gate.block_next();
    fixture
        .namespace
        .put_file("/inline", b"inline", &actor)
        .await
        .expect("A's commit that starts its fold");
    bounded(STEP, fixture.gate.wait_until_blocked())
        .await
        .expect("A's fold waits at its manifest put");
    let claimed = fixture.current_manifest_no().await;

    peer_gate.block_next();
    let peer_commit = tokio::spawn(async move {
        peer_namespace
            .put_file("/b", b"b", &loonfs_test_support::test_actor())
            .await
    });
    bounded(STEP, peer_gate.wait_until_blocked())
        .await
        .expect("B's commit waits at its WAL put");
    fixture.gate.release();
    bounded(STEP, fixture.writer.drain())
        .await
        .expect("A's fold ends")
        .expect("drain");
    assert_eq!(
        fixture.namespace.session_status(),
        NamespaceSessionStatus::Open,
        "A's fold doesn't fence A's session"
    );
    assert_eq!(
        fixture.current_manifest_no().await,
        claimed
            .successor()
            .and_then(|claim| claim.successor())
            .expect("the number after B's claim"),
        "A's fold folded B's log over B's claim"
    );

    peer_gate.release();
    bounded(STEP, peer_commit)
        .await
        .expect("B's commit ends")
        .expect("join")
        .expect("B's commit");
    fixture.assert_sees_the_takeover().await;
}

/// Checks that when a fold through A's maintenance folds over B's claim, a
/// read in A's runtime still sees B's commit, because A's position doesn't
/// move past B's claim.
#[tokio::test]
async fn a_maintenance_fold_over_a_claim_leaves_the_former_writer_reading_the_claim() {
    let fixture = takeover(
        "maintenance-fold-over-claim",
        parked_folds,
        InlineContentPolicy::default(),
    )
    .await;
    let actor = loonfs_test_support::test_actor();
    fixture
        .namespace
        .put_file("/inline", b"inline", &actor)
        .await
        .expect("A's commit");
    let peer = fixture.peer(None).await;
    let maintenance = fixture
        .writer
        .maintenance(writer_id("fold-over-claim-maintenance"));

    fixture.gate.block_next();
    let (folded, ()) = futures::join!(maintenance.fold_wal(&fixture.namespace_id), async {
        bounded(STEP, fixture.gate.wait_until_blocked())
            .await
            .expect("the fold waits at its manifest put");
        peer.open_namespace(&fixture.namespace_id)
            .expect("open namespace")
            .put_file("/b", b"b", &actor)
            .await
            .expect("B takes over and commits");
        fixture.gate.release();
    });
    assert_eq!(folded.expect("fold").outcome, FoldWalOutcome::Published);
    fixture.assert_sees_the_takeover().await;
}

impl Takeover {
    /// Parks A's next commit at its WAL put, runs `during` while it waits,
    /// and checks that A's commit then fails with `writer_fenced`, because B
    /// took over meanwhile. `during` releases the gate.
    async fn while_a_commit_waits(&self, during: impl std::future::Future<Output = ()>) {
        self.gate.block_next();
        let actor = loonfs_test_support::test_actor();
        let (parked, ()) =
            futures::join!(self.namespace.create_directory("/during", &actor), async {
                bounded(STEP, self.gate.wait_until_blocked())
                    .await
                    .expect("A's commit waits at its WAL put");
                during.await;
            });
        assert_eq!(
            parked.expect_err("B took over during A's commit").code(),
            ErrorCode::WriterFenced
        );
    }

    /// Runs a fold through A's maintenance and returns the manifest it
    /// published.
    async fn maintenance_fold(&self) -> ManifestNo {
        let folded = self
            .writer
            .maintenance(writer_id("takeover-maintenance"))
            .fold_wal(&self.namespace_id)
            .await
            .expect("fold");
        assert_eq!(folded.outcome, FoldWalOutcome::Published);
        folded.manifest_no
    }

    /// Waits until the namespace's current manifest is past `manifest_no`.
    async fn wait_for_manifest_after(&self, manifest_no: ManifestNo) {
        bounded(STEP, async {
            while self.current_manifest_no().await <= manifest_no {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("a manifest is published");
    }

    /// Reads `/b` in A's runtime while A's commit waits, and releases the
    /// commit once the read's check has walked to B's claim at `claim`, so
    /// the check's drop of A's position waits for A's commit.
    async fn read_past_the_claim(&self, claim: ManifestNo) {
        self.recording.reset();
        let (entry, ()) = futures::join!(self.reader.stat("/b"), async {
            let claim =
                loonfs_objectstore::keys::metadata_manifest_object(&self.namespace_id, &claim);
            crate::common::wait_for_operation(&self.recording, |operation| {
                matches!(operation, RecordedOperation::Get { key, .. } if *key == claim)
            })
            .await;
            self.gate.release();
        });
        entry.expect("a read in A's runtime sees B's commit");
    }

    /// Lets B claim the namespace and commit `/b`.
    async fn peer_commits(&self) {
        self.peer(None)
            .await
            .open_namespace(&self.namespace_id)
            .expect("open namespace")
            .put_file("/b", b"b", &loonfs_test_support::test_actor())
            .await
            .expect("B takes over and commits");
    }
}

/// Checks that a read in A's runtime with an interval of 0 sees B's commit
/// when B claims the namespace after a manifest of A's own that A's position
/// hasn't moved to yet.
///
/// A's fold through maintenance publishes A's manifest while A's commit
/// waits at its WAL put, so the move of A's position waits for the engine.
/// The read's check finds A's own manifest first and goes on to the next
/// number, which is B's claim, and its drop of the position waits for A's
/// commit too.
#[tokio::test]
async fn a_check_goes_past_this_runtimes_own_manifest_to_a_claim() {
    let fixture = takeover(
        "past-own-manifest",
        parked_commits,
        InlineContentPolicy::default(),
    )
    .await;
    fixture
        .while_a_commit_waits(async {
            let before = fixture.current_manifest_no().await;
            let (own, ()) = futures::join!(fixture.maintenance_fold(), async {
                fixture.wait_for_manifest_after(before).await;
                fixture.peer_commits().await;
                let claim = fixture.current_manifest_no().await;
                assert!(claim > before.successor().expect("A's own manifest"));
                fixture.read_past_the_claim(claim).await;
            });
            assert!(fixture.current_manifest_no().await > own);
        })
        .await;
    fixture.assert_sees_the_takeover().await;
}

/// Checks that B's claim at a manifest number below A's newest own manifest
/// isn't taken as A's own.
///
/// B claims the namespace while A's commit waits at its WAL put, and then a
/// fold through A's maintenance folds over B's claim, so A's own manifest
/// follows B's claim. The read's check finds B's claim right after A's
/// position, and drops the position once A's commit releases the engine.
#[tokio::test]
async fn a_claim_below_this_runtimes_own_manifest_is_not_its_own() {
    let fixture = takeover(
        "claim-below-own",
        parked_commits,
        InlineContentPolicy::default(),
    )
    .await;
    fixture
        .while_a_commit_waits(async {
            fixture.peer_commits().await;
            let claim = fixture.current_manifest_no().await;
            let (own, ()) = futures::join!(fixture.maintenance_fold(), async {
                fixture.wait_for_manifest_after(claim).await;
                fixture.read_past_the_claim(claim).await;
            });
            assert!(own > claim);
        })
        .await;
    fixture.assert_sees_the_takeover().await;
}
