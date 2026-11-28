//! Reads in a former writer's runtime after another writer takes over the
//! namespace while the former writer's folds and publishes are in flight.

use loonfs::{ErrorCode, FoldWalOutcome, LoonFs, Namespace, ReadOnly, SharedObjectStore, Writable};
use loonfs_objectstore::keys::wal_prefix;
use loonfs_objectstore::PutMode;
use loonfs_test_support::clock::bounded;
use loonfs_test_support::ids::{namespace_id, writer_id};
use loonfs_test_support::stores::{
    BlockingStore, KeyPredicate, MemoryStore, OperationContext, OperationKind, RecordedOperation,
    RecordingStore,
};
use loonfs_types::format::wal::decode_wal_object_envelope_zstd;
use loonfs_types::{ChecksumAlgorithm, ManifestNo, NamespaceId};
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Duration;

/// How long a step that the test unblocks gets before the test fails.
const STEP: Duration = Duration::from_secs(10);

type Gate = Arc<BlockingStore<SharedObjectStore>>;

/// Selects the requests that a gate parks.
type Selector = Box<dyn Fn(&OperationContext<'_>) -> bool + Send + Sync>;

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
                        .is_ok_and(|wal| matches!(wal.payload().body, loonfs_types::format::wal::WalObjectBody::Commits(_)))
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
/// `block_next`. `session_fold_at_wal_objects` sets the WAL objects that
/// make A's own fold due, and the setup waits for any fold that its commit
/// starts.
async fn takeover(
    name: &str,
    parked: fn(&NamespaceId) -> Selector,
    session_fold_at_wal_objects: Option<NonZeroU64>,
) -> Takeover {
    let namespace_id = namespace_id(name);
    let store: SharedObjectStore = Arc::new(MemoryStore::new(ChecksumAlgorithm::Crc64nvme));
    let gate = Arc::new(BlockingStore::matching(
        store.clone(),
        parked(&namespace_id),
    ));
    let recording = Arc::new(RecordingStore::new(gate.clone(), KeyPredicate::any()));
    let recorded: SharedObjectStore = recording.clone();
    let builder = LoonFs::builder_with_store(recorded, loonfs::Context::Service)
        .writer_id(writer_id(name))
        .config(loonfs::RuntimeConfig {
            min_publish_interval_ms: 0,
            manifest_revalidation_interval_ms: 0,
            max_staleness_ms: 0,
            ..loonfs::RuntimeConfig::default()
        });
    let builder = match session_fold_at_wal_objects {
        Some(objects) => builder.session_fold_at_wal_objects(objects),
        None => builder,
    };
    let writer = builder.build().await.expect("writer A");
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
    writer.drain().await.expect("A's first commit settles");
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

/// The request with which a read's check in A's runtime reaches B's claim.
#[derive(Debug, Clone, Copy)]
enum ClaimRequest {
    /// The check's walk loads the claim from the store.
    Get,
    /// The check's HEAD for a successor finds the claim, and the walk loads
    /// the claim from the segment cache, because an earlier load in A's
    /// runtime loaded the claim first.
    Head,
}

impl Takeover {
    /// Opens writer B over the shared store, or over `gate` when given.
    async fn peer(&self, gate: Option<Gate>) -> LoonFs<Writable> {
        let store: SharedObjectStore = match gate {
            Some(gate) => gate,
            None => self.store.clone(),
        };
        LoonFs::builder_with_store(store, loonfs::Context::Service)
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

fn parked_commits(namespace_id: &NamespaceId) -> Selector {
    Box::new(data_wal_put(namespace_id))
}

impl Takeover {
    /// Parks A's next commit at its WAL put, runs `during` while it waits,
    /// and checks that A's commit then fails with `writer_fenced`, because B
    /// took over meanwhile. `during` releases the gate.
    async fn while_a_commit_waits(&self, during: impl std::future::Future<Output = ()>) {
        self.gate.block_next();
        let actor = loonfs_test_support::test_actor();
        // The commit is boxed to keep the layout of this future under the
        // compiler's depth limit.
        let commit = Box::pin(self.namespace.create_directory("/during", &actor));
        let (parked, ()) = futures::join!(commit, async {
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

    /// Runs a fold through A's session and returns the manifest it
    /// published.
    async fn maintenance_fold(&self) -> ManifestNo {
        let folded = self
            .writer
            .open_namespace(&self.namespace_id)
            .expect("open namespace")
            .fold_wal()
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
    /// commit once the read's check has walked to B's claim at `claim` with
    /// `reached`, so the check's drop of A's position waits for A's commit.
    async fn read_past_the_claim(&self, claim: ManifestNo, reached: ClaimRequest) {
        self.recording.reset();
        let (entry, ()) = futures::join!(self.reader.stat("/b"), async {
            let claim =
                loonfs_objectstore::keys::metadata_manifest_object(&self.namespace_id, &claim);
            crate::common::wait_for_operation(&self.recording, |operation| match reached {
                ClaimRequest::Get => {
                    matches!(operation, RecordedOperation::Get { key, .. } if *key == claim)
                }
                ClaimRequest::Head => {
                    matches!(operation, RecordedOperation::Head { key } if *key == claim)
                }
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
/// when B claims the namespace after a manifest of A's own.
///
/// A's session's fold publishes A's manifest while A's commit
/// waits at its WAL put, and moves A's position onto it without waiting for
/// the commit. The read's check finds the next number, which is B's claim,
/// and its drop of the position waits for A's commit.
#[tokio::test]
async fn a_check_goes_past_this_runtimes_own_manifest_to_a_claim() {
    let fixture = takeover("past-own-manifest", parked_commits, None).await;
    fixture
        .while_a_commit_waits(async {
            let before = fixture.current_manifest_no().await;
            let (own, ()) = futures::join!(fixture.maintenance_fold(), async {
                fixture.wait_for_manifest_after(before).await;
                fixture.peer_commits().await;
                let claim = fixture.current_manifest_no().await;
                assert!(claim > before.successor().expect("A's own manifest"));
                fixture.read_past_the_claim(claim, ClaimRequest::Get).await;
            });
            assert!(fixture.current_manifest_no().await > own);
        })
        .await;
    fixture.assert_sees_the_takeover().await;
}

/// Checks that a read's check that finds B's claim with its HEAD for a
/// successor, and loads the claim from the segment cache, drops A's
/// position, and that the read sees B's commit.
///
/// B claims the namespace while A's commit waits at its WAL put. A's
/// diagnostics load the newest manifest, which is B's claim, through A's
/// segment cache without moving A's position. The read's check then finds
/// the claim right after A's position.
#[tokio::test]
async fn a_check_finds_a_cached_claim_with_its_successor_head() {
    let fixture = takeover("cached-claim", parked_commits, None).await;
    fixture
        .while_a_commit_waits(async {
            fixture.peer_commits().await;
            let claim = fixture.current_manifest_no().await;
            fixture
                .writer
                .maintenance()
                .diagnostics(&fixture.namespace_id)
                .await
                .expect("diagnostics");
            assert!(fixture.namespace.has_current_writer_position());
            fixture.read_past_the_claim(claim, ClaimRequest::Head).await;
        })
        .await;
    fixture.assert_sees_the_takeover().await;
}
