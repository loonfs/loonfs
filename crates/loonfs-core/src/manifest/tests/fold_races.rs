//! A fold that loses its manifest number to another publication.

use super::fold::{FoldWalResponse, FoldedWalTail};
use super::*;
use crate::error::Result;
use crate::limits::{MAX_CONTENTION_ATTEMPTS, READ_REVALIDATION_BOUND_MS};
use crate::manifest::PublishedManifest;
use crate::namespace::control::LoadedManifest;
use crate::publish::{InlineContent, WriterPosition};
use crate::time::Observation;
use crate::wal::WalPublishError;
use crate::FoldWalOutcome;
use loonfs_objectstore::keys::wal_prefix;
use loonfs_objectstore::layout::{parse_object_key, wal_no_of, DurableObjectFamily};
use loonfs_test_support::clock::ManualClock;
use loonfs_test_support::stores::{RecordedOperation, StoreCounts};
use loonfs_types::{CompactorEpoch, ContentId, DeleteDirectoryBehavior, WalNo, WriterEpoch};
use std::path::Path;

/// A manifest another process publishes while a fold waits at its
/// manifest put.
#[derive(Clone)]
enum Rival {
    CompactorClaim,
    Merge,
    RetentionAdvance,
    Fold(Box<WriterPosition>),
}

impl Rival {
    async fn publish(&self, store: &LocalFsStore, namespace_id: &NamespaceId) {
        match self {
            Self::CompactorClaim => {
                crate::test_support::NamespaceFixture::new(
                    store,
                    namespace_id,
                    &crate::test_support::writer_at("test-writer", 0),
                )
                .engine()
                .claim_compactor()
                .await
                .expect("claim the compactor");
            }
            Self::Merge => {
                let mut merges = 0;
                loop {
                    match crate::test_support::NamespaceFixture::new(
                        store,
                        namespace_id,
                        &crate::test_support::writer_at("test-writer", 0),
                    )
                    .engine()
                    .compact_metadata_unit(
                        None,
                        MetadataCompactionPolicy::CompactImmediately,
                        crate::manifest::CompactionAdmission::AnySize,
                        CompactorEpoch(0),
                        &crate::manifest::MetadataCompactionCancellation::default(),
                    )
                    .await
                    .expect("merge")
                    {
                        CompactionUnitOutcome::Published { .. } => merges += 1,
                        CompactionUnitOutcome::NotNeeded => break,
                        other => panic!("expected a merge, got {other:?}"),
                    }
                }
                assert!(merges > 0, "the merge rival should publish");
            }
            Self::RetentionAdvance => {
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
                .expect("advance the floor");
            }
            Self::Fold(input) => {
                let folded = crate::test_support::namespace_store(store, namespace_id)
                    .fold_wal_tail(Some(WriterPosition::clone(input)), &deadline())
                    .await
                    .expect("rival fold");
                assert_eq!(folded.response.outcome, FoldWalOutcome::Published);
            }
        }
    }
}

struct Writer {
    directory: tempfile::TempDir,
    store: Arc<RecordingStore<LocalFsStore>>,
    engine: NamespaceCommitEngine<Arc<RecordingStore<LocalFsStore>>>,
    context: WriterAt,
    namespace_id: NamespaceId,
}

impl Writer {
    async fn open() -> Self {
        let directory = tempdir().expect("directory");
        let store = Arc::new(RecordingStore::new(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::any(),
        ));
        let namespace_id = NamespaceId::parse("fold-race").expect("namespace");
        let context = test_context();
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
            .create()
            .await
            .expect("create");
        Self {
            engine: NamespaceCommitEngine::new(
                crate::test_support::namespace_store(Arc::clone(&store), &namespace_id),
                context.writer_id.clone(),
            ),
            directory,
            store,
            context,
            namespace_id,
        }
    }

    fn other_process(&self) -> LocalFsStore {
        LocalFsStore::new(self.directory.path()).expect("store")
    }

    /// Copies the store so the same history can be driven another way.
    fn twin(&self) -> (tempfile::TempDir, LocalFsStore) {
        let twin = tempdir().expect("twin directory");
        copy_directory(self.directory.path(), twin.path());
        let store = LocalFsStore::new(twin.path()).expect("twin store");
        (twin, store)
    }

    /// Commits `operations`, then one inline file at each of `files`.
    async fn commit(
        &mut self,
        commit_id: &str,
        mut operations: Vec<FilesystemOperation>,
        files: &[&str],
    ) {
        let mut values = Vec::new();
        for path in files {
            let value = InlineContent::new(
                self.namespace_id.clone(),
                ContentId::generate(),
                Bytes::copy_from_slice(path.as_bytes()),
                loonfs_types::ChecksumAlgorithm::Crc64nvme,
            );
            operations.push(FilesystemOperation::PutFile {
                path: AbsolutePath::parse(*path).expect("path"),
                content: loonfs_types::FileContent::Ref(value.content_ref().clone()),
                behavior: DestinationBehavior::NoReplace,
                expected_inode_id: None,
                expected_revision_no: None,
            });
            values.push(value);
        }
        let candidate = CommitCandidate::with_inline_content(
            CommitIntent {
                commit_id: CommitId::parse(commit_id).expect("commit id"),
                actor_id: loonfs_test_support::test_actor(),
                subject: None,
                message: None,
                preconditions: Vec::new(),
                operations,
            },
            Vec::new(),
            values,
        );
        self.engine
            .publish_batch([candidate], &self.context.clock())
            .await
            .results
            .pop()
            .expect("one result")
            .expect("commit");
    }

    async fn fold(&mut self) -> FoldedWalTail {
        self.fold_against(&[]).await.expect("fold")
    }

    /// Folds the writer's tail and records the fold's manifest, as the
    /// publisher does. The fold's
    /// first manifest put waits while the first rival publishes from another
    /// process, its next put waits for the next rival, and so on. The request
    /// log is cleared after each rival.
    async fn fold_against(&mut self, rivals: &[Rival]) -> Result<FoldedWalTail> {
        let input = self.engine.writer_position().expect("tail");
        let basis = input.read_basis.basis.manifest_no();
        let other = self.other_process();
        let mut store: Arc<dyn ObjectStore> = self.store.clone();
        let mut gates = Vec::new();
        for number in 1..=rivals.len() as u64 {
            let gate = Arc::new(BlockingStore::new(
                store,
                KeyPredicate::exact(metadata_manifest_object(
                    &self.namespace_id,
                    &ManifestNo(basis.0 + number),
                )),
                OperationSelector::PutCreateIfAbsent,
            ));
            gate.block_next();
            gates.push(Arc::clone(&gate));
            store = gate;
        }
        let deadline = deadline();
        let namespace = crate::test_support::namespace_store(&*store, &self.namespace_id);
        let (folded, ()) = futures::join!(namespace.fold_wal_tail(Some(input), &deadline), async {
            for (gate, rival) in gates.iter().zip(rivals) {
                gate.wait_until_blocked().await;
                rival.publish(&other, &self.namespace_id).await;
                self.store.reset();
                gate.release();
            }
        });
        if let Ok(folded) = &folded {
            self.engine
                .record_manifest_published(folded.manifest.clone());
        }
        folded
    }

    fn wal_reads(&self, operations: &[RecordedOperation]) -> BTreeSet<u64> {
        operations
            .iter()
            .filter(|operation| {
                matches!(
                    operation,
                    RecordedOperation::Head { .. }
                        | RecordedOperation::Get { .. }
                        | RecordedOperation::GetWithMetadata { .. }
                ) && operation.key().starts_with(&wal_prefix(&self.namespace_id))
            })
            .map(|operation| wal_no_of(operation.key()).expect("WAL number").0)
            .collect()
    }
}

fn deadline() -> OperationClock {
    OperationClock::start(0, Arc::new(StdMonotonicTimer::default()))
}

fn copy_directory(from: &Path, to: &Path) {
    for entry in std::fs::read_dir(from).expect("read directory") {
        let entry = entry.expect("directory entry");
        let target = to.join(entry.file_name());
        if entry.file_type().expect("file type").is_dir() {
            std::fs::create_dir_all(&target).expect("create directory");
            copy_directory(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).expect("copy file");
        }
    }
}

fn is_content(operation: &RecordedOperation) -> bool {
    parse_object_key(operation.key())
        .is_some_and(|key| key.family() == DurableObjectFamily::ContentBlob)
}

/// The manifest without segment identities, which differ between any two
/// builds of the same rows.
fn layout(
    manifest: &LoadedManifest,
) -> (
    NamespaceManifestPayload,
    Vec<(RunNo, ChangeSeq, RunTier, usize)>,
) {
    let payload = manifest.envelope.payload();
    (
        NamespaceManifestPayload {
            runs: Vec::new(),
            ..payload.clone()
        },
        payload
            .runs
            .iter()
            .map(|run| (run.run_no, run.run_seq, run.tier, run.segments.len()))
            .collect(),
    )
}

/// Request counts without byte totals, which differ with the digits of
/// manifest numbers and epochs.
fn request_counts(store: &RecordingStore<LocalFsStore>) -> StoreCounts {
    StoreCounts {
        read_bytes: 0,
        written_bytes: 0,
        ..store.counts()
    }
}

#[tokio::test]
async fn a_fold_that_loses_to_a_compaction_reads_no_wal_object() {
    for rival in [Rival::Merge, Rival::CompactorClaim, Rival::RetentionAdvance] {
        let mut writer = Writer::open().await;
        writer
            .commit(
                "first",
                vec![FilesystemOperation::CreateDirectory {
                    path: AbsolutePath::parse("/dir").expect("path"),
                    parents: false,
                }],
                &["/dir/a"],
            )
            .await;
        writer.fold().await;
        writer.commit("second", Vec::new(), &["/b"]).await;
        writer.fold().await;
        // The deletion's root inode is in the runs a merge rewrites.
        writer
            .commit(
                "third",
                vec![FilesystemOperation::DeletePath {
                    path: AbsolutePath::parse("/dir").expect("path"),
                    behavior: DeleteDirectoryBehavior::Recursive,
                    expected_inode_id: None,
                }],
                &["/c"],
            )
            .await;
        let (_twin_directory, twin) = writer.twin();

        let held = writer
            .fold_against(std::slice::from_ref(&rival))
            .await
            .expect("held fold");
        let after_park = writer.store.take();
        assert!(writer.wal_reads(&after_park).is_empty(), "{after_park:?}");
        assert!(!after_park.iter().any(is_content), "{after_park:?}");

        rival.publish(&twin, &writer.namespace_id).await;
        let cold = crate::test_support::namespace_store(&twin, &writer.namespace_id)
            .fold_wal_tail(None, &deadline())
            .await
            .expect("cold fold");
        assert_eq!(held.response.outcome, FoldWalOutcome::Published);
        assert_eq!(held.response, cold.response);
        let held = load_current_projection(&*writer.store, &writer.namespace_id)
            .await
            .expect("held fold result");
        let cold = load_current_projection(&twin, &writer.namespace_id)
            .await
            .expect("cold fold result");
        assert_eq!(held.head, cold.head);
        assert_eq!(held.metadata_state, cold.metadata_state);
        assert_eq!(layout(&held.manifest), layout(&cold.manifest));
    }
}

#[tokio::test]
async fn a_fold_that_loses_to_a_fold_still_takes_the_cold_path() {
    let mut writer = Writer::open().await;
    writer.commit("first", Vec::new(), &["/a"]).await;
    let older = writer.engine.writer_position().expect("tail");
    writer.commit("second", Vec::new(), &["/b"]).await;
    let head = writer
        .engine
        .writer_position()
        .expect("tail")
        .read_basis
        .head;

    let folded = writer
        .fold_against(&[Rival::Fold(Box::new(older.clone()))])
        .await
        .expect("fold");
    let after_park = writer.store.take();
    assert!(
        writer.wal_reads(&after_park).contains(&head.wal_no.0),
        "{after_park:?}"
    );
    assert!(!writer
        .wal_reads(&after_park)
        .contains(&older.read_basis.head.wal_no.0));
    assert_eq!(folded.response.outcome, FoldWalOutcome::Published);
    assert_eq!(
        folded.response.manifest_no,
        ManifestNo(older.read_basis.basis.manifest_no().0 + 2)
    );
    assert_eq!(folded.response.manifest_head_seq, head.seq);
}

/// Checks that a fold whose put loses to a fold of the same tail finishes on
/// its next attempt, without writing segments, content, or another manifest.
#[tokio::test]
async fn a_fold_that_loses_to_a_fold_of_its_tail_finishes() {
    let mut writer = Writer::open().await;
    writer.commit("first", Vec::new(), &["/a"]).await;
    let same = writer.engine.writer_position().expect("tail");
    let basis = same.read_basis.basis.manifest_no();
    let folded = writer
        .fold_against(&[Rival::Fold(Box::new(same))])
        .await
        .expect("fold");
    let after_park = writer.store.take();
    assert_eq!(folded.response.outcome, FoldWalOutcome::AlreadyCurrent);
    assert_eq!(
        folded.response.manifest_no,
        basis.successor().expect("next manifest number")
    );
    assert!(writer.wal_reads(&after_park).is_empty(), "{after_park:?}");
    // The one write is the put that lost.
    let writes = after_park
        .iter()
        .filter(|operation| {
            !matches!(
                operation,
                RecordedOperation::Head { .. }
                    | RecordedOperation::Get { .. }
                    | RecordedOperation::GetWithMetadata { .. }
            )
        })
        .map(RecordedOperation::key)
        .collect::<Vec<_>>();
    assert_eq!(
        writes,
        vec![metadata_manifest_object(
            &writer.namespace_id,
            &basis.successor().expect("next manifest number")
        )]
    );
}

/// Checks that a fold from a session's position whose tail another process
/// already folded loses its manifest put, finds the other process's fold
/// above the position's manifest, and finishes without another manifest
/// put.
#[tokio::test]
async fn a_fold_whose_tail_is_already_folded_loses_its_one_manifest_put() {
    let mut writer = Writer::open().await;
    writer.commit("first", Vec::new(), &["/a"]).await;
    let same = writer.engine.writer_position().expect("tail");
    let basis = same.read_basis.basis.manifest_no();
    Rival::Fold(Box::new(same))
        .publish(&writer.other_process(), &writer.namespace_id)
        .await;
    writer.store.reset();
    let folded = writer.fold().await;
    let operations = writer.store.take();
    assert_eq!(folded.response.outcome, FoldWalOutcome::AlreadyCurrent);
    assert_eq!(
        folded.response.manifest_no,
        basis.successor().expect("next manifest number")
    );
    // The fold builds on the position's manifest without a request first,
    // so it writes its tail's objects before the put that loses.
    let manifests = metadata_manifest_prefix(&writer.namespace_id);
    let manifest_writes = operations
        .iter()
        .filter(|operation| {
            operation.key().starts_with(&manifests)
                && !matches!(
                    operation,
                    RecordedOperation::Head { .. }
                        | RecordedOperation::Get { .. }
                        | RecordedOperation::GetWithMetadata { .. }
                )
        })
        .map(RecordedOperation::key)
        .collect::<Vec<_>>();
    assert_eq!(
        manifest_writes,
        vec![metadata_manifest_object(
            &writer.namespace_id,
            &basis.successor().expect("next manifest number")
        )],
        "the one manifest write is the put that lost"
    );
    assert!(writer.wal_reads(&operations).is_empty(), "{operations:?}");
}

/// Checks that a fold from a session's position publishes on the position's
/// manifest with its put and no request for the hint or the manifest's
/// successor, whether the hint points to that manifest or lags below it, and
/// leaves the hint for the runtime to raise.
#[tokio::test]
async fn a_fold_from_a_fresh_position_puts_on_its_basis_and_reads_no_hint() {
    for hint_lags in [false, true] {
        let mut writer = Writer::open().await;
        writer.commit("first", Vec::new(), &["/a"]).await;
        // The runtime that published the session's claim raises the hint,
        // or hasn't raised it yet.
        if !hint_lags {
            crate::test_support::namespace_store(writer.store.as_ref(), &writer.namespace_id)
                .raise_hint_to_current_manifest()
                .await;
        }
        let basis = writer
            .engine
            .writer_position()
            .expect("tail")
            .read_basis
            .basis
            .manifest_no();
        let next = basis.successor().expect("next manifest number");
        writer.store.reset();
        let folded = writer.fold().await;
        assert_eq!(folded.response.outcome, FoldWalOutcome::Published);
        let manifest = |number| metadata_manifest_object(&writer.namespace_id, &number);
        assert_eq!(
            control_requests(&writer.namespace_id, &writer.store.take()),
            vec![
                // The session's basis, for its segments.
                ("get", manifest(basis)),
                ("put", manifest(next)),
            ],
            "the hint lags: {hint_lags}"
        );
    }
}

/// Checks that a fold from a position whose last check is at the
/// revalidation bound loads the tail from the store instead of reading the
/// position's manifest, which garbage collection removes once a successor
/// is older than the grace window.
#[tokio::test]
async fn a_fold_from_a_position_past_the_bound_loads_the_tail_from_the_store() {
    let mut writer = Writer::open().await;
    writer.commit("first", Vec::new(), &["/a"]).await;
    let mut held = writer.engine.writer_position().expect("tail");
    let clock = Arc::new(ManualClock::new(0));
    held.basis_checked = Observation::now(clock.clone());
    let superseded =
        metadata_manifest_object(&writer.namespace_id, &held.read_basis.basis.manifest_no());

    // Another process folds the tail and raises the hint, and garbage
    // collection removes the position's manifest once the fold is older
    // than the grace window.
    let other = writer.other_process();
    let folded_by_other = crate::test_support::namespace_store(&other, &writer.namespace_id)
        .fold_wal()
        .await
        .expect("the other process folds");
    assert_eq!(folded_by_other.outcome, FoldWalOutcome::Published);
    crate::test_support::namespace_store(&other, &writer.namespace_id)
        .raise_hint_to_current_manifest()
        .await;
    writer
        .store
        .delete(&superseded)
        .await
        .expect("remove the superseded manifest");
    clock.advance_ms(READ_REVALIDATION_BOUND_MS);

    writer.store.reset();
    let folded = crate::test_support::namespace_store(&*writer.store, &writer.namespace_id)
        .fold_wal_tail(Some(held), &deadline())
        .await
        .expect("a fold past the bound loads the tail from the store");
    assert_eq!(folded.response.outcome, FoldWalOutcome::AlreadyCurrent);
    assert_eq!(
        folded.manifest.manifest.manifest_no,
        folded_by_other.manifest_no
    );
    let requests = control_requests(&writer.namespace_id, &writer.store.take());
    assert_eq!(
        requests.first(),
        Some(&("get", hint(&writer.namespace_id))),
        "the fold starts from the hint: {requests:?}"
    );
    assert!(
        !requests.contains(&("get", superseded)),
        "the fold never reads the position's manifest: {requests:?}"
    );
}

/// Checks that a fold keeps the tail it read through a claim at each
/// attempt, and that a fold whose every attempt loses stops at the
/// contention limit and leaves the session's position where it was.
#[tokio::test]
async fn a_fold_survives_repeated_compaction_publications() {
    // Each attempt loses to one claim.
    for rivals in [MAX_CONTENTION_ATTEMPTS - 1, MAX_CONTENTION_ATTEMPTS] {
        let mut writer = Writer::open().await;
        writer.commit("first", Vec::new(), &["/a"]).await;
        let basis = writer
            .engine
            .writer_position()
            .expect("tail")
            .read_basis
            .basis;
        let folded = writer
            .fold_against(&vec![Rival::CompactorClaim; rivals])
            .await;
        let after_park = writer.store.take();
        assert!(writer.wal_reads(&after_park).is_empty(), "{after_park:?}");
        if rivals < MAX_CONTENTION_ATTEMPTS {
            let folded = folded.expect("fold");
            assert_eq!(folded.response.outcome, FoldWalOutcome::Published);
            assert_eq!(
                folded.response.manifest_no,
                ManifestNo(basis.manifest_no().0 + rivals as u64 + 1)
            );
        } else {
            let Err(error) = folded else {
                panic!("every attempt lost, so the fold should fail");
            };
            assert_matches!(error, CoreError::WalPublish(WalPublishError::NumberTaken));
            assert_eq!(
                writer
                    .engine
                    .writer_position()
                    .expect("tail")
                    .read_basis
                    .basis,
                basis
            );
        }
    }
}

#[tokio::test]
async fn a_retried_fold_leaves_the_session_where_an_unraced_fold_does() {
    let mut costs = Vec::new();
    for rivals in [Vec::new(), vec![Rival::CompactorClaim]] {
        let mut writer = Writer::open().await;
        writer.commit("first", Vec::new(), &["/a"]).await;
        writer.store.reset();
        let folded = writer.fold_against(&rivals).await.expect("fold");
        let operations = writer.store.take();
        assert!(writer.wal_reads(&operations).is_empty(), "{operations:?}");
        assert_eq!(folded.response.outcome, FoldWalOutcome::Published);
        assert_eq!(
            writer
                .engine
                .writer_position()
                .expect("tail")
                .read_basis
                .basis
                .manifest(),
            &folded.manifest.manifest
        );
        // The runtimes that published the manifests raise the hint, so the
        // next fold's discovery starts at the current manifest in both cases.
        crate::test_support::namespace_store(writer.store.inner(), &writer.namespace_id)
            .raise_hint_to_current_manifest()
            .await;
        writer.commit("second", Vec::new(), &["/b"]).await;
        let publish = request_counts(&writer.store);
        let hint = hint(&writer.namespace_id);
        let operations = writer.store.take();
        assert!(!operations.iter().any(|operation| operation.key() == hint));
        writer.fold().await;
        let fold = request_counts(&writer.store);
        let operations = writer.store.take();
        assert!(writer.wal_reads(&operations).is_empty(), "{operations:?}");
        costs.push((publish, fold));
    }
    assert_eq!(costs[0], costs[1]);
    // The receipt check reads the basis manifest and one segment; the commit
    // is one WAL put.
    assert_eq!(
        costs[1].0,
        StoreCounts {
            gets: 2,
            puts: 1,
            create_if_absent_puts: 1,
            ..StoreCounts::default()
        }
    );
}

#[tokio::test]
async fn a_checkpoint_that_loses_its_fold_to_a_compaction_reads_no_wal_object() {
    let mut writer = Writer::open().await;
    writer.commit("first", Vec::new(), &["/a"]).await;
    let basis = writer
        .engine
        .writer_position()
        .expect("tail")
        .read_basis
        .basis
        .manifest_no();
    let other = writer.other_process();
    let gate = BlockingStore::new(
        writer.store.clone(),
        KeyPredicate::manifest(&writer.namespace_id),
        OperationSelector::PutCreateIfAbsent,
    );
    gate.block_next();
    let (checkpoint, ()) = futures::join!(
        create_checkpoint(&gate, &writer.namespace_id, &writer.context),
        async {
            gate.wait_until_blocked().await;
            Rival::CompactorClaim
                .publish(&other, &writer.namespace_id)
                .await;
            writer.store.reset();
            gate.release();
        }
    );
    let checkpoint = checkpoint.expect("checkpoint");
    let after_park = writer.store.take();
    assert!(writer.wal_reads(&after_park).is_empty(), "{after_park:?}");
    let pin = crate::test_support::namespace_store(&*writer.store, &writer.namespace_id)
        .load_pin(&checkpoint.checkpoint_id)
        .await
        .expect("pin record")
        .expect("pin");
    assert_eq!(pin.state.manifest().manifest_no, ManifestNo(basis.0 + 2));
}

/// Checks that a session's fold that loses its manifest number to another
/// writer's epoch claim folds the new writer's log over the claim, that the
/// session's position doesn't move onto that fold's manifest, and that the
/// session finds that it's fenced at its next commit.
#[tokio::test]
async fn a_fold_that_loses_to_a_writer_claim_folds_the_new_writers_log() {
    let mut writer = Writer::open().await;
    writer.commit("first", Vec::new(), &["/a"]).await;
    let input = writer.engine.writer_position().expect("tail");
    let head = input.read_basis.head.clone();
    let claim_no = input
        .read_basis
        .basis
        .manifest_no()
        .successor()
        .expect("next manifest number");
    let gate = BlockingStore::new(
        writer.store.clone(),
        KeyPredicate::exact(metadata_manifest_object(&writer.namespace_id, &claim_no)),
        OperationSelector::PutCreateIfAbsent,
    );
    gate.block_next();
    let other = writer.other_process();
    let fold_deadline = deadline();
    let namespace = crate::test_support::namespace_store(&gate, &writer.namespace_id);
    let (folded, ()) = futures::join!(
        namespace.fold_wal_tail(Some(input), &fold_deadline),
        async {
            gate.wait_until_blocked().await;
            crate::test_support::namespace_store(&other, &writer.namespace_id)
                .acquire_writer_epoch(
                    &crate::test_support::writer_at("other-writer", 2_000).writer_id,
                )
                .await
                .expect("another writer claims the namespace");
            gate.release();
        }
    );
    let folded = folded.expect("the fold folds over the claim");
    assert_eq!(folded.response.outcome, FoldWalOutcome::Published);
    assert_eq!(
        folded.response.manifest_no,
        claim_no.successor().expect("the number after the claim")
    );
    assert_eq!(
        folded.manifest.writer_epoch,
        WriterEpoch(head.writer_epoch.0 + 1)
    );
    assert_eq!(
        folded.manifest.folded_wal_no,
        head.wal_no.successor().expect("the new writer's fence")
    );

    writer.engine.record_manifest_published(folded.manifest);
    assert!(writer.engine.read_position().is_none());
    let results = writer
        .engine
        .publish_batch(
            [CommitCandidate::new(CommitIntent {
                commit_id: CommitId::parse("after").expect("commit id"),
                actor_id: loonfs_test_support::test_actor(),
                subject: None,
                message: None,
                preconditions: Vec::new(),
                operations: vec![FilesystemOperation::CreateDirectory {
                    path: AbsolutePath::parse("/after").expect("path"),
                    parents: false,
                }],
            })],
            &writer.context.clock(),
        )
        .await
        .results;
    assert_matches!(results.as_slice(), [Err(CoreError::WriterFenced(_))]);
}

/// Checks that a session's position moves to the manifest that covers its
/// fold's tail under the session's writer epoch, whether the fold published
/// that manifest or found it current, that a manifest of another epoch, or
/// one that folds a WAL number past the session's head, drops the position,
/// and that a failed fold leaves the position where it was.
#[tokio::test]
async fn a_fold_moves_the_position_only_to_a_fold_of_its_own_head_and_epoch() {
    enum Position {
        Moves,
        Drops,
        Stays,
    }
    for (epoch_after, wal_after, outcome, expected) in [
        (0, 0, Some(FoldWalOutcome::Published), Position::Moves),
        (1, 0, Some(FoldWalOutcome::Published), Position::Drops),
        (0, 1, Some(FoldWalOutcome::Published), Position::Drops),
        (0, 0, Some(FoldWalOutcome::AlreadyCurrent), Position::Moves),
        (0, 1, Some(FoldWalOutcome::AlreadyCurrent), Position::Drops),
        (0, 0, None, Position::Stays),
    ] {
        let mut writer = Writer::open().await;
        writer.commit("first", Vec::new(), &["/a"]).await;
        let input = writer.engine.writer_position().expect("tail");
        let before = input.read_basis.basis.clone();
        let folded = crate::test_support::namespace_store(&*writer.store, &writer.namespace_id)
            .fold_wal_tail(Some(input), &deadline())
            .await
            .expect("fold");
        let recorded = outcome.map(|outcome| FoldedWalTail {
            response: FoldWalResponse {
                outcome,
                ..folded.response.clone()
            },
            manifest: PublishedManifest {
                writer_epoch: WriterEpoch(folded.manifest.writer_epoch.0 + epoch_after),
                folded_wal_no: WalNo(folded.manifest.folded_wal_no.0 + wal_after),
                ..folded.manifest.clone()
            },
        });
        // As the publisher does, a fold that ended reports the manifest that
        // covers its tail, and a fold that failed reports nothing.
        if let Some(recorded) = recorded {
            writer.engine.record_manifest_published(recorded.manifest);
        }
        let position = writer.engine.read_position();
        let expected = match expected {
            Position::Moves => Some(&folded.manifest.manifest),
            Position::Drops => None,
            Position::Stays => Some(before.manifest()),
        };
        assert_eq!(
            position
                .as_ref()
                .map(|position| position.read_basis.basis.manifest()),
            expected,
            "epoch +{epoch_after}, WAL number +{wal_after}, {outcome:?}"
        );
    }
}

/// Checks that a session's fold that finds its tail already folded by
/// another process, through a commit that the session made after its fold
/// began, moves the position onto that process's manifest with no tail.
#[tokio::test]
async fn a_fold_covered_past_its_start_moves_the_position_onto_the_covering_fold() {
    let mut writer = Writer::open().await;
    writer.commit("first", Vec::new(), &["/a"]).await;
    let input = writer.engine.writer_position().expect("tail");
    writer.commit("second", Vec::new(), &["/b"]).await;
    let newest = writer.engine.writer_position().expect("tail");
    Rival::Fold(Box::new(newest.clone()))
        .publish(&writer.other_process(), &writer.namespace_id)
        .await;
    let folded = crate::test_support::namespace_store(&*writer.store, &writer.namespace_id)
        .fold_wal_tail(Some(input.clone()), &deadline())
        .await
        .expect("fold");
    assert_eq!(folded.response.outcome, FoldWalOutcome::AlreadyCurrent);
    assert_eq!(folded.manifest.folded_wal_no, newest.read_basis.head.wal_no);
    assert!(input.read_basis.head.wal_no < newest.read_basis.head.wal_no);

    writer
        .engine
        .record_manifest_published(folded.manifest.clone());
    let moved = writer
        .engine
        .writer_position()
        .expect("the position moves onto the covering fold");
    assert_eq!(moved.read_basis.basis.manifest(), &folded.manifest.manifest);
    assert_eq!(moved.read_basis.head.seq, newest.read_basis.head.seq);
    assert_eq!(
        moved.read_basis.head.folded_wal_no,
        newest.read_basis.head.wal_no
    );
    assert_eq!(moved.wal_tail_objects(), 0);
    assert!(moved.tail.chunks().is_empty());
}
