//! Snapshot-to-fork pin handoff while collection retains a captured root set.

use super::*;
use crate::authorize::{Authorizer, ReadAccess};
use crate::gc::{gc_namespace, GcOptions};
use loonfs_objectstore::keys::{pin, pin_prefix};
use loonfs_test_support::stores::{MetadataMapStore, OperationKind};
use loonfs_types::format::control::{PinOwner, PinPayload};
use loonfs_types::AttributeInclusion;
use std::sync::atomic::{AtomicUsize, Ordering};

struct Fixture {
    _directory: tempfile::TempDir,
    store: Arc<MetadataMapStore<LocalFsStore>>,
    source: NamespaceId,
    target: NamespaceId,
    snapshot: PinPayload,
    replaced: Vec<String>,
    context: MutationContext,
}

impl Fixture {
    async fn new() -> Self {
        let content_writes = tokio::sync::Semaphore::new(32);
        let directory = tempdir().expect("directory");
        let source = NamespaceId::parse("snapshot-source").expect("source");
        let target = NamespaceId::parse("snapshot-target").expect("target");
        let store = Arc::new(MetadataMapStore::aged(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::any(),
        ));
        let setup = test_context();
        bootstrap_namespace(&store, &source, &setup)
            .await
            .expect("bootstrap");
        for path in ["/one", "/two"] {
            write_file_bytes(&store, &source, path, b"snapshot bytes", &setup, None)
                .await
                .expect("write");
            fold_wal(&store, &source).await.expect("fold");
        }
        let snapshot = crate::pin::create_pin(
            &store,
            &source,
            PinOwner::Snapshot {
                name: "historical".into(),
                expires_at_ms: u64::MAX,
            },
            &setup,
            Default::default(),
            None,
            &content_writes,
        )
        .await
        .expect("snapshot");
        let selected = load_current_manifest(&store, &source)
            .await
            .expect("snapshot manifest");
        write_file_bytes(&store, &source, "/later", b"new bytes", &setup, None)
            .await
            .expect("advance source");
        fold_wal(&store, &source).await.expect("fold");
        let report = compaction_step(
            &store,
            &source,
            selected.state.compactor_epoch(),
            MetadataLsmPolicy::default(),
            MetadataCompactionPolicy::CompactImmediately,
            Arc::default(),
        )
        .await
        .expect("replace old runs");
        assert!(matches!(
            report,
            CompactionStepOutcome::UnitPublished { .. }
        ));
        let current = load_current_manifest(&store, &source)
            .await
            .expect("current");
        let current_segments: BTreeSet<_> = current
            .state
            .envelope
            .payload()
            .runs
            .iter()
            .flat_map(|run| &run.segments)
            .map(metadata_segment_object_key)
            .collect();
        let replaced: Vec<_> = selected
            .state
            .envelope
            .payload()
            .runs
            .iter()
            .flat_map(|run| &run.segments)
            .map(metadata_segment_object_key)
            .filter(|key| !current_segments.contains(key))
            .collect();
        assert!(!replaced.is_empty());
        Self {
            _directory: directory,
            store,
            source,
            target,
            snapshot,
            replaced,
            // Old objects can be collected, but a fork pin created by this
            // attempt is still inside its installation grace.
            context: mutation_context("fork", crate::limits::UNREFERENCED_SEGMENT_MIN_AGE_MS + 1),
        }
    }

    async fn delete_snapshot(&self) {
        crate::pin::delete_snapshot(&self.store, &self.source, &self.snapshot.pin_id)
            .await
            .expect("delete snapshot");
    }

    async fn assert_replaced_present(&self, present: bool) {
        for key in &self.replaced {
            assert_eq!(
                self.store.head(key).await.expect("old segment").is_some(),
                present
            );
        }
    }
}

#[tokio::test]
async fn snapshot_fork_survives_snapshot_deletion_during_an_older_gc_pass() {
    let content_writes = tokio::sync::Semaphore::new(32);
    let fixture = Fixture::new().await;
    let gc_gate = BlockingStore::new(
        fixture.store.clone(),
        KeyPredicate::exact(metadata_manifest_prefix(&fixture.source)),
        OperationClass::List,
    );
    let fork_gate = BlockingStore::new(
        fixture.store.clone(),
        KeyPredicate::exact(metadata_manifest_object(&fixture.target, &ManifestNo(1))),
        OperationClass::PutCreateIfAbsent,
    );
    gc_gate.block_next();
    fork_gate.block_next();
    let collect = async {
        gc_namespace(
            &gc_gate,
            None,
            &fixture.source,
            &GcOptions::default(),
            &fixture.context,
        )
        .await
        .expect("old collector keeps its captured snapshot root");
        fixture.assert_replaced_present(true).await;
        fork_gate.release();
    };
    let fork = async {
        // The collector has captured its pins before this attempt creates one.
        gc_gate.wait_until_blocked().await;
        crate::namespace::fork::fork_namespace(
            &fork_gate,
            &fixture.source,
            &fixture.target,
            &loonfs_test_support::test_actor(),
            Some(&fixture.snapshot.pin_id),
            &fixture.context,
            Arc::new(StdMonotonicTimer::default()),
            Default::default(),
            None,
            &content_writes,
        )
        .await
        .expect("install historical fork")
    };
    let release_snapshot = async {
        fork_gate.wait_until_blocked().await;
        fixture.delete_snapshot().await;
        gc_gate.release();
    };
    tokio::join!(collect, fork, release_snapshot);
    gc_namespace(
        &fixture.store,
        None,
        &fixture.source,
        &GcOptions::default(),
        &fixture.context,
    )
    .await
    .expect("new collector keeps the fork root");
    fixture.assert_replaced_present(true).await;
    let view = load_current_metadata_view(&fixture.store, &fixture.target)
        .await
        .expect("fresh fork reader");
    let access = ReadAccess::live(Authorizer::Unrestricted);
    for path in ["/one", "/two"] {
        let file = view
            .get_file_bytes(&fixture.store, path, None, &access)
            .await
            .expect("snapshot bytes through fork");
        assert_eq!(file.bytes.as_slice(), b"snapshot bytes");
    }
    let later = view
        .resolve_path("/later", AttributeInclusion::Omit, &access)
        .await
        .expect_err("the fork must not include later source writes");
    assert_eq!(later.code(), ErrorCode::PathNotFound);
}

#[tokio::test]
async fn snapshot_fork_refuses_a_snapshot_deleted_before_post_write_verification() {
    let content_writes = tokio::sync::Semaphore::new(32);
    let fixture = Fixture::new().await;
    let key = pin(&fixture.source, &fixture.snapshot.pin_id);
    let reads = AtomicUsize::new(0);
    let gate = BlockingStore::matching(fixture.store.clone(), move |operation| {
        operation.key() == key
            && matches!(operation.kind(), OperationKind::GetWithMetadata)
            && reads.fetch_add(1, Ordering::SeqCst) == 1
    });
    gate.block_next();
    let actor = loonfs_test_support::test_actor();
    let (fork, ()) = tokio::join!(
        crate::namespace::fork::fork_namespace(
            &gate,
            &fixture.source,
            &fixture.target,
            &actor,
            Some(&fixture.snapshot.pin_id),
            &fixture.context,
            Arc::new(StdMonotonicTimer::default()),
            Default::default(),
            None,
            &content_writes,
        ),
        async {
            gate.wait_until_blocked().await;
            assert_eq!(
                fixture
                    .store
                    .list_prefix(&pin_prefix(&fixture.source))
                    .await
                    .expect("snapshot and tentative fork pin")
                    .len(),
                2
            );
            fixture.delete_snapshot().await;
            gc_namespace(
                &fixture.store,
                None,
                &fixture.source,
                &GcOptions::default(),
                &fixture.context,
            )
            .await
            .expect("tentative pin remains a root during verification");
            fixture.assert_replaced_present(true).await;
            gate.release();
        }
    );
    assert!(matches!(fork, Err(CoreError::SnapshotNotFound { .. })));
    assert!(fixture
        .store
        .list_prefix(&pin_prefix(&fixture.source))
        .await
        .expect("failed fork pin cleaned up")
        .is_empty());
    assert!(fixture
        .store
        .head(&metadata_manifest_object(&fixture.target, &ManifestNo(1)))
        .await
        .expect("no target installed")
        .is_none());
    gc_namespace(
        &fixture.store,
        None,
        &fixture.source,
        &GcOptions::default(),
        &fixture.context,
    )
    .await
    .expect("later collection sees no historical root");
    fixture.assert_replaced_present(false).await;
}

#[tokio::test]
async fn snapshot_fork_refuses_a_source_deleted_before_post_write_verification() {
    let content_writes = tokio::sync::Semaphore::new(32);
    let fixture = Fixture::new().await;
    let writer = acquire_writer_epoch(fixture.store.as_ref(), &fixture.source, &fixture.context)
        .await
        .expect("writer");
    let gate = BlockingStore::new(
        fixture.store.clone(),
        KeyPredicate::prefix(pin_prefix(&fixture.source)),
        OperationClass::PutCreateIfAbsent,
    );
    gate.block_next();
    let actor = loonfs_test_support::test_actor();
    let (fork, ()) = tokio::join!(
        crate::namespace::fork::fork_namespace(
            &gate,
            &fixture.source,
            &fixture.target,
            &actor,
            Some(&fixture.snapshot.pin_id),
            &fixture.context,
            Arc::new(StdMonotonicTimer::default()),
            Default::default(),
            None,
            &content_writes,
        ),
        async {
            gate.wait_until_blocked().await;
            crate::namespace::delete::delete_namespace(
                fixture.store.as_ref(),
                &fixture.source,
                Default::default(),
                writer,
                &fixture.context,
                &Deadline::start(Arc::new(StdMonotonicTimer::default())),
                Arc::default(),
                &content_writes,
            )
            .await
            .expect("delete source before the fork pin lands");
            gate.release();
        }
    );
    assert_eq!(
        fork.expect_err("deleted source").code(),
        ErrorCode::NamespaceDeleted
    );
    assert_eq!(
        fixture
            .store
            .list_prefix(&pin_prefix(&fixture.source))
            .await
            .expect("pins"),
        [pin(&fixture.source, &fixture.snapshot.pin_id)]
    );
    assert!(fixture
        .store
        .head(&metadata_manifest_object(&fixture.target, &ManifestNo(1)))
        .await
        .expect("no target installed")
        .is_none());
}

#[tokio::test]
async fn snapshot_fork_refuses_a_snapshot_that_expired_after_the_fork_call_began() {
    let content_writes = tokio::sync::Semaphore::new(32);
    let directory = tempdir().expect("directory");
    let source = NamespaceId::parse("snapshot-source").expect("source");
    let target = NamespaceId::parse("snapshot-target").expect("target");
    let inner = LocalFsStore::new(directory.path()).expect("store");
    let context = test_context();
    bootstrap_namespace(&inner, &source, &context)
        .await
        .expect("bootstrap");
    let snapshot = crate::pin::create_pin(
        &inner,
        &source,
        PinOwner::Snapshot {
            name: "short".into(),
            expires_at_ms: context.now_ms + 10,
        },
        &context,
        Default::default(),
        None,
        &content_writes,
    )
    .await
    .expect("snapshot");
    let store = BlockingStore::new(
        RecordingStore::new(inner, KeyPredicate::prefix(pin_prefix(&source))),
        KeyPredicate::exact(hint(&target)),
        OperationClass::Read,
    );
    let clock = Arc::new(loonfs_test_support::clock::ManualClock::new(0));
    let actor = loonfs_test_support::test_actor();
    store.block_next();
    let (fork, ()) = tokio::join!(
        crate::namespace::fork::fork_namespace(
            &store,
            &source,
            &target,
            &actor,
            Some(&snapshot.pin_id),
            &context,
            clock.clone(),
            Default::default(),
            None,
            &content_writes,
        ),
        async {
            store.wait_until_blocked().await;
            clock.advance_ms(10);
            store.release();
        }
    );
    assert!(
        matches!(fork, Err(CoreError::SnapshotGone { .. })),
        "{fork:?}"
    );
    assert_eq!(
        store.inner().counts().create_if_absent_puts,
        0,
        "the first expiry check refuses before a fork pin is written"
    );
}
