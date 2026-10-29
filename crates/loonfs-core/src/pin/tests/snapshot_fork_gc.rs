//! Snapshot-to-fork pin handoff while collection retains a captured root set.

use super::*;
use crate::authorize::{Authorizer, ReadAccess};
use crate::gc::GcOptions;
use loonfs_objectstore::keys::{pin, pin_prefix};
use loonfs_test_support::stores::{MetadataMapStore, OperationKind};
use loonfs_types::format::control::{PinOwner, PinPayload};
use loonfs_types::AttributeInclusion;
use std::assert_matches;
use std::sync::atomic::{AtomicUsize, Ordering};

struct Fixture {
    _directory: tempfile::TempDir,
    store: Arc<MetadataMapStore<LocalFsStore>>,
    source: NamespaceId,
    target: NamespaceId,
    snapshot: PinPayload,
    replaced: Vec<String>,
    context: WriterAt,
}

impl Fixture {
    async fn new() -> Self {
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
            crate::test_support::NamespaceFixture::new(&store, &source, &setup)
                .write_file_bytes(path, b"snapshot bytes", None)
                .await
                .expect("write");
            crate::test_support::namespace_store(&store, &source)
                .fold_wal()
                .await
                .expect("fold");
        }
        let snapshot = crate::test_support::namespace_store(&store, &source)
            .create_pin(
                PinOwner::User {
                    name: "historical".into(),
                    expires_at_ms: Some(u64::MAX),
                },
                &setup.clock(),
            )
            .await
            .expect("snapshot")
            .response;
        let selected = crate::test_support::namespace_store(&store, &source)
            .load_current_manifest()
            .await
            .expect("snapshot manifest");
        crate::test_support::NamespaceFixture::new(&store, &source, &setup)
            .write_file_bytes("/later", b"new bytes", None)
            .await
            .expect("advance source");
        crate::test_support::namespace_store(&store, &source)
            .fold_wal()
            .await
            .expect("fold");
        let report = crate::test_support::NamespaceFixture::new(
            &store,
            &source,
            &crate::test_support::writer_at("test-writer", 0),
        )
        .engine()
        .compact_metadata_unit(
            None,
            MetadataCompactionPolicy::CompactImmediately,
            crate::manifest::CompactionAdmission::AnySize,
            selected.compactor_epoch(),
            &crate::manifest::MetadataCompactionCancellation::default(),
        )
        .await
        .expect("replace old runs");
        assert_matches!(report, CompactionUnitOutcome::Published { .. });
        let current = crate::test_support::namespace_store(&store, &source)
            .load_current_manifest()
            .await
            .expect("current");
        let current_segments: BTreeSet<_> = current
            .payload()
            .runs
            .iter()
            .flat_map(|run| &run.segments)
            .map(metadata_segment_object_key)
            .collect();
        let replaced: Vec<_> = selected
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
            context: writer_at(
                "fork",
                crate::limits::UNREFERENCED_SEGMENT_GRACE_WINDOW_MS + 1,
            ),
        }
    }

    async fn delete_snapshot(&self) {
        crate::test_support::NamespaceFixture::new(
            self.store.as_ref(),
            &self.source,
            &crate::test_support::writer_at("test-writer", 0),
        )
        .engine()
        .delete_snapshot(&self.snapshot.pin_id)
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
    let fixture = Fixture::new().await;
    let gc_gate = BlockingStore::new(
        fixture.store.clone(),
        KeyPredicate::exact(metadata_manifest_prefix(&fixture.source)),
        OperationSelector::List,
    );
    let fork_gate = BlockingStore::new(
        fixture.store.clone(),
        KeyPredicate::exact(metadata_manifest_object(&fixture.target, &ManifestNo(1))),
        OperationSelector::PutCreateIfAbsent,
    );
    gc_gate.block_next();
    fork_gate.block_next();
    let collect = async {
        crate::test_support::namespace_store(&gc_gate, &fixture.source)
            .gc(None, &GcOptions::default(), &fixture.context.clock())
            .await
            .expect("old collector keeps its captured snapshot root");
        fixture.assert_replaced_present(true).await;
        fork_gate.release();
    };
    let fork = async {
        // The collector has captured its pins before this attempt creates one.
        gc_gate.wait_until_blocked().await;
        crate::test_support::NamespaceFixture::new(&fork_gate, &fixture.source, &fixture.context)
            .engine()
            .fork_namespace(
                &fixture.target,
                &loonfs_test_support::test_actor(),
                Some(&fixture.snapshot.pin_id),
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
    crate::test_support::namespace_store(&fixture.store, &fixture.source)
        .gc(None, &GcOptions::default(), &fixture.context.clock())
        .await
        .expect("new collector keeps the fork root");
    fixture.assert_replaced_present(true).await;
    let namespace = crate::test_support::namespace_store(&fixture.store, &fixture.target);
    let view = namespace
        .load_current_metadata_view()
        .await
        .expect("fresh fork reader");
    let access = ReadAccess::live(Authorizer::Unrestricted);
    for path in ["/one", "/two"] {
        let file = view
            .read_file_bytes(path, None, &access)
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
    let engine =
        crate::test_support::NamespaceFixture::new(&gate, &fixture.source, &fixture.context)
            .engine();
    let (fork, ()) = tokio::join!(
        engine.fork_namespace(&fixture.target, &actor, Some(&fixture.snapshot.pin_id)),
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
            crate::test_support::namespace_store(&fixture.store, &fixture.source)
                .gc(None, &GcOptions::default(), &fixture.context.clock())
                .await
                .expect("tentative pin remains a root during verification");
            fixture.assert_replaced_present(true).await;
            gate.release();
        }
    );
    assert_matches!(fork, Err(CoreError::SnapshotNotFound { .. }));
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
    crate::test_support::namespace_store(&fixture.store, &fixture.source)
        .gc(None, &GcOptions::default(), &fixture.context.clock())
        .await
        .expect("later collection sees no historical root");
    fixture.assert_replaced_present(false).await;
}

#[tokio::test]
async fn snapshot_fork_refuses_a_source_deleted_before_post_write_verification() {
    let fixture = Fixture::new().await;
    let writer = crate::test_support::namespace_store(fixture.store.as_ref(), &fixture.source)
        .acquire_writer_epoch(&fixture.context.writer_id)
        .await
        .expect("writer");
    let gate = BlockingStore::new(
        fixture.store.clone(),
        KeyPredicate::prefix(pin_prefix(&fixture.source)),
        OperationSelector::PutCreateIfAbsent,
    );
    gate.block_next();
    let actor = loonfs_test_support::test_actor();
    let engine =
        crate::test_support::NamespaceFixture::new(&gate, &fixture.source, &fixture.context)
            .engine();
    let (fork, ()) = tokio::join!(
        engine.fork_namespace(&fixture.target, &actor, Some(&fixture.snapshot.pin_id)),
        async {
            gate.wait_until_blocked().await;
            crate::test_support::namespace_store(fixture.store.as_ref(), &fixture.source)
                .delete_namespace(Default::default(), writer, None, &fixture.context.clock())
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
    let directory = tempdir().expect("directory");
    let source = NamespaceId::parse("snapshot-source").expect("source");
    let target = NamespaceId::parse("snapshot-target").expect("target");
    let inner = LocalFsStore::new(directory.path()).expect("store");
    let context = test_context();
    bootstrap_namespace(&inner, &source, &context)
        .await
        .expect("bootstrap");
    let snapshot = crate::test_support::namespace_store(&inner, &source)
        .create_pin(
            PinOwner::User {
                name: "short".into(),
                expires_at_ms: Some(context.now_ms + 10),
            },
            &context.clock(),
        )
        .await
        .expect("snapshot")
        .response;
    let store = BlockingStore::new(
        RecordingStore::new(inner, KeyPredicate::prefix(pin_prefix(&source))),
        KeyPredicate::exact(hint(&target)),
        OperationSelector::Read,
    );
    let clock = Arc::new(loonfs_test_support::clock::ManualClock::new(0));
    let actor = loonfs_test_support::test_actor();
    store.block_next();
    let namespace = {
        let mut fixture = crate::test_support::NamespaceFixture::new(&store, &source, &context);
        fixture.namespace.timer = clock.clone();
        fixture
    }
    .engine();
    let (fork, ()) = tokio::join!(
        namespace.fork_namespace(&target, &actor, Some(&snapshot.pin_id)),
        async {
            store.wait_until_blocked().await;
            clock.advance_ms(10);
            store.release();
        }
    );
    assert_matches!(fork, Err(CoreError::SnapshotNotFound { .. }));
    assert_eq!(
        store.inner().counts().create_if_absent_puts,
        0,
        "the first expiry check refuses before a fork pin is written"
    );
}
