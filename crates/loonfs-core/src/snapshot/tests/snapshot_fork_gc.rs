//! The handoff from a user snapshot to a fork snapshot while collection retains a captured root set.

use super::*;
use crate::authorize::{Authorizer, ReadAccess};
use loonfs_objectstore::keys::{snapshot_prefix, snapshot_record};
use loonfs_objectstore::ObjectStoreExtension;
use loonfs_test_support::stores::{MetadataMapStore, OperationKind};
use loonfs_types::format::control::{NamespaceStatus, SnapshotOwner, SnapshotPayload};
use loonfs_types::format::manifest::NamespaceManifestPayload;
use loonfs_types::options::AttributeInclusion;
use std::assert_matches;
use std::sync::atomic::{AtomicUsize, Ordering};

struct Fixture {
    _directory: tempfile::TempDir,
    store: Arc<MetadataMapStore<LocalFsStore>>,
    source: NamespaceId,
    target: NamespaceId,
    snapshot: SnapshotPayload,
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
        let namespace = crate::test_support::namespace_store(&store, &source);
        let snapshot = namespace
            .create_snapshot_record(
                SnapshotOwner::User {
                    name: Some("historical".into()),
                    expires_at_ms: Some(u64::MAX),
                    deleted_at_ms: None,
                },
                crate::test_support::folded_manifest(&namespace)
                    .await
                    .expect("fold the WAL tail"),
                &setup.clock(),
            )
            .await
            .expect("snapshot");
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
        .with_run_policy(crate::test_support::one_delta_run_policy())
        .engine()
        .compact_metadata_unit(
            None,
            selected.compactor_epoch(),
            &crate::compaction::MetadataCompactionCancellation::default(),
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
            // The aged store reports every object as old, so a collection at
            // this time can delete any object that no root keeps.
            context: writer_at("fork", crate::limits::GC_GRACE_WINDOW_MS + 1),
        }
    }

    async fn delete_snapshot(&self) {
        crate::test_support::NamespaceFixture::new(
            self.store.as_ref(),
            &self.source,
            &crate::test_support::writer_at("test-writer", 0),
        )
        .engine()
        .delete_snapshot(&self.snapshot.snapshot_id)
        .await
        .expect("delete snapshot");
    }

    /// Asserts that the target's only manifest is an initializing manifest 1,
    /// and returns it.
    async fn assert_target_initializing(&self) -> NamespaceManifestPayload {
        assert!(self
            .store
            .head(&metadata_manifest_object(&self.target, &ManifestNo(2)))
            .await
            .expect("head manifest 2")
            .is_none());
        let manifest_1 = crate::test_support::namespace_store(self.store.as_ref(), &self.target)
            .load_current_manifest()
            .await
            .expect("the target's manifest 1");
        assert_eq!(manifest_1.payload().manifest_no, ManifestNo(1));
        assert_eq!(
            manifest_1.payload().status,
            NamespaceStatus::Initializing {}
        );
        manifest_1.payload().clone()
    }

    /// Returns the keys of the source's snapshot records.
    async fn source_snapshot_keys(&self) -> Vec<String> {
        self.store
            .list_prefix(&snapshot_prefix(&self.source))
            .await
            .expect("list the source's snapshots")
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
async fn snapshot_fork_survives_snapshot_deletion_during_an_older_collection() {
    let fixture = Fixture::new().await;
    let gc_gate = BlockingStore::new(
        fixture.store.clone(),
        KeyPredicate::exact(metadata_manifest_prefix(&fixture.source)),
        OperationSelector::List,
    );
    let fork_gate = BlockingStore::new(
        fixture.store.clone(),
        KeyPredicate::exact(metadata_manifest_object(&fixture.target, &ManifestNo(2))),
        OperationSelector::PutCreateIfAbsent,
    );
    gc_gate.block_next();
    fork_gate.block_next();
    let collect = async {
        crate::test_support::namespace_store(&gc_gate, &fixture.source)
            .gc(&fixture.context.clock())
            .await
            .expect("old collector keeps its captured snapshot root");
        fixture.assert_replaced_present(true).await;
        fork_gate.release();
    };
    let fork = async {
        // The collector has captured its snapshots before this attempt creates one.
        gc_gate.wait_until_blocked().await;
        crate::test_support::NamespaceFixture::new(&fork_gate, &fixture.source, &fixture.context)
            .engine()
            .fork_namespace_at_snapshot(
                &fixture.target,
                &loonfs_test_support::test_actor(),
                &fixture.snapshot.snapshot_id,
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
        .gc(&fixture.context.clock())
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

/// Checks that a fork whose snapshot is deleted between the fork snapshot's
/// write and its second check returns `snapshot_not_found`, leaves the target
/// initializing, and keeps the fork snapshot, which keeps the snapshot's
/// manifest for a collection of the source.
#[tokio::test]
async fn snapshot_fork_refuses_a_snapshot_deleted_before_post_write_verification() {
    let fixture = Fixture::new().await;
    let key = snapshot_record(&fixture.source, &fixture.snapshot.snapshot_id);
    let reads = AtomicUsize::new(0);
    // The third read of the snapshot is the check after the fork snapshot's
    // write.
    let gate = BlockingStore::matching(fixture.store.clone(), move |operation| {
        operation.key() == key
            && matches!(operation.kind(), OperationKind::GetWithMetadata)
            && reads.fetch_add(1, Ordering::SeqCst) == 2
    });
    gate.block_next();
    let actor = loonfs_test_support::test_actor();
    let engine =
        crate::test_support::NamespaceFixture::new(&gate, &fixture.source, &fixture.context)
            .engine();
    let (fork, ()) = tokio::join!(
        engine.fork_namespace_at_snapshot(&fixture.target, &actor, &fixture.snapshot.snapshot_id),
        async {
            gate.wait_until_blocked().await;
            assert_eq!(fixture.source_snapshot_keys().await.len(), 2);
            fixture.delete_snapshot().await;
            crate::test_support::namespace_store(&fixture.store, &fixture.source)
                .gc(&fixture.context.clock())
                .await
                .expect("the fork snapshot is a root during the check");
            fixture.assert_replaced_present(true).await;
            gate.release();
        }
    );
    assert_matches!(fork, Err(CoreError::SnapshotNotFound { .. }));
    let manifest_1 = fixture.assert_target_initializing().await;
    let fork_snapshot_id = &manifest_1
        .fork_basis
        .as_ref()
        .expect("a fork basis")
        .source_snapshot_id;
    assert_eq!(
        fixture.source_snapshot_keys().await,
        [snapshot_record(&fixture.source, fork_snapshot_id)]
    );
    crate::test_support::namespace_store(&fixture.store, &fixture.source)
        .gc(&fixture.context.clock())
        .await
        .expect("a later collection keeps the fork snapshot's manifest");
    fixture.assert_replaced_present(true).await;
}

/// Checks that a fork whose source is deleted while the fork snapshot's put
/// waits returns `namespace_deleted`, leaves the target initializing, and
/// keeps the fork snapshot.
#[tokio::test]
async fn snapshot_fork_refuses_a_source_deleted_before_post_write_verification() {
    let fixture = Fixture::new().await;
    let writer = crate::test_support::namespace_store(fixture.store.as_ref(), &fixture.source)
        .acquire_writer_epoch(&fixture.context.writer_id)
        .await
        .expect("writer");
    let gate = BlockingStore::new(
        fixture.store.clone(),
        KeyPredicate::prefix(snapshot_prefix(&fixture.source)),
        OperationSelector::PutCreateIfAbsent,
    );
    gate.block_next();
    let actor = loonfs_test_support::test_actor();
    let engine =
        crate::test_support::NamespaceFixture::new(&gate, &fixture.source, &fixture.context)
            .engine();
    let (fork, ()) = tokio::join!(
        engine.fork_namespace_at_snapshot(&fixture.target, &actor, &fixture.snapshot.snapshot_id),
        async {
            gate.wait_until_blocked().await;
            crate::test_support::namespace_store(fixture.store.as_ref(), &fixture.source)
                .delete_namespace(Default::default(), writer, None, &fixture.context.clock())
                .await
                .expect("delete source before the fork snapshot is written");
            gate.release();
        }
    );
    assert_eq!(
        fork.expect_err("deleted source").code(),
        ErrorCode::NamespaceDeleted
    );
    let manifest_1 = fixture.assert_target_initializing().await;
    let fork_snapshot_id = &manifest_1
        .fork_basis
        .as_ref()
        .expect("a fork basis")
        .source_snapshot_id;
    let mut expected = vec![
        snapshot_record(&fixture.source, &fixture.snapshot.snapshot_id),
        snapshot_record(&fixture.source, fork_snapshot_id),
    ];
    expected.sort();
    assert_eq!(fixture.source_snapshot_keys().await, expected);
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
    let namespace = crate::test_support::namespace_store(&inner, &source);
    let snapshot = namespace
        .create_snapshot_record(
            SnapshotOwner::User {
                name: Some("short".into()),
                expires_at_ms: Some(context.now_ms + 10),
                deleted_at_ms: None,
            },
            crate::test_support::folded_manifest(&namespace)
                .await
                .expect("fold the WAL tail"),
            &context.clock(),
        )
        .await
        .expect("snapshot");
    // The fork's second read of the snapshot, the check before the fork
    // snapshot's write, blocks, and the snapshot expires while it does.
    let key = snapshot_record(&source, &snapshot.snapshot_id);
    let reads = AtomicUsize::new(0);
    let store = BlockingStore::matching(
        RecordingStore::new(inner, KeyPredicate::prefix(snapshot_prefix(&source))),
        move |operation| {
            operation.key() == key
                && matches!(operation.kind(), OperationKind::GetWithMetadata)
                && reads.fetch_add(1, Ordering::SeqCst) == 1
        },
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
        namespace.fork_namespace_at_snapshot(&target, &actor, &snapshot.snapshot_id),
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
        "the expiry check refuses before a fork snapshot is written"
    );
    let manifest_1 = crate::test_support::namespace_store(store.inner().inner(), &target)
        .load_current_manifest()
        .await
        .expect("the target's manifest 1");
    assert_eq!(manifest_1.payload().manifest_no, ManifestNo(1));
    assert_eq!(
        manifest_1.payload().status,
        NamespaceStatus::Initializing {}
    );
}
