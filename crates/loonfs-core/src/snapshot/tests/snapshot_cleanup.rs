//! Collection recovery after failed or concurrent snapshot cleanup.

use super::*;
use crate::limits::{GC_GRACE_WINDOW_MS, MAX_CONTENTION_ATTEMPTS};
use crate::namespace::control::LoadedManifest;
use crate::namespace::held::Held;
use loonfs_objectstore::keys::{self, snapshot_prefix};
use loonfs_objectstore::ObjectStoreExtension;
use loonfs_test_support::ids::page_limit;
use loonfs_test_support::stores::{replace_object, MetadataMapStore};
use loonfs_types::format::control::{SnapshotOwner, SnapshotPayload};
use loonfs_types::PageRequest;
use std::assert_matches;

/// Returns the snapshot record that a failed creation wrote for `owner` on
/// `basis`'s manifest at `created_at_ms`. The record's key comes from
/// `error`, the store error that the creation's put returned.
pub(super) fn record_of_failed_creation(
    error: &CoreError,
    basis: &Held<LoadedManifest>,
    owner: SnapshotOwner,
    created_at_ms: u64,
) -> SnapshotPayload {
    let CoreError::Store { object_key, .. } = error else {
        panic!("expected the store error of the record's put, got {error:?}");
    };
    let (namespace_id, snapshot_id) =
        record::snapshot_record_key_ids(object_key).expect("a snapshot record key");
    let manifest = basis.manifest().manifest();
    SnapshotPayload {
        snapshot_id,
        namespace_id,
        head_seq: manifest.head_seq,
        payload_checksum: manifest.payload_checksum,
        created_at_ms,
        owner,
    }
}

/// Puts `put`, the record of a failed snapshot creation, as a put of that
/// creation that the store applies after the call returned. Checks that the
/// store refuses the put, that the record at the key is `put` with a delete
/// time, that a read by ID and the snapshot listing don't return the
/// snapshot, and that collection deletes the record once the grace window has
/// passed after its delete time.
pub(super) async fn assert_a_late_put_is_refused_by_a_deleted_snapshot<S: ObjectStore>(
    store: &S,
    put: &SnapshotPayload,
) {
    let namespace_id = &put.namespace_id;
    let key = keys::snapshot_record(namespace_id, &put.snapshot_id);
    assert_matches!(
        store
            .put_if_absent(&key, record::encode_snapshot_record(put))
            .await,
        Err(ObjectStoreError::PreconditionFailed { .. }),
        "a put of the creation that the store applies late is refused"
    );
    let records = crate::test_support::deleted_snapshot_records(store, namespace_id).await;
    let [stored] = &records[..] else {
        panic!("expected one snapshot record, got {records:?}");
    };
    let deleted_at_ms = stored
        .owner
        .deleted_at_ms()
        .expect("the record has a delete time");
    let mut undeleted = stored.clone();
    if let SnapshotOwner::User { deleted_at_ms, .. } = &mut undeleted.owner {
        *deleted_at_ms = None;
    }
    assert_eq!(&undeleted, put, "the record is the creation's own");
    let namespace = crate::test_support::namespace_store(store, namespace_id);
    let live = crate::test_support::read_context(store, namespace_id).await;
    let reader = crate::NamespaceEngine::reader(namespace);
    let administrator = crate::test_support::administrator(&reader);
    assert_eq!(
        reader
            .get_snapshot(&administrator, &live, &put.snapshot_id)
            .await
            .expect_err("a deleted snapshot can't be read")
            .code(),
        ErrorCode::SnapshotNotFound
    );
    let listed = reader
        .list_snapshots_page(
            &administrator,
            &live,
            PageRequest {
                limit: page_limit(10),
                cursor: None,
            },
        )
        .await
        .expect("list snapshots");
    assert!(listed.items.is_empty(), "listed {:?}", listed.items);
    for (now_ms, deleted) in [
        (deleted_at_ms + GC_GRACE_WINDOW_MS - 1, 0),
        (deleted_at_ms + GC_GRACE_WINDOW_MS, 1),
    ] {
        let collected = crate::test_support::namespace_store(store, namespace_id)
            .gc(&writer_at("collector", now_ms).clock())
            .await
            .expect("collect");
        assert_eq!(collected.deleted_snapshots_by_owner.user, deleted);
    }
    assert!(store.head(&key).await.expect("snapshot record").is_none());
}

/// Checks that a creation whose every put of the snapshot record had an
/// unknown outcome writes a deleted record at the snapshot's key, so a put
/// of the creation that the store applies after the call returned can't
/// leave an active snapshot, with or without an expiry.
#[tokio::test]
async fn a_creation_whose_puts_were_lost_leaves_a_deleted_snapshot_that_refuses_a_late_put() {
    for expires_at_ms in [None, Some(10 * GC_GRACE_WINDOW_MS)] {
        let directory = tempdir().expect("directory");
        let namespace_id = NamespaceId::parse("demo").expect("namespace");
        let store = FailStore::new(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::prefix(snapshot_prefix(&namespace_id)),
            OperationSelector::PutCreateIfAbsent,
            InjectedError::Transport("lost snapshot write request".to_owned()),
        );
        let context = test_context();
        bootstrap_namespace(&store, &namespace_id, &context)
            .await
            .expect("bootstrap");
        let namespace = crate::test_support::namespace_store(&store, &namespace_id);
        let basis = crate::test_support::folded_manifest(&namespace)
            .await
            .expect("fold the WAL tail");
        let owner = SnapshotOwner::User {
            name: Some("lost".to_owned()),
            expires_at_ms,
            deleted_at_ms: None,
        };
        let clock = context.clock();
        store.fail_next(MAX_CONTENTION_ATTEMPTS);
        let error = namespace
            .create_snapshot_record(owner.clone(), basis.clone(), &clock)
            .await
            .expect_err("every put of the record had an unknown outcome");
        assert_eq!(store.remaining(), 0, "every attempt's put was lost");
        let put = record_of_failed_creation(&error, &basis, owner, clock.request_at_ms());
        assert_a_late_put_is_refused_by_a_deleted_snapshot(&store, &put).await;
    }
}

/// Checks the sequence of events where a quiet namespace's current manifest is
/// replaced and collected between a snapshot's fold and its record put, and
/// the creator's delete of the unverified record fails. Later collections
/// succeed, and they delete the record once its provider age reaches the grace
/// window, although the snapshot has no expiry.
#[tokio::test]
async fn an_unverified_snapshot_left_without_its_manifest_is_deleted_at_the_grace_window() {
    let owner = SnapshotOwner::User {
        name: Some("abandoned".to_owned()),
        expires_at_ms: None,
        deleted_at_ms: None,
    };
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    // Every object is old, as in a quiet namespace, except the snapshot
    // record, whose provider timestamp is the creator's clock.
    let store = BlockingStore::new(
        FailStore::new(
            MetadataMapStore::new(
                MetadataMapStore::aged(
                    LocalFsStore::new(directory.path()).expect("store"),
                    KeyPredicate::any(),
                ),
                KeyPredicate::prefix(snapshot_prefix(&namespace_id)),
                |mut metadata| {
                    metadata.last_modified_at_ms = Some(1_000);
                    metadata
                },
            ),
            KeyPredicate::prefix(snapshot_prefix(&namespace_id)),
            OperationSelector::CompareAndSwap,
            InjectedError::Transport("cleanup unavailable".to_owned()),
        ),
        KeyPredicate::prefix(snapshot_prefix(&namespace_id)),
        OperationSelector::PutCreateIfAbsent,
    );
    let setup = writer_at("creator", 1_000);
    bootstrap_namespace(&store, &namespace_id, &setup)
        .await
        .expect("bootstrap");
    for path in ["/one", "/two"] {
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
            .write_file_bytes(path, b"data", None)
            .await
            .expect("write");
        crate::test_support::namespace_store(&store, &namespace_id)
            .fold_wal()
            .await
            .expect("fold");
    }
    let selected = crate::test_support::namespace_store(&store, &namespace_id)
        .load_current_manifest()
        .await
        .expect("selected manifest");
    store.inner().fail_next(MAX_CONTENTION_ATTEMPTS);
    store.block_next();
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let clock = setup.clock();
    let (creation, ()) = tokio::join!(
        namespace.create_snapshot_record(
            owner,
            Held::new(
                selected.clone(),
                crate::test_support::observed_now(),
                Default::default(),
            ),
            &clock
        ),
        async {
            store.wait_until_blocked().await;
            super::snapshot_verification::compact_and_collect_replaced_segments(
                &store,
                &namespace_id,
                &selected,
            )
            .await;
            assert!(store
                .head(&selected.object_key)
                .await
                .expect("old manifest")
                .is_none());
            store.release();
        }
    );
    assert_eq!(
        creation.expect_err("verification failed").code(),
        ErrorCode::StaleHead,
        "a failed cleanup keeps the verification error"
    );
    assert_eq!(store.inner().attempts(), MAX_CONTENTION_ATTEMPTS);
    store.inner().clear();
    let snapshots = store
        .list_prefix(&snapshot_prefix(&namespace_id))
        .await
        .expect("snapshots");
    assert_eq!(
        snapshots.len(),
        1,
        "failed cleanup left its unverified snapshot"
    );

    let grace_window_ms = GC_GRACE_WINDOW_MS;
    for now_ms in [1_000, 1_000 + grace_window_ms - 1] {
        let young = crate::test_support::namespace_store(&store, &namespace_id)
            .gc(&writer_at("collector", now_ms).clock())
            .await
            .expect("a record without its manifest doesn't fail the collection");
        assert_eq!(young.deleted_snapshots_by_owner.user, 0);
        assert!(store.head(&snapshots[0]).await.expect("snapshot").is_some());
    }
    let collected = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(&writer_at("collector", 1_000 + grace_window_ms).clock())
        .await
        .expect("delete the record once its provider age reaches the grace window");
    assert_eq!(collected.deleted_snapshots_by_owner.user, 1);
    assert!(store.head(&snapshots[0]).await.expect("snapshot").is_none());
    crate::test_support::namespace_store(&store, &namespace_id)
        .gc(&writer_at("collector", 1_001 + grace_window_ms).clock())
        .await
        .expect("next collection also succeeds");
    crate::test_support::namespace_store(&store, &namespace_id)
        .load_current_metadata_view()
        .await
        .expect("current view remains readable");
}

#[tokio::test]
async fn snapshot_removed_after_listing_does_not_make_a_collected_basis_corruption() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let base = MetadataMapStore::aged(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    let setup = test_context();
    bootstrap_namespace(&base, &namespace_id, &setup)
        .await
        .expect("bootstrap");
    let snapshot = create_snapshot(&base, &namespace_id, &setup)
        .await
        .expect("snapshot");
    crate::test_support::NamespaceFixture::new(&base, &namespace_id, &setup)
        .write_file_bytes("/new", b"data", None)
        .await
        .expect("write");
    crate::test_support::namespace_store(&base, &namespace_id)
        .fold_wal()
        .await
        .expect("fold");
    let old_manifest = metadata_manifest_object(&namespace_id, &snapshot.manifest_no);
    let store = BlockingStore::new(
        base,
        KeyPredicate::exact(&old_manifest),
        OperationSelector::Read,
    );
    let aged = writer_at("collector", crate::limits::GC_GRACE_WINDOW_MS + 1);
    store.block_next();
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let clock = aged.clock();
    let (collection, ()) = tokio::join!(namespace.gc(&clock), async {
        store.wait_until_blocked().await;
        store
            .inner()
            .delete(&keys::snapshot_record(&namespace_id, &snapshot.snapshot_id))
            .await
            .expect("release snapshot");
        crate::test_support::namespace_store(store.inner(), &namespace_id)
            .gc(&aged.clock())
            .await
            .expect("other collector");
        assert!(store
            .inner()
            .head(&old_manifest)
            .await
            .expect("manifest")
            .is_none());
        store.release();
    });
    collection.expect("concurrent snapshot release is not corruption");
}

#[tokio::test]
async fn missing_basis_checks_each_snapshot_and_propagates_snapshot_read_errors() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = FailStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::prefix(snapshot_prefix(&namespace_id)),
        OperationSelector::Read,
        InjectedError::Transport("snapshot unavailable".to_owned()),
    );
    let setup = writer_at("creator", 1_000);
    bootstrap_namespace(&store, &namespace_id, &setup)
        .await
        .expect("bootstrap");
    let manifest = crate::test_support::namespace_store(&store, &namespace_id)
        .load_current_manifest()
        .await
        .expect("manifest");
    let mut ids = [
        SnapshotId::generate(manifest.manifest().manifest_no),
        SnapshotId::generate(manifest.manifest().manifest_no),
    ];
    ids.sort();
    for (index, id) in ids.iter().enumerate() {
        record::write_snapshot_record(
            &store,
            &loonfs_types::format::control::SnapshotPayload {
                snapshot_id: id.clone(),
                namespace_id: namespace_id.clone(),
                head_seq: manifest.manifest().head_seq,
                payload_checksum: manifest.manifest().payload_checksum.clone(),
                created_at_ms: setup.now_ms,
                owner: if index == 0 {
                    SnapshotOwner::User {
                        name: Some(format!("snapshot-{index}")),
                        expires_at_ms: Some(2_000),
                        deleted_at_ms: None,
                    }
                } else {
                    SnapshotOwner::Fork {
                        target_namespace_id: NamespaceId::parse("fork").expect("namespace"),
                    }
                },
            },
            &setup.clock(),
        )
        .await
        .expect("write snapshot");
    }
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .write_file_bytes("/new", b"data", None)
        .await
        .expect("write");
    crate::test_support::namespace_store(&store, &namespace_id)
        .fold_wal()
        .await
        .expect("fold");
    // A fork snapshot's manifest is protected from its creation, so losing it
    // is corruption, even when the first listed snapshot for the same
    // manifest is independently eligible for deletion.
    store
        .delete(&manifest.object_key)
        .await
        .expect("remove snapshot's manifest");
    let aged = writer_at("collector", 2_000 + crate::limits::GC_GRACE_WINDOW_MS);
    let error = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(&aged.clock())
        .await
        .expect_err("the fork snapshot still requires the missing basis");
    assert_eq!(error.code(), ErrorCode::NamespaceCorrupt);
    assert!(error.to_string().contains(ids[1].as_str()));
    store.fail_all();
    let error = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(&aged.clock())
        .await
        .expect_err("unreadable snapshot is not absent");
    assert_eq!(error.code(), ErrorCode::ServerError);
    store.clear();
    let key = keys::snapshot_record(&namespace_id, &ids[0]);
    replace_object(&store, &key, Bytes::from_static(b"not json"))
        .await
        .expect("malformed snapshot");
    let error = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(&aged.clock())
        .await
        .expect_err("malformed snapshot is not deletable");
    assert_eq!(error.code(), ErrorCode::NamespaceCorrupt);
    for id in ids {
        assert!(store
            .head(&keys::snapshot_record(&namespace_id, &id))
            .await
            .expect("snapshot")
            .is_some());
    }
}

/// A store whose first create-if-absent put of a snapshot record is lost
/// before it's applied, and whose read-back of that put waits until the test
/// releases it.
fn lost_put_store(
    directory: &std::path::Path,
    namespace_id: &NamespaceId,
) -> BlockingStore<FailStore<LocalFsStore>> {
    let store = BlockingStore::new(
        FailStore::new(
            LocalFsStore::new(directory).expect("store"),
            KeyPredicate::prefix(snapshot_prefix(namespace_id)),
            OperationSelector::PutCreateIfAbsent,
            InjectedError::Transport("lost snapshot write request".to_owned()),
        ),
        KeyPredicate::prefix(snapshot_prefix(namespace_id)),
        OperationSelector::GetWithMetadata,
    );
    store.inner().fail_next(1);
    store.block_next();
    store
}

/// Checks that a creation whose first put was lost, and whose budget passed
/// while the read-back of that put found nothing, sends no second put. The
/// creation returns the lost put's store error, and its cleanup leaves a
/// deleted record, because the lost put could still be applied.
#[tokio::test]
async fn a_creation_whose_budget_ends_after_a_lost_put_sends_no_second_put() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let context = test_context();
    let setup = LocalFsStore::new(directory.path()).expect("store");
    bootstrap_namespace(&setup, &namespace_id, &context)
        .await
        .expect("bootstrap");
    let basis = crate::test_support::folded_manifest(&crate::test_support::namespace_store(
        &setup,
        &namespace_id,
    ))
    .await
    .expect("fold the WAL tail");
    let store = lost_put_store(directory.path(), &namespace_id);
    let timer = Arc::new(loonfs_test_support::clock::ManualClock::new(0));
    let clock = crate::time::OperationClock::start(context.now_ms, timer.clone());
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let (created, ()) = tokio::join!(
        namespace.create_snapshot_record(
            SnapshotOwner::User {
                name: Some("stalled".to_owned()),
                expires_at_ms: None,
                deleted_at_ms: None,
            },
            basis,
            &clock,
        ),
        async {
            store.wait_until_blocked().await;
            timer.advance_ms(crate::limits::METADATA_PUBLICATION_BUDGET_MS + 1);
            store.release();
        }
    );
    let error = created.expect_err("the creation ran out of budget after a lost put");
    assert_matches!(error, CoreError::Store { .. });
    assert_eq!(
        store.inner().attempts(),
        2,
        "the lost put and the cleanup's put of the deleted record"
    );
    assert_eq!(
        crate::test_support::deleted_snapshot_records(&store, &namespace_id)
            .await
            .len(),
        1
    );
}

/// Checks that a fork snapshot's write whose first put was lost, and whose
/// budget passed while the read-back of that put found nothing, sends no
/// second put and writes nothing, so a stalled fork call can't write a fork
/// snapshot again after the fork's retirement deleted it.
#[tokio::test]
async fn a_fork_snapshot_write_whose_budget_ends_after_a_lost_put_writes_nothing() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let context = test_context();
    let setup = LocalFsStore::new(directory.path()).expect("store");
    bootstrap_namespace(&setup, &namespace_id, &context)
        .await
        .expect("bootstrap");
    let manifest = crate::test_support::namespace_store(&setup, &namespace_id)
        .load_current_manifest()
        .await
        .expect("manifest");
    let record = SnapshotPayload {
        snapshot_id: SnapshotId::generate(manifest.manifest().manifest_no),
        namespace_id: namespace_id.clone(),
        head_seq: manifest.manifest().head_seq,
        payload_checksum: manifest.manifest().payload_checksum.clone(),
        created_at_ms: context.now_ms,
        owner: SnapshotOwner::Fork {
            target_namespace_id: NamespaceId::parse("fork").expect("namespace"),
        },
    };
    let store = lost_put_store(directory.path(), &namespace_id);
    let timer = Arc::new(loonfs_test_support::clock::ManualClock::new(0));
    let clock = crate::time::OperationClock::start(context.now_ms, timer.clone());
    let (written, ()) = tokio::join!(
        record::write_snapshot_record(&store, &record, &clock),
        async {
            store.wait_until_blocked().await;
            timer.advance_ms(crate::limits::METADATA_PUBLICATION_BUDGET_MS + 1);
            store.release();
        }
    );
    assert_matches!(written, Err(CoreError::Store { .. }));
    assert_eq!(store.inner().attempts(), 1, "only the lost put");
    assert!(store
        .list_prefix(&snapshot_prefix(&namespace_id))
        .await
        .expect("snapshots")
        .is_empty());
}
