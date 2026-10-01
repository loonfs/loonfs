//! Numbered WAL verification and fence reclamation contracts.

// This module is the physical WAL boundary.
#![allow(clippy::disallowed_methods)]

use crate::commit_engine::{CommitCandidate, NamespaceCommitEngine};
use crate::context::MutationContext;
use crate::limits::READ_REVALIDATION_BOUND_MS;
use crate::namespace::control::load_current_manifest;
use crate::namespace::writer_epoch::{acquire_writer, acquire_writer_epoch};
use crate::path::read::load_current_metadata_view;
use crate::test_support::ops::create;
use crate::time::{Deadline, StdMonotonicTimer};
use loonfs_api::wire::wal::{decode_wal_object_envelope_zstd, encode_wal_object_envelope_zstd};
use loonfs_api::{
    AbsolutePath, AttributeInclusion, ChangeSeq, CommitId, ErrorCode, InodeId, ManifestNo,
    NamespaceId, WalNo, WriterEpoch, WriterId,
};
use loonfs_objectstore::keys::{hint, metadata_manifest_object, wal_object, wal_prefix};
use loonfs_objectstore::{local_fs_store::LocalFsStore, ObjectStore};
use loonfs_test_support::clock::ManualClock;
use loonfs_test_support::stores::{
    BlockingStore, ConcurrencyWatchStore, FailStore, InjectedError, KeyPredicate, MetadataMapStore,
    OperationClass, RecordingStore,
};
use std::sync::Arc;
use tempfile::tempdir;

fn context(now_ms: u64) -> crate::MutationContext {
    crate::MutationContext {
        writer_id: WriterId::parse("writer").expect("writer"),
        now_ms,
    }
}

#[tokio::test]
async fn readers_reject_invalid_numbers_epochs_sequences_and_allocation_summaries() {
    let directory = tempfile::tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let namespace_id = NamespaceId::parse("verification").expect("namespace");
    create(&store, &namespace_id, &context(1_000))
        .await
        .expect("create");
    for _ in 0..2 {
        acquire_writer_epoch(&store, &namespace_id, &context(1_000))
            .await
            .expect("fence");
    }
    let key = wal_object(&namespace_id, &WalNo(2));
    let original =
        decode_wal_object_envelope_zstd(&store.get(&key, None).await.expect("get").expect("fence"))
            .expect("decode");

    for changed in 0..5 {
        let mut payload = original.payload().clone();
        match changed {
            0 => payload.wal_no = WalNo(1),
            1 => payload.writer_epoch = WriterEpoch(0),
            2 => payload.writer_epoch = WriterEpoch(3),
            3 => payload.head_seq = ChangeSeq(1),
            _ => payload.next_inode_id = InodeId(3),
        }
        let bytes = encode_wal_object_envelope_zstd(payload)
            .expect("encode")
            .into_bytes();
        store
            .put_overwrite(&key, bytes.into())
            .await
            .expect("corrupt fixture");
        let error = load_current_metadata_view(&store, &namespace_id)
            .await
            .err()
            .expect("corruption");
        assert_eq!(
            error.code(),
            ErrorCode::NamespaceCorrupt,
            "case {changed}: {error}"
        );
    }
}

#[tokio::test]
async fn a_cold_anchor_reads_each_wal_object_once() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("hinted-fences").expect("namespace");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::prefix(wal_prefix(&namespace_id)),
    );
    create(&store, &namespace_id, &context(1_000))
        .await
        .expect("create");
    let mut engine = NamespaceCommitEngine::new(namespace_id.clone());
    for number in 0..9 {
        publish(&mut engine, &store, &format!("data-{number}"))
            .await
            .expect("data commit");
    }
    acquire_writer_epoch(&store, &namespace_id, &context(1_000))
        .await
        .expect("first fence");
    acquire_writer_epoch(&store, &namespace_id, &context(1_000))
        .await
        .expect("second fence");
    drop(engine);
    store.reset();

    let anchor = crate::namespace::read_anchor::load_read_anchor(&store, &namespace_id)
        .await
        .expect("cold discovery");
    let tail = crate::namespace::read_anchor::project_anchor_tail(&store, None, &anchor)
        .await
        .expect("replay discovery");
    assert_eq!(anchor.read_state.seq, ChangeSeq(9));
    assert_eq!(anchor.read_state.wal_no, WalNo(12));
    assert!(tail
        .rows
        .find_commit_receipt(&CommitId::parse("data-8").expect("commit"))
        .is_some());
    let mut gets = store.take_get_keys();
    gets.sort();
    assert_eq!(
        gets,
        (1..=15)
            .map(|number| wal_object(&namespace_id, &WalNo(number)))
            .collect::<Vec<_>>()
    );
    let absent_reads = gets.len() - anchor.tail.objects().len();
    assert_eq!(absent_reads, 3);
    assert!(absent_reads <= 7);

    let missing = wal_object(&namespace_id, &WalNo(10));
    store
        .delete(&missing)
        .await
        .expect("remove middle WAL object");
    let error = crate::namespace::read_anchor::load_read_anchor(&store, &namespace_id)
        .await
        .expect_err("gap in discovery window");
    assert!(
        matches!(error, crate::control_object::ControlObjectLoadError::Codec { object_key, .. } if object_key == missing)
    );
}

#[tokio::test]
async fn fences_fold_and_are_reclaimed_at_the_folded_boundary() {
    let directory = tempfile::tempdir().expect("directory");
    let store = MetadataMapStore::aged(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    let namespace_id = NamespaceId::parse("fences").expect("namespace");
    let setup = context(1_000);
    create(&store, &namespace_id, &setup).await.expect("create");
    acquire_writer_epoch(&store, &namespace_id, &setup)
        .await
        .expect("first fence");
    crate::manifest::fold_wal(&store, &namespace_id)
        .await
        .expect("fold fence");
    acquire_writer_epoch(&store, &namespace_id, &setup)
        .await
        .expect("second fence");
    let current = load_current_manifest(&store, &namespace_id)
        .await
        .expect("manifest");
    assert_eq!(current.state.envelope.payload().head_seq, ChangeSeq(0));
    assert_eq!(current.state.envelope.payload().folded_wal_no, WalNo(1));
    let options = crate::gc::GcOptions {
        grace_window_ms: crate::limits::GC_MIN_GRACE_WINDOW_MS,
    };
    let aged = context(options.grace_window_ms + 1);
    let report = crate::gc::gc_namespace(&store, &namespace_id, &options, &aged)
        .await
        .expect("collect");
    assert_eq!(report.deleted.wal_objects, 1);
    assert!(store
        .head(&wal_object(&namespace_id, &WalNo(1)))
        .await
        .expect("first")
        .is_none());
    assert!(store
        .head(&wal_object(&namespace_id, &WalNo(2)))
        .await
        .expect("second")
        .is_some());
    load_current_metadata_view(&store, &namespace_id)
        .await
        .expect("genesis remains readable");
}

#[tokio::test]
async fn a_same_sequence_writer_acquisition_does_not_cover_a_fence_fold() {
    use loonfs_test_support::stores::{BlockingStore, OperationClass};
    let directory = tempfile::tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("fold-race").expect("namespace");
    let store = BlockingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::exact(loonfs_objectstore::keys::metadata_manifest_object(
            &namespace_id,
            &loonfs_api::ManifestNo(3),
        )),
        OperationClass::PutCreateIfAbsent,
    );
    let setup = context(1_000);
    create(&store, &namespace_id, &setup).await.expect("create");
    acquire_writer_epoch(&store, &namespace_id, &setup)
        .await
        .expect("first fence");
    store.block_next();
    let (folded, acquired) =
        futures::join!(crate::manifest::fold_wal(&store, &namespace_id), async {
            store.wait_until_blocked().await;
            let acquired = acquire_writer_epoch(store.inner(), &namespace_id, &setup).await;
            store.release();
            acquired
        });
    acquired.expect("second fence");
    folded.expect("fold retries");
    let current = load_current_manifest(&store, &namespace_id)
        .await
        .expect("manifest");
    assert_eq!(current.state.envelope.payload().head_seq, ChangeSeq(0));
    assert_eq!(current.state.envelope.payload().folded_wal_no, WalNo(2));
}

#[tokio::test]
async fn an_acquisition_held_past_the_revalidation_bound_reloads_before_fencing() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("late-claim").expect("namespace");
    let store = BlockingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::exact(hint(&namespace_id)),
        OperationClass::CompareAndSwap,
    );
    let setup = context(1_000);
    create(&store, &namespace_id, &setup).await.expect("create");
    acquire_writer_epoch(&store, &namespace_id, &setup)
        .await
        .expect("first fence");
    let timer = Arc::new(ManualClock::new(0));
    store.block_next();
    let (acquired, ()) = futures::join!(
        acquire_writer(&store, &namespace_id, &setup, timer.clone()),
        async {
            // The claim has landed and its hint raise is held. Meanwhile a fold
            // folds the first fence, a compactor claim supersedes the fold, and
            // collection removes the folded manifest and the folded fence.
            store.wait_until_blocked().await;
            let claim = load_current_manifest(store.inner(), &namespace_id)
                .await
                .expect("claim")
                .state
                .manifest()
                .manifest_no;
            crate::manifest::fold_wal(store.inner(), &namespace_id)
                .await
                .expect("fold");
            crate::manifest::claim_compactor(store.inner(), &namespace_id)
                .await
                .expect("compactor claim");
            for key in [
                metadata_manifest_object(&namespace_id, &claim.successor().expect("next")),
                wal_object(&namespace_id, &WalNo(1)),
            ] {
                store.inner().delete(&key).await.expect("collect");
            }
            timer.advance_ms(READ_REVALIDATION_BOUND_MS);
            store.release();
        }
    );
    let (_, anchor) = acquired.expect("acquire");
    let current = load_current_manifest(&store, &namespace_id)
        .await
        .expect("manifest");
    assert_eq!(anchor.manifest.state.manifest(), current.state.manifest());
    assert_eq!(anchor.read_state.wal_no, WalNo(2));
    assert!(store
        .head(&wal_object(&namespace_id, &WalNo(1)))
        .await
        .expect("head")
        .is_none());
}

#[tokio::test]
async fn fence_retries_whose_anchor_loads_outlast_the_wal_budget_end_with_the_acquisition_budget() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("slow-anchor").expect("namespace");
    let timer = Arc::new(ManualClock::new(0));
    let slow_hint = Arc::clone(&timer);
    let store = FailStore::new(
        MetadataMapStore::new(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::hint(&namespace_id),
            move |metadata| {
                slow_hint.advance_ms(crate::limits::WAL_PUBLISH_BUDGET_MS + 1);
                metadata
            },
        ),
        KeyPredicate::prefix(wal_prefix(&namespace_id)),
        OperationClass::PutCreateIfAbsent,
        InjectedError::PreconditionFailed,
    );
    let setup = context(1_000);
    create(&store, &namespace_id, &setup).await.expect("create");
    store.fail_next(1);
    let error = acquire_writer(&store, &namespace_id, &setup, timer)
        .await
        .expect_err("the acquisition runs out of budget");
    assert!(matches!(
        error,
        crate::error::CoreError::MetadataPublicationBudgetExceeded { .. }
    ));
    assert_eq!(store.attempts(), 1);
    assert!(store
        .list_prefix(&wal_prefix(&namespace_id))
        .await
        .expect("WAL")
        .is_empty());
}

fn directory(name: &str) -> CommitCandidate {
    CommitCandidate::new(crate::path::write::CommitRequest::single(
        CommitId::parse(name).expect("commit"),
        loonfs_test_support::test_actor(),
        None,
        crate::path::write::FilesystemOperation::CreateDirectory {
            path: AbsolutePath::parse(format!("/{name}")).expect("path"),
            parents: false,
        },
    ))
}

pub(crate) async fn publish<S: ObjectStore>(
    engine: &mut NamespaceCommitEngine,
    store: &S,
    name: &str,
) -> crate::error::Result<loonfs_api::Commit> {
    engine
        .publish_batch(
            store,
            vec![directory(name)],
            &context(1_000),
            &Deadline::start(Arc::new(StdMonotonicTimer::default())),
        )
        .await
        .results
        .remove(0)
}

#[tokio::test]
async fn a_number_collision_returns_after_one_attempt_and_a_retry_commits_the_next_number() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("race").expect("namespace");
    let store = std::sync::Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    create(&store, &namespace_id, &context(1_000))
        .await
        .expect("create");
    let mut first = NamespaceCommitEngine::new(namespace_id.clone());
    publish(&mut first, &store, "seed").await.expect("seed");
    let mut second = first.clone();
    store.reset();
    let blocked = BlockingStore::new(
        store.clone(),
        KeyPredicate::exact(wal_object(&namespace_id, &WalNo(3))),
        OperationClass::PutCreateIfAbsent,
    );
    blocked.block_next();
    let (loser, winner) = futures::join!(publish(&mut first, &blocked, "left"), async {
        blocked.wait_until_blocked().await;
        let result = publish(&mut second, &store, "right").await;
        blocked.release();
        result
    });
    assert_eq!(winner.expect("winner").committed_seq, ChangeSeq(2));
    assert_eq!(loser.expect_err("collision").code(), ErrorCode::StaleHead);
    assert_eq!(store.counts().create_if_absent_puts, 2);
    assert_eq!(
        publish(&mut first, &store, "left")
            .await
            .expect("retry")
            .committed_seq,
        ChangeSeq(3)
    );
    assert_eq!(store.counts().create_if_absent_puts, 3);
    assert_eq!(store.counts().compare_and_swaps, 0);
    let view = load_current_metadata_view(&store, &namespace_id)
        .await
        .expect("view");
    for path in ["/left", "/right"] {
        view.resolve_path(
            path,
            AttributeInclusion::Omit,
            &crate::authorize::ReadAccess::live(crate::authorize::Authorizer::Unrestricted),
        )
        .await
        .expect("committed");
    }
}

#[tokio::test]
async fn a_stale_writer_collides_with_the_fence_and_writes_nothing_else() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("fencing").expect("namespace");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    create(&store, &namespace_id, &context(1_000))
        .await
        .expect("create");
    let mut stale = NamespaceCommitEngine::with_unshared_head_state(namespace_id.clone());
    publish(&mut stale, &store, "old")
        .await
        .expect("old writer");
    let acquired = acquire_writer_epoch(&store, &namespace_id, &context(1_000))
        .await
        .expect("takeover fence");
    store.reset();
    for expected in [ErrorCode::StaleHead, ErrorCode::WriterFenced] {
        assert_eq!(
            publish(&mut stale, &store, "stale")
                .await
                .expect_err("displaced writer")
                .code(),
            expected
        );
    }
    assert_eq!(store.counts().puts, 1);
    assert_eq!(store.counts().compare_and_swaps, 0);
    assert!(store
        .head(&wal_object(&namespace_id, &WalNo(4)))
        .await
        .expect("next")
        .is_none());
    let session = std::sync::Arc::new(std::sync::Mutex::new(
        crate::commit_engine::WriterSessionState::Acquired(acquired),
    ));
    let mut active = NamespaceCommitEngine::new(namespace_id.clone()).writer_session(session);
    assert_eq!(
        publish(&mut active, &store, "new")
            .await
            .expect("new writer")
            .committed_seq,
        ChangeSeq(2)
    );
}

#[tokio::test]
async fn cold_open_probes_past_a_lagging_hint_and_reads_a_missing_hint_as_absent() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("lagging").expect("namespace");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    create(&store, &namespace_id, &context(1_000))
        .await
        .expect("create");
    let mut engine = NamespaceCommitEngine::new(namespace_id.clone());
    publish(&mut engine, &store, "one").await.expect("one");
    publish(&mut engine, &store, "two").await.expect("two");
    let hint_bytes = loonfs_api::wire::control::encode_control_state(
        loonfs_api::wire::control::ControlObjectKind::Hint,
        &loonfs_api::wire::control::HintPayload {
            namespace_id: namespace_id.clone(),
            manifest_no: ManifestNo(1),
        },
    )
    .expect("hint");
    store
        .put_overwrite(&hint(&namespace_id), hint_bytes.into())
        .await
        .expect("lag hint");
    drop(engine);
    store.reset();
    load_current_metadata_view(&store, &namespace_id)
        .await
        .expect("cold view")
        .resolve_path(
            "/two",
            AttributeInclusion::Omit,
            &crate::authorize::ReadAccess::live(crate::authorize::Authorizer::Unrestricted),
        )
        .await
        .expect("tip");
    assert!(store.snapshot().iter().any(|operation| operation.key()
        == wal_object(&namespace_id, &WalNo(4))
        && matches!(
            operation,
            loonfs_test_support::stores::RecordedOperation::Get { .. }
        )));
    store
        .delete(&hint(&namespace_id))
        .await
        .expect("remove hint");
    assert_eq!(
        crate::namespace::status::load_namespace(&store, &namespace_id)
            .await
            .expect_err("missing hint")
            .code(),
        ErrorCode::NamespaceNotFound
    );
}

#[tokio::test]
async fn a_number_published_during_a_window_is_read_again_not_reported_missing() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("window-race").expect("namespace");
    let store = BlockingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::prefix(wal_object(&namespace_id, &WalNo(6))),
        OperationClass::Get,
    );
    create(&store, &namespace_id, &context(1_000))
        .await
        .expect("create");
    let mut engine = NamespaceCommitEngine::new(namespace_id.clone());
    for name in ["one", "two", "three"] {
        publish(&mut engine, &store, name).await.expect(name);
    }
    // WAL 1 through 4 exist. The window 4..7 reads 5 as absent, then 5 and 6
    // are published while the read of 6 is parked, so 6 comes back present.
    store.block_next();
    let (anchor, ()) = futures::join!(
        crate::namespace::read_anchor::load_read_anchor(&store, &namespace_id),
        async {
            store.wait_until_blocked().await;
            for name in ["four", "five"] {
                publish(&mut engine, &store, name).await.expect(name);
            }
            store.release();
        }
    );
    let anchor = anchor.expect("discovery across concurrent publishes");
    assert_eq!(anchor.read_state.wal_no, WalNo(6));
    assert_eq!(anchor.tail.objects().len(), 6);
}

#[tokio::test]
async fn a_bounded_tail_load_overlaps_reads_and_matches_sequential_replay() {
    use super::reader::{load_wal_object, load_wal_tail, WalWalk};
    use crate::store_waves::STORE_READ_WAVE;

    let directory = tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let namespace_id = NamespaceId::parse("bounded-replay").expect("namespace");
    create(&store, &namespace_id, &context(1_000))
        .await
        .expect("create");
    let mut engine = NamespaceCommitEngine::new(namespace_id.clone());
    for number in 1..20 {
        publish(&mut engine, &store, &format!("directory-{number}"))
            .await
            .expect("publish");
    }
    let mut walk = WalWalk::after(&namespace_id, WalNo(0), ChangeSeq(0), WriterEpoch(1));
    let mut sequential = Vec::new();
    for number in 1..=20 {
        let loaded = load_wal_object(&store, &namespace_id, WalNo(number)).await;
        let envelope = loaded
            .envelope
            .expect("sequential load")
            .expect("sequential WAL object");
        sequential.push(
            walk.validate(loaded.object_key, envelope)
                .expect("sequential validation"),
        );
    }
    let watched =
        ConcurrencyWatchStore::new(store, KeyPredicate::prefix(wal_prefix(&namespace_id)));
    let tail = load_wal_tail(
        &watched,
        super::WalTailLoadRequest {
            namespace_id: &namespace_id,
            base_seq: ChangeSeq(0),
            head_seq: ChangeSeq(19),
            base_wal_no: WalNo(0),
            tip_wal_no: WalNo(20),
            writer_epoch: WriterEpoch(1),
        },
    )
    .await
    .expect("bounded load");

    assert_eq!(watched.reads().total, 20);
    assert!(watched.reads().peak_in_flight > 1);
    assert!(watched.reads().peak_in_flight <= STORE_READ_WAVE);
    assert_eq!(tail, super::ValidatedWalTail::new(sequential));
}

#[tokio::test]
async fn a_bounded_tail_load_names_the_missing_wal_object() {
    let directory = tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let namespace_id = NamespaceId::parse("gap").expect("namespace");
    create(&store, &namespace_id, &context(1_000))
        .await
        .expect("create");
    let mut engine = NamespaceCommitEngine::new(namespace_id.clone());
    for name in ["one", "two", "three"] {
        publish(&mut engine, &store, name).await.expect(name);
    }
    let missing = wal_object(&namespace_id, &WalNo(3));
    store
        .delete(&missing)
        .await
        .expect("remove the middle WAL object");
    let lowest_missing = wal_object(&namespace_id, &WalNo(2));
    store
        .delete(&lowest_missing)
        .await
        .expect("remove an earlier WAL object");
    let error = super::reader::load_wal_tail(
        &store,
        super::WalTailLoadRequest {
            namespace_id: &namespace_id,
            base_seq: ChangeSeq(0),
            head_seq: ChangeSeq(3),
            base_wal_no: WalNo(0),
            tip_wal_no: WalNo(4),
            writer_epoch: WriterEpoch(1),
        },
    )
    .await
    .expect_err("a gap below the tip is corruption");
    assert!(
        matches!(error, super::WalTailLoadError::MissingWalObject { object_key } if object_key == lowest_missing)
    );
}

#[tokio::test]
async fn a_fold_and_collection_during_tip_discovery_cannot_reuse_a_wal_number() {
    use loonfs_test_support::stores::MetadataMapStore;
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("tip-gc").expect("namespace");
    let grace = crate::limits::GC_MIN_GRACE_WINDOW_MS;
    let options = crate::gc::GcOptions {
        grace_window_ms: grace,
    };
    let store = MetadataMapStore::aged(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    let store = MetadataMapStore::new(
        store,
        KeyPredicate::new(|key| {
            loonfs_objectstore::layout::manifest_no_of(key)
                .is_some_and(|number| number >= ManifestNo(3))
        }),
        move |mut metadata| {
            metadata.last_modified_ms = Some(grace + 1);
            metadata
        },
    );
    let store = BlockingStore::new(
        store,
        KeyPredicate::exact(wal_object(&namespace_id, &WalNo(1))),
        OperationClass::Read,
    );
    create(&store, &namespace_id, &context(1_000))
        .await
        .expect("create");
    let mut engine = NamespaceCommitEngine::new(namespace_id.clone());
    publish(&mut engine, &store, "seed").await.expect("seed");
    engine.invalidate_projection();
    store.block_next();
    let (published, ()) = futures::join!(publish(&mut engine, &store, "after-gc"), async {
        store.wait_until_blocked().await;
        crate::manifest::fold_wal(store.inner(), &namespace_id)
            .await
            .expect("fold old WAL");
        crate::manifest::advance_retention_floor(store.inner(), &namespace_id)
            .await
            .expect("advance floor");
        let aged = MutationContext {
            now_ms: grace + 1,
            ..context(1_000)
        };
        crate::gc::gc_namespace(store.inner(), &namespace_id, &options, &aged)
            .await
            .expect("collect old WAL");
        assert!(store
            .inner()
            .head(&wal_object(&namespace_id, &WalNo(1)))
            .await
            .expect("old fence")
            .is_none());
        store.release();
    });
    assert_eq!(
        published
            .expect("replanned from folded state")
            .committed_seq,
        ChangeSeq(2)
    );
    assert!(store
        .head(&wal_object(&namespace_id, &WalNo(1)))
        .await
        .expect("old number")
        .is_none());
    assert!(store
        .head(&wal_object(&namespace_id, &WalNo(3)))
        .await
        .expect("new number")
        .is_some());
    let view = load_current_metadata_view(&store, &namespace_id)
        .await
        .expect("view");
    for path in ["/seed", "/after-gc"] {
        view.resolve_path(
            path,
            AttributeInclusion::Omit,
            &crate::authorize::ReadAccess::live(crate::authorize::Authorizer::Unrestricted),
        )
        .await
        .expect("file");
    }
}

#[tokio::test]
async fn a_warm_probe_reports_a_broken_chain_at_its_own_epoch_as_corruption() {
    let directory = tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let namespace_id = NamespaceId::parse("warm-probe").expect("namespace");
    create(&store, &namespace_id, &context(1_000))
        .await
        .expect("create");
    acquire_writer_epoch(&store, &namespace_id, &context(1_000))
        .await
        .expect("fence");
    let loaded = crate::namespace::read_anchor::load_read_anchor(&store, &namespace_id)
        .await
        .expect("warm head");
    let fence = decode_wal_object_envelope_zstd(
        &store
            .get(&wal_object(&namespace_id, &loaded.read_state.wal_no), None)
            .await
            .expect("get")
            .expect("fence"),
    )
    .expect("decode");
    let mut broken = fence.payload().clone();
    broken.wal_no = loaded
        .read_state
        .wal_no
        .successor()
        .expect("next WAL number");
    broken.head_seq = ChangeSeq(5);
    store
        .put_if_absent(
            &wal_object(&namespace_id, &broken.wal_no),
            encode_wal_object_envelope_zstd(broken)
                .expect("encode")
                .into_bytes()
                .into(),
        )
        .await
        .expect("same-epoch WAL object");
    let mut warm = crate::RuntimeReadContext {
        basis: loaded.basis(),
        head: loaded.read_state,
        segment_cache: std::sync::Arc::new(crate::cache::MetadataSegmentCache::unshared(
            usize::MAX,
        )),
        head_state: std::sync::Arc::new(crate::cache::HeadStateCache::unshared(usize::MAX)),
    };
    let error = super::probe_namespace_wal(&store, &mut warm)
        .await
        .expect_err("a same-epoch chain break is corruption, not a stale head");
    assert!(
        matches!(
            error,
            crate::control_object::ControlObjectLoadError::Codec { .. }
        ),
        "{error}"
    );
}

#[tokio::test]
async fn fence_publication_enforces_the_wal_budget_before_the_put() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("fence-budget").expect("namespace");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::prefix(wal_prefix(&namespace_id)),
    );
    let head = crate::namespace::state::NamespaceReadState::initial(
        namespace_id.clone(),
        1_000,
        loonfs_test_support::test_actor(),
    );
    let fence = super::prepare_wal_object(namespace_id, WriterEpoch(1), &head, &[]).expect("fence");
    let timer = std::sync::Arc::new(loonfs_test_support::clock::ManualClock::new(0));
    let expired = crate::time::Observation::now(timer.clone());
    timer.advance_ms(1);
    let boundary = crate::time::Observation::now(timer.clone());
    timer.advance_ms(crate::limits::WAL_PUBLISH_BUDGET_MS);
    let error = super::publish_wal_object(&store, &fence, &expired)
        .await
        .expect_err("expired budget");
    assert_eq!(error.code(), ErrorCode::StaleHead);
    assert_eq!(store.counts().puts, 0);
    super::publish_wal_object(&store, &fence, &boundary)
        .await
        .expect("budget boundary");
    assert_eq!(store.counts().create_if_absent_puts, 1);
}

#[tokio::test]
async fn a_wal_put_returning_after_its_budget_has_an_unknown_outcome() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("wal-return-budget").expect("namespace");
    let timer = Arc::new(ManualClock::new(0));
    let tip = crate::time::Observation::now(timer.clone());
    let store = MetadataMapStore::new(
        RecordingStore::new(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::any(),
        ),
        KeyPredicate::prefix(wal_prefix(&namespace_id)),
        move |metadata| {
            timer.advance_ms(crate::limits::WAL_PUBLISH_BUDGET_MS + 1);
            metadata
        },
    );
    let head = crate::namespace::state::NamespaceReadState::initial(
        namespace_id.clone(),
        1_000,
        loonfs_test_support::test_actor(),
    );
    let fence =
        super::prepare_wal_object(namespace_id.clone(), WriterEpoch(1), &head, &[]).expect("fence");
    let error = super::publish_wal_object(&store, &fence, &tip)
        .await
        .expect_err("late put must not acknowledge publication");
    assert!(matches!(
        error,
        crate::error::CoreError::WalPublish(crate::commit::WalPublishError::OutcomeUnknown(_))
    ));
    assert_eq!(store.inner().counts().create_if_absent_puts, 1);
    assert_eq!(store.inner().snapshot().len(), 1);
    assert!(store
        .inner()
        .inner()
        .head(&wal_object(&namespace_id, &WalNo(1)))
        .await
        .expect("landed fence")
        .is_some());
}

#[tokio::test]
async fn a_writer_resuming_after_its_fence_was_collected_does_not_acknowledge_its_put() {
    let temp_dir = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("sleep-budget").expect("namespace");
    let store_clock = ManualClock::new(0);
    let store = MetadataMapStore::aged(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::any(),
    );
    let timer_a = Arc::new(ManualClock::new(0));
    let timer_b = Arc::new(ManualClock::new(0));
    let context_a = context(store_clock.now_ms());
    let context_b = MutationContext {
        writer_id: WriterId::parse("writer-b").expect("writer"),
        ..context_a.clone()
    };
    create(&store, &namespace_id, &context_a)
        .await
        .expect("create");
    let mut writer_a =
        NamespaceCommitEngine::new(namespace_id.clone()).monotonic_timer(timer_a.clone());
    writer_a
        .publish_batch(
            &store,
            [directory("before-sleep")],
            &context_a,
            &Deadline::start(timer_a.clone()),
        )
        .await
        .results
        .remove(0)
        .expect("writer A observes its tip");
    let blocked = BlockingStore::new(
        store,
        KeyPredicate::exact(wal_object(&namespace_id, &WalNo(3))),
        OperationClass::PutCreateIfAbsent,
    );
    blocked.block_next();
    let batch = Deadline::start(timer_a.clone());
    let (mut resumed, ()) = futures::join!(
        writer_a.publish_batch(&blocked, [directory("after-sleep")], &context_a, &batch,),
        async {
            blocked.wait_until_blocked().await;
            let mut writer_b =
                NamespaceCommitEngine::new(namespace_id.clone()).monotonic_timer(timer_b.clone());
            writer_b
                .publish_batch(
                    blocked.inner(),
                    [directory("takeover")],
                    &context_b,
                    &Deadline::start(timer_b.clone()),
                )
                .await
                .results
                .remove(0)
                .expect("writer B takes the epoch and commits");
            let fence_key = wal_object(&namespace_id, &WalNo(3));
            let fence = decode_wal_object_envelope_zstd(
                &blocked
                    .inner()
                    .get(&fence_key, None)
                    .await
                    .expect("get")
                    .expect("fence"),
            )
            .expect("decode fence");
            assert_eq!(fence.payload().writer_epoch, WriterEpoch(2));
            assert!(fence.payload().records.is_empty());
            crate::manifest::fold_wal_with_deadline(
                blocked.inner(),
                &namespace_id,
                &Deadline::start(timer_b.clone()),
                crate::manifest::MetadataLsmPolicy::default(),
            )
            .await
            .expect("fold takeover and commit");
            let options = crate::gc::GcOptions {
                grace_window_ms: crate::limits::GC_MIN_GRACE_WINDOW_MS,
            };
            store_clock.advance_ms(options.grace_window_ms + 1);
            let report = crate::gc::gc_namespace(
                blocked.inner(),
                &namespace_id,
                &options,
                &MutationContext {
                    now_ms: store_clock.now_ms(),
                    ..context_b.clone()
                },
            )
            .await
            .expect("collect folded fence");
            assert_eq!(report.deleted.wal_objects, 4);
            assert!(blocked
                .inner()
                .head(&fence_key)
                .await
                .expect("fence removed")
                .is_none());
            assert_eq!(timer_a.now_ms(), 0);
            timer_a.advance_ms(store_clock.now_ms());
            blocked.release();
        }
    );
    assert_eq!(
        resumed
            .results
            .remove(0)
            .expect_err("reclaimed number must not acknowledge a commit")
            .code(),
        ErrorCode::CommitOutcomeUnknown,
    );
    assert!(blocked
        .inner()
        .head(&wal_object(&namespace_id, &WalNo(3)))
        .await
        .expect("late put landed")
        .is_some());
    let view = load_current_metadata_view(&blocked, &namespace_id)
        .await
        .expect("current view");
    let access = crate::authorize::ReadAccess::live(crate::authorize::Authorizer::Unrestricted);
    assert_eq!(view.head().seq, ChangeSeq(2));
    view.resolve_path("/takeover", AttributeInclusion::Omit, &access)
        .await
        .expect("live commit");
    assert_eq!(
        view.resolve_path("/after-sleep", AttributeInclusion::Omit, &access)
            .await
            .expect_err("late commit is not replayed")
            .code(),
        ErrorCode::PathNotFound
    );
}
