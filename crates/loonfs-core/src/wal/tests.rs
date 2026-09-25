//! Numbered WAL verification and fence reclamation contracts.

// This module is the physical WAL boundary.
#![allow(clippy::disallowed_methods)]

use crate::commit_engine::{CommitCandidate, NamespaceCommitEngine};
use crate::context::MutationContext;
use crate::namespace::{control::load_current_manifest, writer_epoch::acquire_writer_epoch};
use crate::path::read::load_current_metadata_view;
use crate::protocol::PublishTailOptions;
use crate::test_support::ops::create;
use crate::time::{Deadline, StdMonotonicTimer};
use loonfs_api::wire::wal::{decode_wal_segment_envelope_zstd, encode_wal_segment_envelope_zstd};
use loonfs_api::{
    AbsolutePath, AttributeInclusion, ChangeSeq, CommitId, ErrorCode, InodeId, ManifestNo,
    NamespaceId, WalNo, WriterEpoch, WriterId,
};
use loonfs_objectstore::keys::{hint, wal_segment, wal_segment_prefix};
use loonfs_objectstore::{local_fs_store::LocalFsStore, ObjectStore};
use loonfs_test_support::clock::ManualClock;
use loonfs_test_support::stores::{
    BlockingStore, KeyPredicate, MetadataMapStore, OperationClass, RecordingStore,
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
    let key = wal_segment(&namespace_id, &WalNo(2));
    let original = decode_wal_segment_envelope_zstd(
        &store.get(&key, None).await.expect("get").expect("fence"),
    )
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
        let bytes = encode_wal_segment_envelope_zstd(payload)
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
async fn hinted_fences_carry_the_head_without_reading_earlier_wal() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("hinted-fences").expect("namespace");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::prefix(wal_segment_prefix(&namespace_id)),
    );
    create(&store, &namespace_id, &context(1_000))
        .await
        .expect("create");
    let mut engine = NamespaceCommitEngine::new(namespace_id.clone());
    let committed = publish(&mut engine, &store, "data")
        .await
        .expect("data commit");
    acquire_writer_epoch(&store, &namespace_id, &context(1_000))
        .await
        .expect("first fence");
    acquire_writer_epoch(&store, &namespace_id, &context(1_000))
        .await
        .expect("second fence");
    crate::namespace::control::raise_namespace_hint(&store, &namespace_id, WalNo(3), None)
        .await
        .expect("raise hint");
    drop(engine);
    store.reset();

    let head = crate::namespace::control::load_namespace_read_state(&store, &namespace_id)
        .await
        .expect("cold discovery");
    assert_eq!(head.seq, committed.committed_seq);
    assert_eq!(
        store.take_get_keys(),
        vec![
            wal_segment(&namespace_id, &WalNo(3)),
            wal_segment(&namespace_id, &WalNo(4)),
            wal_segment(&namespace_id, &WalNo(5)),
        ]
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
    crate::checkpoint::flush_wal(&store, &namespace_id)
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
    let config = crate::gc::GcConfig {
        grace_window_ms: crate::limits::GC_MIN_GRACE_WINDOW_MS,
    };
    let aged = context(config.grace_window_ms + 1);
    let report = crate::gc::gc_namespace(&store, &namespace_id, &config, &aged)
        .await
        .expect("collect");
    assert_eq!(report.deleted.wal_segments, 1);
    assert!(store
        .head(&wal_segment(&namespace_id, &WalNo(1)))
        .await
        .expect("first")
        .is_none());
    assert!(store
        .head(&wal_segment(&namespace_id, &WalNo(2)))
        .await
        .expect("second")
        .is_some());
    load_current_metadata_view(&store, &namespace_id)
        .await
        .expect("genesis remains readable");
}

#[tokio::test]
async fn a_same_sequence_writer_acquisition_does_not_cover_a_fence_flush() {
    use loonfs_test_support::stores::{BlockingStore, OperationClass};
    let directory = tempfile::tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("flush-race").expect("namespace");
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
    let (flushed, acquired) =
        futures::join!(crate::checkpoint::flush_wal(&store, &namespace_id), async {
            store.wait_until_blocked().await;
            let acquired = acquire_writer_epoch(store.inner(), &namespace_id, &setup).await;
            store.release();
            acquired
        });
    acquired.expect("second fence");
    flushed.expect("flush retries");
    let current = load_current_manifest(&store, &namespace_id)
        .await
        .expect("manifest");
    assert_eq!(current.state.envelope.payload().head_seq, ChangeSeq(0));
    assert_eq!(current.state.envelope.payload().folded_wal_no, WalNo(2));
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
            &PublishTailOptions::default(),
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
        KeyPredicate::exact(wal_segment(&namespace_id, &WalNo(3))),
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
    let mut stale = NamespaceCommitEngine::new(namespace_id.clone());
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
        .head(&wal_segment(&namespace_id, &WalNo(4)))
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
            wal_no: WalNo(0),
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
        == wal_segment(&namespace_id, &WalNo(4))
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
async fn a_bounded_tail_load_names_the_missing_segment() {
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
    let hint_bytes = loonfs_api::wire::control::encode_control_state(
        loonfs_api::wire::control::ControlObjectKind::Hint,
        &loonfs_api::wire::control::HintPayload {
            namespace_id: namespace_id.clone(),
            manifest_no: ManifestNo(1),
            wal_no: WalNo(4),
        },
    )
    .expect("hint");
    store
        .put_overwrite(&hint(&namespace_id), hint_bytes.into())
        .await
        .expect("hint at the tip");
    let missing = wal_segment(&namespace_id, &WalNo(3));
    store
        .delete(&missing)
        .await
        .expect("remove the middle segment");
    let error = load_current_metadata_view(&store, &namespace_id)
        .await
        .err()
        .expect("a gap below the tip is corruption");
    assert_eq!(error.code(), ErrorCode::NamespaceCorrupt, "{error}");
    assert!(error.to_string().contains(&missing), "{error}");
}

#[tokio::test]
async fn a_flush_and_collection_during_tip_discovery_cannot_reuse_a_wal_number() {
    use loonfs_test_support::stores::MetadataMapStore;
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("tip-gc").expect("namespace");
    let grace = crate::limits::GC_MIN_GRACE_WINDOW_MS;
    let config = crate::gc::GcConfig {
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
        KeyPredicate::exact(wal_segment(&namespace_id, &WalNo(1))),
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
        crate::checkpoint::flush_wal(store.inner(), &namespace_id)
            .await
            .expect("fold old WAL");
        crate::checkpoint::advance_retention_floor(store.inner(), &namespace_id)
            .await
            .expect("advance floor");
        let aged = MutationContext {
            now_ms: grace + 1,
            ..context(1_000)
        };
        crate::gc::gc_namespace(store.inner(), &namespace_id, &config, &aged)
            .await
            .expect("collect old WAL");
        assert!(store
            .inner()
            .head(&wal_segment(&namespace_id, &WalNo(1)))
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
        .head(&wal_segment(&namespace_id, &WalNo(1)))
        .await
        .expect("old number")
        .is_none());
    assert!(store
        .head(&wal_segment(&namespace_id, &WalNo(3)))
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
    let fence = decode_wal_segment_envelope_zstd(
        &store
            .get(&wal_segment(&namespace_id, &loaded.read_state.wal_no), None)
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
            &wal_segment(&namespace_id, &broken.wal_no),
            encode_wal_segment_envelope_zstd(broken)
                .expect("encode")
                .into_bytes()
                .into(),
        )
        .await
        .expect("same-epoch segment");
    let mut warm = crate::RuntimeReadContext {
        basis: loaded.basis(),
        head: loaded.read_state,
        segment_cache: std::sync::Arc::new(crate::cache::MetadataSegmentCache::new(
            Default::default(),
        )),
        tail_cache: std::sync::Arc::new(crate::cache::WalTailProjectionCache::new(
            crate::cache::WalTailProjectionCacheConfig {
                max_entries: 1,
                max_rows: usize::MAX,
                max_decoded_bytes: usize::MAX,
            },
            None,
        )),
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
        KeyPredicate::prefix(wal_segment_prefix(&namespace_id)),
    );
    let head = crate::namespace::state::NamespaceReadState::initial(
        namespace_id.clone(),
        1_000,
        loonfs_test_support::test_actor(),
    );
    let fence = super::prepare_segment(namespace_id, WriterEpoch(1), &head, &[]).expect("fence");
    let timer = std::sync::Arc::new(loonfs_test_support::clock::ManualClock::new(0));
    let expired = crate::time::Observation::now(timer.clone());
    timer.advance_ms(1);
    let boundary = crate::time::Observation::now(timer.clone());
    timer.advance_ms(crate::limits::WAL_PUBLISH_BUDGET_MS);
    let error = super::publish_segment(&store, &fence, &expired)
        .await
        .expect_err("expired budget");
    assert_eq!(error.code(), ErrorCode::StaleHead);
    assert_eq!(store.counts().puts, 0);
    super::publish_segment(&store, &fence, &boundary)
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
        KeyPredicate::prefix(wal_segment_prefix(&namespace_id)),
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
        super::prepare_segment(namespace_id.clone(), WriterEpoch(1), &head, &[]).expect("fence");
    let error = super::publish_segment(&store, &fence, &tip)
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
        .head(&wal_segment(&namespace_id, &WalNo(1)))
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
    let options = PublishTailOptions::default();
    writer_a
        .publish_batch(
            &store,
            [directory("before-sleep")],
            &context_a,
            &options,
            &Deadline::start(timer_a.clone()),
        )
        .await
        .results
        .remove(0)
        .expect("writer A observes its tip");
    let blocked = BlockingStore::new(
        store,
        KeyPredicate::exact(wal_segment(&namespace_id, &WalNo(3))),
        OperationClass::PutCreateIfAbsent,
    );
    blocked.block_next();
    let batch = Deadline::start(timer_a.clone());
    let (mut resumed, ()) = futures::join!(
        writer_a.publish_batch(
            &blocked,
            [directory("after-sleep")],
            &context_a,
            &options,
            &batch,
        ),
        async {
            blocked.wait_until_blocked().await;
            let mut writer_b =
                NamespaceCommitEngine::new(namespace_id.clone()).monotonic_timer(timer_b.clone());
            writer_b
                .publish_batch(
                    blocked.inner(),
                    [directory("takeover")],
                    &context_b,
                    &options,
                    &Deadline::start(timer_b.clone()),
                )
                .await
                .results
                .remove(0)
                .expect("writer B takes the epoch and commits");
            let fence_key = wal_segment(&namespace_id, &WalNo(3));
            let fence = decode_wal_segment_envelope_zstd(
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
            crate::checkpoint::flush_wal_with_deadline(
                blocked.inner(),
                &namespace_id,
                &Deadline::start(timer_b.clone()),
            )
            .await
            .expect("fold takeover and commit");
            let config = crate::gc::GcConfig {
                grace_window_ms: crate::limits::GC_MIN_GRACE_WINDOW_MS,
            };
            store_clock.advance_ms(config.grace_window_ms + 1);
            let report = crate::gc::gc_namespace(
                blocked.inner(),
                &namespace_id,
                &config,
                &MutationContext {
                    now_ms: store_clock.now_ms(),
                    ..context_b.clone()
                },
            )
            .await
            .expect("collect folded fence");
            assert_eq!(report.deleted.wal_segments, 4);
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
        .head(&wal_segment(&namespace_id, &WalNo(3)))
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
