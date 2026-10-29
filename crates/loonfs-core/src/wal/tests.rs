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
use loonfs_objectstore::keys::{metadata_manifest_object, wal_object, wal_prefix};
use loonfs_objectstore::{local_fs_store::LocalFsStore, ObjectStore};
use loonfs_test_support::clock::ManualClock;
use loonfs_test_support::stores::{
    replace_object, BlockingStore, ConcurrencyWatchStore, FailStore, InjectedError, KeyPredicate,
    MetadataMapStore, OperationSelector, RecordingStore,
};
use loonfs_types::format::wal::{decode_wal_object_envelope_zstd, encode_wal_object_envelope_zstd};
use loonfs_types::{
    AbsolutePath, AttributeInclusion, ChangeSeq, CommitId, ErrorCode, InodeId, ManifestNo,
    NamespaceId, WalNo, WriterEpoch, WriterId,
};
use std::assert_matches;
use std::sync::Arc;
use tempfile::tempdir;

fn context(now_ms: u64) -> crate::MutationContext {
    crate::test_support::mutation_context("writer", now_ms)
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
        replace_object(&store, &key, bytes.into())
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

/// Rewrites the WAL object at `wal_no` so that it records `basis_floor_seq`.
async fn rewrite_basis_floor(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
    wal_no: WalNo,
    basis_floor_seq: ChangeSeq,
) {
    let key = wal_object(namespace_id, &wal_no);
    let bytes = store
        .get(&key, None)
        .await
        .expect("get")
        .expect("WAL object");
    let mut payload = decode_wal_object_envelope_zstd(&bytes)
        .expect("decode")
        .into_payload();
    payload.basis_floor_seq = basis_floor_seq;
    let bytes = encode_wal_object_envelope_zstd(payload)
        .expect("encode")
        .into_bytes();
    replace_object(store, &key, bytes.into())
        .await
        .expect("rewrite the WAL object");
}

#[tokio::test]
async fn a_wal_object_that_lowers_the_basis_floor_is_corrupt() {
    let directory = tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let setup = context(1_000);

    // Two fences follow the manifest, and the first one is rewritten to
    // record a higher floor than the second.
    let fences = NamespaceId::parse("fences").expect("namespace");
    create(&store, &fences, &setup).await.expect("create");
    for _ in 0..2 {
        acquire_writer_epoch(&store, &fences, &setup)
            .await
            .expect("fence");
    }
    rewrite_basis_floor(&store, &fences, WalNo(1), ChangeSeq(1)).await;

    // A fold acknowledges floor 1 through WAL object 3, and the fence after
    // it is rewritten to record floor 0.
    let folded = NamespaceId::parse("folded").expect("namespace");
    create(&store, &folded, &setup).await.expect("create");
    let mut engine = NamespaceCommitEngine::new(
        folded.clone(),
        std::sync::Arc::new(crate::test_support::content_writes()),
    );
    publish(&mut engine, &store, "first").await.expect("commit");
    crate::manifest::fold_wal(&store, &folded)
        .await
        .expect("fold");
    crate::manifest::advance_retention_floor(
        &store,
        None,
        &folded,
        crate::manifest::RetentionTarget::Head,
        crate::test_support::grace_elapsed_after(1_000),
        crate::test_support::std_timer(),
    )
    .await
    .expect("advance");
    acquire_writer_epoch(&store, &folded, &setup)
        .await
        .expect("fence");
    crate::manifest::fold_wal(&store, &folded)
        .await
        .expect("fold");
    acquire_writer_epoch(&store, &folded, &setup)
        .await
        .expect("fence");
    rewrite_basis_floor(&store, &folded, WalNo(4), ChangeSeq(0)).await;

    for (namespace_id, expected) in [
        (
            &fences,
            "WAL object `2` lowers the basis floor to `0` from `1` at WAL number `1`",
        ),
        (
            &folded,
            "WAL object `4` lowers the basis floor to `0` from `1` at WAL number `3`",
        ),
    ] {
        let error = load_current_metadata_view(&store, namespace_id)
            .await
            .err()
            .expect("corruption");
        assert_eq!(error.code(), ErrorCode::NamespaceCorrupt, "{error}");
        assert!(error.to_string().contains(expected), "{error}");
    }
}

/// Checks that a fence records the floor of the manifest that the
/// acquisition discovered, which is a retention advance published after the
/// claim, not the claim itself.
#[tokio::test]
async fn a_fence_records_the_floor_of_an_advance_published_after_the_claim() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("advanced-claim").expect("namespace");
    // The claim's manifest folds WAL object 2, so the acquisition's first
    // read after the claim is WAL object 3.
    let store = BlockingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::exact(wal_object(&namespace_id, &WalNo(3))),
        OperationSelector::Read,
    );
    let setup = context(1_000);
    create(&store, &namespace_id, &setup).await.expect("create");
    let mut engine = NamespaceCommitEngine::new(
        namespace_id.clone(),
        std::sync::Arc::new(crate::test_support::content_writes()),
    );
    publish(&mut engine, &store, "first").await.expect("commit");
    crate::manifest::fold_wal(&store, &namespace_id)
        .await
        .expect("fold");
    store.block_next();
    let (acquired, advanced) = futures::join!(
        acquire_writer(
            &store,
            &namespace_id,
            &setup,
            crate::test_support::std_timer()
        ),
        async {
            store.wait_until_blocked().await;
            let advanced = crate::manifest::advance_retention_floor(
                store.inner(),
                None,
                &namespace_id,
                crate::manifest::RetentionTarget::Head,
                crate::test_support::grace_elapsed_after(1_000),
                crate::test_support::std_timer(),
            )
            .await
            .expect("advance");
            store.release();
            advanced.response.retention_floor_seq
        }
    );
    assert_eq!(advanced, ChangeSeq(1));
    let acquisition = acquired.expect("acquire");
    let claim = crate::namespace::control::load_manifest_by_no(
        store.inner(),
        &namespace_id,
        acquisition.claim.manifest.manifest_no,
    )
    .await
    .expect("load the claim")
    .expect("the claim exists");
    assert_eq!(claim.state.retention_floor_seq(), ChangeSeq(0));
    assert_eq!(acquisition.anchor.retention_floor_seq(), ChangeSeq(1));
    let fence = acquisition
        .anchor
        .tail
        .objects()
        .last()
        .expect("the fence")
        .envelope()
        .payload();
    assert!(fence.records.is_empty());
    assert_eq!(fence.basis_floor_seq, ChangeSeq(1));
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
    let mut engine = NamespaceCommitEngine::new(
        namespace_id.clone(),
        std::sync::Arc::new(crate::test_support::content_writes()),
    );
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

    let anchor = crate::namespace::read_anchor::load_read_anchor(
        &store,
        &namespace_id,
        crate::test_support::observed_now(),
    )
    .await
    .expect("cold discovery");
    let tail = crate::namespace::read_anchor::project_anchor_tail(&store, None, &anchor)
        .expect("replay discovery");
    assert_eq!(anchor.read_state.seq, ChangeSeq(9));
    assert_eq!(anchor.read_state.wal_no, WalNo(12));
    assert!(tail
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
    let error = crate::namespace::read_anchor::load_read_anchor(
        &store,
        &namespace_id,
        crate::test_support::observed_now(),
    )
    .await
    .expect_err("gap in discovery window");
    assert_matches!(
        error,
        crate::control_object::ControlObjectLoadError::Codec { object_key, .. }
            if object_key == missing
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
        ..Default::default()
    };
    let aged = context(options.grace_window_ms + 1);
    let report = crate::gc::gc_namespace(
        &store,
        None,
        None,
        &namespace_id,
        &options,
        &aged,
        crate::test_support::std_timer(),
    )
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
    use loonfs_test_support::stores::{BlockingStore, OperationSelector};
    let directory = tempfile::tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("fold-race").expect("namespace");
    let store = BlockingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::exact(loonfs_objectstore::keys::metadata_manifest_object(
            &namespace_id,
            &loonfs_types::ManifestNo(3),
        )),
        OperationSelector::PutCreateIfAbsent,
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

/// Checks that an acquisition whose anchor load is held past the revalidation
/// bound, while a fold, a compactor claim, and a collection run, stops when
/// its publication budget runs out and writes no fence over the collected
/// objects.
#[tokio::test]
async fn an_acquisition_held_past_the_revalidation_bound_writes_no_fence() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("late-claim").expect("namespace");
    // The claim's manifest doesn't fold the first fence, so the acquisition's
    // first read after the claim is the first fence.
    let store = BlockingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::exact(wal_object(&namespace_id, &WalNo(1))),
        OperationSelector::Read,
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
            // The claim is written and its first WAL read is held. Meanwhile
            // a fold folds the first fence, a compactor claim supersedes the
            // fold, the runtime that published them raises the hint, and
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
            let compactor_claim = crate::manifest::claim_compactor(
                store.inner(),
                &namespace_id,
                crate::test_support::std_timer(),
            )
            .await
            .expect("compactor claim")
            .published
            .expect("a published claim");
            crate::namespace::control::raise_namespace_hint(
                store.inner(),
                &namespace_id,
                compactor_claim.manifest.manifest_no,
                None,
                crate::test_support::std_timer(),
            )
            .await
            .expect("raise the hint");
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
    // The revalidation bound is longer than the publication budget, so the
    // acquisition reloads its anchor and then runs out of budget at the fence.
    let error = acquired.expect_err("the held acquisition runs out of budget");
    assert_matches!(
        error,
        crate::error::CoreError::MetadataPublicationBudgetExceeded { .. }
    );
    for number in [1, 2] {
        assert!(store
            .head(&wal_object(&namespace_id, &WalNo(number)))
            .await
            .expect("head")
            .is_none());
    }
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
        OperationSelector::PutCreateIfAbsent,
        InjectedError::PreconditionFailed,
    );
    let setup = context(1_000);
    create(&store, &namespace_id, &setup).await.expect("create");
    store.fail_next(1);
    let error = acquire_writer(&store, &namespace_id, &setup, timer)
        .await
        .expect_err("the acquisition runs out of budget");
    assert_matches!(
        error,
        crate::error::CoreError::MetadataPublicationBudgetExceeded { .. }
    );
    assert_eq!(store.attempts(), 1);
    assert!(store
        .list_prefix(&wal_prefix(&namespace_id))
        .await
        .expect("WAL")
        .is_empty());
}

fn directory(name: &str) -> CommitCandidate {
    CommitCandidate::new(crate::path::write::CommitIntent::single(
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
) -> crate::error::Result<loonfs_types::Commit> {
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
    let session = crate::commit_engine::SharedWriterSessionStatus::default();
    let mut first = NamespaceCommitEngine::new(
        namespace_id.clone(),
        std::sync::Arc::new(crate::test_support::content_writes()),
    )
    .writer_session(Arc::clone(&session));
    publish(&mut first, &store, "seed").await.expect("seed");
    let mut second = NamespaceCommitEngine::new(
        namespace_id.clone(),
        std::sync::Arc::new(crate::test_support::content_writes()),
    )
    .writer_session(session);
    store.reset();
    let blocked = BlockingStore::new(
        store.clone(),
        KeyPredicate::exact(wal_object(&namespace_id, &WalNo(3))),
        OperationSelector::PutCreateIfAbsent,
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
    let mut stale = NamespaceCommitEngine::new(
        namespace_id.clone(),
        std::sync::Arc::new(crate::test_support::content_writes()),
    );
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
        crate::commit_engine::WriterSessionStatus::Acquired(acquired),
    ));
    let mut active = NamespaceCommitEngine::new(
        namespace_id.clone(),
        std::sync::Arc::new(crate::test_support::content_writes()),
    )
    .writer_session(session);
    assert_eq!(
        publish(&mut active, &store, "new")
            .await
            .expect("new writer")
            .committed_seq,
        ChangeSeq(2)
    );
}

#[tokio::test]
async fn a_number_published_during_a_window_is_read_again_not_reported_missing() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("window-race").expect("namespace");
    let store = BlockingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::prefix(wal_object(&namespace_id, &WalNo(6))),
        OperationSelector::Get,
    );
    create(&store, &namespace_id, &context(1_000))
        .await
        .expect("create");
    let mut engine = NamespaceCommitEngine::new(
        namespace_id.clone(),
        std::sync::Arc::new(crate::test_support::content_writes()),
    );
    for name in ["one", "two", "three"] {
        publish(&mut engine, &store, name).await.expect(name);
    }
    // WAL 1 through 4 exist. The window 4..7 reads 5 as absent, then 5 and 6
    // are published while the read of 6 is parked, so 6 comes back present.
    store.block_next();
    let (anchor, ()) = futures::join!(
        crate::namespace::read_anchor::load_read_anchor(
            &store,
            &namespace_id,
            crate::test_support::observed_now()
        ),
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
    use crate::in_flight::STORE_READS_IN_FLIGHT;

    let directory = tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let namespace_id = NamespaceId::parse("bounded-replay").expect("namespace");
    create(&store, &namespace_id, &context(1_000))
        .await
        .expect("create");
    let mut engine = NamespaceCommitEngine::new(
        namespace_id.clone(),
        std::sync::Arc::new(crate::test_support::content_writes()),
    );
    for number in 1..20 {
        publish(&mut engine, &store, &format!("directory-{number}"))
            .await
            .expect("publish");
    }
    let mut walk = WalWalk::after(
        &namespace_id,
        WalNo(0),
        ChangeSeq(0),
        WriterEpoch(1),
        ChangeSeq(0),
    );
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
            base_basis_floor_seq: ChangeSeq(0),
            tip_wal_no: WalNo(20),
            writer_epoch: WriterEpoch(1),
        },
    )
    .await
    .expect("bounded load");

    assert_eq!(watched.reads().total, 20);
    assert!(watched.reads().peak_in_flight > 1);
    assert!(watched.reads().peak_in_flight <= STORE_READS_IN_FLIGHT);
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
    let mut engine = NamespaceCommitEngine::new(
        namespace_id.clone(),
        std::sync::Arc::new(crate::test_support::content_writes()),
    );
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
            base_basis_floor_seq: ChangeSeq(0),
            tip_wal_no: WalNo(4),
            writer_epoch: WriterEpoch(1),
        },
    )
    .await
    .expect_err("a gap below the tip is corruption");
    assert_matches!(
        error,
        super::WalTailLoadError::MissingWalObject { object_key } if object_key == lowest_missing
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
        ..Default::default()
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
            metadata.last_modified_at_ms = Some(grace + 1);
            metadata
        },
    );
    let store = BlockingStore::new(
        store,
        KeyPredicate::exact(wal_object(&namespace_id, &WalNo(1))),
        OperationSelector::Read,
    );
    create(&store, &namespace_id, &context(1_000))
        .await
        .expect("create");
    let mut engine = NamespaceCommitEngine::new(
        namespace_id.clone(),
        std::sync::Arc::new(crate::test_support::content_writes()),
    );
    publish(&mut engine, &store, "seed").await.expect("seed");
    engine.invalidate_projection();
    store.block_next();
    let (published, ()) = futures::join!(publish(&mut engine, &store, "after-gc"), async {
        store.wait_until_blocked().await;
        crate::manifest::fold_wal(store.inner(), &namespace_id)
            .await
            .expect("fold old WAL");
        crate::manifest::advance_retention_floor(
            store.inner(),
            None,
            &namespace_id,
            crate::manifest::RetentionTarget::Head,
            crate::test_support::grace_elapsed_after(1_000),
            crate::test_support::std_timer(),
        )
        .await
        .expect("advance floor");
        let aged = MutationContext {
            now_ms: grace + 1,
            ..context(1_000)
        };
        crate::gc::gc_namespace(
            store.inner(),
            None,
            None,
            &namespace_id,
            &options,
            &aged,
            crate::test_support::std_timer(),
        )
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
    let loaded = crate::namespace::read_anchor::load_read_anchor(
        &store,
        &namespace_id,
        crate::test_support::observed_now(),
    )
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
        head_and_wal_tail_cache: std::sync::Arc::new(crate::cache::HeadAndWalTailCache::unshared(
            usize::MAX,
        )),
        object_cache: std::sync::Arc::new(crate::cache::ObjectCache::unshared(0, 0)),
        tail: None,
    };
    let error = super::probe_namespace_wal(&store, &mut warm)
        .await
        .expect_err("a same-epoch chain break is corruption, not a stale head");
    assert_matches!(
        error,
        crate::control_object::ControlObjectLoadError::Codec { .. }
    );
}

/// Returns a warm read context that's `behind` WAL objects behind the tip,
/// with the store's log cleared.
async fn warm_context_behind(
    store: &RecordingStore<LocalFsStore>,
    namespace_id: &NamespaceId,
    behind: u64,
) -> crate::RuntimeReadContext {
    create(store, namespace_id, &context(1_000))
        .await
        .expect("create");
    let mut engine = NamespaceCommitEngine::new(
        namespace_id.clone(),
        Arc::new(crate::test_support::content_writes()),
    );
    publish(&mut engine, store, "first").await.expect("first");
    let loaded = crate::namespace::read_anchor::load_read_anchor(
        store,
        namespace_id,
        crate::test_support::observed_now(),
    )
    .await
    .expect("warm head");
    for number in 0..behind {
        publish(&mut engine, store, &format!("commit-{number}"))
            .await
            .expect("commit");
    }
    store.reset();
    crate::RuntimeReadContext {
        basis: loaded.basis(),
        head: loaded.read_state,
        segment_cache: Arc::new(crate::cache::MetadataSegmentCache::unshared(usize::MAX)),
        head_and_wal_tail_cache: Arc::new(crate::cache::HeadAndWalTailCache::unshared(usize::MAX)),
        object_cache: Arc::new(crate::cache::ObjectCache::unshared(0, 0)),
        tail: None,
    }
}

/// Checks that a warm probe reads the WAL in discovery's widening windows, and
/// that a probe at the tip sends one GET.
#[tokio::test]
async fn a_warm_probe_reads_in_widening_windows_and_sends_one_get_at_the_tip() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("warm-windows").expect("namespace");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::prefix(wal_prefix(&namespace_id)),
    );
    let mut warm = warm_context_behind(&store, &namespace_id, 40).await;
    let cached = warm.head.wal_no.0;
    assert!(super::probe_namespace_wal(&store, &mut warm)
        .await
        .expect("probe 40 objects behind"));
    assert_eq!(warm.head.wal_no, WalNo(cached + 40));
    // A window starts only after the one before it ends, so each window's
    // reads are the next ones in the log, in any order.
    let mut gets = store.take_get_keys().into_iter();
    let mut first = cached + 1;
    for width in [1, 2, 4, 8, 16, 16] {
        let mut window = gets
            .by_ref()
            .take(loonfs_types::usize_from_u64(width))
            .collect::<Vec<_>>();
        window.sort();
        let mut expected = (first..first + width)
            .map(|number| wal_object(&namespace_id, &WalNo(number)))
            .collect::<Vec<_>>();
        expected.sort();
        assert_eq!(window, expected, "the window of {width} from WAL {first}");
        first += width;
    }
    assert_eq!(gets.next(), None);

    assert!(super::probe_namespace_wal(&store, &mut warm)
        .await
        .expect("probe at the tip"));
    assert_eq!(
        store.take_get_keys(),
        [wal_object(&namespace_id, &WalNo(cached + 41))]
    );
}

/// Checks that a warm probe returns false for a missing WAL number below a
/// present one, and that discovery from the unchanged manifest reports the
/// same gap as corruption.
#[tokio::test]
async fn a_warm_probe_leaves_a_gap_in_the_wal_to_discovery_which_reports_corruption() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("warm-gap").expect("namespace");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::prefix(wal_prefix(&namespace_id)),
    );
    let mut warm = warm_context_behind(&store, &namespace_id, 8).await;
    assert_eq!(warm.head.wal_no, WalNo(2));
    let cached = warm.head.clone();
    // The probe reads WAL 6 to 9 in one window, and discovery reads WAL 4 to 7,
    // so both find WAL 6 missing below a present number.
    let missing = wal_object(&namespace_id, &WalNo(6));
    store
        .delete(&missing)
        .await
        .expect("remove a WAL object below the tip");
    assert!(!super::probe_namespace_wal(&store, &mut warm)
        .await
        .expect("a gap sends the read to discovery"));
    assert_eq!(warm.head, cached);
    let error = crate::namespace::read_anchor::load_read_anchor(
        &store,
        &namespace_id,
        crate::test_support::observed_now(),
    )
    .await
    .expect_err("a gap above the folded boundary is corruption");
    assert_matches!(
        error,
        crate::control_object::ControlObjectLoadError::Codec { object_key, .. }
            if object_key == missing
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
    let fence = super::prepare_wal_object(namespace_id, WriterEpoch(1), &head, ChangeSeq(0), &[])
        .expect("fence");
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
    let fence = super::prepare_wal_object(
        namespace_id.clone(),
        WriterEpoch(1),
        &head,
        ChangeSeq(0),
        &[],
    )
    .expect("fence");
    let error = super::publish_wal_object(&store, &fence, &tip)
        .await
        .expect_err("late put must not acknowledge publication");
    assert_matches!(
        error,
        crate::error::CoreError::WalPublish(crate::wal::WalPublishError::OutcomeUnknown(_))
    );
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
    let mut writer_a = NamespaceCommitEngine::new(
        namespace_id.clone(),
        std::sync::Arc::new(crate::test_support::content_writes()),
    )
    .monotonic_timer(timer_a.clone());
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
        OperationSelector::PutCreateIfAbsent,
    );
    blocked.block_next();
    let batch = Deadline::start(timer_a.clone());
    let (mut resumed, ()) = futures::join!(
        writer_a.publish_batch(&blocked, [directory("after-sleep")], &context_a, &batch,),
        async {
            blocked.wait_until_blocked().await;
            let mut writer_b = NamespaceCommitEngine::new(
                namespace_id.clone(),
                std::sync::Arc::new(crate::test_support::content_writes()),
            )
            .monotonic_timer(timer_b.clone());
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
            crate::manifest::fold_wal_tail(
                blocked.inner(),
                None,
                &namespace_id,
                None,
                &Deadline::start(timer_b.clone()),
                crate::manifest::MetadataRunPolicy::default(),
                &crate::test_support::content_writes(),
            )
            .await
            .expect("fold takeover and commit");
            let options = crate::gc::GcOptions {
                grace_window_ms: crate::limits::GC_MIN_GRACE_WINDOW_MS,
                ..Default::default()
            };
            store_clock.advance_ms(options.grace_window_ms + 1);
            let report = crate::gc::gc_namespace(
                blocked.inner(),
                None,
                None,
                &namespace_id,
                &options,
                &MutationContext {
                    now_ms: store_clock.now_ms(),
                    ..context_b.clone()
                },
                crate::test_support::std_timer(),
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
