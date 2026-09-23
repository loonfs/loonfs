//! Numbered WAL verification and fence reclamation contracts.

// This module is the physical WAL boundary.
#![allow(clippy::disallowed_methods)]

use crate::commit_engine::{CommitCandidate, NamespaceCommitEngine};
use crate::context::MutationContext;
use crate::namespace::{
    bootstrap::bootstrap_namespace, control::load_current_manifest,
    writer_epoch::acquire_writer_epoch,
};
use crate::path::read::load_current_metadata_view;
use crate::protocol::PublishTailOptions;
use loonfs_api::wire::wal::{
    decode_wal_segment_envelope_zstd, encode_wal_segment_envelope_zstd, WalCommitPayload,
};
use loonfs_api::{
    AbsolutePath, AttributeInclusion, ChangeSeq, CommitId, ErrorCode, InodeId, ManifestNo,
    NamespaceId, WalNo, WriterEpoch, WriterId,
};
use loonfs_objectstore::keys::{hint, wal_segment, wal_segment_prefix};
use loonfs_objectstore::{local_fs_store::LocalFsStore, ObjectStore};
use loonfs_test_support::stores::{
    BlockingStore, KeyPredicate, MetadataMapStore, OperationClass, RecordingStore,
};
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
    bootstrap_namespace(
        &store,
        &namespace_id,
        &context(1_000),
        &loonfs_test_support::test_actor(),
        &loonfs_api::NamespaceAccess::Unrestricted {},
        false,
    )
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

    let mut data_payload = original.payload().clone();
    data_payload.head_seq = ChangeSeq(1);
    data_payload.head_commit_id = CommitId::parse("wrong-data-head").expect("commit");
    data_payload.records = vec![WalCommitPayload {
        seq: ChangeSeq(1),
        commit_id: CommitId::parse("data-record").expect("commit"),
        committed_by: loonfs_test_support::test_actor(),
        semantic_commit_fingerprint: serde_json::from_str(r#""v1:sha256:test""#)
            .expect("fingerprint"),
        committed_at_ms: 1_000,
        message: None,
        deltas: Vec::new(),
        inline_content: Vec::new(),
    }];
    let data = encode_wal_segment_envelope_zstd(data_payload).expect("data segment");
    assert_eq!(
        super::replay::validate_wal_segment_for_replay(
            &namespace_id,
            ChangeSeq(0),
            data.envelope(),
        ),
        Err(super::WalSegmentError::SegmentSummaryMismatch)
    );

    let mut fence_payload = original.payload().clone();
    fence_payload.head_commit_id = CommitId::parse("wrong-fence-head").expect("commit");
    let fence = encode_wal_segment_envelope_zstd(fence_payload)
        .expect("fence segment")
        .into_envelope();
    let current = load_current_manifest(&store, &namespace_id)
        .await
        .expect("manifest");
    let base_head = crate::namespace::state::NamespaceReadState::from(current.envelope.payload());
    let tail =
        super::ValidatedWalTail::new(vec![super::ValidatedWalSegment::new(key.clone(), fence)]);
    assert_eq!(
        super::replay::project_validated_wal_tail(
            &base_head,
            &super::ProjectedWalTail::default(),
            Some(WriterEpoch(2)),
            &tail,
        ),
        Err(super::WalSegmentError::SegmentSummaryMismatch)
    );

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
async fn hinted_fences_carry_the_head_commit_without_reading_earlier_wal() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("hinted-fences").expect("namespace");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::prefix(wal_segment_prefix(&namespace_id)),
    );
    bootstrap_namespace(
        &store,
        &namespace_id,
        &context(1_000),
        &loonfs_test_support::test_actor(),
        &loonfs_api::NamespaceAccess::Unrestricted {},
        false,
    )
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
    assert_eq!(head.head_commit_id, committed.commit_id);
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
    bootstrap_namespace(
        &store,
        &namespace_id,
        &setup,
        &loonfs_test_support::test_actor(),
        &loonfs_api::NamespaceAccess::Unrestricted {},
        false,
    )
    .await
    .expect("create");
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
    assert_eq!(current.envelope.payload().head_seq, ChangeSeq(0));
    assert_eq!(current.envelope.payload().last_folded_wal_no, WalNo(1));
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
    bootstrap_namespace(
        &store,
        &namespace_id,
        &setup,
        &loonfs_test_support::test_actor(),
        &loonfs_api::NamespaceAccess::Unrestricted {},
        false,
    )
    .await
    .expect("create");
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
    assert_eq!(current.envelope.payload().head_seq, ChangeSeq(0));
    assert_eq!(current.envelope.payload().last_folded_wal_no, WalNo(2));
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
        )
        .await
        .results
        .remove(0)
}

#[tokio::test]
async fn a_number_collision_replans_and_commits_the_next_number_without_a_swap() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("race").expect("namespace");
    let store = std::sync::Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    bootstrap_namespace(
        &store,
        &namespace_id,
        &context(1_000),
        &loonfs_test_support::test_actor(),
        &loonfs_api::NamespaceAccess::Unrestricted {},
        false,
    )
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
    assert_eq!(loser.expect("replanned").committed_seq, ChangeSeq(3));
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
    bootstrap_namespace(
        &store,
        &namespace_id,
        &context(1_000),
        &loonfs_test_support::test_actor(),
        &loonfs_api::NamespaceAccess::Unrestricted {},
        false,
    )
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
    assert_eq!(
        publish(&mut stale, &store, "stale")
            .await
            .expect_err("fenced")
            .code(),
        ErrorCode::WriterFenced
    );
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
    bootstrap_namespace(
        &store,
        &namespace_id,
        &context(1_000),
        &loonfs_test_support::test_actor(),
        &loonfs_api::NamespaceAccess::Unrestricted {},
        false,
    )
    .await
    .expect("create");
    let mut engine = NamespaceCommitEngine::new(namespace_id.clone());
    publish(&mut engine, &store, "one").await.expect("one");
    publish(&mut engine, &store, "two").await.expect("two");
    let hint_bytes = loonfs_api::wire::control::encode_control_state(
        loonfs_api::wire::control::ControlObjectKind::Hint,
        &loonfs_api::wire::control::HintState {
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
    bootstrap_namespace(
        &store,
        &namespace_id,
        &context(1_000),
        &loonfs_test_support::test_actor(),
        &loonfs_api::NamespaceAccess::Unrestricted {},
        false,
    )
    .await
    .expect("create");
    let mut engine = NamespaceCommitEngine::new(namespace_id.clone());
    for name in ["one", "two", "three"] {
        publish(&mut engine, &store, name).await.expect(name);
    }
    let hint_bytes = loonfs_api::wire::control::encode_control_state(
        loonfs_api::wire::control::ControlObjectKind::Hint,
        &loonfs_api::wire::control::HintState {
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
    bootstrap_namespace(
        &store,
        &namespace_id,
        &context(1_000),
        &loonfs_test_support::test_actor(),
        &loonfs_api::NamespaceAccess::Unrestricted {},
        false,
    )
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
