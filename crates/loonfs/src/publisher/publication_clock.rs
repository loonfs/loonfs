//! Runtime wall timestamps stay paired with the retry clock's origin.

use super::*;

async fn publish_after_retry_delay(remaining_ms: u64, apply_then_fail: bool) -> CommitResult {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("retry-clock").expect("namespace");
    let clock = Arc::new(ManualMonotonicTimer::default());
    let delayed = Arc::clone(&clock);
    let retry_time = Arc::new(AtomicU64::new(0));
    let delay_until = Arc::clone(&retry_time);
    let wal_prefix = wal_segment_prefix(&namespace_id);
    let store = FailStore::matching(
        LocalFsStore::new(directory.path()).expect("store"),
        move |operation| {
            if operation.key().starts_with(&wal_prefix)
                && matches!(operation.kind(), OperationKind::Put { bytes, mode: PutMode::CreateIfAbsent } if is_publication(bytes))
            {
                // Both wall and monotonic time advance together. Only the first
                // matching put fails; the retry itself consumes no further time.
                delayed.0.fetch_max(
                    delay_until.load(AtomicOrdering::SeqCst),
                    AtomicOrdering::SeqCst,
                );
                true
            } else {
                false
            }
        },
        InjectedError::Transport("publication response unavailable".to_owned()),
    );
    let store = Arc::new(if apply_then_fail {
        store.apply_then_fail()
    } else {
        store
    });
    let mut runtime = test_runtime(store.clone());
    create_namespace(&runtime, &namespace_id).await;
    let catalog = loonfs_core::control::load_namespace_catalog_entry(&store, &namespace_id)
        .await
        .expect("catalog");
    let engine = runtime
        .core
        .writer_engine(&runtime.bits.identity, &namespace_id);
    let upload = engine.begin_upload(None).await.expect("begin upload");
    engine
        .upload_content(&upload.upload_id, None, b"content")
        .await
        .expect("upload bytes");
    let completed = engine
        .complete_upload(
            &catalog,
            &upload.upload_id,
            None,
            loonfs_core::ResolvedUploadCompletion::KnownContent,
        )
        .await
        .expect("complete upload");
    let issued = loonfs_core::time::current_time_ms().expect("wall time");
    let token = loonfs_core::content::mint_content_token(
        "secret",
        completed.receipt.as_ref().expect("receipt"),
        issued,
    )
    .expect("token");
    let expires = issued + loonfs_core::limits::CONTENT_RECEIPT_TTL_MS;
    let started = expires - remaining_ms;
    let prepared = loonfs_core::content::verify_content_token("secret", &catalog, &token, started)
        .expect("proof valid at admission");
    clock.set(started);
    retry_time.store(started + 10, AtomicOrdering::SeqCst);
    Arc::get_mut(&mut runtime.bits)
        .expect("unshared writer bits")
        .identity
        .test_wall_clock = Some(clock.clone());
    let mut publisher = standalone_publisher(&namespace_id, &runtime);
    publisher.timer = clock;
    publisher.min_publish_interval = Duration::ZERO;
    let candidate = CommitCandidate::prepared(
        CommitRequest::single(
            CommitId::parse("put-content").expect("commit id"),
            loonfs_test_support::test_actor(),
            None,
            FilesystemOperation::PutFile {
                path: AbsolutePath::parse("/file").expect("path"),
                content_ref: Some(prepared.content_ref().clone()),
                inline_content: None,
                behavior: DestinationBehavior::NoReplace,
                expected_inode_id: None,
                expected_revision_no: None,
            },
        ),
        vec![prepared],
    );
    store.fail_next(1);
    publisher.submit(candidate).await
}

#[tokio::test]
async fn runtime_retry_does_not_expire_a_still_valid_content_proof() {
    publish_after_retry_delay(15, false)
        .await
        .expect("ten milliseconds of retry time must not consume fifteen milliseconds of validity");
}

#[tokio::test]
async fn runtime_retry_preserves_uncertainty_when_the_content_proof_expires() {
    let error = publish_after_retry_delay(5, false)
        .await
        .expect_err("an expired proof cannot resolve the earlier publication outcome");
    assert_eq!(error.code(), ErrorCode::CommitOutcomeUnknown);
}

#[tokio::test]
async fn runtime_retry_replays_a_landed_commit_after_its_content_proof_expires() {
    publish_after_retry_delay(5, true)
        .await
        .expect("expiry after the put starts must not undo the landed commit");
}
