//! Upload session cleanup in the pool's collection: the sessions it aborts
//! and deletes, the deadlines it reports, and how it reads session records.

use super::*;

async fn write_upload_session(store: &LocalFsStore, namespace_id: &NamespaceId) -> String {
    let session =
        crate::test_support::proxied_upload_session(namespace_id, 1_000 + UPLOAD_SESSION_LEASE_MS);
    let pool_id = crate::test_support::load_pool_id(store, namespace_id).await;
    crate::test_support::write_upload_records(store, &pool_id, &session, None).await
}

#[tokio::test]
async fn upload_gc_aborts_an_expired_session_then_reaps_it() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    let pool_id = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap")
        .pool_id;
    write_test_file(&store, &namespace_id, "/docs/one.txt", "gc-one", &setup).await;
    let (upload_id, content_ref) = stage_upload(&store, &namespace_id, &setup).await;
    let session_key = loonfs_objectstore::keys::upload_session(&pool_id, &upload_id);
    let content_key = loonfs_objectstore::keys::content_blob(&pool_id, &content_ref.content_id);

    // Inside the lease nothing happens, however old the object looks: the
    // session carries its own expiry, so no provider timestamp decides this.
    let inside = context(setup.now_ms + UPLOAD_SESSION_LEASE_MS - 1);
    let report = collect_pool(&store, &pool_id, &inside).await;
    assert_eq!(report.deleted.upload_sessions, 0);
    assert!(store.head(&content_key).await.expect("head").is_some());

    // Past the lease plus a grace the session is aborted and the object it
    // was writing is deleted — in that order.
    let expired = context(setup.now_ms + UPLOAD_SESSION_LEASE_MS + GRACE_MS + 1);
    let report = collect_pool(&store, &pool_id, &expired).await;
    assert_eq!(
        report.deleted.upload_sessions, 0,
        "the record outlives its abort"
    );
    let session = read_upload_session(&store, &namespace_id, &upload_id)
        .await
        .expect("aborted session retained");
    assert_matches!(session.outcome, Some(UploadOutcome::Aborted { .. }));
    assert!(
        store.head(&content_key).await.expect("head").is_none(),
        "aborting deletes the object the session owned"
    );

    // The aborted record is reaped a grace window after its own stamp.
    let reaped = context(expired.now_ms + GRACE_MS + 1);
    let report = collect_pool(&store, &pool_id, &reaped).await;
    assert_eq!(report.deleted.upload_sessions, 1);
    assert_eq!(
        report.deleted.content_objects, 0,
        "the abort half's unconditional cleanup is not a reclamation it can count"
    );
    assert!(store.head(&session_key).await.expect("head").is_none());

    let again = context(reaped.now_ms + GRACE_MS);
    let report = collect_pool(&store, &pool_id, &again).await;
    assert_eq!(report.deleted.upload_sessions, 0);
}

#[tokio::test]
async fn aborted_upload_cleanup_failure_keeps_the_session_for_retry() {
    let temp_dir = tempdir().expect("tempdir");
    let store = FailStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::content_blob(),
        OperationSelector::Delete,
        InjectedError::Transport("content delete timed out".to_owned()),
    );
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    let pool_id = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap")
        .pool_id;
    let (upload_id, _) = stage_upload(&store, &namespace_id, &setup).await;

    let expired = context(setup.now_ms + UPLOAD_SESSION_LEASE_MS + GRACE_MS + 1);
    collect_pool(&store, &pool_id, &expired).await;

    store.fail_next(1);
    let reaped = context(expired.now_ms + GRACE_MS + 1);
    let report = collect_pool(&store, &pool_id, &reaped).await;
    assert_eq!(report.deleted.upload_sessions, 0);
    assert_matches!(
        read_upload_session(&store, &namespace_id, &upload_id)
            .await
            .expect("aborted session retained")
            .outcome,
        Some(UploadOutcome::Aborted { .. })
    );

    let report = collect_pool(&store, &pool_id, &reaped).await;
    assert_eq!(report.deleted.upload_sessions, 1);
    assert!(read_upload_session(&store, &namespace_id, &upload_id)
        .await
        .is_none());
}

#[tokio::test]
async fn a_collection_reports_the_soonest_deadline_it_retained() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    let pool_id = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap")
        .pool_id;
    let (upload_id, ..) = stage_upload(&store, &namespace_id, &setup).await;
    let expires_at_ms = setup.now_ms + UPLOAD_SESSION_LEASE_MS;

    // One un-expired open session and nothing else: the lease plus the
    // collection's own grace window is the whole answer.
    let inside = context(setup.now_ms + 1);
    let report = collect_pool(&store, &pool_id, &inside).await;
    // The open session, and the content that the session staged.
    assert_eq!(report.retained.total(), 2);
    assert_eq!(
        report.next_reclamation_at_ms,
        Some(expires_at_ms + GRACE_MS),
        "an open session's reclamation waits for its lease and then the grace window"
    );

    // Completing it moves the deadline to the derived content grace, which
    // is the one the next collection is too early for.
    let completed_at = context(setup.now_ms + 2);
    complete_staged_upload(&store, &namespace_id, &upload_id, &completed_at).await;
    let report = collect_pool(&store, &pool_id, &completed_at).await;
    assert_eq!(
        report.next_reclamation_at_ms,
        Some(completed_at.now_ms + CONTENT_RECLAMATION_GRACE_WINDOW_MS),
        "a completed session's content is protected by the derived grace, not the configured one"
    );
}

#[tokio::test]
async fn an_aborted_session_is_reclaimed_from_the_deadline_the_collection_reported() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    let pool_id = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap")
        .pool_id;
    let (upload_id, ..) = stage_upload(&store, &namespace_id, &setup).await;
    let session_key = loonfs_objectstore::keys::upload_session(&pool_id, &upload_id);

    // The collection that aborts the session is the only thing that knows the
    // record now ages out a grace window from this instant.
    let expired = context(setup.now_ms + UPLOAD_SESSION_LEASE_MS + GRACE_MS + 1);
    let report = collect_pool(&store, &pool_id, &expired).await;
    assert_eq!(report.deleted.upload_sessions, 0);
    let reclaim_at_ms = report
        .next_reclamation_at_ms
        .expect("the abort this collection performed is a deadline it created");
    assert_eq!(reclaim_at_ms, expired.now_ms + GRACE_MS);

    // Nothing between the two collections says anything about this namespace:
    // the time the first collection reported is the whole trigger.
    let reclaiming = context(reclaim_at_ms + 1);
    let report = collect_pool(&store, &pool_id, &reclaiming).await;
    assert_eq!(report.deleted.upload_sessions, 1);
    assert!(store.head(&session_key).await.expect("head").is_none());
    assert_eq!(
        report.next_reclamation_at_ms, None,
        "a collection that reclaimed everything it found owes no later collection"
    );
}

#[tokio::test]
async fn upload_gc_reaps_a_session_that_never_staged_anything() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    let pool_id = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap")
        .pool_id;
    let session_key = write_upload_session(&store, &namespace_id).await;

    let expired = context(1_000 + UPLOAD_SESSION_LEASE_MS + GRACE_MS + 1);
    collect_pool(&store, &pool_id, &expired).await;
    let reaped = context(expired.now_ms + GRACE_MS + 1);
    let report = collect_pool(&store, &pool_id, &reaped).await;

    assert_eq!(report.deleted.upload_sessions, 1);
    assert!(store.head(&session_key).await.expect("head").is_none());
}

#[tokio::test]
async fn session_content_ids_are_charged_as_their_records_are_read() {
    let temp_dir = tempdir().expect("tempdir");
    let store = RecordingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::any(),
    );
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    let pool_id = crate::test_support::NamespaceFixture::new(store.inner(), &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap")
        .pool_id;
    for _ in 0..=STORE_READS_IN_FLIGHT {
        complete_upload_for_gc(store.inner(), &namespace_id, b"unpublished\n", &setup).await;
    }

    let memory = Arc::new(crate::cache::ReadWorkingMemory::new(1, None));
    let cache = Arc::new(root_scan_cache(Arc::clone(&memory)));
    store.reset();
    let error = crate::PoolStore {
        segment_cache: Arc::clone(&cache),
        ..crate::test_support::pool_store(&store, &pool_id)
    }
    .gc(None, &options(), &setup.clock())
    .await
    .expect_err("the first session's content id does not fit the budget");

    assert_matches!(error, CoreError::LiveSetExceedsReadMemory { limit: 1, .. });
    let prefix = loonfs_objectstore::keys::upload_session_prefix(&pool_id);
    let session_reads = store
        .snapshot()
        .into_iter()
        .filter(|operation| {
            operation.key().starts_with(&prefix)
                && !matches!(
                    operation,
                    loonfs_test_support::stores::RecordedOperation::List { .. }
                )
        })
        .count();
    assert!(
        session_reads <= STORE_READS_IN_FLIGHT,
        "the collection stopped within one window of reads, after {session_reads}"
    );
    assert_eq!(memory.in_use(), 0);
}

/// Checks that one collection reads each completed upload session record once,
/// with more than one read and at most `STORE_READS_IN_FLIGHT` reads in
/// flight.
#[tokio::test]
async fn a_collection_reads_each_completed_session_record_once() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let store = RecordingStore::new(
        ConcurrencyWatchStore::new(
            LocalFsStore::new(temp_dir.path()).expect("store"),
            KeyPredicate::upload_records(),
        ),
        KeyPredicate::upload_records(),
    );
    let setup = store.inner().inner();
    let early = context(1_000);
    let late = context(early.now_ms + GRACE_MS);
    let pool_id = crate::test_support::NamespaceFixture::new(setup, &namespace_id, &early)
        .create()
        .await
        .expect("bootstrap")
        .pool_id;
    for completed in [&early, &late] {
        for _ in 0..20 {
            complete_upload_for_gc(setup, &namespace_id, b"unpublished\n", completed).await;
        }
    }

    let report = collect_pool(
        &store,
        &pool_id,
        &context(early.now_ms + CONTENT_RECLAMATION_GRACE_WINDOW_MS),
    )
    .await;

    assert_eq!(report.deleted.upload_sessions, 20);
    assert_eq!(report.retained.upload_session_window, 20);
    let reads = store.inner().reads();
    assert_eq!(reads.total, 40, "one read for each of the 40 records");
    assert!(reads.peak_in_flight > 1);
    assert!(reads.peak_in_flight <= STORE_READS_IN_FLIGHT);
    let mut reads_by_key = std::collections::BTreeMap::<String, usize>::new();
    for key in store.take_get_keys() {
        *reads_by_key.entry(key).or_default() += 1;
    }
    assert_eq!(reads_by_key.len(), 40);
    assert!(reads_by_key.values().all(|&reads| reads == 1));
}
