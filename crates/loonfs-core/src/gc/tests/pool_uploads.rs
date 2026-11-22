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

    // Past the lease plus a grace period, the session is aborted and the object
    // it was writing is deleted, in that order.
    let expired = context(setup.now_ms + UPLOAD_SESSION_LEASE_MS + GC_GRACE_WINDOW_MS + 1);
    let report = collect_pool(&store, &pool_id, &expired).await;
    assert_eq!(
        report.deleted.upload_sessions, 0,
        "the record outlives its abort"
    );
    let session = read_upload_session(&store, &pool_id, &upload_id)
        .await
        .expect("aborted session retained");
    assert_matches!(session.outcome, Some(UploadOutcome::Aborted { .. }));
    assert!(
        store.head(&content_key).await.expect("head").is_none(),
        "aborting deletes the object the session owned"
    );

    // The aborted record is reaped a grace window after its own stamp.
    let reaped = context(expired.now_ms + GC_GRACE_WINDOW_MS + 1);
    let report = collect_pool(&store, &pool_id, &reaped).await;
    assert_eq!(report.deleted.upload_sessions, 1);
    assert_eq!(
        report.deleted.content_objects, 0,
        "the abort half's unconditional cleanup is not a reclamation it can count"
    );
    assert!(store.head(&session_key).await.expect("head").is_none());

    let again = context(reaped.now_ms + GC_GRACE_WINDOW_MS);
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

    let expired = context(setup.now_ms + UPLOAD_SESSION_LEASE_MS + GC_GRACE_WINDOW_MS + 1);
    collect_pool(&store, &pool_id, &expired).await;

    store.fail_next(1);
    let reaped = context(expired.now_ms + GC_GRACE_WINDOW_MS + 1);
    let report = collect_pool(&store, &pool_id, &reaped).await;
    assert_eq!(report.deleted.upload_sessions, 0);
    assert_matches!(
        read_upload_session(&store, &pool_id, &upload_id)
            .await
            .expect("aborted session retained")
            .outcome,
        Some(UploadOutcome::Aborted { .. })
    );

    let report = collect_pool(&store, &pool_id, &reaped).await;
    assert_eq!(report.deleted.upload_sessions, 1);
    assert!(read_upload_session(&store, &pool_id, &upload_id)
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
        Some(expires_at_ms + GC_GRACE_WINDOW_MS),
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
    let expired = context(setup.now_ms + UPLOAD_SESSION_LEASE_MS + GC_GRACE_WINDOW_MS + 1);
    let report = collect_pool(&store, &pool_id, &expired).await;
    assert_eq!(report.deleted.upload_sessions, 0);
    let reclaim_at_ms = report
        .next_reclamation_at_ms
        .expect("the abort this collection performed is a deadline it created");
    assert_eq!(reclaim_at_ms, expired.now_ms + GC_GRACE_WINDOW_MS);

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

    let expired = context(1_000 + UPLOAD_SESSION_LEASE_MS + GC_GRACE_WINDOW_MS + 1);
    collect_pool(&store, &pool_id, &expired).await;
    let reaped = context(expired.now_ms + GC_GRACE_WINDOW_MS + 1);
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

    let memory = crate::gc::charged_set::CollectionMemory::new(pool_id.to_string(), 1);
    store.reset();
    let error = crate::test_support::pool_store(&store, &pool_id)
        .gc_within(None, &setup.clock(), memory.clone())
        .await
        .expect_err("the first session's content id does not fit the budget");

    assert_matches!(
        error,
        CoreError::LiveSetExceedsCollectionMemory { limit: 1, .. }
    );
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
    assert_eq!(memory.held_bytes(), 0);
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
    let late = context(early.now_ms + GC_GRACE_WINDOW_MS);
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

/// Checks that a pool's collection reports the listed bytes of every upload
/// record, whichever sweep decides the record.
#[tokio::test]
async fn a_pool_collection_reports_the_listed_bytes_of_every_upload_record() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    let pool_id = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap")
        .pool_id;
    complete_upload_for_gc(&store, &namespace_id, b"completed\n", &setup).await;
    stage_upload(&store, &namespace_id, &setup).await;
    let orphan = crate::test_support::proxied_upload_session(&namespace_id, 2_000);
    let orphan_session_key = crate::test_support::write_upload_records(
        &store,
        &pool_id,
        &orphan,
        Some(UploadOutcome::Aborted {
            aborted_at_ms: 1_000,
        }),
    )
    .await;
    store
        .delete(&orphan_session_key)
        .await
        .expect("delete session record");

    let listed_bytes = pool_listed_bytes(&store, &pool_id).await;
    let report = collect_pool(&store, &pool_id, &context(setup.now_ms + 1)).await;

    assert_eq!(report.listed_bytes, listed_bytes);
    assert_eq!(report.deleted.upload_sessions, 0);
    assert_eq!(report.retained.upload_session_window, 2);
    assert_eq!(report.retained.upload_session_undecided, 0);
}

/// A copy of the pool's `ListedUpload` and its kinds, which are private to
/// the pool's module. The queue charges `size_of` of its slot type, so the
/// copy keeps the same fields in the same order.
#[allow(dead_code)]
struct ListedUploadCopy {
    kind: ListedUploadKindCopy,
    size_bytes: u64,
}

#[allow(dead_code)]
enum ListedUploadKindCopy {
    Session {
        upload_id: UploadId,
        outcome_listing: OutcomeListingCopy,
    },
    OrphanOutcome {
        entry: loonfs_objectstore::ListedObject,
    },
    Unrecognized,
}

#[allow(dead_code)]
#[derive(Clone, Copy)]
enum OutcomeListingCopy {
    Listed,
    Absent,
}

impl crate::heap_bytes::HeapBytes for ListedUploadCopy {
    fn heap_bytes(&self) -> usize {
        match &self.kind {
            ListedUploadKindCopy::Session { upload_id, .. } => upload_id.heap_bytes(),
            ListedUploadKindCopy::OrphanOutcome { entry } => entry.key.heap_bytes(),
            ListedUploadKindCopy::Unrecognized => 0,
        }
    }
}

/// What `sessions` listed sessions charged to a pool collection's budget.
struct SessionCharge {
    sessions: usize,
    peak_bytes: usize,
}

impl SessionCharge {
    fn per_session(&self) -> f64 {
        self.peak_bytes as f64 / self.sessions as f64
    }
}

/// Charges `sessions` listed sessions the way `PoolRoots::load` does: a
/// completed session charges its content ID and its `CompletedUploadSession`,
/// and an incomplete one its content ID and its listed entry.
fn charge_listed_sessions(sessions: usize, completed: bool) -> SessionCharge {
    charge_listed_sessions_until(Some(sessions), completed)
}

/// Charges listed sessions as `charge_listed_sessions` does, up to `limit`
/// sessions or, with no limit, until the next session doesn't fit the
/// budget. `sessions` counts the sessions that were charged whole.
fn charge_listed_sessions_until(limit: Option<usize>, completed: bool) -> SessionCharge {
    use crate::gc::charged_set::{ChargedMap, ChargedQueue, ChargedSet, CollectionMemory};
    use crate::gc::CompletedUploadSession;

    let memory = CollectionMemory::new("pool", crate::limits::MAX_COLLECTION_BYTES);
    let mut content_ids = ChargedSet::<loonfs_types::ContentId>::new(&memory);
    let mut completed_sessions = ChargedMap::<UploadId, CompletedUploadSession>::new(&memory);
    let mut uploads = ChargedQueue::<ListedUploadCopy>::new(&memory);
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let mut sessions = 0;
    while limit.is_none_or(|limit| sessions < limit) {
        let upload_id = UploadId::generate();
        let content_id = loonfs_types::ContentId::generate();
        let charged = (|| {
            content_ids.insert(content_id.clone())?;
            if completed {
                return completed_sessions.insert(
                    upload_id,
                    CompletedUploadSession {
                        namespace_id: namespace_id.clone(),
                        content_id,
                        completed_at_ms: 1_000 + sessions as u64,
                    },
                );
            }
            uploads.push_back(ListedUploadCopy {
                kind: ListedUploadKindCopy::Session {
                    upload_id,
                    outcome_listing: OutcomeListingCopy::Absent,
                },
                size_bytes: 400,
            })
        })();
        match charged {
            Ok(()) => sessions += 1,
            Err(error) => {
                assert!(limit.is_none(), "session {sessions} doesn't fit: {error}");
                assert_matches!(error, CoreError::LiveSetExceedsCollectionMemory { .. });
                break;
            }
        }
    }
    SessionCharge {
        sessions,
        peak_bytes: memory.peak_bytes(),
    }
}

/// Measures what one listed upload session charges to a pool collection's
/// budget, built from the same charged collections `PoolRoots::load` uses.
#[test]
fn a_completed_session_costs_the_pool_collection_a_measured_number_of_bytes() {
    let mut runs = Vec::new();
    for completed in [true, false] {
        let label = if completed { "completed" } else { "incomplete" };
        let small = charge_listed_sessions(10_000, completed);
        let large = charge_listed_sessions(100_000, completed);
        runs.push((label, small, large));
    }
    for (label, small, large) in &runs {
        let ratio = large.per_session() / small.per_session();
        assert!(
            (0.5..=2.0).contains(&ratio),
            "{label}: the charge per session moves only with the tables' growth steps, \
             {ratio:.2} times from 10,000 to 100,000 sessions"
        );
    }
    // A completed session charges 94 bytes of IDs (the 30-byte content ID
    // in the set, and the 30-byte upload ID, the 30-byte content ID and the
    // 4-byte namespace ID of the fixture in the map) and two table slots: 25
    // bytes in the content ID set and 81 in the session map. A full table
    // holds 7 entries in 8 buckets, so the slots cost 121 to 242 bytes
    // depending on where the growth steps fall. An incomplete session charges
    // 60 bytes of IDs and two slots, 85 to 170 bytes.
    for (label, small, large) in &runs {
        let range = if *label == "completed" {
            210.0..=340.0
        } else {
            140.0..=230.0
        };
        for run in [small, large] {
            assert!(
                range.contains(&run.per_session()),
                "{label}: {} sessions charge {:.1} bytes each",
                run.sessions,
                run.per_session()
            );
        }
    }
    // The budget refuses the session that doesn't fit, after more sessions
    // than the largest run above.
    for (label, _, large) in &runs {
        let fill = charge_listed_sessions_until(None, *label == "completed");
        assert!(
            fill.sessions > large.sessions,
            "{label}: the budget held {} sessions",
            fill.sessions
        );
    }
}

/// Runs the pool's collection over a namespace with `sessions` completed
/// upload sessions inside the grace window, and returns the most bytes the
/// collection charged.
async fn pool_collection_peak_with_completed_sessions(sessions: usize) -> usize {
    use crate::gc::charged_set::CollectionMemory;

    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    let pool_id = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap")
        .pool_id;
    for _ in 0..sessions {
        complete_upload_for_gc(&store, &namespace_id, b"unpublished\n", &setup).await;
    }
    let memory = CollectionMemory::new(pool_id.to_string(), crate::limits::MAX_COLLECTION_BYTES);
    let report = crate::test_support::pool_store(&store, &pool_id)
        .gc_within(None, &context(setup.now_ms + 1).clock(), memory.clone())
        .await
        .expect("pool collection");
    assert_eq!(report.deleted.upload_sessions, 0);
    assert_eq!(memory.held_bytes(), 0);
    memory.peak_bytes()
}

/// Checks the synthetic charge against the pool's real collection, which
/// also charges the fixture namespace's members, segments, and filters.
#[tokio::test]
async fn a_real_pool_collection_charges_the_measured_per_session_cost() {
    const SESSIONS: usize = 200;
    let baseline = pool_collection_peak_with_completed_sessions(0).await;
    let with_sessions = pool_collection_peak_with_completed_sessions(SESSIONS).await;
    let synthetic = charge_listed_sessions(SESSIONS, true);
    let real_per_session = (with_sessions - baseline) as f64 / SESSIONS as f64;
    let ratio = real_per_session / synthetic.per_session();
    assert!(
        (0.8..=1.25).contains(&ratio),
        "a real session charges {real_per_session:.1} bytes and a synthetic one {:.1}",
        synthetic.per_session()
    );
}
