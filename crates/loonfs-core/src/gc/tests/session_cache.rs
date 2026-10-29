//! Completed upload sessions that a pass keeps for later passes.

use super::*;
use crate::cache::CompletedUploadSessionCache;
use loonfs_objectstore::keys::{upload_outcome, upload_session, upload_session_prefix};
use loonfs_types::format::control::UploadOutcome;
use std::collections::BTreeMap;

/// Creates a local store that records requests for the namespace's upload
/// session records.
fn session_recording_store(
    root: &std::path::Path,
    namespace_id: &NamespaceId,
) -> RecordingStore<LocalFsStore> {
    RecordingStore::new(
        LocalFsStore::new(root).expect("store"),
        KeyPredicate::prefix(upload_session_prefix(namespace_id)),
    )
}

/// Creates `count` upload sessions in the namespace, all completed at
/// `context`'s time, and returns their upload IDs.
async fn completed_sessions(
    store: &RecordingStore<LocalFsStore>,
    namespace_id: &NamespaceId,
    count: usize,
    context: &MutationContext,
) -> BTreeSet<UploadId> {
    let mut upload_ids = BTreeSet::new();
    for _ in 0..count {
        let (upload_id, _) =
            complete_upload_for_gc(store.inner(), namespace_id, b"unpublished\n", context).await;
        upload_ids.insert(upload_id);
    }
    upload_ids
}

/// Runs one pass at `now_ms` through `cache`, and returns its report and the
/// number of times it read each upload session record.
async fn collect(
    store: &RecordingStore<LocalFsStore>,
    cache: &CompletedUploadSessionCache,
    namespace_id: &NamespaceId,
    now_ms: u64,
) -> (GcResponse, BTreeMap<String, usize>) {
    store.reset();
    let report = crate::test_support::namespace_store(store, namespace_id)
        .gc(Some(cache), &options(), &context(now_ms))
        .await
        .expect("gc pass");
    let mut reads = BTreeMap::new();
    for key in store.take_get_keys() {
        *reads.entry(key).or_default() += 1;
    }
    (report, reads)
}

/// Checks that a second pass over a namespace with completed upload sessions
/// reads none of their records, after the first pass read each record once.
#[tokio::test]
async fn a_second_pass_reads_no_completed_session_record() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let store = session_recording_store(temp_dir.path(), &namespace_id);
    let completed = context(1_000);
    crate::test_support::NamespaceFixture::new(store.inner(), &namespace_id, &completed)
        .create()
        .await
        .expect("bootstrap");
    let upload_ids = completed_sessions(&store, &namespace_id, 20, &completed).await;
    let cache = CompletedUploadSessionCache::unshared(1024 * 1024);

    let (first, first_reads) = collect(&store, &cache, &namespace_id, completed.now_ms + 1).await;
    let (second, second_reads) = collect(&store, &cache, &namespace_id, completed.now_ms + 1).await;

    let expected: BTreeMap<String, usize> = upload_ids
        .iter()
        .map(|upload_id| (upload_outcome(&namespace_id, upload_id), 1))
        .collect();
    assert_eq!(
        first_reads, expected,
        "the first pass reads each outcome record once"
    );
    assert_eq!(second_reads, BTreeMap::new(), "the second pass reads none");
    assert_eq!(first.retained.upload_session_window, 20);
    assert_eq!(second.retained.upload_session_window, 20);
    assert_eq!(cache.len(), 20);
}

/// Checks that when the cache can't keep every completed session, a second
/// pass still deletes and keeps the right records, and reads only the records
/// of the sessions that the cache dropped.
#[tokio::test]
async fn a_pass_reads_only_the_sessions_that_the_cache_dropped() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let store = session_recording_store(temp_dir.path(), &namespace_id);
    let early = context(1_000);
    let late = context(early.now_ms + GRACE_MS);
    crate::test_support::NamespaceFixture::new(store.inner(), &namespace_id, &early)
        .create()
        .await
        .expect("bootstrap");
    let mut upload_ids = completed_sessions(&store, &namespace_id, 20, &early).await;
    let late_ids = completed_sessions(&store, &namespace_id, 20, &late).await;
    upload_ids.extend(late_ids.iter().cloned());
    let cache = CompletedUploadSessionCache::unshared(4096);

    let (first, first_reads) = collect(&store, &cache, &namespace_id, late.now_ms).await;
    assert_eq!(first_reads.len(), 40);
    assert_eq!(first.retained.upload_session_window, 40);
    let dropped: BTreeMap<String, usize> = upload_ids
        .iter()
        .filter(|upload_id| cache.get(upload_id).is_none())
        .map(|upload_id| (upload_outcome(&namespace_id, upload_id), 1))
        .collect();
    assert!(
        !dropped.is_empty() && dropped.len() < 40,
        "the budget keeps some of the 40 sessions, and it dropped {}",
        dropped.len()
    );

    let past_early_grace = early.now_ms + CONTENT_RECLAMATION_GRACE_WINDOW_MS;
    let (second, second_reads) = collect(&store, &cache, &namespace_id, past_early_grace).await;
    assert_eq!(
        second_reads, dropped,
        "the pass reads each dropped record once"
    );
    assert_eq!(second.deleted.upload_sessions, 20);
    assert_eq!(second.retained.upload_session_window, 20);
    let remaining: BTreeSet<UploadId> = store
        .inner()
        .list_prefix(&upload_session_prefix(&namespace_id))
        .await
        .expect("list sessions")
        .iter()
        .filter_map(|key| loonfs_objectstore::layout::upload_id_of(key))
        .collect();
    assert_eq!(remaining, late_ids, "the late sessions are kept");
}

/// Creates an upload session's records, ended with `outcome` if it's set,
/// and returns the session record's key.
async fn put_session(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
    expires_at_ms: u64,
    outcome: Option<UploadOutcome>,
) -> UploadId {
    let session = crate::test_support::proxied_upload_session(namespace_id, expires_at_ms);
    crate::test_support::write_upload_records(store, &session, outcome).await;
    session.upload_id
}

/// Checks that every pass reads the records of open and aborted sessions,
/// once to list the sessions and once to sweep them.
#[tokio::test]
async fn open_and_aborted_session_records_are_read_on_every_pass() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let store = session_recording_store(temp_dir.path(), &namespace_id);
    let setup = context(1_000);
    crate::test_support::NamespaceFixture::new(store.inner(), &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap");
    let expires_at_ms = setup.now_ms + UPLOAD_SESSION_LEASE_MS;
    let open = put_session(store.inner(), &namespace_id, expires_at_ms, None).await;
    let aborted = put_session(
        store.inner(),
        &namespace_id,
        expires_at_ms,
        Some(UploadOutcome::Aborted {
            aborted_at_ms: setup.now_ms,
        }),
    )
    .await;
    let completed = completed_sessions(&store, &namespace_id, 1, &setup).await;
    let completed = upload_outcome(&namespace_id, completed.first().expect("one session"));
    let cache = CompletedUploadSessionCache::unshared(1024 * 1024);

    let (first, first_reads) = collect(&store, &cache, &namespace_id, setup.now_ms + 1).await;
    let (second, second_reads) = collect(&store, &cache, &namespace_id, setup.now_ms + 1).await;

    // The listing shows that the open session has no outcome record, so only
    // its session record is read, once to root its content and once to
    // sweep it. An aborted session's two records are each read twice.
    let every_pass = BTreeMap::from([
        (upload_session(&namespace_id, &open), 2),
        (upload_session(&namespace_id, &aborted), 2),
        (upload_outcome(&namespace_id, &aborted), 2),
    ]);
    let mut first_expected = every_pass.clone();
    first_expected.insert(completed, 1);
    assert_eq!(first_reads, first_expected);
    assert_eq!(second_reads, every_pass);
    assert_eq!(first.retained.upload_session_window, 3);
    assert_eq!(second.retained.upload_session_window, 3);
    assert_eq!(cache.len(), 1, "the cache keeps only the completed session");
}

/// Checks that a pass protects the content of the completed sessions it found
/// in the cache, and that the sweep removes a session from the cache when it
/// deletes the session's record.
#[tokio::test]
async fn a_session_the_sweep_deletes_leaves_the_cache() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let store = session_recording_store(temp_dir.path(), &namespace_id);
    let completed = context(1_000);
    crate::test_support::NamespaceFixture::new(store.inner(), &namespace_id, &completed)
        .create()
        .await
        .expect("bootstrap");
    let upload_ids = completed_sessions(&store, &namespace_id, 3, &completed).await;
    let cache = CompletedUploadSessionCache::unshared(1024 * 1024);
    collect(&store, &cache, &namespace_id, completed.now_ms + 1).await;
    assert_eq!(cache.len(), 3);

    let aged = now_after_newest_object(store.inner(), &namespace_id, GRACE_MS + 1).await;
    let (deleted, reads) = collect(&store, &cache, &namespace_id, aged).await;
    assert_eq!(
        reads,
        BTreeMap::new(),
        "the pass found every session cached"
    );
    assert_eq!(deleted.deleted.upload_sessions, 3);
    assert_eq!(
        deleted.deleted.content_objects, 0,
        "the cached content IDs protect the content in the pass that deletes the records"
    );
    assert_eq!(cache.len(), 0);
    assert!(upload_ids
        .iter()
        .all(|upload_id| cache.get(upload_id).is_none()));

    let (reclaimed, _) = collect(&store, &cache, &namespace_id, aged).await;
    assert_eq!(reclaimed.deleted.content_objects, 3);
}
