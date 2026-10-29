//! Behavior tests for namespace GC.

use super::options::GcOptions;
use crate::commit_engine::{CommitCandidate, NamespaceCommitEngine};
use crate::error::CoreError;
use crate::in_flight::{STORE_READS_IN_FLIGHT, STORE_WRITES_IN_FLIGHT};
use crate::limits::{
    CONTENT_RECLAMATION_GRACE_WINDOW_MS, GC_MIN_GRACE_WINDOW_MS,
    NAMESPACE_RETIREMENT_GRACE_WINDOW_MS, UNREFERENCED_SEGMENT_GRACE_WINDOW_MS,
    UPLOAD_SESSION_LEASE_MS,
};
use crate::manifest::tests::{
    compact_a_family_group, create_checkpoint, write_test_file, writer_at,
};
use crate::manifest::MetadataCompactionPolicy;
use crate::manifest::RetentionTarget;
use crate::path::write::{CommitIntent, FilesystemOperation};
use crate::test_support::WriterAt;
use crate::upload_records::UploadSessionState;
use futures::StreamExt;
use loonfs_objectstore::keys::{
    hint, metadata_manifest_object, metadata_manifest_prefix, metadata_segment,
    metadata_segment_prefix, pin_prefix, wal_prefix,
};
use loonfs_objectstore::ObjectStore;
use loonfs_types::api::v0::GcResponse;
use loonfs_types::format::control::{
    decode_control_object, ControlObjectKind, PinOwner, PinPayload, UploadOutcome,
    UploadOutcomePayload,
};
use loonfs_types::{ContentRef, ManifestNo, NamespaceId, PinId, UploadId};
use std::collections::BTreeSet;
use std::num::NonZeroUsize;
use std::sync::Arc;

use crate::options::DeleteNamespaceOptions;
use bytes::Bytes;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::PutMode;
use loonfs_test_support::stores::{
    replace_object, BlockingStore, ConcurrencyWatchStore, FailStore, InjectedError, KeyPredicate,
    MetadataMapStore, OperationContext, OperationKind, OperationSelector, RecordingStore,
};
use loonfs_types::AttributeInclusion;
use std::assert_matches;
use tempfile::tempdir;

mod concurrent_retirement;
mod fork_chain;
mod listings;
mod many_pins;
mod retirement;
mod session_cache;
mod shared_bases;
mod superseded_roots;
mod upload_records;

const GRACE_MS: u64 = 60 * 60 * 1000;

fn options() -> GcOptions {
    GcOptions {
        grace_window_ms: GRACE_MS,
        ..Default::default()
    }
}

fn context(now_ms: u64) -> WriterAt {
    writer_at("gc-test", now_ms)
}

/// How long after its deletion a namespace retires under `options()`.
fn retirement_ms() -> u64 {
    GRACE_MS.max(NAMESPACE_RETIREMENT_GRACE_WINDOW_MS)
}

async fn pin_exists<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    checkpoint_id: &PinId,
) -> bool {
    crate::test_support::namespace_store(store, namespace_id)
        .load_pin(checkpoint_id)
        .await
        .expect("read pin")
        .is_some()
}

/// Derives "now" from durable object ages so the tests never touch a
/// wall clock: `offset_ms` past the newest object under the namespace.
async fn now_after_newest_object<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    offset_ms: u64,
) -> u64 {
    let prefix = loonfs_objectstore::keys::namespace_prefix(namespace_id);
    let mut newest = 0;
    for key in store.list_prefix(&prefix).await.expect("list namespace") {
        let modified = store
            .head(&key)
            .await
            .expect("head object")
            .expect("object exists")
            .last_modified_at_ms
            .expect("local fs provides timestamps");
        newest = newest.max(modified);
    }
    assert!(newest > 0, "namespace tree must not be empty");
    newest + offset_ms
}

/// Sums the listed sizes of the objects under the namespace prefix that a
/// pass lists: every object except the discovery hint and extension objects.
async fn core_listed_bytes<S: ObjectStore + ?Sized>(store: &S, namespace_id: &NamespaceId) -> u64 {
    use futures::{future, TryStreamExt};
    let prefix = loonfs_objectstore::keys::namespace_prefix(namespace_id);
    let extensions = format!("{prefix}extensions/");
    let hint = hint(namespace_id);
    store
        .list_entries_from_stream(&prefix, None)
        .try_filter(|entry| future::ready(entry.key != hint && !entry.key.starts_with(&extensions)))
        .try_fold(0, |total, entry| future::ok(total + entry.size_bytes))
        .await
        .expect("list namespace")
}

async fn stat_root<S: ObjectStore>(store: &S, namespace_id: &NamespaceId) {
    crate::test_support::namespace_store(store, namespace_id)
        .load_current_metadata_view()
        .await
        .expect("load latest view")
        .resolve_path(
            "/",
            AttributeInclusion::Omit,
            &crate::authorize::ReadAccess::live(crate::authorize::Authorizer::Unrestricted),
        )
        .await
        .expect("resolve root");
}

/// Which upload outcome a blocked put-if-absent creates.
#[derive(Debug, Clone, Copy)]
enum BlockedOutcome {
    Completed,
    Aborted,
}

impl BlockedOutcome {
    fn matches(self, bytes: &[u8]) -> bool {
        let Ok(envelope) =
            decode_control_object::<UploadOutcomePayload>(bytes, ControlObjectKind::UploadOutcome)
        else {
            return false;
        };
        match (self, &envelope.payload().outcome) {
            (Self::Completed, UploadOutcome::Completed { .. })
            | (Self::Aborted, UploadOutcome::Aborted { .. }) => true,
            (Self::Completed, UploadOutcome::Aborted { .. })
            | (Self::Aborted, UploadOutcome::Completed { .. }) => false,
        }
    }
}

/// Blocks the next put that would create an upload outcome record of `target`.
fn blocking_outcome_store(
    inner: LocalFsStore,
    target: BlockedOutcome,
) -> BlockingStore<LocalFsStore> {
    let store =
        BlockingStore::matching(
            inner,
            move |operation: &OperationContext<'_>| match operation.kind() {
                OperationKind::Put {
                    bytes,
                    mode: PutMode::CreateIfAbsent,
                } => target.matches(bytes),
                _ => false,
            },
        );
    store.block_next();
    store
}

#[tokio::test]
async fn gc_rejects_grace_windows_below_the_derived_minimum() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");

    let too_small = GcOptions {
        grace_window_ms: GC_MIN_GRACE_WINDOW_MS - 1,
        ..Default::default()
    };
    let error = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &too_small, &context(1_000).clock())
        .await
        .expect_err("sub-minimum grace window must be rejected");
    assert_matches!(
        &error,
        CoreError::InvalidGcOptions(message)
            if message.contains("below the derived safety minimum"),
        "expected invalid gc config, got {error:?}"
    );
    assert_eq!(
        error.code(),
        crate::error::ErrorCode::InvalidRequest,
        "the rejection surfaces as invalid_request"
    );
    let no_rows = GcOptions {
        content_shard_rows: 0,
        ..Default::default()
    };
    let error = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &no_rows, &context(1_000).clock())
        .await
        .expect_err("zero shard target");
    assert_matches!(
        error,
        CoreError::InvalidGcOptions(message) if message == "`content_shard_rows` must be at least 1"
    );
}

#[tokio::test]
async fn gc_reaps_below_floor_wal_objects_after_the_grace_window() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap");
    write_test_file(&store, &namespace_id, "/docs/one.txt", "gc-one", &setup).await;
    write_test_file(&store, &namespace_id, "/docs/two.txt", "gc-two", &setup).await;
    create_checkpoint(&store, &namespace_id, &setup)
        .await
        .expect("checkpoint");
    crate::test_support::NamespaceFixture::new(
        &store,
        &namespace_id,
        &crate::test_support::writer_at(
            "test-writer",
            crate::test_support::grace_elapsed_after(setup.now_ms),
        ),
    )
    .engine()
    .advance_retention_floor(RetentionTarget::Head)
    .await
    .expect("advance floor");

    let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
    let report = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options(), &aged.clock())
        .await
        .expect("gc pass");

    assert_eq!(report.deleted.wal_objects, 4);
    stat_root(&store, &namespace_id).await;
}

async fn write_upload_session(store: &LocalFsStore, namespace_id: &NamespaceId) -> String {
    let session =
        crate::test_support::proxied_upload_session(namespace_id, 1_000 + UPLOAD_SESSION_LEASE_MS);
    crate::test_support::write_upload_records(store, &session, None).await
}

async fn stage_upload<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &WriterAt,
) -> (UploadId, ContentRef) {
    let begin = crate::test_support::NamespaceFixture::new(store, namespace_id, context)
        .engine()
        .begin_upload()
        .await
        .expect("begin upload");
    let staged = crate::test_support::NamespaceFixture::new(
        store,
        namespace_id,
        &crate::test_support::writer_at("test-writer", context.now_ms),
    )
    .engine()
    .upload_content(&begin.upload_id, b"racing upload\n")
    .await
    .expect("stage upload");
    (
        begin.upload_id.clone(),
        staged.content_ref().expect("staged content").clone(),
    )
}

/// Reads an upload session's records, or returns `None` once its session
/// record is gone.
async fn read_upload_session<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    upload_id: &UploadId,
) -> Option<UploadSessionState> {
    match crate::test_support::namespace_store(store, namespace_id)
        .load_upload_session_state(upload_id)
        .await
    {
        Ok(state) => Some(state),
        Err(CoreError::UploadNotFound { .. }) => None,
        Err(error) => panic!("read upload session: {error}"),
    }
}

#[tokio::test]
async fn deleted_namespace_keeps_its_tombstone_and_segments() {
    let temp_dir = tempdir().expect("tempdir");
    let store = RecordingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::any(),
    );
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap");
    let mut content_keys = publish_owned_content(&store, &namespace_id, 2).await;
    let unreferenced_key =
        loonfs_objectstore::keys::content_blob(&namespace_id, &loonfs_types::ContentId::generate());
    store
        .put_if_absent(&unreferenced_key, Bytes::from_static(b"unreferenced"))
        .await
        .expect("unreferenced object");
    content_keys.push(unreferenced_key);
    content_keys.sort();
    create_checkpoint(&store, &namespace_id, &setup)
        .await
        .expect("user pin");
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .engine()
        .delete_namespace(DeleteNamespaceOptions::default())
        .await
        .expect("delete namespace");
    // The runtime that deleted the namespace raises its hint to the tombstone.
    crate::test_support::namespace_store(&store, &namespace_id)
        .raise_hint_to_current_manifest()
        .await;

    let final_activity = crate::test_support::namespace_store(&store, &namespace_id)
        .load_statistics()
        .await
        .expect("final statistics")
        .activity;
    let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
    let report = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options(), &aged.clock())
        .await
        .expect("gc pass");
    assert!(report.deleted.wal_objects >= 1);
    assert_eq!(report.deleted.metadata_segments, 0);
    assert!(report.deleted.manifests > 0);
    assert_eq!(report.deleted_checkpoints_by_owner.user, 1);
    let reaped = context(aged.now_ms + UNREFERENCED_SEGMENT_GRACE_WINDOW_MS);
    store.reset();
    let report = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options(), &reaped.clock())
        .await
        .expect("gc pass after segment grace");
    assert_eq!(
        report.deleted_checkpoints_by_owner,
        loonfs_types::DeletedCheckpointsByOwner::default()
    );
    assert_eq!(report.deleted.metadata_segments, 0);
    assert!(report.deleted.manifests >= 1);
    assert_eq!(
        report.deleted.retired_content_objects,
        content_keys.len() as u64
    );
    let content_prefix = format!(
        "{}content/",
        loonfs_objectstore::keys::namespace_prefix(&namespace_id)
    );
    let mut deleted_content = store
        .take()
        .into_iter()
        .filter_map(|operation| match operation {
            loonfs_test_support::stores::RecordedOperation::Delete { key, .. }
                if key.starts_with(&content_prefix) =>
            {
                Some(key)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    deleted_content.sort();
    assert_eq!(deleted_content, content_keys);
    for key in &content_keys {
        assert!(store.head(key).await.expect("reclaimed content").is_none());
    }

    for prefix in [wal_prefix(&namespace_id), pin_prefix(&namespace_id)] {
        assert!(
            store.list_prefix(&prefix).await.expect("list").is_empty(),
            "prefix `{prefix}` must be empty after reclamation"
        );
    }
    let current = crate::test_support::namespace_store(&store, &namespace_id)
        .load_current_manifest()
        .await
        .expect("tombstone");
    assert!(current.payload().status.is_deleted());
    let rooted_segments = current
        .payload()
        .runs
        .iter()
        .flat_map(|run| &run.segments)
        .map(loonfs_objectstore::keys::metadata_segment_object_key)
        .collect::<BTreeSet<_>>();
    assert!(!rooted_segments.is_empty());
    assert_eq!(
        store
            .list_prefix(&metadata_segment_prefix(&namespace_id))
            .await
            .expect("rooted segments")
            .into_iter()
            .collect::<BTreeSet<_>>(),
        rooted_segments
    );
    assert_eq!(current.payload().activity, final_activity);

    assert!(store
        .head(&current.object_key)
        .await
        .expect("tombstone")
        .is_some());
    assert!(store
        .head(&hint(&namespace_id))
        .await
        .expect("hint")
        .is_some());
    let again = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
    let report = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options(), &again.clock())
        .await
        .expect("second gc pass");
    assert_eq!(report.deleted.wal_objects, 0);
    assert_eq!(report.deleted.manifests, 0);
    assert_eq!(report.deleted.metadata_segments, 0);
    assert_eq!(report.deleted.content_objects, 0);
    assert_eq!(report.deleted.upload_sessions, 0);
    assert_eq!(report.deleted.retired_content_objects, 0);
    assert_eq!(report.deleted_checkpoints_by_owner, Default::default());
    assert_eq!(
        store
            .list_prefix(&metadata_manifest_prefix(&namespace_id))
            .await
            .expect("manifests"),
        vec![current.object_key]
    );
}

#[tokio::test]
async fn fork_protected_bases_survive_source_deletion_until_the_target_dies() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let source = NamespaceId::parse("source").expect("namespace id");
    let clone = NamespaceId::parse("clone").expect("namespace id");
    let setup = context(1_000);
    crate::test_support::NamespaceFixture::new(&store, &source, &setup)
        .create()
        .await
        .expect("bootstrap");
    write_test_file(&store, &source, "/docs/shared.txt", "gc-shared", &setup).await;
    crate::test_support::NamespaceFixture::new(&store, &source, &setup)
        .engine()
        .fork_namespace(&clone, &loonfs_test_support::test_actor(), None)
        .await
        .expect("fork");
    crate::test_support::NamespaceFixture::new(&store, &source, &setup)
        .engine()
        .delete_namespace(DeleteNamespaceOptions::default())
        .await
        .expect("delete source");
    // The runtime that deleted the source raises its hint to the tombstone.
    crate::test_support::namespace_store(&store, &source)
        .raise_hint_to_current_manifest()
        .await;

    // The deleted source keeps exactly what the living clone needs.
    let fork_record = read_fork_record(&store, &source).await;
    let basis_key = metadata_manifest_object(&source, &fork_record.pin_id.manifest_no());
    let aged = context(now_after_newest_object(&store, &source, GRACE_MS + 1).await);
    let report = crate::test_support::namespace_store(&store, &source)
        .gc(None, &options(), &aged.clock())
        .await
        .expect("gc pass with live clone");
    assert_eq!(report.deleted_checkpoints_by_owner.fork, 0);
    assert!(
        store.head(&basis_key).await.expect("head basis").is_some(),
        "fork basis must survive while the clone lives"
    );
    assert!(report.deleted.wal_objects > 0);
    let namespace = crate::test_support::namespace_store(&store, &clone);
    let clone_view = namespace
        .load_current_metadata_view()
        .await
        .expect("load clone view");
    let bytes = clone_view
        .read_file_bytes(
            "/docs/shared.txt",
            None,
            &crate::authorize::ReadAccess::live(crate::authorize::Authorizer::Unrestricted),
        )
        .await
        .expect("clone reads after source collection");
    assert_eq!(bytes.bytes, b"body\n");

    crate::test_support::NamespaceFixture::new(&store, &clone, &setup)
        .engine()
        .delete_namespace(DeleteNamespaceOptions::default())
        .await
        .expect("delete clone");
    let retired = crate::test_support::namespace_store(&store, &clone)
        .gc(None, &options(), &aged.clock())
        .await
        .expect("retire clone");
    let deadline = retired.reclaimable_at_ms.expect("clone retired");

    let aged = context(deadline);
    crate::test_support::namespace_store(&store, &clone)
        .gc(None, &options(), &aged.clock())
        .await
        .expect("clone releases source pin");
    assert!(!pin_exists(&store, &source, &fork_record.pin_id).await);

    let again = context(now_after_newest_object(&store, &source, GRACE_MS + 1).await);
    let report = crate::test_support::namespace_store(&store, &source)
        .gc(None, &options(), &again.clock())
        .await
        .expect("idempotent pass");
    assert_eq!(report.deleted_checkpoints_by_owner.fork, 0);
    assert_eq!(report.deleted.manifests, 1);
    assert!(store
        .head(&basis_key)
        .await
        .expect("basis after release")
        .is_none());
}

#[tokio::test]
async fn upload_gc_aborts_an_expired_session_then_reaps_it() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap");
    write_test_file(&store, &namespace_id, "/docs/one.txt", "gc-one", &setup).await;
    let (upload_id, content_ref) = stage_upload(&store, &namespace_id, &setup).await;
    let session_key = loonfs_objectstore::keys::upload_session(&namespace_id, &upload_id);
    let content_key = loonfs_objectstore::keys::content_blob(
        &content_ref.owner_namespace_id,
        &content_ref.content_id,
    );

    // Inside the lease nothing happens, however old the object looks: the
    // session carries its own expiry, so no provider timestamp decides this.
    let inside = context(setup.now_ms + UPLOAD_SESSION_LEASE_MS - 1);
    let report = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options(), &inside.clock())
        .await
        .expect("gc pass inside the lease");
    assert_eq!(report.deleted.upload_sessions, 0);
    assert!(store.head(&content_key).await.expect("head").is_some());

    // Past the lease plus a grace the session is aborted and the object it
    // was writing is deleted — in that order.
    let expired = context(setup.now_ms + UPLOAD_SESSION_LEASE_MS + GRACE_MS + 1);
    let report = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options(), &expired.clock())
        .await
        .expect("gc pass past the lease");
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
    let report = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options(), &reaped.clock())
        .await
        .expect("gc pass past the abort grace");
    assert_eq!(report.deleted.upload_sessions, 1);
    assert_eq!(
        report.deleted.content_objects, 0,
        "the abort half's unconditional cleanup is not a reclamation it can count"
    );
    assert!(store.head(&session_key).await.expect("head").is_none());

    let again = context(reaped.now_ms + GRACE_MS);
    let report = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options(), &again.clock())
        .await
        .expect("gc pass after the sweep");
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
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap");
    let (upload_id, _) = stage_upload(&store, &namespace_id, &setup).await;

    let expired = context(setup.now_ms + UPLOAD_SESSION_LEASE_MS + GRACE_MS + 1);
    crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options(), &expired.clock())
        .await
        .expect("abort expired upload");

    store.fail_next(1);
    let reaped = context(expired.now_ms + GRACE_MS + 1);
    let report = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options(), &reaped.clock())
        .await
        .expect("retain after failed cleanup");
    assert_eq!(report.deleted.upload_sessions, 0);
    assert_matches!(
        read_upload_session(&store, &namespace_id, &upload_id)
            .await
            .expect("aborted session retained")
            .outcome,
        Some(UploadOutcome::Aborted { .. })
    );

    let report = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options(), &reaped.clock())
        .await
        .expect("retry cleanup");
    assert_eq!(report.deleted.upload_sessions, 1);
    assert!(read_upload_session(&store, &namespace_id, &upload_id)
        .await
        .is_none());
}

#[tokio::test]
async fn a_pass_reports_the_soonest_deadline_it_retained() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap");
    let (upload_id, ..) = stage_upload(&store, &namespace_id, &setup).await;
    let expires_at_ms = setup.now_ms + UPLOAD_SESSION_LEASE_MS;

    // One un-expired open session and nothing else: the lease plus the
    // pass's own grace window is the whole answer.
    let inside = context(setup.now_ms + 1);
    let report = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options(), &inside.clock())
        .await
        .expect("gc pass inside the lease");
    assert_eq!(report.retained.total(), 3);
    assert_eq!(
        report.next_reclamation_at_ms,
        Some(expires_at_ms + GRACE_MS),
        "an open session's reclamation waits for its lease and then the grace window"
    );

    // Completing it moves the deadline to the derived content grace, which
    // is the one the next pass is too early for.
    let completed_at = context(setup.now_ms + 2);
    complete_staged_upload(&store, &namespace_id, &upload_id, &completed_at).await;
    let report = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options(), &completed_at.clock())
        .await
        .expect("gc pass over the completed session");
    assert_eq!(
        report.next_reclamation_at_ms,
        Some(completed_at.now_ms + CONTENT_RECLAMATION_GRACE_WINDOW_MS),
        "a completed session's content is protected by the derived grace, not the configured one"
    );
}

#[tokio::test]
async fn an_aborted_session_is_reclaimed_from_the_deadline_the_pass_reported() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap");
    let (upload_id, ..) = stage_upload(&store, &namespace_id, &setup).await;
    let session_key = loonfs_objectstore::keys::upload_session(&namespace_id, &upload_id);

    // The pass that aborts the session is the only thing that knows the
    // record now ages out a grace window from this instant.
    let expired = context(setup.now_ms + UPLOAD_SESSION_LEASE_MS + GRACE_MS + 1);
    let report = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options(), &expired.clock())
        .await
        .expect("gc pass past the lease");
    assert_eq!(report.deleted.upload_sessions, 0);
    let reclaim_at_ms = report
        .next_reclamation_at_ms
        .expect("the abort this pass performed is a deadline it created");
    assert_eq!(reclaim_at_ms, expired.now_ms + GRACE_MS);

    // Nothing between the two passes says anything about this namespace:
    // the time the first pass reported is the whole trigger.
    let reclaiming = context(reclaim_at_ms + 1);
    let report = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options(), &reclaiming.clock())
        .await
        .expect("gc pass at the reported deadline");
    assert_eq!(report.deleted.upload_sessions, 1);
    assert!(store.head(&session_key).await.expect("head").is_none());
    assert_eq!(
        report.next_reclamation_at_ms, None,
        "a pass that reclaimed everything it found owes no later visit"
    );
}

/// Completes a staged session against the content it staged.
async fn complete_staged_upload<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    upload_id: &UploadId,
    context: &WriterAt,
) {
    crate::test_support::NamespaceFixture::new(store, namespace_id, context)
        .engine()
        .complete_upload(
            &crate::test_support::namespace_store(store, namespace_id)
                .load_catalog_entry()
                .await
                .expect("load the catalog"),
            upload_id,
            loonfs_types::api::v0::CompleteUploadBody::ServiceProxied {},
        )
        .await
        .expect("complete upload");
}

#[tokio::test]
async fn upload_gc_reaps_a_session_that_never_staged_anything() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap");
    let session_key = write_upload_session(&store, &namespace_id).await;

    let expired = context(1_000 + UPLOAD_SESSION_LEASE_MS + GRACE_MS + 1);
    crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options(), &expired.clock())
        .await
        .expect("gc pass past the lease");
    let reaped = context(expired.now_ms + GRACE_MS + 1);
    let report = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options(), &reaped.clock())
        .await
        .expect("gc pass past the abort grace");

    assert_eq!(report.deleted.upload_sessions, 1);
    assert!(store.head(&session_key).await.expect("head").is_none());
}

#[tokio::test]
async fn upload_completion_wins_before_gc_abort_and_the_session_is_retained() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap");
    let (upload_id, content_ref) = stage_upload(&store, &namespace_id, &setup).await;
    let inside = context(setup.now_ms + UPLOAD_SESSION_LEASE_MS - 1);
    let aged = context(setup.now_ms + UPLOAD_SESSION_LEASE_MS + GRACE_MS + 1);
    let content_key = loonfs_objectstore::keys::content_blob(
        &content_ref.owner_namespace_id,
        &content_ref.content_id,
    );
    let store = blocking_outcome_store(store, BlockedOutcome::Aborted);
    let gc_options = options();
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let clock = aged.clock();
    let gc = namespace.gc(None, &gc_options, &clock);
    let complete = async {
        store.wait_until_blocked().await;
        let result = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &inside)
            .engine()
            .complete_upload(
                &crate::test_support::namespace_store(&store, &namespace_id)
                    .load_catalog_entry()
                    .await
                    .expect("load the catalog"),
                &upload_id,
                loonfs_types::api::v0::CompleteUploadBody::ServiceProxied {},
            )
            .await;
        store.release();
        result
    };
    let (report, completion) = tokio::join!(gc, complete);
    completion.expect("completion wins the blocked abort CAS");
    let report = report.expect("gc pass");
    assert_eq!(report.deleted.upload_sessions, 0);
    let session = read_upload_session(&store, &namespace_id, &upload_id)
        .await
        .expect("completed session retained");
    assert_matches!(session.outcome, Some(UploadOutcome::Completed { .. }));
    assert!(
        store.head(&content_key).await.expect("head").is_some(),
        "the losing abort must not clean up the winner's content"
    );
}

#[tokio::test]
async fn gc_abort_wins_before_completion_and_completion_reports_not_found() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap");
    let (upload_id, content_ref) = stage_upload(&store, &namespace_id, &setup).await;
    let inside = context(setup.now_ms + UPLOAD_SESSION_LEASE_MS - 1);
    let aged = context(setup.now_ms + UPLOAD_SESSION_LEASE_MS + GRACE_MS + 1);
    let content_key = loonfs_objectstore::keys::content_blob(
        &content_ref.owner_namespace_id,
        &content_ref.content_id,
    );
    let store = blocking_outcome_store(store, BlockedOutcome::Completed);
    let engine =
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &inside).engine();
    let catalog = crate::test_support::namespace_store(&store, &namespace_id)
        .load_catalog_entry()
        .await
        .expect("load the catalog");
    let completion = engine.complete_upload(
        &catalog,
        &upload_id,
        loonfs_types::api::v0::CompleteUploadBody::ServiceProxied {},
    );
    let abort = async {
        store.wait_until_blocked().await;
        let report = crate::test_support::namespace_store(&store, &namespace_id)
            .gc(None, &options(), &aged.clock())
            .await;
        store.release();
        report
    };
    let (completion, report) = tokio::join!(completion, abort);
    let error = completion.expect_err("an aborted session is logically absent");
    assert_matches!(&error, CoreError::UploadNotFound { .. });
    assert_eq!(error.code(), crate::error::ErrorCode::UploadNotFound);
    report.expect("gc pass");
    let session = read_upload_session(&store, &namespace_id, &upload_id)
        .await
        .expect("aborted session retained for a grace window");
    assert_matches!(session.outcome, Some(UploadOutcome::Aborted { .. }));
    assert!(
        store.head(&content_key).await.expect("head").is_none(),
        "the winning abort cleans up, and the losing completion does not resurrect"
    );
}

/// Completes an upload and returns the state needed by content GC tests.
async fn complete_upload_for_gc<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    bytes: &[u8],
    context: &WriterAt,
) -> (UploadId, ContentRef) {
    let begin = crate::test_support::NamespaceFixture::new(store, namespace_id, context)
        .engine()
        .begin_upload()
        .await
        .expect("begin upload");
    let staged = crate::test_support::NamespaceFixture::new(
        store,
        namespace_id,
        &crate::test_support::writer_at("test-writer", context.now_ms),
    )
    .engine()
    .upload_content(&begin.upload_id, bytes)
    .await
    .expect("stage upload");
    complete_staged_upload(store, namespace_id, &begin.upload_id, context).await;
    (
        begin.upload_id.clone(),
        staged.content_ref().expect("staged content").clone(),
    )
}

/// Stages `bytes` as a content object and publishes it at `path`, replacing
/// any file there.
async fn put_file_content<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    path: &str,
    bytes: &[u8],
) -> ContentRef {
    put_file_content_with_id(
        store,
        namespace_id,
        path,
        bytes,
        loonfs_types::ContentId::generate(),
    )
    .await
}

async fn put_file_content_with_id<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    path: &str,
    bytes: &[u8],
    content_id: loonfs_types::ContentId,
) -> ContentRef {
    let catalog = crate::test_support::namespace_store(store, namespace_id)
        .load_catalog_entry()
        .await
        .expect("catalog");
    let stored = crate::storage::content::stage_bytes_under_content_id(
        store,
        namespace_id.clone(),
        content_id,
        bytes,
    )
    .await
    .expect("content");
    let content_ref = stored.content_ref().clone();
    NamespaceCommitEngine::new(
        crate::test_support::namespace_store(store, namespace_id),
        context(1_000).writer_id.clone(),
    )
    .publish_batch(
        vec![CommitCandidate::prepared(
            CommitIntent::single(
                loonfs_types::CommitId::generate(),
                loonfs_test_support::test_actor(),
                None,
                FilesystemOperation::PutFile {
                    path: loonfs_types::AbsolutePath::parse(path).expect("path"),
                    content: loonfs_types::FileContent::Ref(content_ref.clone()),
                    behavior: loonfs_types::DestinationBehavior::Replace,
                    expected_inode_id: None,
                    expected_revision_no: None,
                },
            ),
            vec![crate::storage::content::prepare_stored_content(
                &catalog, stored,
            )],
        )],
        &context(1_000).clock(),
    )
    .await
    .results
    .pop()
    .expect("one result")
    .expect("published");
    content_ref
}

/// Publishes `/owned-{index}` with its bytes inline in the WAL and returns
/// the content key a fold or a direct download writes.
async fn publish_inline(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
    index: usize,
    setup: &WriterAt,
) -> String {
    let value = crate::storage::inline_content::InlineContent::new(
        namespace_id.clone(),
        loonfs_types::ContentId::generate(),
        Bytes::copy_from_slice(namespace_id.as_str().as_bytes()),
        loonfs_types::ChecksumAlgorithm::Crc64nvme,
    );
    let key = loonfs_objectstore::keys::content_blob(namespace_id, &value.content_ref().content_id);
    let request = CommitIntent::single(
        loonfs_types::CommitId::parse(format!("write-{index}")).expect("commit"),
        loonfs_test_support::test_actor(),
        None,
        FilesystemOperation::PutFile {
            path: loonfs_types::AbsolutePath::parse(format!("/owned-{index}")).expect("path"),
            content: loonfs_types::FileContent::Ref(value.content_ref().clone()),
            behavior: loonfs_types::DestinationBehavior::NoReplace,
            expected_inode_id: None,
            expected_revision_no: None,
        },
    );
    NamespaceCommitEngine::new(
        crate::test_support::namespace_store(store, namespace_id),
        setup.writer_id.clone(),
    )
    .publish_batch(
        [CommitCandidate::with_inline_content(
            request,
            Vec::new(),
            vec![value],
        )],
        &setup.clock(),
    )
    .await
    .results
    .pop()
    .expect("result")
    .expect("publish inline");
    key
}

/// Folds the tail, moves the floor to the head, and compacts every family
/// group, so each file keeps only its newest revision.
async fn compact_below_the_floor<S: ObjectStore>(store: &S, namespace_id: &NamespaceId) {
    crate::test_support::namespace_store(store, namespace_id)
        .fold_wal()
        .await
        .expect("fold wal");
    crate::test_support::NamespaceFixture::new(
        store,
        namespace_id,
        &crate::test_support::writer_at(
            "test-writer",
            crate::test_support::grace_elapsed_after(1_000),
        ),
    )
    .engine()
    .advance_retention_floor(RetentionTarget::Head)
    .await
    .expect("advance floor");
    for _ in 0..16 {
        let outcome = crate::test_support::NamespaceFixture::new(
            store,
            namespace_id,
            &crate::test_support::writer_at("test-writer", 0),
        )
        .engine()
        .compact_metadata_unit(
            None,
            MetadataCompactionPolicy::CompactImmediately,
            crate::manifest::CompactionAdmission::AnySize,
            loonfs_types::CompactorEpoch(0),
            &crate::manifest::MetadataCompactionCancellation::default(),
        )
        .await
        .expect("compact a family group");
        if matches!(outcome, crate::manifest::CompactionUnitOutcome::NotNeeded) {
            return;
        }
    }
    panic!("compaction did not settle");
}

async fn read_context<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> crate::RuntimeReadContext {
    let anchor = crate::test_support::namespace_store(store, namespace_id)
        .load_read_anchor(crate::test_support::observed_now())
        .await
        .expect("read anchor");
    crate::RuntimeReadContext {
        read_basis: crate::control::ReadBasis {
            basis: anchor.basis(),
            head: anchor.read_state,
        },
        tail: None,
    }
}

#[tokio::test]
async fn content_gc_retains_completed_content_inside_its_grace() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap");
    let (upload_id, content_ref) =
        complete_upload_for_gc(&store, &namespace_id, b"unpublished\n", &setup).await;
    let content_key = loonfs_objectstore::keys::content_blob(
        &content_ref.owner_namespace_id,
        &content_ref.content_id,
    );

    let inside = context(setup.now_ms + CONTENT_RECLAMATION_GRACE_WINDOW_MS - 1);
    let report = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options(), &inside.clock())
        .await
        .expect("gc pass inside the content grace");

    assert_eq!(report.deleted.upload_sessions, 0);
    assert_eq!(report.deleted.content_objects, 0);
    assert!(store.head(&content_key).await.expect("head").is_some());
    assert!(read_upload_session(&store, &namespace_id, &upload_id)
        .await
        .is_some());
}

#[tokio::test]
async fn an_unpublished_completed_upload_loses_its_record_then_its_object() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap");
    write_test_file(&store, &namespace_id, "/docs/other.txt", "gc-other", &setup).await;
    let (upload_id, content_ref) =
        complete_upload_for_gc(&store, &namespace_id, b"unpublished\n", &setup).await;
    let content_key = loonfs_objectstore::keys::content_blob(
        &content_ref.owner_namespace_id,
        &content_ref.content_id,
    );

    let past = context(
        now_after_newest_object(
            &store,
            &namespace_id,
            CONTENT_RECLAMATION_GRACE_WINDOW_MS + 1,
        )
        .await,
    );
    let report = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options(), &past.clock())
        .await
        .expect("gc pass past the content grace");
    assert_eq!(report.deleted.upload_sessions, 1);
    assert_eq!(
        report.deleted.content_objects, 0,
        "the pass read the record as a root before it removed it"
    );
    assert!(read_upload_session(&store, &namespace_id, &upload_id)
        .await
        .is_none());
    assert!(store.head(&content_key).await.expect("head").is_some());

    let report = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options(), &past.clock())
        .await
        .expect("gc pass after the record");
    assert_eq!(report.deleted.content_objects, 1);
    assert!(store.head(&content_key).await.expect("head").is_none());
}

#[tokio::test]
async fn a_revision_compacted_away_below_the_floor_loses_its_object_after_the_grace() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context(1_000))
        .create()
        .await
        .expect("bootstrap");
    let first = put_file_content_with_id(
        &store,
        &namespace_id,
        "/file",
        b"first",
        loonfs_types::ContentId::parse("con_00000000000000000000000001").expect("first id"),
    )
    .await;
    crate::test_support::namespace_store(&store, &namespace_id)
        .fold_wal()
        .await
        .expect("fold the first revision into the base run");
    let second = put_file_content_with_id(
        &store,
        &namespace_id,
        "/file",
        b"second",
        loonfs_types::ContentId::parse("con_00000000000000000000000002").expect("second id"),
    )
    .await;
    compact_below_the_floor(&store, &namespace_id).await;
    let first_key = loonfs_objectstore::keys::content_blob(&namespace_id, &first.content_id);

    // Only the manifest the compaction superseded still names the object.
    let young = context(now_after_newest_object(&store, &namespace_id, 0).await);
    let aged_content = MetadataMapStore::aged(&store, KeyPredicate::content_blob());
    let report = crate::test_support::namespace_store(&aged_content, &namespace_id)
        .gc(None, &options(), &young.clock())
        .await
        .expect("gc pass inside the grace");
    assert_eq!(report.deleted.content_objects, 0);
    assert!(store.head(&first_key).await.expect("head").is_some());

    let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
    let report = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options(), &aged.clock())
        .await
        .expect("gc pass past the grace");
    assert_eq!(report.deleted.content_objects, 1);
    assert!(store.head(&first_key).await.expect("head").is_none());
    assert!(store
        .head(&loonfs_objectstore::keys::content_blob(
            &namespace_id,
            &second.content_id
        ))
        .await
        .expect("head")
        .is_some());
    let error =
        crate::NamespaceEngine::reader(crate::test_support::namespace_store(&store, &namespace_id))
            .read_content_ref(&first, u64::MAX, &read_context(&store, &namespace_id).await)
            .await
            .expect_err("a reclaimed object reads as an unpublished one");
    assert_eq!(error.code(), crate::error::ErrorCode::PathNotFound);
}

/// Checks that once a later commit acknowledges the floor that expired a
/// deletion, a base compaction of each group removes the rows that the
/// deletion hides, and a later layout compaction drops their chains, a pass
/// past the grace deletes the hidden file's content and keeps the content
/// of a visible file.
#[tokio::test]
async fn content_an_expired_deletion_hid_is_deleted_once_its_rows_are_removed() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap");
    let hidden = put_file_content(&store, &namespace_id, "/old/file", b"hidden").await;
    let kept = put_file_content(&store, &namespace_id, "/kept", b"kept").await;
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .delete_path("/old", None)
        .await
        .expect("delete /old");
    crate::test_support::namespace_store(&store, &namespace_id)
        .fold_wal()
        .await
        .expect("fold the deletion");
    crate::test_support::NamespaceFixture::new(
        &store,
        &namespace_id,
        &crate::test_support::writer_at(
            "test-writer",
            crate::test_support::grace_elapsed_after(setup.now_ms),
        ),
    )
    .engine()
    .advance_retention_floor(RetentionTarget::Head)
    .await
    .expect("move the floor past the deletion");
    // Each new file's WAL object records the advanced floor, and each pass
    // folds it and merges every group that the file gave a delta run. The
    // first pass removes the hidden revision, but its layout merge can run
    // before its revisions merge, so the second pass drops the chain.
    for path in ["/later-1", "/later-2"] {
        put_file_content(&store, &namespace_id, path, b"later").await;
        compact_below_the_floor(&store, &namespace_id).await;
    }

    let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
    let report = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options(), &aged.clock())
        .await
        .expect("gc pass past the grace");
    let content_key = |content_ref: &ContentRef| {
        loonfs_objectstore::keys::content_blob(&namespace_id, &content_ref.content_id)
    };
    assert!(report.deleted.content_objects >= 1);
    assert!(store
        .head(&content_key(&hidden))
        .await
        .expect("head")
        .is_none());
    assert!(store
        .head(&content_key(&kept))
        .await
        .expect("head")
        .is_some());
}

#[tokio::test]
async fn content_only_a_pinned_manifest_names_stays_until_the_pin_is_released() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap");
    let pinned = put_file_content_with_id(
        &store,
        &namespace_id,
        "/file",
        b"pinned",
        loonfs_types::ContentId::parse("con_00000000000000000000000001").expect("pinned id"),
    )
    .await;
    let checkpoint = create_checkpoint(&store, &namespace_id, &setup)
        .await
        .expect("checkpoint");
    put_file_content_with_id(
        &store,
        &namespace_id,
        "/file",
        b"current",
        loonfs_types::ContentId::parse("con_00000000000000000000000002").expect("current id"),
    )
    .await;
    compact_below_the_floor(&store, &namespace_id).await;
    let pinned_key = loonfs_objectstore::keys::content_blob(&namespace_id, &pinned.content_id);

    let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
    let report = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options(), &aged.clock())
        .await
        .expect("gc pass with the pin");
    assert_eq!(report.deleted.content_objects, 0);
    assert!(store.head(&pinned_key).await.expect("head").is_some());

    crate::test_support::NamespaceFixture::new(
        &store,
        &namespace_id,
        &crate::test_support::writer_at("test-writer", 0),
    )
    .engine()
    .delete_snapshot(&checkpoint.checkpoint_id)
    .await
    .expect("release the pin");
    let report = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options(), &aged.clock())
        .await
        .expect("gc pass after the release");
    assert_eq!(report.deleted.content_objects, 1);
    assert!(store.head(&pinned_key).await.expect("head").is_none());
}

#[tokio::test]
async fn a_source_keeps_content_its_fork_inherited_while_the_fork_pin_exists() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let source = NamespaceId::parse("source").expect("namespace id");
    let clone = NamespaceId::parse("clone").expect("namespace id");
    let setup = context(1_000);
    crate::test_support::NamespaceFixture::new(&store, &source, &setup)
        .create()
        .await
        .expect("bootstrap");
    put_file_content(&store, &source, "/file", b"inherited").await;
    crate::test_support::NamespaceFixture::new(&store, &source, &setup)
        .engine()
        .fork_namespace(&clone, &loonfs_test_support::test_actor(), None)
        .await
        .expect("fork");
    put_file_content(&store, &source, "/file", b"replaced").await;
    compact_below_the_floor(&store, &source).await;

    let aged = context(now_after_newest_object(&store, &source, GRACE_MS + 1).await);
    let report = crate::test_support::namespace_store(&store, &source)
        .gc(None, &options(), &aged.clock())
        .await
        .expect("gc pass with a live fork");
    assert_eq!(report.deleted.content_objects, 0);
    let file = crate::test_support::namespace_store(&store, &clone)
        .load_current_metadata_view()
        .await
        .expect("clone view")
        .read_file_bytes(
            "/file",
            None,
            &crate::authorize::ReadAccess::live(crate::authorize::Authorizer::Unrestricted),
        )
        .await
        .expect("the fork reads what it inherited");
    assert_eq!(file.bytes, b"inherited");
}

#[tokio::test]
async fn the_wal_tail_roots_inline_content_a_direct_download_wrote_early() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap");
    let key = publish_inline(&store, &namespace_id, 0, &setup).await;
    let target =
        crate::NamespaceEngine::reader(crate::test_support::namespace_store(&store, &namespace_id))
            .direct_download_target(
                "/owned-0",
                None,
                0,
                &read_context(&store, &namespace_id).await,
            )
            .await
            .expect("download target");
    assert_eq!(target.ranges[0].object_key, key);

    let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
    let report = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options(), &aged.clock())
        .await
        .expect("gc pass");
    assert_eq!(report.deleted.content_objects, 0);
    assert!(store.head(&key).await.expect("head").is_some());
}

#[tokio::test]
async fn content_and_temporary_keys_the_layout_does_not_recognize_are_kept() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context(1_000))
        .create()
        .await
        .expect("bootstrap");
    let content = loonfs_objectstore::keys::content_prefix(&namespace_id);
    let temporary = loonfs_objectstore::keys::temporary_prefix(&namespace_id);
    let collected = [
        loonfs_objectstore::keys::content_blob(&namespace_id, &loonfs_types::ContentId::generate()),
        format!("{temporary}piece"),
    ];
    let kept = [
        format!("{content}not-a-content-id"),
        format!("{content}nested/object"),
        format!("{temporary}nested/piece"),
    ];
    for key in collected.iter().chain(&kept) {
        store
            .put_if_absent(key, Bytes::from_static(b"bytes"))
            .await
            .expect("write object");
    }

    let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
    let report = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options(), &aged.clock())
        .await
        .expect("gc pass");
    assert_eq!(
        (
            report.deleted.content_objects,
            report.deleted.temporary_objects
        ),
        (1, 1)
    );
    assert_eq!(report.retained.unrecognized_key, 1);
    for key in &collected {
        assert!(store.head(key).await.expect("head").is_none(), "{key}");
    }
    for key in &kept {
        assert!(store.head(key).await.expect("head").is_some(), "{key}");
    }
}

#[tokio::test]
async fn gc_retains_everything_inside_the_grace_window() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap");
    write_test_file(&store, &namespace_id, "/docs/one.txt", "gc-one", &setup).await;
    create_checkpoint(&store, &namespace_id, &setup)
        .await
        .expect("checkpoint");
    crate::test_support::NamespaceFixture::new(
        &store,
        &namespace_id,
        &crate::test_support::writer_at(
            "test-writer",
            crate::test_support::grace_elapsed_after(setup.now_ms),
        ),
    )
    .engine()
    .advance_retention_floor(RetentionTarget::Head)
    .await
    .expect("advance floor");

    let orphan = metadata_segment(&namespace_id, &loonfs_types::MetadataSegmentId::generate());
    store
        .put_if_absent(&orphan, Bytes::from_static(b"unused"))
        .await
        .expect("young orphan");
    let young = context(now_after_newest_object(&store, &namespace_id, 0).await);
    let listed_bytes = core_listed_bytes(&store, &namespace_id).await;
    let report = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options(), &young.clock())
        .await
        .expect("gc pass");

    assert_eq!(report.deleted.wal_objects, 0);
    assert_eq!(report.deleted.metadata_segments, 0);
    assert_eq!(report.deleted.manifests, 0);
    assert_eq!(report.listed_bytes, listed_bytes);
    assert!(report.retained.total() > 0);
    // Everything unreachable here is simply young, and the pass says so
    // rather than leaving the operator to guess between age and reachability.
    assert!(report.retained.within_grace_window > 0);
    assert_eq!(report.retained.no_provider_timestamp, 0);
    stat_root(&store, &namespace_id).await;
}

#[tokio::test]
async fn published_compaction_segments_are_referenced_and_kept() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap");
    for index in 0..4 {
        write_test_file(
            &store,
            &namespace_id,
            &format!("/docs/{index}.txt"),
            &format!("gc-body-{index}"),
            &setup,
        )
        .await;
        // Folds rather than checkpoints: a checkpoint pins the manifest it
        // published, and a pinned manifest protects its segments forever, which
        // would leave this pass nothing to reap and nothing to prove.
        crate::test_support::namespace_store(&store, &namespace_id)
            .fold_wal()
            .await
            .expect("fold wal");
    }
    let staged = compact_a_family_group(&store, &namespace_id, &setup).await;

    // Far past every window this collector knows, including the staging one.
    let long_after = context(
        now_after_newest_object(
            &store,
            &namespace_id,
            UNREFERENCED_SEGMENT_GRACE_WINDOW_MS * 4,
        )
        .await,
    );
    let report = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options(), &long_after.clock())
        .await
        .expect("gc pass long after the job published");
    assert!(
        report.deleted.metadata_segments > 0,
        "the pass must have reaped the runs the job replaced, or it proves nothing"
    );
    for key in &staged {
        assert!(
            store
                .head(key)
                .await
                .expect("head a output segment")
                .is_some(),
            "a published job's segment must survive every window"
        );
    }
}

#[tokio::test]
async fn a_publication_during_a_pass_never_costs_the_job_its_segments() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    let seed = LocalFsStore::new(temp_dir.path()).expect("store");
    crate::test_support::NamespaceFixture::new(&seed, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap");
    for index in 0..4 {
        write_test_file(
            &seed,
            &namespace_id,
            &format!("/docs/{index}.txt"),
            &format!("gc-body-{index}"),
            &setup,
        )
        .await;
        crate::test_support::namespace_store(&seed, &namespace_id)
            .fold_wal()
            .await
            .expect("fold wal");
    }
    // One clock for the job and the pass: the job stamps its lease at the same
    // instant the pass reads it by, which is what a running job's lease looks
    // like to a collector.
    let pass_clock = context(now_after_newest_object(&seed, &namespace_id, GRACE_MS + 1).await);
    let (policy, spec) =
        crate::manifest::tests::plan_a_family_group_compaction(&seed, &namespace_id, &pass_clock)
            .await;

    // The pass parks at its first listing, which is after it has collected the
    // roots and long before it reaches the compaction prefix. Everything the
    // job does happens in that gap, on a store the gate does not hold.
    let gated = BlockingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::prefix(wal_prefix(&namespace_id)),
        OperationSelector::List,
    );
    gated.block_next();
    let pass_options = options();
    let namespace = crate::test_support::namespace_store(&gated, &namespace_id);
    let clock = pass_clock.clock();
    let (report, staged) = tokio::join!(namespace.gc(None, &pass_options, &clock), async {
        gated.wait_until_blocked().await;
        crate::manifest::tests::publish_planned_compaction(
            &seed,
            &namespace_id,
            &pass_clock,
            policy,
            &spec,
        )
        .await;
        let staged =
            crate::manifest::tests::segment_keys_of_the_current_manifest(&seed, &namespace_id)
                .await;
        gated.release();
        staged
    });
    report.expect("the pass must finish");

    for key in &staged {
        assert!(
            seed.head(key)
                .await
                .expect("head a output segment")
                .is_some(),
            "a segment the manifest names must survive a pass whose live set predates it"
        );
    }
}

#[tokio::test]
async fn a_pass_names_a_pin_it_could_not_advance() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap");
    write_test_file(&store, &namespace_id, "/docs/one.txt", "gc-one", &setup).await;
    create_checkpoint(&store, &namespace_id, &setup)
        .await
        .expect("checkpoint");

    let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS * 2).await);
    let report = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options(), &aged.clock())
        .await
        .expect("gc pass");

    assert_eq!(
        report.deleted_checkpoints_by_owner,
        loonfs_types::DeletedCheckpointsByOwner::default()
    );
    assert_eq!(report.retained.checkpoint_not_deletable, 1);
}

#[tokio::test]
async fn gc_never_deletes_the_live_replay_tail() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap");
    write_test_file(&store, &namespace_id, "/docs/one.txt", "gc-one", &setup).await;
    write_test_file(
        &store,
        &namespace_id,
        "/docs/before-floor.txt",
        "before-floor",
        &setup,
    )
    .await;
    create_checkpoint(&store, &namespace_id, &setup)
        .await
        .expect("checkpoint");
    crate::test_support::NamespaceFixture::new(
        &store,
        &namespace_id,
        &crate::test_support::writer_at(
            "test-writer",
            crate::test_support::grace_elapsed_after(setup.now_ms),
        ),
    )
    .engine()
    .advance_retention_floor(RetentionTarget::Head)
    .await
    .expect("advance floor");
    // A commit past the floor: its WAL object is the live replay gap.
    write_test_file(&store, &namespace_id, "/docs/two.txt", "gc-two", &setup).await;

    let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
    let report = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options(), &aged.clock())
        .await
        .expect("gc pass");

    assert_eq!(report.deleted.wal_objects, 4);
    // Latest reads replay the retained tail over the manifest basis.
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let view = namespace
        .load_current_metadata_view()
        .await
        .expect("load view");
    view.resolve_path(
        "/docs/two.txt",
        AttributeInclusion::Omit,
        &crate::authorize::ReadAccess::live(crate::authorize::Authorizer::Unrestricted),
    )
    .await
    .expect("tail commit stays readable");
}

async fn assert_basis_reaped(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
    _pass: &GcResponse,
    manifest_number: &ManifestNo,
) {
    crate::manifest::load_namespace_manifest_envelope(store, namespace_id, manifest_number)
        .await
        .expect_err("the reaped basis manifest should be gone");
}

#[tokio::test]
async fn gc_retains_unrecognized_manifest_keys() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap");

    let manifest_prefix = metadata_manifest_prefix(&namespace_id);
    let foreign_objects = [
        (
            format!("{manifest_prefix}notes.txt"),
            b"foreign key".as_slice(),
        ),
        (
            format!("{manifest_prefix}invalid.manifest.json.zst"),
            b"invalid manifest number".as_slice(),
        ),
    ];
    for (key, bytes) in &foreign_objects {
        store
            .put_if_absent(key, Bytes::copy_from_slice(bytes))
            .await
            .expect("write foreign manifest-prefix object");
    }

    let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
    let report = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options(), &aged.clock())
        .await
        .expect("gc pass");

    assert_eq!(report.deleted.manifests, 0);
    for (key, expected) in foreign_objects {
        let actual = store
            .get(&key, None)
            .await
            .expect("get foreign manifest-prefix object")
            .expect("unrecognized object is retained");
        assert_eq!(actual.as_ref(), expected);
    }
}

#[tokio::test]
async fn gc_reclaims_manifests_superseded_by_wal_folds() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap");
    for round in 0..3 {
        write_test_file(
            &store,
            &namespace_id,
            &format!("/docs/file-{round}.txt"),
            &format!("gc-adv-{round}"),
            &setup,
        )
        .await;
        crate::test_support::namespace_store(&store, &namespace_id)
            .fold_wal()
            .await
            .expect("fold wal");
    }
    // The runtime that published the folds raises the hint.
    crate::test_support::namespace_store(&store, &namespace_id)
        .raise_hint_to_current_manifest()
        .await;

    // Record-less maintenance: nothing accumulates under `pins/`.
    assert!(
        store
            .list_prefix(&pin_prefix(&namespace_id))
            .await
            .expect("list pins")
            .is_empty(),
        "a wal fold must not create pins"
    );

    let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
    let report = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options(), &aged.clock())
        .await
        .expect("gc pass");

    // The first fold materialized the namespace's first manifest and the
    // next two superseded it; only the current manifest is reachable. Its
    // segments are all still referenced (a fold only appends delta runs).
    assert_eq!(report.deleted.manifests, 6);
    let manifests_left = store
        .list_prefix(&metadata_manifest_prefix(&namespace_id))
        .await
        .expect("list manifests");
    assert_eq!(manifests_left.len(), 1, "only the current manifest stays");

    // Compaction merges the delta runs into fresh base segments; the
    // superseded run segments then age out on the next pass.
    let merge_policy = crate::manifest::MetadataRunPolicy {
        max_delta_runs: NonZeroUsize::MIN,
        ..Default::default()
    };
    for _ in 0..16 {
        let report = {
            let mut fixture = crate::test_support::NamespaceFixture::new(
                &store,
                &namespace_id,
                &crate::test_support::writer_at("test-writer", 0),
            );
            fixture.namespace.run_policy = merge_policy;
            fixture
        }
        .engine()
        .compact_metadata_unit(
            None,
            MetadataCompactionPolicy::default(),
            crate::manifest::CompactionAdmission::AnySize,
            loonfs_types::CompactorEpoch(0),
            &crate::manifest::MetadataCompactionCancellation::default(),
        )
        .await
        .expect("compact step");
        if matches!(report, crate::manifest::CompactionUnitOutcome::NotNeeded) {
            break;
        }
    }
    let aged = context(
        now_after_newest_object(
            &store,
            &namespace_id,
            UNREFERENCED_SEGMENT_GRACE_WINDOW_MS + 1,
        )
        .await,
    );
    let after_merge = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options(), &aged.clock())
        .await
        .expect("gc pass after compaction");
    assert!(
        after_merge.deleted.metadata_segments > 0,
        "merged-away run segments become collectable"
    );

    stat_root(&store, &namespace_id).await;
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let view = namespace
        .load_current_metadata_view()
        .await
        .expect("load view");
    for round in 0..3 {
        view.resolve_path(
            &format!("/docs/file-{round}.txt"),
            AttributeInclusion::Omit,
            &crate::authorize::ReadAccess::live(crate::authorize::Authorizer::Unrestricted),
        )
        .await
        .expect("file readable after sweep");
    }
}

#[tokio::test]
async fn gc_keeps_a_basis_pinned_by_another_owner_after_one_release() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap");
    write_test_file(&store, &namespace_id, "/docs/one.txt", "gc-one", &setup).await;
    let first = crate::test_support::namespace_store(&store, &namespace_id)
        .create_pin(
            PinOwner::User {
                name: "keeper".to_owned(),
                expires_at_ms: None,
            },
            &setup.clock(),
        )
        .await
        .map(|report| crate::pin::checkpoint_summary(report.response))
        .expect("first owner");
    let second = crate::test_support::namespace_store(&store, &namespace_id)
        .create_pin(
            PinOwner::User {
                name: "releaser".to_owned(),
                expires_at_ms: None,
            },
            &setup.clock(),
        )
        .await
        .map(|report| crate::pin::checkpoint_summary(report.response))
        .expect("second owner");
    assert_ne!(first.checkpoint_id, second.checkpoint_id);
    assert_eq!(first.manifest_no, second.manifest_no);
    write_test_file(&store, &namespace_id, "/docs/two.txt", "gc-two", &setup).await;
    create_checkpoint(&store, &namespace_id, &setup)
        .await
        .expect("advance past the shared basis");

    crate::test_support::NamespaceFixture::new(
        &store,
        &namespace_id,
        &crate::test_support::writer_at("test-writer", 0),
    )
    .engine()
    .delete_snapshot(&second.checkpoint_id)
    .await
    .expect("release one owner");
    let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
    let first_pass = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options(), &aged.clock())
        .await
        .expect("first gc pass");
    assert_eq!(
        first_pass.deleted_checkpoints_by_owner,
        loonfs_types::DeletedCheckpointsByOwner::default()
    );

    let second_pass = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options(), &aged.clock())
        .await
        .expect("second gc pass");
    assert_eq!(second_pass.deleted.manifests, 0);
    assert_eq!(
        second_pass.deleted_checkpoints_by_owner,
        loonfs_types::DeletedCheckpointsByOwner::default()
    );

    let keeper = crate::test_support::namespace_store(&store, &namespace_id)
        .load_pin(&first.checkpoint_id)
        .await
        .expect("read keeper record")
        .expect("the surviving owner's record stays")
        .state;
    assert!(
        crate::manifest::load_namespace_manifest_envelope(
            &store,
            &namespace_id,
            &keeper.pin_id.manifest_no(),
        )
        .await
        .is_ok(),
        "shared basis survives while any owner remains"
    );
}

#[tokio::test]
async fn gc_retains_active_checkpoint_bases() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap");
    write_test_file(&store, &namespace_id, "/docs/one.txt", "gc-one", &setup).await;
    let first = create_checkpoint(&store, &namespace_id, &setup)
        .await
        .expect("first checkpoint");
    write_test_file(&store, &namespace_id, "/docs/two.txt", "gc-two", &setup).await;
    create_checkpoint(&store, &namespace_id, &setup)
        .await
        .expect("second checkpoint");
    // The runtime that published the checkpoints' folds raises the hint.
    crate::test_support::namespace_store(&store, &namespace_id)
        .raise_hint_to_current_manifest()
        .await;

    let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
    let report = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options(), &aged.clock())
        .await
        .expect("gc pass");

    // Only the unpinned bootstrap manifest is collectable; both active
    // checkpoint bases stay.
    assert_eq!(report.deleted.manifests, 3);
    assert_eq!(
        report.deleted_checkpoints_by_owner,
        loonfs_types::DeletedCheckpointsByOwner::default()
    );
    let first_record = crate::test_support::namespace_store(&store, &namespace_id)
        .load_pin(&first.checkpoint_id)
        .await
        .expect("read first checkpoint")
        .expect("first checkpoint exists")
        .state;
    assert!(crate::manifest::load_namespace_manifest_envelope(
        &store,
        &namespace_id,
        &first_record.pin_id.manifest_no(),
    )
    .await
    .is_ok());
}

/// Reads the single fork-owned record a fork left under the source.
async fn read_fork_record(store: &LocalFsStore, source: &NamespaceId) -> PinPayload {
    for key in store
        .list_prefix(&pin_prefix(source))
        .await
        .expect("list checkpoints")
    {
        let bytes = store
            .get(&key, None)
            .await
            .expect("get record")
            .expect("record exists");
        let record = decode_control_object::<PinPayload>(&bytes, ControlObjectKind::Pin)
            .expect("decode record")
            .into_payload();
        if matches!(record.owner, PinOwner::Fork { .. }) {
            return record;
        }
    }
    panic!("fork leaves one fork-owned record");
}

#[tokio::test]
async fn retired_targets_release_their_source_pins_and_retry_failed_deletes() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let source = NamespaceId::parse("source").expect("namespace id");
    let clone = NamespaceId::parse("clone").expect("namespace id");
    let setup = context(1_000);
    crate::test_support::NamespaceFixture::new(&store, &source, &setup)
        .create()
        .await
        .expect("bootstrap");
    write_test_file(&store, &source, "/docs/one.txt", "gc-one", &setup).await;
    crate::test_support::NamespaceFixture::new(&store, &source, &setup)
        .engine()
        .fork_namespace(&clone, &loonfs_test_support::test_actor(), None)
        .await
        .expect("fork");
    let target_pin = create_checkpoint(&store, &clone, &setup)
        .await
        .expect("materialize target manifest");
    crate::test_support::namespace_store(&store, &clone)
        .delete_pin(&target_pin.checkpoint_id)
        .await
        .expect("release target pin");
    let fork_record = read_fork_record(&store, &source).await;
    // Advance the source manifest past the fork basis so the basis is
    // reachable only through the fork-owned record.
    write_test_file(&store, &source, "/docs/two.txt", "gc-two", &setup).await;
    create_checkpoint(&store, &source, &setup)
        .await
        .expect("advance manifest past the fork basis");
    // The runtime that published the checkpoint's fold raises the hint.
    crate::test_support::namespace_store(&store, &source)
        .raise_hint_to_current_manifest()
        .await;

    let before = context(now_after_newest_object(&store, &source, GRACE_MS + 1).await);
    let report = crate::test_support::namespace_store(&store, &source)
        .gc(None, &options(), &before.clock())
        .await
        .expect("gc with live target");
    assert_eq!(report.deleted_checkpoints_by_owner.fork, 0);

    crate::test_support::NamespaceFixture::new(&store, &clone, &setup)
        .engine()
        .delete_namespace(DeleteNamespaceOptions::default())
        .await
        .expect("terminal delete of the fork target");
    let aged = context(setup.now_ms + GRACE_MS - 1);

    let waiting = crate::test_support::namespace_store(&store, &source)
        .gc(None, &options(), &aged.clock())
        .await
        .expect("wait for retirement");
    assert_eq!(waiting.deleted_checkpoints_by_owner.fork, 0);
    let retired = crate::test_support::namespace_store(&store, &clone)
        .gc(None, &options(), &aged.clock())
        .await
        .expect("retire materialized target");
    let deadline = retired.reclaimable_at_ms.expect("target retired");
    let waiting = crate::test_support::namespace_store(&store, &source)
        .gc(None, &options(), &context(deadline - 1).clock())
        .await
        .expect("wait for grace");
    assert_eq!(waiting.deleted_checkpoints_by_owner.fork, 0);
    assert!(pin_exists(&store, &source, &fork_record.pin_id).await);
    let aged = context(deadline);

    let pin_key = loonfs_objectstore::keys::pin(&source, &fork_record.pin_id);
    let store = RecordingStore::new(
        FailStore::new(
            store,
            KeyPredicate::exact(&pin_key),
            OperationSelector::Delete,
            InjectedError::Transport("source pin delete failed".to_owned()),
        ),
        KeyPredicate::exact(&pin_key),
    );
    store.inner().fail_next(1);
    crate::test_support::namespace_store(&store, &clone)
        .gc(None, &options(), &aged.clock())
        .await
        .expect_err("a failed source pin delete should fail the pass");
    assert!(pin_exists(&store, &source, &fork_record.pin_id).await);
    let released = crate::test_support::namespace_store(&store, &clone)
        .gc(None, &options(), &aged.clock())
        .await
        .expect("retry source pin delete");
    assert_eq!(released.deleted_checkpoints_by_owner.fork, 1);
    assert!(!pin_exists(&store, &source, &fork_record.pin_id).await);
    let repeated = crate::test_support::namespace_store(&store, &clone)
        .gc(None, &options(), &aged.clock())
        .await
        .expect("repeat source pin delete");
    assert_eq!(repeated.deleted_checkpoints_by_owner.fork, 0);
    assert_eq!(store.counts().deletes, 3);
    assert!(store
        .head(&metadata_manifest_object(
            &source,
            &fork_record.pin_id.manifest_no()
        ))
        .await
        .expect("manifest")
        .is_some());
    let store = store.inner().inner();
    let aged = context(now_after_newest_object(store, &source, GRACE_MS + 1).await);
    let next_pass = crate::test_support::namespace_store(store, &source)
        .gc(None, &options(), &aged.clock())
        .await
        .expect("collect basis");
    assert_basis_reaped(
        store,
        &source,
        &next_pass,
        &fork_record.pin_id.manifest_no(),
    )
    .await;
    stat_root(store, &source).await;
}

#[tokio::test]
async fn a_corrupt_fork_target_manifest_fails_the_pass_and_an_unreadable_hint_retains_the_record() {
    let temp_dir = tempdir().expect("tempdir");
    let source = NamespaceId::parse("source").expect("namespace id");
    let clone = NamespaceId::parse("clone").expect("namespace id");
    let store = FailStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::exact(hint(&clone)),
        OperationSelector::Read,
        InjectedError::Transport("target hint timed out".to_owned()),
    );
    let setup = context(1_000);
    crate::test_support::NamespaceFixture::new(&store, &source, &setup)
        .create()
        .await
        .expect("bootstrap");
    write_test_file(&store, &source, "/docs/one.txt", "gc-one", &setup).await;
    crate::test_support::NamespaceFixture::new(&store, &source, &setup)
        .engine()
        .fork_namespace(&clone, &loonfs_test_support::test_actor(), None)
        .await
        .expect("fork");
    let fork_record = read_fork_record(store.inner(), &source).await;

    let aged = context(setup.now_ms + GRACE_MS);
    store.fail_all();
    let report = crate::test_support::namespace_store(&store, &source)
        .gc(None, &options(), &aged.clock())
        .await
        .expect("an unreadable target is retained conservatively");
    assert_eq!(report.deleted_checkpoints_by_owner.fork, 0);
    assert!(pin_exists(store.inner(), &source, &fork_record.pin_id).await);

    store.clear();
    replace_object(&store, &hint(&clone), Bytes::from_static(b"not json"))
        .await
        .expect("corrupt target manifest");
    let before = namespace_keys(store.inner(), &source).await;
    let error = crate::test_support::namespace_store(&store, &source)
        .gc(None, &options(), &aged.clock())
        .await
        .expect_err("a corrupt target manifest must fail the source pass");
    assert_eq!(error.code(), crate::error::ErrorCode::NamespaceCorrupt);
    assert!(error.message().contains(&hint(&clone)));
    assert_eq!(namespace_keys(store.inner(), &source).await, before);
}

#[tokio::test]
async fn gc_never_releases_a_fork_record_while_its_target_lives() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let source = NamespaceId::parse("source").expect("namespace id");
    let clone = NamespaceId::parse("clone").expect("namespace id");
    let setup = context(1_000);
    crate::test_support::NamespaceFixture::new(&store, &source, &setup)
        .create()
        .await
        .expect("bootstrap");
    write_test_file(&store, &source, "/docs/one.txt", "gc-one", &setup).await;
    crate::test_support::NamespaceFixture::new(&store, &source, &setup)
        .engine()
        .fork_namespace(&clone, &loonfs_test_support::test_actor(), None)
        .await
        .expect("fork");
    let fork_record = read_fork_record(&store, &source).await;
    assert!(
        fork_record.owner.expires_at_ms().is_none(),
        "fork pins carry no lease"
    );
    // Only the fork-owned record can protect the basis after this.
    write_test_file(&store, &source, "/docs/two.txt", "gc-two", &setup).await;
    create_checkpoint(&store, &source, &setup)
        .await
        .expect("advance manifest past the fork basis");

    let grace_deadline = fork_record.created_at_ms + GRACE_MS;
    for now_ms in [
        now_after_newest_object(&store, &source, GRACE_MS + 1).await,
        grace_deadline,
        grace_deadline + GRACE_MS,
        u64::MAX / 2,
    ] {
        let report = crate::test_support::namespace_store(&store, &source)
            .gc(None, &options(), &context(now_ms).clock())
            .await
            .expect("gc pass with a live target");
        assert_eq!(report.deleted_checkpoints_by_owner.fork, 0, "at {now_ms}");
        assert_eq!(report.deleted_checkpoints_by_owner.user, 0, "at {now_ms}");
        assert!(
            pin_exists(&store, &source, &fork_record.pin_id).await,
            "a live target keeps its pin at {now_ms}"
        );
    }
    assert!(crate::manifest::load_namespace_manifest_envelope(
        &store,
        &source,
        &fork_record.pin_id.manifest_no(),
    )
    .await
    .is_ok());
    crate::test_support::namespace_store(&store, &clone)
        .load_current_metadata_view()
        .await
        .expect("target readable after every pass")
        .resolve_path(
            "/docs/one.txt",
            AttributeInclusion::Omit,
            &crate::authorize::ReadAccess::live(crate::authorize::Authorizer::Unrestricted),
        )
        .await
        .expect("forked file readable");
}

#[tokio::test]
async fn a_fork_retry_keeps_young_pins_and_reclaims_the_abandoned_one_after_grace() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let source = NamespaceId::parse("source").expect("namespace id");
    let clone = NamespaceId::parse("clone").expect("namespace id");
    let setup = context(1_000);
    crate::test_support::NamespaceFixture::new(&store, &source, &setup)
        .create()
        .await
        .expect("bootstrap");
    write_test_file(&store, &source, "/docs/one.txt", "gc-one", &setup).await;
    let abandoned = crate::test_support::namespace_store(&store, &source)
        .create_pin(
            PinOwner::Fork {
                target_namespace_id: clone.clone(),
            },
            &setup.clock(),
        )
        .await
        .map(|report| crate::pin::checkpoint_summary(report.response))
        .expect("fork pin from the abandoned attempt");

    crate::test_support::NamespaceFixture::new(&store, &source, &setup)
        .engine()
        .fork_namespace(&clone, &loonfs_test_support::test_actor(), None)
        .await
        .expect("fork retry after abandonment");
    let retry = store
        .list_prefix(&pin_prefix(&source))
        .await
        .expect("list checkpoints")
        .len();
    assert_eq!(retry, 2, "the retry pins for itself instead of reusing");
    assert!(
        pin_exists(&store, &source, &abandoned.checkpoint_id).await,
        "the retry leaves the abandoned record alone"
    );

    let before_creation_grace = context(setup.now_ms + 1);
    let report = crate::test_support::namespace_store(&store, &source)
        .gc(None, &options(), &before_creation_grace.clock())
        .await
        .expect("gc pass with a target that reads through another record");
    assert_eq!(report.deleted_checkpoints_by_owner.fork, 0);
    assert!(pin_exists(&store, &source, &abandoned.checkpoint_id).await);
    let report = crate::test_support::namespace_store(&store, &source)
        .gc(None, &options(), &context(setup.now_ms + GRACE_MS).clock())
        .await
        .expect("an aged pin the target does not name is the abandoned attempt's");
    assert_eq!(report.deleted_checkpoints_by_owner.fork, 1);
    assert!(!pin_exists(&store, &source, &abandoned.checkpoint_id).await);
    assert_eq!(
        store
            .list_prefix(&pin_prefix(&source))
            .await
            .expect("list checkpoints")
            .len(),
        1,
        "the retry's own pin stays"
    );
    crate::test_support::namespace_store(&store, &clone)
        .load_current_metadata_view()
        .await
        .expect("target readable after retry and collection")
        .resolve_path(
            "/docs/one.txt",
            AttributeInclusion::Omit,
            &crate::authorize::ReadAccess::live(crate::authorize::Authorizer::Unrestricted),
        )
        .await
        .expect("forked file readable");
}

#[tokio::test]
async fn a_corrupt_pin_and_an_unreadable_one_both_fail_the_pass() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let store = FailStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::prefix(pin_prefix(&namespace_id)),
        OperationSelector::Read,
        InjectedError::Transport("pin read timed out".to_owned()),
    );
    let setup = context(1_000);
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap");
    write_test_file(&store, &namespace_id, "/docs/one.txt", "gc-one", &setup).await;
    create_checkpoint(&store, &namespace_id, &setup)
        .await
        .expect("checkpoint");

    let record_key = store
        .inner()
        .list_prefix(&pin_prefix(&namespace_id))
        .await
        .expect("list checkpoints")
        .first()
        .expect("the checkpoint wrote a record")
        .clone();
    let before = namespace_keys(store.inner(), &namespace_id).await;
    let aged = context(setup.now_ms);

    store.fail_all();
    let error = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options(), &aged.clock())
        .await
        .expect_err("a record the store will not read fails the pass");
    assert_eq!(error.code(), crate::error::ErrorCode::ServerError);
    assert!(
        error.message().contains(&record_key),
        "the error names the object: {error}"
    );
    assert_eq!(
        namespace_keys(store.inner(), &namespace_id).await,
        before,
        "a failed pass deletes nothing"
    );

    store.clear();
    replace_object(&store, &record_key, Bytes::from_static(b"not json"))
        .await
        .expect("corrupt record");
    let aged = context(setup.now_ms);
    let error = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options(), &aged.clock())
        .await
        .expect_err("a corrupt record fails the pass");
    assert_eq!(error.code(), crate::error::ErrorCode::NamespaceCorrupt);
    assert!(
        error.message().contains(&record_key),
        "the error names the object: {error}"
    );
    assert_eq!(
        namespace_keys(store.inner(), &namespace_id).await,
        before,
        "a failed pass deletes nothing"
    );
}

#[tokio::test]
async fn a_corrupt_or_unreadable_current_manifest_fails_the_pass() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let store = FailStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::prefix(metadata_manifest_prefix(&namespace_id)),
        OperationSelector::Read,
        InjectedError::Transport("manifest read timed out".to_owned()),
    );
    let setup = context(1_000);
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap");
    write_test_file(&store, &namespace_id, "/docs/one.txt", "gc-one", &setup).await;
    create_checkpoint(&store, &namespace_id, &setup)
        .await
        .expect("checkpoint");

    let manifest_keys = store
        .inner()
        .list_prefix(&metadata_manifest_prefix(&namespace_id))
        .await
        .expect("list manifests");
    assert!(
        !manifest_keys.is_empty(),
        "the checkpoint published a manifest"
    );
    let before = namespace_keys(store.inner(), &namespace_id).await;
    let aged = context(now_after_newest_object(store.inner(), &namespace_id, GRACE_MS + 1).await);

    store.fail_all();
    let error = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options(), &aged.clock())
        .await
        .expect_err("discovery read fails closed");
    assert_eq!(error.code(), crate::error::ErrorCode::ServerError);
    assert_eq!(
        namespace_keys(store.inner(), &namespace_id).await,
        before,
        "a degraded pass reclaims nothing in the affected families"
    );

    store.clear();
    for key in &manifest_keys {
        replace_object(&store, key, Bytes::from_static(b"not json"))
            .await
            .expect("corrupt manifest");
    }
    let aged = context(now_after_newest_object(store.inner(), &namespace_id, GRACE_MS + 1).await);
    let error = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options(), &aged.clock())
        .await
        .expect_err("a corrupt manifest fails the pass");
    assert_eq!(error.code(), crate::error::ErrorCode::NamespaceCorrupt);
    assert!(
        manifest_keys
            .iter()
            .any(|key| error.message().contains(key)),
        "the error names the object: {error}"
    );
    assert_eq!(
        namespace_keys(store.inner(), &namespace_id).await,
        before,
        "a failed pass deletes nothing"
    );
}

#[tokio::test]
async fn gc_retains_everything_without_provider_timestamps() {
    let temp_dir = tempdir().expect("tempdir");
    // Rule 1 treats missing provider timestamps as young, so nothing ages out.
    let store = MetadataMapStore::without_last_modified(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::any(),
    );
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap");
    write_test_file(&store, &namespace_id, "/docs/one.txt", "gc-one", &setup).await;
    create_checkpoint(&store, &namespace_id, &setup)
        .await
        .expect("checkpoint");
    crate::test_support::NamespaceFixture::new(
        &store,
        &namespace_id,
        &crate::test_support::writer_at(
            "test-writer",
            crate::test_support::grace_elapsed_after(setup.now_ms),
        ),
    )
    .engine()
    .advance_retention_floor(RetentionTarget::Head)
    .await
    .expect("advance floor");

    // Far past any window by wall clock, but no object carries a
    // provider timestamp.
    let aged = context(now_after_newest_object(store.inner(), &namespace_id, GRACE_MS + 1).await);
    let report = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options(), &aged.clock())
        .await
        .expect("gc pass");

    assert_eq!(report.deleted.wal_objects, 0);
    assert_eq!(report.deleted.metadata_segments, 0);
    assert_eq!(report.deleted.manifests, 0);
    assert_eq!(
        report.deleted_checkpoints_by_owner,
        loonfs_types::DeletedCheckpointsByOwner::default()
    );
    assert_eq!(report.deleted_checkpoints_by_owner.fork, 0);
    assert!(report.retained.total() > 0);
    stat_root(&store, &namespace_id).await;
}

#[tokio::test]
async fn gc_of_an_absent_namespace_reads_the_hint_and_reports_it_missing() {
    let temp_dir = tempdir().expect("tempdir");
    let inner = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("orphan").expect("namespace id");
    let store = RecordingStore::new(inner, KeyPredicate::any());

    let error = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options(), &context(u64::MAX).clock())
        .await
        .expect_err("gc absent namespace");
    assert_eq!(error.code(), crate::error::ErrorCode::NamespaceNotFound);
    assert_eq!(store.counts().lists, 0);
    assert_eq!(store.counts().deletes, 0);
}

async fn namespace_keys(store: &LocalFsStore, namespace_id: &NamespaceId) -> BTreeSet<String> {
    store
        .list_prefix(&loonfs_objectstore::keys::namespace_prefix(namespace_id))
        .await
        .expect("list namespace")
        .into_iter()
        .filter(|key| !key.starts_with(&format!("namespaces/{namespace_id}/gc/")))
        .collect()
}

#[tokio::test]
async fn provider_age_reserves_the_clock_margin_before_deletion() {
    use crate::limits::GC_SAFETY_MARGIN_MS;

    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("clock-margin").expect("namespace");
    let inner = LocalFsStore::new(directory.path()).expect("store");
    crate::test_support::NamespaceFixture::new(&inner, &namespace_id, &context(1_000))
        .create()
        .await
        .expect("namespace");
    let key =
        loonfs_objectstore::keys::content_blob(&namespace_id, &loonfs_types::ContentId::generate());
    let provider_stamp = 1_000_000;
    let store = RecordingStore::new(
        MetadataMapStore::new(inner, KeyPredicate::exact(&key), move |mut metadata| {
            metadata.last_modified_at_ms = Some(provider_stamp);
            metadata
        }),
        KeyPredicate::exact(&key),
    );
    store
        .put_if_absent(&key, Bytes::from_static(b"orphan"))
        .await
        .expect("orphan");
    store.reset();
    let options = GcOptions {
        grace_window_ms: GC_MIN_GRACE_WINDOW_MS,
        ..Default::default()
    };
    let publication_bound = GC_MIN_GRACE_WINDOW_MS - GC_SAFETY_MARGIN_MS;
    let now_ms = provider_stamp + publication_bound - 1 + GC_SAFETY_MARGIN_MS;
    let retained = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options, &context(now_ms).clock())
        .await
        .expect("pass before cutoff");
    assert_eq!(retained.deleted.content_objects, 0);
    assert_eq!(retained.retained.within_grace_window, 1);
    assert_eq!(store.counts().deletes, 0);
    let collected = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options, &context(now_ms + 1).clock())
        .await
        .expect("pass at cutoff");
    assert_eq!(collected.deleted.content_objects, 1);
    assert_eq!(store.counts().deletes, 1);
    assert_eq!(store.counts().heads, 0);
}

async fn retired_content_namespace<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
) -> (WriterAt, Vec<String>) {
    let setup = context(1_000);
    crate::test_support::NamespaceFixture::new(store, namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap");
    let keys = publish_owned_content(store, namespace_id, 3).await;
    crate::test_support::NamespaceFixture::new(store, namespace_id, &setup)
        .engine()
        .delete_namespace(Default::default())
        .await
        .expect("delete");
    let report = crate::test_support::namespace_store(store, namespace_id)
        .gc(None, &options(), &setup.clock())
        .await
        .expect("retire");
    (context(report.reclaimable_at_ms.expect("deadline")), keys)
}

async fn publish_owned_content<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    count: usize,
) -> Vec<String> {
    let mut keys = Vec::new();
    for number in 0..count {
        let content_ref =
            put_file_content(store, namespace_id, &format!("/file-{number}"), b"content").await;
        keys.push(loonfs_objectstore::keys::content_blob(
            namespace_id,
            &content_ref.content_id,
        ));
        if (number + 1) % 32 == 0 {
            crate::test_support::namespace_store(store, namespace_id)
                .fold_wal()
                .await
                .expect("fold published content");
        }
    }
    keys
}

#[tokio::test]
async fn completed_upload_waits_for_namespace_retirement_then_reclaims() {
    for first_run_ms in [1_000, GRACE_MS] {
        let directory = tempdir().expect("tempdir");
        let store = LocalFsStore::new(directory.path()).expect("store");
        let namespace_id = NamespaceId::parse("completed-retired").expect("namespace");
        let setup = context(1_000);
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
            .create()
            .await
            .expect("bootstrap");
        let (upload_id, content) =
            complete_upload_for_gc(&store, &namespace_id, b"content", &setup).await;
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
            .engine()
            .delete_namespace(Default::default())
            .await
            .expect("delete");
        let listed_bytes = core_listed_bytes(&store, &namespace_id).await;
        let report = crate::test_support::namespace_store(&store, &namespace_id)
            .gc(None, &options(), &context(first_run_ms).clock())
            .await
            .expect("unretired run");
        assert_eq!(report.listed_bytes, listed_bytes);
        assert_eq!(report.deleted.upload_sessions, 0);
        assert_eq!(
            report.retained.upload_session_undecided + report.retained.upload_session_window,
            1
        );
        let key = loonfs_objectstore::keys::content_blob(&namespace_id, &content.content_id);
        assert!(store.head(&key).await.expect("object").is_some());
        let listed_bytes = core_listed_bytes(&store, &namespace_id).await;
        let report = crate::test_support::namespace_store(&store, &namespace_id)
            .gc(
                None,
                &options(),
                &context(report.reclaimable_at_ms.expect("retired")).clock(),
            )
            .await
            .expect("retired run");
        assert_eq!(report.listed_bytes, listed_bytes);
        assert_eq!(report.deleted.upload_sessions, 1);
        assert_eq!(report.deleted.retired_content_objects, 1);
        assert!(store.head(&key).await.expect("object").is_none());
        assert!(store
            .head(&loonfs_objectstore::keys::upload_session(
                &namespace_id,
                &upload_id
            ))
            .await
            .expect("session")
            .is_none());
    }
}

#[tokio::test]
async fn gc_keeps_pinned_and_current_numbers_and_preserves_discovery_from_a_lagging_hint() {
    use loonfs_types::format::control::{encode_control_state, ControlObjectKind, HintPayload};
    let directory = tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let setup = context(1_000);
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap");
    write_test_file(&store, &namespace_id, "/one", "one", &setup).await;
    let pinned = create_checkpoint(&store, &namespace_id, &setup)
        .await
        .expect("pin");
    for index in 2..=3 {
        write_test_file(
            &store,
            &namespace_id,
            &format!("/file-{index}"),
            &format!("write-{index}"),
            &setup,
        )
        .await;
        crate::test_support::namespace_store(&store, &namespace_id)
            .fold_wal()
            .await
            .expect("fold");
    }
    let current = crate::test_support::namespace_store(&store, &namespace_id)
        .load_current_manifest()
        .await
        .expect("current");
    let bytes = encode_control_state(
        ControlObjectKind::Hint,
        &HintPayload {
            namespace_id: namespace_id.clone(),
            manifest_no: ManifestNo(1),
        },
    )
    .expect("hint");
    replace_object(
        &store,
        &loonfs_objectstore::keys::hint(&namespace_id),
        Bytes::from(bytes),
    )
    .await
    .expect("rewind hint");
    let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
    let report = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options(), &aged.clock())
        .await
        .expect("collect");
    assert_eq!(report.deleted.manifests, 0);
    assert_eq!(
        crate::test_support::namespace_store(&store, &namespace_id)
            .load_current_manifest()
            .await
            .expect("discovery after deferred sweep"),
        current
    );
    write_test_file(&store, &namespace_id, "/four", "four", &setup).await;
    crate::test_support::namespace_store(&store, &namespace_id)
        .fold_wal()
        .await
        .expect("fold");
    // The runtime that published the fold raises the hint past the rewind.
    crate::test_support::namespace_store(&store, &namespace_id)
        .raise_hint_to_current_manifest()
        .await;
    let current = crate::test_support::namespace_store(&store, &namespace_id)
        .load_current_manifest()
        .await
        .expect("current");
    let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
    let report = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options(), &aged.clock())
        .await
        .expect("collect after publication");
    assert_eq!(report.deleted.manifests, 7);
    assert_eq!(
        store
            .list_prefix(&metadata_manifest_prefix(&namespace_id))
            .await
            .expect("list for assertion"),
        vec![
            metadata_manifest_object(&namespace_id, &pinned.manifest_no),
            metadata_manifest_object(&namespace_id, &current.manifest().manifest_no),
        ]
    );
    assert_eq!(
        crate::test_support::namespace_store(&store, &namespace_id)
            .load_current_manifest()
            .await
            .expect("discovery after sweep"),
        current
    );
}

#[tokio::test]
async fn concurrent_collectors_keep_pinned_and_current_roots_and_young_objects() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("concurrent").expect("namespace");
    let inner = LocalFsStore::new(directory.path()).expect("store");
    let setup = context(1_000);
    crate::test_support::NamespaceFixture::new(&inner, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap");
    write_test_file(&inner, &namespace_id, "/one", "one", &setup).await;
    let pin = create_checkpoint(&inner, &namespace_id, &setup)
        .await
        .expect("pin");
    write_test_file(&inner, &namespace_id, "/two", "two", &setup).await;
    crate::test_support::namespace_store(&inner, &namespace_id)
        .fold_wal()
        .await
        .expect("fold");
    let protected: BTreeSet<_> = inner
        .list_prefix(&metadata_segment_prefix(&namespace_id))
        .await
        .expect("segments")
        .into_iter()
        .collect();
    let old = metadata_segment(
        &namespace_id,
        &loonfs_types::MetadataSegmentId::parse("seg_00000000000000000000000001").expect("segment"),
    );
    let young = metadata_segment(
        &namespace_id,
        &loonfs_types::MetadataSegmentId::parse("seg_00000000000000000000000002").expect("segment"),
    );
    for key in [&old, &young] {
        inner
            .put_if_absent(key, Bytes::from_static(b"unused"))
            .await
            .expect("orphan");
    }
    let store = BlockingStore::new(
        MetadataMapStore::aged(inner, KeyPredicate::exact(&old)),
        KeyPredicate::exact(&old),
        OperationSelector::Delete,
    );
    store.block_next();
    let clock = context(UNREFERENCED_SEGMENT_GRACE_WINDOW_MS + 1).clock();
    let gc_options = options();
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let (first, second) = tokio::join!(namespace.gc(None, &gc_options, &clock), async {
        store.wait_until_blocked().await;
        let result = crate::test_support::namespace_store(store.inner(), &namespace_id)
            .gc(None, &gc_options, &clock)
            .await;
        store.release();
        result
    });
    assert_eq!(first.expect("first collector").deleted.metadata_segments, 1);
    assert_eq!(
        second.expect("second collector").deleted.metadata_segments,
        1
    );
    assert!(store.head(&old).await.expect("old segment").is_none());
    assert!(store.head(&young).await.expect("young segment").is_some());
    for key in protected {
        assert!(store.head(&key).await.expect("protected segment").is_some());
    }
    assert!(store
        .head(&metadata_manifest_object(&namespace_id, &pin.manifest_no))
        .await
        .expect("pin manifest")
        .is_some());
    stat_root(&store, &namespace_id).await;
}

#[tokio::test]
async fn unreferenced_segments_are_deleted_concurrently_within_the_write_wave() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("delete-wave").expect("namespace");
    let inner = LocalFsStore::new(directory.path()).expect("store");
    let setup = context(1_000);
    crate::test_support::NamespaceFixture::new(&inner, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap");
    write_test_file(&inner, &namespace_id, "/one", "one", &setup).await;
    crate::test_support::namespace_store(&inner, &namespace_id)
        .fold_wal()
        .await
        .expect("fold");
    let protected = inner
        .list_prefix(&metadata_segment_prefix(&namespace_id))
        .await
        .expect("segments");
    let orphans: BTreeSet<_> = (1..=2 * STORE_WRITES_IN_FLIGHT + 1)
        .map(|number| {
            metadata_segment(
                &namespace_id,
                &loonfs_types::MetadataSegmentId::parse(format!("seg_{number:026}"))
                    .expect("segment"),
            )
        })
        .collect();
    for key in &orphans {
        inner
            .put_if_absent(key, Bytes::from_static(b"unused"))
            .await
            .expect("orphan");
    }
    let watched = KeyPredicate::new({
        let orphans = orphans.clone();
        move |key| orphans.contains(key)
    });
    let store = ConcurrencyWatchStore::new(MetadataMapStore::aged(inner, watched.clone()), watched);
    let report = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(
            None,
            &options(),
            &context(UNREFERENCED_SEGMENT_GRACE_WINDOW_MS + 1).clock(),
        )
        .await
        .expect("gc pass");
    let deletes = store.deletes();
    assert!(deletes.peak_in_flight > 1);
    assert!(deletes.peak_in_flight <= STORE_WRITES_IN_FLIGHT);
    assert_eq!(deletes.total, orphans.len());
    assert_eq!(report.deleted.metadata_segments, orphans.len() as u64);
    for key in &orphans {
        assert!(store.head(key).await.expect("orphan").is_none());
    }
    for key in &protected {
        assert!(store.head(key).await.expect("protected segment").is_some());
    }
}

#[tokio::test]
async fn retired_owner_calls_restart_retry_deletes_and_collect_late_writes() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("owner-sweep").expect("namespace");
    let inner = MetadataMapStore::aged(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    let (deadline, keys) = retired_content_namespace(&inner, &namespace_id).await;
    let store = RecordingStore::new(
        FailStore::new(
            inner,
            KeyPredicate::exact(&keys[0]),
            OperationSelector::Delete,
            InjectedError::Transport("delete failed".to_owned()),
        ),
        KeyPredicate::any(),
    );
    let before = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options(), &context(deadline.now_ms - 1).clock())
        .await
        .expect("before deadline");
    assert_eq!(before.deleted.retired_content_objects, 0);
    assert_eq!(before.next_reclamation_at_ms, Some(deadline.now_ms));
    crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options(), &context(deadline.now_ms - 1).clock())
        .await
        .expect("collect older manifests");
    store.inner().fail_next(1);
    match crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options(), &deadline.clock())
        .await
    {
        Err(CoreError::Store { object_key, .. }) => assert_eq!(object_key, keys[0]),
        other => panic!("expected the failed delete's store error, got {other:?}"),
    }
    assert!(store.head(&keys[0]).await.expect("failed delete").is_some());
    let content_prefix = format!("namespaces/{namespace_id}/content/");
    let remaining = store
        .list_prefix(&content_prefix)
        .await
        .expect("remaining content");
    let retried = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options(), &deadline.clock())
        .await
        .expect("retry owner sweep");
    assert_eq!(
        retried.deleted.retired_content_objects,
        remaining.len() as u64
    );
    assert!(store
        .list_prefix(&content_prefix)
        .await
        .expect("content after the retry")
        .is_empty());
    store
        .put_if_absent(&keys[0], Bytes::from_static(b"late write"))
        .await
        .expect("late content");
    let late = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options(), &deadline.clock())
        .await
        .expect("collect earlier key");
    assert_eq!(late.deleted.retired_content_objects, 1);
    assert!(store
        .head(&hint(&namespace_id))
        .await
        .expect("tombstone")
        .is_some());
}

#[tokio::test]
async fn expiry_and_creation_grace_delete_pins_without_a_released_state() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("pin-grace").expect("namespace");
    let target = NamespaceId::parse("absent").expect("target");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let setup = context(1_000);
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap");
    let mut pins = Vec::new();
    for owner in [
        PinOwner::User {
            name: "permanent".to_owned(),
            expires_at_ms: None,
        },
        PinOwner::User {
            name: "expiring".to_owned(),
            expires_at_ms: Some(2_000),
        },
        PinOwner::User {
            name: "snapshot".to_owned(),
            expires_at_ms: Some(2_000),
        },
        PinOwner::Fork {
            target_namespace_id: target.clone(),
        },
    ] {
        pins.push(
            crate::test_support::namespace_store(&store, &namespace_id)
                .create_pin(owner, &setup.clock())
                .await
                .map(|report| crate::pin::checkpoint_summary(report.response))
                .expect("pin"),
        );
    }
    let before = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options(), &context(1_000 + GRACE_MS - 1).clock())
        .await
        .expect("before creation grace");
    assert_eq!(
        before.deleted_checkpoints_by_owner,
        loonfs_types::DeletedCheckpointsByOwner::default()
    );
    let abandoned = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options(), &context(1_000 + GRACE_MS).clock())
        .await
        .expect("creation grace");
    assert_eq!(abandoned.deleted_checkpoints_by_owner.fork, 1);
    assert!(!pin_exists(&store, &namespace_id, &pins[3].checkpoint_id).await);
    assert!(namespace_keys(&store, &target).await.is_empty());
    let before_expiry = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options(), &context(2_000 + GRACE_MS - 1).clock())
        .await
        .expect("before expiry grace");
    assert_eq!(
        before_expiry.deleted_checkpoints_by_owner,
        loonfs_types::DeletedCheckpointsByOwner::default()
    );
    let expired = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options(), &context(2_000 + GRACE_MS).clock())
        .await
        .expect("expiry grace");
    assert_eq!(expired.deleted_checkpoints_by_owner.user, 2);
    assert!(pin_exists(&store, &namespace_id, &pins[0].checkpoint_id).await);
    assert!(!pin_exists(&store, &namespace_id, &pins[1].checkpoint_id).await);
    assert!(!pin_exists(&store, &namespace_id, &pins[2].checkpoint_id).await);
    create_checkpoint(&store, &namespace_id, &setup)
        .await
        .expect("second permanent pin");
    let deleted_at = 3_000 + GRACE_MS;
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context(deleted_at))
        .engine()
        .delete_namespace(Default::default())
        .await
        .expect("delete");
    let retired = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options(), &context(deleted_at).clock())
        .await
        .expect("retire");
    assert_eq!(
        retired.deleted_checkpoints_by_owner,
        loonfs_types::DeletedCheckpointsByOwner {
            user: 2,
            ..Default::default()
        }
    );
    assert_eq!(
        retired.reclaimable_at_ms,
        Some(deleted_at + retirement_ms())
    );
}

#[tokio::test]
async fn a_pin_naming_an_absent_manifest_is_corruption_before_sweeping() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("missing-manifest").expect("namespace");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    let setup = context(1_000);
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap");
    let initial = crate::test_support::namespace_store(&store, &namespace_id)
        .create_pin(
            PinOwner::User {
                name: "initial".to_owned(),
                expires_at_ms: None,
            },
            &setup.clock(),
        )
        .await
        .map(|report| crate::pin::checkpoint_summary(report.response))
        .expect("pin");
    write_test_file(&store, &namespace_id, "/file", "new", &setup).await;
    crate::test_support::namespace_store(&store, &namespace_id)
        .fold_wal()
        .await
        .expect("fold");
    // The runtime that published the fold raises the hint, so discovery
    // starts above the deleted manifest.
    crate::test_support::namespace_store(&store, &namespace_id)
        .raise_hint_to_current_manifest()
        .await;
    store
        .delete(&metadata_manifest_object(
            &namespace_id,
            &initial.manifest_no,
        ))
        .await
        .expect("delete manifest");
    store.reset();
    let error = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options(), &context(GRACE_MS * 3).clock())
        .await
        .expect_err("missing pin manifest");
    assert_eq!(error.code(), loonfs_types::ErrorCode::NamespaceCorrupt);
    assert!(error.to_string().contains(&loonfs_objectstore::keys::pin(
        &namespace_id,
        &initial.checkpoint_id
    )));
    assert_eq!(store.counts().puts, 0);
    assert_eq!(store.counts().deletes, 0);
}

#[tokio::test]
async fn fork_pin_grace_skips_targets_and_aged_pins_read_only_manifest_discovery() {
    let directory = tempdir().expect("directory");
    let source = NamespaceId::parse("source").expect("source");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::prefix("namespaces/target-"),
    );
    let setup = context(1_000);
    crate::test_support::NamespaceFixture::new(&store, &source, &setup)
        .create()
        .await
        .expect("bootstrap");
    let mut targets = Vec::new();
    for number in 0..6 {
        let target = NamespaceId::parse(format!("target-{number}")).expect("target");
        crate::test_support::NamespaceFixture::new(&store, &source, &setup)
            .engine()
            .fork_namespace(&target, &loonfs_test_support::test_actor(), None)
            .await
            .expect("fork");
        targets.push(target);
    }
    write_test_file(&store, &targets[0], "/own.txt", "target-write", &setup).await;
    store
        .inner()
        .delete(&hint(&targets[5]))
        .await
        .expect("absent target hint");
    store.reset();
    let young = crate::test_support::namespace_store(&store, &source)
        .gc(
            None,
            &options(),
            &context(setup.now_ms + GRACE_MS - 1).clock(),
        )
        .await
        .expect("young pins");
    assert_eq!(young.deleted_checkpoints_by_owner.fork, 0);
    assert!(store.snapshot().is_empty());
    let aged = crate::test_support::namespace_store(&store, &source)
        .gc(None, &options(), &context(setup.now_ms + GRACE_MS).clock())
        .await
        .expect("aged pins");
    assert_eq!(aged.deleted_checkpoints_by_owner.fork, 1);
    let counts = store.counts();
    assert_eq!(counts.lists, 0);
    assert_eq!(counts.puts, 0);
    assert_eq!(counts.compare_and_swaps, 0);
    assert_eq!(counts.deletes, 0);
    assert!(store
        .snapshot()
        .iter()
        .all(|operation| !operation.key().contains("/wal/")));
    assert_eq!(
        store
            .inner()
            .list_prefix(&pin_prefix(&source))
            .await
            .expect("source pins")
            .len(),
        5
    );
}

#[tokio::test]
async fn create_on_a_deleted_id_fails_before_and_after_content_reclamation() {
    let directory = tempdir().expect("directory");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    let namespace_id = NamespaceId::parse("terminal").expect("namespace");
    let (deadline, keys) = retired_content_namespace(&store, &namespace_id).await;
    for reclaimed in [false, true] {
        for allow_existing in [false, true] {
            store.reset();
            let error =
                crate::test_support::NamespaceFixture::new(&store, &namespace_id, &deadline)
                    .engine()
                    .bootstrap_namespace(
                        &loonfs_test_support::test_actor(),
                        &crate::options::CreateNamespaceOptions {
                            allow_existing,
                            ..Default::default()
                        },
                    )
                    .await
                    .expect_err("deleted id");
            assert_eq!(error.code(), loonfs_types::ErrorCode::NamespaceDeleted);
            assert_eq!(store.counts().puts, 0);
            assert_eq!(store.counts().compare_and_swaps, 0);
            assert_eq!(store.counts().deletes, 0);
        }
        for key in &keys {
            assert_eq!(store.head(key).await.expect("content").is_none(), reclaimed);
        }
        if !reclaimed {
            let report = crate::test_support::namespace_store(&store, &namespace_id)
                .gc(None, &options(), &deadline.clock())
                .await
                .expect("reclaim content");
            assert_eq!(report.deleted.retired_content_objects, keys.len() as u64);
        }
    }
}

fn root_scan_cache(
    memory: Arc<crate::cache::ReadWorkingMemory>,
) -> crate::cache::MetadataSegmentCache {
    crate::cache::MetadataSegmentCache::new(
        Arc::new(crate::cache::SharedSegmentBlocks::new(0, 0)),
        crate::cache::ObjectStoreScope::new(0),
        memory,
        crate::limits::READAHEAD_WINDOW_BYTES,
    )
}

async fn folded_namespace_with_two_files(store: &LocalFsStore, namespace_id: &NamespaceId) {
    let setup = context(1_000);
    crate::test_support::NamespaceFixture::new(store, namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap");
    write_test_file(store, namespace_id, "/docs/one.txt", "gc-one", &setup).await;
    write_test_file(store, namespace_id, "/docs/two.txt", "gc-two", &setup).await;
    create_checkpoint(store, namespace_id, &setup)
        .await
        .expect("checkpoint");
}

#[tokio::test]
async fn a_pass_reads_its_roots_through_the_given_cache_and_releases_their_memory() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Peak(AtomicUsize);
    impl crate::cache::ReadWorkingMemoryObserver for Peak {
        fn in_use(&self, bytes: usize) {
            self.0.fetch_max(bytes, Ordering::SeqCst);
        }
        fn reservation_failed(&self) {}
    }

    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    folded_namespace_with_two_files(&store, &namespace_id).await;

    let peak = Arc::new(Peak(AtomicUsize::new(0)));
    let observer: Arc<dyn crate::cache::ReadWorkingMemoryObserver> = peak.clone();
    let memory = Arc::new(crate::cache::ReadWorkingMemory::new(
        64 * 1024 * 1024,
        Some(observer),
    ));
    let cache = Arc::new(root_scan_cache(Arc::clone(&memory)));
    let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
    let report = crate::NamespaceStore {
        segment_cache: Arc::clone(&cache),
        ..crate::test_support::namespace_store(&store, &namespace_id)
    }
    .gc(None, &options(), &aged.clock())
    .await
    .expect("gc pass");

    assert_eq!(report.deleted.content_objects, 0);
    assert!(
        peak.0.load(Ordering::SeqCst) > 0,
        "the roots were read and held through the shared memory"
    );
    assert_eq!(memory.in_use(), 0, "the pass released what it held");
}

#[tokio::test]
async fn a_pass_fails_when_its_pinned_manifests_do_not_fit_the_read_memory() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    folded_namespace_with_two_files(&store, &namespace_id).await;
    let head = crate::test_support::namespace_store(&store, &namespace_id)
        .load_current_manifest()
        .await
        .expect("current manifest");
    let budget = 8 * crate::heap_bytes::manifest_heap_bytes(&head.envelope);
    let memory = Arc::new(crate::cache::ReadWorkingMemory::new(budget, None));
    let cache = Arc::new(root_scan_cache(Arc::clone(&memory)));
    let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
    crate::NamespaceStore {
        segment_cache: Arc::clone(&cache),
        ..crate::test_support::namespace_store(&store, &namespace_id)
    }
    .gc(None, &options(), &aged.clock())
    .await
    .expect("one pinned manifest fits the budget");

    let setup = context(1_000);
    for index in 0..9 {
        let (path, commit_id) = (
            format!("/docs/pinned-{index}.txt"),
            format!("gc-pin-{index}"),
        );
        write_test_file(&store, &namespace_id, &path, &commit_id, &setup).await;
        create_checkpoint(&store, &namespace_id, &setup)
            .await
            .expect("checkpoint");
    }
    let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
    let error = crate::NamespaceStore {
        segment_cache: Arc::clone(&cache),
        ..crate::test_support::namespace_store(&store, &namespace_id)
    }
    .gc(None, &options(), &aged.clock())
    .await
    .expect_err("ten pinned manifests do not fit the budget");

    let CoreError::LiveSetExceedsReadMemory { limit, .. } = error else {
        panic!("expected LiveSetExceedsReadMemory, got {error:?}");
    };
    assert_eq!(limit, budget);
    assert_eq!(memory.in_use(), 0, "the failed pass released what it held");
    let content_prefix = loonfs_objectstore::keys::content_prefix(&namespace_id);
    let mut listing = store.list_prefix_stream(&content_prefix);
    let mut content_objects = 0;
    while let Some(key) = listing.next().await {
        key.expect("listed key");
        content_objects += 1;
    }
    assert_eq!(content_objects, 11, "the failed pass deleted nothing");
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
    crate::test_support::NamespaceFixture::new(store.inner(), &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap");
    for _ in 0..=STORE_READS_IN_FLIGHT {
        complete_upload_for_gc(store.inner(), &namespace_id, b"unpublished\n", &setup).await;
    }

    let memory = Arc::new(crate::cache::ReadWorkingMemory::new(1, None));
    let cache = Arc::new(root_scan_cache(Arc::clone(&memory)));
    store.reset();
    let error = crate::NamespaceStore {
        segment_cache: Arc::clone(&cache),
        ..crate::test_support::namespace_store(&store, &namespace_id)
    }
    .gc(None, &options(), &setup.clock())
    .await
    .expect_err("the first session's content id does not fit the budget");

    assert_matches!(error, CoreError::LiveSetExceedsReadMemory { limit: 1, .. });
    let prefix = loonfs_objectstore::keys::upload_session_prefix(&namespace_id);
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
        "the pass stopped within one window of reads, after {session_reads}"
    );
    assert_eq!(memory.in_use(), 0);
}

/// Checks that one pass reads each completed upload session record once,
/// with more than one read and at most `STORE_READS_IN_FLIGHT` reads in
/// flight.
#[tokio::test]
async fn a_pass_reads_each_completed_session_record_once() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let sessions = loonfs_objectstore::keys::upload_session_prefix(&namespace_id);
    let store = RecordingStore::new(
        ConcurrencyWatchStore::new(
            LocalFsStore::new(temp_dir.path()).expect("store"),
            KeyPredicate::prefix(&sessions),
        ),
        KeyPredicate::prefix(&sessions),
    );
    let setup = store.inner().inner();
    let early = context(1_000);
    let late = context(early.now_ms + GRACE_MS);
    crate::test_support::NamespaceFixture::new(setup, &namespace_id, &early)
        .create()
        .await
        .expect("bootstrap");
    for completed in [&early, &late] {
        for _ in 0..20 {
            complete_upload_for_gc(setup, &namespace_id, b"unpublished\n", completed).await;
        }
    }

    let report = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(
            None,
            &options(),
            &context(early.now_ms + CONTENT_RECLAMATION_GRACE_WINDOW_MS).clock(),
        )
        .await
        .expect("gc pass");

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
