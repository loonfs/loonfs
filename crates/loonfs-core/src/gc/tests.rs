//! Behavior tests for namespace GC.

#![allow(clippy::panic)]

use super::collect::gc_namespace;
use super::config::GcConfig;
use crate::checkpoint::advance_retention_floor;
use crate::checkpoint::record::delete_checkpoint_record;
use crate::checkpoint::tests::{
    compact_a_family_group, create_checkpoint, mutation_context, write_test_file,
};
use crate::checkpoint::MetadataCompactionPolicy;
use crate::commit_engine::{CommitCandidate, NamespaceCommitEngine};
use crate::context::MutationContext;
use crate::error::CoreError;
use crate::limits::{
    CONTENT_RECLAMATION_GRACE_MS, GC_MIN_GRACE_WINDOW_MS, UNREFERENCED_SEGMENT_MIN_AGE_MS,
    UPLOAD_SESSION_LEASE_MS,
};
use crate::path::write::{CommitRequest, FilesystemOperation};
use loonfs_api::v0::GcResponse;
use loonfs_api::wire::control::{
    decode_control_object, CheckpointOwner, CheckpointRecordState, ControlObjectKind,
    ProxiedStaging, UploadSessionMode, UploadSessionRecordStatus, UploadSessionState,
};
use loonfs_api::{CheckpointId, ContentRef, ContentStoreId, ManifestNo, NamespaceId, UploadId};
use loonfs_objectstore::keys::{
    checkpoint_prefix, hint, metadata_manifest_object, metadata_manifest_prefix, metadata_segment,
    metadata_segment_prefix, wal_segment_prefix,
};
use loonfs_objectstore::ObjectStore;
use std::collections::BTreeSet;
use std::num::NonZeroUsize;

use crate::commit_engine::delete_namespace;
use crate::namespace::bootstrap::bootstrap_namespace;
use crate::namespace::fork::fork_namespace;
use crate::options::DeleteNamespaceOptions;
use crate::path::read::load_current_metadata_view;
use bytes::Bytes;
use loonfs_api::AttributeInclusion;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::PutMode;
use loonfs_test_support::stores::{
    BlockingStore, FailStore, InjectedError, KeyPredicate, MetadataMapStore, OperationClass,
    OperationContext, OperationKind, RecordingStore,
};
use tempfile::tempdir;

mod many_pins;
mod retirement;

const GRACE_MS: u64 = 60 * 60 * 1000;

fn config() -> GcConfig {
    GcConfig {
        grace_window_ms: GRACE_MS,
    }
}

fn context(now_ms: u64) -> MutationContext {
    mutation_context("gc-test", now_ms)
}

async fn checkpoint_exists<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    checkpoint_id: &CheckpointId,
) -> bool {
    crate::checkpoint::load_checkpoint_record(store, namespace_id, checkpoint_id)
        .await
        .expect("read checkpoint record")
        .is_some()
}

/// Derives "now" from durable object ages so the tests never touch a
/// wall clock: `offset_ms` past the newest object under the namespace.
async fn now_after_newest_object(
    store: &LocalFsStore,
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
            .last_modified_ms
            .expect("local fs provides timestamps");
        newest = newest.max(modified);
    }
    assert!(newest > 0, "namespace tree must not be empty");
    newest + offset_ms
}

async fn stat_root<S: ObjectStore>(store: &S, namespace_id: &NamespaceId) {
    load_current_metadata_view(store, namespace_id)
        .await
        .expect("load latest view")
        .resolve_path("/", AttributeInclusion::Omit)
        .await
        .expect("resolve root");
}

#[derive(Debug, Clone, Copy)]
enum BlockingControlCasTarget {
    UploadCompleted,
    UploadAborted,
}

impl BlockingControlCasTarget {
    fn matches(self, bytes: &[u8]) -> bool {
        match self {
            BlockingControlCasTarget::UploadCompleted | BlockingControlCasTarget::UploadAborted => {
                let Ok(envelope) = decode_control_object::<UploadSessionState>(
                    bytes,
                    ControlObjectKind::UploadSession,
                ) else {
                    return false;
                };
                match self {
                    BlockingControlCasTarget::UploadCompleted => matches!(
                        envelope.payload().status,
                        UploadSessionRecordStatus::Completed { .. }
                    ),
                    BlockingControlCasTarget::UploadAborted => {
                        matches!(
                            envelope.payload().status,
                            UploadSessionRecordStatus::Aborted { .. }
                        )
                    }
                }
            }
        }
    }
}

fn blocking_control_cas_store(
    inner: LocalFsStore,
    target: BlockingControlCasTarget,
) -> BlockingStore<LocalFsStore> {
    let store = BlockingStore::matching(inner, move |operation: &OperationContext<'_>| {
        let bytes = match operation.kind() {
            OperationKind::CompareAndSwap { bytes, .. }
            | OperationKind::Put {
                bytes,
                mode: PutMode::CompareAndSwap { .. },
            } => bytes,
            _ => return false,
        };
        target.matches(bytes)
    });
    store.block_next();
    store
}

#[tokio::test]
async fn gc_rejects_grace_windows_below_the_derived_minimum() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");

    let too_small = GcConfig {
        grace_window_ms: GC_MIN_GRACE_WINDOW_MS - 1,
    };
    let error = gc_namespace(&store, &namespace_id, &too_small, &context(1_000))
        .await
        .expect_err("sub-minimum grace window must be rejected");
    assert!(
        matches!(&error, CoreError::InvalidGcConfig(message)
            if message.contains("below the derived safety minimum")),
        "expected invalid gc config, got {error:?}"
    );
    assert_eq!(
        error.code(),
        crate::error::ErrorCode::InvalidRequest,
        "the rejection surfaces as invalid_request"
    );
}

#[tokio::test]
async fn gc_reaps_below_floor_segments_after_the_grace_window() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    bootstrap_namespace(&store, &namespace_id, &setup, false)
        .await
        .expect("bootstrap");
    write_test_file(&store, &namespace_id, "/docs/one.txt", "gc-one", &setup).await;
    write_test_file(&store, &namespace_id, "/docs/two.txt", "gc-two", &setup).await;
    create_checkpoint(&store, &namespace_id, &setup)
        .await
        .expect("checkpoint");
    advance_retention_floor(&store, &namespace_id, &setup)
        .await
        .expect("advance floor");

    let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
    let report = gc_namespace(&store, &namespace_id, &config(), &aged)
        .await
        .expect("gc pass");

    assert_eq!(report.deleted.wal_segments, 4);
    stat_root(&store, &namespace_id).await;
}

async fn write_upload_session(store: &LocalFsStore, namespace_id: &NamespaceId) -> String {
    let upload_id = loonfs_api::UploadId::parse("upl_0123456789abcdef0123456789abcdef")
        .expect("valid upload id");
    let state = loonfs_api::wire::control::UploadSessionState {
        namespace_id: namespace_id.clone(),
        upload_id: upload_id.clone(),
        content_id: loonfs_api::ContentId::generate(),
        created_at_ms: 1_000,
        mode: UploadSessionMode::ServiceProxied {
            staging: ProxiedStaging::Idle,
        },
        status: loonfs_api::wire::control::UploadSessionRecordStatus::Open {
            expires_at_ms: 1_000 + UPLOAD_SESSION_LEASE_MS,
        },
    };
    let bytes = loonfs_api::wire::control::encode_control_state(
        loonfs_api::wire::control::ControlObjectKind::UploadSession,
        &state,
    )
    .expect("encode session");
    let key = loonfs_objectstore::keys::upload_session(namespace_id, &upload_id);
    store
        .put_if_absent(&key, bytes::Bytes::from(bytes))
        .await
        .expect("write session");
    key
}

async fn stage_upload<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) -> (UploadId, ContentRef, ContentStoreId) {
    let begin = crate::protocol::begin_upload(
        store,
        namespace_id,
        loonfs_api::v0::BeginUploadRequest::ServiceProxied {},
        context,
    )
    .await
    .expect("begin upload");
    let staged =
        crate::protocol::upload_content(store, namespace_id, begin.upload_id(), b"racing upload\n")
            .await
            .expect("stage upload");
    let content_store_id =
        crate::namespace::catalog::load_namespace_content_store_id(store, namespace_id)
            .await
            .expect("content store id");
    (
        begin.upload_id().clone(),
        staged.content_ref,
        content_store_id,
    )
}

async fn read_upload_session<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    upload_id: &UploadId,
) -> Option<UploadSessionState> {
    let key = loonfs_objectstore::keys::upload_session(namespace_id, upload_id);
    let body = store.get(&key, None).await.expect("read upload session")?;
    Some(
        decode_control_object::<UploadSessionState>(&body, ControlObjectKind::UploadSession)
            .expect("decode upload session")
            .into_payload(),
    )
}

#[tokio::test]
async fn deleted_namespace_reclaims_down_to_its_tombstone() {
    // After deletion, user checkpoints, WAL, manifests, and segments can age
    // out. Only tombstone objects remain to prevent id reuse.
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    bootstrap_namespace(&store, &namespace_id, &setup, false)
        .await
        .expect("bootstrap");
    write_test_file(&store, &namespace_id, "/docs/one.txt", "gc-one", &setup).await;
    write_test_file(&store, &namespace_id, "/docs/two.txt", "gc-two", &setup).await;
    create_checkpoint(&store, &namespace_id, &setup)
        .await
        .expect("user pin");
    delete_namespace(
        &store,
        &namespace_id,
        DeleteNamespaceOptions::default(),
        &setup,
    )
    .await
    .expect("delete namespace");

    let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
    let report = gc_namespace(&store, &namespace_id, &config(), &aged)
        .await
        .expect("gc pass");
    assert!(report.deleted.wal_segments >= 1);
    assert_eq!(report.deleted.metadata_segments, 0);
    assert_eq!(report.deleted.manifests, 4);
    assert_eq!(report.deleted_checkpoints_by_owner.expired, 1);
    let reaped = context(aged.now_ms + UNREFERENCED_SEGMENT_MIN_AGE_MS);
    let report = gc_namespace(&store, &namespace_id, &config(), &reaped)
        .await
        .expect("gc pass after segment grace");
    assert_eq!(
        report.deleted_checkpoints_by_owner,
        loonfs_api::DeletedCheckpointsByOwner::default()
    );
    assert!(report.deleted.metadata_segments >= 1);
    assert!(report.deleted.manifests >= 1);

    for prefix in [
        wal_segment_prefix(&namespace_id),
        metadata_segment_prefix(&namespace_id),
        checkpoint_prefix(&namespace_id),
    ] {
        assert!(
            store.list_prefix(&prefix).await.expect("list").is_empty(),
            "prefix `{prefix}` must be empty after reclamation"
        );
    }
    let current = crate::namespace::control::load_current_manifest(&store, &namespace_id)
        .await
        .expect("tombstone");
    assert!(current.envelope.payload().status.is_deleted());
    assert!(current
        .envelope
        .payload()
        .status
        .reclaim_after_ms()
        .is_some());
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
    // Idempotent, and never degraded by its own reclamation.
    let again = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
    let report = gc_namespace(&store, &namespace_id, &config(), &again)
        .await
        .expect("second gc pass");
    assert_eq!(report.deleted.wal_segments, 0);
    assert_eq!(report.deleted.manifests, 0);
    assert_eq!(
        store
            .list_prefix(&metadata_manifest_prefix(&namespace_id))
            .await
            .expect("manifests"),
        vec![current.object_key]
    );
    assert_eq!(
        bootstrap_namespace(&store, &namespace_id, &setup, false)
            .await
            .expect_err("retired id")
            .code(),
        loonfs_api::ErrorCode::NamespaceDeleted
    );
}

#[tokio::test]
async fn fork_protected_bases_survive_source_deletion_until_the_target_dies() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let source = NamespaceId::parse("source").expect("namespace id");
    let clone = NamespaceId::parse("clone").expect("namespace id");
    let setup = context(1_000);
    bootstrap_namespace(&store, &source, &setup, false)
        .await
        .expect("bootstrap");
    write_test_file(&store, &source, "/docs/shared.txt", "gc-shared", &setup).await;
    fork_namespace(&store, &source, &clone, None, &setup)
        .await
        .expect("fork");
    delete_namespace(&store, &source, DeleteNamespaceOptions::default(), &setup)
        .await
        .expect("delete source");

    // The deleted source keeps exactly what the living clone needs.
    let fork_record = read_fork_record(&store, &source).await;
    let basis_key = metadata_manifest_object(&source, &fork_record.manifest_no);
    let aged = context(now_after_newest_object(&store, &source, GRACE_MS + 1).await);
    let report = gc_namespace(&store, &source, &config(), &aged)
        .await
        .expect("gc pass with live clone");
    assert_eq!(report.deleted_checkpoints_by_owner.fork, 0);
    assert!(
        store.head(&basis_key).await.expect("head basis").is_some(),
        "fork basis must survive while the clone lives"
    );
    let clone_view = load_current_metadata_view(&store, &clone)
        .await
        .expect("load clone view");
    clone_view
        .resolve_path("/docs/shared.txt", AttributeInclusion::Omit)
        .await
        .expect("clone reads through the deleted source");

    delete_namespace(&store, &clone, DeleteNamespaceOptions::default(), &setup)
        .await
        .expect("delete clone");
    let retired = gc_namespace(&store, &clone, &config(), &aged)
        .await
        .expect("retire clone");
    let deadline = retired.reclaim_after_ms.expect("clone retired");

    let aged = context(deadline);
    gc_namespace(&store, &clone, &config(), &aged)
        .await
        .expect("clone releases source pin");
    assert!(!checkpoint_exists(&store, &source, &fork_record.pin_id).await);

    let again = context(deadline + GRACE_MS);
    let report = gc_namespace(&store, &source, &config(), &again)
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
    bootstrap_namespace(&store, &namespace_id, &setup, false)
        .await
        .expect("bootstrap");
    write_test_file(&store, &namespace_id, "/docs/one.txt", "gc-one", &setup).await;
    let (upload_id, content_ref, content_store_id) =
        stage_upload(&store, &namespace_id, &setup).await;
    let session_key = loonfs_objectstore::keys::upload_session(&namespace_id, &upload_id);
    let content_key = loonfs_objectstore::keys::content_blob(
        &content_store_id,
        &content_ref.owner_namespace_id,
        &content_ref.content_id,
    );

    // Inside the lease nothing happens, however old the object looks: the
    // session carries its own expiry, so no provider timestamp decides this.
    let inside = context(setup.now_ms + UPLOAD_SESSION_LEASE_MS - 1);
    let report = gc_namespace(&store, &namespace_id, &config(), &inside)
        .await
        .expect("gc pass inside the lease");
    assert_eq!(report.deleted.upload_sessions, 0);
    assert!(store.head(&content_key).await.expect("head").is_some());

    // Past the lease plus a grace the session is aborted and the object it
    // was writing is deleted — in that order.
    let expired = context(setup.now_ms + UPLOAD_SESSION_LEASE_MS + GRACE_MS + 1);
    let report = gc_namespace(&store, &namespace_id, &config(), &expired)
        .await
        .expect("gc pass past the lease");
    assert_eq!(
        report.deleted.upload_sessions, 0,
        "the record outlives its abort"
    );
    let session = read_upload_session(&store, &namespace_id, &upload_id)
        .await
        .expect("aborted session retained");
    assert!(matches!(
        session.status,
        UploadSessionRecordStatus::Aborted { .. }
    ));
    assert!(
        store.head(&content_key).await.expect("head").is_none(),
        "aborting deletes the object the session owned"
    );

    // The aborted record is reaped a grace window after its own stamp.
    let reaped = context(expired.now_ms + GRACE_MS + 1);
    let report = gc_namespace(&store, &namespace_id, &config(), &reaped)
        .await
        .expect("gc pass past the abort grace");
    assert_eq!(report.deleted.upload_sessions, 1);
    assert_eq!(
        report.deleted.content_objects, 0,
        "the abort half's unconditional cleanup is not a reclamation it can count"
    );
    assert!(store.head(&session_key).await.expect("head").is_none());

    let again = context(reaped.now_ms + GRACE_MS);
    let report = gc_namespace(&store, &namespace_id, &config(), &again)
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
        OperationClass::Delete,
        InjectedError::Transport("content delete timed out".to_owned()),
    );
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    bootstrap_namespace(&store, &namespace_id, &setup, false)
        .await
        .expect("bootstrap");
    let (upload_id, _, _) = stage_upload(&store, &namespace_id, &setup).await;

    let expired = context(setup.now_ms + UPLOAD_SESSION_LEASE_MS + GRACE_MS + 1);
    gc_namespace(&store, &namespace_id, &config(), &expired)
        .await
        .expect("abort expired upload");

    store.fail_next(1);
    let reaped = context(expired.now_ms + GRACE_MS + 1);
    let report = gc_namespace(&store, &namespace_id, &config(), &reaped)
        .await
        .expect("retain after failed cleanup");
    assert_eq!(report.deleted.upload_sessions, 0);
    assert!(matches!(
        read_upload_session(&store, &namespace_id, &upload_id)
            .await
            .expect("aborted session retained")
            .status,
        UploadSessionRecordStatus::Aborted { .. }
    ));

    let report = gc_namespace(&store, &namespace_id, &config(), &reaped)
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
    bootstrap_namespace(&store, &namespace_id, &setup, false)
        .await
        .expect("bootstrap");
    let (upload_id, ..) = stage_upload(&store, &namespace_id, &setup).await;
    let expires_at_ms = setup.now_ms + UPLOAD_SESSION_LEASE_MS;

    // One un-expired open session and nothing else: the lease plus the
    // pass's own grace window is the whole answer.
    let inside = context(setup.now_ms + 1);
    let report = gc_namespace(&store, &namespace_id, &config(), &inside)
        .await
        .expect("gc pass inside the lease");
    assert_eq!(report.retained_candidates, 2);
    assert_eq!(
        report.next_reclamation_at_ms,
        Some(expires_at_ms + GRACE_MS),
        "an open session's reclamation waits for its lease and then the grace window"
    );

    // Completing it moves the deadline to the derived content grace, which
    // is the one the next pass is too early for.
    let completed_at = context(setup.now_ms + 2);
    complete_staged_upload(&store, &namespace_id, &upload_id, &completed_at).await;
    let report = gc_namespace(&store, &namespace_id, &config(), &completed_at)
        .await
        .expect("gc pass over the completed session");
    assert_eq!(
        report.next_reclamation_at_ms,
        Some(completed_at.now_ms + CONTENT_RECLAMATION_GRACE_MS),
        "a completed session's content is protected by the derived grace, not the configured one"
    );
}

#[tokio::test]
async fn an_aborted_session_is_reclaimed_from_the_deadline_the_pass_reported() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    bootstrap_namespace(&store, &namespace_id, &setup, false)
        .await
        .expect("bootstrap");
    let (upload_id, ..) = stage_upload(&store, &namespace_id, &setup).await;
    let session_key = loonfs_objectstore::keys::upload_session(&namespace_id, &upload_id);

    // The pass that aborts the session is the only thing that knows the
    // record now ages out a grace window from this instant.
    let expired = context(setup.now_ms + UPLOAD_SESSION_LEASE_MS + GRACE_MS + 1);
    let report = gc_namespace(&store, &namespace_id, &config(), &expired)
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
    let report = gc_namespace(&store, &namespace_id, &config(), &reclaiming)
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
    context: &MutationContext,
) {
    let content_store_id =
        crate::namespace::catalog::load_namespace_content_store_id(store, namespace_id)
            .await
            .expect("content store id");
    crate::protocol::complete_upload(
        store,
        namespace_id,
        &content_store_id,
        upload_id,
        crate::protocol::ResolvedUploadCompletion::KnownContent,
        context,
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
    bootstrap_namespace(&store, &namespace_id, &setup, false)
        .await
        .expect("bootstrap");
    let session_key = write_upload_session(&store, &namespace_id).await;

    let expired = context(1_000 + UPLOAD_SESSION_LEASE_MS + GRACE_MS + 1);
    gc_namespace(&store, &namespace_id, &config(), &expired)
        .await
        .expect("gc pass past the lease");
    let reaped = context(expired.now_ms + GRACE_MS + 1);
    let report = gc_namespace(&store, &namespace_id, &config(), &reaped)
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
    bootstrap_namespace(&store, &namespace_id, &setup, false)
        .await
        .expect("bootstrap");
    let (upload_id, content_ref, content_store_id) =
        stage_upload(&store, &namespace_id, &setup).await;
    let aged = context(setup.now_ms + UPLOAD_SESSION_LEASE_MS + GRACE_MS + 1);
    let content_key = loonfs_objectstore::keys::content_blob(
        &content_store_id,
        &content_ref.owner_namespace_id,
        &content_ref.content_id,
    );
    let store = blocking_control_cas_store(store, BlockingControlCasTarget::UploadAborted);
    let gc_config = config();
    let gc = gc_namespace(&store, &namespace_id, &gc_config, &aged);
    let complete = async {
        store.wait_until_blocked().await;
        let result = crate::protocol::complete_upload(
            &store,
            &namespace_id,
            &content_store_id,
            &upload_id,
            crate::protocol::ResolvedUploadCompletion::KnownContent,
            &aged,
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
    assert!(matches!(
        session.status,
        UploadSessionRecordStatus::Completed { .. }
    ));
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
    bootstrap_namespace(&store, &namespace_id, &setup, false)
        .await
        .expect("bootstrap");
    let (upload_id, content_ref, content_store_id) =
        stage_upload(&store, &namespace_id, &setup).await;
    let aged = context(setup.now_ms + UPLOAD_SESSION_LEASE_MS + GRACE_MS + 1);
    let content_key = loonfs_objectstore::keys::content_blob(
        &content_store_id,
        &content_ref.owner_namespace_id,
        &content_ref.content_id,
    );
    let store = blocking_control_cas_store(store, BlockingControlCasTarget::UploadCompleted);
    let completion = crate::protocol::complete_upload(
        &store,
        &namespace_id,
        &content_store_id,
        &upload_id,
        crate::protocol::ResolvedUploadCompletion::KnownContent,
        &aged,
    );
    let abort = async {
        store.wait_until_blocked().await;
        let report = gc_namespace(&store, &namespace_id, &config(), &aged).await;
        store.release();
        report
    };
    let (completion, report) = tokio::join!(completion, abort);
    let error = completion.expect_err("an aborted session is logically absent");
    assert!(matches!(&error, CoreError::UploadNotFound { .. }));
    assert_eq!(error.code(), crate::error::ErrorCode::UploadNotFound);
    report.expect("gc pass");
    let session = read_upload_session(&store, &namespace_id, &upload_id)
        .await
        .expect("aborted session retained for a grace window");
    assert!(matches!(
        session.status,
        UploadSessionRecordStatus::Aborted { .. }
    ));
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
    context: &MutationContext,
) -> (
    UploadId,
    ContentRef,
    ContentStoreId,
    crate::publish::PreparedContent,
) {
    let begin = crate::protocol::begin_upload(
        store,
        namespace_id,
        loonfs_api::v0::BeginUploadRequest::ServiceProxied {},
        context,
    )
    .await
    .expect("begin upload");
    let staged = crate::protocol::upload_content(store, namespace_id, begin.upload_id(), bytes)
        .await
        .expect("stage upload");
    let content_store_id =
        crate::namespace::catalog::load_namespace_content_store_id(store, namespace_id)
            .await
            .expect("content store id");
    let completed = crate::protocol::complete_upload(
        store,
        namespace_id,
        &content_store_id,
        begin.upload_id(),
        crate::protocol::ResolvedUploadCompletion::KnownContent,
        context,
    )
    .await
    .expect("complete upload");
    (
        begin.upload_id().clone(),
        staged.content_ref,
        content_store_id,
        completed.prepared,
    )
}

async fn publish_completed_content<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    path: &str,
    content_ref: ContentRef,
    prepared: crate::publish::PreparedContent,
    context: &MutationContext,
) {
    NamespaceCommitEngine::new(namespace_id.clone())
        .publish_batch(
            store,
            vec![CommitCandidate::prepared(
                CommitRequest::single(
                    loonfs_api::CommitId::parse("publish-completed-content").expect("commit id"),
                    loonfs_test_support::test_actor(),
                    None,
                    FilesystemOperation::PutFile {
                        path: loonfs_api::AbsolutePath::parse(path).expect("path"),
                        content_ref,
                        behavior: loonfs_api::DestinationBehavior::NoReplace,
                        expected_inode_id: None,
                        expected_revision_no: None,
                    },
                ),
                vec![prepared],
            )],
            context,
            &crate::protocol::PublishTailOptions::default(),
        )
        .await
        .results
        .pop()
        .expect("one result")
        .expect("published");
}

#[tokio::test]
async fn content_gc_retains_completed_content_inside_its_grace() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    bootstrap_namespace(&store, &namespace_id, &setup, false)
        .await
        .expect("bootstrap");
    let (upload_id, content_ref, content_store_id, _prepared) =
        complete_upload_for_gc(&store, &namespace_id, b"unpublished\n", &setup).await;
    let content_key = loonfs_objectstore::keys::content_blob(
        &content_store_id,
        &content_ref.owner_namespace_id,
        &content_ref.content_id,
    );

    let inside = context(setup.now_ms + CONTENT_RECLAMATION_GRACE_MS - 1);
    let report = gc_namespace(&store, &namespace_id, &config(), &inside)
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
async fn content_gc_reclaims_completed_content_nothing_references() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    bootstrap_namespace(&store, &namespace_id, &setup, false)
        .await
        .expect("bootstrap");
    write_test_file(&store, &namespace_id, "/docs/other.txt", "gc-other", &setup).await;
    let (upload_id, content_ref, content_store_id, _prepared) =
        complete_upload_for_gc(&store, &namespace_id, b"unpublished\n", &setup).await;
    let content_key = loonfs_objectstore::keys::content_blob(
        &content_store_id,
        &content_ref.owner_namespace_id,
        &content_ref.content_id,
    );

    let past = context(setup.now_ms + CONTENT_RECLAMATION_GRACE_MS + 1);
    let report = gc_namespace(&store, &namespace_id, &config(), &past)
        .await
        .expect("gc pass past the content grace");

    assert_eq!(report.deleted.upload_sessions, 1);
    assert_eq!(report.deleted.content_objects, 1);
    assert!(
        store.head(&content_key).await.expect("head").is_none(),
        "completed content nothing published is reclaimable"
    );
    assert!(read_upload_session(&store, &namespace_id, &upload_id)
        .await
        .is_none());
}

#[tokio::test]
async fn completed_content_delete_failure_keeps_the_session_for_retry() {
    let temp_dir = tempdir().expect("tempdir");
    let store = FailStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::content_blob(),
        OperationClass::Delete,
        InjectedError::Transport("content delete timed out".to_owned()),
    );
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    bootstrap_namespace(&store, &namespace_id, &setup, false)
        .await
        .expect("bootstrap");
    let (upload_id, content_ref, content_store_id, _prepared) =
        complete_upload_for_gc(&store, &namespace_id, b"unpublished\n", &setup).await;
    let content_key = loonfs_objectstore::keys::content_blob(
        &content_store_id,
        &content_ref.owner_namespace_id,
        &content_ref.content_id,
    );
    let past = context(setup.now_ms + CONTENT_RECLAMATION_GRACE_MS + 1);

    store.fail_next(1);
    let report = gc_namespace(&store, &namespace_id, &config(), &past)
        .await
        .expect("retain after failed content delete");
    assert_eq!(report.deleted.upload_sessions, 0);
    assert_eq!(report.deleted.content_objects, 0);
    assert!(store
        .head(&content_key)
        .await
        .expect("head content")
        .is_some());
    assert!(read_upload_session(&store, &namespace_id, &upload_id)
        .await
        .is_some());

    let report = gc_namespace(&store, &namespace_id, &config(), &past)
        .await
        .expect("retry content delete");
    assert_eq!(report.deleted.upload_sessions, 1);
    assert_eq!(report.deleted.content_objects, 1);
    assert!(store
        .head(&content_key)
        .await
        .expect("head content")
        .is_none());
    assert!(read_upload_session(&store, &namespace_id, &upload_id)
        .await
        .is_none());
}

#[tokio::test]
async fn completed_uploads_use_publication_lookups_without_scanning_segments() {
    for materialize in [false, true] {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let setup = context(1_000);
        bootstrap_namespace(&store, &namespace_id, &setup, false)
            .await
            .expect("bootstrap");
        let (upload_id, content_ref, content_store_id, prepared) =
            complete_upload_for_gc(&store, &namespace_id, b"published\n", &setup).await;
        publish_completed_content(
            &store,
            &namespace_id,
            "/docs/published.txt",
            content_ref.clone(),
            prepared,
            &setup,
        )
        .await;
        if materialize {
            // Materializing and then dropping the WAL below the floor
            // leaves the manifest as the only place the reference lives.
            crate::checkpoint::flush_wal(&store, &namespace_id, &setup)
                .await
                .expect("flush wal");
            advance_retention_floor(&store, &namespace_id, &setup)
                .await
                .expect("advance floor");
        }
        let (orphan_upload_id, orphan, _, _) =
            complete_upload_for_gc(&store, &namespace_id, b"never published", &setup).await;
        let orphan_key = loonfs_objectstore::keys::content_blob(
            &content_store_id,
            &namespace_id,
            &orphan.content_id,
        );
        let publication_keys: BTreeSet<_> = if materialize {
            crate::namespace::control::load_current_manifest(&store, &namespace_id)
                .await
                .expect("manifest")
                .envelope
                .payload()
                .runs
                .iter()
                .flat_map(|run| &run.segments)
                .filter(|segment| {
                    segment.family
                        == loonfs_api::wire::manifest::MetadataRowFamily::ContentPublications
                })
                .map(loonfs_objectstore::keys::metadata_segment_object_key)
                .collect()
        } else {
            BTreeSet::new()
        };
        let store = RecordingStore::new(
            store,
            KeyPredicate::prefix(metadata_segment_prefix(&namespace_id)),
        );
        let content_key = loonfs_objectstore::keys::content_blob(
            &content_store_id,
            &content_ref.owner_namespace_id,
            &content_ref.content_id,
        );

        let past = context(setup.now_ms + CONTENT_RECLAMATION_GRACE_MS + 1);
        let report = gc_namespace(&store, &namespace_id, &config(), &past)
            .await
            .expect("gc pass past the content grace");

        assert_eq!(
            report.deleted.upload_sessions, 2,
            "materialize={materialize}"
        );
        assert_eq!(
            report.deleted.content_objects, 1,
            "materialize={materialize}"
        );
        assert_eq!(store.counts().gets, if materialize { 1 } else { 0 });
        assert_eq!(store.counts().gets_with_metadata, 0);
        for (key, range) in store.take_gets() {
            assert!(publication_keys.contains(&key));
            assert!(range.is_some());
        }
        assert!(store
            .head(&orphan_key)
            .await
            .expect("orphan object")
            .is_none());
        assert!(
            read_upload_session(&store, &namespace_id, &orphan_upload_id)
                .await
                .is_none()
        );
        assert!(
            store.head(&content_key).await.expect("head").is_some(),
            "published content survives its session (materialize={materialize})"
        );
        assert!(read_upload_session(&store, &namespace_id, &upload_id)
            .await
            .is_none());
    }
}

#[tokio::test]
async fn gc_retains_everything_inside_the_grace_window() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    bootstrap_namespace(&store, &namespace_id, &setup, false)
        .await
        .expect("bootstrap");
    write_test_file(&store, &namespace_id, "/docs/one.txt", "gc-one", &setup).await;
    create_checkpoint(&store, &namespace_id, &setup)
        .await
        .expect("checkpoint");
    advance_retention_floor(&store, &namespace_id, &setup)
        .await
        .expect("advance floor");

    let orphan = metadata_segment(&namespace_id, &loonfs_api::MetadataSegmentId::generate());
    store
        .put_if_absent(&orphan, Bytes::from_static(b"unused"))
        .await
        .expect("young orphan");
    let young = context(now_after_newest_object(&store, &namespace_id, 0).await);
    let report = gc_namespace(&store, &namespace_id, &config(), &young)
        .await
        .expect("gc pass");

    assert_eq!(report.deleted.wal_segments, 0);
    assert_eq!(report.deleted.metadata_segments, 0);
    assert_eq!(report.deleted.manifests, 0);
    assert!(report.retained_candidates > 0);
    // The breakdown is the same total, said in reasons: nothing is counted
    // into one without the other, so the two can never disagree.
    assert_eq!(reason_total(&report), report.retained_candidates);
    // Everything unreachable here is simply young, and the pass says so
    // rather than leaving the operator to guess between age and reachability.
    assert!(report.retained.within_grace_window > 0);
    assert_eq!(report.retained.no_provider_timestamp, 0);
    stat_root(&store, &namespace_id).await;
}

/// The sum of every reason, which `retained_candidates` must equal.
fn reason_total(report: &GcResponse) -> u64 {
    report
        .retained
        .by_reason()
        .into_iter()
        .map(|(_, count)| count)
        .sum()
}

#[tokio::test]
async fn published_compaction_segments_are_referenced_and_kept() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    bootstrap_namespace(&store, &namespace_id, &setup, false)
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
        // Flushes rather than checkpoints: a checkpoint pins the manifest it
        // published, and a pinned manifest protects its segments forever, which
        // would leave this pass nothing to reap and nothing to prove.
        crate::checkpoint::flush_wal(&store, &namespace_id, &setup)
            .await
            .expect("flush wal");
    }
    let staged = compact_a_family_group(&store, &namespace_id, &setup).await;

    // Far past every window this collector knows, including the staging one.
    let long_after = context(
        now_after_newest_object(&store, &namespace_id, UNREFERENCED_SEGMENT_MIN_AGE_MS * 4).await,
    );
    let report = gc_namespace(&store, &namespace_id, &config(), &long_after)
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
    bootstrap_namespace(&seed, &namespace_id, &setup, false)
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
        crate::checkpoint::flush_wal(&seed, &namespace_id, &setup)
            .await
            .expect("flush wal");
    }
    // One clock for the job and the pass: the job stamps its lease at the same
    // instant the pass reads it by, which is what a running job's lease looks
    // like to a collector.
    let pass_clock = context(now_after_newest_object(&seed, &namespace_id, GRACE_MS + 1).await);
    let (policy, spec) =
        crate::checkpoint::tests::plan_a_family_group_compaction(&seed, &namespace_id, &pass_clock)
            .await;

    // The pass parks at its first listing, which is after it has collected the
    // roots and long before it reaches the compaction prefix. Everything the
    // job does happens in that gap, on a store the gate does not hold.
    let gated = BlockingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::prefix(wal_segment_prefix(&namespace_id)),
        OperationClass::List,
    );
    gated.block_next();
    let pass_config = config();
    let (report, staged) = tokio::join!(
        gc_namespace(&gated, &namespace_id, &pass_config, &pass_clock),
        async {
            gated.wait_until_blocked().await;
            crate::checkpoint::tests::publish_planned_compaction(
                &seed,
                &namespace_id,
                &pass_clock,
                policy,
                &spec,
            )
            .await;
            let staged = crate::checkpoint::tests::segment_keys_of_the_current_manifest(
                &seed,
                &namespace_id,
            )
            .await;
            gated.release();
            staged
        }
    );
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
async fn a_pass_names_a_checkpoint_record_it_could_not_advance() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    bootstrap_namespace(&store, &namespace_id, &setup, false)
        .await
        .expect("bootstrap");
    write_test_file(&store, &namespace_id, "/docs/one.txt", "gc-one", &setup).await;
    create_checkpoint(&store, &namespace_id, &setup)
        .await
        .expect("checkpoint");

    let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS * 2).await);
    let report = gc_namespace(&store, &namespace_id, &config(), &aged)
        .await
        .expect("gc pass");

    assert_eq!(
        report.deleted_checkpoints_by_owner,
        loonfs_api::DeletedCheckpointsByOwner::default()
    );
    assert_eq!(report.retained.checkpoint_not_deletable, 1);
    assert_eq!(reason_total(&report), report.retained_candidates);
}

#[tokio::test]
async fn gc_never_deletes_the_live_replay_tail() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    bootstrap_namespace(&store, &namespace_id, &setup, false)
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
    advance_retention_floor(&store, &namespace_id, &setup)
        .await
        .expect("advance floor");
    // A commit past the floor: its segment is the live replay gap.
    write_test_file(&store, &namespace_id, "/docs/two.txt", "gc-two", &setup).await;

    let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
    let report = gc_namespace(&store, &namespace_id, &config(), &aged)
        .await
        .expect("gc pass");

    assert_eq!(report.deleted.wal_segments, 4);
    // Latest reads replay the retained tail over the manifest basis.
    let view = load_current_metadata_view(&store, &namespace_id)
        .await
        .expect("load view");
    view.resolve_path("/docs/two.txt", AttributeInclusion::Omit)
        .await
        .expect("tail commit stays readable");
}

async fn assert_basis_reaped(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
    _pass: &GcResponse,
    manifest_number: &ManifestNo,
) {
    assert!(crate::checkpoint::load_namespace_manifest_envelope(
        store,
        namespace_id,
        manifest_number
    )
    .await
    .is_err());
}

#[tokio::test]
async fn gc_retains_unrecognized_manifest_keys() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    bootstrap_namespace(&store, &namespace_id, &setup, false)
        .await
        .expect("bootstrap");

    let manifest_prefix = metadata_manifest_prefix(&namespace_id);
    let foreign_objects = [
        (
            format!("{manifest_prefix}notes.txt"),
            b"foreign key".as_slice(),
        ),
        (
            format!("{manifest_prefix}invalid.manifest.json"),
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
    let report = gc_namespace(&store, &namespace_id, &config(), &aged)
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
async fn gc_reclaims_manifests_superseded_by_wal_flushes() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    bootstrap_namespace(&store, &namespace_id, &setup, false)
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
        crate::checkpoint::flush_wal(&store, &namespace_id, &setup)
            .await
            .expect("flush wal");
    }

    // Record-less maintenance: nothing accumulates under `pins/`.
    assert!(
        store
            .list_prefix(&checkpoint_prefix(&namespace_id))
            .await
            .expect("list checkpoint records")
            .is_empty(),
        "a wal flush must not create checkpoint records"
    );

    let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
    let report = gc_namespace(&store, &namespace_id, &config(), &aged)
        .await
        .expect("gc pass");

    // The first flush materialized the namespace's first manifest and the
    // next two superseded it; only the current manifest is reachable. Its
    // segments are all still referenced (a flush only appends delta runs).
    assert_eq!(report.deleted.manifests, 6);
    let manifests_left = store
        .list_prefix(&metadata_manifest_prefix(&namespace_id))
        .await
        .expect("list manifests");
    assert_eq!(manifests_left.len(), 1, "only the current manifest stays");

    // Reorganization folds the delta runs into fresh base segments; the
    // superseded run segments then age out on the next pass.
    let fold_policy = crate::checkpoint::MetadataLsmPolicy {
        max_delta_runs: NonZeroUsize::MIN,
        ..Default::default()
    };
    for _ in 0..16 {
        let report = crate::checkpoint::reorganize_metadata_step(
            &store,
            &namespace_id,
            0,
            fold_policy,
            MetadataCompactionPolicy::default(),
        )
        .await
        .expect("reorganize step");
        if matches!(
            report.outcome,
            crate::checkpoint::MetadataReorganizeOutcome::NotNeeded { .. }
        ) {
            break;
        }
    }
    let aged = context(
        now_after_newest_object(&store, &namespace_id, UNREFERENCED_SEGMENT_MIN_AGE_MS + 1).await,
    );
    let after_fold = gc_namespace(&store, &namespace_id, &config(), &aged)
        .await
        .expect("gc pass after reorganization");
    assert!(
        after_fold.deleted.metadata_segments > 0,
        "folded-away run segments become collectable"
    );

    stat_root(&store, &namespace_id).await;
    let view = load_current_metadata_view(&store, &namespace_id)
        .await
        .expect("load view");
    for round in 0..3 {
        view.resolve_path(&format!("/docs/file-{round}.txt"), AttributeInclusion::Omit)
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
    bootstrap_namespace(&store, &namespace_id, &setup, false)
        .await
        .expect("bootstrap");
    write_test_file(&store, &namespace_id, "/docs/one.txt", "gc-one", &setup).await;
    let first = crate::checkpoint::create_checkpoint(
        &store,
        &namespace_id,
        CheckpointOwner::User {
            name: "keeper".to_owned(),
            expires_at_ms: None,
        },
        &setup,
    )
    .await
    .expect("first owner");
    let second = crate::checkpoint::create_checkpoint(
        &store,
        &namespace_id,
        CheckpointOwner::User {
            name: "releaser".to_owned(),
            expires_at_ms: None,
        },
        &setup,
    )
    .await
    .expect("second owner");
    assert_ne!(first.checkpoint_id, second.checkpoint_id);
    assert_eq!(first.manifest_no, second.manifest_no);
    write_test_file(&store, &namespace_id, "/docs/two.txt", "gc-two", &setup).await;
    create_checkpoint(&store, &namespace_id, &setup)
        .await
        .expect("advance past the shared basis");

    crate::checkpoint::delete_checkpoint(&store, &namespace_id, &second.checkpoint_id)
        .await
        .expect("release one owner");
    let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
    let first_pass = gc_namespace(&store, &namespace_id, &config(), &aged)
        .await
        .expect("first gc pass");
    assert_eq!(
        first_pass.deleted_checkpoints_by_owner,
        loonfs_api::DeletedCheckpointsByOwner::default()
    );

    let second_pass = gc_namespace(&store, &namespace_id, &config(), &aged)
        .await
        .expect("second gc pass");
    assert_eq!(second_pass.deleted.manifests, 0);
    assert_eq!(
        second_pass.deleted_checkpoints_by_owner,
        loonfs_api::DeletedCheckpointsByOwner::default()
    );

    let keeper =
        crate::checkpoint::load_checkpoint_record(&store, &namespace_id, &first.checkpoint_id)
            .await
            .expect("read keeper record")
            .expect("the surviving owner's record stays")
            .state;
    assert!(
        crate::checkpoint::load_namespace_manifest_envelope(
            &store,
            &namespace_id,
            &keeper.manifest_no,
        )
        .await
        .is_ok(),
        "shared basis survives while any owner remains"
    );
}

#[tokio::test]
async fn fork_owned_checkpoints_reject_user_release() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let source = NamespaceId::parse("source").expect("namespace id");
    let clone = NamespaceId::parse("clone").expect("namespace id");
    let setup = context(1_000);
    bootstrap_namespace(&store, &source, &setup, false)
        .await
        .expect("bootstrap");
    write_test_file(&store, &source, "/docs/one.txt", "gc-one", &setup).await;
    fork_namespace(&store, &source, &clone, None, &setup)
        .await
        .expect("fork");

    let fork_record = read_fork_record(&store, &source).await;

    let error = crate::checkpoint::delete_checkpoint(&store, &source, &fork_record.pin_id)
        .await
        .expect_err("fork-owned release must fail");
    assert!(
        matches!(
            &error,
            CoreError::InvalidCheckpointRequest(message)
                if message.contains("owned by fork target")
        ),
        "expected invalid checkpoint request, got {error:?}"
    );
}

#[tokio::test]
async fn snapshot_owned_checkpoints_reject_user_release() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    bootstrap_namespace(&store, &namespace_id, &setup, false)
        .await
        .expect("bootstrap");
    write_test_file(&store, &namespace_id, "/docs/one.txt", "gc-one", &setup).await;
    let snapshot = crate::checkpoint::create_checkpoint(
        &store,
        &namespace_id,
        CheckpointOwner::Snapshot {
            name: "report-run".to_owned(),
            expires_at_ms: u64::MAX,
        },
        &setup,
    )
    .await
    .expect("snapshot checkpoint");

    let error =
        crate::checkpoint::delete_checkpoint(&store, &namespace_id, &snapshot.checkpoint_id)
            .await
            .expect_err("snapshot-owned release must fail");
    assert!(
        matches!(
            &error,
            CoreError::InvalidCheckpointRequest(message)
                if message.contains("is a snapshot")
        ),
        "expected invalid checkpoint request, got {error:?}"
    );
    assert!(checkpoint_exists(&store, &namespace_id, &snapshot.checkpoint_id).await);
}

#[tokio::test]
async fn gc_retains_active_checkpoint_bases() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    bootstrap_namespace(&store, &namespace_id, &setup, false)
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

    let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
    let report = gc_namespace(&store, &namespace_id, &config(), &aged)
        .await
        .expect("gc pass");

    // Only the unpinned bootstrap manifest is collectable; both active
    // checkpoint bases stay.
    assert_eq!(report.deleted.manifests, 3);
    assert_eq!(
        report.deleted_checkpoints_by_owner,
        loonfs_api::DeletedCheckpointsByOwner::default()
    );
    let first_record =
        crate::checkpoint::load_checkpoint_record(&store, &namespace_id, &first.checkpoint_id)
            .await
            .expect("read first checkpoint")
            .expect("first checkpoint exists")
            .state;
    assert!(crate::checkpoint::load_namespace_manifest_envelope(
        &store,
        &namespace_id,
        &first_record.manifest_no,
    )
    .await
    .is_ok());
}

/// Reads the single fork-owned record a fork left under the source.
async fn read_fork_record(store: &LocalFsStore, source: &NamespaceId) -> CheckpointRecordState {
    for key in store
        .list_prefix(&checkpoint_prefix(source))
        .await
        .expect("list checkpoints")
    {
        let bytes = store
            .get(&key, None)
            .await
            .expect("get record")
            .expect("record exists");
        let record = decode_control_object::<CheckpointRecordState>(
            &bytes,
            ControlObjectKind::CheckpointRecord,
        )
        .expect("decode record")
        .into_payload();
        if matches!(record.owner, CheckpointOwner::Fork { .. }) {
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
    bootstrap_namespace(&store, &source, &setup, false)
        .await
        .expect("bootstrap");
    write_test_file(&store, &source, "/docs/one.txt", "gc-one", &setup).await;
    fork_namespace(&store, &source, &clone, None, &setup)
        .await
        .expect("fork");
    let target_pin = create_checkpoint(&store, &clone, &setup)
        .await
        .expect("materialize target manifest");
    delete_checkpoint_record(&store, &clone, &target_pin.checkpoint_id)
        .await
        .expect("release target pin");
    let fork_record = read_fork_record(&store, &source).await;
    // Advance the source manifest past the fork basis so the basis is
    // reachable only through the fork-owned record.
    write_test_file(&store, &source, "/docs/two.txt", "gc-two", &setup).await;
    create_checkpoint(&store, &source, &setup)
        .await
        .expect("advance manifest past the fork basis");

    let before = context(now_after_newest_object(&store, &source, GRACE_MS + 1).await);
    let report = gc_namespace(&store, &source, &config(), &before)
        .await
        .expect("gc with live target");
    assert_eq!(report.deleted_checkpoints_by_owner.fork, 0);

    delete_namespace(&store, &clone, DeleteNamespaceOptions::default(), &setup)
        .await
        .expect("terminal delete of the fork target");
    let aged = context(now_after_newest_object(&store, &source, GRACE_MS + 1).await);

    let waiting = gc_namespace(&store, &source, &config(), &aged)
        .await
        .expect("wait for retirement");
    assert_eq!(waiting.deleted_checkpoints_by_owner.fork, 0);
    let retired = gc_namespace(&store, &clone, &config(), &aged)
        .await
        .expect("retire materialized target");
    let deadline = retired.reclaim_after_ms.expect("target retired");
    let waiting = gc_namespace(&store, &source, &config(), &context(deadline - 1))
        .await
        .expect("wait for grace");
    assert_eq!(waiting.deleted_checkpoints_by_owner.fork, 0);
    assert!(checkpoint_exists(&store, &source, &fork_record.pin_id).await);
    let aged = context(deadline);

    let pin_key = loonfs_objectstore::keys::checkpoint_record(&source, &fork_record.pin_id);
    let store = RecordingStore::new(
        FailStore::new(
            store,
            KeyPredicate::exact(&pin_key),
            OperationClass::Delete,
            InjectedError::Transport("source pin delete failed".to_owned()),
        ),
        KeyPredicate::exact(&pin_key),
    );
    store.inner().fail_next(1);
    assert!(gc_namespace(&store, &clone, &config(), &aged)
        .await
        .is_err());
    assert!(checkpoint_exists(&store, &source, &fork_record.pin_id).await);
    let released = gc_namespace(&store, &clone, &config(), &aged)
        .await
        .expect("retry source pin delete");
    assert_eq!(released.deleted_checkpoints_by_owner.fork, 1);
    assert!(!checkpoint_exists(&store, &source, &fork_record.pin_id).await);
    let repeated = gc_namespace(&store, &clone, &config(), &aged)
        .await
        .expect("repeat source pin delete");
    assert_eq!(repeated.deleted_checkpoints_by_owner.fork, 0);
    assert_eq!(store.counts().deletes, 3);
    assert!(store
        .head(&metadata_manifest_object(&source, &fork_record.manifest_no))
        .await
        .expect("manifest")
        .is_some());
    let store = store.inner().inner();
    let next_pass = gc_namespace(store, &source, &config(), &aged)
        .await
        .expect("collect basis");
    assert_basis_reaped(store, &source, &next_pass, &fork_record.manifest_no).await;
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
        OperationClass::Read,
        InjectedError::Transport("target hint timed out".to_owned()),
    );
    let setup = context(1_000);
    bootstrap_namespace(&store, &source, &setup, false)
        .await
        .expect("bootstrap");
    write_test_file(&store, &source, "/docs/one.txt", "gc-one", &setup).await;
    fork_namespace(&store, &source, &clone, None, &setup)
        .await
        .expect("fork");
    let fork_record = read_fork_record(store.inner(), &source).await;

    let aged = context(setup.now_ms + GRACE_MS);
    store.fail_all();
    let report = gc_namespace(&store, &source, &config(), &aged)
        .await
        .expect("an unreadable target is retained conservatively");
    assert_eq!(report.deleted_checkpoints_by_owner.fork, 0);
    assert!(checkpoint_exists(store.inner(), &source, &fork_record.pin_id).await);

    store.clear();
    let current = crate::namespace::control::load_current_manifest(store.inner(), &clone)
        .await
        .expect("target manifest");
    let manifest_key = current.object_key;
    let mut payload = current.envelope.into_payload();
    let basis = payload.fork_basis.as_mut().expect("fork basis");
    basis.manifest.manifest_no = ManifestNo(basis.manifest.manifest_no.0 + 1);
    let bytes = loonfs_api::wire::manifest::encode_namespace_manifest_json(payload)
        .expect("manifest")
        .into_bytes();
    store
        .put_overwrite(&manifest_key, Bytes::from(bytes))
        .await
        .expect("write drifted manifest");
    let error = gc_namespace(&store, &source, &config(), &aged)
        .await
        .expect_err("a target naming this record with another manifest is corruption");
    assert_eq!(error.code(), crate::error::ErrorCode::NamespaceCorrupt);
    assert!(error.message().contains(fork_record.pin_id.as_str()));

    store
        .put_overwrite(&hint(&clone), Bytes::from_static(b"not json"))
        .await
        .expect("corrupt target manifest");
    let before = namespace_keys(store.inner(), &source).await;
    let error = gc_namespace(&store, &source, &config(), &aged)
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
    bootstrap_namespace(&store, &source, &setup, false)
        .await
        .expect("bootstrap");
    write_test_file(&store, &source, "/docs/one.txt", "gc-one", &setup).await;
    fork_namespace(&store, &source, &clone, None, &setup)
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
        let report = gc_namespace(&store, &source, &config(), &context(now_ms))
            .await
            .expect("gc pass with a live target");
        assert_eq!(report.deleted_checkpoints_by_owner.fork, 0, "at {now_ms}");
        assert_eq!(
            report.deleted_checkpoints_by_owner.expired, 0,
            "at {now_ms}"
        );
        assert!(
            checkpoint_exists(&store, &source, &fork_record.pin_id).await,
            "a live target keeps its pin at {now_ms}"
        );
    }
    assert!(crate::checkpoint::load_namespace_manifest_envelope(
        &store,
        &source,
        &fork_record.manifest_no,
    )
    .await
    .is_ok());
    load_current_metadata_view(&store, &clone)
        .await
        .expect("target readable after every pass")
        .resolve_path("/docs/one.txt", AttributeInclusion::Omit)
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
    bootstrap_namespace(&store, &source, &setup, false)
        .await
        .expect("bootstrap");
    write_test_file(&store, &source, "/docs/one.txt", "gc-one", &setup).await;
    let abandoned = crate::checkpoint::create_checkpoint(
        &store,
        &source,
        CheckpointOwner::Fork {
            target_namespace_id: clone.clone(),
        },
        &setup,
    )
    .await
    .expect("fork pin from the abandoned attempt");

    fork_namespace(&store, &source, &clone, None, &setup)
        .await
        .expect("fork retry after abandonment");
    let retry = store
        .list_prefix(&checkpoint_prefix(&source))
        .await
        .expect("list checkpoints")
        .len();
    assert_eq!(retry, 2, "the retry pins for itself instead of reusing");
    assert!(
        checkpoint_exists(&store, &source, &abandoned.checkpoint_id).await,
        "the retry leaves the abandoned record alone"
    );

    let before_creation_grace = context(setup.now_ms + 1);
    let report = gc_namespace(&store, &source, &config(), &before_creation_grace)
        .await
        .expect("gc pass with a target that reads through another record");
    assert_eq!(report.deleted_checkpoints_by_owner.fork, 0);
    assert!(checkpoint_exists(&store, &source, &abandoned.checkpoint_id).await);
    let report = gc_namespace(
        &store,
        &source,
        &config(),
        &context(setup.now_ms + GRACE_MS),
    )
    .await
    .expect("an aged pin the target does not name is the abandoned attempt's");
    assert_eq!(report.deleted_checkpoints_by_owner.fork, 1);
    assert!(!checkpoint_exists(&store, &source, &abandoned.checkpoint_id).await);
    assert_eq!(
        store
            .list_prefix(&checkpoint_prefix(&source))
            .await
            .expect("list checkpoints")
            .len(),
        1,
        "the retry's own pin stays"
    );
    load_current_metadata_view(&store, &clone)
        .await
        .expect("target readable after retry and collection")
        .resolve_path("/docs/one.txt", AttributeInclusion::Omit)
        .await
        .expect("forked file readable");
}

#[tokio::test]
async fn a_corrupt_checkpoint_record_and_an_unreadable_one_both_fail_the_pass() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let store = FailStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::prefix(checkpoint_prefix(&namespace_id)),
        OperationClass::Read,
        InjectedError::Transport("checkpoint record read timed out".to_owned()),
    );
    let setup = context(1_000);
    bootstrap_namespace(&store, &namespace_id, &setup, false)
        .await
        .expect("bootstrap");
    write_test_file(&store, &namespace_id, "/docs/one.txt", "gc-one", &setup).await;
    create_checkpoint(&store, &namespace_id, &setup)
        .await
        .expect("checkpoint");

    let record_key = store
        .inner()
        .list_prefix(&checkpoint_prefix(&namespace_id))
        .await
        .expect("list checkpoints")
        .first()
        .expect("the checkpoint wrote a record")
        .clone();
    let before = namespace_keys(store.inner(), &namespace_id).await;
    let aged = context(setup.now_ms);

    store.fail_all();
    let error = gc_namespace(&store, &namespace_id, &config(), &aged)
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
    store
        .put_overwrite(&record_key, Bytes::from_static(b"not json"))
        .await
        .expect("corrupt record");
    let aged = context(setup.now_ms);
    let error = gc_namespace(&store, &namespace_id, &config(), &aged)
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
        OperationClass::Read,
        InjectedError::Transport("manifest read timed out".to_owned()),
    );
    let setup = context(1_000);
    bootstrap_namespace(&store, &namespace_id, &setup, false)
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
    let error = gc_namespace(&store, &namespace_id, &config(), &aged)
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
        store
            .put_overwrite(key, Bytes::from_static(b"not json"))
            .await
            .expect("corrupt manifest");
    }
    let aged = context(now_after_newest_object(store.inner(), &namespace_id, GRACE_MS + 1).await);
    let error = gc_namespace(&store, &namespace_id, &config(), &aged)
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
    bootstrap_namespace(&store, &namespace_id, &setup, false)
        .await
        .expect("bootstrap");
    write_test_file(&store, &namespace_id, "/docs/one.txt", "gc-one", &setup).await;
    create_checkpoint(&store, &namespace_id, &setup)
        .await
        .expect("checkpoint");
    advance_retention_floor(&store, &namespace_id, &setup)
        .await
        .expect("advance floor");

    // Far past any window by wall clock, but no object carries a
    // provider timestamp.
    let aged = context(now_after_newest_object(store.inner(), &namespace_id, GRACE_MS + 1).await);
    let report = gc_namespace(&store, &namespace_id, &config(), &aged)
        .await
        .expect("gc pass");

    assert_eq!(report.deleted.wal_segments, 0);
    assert_eq!(report.deleted.metadata_segments, 0);
    assert_eq!(report.deleted.manifests, 0);
    assert_eq!(
        report.deleted_checkpoints_by_owner,
        loonfs_api::DeletedCheckpointsByOwner::default()
    );
    assert_eq!(report.deleted_checkpoints_by_owner.fork, 0);
    assert!(report.retained_candidates > 0);
    stat_root(&store, &namespace_id).await;
}

#[tokio::test]
async fn gc_of_an_absent_namespace_reads_the_hint_and_sweeps_nothing() {
    let temp_dir = tempdir().expect("tempdir");
    let inner = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("orphan").expect("namespace id");
    let store = RecordingStore::new(inner, KeyPredicate::any());

    let report = gc_namespace(&store, &namespace_id, &config(), &context(u64::MAX))
        .await
        .expect("gc absent namespace");
    assert_eq!(report, GcResponse::empty(namespace_id.clone()));
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
    use super::reap::{delete_if_aged, GraceAge};
    use crate::limits::GC_SAFETY_MARGIN_MS;

    let dir = tempdir().expect("tempdir");
    let provider_stamp = 1_000_000;
    let store = RecordingStore::new(
        MetadataMapStore::new(
            LocalFsStore::new(dir.path()).expect("store"),
            KeyPredicate::any(),
            move |mut metadata| {
                metadata.last_modified_ms = Some(provider_stamp);
                metadata
            },
        ),
        KeyPredicate::any(),
    );
    let key = "orphan";
    store
        .put_if_absent(key, Bytes::from_static(b"orphan"))
        .await
        .expect("write orphan");
    let publication_bound = GC_MIN_GRACE_WINDOW_MS - GC_SAFETY_MARGIN_MS;
    let collector = context(provider_stamp + publication_bound - 1 + GC_SAFETY_MARGIN_MS);
    assert_eq!(
        delete_if_aged(&store, key, GC_MIN_GRACE_WINDOW_MS, collector.now_ms)
            .await
            .expect("age before cutoff"),
        GraceAge::Young,
    );
    assert_eq!(store.counts().deletes, 0);
    assert_eq!(
        delete_if_aged(&store, key, GC_MIN_GRACE_WINDOW_MS, collector.now_ms + 1)
            .await
            .expect("age at cutoff"),
        GraceAge::Aged,
    );
    assert_eq!(store.counts().deletes, 1);
}

#[tokio::test]
async fn competing_collectors_preserve_the_winning_retirement_deadline() {
    let directory = tempdir().expect("tempdir");
    let inner = LocalFsStore::new(directory.path()).expect("store");
    let namespace_id = NamespaceId::parse("retirement-race").expect("namespace id");
    let setup = context(1_000);
    bootstrap_namespace(&inner, &namespace_id, &setup, false)
        .await
        .expect("bootstrap");
    delete_namespace(
        &inner,
        &namespace_id,
        DeleteNamespaceOptions::default(),
        &setup,
    )
    .await
    .expect("delete");
    let store = BlockingStore::new(
        inner,
        KeyPredicate::manifest(&namespace_id),
        OperationClass::PutCreateIfAbsent,
    );
    store.block_next();
    let first_clock = context(GRACE_MS);
    let second_clock = context(GRACE_MS * 10);
    let config = config();
    let (first, second) = tokio::join!(
        gc_namespace(&store, &namespace_id, &config, &first_clock),
        async {
            store.wait_until_blocked().await;
            let result = gc_namespace(store.inner(), &namespace_id, &config, &second_clock).await;
            store.release();
            result
        }
    );
    let deadline = second_clock.now_ms + GRACE_MS;
    assert_eq!(
        first.expect("losing collector").reclaim_after_ms,
        Some(deadline)
    );
    assert_eq!(
        second.expect("winning collector").reclaim_after_ms,
        Some(deadline)
    );
}

#[tokio::test]
async fn uncertain_retirement_reads_back_and_failed_retirement_writes_nothing_further() {
    for landed in [false, true] {
        let directory = tempdir().expect("tempdir");
        let inner = LocalFsStore::new(directory.path()).expect("store");
        let namespace_id = NamespaceId::parse("retirement-uncertain").expect("namespace id");
        let setup = context(1_000);
        bootstrap_namespace(&inner, &namespace_id, &setup, false)
            .await
            .expect("bootstrap");
        delete_namespace(
            &inner,
            &namespace_id,
            DeleteNamespaceOptions::default(),
            &setup,
        )
        .await
        .expect("delete");
        let store = FailStore::new(
            inner,
            KeyPredicate::manifest(&namespace_id),
            OperationClass::PutCreateIfAbsent,
            InjectedError::Transport("uncertain retirement".to_owned()),
        );
        let store = if landed {
            store.apply_then_fail()
        } else {
            store
        };
        store.fail_next(1);
        let store = RecordingStore::new(store, KeyPredicate::any());
        let result = gc_namespace(&store, &namespace_id, &config(), &context(GRACE_MS)).await;
        assert_eq!(result.is_ok(), landed);
        let head = crate::namespace::control::load_namespace_read_state(&store, &namespace_id)
            .await
            .expect("head");
        assert_eq!(
            head.status.reclaim_after_ms(),
            landed.then_some(GRACE_MS * 2)
        );
        if !landed {
            assert_eq!(store.counts().create_if_absent_puts, 1);
        }
    }
}

async fn retired_content_namespace<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
) -> (ContentStoreId, MutationContext) {
    let setup = context(1_000);
    bootstrap_namespace(store, namespace_id, &setup, false)
        .await
        .expect("bootstrap");
    let content_store_id =
        crate::namespace::catalog::load_namespace_content_store_id(store, namespace_id)
            .await
            .expect("content store");
    delete_namespace(store, namespace_id, Default::default(), &setup)
        .await
        .expect("delete");
    let report = gc_namespace(store, namespace_id, &config(), &setup)
        .await
        .expect("retire");
    (
        content_store_id,
        context(report.reclaim_after_ms.expect("deadline")),
    )
}

async fn owned_content_keys<S: ObjectStore>(
    store: &S,
    content_store_id: &ContentStoreId,
    namespace_id: &NamespaceId,
) -> Vec<String> {
    let mut keys = Vec::new();
    for number in [2, 3, 4] {
        let content_id =
            loonfs_api::ContentId::parse(format!("con_{number:032x}")).expect("content id");
        let key =
            loonfs_objectstore::keys::content_blob(content_store_id, namespace_id, &content_id);
        store
            .put_if_absent(&key, Bytes::from_static(b"content"))
            .await
            .expect("content");
        keys.push(key);
    }
    keys
}

#[tokio::test]
async fn completed_upload_waits_for_namespace_retirement_then_reclaims() {
    for first_run_ms in [1_000, CONTENT_RECLAMATION_GRACE_MS + 1_000] {
        let directory = tempdir().expect("tempdir");
        let store = LocalFsStore::new(directory.path()).expect("store");
        let namespace_id = NamespaceId::parse("completed-retired").expect("namespace");
        let setup = context(1_000);
        bootstrap_namespace(&store, &namespace_id, &setup, false)
            .await
            .expect("bootstrap");
        let (upload_id, content, content_store_id, _) =
            complete_upload_for_gc(&store, &namespace_id, b"content", &setup).await;
        delete_namespace(&store, &namespace_id, Default::default(), &setup)
            .await
            .expect("delete");
        let report = gc_namespace(&store, &namespace_id, &config(), &context(first_run_ms))
            .await
            .expect("unretired run");
        assert_eq!(report.deleted.upload_sessions, 0);
        assert_eq!(
            report.retained.upload_session_undecided + report.retained.upload_session_window,
            1
        );
        let key = loonfs_objectstore::keys::content_blob(
            &content_store_id,
            &namespace_id,
            &content.content_id,
        );
        assert!(store.head(&key).await.expect("object").is_some());
        let report = gc_namespace(
            &store,
            &namespace_id,
            &config(),
            &context(report.reclaim_after_ms.expect("retired")),
        )
        .await
        .expect("retired run");
        assert_eq!(report.deleted.upload_sessions, 1);
        assert_eq!(report.deleted.content_objects, 1);
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
    use loonfs_api::wire::control::{encode_control_state, ControlObjectKind, HintState};
    let directory = tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let setup = context(1_000);
    bootstrap_namespace(&store, &namespace_id, &setup, false)
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
        crate::checkpoint::flush_wal(&store, &namespace_id, &setup)
            .await
            .expect("flush");
    }
    let current = crate::namespace::control::load_current_manifest(&store, &namespace_id)
        .await
        .expect("current");
    let bytes = encode_control_state(
        ControlObjectKind::Hint,
        &HintState {
            namespace_id: namespace_id.clone(),
            manifest_no: ManifestNo(1),
            wal_no: loonfs_api::WalNo(0),
        },
    )
    .expect("hint");
    store
        .put_overwrite(
            &loonfs_objectstore::keys::hint(&namespace_id),
            Bytes::from(bytes),
        )
        .await
        .expect("rewind hint");
    let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
    let report = gc_namespace(&store, &namespace_id, &config(), &aged)
        .await
        .expect("collect");
    assert_eq!(report.deleted.manifests, 0);
    assert_eq!(
        crate::namespace::control::load_current_manifest(&store, &namespace_id)
            .await
            .expect("discovery after deferred sweep")
            .state,
        current.state
    );
    write_test_file(&store, &namespace_id, "/four", "four", &setup).await;
    crate::checkpoint::flush_wal(&store, &namespace_id, &setup)
        .await
        .expect("refresh hint by publishing");
    let current = crate::namespace::control::load_current_manifest(&store, &namespace_id)
        .await
        .expect("current");
    let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
    let report = gc_namespace(&store, &namespace_id, &config(), &aged)
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
            metadata_manifest_object(&namespace_id, &current.state.manifest.manifest_no),
        ]
    );
    assert_eq!(
        crate::namespace::control::load_current_manifest(&store, &namespace_id)
            .await
            .expect("discovery after sweep")
            .state,
        current.state
    );
}

#[tokio::test]
async fn concurrent_collectors_keep_pinned_and_current_roots_and_young_objects() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("concurrent").expect("namespace");
    let inner = LocalFsStore::new(directory.path()).expect("store");
    let setup = context(1_000);
    bootstrap_namespace(&inner, &namespace_id, &setup, false)
        .await
        .expect("bootstrap");
    write_test_file(&inner, &namespace_id, "/one", "one", &setup).await;
    let pin = create_checkpoint(&inner, &namespace_id, &setup)
        .await
        .expect("pin");
    write_test_file(&inner, &namespace_id, "/two", "two", &setup).await;
    crate::checkpoint::flush_wal(&inner, &namespace_id, &setup)
        .await
        .expect("flush");
    let protected: BTreeSet<_> = inner
        .list_prefix(&metadata_segment_prefix(&namespace_id))
        .await
        .expect("segments")
        .into_iter()
        .collect();
    let old = metadata_segment(
        &namespace_id,
        &loonfs_api::MetadataSegmentId::parse("seg_00000000000000000000000000000001")
            .expect("segment"),
    );
    let young = metadata_segment(
        &namespace_id,
        &loonfs_api::MetadataSegmentId::parse("seg_00000000000000000000000000000002")
            .expect("segment"),
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
        OperationClass::Delete,
    );
    store.block_next();
    let clock = context(UNREFERENCED_SEGMENT_MIN_AGE_MS + 1);
    let config = config();
    let (first, second) = tokio::join!(
        gc_namespace(&store, &namespace_id, &config, &clock),
        async {
            store.wait_until_blocked().await;
            let result = gc_namespace(store.inner(), &namespace_id, &config, &clock).await;
            store.release();
            result
        }
    );
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
async fn retired_owner_calls_restart_retry_deletes_and_collect_late_writes() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("owner-sweep").expect("namespace");
    let inner = MetadataMapStore::aged(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    let (content_store_id, deadline) = retired_content_namespace(&inner, &namespace_id).await;
    let keys = owned_content_keys(&inner, &content_store_id, &namespace_id).await;
    let store = RecordingStore::new(
        FailStore::new(
            inner,
            KeyPredicate::exact(&keys[0]),
            OperationClass::Delete,
            InjectedError::Transport("delete failed".to_owned()),
        ),
        KeyPredicate::any(),
    );
    let before = gc_namespace(
        &store,
        &namespace_id,
        &config(),
        &context(deadline.now_ms - 1),
    )
    .await
    .expect("before deadline");
    assert_eq!(before.deleted.retired_content_objects, 0);
    assert_eq!(before.next_reclamation_at_ms, Some(deadline.now_ms));
    gc_namespace(
        &store,
        &namespace_id,
        &config(),
        &context(deadline.now_ms - 1),
    )
    .await
    .expect("collect older manifests");
    store.inner().fail_next(1);
    assert!(gc_namespace(&store, &namespace_id, &config(), &deadline)
        .await
        .is_err());
    assert!(store.head(&keys[0]).await.expect("failed delete").is_some());
    let retried = gc_namespace(&store, &namespace_id, &config(), &deadline)
        .await
        .expect("retry owner sweep");
    assert_eq!(retried.deleted.retired_content_objects, 3);
    store
        .put_if_absent(&keys[0], Bytes::from_static(b"late write"))
        .await
        .expect("late content");
    let late = gc_namespace(&store, &namespace_id, &config(), &deadline)
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
async fn retired_owner_head_recheck_fails_without_writes() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("owner-head-recheck").expect("namespace");
    let inner = LocalFsStore::new(directory.path()).expect("store");
    let (content_store_id, deadline) = retired_content_namespace(&inner, &namespace_id).await;
    let keys = owned_content_keys(&inner, &content_store_id, &namespace_id).await;
    let store = RecordingStore::new(
        BlockingStore::new(
            FailStore::new(
                inner,
                KeyPredicate::manifest(&namespace_id),
                OperationClass::Read,
                InjectedError::Transport("head recheck failed".to_owned()),
            ),
            KeyPredicate::exact(loonfs_objectstore::keys::upload_session_prefix(
                &namespace_id,
            )),
            OperationClass::List,
        ),
        KeyPredicate::any(),
    );
    store.inner().block_next();
    let config = config();
    let (result, ()) = tokio::join!(
        gc_namespace(&store, &namespace_id, &config, &deadline),
        async {
            store.inner().wait_until_blocked().await;
            store.inner().inner().fail_next(1);
            store.inner().release();
        }
    );
    assert_eq!(
        result.expect_err("head recheck fails").code(),
        crate::error::ErrorCode::ServerError
    );
    assert_eq!(store.counts().deletes, 0);
    assert_eq!(store.counts().puts, 0);
    assert_eq!(store.counts().compare_and_swaps, 0);
    for key in keys {
        assert!(store.head(&key).await.expect("owned content").is_some());
    }
}

#[tokio::test]
async fn expiry_and_creation_grace_delete_pins_without_a_released_state() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("pin-grace").expect("namespace");
    let target = NamespaceId::parse("absent").expect("target");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let setup = context(1_000);
    bootstrap_namespace(&store, &namespace_id, &setup, false)
        .await
        .expect("bootstrap");
    let mut pins = Vec::new();
    for owner in [
        CheckpointOwner::User {
            name: "permanent".to_owned(),
            expires_at_ms: None,
        },
        CheckpointOwner::User {
            name: "expiring".to_owned(),
            expires_at_ms: Some(2_000),
        },
        CheckpointOwner::Snapshot {
            name: "snapshot".to_owned(),
            expires_at_ms: 2_000,
        },
        CheckpointOwner::Fork {
            target_namespace_id: target.clone(),
        },
    ] {
        pins.push(
            crate::checkpoint::create_checkpoint(&store, &namespace_id, owner, &setup)
                .await
                .expect("pin"),
        );
    }
    let before = gc_namespace(
        &store,
        &namespace_id,
        &config(),
        &context(1_000 + GRACE_MS - 1),
    )
    .await
    .expect("before creation grace");
    assert_eq!(
        before.deleted_checkpoints_by_owner,
        loonfs_api::DeletedCheckpointsByOwner::default()
    );
    let abandoned = gc_namespace(&store, &namespace_id, &config(), &context(1_000 + GRACE_MS))
        .await
        .expect("creation grace");
    assert_eq!(abandoned.deleted_checkpoints_by_owner.fork, 1);
    assert!(!checkpoint_exists(&store, &namespace_id, &pins[3].checkpoint_id).await);
    assert!(namespace_keys(&store, &target).await.is_empty());
    let before_expiry = gc_namespace(
        &store,
        &namespace_id,
        &config(),
        &context(2_000 + GRACE_MS - 1),
    )
    .await
    .expect("before expiry grace");
    assert_eq!(
        before_expiry.deleted_checkpoints_by_owner,
        loonfs_api::DeletedCheckpointsByOwner::default()
    );
    let expired = gc_namespace(&store, &namespace_id, &config(), &context(2_000 + GRACE_MS))
        .await
        .expect("expiry grace");
    assert_eq!(expired.deleted_checkpoints_by_owner.expired, 1);
    assert_eq!(expired.deleted_checkpoints_by_owner.snapshot, 1);
    assert!(checkpoint_exists(&store, &namespace_id, &pins[0].checkpoint_id).await);
    assert!(!checkpoint_exists(&store, &namespace_id, &pins[1].checkpoint_id).await);
    assert!(!checkpoint_exists(&store, &namespace_id, &pins[2].checkpoint_id).await);
    create_checkpoint(&store, &namespace_id, &setup)
        .await
        .expect("second permanent pin");
    let deleted_at = 3_000 + GRACE_MS;
    delete_namespace(
        &store,
        &namespace_id,
        Default::default(),
        &context(deleted_at),
    )
    .await
    .expect("delete");
    let retired = gc_namespace(&store, &namespace_id, &config(), &context(deleted_at))
        .await
        .expect("retire");
    assert_eq!(
        retired.deleted_checkpoints_by_owner,
        loonfs_api::DeletedCheckpointsByOwner {
            expired: 2,
            ..Default::default()
        }
    );
    assert_eq!(retired.reclaim_after_ms, Some(deleted_at + GRACE_MS));
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
    bootstrap_namespace(&store, &namespace_id, &setup, false)
        .await
        .expect("bootstrap");
    let initial = crate::checkpoint::create_checkpoint(
        &store,
        &namespace_id,
        CheckpointOwner::User {
            name: "initial".to_owned(),
            expires_at_ms: None,
        },
        &setup,
    )
    .await
    .expect("pin");
    write_test_file(&store, &namespace_id, "/file", "new", &setup).await;
    crate::checkpoint::flush_wal(&store, &namespace_id, &setup)
        .await
        .expect("flush");
    store
        .delete(&metadata_manifest_object(
            &namespace_id,
            &initial.manifest_no,
        ))
        .await
        .expect("delete manifest");
    store.reset();
    let error = gc_namespace(&store, &namespace_id, &config(), &context(GRACE_MS * 3))
        .await
        .expect_err("missing pin manifest");
    assert_eq!(error.code(), loonfs_api::ErrorCode::NamespaceCorrupt);
    assert!(error
        .to_string()
        .contains(&loonfs_objectstore::keys::checkpoint_record(
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
    bootstrap_namespace(&store, &source, &setup, false)
        .await
        .expect("bootstrap");
    let mut targets = Vec::new();
    for number in 0..6 {
        let target = NamespaceId::parse(format!("target-{number}")).expect("target");
        fork_namespace(&store, &source, &target, None, &setup)
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
    let young = gc_namespace(
        &store,
        &source,
        &config(),
        &context(setup.now_ms + GRACE_MS - 1),
    )
    .await
    .expect("young pins");
    assert_eq!(young.deleted_checkpoints_by_owner.fork, 0);
    assert!(store.snapshot().is_empty());
    let aged = gc_namespace(
        &store,
        &source,
        &config(),
        &context(setup.now_ms + GRACE_MS),
    )
    .await
    .expect("aged pins");
    assert_eq!(aged.deleted_checkpoints_by_owner.fork, 1);
    let mut expected = vec![hint(&targets[5])];
    for (index, target) in targets[..5].iter().enumerate() {
        let manifest_no = if index == 0 {
            ManifestNo(2)
        } else {
            ManifestNo(1)
        };
        expected.extend([
            hint(target),
            metadata_manifest_object(target, &manifest_no),
            metadata_manifest_object(target, &manifest_no.successor().expect("successor")),
        ]);
    }
    let mut actual = store.take_get_keys();
    actual.sort();
    expected.sort();
    assert_eq!(actual, expected);
    assert_eq!(
        store
            .inner()
            .list_prefix(&checkpoint_prefix(&source))
            .await
            .expect("source pins")
            .len(),
        5
    );
}
