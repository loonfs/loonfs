//! Behavior tests for namespace GC.

use super::config::GcConfig;
use super::live_set::collect_live_set;
use super::run::{gc_namespace, gc_namespace_with_reverify_chunk};
use crate::checkpoint::advance_retention_floor;
use crate::checkpoint::record::release_checkpoint_record;
use crate::checkpoint::tests::{create_checkpoint, mutation_context, write_test_file};
use crate::commit_engine::{CommitCandidate, NamespaceCommitEngine};
use crate::context::MutationContext;
use crate::error::CoreError;
use crate::limits::{
    CONTENT_RECLAMATION_GRACE_MS, FORK_CHECKPOINT_LEASE_MS, GC_MIN_GRACE_WINDOW_MS,
    UPLOAD_SESSION_LEASE_MS,
};
use crate::path::write::{CommitRequest, FilesystemOperation};
use loonfs_api::v0::GcResponse;
use loonfs_api::wire::control::{
    decode_control_object, CheckpointOwner, CheckpointRecordLifecycle, CheckpointRecordState,
    ControlObjectKind, UploadSessionLifecycle, UploadSessionState,
};
use loonfs_api::{ContentRef, ContentStoreId, NamespaceId, UploadId};
use loonfs_objectstore::keys::{
    checkpoint_prefix, metadata_manifest_object, metadata_manifest_prefix, metadata_table,
    metadata_table_prefix, wal_segment, wal_segment_prefix,
};
use loonfs_objectstore::ObjectStore;
use std::collections::BTreeSet;
use std::num::NonZeroUsize;

use crate::commit_engine::delete_namespace;
use crate::namespace::bootstrap::bootstrap_namespace;
use crate::namespace::fork::fork_namespace;
use crate::options::DeleteNamespaceOptions;
use crate::path::read::{load_metadata_view, ReadLoadContext};
use bytes::Bytes;
use futures::stream::BoxStream;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::{ByteRange, ObjectBody, ObjectMetadata, ObjectStoreError, PutMode};
use loonfs_test_support::stores::{
    BlockingStore, CountingStore, KeyPredicate, MetadataMapStore, OperationContext, OperationKind,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use tempfile::tempdir;

const GRACE_MS: u64 = 60 * 60 * 1000;

fn config() -> GcConfig {
    GcConfig {
        grace_window_ms: GRACE_MS,
        max_objects: None,
        cursor: None,
    }
}

fn context(now_ms: u64) -> MutationContext {
    mutation_context("gc-test", now_ms)
}

/// The durable lifecycle of one checkpoint record, stamp included.
async fn checkpoint_lifecycle<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    checkpoint_id: &loonfs_api::CheckpointId,
) -> CheckpointRecordLifecycle {
    crate::checkpoint::read_checkpoint_record(store, namespace_id, checkpoint_id)
        .await
        .expect("read checkpoint record")
        .expect("checkpoint record exists")
        .state
        .state
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
    load_metadata_view(store, namespace_id, ReadLoadContext::latest())
        .await
        .expect("load latest view")
        .resolve_path("/")
        .await
        .expect("resolve root");
}

#[derive(Debug)]
struct IncompleteGcAccountingStore {
    inner: LocalFsStore,
    deletes: AtomicUsize,
    lists: AtomicUsize,
}

#[derive(Debug, Clone, Copy)]
enum BlockingControlCasTarget {
    CheckpointReleased,
    UploadCompleted,
    UploadAborted,
}

impl BlockingControlCasTarget {
    fn matches(self, bytes: &[u8]) -> bool {
        match self {
            BlockingControlCasTarget::CheckpointReleased => {
                let Ok(envelope) = decode_control_object::<CheckpointRecordState>(
                    bytes,
                    ControlObjectKind::CheckpointRecord,
                ) else {
                    return false;
                };
                matches!(
                    envelope.state.state,
                    CheckpointRecordLifecycle::Released { .. }
                )
            }
            BlockingControlCasTarget::UploadCompleted | BlockingControlCasTarget::UploadAborted => {
                let Ok(envelope) = decode_control_object::<UploadSessionState>(
                    bytes,
                    ControlObjectKind::UploadSession,
                ) else {
                    return false;
                };
                match self {
                    BlockingControlCasTarget::UploadCompleted => matches!(
                        envelope.state.state,
                        UploadSessionLifecycle::Completed { .. }
                    ),
                    BlockingControlCasTarget::UploadAborted => {
                        matches!(envelope.state.state, UploadSessionLifecycle::Aborted { .. })
                    }
                    _ => false,
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

#[async_trait::async_trait]
impl ObjectStore for IncompleteGcAccountingStore {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.inner.head(key).await
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        self.inner.get_with_metadata(key).await
    }

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        self.inner.get(key, range).await
    }

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        self.inner.put(key, bytes, mode).await
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.deletes.fetch_add(1, Ordering::SeqCst);
        self.inner.delete(key).await
    }

    fn list_prefix_stream(
        &self,
        prefix: &str,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        self.lists.fetch_add(1, Ordering::SeqCst);
        self.inner.list_prefix_stream(prefix)
    }
}

/// The derived floor is enforced at validation: a pass configured below
/// it is rejected as an invalid request before touching the store.
#[tokio::test]
async fn gc_rejects_grace_windows_below_the_derived_minimum() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");

    let too_small = GcConfig {
        grace_window_ms: GC_MIN_GRACE_WINDOW_MS - 1,
        ..GcConfig::default()
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

    let zero_budget = GcConfig {
        max_objects: Some(0),
        ..config()
    };
    let error = gc_namespace(&store, &namespace_id, &zero_budget, &context(1_000))
        .await
        .expect_err("zero budget must be rejected");
    assert!(matches!(error, CoreError::InvalidGcConfig(_)));
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

    // The only segment sits at the floor with no replay gap above it.
    assert_eq!(report.deleted_wal_segments, 1);
    assert!(!report.degraded_retention);
    stat_root(&store, &namespace_id).await;
}

async fn write_upload_session(store: &LocalFsStore, namespace_id: &NamespaceId) -> String {
    let upload_id = loonfs_api::UploadId::parse("upl_0123456789abcdef0123456789abcdef")
        .expect("valid upload id");
    let state = loonfs_api::wire::control::UploadSessionState {
        namespace_id: namespace_id.clone(),
        upload_id: upload_id.clone(),
        mode: loonfs_api::v0::UploadMode::ServiceProxied,
        content_id: loonfs_api::ContentId::generate(),
        claimed_checksum: None,
        direct_put_content_ref: None,
        staged_content_ref: None,
        created_at_ms: 1_000,
        state: loonfs_api::wire::control::UploadSessionLifecycle::Open {
            expires_at_ms: 1_000 + UPLOAD_SESSION_LEASE_MS,
        },
        provider_multipart_upload_id: None,
        multipart_part_size_bytes: None,
    };
    let envelope = loonfs_api::wire::control::UploadSessionEnvelope::from_state(
        loonfs_api::wire::control::ControlObjectKind::UploadSession,
        state,
    )
    .expect("session envelope");
    let bytes =
        loonfs_api::wire::control::encode_control_object(&envelope).expect("encode session");
    let key = loonfs_objectstore::keys::upload_session(namespace_id.as_str(), upload_id.as_str());
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
        loonfs_api::v0::BeginUploadRequest::default(),
        context,
    )
    .await
    .expect("begin upload");
    let staged =
        crate::protocol::upload_content(store, namespace_id, &begin.upload_id, b"racing upload\n")
            .await
            .expect("stage upload");
    let content_store_id =
        crate::namespace::catalog::load_namespace_content_store_id(store, namespace_id)
            .await
            .expect("content store id");
    (begin.upload_id, staged.content_ref, content_store_id)
}

async fn read_upload_session<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    upload_id: &UploadId,
) -> Option<UploadSessionState> {
    let key = loonfs_objectstore::keys::upload_session(namespace_id.as_str(), upload_id.as_str());
    let body = store.get(&key, None).await.expect("read upload session")?;
    Some(
        decode_control_object::<UploadSessionState>(&body, ControlObjectKind::UploadSession)
            .expect("decode upload session")
            .state,
    )
}

#[tokio::test]
async fn active_record_with_a_missing_basis_is_released_not_degrading() {
    // The crash window between record write and verification can leave
    // an active record pinning a basis an earlier pass already deleted.
    // Such a record can never serve a read; the pass releases it with
    // the same compare-and-swap the creator's verification failure
    // would have run — and the absent basis never degrades sweeping.
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    bootstrap_namespace(&store, &namespace_id, &setup, false)
        .await
        .expect("bootstrap");
    write_test_file(&store, &namespace_id, "/docs/one.txt", "gc-one", &setup).await;
    let pinned = create_checkpoint(&store, &namespace_id, &setup)
        .await
        .expect("first checkpoint");

    // Advance the root past the pinned basis so deleting the basis
    // object leaves the namespace itself healthy.
    write_test_file(&store, &namespace_id, "/docs/two.txt", "gc-two", &setup).await;
    let moved_on = crate::checkpoint::create_checkpoint(
        &store,
        &namespace_id,
        CheckpointOwner::User {
            name: "other-pin".to_owned(),
        },
        None,
        &setup,
    )
    .await
    .expect("second checkpoint");
    assert_ne!(moved_on.manifest_id, pinned.manifest_id);

    // Simulate the crash residue: the pinned record stays active while
    // its basis manifest object vanishes.
    let record = crate::checkpoint::record::read_checkpoint_record(
        &store,
        &namespace_id,
        &pinned.checkpoint_id,
    )
    .await
    .expect("read record")
    .expect("record exists")
    .state;
    let basis_key = metadata_manifest_object(namespace_id.as_str(), &record.manifest_object_id);
    store.delete(&basis_key).await.expect("drop basis manifest");

    let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
    let report = gc_namespace(&store, &namespace_id, &config(), &aged)
        .await
        .expect("gc pass");
    assert_eq!(report.released_missing_basis_checkpoints, 1);
    assert!(
        !report.degraded_retention,
        "a verifiably absent basis is not ambiguity"
    );
    let released = crate::checkpoint::record::read_checkpoint_record(
        &store,
        &namespace_id,
        &pinned.checkpoint_id,
    )
    .await
    .expect("read record")
    .expect("record still present")
    .state;
    assert_eq!(
        released.state,
        loonfs_api::wire::control::CheckpointRecordLifecycle::Released {
            released_at_ms: aged.now_ms
        }
    );

    // Idempotent: the released record is no longer a zombie, and the
    // namespace still reads (the live pin and root are untouched).
    let again = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
    let report = gc_namespace(&store, &namespace_id, &config(), &again)
        .await
        .expect("second gc pass");
    assert_eq!(report.released_missing_basis_checkpoints, 0);
    assert!(!report.degraded_retention);
    stat_root(&store, &namespace_id).await;
}

#[tokio::test]
async fn deleted_namespace_reclaims_down_to_its_tombstone() {
    // A terminal namespace forgets: user pins, the final replay chain,
    // manifests, and tables all age out; only the id-retiring tombstone
    // objects survive. The user checkpoint here would have made the
    // tree immortal under the live rules.
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
    assert!(report.deleted_wal_segments >= 1);
    assert!(report.deleted_metadata_tables >= 1);
    assert!(report.deleted_manifests >= 1);
    // The pin on a tombstone has one route out, the same as every other
    // pin: released here, deleted a grace window after that release.
    assert_eq!(report.released_expired_checkpoints, 1);
    assert!(!report.degraded_retention);
    let reaped = context(aged.now_ms + GRACE_MS);
    let report = gc_namespace(&store, &namespace_id, &config(), &reaped)
        .await
        .expect("gc pass past the release grace window");
    assert!(report.deleted_checkpoint_records >= 1);
    assert!(!report.degraded_retention);

    for prefix in [
        wal_segment_prefix(namespace_id.as_str()),
        metadata_table_prefix(namespace_id.as_str()),
        metadata_manifest_prefix(namespace_id.as_str()),
        checkpoint_prefix(namespace_id.as_str()),
    ] {
        assert!(
            store.list_prefix(&prefix).await.expect("list").is_empty(),
            "prefix `{prefix}` must be empty after reclamation"
        );
    }
    // The tombstone is the head; the root and floor survive alongside it
    // wherever the namespace published them, because neither is ever a
    // collection candidate.
    for key in [
        loonfs_objectstore::keys::wal_head(namespace_id.as_str()),
        loonfs_objectstore::keys::metadata_root(namespace_id.as_str()),
    ] {
        assert!(
            store.head(&key).await.expect("head").is_some(),
            "tombstone object `{key}` must survive"
        );
    }

    // Idempotent, and never degraded by its own reclamation.
    let again = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
    let report = gc_namespace(&store, &namespace_id, &config(), &again)
        .await
        .expect("second gc pass");
    assert_eq!(report.deleted_wal_segments, 0);
    assert_eq!(report.deleted_manifests, 0);
    assert!(!report.degraded_retention);
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
    fork_namespace(&store, &source, &clone, &setup)
        .await
        .expect("fork");
    delete_namespace(&store, &source, DeleteNamespaceOptions::default(), &setup)
        .await
        .expect("delete source");

    // The deleted source keeps exactly what the living clone needs.
    let fork_record = read_fork_record(&store, &source).await;
    let basis_key = metadata_manifest_object(source.as_str(), &fork_record.manifest_object_id);
    let aged = context(now_after_newest_object(&store, &source, GRACE_MS + 1).await);
    let report = gc_namespace(&store, &source, &config(), &aged)
        .await
        .expect("gc pass with live clone");
    assert_eq!(report.released_fork_checkpoints, 0);
    assert!(!report.degraded_retention);
    assert!(
        store.head(&basis_key).await.expect("head basis").is_some(),
        "fork basis must survive while the clone lives"
    );
    let clone_view = load_metadata_view(&store, &clone, ReadLoadContext::latest())
        .await
        .expect("load clone view");
    clone_view
        .resolve_path("/docs/shared.txt")
        .await
        .expect("clone reads through the deleted source");

    // Once the clone is terminally deleted too, the record stops rooting at
    // collection time: its target is provably gone, and both namespaces are
    // immutable tombstones, so nothing will ever read through it again. One
    // pass reclaims the basis and releases the record.
    delete_namespace(&store, &clone, DeleteNamespaceOptions::default(), &setup)
        .await
        .expect("delete clone");
    let aged = context(now_after_newest_object(&store, &source, GRACE_MS + 1).await);
    let report = gc_namespace(&store, &source, &config(), &aged)
        .await
        .expect("gc pass after clone delete");
    assert_eq!(report.released_fork_checkpoints, 1);
    assert!(report.deleted_manifests >= 1);
    assert!(
        store.head(&basis_key).await.expect("head basis").is_none(),
        "the basis ages out once no living target needs it"
    );

    // Idempotent: the released record ages out on later passes and
    // nothing resurrects.
    let again = context(now_after_newest_object(&store, &source, GRACE_MS + 1).await);
    let report = gc_namespace(&store, &source, &config(), &again)
        .await
        .expect("idempotent pass");
    assert_eq!(report.released_fork_checkpoints, 0);
    assert_eq!(report.deleted_manifests, 0);
    assert!(!report.degraded_retention);
}

/// The whole upload arm end to end: a lease that passes turns into a
/// durable abort with its provider object gone, and the record itself
/// survives one more grace so the abort is observable before it is reaped.
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
    let session_key =
        loonfs_objectstore::keys::upload_session(namespace_id.as_str(), upload_id.as_str());
    let content_key =
        loonfs_objectstore::keys::content_blob(content_store_id.as_str(), &content_ref.content_id);

    // Inside the lease nothing happens, however old the object looks: the
    // session carries its own expiry, so no provider timestamp decides this.
    let inside = context(setup.now_ms + UPLOAD_SESSION_LEASE_MS - 1);
    let report = gc_namespace(&store, &namespace_id, &config(), &inside)
        .await
        .expect("gc pass inside the lease");
    assert_eq!(report.deleted_upload_sessions, 0);
    assert!(store.head(&content_key).await.expect("head").is_some());

    // Past the lease plus a grace the session is aborted and the object it
    // was writing is deleted — in that order.
    let expired = context(setup.now_ms + UPLOAD_SESSION_LEASE_MS + GRACE_MS + 1);
    let report = gc_namespace(&store, &namespace_id, &config(), &expired)
        .await
        .expect("gc pass past the lease");
    assert_eq!(
        report.deleted_upload_sessions, 0,
        "the record outlives its abort"
    );
    let session = read_upload_session(&store, &namespace_id, &upload_id)
        .await
        .expect("aborted session retained");
    assert!(matches!(
        session.state,
        UploadSessionLifecycle::Aborted { .. }
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
    assert_eq!(report.deleted_upload_sessions, 1);
    assert_eq!(
        report.deleted_content_objects, 0,
        "the abort half's unconditional cleanup is not a reclamation it can count"
    );
    assert!(store.head(&session_key).await.expect("head").is_none());

    let again = context(reaped.now_ms + GRACE_MS);
    let report = gc_namespace(&store, &namespace_id, &config(), &again)
        .await
        .expect("gc pass after the sweep");
    assert_eq!(report.deleted_upload_sessions, 0);
}

/// A session record written straight into the store, never touched by an
/// upload, still ages out on nothing but its own recorded lease.
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

    assert_eq!(report.deleted_upload_sessions, 1);
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
    let content_key =
        loonfs_objectstore::keys::content_blob(content_store_id.as_str(), &content_ref.content_id);
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
            &loonfs_api::v0::CompleteUploadRequest::for_content_ref(content_ref.clone()),
            &aged,
        )
        .await;
        store.release();
        result
    };
    let (report, completion) = tokio::join!(gc, complete);
    completion.expect("completion wins the blocked abort CAS");
    let report = report.expect("gc pass");
    assert_eq!(report.deleted_upload_sessions, 0);
    let session = read_upload_session(&store, &namespace_id, &upload_id)
        .await
        .expect("completed session retained");
    assert!(matches!(
        session.state,
        UploadSessionLifecycle::Completed { .. }
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
    let content_key =
        loonfs_objectstore::keys::content_blob(content_store_id.as_str(), &content_ref.content_id);
    let store = blocking_control_cas_store(store, BlockingControlCasTarget::UploadCompleted);
    let request = loonfs_api::v0::CompleteUploadRequest::for_content_ref(content_ref.clone());
    let completion = crate::protocol::complete_upload(
        &store,
        &namespace_id,
        &content_store_id,
        &upload_id,
        &request,
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
        session.state,
        UploadSessionLifecycle::Aborted { .. }
    ));
    assert!(
        store.head(&content_key).await.expect("head").is_none(),
        "the winning abort cleans up, and the losing completion does not resurrect"
    );
}

/// Runs one upload through to its durable completed state and hands back
/// everything the content half of the sweep reasons about.
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
        loonfs_api::v0::BeginUploadRequest::default(),
        context,
    )
    .await
    .expect("begin upload");
    let staged = crate::protocol::upload_content(store, namespace_id, &begin.upload_id, bytes)
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
        &begin.upload_id,
        &loonfs_api::v0::CompleteUploadRequest::for_content_ref(staged.content_ref.clone()),
        context,
    )
    .await
    .expect("complete upload");
    (
        begin.upload_id,
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
                    None,
                    FilesystemOperation::PutFile {
                        absolute_path: loonfs_api::AbsolutePath::parse(path).expect("path"),
                        content_ref,
                        behavior: loonfs_api::DestinationBehavior::NoReplace,
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

/// Inside the derived grace a completed session's content is untouchable,
/// because a receipt could still be minted for it and a commit carrying that
/// receipt could still be in flight.
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
    let content_key =
        loonfs_objectstore::keys::content_blob(content_store_id.as_str(), &content_ref.content_id);

    let inside = context(setup.now_ms + CONTENT_RECLAMATION_GRACE_MS - 1);
    let report = gc_namespace(&store, &namespace_id, &config(), &inside)
        .await
        .expect("gc pass inside the content grace");

    assert_eq!(report.deleted_upload_sessions, 0);
    assert_eq!(report.deleted_content_objects, 0);
    assert!(store.head(&content_key).await.expect("head").is_some());
    assert!(read_upload_session(&store, &namespace_id, &upload_id)
        .await
        .is_some());
}

/// Past the grace, content no metadata references is provably nobody's: no
/// receipt survives that could admit a commit for it, so the set of
/// references can no longer grow.
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
    let content_key =
        loonfs_objectstore::keys::content_blob(content_store_id.as_str(), &content_ref.content_id);

    let past = context(setup.now_ms + CONTENT_RECLAMATION_GRACE_MS + 1);
    let report = gc_namespace(&store, &namespace_id, &config(), &past)
        .await
        .expect("gc pass past the content grace");

    assert_eq!(report.deleted_upload_sessions, 1);
    assert_eq!(report.deleted_content_objects, 1);
    assert!(
        store.head(&content_key).await.expect("head").is_none(),
        "completed content nothing published is reclaimable"
    );
    assert!(read_upload_session(&store, &namespace_id, &upload_id)
        .await
        .is_none());
}

/// Published content is metadata's now. The session record still ages out —
/// it has nothing left to say — but the object it named stays, whether the
/// commit that referenced it is still only in the WAL or already
/// materialized into a manifest.
#[tokio::test]
async fn content_gc_never_reclaims_published_content() {
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
        let content_key = loonfs_objectstore::keys::content_blob(
            content_store_id.as_str(),
            &content_ref.content_id,
        );

        let past = context(setup.now_ms + CONTENT_RECLAMATION_GRACE_MS + 1);
        let report = gc_namespace(&store, &namespace_id, &config(), &past)
            .await
            .expect("gc pass past the content grace");

        assert_eq!(
            report.deleted_upload_sessions, 1,
            "materialize={materialize}"
        );
        assert_eq!(
            report.deleted_content_objects, 0,
            "materialize={materialize}"
        );
        assert!(
            store.head(&content_key).await.expect("head").is_some(),
            "published content survives its session (materialize={materialize})"
        );
        assert!(read_upload_session(&store, &namespace_id, &upload_id)
            .await
            .is_none());
        assert!(!report.degraded_retention);
    }
}

/// Builds a namespace whose content reference scan has real work to do: a
/// materialized manifest to open and page through, and a WAL tail to fetch
/// on top of it.
async fn namespace_with_a_scan_worth_bounding(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
    setup: &MutationContext,
) {
    bootstrap_namespace(store, namespace_id, setup, false)
        .await
        .expect("bootstrap");
    for index in 0..3 {
        write_test_file(
            store,
            namespace_id,
            &format!("/docs/materialized-{index}.txt"),
            &format!("scan-fixture-{index}"),
            setup,
        )
        .await;
    }
    crate::checkpoint::flush_wal(store, namespace_id, setup)
        .await
        .expect("flush wal");
    for index in 0..3 {
        write_test_file(
            store,
            namespace_id,
            &format!("/docs/tail-{index}.txt"),
            &format!("scan-fixture-tail-{index}"),
            setup,
        )
        .await;
    }
}

/// The reference scan is the one place a bounded pass used to do unbounded
/// work. A one-object budget cannot finish it, and the honest response to
/// that is to reclaim nothing and say so: the session and its content stay
/// exactly where they were, `content_reclamation_deferred` reports the
/// skip, and the walk keeps moving past the session rather than pinning
/// itself to it. A later pass with room for the scan reaches the verdict an
/// unbounded pass would have reached.
#[tokio::test]
async fn a_budget_that_dies_inside_the_reference_scan_defers_and_walks_on() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    namespace_with_a_scan_worth_bounding(&store, &namespace_id, &setup).await;
    let (upload_id, content_ref, content_store_id, _prepared) =
        complete_upload_for_gc(&store, &namespace_id, b"unpublished\n", &setup).await;
    let content_key =
        loonfs_objectstore::keys::content_blob(content_store_id.as_str(), &content_ref.content_id);
    let live = collect_live_set(&store, &namespace_id, &setup)
        .await
        .expect("collect live set");
    assert!(
        !live.manifests.is_empty() && !live.wal_segments.is_empty(),
        "the fixture must give the scan more than one object to read"
    );

    // One object per pass: the sweep advances a key at a time until it
    // reaches the session, and the scan behind that session never fits in
    // one object. The walk has to get past it anyway.
    let past = context(setup.now_ms + CONTENT_RECLAMATION_GRACE_MS + 1);
    let mut tiny = config();
    tiny.max_objects = Some(1);
    let mut cursor: Option<String> = None;
    let mut deferred = false;
    let mut passes = 0;
    loop {
        passes += 1;
        assert!(
            passes <= 64,
            "a one-object budget must still walk the namespace to the end"
        );
        tiny.cursor.clone_from(&cursor);
        let pass = gc_namespace(&store, &namespace_id, &tiny, &past)
            .await
            .expect("one-object pass");
        assert_eq!(pass.deleted_upload_sessions, 0);
        assert_eq!(pass.deleted_content_objects, 0);
        deferred |= pass.content_reclamation_deferred;
        let Some(next) = pass.next_cursor else {
            break;
        };
        // The whole point of deferring rather than parking: a pass either
        // finishes or hands back a cursor strictly past the one it came in
        // with. It never asks to be run again from where it started.
        assert_ne!(
            Some(next.as_str()),
            cursor.as_deref(),
            "pass {passes} handed back the cursor it came in with"
        );
        cursor = Some(next);
    }
    assert!(
        deferred,
        "a one-object budget cannot afford the scan, and the pass must say so"
    );
    assert!(
        store.head(&content_key).await.expect("head").is_some(),
        "a deferred pass reclaims nothing"
    );
    assert!(
        read_upload_session(&store, &namespace_id, &upload_id)
            .await
            .is_some(),
        "the session that triggered the scan is retained, not reclaimed"
    );

    // Try again with a budget the scan fits inside: now it decides.
    let mut enough = config();
    enough.max_objects = Some(1_024);
    let resumed = gc_namespace(&store, &namespace_id, &enough, &past)
        .await
        .expect("pass with room for the scan");
    assert!(!resumed.content_reclamation_deferred);
    assert_eq!(resumed.deleted_upload_sessions, 1);
    assert_eq!(resumed.deleted_content_objects, 1);
    assert!(store.head(&content_key).await.expect("head").is_none());
    assert!(read_upload_session(&store, &namespace_id, &upload_id)
        .await
        .is_none());
}

/// The only reference keeping this content alive lives in the newest WAL
/// segment, the very last root the scan reads. A pass that stopped short
/// and answered from what it had collected would call the content
/// unreferenced and delete it. Running every budget from one up past the
/// whole scan's cost, each to the end of its walk, leaves no interleaving
/// where a partial reference set decides anything — and the budgets that
/// can afford the scan still reach the right verdict.
#[tokio::test]
async fn no_budget_lets_a_partial_reference_set_decide_a_deletion() {
    let temp_dir = tempdir().expect("tempdir");
    let seed_root = temp_dir.path().join("seed");
    let seed = LocalFsStore::new(&seed_root).expect("seed store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    namespace_with_a_scan_worth_bounding(&seed, &namespace_id, &setup).await;
    let (upload_id, content_ref, content_store_id, prepared) =
        complete_upload_for_gc(&seed, &namespace_id, b"published-last\n", &setup).await;
    // The publish that saves this content lands in the newest WAL segment,
    // so the reference sorts behind everything else the scan reads.
    publish_completed_content(
        &seed,
        &namespace_id,
        "/docs/published.txt",
        content_ref.clone(),
        prepared,
        &setup,
    )
    .await;
    let content_key =
        loonfs_objectstore::keys::content_blob(content_store_id.as_str(), &content_ref.content_id);
    let past = context(setup.now_ms + CONTENT_RECLAMATION_GRACE_MS + 1);
    let mut some_budget_reached_the_verdict = false;
    let mut some_budget_deferred_instead = false;

    for max_objects in 1..=16 {
        let trial_root = temp_dir.path().join(format!("trial-{max_objects}"));
        copy_tree(&seed_root, &trial_root);
        let store = LocalFsStore::new(&trial_root).expect("trial store");
        let mut bounded = config();
        bounded.max_objects = Some(max_objects);
        let mut cursor: Option<String> = None;
        let mut deferred = false;
        let mut passes = 0;
        loop {
            passes += 1;
            assert!(
                passes <= 256,
                "max_objects={max_objects}: the walk must reach its end"
            );
            bounded.cursor.clone_from(&cursor);
            let pass = gc_namespace(&store, &namespace_id, &bounded, &past)
                .await
                .expect("bounded pass");
            assert_eq!(pass.deleted_content_objects, 0, "max_objects={max_objects}");
            assert!(
                store.head(&content_key).await.expect("head").is_some(),
                "max_objects={max_objects}: referenced content survives every budget"
            );
            deferred |= pass.content_reclamation_deferred;
            let Some(next) = pass.next_cursor else {
                break;
            };
            assert_ne!(
                Some(next.as_str()),
                cursor.as_deref(),
                "max_objects={max_objects}: pass {passes} handed back its own cursor"
            );
            cursor = Some(next);
        }
        assert!(
            store.head(&content_key).await.expect("head").is_some(),
            "max_objects={max_objects}"
        );
        // Reaching the referenced verdict is what deletes the record: the
        // content is metadata's from here on. A surviving record means the
        // budget deferred instead — the case this test is really about —
        // and the two must line up exactly, because a session this walk
        // passed over is a session the reference scan could not afford.
        let decided = read_upload_session(&store, &namespace_id, &upload_id)
            .await
            .is_none();
        assert_eq!(
            deferred, !decided,
            "max_objects={max_objects}: the session survives exactly when the scan was deferred"
        );
        some_budget_reached_the_verdict |= decided;
        some_budget_deferred_instead |= deferred;
    }

    assert!(
        some_budget_reached_the_verdict,
        "a budget large enough to finish the scan must still decide the session"
    );
    assert!(
        some_budget_deferred_instead,
        "the sweep must actually run out mid-scan somewhere in this range, or \
         the test proves nothing about partial reference sets"
    );
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

    let young = context(now_after_newest_object(&store, &namespace_id, 0).await);
    let report = gc_namespace(&store, &namespace_id, &config(), &young)
        .await
        .expect("gc pass");

    assert_eq!(report.deleted_wal_segments, 0);
    assert_eq!(report.deleted_metadata_tables, 0);
    assert_eq!(report.deleted_manifests, 0);
    assert!(report.retained_candidates > 0);
    stat_root(&store, &namespace_id).await;
}

#[tokio::test]
async fn gc_never_deletes_the_live_replay_chain() {
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
    // A commit past the floor: its segment is the live replay gap.
    write_test_file(&store, &namespace_id, "/docs/two.txt", "gc-two", &setup).await;

    let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
    let report = gc_namespace(&store, &namespace_id, &config(), &aged)
        .await
        .expect("gc pass");

    assert_eq!(report.deleted_wal_segments, 1);
    // Latest reads replay the retained tail over the root basis.
    let view = load_metadata_view(&store, &namespace_id, ReadLoadContext::latest())
        .await
        .expect("load view");
    view.resolve_path("/docs/two.txt")
        .await
        .expect("tail commit stays readable");
}

/// A released record whose basis has aged out loses the record first,
/// and the basis only on the following pass — never the other way
/// around.
#[tokio::test]
async fn gc_reaps_dead_checkpoints_before_their_basis_across_passes() {
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
    let first_record =
        crate::checkpoint::read_checkpoint_record(&store, &namespace_id, &first.checkpoint_id)
            .await
            .expect("read first record")
            .expect("first record exists")
            .state;
    release_checkpoint_record(&store, &namespace_id, &first.checkpoint_id, setup.now_ms)
        .await
        .expect("mark first dead");

    let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
    let first_pass = gc_namespace(&store, &namespace_id, &config(), &aged)
        .await
        .expect("first gc pass");

    // Pass one deletes the dead record but the record still rooted its
    // basis, so the referenced manifest and tables survive the pass.
    assert_eq!(first_pass.deleted_checkpoint_records, 1);
    assert!(!first_pass.degraded_retention);
    assert!(
        crate::checkpoint::read_checkpoint_record(&store, &namespace_id, &first.checkpoint_id)
            .await
            .expect("read record")
            .is_none()
    );
    let basis = crate::checkpoint::load_namespace_manifest_envelope(
        &store,
        &namespace_id,
        &first_record.manifest_object_id,
    )
    .await
    .expect("dead basis manifest survives its record");
    for file in &basis.payload.metadata_files {
        assert!(
            store
                .head(&file.object_key)
                .await
                .expect("head table")
                .is_some(),
            "dead basis table survives its record"
        );
    }

    // Pass two finds the basis unreferenced and aged, and reaps it.
    let second_pass = gc_namespace(&store, &namespace_id, &config(), &aged)
        .await
        .expect("second gc pass");
    assert!(
        second_pass.deleted_manifests >= 1,
        "dead basis manifest reaped once its record is gone"
    );
    assert!(crate::checkpoint::load_namespace_manifest_envelope(
        &store,
        &namespace_id,
        &first_record.manifest_object_id,
    )
    .await
    .is_err());
    stat_root(&store, &namespace_id).await;
}

/// Objects whose keys do not name a valid manifest are not proven GC
/// candidates, so the pass retains their exact bytes.
#[tokio::test]
async fn gc_retains_unrecognized_manifest_keys() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    bootstrap_namespace(&store, &namespace_id, &setup, false)
        .await
        .expect("bootstrap");

    let manifest_prefix = metadata_manifest_prefix(namespace_id.as_str());
    let foreign_objects = [
        (
            format!("{manifest_prefix}notes.txt"),
            b"foreign key".as_slice(),
        ),
        (
            format!("{manifest_prefix}invalid.manifest.json"),
            b"invalid manifest id".as_slice(),
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

    assert_eq!(report.deleted_manifests, 0);
    for (key, expected) in foreign_objects {
        let actual = store
            .get(&key, None)
            .await
            .expect("get foreign manifest-prefix object")
            .expect("unrecognized object is retained");
        assert_eq!(actual.as_ref(), expected);
    }
}

/// Repeated WAL flushes leave superseded manifests unpinned, so GC
/// reclaims them once aged. This is what keeps retained metadata
/// bounded when maintenance runs continuously.
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

    // Record-less maintenance: nothing accumulates under `checkpoints/`.
    assert!(
        store
            .list_prefix(&checkpoint_prefix(namespace_id.as_str()))
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
    // next two superseded it; only the root's manifest is reachable. Its
    // tables are all still referenced (a flush only appends L0 runs).
    assert_eq!(report.deleted_manifests, 2);
    assert!(!report.degraded_retention);
    let manifests_left = store
        .list_prefix(&metadata_manifest_prefix(namespace_id.as_str()))
        .await
        .expect("list manifests");
    assert_eq!(manifests_left.len(), 1, "only the live root manifest stays");

    // Reorganization folds the L0 runs into fresh base segments; the
    // superseded run tables then age out on the next pass.
    let fold_policy = crate::checkpoint::MetadataLsmPolicy {
        max_l0_runs: NonZeroUsize::MIN,
        ..Default::default()
    };
    for _ in 0..16 {
        let report =
            crate::checkpoint::reorganize_metadata_step(&store, &namespace_id, &setup, fold_policy)
                .await
                .expect("reorganize step");
        if matches!(
            report.outcome,
            crate::checkpoint::MetadataReorganizeOutcome::NotNeeded { .. }
        ) {
            break;
        }
    }
    let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
    let after_fold = gc_namespace(&store, &namespace_id, &config(), &aged)
        .await
        .expect("gc pass after reorganization");
    assert!(
        after_fold.deleted_metadata_tables > 0,
        "folded-away run tables become collectable"
    );
    assert!(!after_fold.degraded_retention);

    stat_root(&store, &namespace_id).await;
    let view = load_metadata_view(&store, &namespace_id, ReadLoadContext::latest())
        .await
        .expect("load view");
    for round in 0..3 {
        view.resolve_path(&format!("/docs/file-{round}.txt"))
            .await
            .expect("file readable after sweep");
    }
}

/// The user release lifecycle end to end: release flips the record,
/// the record reaps first, and the basis follows one pass later.
#[tokio::test]
async fn gc_reaps_released_checkpoints_before_their_basis_across_passes() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    bootstrap_namespace(&store, &namespace_id, &setup, false)
        .await
        .expect("bootstrap");
    write_test_file(&store, &namespace_id, "/docs/one.txt", "gc-one", &setup).await;
    let pinned = create_checkpoint(&store, &namespace_id, &setup)
        .await
        .expect("pin checkpoint");
    write_test_file(&store, &namespace_id, "/docs/two.txt", "gc-two", &setup).await;
    create_checkpoint(&store, &namespace_id, &setup)
        .await
        .expect("advance past the pinned basis");

    let first_release =
        crate::checkpoint::release_checkpoint(&store, &namespace_id, &pinned.checkpoint_id, &setup)
            .await
            .expect("release");
    assert!(first_release.was_active);
    let repeat_release =
        crate::checkpoint::release_checkpoint(&store, &namespace_id, &pinned.checkpoint_id, &setup)
            .await
            .expect("repeat release");
    assert!(!repeat_release.was_active);

    let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
    let first_pass = gc_namespace(&store, &namespace_id, &config(), &aged)
        .await
        .expect("first gc pass");
    assert_eq!(first_pass.deleted_checkpoint_records, 1);

    let second_pass = gc_namespace(&store, &namespace_id, &config(), &aged)
        .await
        .expect("second gc pass");
    assert!(second_pass.deleted_manifests >= 1);
    // Releasing an already-reaped record stays idempotent success.
    let after_reap =
        crate::checkpoint::release_checkpoint(&store, &namespace_id, &pinned.checkpoint_id, &setup)
            .await
            .expect("release after reap");
    assert!(!after_reap.was_active);
    stat_root(&store, &namespace_id).await;
}

/// Release runs one way to one end state, so a caller and a
/// garbage-collection pass asking for it at the same moment converge.
/// Whichever compare-and-swap lands writes the stamp; the other side sees
/// the end state it wanted and reports success without touching the record.
#[tokio::test]
async fn caller_release_and_expiry_release_converge_on_the_winners_stamp() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    bootstrap_namespace(&store, &namespace_id, &setup, false)
        .await
        .expect("bootstrap");
    write_test_file(&store, &namespace_id, "/docs/one.txt", "gc-one", &setup).await;
    let pin = |name: &'static str| {
        crate::checkpoint::create_checkpoint(
            &store,
            &namespace_id,
            CheckpointOwner::User {
                name: name.to_owned(),
            },
            Some(setup.now_ms + GRACE_MS),
            &setup,
        )
    };
    let pass_first = pin("pass-first").await.expect("expiring checkpoint");
    let caller_first = pin("caller-first").await.expect("expiring checkpoint");
    let expired = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);

    // The caller gets there first: the pass finds the record already
    // released, leaves the stamp alone, and counts no release of its own.
    let caller_stamp = expired.now_ms + 1;
    let released = crate::checkpoint::release_checkpoint(
        &store,
        &namespace_id,
        &caller_first.checkpoint_id,
        &context(caller_stamp),
    )
    .await
    .expect("caller release");
    assert!(released.was_active);
    let report = gc_namespace(&store, &namespace_id, &config(), &expired)
        .await
        .expect("gc pass");
    assert_eq!(
        report.released_expired_checkpoints, 1,
        "only the record the caller left alone is released here"
    );
    assert_eq!(
        checkpoint_lifecycle(&store, &namespace_id, &caller_first.checkpoint_id).await,
        CheckpointRecordLifecycle::Released {
            released_at_ms: caller_stamp
        },
        "the winner's stamp stands"
    );

    // The pass got there first: the caller reports the same end state, and
    // the pass's stamp is what ages the record out.
    assert_eq!(
        checkpoint_lifecycle(&store, &namespace_id, &pass_first.checkpoint_id).await,
        CheckpointRecordLifecycle::Released {
            released_at_ms: expired.now_ms
        }
    );
    let late = crate::checkpoint::release_checkpoint(
        &store,
        &namespace_id,
        &pass_first.checkpoint_id,
        &context(caller_stamp),
    )
    .await
    .expect("a release that lost is still success");
    assert!(!late.was_active);
    assert_eq!(
        checkpoint_lifecycle(&store, &namespace_id, &pass_first.checkpoint_id).await,
        CheckpointRecordLifecycle::Released {
            released_at_ms: expired.now_ms
        },
        "the loser rewrites nothing"
    );
}

/// The same convergence with the two compare-and-swaps genuinely in flight:
/// the pass's release is held mid-write while the caller's lands, so the
/// pass loses its etag. Losing is retention, never an error.
#[tokio::test]
async fn a_release_that_loses_its_etag_retains_without_erroring() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    bootstrap_namespace(&store, &namespace_id, &setup, false)
        .await
        .expect("bootstrap");
    write_test_file(&store, &namespace_id, "/docs/one.txt", "gc-one", &setup).await;
    let pinned = crate::checkpoint::create_checkpoint(
        &store,
        &namespace_id,
        CheckpointOwner::User {
            name: "short-lived".to_owned(),
        },
        Some(setup.now_ms + GRACE_MS),
        &setup,
    )
    .await
    .expect("expiring checkpoint");
    let expired = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
    let caller_stamp = expired.now_ms + 1;

    let store = blocking_control_cas_store(store, BlockingControlCasTarget::CheckpointReleased);
    let gc_config = config();
    let pass = gc_namespace(&store, &namespace_id, &gc_config, &expired);
    let caller = async {
        store.wait_until_blocked().await;
        let released = crate::checkpoint::release_checkpoint(
            &store,
            &namespace_id,
            &pinned.checkpoint_id,
            &context(caller_stamp),
        )
        .await;
        store.release();
        released
    };
    let (report, released) = tokio::join!(pass, caller);
    assert!(released.expect("caller release").was_active);
    let report = report.expect("the pass finishes");
    assert_eq!(report.released_expired_checkpoints, 0);
    assert_eq!(report.deleted_checkpoint_records, 0);
    assert_eq!(
        checkpoint_lifecycle(&store, &namespace_id, &pinned.checkpoint_id).await,
        CheckpointRecordLifecycle::Released {
            released_at_ms: caller_stamp
        }
    );
}

/// A released record waits out the grace window measured from its own
/// release stamp — not from any provider timestamp — and is deleted after
/// it, its basis following on the pass after that.
#[tokio::test]
async fn gc_deletes_a_released_record_only_after_its_release_ages() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    bootstrap_namespace(&store, &namespace_id, &setup, false)
        .await
        .expect("bootstrap");
    write_test_file(&store, &namespace_id, "/docs/one.txt", "gc-one", &setup).await;
    let pinned = create_checkpoint(&store, &namespace_id, &setup)
        .await
        .expect("pin checkpoint");
    write_test_file(&store, &namespace_id, "/docs/two.txt", "gc-two", &setup).await;
    create_checkpoint(&store, &namespace_id, &setup)
        .await
        .expect("advance past the pinned basis");

    // Release long after every object was written, so the object's own age
    // is far past the grace window and only the release stamp is young.
    let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS * 4).await);
    crate::checkpoint::release_checkpoint(&store, &namespace_id, &pinned.checkpoint_id, &aged)
        .await
        .expect("release");
    assert_eq!(
        checkpoint_lifecycle(&store, &namespace_id, &pinned.checkpoint_id).await,
        CheckpointRecordLifecycle::Released {
            released_at_ms: aged.now_ms
        }
    );

    let inside_grace = context(aged.now_ms + GRACE_MS - 1);
    let report = gc_namespace(&store, &namespace_id, &config(), &inside_grace)
        .await
        .expect("pass inside the release grace window");
    assert_eq!(
        report.deleted_checkpoint_records, 0,
        "an old object with a young release is retained"
    );
    assert!(crate::checkpoint::read_checkpoint_record(
        &store,
        &namespace_id,
        &pinned.checkpoint_id
    )
    .await
    .expect("read record")
    .is_some());

    let past_grace = context(aged.now_ms + GRACE_MS);
    let report = gc_namespace(&store, &namespace_id, &config(), &past_grace)
        .await
        .expect("pass past the release grace window");
    assert_eq!(report.deleted_checkpoint_records, 1);
    assert!(crate::checkpoint::read_checkpoint_record(
        &store,
        &namespace_id,
        &pinned.checkpoint_id
    )
    .await
    .expect("read record")
    .is_none());
}

/// An expiring pin protects until its expiry, then is released by the pass
/// that observes the expiry and follows the ordinary released cascade.
#[tokio::test]
async fn gc_reaps_expired_checkpoints_before_their_basis_across_passes() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    bootstrap_namespace(&store, &namespace_id, &setup, false)
        .await
        .expect("bootstrap");
    write_test_file(&store, &namespace_id, "/docs/one.txt", "gc-one", &setup).await;
    // Expiry compares the caller's `now_ms` against the record's stamp;
    // object ages come from provider timestamps. Pin one record already
    // expired at any provider-derived "now" and one that never expires.
    let expiring = crate::checkpoint::create_checkpoint(
        &store,
        &namespace_id,
        CheckpointOwner::User {
            name: "short-lived".to_owned(),
        },
        Some(setup.now_ms + GRACE_MS),
        &setup,
    )
    .await
    .expect("expiring checkpoint");
    let lasting = crate::checkpoint::create_checkpoint(
        &store,
        &namespace_id,
        CheckpointOwner::User {
            name: "long-lived".to_owned(),
        },
        Some(u64::MAX),
        &setup,
    )
    .await
    .expect("lasting checkpoint");
    write_test_file(&store, &namespace_id, "/docs/two.txt", "gc-two", &setup).await;
    create_checkpoint(&store, &namespace_id, &setup)
        .await
        .expect("advance past the expiring basis");

    // Past expiry: the pass releases the record, and only a later pass —
    // one grace window past the release stamp — deletes it.
    let expired = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
    assert!(
        expired.now_ms > 1_000 + GRACE_MS,
        "provider clock sits past the expiry"
    );
    let first_pass = gc_namespace(&store, &namespace_id, &config(), &expired)
        .await
        .expect("post-expiry pass");
    assert_eq!(first_pass.released_expired_checkpoints, 1);
    assert_eq!(first_pass.deleted_checkpoint_records, 0);
    assert_eq!(
        checkpoint_lifecycle(&store, &namespace_id, &expiring.checkpoint_id).await,
        CheckpointRecordLifecycle::Released {
            released_at_ms: expired.now_ms
        }
    );
    let aged_out = context(expired.now_ms + GRACE_MS);
    let second_pass = gc_namespace(&store, &namespace_id, &config(), &aged_out)
        .await
        .expect("second post-expiry pass");
    assert_eq!(second_pass.deleted_checkpoint_records, 1);
    assert!(crate::checkpoint::read_checkpoint_record(
        &store,
        &namespace_id,
        &expiring.checkpoint_id
    )
    .await
    .expect("read record")
    .is_none());
    // The unexpired pin — same basis, different owner — still roots it.
    let survivor =
        crate::checkpoint::read_checkpoint_record(&store, &namespace_id, &lasting.checkpoint_id)
            .await
            .expect("read lasting record")
            .expect("lasting record survives")
            .state;
    assert!(crate::checkpoint::load_namespace_manifest_envelope(
        &store,
        &namespace_id,
        &survivor.manifest_object_id,
    )
    .await
    .is_ok());
    stat_root(&store, &namespace_id).await;
}

/// Two owners of one basis hold two records: releasing one leaves the
/// other rooting the shared basis.
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
        },
        None,
        &setup,
    )
    .await
    .expect("first owner");
    let second = crate::checkpoint::create_checkpoint(
        &store,
        &namespace_id,
        CheckpointOwner::User {
            name: "releaser".to_owned(),
        },
        None,
        &setup,
    )
    .await
    .expect("second owner");
    assert_ne!(first.checkpoint_id, second.checkpoint_id);
    assert_eq!(first.manifest_id, second.manifest_id);
    write_test_file(&store, &namespace_id, "/docs/two.txt", "gc-two", &setup).await;
    create_checkpoint(&store, &namespace_id, &setup)
        .await
        .expect("advance past the shared basis");

    crate::checkpoint::release_checkpoint(&store, &namespace_id, &second.checkpoint_id, &setup)
        .await
        .expect("release one owner");
    let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
    let first_pass = gc_namespace(&store, &namespace_id, &config(), &aged)
        .await
        .expect("first gc pass");
    assert_eq!(first_pass.deleted_checkpoint_records, 1);
    let second_pass = gc_namespace(&store, &namespace_id, &config(), &aged)
        .await
        .expect("second gc pass");
    let _ = second_pass;
    assert!(
        crate::checkpoint::read_checkpoint_record(&store, &namespace_id, &first.checkpoint_id)
            .await
            .expect("read keeper record")
            .is_some(),
        "the surviving owner's record stays"
    );
    let keeper =
        crate::checkpoint::read_checkpoint_record(&store, &namespace_id, &first.checkpoint_id)
            .await
            .expect("read keeper record")
            .expect("keeper record exists")
            .state;
    assert!(
        crate::checkpoint::load_namespace_manifest_envelope(
            &store,
            &namespace_id,
            &keeper.manifest_object_id,
        )
        .await
        .is_ok(),
        "shared basis survives while any owner remains"
    );
}

/// Fork-owned records refuse the user release operation: their release
/// is decided by garbage collection from the fork target's fate.
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
    fork_namespace(&store, &source, &clone, &setup)
        .await
        .expect("fork");

    let fork_record = read_fork_record(&store, &source).await;

    let error =
        crate::checkpoint::release_checkpoint(&store, &source, &fork_record.checkpoint_id, &setup)
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
    assert!(report.deleted_manifests <= 1);
    assert_eq!(report.deleted_checkpoint_records, 0);
    let first_record =
        crate::checkpoint::read_checkpoint_record(&store, &namespace_id, &first.checkpoint_id)
            .await
            .expect("read first checkpoint")
            .expect("first checkpoint exists")
            .state;
    assert!(crate::checkpoint::load_namespace_manifest_envelope(
        &store,
        &namespace_id,
        &first_record.manifest_object_id,
    )
    .await
    .is_ok());
}

/// Reads the single fork-owned record a fork left under the source.
async fn read_fork_record(store: &LocalFsStore, source: &NamespaceId) -> CheckpointRecordState {
    for key in store
        .list_prefix(&checkpoint_prefix(source.as_str()))
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
        .state;
        if matches!(record.owner, CheckpointOwner::Fork { .. }) {
            return record;
        }
    }
    unreachable!("fork leaves one fork-owned record");
}

/// The fork-record cascade: a live target keeps the record a root; a
/// terminal target delete releases the record by compare-and-swap; the
/// record reaps a grace window after that release, and its basis on the
/// pass after that.
#[tokio::test]
async fn gc_releases_fork_checkpoints_of_terminally_deleted_targets_across_passes() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let source = NamespaceId::parse("source").expect("namespace id");
    let clone = NamespaceId::parse("clone").expect("namespace id");
    let setup = context(1_000);
    bootstrap_namespace(&store, &source, &setup, false)
        .await
        .expect("bootstrap");
    write_test_file(&store, &source, "/docs/one.txt", "gc-one", &setup).await;
    fork_namespace(&store, &source, &clone, &setup)
        .await
        .expect("fork");
    let fork_record = read_fork_record(&store, &source).await;
    // Advance the source root past the fork basis so the basis is
    // reachable only through the fork-owned record.
    write_test_file(&store, &source, "/docs/two.txt", "gc-two", &setup).await;
    create_checkpoint(&store, &source, &setup)
        .await
        .expect("advance root past the fork basis");

    let before = context(now_after_newest_object(&store, &source, GRACE_MS + 1).await);
    let report = gc_namespace(&store, &source, &config(), &before)
        .await
        .expect("gc with live target");
    assert_eq!(report.released_fork_checkpoints, 0);

    delete_namespace(&store, &clone, DeleteNamespaceOptions::default(), &setup)
        .await
        .expect("terminal delete of the fork target");
    let aged = context(now_after_newest_object(&store, &source, GRACE_MS + 1).await);

    // Pass one flips the record; the record still roots its basis.
    let first_pass = gc_namespace(&store, &source, &config(), &aged)
        .await
        .expect("first gc pass");
    assert_eq!(first_pass.released_fork_checkpoints, 1);
    assert_eq!(first_pass.deleted_checkpoint_records, 0);
    assert_eq!(
        checkpoint_lifecycle(&store, &source, &fork_record.checkpoint_id).await,
        CheckpointRecordLifecycle::Released {
            released_at_ms: aged.now_ms
        }
    );

    // The release stamp starts the record's own grace window.
    let aged_out = context(aged.now_ms + GRACE_MS);
    let second_pass = gc_namespace(&store, &source, &config(), &aged_out)
        .await
        .expect("second gc pass");
    assert_eq!(second_pass.deleted_checkpoint_records, 1);
    assert!(
        crate::checkpoint::load_namespace_manifest_envelope(
            &store,
            &source,
            &fork_record.manifest_object_id,
        )
        .await
        .is_ok(),
        "basis survives the pass that deletes its record"
    );

    // Pass three reaps the unreferenced basis.
    let third_pass = gc_namespace(&store, &source, &config(), &aged_out)
        .await
        .expect("third gc pass");
    assert!(third_pass.deleted_manifests >= 1);
    assert!(crate::checkpoint::load_namespace_manifest_envelope(
        &store,
        &source,
        &fork_record.manifest_object_id,
    )
    .await
    .is_err());
    stat_root(&store, &source).await;
}

/// A finished fork owns its source pin for as long as the target lives.
/// The lease bounds the attempt, not the result: once the target head is
/// there, no number of passes at any clock past the lease can release the
/// record or reach the basis behind it.
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
    fork_namespace(&store, &source, &clone, &setup)
        .await
        .expect("fork");
    let fork_record = read_fork_record(&store, &source).await;
    assert!(
        fork_record.expires_at_ms.is_some(),
        "a fork record carries the attempt's lease"
    );
    // Only the fork-owned record can protect the basis after this.
    write_test_file(&store, &source, "/docs/two.txt", "gc-two", &setup).await;
    create_checkpoint(&store, &source, &setup)
        .await
        .expect("advance root past the fork basis");

    // Every clock: inside the lease, one tick past it, and absurdly past it.
    let lease = fork_record.expires_at_ms.expect("lease");
    for now_ms in [
        now_after_newest_object(&store, &source, GRACE_MS + 1).await,
        lease,
        lease + FORK_CHECKPOINT_LEASE_MS,
        u64::MAX / 2,
    ] {
        let report = gc_namespace(&store, &source, &config(), &context(now_ms))
            .await
            .expect("gc pass with a live target");
        assert_eq!(report.released_fork_checkpoints, 0, "at {now_ms}");
        assert_eq!(report.released_expired_checkpoints, 0, "at {now_ms}");
        assert_eq!(
            checkpoint_lifecycle(&store, &source, &fork_record.checkpoint_id).await,
            CheckpointRecordLifecycle::Active,
            "a live target keeps its pin at {now_ms}"
        );
    }
    assert!(crate::checkpoint::load_namespace_manifest_envelope(
        &store,
        &source,
        &fork_record.manifest_object_id,
    )
    .await
    .is_ok());
    load_metadata_view(&store, &clone, ReadLoadContext::latest())
        .await
        .expect("target readable after every pass")
        .resolve_path("/docs/one.txt")
        .await
        .expect("forked file readable");
}

/// The abandoned-fork arm: an attempt that never installed its target head
/// is proven abandoned by its own lease, and by nothing else. The source's
/// basis is safe for the whole lease, and the record then follows the
/// ordinary released cascade.
#[tokio::test]
async fn gc_releases_abandoned_fork_checkpoints_once_the_lease_expires() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let source = NamespaceId::parse("source").expect("namespace id");
    let clone = NamespaceId::parse("clone").expect("namespace id");
    let setup = context(1_000);
    bootstrap_namespace(&store, &source, &setup, false)
        .await
        .expect("bootstrap");
    write_test_file(&store, &source, "/docs/one.txt", "gc-one", &setup).await;
    // The tightest legal grace window, so a clock inside the lease can still
    // be well past every object's own age: the point of the arm is that the
    // lease decides, not the ages.
    let tight = GcConfig {
        grace_window_ms: GC_MIN_GRACE_WINDOW_MS,
        max_objects: None,
        cursor: None,
    };
    // The crash window itself: the fork wrote its leased source record and
    // died before installing the target head, so nothing under the target
    // prefix ever existed.
    let attempt = context(now_after_newest_object(&store, &source, 0).await);
    let lease = attempt.now_ms + FORK_CHECKPOINT_LEASE_MS;
    let abandoned = crate::checkpoint::create_checkpoint(
        &store,
        &source,
        CheckpointOwner::Fork {
            target_namespace_id: clone.clone(),
        },
        Some(lease),
        &attempt,
    )
    .await
    .expect("leased fork record");
    let fork_record = read_fork_record(&store, &source).await;
    write_test_file(&store, &source, "/docs/two.txt", "gc-two", &setup).await;
    create_checkpoint(&store, &source, &setup)
        .await
        .expect("advance root past the abandoned basis");

    // Inside the lease the record is a root, whatever the object ages say:
    // a live retry could still be between its two writes.
    assert!(
        lease - 1 > attempt.now_ms + tight.grace_window_ms,
        "the second clock below is past the grace window and still inside the lease"
    );
    for now_ms in [attempt.now_ms + tight.grace_window_ms + 1, lease - 1] {
        let report = gc_namespace(&store, &source, &tight, &context(now_ms))
            .await
            .expect("gc inside the lease");
        assert_eq!(report.released_fork_checkpoints, 0, "at {now_ms}");
        assert_eq!(
            checkpoint_lifecycle(&store, &source, &abandoned.checkpoint_id).await,
            CheckpointRecordLifecycle::Active
        );
        assert!(crate::checkpoint::load_namespace_manifest_envelope(
            &store,
            &source,
            &fork_record.manifest_object_id,
        )
        .await
        .is_ok());
    }

    // Past the lease: the attempt is provably gone.
    let expired = context(lease);
    let report = gc_namespace(&store, &source, &tight, &expired)
        .await
        .expect("gc past the lease");
    assert_eq!(report.released_fork_checkpoints, 1);
    assert_eq!(
        checkpoint_lifecycle(&store, &source, &abandoned.checkpoint_id).await,
        CheckpointRecordLifecycle::Released {
            released_at_ms: expired.now_ms
        }
    );

    // From there it is an ordinary released record.
    let aged_out = context(expired.now_ms + tight.grace_window_ms);
    let reaping = gc_namespace(&store, &source, &tight, &aged_out)
        .await
        .expect("gc past the release grace window");
    assert_eq!(reaping.deleted_checkpoint_records, 1);
    stat_root(&store, &source).await;
}

/// A fork retry after an abandoned attempt is simply another attempt: it
/// takes its own record under its own id, and the abandoned one is left to
/// age out on its own schedule.
#[tokio::test]
async fn a_fork_retry_after_abandonment_takes_a_record_of_its_own() {
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
        Some(setup.now_ms + FORK_CHECKPOINT_LEASE_MS),
        &setup,
    )
    .await
    .expect("leased fork record from the attempt that died");

    fork_namespace(&store, &source, &clone, &setup)
        .await
        .expect("fork retry after abandonment");
    let retry = store
        .list_prefix(&checkpoint_prefix(source.as_str()))
        .await
        .expect("list checkpoints")
        .len();
    assert_eq!(retry, 2, "the retry pins for itself instead of reusing");
    assert_eq!(
        checkpoint_lifecycle(&store, &source, &abandoned.checkpoint_id).await,
        CheckpointRecordLifecycle::Active,
        "the abandoned record is untouched; its lease ends it"
    );
    load_metadata_view(&store, &clone, ReadLoadContext::latest())
        .await
        .expect("target readable after retry")
        .resolve_path("/docs/one.txt")
        .await
        .expect("forked file readable");
}

/// Unreadable checkpoint records are ambiguous roots on their own,
/// without any pin involved: the record is retained and the pass
/// degrades.
#[tokio::test]
async fn gc_retains_unreadable_checkpoint_records_and_degrades() {
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

    for key in store
        .list_prefix(&checkpoint_prefix(namespace_id.as_str()))
        .await
        .expect("list checkpoints")
    {
        store
            .put_overwrite(&key, bytes::Bytes::from_static(b"not json"))
            .await
            .expect("corrupt record");
    }

    let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
    let report = gc_namespace(&store, &namespace_id, &config(), &aged)
        .await
        .expect("gc pass");
    assert!(report.degraded_retention);
    assert_eq!(report.deleted_checkpoint_records, 0);
    assert_eq!(report.deleted_manifests, 0);
    assert_eq!(report.deleted_metadata_tables, 0);
    assert!(
        !store
            .list_prefix(&checkpoint_prefix(namespace_id.as_str()))
            .await
            .expect("list checkpoints")
            .is_empty(),
        "unreadable record retained"
    );
}

/// Rule 1's timestamp arm: an object without a provider timestamp reads
/// as young, so a store that reports none never deletes anything.
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

    assert_eq!(report.deleted_wal_segments, 0);
    assert_eq!(report.deleted_metadata_tables, 0);
    assert_eq!(report.deleted_manifests, 0);
    assert_eq!(report.deleted_checkpoint_records, 0);
    assert_eq!(report.released_fork_checkpoints, 0);
    assert!(report.retained_candidates > 0);
    stat_root(&store, &namespace_id).await;
}

/// The chunked delete-time re-verification path (rule 3) must reach the
/// same outcomes as a whole-batch sweep; chunk size one re-collects the
/// live set before every candidate.
#[tokio::test]
async fn gc_sweep_reverification_chunks_preserve_outcomes() {
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
    release_checkpoint_record(&store, &namespace_id, &first.checkpoint_id, setup.now_ms)
        .await
        .expect("mark first dead");

    let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
    let first_pass = gc_namespace_with_reverify_chunk(&store, &namespace_id, &config(), &aged, 1)
        .await
        .expect("first gc pass");
    assert_eq!(first_pass.deleted_checkpoint_records, 1);
    assert!(!first_pass.degraded_retention);

    let second_pass = gc_namespace_with_reverify_chunk(&store, &namespace_id, &config(), &aged, 1)
        .await
        .expect("second gc pass");
    assert!(second_pass.deleted_manifests >= 1);
    stat_root(&store, &namespace_id).await;
}

/// A namespace with no head does not exist, so the pass lists nothing and
/// deletes nothing: the head is every installation's first and only write,
/// so nothing can be under the prefix without it.
#[tokio::test]
async fn gc_of_an_absent_namespace_lists_and_deletes_nothing() {
    let temp_dir = tempdir().expect("tempdir");
    let inner = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("orphan").expect("namespace id");
    let store = IncompleteGcAccountingStore {
        inner,
        deletes: AtomicUsize::new(0),
        lists: AtomicUsize::new(0),
    };

    let report = gc_namespace(&store, &namespace_id, &config(), &context(u64::MAX))
        .await
        .expect("gc absent namespace");
    assert_eq!(report, GcResponse::empty(namespace_id.clone()));
    assert_eq!(store.lists.load(Ordering::SeqCst), 0);
    assert_eq!(store.deletes.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn gc_degrades_to_retention_when_a_pin_checkpoint_is_unreadable() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let source = NamespaceId::parse("source").expect("namespace id");
    let clone = NamespaceId::parse("clone").expect("namespace id");
    let setup = context(1_000);
    bootstrap_namespace(&store, &source, &setup, false)
        .await
        .expect("bootstrap");
    write_test_file(&store, &source, "/docs/one.txt", "gc-one", &setup).await;
    fork_namespace(&store, &source, &clone, &setup)
        .await
        .expect("fork");

    // Corrupt the pinned checkpoint: ambiguous roots must retain.
    for key in store
        .list_prefix(&loonfs_objectstore::keys::checkpoint_prefix(
            source.as_str(),
        ))
        .await
        .expect("list checkpoints")
    {
        store
            .put_overwrite(&key, bytes::Bytes::from_static(b"not json"))
            .await
            .expect("corrupt record");
    }

    let aged = context(now_after_newest_object(&store, &source, GRACE_MS + 1).await);
    let report = gc_namespace(&store, &source, &config(), &aged)
        .await
        .expect("gc pass");
    assert!(report.degraded_retention);
    assert_eq!(report.deleted_manifests, 0);
    assert_eq!(report.deleted_metadata_tables, 0);
}

async fn add_bounded_gc_fixture(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
    setup: &MutationContext,
) {
    bootstrap_namespace(store, namespace_id, setup, false)
        .await
        .expect("bootstrap");
    let mut checkpoints = Vec::new();
    for index in 0..6 {
        write_test_file(
            store,
            namespace_id,
            &format!("/docs/{index}.txt"),
            &format!("bounded-gc-{index}"),
            setup,
        )
        .await;
        checkpoints.push(
            create_checkpoint(store, namespace_id, setup)
                .await
                .expect("checkpoint"),
        );
    }
    for checkpoint in &checkpoints[..checkpoints.len() - 1] {
        release_checkpoint_record(store, namespace_id, &checkpoint.checkpoint_id, setup.now_ms)
            .await
            .expect("release checkpoint");
    }
    advance_retention_floor(store, namespace_id, setup)
        .await
        .expect("advance floor");

    for index in 0..6 {
        for key in [
            wal_segment(
                namespace_id.as_str(),
                &format!("00000000000000000000-orphan-{index:02}"),
            ),
            metadata_table(namespace_id.as_str(), &format!("000-orphan-{index:02}")),
            format!(
                "{}000-orphan-{index:02}.manifest.json",
                metadata_manifest_prefix(namespace_id.as_str())
            ),
        ] {
            store
                .put_if_absent(&key, Bytes::from_static(b"orphan"))
                .await
                .expect("write orphan");
        }
    }
    write_upload_session(store, namespace_id).await;
}

fn copy_tree(source: &std::path::Path, target: &std::path::Path) {
    std::fs::create_dir_all(target).expect("create copied store directory");
    for entry in std::fs::read_dir(source).expect("read source store") {
        let entry = entry.expect("read source entry");
        let source_path = entry.path();
        let target_path = target.join(entry.file_name());
        if entry.file_type().expect("read source file type").is_dir() {
            copy_tree(&source_path, &target_path);
        } else {
            std::fs::copy(&source_path, &target_path).expect("copy store object");
        }
    }
}

async fn namespace_keys(store: &LocalFsStore, namespace_id: &NamespaceId) -> BTreeSet<String> {
    store
        .list_prefix(&loonfs_objectstore::keys::namespace_prefix(namespace_id))
        .await
        .expect("list namespace")
        .into_iter()
        .collect()
}

fn accumulate_report(total: &mut GcResponse, pass: &GcResponse) {
    total.deleted_wal_segments += pass.deleted_wal_segments;
    total.deleted_metadata_tables += pass.deleted_metadata_tables;
    total.deleted_manifests += pass.deleted_manifests;
    total.deleted_checkpoint_records += pass.deleted_checkpoint_records;
    total.released_fork_checkpoints += pass.released_fork_checkpoints;
    total.deleted_upload_sessions += pass.deleted_upload_sessions;
    total.deleted_content_objects += pass.deleted_content_objects;
    total.released_missing_basis_checkpoints += pass.released_missing_basis_checkpoints;
    total.retained_candidates += pass.retained_candidates;
    total.degraded_retention |= pass.degraded_retention;
    total.content_reclamation_deferred |= pass.content_reclamation_deferred;
}

#[tokio::test]
async fn bounded_passes_delete_exactly_the_unbounded_pass_set() {
    let temp_dir = tempdir().expect("tempdir");
    let unbounded_root = temp_dir.path().join("unbounded");
    let bounded_root = temp_dir.path().join("bounded");
    let unbounded_store = LocalFsStore::new(&unbounded_root).expect("unbounded store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    add_bounded_gc_fixture(&unbounded_store, &namespace_id, &setup).await;
    copy_tree(&unbounded_root, &bounded_root);
    let bounded_store = LocalFsStore::new(&bounded_root).expect("bounded store");

    let unbounded_now = now_after_newest_object(
        &unbounded_store,
        &namespace_id,
        UPLOAD_SESSION_LEASE_MS + 2 * GRACE_MS + 1,
    )
    .await;
    let unbounded_report = gc_namespace(
        &unbounded_store,
        &namespace_id,
        &config(),
        &context(unbounded_now),
    )
    .await
    .expect("unbounded pass");

    let bounded_now = now_after_newest_object(
        &bounded_store,
        &namespace_id,
        UPLOAD_SESSION_LEASE_MS + 2 * GRACE_MS + 1,
    )
    .await;
    let mut bounded_config = config();
    bounded_config.max_objects = Some(3);
    let mut bounded_report = GcResponse::empty(namespace_id.clone());
    let mut passes = 0;
    loop {
        let pass = gc_namespace(
            &bounded_store,
            &namespace_id,
            &bounded_config,
            &context(bounded_now),
        )
        .await
        .expect("bounded pass");
        passes += 1;
        accumulate_report(&mut bounded_report, &pass);
        let Some(cursor) = pass.next_cursor else {
            break;
        };
        bounded_config.cursor = Some(cursor);
    }

    assert!(passes > 5, "fixture should require substantial resumption");
    assert_eq!(
        namespace_keys(&bounded_store, &namespace_id).await,
        namespace_keys(&unbounded_store, &namespace_id).await
    );
    assert_eq!(
        (
            bounded_report.deleted_wal_segments,
            bounded_report.deleted_metadata_tables,
            bounded_report.deleted_manifests,
            bounded_report.deleted_checkpoint_records,
            bounded_report.deleted_upload_sessions,
            bounded_report.deleted_content_objects,
        ),
        (
            unbounded_report.deleted_wal_segments,
            unbounded_report.deleted_metadata_tables,
            unbounded_report.deleted_manifests,
            unbounded_report.deleted_checkpoint_records,
            unbounded_report.deleted_upload_sessions,
            unbounded_report.deleted_content_objects,
        )
    );
}

#[tokio::test]
async fn budget_caps_candidate_operations_and_cursor_resumes_mid_family() {
    let temp_dir = tempdir().expect("tempdir");
    let inner = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    bootstrap_namespace(&inner, &namespace_id, &setup, false)
        .await
        .expect("bootstrap");
    let orphan_keys: Vec<String> = (0..5)
        .map(|index| {
            wal_segment(
                namespace_id.as_str(),
                &format!("00000000000000000000-orphan-{index:02}"),
            )
        })
        .collect();
    for key in &orphan_keys {
        inner
            .put_if_absent(key, Bytes::from_static(b"orphan"))
            .await
            .expect("write orphan");
    }
    let aged = context(now_after_newest_object(&inner, &namespace_id, GRACE_MS + 1).await);
    let wal_prefix = wal_segment_prefix(namespace_id.as_str());
    let store = CountingStore::new(inner, KeyPredicate::prefix(wal_prefix));
    let mut bounded = config();
    bounded.max_objects = Some(2);

    let first = gc_namespace(&store, &namespace_id, &bounded, &aged)
        .await
        .expect("first bounded pass");
    assert_eq!(first.deleted_wal_segments, 2);
    assert!(first.next_cursor.is_some());
    assert_eq!(store.snapshot().heads, 2);
    assert_eq!(store.snapshot().deletes, 2);
    for key in &orphan_keys[..2] {
        assert!(store.head(key).await.expect("head orphan").is_none());
    }
    assert!(store
        .head(&orphan_keys[2])
        .await
        .expect("head next orphan")
        .is_some());

    bounded.cursor = first.next_cursor;
    store.reset();
    let second = gc_namespace(&store, &namespace_id, &bounded, &aged)
        .await
        .expect("second bounded pass");
    assert_eq!(second.deleted_wal_segments, 2);
    assert!(second.next_cursor.is_some());
    assert_eq!(store.snapshot().heads, 2);
    assert_eq!(store.snapshot().deletes, 2);
    for key in &orphan_keys[..4] {
        assert!(store.head(key).await.expect("head orphan").is_none());
    }

    bounded.cursor = second.next_cursor;
    loop {
        store.reset();
        let pass = gc_namespace(&store, &namespace_id, &bounded, &aged)
            .await
            .expect("remaining bounded pass");
        assert!(store.snapshot().heads <= 2);
        assert!(store.snapshot().deletes <= 2);
        let Some(cursor) = pass.next_cursor else {
            break;
        };
        bounded.cursor = Some(cursor);
    }
    for key in &orphan_keys {
        assert!(store.head(key).await.expect("head orphan").is_none());
    }
}

#[tokio::test]
async fn stale_cursor_rebuilds_roots_before_resuming() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    bootstrap_namespace(&store, &namespace_id, &setup, false)
        .await
        .expect("bootstrap");
    for index in 0..2 {
        let key = wal_segment(
            namespace_id.as_str(),
            &format!("00000000000000000000-orphan-{index:02}"),
        );
        store
            .put_if_absent(&key, Bytes::from_static(b"orphan"))
            .await
            .expect("write orphan");
    }
    let first_now = now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await;
    let mut bounded = config();
    bounded.max_objects = Some(1);
    let first = gc_namespace(&store, &namespace_id, &bounded, &context(first_now))
        .await
        .expect("first bounded pass");
    let cursor = first.next_cursor.expect("work remains");

    write_test_file(
        &store,
        &namespace_id,
        "/docs/new.txt",
        "stale-cursor-new-wal",
        &setup,
    )
    .await;
    create_checkpoint(&store, &namespace_id, &setup)
        .await
        .expect("new checkpoint");
    let resume_now = now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await;
    let resume_context = context(resume_now);
    let live = collect_live_set(&store, &namespace_id, &resume_context)
        .await
        .expect("collect advanced live set");

    let mut resume = config();
    resume.cursor = Some(cursor);
    gc_namespace(&store, &namespace_id, &resume, &resume_context)
        .await
        .expect("resume stale cursor");

    for key in live
        .wal_segments
        .iter()
        .chain(live.tables.iter())
        .chain(live.checkpoint_keys.iter())
    {
        assert!(
            store.head(key).await.expect("head live object").is_some(),
            "live object `{key}` must survive stale-cursor resumption"
        );
    }
    for manifest_object_id in live.manifests {
        let key = metadata_manifest_object(namespace_id.as_str(), &manifest_object_id);
        assert!(
            store
                .head(&key)
                .await
                .expect("head live manifest")
                .is_some(),
            "live manifest `{key}` must survive stale-cursor resumption"
        );
    }
    stat_root(&store, &namespace_id).await;
}
