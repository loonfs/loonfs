//! Behavior tests for namespace GC.

#![allow(clippy::panic)]

use super::config::GcConfig;
use super::mark_table::MarkTables;
use super::run::gc_namespace;
use crate::checkpoint::advance_retention_floor;
use crate::checkpoint::record::release_checkpoint_record;
use crate::checkpoint::tests::{
    compact_a_family_group, create_checkpoint, mutation_context, write_test_file,
};
use crate::checkpoint::MetadataCompactionPolicy;
use crate::commit_engine::{CommitCandidate, NamespaceCommitEngine};
use crate::context::MutationContext;
use crate::error::CoreError;
use crate::limits::{
    CONTENT_RECLAMATION_GRACE_MS, FORK_CHECKPOINT_LEASE_MS, GC_MIN_GRACE_WINDOW_MS,
    UNREFERENCED_SEGMENT_MIN_AGE_MS, UPLOAD_SESSION_LEASE_MS,
};
use crate::path::write::{CommitRequest, FilesystemOperation};
use loonfs_api::v0::GcResponse;
use loonfs_api::wire::control::{
    decode_control_object, CheckpointOwner, CheckpointRecordState, CheckpointStatus,
    ControlObjectKind, ProxiedStaging, UploadSessionMode, UploadSessionRecordStatus,
    UploadSessionState,
};
use loonfs_api::wire::gc::*;
use loonfs_api::{CheckpointId, ContentRef, ContentStoreId, ManifestNo, NamespaceId, UploadId};
use loonfs_objectstore::keys::{
    checkpoint_prefix, metadata_manifest_object, metadata_manifest_prefix, metadata_segment,
    metadata_segment_prefix, wal_head, wal_segment, wal_segment_prefix,
};
use loonfs_objectstore::ObjectStore;
use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;

use crate::commit_engine::delete_namespace;
use crate::namespace::bootstrap::bootstrap_namespace;
use crate::namespace::fork::fork_namespace;
use crate::options::DeleteNamespaceOptions;
use crate::path::read::load_current_metadata_view;
use bytes::Bytes;
use futures::stream::BoxStream;
use loonfs_api::AttributeInclusion;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::{ByteRange, ObjectBody, ObjectMetadata, ObjectStoreError, PutMode};
use loonfs_test_support::stores::{
    BlockingStore, FailStore, InjectedError, KeyPredicate, MetadataMapStore, OperationClass,
    OperationContext, OperationKind, RecordingStore,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use tempfile::tempdir;

const GRACE_MS: u64 = 60 * 60 * 1000;

fn config() -> GcConfig {
    GcConfig {
        grace_window_ms: GRACE_MS,
        max_steps: None,
        cursor: None,
    }
}

fn context(now_ms: u64) -> MutationContext {
    mutation_context("gc-test", now_ms)
}

/// The roots one unbounded collection finds, for tests that assert against
/// the same set a pass marks.
async fn live_set<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) -> LiveSet {
    marked(store, namespace_id, context).await.0
}

/// Returns the budget units required to mark this namespace. Bounded tests
/// add candidate work to this measured value instead of hard-coding it.
async fn marking_units<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) -> u64 {
    marked(store, namespace_id, context).await.1
}

#[derive(Default)]
struct LiveSet {
    manifests: BTreeSet<ManifestNo>,
    wal_segments: BTreeSet<String>,
    segments: BTreeSet<String>,
    checkpoint_keys: BTreeSet<String>,
}

async fn mark_state<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) -> Result<(GcRunState, u64), CoreError> {
    let mut state = GcRunState {
        namespace_id: namespace_id.clone(),
        gc_run_id: loonfs_api::GcRunId::generate(),
        step_no: 0,
        started_at_ms: context.now_ms,
        grace_window_ms: GRACE_MS,
        phase: GcPhase::Starting {},
    };
    let id = state.gc_run_id.clone();
    let mut pass = super::run::Pass::new(store, namespace_id, &id, context);
    let mut report = GcResponse::empty(namespace_id.clone());
    let mut units = 0;
    while !matches!(state.phase, GcPhase::Sweeping { .. }) {
        pass.step(&mut state, &mut report, context.now_ms).await?;
        units += 1;
        assert!(units < 100_000, "marking must converge");
    }
    Ok((state, units))
}

async fn marked<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) -> (LiveSet, u64) {
    let (state, units) = mark_state(store, namespace_id, context)
        .await
        .expect("mark roots");
    let GcPhase::Sweeping { table, .. } = state.phase else {
        panic!("mark_state must finish at sweeping")
    };
    let mut tables = MarkTables::new(store, namespace_id, &state.gc_run_id);
    let mut position = GcMarkPosition::default();
    let mut live = LiveSet::default();
    while let Some(entry) = tables
        .peek(&table, position)
        .await
        .expect("read marked page")
    {
        if let Some(key) = entry.key.strip_prefix("object/") {
            use loonfs_objectstore::layout::{parse_object_key, DurableObjectFamily};
            match parse_object_key(key).map(|parsed| parsed.family()) {
                Some(DurableObjectFamily::WalSegment) => {
                    live.wal_segments.insert(key.to_owned());
                }
                Some(DurableObjectFamily::MetadataSegment) => {
                    live.segments.insert(key.to_owned());
                }
                Some(DurableObjectFamily::CheckpointRecord) => {
                    live.checkpoint_keys.insert(key.to_owned());
                }
                _ => {}
            }
        }
        if let GcMarkValue::Manifest { manifest } = entry.value {
            live.manifests.insert(manifest.manifest_no);
        }
        MarkTables::<S>::advance(&table, &mut position);
    }
    (live, units)
}

/// The durable lifecycle of one checkpoint record, stamp included.
async fn checkpoint_lifecycle<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    checkpoint_id: &CheckpointId,
) -> CheckpointStatus {
    crate::checkpoint::load_checkpoint_record(store, namespace_id, checkpoint_id)
        .await
        .expect("read checkpoint record")
        .expect("checkpoint record exists")
        .state
        .status
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

#[derive(Debug)]
struct IncompleteGcAccountingStore {
    inner: LocalFsStore,
    deletes: AtomicUsize,
    lists: AtomicUsize,
}

#[derive(Debug)]
struct ListingCursorStore<S> {
    inner: S,
    calls: Mutex<Vec<(String, Option<String>)>>,
}

impl<S> ListingCursorStore<S> {
    fn new(inner: S) -> Self {
        Self {
            inner,
            calls: Mutex::new(Vec::new()),
        }
    }

    fn take_calls(&self) -> Vec<(String, Option<String>)> {
        std::mem::take(&mut *self.calls.lock().expect("listing calls lock poisoned"))
    }
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
                matches!(envelope.payload().status, CheckpointStatus::Released { .. })
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
                        envelope.payload().status,
                        UploadSessionRecordStatus::Completed { .. }
                    ),
                    BlockingControlCasTarget::UploadAborted => {
                        matches!(
                            envelope.payload().status,
                            UploadSessionRecordStatus::Aborted { .. }
                        )
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

    fn list_prefix_from_stream(
        &self,
        prefix: &str,
        start_after: Option<&str>,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        self.lists.fetch_add(1, Ordering::SeqCst);
        self.inner.list_prefix_from_stream(prefix, start_after)
    }
}

#[async_trait::async_trait]
impl<S: ObjectStore> ObjectStore for ListingCursorStore<S> {
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
        self.inner.delete(key).await
    }

    fn list_prefix_from_stream(
        &self,
        prefix: &str,
        start_after: Option<&str>,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        self.calls
            .lock()
            .expect("listing calls lock poisoned")
            .push((prefix.to_owned(), start_after.map(str::to_owned)));
        self.inner.list_prefix_from_stream(prefix, start_after)
    }
}

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
        max_steps: Some(0),
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
    assert_eq!(report.deleted.wal_segments, 1);
    assert!(!report.retention_degraded);
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
            expires_at_ms: None,
        },
        &setup,
    )
    .await
    .expect("second checkpoint");
    assert_ne!(moved_on.manifest_no, pinned.manifest_no);

    // Simulate the crash residue: the pinned record stays active while
    // its basis manifest object vanishes.
    let record = crate::checkpoint::record::load_checkpoint_record(
        &store,
        &namespace_id,
        &pinned.checkpoint_id,
    )
    .await
    .expect("read record")
    .expect("record exists")
    .state;
    let basis_key = metadata_manifest_object(&namespace_id, &record.manifest.manifest_no);
    store.delete(&basis_key).await.expect("drop basis manifest");

    let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
    let report = gc_namespace(&store, &namespace_id, &config(), &aged)
        .await
        .expect("gc pass");
    assert_eq!(report.released_checkpoints.missing_basis, 1);
    assert!(
        !report.retention_degraded,
        "a verifiably absent basis is not ambiguity"
    );
    let released = crate::checkpoint::record::load_checkpoint_record(
        &store,
        &namespace_id,
        &pinned.checkpoint_id,
    )
    .await
    .expect("read record")
    .expect("record still present")
    .state;
    assert_eq!(
        released.status,
        loonfs_api::wire::control::CheckpointStatus::Released {
            released_at_ms: aged.now_ms
        }
    );

    // Idempotent: the released record is no longer a zombie, and the
    // namespace still reads (the live pin and root are untouched).
    let again = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
    let report = gc_namespace(&store, &namespace_id, &config(), &again)
        .await
        .expect("second gc pass");
    assert_eq!(report.released_checkpoints.missing_basis, 0);
    assert!(!report.retention_degraded);
    stat_root(&store, &namespace_id).await;
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
    assert_eq!(report.deleted.manifests, 0);
    // The pin on a tombstone has one route out, the same as every other
    // pin: released here, deleted a grace window after that release.
    assert_eq!(report.released_checkpoints.expired, 1);
    assert!(!report.retention_degraded);
    let reaped = context(aged.now_ms + UNREFERENCED_SEGMENT_MIN_AGE_MS);
    let report = gc_namespace(&store, &namespace_id, &config(), &reaped)
        .await
        .expect("gc pass past the release grace window");
    assert!(report.deleted.checkpoint_records >= 1);
    assert!(report.deleted.metadata_segments >= 1);
    assert!(report.deleted.manifests >= 1);
    assert!(!report.retention_degraded);

    for prefix in [
        wal_segment_prefix(&namespace_id),
        metadata_segment_prefix(&namespace_id),
        metadata_manifest_prefix(&namespace_id),
        checkpoint_prefix(&namespace_id),
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
        loonfs_objectstore::keys::wal_head(&namespace_id),
        loonfs_objectstore::keys::hint(&namespace_id),
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
    assert_eq!(report.deleted.wal_segments, 0);
    assert_eq!(report.deleted.manifests, 0);
    assert!(!report.retention_degraded);
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
    let basis_key = metadata_manifest_object(&source, &fork_record.manifest.manifest_no);
    let aged = context(now_after_newest_object(&store, &source, GRACE_MS + 1).await);
    let report = gc_namespace(&store, &source, &config(), &aged)
        .await
        .expect("gc pass with live clone");
    assert_eq!(report.released_checkpoints.fork, 0);
    assert!(!report.retention_degraded);
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
    let report = gc_namespace(&store, &source, &config(), &aged)
        .await
        .expect("gc pass after clone delete");
    assert_eq!(report.released_checkpoints.fork, 1);
    assert!(report.deleted.manifests >= 1);
    assert!(
        store.head(&basis_key).await.expect("head basis").is_none(),
        "the basis ages out once no living target needs it"
    );

    // Idempotent: the released record ages out on later passes and
    // nothing resurrects.
    let again = context(deadline + GRACE_MS);
    let report = gc_namespace(&store, &source, &config(), &again)
        .await
        .expect("idempotent pass");
    assert_eq!(report.released_checkpoints.fork, 0);
    assert_eq!(report.deleted.manifests, 0);
    assert!(!report.retention_degraded);
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
    assert_eq!(report.retained_candidates, 1);
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
            &content_store_id,
            &content_ref.owner_namespace_id,
            &content_ref.content_id,
        );

        let past = context(setup.now_ms + CONTENT_RECLAMATION_GRACE_MS + 1);
        let report = gc_namespace(&store, &namespace_id, &config(), &past)
            .await
            .expect("gc pass past the content grace");

        assert_eq!(
            report.deleted.upload_sessions, 1,
            "materialize={materialize}"
        );
        assert_eq!(
            report.deleted.content_objects, 0,
            "materialize={materialize}"
        );
        assert!(
            store.head(&content_key).await.expect("head").is_some(),
            "published content survives its session (materialize={materialize})"
        );
        assert!(read_upload_session(&store, &namespace_id, &upload_id)
            .await
            .is_none());
        assert!(!report.retention_degraded);
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

#[tokio::test]
async fn a_corrupt_marked_manifest_fails_the_scan_and_an_unreadable_one_makes_it_unavailable() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let store = FailStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::prefix(metadata_manifest_prefix(&namespace_id)),
        OperationClass::Read,
        InjectedError::Transport("marked manifest timed out".to_owned()),
    );
    let setup = context(1_000);
    namespace_with_a_scan_worth_bounding(store.inner(), &namespace_id, &setup).await;
    let live = live_set(store.inner(), &namespace_id, &setup).await;
    let manifest_number = *live.manifests.iter().next().expect("live manifest");
    let manifest_key = metadata_manifest_object(&namespace_id, &manifest_number);

    store.fail_all();
    let error = mark_state(&store, &namespace_id, &setup)
        .await
        .expect_err("discovery read fails closed");
    assert_eq!(error.code(), crate::error::ErrorCode::ServerError);

    store.clear();
    store
        .put_overwrite(&manifest_key, Bytes::from_static(b"not json"))
        .await
        .expect("corrupt marked manifest");
    let error = mark_state(&store, &namespace_id, &setup)
        .await
        .expect_err("corruption after marking must still surface");
    assert_eq!(error.code(), crate::error::ErrorCode::NamespaceCorrupt);
    assert!(error.message().contains(&manifest_key));
}

#[tokio::test]
async fn corrupt_metadata_rows_fail_the_pass_and_unreadable_ones_retain_the_content() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let store = FailStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::prefix(metadata_segment_prefix(&namespace_id)),
        OperationClass::Read,
        InjectedError::Transport("metadata rows timed out".to_owned()),
    );
    let setup = context(1_000);
    namespace_with_a_scan_worth_bounding(store.inner(), &namespace_id, &setup).await;
    let (upload_id, content_ref, content_store_id, _) =
        complete_upload_for_gc(store.inner(), &namespace_id, b"unpublished\n", &setup).await;
    let content_key = loonfs_objectstore::keys::content_blob(
        &content_store_id,
        &content_ref.owner_namespace_id,
        &content_ref.content_id,
    );
    let segment_keys = store
        .inner()
        .list_prefix(&metadata_segment_prefix(&namespace_id))
        .await
        .expect("list metadata segments");
    assert!(
        !segment_keys.is_empty(),
        "the fixture must publish metadata segments"
    );
    let past = context(setup.now_ms + CONTENT_RECLAMATION_GRACE_MS + 1);

    store.fail_all();
    gc_namespace(&store, &namespace_id, &config(), &past)
        .await
        .expect_err("a failed revision read stops before sweeping");
    assert!(store
        .inner()
        .head(&content_key)
        .await
        .expect("head content")
        .is_some());
    assert!(read_upload_session(&store, &namespace_id, &upload_id)
        .await
        .is_some());

    store.clear();
    for key in &segment_keys {
        let mut bytes = store
            .get(key, None)
            .await
            .expect("read metadata segment")
            .expect("metadata segment exists")
            .to_vec();
        bytes[0] ^= 0xff;
        store
            .put_overwrite(key, Bytes::from(bytes))
            .await
            .expect("corrupt metadata segment");
    }
    let error = gc_namespace(&store, &namespace_id, &config(), &past)
        .await
        .expect_err("corrupt content-reference rows must fail the pass");
    assert_eq!(error.code(), crate::error::ErrorCode::NamespaceCorrupt);
    assert!(segment_keys.iter().any(|key| error.message().contains(key)));
    assert!(store
        .inner()
        .head(&content_key)
        .await
        .expect("head content")
        .is_some());
    assert!(read_upload_session(&store, &namespace_id, &upload_id)
        .await
        .is_some());
}

#[tokio::test]
async fn a_budget_that_covers_the_roots_exactly_finishes_marking() {
    let temp_dir = tempdir().expect("tempdir");
    let inner = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    namespace_with_a_scan_worth_bounding(&inner, &namespace_id, &setup).await;
    let aged = context(now_after_newest_object(&inner, &namespace_id, GRACE_MS + 1).await);
    let (live, marking) = marked(&inner, &namespace_id, &aged).await;
    let retained = live.wal_segments;
    assert!(!retained.is_empty(), "the fixture must retain a chain");

    let segment_reads = KeyPredicate::prefix(wal_segment_prefix(&namespace_id));
    let store = RecordingStore::new(inner, segment_reads);
    let mut exact = config();
    exact.max_steps = Some(marking);
    let report = gc_namespace(&store, &namespace_id, &exact, &aged)
        .await
        .expect("pass with a budget for the roots");

    assert_eq!(
        store.take_get_keys().into_iter().collect::<BTreeSet<_>>(),
        retained,
        "marking read every retained segment"
    );
    assert!(report.budget_exhausted);
    assert!(
        !report.content_reclamation_deferred,
        "this pass did finish marking, so it has a root set and a reference set"
    );

    // After marking, another call spends its budget on candidate decisions.
    let mut one_more = config();
    one_more.max_steps = Some(marking + 1);
    let walked = gc_namespace(&store, &namespace_id, &one_more, &aged)
        .await
        .expect("pass with one candidate of room");
    assert!(walked.budget_exhausted);
    assert!(
        walked.next_cursor.is_some(),
        "a pass that decided a candidate reports where it walked to"
    );
}

#[tokio::test]
async fn a_complete_pass_fetches_each_retained_segment_once() {
    let temp_dir = tempdir().expect("tempdir");
    let inner = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    namespace_with_a_scan_worth_bounding(&inner, &namespace_id, &setup).await;
    let (upload_id, content_ref, content_store_id, prepared) =
        complete_upload_for_gc(&inner, &namespace_id, b"wal-only\n", &setup).await;
    // Nothing has been flushed since this publish, so the newest WAL
    // segment is the only place the reference lives.
    publish_completed_content(
        &inner,
        &namespace_id,
        "/docs/wal-only.txt",
        content_ref.clone(),
        prepared,
        &setup,
    )
    .await;
    let content_key = loonfs_objectstore::keys::content_blob(
        &content_store_id,
        &content_ref.owner_namespace_id,
        &content_ref.content_id,
    );
    let past = context(setup.now_ms + CONTENT_RECLAMATION_GRACE_MS + 1);
    let retained = live_set(&inner, &namespace_id, &past).await.wal_segments;
    assert!(!retained.is_empty(), "the fixture must retain a chain");

    let segment_reads = KeyPredicate::prefix(wal_segment_prefix(&namespace_id));
    let store = RecordingStore::new(inner, segment_reads);
    let report = gc_namespace(&store, &namespace_id, &config(), &past)
        .await
        .expect("unbounded pass");

    let mut fetches: BTreeMap<String, usize> = BTreeMap::new();
    for key in store.take_get_keys() {
        *fetches.entry(key).or_default() += 1;
    }
    assert_eq!(
        fetches.keys().cloned().collect::<BTreeSet<_>>(),
        retained,
        "a pass reads the retained chain and nothing else under the segment prefix"
    );
    for (key, count) in &fetches {
        assert_eq!(*count, 1, "segment `{key}` was fetched {count} times");
    }
    assert_eq!(
        report.deleted.content_objects, 0,
        "a reference that only a retained WAL record carries still protects its bytes"
    );
    assert!(store.head(&content_key).await.expect("head").is_some());
    assert_eq!(
        report.deleted.upload_sessions, 1,
        "the session itself has said everything it will say"
    );
    assert!(read_upload_session(&store, &namespace_id, &upload_id)
        .await
        .is_none());
}

#[tokio::test]
async fn a_budget_that_dies_among_the_checkpoint_records_decides_nothing() {
    let temp_dir = tempdir().expect("tempdir");
    let inner = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    add_bounded_gc_fixture(&inner, &namespace_id, &setup).await;
    let aged = context(
        now_after_newest_object(
            &inner,
            &namespace_id,
            UPLOAD_SESSION_LEASE_MS + 2 * GRACE_MS + 1,
        )
        .await,
    );

    let record_reads = KeyPredicate::prefix(checkpoint_prefix(&namespace_id));
    let store = RecordingStore::new(inner, record_reads);
    let mut bounded = config();
    bounded.max_steps = Some(3);
    let report = gc_namespace(&store, &namespace_id, &bounded, &aged)
        .await
        .expect("pass stopped among the records");

    assert!(report.budget_exhausted);
    assert!(report.next_cursor.is_some());
    assert_eq!(report.retained_candidates, 0);
    assert_eq!(
        (
            report.deleted.wal_segments,
            report.deleted.metadata_segments,
            report.deleted.manifests,
            report.deleted.checkpoint_records,
        ),
        (0, 0, 0, 0)
    );
    assert_eq!(
        store.count(OperationClass::Read),
        1,
        "the control snapshot and owned root precede the first checkpoint"
    );
}

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
    let content_key = loonfs_objectstore::keys::content_blob(
        &content_store_id,
        &content_ref.owner_namespace_id,
        &content_ref.content_id,
    );
    let past = context(setup.now_ms + CONTENT_RECLAMATION_GRACE_MS + 1);
    for max_steps in [1, 2, 5, 17, 64] {
        let trial_root = temp_dir.path().join(format!("trial-{max_steps}"));
        copy_tree(&seed_root, &trial_root);
        let store = LocalFsStore::new(&trial_root).expect("trial store");
        let mut bounded = config();
        bounded.max_steps = Some(max_steps);
        let mut previous = None;
        for pass_no in 0..1000 {
            let pass = gc_namespace(&store, &namespace_id, &bounded, &past)
                .await
                .expect("bounded pass");
            assert_eq!(pass.deleted.content_objects, 0);
            assert!(store.head(&content_key).await.expect("head").is_some());
            let state = super::run::load_run(&store, &namespace_id)
                .await
                .expect("progress")
                .expect("run");
            assert_ne!(
                previous.as_ref(),
                Some(&state.state),
                "budget {max_steps} must advance durable progress"
            );
            previous = Some(state.state);
            bounded.cursor = pass.next_cursor;
            if bounded.cursor.is_none() {
                break;
            }
            assert!(pass_no < 999, "budget {max_steps} must finish");
        }
        assert!(
            read_upload_session(&store, &namespace_id, &upload_id)
                .await
                .is_none(),
            "every budget eventually decides the completed session"
        );
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

/// Reports every object written before this point as ancient, leaving
/// everything written after it with its real age.
///
/// Real filesystem stamps put a whole fixture inside the same millisecond,
/// so a test that needs "written long ago" and "written just now" in one
/// namespace has to say which is which itself.
fn aged_before_now(
    inner: LocalFsStore,
    already_written: BTreeSet<String>,
) -> MetadataMapStore<LocalFsStore> {
    MetadataMapStore::aged(
        inner,
        KeyPredicate::new(move |key| already_written.contains(key)),
    )
}

/// Every reason's count, summed — what `retained_candidates` must equal.
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
    let pinned = create_checkpoint(&store, &namespace_id, &setup)
        .await
        .expect("checkpoint");

    // Released just now: a candidate the pass must hold for its own grace
    // window before the key can go.
    let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS * 2).await);
    crate::checkpoint::release_checkpoint(&store, &namespace_id, &pinned.checkpoint_id, &aged)
        .await
        .expect("release checkpoint");
    let report = gc_namespace(&store, &namespace_id, &config(), &aged)
        .await
        .expect("gc pass");

    assert_eq!(report.deleted.checkpoint_records, 0);
    assert_eq!(report.retained.checkpoint_not_releasable, 1);
    assert_eq!(reason_total(&report), report.retained_candidates);
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

    assert_eq!(report.deleted.wal_segments, 1);
    // Latest reads replay the retained tail over the root basis.
    let view = load_current_metadata_view(&store, &namespace_id)
        .await
        .expect("load view");
    view.resolve_path("/docs/two.txt", AttributeInclusion::Omit)
        .await
        .expect("tail commit stays readable");
}

async fn gc_pass(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
    config: &GcConfig,
    context: &MutationContext,
    max_steps: Option<usize>,
) -> Result<GcResponse, CoreError> {
    let mut config = config.clone();
    config.max_steps = max_steps.map(|value| value as u64);
    let mut total = GcResponse::empty(namespace_id.clone());
    loop {
        let pass = gc_namespace(store, namespace_id, &config, context).await?;
        config.cursor.clone_from(&pass.next_cursor);
        accumulate_report(&mut total, &pass);
        if config.cursor.is_none() {
            return Ok(total);
        }
    }
}

async fn assert_record_and_basis_reaped(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
    pass: &GcResponse,
    checkpoint_id: &loonfs_api::CheckpointId,
    manifest_number: &ManifestNo,
) {
    assert_eq!(pass.deleted.checkpoint_records, 1);
    assert!(!pass.retention_degraded);
    assert!(
        crate::checkpoint::load_checkpoint_record(store, namespace_id, checkpoint_id)
            .await
            .expect("read record")
            .is_none(),
        "the released record goes on the pass that decides it"
    );
    assert!(store
        .head(&metadata_manifest_object(namespace_id, manifest_number))
        .await
        .expect("probe basis")
        .is_none());
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
async fn gc_reaps_dead_checkpoints_before_their_basis_across_passes() {
    assert_a_dead_records_cascade(None).await;
}

#[tokio::test]
async fn a_chunked_sweep_reaches_the_same_dead_record_cascade() {
    assert_a_dead_records_cascade(Some(1)).await;
}

async fn assert_a_dead_records_cascade(max_steps: Option<usize>) {
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
        crate::checkpoint::load_checkpoint_record(&store, &namespace_id, &first.checkpoint_id)
            .await
            .expect("read first record")
            .expect("first record exists")
            .state;
    release_checkpoint_record(&store, &namespace_id, &first.checkpoint_id, setup.now_ms)
        .await
        .expect("mark first dead");

    let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
    let first_pass = gc_pass(&store, &namespace_id, &config(), &aged, max_steps)
        .await
        .expect("first gc pass");
    assert_record_and_basis_reaped(
        &store,
        &namespace_id,
        &first_pass,
        &first.checkpoint_id,
        &first_record.manifest.manifest_no,
    )
    .await;

    let second_pass = gc_pass(&store, &namespace_id, &config(), &aged, max_steps)
        .await
        .expect("second gc pass");
    assert_basis_reaped(
        &store,
        &namespace_id,
        &second_pass,
        &first_record.manifest.manifest_no,
    )
    .await;
    stat_root(&store, &namespace_id).await;
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

    // Record-less maintenance: nothing accumulates under `checkpoints/`.
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
    // next two superseded it; only the root's manifest is reachable. Its
    // segments are all still referenced (a flush only appends delta runs).
    assert_eq!(report.deleted.manifests, 2);
    assert!(!report.retention_degraded);
    let manifests_left = store
        .list_prefix(&metadata_manifest_prefix(&namespace_id))
        .await
        .expect("list manifests");
    assert_eq!(manifests_left.len(), 1, "only the live root manifest stays");

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
    assert!(!after_fold.retention_degraded);

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
    assert_eq!(first_release.checkpoint_id, pinned.checkpoint_id);
    let repeat_release =
        crate::checkpoint::release_checkpoint(&store, &namespace_id, &pinned.checkpoint_id, &setup)
            .await
            .expect("repeat release");
    assert_eq!(repeat_release.checkpoint_id, pinned.checkpoint_id);

    let pinned_record =
        crate::checkpoint::load_checkpoint_record(&store, &namespace_id, &pinned.checkpoint_id)
            .await
            .expect("read pinned record")
            .expect("pinned record exists")
            .state;
    let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
    let first_pass = gc_namespace(&store, &namespace_id, &config(), &aged)
        .await
        .expect("first gc pass");
    assert_record_and_basis_reaped(
        &store,
        &namespace_id,
        &first_pass,
        &pinned.checkpoint_id,
        &pinned_record.manifest.manifest_no,
    )
    .await;

    let second_pass = gc_namespace(&store, &namespace_id, &config(), &aged)
        .await
        .expect("second gc pass");
    assert_basis_reaped(
        &store,
        &namespace_id,
        &second_pass,
        &pinned_record.manifest.manifest_no,
    )
    .await;
    // Releasing an already-reaped record stays idempotent success.
    let after_reap =
        crate::checkpoint::release_checkpoint(&store, &namespace_id, &pinned.checkpoint_id, &setup)
            .await
            .expect("release after reap");
    assert_eq!(after_reap.checkpoint_id, pinned.checkpoint_id);
    stat_root(&store, &namespace_id).await;
}

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
    let pin = |name: &'static str| async {
        crate::checkpoint::create_checkpoint(
            &store,
            &namespace_id,
            CheckpointOwner::User {
                name: name.to_owned(),
                expires_at_ms: Some(setup.now_ms + GRACE_MS),
            },
            &setup,
        )
        .await
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
    assert_eq!(released.checkpoint_id, caller_first.checkpoint_id);
    let report = gc_namespace(&store, &namespace_id, &config(), &expired)
        .await
        .expect("gc pass");
    assert_eq!(
        report.released_checkpoints.expired, 1,
        "only the record the caller left alone is released here"
    );
    assert_eq!(
        checkpoint_lifecycle(&store, &namespace_id, &caller_first.checkpoint_id).await,
        CheckpointStatus::Released {
            released_at_ms: caller_stamp
        },
        "the winner's stamp stands"
    );

    // The pass got there first: the caller reports the same end state, and
    // the pass's stamp is what ages the record out.
    assert_eq!(
        checkpoint_lifecycle(&store, &namespace_id, &pass_first.checkpoint_id).await,
        CheckpointStatus::Released {
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
    assert_eq!(late.checkpoint_id, pass_first.checkpoint_id);
    assert_eq!(
        checkpoint_lifecycle(&store, &namespace_id, &pass_first.checkpoint_id).await,
        CheckpointStatus::Released {
            released_at_ms: expired.now_ms
        },
        "the loser rewrites nothing"
    );
}

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
            expires_at_ms: Some(setup.now_ms + GRACE_MS),
        },
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
    assert_eq!(
        released.expect("caller release").checkpoint_id,
        pinned.checkpoint_id
    );
    let report = report.expect("the pass finishes");
    assert_eq!(report.released_checkpoints.expired, 0);
    assert_eq!(report.deleted.checkpoint_records, 0);
    assert_eq!(
        checkpoint_lifecycle(&store, &namespace_id, &pinned.checkpoint_id).await,
        CheckpointStatus::Released {
            released_at_ms: caller_stamp
        }
    );
}

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
        CheckpointStatus::Released {
            released_at_ms: aged.now_ms
        }
    );

    let inside_grace = context(aged.now_ms + GRACE_MS - 1);
    let report = gc_namespace(&store, &namespace_id, &config(), &inside_grace)
        .await
        .expect("pass inside the release grace window");
    assert_eq!(
        report.deleted.checkpoint_records, 0,
        "an old object with a young release is retained"
    );
    assert!(crate::checkpoint::load_checkpoint_record(
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
    assert_eq!(report.deleted.checkpoint_records, 1);
    assert!(crate::checkpoint::load_checkpoint_record(
        &store,
        &namespace_id,
        &pinned.checkpoint_id
    )
    .await
    .expect("read record")
    .is_none());
}

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
            expires_at_ms: Some(setup.now_ms + GRACE_MS),
        },
        &setup,
    )
    .await
    .expect("expiring checkpoint");
    let lasting = crate::checkpoint::create_checkpoint(
        &store,
        &namespace_id,
        CheckpointOwner::User {
            name: "long-lived".to_owned(),
            expires_at_ms: Some(u64::MAX),
        },
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
    assert_eq!(first_pass.released_checkpoints.expired, 1);
    assert_eq!(first_pass.deleted.checkpoint_records, 0);
    assert_eq!(
        checkpoint_lifecycle(&store, &namespace_id, &expiring.checkpoint_id).await,
        CheckpointStatus::Released {
            released_at_ms: expired.now_ms
        }
    );
    let expiring_record =
        crate::checkpoint::load_checkpoint_record(&store, &namespace_id, &expiring.checkpoint_id)
            .await
            .expect("read expiring record")
            .expect("the released record is still there to read")
            .state;
    let aged_out = context(expired.now_ms + GRACE_MS);
    let second_pass = gc_namespace(&store, &namespace_id, &config(), &aged_out)
        .await
        .expect("second post-expiry pass");
    assert_eq!(second_pass.deleted.checkpoint_records, 1);
    assert!(crate::checkpoint::load_checkpoint_record(
        &store,
        &namespace_id,
        &expiring.checkpoint_id
    )
    .await
    .expect("record read")
    .is_none());
    assert!(store
        .head(&metadata_manifest_object(
            &namespace_id,
            &expiring_record.manifest.manifest_no
        ))
        .await
        .expect("pinned basis")
        .is_some());
    // The unexpired pin — same basis, different owner — still roots it.
    let survivor =
        crate::checkpoint::load_checkpoint_record(&store, &namespace_id, &lasting.checkpoint_id)
            .await
            .expect("read lasting record")
            .expect("lasting record survives")
            .state;
    assert!(crate::checkpoint::load_namespace_manifest_envelope(
        &store,
        &namespace_id,
        &survivor.manifest.manifest_no,
    )
    .await
    .is_ok());
    stat_root(&store, &namespace_id).await;
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

    crate::checkpoint::release_checkpoint(&store, &namespace_id, &second.checkpoint_id, &setup)
        .await
        .expect("release one owner");
    let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
    let first_pass = gc_namespace(&store, &namespace_id, &config(), &aged)
        .await
        .expect("first gc pass");
    assert_eq!(first_pass.deleted.checkpoint_records, 1);

    let second_pass = gc_namespace(&store, &namespace_id, &config(), &aged)
        .await
        .expect("second gc pass");
    assert_eq!(second_pass.deleted.manifests, 0);
    assert_eq!(second_pass.deleted.checkpoint_records, 0);

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
            &keeper.manifest.manifest_no,
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

    let error = crate::checkpoint::release_checkpoint(
        &store,
        &namespace_id,
        &snapshot.checkpoint_id,
        &setup,
    )
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
    assert_eq!(
        checkpoint_lifecycle(&store, &namespace_id, &snapshot.checkpoint_id).await,
        CheckpointStatus::Active {}
    );
}

#[tokio::test]
async fn gc_releases_an_expired_snapshot_under_its_own_count() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    bootstrap_namespace(&store, &namespace_id, &setup, false)
        .await
        .expect("bootstrap");
    write_test_file(&store, &namespace_id, "/docs/one.txt", "gc-one", &setup).await;
    let expiring = crate::checkpoint::create_checkpoint(
        &store,
        &namespace_id,
        CheckpointOwner::Snapshot {
            name: "short-lived".to_owned(),
            expires_at_ms: setup.now_ms + GRACE_MS,
        },
        &setup,
    )
    .await
    .expect("expiring snapshot");
    let lasting = crate::checkpoint::create_checkpoint(
        &store,
        &namespace_id,
        CheckpointOwner::Snapshot {
            name: "long-lived".to_owned(),
            expires_at_ms: u64::MAX,
        },
        &setup,
    )
    .await
    .expect("lasting snapshot");
    write_test_file(&store, &namespace_id, "/docs/two.txt", "gc-two", &setup).await;
    create_checkpoint(&store, &namespace_id, &setup)
        .await
        .expect("advance past the expiring basis");

    let expired = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
    let pass = gc_namespace(&store, &namespace_id, &config(), &expired)
        .await
        .expect("post-expiry pass");
    assert_eq!(pass.released_checkpoints.snapshot, 1);
    assert_eq!(
        pass.released_checkpoints.expired, 0,
        "a snapshot release is not counted as a user pin expiring"
    );
    assert_eq!(pass.deleted.checkpoint_records, 0);
    assert_eq!(
        checkpoint_lifecycle(&store, &namespace_id, &expiring.checkpoint_id).await,
        CheckpointStatus::Released {
            released_at_ms: expired.now_ms
        }
    );
    assert_eq!(
        checkpoint_lifecycle(&store, &namespace_id, &lasting.checkpoint_id).await,
        CheckpointStatus::Active {},
        "the unexpired snapshot survives the pass"
    );
    stat_root(&store, &namespace_id).await;
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
    assert!(report.deleted.manifests <= 1);
    assert_eq!(report.deleted.checkpoint_records, 0);
    let first_record =
        crate::checkpoint::load_checkpoint_record(&store, &namespace_id, &first.checkpoint_id)
            .await
            .expect("read first checkpoint")
            .expect("first checkpoint exists")
            .state;
    assert!(crate::checkpoint::load_namespace_manifest_envelope(
        &store,
        &namespace_id,
        &first_record.manifest.manifest_no,
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
    fork_namespace(&store, &source, &clone, None, &setup)
        .await
        .expect("fork");
    let target_pin = create_checkpoint(&store, &clone, &setup)
        .await
        .expect("materialize target root");
    release_checkpoint_record(&store, &clone, &target_pin.checkpoint_id, setup.now_ms)
        .await
        .expect("release target pin");
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
    assert_eq!(report.released_checkpoints.fork, 0);

    delete_namespace(&store, &clone, DeleteNamespaceOptions::default(), &setup)
        .await
        .expect("terminal delete of the fork target");
    let aged = context(now_after_newest_object(&store, &source, GRACE_MS + 1).await);

    let waiting = gc_namespace(&store, &source, &config(), &aged)
        .await
        .expect("wait for retirement");
    assert_eq!(waiting.released_checkpoints.fork, 0);
    let retired = gc_namespace(&store, &clone, &config(), &aged)
        .await
        .expect("retire materialized target");
    let deadline = retired.reclaim_after_ms.expect("target retired");
    let waiting = gc_namespace(&store, &source, &config(), &context(deadline - 1))
        .await
        .expect("wait for grace");
    assert_eq!(waiting.released_checkpoints.fork, 0);
    let aged = context(deadline);

    // Pass one flips the record; the record still roots its basis.
    let first_pass = gc_namespace(&store, &source, &config(), &aged)
        .await
        .expect("first gc pass");
    assert_eq!(first_pass.released_checkpoints.fork, 1);
    assert_eq!(first_pass.deleted.checkpoint_records, 0);
    assert_eq!(
        checkpoint_lifecycle(&store, &source, &fork_record.checkpoint_id).await,
        CheckpointStatus::Released {
            released_at_ms: aged.now_ms
        }
    );

    // The release stamp starts the record's own grace window.
    let aged_out = context(aged.now_ms + GRACE_MS);
    let second_pass = gc_namespace(&store, &source, &config(), &aged_out)
        .await
        .expect("second gc pass");
    assert_record_and_basis_reaped(
        &store,
        &source,
        &second_pass,
        &fork_record.checkpoint_id,
        &fork_record.manifest.manifest_no,
    )
    .await;

    // Pass three reaps the unreferenced basis.
    let third_pass = gc_namespace(&store, &source, &config(), &aged_out)
        .await
        .expect("third gc pass");
    assert_basis_reaped(
        &store,
        &source,
        &third_pass,
        &fork_record.manifest.manifest_no,
    )
    .await;
    stat_root(&store, &source).await;
}

#[tokio::test]
async fn a_corrupt_fork_target_head_fails_the_pass_and_an_unreadable_one_retains_the_record() {
    let temp_dir = tempdir().expect("tempdir");
    let source = NamespaceId::parse("source").expect("namespace id");
    let clone = NamespaceId::parse("clone").expect("namespace id");
    let store = FailStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::exact(wal_head(&clone)),
        OperationClass::Read,
        InjectedError::Transport("target head timed out".to_owned()),
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

    store.fail_all();
    let report = gc_namespace(&store, &source, &config(), &setup)
        .await
        .expect("an unreadable target is retained conservatively");
    assert_eq!(report.released_checkpoints.fork, 0);
    assert_eq!(
        checkpoint_lifecycle(store.inner(), &source, &fork_record.checkpoint_id).await,
        CheckpointStatus::Active {}
    );

    store.clear();
    let head_key = wal_head(&clone);
    let bytes = store
        .get(&head_key, None)
        .await
        .expect("read target head")
        .expect("target head exists");
    let mut head = decode_control_object::<loonfs_api::wire::control::HeadState>(
        &bytes,
        ControlObjectKind::WalHead,
    )
    .expect("decode target head")
    .into_payload();
    let basis = head.fork_basis.as_mut().expect("a fork target has a basis");
    basis.manifest.manifest_no = ManifestNo(basis.manifest.manifest_no.0 + 1);
    let drifted = head;
    store
        .put_overwrite(
            &head_key,
            Bytes::from(
                loonfs_api::wire::control::encode_control_state(
                    ControlObjectKind::WalHead,
                    &drifted,
                )
                .expect("encode head"),
            ),
        )
        .await
        .expect("write drifted target head");
    let error = gc_namespace(&store, &source, &config(), &setup)
        .await
        .expect_err("a target naming this record with another manifest is corruption");
    assert_eq!(error.code(), crate::error::ErrorCode::NamespaceCorrupt);
    assert!(error.message().contains(fork_record.checkpoint_id.as_str()));

    store
        .put_overwrite(&wal_head(&clone), Bytes::from_static(b"not json"))
        .await
        .expect("corrupt target head");
    let before = namespace_keys(store.inner(), &source).await;
    let error = gc_namespace(&store, &source, &config(), &setup)
        .await
        .expect_err("a corrupt target head must fail the source pass");
    assert_eq!(error.code(), crate::error::ErrorCode::NamespaceCorrupt);
    assert!(error.message().contains(&wal_head(&clone)));
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
        fork_record.owner.expires_at_ms().is_some(),
        "a fork record carries the attempt's lease"
    );
    // Only the fork-owned record can protect the basis after this.
    write_test_file(&store, &source, "/docs/two.txt", "gc-two", &setup).await;
    create_checkpoint(&store, &source, &setup)
        .await
        .expect("advance root past the fork basis");

    // Every clock: inside the lease, one tick past it, and absurdly past it.
    let lease = fork_record.owner.expires_at_ms().expect("lease");
    for now_ms in [
        now_after_newest_object(&store, &source, GRACE_MS + 1).await,
        lease,
        lease + FORK_CHECKPOINT_LEASE_MS,
        u64::MAX / 2,
    ] {
        let report = gc_namespace(&store, &source, &config(), &context(now_ms))
            .await
            .expect("gc pass with a live target");
        assert_eq!(report.released_checkpoints.fork, 0, "at {now_ms}");
        assert_eq!(report.released_checkpoints.expired, 0, "at {now_ms}");
        assert_eq!(
            checkpoint_lifecycle(&store, &source, &fork_record.checkpoint_id).await,
            CheckpointStatus::Active {},
            "a live target keeps its pin at {now_ms}"
        );
    }
    assert!(crate::checkpoint::load_namespace_manifest_envelope(
        &store,
        &source,
        &fork_record.manifest.manifest_no,
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
async fn a_fork_pin_with_a_missing_basis_survives_the_missing_basis_pass() {
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
    // Keep the source root valid after deleting the fork basis.
    write_test_file(&store, &source, "/docs/two.txt", "gc-two", &setup).await;
    create_checkpoint(&store, &source, &setup)
        .await
        .expect("advance root past the fork basis");
    let basis_key = metadata_manifest_object(&source, &fork_record.manifest.manifest_no);
    store.delete(&basis_key).await.expect("drop basis manifest");

    let aged = context(now_after_newest_object(&store, &source, GRACE_MS + 1).await);
    let report = gc_namespace(&store, &source, &config(), &aged)
        .await
        .expect("gc pass");
    assert_eq!(report.released_checkpoints.missing_basis, 0);
    assert_eq!(report.released_checkpoints.fork, 0);
    assert_eq!(
        checkpoint_lifecycle(&store, &source, &fork_record.checkpoint_id).await,
        CheckpointStatus::Active {},
        "a live target keeps its fork checkpoint"
    );
}

/// A released fork checkpoint still protects a live target that names it.
#[tokio::test]
async fn gc_retains_a_released_fork_record_when_its_target_lives() {
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

    // Move the source root beyond the fork basis, then release its checkpoint.
    write_test_file(&store, &source, "/docs/two.txt", "gc-two", &setup).await;
    create_checkpoint(&store, &source, &setup)
        .await
        .expect("advance root past the fork basis");
    release_checkpoint_record(&store, &source, &fork_record.checkpoint_id, setup.now_ms)
        .await
        .expect("simulate the racing checkpoint release");
    delete_namespace(&store, &source, DeleteNamespaceOptions::default(), &setup)
        .await
        .expect("delete source so only the fork pin protects its basis");

    let aged = context(now_after_newest_object(&store, &source, GRACE_MS + 1).await);
    let report = gc_namespace(&store, &source, &config(), &aged)
        .await
        .expect("gc with a released fork record and live target");
    assert_eq!(report.deleted.checkpoint_records, 0);
    assert_eq!(report.released_checkpoints.fork, 0);
    assert_eq!(
        checkpoint_lifecycle(&store, &source, &fork_record.checkpoint_id).await,
        CheckpointStatus::Released {
            released_at_ms: setup.now_ms
        }
    );
    assert!(crate::checkpoint::load_namespace_manifest_envelope(
        &store,
        &source,
        &fork_record.manifest.manifest_no,
    )
    .await
    .is_ok());
    load_current_metadata_view(&store, &clone)
        .await
        .expect("target remains readable after source collection")
        .resolve_path("/docs/one.txt", AttributeInclusion::Omit)
        .await
        .expect("forked file remains readable");
}

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
        max_steps: None,
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
            expires_at_ms: lease,
        },
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
        assert_eq!(report.released_checkpoints.fork, 0, "at {now_ms}");
        assert_eq!(
            checkpoint_lifecycle(&store, &source, &abandoned.checkpoint_id).await,
            CheckpointStatus::Active {}
        );
        assert!(crate::checkpoint::load_namespace_manifest_envelope(
            &store,
            &source,
            &fork_record.manifest.manifest_no,
        )
        .await
        .is_ok());
    }

    // Past the lease: the attempt is provably gone.
    let expired = context(lease);
    let report = gc_namespace(&store, &source, &tight, &expired)
        .await
        .expect("gc past the lease");
    assert_eq!(report.released_checkpoints.fork, 1);
    assert_eq!(
        checkpoint_lifecycle(&store, &source, &abandoned.checkpoint_id).await,
        CheckpointStatus::Released {
            released_at_ms: expired.now_ms
        }
    );

    // From there it is an ordinary released record.
    let aged_out = context(expired.now_ms + tight.grace_window_ms);
    let reaping = gc_namespace(&store, &source, &tight, &aged_out)
        .await
        .expect("gc past the release grace window");
    assert_record_and_basis_reaped(
        &store,
        &source,
        &reaping,
        &abandoned.checkpoint_id,
        &fork_record.manifest.manifest_no,
    )
    .await;
    stat_root(&store, &source).await;
}

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
            expires_at_ms: setup.now_ms + FORK_CHECKPOINT_LEASE_MS,
        },
        &setup,
    )
    .await
    .expect("leased fork record from the attempt that died");

    fork_namespace(&store, &source, &clone, None, &setup)
        .await
        .expect("fork retry after abandonment");
    let retry = store
        .list_prefix(&checkpoint_prefix(&source))
        .await
        .expect("list checkpoints")
        .len();
    assert_eq!(retry, 2, "the retry pins for itself instead of reusing");
    assert_eq!(
        checkpoint_lifecycle(&store, &source, &abandoned.checkpoint_id).await,
        CheckpointStatus::Active {},
        "the retry leaves the abandoned record alone"
    );

    // The target now reads through the retry's checkpoint, so the abandoned
    // record protects nothing and is reclaimable inside its own lease.
    let inside_the_lease = context(setup.now_ms + 1);
    let report = gc_namespace(&store, &source, &config(), &inside_the_lease)
        .await
        .expect("gc pass with a target that reads through another record");
    assert_eq!(report.released_checkpoints.fork, 1);
    assert_eq!(
        checkpoint_lifecycle(&store, &source, &abandoned.checkpoint_id).await,
        CheckpointStatus::Released {
            released_at_ms: inside_the_lease.now_ms
        }
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
    let aged = context(now_after_newest_object(store.inner(), &namespace_id, GRACE_MS + 1).await);

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
    let aged = context(now_after_newest_object(store.inner(), &namespace_id, GRACE_MS + 1).await);
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
        .expect_err("a corrupt root manifest fails the pass");
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
    assert_eq!(report.deleted.checkpoint_records, 0);
    assert_eq!(report.released_checkpoints.fork, 0);
    assert!(report.retained_candidates > 0);
    stat_root(&store, &namespace_id).await;
}

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
                namespace_id,
                &loonfs_api::WalSegmentId::parse(format!("wal_{index:020}-0000000000000000"))
                    .expect("valid WAL segment id"),
            ),
            metadata_segment(
                namespace_id,
                &loonfs_api::MetadataSegmentId::parse(format!("seg_{index:032x}"))
                    .expect("valid metadata segment id"),
            ),
            format!(
                "{}000-orphan-{index:02}.manifest.json",
                metadata_manifest_prefix(namespace_id)
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
        .filter(|key| !key.starts_with(&format!("namespaces/{namespace_id}/gc/")))
        .collect()
}

fn accumulate_report(total: &mut GcResponse, pass: &GcResponse) {
    total.deleted.add(&pass.deleted);
    total.released_checkpoints.add(&pass.released_checkpoints);
    total.retained_candidates += pass.retained_candidates;
    total.retained.add(&pass.retained);
    total.retention_degraded |= pass.retention_degraded;
    total.content_reclamation_deferred |= pass.content_reclamation_deferred;
    total.next_reclamation_at_ms = match (total.next_reclamation_at_ms, pass.next_reclamation_at_ms)
    {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    };
}

#[tokio::test]
async fn interrupted_revision_scan_resumes_at_the_saved_page_entry_and_block() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let store = FailStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::prefix(metadata_segment_prefix(&namespace_id)),
        OperationClass::Read,
        InjectedError::Transport("revision read interrupted".to_owned()),
    );
    add_bounded_gc_fixture(store.inner(), &namespace_id, &context(1_000)).await;
    let now = context(now_after_newest_object(store.inner(), &namespace_id, GRACE_MS + 1).await);
    let mut bounded = config();
    bounded.max_steps = Some(1);
    let mut passes = 0;
    let mut first_revision_position = None;
    let saved = loop {
        let pass = gc_namespace(&store, &namespace_id, &bounded, &now)
            .await
            .expect("bounded pass");
        bounded.cursor = pass.next_cursor;
        assert!(bounded.cursor.is_some(), "run must reach revision scanning");
        passes += 1;
        assert!(passes < 1_000, "marking must converge");
        let loaded = super::run::load_run(&store, &namespace_id)
            .await
            .expect("load progress")
            .expect("run");
        if let GcPhase::Revisions {
            position,
            block_index: 1,
            content,
            ..
        } = &loaded.state.phase
        {
            if first_revision_position.is_some_and(|first| first != *position)
                && content.merge.is_none()
            {
                break loaded.state;
            }
            first_revision_position.get_or_insert(*position);
        }
    };
    let GcPhase::Revisions {
        position,
        block_index,
        ..
    } = &saved.phase
    else {
        panic!("expected revision scan");
    };
    assert_eq!(*block_index, 1);
    let run_key = super::run::run_key(&namespace_id);
    let bytes = store
        .get(&run_key, None)
        .await
        .expect("saved bytes")
        .expect("run");
    let payload: serde_json::Value = serde_json::from_slice(&bytes).expect("run document");
    assert_eq!(
        payload["payload"]["phase"]["position"],
        serde_json::json!({
            "page_index": position.page_index,
            "entry_index": position.entry_index,
        })
    );
    assert_eq!(payload["payload"]["phase"]["block_index"], *block_index);

    store.fail_all();
    gc_namespace(&store, &namespace_id, &bounded, &now)
        .await
        .expect_err("revision read must fail");
    assert!(store.attempts() > 0);
    assert_eq!(
        store.get(&run_key, None).await.expect("saved bytes"),
        Some(bytes)
    );
    store.clear();

    gc_namespace(&store, &namespace_id, &bounded, &context(now.now_ms + 1))
        .await
        .expect("resume revision scan");
    let resumed = super::run::load_run(&store, &namespace_id)
        .await
        .expect("load resumed progress")
        .expect("run")
        .state;
    let GcPhase::Revisions {
        position: actual_position,
        block_index: actual_block_index,
        ..
    } = resumed.phase
    else {
        panic!("expected revision scan");
    };
    assert_eq!(actual_position.page_index, position.page_index);
    assert_eq!(actual_position.entry_index, position.entry_index + 1);
    assert_eq!(actual_block_index, 0);
    assert_eq!(resumed.step_no, saved.step_no + 1);
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
    // Keep every invocation small, including marking and cleanup.
    bounded_config.max_steps = Some(3);
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
            bounded_report.deleted.wal_segments,
            bounded_report.deleted.metadata_segments,
            bounded_report.deleted.manifests,
            bounded_report.deleted.checkpoint_records,
            bounded_report.deleted.upload_sessions,
            bounded_report.deleted.content_objects,
        ),
        (
            unbounded_report.deleted.wal_segments,
            unbounded_report.deleted.metadata_segments,
            unbounded_report.deleted.manifests,
            unbounded_report.deleted.checkpoint_records,
            unbounded_report.deleted.upload_sessions,
            unbounded_report.deleted.content_objects,
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
                &namespace_id,
                &loonfs_api::WalSegmentId::parse(format!("wal_{index:020}-0000000000000000"))
                    .expect("valid WAL segment id"),
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
    let wal_prefix = wal_segment_prefix(&namespace_id);
    let store = RecordingStore::new(
        ListingCursorStore::new(inner),
        KeyPredicate::prefix(wal_prefix.clone()),
    );
    let mut bounded = config();
    // Two candidates a pass, plus the roots the pass marks before it walks.
    bounded.max_steps = Some(marking_units(&store, &namespace_id, &aged).await + 2);
    store.reset();

    let first = gc_namespace(&store, &namespace_id, &bounded, &aged)
        .await
        .expect("first bounded pass");
    assert_eq!(first.deleted.wal_segments, 2);
    assert!(first.next_cursor.is_some());
    assert_eq!(store.counts().heads, 2);
    assert_eq!(store.counts().deletes, 2);
    for key in &orphan_keys[..2] {
        assert!(store.head(key).await.expect("head orphan").is_none());
    }
    assert!(store
        .head(&orphan_keys[2])
        .await
        .expect("head next orphan")
        .is_some());

    store.inner().take_calls();
    bounded.cursor = first.next_cursor;
    bounded.max_steps = Some(2);
    store.reset();
    let second = gc_namespace(&store, &namespace_id, &bounded, &aged)
        .await
        .expect("second bounded pass");
    assert_eq!(second.deleted.wal_segments, 2);
    assert!(second.next_cursor.is_some());
    assert_eq!(store.counts().heads, 2);
    assert_eq!(store.counts().deletes, 2);
    let resumed_wal_starts: Vec<Option<String>> = store
        .inner()
        .take_calls()
        .into_iter()
        .filter_map(|(prefix, start_after)| (prefix == wal_prefix).then_some(start_after))
        .collect();
    assert_eq!(
        resumed_wal_starts,
        vec![Some(orphan_keys[1].clone())],
        "the resumed family listing starts strictly after the cursor key"
    );
    for key in &orphan_keys[..4] {
        assert!(store.head(key).await.expect("head orphan").is_none());
    }

    bounded.cursor = second.next_cursor;
    loop {
        store.reset();
        let pass = gc_namespace(&store, &namespace_id, &bounded, &aged)
            .await
            .expect("remaining bounded pass");
        assert!(store.counts().heads <= 2);
        assert!(store.counts().deletes <= 2);
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
async fn stale_cursor_preserves_new_publications_with_the_original_cutoff() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    bootstrap_namespace(&store, &namespace_id, &setup, false)
        .await
        .expect("bootstrap");
    for index in 0..2 {
        let key = wal_segment(
            &namespace_id,
            &loonfs_api::WalSegmentId::parse(format!("wal_{index:020}-0000000000000000"))
                .expect("valid WAL segment id"),
        );
        store
            .put_if_absent(&key, Bytes::from_static(b"orphan"))
            .await
            .expect("write orphan");
    }
    let first_now = now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await;
    let mut bounded = config();
    // Stop shortly after marking so a publication lands before resumption.
    bounded.max_steps = Some(marking_units(&store, &namespace_id, &context(first_now)).await + 1);
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
    let live = live_set(&store, &namespace_id, &resume_context).await;

    let mut resume = config();
    resume.cursor = Some(cursor);
    gc_namespace(&store, &namespace_id, &resume, &resume_context)
        .await
        .expect("resume stale cursor");

    for key in live
        .wal_segments
        .iter()
        .chain(live.segments.iter())
        .chain(live.checkpoint_keys.iter())
    {
        assert!(
            store.head(key).await.expect("head live object").is_some(),
            "live object `{key}` must survive stale-cursor resumption"
        );
    }
    for manifest_number in live.manifests {
        let key = metadata_manifest_object(&namespace_id, &manifest_number);
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

#[tokio::test]
async fn one_step_calls_resume_on_another_host_without_aging_new_objects() {
    let dir = tempdir().expect("tempdir");
    let inner = LocalFsStore::new(dir.path()).expect("store");
    let ns = NamespaceId::parse("demo").expect("namespace");
    let setup = context(1000);
    namespace_with_a_scan_worth_bounding(&inner, &ns, &setup).await;
    let old_keys = namespace_keys(&inner, &ns).await;
    let start = context(now_after_newest_object(&inner, &ns, 0).await);
    let store = aged_before_now(inner, old_keys);
    let mut bounded = config();
    bounded.max_steps = Some(1);
    let first = gc_namespace(&store, &ns, &bounded, &start)
        .await
        .expect("reserve and capture");
    bounded.cursor = first.next_cursor;
    let late_key = wal_segment(
        &ns,
        &loonfs_api::WalSegmentId::parse("wal_00000000000000000000-ffffffffffffffff")
            .expect("segment"),
    );
    store
        .put_if_absent(&late_key, Bytes::from_static(b"new orphan"))
        .await
        .expect("late object");
    let other_host = mutation_context("gc-host-two", start.now_ms + 10 * GRACE_MS);
    let mut steps = 0;
    loop {
        let pass = gc_namespace(&store, &ns, &bounded, &other_host)
            .await
            .expect("resume on another host");
        let run = super::run::load_run(&store, &ns)
            .await
            .expect("load")
            .expect("run");
        assert_eq!(run.state.started_at_ms, start.now_ms);
        assert!(store
            .head(&late_key)
            .await
            .expect("head new object")
            .is_some());
        steps += 1;
        bounded.cursor = pass.next_cursor;
        if bounded.cursor.is_none() {
            break;
        }
        assert!(steps < 1000, "a one-step budget must finish");
    }
    assert!(steps > 20, "the scan and merges must require resumption");
    assert!(store
        .list_prefix(&loonfs_objectstore::keys::gc_runs_prefix(&ns))
        .await
        .expect("scratch")
        .is_empty());
    let fresh = gc_namespace(&store, &ns, &config(), &other_host)
        .await
        .expect("new collection");
    assert!(fresh.deleted.wal_segments > 0);
    assert!(
        store.head(&late_key).await.expect("head").is_none(),
        "the next run may use its newer cutoff"
    );
}

#[tokio::test]
async fn overlapping_collectors_share_progress_and_finish_the_same_run() {
    let dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(dir.path()).expect("store");
    let ns = NamespaceId::parse("demo").expect("namespace");
    let setup = context(1000);
    add_bounded_gc_fixture(&store, &ns, &setup).await;
    let now = context(
        now_after_newest_object(&store, &ns, UPLOAD_SESSION_LEASE_MS + 2 * GRACE_MS + 1).await,
    );
    let mut bounded = config();
    bounded.max_steps = Some(1);
    bounded.cursor = gc_namespace(&store, &ns, &bounded, &now)
        .await
        .expect("start")
        .next_cursor;
    let second_host = mutation_context("second-collector", now.now_ms + GRACE_MS);
    for step in 0..2000 {
        let before = super::run::load_run(&store, &ns)
            .await
            .expect("load")
            .expect("run");
        let (left, right) = tokio::join!(
            gc_namespace(&store, &ns, &bounded, &now),
            gc_namespace(&store, &ns, &bounded, &second_host)
        );
        let left = left.expect("first collector");
        let right = right.expect("second collector");
        let after = super::run::load_run(&store, &ns)
            .await
            .expect("load")
            .expect("run");
        assert_eq!(after.state.gc_run_id, before.state.gc_run_id);
        assert!(after.state.step_no > before.state.step_no);
        if matches!(after.state.phase, GcPhase::Complete {}) {
            break;
        }
        bounded.cursor = right.next_cursor.or(left.next_cursor);
        assert!(step < 1999, "concurrent marking must converge");
    }
    let view = load_current_metadata_view(&store, &ns)
        .await
        .expect("current view");
    view.resolve_path("/docs/5.txt", AttributeInclusion::Omit)
        .await
        .expect("published file remains readable");
}

#[tokio::test]
async fn a_paused_old_worker_cannot_advance_a_newer_run() {
    let dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(dir.path()).expect("store");
    let ns = NamespaceId::parse("demo").expect("namespace");
    let setup = context(1000);
    namespace_with_a_scan_worth_bounding(&store, &ns, &setup).await;
    let now = context(now_after_newest_object(&store, &ns, GRACE_MS + 1).await);
    let mut bounded = config();
    bounded.max_steps = Some(1);
    let first = gc_namespace(&store, &ns, &bounded, &now)
        .await
        .expect("start");
    bounded.cursor = first.next_cursor;
    let gated = BlockingStore::new(
        LocalFsStore::new(dir.path()).expect("second store"),
        KeyPredicate::prefix(metadata_manifest_prefix(&ns)),
        OperationClass::Read,
    );
    gated.block_next();
    let (old, new_run) = tokio::join!(gc_namespace(&gated, &ns, &bounded, &now), async {
        gated.wait_until_blocked().await;
        let mut finish = config();
        finish.cursor.clone_from(&bounded.cursor);
        gc_namespace(&store, &ns, &finish, &now)
            .await
            .expect("another host finishes the reserved run");
        let mut start_next = config();
        start_next.max_steps = Some(1);
        gc_namespace(&store, &ns, &start_next, &now)
            .await
            .expect("reserve next run");
        let new_run = super::run::load_run(&store, &ns)
            .await
            .expect("load")
            .expect("run");
        gated.release();
        new_run.state
    });
    old.expect("stale worker settles its lost CAS");
    assert_eq!(
        super::run::load_run(&store, &ns)
            .await
            .expect("load")
            .expect("run")
            .state,
        new_run
    );
    gc_namespace(&store, &ns, &config(), &now)
        .await
        .expect("finish new run and old scratch");
    assert!(store
        .list_prefix(&loonfs_objectstore::keys::gc_runs_prefix(&ns))
        .await
        .expect("scratch")
        .is_empty());
}

#[tokio::test]
async fn a_missing_completed_mark_page_stops_before_deleting_candidates() {
    let dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(dir.path()).expect("store");
    let ns = NamespaceId::parse("demo").expect("namespace");
    let setup = context(1000);
    namespace_with_a_scan_worth_bounding(&store, &ns, &setup).await;
    let now = context(now_after_newest_object(&store, &ns, GRACE_MS + 1).await);
    let mut bounded = config();
    bounded.max_steps = Some(1);
    loop {
        bounded.cursor = gc_namespace(&store, &ns, &bounded, &now)
            .await
            .expect("mark")
            .next_cursor;
        let state = super::run::load_run(&store, &ns)
            .await
            .expect("load")
            .expect("run")
            .state;
        if let GcPhase::Sweeping { table, .. } = state.phase {
            assert!(table.page_count > 0);
            let page_key =
                loonfs_objectstore::keys::gc_mark_page(&ns, &state.gc_run_id, &table.table_id, 0);
            store.delete(&page_key).await.expect("remove page");
            break;
        }
    }
    let before = namespace_keys(&store, &ns).await;
    bounded.max_steps = None;
    gc_namespace(&store, &ns, &bounded, &now)
        .await
        .expect_err("missing evidence is never an absent reference");
    assert_eq!(namespace_keys(&store, &ns).await, before);
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
async fn host_clock_error_advances_record_expiry_but_release_keeps_its_grace() {
    use crate::limits::GC_SAFETY_MARGIN_MS;

    let dir = tempdir().expect("tempdir");
    let store = MetadataMapStore::without_last_modified(
        LocalFsStore::new(dir.path()).expect("store"),
        KeyPredicate::any(),
    );
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let creator = context(1_000_000);
    bootstrap_namespace(&store, &namespace_id, &creator, false)
        .await
        .expect("bootstrap");
    let expires_at_ms = creator.now_ms + 2 * GC_SAFETY_MARGIN_MS;
    let checkpoint = crate::checkpoint::create_checkpoint(
        &store,
        &namespace_id,
        CheckpointOwner::User {
            name: "clock-boundary".to_owned(),
            expires_at_ms: Some(expires_at_ms),
        },
        &creator,
    )
    .await
    .expect("checkpoint");
    let key = loonfs_objectstore::keys::checkpoint_record(&namespace_id, &checkpoint.checkpoint_id);
    let store = RecordingStore::new(store, KeyPredicate::exact(key.clone()));
    let config = GcConfig {
        grace_window_ms: GC_MIN_GRACE_WINDOW_MS,
        ..config()
    };
    let clock_error = GC_SAFETY_MARGIN_MS / 2;
    let creator_at_expiry_check = expires_at_ms - clock_error;
    let collector = context(creator_at_expiry_check + clock_error);
    gc_namespace(
        &store,
        &namespace_id,
        &config,
        &context(collector.now_ms - 1),
    )
    .await
    .expect("before expiry");
    assert_eq!(store.counts().compare_and_swaps, 0);
    gc_namespace(&store, &namespace_id, &config, &collector)
        .await
        .expect("at expiry on the faster host");
    assert_eq!(store.counts().compare_and_swaps, 1);
    assert_eq!(
        checkpoint_lifecycle(&store, &namespace_id, &checkpoint.checkpoint_id).await,
        CheckpointStatus::Released {
            released_at_ms: collector.now_ms
        }
    );
    gc_namespace(
        &store,
        &namespace_id,
        &config,
        &context(collector.now_ms + GC_MIN_GRACE_WINDOW_MS - 1),
    )
    .await
    .expect("before release grace cutoff");
    assert_eq!(store.counts().deletes, 0);
    gc_namespace(
        &store,
        &namespace_id,
        &config,
        &context(collector.now_ms + GC_MIN_GRACE_WINDOW_MS),
    )
    .await
    .expect("at release grace cutoff");
    assert_eq!(store.counts().deletes, 1);
    assert!(store.head(&key).await.expect("head checkpoint").is_none());
}

#[tokio::test]
async fn retirement_requires_deleted_roots_and_uses_the_resuming_invocation_clock() {
    let directory = tempdir().expect("tempdir");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let namespace_id = NamespaceId::parse("retirement").expect("namespace id");
    let setup = context(1_000);
    bootstrap_namespace(&store, &namespace_id, &setup, false)
        .await
        .expect("bootstrap");
    let bounded = GcConfig {
        max_steps: Some(1),
        ..config()
    };
    let started = gc_namespace(&store, &namespace_id, &bounded, &setup)
        .await
        .expect("reserve active roots");
    delete_namespace(
        &store,
        &namespace_id,
        DeleteNamespaceOptions::default(),
        &setup,
    )
    .await
    .expect("delete");
    let resume = GcConfig {
        cursor: started.next_cursor,
        ..config()
    };
    let finished = gc_namespace(&store, &namespace_id, &resume, &context(GRACE_MS * 10))
        .await
        .expect("finish old run");
    assert_eq!(finished.reclaim_after_ms, None);
    let started = gc_namespace(&store, &namespace_id, &bounded, &context(GRACE_MS * 11))
        .await
        .expect("reserve deleted roots");
    let resume = GcConfig {
        cursor: started.next_cursor,
        ..config()
    };
    let fresh = GRACE_MS * 100;
    let finished = gc_namespace(&store, &namespace_id, &resume, &context(fresh))
        .await
        .expect("retire");
    assert_eq!(finished.reclaim_after_ms, Some(fresh + GRACE_MS));
    assert_eq!(finished.next_reclamation_at_ms, finished.reclaim_after_ms);
    let finished = gc_namespace(&store, &namespace_id, &config(), &context(fresh + GRACE_MS))
        .await
        .expect("deadline reached");
    assert_eq!(finished.reclaim_after_ms, Some(fresh + GRACE_MS));
    assert_eq!(finished.next_reclamation_at_ms, None);
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
        KeyPredicate::exact(wal_head(&namespace_id)),
        OperationClass::CompareAndSwap,
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
async fn uncertain_retirement_reads_back_and_failed_retirement_saves_no_progress() {
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
        let (mut state, _) = mark_state(&inner, &namespace_id, &setup)
            .await
            .expect("mark deleted roots");
        if let GcPhase::Sweeping { family, .. } = &mut state.phase {
            *family = GcCandidateFamily::Checkpoints;
        }
        let run_key = super::run::run_key(&namespace_id);
        let bytes = Bytes::from(
            loonfs_api::wire::control::encode_control_state(ControlObjectKind::GcRun, &state)
                .expect("encode run"),
        );
        inner
            .put_overwrite(&run_key, bytes.clone())
            .await
            .expect("save sweep");
        let store = FailStore::new(
            inner,
            KeyPredicate::exact(wal_head(&namespace_id)),
            OperationClass::CompareAndSwap,
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
        let head = crate::namespace::control::load_head_object(&store, &namespace_id)
            .await
            .expect("head");
        assert_eq!(
            head.state.status.reclaim_after_ms(),
            landed.then_some(GRACE_MS * 2)
        );
        if !landed {
            assert_eq!(
                store
                    .get(&run_key, None)
                    .await
                    .expect("run")
                    .expect("run exists"),
                bytes
            );
            assert_eq!(store.counts().compare_and_swaps, 1);
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
async fn retired_owner_sweep_is_bounded_resumable_and_lists_again() {
    let directory = tempdir().expect("tempdir");
    let mut store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    let namespace_id = NamespaceId::parse("a").expect("namespace");
    let (content_store_id, now) = retired_content_namespace(&store, &namespace_id).await;
    let keys = owned_content_keys(&store, &content_store_id, &namespace_id).await;
    let sibling = NamespaceId::parse("ab").expect("sibling");
    let sibling_keys = owned_content_keys(&store, &content_store_id, &sibling).await;
    let prefix = loonfs_objectstore::keys::content_owner_prefix(&content_store_id, &namespace_id);
    let unknown = format!("{prefix}unknown");
    store
        .put_if_absent(&unknown, Bytes::new())
        .await
        .expect("unknown key");
    let early = gc_namespace(
        &store,
        &namespace_id,
        &GcConfig {
            max_steps: Some(1),
            ..config()
        },
        &context(now.now_ms - 1),
    )
    .await
    .expect("early run");
    let early = gc_namespace(
        &store,
        &namespace_id,
        &GcConfig {
            cursor: early.next_cursor,
            ..config()
        },
        &now,
    )
    .await
    .expect("resume past deadline");
    assert_eq!(early.next_reclamation_at_ms, Some(now.now_ms));
    assert_eq!(early.deleted.retired_content_objects, 0);
    let mut bounded = GcConfig {
        max_steps: Some(1),
        ..config()
    };
    let mut deleted = 0;
    let mut retained = 0;
    for call in 0..100 {
        store.take();
        let report = gc_namespace(&store, &namespace_id, &bounded, &now)
            .await
            .expect("bounded run");
        assert!(store.counts().deletes <= 1);
        assert!(store.counts().lists <= 1);
        deleted += report.deleted.retired_content_objects;
        retained += report.retained.unrecognized_key;
        bounded.cursor = report.next_cursor;
        if bounded.cursor.is_none() {
            break;
        }
        store = RecordingStore::new(
            LocalFsStore::new(directory.path()).expect("reopen store"),
            KeyPredicate::any(),
        );
        assert!(call < 99, "bounded run must complete");
    }
    assert_eq!(deleted, keys.len() as u64);
    assert_eq!(retained, 1);
    assert_eq!(
        store.list_prefix(&prefix).await.expect("owner prefix"),
        std::slice::from_ref(&unknown)
    );
    for key in sibling_keys.iter().chain(
        [
            loonfs_objectstore::keys::wal_head(&namespace_id),
            loonfs_objectstore::keys::content_store(&content_store_id),
        ]
        .iter(),
    ) {
        assert!(store.head(key).await.expect("head").is_some());
    }
    let late_id = loonfs_api::ContentId::parse(format!("con_{:032x}", 1)).expect("late id");
    let late_key =
        loonfs_objectstore::keys::content_blob(&content_store_id, &namespace_id, &late_id);
    assert!(late_key < keys[0]);
    store
        .put_if_absent(&late_key, Bytes::from_static(b"late"))
        .await
        .expect("late object");
    let report = gc_namespace(&store, &namespace_id, &config(), &now)
        .await
        .expect("next run");
    assert_eq!(report.deleted.retired_content_objects, 1);
    assert_eq!(
        store.list_prefix(&prefix).await.expect("owner prefix"),
        [unknown]
    );
}

#[tokio::test]
async fn retired_owner_delete_failure_retries_the_failed_key() {
    let directory = tempdir().expect("tempdir");
    let inner = LocalFsStore::new(directory.path()).expect("store");
    let namespace_id = NamespaceId::parse("retry").expect("namespace");
    let (content_store_id, now) = retired_content_namespace(&inner, &namespace_id).await;
    let keys = owned_content_keys(&inner, &content_store_id, &namespace_id).await;
    let store = FailStore::new(
        inner,
        KeyPredicate::exact(keys[1].clone()),
        OperationClass::Delete,
        InjectedError::Transport("delete failed".to_owned()),
    );
    store.fail_next(1);
    assert!(gc_namespace(&store, &namespace_id, &config(), &now)
        .await
        .is_err());
    let state = super::run::load_run(&store, &namespace_id)
        .await
        .expect("load")
        .expect("run")
        .state;
    assert!(
        matches!(state.phase, GcPhase::Sweeping { family: GcCandidateFamily::OwnedContent, last_key: Some(ref key), .. } if key == &keys[0])
    );
    assert!(store.head(&keys[1]).await.expect("head").is_some());
    let report = gc_namespace(&store, &namespace_id, &config(), &now)
        .await
        .expect("retry");
    assert_eq!(report.deleted.retired_content_objects, 2);
    for key in keys {
        assert!(store.head(&key).await.expect("head").is_none());
    }
}

#[tokio::test]
async fn retired_owner_head_recheck_fails_without_writes() {
    use loonfs_api::wire::control::{encode_control_state, NamespaceStatus};
    for invalid in 0..5 {
        let directory = tempdir().expect("tempdir");
        let inner = LocalFsStore::new(directory.path()).expect("store");
        let namespace_id = NamespaceId::parse("head-check").expect("namespace");
        let (content_store_id, now) = retired_content_namespace(&inner, &namespace_id).await;
        owned_content_keys(&inner, &content_store_id, &namespace_id).await;
        let bounded = GcConfig {
            max_steps: Some(1),
            ..config()
        };
        for call in 0..100 {
            gc_namespace(&inner, &namespace_id, &bounded, &now)
                .await
                .expect("advance");
            let state = super::run::load_run(&inner, &namespace_id)
                .await
                .expect("load")
                .expect("run")
                .state;
            if matches!(
                state.phase,
                GcPhase::Sweeping {
                    family: GcCandidateFamily::OwnedContent,
                    last_key: None,
                    ..
                }
            ) {
                break;
            }
            assert!(call < 99, "must reach owner family");
        }
        let mut head = crate::namespace::control::load_head_object(&inner, &namespace_id)
            .await
            .expect("head")
            .state;
        match invalid {
            0 => head.content_store_id = ContentStoreId::generate(),
            1 => head.status = NamespaceStatus::Active {},
            2 => {
                head.status = NamespaceStatus::Deleted {
                    reclaim_after_ms: None,
                }
            }
            3 => {
                head.status = NamespaceStatus::Deleted {
                    reclaim_after_ms: Some(now.now_ms + 1),
                }
            }
            _ => {}
        }
        inner
            .put_overwrite(
                &wal_head(&namespace_id),
                Bytes::from(
                    encode_control_state(ControlObjectKind::WalHead, &head).expect("encode"),
                ),
            )
            .await
            .expect("replace head");
        let failure = FailStore::new(
            inner,
            KeyPredicate::exact(wal_head(&namespace_id)),
            OperationClass::Read,
            InjectedError::Transport("head read failed".to_owned()),
        );
        if invalid == 4 {
            failure.fail_next(1);
        }
        let store = RecordingStore::new(failure, KeyPredicate::any());
        let error = gc_namespace(&store, &namespace_id, &config(), &now)
            .await
            .expect_err("reject invalid head");
        if invalid < 4 {
            assert!(matches!(error, CoreError::NamespaceCorrupt(_)), "{error:?}");
        }
        assert_eq!(store.counts().deletes, 0);
        assert_eq!(store.counts().puts, 0);
    }
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
            manifest_no: ManifestNo(0),
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
    assert_eq!(report.deleted.manifests, 2);
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
