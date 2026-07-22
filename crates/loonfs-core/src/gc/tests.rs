//! Behavior tests for namespace GC.

use super::config::GcConfig;
use super::reap::list_prefix;
use super::run::{gc_namespace, gc_namespace_with_reverify_chunk};
use crate::checkpoint::advance_retention_floor;
use crate::checkpoint::record::set_checkpoint_record_state;
use crate::context::MutationContext;
use crate::error::CoreError;
use crate::limits::GC_MIN_GRACE_WINDOW_MS;
use loonfs_api::wire::control::{
    decode_control_object, CheckpointOwner, CheckpointRecordLifecycle, CheckpointRecordState,
    ControlObjectKind,
};
use loonfs_api::NamespaceId;
use loonfs_objectstore::keys::{
    checkpoint_prefix, metadata_manifest_object, metadata_manifest_prefix, metadata_table_prefix,
    namespace_config, wal_head, wal_segment_prefix,
};
use loonfs_objectstore::ObjectStore;

/// GC lifecycle tests pin as one user owner; owner-specific release
/// rules are exercised in the fork and release suites.
async fn create_checkpoint<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) -> Result<loonfs_api::CreateCheckpointResponse, CoreError> {
    crate::checkpoint::create_checkpoint(
        store,
        namespace_id,
        CheckpointOwner::User {
            name: "test-pin".to_owned(),
        },
        None,
        context,
    )
    .await
}
use crate::commit_engine::{NamespaceCommitEngine, NamespaceMutationCandidate};
use crate::namespace::bootstrap::bootstrap_namespace;
use crate::namespace::delete::delete_namespace;
use crate::namespace::fork::fork_namespace;
use crate::options::DeleteNamespaceOptions;
use crate::path::read::{load_metadata_view, ReadLoadContext};
use crate::publish::PathMutationIntent;
use crate::storage::content::{prepare_stored_content, store_bytes_as_content};
use bytes::Bytes;
use futures::stream::BoxStream;
use loonfs_api::{AbsolutePath, CommitId, DestinationBehavior};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::{ByteRange, ObjectBody, ObjectMetadata, ObjectStoreError, PutMode};
use std::sync::atomic::{AtomicUsize, Ordering};
use tempfile::tempdir;

const GRACE_MS: u64 = 60 * 60 * 1000;
const REAP_MS: u64 = 7 * 24 * 60 * 60 * 1000;

fn config() -> GcConfig {
    GcConfig {
        grace_window_ms: GRACE_MS,
        reap_window_ms: REAP_MS,
    }
}

fn context(now_ms: u64) -> MutationContext {
    MutationContext {
        writer_id: "gc-test".to_owned(),
        writer_session_id: "wrs_gc_test".to_owned(),
        writer_version: "gc-test/0.1.0".to_owned(),
        now_ms,
    }
}

/// Derives "now" from durable object ages so the tests never touch a
/// wall clock: `offset_ms` past the newest object under the namespace.
async fn now_after_newest_object(
    store: &LocalFsStore,
    namespace_id: &NamespaceId,
    offset_ms: u64,
) -> u64 {
    let prefix = format!("namespaces/{}/", namespace_id.as_str());
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

async fn write_file<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    path: &str,
    commit_id: &str,
    context: &MutationContext,
) {
    let stored = store_bytes_as_content(store, namespace_id, b"body\n")
        .await
        .expect("store content");
    let content_ref = stored.content_ref.clone();
    let prepared = prepare_stored_content(namespace_id.clone(), stored);
    NamespaceCommitEngine::new(namespace_id.clone())
        .publish_batch(
            store,
            vec![NamespaceMutationCandidate::path_prepared(
                PathMutationIntent::PutFile {
                    commit_id: CommitId::parse(commit_id).expect("commit id"),
                    absolute_path: AbsolutePath::parse(path).expect("path"),
                    content_ref: content_ref.clone(),
                    behavior: DestinationBehavior::NoReplace,
                },
                vec![prepared],
            )],
            context,
        )
        .await
        .results
        .pop()
        .expect("one result")
        .expect("write file");
}

async fn stat_root<S: ObjectStore>(store: &S, namespace_id: &NamespaceId) {
    load_metadata_view(store, namespace_id, ReadLoadContext::latest())
        .await
        .expect("load latest view")
        .resolve_path("/")
        .await
        .expect("resolve root");
}

/// `LocalFsStore` with provider timestamps stripped: rule 1 reads an
/// object without a timestamp as young, so nothing ever ages out.
#[derive(Debug)]
struct TimestamplessStore(LocalFsStore);

fn strip_timestamp(mut metadata: ObjectMetadata) -> ObjectMetadata {
    metadata.last_modified_ms = None;
    metadata
}

#[async_trait::async_trait]
impl ObjectStore for TimestamplessStore {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        Ok(self.0.head(key).await?.map(strip_timestamp))
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        Ok(self.0.get_with_metadata(key).await?.map(|mut body| {
            body.metadata = strip_timestamp(body.metadata);
            body
        }))
    }

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        self.0.get(key, range).await
    }

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        Ok(strip_timestamp(self.0.put(key, bytes, mode).await?))
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.0.delete(key).await
    }

    fn list_prefix_stream(
        &self,
        prefix: &str,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        self.0.list_prefix_stream(prefix)
    }
}

#[derive(Debug)]
struct IncompleteGcAccountingStore {
    inner: LocalFsStore,
    deletes: AtomicUsize,
    lists: AtomicUsize,
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
        reap_window_ms: REAP_MS,
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

    let inverted = GcConfig {
        grace_window_ms: GRACE_MS,
        reap_window_ms: GRACE_MS - 1,
    };
    let error = gc_namespace(&store, &namespace_id, &inverted, &context(1_000))
        .await
        .expect_err("reap below grace must be rejected");
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
    write_file(&store, &namespace_id, "/docs/one.txt", "gc-one", &setup).await;
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
    assert!(!report.incomplete_namespace_ignored);
    stat_root(&store, &namespace_id).await;
}

async fn write_upload_session(store: &LocalFsStore, namespace_id: &NamespaceId) -> String {
    let upload_id = loonfs_api::UploadId::parse("upl_0123456789abcdef0123456789abcdef")
        .expect("valid upload id");
    let state = loonfs_api::wire::control::UploadSessionState {
        namespace_id: namespace_id.clone(),
        upload_id: upload_id.clone(),
        mode: loonfs_api::v0::UploadMode::ServiceProxied,
        direct_put_content_ref: None,
        staged_content_ref: None,
        completed: None,
        created_at_ms: 1_000,
    };
    let envelope = loonfs_api::wire::control::UploadSessionEnvelope::from_state(
        loonfs_api::wire::control::ControlObjectKind::UploadSession,
        "gc-test/0.1.0",
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
    write_file(&store, &namespace_id, "/docs/one.txt", "gc-one", &setup).await;
    let pinned = create_checkpoint(&store, &namespace_id, &setup)
        .await
        .expect("first checkpoint");

    // Advance the root past the pinned basis so deleting the basis
    // object leaves the namespace itself healthy.
    write_file(&store, &namespace_id, "/docs/two.txt", "gc-two", &setup).await;
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
        loonfs_api::wire::control::CheckpointRecordLifecycle::Released
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
    write_file(&store, &namespace_id, "/docs/one.txt", "gc-one", &setup).await;
    write_file(&store, &namespace_id, "/docs/two.txt", "gc-two", &setup).await;
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
    for key in [
        loonfs_objectstore::keys::wal_head(namespace_id.as_str()),
        namespace_config(namespace_id.as_str()),
        loonfs_objectstore::keys::metadata_root(namespace_id.as_str()),
        loonfs_objectstore::keys::wal_floor(namespace_id.as_str()),
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
    write_file(&store, &source, "/docs/shared.txt", "gc-shared", &setup).await;
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

    // Once the clone is terminally deleted too, the record stops
    // rooting at collection time (its target is provably gone and both
    // namespaces are immutable tombstones, so revival is impossible):
    // one pass reclaims the basis and releases the record.
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

#[tokio::test]
async fn upload_sessions_reap_after_the_window_and_survive_inside_it() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    bootstrap_namespace(&store, &namespace_id, &setup, false)
        .await
        .expect("bootstrap");
    write_file(&store, &namespace_id, "/docs/one.txt", "gc-one", &setup).await;
    let session_key = write_upload_session(&store, &namespace_id).await;

    // Past the grace window but inside the reap window: sessions are
    // aged on the reap window, so this pass retains the session.
    let inside = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
    let report = gc_namespace(&store, &namespace_id, &config(), &inside)
        .await
        .expect("gc pass inside the reap window");
    assert_eq!(report.deleted_upload_sessions, 0);
    assert!(store
        .head(&session_key)
        .await
        .expect("head session")
        .is_some());

    // Past the reap window the session is dead whatever its state:
    // age is the whole decision, and the pass counts the deletion.
    let aged = context(now_after_newest_object(&store, &namespace_id, REAP_MS + 1).await);
    let report = gc_namespace(&store, &namespace_id, &config(), &aged)
        .await
        .expect("gc pass past the reap window");
    assert_eq!(report.deleted_upload_sessions, 1);
    assert!(store
        .head(&session_key)
        .await
        .expect("head session")
        .is_none());

    // The pass is idempotent: nothing left to count.
    let again = context(now_after_newest_object(&store, &namespace_id, REAP_MS + 1).await);
    let report = gc_namespace(&store, &namespace_id, &config(), &again)
        .await
        .expect("gc pass after the sweep");
    assert_eq!(report.deleted_upload_sessions, 0);
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
    write_file(&store, &namespace_id, "/docs/one.txt", "gc-one", &setup).await;
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
    write_file(&store, &namespace_id, "/docs/one.txt", "gc-one", &setup).await;
    create_checkpoint(&store, &namespace_id, &setup)
        .await
        .expect("checkpoint");
    advance_retention_floor(&store, &namespace_id, &setup)
        .await
        .expect("advance floor");
    // A commit past the floor: its segment is the live replay gap.
    write_file(&store, &namespace_id, "/docs/two.txt", "gc-two", &setup).await;

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
    write_file(&store, &namespace_id, "/docs/one.txt", "gc-one", &setup).await;
    let first = create_checkpoint(&store, &namespace_id, &setup)
        .await
        .expect("first checkpoint");
    write_file(&store, &namespace_id, "/docs/two.txt", "gc-two", &setup).await;
    create_checkpoint(&store, &namespace_id, &setup)
        .await
        .expect("second checkpoint");
    let first_record =
        crate::checkpoint::read_checkpoint_record(&store, &namespace_id, &first.checkpoint_id)
            .await
            .expect("read first record")
            .expect("first record exists")
            .state;
    set_checkpoint_record_state(
        &store,
        &namespace_id,
        &first.checkpoint_id,
        loonfs_api::wire::control::CheckpointRecordLifecycle::Released,
        &setup.writer_version,
    )
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
        write_file(
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

    // Three flushes superseded the bootstrap manifest and two
    // intermediates; only the root's manifest is reachable. Its tables
    // are all still referenced (a flush only appends L0 runs).
    assert_eq!(report.deleted_manifests, 3);
    assert!(!report.degraded_retention);
    let manifests_left = list_prefix(&store, &metadata_manifest_prefix(namespace_id.as_str()))
        .await
        .expect("list manifests");
    assert_eq!(manifests_left.len(), 1, "only the live root manifest stays");

    // Reorganization folds the L0 runs into fresh base segments; the
    // superseded run tables then age out on the next pass.
    let fold_policy = crate::checkpoint::MetadataLsmPolicy {
        max_l0_runs: 1,
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
    write_file(&store, &namespace_id, "/docs/one.txt", "gc-one", &setup).await;
    let pinned = create_checkpoint(&store, &namespace_id, &setup)
        .await
        .expect("pin checkpoint");
    write_file(&store, &namespace_id, "/docs/two.txt", "gc-two", &setup).await;
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

/// An expiring pin protects until its expiry, then follows the same
/// records-first cascade with no explicit release.
#[tokio::test]
async fn gc_reaps_expired_checkpoints_before_their_basis_across_passes() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    bootstrap_namespace(&store, &namespace_id, &setup, false)
        .await
        .expect("bootstrap");
    write_file(&store, &namespace_id, "/docs/one.txt", "gc-one", &setup).await;
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
    write_file(&store, &namespace_id, "/docs/two.txt", "gc-two", &setup).await;
    create_checkpoint(&store, &namespace_id, &setup)
        .await
        .expect("advance past the expiring basis");

    // Past expiry: record first, basis on the following pass.
    let expired = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
    assert!(
        expired.now_ms > 1_000 + GRACE_MS,
        "provider clock sits past the expiry"
    );
    let first_pass = gc_namespace(&store, &namespace_id, &config(), &expired)
        .await
        .expect("post-expiry pass");
    assert_eq!(first_pass.deleted_checkpoint_records, 1);
    let second_pass = gc_namespace(&store, &namespace_id, &config(), &expired)
        .await
        .expect("second post-expiry pass");
    let _ = second_pass;
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
    write_file(&store, &namespace_id, "/docs/one.txt", "gc-one", &setup).await;
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
    write_file(&store, &namespace_id, "/docs/two.txt", "gc-two", &setup).await;
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
    write_file(&store, &source, "/docs/one.txt", "gc-one", &setup).await;
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
    write_file(&store, &namespace_id, "/docs/one.txt", "gc-one", &setup).await;
    let first = create_checkpoint(&store, &namespace_id, &setup)
        .await
        .expect("first checkpoint");
    write_file(&store, &namespace_id, "/docs/two.txt", "gc-two", &setup).await;
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
/// record reaps on the next pass and its basis on the pass after that.
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
    write_file(&store, &source, "/docs/one.txt", "gc-one", &setup).await;
    fork_namespace(&store, &source, &clone, &setup)
        .await
        .expect("fork");
    let fork_record = read_fork_record(&store, &source).await;
    // Advance the source root past the fork basis so the basis is
    // reachable only through the fork-owned record.
    write_file(&store, &source, "/docs/two.txt", "gc-two", &setup).await;
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
    let flipped =
        crate::checkpoint::read_checkpoint_record(&store, &source, &fork_record.checkpoint_id)
            .await
            .expect("read record")
            .expect("record survives the pass that released it")
            .state;
    assert_eq!(flipped.state, CheckpointRecordLifecycle::Released);

    // The flip refreshed the record's timestamp; age it out again.
    let aged = context(now_after_newest_object(&store, &source, GRACE_MS + 1).await);
    let second_pass = gc_namespace(&store, &source, &config(), &aged)
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
    let third_pass = gc_namespace(&store, &source, &config(), &aged)
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

/// Rule 1 for fork records: a record younger than the grace window is
/// never released, even when its target is already terminally deleted.
#[tokio::test]
async fn gc_retains_young_fork_checkpoints_of_terminally_deleted_targets() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let source = NamespaceId::parse("source").expect("namespace id");
    let clone = NamespaceId::parse("clone").expect("namespace id");
    let setup = context(1_000);
    bootstrap_namespace(&store, &source, &setup, false)
        .await
        .expect("bootstrap");
    write_file(&store, &source, "/docs/one.txt", "gc-one", &setup).await;
    fork_namespace(&store, &source, &clone, &setup)
        .await
        .expect("fork");
    let fork_record = read_fork_record(&store, &source).await;
    delete_namespace(&store, &clone, DeleteNamespaceOptions::default(), &setup)
        .await
        .expect("terminal delete of the fork target");

    let young = context(now_after_newest_object(&store, &source, 0).await);
    let report = gc_namespace(&store, &source, &config(), &young)
        .await
        .expect("gc pass");

    assert_eq!(report.released_fork_checkpoints, 0);
    let record =
        crate::checkpoint::read_checkpoint_record(&store, &source, &fork_record.checkpoint_id)
            .await
            .expect("read record")
            .expect("young record survives")
            .state;
    assert_eq!(
        record.state,
        CheckpointRecordLifecycle::Active,
        "young fork record stays active even though it is releasable"
    );
    stat_root(&store, &source).await;
}

/// The abandoned-fork arm: once the target tree is completely gone and
/// the record has aged past the reap window, the record is released —
/// but never before that window, and never while any target object
/// remains.
#[tokio::test]
async fn gc_releases_abandoned_fork_checkpoints_after_the_reap_window() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let source = NamespaceId::parse("source").expect("namespace id");
    let clone = NamespaceId::parse("clone").expect("namespace id");
    let setup = context(1_000);
    bootstrap_namespace(&store, &source, &setup, false)
        .await
        .expect("bootstrap");
    write_file(&store, &source, "/docs/one.txt", "gc-one", &setup).await;
    fork_namespace(&store, &source, &clone, &setup)
        .await
        .expect("fork");
    let fork_record = read_fork_record(&store, &source).await;

    // Simulate rule 9 having reaped the abandoned target bootstrap.
    for key in store
        .list_prefix(&format!("namespaces/{}/", clone.as_str()))
        .await
        .expect("list target tree")
    {
        store.delete(&key).await.expect("reap target object");
    }

    // Aged past grace but inside the reap window: ambiguity retains.
    let inside_reap = context(now_after_newest_object(&store, &source, GRACE_MS + 1).await);
    let early = gc_namespace(&store, &source, &config(), &inside_reap)
        .await
        .expect("gc inside the reap window");
    assert_eq!(early.released_fork_checkpoints, 0);

    // Past the reap window: the record is provably abandoned.
    let past_reap = context(now_after_newest_object(&store, &source, REAP_MS + 1).await);
    let report = gc_namespace(&store, &source, &config(), &past_reap)
        .await
        .expect("gc past the reap window");
    assert_eq!(report.released_fork_checkpoints, 1);
    let record =
        crate::checkpoint::read_checkpoint_record(&store, &source, &fork_record.checkpoint_id)
            .await
            .expect("read record")
            .expect("record survives the releasing pass")
            .state;
    assert_eq!(record.state, CheckpointRecordLifecycle::Released);
    stat_root(&store, &source).await;
}

/// A fork retry after abandonment revives the released record through
/// the freshen compare-and-swap and completes the target.
#[tokio::test]
async fn abandoned_fork_retry_revives_and_freshens_the_source_record() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let source = NamespaceId::parse("source").expect("namespace id");
    let clone = NamespaceId::parse("clone").expect("namespace id");
    let setup = context(1_000);
    bootstrap_namespace(&store, &source, &setup, false)
        .await
        .expect("bootstrap");
    write_file(&store, &source, "/docs/one.txt", "gc-one", &setup).await;
    fork_namespace(&store, &source, &clone, &setup)
        .await
        .expect("fork");
    let fork_record = read_fork_record(&store, &source).await;

    // Abandonment: the target tree is reaped and a GC pass has already
    // released the source record.
    for key in store
        .list_prefix(&format!("namespaces/{}/", clone.as_str()))
        .await
        .expect("list target tree")
    {
        store.delete(&key).await.expect("reap target object");
    }
    set_checkpoint_record_state(
        &store,
        &source,
        &fork_record.checkpoint_id,
        CheckpointRecordLifecycle::Released,
        &setup.writer_version,
    )
    .await
    .expect("release the fork record");

    fork_namespace(&store, &source, &clone, &setup)
        .await
        .expect("fork retry after abandonment");
    let revived =
        crate::checkpoint::read_checkpoint_record(&store, &source, &fork_record.checkpoint_id)
            .await
            .expect("read record")
            .expect("record revived")
            .state;
    assert_eq!(revived.state, CheckpointRecordLifecycle::Active);
    crate::path::read::load_metadata_view(
        &store,
        &clone,
        crate::path::read::ReadLoadContext::latest(),
    )
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
    write_file(&store, &namespace_id, "/docs/one.txt", "gc-one", &setup).await;
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
    let store = TimestamplessStore(LocalFsStore::new(temp_dir.path()).expect("store"));
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    bootstrap_namespace(&store, &namespace_id, &setup, false)
        .await
        .expect("bootstrap");
    write_file(&store, &namespace_id, "/docs/one.txt", "gc-one", &setup).await;
    create_checkpoint(&store, &namespace_id, &setup)
        .await
        .expect("checkpoint");
    advance_retention_floor(&store, &namespace_id, &setup)
        .await
        .expect("advance floor");

    // Far past any window by wall clock, but no object carries a
    // provider timestamp.
    let aged = context(now_after_newest_object(&store.0, &namespace_id, GRACE_MS + 1).await);
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
    write_file(&store, &namespace_id, "/docs/one.txt", "gc-one", &setup).await;
    let first = create_checkpoint(&store, &namespace_id, &setup)
        .await
        .expect("first checkpoint");
    write_file(&store, &namespace_id, "/docs/two.txt", "gc-two", &setup).await;
    create_checkpoint(&store, &namespace_id, &setup)
        .await
        .expect("second checkpoint");
    set_checkpoint_record_state(
        &store,
        &namespace_id,
        &first.checkpoint_id,
        loonfs_api::wire::control::CheckpointRecordLifecycle::Released,
        &setup.writer_version,
    )
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

#[tokio::test]
async fn gc_ignores_incomplete_namespace_without_listing_or_deleting() {
    let temp_dir = tempdir().expect("tempdir");
    let inner = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("orphan").expect("namespace id");
    // A partial tree: a head object but no namespace.json completion
    // marker.
    inner
        .put_if_absent(
            &format!("namespaces/{}/wal/head.json", namespace_id.as_str()),
            bytes::Bytes::from_static(b"{}"),
        )
        .await
        .expect("write partial head");
    let store = IncompleteGcAccountingStore {
        inner,
        deletes: AtomicUsize::new(0),
        lists: AtomicUsize::new(0),
    };

    let report = gc_namespace(&store, &namespace_id, &config(), &context(u64::MAX))
        .await
        .expect("gc incomplete tree");
    assert!(report.incomplete_namespace_ignored);
    assert_eq!(store.lists.load(Ordering::SeqCst), 0);
    assert_eq!(store.deletes.load(Ordering::SeqCst), 0);
    assert!(store
        .head(&wal_head(namespace_id.as_str()))
        .await
        .expect("head partial object")
        .is_some());
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
    write_file(&store, &source, "/docs/one.txt", "gc-one", &setup).await;
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
