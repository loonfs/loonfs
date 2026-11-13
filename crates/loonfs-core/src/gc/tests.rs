//! Behavior tests for namespace GC.

use super::options::GcOptions;
use crate::commit::engine::{CommitCandidate, NamespaceCommitEngine};
use crate::error::CoreError;
use crate::in_flight::{STORE_READS_IN_FLIGHT, STORE_WRITES_IN_FLIGHT};
use crate::limits::{
    CONTENT_RECLAMATION_GRACE_WINDOW_MS, GC_MIN_GRACE_WINDOW_MS,
    NAMESPACE_RETIREMENT_GRACE_WINDOW_MS, UNREFERENCED_SEGMENT_GRACE_WINDOW_MS,
    UPLOAD_SESSION_LEASE_MS,
};
use crate::manifest::tests::{compact_a_family_group, create_snapshot, write_test_file, writer_at};
use crate::manifest::RetentionTarget;
use crate::path::write::{CommitIntent, FilesystemOperation};
use crate::test_support::WriterAt;
use crate::upload::records::UploadSessionState;
use futures::StreamExt;
use loonfs_objectstore::keys::{
    hint, metadata_manifest_object, metadata_manifest_prefix, metadata_segment,
    metadata_segment_prefix, snapshot_prefix, wal_prefix,
};
use loonfs_objectstore::ObjectStore;
use loonfs_objectstore::ObjectStoreExtension;
use loonfs_types::format::control::{
    decode_control_object, ControlObjectKind, SnapshotOwner, SnapshotPayload, UploadOutcome,
    UploadOutcomePayload,
};
use loonfs_types::{ContentRef, ManifestNo, NamespaceId, SnapshotId, UploadId};
use loonfs_types::{GcResponse, PoolGcResponse, PoolId};
use std::collections::BTreeSet;
use std::num::NonZeroUsize;
use std::sync::Arc;

use bytes::Bytes;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::PutMode;
use loonfs_test_support::ids::numbered_content_id;
use loonfs_test_support::stores::{
    replace_object, BlockingStore, ConcurrencyWatchStore, FailStore, InjectedError, KeyPredicate,
    MetadataMapStore, OperationContext, OperationKind, OperationSelector, RecordingStore,
};
use loonfs_types::options::AttributeInclusion;
use loonfs_types::options::DeleteNamespaceOptions;
use std::assert_matches;
use tempfile::tempdir;

mod concurrent_retirement;
mod extension_segments;
mod fork_chain;
mod listings;
mod many_snapshots;
mod pool_content;
mod pool_members;
mod pool_records;
mod pool_uploads;
mod replaced_manifests;
mod retirement;
mod session_cache;
mod shared_bases;
mod upload_records;

const GRACE_MS: u64 = 60 * 60 * 1000;

fn options() -> GcOptions {
    GcOptions {
        grace_window_ms: GRACE_MS,
    }
}

fn context(now_ms: u64) -> WriterAt {
    writer_at("gc-test", now_ms)
}

/// How long after its deletion a namespace retires under `options()`.
fn retirement_ms() -> u64 {
    GRACE_MS.max(NAMESPACE_RETIREMENT_GRACE_WINDOW_MS)
}

async fn snapshot_record_exists<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    snapshot_id: &SnapshotId,
) -> bool {
    crate::test_support::namespace_store(store, namespace_id)
        .load_snapshot_record(snapshot_id)
        .await
        .expect("read snapshot")
        .is_some()
}

/// Derives "now" from durable object ages so the tests never touch a
/// wall clock: `offset_ms` past the newest object under the namespace.
async fn now_after_newest_object<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    offset_ms: u64,
) -> u64 {
    // Every pool's objects count, so the helper doesn't read a manifest that
    // a test may have corrupted.
    let mut keys = store
        .list_prefix(&loonfs_objectstore::keys::namespace_prefix(namespace_id))
        .await
        .expect("list namespace");
    keys.extend(store.list_prefix("pools/").await.expect("list pools"));
    let mut newest = 0;
    for key in keys {
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
/// collection lists: every object except the discovery hint and extension objects.
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

/// Sums the listed sizes of the objects under the pool prefix that a pool's
/// collection lists: every object except the pool record.
async fn pool_listed_bytes<S: ObjectStore + ?Sized>(store: &S, pool_id: &PoolId) -> u64 {
    use futures::{future, TryStreamExt};
    let pool_record = loonfs_objectstore::keys::pool_record(pool_id);
    store
        .list_entries_from_stream(&loonfs_objectstore::keys::pool_prefix(pool_id), None)
        .try_filter(|entry| future::ready(entry.key != pool_record))
        .try_fold(0, |total, entry| future::ok(total + entry.size_bytes))
        .await
        .expect("list pool")
}

/// Runs one collection of `pool_id` under `options()` at the time of `context`.
async fn collect_pool<S: ObjectStore>(
    store: S,
    pool_id: &PoolId,
    context: &WriterAt,
) -> PoolGcResponse {
    crate::test_support::pool_store(store, pool_id)
        .gc(None, &options(), &context.clock())
        .await
        .expect("pool collection")
}

/// Runs one collection of `namespace_id` under `options()` at the time of
/// `context`.
async fn collect<S: ObjectStore>(
    store: S,
    namespace_id: &NamespaceId,
    context: &WriterAt,
) -> GcResponse {
    crate::test_support::namespace_store(store, namespace_id)
        .gc(&options(), &context.clock())
        .await
        .expect("collection")
}

/// Advances the retention floor to the head once the grace after `setup` has
/// passed.
async fn advance_floor_past<S: ObjectStore + Clone>(
    store: S,
    namespace_id: &NamespaceId,
    setup: &WriterAt,
) {
    crate::test_support::NamespaceFixture::new(
        store,
        namespace_id,
        &crate::test_support::writer_at(
            "test-writer",
            crate::test_support::grace_elapsed_after(setup.now_ms),
        ),
    )
    .engine()
    .advance_retention_floor(RetentionTarget::Head)
    .await
    .expect("advance floor");
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

#[tokio::test]
async fn gc_rejects_grace_windows_below_the_derived_minimum() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");

    let too_small = GcOptions {
        grace_window_ms: GC_MIN_GRACE_WINDOW_MS - 1,
    };
    let error = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(&too_small, &context(1_000).clock())
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
    let namespace = crate::test_support::namespace_store(store, namespace_id);
    let catalog = namespace.load_catalog_entry().await.expect("catalog");
    match namespace
        .load_upload_session_state(&catalog, upload_id)
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
    let pool_id = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap")
        .pool_id;
    let mut content_keys = publish_owned_content(&store, &namespace_id, 2).await;
    content_keys.sort();
    let unreferenced_key =
        loonfs_objectstore::keys::content_blob(&pool_id, &numbered_content_id(3));
    store
        .put_if_absent(&unreferenced_key, Bytes::from_static(b"unreferenced"))
        .await
        .expect("unreferenced object");
    create_snapshot(&store, &namespace_id, &setup)
        .await
        .expect("user snapshot");
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
    // The pool of a deleted first member that a snapshot keeps from retirement
    // still sweeps the content that no view refers to.
    assert_eq!(
        collect_pool(&store, &pool_id, &aged)
            .await
            .deleted
            .content_objects,
        1
    );
    let report = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(&options(), &aged.clock())
        .await
        .expect("collection");
    assert!(report.deleted.wal_objects >= 1);
    assert_eq!(report.deleted.metadata_segments, 0);
    assert!(report.deleted.manifests > 0);
    assert_eq!(report.deleted_snapshots_by_owner.user, 1);
    assert!(store
        .head(&unreferenced_key)
        .await
        .expect("unreferenced object")
        .is_none());
    let reaped = context(aged.now_ms + UNREFERENCED_SEGMENT_GRACE_WINDOW_MS);
    store.reset();
    let report = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(&options(), &reaped.clock())
        .await
        .expect("collection after segment grace");
    assert_eq!(
        report.deleted_snapshots_by_owner,
        loonfs_types::DeletedSnapshotsByOwner::default()
    );
    assert_eq!(report.deleted.metadata_segments, 0);
    assert!(report.deleted.manifests >= 1);
    // The retired first member leaves the pool without members, so the pool's
    // collection deletes the aged content.
    assert_eq!(
        collect_pool(&store, &pool_id, &reaped)
            .await
            .deleted
            .content_objects,
        content_keys.len() as u64
    );
    let content_prefix = loonfs_objectstore::keys::content_prefix(&pool_id);
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

    for prefix in [wal_prefix(&namespace_id), snapshot_prefix(&namespace_id)] {
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
        .gc(&options(), &again.clock())
        .await
        .expect("second collection");
    assert_eq!(report.deleted.wal_objects, 0);
    assert_eq!(report.deleted.manifests, 0);
    assert_eq!(report.deleted.metadata_segments, 0);
    assert_eq!(report.deleted_snapshots_by_owner, Default::default());
    assert_eq!(
        collect_pool(&store, &pool_id, &again).await.deleted,
        Default::default()
    );
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
    let fixture = crate::test_support::NamespaceFixture::new(&store, &source, &setup);
    fixture
        .engine()
        .fork_namespace(
            &clone,
            &loonfs_test_support::test_actor(),
            crate::test_support::folded_manifest(&fixture.namespace)
                .await
                .expect("fold the WAL tail"),
        )
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
    let basis_key = metadata_manifest_object(&source, &fork_record.snapshot_id.manifest_no());
    let aged = context(now_after_newest_object(&store, &source, GRACE_MS + 1).await);
    let report = collect(&store, &source, &aged).await;
    assert_eq!(report.deleted_snapshots_by_owner.fork, 0);
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
    let retired = collect(&store, &clone, &aged).await;
    let deadline = retired.retires_at_ms.expect("clone retired");

    let aged = context(deadline);
    collect(&store, &clone, &aged).await;
    assert!(!snapshot_record_exists(&store, &source, &fork_record.snapshot_id).await);

    let again = context(now_after_newest_object(&store, &source, GRACE_MS + 1).await);
    let report = collect(&store, &source, &again).await;
    assert_eq!(report.deleted_snapshots_by_owner.fork, 0);
    assert_eq!(report.deleted.manifests, 1);
    assert!(store
        .head(&basis_key)
        .await
        .expect("basis after release")
        .is_none());
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
            upload_id,
            loonfs_types::CompleteUploadBody::ServiceProxied {},
        )
        .await
        .expect("complete upload");
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
    let stored = crate::storage::content_staging::stage_bytes_under_content_id(
        store,
        catalog.pool_id(),
        content_id,
        bytes,
        crate::storage::content_staging::ContentKeyOwner::FreshIdentity,
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
        loonfs_types::ContentId::generate(),
        Bytes::copy_from_slice(namespace_id.as_str().as_bytes()),
        loonfs_types::ChecksumAlgorithm::Crc64nvme,
    );
    let catalog = crate::test_support::namespace_store(store, namespace_id)
        .load_catalog_entry()
        .await
        .expect("catalog");
    let key = loonfs_objectstore::keys::content_span(
        catalog.pool_id(),
        &value.content_ref().content_id,
        0,
        value.content_ref().size_bytes,
    );
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
    create_snapshot(&store, &namespace_id, &setup)
        .await
        .expect("snapshot");
    advance_floor_past(&store, &namespace_id, &setup).await;

    let orphan = metadata_segment(&namespace_id, &loonfs_types::MetadataSegmentId::generate());
    store
        .put_if_absent(&orphan, Bytes::from_static(b"unused"))
        .await
        .expect("young orphan");
    let young = context(now_after_newest_object(&store, &namespace_id, 0).await);
    let listed_bytes = core_listed_bytes(&store, &namespace_id).await;
    let report = collect(&store, &namespace_id, &young).await;

    assert_eq!(report.deleted.wal_objects, 0);
    assert_eq!(report.deleted.metadata_segments, 0);
    assert_eq!(report.deleted.manifests, 0);
    assert_eq!(report.listed_bytes, listed_bytes);
    assert!(report.retained.total() > 0);
    // Everything unreachable here is simply young, and the collection says so
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
        // Folds rather than creating snapshots: a snapshot keeps the manifest it
        // published, and a snapshot's manifest protects its segments forever, which
        // would leave this collection nothing to reap and nothing to prove.
        crate::test_support::namespace_store(&store, &namespace_id)
            .fold_wal()
            .await
            .expect("fold wal");
    }
    let staged = compact_a_family_group(&store, &namespace_id).await;

    // Far past every window this collector knows, including the staging one.
    let long_after = context(
        now_after_newest_object(
            &store,
            &namespace_id,
            UNREFERENCED_SEGMENT_GRACE_WINDOW_MS * 4,
        )
        .await,
    );
    let report = collect(&store, &namespace_id, &long_after).await;
    assert!(
        report.deleted.metadata_segments > 0,
        "the collection must have reaped the runs the job replaced, or it proves nothing"
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
async fn a_publication_during_a_collection_never_costs_the_job_its_segments() {
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
    // One clock for the job and the collection: the job stamps its lease at the same
    // instant the collection reads it by, which is what a running job's lease looks
    // like to a collector.
    let collection_clock =
        context(now_after_newest_object(&seed, &namespace_id, GRACE_MS + 1).await);
    let (policy, spec) =
        crate::manifest::tests::plan_a_family_group_compaction(&seed, &namespace_id).await;

    // The collection parks at its first listing, which is after it has collected the
    // roots and long before it reaches the compaction prefix. Everything the
    // job does happens in that gap, on a store the gate does not hold.
    let gated = BlockingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::prefix(wal_prefix(&namespace_id)),
        OperationSelector::List,
    );
    gated.block_next();
    let collection_options = options();
    let namespace = crate::test_support::namespace_store(&gated, &namespace_id);
    let clock = collection_clock.clock();
    let (report, staged) = tokio::join!(namespace.gc(&collection_options, &clock), async {
        gated.wait_until_blocked().await;
        crate::manifest::tests::publish_planned_compaction(&seed, &namespace_id, policy, &spec)
            .await;
        let staged =
            crate::manifest::tests::segment_keys_of_the_current_manifest(&seed, &namespace_id)
                .await;
        gated.release();
        staged
    });
    report.expect("the collection must finish");

    for key in &staged {
        assert!(
            seed.head(key)
                .await
                .expect("head a output segment")
                .is_some(),
            "a segment the manifest names must survive a collection whose live set predates it"
        );
    }
}

#[tokio::test]
async fn a_collection_names_a_snapshot_it_could_not_advance() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap");
    write_test_file(&store, &namespace_id, "/docs/one.txt", "gc-one", &setup).await;
    create_snapshot(&store, &namespace_id, &setup)
        .await
        .expect("snapshot");

    let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS * 2).await);
    let report = collect(&store, &namespace_id, &aged).await;

    assert_eq!(
        report.deleted_snapshots_by_owner,
        loonfs_types::DeletedSnapshotsByOwner::default()
    );
    assert_eq!(report.retained.snapshot_not_deletable, 1);
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
    create_snapshot(&store, &namespace_id, &setup)
        .await
        .expect("snapshot");
    advance_floor_past(&store, &namespace_id, &setup).await;
    // A commit past the floor: its WAL object is the live replay gap.
    write_test_file(&store, &namespace_id, "/docs/two.txt", "gc-two", &setup).await;

    let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
    let report = collect(&store, &namespace_id, &aged).await;

    // The advance's floor object is folded too.
    assert_eq!(report.deleted.wal_objects, 5);
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
    let report = collect(&store, &namespace_id, &aged).await;

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

    // Record-less maintenance: nothing accumulates under `snapshots/`.
    assert!(
        store
            .list_prefix(&snapshot_prefix(&namespace_id))
            .await
            .expect("list snapshots")
            .is_empty(),
        "a wal fold must not create snapshots"
    );

    let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
    let report = collect(&store, &namespace_id, &aged).await;

    // The first fold materialized the namespace's first manifest and the
    // next two superseded it; only the current manifest is reachable. Its
    // segments are all still referenced (a fold only appends delta runs).
    assert_eq!(report.deleted.manifests, 6);
    let manifests_left = store
        .list_prefix(&metadata_manifest_prefix(&namespace_id))
        .await
        .expect("list manifests");
    assert_eq!(manifests_left.len(), 1, "only the current manifest stays");

    // Compaction rewrites the delta runs into fresh base segments; the
    // superseded run segments then age out on the next collection.
    crate::test_support::compact_every_group(&store, &namespace_id).await;
    let aged = context(
        now_after_newest_object(
            &store,
            &namespace_id,
            UNREFERENCED_SEGMENT_GRACE_WINDOW_MS + 1,
        )
        .await,
    );
    let after_compaction = collect(&store, &namespace_id, &aged).await;
    assert!(
        after_compaction.deleted.metadata_segments > 0,
        "compacted-away run segments become collectable"
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
async fn gc_keeps_a_basis_kept_by_another_owner_after_one_release() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap");
    write_test_file(&store, &namespace_id, "/docs/one.txt", "gc-one", &setup).await;
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let first = namespace
        .create_snapshot_record(
            SnapshotOwner::User {
                name: Some("keeper".to_owned()),
                expires_at_ms: None,
            },
            crate::test_support::folded_manifest(&namespace)
                .await
                .expect("fold the WAL tail"),
            &setup.clock(),
        )
        .await
        .map(crate::snapshot::snapshot_record_summary)
        .expect("first owner");
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let second = namespace
        .create_snapshot_record(
            SnapshotOwner::User {
                name: Some("releaser".to_owned()),
                expires_at_ms: None,
            },
            crate::test_support::folded_manifest(&namespace)
                .await
                .expect("fold the WAL tail"),
            &setup.clock(),
        )
        .await
        .map(crate::snapshot::snapshot_record_summary)
        .expect("second owner");
    assert_ne!(first.snapshot_id, second.snapshot_id);
    assert_eq!(first.manifest_no, second.manifest_no);
    write_test_file(&store, &namespace_id, "/docs/two.txt", "gc-two", &setup).await;
    create_snapshot(&store, &namespace_id, &setup)
        .await
        .expect("advance past the shared basis");

    crate::test_support::NamespaceFixture::new(
        &store,
        &namespace_id,
        &crate::test_support::writer_at("test-writer", 0),
    )
    .engine()
    .delete_snapshot(&second.snapshot_id)
    .await
    .expect("release one owner");
    let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
    let first_collection = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(&options(), &aged.clock())
        .await
        .expect("first collection");
    assert_eq!(
        first_collection.deleted_snapshots_by_owner,
        loonfs_types::DeletedSnapshotsByOwner::default()
    );

    let second_collection = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(&options(), &aged.clock())
        .await
        .expect("second collection");
    assert_eq!(second_collection.deleted.manifests, 0);
    assert_eq!(
        second_collection.deleted_snapshots_by_owner,
        loonfs_types::DeletedSnapshotsByOwner::default()
    );

    let keeper = crate::test_support::namespace_store(&store, &namespace_id)
        .load_snapshot_record(&first.snapshot_id)
        .await
        .expect("read keeper record")
        .expect("the surviving owner's record stays")
        .state;
    assert!(
        crate::manifest::load_namespace_manifest_envelope(
            &store,
            &namespace_id,
            &keeper.snapshot_id.manifest_no(),
        )
        .await
        .is_ok(),
        "shared basis survives while any owner remains"
    );
}

#[tokio::test]
async fn gc_retains_active_snapshot_bases() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap");
    write_test_file(&store, &namespace_id, "/docs/one.txt", "gc-one", &setup).await;
    let first = create_snapshot(&store, &namespace_id, &setup)
        .await
        .expect("first snapshot");
    write_test_file(&store, &namespace_id, "/docs/two.txt", "gc-two", &setup).await;
    create_snapshot(&store, &namespace_id, &setup)
        .await
        .expect("second snapshot");
    // The runtime that published the snapshots' folds raises the hint.
    crate::test_support::namespace_store(&store, &namespace_id)
        .raise_hint_to_current_manifest()
        .await;

    let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
    let report = collect(&store, &namespace_id, &aged).await;

    // Only the bootstrap manifest, which no snapshot keeps, is collectable; both active
    // snapshot bases stay.
    assert_eq!(report.deleted.manifests, 3);
    assert_eq!(
        report.deleted_snapshots_by_owner,
        loonfs_types::DeletedSnapshotsByOwner::default()
    );
    let first_record = crate::test_support::namespace_store(&store, &namespace_id)
        .load_snapshot_record(&first.snapshot_id)
        .await
        .expect("read first snapshot")
        .expect("first snapshot exists")
        .state;
    assert!(crate::manifest::load_namespace_manifest_envelope(
        &store,
        &namespace_id,
        &first_record.snapshot_id.manifest_no(),
    )
    .await
    .is_ok());
}

/// Reads the single fork-owned record a fork left under the source.
async fn read_fork_record(store: &LocalFsStore, source: &NamespaceId) -> SnapshotPayload {
    for key in store
        .list_prefix(&snapshot_prefix(source))
        .await
        .expect("list snapshots")
    {
        let bytes = store
            .get(&key, None)
            .await
            .expect("get record")
            .expect("record exists");
        let record = decode_control_object::<SnapshotPayload>(&bytes, ControlObjectKind::Snapshot)
            .expect("decode record")
            .into_payload();
        if matches!(record.owner, SnapshotOwner::Fork { .. }) {
            return record;
        }
    }
    panic!("fork leaves one fork-owned record");
}

#[tokio::test]
async fn retired_targets_release_their_source_snapshots_and_retry_failed_deletes() {
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
    let fixture = crate::test_support::NamespaceFixture::new(&store, &source, &setup);
    fixture
        .engine()
        .fork_namespace(
            &clone,
            &loonfs_test_support::test_actor(),
            crate::test_support::folded_manifest(&fixture.namespace)
                .await
                .expect("fold the WAL tail"),
        )
        .await
        .expect("fork");
    let target_snapshot = create_snapshot(&store, &clone, &setup)
        .await
        .expect("materialize target manifest");
    crate::test_support::namespace_store(&store, &clone)
        .delete_snapshot_record(&target_snapshot.snapshot_id)
        .await
        .expect("release target snapshot");
    let fork_record = read_fork_record(&store, &source).await;
    // Advance the source manifest past the fork basis so the basis is
    // reachable only through the fork-owned record.
    write_test_file(&store, &source, "/docs/two.txt", "gc-two", &setup).await;
    create_snapshot(&store, &source, &setup)
        .await
        .expect("advance manifest past the fork basis");
    // The runtime that published the snapshot's fold raises the hint.
    crate::test_support::namespace_store(&store, &source)
        .raise_hint_to_current_manifest()
        .await;

    let before = context(now_after_newest_object(&store, &source, GRACE_MS + 1).await);
    let report = collect(&store, &source, &before).await;
    assert_eq!(report.deleted_snapshots_by_owner.fork, 0);

    crate::test_support::NamespaceFixture::new(&store, &clone, &setup)
        .engine()
        .delete_namespace(DeleteNamespaceOptions::default())
        .await
        .expect("terminal delete of the fork target");
    let aged = context(setup.now_ms + GRACE_MS - 1);

    let waiting = collect(&store, &source, &aged).await;
    assert_eq!(waiting.deleted_snapshots_by_owner.fork, 0);
    let retired = collect(&store, &clone, &aged).await;
    let deadline = retired.retires_at_ms.expect("target retired");
    let waiting = collect(&store, &source, &context(deadline - 1)).await;
    assert_eq!(waiting.deleted_snapshots_by_owner.fork, 0);
    assert!(snapshot_record_exists(&store, &source, &fork_record.snapshot_id).await);
    let aged = context(deadline);

    let snapshot_key = loonfs_objectstore::keys::snapshot_record(&source, &fork_record.snapshot_id);
    let store = RecordingStore::new(
        FailStore::new(
            store,
            KeyPredicate::exact(&snapshot_key),
            OperationSelector::Delete,
            InjectedError::Transport("fork snapshot delete failed".to_owned()),
        ),
        KeyPredicate::exact(&snapshot_key),
    );
    store.inner().fail_next(1);
    crate::test_support::namespace_store(&store, &clone)
        .gc(&options(), &aged.clock())
        .await
        .expect_err("a failed fork snapshot delete should fail the collection");
    assert!(snapshot_record_exists(&store, &source, &fork_record.snapshot_id).await);
    let released = collect(&store, &clone, &aged).await;
    assert_eq!(released.deleted_snapshots_by_owner.fork, 1);
    assert!(!snapshot_record_exists(&store, &source, &fork_record.snapshot_id).await);
    let repeated = collect(&store, &clone, &aged).await;
    assert_eq!(repeated.deleted_snapshots_by_owner.fork, 0);
    assert_eq!(store.counts().deletes, 3);
    assert!(store
        .head(&metadata_manifest_object(
            &source,
            &fork_record.snapshot_id.manifest_no()
        ))
        .await
        .expect("manifest")
        .is_some());
    let store = store.inner().inner();
    let aged = context(now_after_newest_object(store, &source, GRACE_MS + 1).await);
    collect(store, &source, &aged).await;
    assert_basis_reaped(store, &source, &fork_record.snapshot_id.manifest_no()).await;
    stat_root(store, &source).await;
}

#[tokio::test]
async fn an_unreadable_or_corrupt_fork_target_keeps_its_snapshot_and_the_collection_continues() {
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
    let fixture = crate::test_support::NamespaceFixture::new(&store, &source, &setup);
    fixture
        .engine()
        .fork_namespace(
            &clone,
            &loonfs_test_support::test_actor(),
            crate::test_support::folded_manifest(&fixture.namespace)
                .await
                .expect("fold the WAL tail"),
        )
        .await
        .expect("fork");
    let fork_record = read_fork_record(store.inner(), &source).await;

    let aged = context(setup.now_ms + GRACE_MS);
    store.fail_all();
    let report = collect(&store, &source, &aged).await;
    assert_eq!(report.deleted_snapshots_by_owner.fork, 0);
    assert!(snapshot_record_exists(store.inner(), &source, &fork_record.snapshot_id).await);

    store.clear();
    replace_object(&store, &hint(&clone), Bytes::from_static(b"not json"))
        .await
        .expect("corrupt target manifest");
    let report = collect(&store, &source, &aged).await;
    assert_eq!(report.deleted_snapshots_by_owner.fork, 0);
    assert!(snapshot_record_exists(store.inner(), &source, &fork_record.snapshot_id).await);
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
    let fixture = crate::test_support::NamespaceFixture::new(&store, &source, &setup);
    fixture
        .engine()
        .fork_namespace(
            &clone,
            &loonfs_test_support::test_actor(),
            crate::test_support::folded_manifest(&fixture.namespace)
                .await
                .expect("fold the WAL tail"),
        )
        .await
        .expect("fork");
    let fork_record = read_fork_record(&store, &source).await;
    assert!(
        fork_record.owner.expires_at_ms().is_none(),
        "fork snapshots carry no lease"
    );
    // Only the fork-owned record can protect the basis after this.
    write_test_file(&store, &source, "/docs/two.txt", "gc-two", &setup).await;
    create_snapshot(&store, &source, &setup)
        .await
        .expect("advance manifest past the fork basis");

    let grace_deadline = fork_record.created_at_ms + GRACE_MS;
    for now_ms in [
        now_after_newest_object(&store, &source, GRACE_MS + 1).await,
        grace_deadline,
        grace_deadline + GRACE_MS,
        u64::MAX / 2,
    ] {
        let report = collect(&store, &source, &context(now_ms)).await;
        assert_eq!(report.deleted_snapshots_by_owner.fork, 0, "at {now_ms}");
        assert_eq!(report.deleted_snapshots_by_owner.user, 0, "at {now_ms}");
        assert!(
            snapshot_record_exists(&store, &source, &fork_record.snapshot_id).await,
            "a live target keeps its snapshot at {now_ms}"
        );
    }
    assert!(crate::manifest::load_namespace_manifest_envelope(
        &store,
        &source,
        &fork_record.snapshot_id.manifest_no(),
    )
    .await
    .is_ok());
    crate::test_support::namespace_store(&store, &clone)
        .load_current_metadata_view()
        .await
        .expect("target readable after every collection")
        .resolve_path(
            "/docs/one.txt",
            AttributeInclusion::Omit,
            &crate::authorize::ReadAccess::live(crate::authorize::Authorizer::Unrestricted),
        )
        .await
        .expect("forked file readable");
}

#[tokio::test]
async fn a_fork_retry_keeps_young_snapshots_and_reclaims_the_abandoned_one_after_grace() {
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
    let namespace = crate::test_support::namespace_store(&store, &source);
    let abandoned = namespace
        .create_snapshot_record(
            SnapshotOwner::Fork {
                target_namespace_id: clone.clone(),
            },
            crate::test_support::folded_manifest(&namespace)
                .await
                .expect("fold the WAL tail"),
            &setup.clock(),
        )
        .await
        .map(crate::snapshot::snapshot_record_summary)
        .expect("fork snapshot from the abandoned attempt");

    let fixture = crate::test_support::NamespaceFixture::new(&store, &source, &setup);
    fixture
        .engine()
        .fork_namespace(
            &clone,
            &loonfs_test_support::test_actor(),
            crate::test_support::folded_manifest(&fixture.namespace)
                .await
                .expect("fold the WAL tail"),
        )
        .await
        .expect("fork retry after abandonment");
    let retry = store
        .list_prefix(&snapshot_prefix(&source))
        .await
        .expect("list snapshots")
        .len();
    assert_eq!(
        retry, 2,
        "the retry takes its own snapshot instead of reusing one"
    );
    assert!(
        snapshot_record_exists(&store, &source, &abandoned.snapshot_id).await,
        "the retry leaves the abandoned record alone"
    );

    let before_creation_grace = context(setup.now_ms + 1);
    let report = collect(&store, &source, &before_creation_grace).await;
    assert_eq!(report.deleted_snapshots_by_owner.fork, 0);
    assert!(snapshot_record_exists(&store, &source, &abandoned.snapshot_id).await);
    let report = collect(&store, &source, &context(setup.now_ms + GRACE_MS)).await;
    assert_eq!(report.deleted_snapshots_by_owner.fork, 1);
    assert!(!snapshot_record_exists(&store, &source, &abandoned.snapshot_id).await);
    assert_eq!(
        store
            .list_prefix(&snapshot_prefix(&source))
            .await
            .expect("list snapshots")
            .len(),
        1,
        "the retry's own snapshot stays"
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
async fn a_corrupt_snapshot_and_an_unreadable_one_both_fail_the_collection() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let store = FailStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::prefix(snapshot_prefix(&namespace_id)),
        OperationSelector::Read,
        InjectedError::Transport("snapshot read timed out".to_owned()),
    );
    let setup = context(1_000);
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap");
    write_test_file(&store, &namespace_id, "/docs/one.txt", "gc-one", &setup).await;
    create_snapshot(&store, &namespace_id, &setup)
        .await
        .expect("snapshot");

    let record_key = store
        .inner()
        .list_prefix(&snapshot_prefix(&namespace_id))
        .await
        .expect("list snapshots")
        .first()
        .expect("the snapshot wrote a record")
        .clone();
    let before = namespace_keys(store.inner(), &namespace_id).await;
    let aged = context(setup.now_ms);

    store.fail_all();
    let error = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(&options(), &aged.clock())
        .await
        .expect_err("a record the store will not read fails the collection");
    assert_eq!(error.code(), crate::error::ErrorCode::ServerError);
    assert!(
        error.to_string().contains(&record_key),
        "the error names the object: {error}"
    );
    assert_eq!(
        namespace_keys(store.inner(), &namespace_id).await,
        before,
        "a failed collection deletes nothing"
    );

    store.clear();
    replace_object(&store, &record_key, Bytes::from_static(b"not json"))
        .await
        .expect("corrupt record");
    let aged = context(setup.now_ms);
    let error = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(&options(), &aged.clock())
        .await
        .expect_err("a corrupt record fails the collection");
    assert_eq!(error.code(), crate::error::ErrorCode::NamespaceCorrupt);
    assert!(
        error.to_string().contains(&record_key),
        "the error names the object: {error}"
    );
    assert_eq!(
        namespace_keys(store.inner(), &namespace_id).await,
        before,
        "a failed collection deletes nothing"
    );
}

#[tokio::test]
async fn a_corrupt_or_unreadable_current_manifest_fails_the_collection() {
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
    create_snapshot(&store, &namespace_id, &setup)
        .await
        .expect("snapshot");

    let manifest_keys = store
        .inner()
        .list_prefix(&metadata_manifest_prefix(&namespace_id))
        .await
        .expect("list manifests");
    assert!(
        !manifest_keys.is_empty(),
        "the snapshot published a manifest"
    );
    let before = namespace_keys(store.inner(), &namespace_id).await;
    let aged = context(now_after_newest_object(store.inner(), &namespace_id, GRACE_MS + 1).await);

    store.fail_all();
    let error = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(&options(), &aged.clock())
        .await
        .expect_err("discovery read fails closed");
    assert_eq!(error.code(), crate::error::ErrorCode::ServerError);
    assert_eq!(
        namespace_keys(store.inner(), &namespace_id).await,
        before,
        "a degraded collection reclaims nothing in the affected families"
    );

    store.clear();
    for key in &manifest_keys {
        replace_object(&store, key, Bytes::from_static(b"not json"))
            .await
            .expect("corrupt manifest");
    }
    let aged = context(now_after_newest_object(store.inner(), &namespace_id, GRACE_MS + 1).await);
    let error = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(&options(), &aged.clock())
        .await
        .expect_err("a corrupt manifest fails the collection");
    assert_eq!(error.code(), crate::error::ErrorCode::NamespaceCorrupt);
    assert!(
        manifest_keys
            .iter()
            .any(|key| error.to_string().contains(key)),
        "the error names the object: {error}"
    );
    assert_eq!(
        namespace_keys(store.inner(), &namespace_id).await,
        before,
        "a failed collection deletes nothing"
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
    create_snapshot(&store, &namespace_id, &setup)
        .await
        .expect("snapshot");
    advance_floor_past(&store, &namespace_id, &setup).await;

    // Far past any window by wall clock, but no object carries a
    // provider timestamp.
    let aged = context(now_after_newest_object(store.inner(), &namespace_id, GRACE_MS + 1).await);
    let report = collect(&store, &namespace_id, &aged).await;

    assert_eq!(report.deleted.wal_objects, 0);
    assert_eq!(report.deleted.metadata_segments, 0);
    assert_eq!(report.deleted.manifests, 0);
    assert_eq!(
        report.deleted_snapshots_by_owner,
        loonfs_types::DeletedSnapshotsByOwner::default()
    );
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
        .gc(&options(), &context(u64::MAX).clock())
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

/// Creates a namespace with three content objects and deletes it, and returns
/// the time of its retirement deadline, its pool, and the content keys.
async fn retired_content_namespace<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
) -> (WriterAt, PoolId, Vec<String>) {
    let setup = context(1_000);
    let pool_id = crate::test_support::NamespaceFixture::new(store, namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap")
        .pool_id;
    let keys = publish_owned_content(store, namespace_id, 3).await;
    crate::test_support::NamespaceFixture::new(store, namespace_id, &setup)
        .engine()
        .delete_namespace(Default::default())
        .await
        .expect("delete");
    let report = crate::test_support::namespace_store(store, namespace_id)
        .gc(&options(), &setup.clock())
        .await
        .expect("retire");
    (
        context(report.retires_at_ms.expect("deadline")),
        pool_id,
        keys,
    )
}

async fn publish_owned_content<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    count: usize,
) -> Vec<String> {
    let pool_id = crate::test_support::load_pool_id(store, namespace_id).await;
    let mut keys = Vec::new();
    for number in 0..count {
        let content_ref = put_file_content_with_id(
            store,
            namespace_id,
            &format!("/file-{number}"),
            b"content",
            numbered_content_id(number + 1),
        )
        .await;
        keys.push(loonfs_objectstore::keys::content_blob(
            &pool_id,
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
        let pool_id = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
            .create()
            .await
            .expect("bootstrap")
            .pool_id;
        let (upload_id, content) =
            complete_upload_for_gc(&store, &namespace_id, b"content", &setup).await;
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
            .engine()
            .delete_namespace(Default::default())
            .await
            .expect("delete");
        let listed_bytes = core_listed_bytes(&store, &namespace_id).await;
        let unretired = context(first_run_ms);
        let report = crate::test_support::namespace_store(&store, &namespace_id)
            .gc(&options(), &unretired.clock())
            .await
            .expect("unretired run");
        assert_eq!(report.listed_bytes, listed_bytes);
        let listed_bytes = pool_listed_bytes(&store, &pool_id).await;
        let pool_report = collect_pool(&store, &pool_id, &unretired).await;
        assert_eq!(pool_report.listed_bytes, listed_bytes);
        assert_eq!(pool_report.deleted.upload_sessions, 0);
        assert_eq!(
            pool_report.retained.upload_session_undecided
                + pool_report.retained.upload_session_window,
            1
        );
        let key = loonfs_objectstore::keys::content_blob(&pool_id, &content.content_id);
        assert!(store.head(&key).await.expect("object").is_some());
        let retired = context(
            now_after_newest_object(&store, &namespace_id, GRACE_MS + 1)
                .await
                .max(report.retires_at_ms.expect("retired")),
        );
        crate::test_support::namespace_store(&store, &namespace_id)
            .gc(&options(), &retired.clock())
            .await
            .expect("retired run");
        let listed_bytes = pool_listed_bytes(&store, &pool_id).await;
        let report = collect_pool(&store, &pool_id, &retired).await;
        assert_eq!(report.listed_bytes, listed_bytes);
        assert_eq!(report.deleted.upload_sessions, 1);
        // The listed session protected its content in the collection that
        // deleted the session, so the next collection deletes the content.
        assert_eq!(report.deleted.content_objects, 0);
        let report = collect_pool(&store, &pool_id, &retired).await;
        assert_eq!(report.deleted.content_objects, 1);
        assert!(store.head(&key).await.expect("object").is_none());
        assert!(store
            .head(&loonfs_objectstore::keys::upload_session(
                &pool_id, &upload_id
            ))
            .await
            .expect("session")
            .is_none());
    }
}

#[tokio::test]
async fn gc_keeps_snapshot_and_current_numbers_and_preserves_discovery_from_a_lagging_hint() {
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
    let snapshot = create_snapshot(&store, &namespace_id, &setup)
        .await
        .expect("snapshot");
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
        .gc(&options(), &aged.clock())
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
        .gc(&options(), &aged.clock())
        .await
        .expect("collect after publication");
    assert_eq!(report.deleted.manifests, 7);
    assert_eq!(
        store
            .list_prefix(&metadata_manifest_prefix(&namespace_id))
            .await
            .expect("list for assertion"),
        vec![
            metadata_manifest_object(&namespace_id, &snapshot.manifest_no),
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
async fn concurrent_collectors_keep_snapshot_and_current_roots_and_young_objects() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("concurrent").expect("namespace");
    let inner = LocalFsStore::new(directory.path()).expect("store");
    let setup = context(1_000);
    crate::test_support::NamespaceFixture::new(&inner, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap");
    write_test_file(&inner, &namespace_id, "/one", "one", &setup).await;
    let snapshot = create_snapshot(&inner, &namespace_id, &setup)
        .await
        .expect("snapshot");
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
    let (first, second) = tokio::join!(namespace.gc(&gc_options, &clock), async {
        store.wait_until_blocked().await;
        let result = crate::test_support::namespace_store(store.inner(), &namespace_id)
            .gc(&gc_options, &clock)
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
        .head(&metadata_manifest_object(
            &namespace_id,
            &snapshot.manifest_no
        ))
        .await
        .expect("snapshot manifest")
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
            &options(),
            &context(UNREFERENCED_SEGMENT_GRACE_WINDOW_MS + 1).clock(),
        )
        .await
        .expect("collection");
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

/// Checks that the pool's collection deletes the content of a pool whose first
/// member was retired under the age rule, retries a failed delete, keeps the
/// pool record, and deletes a late write and an orphaned outcome record.
#[tokio::test]
async fn a_pool_without_members_retries_deletes_and_collects_late_writes() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("owner-sweep").expect("namespace");
    let inner = MetadataMapStore::aged(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    let (deadline, pool_id, keys) = retired_content_namespace(&inner, &namespace_id).await;
    let store = RecordingStore::new(
        FailStore::new(
            inner,
            KeyPredicate::exact(&keys[0]),
            OperationSelector::Delete,
            InjectedError::Transport("delete failed".to_owned()),
        ),
        KeyPredicate::any(),
    );
    let early = context(deadline.now_ms - 1);
    let before = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(&options(), &early.clock())
        .await
        .expect("before deadline");
    assert_eq!(before.next_reclamation_at_ms, Some(deadline.now_ms));
    let before = collect_pool(&store, &pool_id, &early).await;
    assert_eq!(before.deleted.content_objects, 0);
    crate::test_support::namespace_store(&store, &namespace_id)
        .gc(&options(), &deadline.clock())
        .await
        .expect("retire the first member");
    store.inner().fail_next(1);
    match crate::test_support::pool_store(&store, &pool_id)
        .gc(None, &options(), &deadline.clock())
        .await
    {
        Err(CoreError::Store { object_key, .. }) => assert_eq!(object_key, keys[0]),
        other => panic!("expected the failed delete's store error, got {other:?}"),
    }
    assert!(store.head(&keys[0]).await.expect("failed delete").is_some());
    let content_prefix = loonfs_objectstore::keys::content_prefix(&pool_id);
    let remaining = store
        .list_prefix(&content_prefix)
        .await
        .expect("remaining content");
    let retried = collect_pool(&store, &pool_id, &deadline).await;
    assert_eq!(retried.deleted.content_objects, remaining.len() as u64);
    assert!(store
        .list_prefix(&content_prefix)
        .await
        .expect("content after the retry")
        .is_empty());
    store
        .put_if_absent(&keys[0], Bytes::from_static(b"late write"))
        .await
        .expect("late content");
    let outcome = crate::test_support::proxied_upload_session(&namespace_id, deadline.now_ms);
    let outcome_key = loonfs_objectstore::keys::upload_outcome(&pool_id, &outcome.upload_id);
    store
        .put_if_absent(&outcome_key, Bytes::from_static(b"late outcome"))
        .await
        .expect("orphaned outcome record");
    let late = collect_pool(&store, &pool_id, &deadline).await;
    assert_eq!(late.deleted.content_objects, 1);
    assert_eq!(late.deleted.upload_sessions, 1);
    assert!(store.head(&outcome_key).await.expect("outcome").is_none());
    assert!(store
        .head(&loonfs_objectstore::keys::pool_record(&pool_id))
        .await
        .expect("pool record")
        .is_some());
    assert!(store
        .head(&hint(&namespace_id))
        .await
        .expect("tombstone")
        .is_some());
}

#[tokio::test]
async fn expiry_and_creation_grace_delete_snapshots_without_a_released_state() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("snapshot-grace").expect("namespace");
    let target = NamespaceId::parse("absent").expect("target");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let setup = context(1_000);
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap");
    let mut snapshots = Vec::new();
    for owner in [
        SnapshotOwner::User {
            name: Some("permanent".to_owned()),
            expires_at_ms: None,
        },
        SnapshotOwner::User {
            name: Some("expiring".to_owned()),
            expires_at_ms: Some(2_000),
        },
        SnapshotOwner::User {
            name: Some("snapshot".to_owned()),
            expires_at_ms: Some(2_000),
        },
        SnapshotOwner::Fork {
            target_namespace_id: target.clone(),
        },
    ] {
        let namespace = crate::test_support::namespace_store(&store, &namespace_id);
        snapshots.push(
            namespace
                .create_snapshot_record(
                    owner,
                    crate::test_support::folded_manifest(&namespace)
                        .await
                        .expect("fold the WAL tail"),
                    &setup.clock(),
                )
                .await
                .map(crate::snapshot::snapshot_record_summary)
                .expect("snapshot"),
        );
    }
    let before = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(&options(), &context(1_000 + GRACE_MS - 1).clock())
        .await
        .expect("before creation grace");
    assert_eq!(
        before.deleted_snapshots_by_owner,
        loonfs_types::DeletedSnapshotsByOwner::default()
    );
    let abandoned = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(&options(), &context(1_000 + GRACE_MS).clock())
        .await
        .expect("creation grace");
    assert_eq!(abandoned.deleted_snapshots_by_owner.fork, 1);
    assert!(!snapshot_record_exists(&store, &namespace_id, &snapshots[3].snapshot_id).await);
    assert!(namespace_keys(&store, &target).await.is_empty());
    let before_expiry = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(&options(), &context(2_000 + GRACE_MS - 1).clock())
        .await
        .expect("before expiry grace");
    assert_eq!(
        before_expiry.deleted_snapshots_by_owner,
        loonfs_types::DeletedSnapshotsByOwner::default()
    );
    let expired = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(&options(), &context(2_000 + GRACE_MS).clock())
        .await
        .expect("expiry grace");
    assert_eq!(expired.deleted_snapshots_by_owner.user, 2);
    assert!(snapshot_record_exists(&store, &namespace_id, &snapshots[0].snapshot_id).await);
    assert!(!snapshot_record_exists(&store, &namespace_id, &snapshots[1].snapshot_id).await);
    assert!(!snapshot_record_exists(&store, &namespace_id, &snapshots[2].snapshot_id).await);
    create_snapshot(&store, &namespace_id, &setup)
        .await
        .expect("second permanent snapshot");
    let deleted_at = 3_000 + GRACE_MS;
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context(deleted_at))
        .engine()
        .delete_namespace(Default::default())
        .await
        .expect("delete");
    let retired = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(&options(), &context(deleted_at).clock())
        .await
        .expect("retire");
    assert_eq!(
        retired.deleted_snapshots_by_owner,
        loonfs_types::DeletedSnapshotsByOwner {
            user: 2,
            ..Default::default()
        }
    );
    assert_eq!(retired.retires_at_ms, Some(deleted_at + retirement_ms()));
}

#[tokio::test]
async fn a_snapshot_naming_an_absent_manifest_is_corruption_before_sweeping() {
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
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let initial = namespace
        .create_snapshot_record(
            SnapshotOwner::User {
                name: Some("initial".to_owned()),
                expires_at_ms: None,
            },
            crate::test_support::folded_manifest(&namespace)
                .await
                .expect("fold the WAL tail"),
            &setup.clock(),
        )
        .await
        .map(crate::snapshot::snapshot_record_summary)
        .expect("snapshot");
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
        .gc(&options(), &context(GRACE_MS * 3).clock())
        .await
        .expect_err("missing snapshot manifest");
    assert_eq!(error.code(), loonfs_types::ErrorCode::NamespaceCorrupt);
    assert!(error
        .to_string()
        .contains(&loonfs_objectstore::keys::snapshot_record(
            &namespace_id,
            &initial.snapshot_id
        )));
    assert_eq!(store.counts().puts, 0);
    assert_eq!(store.counts().deletes, 0);
}

#[tokio::test]
async fn fork_snapshot_grace_skips_targets_and_aged_snapshots_read_only_manifest_discovery() {
    let directory = tempdir().expect("directory");
    let root = NamespaceId::parse("root").expect("root");
    let source = NamespaceId::parse("source").expect("source");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::prefix("namespaces/target-"),
    );
    let setup = context(1_000);
    crate::test_support::NamespaceFixture::new(&store, &root, &setup)
        .create()
        .await
        .expect("bootstrap");
    // A fork's collection reads no other member's roots, so only snapshot cleanup reads
    // the targets.
    let fixture = crate::test_support::NamespaceFixture::new(&store, &root, &setup);
    fixture
        .engine()
        .fork_namespace(
            &source,
            &loonfs_test_support::test_actor(),
            crate::test_support::folded_manifest(&fixture.namespace)
                .await
                .expect("fold the WAL tail"),
        )
        .await
        .expect("fork the source");
    let mut targets = Vec::new();
    for number in 0..6 {
        let target = NamespaceId::parse(format!("target-{number}")).expect("target");
        let fixture = crate::test_support::NamespaceFixture::new(&store, &source, &setup);
        fixture
            .engine()
            .fork_namespace(
                &target,
                &loonfs_test_support::test_actor(),
                crate::test_support::folded_manifest(&fixture.namespace)
                    .await
                    .expect("fold the WAL tail"),
            )
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
    let young = collect(&store, &source, &context(setup.now_ms + GRACE_MS - 1)).await;
    assert_eq!(young.deleted_snapshots_by_owner.fork, 0);
    assert!(store.snapshot().is_empty());
    let aged = collect(&store, &source, &context(setup.now_ms + GRACE_MS)).await;
    assert_eq!(aged.deleted_snapshots_by_owner.fork, 1);
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
            .list_prefix(&snapshot_prefix(&source))
            .await
            .expect("fork snapshots")
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
    let (deadline, pool_id, keys) = retired_content_namespace(&store, &namespace_id).await;
    for reclaimed in [false, true] {
        for allow_existing in [false, true] {
            store.reset();
            let error =
                crate::test_support::NamespaceFixture::new(&store, &namespace_id, &deadline)
                    .engine()
                    .bootstrap_namespace(
                        &loonfs_test_support::test_actor(),
                        &loonfs_types::options::CreateNamespaceOptions {
                            allow_existing,
                            ..Default::default()
                        },
                    )
                    .await
                    .expect_err("deleted id");
            assert_eq!(error.code(), loonfs_types::ErrorCode::NamespaceDeleted);
            // The creation's hint put is refused, and nothing is deleted.
            assert_eq!(store.counts().create_if_absent_puts, 1);
            assert_eq!(store.counts().compare_and_swaps, 0);
            assert_eq!(store.counts().deletes, 0);
        }
        for key in &keys {
            assert_eq!(store.head(key).await.expect("content").is_none(), reclaimed);
        }
        if !reclaimed {
            crate::test_support::namespace_store(&store, &namespace_id)
                .gc(&options(), &deadline.clock())
                .await
                .expect("retire");
            let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
            let report = collect_pool(&store, &pool_id, &aged).await;
            assert_eq!(report.deleted.content_objects, keys.len() as u64);
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
    )
}

async fn folded_namespace_with_two_files(store: &LocalFsStore, namespace_id: &NamespaceId) {
    let setup = context(1_000);
    crate::test_support::NamespaceFixture::new(store, namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap");
    for (number, path) in [(1, "/docs/one.txt"), (2, "/docs/two.txt")] {
        put_file_content_with_id(
            store,
            namespace_id,
            path,
            b"body\n",
            numbered_content_id(number),
        )
        .await;
    }
    create_snapshot(store, namespace_id, &setup)
        .await
        .expect("snapshot");
}

#[tokio::test]
async fn a_collection_reads_its_roots_through_the_given_cache_and_releases_their_memory() {
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
    crate::NamespaceStore {
        segment_cache: Arc::clone(&cache),
        ..crate::test_support::namespace_store(&store, &namespace_id)
    }
    .gc(&options(), &aged.clock())
    .await
    .expect("collection");
    assert!(
        peak.0.load(Ordering::SeqCst) > 0,
        "the roots were read and held through the shared memory"
    );
    assert_eq!(memory.in_use(), 0, "the collection released what it held");

    peak.0.store(0, Ordering::SeqCst);
    let pool_id = crate::test_support::load_pool_id(&store, &namespace_id).await;
    let report = crate::PoolStore {
        segment_cache: Arc::clone(&cache),
        ..crate::test_support::pool_store(&store, &pool_id)
    }
    .gc(None, &options(), &aged.clock())
    .await
    .expect("pool collection");
    assert_eq!(report.deleted.content_objects, 0);
    assert!(
        peak.0.load(Ordering::SeqCst) > 0,
        "the pool's roots were read and held through the shared memory"
    );
    assert_eq!(
        memory.in_use(),
        0,
        "the pool's collection released what it held"
    );
}

#[tokio::test]
async fn a_collection_fails_when_its_snapshot_manifests_do_not_fit_the_read_memory() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    folded_namespace_with_two_files(&store, &namespace_id).await;
    // Each rooted manifest charges its object keys and its snapshot's record, so
    // the most that a collection with one snapshot's manifest holds fits one
    // snapshot's manifest but not ten.
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Peak(AtomicUsize);
    impl crate::cache::ReadWorkingMemoryObserver for Peak {
        fn in_use(&self, bytes: usize) {
            self.0.fetch_max(bytes, Ordering::SeqCst);
        }
        fn reservation_failed(&self) {}
    }
    let peak = Arc::new(Peak(AtomicUsize::new(0)));
    let observer: Arc<dyn crate::cache::ReadWorkingMemoryObserver> = peak.clone();
    let measured = Arc::new(crate::cache::ReadWorkingMemory::new(
        64 * 1024 * 1024,
        Some(observer),
    ));
    let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
    crate::NamespaceStore {
        segment_cache: Arc::new(root_scan_cache(measured)),
        ..crate::test_support::namespace_store(&store, &namespace_id)
    }
    .gc(&options(), &aged.clock())
    .await
    .expect("measure one snapshot's manifest");
    let budget = peak.0.load(Ordering::SeqCst);
    let memory = Arc::new(crate::cache::ReadWorkingMemory::new(budget, None));
    let cache = Arc::new(root_scan_cache(Arc::clone(&memory)));
    crate::NamespaceStore {
        segment_cache: Arc::clone(&cache),
        ..crate::test_support::namespace_store(&store, &namespace_id)
    }
    .gc(&options(), &aged.clock())
    .await
    .expect("one snapshot's manifest fits the budget");

    let setup = context(1_000);
    for index in 0..9 {
        let (path, commit_id) = (
            format!("/docs/snapshot-{index}.txt"),
            format!("gc-snapshot-{index}"),
        );
        write_test_file(&store, &namespace_id, &path, &commit_id, &setup).await;
        create_snapshot(&store, &namespace_id, &setup)
            .await
            .expect("snapshot");
    }
    let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
    let error = crate::NamespaceStore {
        segment_cache: Arc::clone(&cache),
        ..crate::test_support::namespace_store(&store, &namespace_id)
    }
    .gc(&options(), &aged.clock())
    .await
    .expect_err("ten snapshots' manifests do not fit the budget");

    let CoreError::LiveSetExceedsReadMemory { limit, .. } = error else {
        panic!("expected LiveSetExceedsReadMemory, got {error:?}");
    };
    assert_eq!(limit, budget);
    assert_eq!(
        memory.in_use(),
        0,
        "the failed collection released what it held"
    );
    let pool_id = crate::test_support::load_pool_id(&store, &namespace_id).await;
    let content_prefix = loonfs_objectstore::keys::content_prefix(&pool_id);
    let mut listing = store.list_prefix_stream(&content_prefix);
    let mut content_objects = 0;
    while let Some(key) = listing.next().await {
        key.expect("listed key");
        content_objects += 1;
    }
    assert_eq!(content_objects, 11, "the failed collection deleted nothing");
}
