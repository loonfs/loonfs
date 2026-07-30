//! Upload session begin, content, direct-put, and completion guards.

#![allow(clippy::panic)]
// These integration tests use panic in unexpected match arms for precise diagnostics.

use crate::common::commit_split_support::*;
use crate::common::namespace_engine;
use bytes::Bytes;
use loonfs_api::{
    sha256_digest,
    v0::{CompleteUploadRequest, UploadMode},
    wire::control::{
        encode_control_object, ControlObjectKind, UploadSessionEnvelope, UploadSessionState,
    },
    ContentRef, ContentRefKind, DestinationBehavior, NamespaceId, UploadId,
};
use loonfs_core::content::store_bytes_as_content;
use loonfs_core::{
    BeginDirectPutUploadTargetResponse, Error as CoreError, ErrorCode, MutationContext,
};
use loonfs_objectstore::keys::{upload_session, upload_session_prefix};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::ObjectStore;
use loonfs_test_support::stores::{FailStore, InjectedError, KeyPredicate, OperationClass};
use std::path::Path;
use tempfile::tempdir;

async fn begin_upload<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) -> Result<loonfs_api::v0::BeginUploadResponse, CoreError> {
    namespace_engine(store, namespace_id, context)
        .begin_upload(loonfs_api::v0::BeginUploadRequest::default())
        .await
}

async fn begin_direct_put_upload_target<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    content_ref: ContentRef,
    context: &MutationContext,
) -> Result<BeginDirectPutUploadTargetResponse, CoreError> {
    namespace_engine(store, namespace_id, context)
        .begin_direct_put_upload_target(content_ref)
        .await
}

async fn upload_content<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    upload_id: &UploadId,
    bytes: &[u8],
    context: &MutationContext,
) -> Result<loonfs_api::v0::UploadContentResponse, CoreError> {
    namespace_engine(store, namespace_id, context)
        .upload_content(upload_id, bytes)
        .await
}

async fn complete_upload<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    upload_id: &UploadId,
    request: &CompleteUploadRequest,
    context: &MutationContext,
) -> Result<loonfs_api::v0::CompleteUploadResponse, CoreError> {
    namespace_engine(store, namespace_id, context)
        .complete_upload(upload_id, request)
        .await
}

fn replay_read_guard_store(root: impl AsRef<Path>, namespace: &str) -> FailStore<LocalFsStore> {
    let wal_prefix = format!("namespaces/{namespace}/wal/segments/");
    let manifest_prefix = format!("namespaces/{namespace}/metadata/manifests/");
    let store = FailStore::new(
        LocalFsStore::new(root.as_ref()).expect("store"),
        KeyPredicate::new(move |key| {
            key.starts_with(&wal_prefix) || key.starts_with(&manifest_prefix)
        }),
        OperationClass::Read,
        InjectedError::Transport("begin_upload unexpectedly read replay object".to_owned()),
    );
    store.fail_all();
    store
}

/// Upload admission is exactly the head: absent means the namespace was
/// never created, and the deletion tombstone refuses.
#[tokio::test]
async fn begin_upload_rejects_missing_and_deleted_namespaces() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();

    let missing_error = begin_upload(&store, &namespace_id, &context)
        .await
        .expect_err("missing namespace");
    assert_eq!(missing_error.code(), ErrorCode::NamespaceNotFound);

    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    begin_upload(&store, &namespace_id, &context)
        .await
        .expect("a live namespace admits uploads");

    loonfs_core::NamespaceEngine::builder(LocalFsStore::new(temp_dir.path()).expect("store"))
        .namespace_id(namespace_id.clone())
        .writer_id("writer-a")
        .build()
        .expect("engine")
        .delete_namespace(loonfs_core::DeleteNamespaceOptions::default())
        .await
        .expect("delete namespace");

    let deleted_error = begin_upload(&store, &namespace_id, &context)
        .await
        .expect_err("deleted namespace");
    assert_eq!(deleted_error.code(), ErrorCode::NamespaceDeleted);
}

#[tokio::test]
async fn begin_direct_put_rejects_unsupported_content_ref_without_session() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");

    let content_ref = ContentRef {
        kind: ContentRefKind::Unsupported("future_kind".to_owned()),
        digest: sha256_digest(b"hello"),
        size_bytes: 5,
    };
    let error = begin_direct_put_upload_target(&store, &namespace_id, content_ref, &context)
        .await
        .expect_err("unsupported direct_put content ref");

    assert_eq!(error.code(), ErrorCode::InvalidRequest);
    assert_eq!(
        store
            .list_prefix(&upload_session_prefix(namespace_id.as_str()))
            .await
            .expect("list upload sessions"),
        Vec::<String>::new()
    );
}

#[tokio::test]
async fn begin_upload_does_not_read_manifest_or_wal_replay_objects() {
    let temp_dir = tempdir().expect("tempdir");
    let setup_store = LocalFsStore::new(temp_dir.path()).expect("setup store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();

    bootstrap_namespace(&setup_store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    put_file_bytes(
        &setup_store,
        &namespace_id,
        "/docs/hello.txt",
        b"hello",
        DestinationBehavior::NoReplace,
        &context,
        Some("upload-guard-create"),
    )
    .await
    .expect("create file");
    create_checkpoint(&setup_store, &namespace_id, &context)
        .await
        .expect("checkpoint");
    put_file_bytes(
        &setup_store,
        &namespace_id,
        "/docs/hello.txt",
        b"updated",
        DestinationBehavior::Replace,
        &context,
        Some("upload-guard-replace"),
    )
    .await
    .expect("replace file");

    let guarded_store = replay_read_guard_store(temp_dir.path(), namespace_id.as_str());
    let begin = begin_upload(&guarded_store, &namespace_id, &context)
        .await
        .expect("begin upload");
    assert_eq!(begin.namespace_id, namespace_id);
    assert_eq!(guarded_store.attempts(), 0);
}

#[tokio::test]
async fn complete_upload_does_not_get_content_blob_after_staging() {
    let temp_dir = tempdir().expect("tempdir");
    let store = content_blob_counting_store(temp_dir.path());
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();

    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    let begin = begin_upload(&store, &namespace_id, &context)
        .await
        .expect("begin upload");
    let uploaded = upload_content(&store, &namespace_id, &begin.upload_id, b"hello", &context)
        .await
        .expect("upload content");

    store.reset();
    let completed = complete_upload(
        &store,
        &namespace_id,
        &begin.upload_id,
        &CompleteUploadRequest {
            content_ref: uploaded.content_ref.clone(),
        },
        &context,
    )
    .await
    .expect("complete upload");
    assert_eq!(completed.content_ref, uploaded.content_ref);
    assert_eq!(store.count(OperationClass::Read), 0);

    store.reset();
    let completed_again = complete_upload(
        &store,
        &namespace_id,
        &begin.upload_id,
        &CompleteUploadRequest {
            content_ref: uploaded.content_ref,
        },
        &context,
    )
    .await
    .expect("complete upload idempotently");
    assert_eq!(completed_again.content_ref, completed.content_ref);
    assert_eq!(store.count(OperationClass::Read), 0);

    let mismatch_begin = begin_upload(&store, &namespace_id, &context)
        .await
        .expect("begin mismatch");
    let mismatch_uploaded = upload_content(
        &store,
        &namespace_id,
        &mismatch_begin.upload_id,
        b"staged",
        &context,
    )
    .await
    .expect("upload mismatch content");
    let wrong_ref = ContentRef::whole_file_v0(b"different");
    assert_ne!(wrong_ref, mismatch_uploaded.content_ref);

    store.reset();
    let mismatch = complete_upload(
        &store,
        &namespace_id,
        &mismatch_begin.upload_id,
        &CompleteUploadRequest {
            content_ref: wrong_ref,
        },
        &context,
    )
    .await
    .expect_err("mismatched content ref");
    assert_eq!(mismatch.code(), ErrorCode::InvalidRequest);
    assert_eq!(store.count(OperationClass::Read), 0);
}

#[tokio::test]
async fn complete_upload_rejects_direct_put_session_without_bound_target() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    let stored = store_bytes_as_content(&store, &namespace_id, b"hello")
        .await
        .expect("store content");

    let upload_id =
        UploadId::parse("upl_00000000000000000000000000000001").expect("valid upload id");
    let state = UploadSessionState {
        namespace_id: namespace_id.clone(),
        upload_id: upload_id.clone(),
        mode: UploadMode::DirectPut,
        direct_put_content_ref: None,
        staged_content_ref: None,
        completed: None,
        created_at_ms: context.now_ms,
        state: loonfs_api::wire::control::UploadSessionLifecycle::Active,
    };
    let envelope = UploadSessionEnvelope::from_state(ControlObjectKind::UploadSession, state)
        .expect("upload session envelope");
    let encoded = encode_control_object(&envelope).expect("encode upload session");
    store
        .put_if_absent(
            &upload_session(namespace_id.as_str(), upload_id.as_str()),
            Bytes::from(encoded),
        )
        .await
        .expect("write malformed upload session");

    let error = complete_upload(
        &store,
        &namespace_id,
        &upload_id,
        &CompleteUploadRequest {
            content_ref: stored.content_ref,
        },
        &context,
    )
    .await
    .expect_err("direct_put session without target should fail closed");

    assert_eq!(error.code(), ErrorCode::InvalidRequest);
}

#[tokio::test]
async fn upload_content_rejects_invalid_upload_id_before_key_construction() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");

    let invalid_upload_id = ["upl", "123"].join("-");
    let error = UploadId::parse(&invalid_upload_id)
        .map_err(CoreError::InvalidUploadId)
        .expect_err("invalid upload_id should be rejected");

    assert_eq!(error.code(), ErrorCode::InvalidRequest);
    assert_eq!(
        store
            .list_prefix(&upload_session_prefix(namespace_id.as_str()))
            .await
            .expect("list upload sessions"),
        Vec::<String>::new()
    );
}
