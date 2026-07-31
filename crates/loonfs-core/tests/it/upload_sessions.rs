//! Upload session begin, content, direct-put, and completion guards.

#![allow(clippy::panic)]
// These integration tests use panic in unexpected match arms for precise diagnostics.

use crate::common::commit_split_support::*;
use crate::common::namespace_engine;
use bytes::Bytes;
use loonfs_api::v0::DirectPutContentClaim;
use loonfs_api::{
    v0::{CompleteUploadRequest, UploadMode},
    wire::control::{
        encode_control_object, ControlObjectKind, UploadSessionEnvelope, UploadSessionState,
    },
    ContentRef, DestinationBehavior, NamespaceId, UploadId,
};
use loonfs_api::{ContentId, StorageChecksum};
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
    claim: DirectPutContentClaim,
    context: &MutationContext,
) -> Result<BeginDirectPutUploadTargetResponse, CoreError> {
    namespace_engine(store, namespace_id, context)
        .begin_direct_put_upload_target(claim)
        .await
}

/// What a well-behaved direct-put client would send for these bytes.
fn direct_put_claim(bytes: &[u8]) -> DirectPutContentClaim {
    DirectPutContentClaim {
        size_bytes: bytes.len() as u64,
        sha256: StorageChecksum::sha256(bytes).value,
    }
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

/// The server, not the client, names the object a direct upload writes,
/// and it names it before any byte moves.
#[tokio::test]
async fn begin_direct_put_mints_the_target_object_up_front() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    let bytes = b"direct put payload";

    let first =
        begin_direct_put_upload_target(&store, &namespace_id, direct_put_claim(bytes), &context)
            .await
            .expect("first direct put target");
    let second =
        begin_direct_put_upload_target(&store, &namespace_id, direct_put_claim(bytes), &context)
            .await
            .expect("second direct put target");

    let content_ref = &first.target.content_ref;
    assert_eq!(content_ref.size_bytes, bytes.len() as u64);
    assert_eq!(
        content_ref.storage_checksum,
        StorageChecksum::sha256(bytes),
        "the signed checksum is the client's claim, verified again at completion"
    );
    assert_eq!(
        content_ref.whole_file_sha256.as_deref(),
        Some(content_ref.storage_checksum.value.as_str())
    );
    assert!(first
        .target
        .object_key
        .ends_with(content_ref.content_id.as_str()));
    // Same bytes, two sessions, two objects: nothing is shared, so neither
    // upload can observe the other.
    assert_ne!(
        first.target.content_ref.content_id,
        second.target.content_ref.content_id
    );
    assert_ne!(first.target.object_key, second.target.object_key);
}

#[tokio::test]
async fn begin_direct_put_rejects_a_malformed_claim_without_creating_a_session() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");

    // The client no longer names an object, so the only thing left to
    // reject is a malformed claim about its own bytes.
    let claim = DirectPutContentClaim {
        size_bytes: 5,
        sha256: "not-a-sha256".to_owned(),
    };
    let error = begin_direct_put_upload_target(&store, &namespace_id, claim, &context)
        .await
        .expect_err("malformed direct_put claim");

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
            multipart_parts: None,
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
            multipart_parts: None,
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
    let wrong_ref = ContentRef::blob_v1(ContentId::generate(), b"different");
    assert_ne!(wrong_ref, mismatch_uploaded.content_ref);

    store.reset();
    let mismatch = complete_upload(
        &store,
        &namespace_id,
        &mismatch_begin.upload_id,
        &CompleteUploadRequest {
            content_ref: wrong_ref,
            multipart_parts: None,
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
        content_id: stored.content_ref.content_id.clone(),
        claimed_checksum: None,
        direct_put_content_ref: None,
        staged_content_ref: None,
        created_at_ms: context.now_ms,
        state: loonfs_api::wire::control::UploadSessionLifecycle::Open {
            expires_at_ms: context.now_ms + 60_000,
        },
        provider_multipart_upload_id: None,
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
            multipart_parts: None,
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

/// A multipart session's whole life, on a store that reproduces the
/// providers' actual multipart behaviour.
mod direct_multipart {
    use super::*;
    use loonfs_api::v0::{CompletedUploadPart, DirectMultipartContentClaim};
    use loonfs_api::wire::control::{decode_control_object, UploadSessionLifecycle};
    use loonfs_core::{gc_namespace, GcConfig};
    use loonfs_objectstore::keys::content_blob;
    use loonfs_test_support::stores::{MultipartChecksumEnforcement, MultipartStore};

    const PART: &[u8] = b"a part's worth of bytes, repeated enough to be a part\n";

    /// One namespace with one open multipart session over three parts.
    struct Session {
        namespace_id: NamespaceId,
        upload_id: UploadId,
        content_ref: ContentRef,
        object_key: String,
        provider_upload_id: String,
        payload: Vec<u8>,
    }

    async fn open_session(
        store: &MultipartStore<LocalFsStore>,
        context: &MutationContext,
    ) -> Session {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        bootstrap_namespace(store, &namespace_id, context, false)
            .await
            .expect("bootstrap");
        let payload = PART.repeat(3);
        let begin = namespace_engine(store, &namespace_id, context)
            .begin_direct_multipart_upload_target(DirectMultipartContentClaim {
                size_bytes: payload.len() as u64,
                crc64nvme: StorageChecksum::crc64nvme(&payload).value,
            })
            .await
            .expect("begin direct multipart");
        let catalog = loonfs_core::control::load_namespace_catalog_entry(store, &namespace_id)
            .await
            .expect("catalog");
        let object_key = content_blob(
            catalog.content_store_id().as_str(),
            &begin.target.content_ref.content_id,
        );
        let provider_upload_id = session_state(store, &namespace_id, &begin.upload_id)
            .await
            .provider_multipart_upload_id
            .expect("a multipart session records its provider upload");

        Session {
            namespace_id,
            upload_id: begin.upload_id,
            content_ref: begin.target.content_ref,
            object_key,
            provider_upload_id,
            payload,
        }
    }

    async fn session_state<S: ObjectStore + ?Sized>(
        store: &S,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
    ) -> UploadSessionState {
        let key = upload_session(namespace_id.as_str(), upload_id.as_str());
        let bytes = store
            .get(&key, None)
            .await
            .expect("read session")
            .expect("session exists");
        decode_control_object::<UploadSessionState>(&bytes, ControlObjectKind::UploadSession)
            .expect("decode session")
            .state
    }

    /// Uploads every part, the way a client would after asking for the
    /// signed URLs, and returns the bookkeeping it carries to completion.
    fn upload_every_part(
        store: &MultipartStore<LocalFsStore>,
        session: &Session,
    ) -> Vec<CompletedUploadPart> {
        let mut parts = Vec::new();
        for (index, chunk) in session.payload.chunks(PART.len()).enumerate() {
            let part_number = index as u32 + 1;
            let etag = store.upload_part(&session.provider_upload_id, part_number, chunk);
            parts.push(CompletedUploadPart {
                part_number,
                etag,
                crc64nvme: StorageChecksum::crc64nvme(chunk).value,
            });
        }
        parts
    }

    fn complete_request(
        session: &Session,
        parts: Vec<CompletedUploadPart>,
    ) -> CompleteUploadRequest {
        CompleteUploadRequest {
            content_ref: session.content_ref.clone(),
            multipart_parts: Some(parts),
        }
    }

    /// The whole point of the transport: parts go straight to the provider,
    /// the server assembles them, and it believes the assembly only after
    /// reading the object's own checksum back.
    #[tokio::test]
    async fn a_multipart_upload_completes_against_the_assembled_object() {
        let temp_dir = tempdir().expect("tempdir");
        let store = MultipartStore::new(LocalFsStore::new(temp_dir.path()).expect("store"));
        let context = mutation_context();
        let session = open_session(&store, &context).await;

        let parts = upload_every_part(&store, &session);
        assert_eq!(parts.len(), 3, "the payload cut into more than one part");

        let completed = complete_upload(
            &store,
            &session.namespace_id,
            &session.upload_id,
            &complete_request(&session, parts),
            &context,
        )
        .await
        .expect("complete a verified multipart upload");

        assert_eq!(completed.content_ref, session.content_ref);
        assert_eq!(
            completed.content_ref.storage_checksum,
            StorageChecksum::crc64nvme(&session.payload),
            "a provider-assembled object's evidence is the crc it computed"
        );
        assert!(
            completed.content_ref.whole_file_sha256.is_none(),
            "nobody trustworthy hashed these bytes, so no sha256 is claimed"
        );
        assert_eq!(
            store
                .get(&session.object_key, None)
                .await
                .expect("read object")
                .expect("object exists"),
            Bytes::from(session.payload.clone())
        );
        assert_eq!(store.open_uploads(), 0, "completion consumes the upload");
    }

    /// Re-uploading a part is how a client retries one. The last write wins
    /// and the assembled object follows it.
    #[tokio::test]
    async fn a_re_uploaded_part_is_the_one_that_lands() {
        let temp_dir = tempdir().expect("tempdir");
        let store = MultipartStore::new(LocalFsStore::new(temp_dir.path()).expect("store"));
        let context = mutation_context();
        let session = open_session(&store, &context).await;

        // Part two arrives wrong first, then correct: same length both
        // times, so only the checksum can tell them apart.
        let wrong = vec![b'x'; PART.len()];
        store.upload_part(&session.provider_upload_id, 2, &wrong);
        let parts = upload_every_part(&store, &session);

        complete_upload(
            &store,
            &session.namespace_id,
            &session.upload_id,
            &complete_request(&session, parts),
            &context,
        )
        .await
        .expect("the corrected part is the one completion assembles");

        assert_eq!(
            store
                .get(&session.object_key, None)
                .await
                .expect("read object")
                .expect("object exists"),
            Bytes::from(session.payload.clone())
        );
    }

    /// A completion whose response was lost. The provider has consumed the
    /// upload, so replaying it reports an upload nobody has heard of — but
    /// the durable session says what was promised and the object says what
    /// landed, and those two settle it on every provider identically.
    #[tokio::test]
    async fn a_lost_completion_reconciles_from_the_session_and_the_object() {
        let temp_dir = tempdir().expect("tempdir");
        let store = MultipartStore::new(LocalFsStore::new(temp_dir.path()).expect("store"));
        let context = mutation_context();
        let session = open_session(&store, &context).await;
        let parts = upload_every_part(&store, &session);
        let request = complete_request(&session, parts);

        let first = complete_upload(
            &store,
            &session.namespace_id,
            &session.upload_id,
            &request,
            &context,
        )
        .await
        .expect("first completion");

        // The client never saw that answer and asks again.
        let replayed = complete_upload(
            &store,
            &session.namespace_id,
            &session.upload_id,
            &request,
            &context,
        )
        .await
        .expect("a lost completion is answered, not failed");

        assert_eq!(replayed.content_ref, first.content_ref);
        assert_eq!(
            store
                .get(&session.object_key, None)
                .await
                .expect("read object")
                .expect("object survived the replay"),
            Bytes::from(session.payload.clone())
        );
    }

    /// An assembly that is not what was promised. There are no parts left to
    /// retry against — completion consumed them — so the session ends rather
    /// than waiting for a retry that could never work, and the wrong object
    /// goes with it.
    #[tokio::test]
    async fn a_completion_that_does_not_verify_ends_the_session_and_deletes_the_object() {
        let temp_dir = tempdir().expect("tempdir");
        let store = MultipartStore::new(LocalFsStore::new(temp_dir.path()).expect("store"));
        let context = mutation_context();
        let session = open_session(&store, &context).await;

        // Every part is checksum-correct, but the last one is not the byte
        // count the session was opened for, so the assembly cannot be the
        // object that was promised. This is the shape a provider that only
        // witnesses the whole-object checksum lets through.
        let mut parts = upload_every_part(&store, &session);
        let short = &PART[..PART.len() / 2];
        let etag = store.upload_part(&session.provider_upload_id, 3, short);
        parts[2] = CompletedUploadPart {
            part_number: 3,
            etag,
            crc64nvme: StorageChecksum::crc64nvme(short).value,
        };

        let error = complete_upload(
            &store,
            &session.namespace_id,
            &session.upload_id,
            &complete_request(&session, parts),
            &context,
        )
        .await
        .expect_err("an unverified assembly cannot complete");
        assert_eq!(error.code(), ErrorCode::InvalidRequest);

        assert!(
            matches!(
                session_state(&store, &session.namespace_id, &session.upload_id)
                    .await
                    .state,
                UploadSessionLifecycle::Aborted { .. }
            ),
            "a failed verification is terminal, not retryable"
        );
        assert!(
            store
                .head(&session.object_key)
                .await
                .expect("head")
                .is_none(),
            "the wrong object is deleted, not left publishable"
        );

        // And the terminal state holds: a second completion reports absence.
        let parts = vec![CompletedUploadPart {
            part_number: 1,
            etag: "\"whatever\"".to_owned(),
            crc64nvme: StorageChecksum::crc64nvme(PART).value,
        }];
        let error = complete_upload(
            &store,
            &session.namespace_id,
            &session.upload_id,
            &complete_request(&session, parts),
            &context,
        )
        .await
        .expect_err("an aborted session cannot complete");
        assert_eq!(error.code(), ErrorCode::UploadNotFound);
    }

    /// A provider that treats the whole-object checksum as a precondition
    /// refuses the assembly outright. The session still ends terminally:
    /// the upload is spent either way.
    #[tokio::test]
    async fn a_refused_assembly_is_terminal_too() {
        let temp_dir = tempdir().expect("tempdir");
        let store = MultipartStore::with_enforcement(
            LocalFsStore::new(temp_dir.path()).expect("store"),
            MultipartChecksumEnforcement::Precondition,
        );
        let context = mutation_context();
        let session = open_session(&store, &context).await;

        let mut parts = upload_every_part(&store, &session);
        let short = &PART[..PART.len() / 2];
        let etag = store.upload_part(&session.provider_upload_id, 3, short);
        parts[2] = CompletedUploadPart {
            part_number: 3,
            etag,
            crc64nvme: StorageChecksum::crc64nvme(short).value,
        };

        let error = complete_upload(
            &store,
            &session.namespace_id,
            &session.upload_id,
            &complete_request(&session, parts),
            &context,
        )
        .await
        .expect_err("a refused assembly cannot complete");
        assert_eq!(error.code(), ErrorCode::InvalidRequest);
        assert!(matches!(
            session_state(&store, &session.namespace_id, &session.upload_id)
                .await
                .state,
            UploadSessionLifecycle::Aborted { .. }
        ));
        assert!(store
            .head(&session.object_key)
            .await
            .expect("head")
            .is_none());
    }

    /// A session abandoned mid-upload leaves parts sitting at the provider.
    /// Upload collection abandons them along with the object, because the
    /// session record is where the provider's upload id lives.
    #[tokio::test]
    async fn upload_collection_abandons_the_provider_upload_of_an_expired_session() {
        let temp_dir = tempdir().expect("tempdir");
        let store = MultipartStore::new(LocalFsStore::new(temp_dir.path()).expect("store"));
        let context = mutation_context();
        let session = open_session(&store, &context).await;
        upload_every_part(&store, &session);
        assert_eq!(store.open_uploads(), 1);

        // The session carries its own expiry, so collection is decided
        // against that stamp and never against an object's provider
        // timestamp.
        let UploadSessionLifecycle::Open { expires_at_ms } =
            session_state(&store, &session.namespace_id, &session.upload_id)
                .await
                .state
        else {
            panic!("a fresh session is open");
        };
        let config = GcConfig::default();
        let expired = MutationContext {
            writer_id: context.writer_id.clone(),
            now_ms: expires_at_ms + config.grace_window_ms + 1,
        };
        gc_namespace(&store, &session.namespace_id, &config, &expired)
            .await
            .expect("garbage collection");

        assert!(matches!(
            session_state(&store, &session.namespace_id, &session.upload_id)
                .await
                .state,
            UploadSessionLifecycle::Aborted { .. }
        ));
        assert_eq!(
            store.open_uploads(),
            0,
            "the parts an abandoned session accumulated are released"
        );
        assert!(store.aborts() >= 1);
        assert!(store
            .head(&session.object_key)
            .await
            .expect("head")
            .is_none());
    }
}
