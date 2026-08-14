//! Upload session begin, content, direct-put, and completion guards.

#![allow(clippy::panic)]
// These integration tests use panic in unexpected match arms for precise diagnostics.

use crate::common::commit_split_support::*;
use crate::common::namespace_engine;
use bytes::Bytes;
use loonfs_api::v0::{CompleteMultipartUploadRequest, UploadContentClaim};
use loonfs_api::{
    wire::control::{ControlObjectKind, UploadSessionState, UploadSessionTransport},
    ContentRef, DestinationBehavior, NamespaceId, UploadId,
};
use loonfs_api::{Checksum, ChecksumAlgorithm};
use loonfs_core::{
    BeginDirectPutUploadTargetResponse, Error as CoreError, ErrorCode, MutationContext,
};
use loonfs_objectstore::keys::{upload_session, upload_session_prefix};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::ObjectStore;
use loonfs_test_support::stores::{
    BlockingStore, FailStore, InjectedError, KeyPredicate, OperationClass,
};
use std::path::Path;
use std::sync::Arc;
use tempfile::tempdir;

async fn begin_upload<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) -> Result<loonfs_api::v0::BeginUploadResponse, CoreError> {
    namespace_engine(store, namespace_id, context)
        .begin_upload(loonfs_api::v0::BeginUploadRequest::ServiceProxied {})
        .await
}

async fn begin_direct_put_upload_target<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    claim: UploadContentClaim,
    context: &MutationContext,
) -> Result<BeginDirectPutUploadTargetResponse, CoreError> {
    namespace_engine(store, namespace_id, context)
        .begin_direct_put_upload_target(claim)
        .await
}

/// What a well-behaved direct-put client would send for these bytes.
fn direct_put_claim(bytes: &[u8]) -> UploadContentClaim {
    UploadContentClaim {
        size_bytes: bytes.len() as u64,
        checksum: Checksum::sha256(bytes),
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
    context: &MutationContext,
) -> Result<loonfs_api::v0::UploadSessionResponse, CoreError> {
    namespace_engine(store, namespace_id, context)
        .complete_upload(upload_id)
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
        content_ref.checksum,
        Checksum::sha256(bytes),
        "the signed checksum is the client's claim, verified again at completion"
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
    let claim = UploadContentClaim {
        size_bytes: 5,
        checksum: Checksum {
            algorithm: ChecksumAlgorithm::Sha256,
            value: "not-a-sha256".to_owned(),
        },
    };
    let error = begin_direct_put_upload_target(&store, &namespace_id, claim, &context)
        .await
        .expect_err("malformed direct_put claim");

    assert_eq!(error.code(), ErrorCode::InvalidRequest);
    assert_eq!(
        store
            .list_prefix(&upload_session_prefix(&namespace_id))
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
    assert_eq!(begin.namespace_id(), &namespace_id);
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
    let uploaded = upload_content(&store, &namespace_id, begin.upload_id(), b"hello", &context)
        .await
        .expect("upload content");

    store.reset();
    let completed = complete_upload(&store, &namespace_id, begin.upload_id(), &context)
        .await
        .expect("complete upload");
    assert_eq!(completed.content_ref(), Some(&uploaded.content_ref));
    assert_eq!(store.count(OperationClass::Read), 0);

    store.reset();
    let completed_again = complete_upload(&store, &namespace_id, begin.upload_id(), &context)
        .await
        .expect("complete upload idempotently");
    assert_eq!(completed_again, completed);
    assert_eq!(store.count(OperationClass::Read), 0);
}

/// A direct-put session that never recorded the reference its write was
/// signed against used to be a record this test could write and complete
/// against. The transport variant carries that reference, so there is no
/// such record to write any more — in Rust or on the wire. What is left of
/// this case lives where the durable bytes are read: see
/// `upload_sessions_reject_a_transport_missing_its_own_fields` in
/// loonfs-api's golden format tests.
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
            .list_prefix(&upload_session_prefix(&namespace_id))
            .await
            .expect("list upload sessions"),
        Vec::<String>::new()
    );
}

/// The streamed proxied upload: same session, same reference, same
/// idempotency, without the payload ever being held.
mod streamed_content {
    use super::*;
    use futures::StreamExt;
    use loonfs_objectstore::keys::content_blob;

    /// A payload larger than one transfer part, cut into stream chunks whose
    /// boundaries have nothing to do with the store's.
    fn payload() -> Vec<u8> {
        (0..3 * 1024 * 1024 + 17)
            .map(|offset| (offset % 251) as u8)
            .collect()
    }

    fn body(bytes: &[u8]) -> loonfs_objectstore::ByteStream {
        let chunks: Vec<Bytes> = bytes.chunks(9_973).map(Bytes::copy_from_slice).collect();
        futures::stream::iter(chunks.into_iter().map(Ok)).boxed()
    }

    async fn upload_streamed<S: ObjectStore + ?Sized>(
        store: &S,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
        bytes: &[u8],
        context: &MutationContext,
    ) -> Result<loonfs_api::v0::UploadContentResponse, CoreError> {
        namespace_engine(store, namespace_id, context)
            .upload_streamed_content(upload_id, body(bytes))
            .await
    }

    /// A streamed upload produces exactly the reference the buffered one
    /// would, because the same digest is computed over the same bytes — the
    /// only difference is that they were never all in memory at once.
    #[tokio::test]
    async fn a_streamed_upload_produces_the_reference_the_buffered_one_would() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let context = mutation_context();
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        bootstrap_namespace(&store, &namespace_id, &context, false)
            .await
            .expect("bootstrap");
        let bytes = payload();

        let begin = begin_upload(&store, &namespace_id, &context)
            .await
            .expect("begin upload");
        let staged = upload_streamed(&store, &namespace_id, begin.upload_id(), &bytes, &context)
            .await
            .expect("stream a multi-part payload into the session");

        assert_eq!(staged.content_ref.size_bytes, bytes.len() as u64);
        assert_eq!(
            staged.content_ref.checksum,
            Checksum::sha256(&bytes),
            "the server hashed the whole stream itself"
        );

        // Completion is unchanged: it trusts what the server already checked.
        let completed = complete_upload(&store, &namespace_id, begin.upload_id(), &context)
            .await
            .expect("complete a streamed upload");
        assert_eq!(completed.content_ref(), Some(&staged.content_ref));
    }

    /// Re-sending a body is the case that forces the shape: the digest only
    /// exists once the payload has been read, so the decision comes after
    /// the read — and the object the session already owns must survive it
    /// either way.
    #[tokio::test]
    async fn a_repeated_body_replays_and_a_different_one_conflicts_without_rewriting() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let context = mutation_context();
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        bootstrap_namespace(&store, &namespace_id, &context, false)
            .await
            .expect("bootstrap");
        let bytes = payload();

        let begin = begin_upload(&store, &namespace_id, &context)
            .await
            .expect("begin upload");
        let first = upload_streamed(&store, &namespace_id, begin.upload_id(), &bytes, &context)
            .await
            .expect("first streamed upload");
        let repeated = upload_streamed(&store, &namespace_id, begin.upload_id(), &bytes, &context)
            .await
            .expect("the same bytes again is the same upload");
        assert_eq!(first, repeated);

        let mut different = bytes.clone();
        different[0] ^= 0xff;
        let error = upload_streamed(
            &store,
            &namespace_id,
            begin.upload_id(),
            &different,
            &context,
        )
        .await
        .expect_err("different bytes into one session conflict");
        assert_eq!(error.code(), ErrorCode::UploadContentConflict);

        let catalog = loonfs_core::control::load_namespace_catalog_entry(&store, &namespace_id)
            .await
            .expect("catalog");
        let object_key = content_blob(catalog.content_store_id(), &first.content_ref.content_id);
        assert_eq!(
            store
                .get(&object_key, None)
                .await
                .expect("read staged object")
                .expect("staged object exists"),
            Bytes::from(bytes),
            "a refused upload must not have rewritten what the session staged"
        );
    }

    /// Two staging requests against one session, overlapped so the second
    /// starts while the first is still writing the object.
    ///
    /// This is the shape that used to corrupt the session. Both requests
    /// would find the key absent, both would assemble over it, and the one
    /// that lost the record compare-and-swap would leave its bytes behind
    /// under the winner's digest. The payloads here are the same length and
    /// different bytes, which is exactly the pair a size comparison cannot
    /// tell apart, so nothing downstream could have caught it.
    ///
    /// The staging claim is what settles it: exactly one request writes, the
    /// other is refused before it touches the object, and the reference the
    /// session recorded describes the object byte for byte.
    #[tokio::test]
    async fn one_session_admits_one_writer_and_records_what_that_writer_wrote() {
        let temp_dir = tempdir().expect("tempdir");
        let blocking = Arc::new(BlockingStore::new(
            LocalFsStore::new(temp_dir.path()).expect("store"),
            KeyPredicate::content_blob(),
            OperationClass::Put,
        ));
        let context = mutation_context();
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        bootstrap_namespace(blocking.as_ref(), &namespace_id, &context, false)
            .await
            .expect("bootstrap");
        let begin = begin_upload(blocking.as_ref(), &namespace_id, &context)
            .await
            .expect("begin upload");

        let first_bytes = payload();
        let mut second_bytes = first_bytes.clone();
        // Same length, different bytes.
        second_bytes[0] ^= 0xff;
        assert_eq!(first_bytes.len(), second_bytes.len());

        // Park only the first content write, so the second request runs to
        // whatever conclusion the protocol gives it rather than parking too.
        blocking.block_next();
        let first = tokio::spawn({
            let blocking = Arc::clone(&blocking);
            let namespace_id = namespace_id.clone();
            let upload_id = begin.upload_id().clone();
            let context = context.clone();
            let bytes = first_bytes.clone();
            async move {
                upload_streamed(
                    blocking.as_ref(),
                    &namespace_id,
                    &upload_id,
                    &bytes,
                    &context,
                )
                .await
            }
        });
        blocking.wait_until_blocked().await;

        // The second request arrives while the first still holds the claim.
        let second = upload_streamed(
            blocking.as_ref(),
            &namespace_id,
            begin.upload_id(),
            &second_bytes,
            &context,
        )
        .await;

        blocking.release();
        let first = first.await.expect("first staging task");

        // Whichever request the protocol lets through, the session must end
        // up holding a digest that describes the object. This is the
        // assertion the shared-key race breaks: without the claim both
        // requests write, and the session records one request's digest over
        // the other request's bytes.
        let staged = match (first, second) {
            (Ok(staged), Err(refused)) => {
                assert_eq!(refused.code(), ErrorCode::UploadContentConflict);
                staged
            }
            (Err(refused), Ok(staged)) => {
                assert_eq!(refused.code(), ErrorCode::UploadContentConflict);
                staged
            }
            (Ok(_), Ok(_)) => panic!("two requests staged into one session"),
            (Err(first), Err(second)) => {
                panic!("no request staged: {first:?} then {second:?}")
            }
        };

        let catalog =
            loonfs_core::control::load_namespace_catalog_entry(blocking.as_ref(), &namespace_id)
                .await
                .expect("catalog");
        let object_key = content_blob(catalog.content_store_id(), &staged.content_ref.content_id);
        let stored = blocking
            .get(&object_key, None)
            .await
            .expect("read staged object")
            .expect("staged object exists");
        assert_eq!(
            staged.content_ref,
            ContentRef::blob_v1(staged.content_ref.content_id.clone(), &stored),
            "the recorded reference must describe the object byte for byte"
        );
        // The claim admits the first writer, so those are the bytes that last.
        assert_eq!(stored, Bytes::from(first_bytes));
    }
}

/// A multipart session's whole life, on a store that reproduces the
/// providers' actual multipart behaviour.
mod direct_multipart {
    use super::*;
    use loonfs_api::v0::{CompletedUploadPart, DirectMultipartUploadOptions, UploadContentClaim};
    use loonfs_api::wire::control::{decode_control_object, UploadSessionLifecycle};
    use loonfs_core::{gc_namespace, GcConfig};
    use loonfs_objectstore::keys::content_blob;
    use loonfs_test_support::stores::{MultipartChecksumEnforcement, MultipartStore};
    use std::sync::Arc;

    const PART: &[u8] = b"a part's worth of bytes, repeated enough to be a part\n";

    /// One namespace with one open multipart session over three parts.
    struct Session {
        namespace_id: NamespaceId,
        upload_id: UploadId,
        /// What the client will claim at completion, and therefore what the
        /// server will build the reference from. The session itself knows
        /// none of it yet.
        claim: UploadContentClaim,
        object_key: String,
        provider_upload_id: String,
        payload: Vec<u8>,
    }

    async fn open_session<S: ObjectStore>(
        store: &MultipartStore<S>,
        context: &MutationContext,
    ) -> Session {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        bootstrap_namespace(store, &namespace_id, context, false)
            .await
            .expect("bootstrap");
        let payload = PART.repeat(3);
        let begin = namespace_engine(store, &namespace_id, context)
            .begin_direct_multipart_upload_target(DirectMultipartUploadOptions::default())
            .await
            .expect("begin direct multipart");
        let state = session_state(store, &namespace_id, &begin.upload_id).await;
        // A multipart session is opened knowing nothing about its payload,
        // which is why its transport carries no content reference at all.
        let UploadSessionTransport::DirectMultipart {
            provider_upload_id,
            part_size_bytes,
            checksum_algorithm,
        } = state.transport.clone()
        else {
            panic!("a multipart begin opens a multipart session");
        };
        assert_eq!(
            part_size_bytes.get(),
            begin.target.part_size_bytes,
            "the geometry it handed out is the geometry it recorded"
        );
        assert_eq!(checksum_algorithm, begin.target.checksum_algorithm);
        let catalog = loonfs_core::control::load_namespace_catalog_entry(store, &namespace_id)
            .await
            .expect("catalog");
        let object_key = content_blob(catalog.content_store_id(), &state.content_id);

        Session {
            namespace_id,
            upload_id: begin.upload_id,
            claim: UploadContentClaim {
                size_bytes: payload.len() as u64,
                checksum: Checksum::crc64nvme(&payload),
            },
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
        let key = upload_session(namespace_id, upload_id);
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
    fn upload_every_part<S: ObjectStore>(
        store: &MultipartStore<S>,
        session: &Session,
    ) -> Vec<CompletedUploadPart> {
        let mut parts = Vec::new();
        for (index, chunk) in session.payload.chunks(PART.len()).enumerate() {
            let part_number = index as u32 + 1;
            let etag = store.upload_part(&session.provider_upload_id, part_number, chunk);
            parts.push(CompletedUploadPart {
                part_number,
                etag,
                checksum: Checksum::crc64nvme(chunk),
            });
        }
        parts
    }

    fn complete_request(
        session: &Session,
        parts: Vec<CompletedUploadPart>,
    ) -> CompleteMultipartUploadRequest {
        CompleteMultipartUploadRequest {
            content: session.claim.clone(),
            parts,
        }
    }

    async fn complete_upload<S: ObjectStore + ?Sized>(
        store: &S,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
        request: &CompleteMultipartUploadRequest,
        context: &MutationContext,
    ) -> Result<loonfs_api::v0::UploadSessionResponse, CoreError> {
        namespace_engine(store, namespace_id, context)
            .complete_multipart_upload(upload_id, request)
            .await
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

        assert!(
            completed
                .content_ref()
                .expect("completed content ref")
                .content_id
                .as_str()
                .ends_with(session.object_key.rsplit('/').next().expect("key tail")),
            "the completion names the identity the session held all along"
        );
        assert_eq!(
            completed
                .content_ref()
                .expect("completed content ref")
                .size_bytes,
            session.payload.len() as u64
        );
        assert_eq!(
            completed
                .content_ref()
                .expect("completed content ref")
                .checksum,
            Checksum::crc64nvme(&session.payload),
            "a provider-assembled object's evidence is the crc it computed"
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

    #[tokio::test]
    async fn a_multipart_completion_rejects_an_empty_part_list() {
        let temp_dir = tempdir().expect("tempdir");
        let store = MultipartStore::new(LocalFsStore::new(temp_dir.path()).expect("store"));
        let context = mutation_context();
        let session = open_session(&store, &context).await;

        let error = complete_upload(
            &store,
            &session.namespace_id,
            &session.upload_id,
            &complete_request(&session, Vec::new()),
            &context,
        )
        .await
        .expect_err("a multipart completion needs at least one part");
        assert_eq!(error.code(), ErrorCode::InvalidRequest);
        assert!(error.to_string().contains("at least one uploaded part"));
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

        assert_eq!(replayed, first);
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
            checksum: Checksum::crc64nvme(short),
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
            checksum: Checksum::crc64nvme(PART),
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
    /// refuses the assembly outright. A refusal and a completion whose
    /// response was lost arrive as the same transport failure, so the first
    /// attempt reports the store failure and leaves the session open. The
    /// object ends the session instead: the retry finds the upload consumed
    /// and nothing at the key, and the completion contract already calls
    /// that terminal.
    #[tokio::test]
    async fn a_refused_assembly_is_a_store_failure_and_the_retry_is_terminal() {
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
            checksum: Checksum::crc64nvme(short),
        };
        let request = complete_request(&session, parts);

        let error = complete_upload(
            &store,
            &session.namespace_id,
            &session.upload_id,
            &request,
            &context,
        )
        .await
        .expect_err("a refused assembly cannot complete");
        assert_eq!(error.code(), ErrorCode::ServerError);
        assert!(
            matches!(
                session_state(&store, &session.namespace_id, &session.upload_id)
                    .await
                    .state,
                UploadSessionLifecycle::Open { .. }
            ),
            "a provider failure the server cannot read is not proof about content"
        );

        let error = complete_upload(
            &store,
            &session.namespace_id,
            &session.upload_id,
            &request,
            &context,
        )
        .await
        .expect_err("neither an upload nor an object is left to complete");
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

    /// A read-back the store refuses to answer says nothing about the object
    /// the provider assembled. The completion reports the store failure, and
    /// the object and the session survive it. The retry then completes from
    /// the object: the provider reports an upload it no longer knows, and
    /// the read-back settles the rest.
    #[tokio::test]
    async fn a_verification_the_store_refuses_leaves_the_completion_retryable() {
        let temp_dir = tempdir().expect("tempdir");
        let failing = Arc::new(FailStore::new(
            LocalFsStore::new(temp_dir.path()).expect("store"),
            KeyPredicate::content_blob(),
            OperationClass::Read,
            InjectedError::Transport("injected content read failure".to_owned()),
        ));
        let store = MultipartStore::new(Arc::clone(&failing));
        let context = mutation_context();
        let session = open_session(&store, &context).await;
        let parts = upload_every_part(&store, &session);
        let request = complete_request(&session, parts);

        failing.fail_next(1);
        let error = complete_upload(
            &store,
            &session.namespace_id,
            &session.upload_id,
            &request,
            &context,
        )
        .await
        .expect_err("a verification that cannot run does not complete the upload");
        assert_eq!(error.code(), ErrorCode::ServerError);
        assert_eq!(failing.attempts(), 1, "the read-back is what failed");
        assert!(
            matches!(
                session_state(&store, &session.namespace_id, &session.upload_id)
                    .await
                    .state,
                UploadSessionLifecycle::Open { .. }
            ),
            "a store failure must not end the session"
        );
        assert_eq!(
            store
                .get(&session.object_key, None)
                .await
                .expect("read object")
                .expect("the assembled object survives a failed read-back"),
            Bytes::from(session.payload.clone())
        );

        let completed = complete_upload(
            &store,
            &session.namespace_id,
            &session.upload_id,
            &request,
            &context,
        )
        .await
        .expect("the retried completion verifies the assembled object");
        assert_eq!(
            completed
                .content_ref()
                .expect("completed content ref")
                .checksum,
            Checksum::crc64nvme(&session.payload)
        );
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
        let UploadSessionLifecycle::Open { expires_at_ms, .. } =
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

    /// The geometry is the one thing a begin request may choose, and it is
    /// bounded by what every supported provider accepts for a non-final
    /// part. The size it settles on is what bounds the object, too.
    #[tokio::test]
    async fn a_multipart_session_takes_a_part_size_inside_the_providers_bounds() {
        let temp_dir = tempdir().expect("tempdir");
        let store = MultipartStore::new(LocalFsStore::new(temp_dir.path()).expect("store"));
        let context = mutation_context();
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        bootstrap_namespace(&store, &namespace_id, &context, false)
            .await
            .expect("bootstrap");

        let chosen = namespace_engine(&store, &namespace_id, &context)
            .begin_direct_multipart_upload_target(DirectMultipartUploadOptions {
                part_size_bytes: Some(16 * 1024 * 1024),
            })
            .await
            .expect("a part size inside the bounds is honoured");
        assert_eq!(chosen.target.part_size_bytes, 16 * 1024 * 1024);

        for out_of_bounds in [5 * 1024 * 1024 - 1, 5 * 1024 * 1024 * 1024 + 1] {
            let error = namespace_engine(&store, &namespace_id, &context)
                .begin_direct_multipart_upload_target(DirectMultipartUploadOptions {
                    part_size_bytes: Some(out_of_bounds),
                })
                .await
                .expect_err("a part size no provider accepts is refused");
            assert_eq!(error.code(), ErrorCode::InvalidRequest);
        }
    }
}
