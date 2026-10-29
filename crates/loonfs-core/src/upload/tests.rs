//! Upload session lifecycle tests.

use super::abort::*;
use super::complete::*;
use super::complete::{claimed_content_ref, multipart_parts};
use super::direct::{multipart_session_upload, validate_upload_checksum};
use super::session::{create_upload_session, NewUploadSession};
use crate::error::{CoreError, Result};
use crate::limits::{COMPLETED_UPLOAD_RECEIPT_WINDOW_MS, UPLOAD_SESSION_LEASE_MS};
use crate::test_support::WriterAt;
use bytes::Bytes;
use loonfs_objectstore::keys::{
    content_blob, upload_outcome, upload_session, upload_session_prefix,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::{ObjectStore, PutMode};
use loonfs_test_support::stores::{
    BlockingStore, FailStore, InjectedError, KeyPredicate, OperationSelector, RecordedOperation,
    RecordingStore,
};
use loonfs_types::format::control::{UploadOutcome, UploadSessionMode, UploadSessionPayload};
use loonfs_types::{
    Checksum, ChecksumAlgorithm, CompleteUploadBody, CompletedUploadPart, ContentId, ContentRef,
    NamespaceId, UploadContentClaim, UploadId, UploadMode, UploadPartChecksumClaim, UploadSession,
    UploadSessionStatus,
};
use std::assert_matches;
use std::num::NonZeroU64;
use tempfile::tempdir;

const BYTES: &[u8] = b"terminal states\n";

#[tokio::test]
async fn a_failed_final_session_read_publishes_no_content() {
    use futures::StreamExt;
    use std::sync::Arc;

    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("failed-session-check").expect("namespace id");
    let context = context(1_000);
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .create()
        .await
        .expect("bootstrap");
    let session = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .engine()
        .begin_upload()
        .await
        .expect("begin upload");
    let loaded = crate::test_support::namespace_store(&store, &namespace_id)
        .load_upload_session_state(&session.upload_id)
        .await
        .expect("load session");
    let session_key = upload_session(&namespace_id, &session.upload_id);
    let failing = Arc::new(FailStore::new(
        store,
        KeyPredicate::exact(&session_key),
        OperationSelector::Read,
        InjectedError::Transport("session read failed".to_owned()),
    ));
    let recording = Arc::new(RecordingStore::new(
        Arc::clone(&failing),
        KeyPredicate::prefix(upload_session_prefix(&namespace_id)),
    ));
    let body = {
        let failing = Arc::clone(&failing);
        let recording = Arc::clone(&recording);
        futures::stream::once(async move {
            recording.reset();
            failing.fail_next(1);
            Ok(Bytes::from_static(BYTES))
        })
        .boxed()
    };

    let error = crate::test_support::NamespaceFixture::new(
        &recording,
        &namespace_id,
        &crate::test_support::writer_at("test-writer", 1_000),
    )
    .engine()
    .upload_content_stream(&session.upload_id, body)
    .await
    .expect_err("the final session read must succeed");

    assert_matches!(error, CoreError::ControlObjectLoad(_));
    assert_eq!(recording.count(OperationSelector::Read), 2);
    assert_eq!(recording.counts().puts, 0);
    assert!(recording
        .get(
            &content_blob(&namespace_id, &loaded.session.content_id),
            None
        )
        .await
        .expect("get content")
        .is_none());
    let loaded = crate::test_support::namespace_store(&recording, &namespace_id)
        .load_upload_session_state(&session.upload_id)
        .await
        .expect("load the open session");
    assert_eq!(loaded.outcome, None);
}

#[tokio::test(start_paused = true)]
async fn a_retried_upload_of_the_same_bytes_succeeds_and_other_bytes_conflict() {
    let temp_dir = tempdir().expect("tempdir");
    let store = FailStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::content_blob(),
        OperationSelector::Put,
        InjectedError::Transport("content write timed out".to_owned()),
    )
    .apply_then_fail();
    let namespace_id = NamespaceId::parse("retried-buffered-write").expect("namespace id");
    let context = context(1_000);
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .create()
        .await
        .expect("bootstrap");
    let session = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .engine()
        .begin_upload()
        .await
        .expect("begin upload");

    // The write is applied, but its response is lost.
    store.fail_next(1);
    crate::test_support::NamespaceFixture::new(
        &store,
        &namespace_id,
        &crate::test_support::writer_at("test-writer", 1_000),
    )
    .engine()
    .upload_content(&session.upload_id, BYTES)
    .await
    .expect_err("the content write fails");
    let retry = crate::test_support::NamespaceFixture::new(
        &store,
        &namespace_id,
        &crate::test_support::writer_at("test-writer", 1_000),
    )
    .engine()
    .upload_content(&session.upload_id, BYTES)
    .await
    .expect("a retry of the same bytes finds its own object");
    let other = crate::test_support::NamespaceFixture::new(
        &store,
        &namespace_id,
        &crate::test_support::writer_at("test-writer", 1_000),
    )
    .engine()
    .upload_content(&session.upload_id, b"other bytes\n")
    .await
    .expect_err("other bytes conflict with the stored object");

    assert_eq!(
        retry.content_ref(),
        Some(&ContentRef::blob_v1(
            namespace_id.clone(),
            crate::test_support::namespace_store(&store, &namespace_id)
                .load_upload_session_state(&session.upload_id)
                .await
                .expect("load session")
                .session
                .content_id,
            BYTES,
            store.checksum_algorithm(),
        ))
    );
    assert_matches!(other, CoreError::UploadContentConflict { .. });
    let completed = complete(&store, &namespace_id, &session.upload_id, &context)
        .await
        .expect("complete the upload");
    assert_eq!(completed.response.content_ref(), retry.content_ref());
}

#[tokio::test]
async fn a_buffered_upload_aborted_during_its_write_is_refused_and_records_nothing() {
    let temp_dir = tempdir().expect("tempdir");
    let store = BlockingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::content_blob(),
        OperationSelector::Put,
    );
    let namespace_id = NamespaceId::parse("aborted-buffered-write").expect("namespace id");
    let setup = context(1_000);
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap");
    let session = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .engine()
        .begin_upload()
        .await
        .expect("begin upload");
    store.block_next();
    let engine = crate::test_support::NamespaceFixture::new(
        &store,
        &namespace_id,
        &crate::test_support::writer_at("test-writer", 1_000),
    )
    .engine();
    let (result, ()) = tokio::join!(engine.upload_content(&session.upload_id, BYTES), async {
        store.wait_until_blocked().await;
        abort(store.inner(), &namespace_id, &session.upload_id, &setup)
            .await
            .expect("abort during the write");
        store.release();
    });

    assert_matches!(result, Err(CoreError::UploadNotFound { .. }));
    let loaded = crate::test_support::namespace_store(&store, &namespace_id)
        .load_upload_session_state(&session.upload_id)
        .await
        .expect("load session");
    assert_matches!(loaded.outcome, Some(UploadOutcome::Aborted { .. }));
}

fn context(now_ms: u64) -> WriterAt {
    crate::test_support::writer_at("upload-test", now_ms)
}

/// One store with a namespace and one open, staged session in it.
async fn staged_session(
    store: &LocalFsStore,
    context: &WriterAt,
) -> (NamespaceId, UploadId, ContentRef, String) {
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    crate::test_support::NamespaceFixture::new(store, &namespace_id, context)
        .create()
        .await
        .expect("bootstrap");
    let begin = crate::test_support::NamespaceFixture::new(store, &namespace_id, context)
        .engine()
        .begin_upload()
        .await
        .expect("begin upload");
    let staged = crate::test_support::NamespaceFixture::new(
        store,
        &namespace_id,
        &crate::test_support::writer_at("test-writer", context.now_ms),
    )
    .engine()
    .upload_content(&begin.upload_id, BYTES)
    .await
    .expect("stage upload");
    let content_key = content_blob(
        &staged
            .content_ref()
            .expect("staged content")
            .owner_namespace_id,
        &staged.content_ref().expect("staged content").content_id,
    );
    (
        namespace_id,
        begin.upload_id.clone(),
        staged.content_ref().expect("staged content").clone(),
        content_key,
    )
}

async fn complete<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    upload_id: &UploadId,
    context: &WriterAt,
) -> Result<CompletedUpload> {
    crate::test_support::NamespaceFixture::new(store, namespace_id, context)
        .engine()
        .complete_upload(
            &crate::test_support::namespace_store(store, namespace_id)
                .load_catalog_entry()
                .await
                .expect("load the catalog"),
            upload_id,
            CompleteUploadBody::ServiceProxied {},
        )
        .await
}

async fn abort<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    upload_id: &UploadId,
    context: &WriterAt,
) -> Result<UploadSession> {
    let catalog = crate::test_support::namespace_store(store, namespace_id)
        .load_catalog_entry()
        .await?;
    crate::test_support::NamespaceFixture::new(store, catalog.namespace_id(), context)
        .engine()
        .abort_upload(upload_id)
        .await
}

#[tokio::test]
async fn an_applied_outcome_put_that_fails_its_precondition_is_this_requests_own() {
    let temp_dir = tempdir().expect("tempdir");
    let inner = LocalFsStore::new(temp_dir.path()).expect("store");
    let (namespace_id, upload_id, content_ref, _) = staged_session(&inner, &context(1_000)).await;
    // The provider client resends the put that created the outcome
    // record, and the resent put fails its precondition because of the
    // first one.
    let store = FailStore::new(
        inner,
        KeyPredicate::exact(upload_outcome(&namespace_id, &upload_id)),
        OperationSelector::PutCreateIfAbsent,
        InjectedError::PreconditionFailed,
    )
    .apply_then_fail();
    store.fail_next(1);

    let completed = complete(&store, &namespace_id, &upload_id, &context(2_000))
        .await
        .expect("the outcome under the session is this request's own");

    assert_eq!(completed.response.content_ref(), Some(&content_ref));
    let state = crate::test_support::namespace_store(&store, &namespace_id)
        .load_upload_session_state(&upload_id)
        .await
        .expect("session");
    assert_eq!(
        state.outcome,
        Some(UploadOutcome::Completed {
            completed_at_ms: 2_000,
            content_ref,
        })
    );
}

#[tokio::test]
async fn a_completion_whose_outcome_acknowledgement_is_lost_reads_its_record_back() {
    let temp_dir = tempdir().expect("tempdir");
    let inner = LocalFsStore::new(temp_dir.path()).expect("store");
    let (namespace_id, upload_id, content_ref, _) = staged_session(&inner, &context(1_000)).await;
    let key = upload_outcome(&namespace_id, &upload_id);
    let failing = FailStore::new(
        inner,
        KeyPredicate::exact(&key),
        OperationSelector::PutCreateIfAbsent,
        InjectedError::Transport("lost completion acknowledgement".to_owned()),
    )
    .apply_then_fail();
    failing.fail_next(1);
    let store = RecordingStore::new(failing, KeyPredicate::exact(&key));

    let first = complete(&store, &namespace_id, &upload_id, &context(2_000))
        .await
        .expect("the read-back finds this completion's own record");
    assert_eq!(store.counts().create_if_absent_puts, 1);
    let stored = store
        .get_with_metadata(&key)
        .await
        .expect("read outcome")
        .expect("outcome");

    store.reset();
    let replay = complete(&store, &namespace_id, &upload_id, &context(3_000))
        .await
        .expect("replay completion");
    assert_eq!(replay.response, first.response);
    assert_eq!(replay.response.content_ref(), Some(&content_ref));
    assert_matches!(
        replay.response.status,
        UploadSessionStatus::Completed {
            completed_at_ms: 2_000,
            ..
        }
    );
    assert_eq!(store.counts().puts, 0);
    assert_eq!(store.counts().deletes, 0);
    assert_eq!(
        store
            .get_with_metadata(&key)
            .await
            .expect("read replayed outcome")
            .expect("outcome"),
        stored
    );
}

/// Returns how many times each key under the uploads prefix was written.
fn writes_per_key(store: &RecordingStore<LocalFsStore>) -> Vec<(String, usize)> {
    let mut writes = std::collections::BTreeMap::new();
    for operation in store.snapshot() {
        if matches!(operation, RecordedOperation::Put { .. }) {
            *writes.entry(operation.key().to_owned()).or_insert(0) += 1;
        }
    }
    writes.into_iter().collect()
}

#[tokio::test]
async fn every_upload_writes_its_session_record_and_its_outcome_record_once() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    crate::test_support::NamespaceFixture::new(
        &LocalFsStore::new(temp_dir.path()).expect("store"),
        &namespace_id,
        &setup,
    )
    .create()
    .await
    .expect("bootstrap");
    let store = RecordingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::prefix(upload_session_prefix(&namespace_id)),
    );
    let catalog = crate::test_support::namespace_store(&store, &namespace_id)
        .load_catalog_entry()
        .await
        .expect("catalog");

    let proxied = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .engine()
        .begin_upload()
        .await
        .expect("begin proxied upload");
    crate::test_support::NamespaceFixture::new(
        &store,
        &namespace_id,
        &crate::test_support::writer_at("test-writer", 1_000),
    )
    .engine()
    .upload_content(&proxied.upload_id, BYTES)
    .await
    .expect("upload proxied bytes");
    complete(&store, &namespace_id, &proxied.upload_id, &setup)
        .await
        .expect("complete proxied upload");

    let direct = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .engine()
        .begin_direct_put_upload_target()
        .await
        .expect("begin direct put");
    store
        .put(
            &direct.object_key,
            Bytes::from_static(BYTES),
            PutMode::CreateIfAbsent,
        )
        .await
        .expect("write direct-put bytes");
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .engine()
        .complete_upload(
            &crate::test_support::namespace_store(&store, &namespace_id)
                .load_catalog_entry()
                .await
                .expect("load the catalog"),
            &direct.session.upload_id,
            CompleteUploadBody::DirectPut {
                content: UploadContentClaim {
                    size_bytes: BYTES.len() as u64,
                    checksum: Checksum::crc64nvme(BYTES),
                },
            },
        )
        .await
        .expect("complete direct put");

    let prepared =
        crate::test_support::NamespaceFixture::new(&store, catalog.namespace_id(), &setup)
            .engine()
            .stage_owned_bytes(&catalog, None, BYTES)
            .await
            .expect("stage owned bytes");
    let owned = prepared.upload_id().expect("owned staging session").clone();

    let mut expected = Vec::new();
    for upload_id in [&proxied.upload_id, &direct.session.upload_id, &owned] {
        expected.push((upload_session(&namespace_id, upload_id), 1));
        expected.push((upload_outcome(&namespace_id, upload_id), 1));
    }
    expected.sort();
    assert_eq!(writes_per_key(&store), expected);
    assert_eq!(store.counts().compare_and_swaps, 0);
}

#[test]
fn multipart_validation_uses_the_algorithm_frozen_in_the_session() {
    let session = UploadSessionPayload {
        namespace_id: NamespaceId::parse("demo").expect("namespace id"),
        upload_id: UploadId::parse("upl_00000000000000000000000001").expect("upload id"),
        content_id: ContentId::parse("con_00000000000000000000000001").expect("content id"),
        subject_id: None,
        mode: UploadSessionMode::DirectMultipart {
            provider_upload_id: "provider-upload".to_owned(),
            part_size_bytes: NonZeroU64::new(8 * 1024 * 1024).expect("part size"),
            // Deliberately not the process default. This models a
            // session reopened after that default changed.
            checksum_algorithm: ChecksumAlgorithm::Crc32c,
        },
        expires_at_ms: 2,
    };
    let (_, required_algorithm) = multipart_session_upload(&session).expect("multipart session");
    assert_eq!(required_algorithm, ChecksumAlgorithm::Crc32c);

    let content = UploadContentClaim {
        size_bytes: 7,
        checksum: Checksum::crc32c(b"payload"),
    };
    let content_ref = claimed_content_ref(
        session.namespace_id.clone(),
        session.content_id.clone(),
        &content,
        required_algorithm,
    )
    .expect("the stored algorithm accepts the content claim");
    assert_eq!(content_ref.checksum.algorithm(), ChecksumAlgorithm::Crc32c);

    let part = CompletedUploadPart {
        part_number: 1,
        etag: "etag".to_owned(),
        checksum: Checksum::crc32c(b"payload"),
    };
    assert_eq!(
        multipart_parts(&[part], required_algorithm)
            .expect("the stored algorithm accepts the part")[0]
            .checksum
            .algorithm(),
        ChecksumAlgorithm::Crc32c
    );

    let wrong_part = CompletedUploadPart {
        part_number: 1,
        etag: "etag".to_owned(),
        checksum: Checksum::crc64nvme(b"payload"),
    };
    assert_matches!(
        multipart_parts(&[wrong_part], required_algorithm)
            .expect_err("a part checksum in another algorithm should be rejected"),
        CoreError::InvalidUploadContent(_)
    );
    let wrong_signing_claim = UploadPartChecksumClaim {
        part_number: 1,
        checksum: Checksum::crc64nvme(b"payload"),
    };
    assert_matches!(
        validate_upload_checksum(&wrong_signing_claim.checksum, required_algorithm)
            .expect_err("a signing claim in another algorithm should be rejected"),
        CoreError::InvalidUploadContent(_)
    );
    let wrong_content = UploadContentClaim {
        size_bytes: 7,
        checksum: Checksum::crc64nvme(b"payload"),
    };
    assert_matches!(
        claimed_content_ref(
            session.namespace_id.clone(),
            session.content_id,
            &wrong_content,
            required_algorithm
        )
        .expect_err("a content claim in another algorithm should be rejected"),
        CoreError::InvalidUploadContent(_)
    );
}

#[tokio::test]
async fn a_direct_put_requires_the_session_algorithm_and_replays_completion() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap");
    let begin = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .engine()
        .begin_direct_put_upload_target()
        .await
        .expect("begin direct put");

    let wrong_algorithm =
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context(2_000))
            .engine()
            .complete_upload(
                &crate::test_support::namespace_store(&store, &namespace_id)
                    .load_catalog_entry()
                    .await
                    .expect("load the catalog"),
                &begin.session.upload_id,
                CompleteUploadBody::DirectPut {
                    content: UploadContentClaim {
                        size_bytes: BYTES.len() as u64,
                        checksum: Checksum::crc32c(BYTES),
                    },
                },
            )
            .await
            .expect_err("a different checksum algorithm is invalid content");
    assert_matches!(wrong_algorithm, CoreError::InvalidUploadContent(_));
    let open = crate::test_support::namespace_store(&store, &namespace_id)
        .load_upload_session_state(&begin.session.upload_id)
        .await
        .expect("session remains readable");
    assert_eq!(open.outcome, None);

    store
        .put(
            &begin.object_key,
            Bytes::from_static(BYTES),
            PutMode::CreateIfAbsent,
        )
        .await
        .expect("write direct-put bytes");
    let completion = CompleteUploadBody::DirectPut {
        content: UploadContentClaim {
            size_bytes: BYTES.len() as u64,
            checksum: Checksum::crc64nvme(BYTES),
        },
    };
    let first = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context(3_000))
        .engine()
        .complete_upload(
            &crate::test_support::namespace_store(&store, &namespace_id)
                .load_catalog_entry()
                .await
                .expect("load the catalog"),
            &begin.session.upload_id,
            completion.clone(),
        )
        .await
        .expect("complete direct put");
    let replay = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context(3_000))
        .engine()
        .complete_upload(
            &crate::test_support::namespace_store(&store, &namespace_id)
                .load_catalog_entry()
                .await
                .expect("load the catalog"),
            &begin.session.upload_id,
            completion,
        )
        .await
        .expect("replay direct-put completion");
    assert_eq!(replay, first);
}

#[tokio::test]
async fn provider_abort_failure_stops_before_object_delete() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context(1_000))
        .create()
        .await
        .expect("bootstrap");
    let content_id = ContentId::generate();
    let content_key = content_blob(&namespace_id, &content_id);
    store
        .put(
            &content_key,
            Bytes::from_static(BYTES),
            PutMode::CreateIfAbsent,
        )
        .await
        .expect("write unpublished content");
    let abandoned = AbandonedUpload {
        owner_namespace_id: namespace_id.clone(),
        content_id,
        provider_multipart_upload_id: Some("unsupported-provider-upload".to_owned()),
    };

    assert!(
        !abandoned.release(&store,).await,
        "an unsupported provider abort is incomplete cleanup"
    );
    assert!(
        store
            .head(&content_key)
            .await
            .expect("head content")
            .is_some(),
        "object deletion waits until provider abort succeeds"
    );
}

#[tokio::test]
async fn a_completion_after_an_abort_fails_terminally_and_touches_nothing() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let setup = context(1_000);
    let (namespace_id, upload_id, _content_ref, content_key) = staged_session(&store, &setup).await;

    abort(&store, &namespace_id, &upload_id, &context(2_000))
        .await
        .expect("abort");
    assert!(store.head(&content_key).await.expect("head").is_none());

    let error = complete(&store, &namespace_id, &upload_id, &context(3_000))
        .await
        .expect_err("an aborted session cannot complete");
    assert_matches!(error, CoreError::UploadNotFound { .. });

    let state = crate::test_support::namespace_store(&store, &namespace_id)
        .load_upload_session_state(&upload_id)
        .await
        .expect("session still readable");
    assert_eq!(
        state.outcome,
        Some(UploadOutcome::Aborted {
            aborted_at_ms: 2_000
        })
    );
    assert!(store.head(&content_key).await.expect("head").is_none());
}

#[tokio::test]
async fn a_mismatched_direct_put_claim_is_rejected_and_a_matching_one_completes() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap");
    let begin = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .engine()
        .begin_direct_put_upload_target()
        .await
        .expect("begin direct put");
    let written = vec![b'x'; BYTES.len()];
    store
        .put(
            &begin.object_key,
            Bytes::from(written.clone()),
            PutMode::CreateIfAbsent,
        )
        .await
        .expect("write direct-put bytes");

    let error = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context(2_000))
        .engine()
        .complete_upload(
            &crate::test_support::namespace_store(&store, &namespace_id)
                .load_catalog_entry()
                .await
                .expect("load the catalog"),
            &begin.session.upload_id,
            CompleteUploadBody::DirectPut {
                content: UploadContentClaim {
                    size_bytes: BYTES.len() as u64,
                    checksum: Checksum::crc64nvme(BYTES),
                },
            },
        )
        .await
        .expect_err("a claim the object does not match cannot complete");
    assert_matches!(error, CoreError::InvalidUploadContent(_));
    let state = crate::test_support::namespace_store(&store, &namespace_id)
        .load_upload_session_state(&begin.session.upload_id)
        .await
        .expect("session remains readable");
    assert_eq!(state.outcome, None);
    assert!(store
        .head(&begin.object_key)
        .await
        .expect("head direct-put content")
        .is_some());

    let completed =
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context(3_000))
            .engine()
            .complete_upload(
                &crate::test_support::namespace_store(&store, &namespace_id)
                    .load_catalog_entry()
                    .await
                    .expect("load the catalog"),
                &begin.session.upload_id,
                CompleteUploadBody::DirectPut {
                    content: UploadContentClaim {
                        size_bytes: written.len() as u64,
                        checksum: Checksum::crc64nvme(&written),
                    },
                },
            )
            .await
            .expect("the matching claim completes the session");
    assert_eq!(
        completed
            .response
            .content_ref()
            .expect("completed content ref")
            .checksum,
        Checksum::crc64nvme(&written)
    );
}

#[tokio::test]
async fn an_abort_after_completion_is_refused_and_keeps_the_content() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let setup = context(1_000);
    let (namespace_id, upload_id, _content_ref, content_key) = staged_session(&store, &setup).await;
    complete(&store, &namespace_id, &upload_id, &context(2_000))
        .await
        .expect("complete");

    let error = abort(&store, &namespace_id, &upload_id, &context(3_000))
        .await
        .expect_err("a completed session cannot be aborted");
    assert_matches!(error, CoreError::UploadAlreadyCompleted { .. });
    assert!(
        store.head(&content_key).await.expect("head").is_some(),
        "a refused abort must not clean up published-able content"
    );
}

#[tokio::test]
async fn a_repeated_abort_reports_the_first_stamp() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let setup = context(1_000);
    let (namespace_id, upload_id, _content_ref, _content_key) =
        staged_session(&store, &setup).await;

    let first = abort(&store, &namespace_id, &upload_id, &context(2_000))
        .await
        .expect("first abort");
    let second = abort(&store, &namespace_id, &upload_id, &context(9_000))
        .await
        .expect("repeated abort");

    assert_eq!(first.mode, UploadMode::ServiceProxied);
    assert_matches!(
        &first.status,
        UploadSessionStatus::Aborted {
            aborted_at_ms: 2_000
        }
    );
    assert_eq!(second, first);
}

#[tokio::test]
async fn staging_into_a_terminal_session_is_refused() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let setup = context(1_000);
    let (namespace_id, upload_id, _content_ref, _content_key) =
        staged_session(&store, &setup).await;
    complete(&store, &namespace_id, &upload_id, &context(2_000))
        .await
        .expect("complete");
    let error = crate::test_support::NamespaceFixture::new(
        &store,
        &namespace_id,
        &crate::test_support::writer_at("test-writer", 2_000),
    )
    .engine()
    .upload_content(&upload_id, BYTES)
    .await
    .expect_err("a completed session takes no more bytes");
    assert_matches!(error, CoreError::UploadAlreadyCompleted { .. });

    let aborted = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .engine()
        .begin_upload()
        .await
        .expect("begin a second upload");
    abort(&store, &namespace_id, &aborted.upload_id, &context(3_000))
        .await
        .expect("abort");
    let error = crate::test_support::NamespaceFixture::new(
        &store,
        &namespace_id,
        &crate::test_support::writer_at("test-writer", 3_000),
    )
    .engine()
    .upload_content(&aborted.upload_id, BYTES)
    .await
    .expect_err("an aborted session takes no more bytes");
    assert_matches!(error, CoreError::UploadNotFound { .. });
}

#[tokio::test]
async fn an_open_session_takes_upload_work_only_before_its_expiry() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let setup = context(1_000);
    let (namespace_id, staged_id, _, _) = staged_session(&store, &setup).await;
    let catalog = crate::test_support::namespace_store(&store, &namespace_id)
        .load_catalog_entry()
        .await
        .expect("catalog");
    let proxied_id = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .engine()
        .begin_upload()
        .await
        .expect("begin proxied upload")
        .upload_id;
    let multipart_id = create_upload_session(
        &store,
        &catalog,
        None,
        NewUploadSession::direct_multipart(
            ContentId::generate(),
            "provider-upload",
            NonZeroU64::new(5 * 1024 * 1024).expect("part size"),
            store.checksum_algorithm(),
        ),
        &setup.clock(),
    )
    .await
    .expect("begin multipart upload")
    .upload_id;
    let part = [UploadPartChecksumClaim {
        part_number: 1,
        checksum: Checksum::crc64nvme(BYTES),
    }];
    let expires_at_ms = setup.now_ms + UPLOAD_SESSION_LEASE_MS;

    for (now_ms, open) in [(expires_at_ms, false), (expires_at_ms - 1, true)] {
        let results = [
            (
                "staging",
                crate::test_support::NamespaceFixture::new(
                    &store,
                    &namespace_id,
                    &crate::test_support::writer_at("test-writer", now_ms),
                )
                .engine()
                .upload_content(&proxied_id, BYTES)
                .await
                .map(drop),
            ),
            (
                "part signing",
                crate::test_support::NamespaceFixture::new(
                    &store,
                    &namespace_id,
                    &crate::test_support::writer_at("test-writer", now_ms),
                )
                .engine()
                .direct_multipart_part_targets(&multipart_id, &part)
                .await
                .map(drop),
            ),
            (
                "completion",
                complete(&store, &namespace_id, &staged_id, &context(now_ms))
                    .await
                    .map(drop),
            ),
        ];
        for (operation, result) in results {
            assert_matches!(
                (open, &result),
                (true, Ok(())) | (false, Err(CoreError::UploadNotFound { .. })),
                "{operation} at {now_ms} answered {result:?}"
            );
        }
    }
}

#[tokio::test]
async fn an_expired_open_session_still_reports_its_status_and_aborts() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let setup = context(1_000);
    let (namespace_id, upload_id, _, _) = staged_session(&store, &setup).await;
    let expired = context(setup.now_ms + UPLOAD_SESSION_LEASE_MS);

    let view = crate::test_support::NamespaceFixture::new(
        &store,
        &namespace_id,
        &crate::test_support::writer_at("test-writer", expired.now_ms),
    )
    .engine()
    .get_upload(&upload_id)
    .await
    .expect("status of an expired session");
    assert_matches!(view.session.status, UploadSessionStatus::Open { .. });
    let aborted = abort(&store, &namespace_id, &upload_id, &expired)
        .await
        .expect("abort an expired session");
    assert_eq!(
        aborted.status,
        UploadSessionStatus::Aborted {
            aborted_at_ms: expired.now_ms
        }
    );
}

#[tokio::test]
async fn only_a_completed_session_mints_evidence() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let setup = context(1_000);
    let (namespace_id, upload_id, content_ref, _content_key) = staged_session(&store, &setup).await;

    let crate::UploadSessionView {
        session: open,
        evidence,
        ..
    } = crate::test_support::NamespaceFixture::new(
        &store,
        &namespace_id,
        &crate::test_support::writer_at("test-writer", 1_500),
    )
    .engine()
    .get_upload(&upload_id)
    .await
    .expect("status of an open session");
    assert_matches!(open.status, UploadSessionStatus::Open { .. });
    assert!(
        evidence.is_none(),
        "an open session provides no stored checksum evidence"
    );

    complete(&store, &namespace_id, &upload_id, &context(2_000))
        .await
        .expect("complete");
    let crate::UploadSessionView {
        session: completed,
        evidence,
        ..
    } = crate::test_support::NamespaceFixture::new(
        &store,
        &namespace_id,
        &crate::test_support::writer_at("test-writer", 2_500),
    )
    .engine()
    .get_upload(&upload_id)
    .await
    .expect("status of a completed session");
    assert_matches!(completed.status, UploadSessionStatus::Completed { .. });
    assert_eq!(
        evidence.expect("a completed session mints").content_ref(),
        &content_ref
    );

    // A second session, aborted, to check the other terminal state.
    let begin = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .engine()
        .begin_upload()
        .await
        .expect("begin second upload");
    abort(&store, &namespace_id, &begin.upload_id, &context(3_000))
        .await
        .expect("abort");
    let crate::UploadSessionView {
        session: aborted,
        evidence,
        ..
    } = crate::test_support::NamespaceFixture::new(
        &store,
        &namespace_id,
        &crate::test_support::writer_at("test-writer", 3_500),
    )
    .engine()
    .get_upload(&begin.upload_id)
    .await
    .expect("status of an aborted session");
    assert_matches!(aborted.status, UploadSessionStatus::Aborted { .. });
    assert!(
        evidence.is_none(),
        "an aborted session provides no stored checksum evidence"
    );
}

#[tokio::test]
async fn a_completed_session_keeps_its_content_ref_after_token_minting_closes() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let setup = context(1_000);
    let (namespace_id, upload_id, content_ref, _content_key) = staged_session(&store, &setup).await;
    let completed_at_ms = 2_000;
    complete(&store, &namespace_id, &upload_id, &context(completed_at_ms))
        .await
        .expect("complete");

    // Long after the first evidence would have expired, the durable
    // session still answers with usable evidence.
    let much_later = completed_at_ms + COMPLETED_UPLOAD_RECEIPT_WINDOW_MS - 1;
    let crate::UploadSessionView {
        session: _,
        evidence,
        ..
    } = crate::test_support::NamespaceFixture::new(
        &store,
        &namespace_id,
        &crate::test_support::writer_at("test-writer", much_later),
    )
    .engine()
    .get_upload(&upload_id)
    .await
    .expect("status inside the receipt window");
    assert_eq!(evidence.expect("still minting").content_ref(), &content_ref);

    let past = completed_at_ms + COMPLETED_UPLOAD_RECEIPT_WINDOW_MS;
    let crate::UploadSessionView {
        session: status,
        evidence,
        ..
    } = crate::test_support::NamespaceFixture::new(
        &store,
        &namespace_id,
        &crate::test_support::writer_at("test-writer", past),
    )
    .engine()
    .get_upload(&upload_id)
    .await
    .expect("status past the receipt window");
    let completed = match status.status {
        UploadSessionStatus::Completed {
            content_ref,
            content_token,
            ..
        } => Some((content_ref, content_token)),
        _ => None,
    };
    assert_eq!(completed, Some((content_ref.clone(), None)));
    assert!(
        evidence.is_none(),
        "past the window no evidence exists, which is what lets content GC decide"
    );

    // The same rule governs a very late idempotent completion replay.
    let replay = complete(&store, &namespace_id, &upload_id, &context(past))
        .await
        .expect("replay still succeeds");
    assert_eq!(replay.response.content_ref(), Some(&content_ref));
    assert!(replay.response.content_token().is_none());
    assert!(replay.evidence.is_none());
}
async fn delete_upload_namespace<S: ObjectStore>(store: &S, namespace_id: &NamespaceId) {
    crate::test_support::NamespaceFixture::new(store, namespace_id, &context(3_000))
        .engine()
        .delete_namespace(Default::default())
        .await
        .expect("delete namespace");
}

#[tokio::test]
async fn deleted_namespace_refuses_multipart_part_targets() {
    let directory = tempdir().expect("tempdir");
    let inner = LocalFsStore::new(directory.path()).expect("store");
    let (namespace_id, _, _, _) = staged_session(&inner, &context(1_000)).await;
    let catalog = crate::test_support::namespace_store(&inner, &namespace_id)
        .load_catalog_entry()
        .await
        .expect("catalog");
    let upload_id = create_upload_session(
        &inner,
        &catalog,
        None,
        NewUploadSession::direct_multipart(
            ContentId::generate(),
            "provider-upload",
            NonZeroU64::new(5 * 1024 * 1024).expect("part size"),
            inner.checksum_algorithm(),
        ),
        &context(1_000).clock(),
    )
    .await
    .expect("multipart session")
    .upload_id;
    delete_upload_namespace(&inner, &namespace_id).await;
    let store = loonfs_test_support::stores::RecordingStore::new(inner, KeyPredicate::any());
    let error = crate::test_support::NamespaceFixture::new(
        &store,
        &namespace_id,
        &crate::test_support::writer_at("test-writer", 4_000),
    )
    .engine()
    .direct_multipart_part_targets(
        &upload_id,
        &[UploadPartChecksumClaim {
            part_number: 1,
            checksum: Checksum::crc64nvme(BYTES),
        }],
    )
    .await
    .expect_err("deleted namespace");
    assert_matches!(error, CoreError::NamespaceDeleted { .. });
    assert_eq!(store.counts().puts, 0);
    assert_eq!(store.counts().deletes, 0);
}

#[tokio::test]
async fn deleted_namespace_refuses_proxied_content() {
    let directory = tempdir().expect("tempdir");
    let inner = LocalFsStore::new(directory.path()).expect("store");
    let (namespace_id, upload_id, _, _) = staged_session(&inner, &context(1_000)).await;
    delete_upload_namespace(&inner, &namespace_id).await;
    let store = loonfs_test_support::stores::RecordingStore::new(inner, KeyPredicate::any());
    let error = crate::test_support::NamespaceFixture::new(
        &store,
        &namespace_id,
        &crate::test_support::writer_at("test-writer", 4_000),
    )
    .engine()
    .upload_content(&upload_id, BYTES)
    .await
    .expect_err("deleted namespace");
    assert_matches!(error, CoreError::NamespaceDeleted { .. });
    assert_eq!(store.counts().puts, 0);
    assert_eq!(store.counts().deletes, 0);
}

#[tokio::test]
async fn deleted_namespace_refuses_upload_completion() {
    let directory = tempdir().expect("tempdir");
    let inner = LocalFsStore::new(directory.path()).expect("store");
    let (namespace_id, upload_id, _, _) = staged_session(&inner, &context(1_000)).await;
    delete_upload_namespace(&inner, &namespace_id).await;
    let store = loonfs_test_support::stores::RecordingStore::new(inner, KeyPredicate::any());
    let error = complete(&store, &namespace_id, &upload_id, &context(4_000))
        .await
        .expect_err("deleted namespace");
    assert_matches!(error, CoreError::NamespaceDeleted { .. });
    assert_eq!(store.counts().puts, 0);
    assert_eq!(store.counts().deletes, 0);
}

#[tokio::test]
async fn deleted_namespace_refuses_upload_status_evidence() {
    let directory = tempdir().expect("tempdir");
    let inner = LocalFsStore::new(directory.path()).expect("store");
    let (namespace_id, upload_id, _, _) = staged_session(&inner, &context(1_000)).await;
    complete(&inner, &namespace_id, &upload_id, &context(2_000))
        .await
        .expect("complete");
    delete_upload_namespace(&inner, &namespace_id).await;
    let store = loonfs_test_support::stores::RecordingStore::new(inner, KeyPredicate::any());
    let error = crate::test_support::NamespaceFixture::new(
        &store,
        &namespace_id,
        &crate::test_support::writer_at("test-writer", 4_000),
    )
    .engine()
    .get_upload(&upload_id)
    .await
    .expect_err("deleted namespace");
    assert_matches!(error, CoreError::NamespaceDeleted { .. });
    assert_eq!(store.counts().puts, 0);
    assert_eq!(store.counts().deletes, 0);
}
