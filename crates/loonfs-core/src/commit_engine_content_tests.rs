//! Publication deadlines for completed content.

use super::*;
use crate::gc::{gc_namespace, GcConfig};
use crate::limits::{
    COMPLETED_UPLOAD_ADMISSION_WINDOW_MS, CONTENT_RECLAMATION_GRACE_MS, GC_MIN_GRACE_WINDOW_MS,
};
use crate::namespace::bootstrap::bootstrap_namespace;
use crate::namespace::control::load_namespace_read_state;
use crate::protocol::{
    begin_service_proxied_upload, complete_upload, upload_content, CompletedUpload,
    ResolvedUploadCompletion,
};
use loonfs_api::{AbsolutePath, DestinationBehavior, WriterId};
use loonfs_objectstore::keys::{content_blob, hint, wal_segment_prefix};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::stores::{BlockingStore, KeyPredicate, OperationClass};
use std::sync::atomic::{AtomicU64, Ordering};
use tempfile::tempdir;

#[derive(Debug, Default)]
struct PublicationTimer(AtomicU64);

impl MonotonicTimer for PublicationTimer {
    fn monotonic_now_ms(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

fn context(now_ms: u64) -> MutationContext {
    MutationContext {
        writer_id: WriterId::parse("writer").expect("writer id"),
        now_ms,
    }
}

async fn completed_upload<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) -> CompletedUpload {
    let upload = begin_service_proxied_upload(store, namespace_id, None, context)
        .await
        .expect("begin upload");
    upload_content(
        store,
        namespace_id,
        &upload.upload_id,
        None,
        b"completed content",
    )
    .await
    .expect("stage upload");
    let completed = complete_upload(
        store,
        namespace_id,
        &upload.upload_id,
        None,
        ResolvedUploadCompletion::KnownContent,
        context,
    )
    .await
    .expect("complete upload");
    completed
}

fn put_candidate(completed: &CompletedUpload) -> CommitCandidate {
    CommitCandidate::prepared(
        CommitRequest::single(
            CommitId::parse("publish-content").expect("commit id"),
            loonfs_test_support::test_actor(),
            None,
            FilesystemOperation::PutFile {
                path: AbsolutePath::parse("/content").expect("path"),
                content_ref: Some(completed.prepared.content_ref().clone()),
                inline_content: None,
                behavior: DestinationBehavior::NoReplace,
                expected_inode_id: None,
                expected_revision_no: None,
            },
        ),
        vec![completed.prepared.clone()],
    )
}

fn directory_request(commit_id: &str, name: &str) -> CommitRequest {
    CommitRequest::single(
        CommitId::parse(commit_id).expect("commit id"),
        loonfs_test_support::test_actor(),
        None,
        FilesystemOperation::CreateDirectory {
            path: AbsolutePath::parse(format!("/{name}")).expect("path"),
            parents: false,
        },
    )
}

#[tokio::test]
async fn a_completed_upload_token_cannot_publish_after_namespace_deletion() {
    use crate::content::{mint_content_token, verify_content_token};
    use crate::namespace::catalog::load_namespace_catalog_entry;
    use loonfs_test_support::clock::ManualClock;
    use loonfs_test_support::stores::RecordingStore;

    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("deleted-token").expect("namespace");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    let clock = Arc::new(ManualClock::new(1_000));
    let setup = context(clock.now_ms());
    bootstrap_namespace(
        &store,
        &namespace_id,
        &setup,
        &loonfs_test_support::test_actor(),
        &loonfs_api::NamespaceAccess::Unrestricted {},
        false,
    )
    .await
    .expect("bootstrap");
    let completed = completed_upload(&store, &namespace_id, &setup).await;
    let catalog = load_namespace_catalog_entry(&store, &namespace_id)
        .await
        .expect("catalog");
    let token = mint_content_token(
        "secret",
        completed.receipt.as_ref().expect("completed receipt"),
        clock.now_ms(),
    )
    .expect("mint token");
    clock.advance_ms(1);
    delete_namespace(
        &store,
        &namespace_id,
        Default::default(),
        &context(clock.now_ms()),
    )
    .await
    .expect("delete");
    let prepared = verify_content_token("secret", &catalog, &token, clock.now_ms())
        .expect("token remains valid");
    let candidate =
        CommitCandidate::prepared(put_candidate(&completed).request().clone(), vec![prepared]);
    let mut engine = NamespaceCommitEngine::new(namespace_id).monotonic_timer(clock.clone());
    store.reset();
    let result = engine
        .publish_batch(
            &store,
            vec![candidate],
            &context(clock.now_ms()),
            &PublishTailOptions::default(),
        )
        .await;
    assert!(matches!(
        &result.results[..],
        [Err(CoreError::NamespaceDeleted { .. })]
    ));
    assert_eq!(store.counts().puts, 0);
}

#[tokio::test]
async fn content_reclaimed_during_view_load_cannot_be_published() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let store = BlockingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::exact(hint(&namespace_id)),
        OperationClass::Read,
    );
    let setup = context(1_000);
    bootstrap_namespace(
        &store,
        &namespace_id,
        &setup,
        &loonfs_test_support::test_actor(),
        &loonfs_api::NamespaceAccess::Unrestricted {},
        false,
    )
    .await
    .expect("bootstrap");
    let completed = completed_upload(&store, &namespace_id, &setup).await;
    let content_key = content_blob(&namespace_id, &completed.prepared.content_ref().content_id);
    let publication = context(setup.now_ms + COMPLETED_UPLOAD_ADMISSION_WINDOW_MS - 1);
    let reclaimed = context(setup.now_ms + CONTENT_RECLAMATION_GRACE_MS + 1);
    let timer = Arc::new(PublicationTimer::default());
    let mut engine =
        NamespaceCommitEngine::new(namespace_id.clone()).monotonic_timer(timer.clone());
    engine
        .session_writer_epoch(&store, &setup)
        .await
        .expect("acquire writer");
    let head_before = load_namespace_read_state(&store, &namespace_id)
        .await
        .expect("head");
    store.block_next();
    let options = PublishTailOptions::default();
    let publish = engine.publish_batch(
        &store,
        vec![put_candidate(&completed)],
        &publication,
        &options,
    );
    let collect = async {
        store.wait_until_blocked().await;
        timer
            .0
            .store(reclaimed.now_ms - publication.now_ms, Ordering::SeqCst);
        let report = gc_namespace(
            &store,
            &namespace_id,
            &GcConfig {
                grace_window_ms: GC_MIN_GRACE_WINDOW_MS,
            },
            &reclaimed,
        )
        .await
        .expect("gc");
        assert_eq!(report.deleted.content_objects, 1);
        assert!(store
            .head(&content_key)
            .await
            .expect("content head")
            .is_none());
        store.release();
    };
    let (result, ()) = futures::join!(publish, collect);
    assert_eq!(
        result.results[0]
            .as_ref()
            .expect_err("reclaimed content must not publish")
            .code(),
        loonfs_api::ErrorCode::ContentNotPrepared
    );
    let head_after = load_namespace_read_state(&store, &namespace_id)
        .await
        .expect("head");
    assert_eq!(head_after.seq, head_before.seq);
    assert_eq!(head_after.wal_no, head_before.wal_no);
    assert_eq!(
        store
            .list_prefix(&wal_segment_prefix(&namespace_id))
            .await
            .expect("WAL segments")
            .len(),
        1
    );
}

#[tokio::test]
async fn publication_retry_does_not_refresh_expired_content_evidence() {
    assert_expired_content_stays_rejected_on_retry(2).await;
}

#[tokio::test]
async fn publication_retry_uses_a_fresh_tip_budget() {
    assert_expired_content_stays_rejected_on_retry(crate::limits::WAL_PUBLISH_BUDGET_MS + 1).await;
}

async fn assert_expired_content_stays_rejected_on_retry(elapsed_ms: u64) {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("retry-expiry").expect("namespace id");
    let store = BlockingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::exact(hint(&namespace_id)),
        OperationClass::Read,
    );
    let setup = context(1_000);
    bootstrap_namespace(
        &store,
        &namespace_id,
        &setup,
        &loonfs_test_support::test_actor(),
        &loonfs_api::NamespaceAccess::Unrestricted {},
        false,
    )
    .await
    .expect("bootstrap");
    let completed = completed_upload(&store, &namespace_id, &setup).await;
    let timer = Arc::new(PublicationTimer::default());
    let mut engine =
        NamespaceCommitEngine::new(namespace_id.clone()).monotonic_timer(timer.clone());
    let options = PublishTailOptions::default();
    let replay = CommitCandidate::new(directory_request("original", "original"));
    let original = engine
        .publish_batch(&store, vec![replay.clone()], &setup, &options)
        .await
        .results
        .remove(0)
        .expect("original commit");
    let head_before = load_namespace_read_state(&store, &namespace_id)
        .await
        .expect("head");
    engine.invalidate_projection();
    let publication = context(setup.now_ms + COMPLETED_UPLOAD_ADMISSION_WINDOW_MS - 1);
    let primary = put_candidate(&completed);
    let alias = CommitCandidate::new(primary.request().clone());
    let later = CommitCandidate::new(directory_request("later", "later"));
    store.block_next();
    let candidates = vec![primary, alias, later, replay];
    let batch_started_ms = engine.monotonic_now_ms();
    let publish = async {
        let mut result = engine
            .publish_batch_attempt(
                &store,
                &candidates,
                &publication,
                &options,
                batch_started_ms,
            )
            .await;
        // The first attempt finds the proof expired after loading its view
        // and aborts; the retry owner tries again from the same origin.
        while result.results.iter().any(|result| {
            matches!(
                result,
                Err(CoreError::WalPublish(
                    crate::commit::WalPublishError::StaleHead
                ))
            )
        }) {
            result = engine
                .publish_batch_attempt(
                    &store,
                    &candidates,
                    &publication,
                    &options,
                    batch_started_ms,
                )
                .await;
        }
        result
    };
    let advance = async {
        store.wait_until_blocked().await;
        // Cross the inclusive proof deadline during the first view load only.
        // Retrying the other candidate must not make this evidence valid again.
        timer.0.store(elapsed_ms, Ordering::SeqCst);
        store.release();
    };
    let (result, ()) = futures::join!(publish, advance);
    for outcome in &result.results[..2] {
        assert_eq!(
            outcome.as_ref().expect_err("expired evidence").code(),
            loonfs_api::ErrorCode::ContentNotPrepared
        );
    }
    assert_eq!(
        result.results[2]
            .as_ref()
            .expect("valid peer")
            .committed_seq,
        ChangeSeq(2)
    );
    assert_eq!(
        result.results[2]
            .as_ref()
            .expect("valid peer")
            .committed_at_ms,
        publication.now_ms
    );
    assert_eq!(result.results[3].as_ref().expect("replay"), &original);
    let head_after = load_namespace_read_state(&store, &namespace_id)
        .await
        .expect("head");
    assert_eq!(head_after.seq, ChangeSeq(2));
    assert_eq!(
        head_after.wal_no,
        head_before.wal_no.successor().expect("next")
    );
    assert_eq!(
        store
            .list_prefix(&wal_segment_prefix(&namespace_id))
            .await
            .expect("WAL")
            .len(),
        3
    );
    drop(engine);
    let reopened = crate::path::read::load_current_metadata_view(&store, &namespace_id)
        .await
        .expect("reopen");
    let access = crate::authorize::ReadAccess::live(crate::authorize::Authorizer::Unrestricted);
    assert_eq!(
        reopened
            .resolve_path("/content", loonfs_api::AttributeInclusion::Omit, &access)
            .await
            .expect_err("expired content was not published")
            .code(),
        loonfs_api::ErrorCode::PathNotFound
    );
    reopened
        .resolve_path("/later", loonfs_api::AttributeInclusion::Omit, &access)
        .await
        .expect("valid peer published");
}

#[tokio::test]
async fn content_expiring_after_the_put_starts_does_not_undo_the_commit() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let store = BlockingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::prefix(wal_segment_prefix(&namespace_id)),
        OperationClass::Put,
    );
    let setup = context(1_000);
    bootstrap_namespace(
        &store,
        &namespace_id,
        &setup,
        &loonfs_test_support::test_actor(),
        &loonfs_api::NamespaceAccess::Unrestricted {},
        false,
    )
    .await
    .expect("bootstrap");
    let completed = completed_upload(&store, &namespace_id, &setup).await;
    let timer = Arc::new(PublicationTimer::default());
    let mut engine =
        NamespaceCommitEngine::new(namespace_id.clone()).monotonic_timer(timer.clone());
    let options = PublishTailOptions::default();
    let replay = CommitCandidate::new(directory_request("original", "original"));
    let original = engine
        .publish_batch(&store, vec![replay.clone()], &setup, &options)
        .await
        .results
        .remove(0)
        .expect("original commit");
    let head_before = load_namespace_read_state(&store, &namespace_id)
        .await
        .expect("head");
    let publication = context(setup.now_ms + COMPLETED_UPLOAD_ADMISSION_WINDOW_MS - 1);
    let primary = put_candidate(&completed);
    let alias = CommitCandidate::new(primary.request().clone());
    let later = CommitCandidate::new(directory_request("later", "later"));
    store.block_next();
    let publish = engine.publish_batch(
        &store,
        vec![primary, alias, later, replay],
        &publication,
        &options,
    );
    let advance = async {
        store.wait_until_blocked().await;
        let elapsed_ms = 2;
        assert!(elapsed_ms < crate::limits::WAL_PUBLISH_BUDGET_MS);
        timer.0.store(elapsed_ms, Ordering::SeqCst);
        store.release();
    };
    let (result, ()) = futures::join!(publish, advance);
    assert_eq!(
        result.results[0].as_ref().expect("primary").committed_seq,
        ChangeSeq(2)
    );
    assert_eq!(
        result.results[0].as_ref().expect("primary"),
        result.results[1].as_ref().expect("alias")
    );
    assert_eq!(
        result.results[2].as_ref().expect("later").committed_seq,
        ChangeSeq(3)
    );
    assert_eq!(
        result.results[3].as_ref().expect("independent replay"),
        &original
    );
    let head_after = load_namespace_read_state(&store, &namespace_id)
        .await
        .expect("head");
    assert_eq!(head_after.seq, ChangeSeq(3));
    assert_eq!(
        head_after.wal_no,
        head_before.wal_no.successor().expect("next")
    );
    assert_eq!(
        store
            .list_prefix(&wal_segment_prefix(&namespace_id))
            .await
            .expect("WAL segments")
            .len(),
        3
    );
    drop(engine);
    let reopened = crate::path::read::load_current_metadata_view(&store, &namespace_id)
        .await
        .expect("reopen namespace");
    for path in ["/content", "/later"] {
        reopened
            .resolve_path(
                path,
                loonfs_api::AttributeInclusion::Omit,
                &crate::authorize::ReadAccess::live(crate::authorize::Authorizer::Unrestricted),
            )
            .await
            .expect("committed path");
    }
    reopened
        .resolve_path(
            "/original",
            loonfs_api::AttributeInclusion::Omit,
            &crate::authorize::ReadAccess::live(crate::authorize::Authorizer::Unrestricted),
        )
        .await
        .expect("original remains");
}

#[tokio::test]
async fn swap_accepts_any_valid_matching_proof_and_expired_receipt_replays_without_content_io() {
    use crate::checkpoint::{
        MetadataCompactionPolicy, MetadataLsmPolicy, MetadataReorganizeOutcome,
    };
    use crate::content::{mint_content_token, verify_content_token};
    use crate::namespace::catalog::load_namespace_catalog_entry;
    use loonfs_test_support::stores::{RecordedOperation, RecordingStore};
    use std::num::NonZeroUsize;

    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    let setup = context(1_000);
    bootstrap_namespace(
        &store,
        &namespace_id,
        &setup,
        &loonfs_test_support::test_actor(),
        &loonfs_api::NamespaceAccess::Unrestricted {},
        false,
    )
    .await
    .expect("bootstrap");
    let completed = completed_upload(&store, &namespace_id, &setup).await;
    let unused = completed_upload(&store, &namespace_id, &setup).await;
    let catalog = load_namespace_catalog_entry(&store, &namespace_id)
        .await
        .expect("catalog");
    let mut proofs = vec![completed.prepared.clone()];
    for upload in [&completed, &unused] {
        let token = mint_content_token(
            "secret",
            upload.receipt.as_ref().expect("receipt"),
            setup.now_ms,
        )
        .expect("mint");
        proofs.push(
            verify_content_token("secret", &catalog, &token, setup.now_ms).expect("prepare token"),
        );
    }
    let candidate =
        CommitCandidate::prepared(put_candidate(&completed).request().clone(), proofs.clone());
    let metadata = CommitCandidate::prepared(
        directory_request("metadata", "metadata"),
        proofs[1..].to_vec(),
    );
    let publication = context(setup.now_ms + COMPLETED_UPLOAD_ADMISSION_WINDOW_MS);
    let mut engine = NamespaceCommitEngine::new(namespace_id.clone())
        .monotonic_timer(Arc::new(PublicationTimer::default()));
    let options = PublishTailOptions::default();
    store.reset();
    let result = engine
        .publish_batch(
            &store,
            vec![candidate.clone(), metadata],
            &publication,
            &options,
        )
        .await;
    let original = result.results[0]
        .as_ref()
        .expect("proof is valid at its exact deadline")
        .clone();
    result.results[1]
        .as_ref()
        .expect("unused proofs do not constrain metadata");
    assert_eq!(original.committed_at_ms, publication.now_ms);
    assert!(
        !original.events.is_empty(),
        "a new commit returns its events"
    );
    assert!(
        store.snapshot().iter().all(|operation| !matches!(
            loonfs_objectstore::layout::parse_object_key(operation.key()),
            Some(key)
                if key.family() == loonfs_objectstore::layout::DurableObjectFamily::ContentBlob
        )),
        "{:?}",
        store.snapshot()
    );
    crate::checkpoint::flush_wal(&store, &namespace_id)
        .await
        .expect("flush commits");
    loop {
        let outcome = crate::checkpoint::reorganize_metadata_step(
            &store,
            &namespace_id,
            0,
            MetadataLsmPolicy {
                max_delta_runs: NonZeroUsize::MIN,
                ..MetadataLsmPolicy::default()
            },
            MetadataCompactionPolicy::default(),
        )
        .await
        .expect("reorganize metadata");
        if matches!(outcome, MetadataReorganizeOutcome::UnitPublished { .. }) {
            continue;
        }
        assert!(
            matches!(outcome, MetadataReorganizeOutcome::NotNeeded { .. }),
            "unexpected reorganization outcome: {outcome:?}"
        );
        break;
    }
    let expired = context(setup.now_ms + CONTENT_RECLAMATION_GRACE_MS + 1);
    let later = CommitCandidate::new(directory_request("later", "later"));
    store.reset();
    let replay = engine
        .publish_batch(&store, vec![candidate, later], &expired, &options)
        .await;
    assert_eq!(
        replay.results[0].as_ref().expect("durable replay"),
        &original
    );
    replay.results[1].as_ref().expect("new metadata commit");
    assert!(
        store.snapshot().iter().all(|operation| {
            let family = loonfs_objectstore::layout::parse_object_key(operation.key())
                .map(|key| key.family());
            let reads_wal = matches!(
                operation,
                RecordedOperation::Get {
                    result_bytes: 1..,
                    ..
                } | RecordedOperation::GetWithMetadata {
                    result_bytes: 1..,
                    ..
                }
            ) && family
                == Some(loonfs_objectstore::layout::DurableObjectFamily::WalSegment);
            family != Some(loonfs_objectstore::layout::DurableObjectFamily::ContentBlob)
                && !reads_wal
        }),
        "{:?}",
        store.snapshot()
    );
}

#[tokio::test]
async fn retained_receipt_minting_stops_at_the_upload_issuance_deadline() {
    use crate::content::mint_content_token;
    use crate::limits::{COMPLETED_UPLOAD_RECEIPT_WINDOW_MS, CONTENT_RECEIPT_TTL_MS};
    use base64::Engine as _;

    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let setup = context(1_000);
    bootstrap_namespace(
        &store,
        &namespace_id,
        &setup,
        &loonfs_test_support::test_actor(),
        &loonfs_api::NamespaceAccess::Unrestricted {},
        false,
    )
    .await
    .expect("bootstrap");
    let completed = completed_upload(&store, &namespace_id, &setup).await;
    let receipt = completed.receipt.expect("eligible receipt");
    let deadline_ms = setup.now_ms + COMPLETED_UPLOAD_RECEIPT_WINDOW_MS;
    for now_ms in [setup.now_ms, deadline_ms - 1] {
        let token = mint_content_token("secret", &receipt, now_ms).expect("eligible mint");
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(token.token.split_once('.').expect("signed token").0)
            .expect("payload");
        let payload: serde_json::Value = serde_json::from_slice(&payload).expect("token payload");
        let expires_at_ms = payload["expires_at_ms"].as_u64().expect("expiry");
        assert_eq!(expires_at_ms, now_ms + CONTENT_RECEIPT_TTL_MS);
        assert!(expires_at_ms <= setup.now_ms + COMPLETED_UPLOAD_ADMISSION_WINDOW_MS);
    }
    for now_ms in [deadline_ms, setup.now_ms + CONTENT_RECLAMATION_GRACE_MS * 2] {
        assert_eq!(
            mint_content_token("secret", &receipt, now_ms),
            Err(ContentTokenError::Expired)
        );
    }
}
