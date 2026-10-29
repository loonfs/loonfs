//! Publication deadlines for completed content.

use super::*;
use crate::gc::GcOptions;
use crate::limits::{
    COMPLETED_UPLOAD_ADMISSION_WINDOW_MS, CONTENT_RECLAMATION_GRACE_WINDOW_MS,
    GC_MIN_GRACE_WINDOW_MS,
};
use crate::protocol::CompletedUpload;
use crate::test_support::WriterAt;
use loonfs_objectstore::keys::{content_blob, hint, wal_prefix};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::stores::{
    BlockingStore, KeyPredicate, MetadataMapStore, OperationSelector,
};
use loonfs_types::api::v0::CompleteUploadBody;
use loonfs_types::{AbsolutePath, DestinationBehavior};
use std::assert_matches;
use std::sync::atomic::{AtomicU64, Ordering};
use tempfile::tempdir;

#[derive(Debug, Default)]
struct PublicationTimer(AtomicU64);

impl MonotonicTimer for PublicationTimer {
    fn monotonic_now_ms(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

fn context(now_ms: u64) -> WriterAt {
    crate::test_support::writer_at("writer", now_ms)
}

async fn completed_upload<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &WriterAt,
) -> CompletedUpload {
    let upload = crate::test_support::NamespaceFixture::new(store, namespace_id, context)
        .engine()
        .begin_upload()
        .await
        .expect("begin upload");
    crate::test_support::NamespaceFixture::new(
        store,
        namespace_id,
        &crate::test_support::writer_at("test-writer", context.now_ms),
    )
    .engine()
    .upload_content(&upload.upload_id, b"completed content")
    .await
    .expect("stage upload");
    let completed = crate::test_support::NamespaceFixture::new(store, namespace_id, context)
        .engine()
        .complete_upload(
            &crate::test_support::namespace_store(store, namespace_id)
                .load_catalog_entry()
                .await
                .expect("load the catalog"),
            &upload.upload_id,
            CompleteUploadBody::ServiceProxied {},
        )
        .await
        .expect("complete upload");
    completed
}

fn put_candidate(completed: &CompletedUpload) -> CommitCandidate {
    CommitCandidate::prepared(
        CommitIntent::single(
            CommitId::parse("publish-content").expect("commit id"),
            loonfs_test_support::test_actor(),
            None,
            FilesystemOperation::PutFile {
                path: AbsolutePath::parse("/content").expect("path"),
                content: loonfs_types::FileContent::Ref(completed.prepared.content_ref().clone()),
                behavior: DestinationBehavior::NoReplace,
                expected_inode_id: None,
                expected_revision_no: None,
            },
        ),
        vec![completed.prepared.clone()],
    )
}

fn directory_request(commit_id: &str, name: &str) -> CommitIntent {
    CommitIntent::single(
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
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap");
    let completed = completed_upload(&store, &namespace_id, &setup).await;
    let catalog = crate::test_support::namespace_store(&store, &namespace_id)
        .load_catalog_entry()
        .await
        .expect("catalog");
    let token = mint_content_token(
        "secret",
        completed.evidence.as_ref().expect("completed evidence"),
        clock.now_ms(),
    )
    .expect("mint token");
    clock.advance_ms(1);
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context(clock.now_ms()))
        .engine()
        .delete_namespace(Default::default())
        .await
        .expect("delete");
    let prepared = verify_content_token("secret", &catalog, &token, clock.now_ms())
        .expect("token remains valid");
    let candidate =
        CommitCandidate::prepared(put_candidate(&completed).intent().clone(), vec![prepared]);
    let mut engine = NamespaceCommitEngine::new(
        crate::NamespaceStore {
            timer: clock.clone(),
            ..crate::test_support::namespace_store(&store, &namespace_id)
        },
        context(clock.now_ms()).writer_id.clone(),
    );
    store.reset();
    let result = engine
        .publish_batch(
            vec![candidate],
            &OperationClock::start(clock.now_ms(), clock.clone()),
        )
        .await;
    assert_matches!(
        &result.results[..],
        [Err(CoreError::NamespaceDeleted { .. })]
    );
    assert_eq!(store.counts().puts, 0);
}

#[tokio::test]
async fn content_reclaimed_during_view_load_cannot_be_published() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let store = BlockingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::exact(hint(&namespace_id)),
        OperationSelector::Read,
    );
    let setup = context(1_000);
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap");
    let completed = completed_upload(&store, &namespace_id, &setup).await;
    let content_key = content_blob(&namespace_id, &completed.prepared.content_ref().content_id);
    let publication = context(setup.now_ms + COMPLETED_UPLOAD_ADMISSION_WINDOW_MS - 1);
    let reclaimed = context(setup.now_ms + CONTENT_RECLAMATION_GRACE_WINDOW_MS + 1);
    let timer = Arc::new(PublicationTimer::default());
    let mut engine = NamespaceCommitEngine::new(
        crate::NamespaceStore {
            timer: timer.clone(),
            ..crate::test_support::namespace_store(&store, &namespace_id)
        },
        publication.writer_id.clone(),
    );
    engine
        .position
        .session_writer_epoch(&engine.namespace, &setup.writer_id)
        .await
        .expect("acquire writer");
    engine.invalidate_projection();
    let head_before = crate::test_support::namespace_store(&store, &namespace_id)
        .load_read_state()
        .await
        .expect("head");
    store.block_next();
    let batch = OperationClock::start(publication.now_ms, timer.clone());
    let publish = engine.publish_batch(vec![put_candidate(&completed)], &batch);
    let collect = async {
        store.wait_until_blocked().await;
        timer
            .0
            .store(reclaimed.now_ms - publication.now_ms, Ordering::SeqCst);
        let aged = MetadataMapStore::aged(&store, KeyPredicate::content_blob());
        let options = GcOptions {
            grace_window_ms: GC_MIN_GRACE_WINDOW_MS,
            ..Default::default()
        };
        crate::test_support::namespace_store(&aged, &namespace_id)
            .gc(None, &options, &reclaimed.clock())
            .await
            .expect("gc removes the session record");
        let report = crate::test_support::namespace_store(&aged, &namespace_id)
            .gc(None, &options, &reclaimed.clock())
            .await
            .expect("gc reclaims the content");
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
        loonfs_types::ErrorCode::ContentNotPrepared
    );
    let head_after = crate::test_support::namespace_store(&store, &namespace_id)
        .load_read_state()
        .await
        .expect("head");
    assert_eq!(head_after.seq, head_before.seq);
    assert_eq!(head_after.wal_no, head_before.wal_no);
    assert_eq!(
        store
            .list_prefix(&wal_prefix(&namespace_id))
            .await
            .expect("WAL objects")
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
        OperationSelector::Read,
    );
    let setup = context(1_000);
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap");
    let completed = completed_upload(&store, &namespace_id, &setup).await;
    let timer = Arc::new(PublicationTimer::default());
    let mut engine = NamespaceCommitEngine::new(
        crate::NamespaceStore {
            timer: timer.clone(),
            ..crate::test_support::namespace_store(&store, &namespace_id)
        },
        setup.writer_id.clone(),
    );
    let replay = CommitCandidate::new(directory_request("original", "original"));
    let original = engine
        .publish_batch(
            vec![replay.clone()],
            &OperationClock::start(setup.now_ms, timer.clone()),
        )
        .await
        .results
        .remove(0)
        .expect("original commit");
    let head_before = crate::test_support::namespace_store(&store, &namespace_id)
        .load_read_state()
        .await
        .expect("head");
    engine.invalidate_projection();
    let publication = context(setup.now_ms + COMPLETED_UPLOAD_ADMISSION_WINDOW_MS - 1);
    let primary = put_candidate(&completed);
    let alias = CommitCandidate::new(primary.intent().clone());
    let later = CommitCandidate::new(directory_request("later", "later"));
    store.block_next();
    let candidates = vec![primary, alias, later, replay];
    let batch = OperationClock::start(publication.now_ms, timer.clone());
    let publish = async {
        let mut result = engine.publish_batch(&candidates, &batch).await;
        // The first attempt finds the proof expired after loading its view
        // and aborts; the retry owner tries again from the same origin.
        while result.results.iter().any(|result| {
            matches!(
                result,
                Err(CoreError::WalPublish(
                    crate::wal::WalPublishError::NumberTaken
                ))
            )
        }) {
            result = engine.publish_batch(&candidates, &batch).await;
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
            loonfs_types::ErrorCode::ContentNotPrepared
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
    let head_after = crate::test_support::namespace_store(&store, &namespace_id)
        .load_read_state()
        .await
        .expect("head");
    assert_eq!(head_after.seq, ChangeSeq(2));
    assert_eq!(
        head_after.wal_no,
        head_before.wal_no.successor().expect("next")
    );
    assert_eq!(
        store
            .list_prefix(&wal_prefix(&namespace_id))
            .await
            .expect("WAL")
            .len(),
        3
    );
    drop(engine);
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let reopened = namespace
        .load_current_metadata_view()
        .await
        .expect("reopen");
    let access = crate::authorize::ReadAccess::live(crate::authorize::Authorizer::Unrestricted);
    assert_eq!(
        reopened
            .resolve_path("/content", loonfs_types::AttributeInclusion::Omit, &access)
            .await
            .expect_err("expired content was not published")
            .code(),
        loonfs_types::ErrorCode::PathNotFound
    );
    reopened
        .resolve_path("/later", loonfs_types::AttributeInclusion::Omit, &access)
        .await
        .expect("valid peer published");
}

#[tokio::test]
async fn content_expiring_after_the_put_starts_does_not_undo_the_commit() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let store = BlockingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::prefix(wal_prefix(&namespace_id)),
        OperationSelector::Put,
    );
    let setup = context(1_000);
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap");
    let completed = completed_upload(&store, &namespace_id, &setup).await;
    let timer = Arc::new(PublicationTimer::default());
    let mut engine = NamespaceCommitEngine::new(
        crate::NamespaceStore {
            timer: timer.clone(),
            ..crate::test_support::namespace_store(&store, &namespace_id)
        },
        setup.writer_id.clone(),
    );
    let replay = CommitCandidate::new(directory_request("original", "original"));
    let original = engine
        .publish_batch(
            vec![replay.clone()],
            &OperationClock::start(setup.now_ms, timer.clone()),
        )
        .await
        .results
        .remove(0)
        .expect("original commit");
    let head_before = crate::test_support::namespace_store(&store, &namespace_id)
        .load_read_state()
        .await
        .expect("head");
    let publication = context(setup.now_ms + COMPLETED_UPLOAD_ADMISSION_WINDOW_MS - 1);
    let primary = put_candidate(&completed);
    let alias = CommitCandidate::new(primary.intent().clone());
    let later = CommitCandidate::new(directory_request("later", "later"));
    store.block_next();
    let batch = OperationClock::start(publication.now_ms, timer.clone());
    let publish = engine.publish_batch(vec![primary, alias, later, replay], &batch);
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
    let head_after = crate::test_support::namespace_store(&store, &namespace_id)
        .load_read_state()
        .await
        .expect("head");
    assert_eq!(head_after.seq, ChangeSeq(3));
    assert_eq!(
        head_after.wal_no,
        head_before.wal_no.successor().expect("next")
    );
    assert_eq!(
        store
            .list_prefix(&wal_prefix(&namespace_id))
            .await
            .expect("WAL objects")
            .len(),
        3
    );
    drop(engine);
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let reopened = namespace
        .load_current_metadata_view()
        .await
        .expect("reopen namespace");
    for path in ["/content", "/later"] {
        reopened
            .resolve_path(
                path,
                loonfs_types::AttributeInclusion::Omit,
                &crate::authorize::ReadAccess::live(crate::authorize::Authorizer::Unrestricted),
            )
            .await
            .expect("committed path");
    }
    reopened
        .resolve_path(
            "/original",
            loonfs_types::AttributeInclusion::Omit,
            &crate::authorize::ReadAccess::live(crate::authorize::Authorizer::Unrestricted),
        )
        .await
        .expect("original remains");
}

#[tokio::test]
async fn swap_accepts_any_valid_matching_proof_and_expired_receipt_replays_without_content_io() {
    use crate::content::{mint_content_token, verify_content_token};
    use crate::manifest::{CompactionUnitOutcome, MetadataCompactionPolicy, MetadataRunPolicy};
    use loonfs_test_support::stores::{RecordedOperation, RecordingStore};
    use std::num::NonZeroUsize;

    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    let setup = context(1_000);
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap");
    let completed = completed_upload(&store, &namespace_id, &setup).await;
    let unused = completed_upload(&store, &namespace_id, &setup).await;
    let catalog = crate::test_support::namespace_store(&store, &namespace_id)
        .load_catalog_entry()
        .await
        .expect("catalog");
    let mut proofs = vec![completed.prepared.clone()];
    for upload in [&completed, &unused] {
        let token = mint_content_token(
            "secret",
            upload.evidence.as_ref().expect("evidence"),
            setup.now_ms,
        )
        .expect("mint");
        proofs.push(
            verify_content_token("secret", &catalog, &token, setup.now_ms).expect("prepare token"),
        );
    }
    let candidate =
        CommitCandidate::prepared(put_candidate(&completed).intent().clone(), proofs.clone());
    let metadata = CommitCandidate::prepared(
        directory_request("metadata", "metadata"),
        proofs[1..].to_vec(),
    );
    let publication = context(setup.now_ms + COMPLETED_UPLOAD_ADMISSION_WINDOW_MS - 1);
    let timer = Arc::new(PublicationTimer::default());
    let mut engine = NamespaceCommitEngine::new(
        crate::NamespaceStore {
            timer: timer.clone(),
            ..crate::test_support::namespace_store(&store, &namespace_id)
        },
        publication.writer_id.clone(),
    );
    store.reset();
    let result = engine
        .publish_batch(
            vec![candidate.clone(), metadata],
            &OperationClock::start(publication.now_ms, timer.clone()),
        )
        .await;
    let original = result.results[0]
        .as_ref()
        .expect("proof is valid one millisecond before its deadline")
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
    crate::test_support::namespace_store(&store, &namespace_id)
        .fold_wal()
        .await
        .expect("fold commits");
    loop {
        let outcome = {
            let mut fixture = crate::test_support::NamespaceFixture::new(
                &store,
                &namespace_id,
                &crate::test_support::writer_at("test-writer", 0),
            );
            fixture.namespace.run_policy = MetadataRunPolicy {
                max_delta_runs: NonZeroUsize::MIN,
                ..MetadataRunPolicy::default()
            };
            fixture
        }
        .engine()
        .compact_metadata_unit(
            None,
            MetadataCompactionPolicy::default(),
            crate::manifest::CompactionAdmission::AnySize,
            loonfs_types::CompactorEpoch(0),
            &crate::manifest::MetadataCompactionCancellation::default(),
        )
        .await
        .expect("compact metadata");
        if matches!(outcome, CompactionUnitOutcome::Published { .. }) {
            continue;
        }
        assert_matches!(
            outcome,
            CompactionUnitOutcome::NotNeeded,
            "unexpected compaction outcome: {outcome:?}"
        );
        break;
    }
    let expired = context(setup.now_ms + CONTENT_RECLAMATION_GRACE_WINDOW_MS + 1);
    let later = CommitCandidate::new(directory_request("later", "later"));
    store.reset();
    let replay = engine
        .publish_batch(
            vec![candidate, later],
            &OperationClock::start(expired.now_ms, timer.clone()),
        )
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
                == Some(loonfs_objectstore::layout::DurableObjectFamily::WalObject);
            family != Some(loonfs_objectstore::layout::DurableObjectFamily::ContentBlob)
                && !reads_wal
        }),
        "{:?}",
        store.snapshot()
    );
}

#[tokio::test]
async fn retained_evidence_minting_stops_at_the_upload_issuance_deadline() {
    use crate::content::mint_content_token;
    use crate::limits::{COMPLETED_UPLOAD_RECEIPT_WINDOW_MS, CONTENT_RECEIPT_TTL_MS};

    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let setup = context(1_000);
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap");
    let completed = completed_upload(&store, &namespace_id, &setup).await;
    let evidence = completed.evidence.expect("eligible evidence");
    let deadline_ms = setup.now_ms + COMPLETED_UPLOAD_RECEIPT_WINDOW_MS;
    for now_ms in [setup.now_ms, deadline_ms - 1] {
        let token = mint_content_token("secret", &evidence, now_ms).expect("eligible mint");
        let payload =
            loonfs_types::decode_token_bytes(token.token.split_once('.').expect("signed token").0)
                .expect("payload");
        let payload: serde_json::Value = serde_json::from_slice(&payload).expect("token payload");
        let expires_at_ms = payload["token"]["expires_at_ms"].as_u64().expect("expiry");
        assert_eq!(expires_at_ms, now_ms + CONTENT_RECEIPT_TTL_MS);
        assert!(expires_at_ms <= setup.now_ms + COMPLETED_UPLOAD_ADMISSION_WINDOW_MS);
    }
    for now_ms in [
        deadline_ms,
        setup.now_ms + CONTENT_RECLAMATION_GRACE_WINDOW_MS * 2,
    ] {
        assert_eq!(
            mint_content_token("secret", &evidence, now_ms),
            Err(ContentTokenError::Expired)
        );
    }
}
