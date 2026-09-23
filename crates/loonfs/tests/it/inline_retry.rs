//! Retrying an inline commit with a retained receipt must not upload the content again.

use bytes::Bytes;
use loonfs::publish::{CommitCandidate, CommitRequest, FilesystemOperation, InlineContent};
use loonfs::{CreateNamespaceOptions, FsWriter, InlineContentOptions, SharedObjectStore};
use loonfs_api::{
    AbsolutePath, AccessGrants, AccessRight, AccessRights, CommitId, ContentId,
    DestinationBehavior, ErrorCode, NamespaceAccess, NamespaceId, PrincipalId, PrincipalScope,
    PrincipalSet, Subject, SubjectId,
};
use loonfs_objectstore::layout::{parse_object_key, DurableObjectFamily};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::clock::ManualClock;
use loonfs_test_support::stores::{
    BlockingStore, FailStore, InjectedError, KeyPredicate, OperationClass, RecordedOperation,
    RecordingStore,
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;

fn namespace() -> NamespaceId {
    NamespaceId::parse("inline-retry").expect("namespace")
}

fn subject(name: &str) -> Subject {
    Subject {
        principal_scope: PrincipalScope::parse("scope").expect("scope"),
        subject_id: SubjectId::parse(name).expect("subject"),
        principals: PrincipalSet::new(BTreeSet::from([
            PrincipalId::parse(name).expect("principal")
        ]))
        .expect("principals"),
    }
}

fn grants(name: &str, rights: &[AccessRight]) -> AccessGrants {
    AccessGrants::new(BTreeMap::from([(
        PrincipalId::parse(name).expect("principal"),
        AccessRights::from_iter(rights.iter().copied()),
    )]))
    .expect("grants")
}

fn access(grants: AccessGrants) -> FilesystemOperation {
    FilesystemOperation::UpdateAccess {
        path: AbsolutePath::parse("/team").expect("path"),
        boundary: true,
        grants,
        expected_inode_id: None,
        expected_access_revision_no: None,
    }
}

fn request(id: &str, who: &str, operation: FilesystemOperation) -> CommitRequest {
    CommitRequest::single(
        CommitId::parse(id).expect("commit id"),
        loonfs_test_support::test_actor(),
        None,
        operation,
    )
    .with_subject(subject(who))
}

fn inline(id: &str, who: &str, bytes: &'static [u8]) -> CommitCandidate {
    let value = InlineContent::new(
        namespace(),
        ContentId::generate(),
        Bytes::from_static(bytes),
    );
    CommitCandidate::with_inline_content(
        request(
            id,
            who,
            FilesystemOperation::PutFile {
                path: AbsolutePath::parse("/team/file").expect("path"),
                content_ref: Some(value.content_ref().clone()),
                inline_content: None,
                behavior: DestinationBehavior::Replace,
                expected_inode_id: None,
                expected_revision_no: None,
            },
        ),
        Vec::new(),
        vec![value],
    )
}

async fn open(store: SharedObjectStore, segment_budget: usize) -> FsWriter {
    FsWriter::builder_with_store(store)
        .writer_id("inline-retry")
        .min_publish_interval_ms(0)
        .monotonic_timer(Arc::new(ManualClock::new(0)))
        .inline_content(InlineContentOptions {
            inline_content_segment_budget_bytes: segment_budget,
            ..Default::default()
        })
        .build()
        .await
        .expect("writer")
}

async fn seed(writer: &FsWriter) {
    writer
        .create_namespace(
            &namespace(),
            CreateNamespaceOptions {
                access: NamespaceAccess::Acl {
                    principal_scope: PrincipalScope::parse("scope").expect("scope"),
                    root_grants: grants("root", &[AccessRight::Admin]),
                },
                ..CreateNamespaceOptions::new(loonfs_test_support::test_actor())
            },
        )
        .await
        .expect("namespace");
    let mut seed = request(
        "seed",
        "root",
        FilesystemOperation::CreateDirectory {
            path: AbsolutePath::parse("/team").expect("path"),
            parents: false,
        },
    );
    seed.operations.push(access(grants(
        "alice",
        &[AccessRight::Read, AccessRight::Write, AccessRight::Create],
    )));
    writer
        .create_commit(&namespace(), seed)
        .await
        .expect("seed");
}

enum ReceiptState {
    Warm,
    Restarted,
    Folded,
    ManifestOnly,
}

async fn retained_receipt_skips_fallback(state: ReceiptState) {
    let directory = tempfile::tempdir().expect("directory");
    let failing = Arc::new(FailStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::content_blob(),
        OperationClass::Put,
        InjectedError::PermissionDenied("content writes refused".to_owned()),
    ));
    let recording = Arc::new(RecordingStore::new(failing.clone(), KeyPredicate::any()));
    let mut writer = open(recording.clone(), 1).await;
    seed(&writer).await;
    let original = writer
        .commit_candidate(&namespace(), inline("retained", "alice", b"recorded"))
        .await
        .expect("original commit");
    writer
        .create_commit(
            &namespace(),
            request("revoke", "root", access(AccessGrants::default())),
        )
        .await
        .expect("revoke access");
    if matches!(state, ReceiptState::Folded | ReceiptState::ManifestOnly) {
        writer
            .maintenance_handle("inline-retry")
            .expect("maintenance")
            .flush_wal(&namespace())
            .await
            .expect("fold receipts");
    }
    if matches!(state, ReceiptState::Restarted | ReceiptState::ManifestOnly) {
        writer.shutdown().await.expect("shutdown original writer");
        writer = open(recording.clone(), 1).await;
    }
    if matches!(state, ReceiptState::ManifestOnly) {
        writer
            .create_commit(
                &namespace(),
                request(
                    "warm-after-fold",
                    "root",
                    FilesystemOperation::CreateDirectory {
                        path: AbsolutePath::parse("/after-fold").expect("path"),
                        parents: false,
                    },
                ),
            )
            .await
            .expect("load a publisher projection after the receipt was folded");
    }
    failing.fail_all();
    recording.reset();
    let replay = writer
        .commit_candidate(&namespace(), inline("retained", "alice", b"recorded"))
        .await
        .expect("receipt replays after access revocation");
    assert_eq!(replay.committed_seq, original.committed_seq);
    assert_eq!(
        writer
            .commit_candidate(&namespace(), inline("retained", "alice", b"modified"))
            .await
            .expect_err("changed bytes conflict")
            .code(),
        ErrorCode::CommitIdReuseConflict
    );
    assert_eq!(
        writer
            .commit_candidate(&namespace(), inline("retained", "bob", b"recorded"))
            .await
            .expect_err("another subject conflicts")
            .code(),
        ErrorCode::CommitIdReuseConflict
    );
    assert_eq!(failing.attempts(), 0);
    assert!(!recording.snapshot().iter().any(|operation| {
        parse_object_key(operation.key())
            .is_some_and(|key| key.family() == DurableObjectFamily::UploadSession)
    }));
    writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn warm_receipts_answer_retries_without_content_writes() {
    retained_receipt_skips_fallback(ReceiptState::Warm).await;
}

#[tokio::test]
async fn restarted_receipts_answer_retries_without_content_writes() {
    retained_receipt_skips_fallback(ReceiptState::Restarted).await;
}

#[tokio::test]
async fn folded_receipts_answer_retries_without_content_writes() {
    retained_receipt_skips_fallback(ReceiptState::Folded).await;
}

#[tokio::test]
async fn manifest_receipts_answer_retries_with_a_warm_publisher_without_content_writes() {
    retained_receipt_skips_fallback(ReceiptState::ManifestOnly).await;
}

#[tokio::test]
async fn inline_publication_without_fallback_keeps_its_store_requests() {
    let directory = tempfile::tempdir().expect("directory");
    let recording = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let writer = open(recording.clone(), 1024).await;
    seed(&writer).await;
    recording.reset();
    writer
        .commit_candidate(&namespace(), inline("inline", "alice", b"recorded"))
        .await
        .expect("inline publication");
    let operations = recording.take();
    assert_eq!(operations.len(), 1, "{operations:?}");
    assert!(matches!(&operations[0], RecordedOperation::Put { key, .. }
        if key.starts_with(&loonfs_objectstore::keys::wal_segment_prefix(&namespace()))));
    writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn cold_receipt_lookup_does_not_acquire_authority_or_block_other_submissions() {
    let directory = tempfile::tempdir().expect("directory");
    let blocking = Arc::new(BlockingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
        OperationClass::Read,
    ));
    let recording = Arc::new(RecordingStore::new(blocking.clone(), KeyPredicate::any()));
    let original_writer = open(recording.clone(), 1).await;
    seed(&original_writer).await;
    let original = original_writer
        .commit_candidate(&namespace(), inline("retained", "alice", b"recorded"))
        .await
        .expect("original commit");
    let cold_writer = open(recording.clone(), 1).await;
    recording.reset();
    blocking.block_next();
    let namespace_id = namespace();
    let retry =
        cold_writer.commit_candidate(&namespace_id, inline("retained", "alice", b"recorded"));
    let submissions = async {
        blocking.wait_until_blocked().await;
        assert_eq!(recording.count(OperationClass::Put), 0);
        original_writer
            .create_commit(
                &namespace_id,
                request("old-writer", "root", access(AccessGrants::default())),
            )
            .await
            .expect("lookup has not fenced the original writer");
        cold_writer
            .create_commit(
                &namespace_id,
                request("new-writer", "root", access(AccessGrants::default())),
            )
            .await
            .expect("another submission can use the publisher engine");
        blocking.release();
    };
    let (replay, ()) = timeout(Duration::from_secs(10), async {
        tokio::join!(retry, submissions)
    })
    .await
    .expect("submissions finish while the lookup is blocked");
    assert_eq!(
        replay.expect("replay").committed_seq,
        original.committed_seq
    );
    original_writer
        .shutdown()
        .await
        .expect("shutdown original writer");
    cold_writer.shutdown().await.expect("shutdown cold writer");
}

#[tokio::test]
async fn failed_receipt_lookup_writes_no_durable_state() {
    let directory = tempfile::tempdir().expect("directory");
    let failing = Arc::new(FailStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
        OperationClass::Read,
        InjectedError::PermissionDenied("metadata reads refused".to_owned()),
    ));
    let recording = Arc::new(RecordingStore::new(failing.clone(), KeyPredicate::any()));
    let writer = open(recording.clone(), 1).await;
    seed(&writer).await;
    writer
        .commit_candidate(&namespace(), inline("retained", "alice", b"recorded"))
        .await
        .expect("original commit");
    writer.shutdown().await.expect("shutdown original writer");
    let writer = open(recording.clone(), 1).await;
    failing.fail_all();
    recording.reset();
    assert_eq!(
        writer
            .commit_candidate(&namespace(), inline("retained", "alice", b"recorded"))
            .await
            .expect_err("lookup fails")
            .code(),
        ErrorCode::StoragePermissionDenied
    );
    assert!(failing.attempts() > 0);
    assert_eq!(recording.count(OperationClass::Put), 0);
    assert_eq!(recording.count(OperationClass::Delete), 0);
    writer.shutdown().await.expect("shutdown");
}
