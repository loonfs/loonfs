//! Inline writer preparation, publication, fallback, and maintenance contracts.

use super::*;
use crate::{InlineContentPolicy, MetadataCache, MetadataMaintenanceOptions, PutFileOptions};
use loonfs_objectstore::layout::{parse_object_key, DurableObjectFamily};
use loonfs_test_support::clock::ManualClock;
use loonfs_test_support::stores::RecordedOperation;
use loonfs_types::format::wal::decode_wal_object_envelope_zstd;

fn policy() -> InlineContentPolicy {
    InlineContentPolicy {
        inline_content_threshold_bytes: Some(4),
        ..Default::default()
    }
}

#[tokio::test]
async fn inline_unknown_publish_recovers_after_terminal_reload_failure() {
    check_terminal_reload_failure(false).await;
}

#[tokio::test]
async fn receipt_replay_survives_another_requests_terminal_reload_failure() {
    check_terminal_reload_failure(true).await;
}

async fn check_terminal_reload_failure(include_replay: bool) {
    use std::sync::atomic::{AtomicBool, Ordering};

    let directory = tempdir().expect("directory");
    let namespace = NamespaceId::parse("inline-unknown").expect("namespace");
    let armed = Arc::new(AtomicBool::new(false));
    let unreadable = Arc::new(AtomicBool::new(false));
    let put_armed = Arc::clone(&armed);
    let put_unreadable = Arc::clone(&unreadable);
    let wal_prefix = wal_prefix(&namespace);
    let lost_ack = FailStore::matching(
        LocalFsStore::new(directory.path()).expect("store"),
        move |operation| {
            if operation.key().starts_with(&wal_prefix)
                && matches!(operation.kind(), OperationKind::Put { bytes, mode: PutMode::CreateIfAbsent } if is_publication(bytes))
                && put_armed.swap(false, Ordering::SeqCst)
            {
                put_unreadable.store(true, Ordering::SeqCst);
                true
            } else {
                false
            }
        },
        InjectedError::Transport("lost inline WAL acknowledgement".to_owned()),
    )
    .apply_then_fail();
    lost_ack.fail_all();
    let read_unreadable = Arc::clone(&unreadable);
    let store = Arc::new(FailStore::matching(
        lost_ack,
        move |operation| {
            read_unreadable.load(Ordering::SeqCst)
                && matches!(
                    operation.kind(),
                    OperationKind::Get { .. }
                        | OperationKind::GetWithMetadata
                        | OperationKind::Head
                )
        },
        InjectedError::Transport("recovery reads unavailable".to_owned()),
    ));
    store.fail_all();
    let writer = crate::LoonFs::builder_with_store(store.clone())
        .writer_id("inline-writer")
        .inline_content(InlineContentPolicy {
            inline_content_threshold_bytes: Some(4),
            inline_content_fold_at_bytes: 4,
            inline_content_tail_limit_bytes: 4,
            ..Default::default()
        })
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("writer");
    let namespace_reader = writer.namespace(&namespace);
    writer
        .create_namespace(&namespace, &loonfs_test_support::test_actor())
        .await
        .expect("namespace");
    let namespace_writer = writer.open_namespace(&namespace).expect("open namespace");
    let actor = loonfs_test_support::test_actor();
    let mut warmup_options = crate::CreateDirectoryOptions::default();
    warmup_options.commit.commit_id = Some(CommitId::parse("warmup").expect("commit"));
    let warmup = namespace_writer
        .create_directory_with_options("/warmup", &actor, &warmup_options)
        .await
        .expect("writer epoch");
    let permits = hold_every_fold_permit(&writer).await;
    armed.store(true, Ordering::SeqCst);
    let uncertain = put_options("uncertain");
    let first = namespace_writer.put_file_with_options("/file", b"four", &actor, &uncertain);
    let first_error = if include_replay {
        let slots = hold_every_publication_permit(&writer).await;
        let publisher = namespace_writer.session().publisher.clone();
        let (replayed, first, ()) = tokio::join!(
            namespace_writer.create_directory_with_options("/warmup", &actor, &warmup_options),
            first,
            async {
                timeout(
                    Duration::from_secs(10),
                    wait_for_queued_candidates(&publisher, 2),
                )
                .await
                .expect("both candidates queued");
                assert_eq!(publisher.lock_state().queue.len(), 1);
                drop(slots);
            }
        );
        assert_eq!(
            replayed.expect("independent receipt remains successful"),
            warmup
        );
        first.expect_err("WAL acknowledgement and recovery reads lost")
    } else {
        first
            .await
            .expect_err("WAL acknowledgement and recovery reads lost")
    };
    assert!(unreadable.load(Ordering::SeqCst));
    let durable =
        loonfs_core::cache::load_namespace_wal_tail_usage(store.inner().inner(), &namespace)
            .await
            .expect("raw durable tail");
    assert_eq!(durable.wal_tail_inline_bytes, 4);
    assert!(writer
        .mode
        .publisher
        .wal_tail_inline_bytes(&namespace)
        .await
        .is_some_and(|bytes| bytes >= durable.wal_tail_inline_bytes));
    assert_eq!(writer.mode.publisher.shared.admission.used_requests(), 0);
    assert_eq!(first_error.code(), ErrorCode::CommitOutcomeUnknown);

    unreadable.store(false, Ordering::SeqCst);
    let replay = namespace_writer
        .put_file_with_options(
            "/file",
            b"four",
            &loonfs_test_support::test_actor(),
            &put_options("uncertain"),
        )
        .await
        .expect("replay after recovery");
    assert_eq!(replay.committed_seq, durable.head_seq);
    assert_eq!(
        writer
            .mode
            .publisher
            .wal_tail_inline_bytes(&namespace)
            .await,
        Some(4)
    );
    namespace_writer
        .put_file_with_options(
            "/next",
            b"next",
            &loonfs_test_support::test_actor(),
            &put_options("next"),
        )
        .await
        .expect("full known tail stages next value");
    let recovered =
        loonfs_core::cache::load_namespace_wal_tail_usage(store.inner().inner(), &namespace)
            .await
            .expect("recovered usage");
    assert_eq!(recovered.wal_tail_inline_bytes, 4);
    assert_eq!(writer.mode.publisher.shared.admission.used_requests(), 0);
    assert_eq!(
        namespace_reader
            .read_file("/file")
            .await
            .expect("first bytes")
            .bytes,
        b"four"
    );
    assert_eq!(
        namespace_reader
            .read_file("/next")
            .await
            .expect("next bytes")
            .bytes,
        b"next"
    );
    drop(permits);
    writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_new_session_keeps_one_wal_object_budget_inline_until_it_observes_the_tail() {
    let (_directory, store, writer, namespace, namespace_writer) =
        writer_with_policy(InlineContentPolicy {
            inline_content_wal_object_budget_bytes: 4,
            inline_content_fold_at_bytes: 8,
            inline_content_tail_limit_bytes: 8,
            ..policy()
        })
        .await;
    for (path, bytes) in [("/four", b"four".as_slice()), ("/three", b"abc")] {
        namespace_writer
            .put_file_with_options(
                path,
                bytes,
                &loonfs_test_support::test_actor(),
                &put_options(&path[1..]),
            )
            .await
            .expect("fill tail below the fold trigger");
    }
    namespace_writer.close().await.expect("close session");
    let namespace_writer = writer.open_namespace(&namespace).expect("reopen namespace");
    let folds = hold_every_fold_permit(&writer).await;
    let slots = hold_every_publication_permit(&writer).await;
    store.reset();
    let actor = loonfs_test_support::test_actor();
    let (a, b, c) = (put_options("a"), put_options("b"), put_options("c"));
    let (first, second, third, ()) = tokio::join!(
        namespace_writer.put_file_with_options("/a", b"aaaa", &actor, &a),
        namespace_writer.put_file_with_options("/b", b"bbbb", &actor, &b),
        namespace_writer.put_file_with_options("/c", b"cccc", &actor, &c),
        async {
            let publisher = namespace_writer.session().publisher.clone();
            timeout(
                Duration::from_secs(10),
                wait_for_queued_candidates(&publisher, 3),
            )
            .await
            .expect("all three admitted before the first publish");
            drop(slots);
        }
    );
    for result in [first, second, third] {
        result.expect("put");
    }
    let inline_bytes: usize = written_records(&store)
        .await
        .iter()
        .flat_map(|record| &record.inline_content)
        .map(|value| value.bytes.len())
        .sum();
    assert_eq!(inline_bytes, 4);
    let staged = store
        .snapshot()
        .into_iter()
        .filter(|operation| {
            matches!(operation, RecordedOperation::Put { key, .. }
                if parse_object_key(key)
                    .is_some_and(|parsed| parsed.family() == DurableObjectFamily::ContentBlob))
        })
        .count();
    assert_eq!(staged, 2);
    drop(folds);
    writer.shutdown().await.expect("shutdown");
}

async fn writer_with_policy(
    policy: InlineContentPolicy,
) -> (
    tempfile::TempDir,
    Arc<RecordingStore<LocalFsStore>>,
    crate::LoonFs<crate::Writable>,
    NamespaceId,
    crate::Namespace<crate::Writable>,
) {
    writer_with_policy_and_byte_limit(policy, 8192).await
}

async fn writer_with_policy_and_byte_limit(
    policy: InlineContentPolicy,
    max_estimated_bytes_per_namespace: usize,
) -> (
    tempfile::TempDir,
    Arc<RecordingStore<LocalFsStore>>,
    crate::LoonFs<crate::Writable>,
    NamespaceId,
    crate::Namespace<crate::Writable>,
) {
    let directory = tempdir().expect("directory");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let writer = crate::LoonFs::builder_with_store(store.clone())
        .writer_id("inline-writer")
        .inline_content(policy)
        .publication_limits(crate::PublicationLimits {
            max_estimated_bytes_per_namespace: NonZeroUsize::new(max_estimated_bytes_per_namespace)
                .expect("namespace byte limit"),
            ..Default::default()
        })
        .min_publish_interval_ms(0)
        .monotonic_timer(Arc::new(ManualClock::new(0)))
        .build()
        .await
        .expect("writer");
    let namespace = NamespaceId::parse("inline").expect("namespace");
    writer
        .create_namespace(&namespace, &loonfs_test_support::test_actor())
        .await
        .expect("namespace");
    let namespace_writer = writer.open_namespace(&namespace).expect("open namespace");
    namespace_writer
        .create_directory("/warmup", &loonfs_test_support::test_actor())
        .await
        .expect("acquire writer epoch");
    store.reset();
    (directory, store, writer, namespace, namespace_writer)
}

fn put_options(id: &str) -> PutFileOptions {
    let mut options = PutFileOptions::default();
    options.commit.commit_id = Some(CommitId::parse(id).expect("commit id"));
    options
}

fn family_requests(store: &RecordingStore<LocalFsStore>, family: DurableObjectFamily) -> usize {
    store
        .snapshot()
        .iter()
        .filter(|operation| {
            parse_object_key(operation.key()).is_some_and(|key| key.family() == family)
        })
        .count()
}

async fn written_records(
    store: &RecordingStore<LocalFsStore>,
) -> Vec<loonfs_types::format::wal::WalCommitPayload> {
    let keys: Vec<_> = store
        .snapshot()
        .into_iter()
        .filter_map(|operation| match operation {
            RecordedOperation::Put { key, .. }
                if parse_object_key(&key)
                    .is_some_and(|parsed| parsed.family() == DurableObjectFamily::WalObject) =>
            {
                Some(key)
            }
            _ => None,
        })
        .collect();
    let mut records = Vec::new();
    for key in keys {
        let bytes = store.get(&key, None).await.expect("get WAL").expect("WAL");
        records.extend(
            decode_wal_object_envelope_zstd(&bytes)
                .expect("decode WAL")
                .payload()
                .records
                .clone(),
        );
    }
    records
}

#[tokio::test]
async fn small_writes_use_one_wal_put_and_retry_by_bytes() {
    let (_directory, store, writer, namespace, namespace_writer) =
        writer_with_policy(policy()).await;
    let namespace_reader = writer.namespace(&namespace);
    let options = put_options("small");
    let first = namespace_writer
        .put_file_with_options(
            "/file",
            b"same",
            &loonfs_test_support::test_actor(),
            &options,
        )
        .await
        .expect("put");
    assert_eq!(store.count(OperationClass::Put), 1);
    assert_eq!(family_requests(&store, DurableObjectFamily::ContentBlob), 0);
    assert_eq!(
        family_requests(&store, DurableObjectFamily::UploadSession),
        0
    );
    assert_eq!(
        namespace_reader
            .read_file("/file")
            .await
            .expect("read")
            .bytes,
        b"same"
    );
    assert_eq!(family_requests(&store, DurableObjectFamily::ContentBlob), 0);
    store.reset();
    assert_eq!(
        namespace_writer
            .put_file_with_options(
                "/file",
                b"same",
                &loonfs_test_support::test_actor(),
                &options
            )
            .await
            .expect("replay"),
        first
    );
    assert_eq!(store.count(OperationClass::Put), 0);
    assert_eq!(
        namespace_writer
            .put_file_with_options(
                "/file",
                b"diff",
                &loonfs_test_support::test_actor(),
                &options
            )
            .await
            .expect_err("conflict")
            .code(),
        loonfs_types::ErrorCode::CommitIdReuseConflict
    );
    assert_eq!(store.count(OperationClass::Put), 0);
    writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn disabled_and_above_threshold_writes_keep_uploaded_object_identity() {
    for options in [
        InlineContentPolicy {
            inline_content_threshold_bytes: None,
            ..Default::default()
        },
        policy(),
    ] {
        let (_directory, store, writer, _namespace, namespace) = writer_with_policy(options).await;
        let bytes: &[u8] = if writer
            .mode
            .bits
            .inline_content
            .inline_content_threshold_bytes
            .is_none()
        {
            b"tiny"
        } else {
            b"large"
        };
        namespace
            .put_file_with_options(
                "/file",
                bytes,
                &loonfs_test_support::test_actor(),
                &put_options("staged"),
            )
            .await
            .expect("put");
        assert!(family_requests(&store, DurableObjectFamily::ContentBlob) > 0);
        assert!(written_records(&store).await[0].inline_content.is_empty());
        assert_eq!(
            namespace
                .put_file_with_options(
                    "/file",
                    bytes,
                    &loonfs_test_support::test_actor(),
                    &put_options("staged")
                )
                .await
                .expect_err("fresh object conflicts")
                .code(),
            loonfs_types::ErrorCode::CommitIdReuseConflict
        );
        writer.shutdown().await.expect("shutdown");
    }
}

#[tokio::test]
async fn inline_preparation_makes_no_request_and_retained_values_replay() {
    let (_directory, store, writer, namespace, namespace_writer) =
        writer_with_policy(policy()).await;
    let namespace_reader = writer.namespace(&namespace);
    for bytes in [b"".as_slice(), b"same"] {
        store.reset();
        let prepared = namespace_writer
            .prepare_content(bytes)
            .await
            .expect("prepare");
        assert!(store.snapshot().is_empty());
        let path = if bytes.is_empty() { "/empty" } else { "/file" };
        let options = put_options(if bytes.is_empty() {
            "empty"
        } else {
            "prepared"
        });
        let first = namespace_writer
            .put_file_prepared_with_options(
                path,
                prepared.clone(),
                &loonfs_test_support::test_actor(),
                &options,
            )
            .await
            .expect("publish");
        assert_eq!(store.count(OperationClass::Put), 1);
        let reference = namespace_reader
            .read_file(path)
            .await
            .expect("read")
            .entry
            .content_ref()
            .expect("reference")
            .clone();
        assert_eq!(&reference, prepared.content_ref());
        store.reset();
        assert_eq!(
            namespace_writer
                .put_file_prepared_with_options(
                    path,
                    prepared,
                    &loonfs_test_support::test_actor(),
                    &options
                )
                .await
                .expect("replay"),
            first
        );
        assert_eq!(store.count(OperationClass::Put), 0);
    }
    writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn stream_preparation_preserves_chunks_across_the_threshold() {
    use futures::StreamExt;
    let (_directory, store, writer, namespace, namespace_writer) =
        writer_with_policy(policy()).await;
    let namespace_reader = writer.namespace(&namespace);
    for (index, chunks) in [
        vec![],
        vec![b"".as_slice(), b"sa", b"me"],
        vec![b"sa", b"mething longer"],
    ]
    .into_iter()
    .enumerate()
    {
        let expected = chunks.concat();
        let stream = futures::stream::iter(
            chunks
                .into_iter()
                .map(|chunk| Ok(Bytes::copy_from_slice(chunk))),
        )
        .boxed();
        store.reset();
        let prepared = namespace_writer
            .prepare_content_stream(stream)
            .await
            .expect("prepare stream");
        if expected.len() <= 4 {
            assert!(store.snapshot().is_empty());
        } else {
            assert!(family_requests(&store, DurableObjectFamily::ContentBlob) > 0);
        }
        let path = format!("/stream-{index}");
        let options = put_options(&format!("stream-{index}"));
        let first = namespace_writer
            .put_file_prepared_with_options(
                &path,
                prepared.clone(),
                &loonfs_test_support::test_actor(),
                &options,
            )
            .await
            .expect("publish");
        assert_eq!(
            namespace_reader.read_file(&path).await.expect("read").bytes,
            expected
        );
        store.reset();
        assert_eq!(
            namespace_writer
                .put_file_prepared_with_options(
                    &path,
                    prepared,
                    &loonfs_test_support::test_actor(),
                    &options
                )
                .await
                .expect("replay"),
            first
        );
        assert_eq!(store.count(OperationClass::Put), 0);
    }
    writer.shutdown().await.expect("shutdown");
}

fn put_operation(path: &str, prepared: &crate::publish::PreparedContent) -> FilesystemOperation {
    FilesystemOperation::PutFile {
        path: AbsolutePath::parse(path).expect("path"),
        content_ref: Some(prepared.content_ref().clone()),
        inline_content: None,
        behavior: DestinationBehavior::NoReplace,
        expected_inode_id: None,
        expected_revision_no: None,
    }
}

#[tokio::test]
async fn appends_fill_the_wal_object_budget_before_inline_values() {
    let (_directory, store, writer, namespace, namespace_writer) =
        writer_with_policy(InlineContentPolicy {
            inline_content_wal_object_budget_bytes: 8,
            ..policy()
        })
        .await;
    let prepared = namespace_writer
        .prepare_content(b"four")
        .await
        .expect("prepare");
    let request = CommitRequest {
        commit_id: CommitId::parse("put-then-append").expect("commit"),
        actor_id: loonfs_test_support::test_actor(),
        subject: None,
        message: None,
        preconditions: Vec::new(),
        operations: vec![
            put_operation("/file", &prepared),
            FilesystemOperation::AppendFile {
                path: AbsolutePath::parse("/file").expect("path"),
                inline_content: b" and six".to_vec(),
                expected_inode_id: None,
                expected_revision_no: None,
            },
        ],
    };
    namespace_writer
        .commit_prepared(request, vec![prepared])
        .await
        .expect("commit");
    let records = written_records(&store).await;
    let [piece] = records[0].inline_content.as_slice() else {
        panic!("expected the append's piece alone, got {records:?}");
    };
    assert_eq!(
        (piece.offset, piece.bytes.as_slice()),
        (4, b" and six".as_slice())
    );
    assert!(family_requests(&store, DurableObjectFamily::ContentBlob) > 0);
    for fold in [false, true] {
        if fold {
            writer
                .maintenance(loonfs_test_support::ids::writer_id("folder"))
                .fold_wal(&namespace)
                .await
                .expect("fold");
        }
        assert_eq!(
            writer
                .namespace(&namespace)
                .read_file("/file")
                .await
                .expect("read")
                .bytes,
            b"four and six"
        );
    }
    writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn tail_fallback_keeps_inline_identity_across_retries_and_a_fold() {
    let (_directory, store, writer, namespace, namespace_writer) =
        writer_with_policy(InlineContentPolicy {
            inline_content_fold_at_bytes: 4,
            inline_content_tail_limit_bytes: 4,
            ..policy()
        })
        .await;
    let namespace_reader = writer.namespace(&namespace);
    let fold_permits = hold_every_fold_permit(&writer).await;
    namespace_writer
        .put_file_with_options(
            "/first",
            b"full",
            &loonfs_test_support::test_actor(),
            &put_options("first"),
        )
        .await
        .expect("fill tail");
    let prepared = namespace_writer
        .prepare_content(b"next")
        .await
        .expect("prepare");
    let request = CommitRequest::single(
        CommitId::parse("fallback").expect("commit"),
        loonfs_test_support::test_actor(),
        None,
        put_operation("/fallback", &prepared),
    );
    let candidate = CommitCandidate::prepared(request, vec![prepared.clone()]);
    let fingerprint = candidate.semantic_identity(&namespace).expect("identity");
    store.reset();
    let first = namespace_writer
        .put_file_prepared_with_options(
            "/fallback",
            prepared.clone(),
            &loonfs_test_support::test_actor(),
            &put_options("fallback"),
        )
        .await
        .expect("fallback");
    let records = written_records(&store).await;
    assert_eq!(records.len(), 1);
    assert!(records[0].inline_content.is_empty());
    assert_eq!(records[0].semantic_commit_fingerprint, fingerprint);
    assert!(family_requests(&store, DurableObjectFamily::ContentBlob) > 0);
    let file = namespace_reader.read_file("/fallback").await.expect("read");
    assert_eq!(file.bytes, b"next");
    assert_ne!(
        file.entry.content_ref().expect("reference").content_id,
        prepared.content_ref().content_id
    );
    store.reset();
    assert_eq!(
        namespace_writer
            .put_file_prepared_with_options(
                "/fallback",
                prepared.clone(),
                &loonfs_test_support::test_actor(),
                &put_options("fallback")
            )
            .await
            .expect("replay staged fallback"),
        first
    );
    assert!(written_records(&store).await.is_empty());
    drop(fold_permits);
    namespace_writer.wait_for_fold().await.expect("fold");
    store.reset();
    assert_eq!(
        namespace_writer
            .put_file_prepared_with_options(
                "/fallback",
                prepared,
                &loonfs_test_support::test_actor(),
                &put_options("fallback")
            )
            .await
            .expect("replay inline after fold"),
        first
    );
    assert_eq!(store.count(OperationClass::Put), 0);
    writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn retained_receipts_answer_retries_before_fallback_when_content_writes_fail() {
    for bytes in [b"four".as_slice(), b"longer"] {
        let directory = tempdir().expect("directory");
        let failing = Arc::new(FailStore::new(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::content_blob(),
            OperationClass::Put,
            InjectedError::PermissionDenied("content writes refused".to_owned()),
        ));
        let store = Arc::new(RecordingStore::new(failing.clone(), KeyPredicate::any()));
        let writer = crate::LoonFs::builder_with_store(store.clone())
            .writer_id("inline-writer")
            .inline_content(InlineContentPolicy {
                inline_content_threshold_bytes: Some(8),
                inline_content_wal_object_budget_bytes: 4,
                inline_content_fold_at_bytes: 5,
                inline_content_tail_limit_bytes: 5,
            })
            .min_publish_interval_ms(0)
            .monotonic_timer(Arc::new(ManualClock::new(0)))
            .build()
            .await
            .expect("writer");
        let namespace = NamespaceId::parse("receipt").expect("namespace");
        writer
            .create_namespace(&namespace, &loonfs_test_support::test_actor())
            .await
            .expect("namespace");
        let namespace_writer = writer.open_namespace(&namespace).expect("open namespace");
        let prepared = namespace_writer
            .prepare_content(bytes)
            .await
            .expect("prepare");
        let original = namespace_writer
            .put_file_prepared_with_options(
                "/file",
                prepared.clone(),
                &loonfs_test_support::test_actor(),
                &put_options("retry"),
            )
            .await
            .expect("publish");
        assert_eq!(
            writer
                .mode
                .publisher
                .wal_tail_inline_bytes(&namespace)
                .await,
            Some(if bytes.len() == 4 { 4 } else { 0 })
        );
        failing.fail_all();
        store.reset();
        assert_eq!(
            namespace_writer
                .put_file_prepared_with_options(
                    "/file",
                    prepared,
                    &loonfs_test_support::test_actor(),
                    &put_options("retry")
                )
                .await
                .expect("receipt replays despite failed content writes"),
            original
        );
        let mut different = bytes.to_vec();
        different[0] = b'x';
        let error = namespace_writer
            .put_file_with_options(
                "/file",
                &different,
                &loonfs_test_support::test_actor(),
                &put_options("retry"),
            )
            .await
            .expect_err("receipt rejects different bytes before staging");
        assert_eq!(error.code(), ErrorCode::CommitIdReuseConflict);
        assert_eq!(store.count(OperationClass::Put), 0);
        assert_eq!(failing.attempts(), 0);
        writer.shutdown().await.expect("shutdown");
    }
}

#[tokio::test]
async fn full_queue_refuses_overflow_staging_without_store_writes() {
    let (_directory, store, writer, namespace, namespace_writer) =
        writer_with_policy(InlineContentPolicy {
            inline_content_wal_object_budget_bytes: 4,
            inline_content_threshold_bytes: Some(8),
            ..policy()
        })
        .await;
    let registry = writer.mode.publisher.clone();
    let mut permits = Vec::new();
    while let Ok(permit) = registry.shared.admission.acquire(&namespace, 0) {
        permits.push(permit);
    }
    store.reset();
    let error = namespace_writer
        .put_file_with_options(
            "/file",
            b"longer",
            &loonfs_test_support::test_actor(),
            &put_options("full-queue"),
        )
        .await
        .expect_err("queue full");
    assert_eq!(error.code(), ErrorCode::CommitQueueFull);
    assert_eq!(family_requests(&store, DurableObjectFamily::ContentBlob), 0);
    assert_eq!(
        family_requests(&store, DurableObjectFamily::UploadSession),
        0
    );
    drop(permits);
    writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn overflow_staging_keeps_bulk_commit_order_and_one_atomic_commit() {
    const VALUES: usize = 128;
    const VALUE_BYTES: usize = 16 * 1024;
    const ADMISSION_BYTES: usize = 256 * 1024;
    let (_directory, store, writer, namespace, namespace_writer) =
        writer_with_policy_and_byte_limit(
            InlineContentPolicy {
                inline_content_wal_object_budget_bytes: VALUE_BYTES,
                inline_content_threshold_bytes: Some(VALUE_BYTES),
                ..policy()
            },
            ADMISSION_BYTES,
        )
        .await;
    let namespace_reader = writer.namespace(&namespace);
    let payloads = (0..VALUES)
        .map(|index| Bytes::from(vec![u8::try_from(index).expect("byte index"); VALUE_BYTES]))
        .collect::<Vec<_>>();
    assert!(payloads.len() * VALUE_BYTES > ADMISSION_BYTES);
    let mut prepared = Vec::new();
    for bytes in &payloads {
        prepared.push(
            namespace_writer
                .prepare_content(bytes)
                .await
                .expect("prepare"),
        );
    }
    let request = CommitRequest {
        commit_id: CommitId::parse("batch").expect("commit"),
        actor_id: loonfs_test_support::test_actor(),
        subject: None,
        message: None,
        preconditions: Vec::new(),
        operations: prepared
            .iter()
            .enumerate()
            .map(|(index, value)| put_operation(&format!("/file-{index}"), value))
            .collect(),
    };
    let expected_inline_id = prepared[0].content_ref().content_id.clone();
    prepared.reverse();
    let candidate = CommitCandidate::prepared(request, prepared);
    let fingerprint = candidate.semantic_identity(&namespace).expect("identity");
    let published = namespace_writer
        .commit_candidate(candidate.clone())
        .await
        .expect("commit");
    let records = written_records(&store).await;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].semantic_commit_fingerprint, fingerprint);
    assert_eq!(records[0].inline_content.len(), 1);
    assert_eq!(records[0].inline_content[0].content_id, expected_inline_id);
    let staged_content_ids = store
        .snapshot()
        .into_iter()
        .filter_map(|operation| match operation {
            RecordedOperation::Put { key, .. } => parse_object_key(&key)
                .filter(|parsed| parsed.family() == DurableObjectFamily::ContentBlob)
                .and_then(|parsed| parsed.identifier().map(str::to_owned)),
            _ => None,
        })
        .collect::<Vec<_>>();
    let operation_content_ids = records[0]
        .deltas
        .iter()
        .filter_map(|delta| match &delta.delta {
            loonfs_types::format::wal::WalDelta::AppendFileRevision { content_ref, .. } => {
                Some(content_ref.content_id.as_str())
            }
            _ => None,
        })
        .skip(1)
        .collect::<Vec<_>>();
    assert_eq!(staged_content_ids, operation_content_ids);
    for index in [0, VALUES - 1] {
        assert_eq!(
            namespace_reader
                .read_file(&format!("/file-{index}"))
                .await
                .expect("read")
                .bytes,
            payloads[index]
        );
    }
    store.reset();
    assert_eq!(
        namespace_writer
            .commit_candidate(candidate)
            .await
            .expect("retained receipt"),
        published
    );
    assert_eq!(store.count(OperationClass::Put), 0);
    writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn queued_writes_share_tail_reservations_and_split_at_the_wal_object_budget() {
    for limited_tail in [false, true] {
        let mut options = InlineContentPolicy {
            inline_content_wal_object_budget_bytes: 4,
            ..policy()
        };
        if limited_tail {
            options.inline_content_fold_at_bytes = 5;
            options.inline_content_tail_limit_bytes = 5;
        }
        let (_directory, store, writer, _namespace, namespace) = writer_with_policy(options).await;
        let slots = hold_every_publication_permit(&writer).await;
        let publisher = namespace.session().publisher.clone();
        let actor = loonfs_test_support::test_actor();
        let (one, two) = (put_options("one"), put_options("two"));
        let (first, second, ()) = tokio::join!(
            namespace.put_file_with_options("/one", b"one", &actor, &one),
            namespace.put_file_with_options("/two", b"two", &actor, &two),
            async {
                timeout(Duration::from_secs(10), async {
                    while queued_candidates(&publisher.lock_state()) < 2 {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("both candidates admitted while publication is blocked");
                assert_eq!(
                    publisher.lock_state().queue.len(),
                    if limited_tail { 1 } else { 2 }
                );
                drop(slots);
            }
        );
        first.expect("first");
        second.expect("second");
        let records = written_records(&store).await;
        assert_eq!(records.len(), 2);
        let inline_bytes: usize = records
            .iter()
            .flat_map(|record| &record.inline_content)
            .map(|value| value.bytes.len())
            .sum();
        assert_eq!(inline_bytes, if limited_tail { 3 } else { 6 });
        if limited_tail {
            store.reset();
            namespace
                .put_file_with_options(
                    "/warmup",
                    b"x",
                    &loonfs_test_support::test_actor(),
                    &put_options("fails"),
                )
                .await
                .expect_err("directory cannot be replaced by a file");
            assert_eq!(store.count(OperationClass::Put), 0);
            let folds = hold_every_fold_permit(&writer).await;
            namespace
                .put_file_with_options(
                    "/after-failure",
                    b"ok",
                    &loonfs_test_support::test_actor(),
                    &put_options("after-failure"),
                )
                .await
                .expect("reservation released on failure");
            assert_eq!(store.count(OperationClass::Put), 1);
            assert_eq!(written_records(&store).await[0].inline_content.len(), 1);
            drop(folds);
        } else {
            assert_eq!(store.count(OperationClass::Put), 2);
            assert_eq!(family_requests(&store, DurableObjectFamily::ContentBlob), 0);
        }
        writer.shutdown().await.expect("shutdown");
    }
}

#[tokio::test]
async fn repeated_projection_invalidation_does_not_repeat_the_tail_limit_overshoot() {
    let (_directory, _store, writer, namespace, namespace_writer) =
        writer_with_policy(InlineContentPolicy {
            inline_content_fold_at_bytes: 4,
            inline_content_tail_limit_bytes: 4,
            ..policy()
        })
        .await;
    let folds = hold_every_fold_permit(&writer).await;
    let registry = writer.mode.publisher.clone();
    for index in 0..4 {
        writer.invalidate_namespace(&namespace);
        namespace_writer
            .put_file_with_options(
                &format!("/file-{index}"),
                b"full",
                &loonfs_test_support::test_actor(),
                &put_options(&format!("write-{index}")),
            )
            .await
            .expect("publish");
    }
    assert_eq!(registry.wal_tail_inline_bytes(&namespace).await, Some(4));
    drop(folds);
    writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn fold_completion_reports_only_inline_bytes_published_since_it_began() {
    let directory = tempdir().expect("directory");
    let namespace = NamespaceId::parse("inline-fold-race").expect("namespace");
    let store = Arc::new(blocking_fold_store(
        LocalFsStore::new(directory.path()).expect("store"),
        metadata_manifest_prefix(&namespace),
    ));
    let writer = crate::LoonFs::builder_with_store(store.clone())
        .writer_id("inline-writer")
        .inline_content(InlineContentPolicy {
            inline_content_threshold_bytes: Some(4),
            inline_content_fold_at_bytes: 4,
            inline_content_tail_limit_bytes: 8,
            ..Default::default()
        })
        .min_publish_interval_ms(0)
        .monotonic_timer(Arc::new(ManualClock::new(0)))
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace, &loonfs_test_support::test_actor())
        .await
        .expect("namespace");
    let namespace_writer = writer.open_namespace(&namespace).expect("open namespace");
    namespace_writer
        .create_directory("/warmup", &loonfs_test_support::test_actor())
        .await
        .expect("acquire writer epoch");

    store.block_next();
    namespace_writer
        .put_file_with_options(
            "/first",
            b"four",
            &loonfs_test_support::test_actor(),
            &put_options("first"),
        )
        .await
        .expect("start fold");
    store.wait_until_blocked().await;
    namespace_writer
        .put_file_with_options(
            "/during",
            b"next",
            &loonfs_test_support::test_actor(),
            &put_options("during"),
        )
        .await
        .expect("publish during fold");
    store.release();
    namespace_writer.wait_for_fold().await.expect("finish fold");
    assert_eq!(
        writer
            .mode
            .publisher
            .wal_tail_inline_bytes(&namespace)
            .await,
        Some(4)
    );

    let fold_permits = hold_every_fold_permit(&writer).await;
    commit_two_values(&writer, &namespace, "after-fold").await;
    let usage = loonfs_core::cache::load_namespace_wal_tail_usage(store.as_ref(), &namespace)
        .await
        .expect("tail usage");
    assert_eq!(usage.wal_tail_inline_bytes, 8);
    assert_eq!(
        writer
            .mode
            .publisher
            .wal_tail_inline_bytes(&namespace)
            .await,
        Some(8)
    );
    commit_two_values(&writer, &namespace, "recovered").await;
    let usage = loonfs_core::cache::load_namespace_wal_tail_usage(store.as_ref(), &namespace)
        .await
        .expect("tail usage after recovery");
    assert_eq!(usage.wal_tail_inline_bytes, 8);

    drop(fold_permits);
    writer.shutdown().await.expect("shutdown");
}

async fn commit_two_values(
    writer: &crate::LoonFs<crate::Writable>,
    namespace: &NamespaceId,
    label: &str,
) {
    let namespace_writer = writer.open_namespace(namespace).expect("open namespace");
    let prepared = vec![
        namespace_writer
            .prepare_content(b"more")
            .await
            .expect("prepare first value"),
        namespace_writer
            .prepare_content(b"last")
            .await
            .expect("prepare second value"),
    ];
    let request = CommitRequest {
        commit_id: CommitId::parse(label).expect("commit"),
        actor_id: loonfs_test_support::test_actor(),
        subject: None,
        message: None,
        operations: vec![
            put_operation(&format!("/{label}-1"), &prepared[0]),
            put_operation(&format!("/{label}-2"), &prepared[1]),
        ],
        preconditions: Vec::new(),
    };
    namespace_writer
        .commit_candidate(CommitCandidate::prepared(request, prepared))
        .await
        .expect("publish two values");
}

#[tokio::test]
async fn inline_bytes_make_automatic_and_explicit_folds_due_before_wal_object_count() {
    for mode in ["automatic", "explicit"] {
        // Every fold takes a fold permit, so holding them would stop an
        // explicit fold too. Only the automatic case folds at four bytes on
        // the session's own path and holds the permits until it is checked.
        let automatic = mode == "automatic";
        let (_directory, store, writer, namespace, namespace_writer) =
            writer_with_policy(InlineContentPolicy {
                inline_content_fold_at_bytes: if automatic {
                    4
                } else {
                    InlineContentPolicy::default().inline_content_fold_at_bytes
                },
                ..policy()
            })
            .await;
        let namespace_reader = writer.namespace(&namespace);
        let permits = if automatic {
            Some(hold_every_fold_permit(&writer).await)
        } else {
            None
        };
        namespace_writer
            .put_file_with_options(
                "/file",
                b"four",
                &loonfs_test_support::test_actor(),
                &put_options("fold"),
            )
            .await
            .expect("put");
        let usage = loonfs_core::cache::load_namespace_wal_tail_usage(store.as_ref(), &namespace)
            .await
            .expect("usage");
        assert_eq!(usage.wal_tail_inline_bytes, 4);
        assert!(usage.wal_tail_objects < FOLD_AT_WAL_OBJECTS);
        let maintenance = writer.maintenance(loonfs_test_support::ids::writer_id("maintenance"));
        let options = MetadataMaintenanceOptions {
            inline_content_fold_at_bytes: NonZeroUsize::new(4).expect("threshold"),
            ..Default::default()
        };
        let independent = crate::LoonFs::builder_with_store(store.clone())
            .writer_id("independent")
            .build()
            .await
            .expect("independent maintenance")
            .maintenance(loonfs_test_support::ids::writer_id("independent"));
        store.reset();
        let step = independent
            .maintain_metadata_with_options(&namespace, &options)
            .await
            .expect("maintenance without a writer");
        assert_eq!(step.wal_fold, crate::WalFoldStepOutcome::NotNeeded);
        assert_eq!(
            family_requests(&store, DurableObjectFamily::WalObject),
            7,
            "one windowed WAL discovery without a byte-count replay"
        );
        if mode == "explicit" {
            let step = maintenance
                .maintain_metadata_with_options(&namespace, &options)
                .await
                .expect("explicit fold");
            assert!(matches!(
                step.wal_fold,
                crate::WalFoldStepOutcome::Folded { .. }
            ));
            // The fold does not lower the writer's count; with nothing left
            // unfolded, the next pass does not consult it.
            let again = maintenance
                .maintain_metadata_with_options(&namespace, &options)
                .await
                .expect("pass after the fold");
            assert_eq!(again.wal_fold, crate::WalFoldStepOutcome::NotNeeded);
        }
        drop(permits);
        namespace_writer
            .wait_for_fold()
            .await
            .expect("automatic fold settles");
        let usage = loonfs_core::cache::load_namespace_wal_tail_usage(store.as_ref(), &namespace)
            .await
            .expect("usage after fold");
        assert_eq!(usage.wal_tail_objects, 0);
        assert_eq!(usage.wal_tail_inline_bytes, 0);
        assert_eq!(
            namespace_reader
                .read_file("/file")
                .await
                .expect("read folded content")
                .bytes,
            b"four"
        );
        writer.shutdown().await.expect("shutdown");
    }
}

#[tokio::test]
async fn invalid_inline_policy_is_rejected_before_store_access() {
    use loonfs_types::format::wal::{
        MAX_WAL_INLINE_CONTENT_BYTES, MAX_WAL_OBJECT_INLINE_CONTENT_BYTES,
    };
    let directory = tempdir().expect("directory");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    for options in [
        InlineContentPolicy {
            inline_content_threshold_bytes: Some(MAX_WAL_INLINE_CONTENT_BYTES + 1),
            ..policy()
        },
        InlineContentPolicy {
            inline_content_wal_object_budget_bytes: MAX_WAL_OBJECT_INLINE_CONTENT_BYTES + 1,
            ..policy()
        },
        InlineContentPolicy {
            inline_content_fold_at_bytes: 33 * 1024 * 1024,
            ..policy()
        },
        InlineContentPolicy {
            inline_content_wal_object_budget_bytes: 0,
            ..policy()
        },
        InlineContentPolicy {
            inline_content_fold_at_bytes: 0,
            ..policy()
        },
        InlineContentPolicy {
            inline_content_tail_limit_bytes: 0,
            ..policy()
        },
    ] {
        let error = crate::LoonFs::builder_with_store(store.clone())
            .writer_id("writer")
            .inline_content(options)
            .build()
            .await
            .err()
            .expect("invalid policy");
        assert!(matches!(error, Error::Config(_)));
        assert!(store.snapshot().is_empty());
    }
    crate::LoonFs::builder_with_store(store)
        .writer_id("writer")
        .inline_content(InlineContentPolicy {
            inline_content_threshold_bytes: Some(0),
            ..policy()
        })
        .build()
        .await
        .expect("zero threshold permits empty content");
}

#[tokio::test]
async fn a_delayed_fold_callback_preserves_a_freshly_observed_tail() {
    check_delayed_fold_callback(MetadataCache::default()).await;
}

#[tokio::test]
async fn a_delayed_fold_callback_preserves_an_uncached_tail() {
    check_delayed_fold_callback(
        MetadataCache::builder()
            .max_segment_bytes(0)
            .max_head_state_bytes(0)
            .build(),
    )
    .await;
}

/// A publish can observe the folded manifest before the fold's completion
/// callback runs. The callback must leave that fresh count alone.
async fn check_delayed_fold_callback(cache: MetadataCache) {
    let directory = tempdir().expect("directory");
    let namespace = NamespaceId::parse("fold-accounting").expect("namespace");
    let store = Arc::new(BlockingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::exact(loonfs_objectstore::keys::hint(&namespace)),
        OperationClass::CompareAndSwap,
    ));
    // The session folds only a full tail, so nothing but the explicit fold
    // runs until the last commit fills it.
    let writer = crate::LoonFs::builder_with_store(store.clone())
        .writer_id("inline-writer")
        .metadata_cache(cache)
        .inline_content(InlineContentPolicy {
            inline_content_threshold_bytes: Some(4),
            inline_content_fold_at_bytes: 8,
            inline_content_tail_limit_bytes: 8,
            ..Default::default()
        })
        .min_publish_interval_ms(0)
        .monotonic_timer(Arc::new(ManualClock::new(0)))
        .build()
        .await
        .expect("writer");
    let namespace_reader = writer.namespace(&namespace);
    writer
        .create_namespace(&namespace, &loonfs_test_support::test_actor())
        .await
        .expect("namespace");
    let namespace_writer = writer.open_namespace(&namespace).expect("open namespace");
    namespace_writer
        .put_file_with_options(
            "/first",
            b"four",
            &loonfs_test_support::test_actor(),
            &put_options("first"),
        )
        .await
        .expect("first inline commit");
    let maintenance = writer.maintenance(loonfs_test_support::ids::writer_id("maintenance"));
    store.block_next();
    let fold = maintenance.fold_wal(&namespace);
    let publish_after_fold = async {
        timeout(Duration::from_secs(10), store.wait_until_blocked())
            .await
            .expect("fold reached its hint update");
        // The immutable manifest is durable; its best-effort hint update waits.
        let usage = loonfs_core::cache::load_namespace_wal_tail_usage(store.as_ref(), &namespace)
            .await
            .expect("observe the completed fold");
        assert_eq!(usage.wal_tail_inline_bytes, 0);
        writer.invalidate_namespace(&namespace);
        namespace_writer
            .put_file_with_options(
                "/during",
                b"next",
                &loonfs_test_support::test_actor(),
                &put_options("during"),
            )
            .await
            .expect("publish against the new manifest");
        assert_eq!(
            writer
                .mode
                .publisher
                .wal_tail_inline_bytes(&namespace)
                .await,
            Some(4)
        );
        store.release();
    };
    let (folded, ()) = tokio::join!(fold, publish_after_fold);
    folded.expect("delayed fold completion");
    assert_eq!(
        writer
            .mode
            .publisher
            .wal_tail_inline_bytes(&namespace)
            .await,
        Some(4)
    );
    let prepared = vec![
        namespace_writer
            .prepare_content(b"more")
            .await
            .expect("prepare"),
        namespace_writer
            .prepare_content(b"last")
            .await
            .expect("prepare"),
    ];
    let request = CommitRequest {
        commit_id: CommitId::parse("after-fold").expect("commit"),
        actor_id: loonfs_test_support::test_actor(),
        subject: None,
        message: None,
        preconditions: Vec::new(),
        operations: vec![
            put_operation("/more", &prepared[0]),
            put_operation("/last", &prepared[1]),
        ],
    };
    let permits = hold_every_fold_permit(&writer).await;
    namespace_writer
        .commit_candidate(CommitCandidate::prepared(request, prepared))
        .await
        .expect("publish with only four inline bytes available");
    let usage = loonfs_core::cache::load_namespace_wal_tail_usage(store.as_ref(), &namespace)
        .await
        .expect("actual tail usage");
    assert_eq!(usage.wal_tail_inline_bytes, 8);
    for (path, bytes) in [
        ("/first", b"four"),
        ("/during", b"next"),
        ("/more", b"more"),
        ("/last", b"last"),
    ] {
        assert_eq!(
            namespace_reader.read_file(path).await.expect("read").bytes,
            bytes
        );
    }
    drop(permits);
    writer.shutdown().await.expect("shutdown");
}
