//! Inline writer preparation, publication, fallback, and maintenance contracts.

use super::*;
use crate::{InlineContentOptions, MetadataMaintenanceOptions, PutFileOptions};
use loonfs_api::wire::wal::decode_wal_segment_envelope_zstd;
use loonfs_objectstore::layout::{parse_object_key, DurableObjectFamily};
use loonfs_test_support::clock::ManualClock;
use loonfs_test_support::stores::RecordedOperation;

fn policy() -> InlineContentOptions {
    InlineContentOptions {
        inline_content_threshold_bytes: Some(4),
        ..Default::default()
    }
}

async fn writer_with_policy(
    policy: InlineContentOptions,
) -> (
    tempfile::TempDir,
    Arc<RecordingStore<LocalFsStore>>,
    crate::FsWriter,
    NamespaceId,
) {
    writer_with_policy_and_byte_limit(policy, 8192).await
}

async fn writer_with_policy_and_byte_limit(
    policy: InlineContentOptions,
    max_estimated_bytes_per_namespace: usize,
) -> (
    tempfile::TempDir,
    Arc<RecordingStore<LocalFsStore>>,
    crate::FsWriter,
    NamespaceId,
) {
    let directory = tempdir().expect("directory");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let writer = crate::FsWriter::builder_with_store(store.clone())
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
        .create_namespace(
            &namespace,
            CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("namespace");
    writer
        .create_directory(
            &namespace,
            "/warmup",
            crate::CreateDirectoryOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("acquire writer epoch");
    store.reset();
    (directory, store, writer, namespace)
}

fn put_options(id: &str) -> PutFileOptions {
    let mut options = PutFileOptions::new(loonfs_test_support::test_actor());
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
) -> Vec<loonfs_api::wire::wal::WalCommitPayload> {
    let keys: Vec<_> = store
        .snapshot()
        .into_iter()
        .filter_map(|operation| match operation {
            RecordedOperation::Put { key, .. }
                if parse_object_key(&key)
                    .is_some_and(|parsed| parsed.family() == DurableObjectFamily::WalSegment) =>
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
            decode_wal_segment_envelope_zstd(&bytes)
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
    let (_directory, store, writer, namespace) = writer_with_policy(policy()).await;
    let options = put_options("small");
    let first = writer
        .put_file_bytes(&namespace, "/file", b"same", options.clone())
        .await
        .expect("put");
    assert_eq!(store.count(OperationClass::Put), 1);
    assert_eq!(family_requests(&store, DurableObjectFamily::ContentBlob), 0);
    assert_eq!(
        family_requests(&store, DurableObjectFamily::UploadSession),
        0
    );
    assert_eq!(
        writer
            .reader()
            .get_file_bytes(&namespace, "/file")
            .await
            .expect("read")
            .bytes,
        b"same"
    );
    assert_eq!(family_requests(&store, DurableObjectFamily::ContentBlob), 0);
    store.reset();
    assert_eq!(
        writer
            .put_file_bytes(&namespace, "/file", b"same", options.clone())
            .await
            .expect("replay"),
        first
    );
    assert_eq!(store.count(OperationClass::Put), 0);
    assert_eq!(
        writer
            .put_file_bytes(&namespace, "/file", b"diff", options)
            .await
            .expect_err("conflict")
            .code(),
        loonfs_api::ErrorCode::CommitIdReuseConflict
    );
    assert_eq!(store.count(OperationClass::Put), 0);
    writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn disabled_and_above_threshold_writes_keep_uploaded_object_identity() {
    for options in [
        InlineContentOptions {
            inline_content_threshold_bytes: None,
            ..Default::default()
        },
        policy(),
    ] {
        let (_directory, store, writer, namespace) = writer_with_policy(options).await;
        let bytes: &[u8] = if writer
            .bits
            .inline_content
            .inline_content_threshold_bytes
            .is_none()
        {
            b"tiny"
        } else {
            b"large"
        };
        writer
            .put_file_bytes(&namespace, "/file", bytes, put_options("staged"))
            .await
            .expect("put");
        assert!(family_requests(&store, DurableObjectFamily::ContentBlob) > 0);
        assert!(written_records(&store).await[0].inline_content.is_empty());
        assert_eq!(
            writer
                .put_file_bytes(&namespace, "/file", bytes, put_options("staged"))
                .await
                .expect_err("fresh object conflicts")
                .code(),
            loonfs_api::ErrorCode::CommitIdReuseConflict
        );
        writer.shutdown().await.expect("shutdown");
    }
}

#[tokio::test]
async fn inline_preparation_makes_no_request_and_retained_values_replay() {
    let (_directory, store, writer, namespace) = writer_with_policy(policy()).await;
    for bytes in [b"".as_slice(), b"same"] {
        store.reset();
        let prepared = writer
            .prepare_file_bytes(&namespace, bytes)
            .await
            .expect("prepare");
        assert!(store.snapshot().is_empty());
        let path = if bytes.is_empty() { "/empty" } else { "/file" };
        let options = put_options(if bytes.is_empty() {
            "empty"
        } else {
            "prepared"
        });
        let first = writer
            .put_file_prepared(&namespace, path, prepared.clone(), options.clone())
            .await
            .expect("publish");
        assert_eq!(store.count(OperationClass::Put), 1);
        let reference = writer
            .reader()
            .get_file_bytes(&namespace, path)
            .await
            .expect("read")
            .entry
            .content_ref()
            .expect("reference")
            .clone();
        assert_eq!(&reference, prepared.content_ref());
        store.reset();
        assert_eq!(
            writer
                .put_file_prepared(&namespace, path, prepared, options)
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
    let (_directory, store, writer, namespace) = writer_with_policy(policy()).await;
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
        let prepared = writer
            .prepare_file_stream(&namespace, stream)
            .await
            .expect("prepare stream");
        if expected.len() <= 4 {
            assert!(store.snapshot().is_empty());
        } else {
            assert!(family_requests(&store, DurableObjectFamily::ContentBlob) > 0);
        }
        let path = format!("/stream-{index}");
        let options = put_options(&format!("stream-{index}"));
        let first = writer
            .put_file_prepared(&namespace, &path, prepared.clone(), options.clone())
            .await
            .expect("publish");
        assert_eq!(
            writer
                .reader()
                .get_file_bytes(&namespace, &path)
                .await
                .expect("read")
                .bytes,
            expected
        );
        store.reset();
        assert_eq!(
            writer
                .put_file_prepared(&namespace, &path, prepared, options)
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
async fn tail_fallback_keeps_inline_identity_across_retries_and_a_fold() {
    let (_directory, store, writer, namespace) = writer_with_policy(InlineContentOptions {
        inline_content_fold_at_bytes: 4,
        inline_content_tail_limit_bytes: 4,
        ..policy()
    })
    .await;
    let fold_permits = writer
        .bits
        .wal_fold_permits
        .acquire_many(crate::DEFAULT_MAX_CONCURRENT_FOLDS as u32)
        .await
        .expect("hold folds");
    writer
        .put_file_bytes(&namespace, "/first", b"full", put_options("first"))
        .await
        .expect("fill tail");
    let prepared = writer
        .prepare_file_bytes(&namespace, b"next")
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
    let first = writer
        .put_file_prepared(
            &namespace,
            "/fallback",
            prepared.clone(),
            put_options("fallback"),
        )
        .await
        .expect("fallback");
    let records = written_records(&store).await;
    assert_eq!(records.len(), 1);
    assert!(records[0].inline_content.is_empty());
    assert_eq!(records[0].semantic_commit_fingerprint, fingerprint);
    assert!(family_requests(&store, DurableObjectFamily::ContentBlob) > 0);
    let file = writer
        .reader()
        .get_file_bytes(&namespace, "/fallback")
        .await
        .expect("read");
    assert_eq!(file.bytes, b"next");
    assert_ne!(
        file.entry.content_ref().expect("reference").content_id,
        prepared.content_ref().content_id
    );
    store.reset();
    assert_eq!(
        writer
            .put_file_prepared(
                &namespace,
                "/fallback",
                prepared.clone(),
                put_options("fallback")
            )
            .await
            .expect("replay staged fallback"),
        first
    );
    assert!(written_records(&store).await.is_empty());
    drop(fold_permits);
    writer.wait_for_fold(&namespace).await.expect("fold");
    store.reset();
    assert_eq!(
        writer
            .put_file_prepared(&namespace, "/fallback", prepared, put_options("fallback"))
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
        let writer = crate::FsWriter::builder_with_store(store.clone())
            .writer_id("inline-writer")
            .inline_content(InlineContentOptions {
                inline_content_threshold_bytes: Some(8),
                inline_content_segment_budget_bytes: 4,
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
            .create_namespace(
                &namespace,
                CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect("namespace");
        let prepared = writer
            .prepare_file_bytes(&namespace, bytes)
            .await
            .expect("prepare");
        let original = writer
            .put_file_prepared(&namespace, "/file", prepared.clone(), put_options("retry"))
            .await
            .expect("publish");
        assert_eq!(
            writer.publisher().wal_tail_inline_bytes(&namespace).await,
            Some(if bytes.len() == 4 { 4 } else { 0 })
        );
        failing.fail_all();
        store.reset();
        assert_eq!(
            writer
                .put_file_prepared(&namespace, "/file", prepared, put_options("retry"))
                .await
                .expect("receipt replays despite failed content writes"),
            original
        );
        let mut different = bytes.to_vec();
        different[0] = b'x';
        let error = writer
            .put_file_bytes(&namespace, "/file", &different, put_options("retry"))
            .await
            .expect_err("receipt rejects different bytes before staging");
        assert_eq!(error.code(), ErrorCode::CommitIdReuseConflict);
        assert_eq!(store.count(OperationClass::Put), 0);
        assert_eq!(failing.attempts(), 0);
        writer.shutdown().await.expect("shutdown");
    }
}

#[tokio::test]
async fn full_queue_refuses_segment_fallback_without_store_writes() {
    let (_directory, store, writer, namespace) = writer_with_policy(InlineContentOptions {
        inline_content_segment_budget_bytes: 4,
        inline_content_threshold_bytes: Some(8),
        ..policy()
    })
    .await;
    let registry = writer.publisher();
    let mut permits = Vec::new();
    while let Ok(permit) = registry.shared.admission.acquire(&namespace, 0) {
        permits.push(permit);
    }
    store.reset();
    let error = writer
        .put_file_bytes(&namespace, "/file", b"longer", put_options("full-queue"))
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
async fn segment_fallback_keeps_bulk_commit_order_and_one_atomic_commit() {
    const VALUES: usize = 128;
    const VALUE_BYTES: usize = 16 * 1024;
    const ADMISSION_BYTES: usize = 256 * 1024;
    let (_directory, store, writer, namespace) = writer_with_policy_and_byte_limit(
        InlineContentOptions {
            inline_content_segment_budget_bytes: VALUE_BYTES,
            inline_content_threshold_bytes: Some(VALUE_BYTES),
            ..policy()
        },
        ADMISSION_BYTES,
    )
    .await;
    let payloads = (0..VALUES)
        .map(|index| Bytes::from(vec![u8::try_from(index).expect("byte index"); VALUE_BYTES]))
        .collect::<Vec<_>>();
    assert!(payloads.len() * VALUE_BYTES > ADMISSION_BYTES);
    let mut prepared = Vec::new();
    for bytes in &payloads {
        prepared.push(
            writer
                .prepare_file_bytes(&namespace, bytes)
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
    let published = writer
        .commit_candidate(&namespace, candidate.clone())
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
            loonfs_api::wire::wal::WalDelta::AppendFileRevision { content_ref, .. } => {
                Some(content_ref.content_id.as_str())
            }
            _ => None,
        })
        .skip(1)
        .collect::<Vec<_>>();
    assert_eq!(staged_content_ids, operation_content_ids);
    for index in [0, VALUES - 1] {
        assert_eq!(
            writer
                .reader()
                .get_file_bytes(&namespace, &format!("/file-{index}"))
                .await
                .expect("read")
                .bytes,
            payloads[index]
        );
    }
    store.reset();
    assert_eq!(
        writer
            .commit_candidate(&namespace, candidate)
            .await
            .expect("retained receipt"),
        published
    );
    assert_eq!(store.count(OperationClass::Put), 0);
    writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn queued_writes_share_tail_reservations_and_split_at_the_segment_budget() {
    for limited_tail in [false, true] {
        let mut options = InlineContentOptions {
            inline_content_segment_budget_bytes: 4,
            ..policy()
        };
        if limited_tail {
            options.inline_content_fold_at_bytes = 5;
            options.inline_content_tail_limit_bytes = 5;
        }
        let (_directory, store, writer, namespace) = writer_with_policy(options).await;
        let registry = writer.publisher();
        let slots = registry
            .shared
            .admission
            .publications
            .acquire_many(8)
            .await
            .expect("hold publications");
        let publisher = registry.test_publisher_for(&namespace).expect("publisher");
        let (first, second, ()) = tokio::join!(
            writer.put_file_bytes(&namespace, "/one", b"one", put_options("one")),
            writer.put_file_bytes(&namespace, "/two", b"two", put_options("two")),
            async {
                timeout(Duration::from_secs(10), async {
                    while publisher.queued_commits() < 2 {
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
            writer
                .put_file_bytes(&namespace, "/warmup", b"x", put_options("fails"))
                .await
                .expect_err("directory cannot be replaced by a file");
            assert_eq!(store.count(OperationClass::Put), 0);
            let folds = writer
                .bits
                .wal_fold_permits
                .acquire_many(crate::DEFAULT_MAX_CONCURRENT_FOLDS as u32)
                .await
                .expect("hold folds");
            writer
                .put_file_bytes(
                    &namespace,
                    "/after-failure",
                    b"ok",
                    put_options("after-failure"),
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
    let (_directory, _store, writer, namespace) = writer_with_policy(InlineContentOptions {
        inline_content_fold_at_bytes: 4,
        inline_content_tail_limit_bytes: 4,
        ..policy()
    })
    .await;
    let folds = writer
        .bits
        .wal_fold_permits
        .acquire_many(crate::DEFAULT_MAX_CONCURRENT_FOLDS as u32)
        .await
        .expect("hold folds");
    let registry = writer.publisher();
    for index in 0..4 {
        writer.invalidate_namespace(&namespace);
        writer
            .put_file_bytes(
                &namespace,
                &format!("/file-{index}"),
                b"full",
                put_options(&format!("write-{index}")),
            )
            .await
            .expect("publish");
    }
    assert_eq!(registry.wal_tail_inline_bytes(&namespace).await, Some(4));
    drop(folds);
    writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn fold_completion_keeps_inline_bytes_published_while_it_ran() {
    let directory = tempdir().expect("directory");
    let namespace = NamespaceId::parse("inline-fold-race").expect("namespace");
    let store = Arc::new(blocking_fold_store(
        LocalFsStore::new(directory.path()).expect("store"),
        metadata_manifest_prefix(&namespace),
    ));
    let writer = crate::FsWriter::builder_with_store(store.clone())
        .writer_id("inline-writer")
        .inline_content(InlineContentOptions {
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
        .create_namespace(
            &namespace,
            CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("namespace");
    writer
        .create_directory(
            &namespace,
            "/warmup",
            crate::CreateDirectoryOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("acquire writer epoch");

    store.block_next();
    writer
        .put_file_bytes(&namespace, "/first", b"four", put_options("first"))
        .await
        .expect("start fold");
    store.wait_until_blocked().await;
    writer
        .put_file_bytes(&namespace, "/during", b"next", put_options("during"))
        .await
        .expect("publish during fold");
    store.release();
    writer.wait_for_fold(&namespace).await.expect("finish fold");
    assert_eq!(
        writer.publisher().wal_tail_inline_bytes(&namespace).await,
        Some(4)
    );

    let fold_permits = writer
        .bits
        .wal_fold_permits
        .acquire_many(crate::DEFAULT_MAX_CONCURRENT_FOLDS as u32)
        .await
        .expect("hold next fold");
    let prepared = vec![
        writer
            .prepare_file_bytes(&namespace, b"more")
            .await
            .expect("prepare first value"),
        writer
            .prepare_file_bytes(&namespace, b"last")
            .await
            .expect("prepare second value"),
    ];
    let request = CommitRequest {
        commit_id: CommitId::parse("after-fold").expect("commit"),
        actor_id: loonfs_test_support::test_actor(),
        subject: None,
        message: None,
        operations: vec![
            put_operation("/after-fold-1", &prepared[0]),
            put_operation("/after-fold-2", &prepared[1]),
        ],
        preconditions: Vec::new(),
    };
    writer
        .commit_candidate(&namespace, CommitCandidate::prepared(request, prepared))
        .await
        .expect("publish after fold");
    let usage = loonfs_core::cache::load_namespace_wal_tail_usage(store.as_ref(), &namespace)
        .await
        .expect("tail usage");
    assert_eq!(usage.wal_tail_inline_bytes, 8);

    drop(fold_permits);
    writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn inline_bytes_make_automatic_and_explicit_folds_due_before_segment_count() {
    for mode in ["automatic", "explicit", "scheduled"] {
        let (_directory, store, writer, namespace) = writer_with_policy(InlineContentOptions {
            inline_content_fold_at_bytes: 4,
            ..policy()
        })
        .await;
        let permits = writer
            .bits
            .wal_fold_permits
            .acquire_many(crate::DEFAULT_MAX_CONCURRENT_FOLDS as u32)
            .await
            .expect("hold folds");
        writer
            .put_file_bytes(&namespace, "/file", b"four", put_options("fold"))
            .await
            .expect("put");
        let usage = loonfs_core::cache::load_namespace_wal_tail_usage(store.as_ref(), &namespace)
            .await
            .expect("usage");
        assert_eq!(usage.wal_tail_inline_bytes, 4);
        assert!(usage.wal_tail_segments < FOLD_AT_WAL_SEGMENTS);
        let maintenance = writer
            .maintenance_handle("maintenance")
            .expect("maintenance");
        let options = MetadataMaintenanceOptions {
            inline_content_fold_at_bytes: NonZeroUsize::new(4).expect("threshold"),
            ..Default::default()
        };
        store.reset();
        assert_eq!(
            maintenance
                .metadata_probe(&namespace, &options)
                .await
                .expect("probe"),
            crate::MaintenanceProbe::Idle
        );
        assert_eq!(
            family_requests(&store, DurableObjectFamily::WalSegment),
            4,
            "one WAL tip check without a byte-count replay"
        );
        let independent = crate::FsMaintenance::builder_with_store(store.clone())
            .actor_id("independent")
            .build()
            .await
            .expect("independent maintenance");
        store.reset();
        let step = independent
            .maintain_metadata(&namespace, options.clone())
            .await
            .expect("maintenance without a writer");
        assert_eq!(step.wal_flush, crate::WalFlushStepOutcome::NotNeeded);
        assert_eq!(
            family_requests(&store, DurableObjectFamily::WalSegment),
            8,
            "two WAL tip checks without a byte-count replay"
        );
        if mode == "explicit" {
            let step = maintenance
                .maintain_metadata(&namespace, options)
                .await
                .expect("explicit fold");
            assert!(matches!(
                step.wal_flush,
                crate::WalFlushStepOutcome::Flushed { .. }
            ));
            assert_eq!(
                writer.publisher().wal_tail_inline_bytes(&namespace).await,
                Some(0)
            );
        } else if mode == "scheduled" {
            let jobs = crate::MaintenanceRegistry::new();
            jobs.register(Arc::new(
                crate::MetadataMaintenanceJob::new(maintenance).options(options),
            ))
            .expect("metadata job");
            let runner = crate::MaintenanceRunner::builder(jobs)
                .build()
                .expect("runner");
            runner.handle().hint(crate::MaintenanceHint::Published(
                crate::NamespacePublication {
                    namespace_id: namespace.clone(),
                    committed_through_seq: Some(usage.head_seq),
                    wal_tail_segments: usage.wal_tail_segments,
                    wal_tail_inline_bytes: usage.wal_tail_inline_bytes,
                },
            ));
            timeout(Duration::from_secs(10), runner.drain())
                .await
                .expect("scheduled maintenance settles after folding")
                .expect("scheduled fold");
            let usage =
                loonfs_core::cache::load_namespace_wal_tail_usage(store.as_ref(), &namespace)
                    .await
                    .expect("usage after scheduled fold");
            assert_eq!(usage.wal_tail_segments, 0);
            assert_eq!(usage.wal_tail_inline_bytes, 0);
            runner.shutdown().await.expect("runner shutdown");
        }
        drop(permits);
        writer
            .wait_for_fold(&namespace)
            .await
            .expect("automatic fold settles");
        let usage = loonfs_core::cache::load_namespace_wal_tail_usage(store.as_ref(), &namespace)
            .await
            .expect("usage after fold");
        assert_eq!(usage.wal_tail_segments, 0);
        assert_eq!(usage.wal_tail_inline_bytes, 0);
        assert_eq!(
            writer
                .reader()
                .get_file_bytes(&namespace, "/file")
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
    use loonfs_api::wire::wal::{
        MAX_WAL_INLINE_CONTENT_BYTES, MAX_WAL_SEGMENT_INLINE_CONTENT_BYTES,
    };
    let directory = tempdir().expect("directory");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    for options in [
        InlineContentOptions {
            inline_content_threshold_bytes: Some(MAX_WAL_INLINE_CONTENT_BYTES + 1),
            ..policy()
        },
        InlineContentOptions {
            inline_content_segment_budget_bytes: MAX_WAL_SEGMENT_INLINE_CONTENT_BYTES + 1,
            ..policy()
        },
        InlineContentOptions {
            inline_content_fold_at_bytes: 33 * 1024 * 1024,
            ..policy()
        },
        InlineContentOptions {
            inline_content_segment_budget_bytes: 0,
            ..policy()
        },
        InlineContentOptions {
            inline_content_fold_at_bytes: 0,
            ..policy()
        },
        InlineContentOptions {
            inline_content_tail_limit_bytes: 0,
            ..policy()
        },
    ] {
        let error = crate::FsWriter::builder_with_store(store.clone())
            .writer_id("writer")
            .inline_content(options)
            .build()
            .await
            .err()
            .expect("invalid policy");
        assert!(matches!(error, RuntimeError::Config(_)));
        assert!(store.snapshot().is_empty());
    }
    crate::FsWriter::builder_with_store(store)
        .writer_id("writer")
        .inline_content(InlineContentOptions {
            inline_content_threshold_bytes: Some(0),
            ..policy()
        })
        .build()
        .await
        .expect("zero threshold permits empty content");
}
