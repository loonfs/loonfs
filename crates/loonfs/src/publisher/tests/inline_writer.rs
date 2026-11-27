//! Inline writer preparation, publication, and maintenance contracts.

use super::super::admission::estimated_admission_bytes;
use super::*;
use crate::{InlineContentPolicy, MaintainMetadataOptions, PutFileOptions};
use loonfs_core::limits::FOLD_AT_WAL_BYTES;
use loonfs_objectstore::layout::{parse_object_key, DurableObjectFamily};
use loonfs_test_support::bytes::incompressible_bytes;
use loonfs_test_support::clock::ManualClock;
use loonfs_test_support::stores::RecordedOperation;
use loonfs_types::format::wal::{
    decode_wal_object_envelope_zstd, MAX_WAL_INLINE_CONTENT_BYTES,
    MAX_WAL_OBJECT_INLINE_CONTENT_BYTES,
};
use std::assert_matches;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicBool, Ordering};

fn policy() -> InlineContentPolicy {
    InlineContentPolicy {
        inline_content_threshold_bytes: 4,
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

/// Builds a store over `root` that writes the next WAL publication after
/// `armed` is set and then reports a transport error for it, and returns the
/// store with a flag that the lost acknowledgement sets.
fn lost_wal_ack_store(
    root: &Path,
    namespace: &NamespaceId,
    armed: &Arc<AtomicBool>,
) -> (FailStore<LocalFsStore>, Arc<AtomicBool>) {
    let lost = Arc::new(AtomicBool::new(false));
    let (put_armed, put_lost) = (Arc::clone(armed), Arc::clone(&lost));
    let wal_prefix = wal_prefix(namespace);
    let store = FailStore::matching(
        LocalFsStore::new(root).expect("store"),
        move |operation| {
            if operation.key().starts_with(&wal_prefix)
                && matches!(operation.kind(), OperationKind::Put { bytes, mode: PutMode::CreateIfAbsent } if is_publication(bytes))
                && put_armed.swap(false, Ordering::SeqCst)
            {
                put_lost.store(true, Ordering::SeqCst);
                true
            } else {
                false
            }
        },
        InjectedError::Transport("lost inline WAL acknowledgement".to_owned()),
    )
    .apply_then_fail();
    store.fail_all();
    (store, lost)
}

async fn check_terminal_reload_failure(include_replay: bool) {
    let directory = tempdir().expect("directory");
    let namespace = NamespaceId::parse("inline-unknown").expect("namespace");
    let armed = Arc::new(AtomicBool::new(false));
    let (lost_ack, unreadable) = lost_wal_ack_store(directory.path(), &namespace, &armed);
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
    let writer = crate::LoonFs::builder_with_store(store.clone(), crate::Context::Service)
        .writer_id(loonfs_test_support::ids::writer_id("inline-writer"))
        .config(crate::RuntimeConfig {
            inline_content: InlineContentPolicy {
                inline_content_threshold_bytes: 4,
                ..Default::default()
            },
            min_publish_interval_ms: 0,
            ..crate::RuntimeConfig::default()
        })
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
                bounded(
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
    let durable = loonfs_core::test_support::namespace_store(store.inner().inner(), &namespace)
        .load_wal_tail_usage()
        .await
        .expect("raw durable tail");
    assert_eq!(durable.wal_tail_inline_bytes, 4);
    // The unknown outcome dropped the position, so the tail stays unknown
    // until the replay's publish reads it.
    assert_eq!(
        writer
            .mode
            .publisher
            .wal_tail_inline_bytes(&namespace)
            .await,
        None
    );
    assert_eq!(
        writer
            .mode
            .publisher
            .shared
            .context
            .admission
            .used_requests(),
        0
    );
    assert_eq!(first_error.code(), ErrorCode::OutcomeUnknown);

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
        .expect("the next value publishes inline");
    let recovered = loonfs_core::test_support::namespace_store(store.inner().inner(), &namespace)
        .load_wal_tail_usage()
        .await
        .expect("recovered usage");
    assert_eq!(recovered.wal_tail_inline_bytes, 8);
    assert_eq!(
        writer
            .mode
            .publisher
            .shared
            .context
            .admission
            .used_requests(),
        0
    );
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

/// Checks that a put whose outcome is unknown starts no fold, though its WAL
/// object reaches the fold trigger, and that the batch's retry starts the
/// fold once the tail it reads from the store includes that object.
#[tokio::test]
async fn the_fold_trigger_waits_for_the_reloaded_tail_after_an_unknown_outcome() {
    let directory = tempdir().expect("directory");
    let namespace = NamespaceId::parse("inline-unknown-fold").expect("namespace");
    let armed = Arc::new(AtomicBool::new(false));
    let (lost_ack, lost) = lost_wal_ack_store(directory.path(), &namespace, &armed);
    // Parks the retry's first read of the tail, which starts after the
    // attempt that lost the acknowledgement has checked the fold trigger.
    let store = Arc::new(BlockingStore::matching(lost_ack, move |operation| {
        lost.load(Ordering::SeqCst) && !matches!(operation.kind(), OperationKind::Put { .. })
    }));
    store.block_next();
    // The claim's fence and the warmup leave two WAL objects, so the put's
    // object reaches the fold trigger.
    let writer = crate::LoonFs::builder_with_store(store.clone(), crate::Context::Service)
        .writer_id(loonfs_test_support::ids::writer_id("inline-writer"))
        .session_fold_at_wal_objects(NonZeroU64::new(3).expect("three WAL objects"))
        .config(crate::RuntimeConfig {
            inline_content: policy(),
            min_publish_interval_ms: 0,
            ..crate::RuntimeConfig::default()
        })
        .build()
        .await
        .expect("writer");
    let actor = loonfs_test_support::test_actor();
    writer
        .create_namespace(&namespace, &actor)
        .await
        .expect("namespace");
    let namespace_writer = writer.open_namespace(&namespace).expect("open namespace");
    namespace_writer
        .create_directory("/warmup", &actor)
        .await
        .expect("acquire writer epoch");
    writer.drain().await.expect("drain");
    let publisher = namespace_writer.session().publisher.clone();
    let folds = hold_every_fold_permit(&writer).await;
    armed.store(true, Ordering::SeqCst);
    let unknown = put_options("unknown");
    let (put, ()) = tokio::join!(
        namespace_writer.put_file_with_options("/file", b"four", &actor, &unknown),
        async {
            bounded(Duration::from_secs(10), store.wait_until_blocked())
                .await
                .expect("the retry reads the tail from the store");
            assert!(publisher_state(&publisher).fold.is_none());
            store.release();
        }
    );
    put.expect("the retry finds the receipt of the put that was written");
    assert!(publisher_state(&publisher).fold.is_some());
    drop(folds);
    namespace_writer.wait_for_fold().await.expect("fold");
    let usage = loonfs_core::test_support::namespace_store(store.as_ref(), &namespace)
        .load_wal_tail_usage()
        .await
        .expect("tail usage after the fold");
    assert_eq!(
        (usage.wal_tail_objects, usage.wal_tail_inline_bytes),
        (0, 0)
    );
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
    writer_with(
        policy,
        max_estimated_bytes_per_namespace,
        FOLD_AT_WAL_OBJECTS,
    )
    .await
}

async fn writer_with(
    policy: InlineContentPolicy,
    max_estimated_bytes_per_namespace: usize,
    session_fold_at_wal_objects: NonZeroU64,
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
    let writer = crate::LoonFs::builder_with_store(store.clone(), crate::Context::Service)
        .writer_id(loonfs_test_support::ids::writer_id("inline-writer"))
        .session_fold_at_wal_objects(session_fold_at_wal_objects)
        .config(crate::RuntimeConfig {
            inline_content: policy,
            publication: crate::PublicationPolicy {
                max_estimated_bytes_per_namespace: NonZeroUsize::new(
                    max_estimated_bytes_per_namespace,
                )
                .expect("namespace byte limit"),
                ..Default::default()
            },
            min_publish_interval_ms: 0,
            ..crate::RuntimeConfig::default()
        })
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
    writer.drain().await.expect("drain");
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
                .records()
                .to_vec(),
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
    assert_eq!(store.count(OperationSelector::Put), 1);
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
    assert_eq!(store.count(OperationSelector::Put), 0);
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
    assert_eq!(store.count(OperationSelector::Put), 0);
    writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn zero_threshold_and_above_threshold_writes_keep_uploaded_object_identity() {
    for options in [
        InlineContentPolicy {
            inline_content_threshold_bytes: 0,
            ..Default::default()
        },
        policy(),
    ] {
        let (_directory, store, writer, _namespace, namespace) = writer_with_policy(options).await;
        let bytes: &[u8] = if writer
            .inline_content_policy()
            .inline_content_threshold_bytes
            == 0
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
        assert_eq!(store.count(OperationSelector::Put), 1);
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
        assert_eq!(store.count(OperationSelector::Put), 0);
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
        assert_eq!(store.count(OperationSelector::Put), 0);
    }
    writer.shutdown().await.expect("shutdown");
}

/// Checks that a commit whose put and append pass the WAL object budget
/// together keeps both in its WAL object, and that the file reads back
/// before and after a fold.
#[tokio::test]
async fn a_put_and_an_append_over_the_wal_object_budget_stay_in_the_wal() {
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
    let request = CommitIntent {
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
    let pieces: Vec<_> = records[0]
        .inline_content
        .iter()
        .map(|piece| (piece.offset, piece.bytes.as_slice()))
        .collect();
    assert_eq!(
        pieces,
        [(0, b"four".as_slice()), (4, b" and six".as_slice())]
    );
    assert_eq!(family_requests(&store, DurableObjectFamily::ContentBlob), 0);
    for fold in [false, true] {
        if fold {
            writer
                .open_namespace(&namespace)
                .expect("open namespace")
                .fold_wal()
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
async fn retained_receipts_answer_retries_when_content_writes_fail() {
    for bytes in [b"four".as_slice(), b"longer"] {
        let directory = tempdir().expect("directory");
        let failing = Arc::new(FailStore::new(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::content_blob(),
            OperationSelector::Put,
            InjectedError::PermissionDenied("content writes refused".to_owned()),
        ));
        let store = Arc::new(RecordingStore::new(failing.clone(), KeyPredicate::any()));
        let writer = crate::LoonFs::builder_with_store(store.clone(), crate::Context::Service)
            .writer_id(loonfs_test_support::ids::writer_id("inline-writer"))
            .config(crate::RuntimeConfig {
                inline_content: InlineContentPolicy {
                    inline_content_threshold_bytes: 8,
                    inline_content_wal_object_budget_bytes: 4,
                    ..Default::default()
                },
                min_publish_interval_ms: 0,
                ..crate::RuntimeConfig::default()
            })
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
            Some(bytes.len())
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
            .expect_err("receipt rejects different bytes");
        assert_eq!(error.code(), ErrorCode::CommitIdReuseConflict);
        assert_eq!(store.count(OperationSelector::Put), 0);
        assert_eq!(failing.attempts(), 0);
        writer.shutdown().await.expect("shutdown");
    }
}

/// Checks that a commit whose inline value stays inline is charged, while it
/// waits in the queue, the admission bytes of its own weight, which counts the
/// inline vector's capacity as well as its values.
#[tokio::test]
async fn a_commit_whose_values_stay_inline_is_charged_its_own_weight() {
    let (_directory, _store, writer, namespace, namespace_writer) =
        writer_with_policy(InlineContentPolicy {
            inline_content_threshold_bytes: 8,
            ..policy()
        })
        .await;
    let publisher = namespace_writer.session().publisher.clone();
    let prepared = namespace_writer
        .prepare_content(b"longer")
        .await
        .expect("prepare");
    let candidate = CommitCandidate::prepared(
        CommitIntent {
            commit_id: CommitId::parse("inline").expect("commit"),
            actor_id: loonfs_test_support::test_actor(),
            subject: None,
            message: None,
            preconditions: Vec::new(),
            operations: vec![put_operation("/file", &prepared)],
        },
        vec![prepared],
    );
    let expected = estimated_admission_bytes(&namespace, candidate.estimated_retained_bytes());
    let slots = hold_every_publication_permit(&writer).await;
    let (commit, ()) = tokio::join!(namespace_writer.commit_candidate(candidate), async {
        bounded(
            Duration::from_secs(10),
            wait_for_queued_candidates(&publisher, 1),
        )
        .await
        .expect("the inline commit is queued");
        assert_eq!(
            publisher.context.admission.namespace_usage(&namespace),
            (1, expected)
        );
        assert_eq!(writer.execution_budget().stats().admitted_bytes, expected);
        drop(slots);
    });
    commit.expect("commit");
    writer.shutdown().await.expect("shutdown");
}

/// Checks that a commit whose inline values pass the WAL object budget
/// publishes every value inline in one WAL object of its own, and replays
/// from its receipt, and that a commit whose inline values pass
/// `MAX_WAL_OBJECT_INLINE_CONTENT_BYTES`, or whose inline value passes
/// `MAX_WAL_INLINE_CONTENT_BYTES`, is refused without a store write.
#[tokio::test]
async fn a_commit_over_the_wal_object_budget_publishes_its_values_in_one_wal_object() {
    const VALUES: usize = 128;
    const VALUE_BYTES: usize = 16 * 1024;
    let (_directory, store, writer, namespace, namespace_writer) =
        writer_with_policy_and_byte_limit(
            InlineContentPolicy {
                inline_content_wal_object_budget_bytes: VALUE_BYTES,
                inline_content_threshold_bytes: MAX_WAL_INLINE_CONTENT_BYTES,
                ..policy()
            },
            16 * 1024 * 1024,
        )
        .await;
    let namespace_reader = writer.namespace(&namespace);
    let payloads = (0..VALUES)
        .map(|index| Bytes::from(vec![u8::try_from(index).expect("byte index"); VALUE_BYTES]))
        .collect::<Vec<_>>();
    let candidate = prepared_puts(&namespace_writer, "batch", &payloads).await;
    let fingerprint = candidate.semantic_identity(&namespace);
    let published = namespace_writer
        .commit_candidate(candidate.clone())
        .await
        .expect("commit");
    let records = written_records(&store).await;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].semantic_commit_fingerprint, fingerprint);
    assert_eq!(records[0].inline_content.len(), VALUES);
    assert_eq!(family_requests(&store, DurableObjectFamily::ContentBlob), 0);
    for index in [0, VALUES - 1] {
        assert_eq!(
            namespace_reader
                .read_file(&format!("/batch-{index}"))
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
    assert_eq!(store.count(OperationSelector::Put), 0);

    let oversized = (0..=MAX_WAL_OBJECT_INLINE_CONTENT_BYTES / MAX_WAL_INLINE_CONTENT_BYTES)
        .map(|index| {
            Bytes::from(vec![
                u8::try_from(index).expect("byte index");
                MAX_WAL_INLINE_CONTENT_BYTES
            ])
        })
        .collect::<Vec<_>>();
    let candidate = prepared_puts(&namespace_writer, "oversized", &oversized).await;
    store.reset();
    let error = namespace_writer
        .commit_candidate(candidate)
        .await
        .expect_err("a commit over the WAL object's inline limit");
    assert_eq!(error.code(), ErrorCode::InvalidRequest);
    assert_eq!(store.count(OperationSelector::Put), 0);

    let value = loonfs_core::publish::InlineContent::new(
        loonfs_types::ContentId::generate(),
        Bytes::from(vec![b'p'; MAX_WAL_INLINE_CONTENT_BYTES + 1]),
        loonfs_types::ChecksumAlgorithm::Crc64nvme,
    );
    let request = CommitIntent::single(
        CommitId::parse("over-the-piece-limit").expect("commit"),
        loonfs_test_support::test_actor(),
        None,
        FilesystemOperation::PutFile {
            path: AbsolutePath::parse("/over").expect("path"),
            content: loonfs_types::FileContent::Ref(value.content_ref().clone()),
            behavior: DestinationBehavior::Replace,
            expected_inode_id: None,
            expected_revision_no: None,
        },
    );
    let error = namespace_writer
        .commit_candidate(CommitCandidate::with_inline_content(
            request,
            Vec::new(),
            vec![value],
        ))
        .await
        .expect_err("an inline value over the piece limit");
    assert_eq!(error.code(), ErrorCode::ContentTooLarge);
    assert_eq!(store.count(OperationSelector::Put), 0);
    writer.shutdown().await.expect("shutdown");
}

/// Prepares `payloads` and returns a commit that puts each at
/// `/{label}-{index}`.
async fn prepared_puts(
    namespace_writer: &crate::Namespace<crate::Writable>,
    label: &str,
    payloads: &[Bytes],
) -> CommitCandidate {
    let mut prepared = Vec::new();
    for bytes in payloads {
        prepared.push(
            namespace_writer
                .prepare_content(bytes)
                .await
                .expect("prepare"),
        );
    }
    let request = CommitIntent {
        commit_id: CommitId::parse(label).expect("commit"),
        actor_id: loonfs_test_support::test_actor(),
        subject: None,
        message: None,
        preconditions: Vec::new(),
        operations: prepared
            .iter()
            .enumerate()
            .map(|(index, value)| put_operation(&format!("/{label}-{index}"), value))
            .collect(),
    };
    CommitCandidate::prepared(request, prepared)
}

/// Checks that two queued writes whose inline bytes together pass the WAL
/// object budget publish in two WAL objects, each with its value inline.
#[tokio::test]
async fn queued_writes_split_at_the_wal_object_budget() {
    let (_directory, store, writer, _namespace, namespace) =
        writer_with_policy(InlineContentPolicy {
            inline_content_wal_object_budget_bytes: 4,
            ..policy()
        })
        .await;
    let slots = hold_every_publication_permit(&writer).await;
    let publisher = namespace.session().publisher.clone();
    let actor = loonfs_test_support::test_actor();
    let (one, two) = (put_options("one"), put_options("two"));
    let (first, second, ()) = tokio::join!(
        namespace.put_file_with_options("/one", b"one", &actor, &one),
        namespace.put_file_with_options("/two", b"two", &actor, &two),
        async {
            bounded(Duration::from_secs(10), async {
                while queued_candidates(&publisher.lock_state()) < 2 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("both candidates admitted while publication is blocked");
            assert_eq!(publisher.lock_state().queue.len(), 2);
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
    assert_eq!(inline_bytes, 6);
    assert_eq!(store.count(OperationSelector::Put), 2);
    assert_eq!(family_requests(&store, DurableObjectFamily::ContentBlob), 0);
    writer.shutdown().await.expect("shutdown");
}

/// Checks that a session's fold that finishes with the tail over the fold
/// trigger folds again, so the commit published while the fold was parked
/// is folded without another commit, and the next commit finds an empty
/// tail.
#[tokio::test]
async fn a_fold_that_leaves_the_tail_over_the_trigger_folds_again() {
    let directory = tempdir().expect("directory");
    let namespace = NamespaceId::parse("inline-fold-race").expect("namespace");
    let store = Arc::new(blocking_fold_store(
        LocalFsStore::new(directory.path()).expect("store"),
        metadata_manifest_prefix(&namespace),
    ));
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    // Each commit's WAL object reaches the fold trigger.
    let writer = crate::LoonFs::builder_with_store(store.clone(), crate::Context::Service)
        .metrics_recorder(recorder.clone())
        .writer_id(loonfs_test_support::ids::writer_id("inline-writer"))
        .session_fold_at_wal_objects(NonZeroU64::MIN)
        .config(crate::RuntimeConfig {
            inline_content: InlineContentPolicy {
                inline_content_threshold_bytes: 4,
                ..Default::default()
            },
            min_publish_interval_ms: 0,
            ..crate::RuntimeConfig::default()
        })
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
    writer.drain().await.expect("the warmup's fold settles");
    let folds_before = counter(&recorder, "loonfs.publisher.wal_folds");

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
        .expect("publish over the trigger during the fold");
    store.release();
    namespace_writer.wait_for_fold().await.expect("fold twice");
    assert_eq!(
        counter(&recorder, "loonfs.publisher.wal_folds"),
        folds_before + 2
    );
    let usage = loonfs_core::test_support::namespace_store(store.as_ref(), &namespace)
        .load_wal_tail_usage()
        .await
        .expect("tail usage after the second fold");
    assert_eq!(
        (usage.wal_tail_objects, usage.wal_tail_inline_bytes),
        (0, 0)
    );
    assert_eq!(
        writer
            .mode
            .publisher
            .wal_tail_inline_bytes(&namespace)
            .await,
        Some(0)
    );

    let fold_permits = hold_every_fold_permit(&writer).await;
    commit_two_values(&writer, &namespace, "after-fold").await;
    let usage = loonfs_core::test_support::namespace_store(store.as_ref(), &namespace)
        .load_wal_tail_usage()
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
    let usage = loonfs_core::test_support::namespace_store(store.as_ref(), &namespace)
        .load_wal_tail_usage()
        .await
        .expect("tail usage after recovery");
    assert_eq!(usage.wal_tail_inline_bytes, 16);

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
    let request = CommitIntent {
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

/// The size of each file in the WAL-byte trigger tests: the largest inline
/// value, so each put publishes a WAL object of about this size.
const LARGE_FILE_BYTES: usize = MAX_WAL_INLINE_CONTENT_BYTES;

/// An inline policy that keeps each file of [`LARGE_FILE_BYTES`] inline.
fn large_file_policy() -> InlineContentPolicy {
    InlineContentPolicy {
        inline_content_threshold_bytes: LARGE_FILE_BYTES,
        ..InlineContentPolicy::default()
    }
}

/// Puts incompressible files of [`LARGE_FILE_BYTES`] until the session's
/// WAL tail reaches `FOLD_AT_WAL_BYTES`, checks after each put that the
/// session starts its fold only once the tail's WAL bytes reach the trigger,
/// and returns the number of files.
async fn put_large_files_until_the_fold_trigger(
    writer: &crate::LoonFs<crate::Writable>,
    namespace: &NamespaceId,
    namespace_writer: &crate::Namespace<crate::Writable>,
) -> usize {
    let publisher = namespace_writer.session().publisher.clone();
    for index in 0..2 * FOLD_AT_WAL_BYTES / LARGE_FILE_BYTES {
        let seed = u64::try_from(index).expect("a small index");
        namespace_writer
            .put_file(
                &format!("/large-{index}"),
                &incompressible_bytes(seed, LARGE_FILE_BYTES),
                &loonfs_test_support::test_actor(),
            )
            .await
            .expect("put a large file");
        let wal_bytes = writer
            .mode
            .publisher
            .wal_tail_bytes(namespace)
            .await
            .expect("the session's position");
        let started = publisher_state(&publisher).fold.is_some();
        assert_eq!(
            started,
            wal_bytes >= FOLD_AT_WAL_BYTES,
            "after {} files the tail has {wal_bytes} WAL bytes",
            index + 1
        );
        if started {
            return index + 1;
        }
    }
    panic!("the large files never reached the fold trigger");
}

/// Checks that a tail of few large commits makes the session's fold and an
/// explicit maintenance fold due at `FOLD_AT_WAL_BYTES` of WAL bytes, long
/// before the WAL object trigger, and that a tail with more than 2 MiB of
/// inline content below that size starts no fold.
#[tokio::test]
async fn wal_bytes_make_automatic_and_explicit_folds_due_before_wal_object_count() {
    for mode in ["automatic", "explicit"] {
        let (_directory, store, writer, namespace, namespace_writer) =
            writer_with_policy_and_byte_limit(large_file_policy(), 4 * LARGE_FILE_BYTES).await;
        let namespace_reader = writer.namespace(&namespace);
        // Every fold takes a fold permit, so keeping them keeps the session's
        // fold waiting until each case has checked the tail.
        let permits = hold_every_fold_permit(&writer).await;
        let files =
            put_large_files_until_the_fold_trigger(&writer, &namespace, &namespace_writer).await;
        assert!(files * LARGE_FILE_BYTES > 2 * 1024 * 1024);
        let usage = loonfs_core::test_support::namespace_store(store.as_ref(), &namespace)
            .load_wal_tail_usage()
            .await
            .expect("usage");
        assert!(usage.wal_tail_objects < FOLD_AT_WAL_OBJECTS.get());
        assert_eq!(
            Some(usage.wal_tail_bytes),
            writer.mode.publisher.wal_tail_bytes(&namespace).await,
            "the store and the session's position count the same WAL bytes"
        );
        let maintenance = writer.clone();
        let options = MaintainMetadataOptions::default();
        if mode == "explicit" {
            // The session's position shows the tail's WAL bytes, so the
            // explicit step finds a fold due, and it waits for the
            // namespace's fold lock or for a permit, behind the session's
            // fold. A step that found nothing due would return at once.
            let handle = maintenance
                .open_namespace(&namespace)
                .expect("open namespace");
            let step = handle.maintain_metadata_with_options(&options);
            tokio::pin!(step);
            for _ in 0..100 {
                assert!(
                    futures::poll!(step.as_mut()).is_pending(),
                    "the tail's WAL bytes make the explicit fold due"
                );
                tokio::task::yield_now().await;
            }
            drop(permits);
            step.await.expect("explicit fold");
        } else {
            drop(permits);
        }
        namespace_writer
            .wait_for_fold()
            .await
            .expect("automatic fold settles");
        // The fold moved the session's position to its manifest, so the next
        // metadata step finds nothing left to fold.
        let again = maintenance
            .open_namespace(&namespace)
            .expect("open namespace")
            .maintain_metadata_with_options(&options)
            .await
            .expect("metadata step after the fold");
        assert_eq!(again.wal_fold, None);
        let usage = loonfs_core::test_support::namespace_store(store.as_ref(), &namespace)
            .load_wal_tail_usage()
            .await
            .expect("usage after fold");
        assert_eq!(usage.wal_tail_objects, 0);
        assert_eq!(usage.wal_tail_bytes, 0);
        assert_eq!(
            namespace_reader
                .read_file("/large-0")
                .await
                .expect("read folded content")
                .bytes,
            incompressible_bytes(0, LARGE_FILE_BYTES)
        );
        writer.shutdown().await.expect("shutdown");
    }
}

/// Checks that a metadata maintenance step folds a tail at `FOLD_AT_WAL_BYTES` after
/// the session's position is dropped. The session's own
/// fold fails, and the step loads the tail from the store and counts the WAL
/// bytes that it loaded.
#[tokio::test]
async fn a_metadata_step_without_a_current_position_counts_the_wal_bytes_that_it_loads() {
    let directory = tempdir().expect("directory");
    let namespace = NamespaceId::parse("stale-position").expect("namespace");
    let store = Arc::new(FailStore::matching(
        LocalFsStore::new(directory.path()).expect("store"),
        is_fold,
        InjectedError::Transport("injected fold failure".to_owned()),
    ));
    let timer = Arc::new(ManualClock::new(0));
    let writer = crate::LoonFs::builder_with_store(store.clone(), crate::Context::Service)
        .writer_id(loonfs_test_support::ids::writer_id("inline-writer"))
        .config(crate::RuntimeConfig {
            inline_content: large_file_policy(),
            min_publish_interval_ms: 0,
            ..crate::RuntimeConfig::default()
        })
        .monotonic_timer(timer.clone())
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace, &loonfs_test_support::test_actor())
        .await
        .expect("namespace");
    let namespace_writer = writer.open_namespace(&namespace).expect("open namespace");
    store.fail_all();
    put_large_files_until_the_fold_trigger(&writer, &namespace, &namespace_writer).await;
    let _ = namespace_writer.wait_for_fold().await;
    assert!(
        store.attempts() > 0,
        "the session's own fold ran and failed"
    );
    store.clear();
    let usage = loonfs_core::test_support::namespace_store(store.as_ref(), &namespace)
        .load_wal_tail_usage()
        .await
        .expect("usage");
    assert!(usage.wal_tail_bytes >= FOLD_AT_WAL_BYTES);
    assert!(usage.wal_tail_objects < FOLD_AT_WAL_OBJECTS.get());
    writer
        .mode
        .publisher
        .live_publisher(&namespace)
        .expect("a live session")
        .drop_position()
        .await;
    assert!(writer.mode.publisher.current_position(&namespace).is_none());
    let step = writer
        .open_namespace(&namespace)
        .expect("open namespace")
        .maintain_metadata_with_options(&MaintainMetadataOptions::default())
        .await
        .expect("metadata step");
    assert_matches!(
        step.wal_fold,
        Some(crate::FoldWalResponse {
            outcome: crate::FoldWalOutcome::Published,
            ..
        })
    );
    let usage = loonfs_core::test_support::namespace_store(store.as_ref(), &namespace)
        .load_wal_tail_usage()
        .await
        .expect("usage after the metadata step");
    assert_eq!(usage.wal_tail_objects, 0);
    writer.shutdown().await.expect("shutdown");
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
    for (options, message) in [
        (
            InlineContentPolicy {
                inline_content_threshold_bytes: MAX_WAL_INLINE_CONTENT_BYTES + 1,
                ..policy()
            },
            format!(
                "`inline_content.inline_content_threshold_bytes` must not exceed \
                 {MAX_WAL_INLINE_CONTENT_BYTES}"
            ),
        ),
        (
            InlineContentPolicy {
                inline_content_wal_object_budget_bytes: MAX_WAL_OBJECT_INLINE_CONTENT_BYTES + 1,
                ..policy()
            },
            format!(
                "`inline_content.inline_content_wal_object_budget_bytes` must be between 1 and \
                 {MAX_WAL_OBJECT_INLINE_CONTENT_BYTES}"
            ),
        ),
        (
            InlineContentPolicy {
                inline_content_wal_object_budget_bytes: 0,
                ..policy()
            },
            format!(
                "`inline_content.inline_content_wal_object_budget_bytes` must be between 1 and \
                 {MAX_WAL_OBJECT_INLINE_CONTENT_BYTES}"
            ),
        ),
        (
            InlineContentPolicy {
                inline_content_tail_limit_bytes: MAX_WAL_OBJECT_INLINE_CONTENT_BYTES - 1,
                ..policy()
            },
            format!(
                "`inline_content.inline_content_tail_limit_bytes` must be at least \
                 {MAX_WAL_OBJECT_INLINE_CONTENT_BYTES}, the inline bytes of one WAL object"
            ),
        ),
    ] {
        let error = crate::LoonFs::builder_with_store(store.clone(), crate::Context::Service)
            .writer_id(loonfs_test_support::ids::writer_id("writer"))
            .config(crate::RuntimeConfig {
                inline_content: options,
                ..crate::RuntimeConfig::default()
            })
            .build()
            .await
            .err()
            .expect("invalid policy");
        assert_matches!(&error, Error::Config(actual) if *actual == message);
        assert!(store.snapshot().is_empty());
    }
    crate::LoonFs::builder_with_store(store, crate::Context::Service)
        .writer_id(loonfs_test_support::ids::writer_id("writer"))
        .config(crate::RuntimeConfig {
            inline_content: InlineContentPolicy {
                inline_content_threshold_bytes: 0,
                ..policy()
            },
            ..crate::RuntimeConfig::default()
        })
        .build()
        .await
        .expect("zero threshold permits empty content");
}

/// A publish can observe the folded manifest before the fold's completion
/// callback runs. The callback must leave that fresh count alone.
#[tokio::test]
async fn a_delayed_fold_callback_preserves_a_freshly_observed_tail() {
    let directory = tempdir().expect("directory");
    let namespace = NamespaceId::parse("fold-accounting").expect("namespace");
    let store = Arc::new(HeldFoldAnswer::new(directory.path(), &namespace));
    // The commits stay far below the session's fold triggers, so nothing but
    // the explicit fold runs.
    let writer = crate::LoonFs::builder_with_store(store.clone(), crate::Context::Service)
        .writer_id(loonfs_test_support::ids::writer_id("inline-writer"))
        .config(crate::RuntimeConfig {
            inline_content: InlineContentPolicy {
                inline_content_threshold_bytes: 4,
                ..Default::default()
            },
            min_publish_interval_ms: 0,
            ..crate::RuntimeConfig::default()
        })
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
    let maintenance = writer.clone();
    store.hold_next();
    let namespace_2 = maintenance
        .open_namespace(&namespace)
        .expect("open namespace");
    let fold = namespace_2.fold_wal();
    let publish_after_fold = async {
        bounded(Duration::from_secs(10), store.wait_until_held())
            .await
            .expect("fold reached its manifest put");
        // The immutable manifest is durable, and the answer to its put is held.
        let usage = loonfs_core::test_support::namespace_store(store.as_ref(), &namespace)
            .load_wal_tail_usage()
            .await
            .expect("observe the completed fold");
        assert_eq!(usage.wal_tail_inline_bytes, 0);
        writer
            .mode
            .publisher
            .invalidate_namespace(&namespace, NamespaceChange::Unknown)
            .await;
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
    let request = CommitIntent {
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
        .expect("publish both values inline");
    let usage = loonfs_core::test_support::namespace_store(store.as_ref(), &namespace)
        .load_wal_tail_usage()
        .await
        .expect("actual tail usage");
    assert_eq!(usage.wal_tail_inline_bytes, 12);
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
