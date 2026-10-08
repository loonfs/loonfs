//! Inline materialization ordering, failure, and concurrent fold contracts.

use super::*;
use crate::namespace::control::load_current_manifest;
use crate::storage::content::{content_object_key_for_ref, CONTENT_READ_CHUNK_BYTES};
use crate::storage::tail_content::{assemble_tail_content, write_tail_content};
use loonfs_objectstore::keys::content_blob;
use loonfs_objectstore::layout::{parse_object_key, DurableObjectFamily};
use loonfs_objectstore::PutMode;
use loonfs_test_support::stores::{BlockingStore, FailStore, InjectedError, OperationClass};
use loonfs_types::{Checksum, ContentRefKind, ErrorCode};
use std::sync::atomic::{AtomicU64, Ordering};

fn family(operation: &RecordedOperation) -> Option<DurableObjectFamily> {
    parse_object_key(operation.key()).map(|key| key.family())
}

fn creates_content(operation: &RecordedOperation) -> bool {
    matches!(
        operation,
        RecordedOperation::Put { .. } | RecordedOperation::PutImmutableStream { .. }
    ) && family(operation) == Some(DurableObjectFamily::ContentBlob)
}

fn content_puts(store: &RecordingStore<LocalFsStore>) -> Vec<String> {
    store
        .snapshot()
        .iter()
        .filter(|operation| creates_content(operation))
        .map(|operation| operation.key().to_owned())
        .collect()
}

#[tokio::test]
async fn an_own_fold_replays_only_later_objects_after_projection_invalidation() {
    let (_directory, store, mut engine, context) = setup().await;
    let before = candidate(
        "before",
        vec![inline(&engine.namespace_id, Bytes::from_static(b"before"))],
    );
    publish(&mut engine, &store, &context, before)
        .await
        .expect("before");
    let input = engine.begin_wal_fold().expect("fold input");
    engine.invalidate_projection();
    let during = candidate(
        "during",
        vec![inline(&engine.namespace_id, Bytes::from_static(b"during"))],
    );
    publish(&mut engine, &store, &context, during.clone())
        .await
        .expect("during");
    let expected = store
        .snapshot()
        .into_iter()
        .rev()
        .find_map(|operation| match operation {
            RecordedOperation::Put { key, .. }
                if key.starts_with(&wal_prefix(&engine.namespace_id)) =>
            {
                Some(key)
            }
            _ => None,
        })
        .expect("published WAL");
    let folded = fold_wal_tail(
        &store,
        None,
        &engine.namespace_id,
        Some(input.clone()),
        &Deadline::start(Arc::new(StdMonotonicTimer::default())),
    )
    .await
    .expect("fold");
    engine.record_wal_fold(Some(&folded));
    assert!(engine.publish_tail.is_none());
    store.reset();
    publish(&mut engine, &store, &context, during)
        .await
        .expect("retained receipt");
    let operations = store.take();
    let wal_gets = operations
        .iter()
        .filter(|operation| {
            matches!(operation, RecordedOperation::Get { .. })
                && operation
                    .key()
                    .starts_with(&wal_prefix(&engine.namespace_id))
        })
        .map(RecordedOperation::key)
        .collect::<Vec<_>>();
    assert!(wal_gets.contains(&expected.as_str()), "{operations:?}");
    assert!(wal_gets.iter().all(|key| {
        loonfs_objectstore::layout::wal_no_of(key).expect("WAL number") > input.head.wal_no
    }));
    assert!(
        !operations
            .iter()
            .any(|operation| matches!(operation, RecordedOperation::Put { .. })),
        "{operations:?}"
    );
    assert_eq!(
        engine
            .wal_fold_input()
            .expect("projection")
            .wal_tail_objects,
        1
    );
}

#[tokio::test]
async fn an_own_fold_discovers_later_commits_after_the_projection_is_dropped() {
    // A receipt replay does not land a put, so it also tests that discovery
    // itself refreshes the tip and basis observations.
    for replay_receipt in [false, true] {
        let (_directory, store, mut engine, context) = setup().await;
        let directory = |name: &str| {
            CommitCandidate::new(CommitRequest::single(
                CommitId::generate(),
                loonfs_test_support::test_actor(),
                None,
                FilesystemOperation::CreateDirectory {
                    path: AbsolutePath::parse(name).expect("path"),
                    parents: false,
                },
            ))
        };
        publish(&mut engine, &store, &context, directory("/before"))
            .await
            .expect("before fold");
        let timer = Arc::new(loonfs_test_support::clock::ManualClock::new(0));
        engine = engine.monotonic_timer(timer.clone());
        let input = engine.begin_wal_fold().expect("fold input");
        let during = directory("/during");
        engine
            .publish_batch(
                &store,
                [during.clone()],
                &context,
                &Deadline::start(timer.clone()),
            )
            .await
            .results
            .pop()
            .expect("result")
            .expect("during fold");
        engine.invalidate_projection();
        let folded = fold_wal_tail(
            &store,
            None,
            &engine.namespace_id,
            Some(input.clone()),
            &Deadline::start(timer.clone()),
        )
        .await
        .expect("fold");
        assert_eq!(folded.response.outcome, FoldWalOutcome::Published);
        engine.record_wal_fold(Some(&folded));
        timer.advance_ms(100);
        store.reset();

        let candidate = if replay_receipt {
            during
        } else {
            directory("/during/child")
        };
        engine
            .publish_batch(
                &store,
                [candidate],
                &context,
                &Deadline::start(timer.clone()),
            )
            .await
            .results
            .pop()
            .expect("result")
            .expect("discover the commit made during the fold");
        let operations = store.take();
        let wal_gets = operations
            .iter()
            .filter(|operation| {
                matches!(operation, RecordedOperation::Get { .. })
                    && family(operation) == Some(DurableObjectFamily::WalObject)
            })
            .map(|operation| {
                loonfs_objectstore::layout::wal_no_of(operation.key()).expect("WAL number")
            })
            .collect::<Vec<_>>();
        assert!(wal_gets.contains(&input.head.wal_no.successor().expect("next WAL")));
        assert!(wal_gets.iter().all(|number| *number > input.head.wal_no));
        let gets = |wanted: DurableObjectFamily| {
            operations
                .iter()
                .filter(|operation| {
                    matches!(
                        operation,
                        RecordedOperation::Get { .. } | RecordedOperation::GetWithMetadata { .. }
                    ) && family(operation) == Some(wanted)
                })
                .count()
        };
        assert_eq!(
            gets(DurableObjectFamily::Hint),
            2,
            "discovery and its GC recheck read the hint; {operations:?}"
        );
        assert_eq!(
            gets(DurableObjectFamily::MetadataManifest),
            2,
            "the fold's manifest and its absent successor; {operations:?}"
        );
        assert_eq!(
            operations
                .iter()
                .filter(|operation| matches!(operation, RecordedOperation::Head { .. }))
                .count(),
            1,
            "one successor probe; {operations:?}"
        );
        if replay_receipt {
            assert!(!operations
                .iter()
                .any(|operation| matches!(operation, RecordedOperation::Put { .. })));
        }
        assert_eq!(
            engine.projection_observed.as_ref().expect("tip").age_ms(),
            0
        );
        assert_eq!(engine.basis_checked.as_ref().expect("basis").age_ms(), 0);
    }
}

#[tokio::test]
async fn a_takeover_after_an_own_fold_fences_the_writer_without_a_view() {
    let (_directory, store, mut engine, context) = setup().await;
    let before = candidate(
        "before",
        vec![inline(&engine.namespace_id, Bytes::from_static(b"before"))],
    );
    publish(&mut engine, &store, &context, before)
        .await
        .expect("before fold");
    let input = engine.begin_wal_fold().expect("fold input");
    engine.invalidate_projection();
    let folded = fold_wal_tail(
        &store,
        None,
        &engine.namespace_id,
        Some(input),
        &Deadline::start(Arc::new(StdMonotonicTimer::default())),
    )
    .await
    .expect("fold");
    assert_eq!(folded.response.outcome, FoldWalOutcome::Published);
    engine.record_wal_fold(Some(&folded));
    NamespaceCommitEngine::new(engine.namespace_id.clone())
        .session_writer_epoch(
            &store,
            &MutationContext {
                writer_id: WriterId::parse("successor").expect("writer"),
                now_ms: 1_000,
            },
        )
        .await
        .expect("claim and fence");
    let after = candidate(
        "after",
        vec![inline(&engine.namespace_id, Bytes::from_static(b"after"))],
    );
    let error = publish(&mut engine, &store, &context, after)
        .await
        .expect_err("fenced");
    assert_eq!(error.code(), ErrorCode::WriterFenced, "{error:?}");
}

pub(super) fn assert_content_before_metadata(store: &RecordingStore<LocalFsStore>, count: usize) {
    let operations = store.snapshot();
    let first_metadata = operations
        .iter()
        .position(|operation| {
            matches!(operation, RecordedOperation::Put { .. })
                && matches!(
                    family(operation),
                    Some(
                        DurableObjectFamily::MetadataSegment
                            | DurableObjectFamily::MetadataManifest
                    )
                )
        })
        .expect("metadata write");
    assert_eq!(content_puts(store).len(), count);
    assert_eq!(
        operations[..first_metadata]
            .iter()
            .filter(|operation| creates_content(operation))
            .count(),
        count
    );
}

#[derive(Debug)]
struct SteppingTimer(AtomicU64);

impl MonotonicTimer for SteppingTimer {
    fn monotonic_now_ms(&self) -> u64 {
        self.0.fetch_add(20 * 60 * 1000, Ordering::SeqCst)
    }
}

#[tokio::test]
async fn failed_manifest_and_over_budget_retries_keep_materialized_content() {
    let (_directory, store, mut engine, context) = setup().await;
    let values = vec![
        inline(&engine.namespace_id, Bytes::from_static(b"retained")),
        inline(&engine.namespace_id, Bytes::new()),
    ];
    publish(
        &mut engine,
        &store,
        &context,
        candidate("retry-fold", values.clone()),
    )
    .await
    .expect("publish");
    let input = engine.wal_fold_input().expect("tail");
    let failing = FailStore::new(
        store.clone(),
        KeyPredicate::manifest(&engine.namespace_id),
        OperationClass::Put,
        InjectedError::PermissionDenied("manifest failure".to_owned()),
    );
    failing.fail_all();
    store.reset();
    assert!(matches!(
        fold_wal_tail(
            &failing,
            None,
            &engine.namespace_id,
            Some(input.clone()),
            &crate::time::Deadline::start(Arc::new(StdMonotonicTimer::default()))
        )
        .await,
        Err(CoreError::Store {
            class: crate::error::StoreFailureClass::PermissionDenied,
            ..
        })
    ));
    assert_content_before_metadata(&store, values.len());
    let keys = content_puts(&store);
    assert_eq!(
        load_current_manifest(&store, &engine.namespace_id)
            .await
            .expect("manifest")
            .state
            .manifest(),
        *input.basis.manifest()
    );
    for value in &values {
        let key = content_object_key_for_ref(value.content_ref()).expect("key");
        assert_eq!(
            store
                .get(&key, None)
                .await
                .expect("get")
                .expect("retained object"),
            value.bytes().as_ref()
        );
    }
    failing.clear();
    store.reset();
    let timer = Arc::new(SteppingTimer(AtomicU64::new(0)));
    assert!(matches!(
        fold_wal_tail(
            &store,
            None,
            &engine.namespace_id,
            Some(input.clone()),
            &crate::time::Deadline::start(timer.clone())
        )
        .await,
        Err(CoreError::MetadataPublicationBudgetExceeded { .. })
    ));
    assert_eq!(store.counts().puts, values.len());
    assert_eq!(store.counts().deletes, 0);
    assert_eq!(content_puts(&store).len(), values.len());
    assert_eq!(
        load_current_manifest(&store, &engine.namespace_id)
            .await
            .expect("manifest")
            .state
            .manifest(),
        *input.basis.manifest()
    );
    store.reset();
    let folded = fold_wal_tail(
        &store,
        None,
        &engine.namespace_id,
        Some(input),
        &crate::time::Deadline::start(Arc::new(StdMonotonicTimer::default())),
    )
    .await
    .expect("retry");
    assert_eq!(folded.response.outcome, FoldWalOutcome::Published);
    assert_content_before_metadata(&store, values.len());
    let mut retry_keys = content_puts(&store);
    retry_keys.sort();
    let mut keys = keys;
    keys.sort();
    assert_eq!(retry_keys, keys);
    for key in &keys {
        assert!(store.snapshot().iter().any(|operation| matches!(operation, RecordedOperation::Head { key: actual } if actual == key)));
    }
}

#[tokio::test]
async fn competing_engines_materialize_identical_objects_and_publish_one_manifest() {
    let (_directory, store, mut first, context) = setup().await;
    let values = vec![inline(&first.namespace_id, Bytes::from_static(b"shared"))];
    let candidate = candidate("race", values);
    publish(&mut first, &store, &context, candidate.clone())
        .await
        .expect("publish");
    let mut second = NamespaceCommitEngine::with_unshared_head_state(first.namespace_id.clone());
    publish(&mut second, &store, &context, candidate)
        .await
        .expect("replay");
    let first_input = first.wal_fold_input().expect("first tail");
    let second_input = second.wal_fold_input().expect("second tail");
    assert_eq!(first_input.tail_state, second_input.tail_state);
    let blocked = BlockingStore::new(
        store.clone(),
        KeyPredicate::content_blob(),
        OperationClass::Put,
    );
    blocked.block_next();
    store.reset();
    let first_timer = Arc::new(StdMonotonicTimer::default());
    let deadline = crate::time::Deadline::start(first_timer);
    let first_fold = fold_wal_tail(
        &blocked,
        None,
        &first.namespace_id,
        Some(first_input),
        &deadline,
    );
    let second_fold = async {
        blocked.wait_until_blocked().await;
        let result = fold_wal_tail(
            &store,
            None,
            &second.namespace_id,
            Some(second_input),
            &crate::time::Deadline::start(Arc::new(StdMonotonicTimer::default())),
        )
        .await;
        blocked.release();
        result
    };
    let (first_result, second_result) = tokio::join!(first_fold, second_fold);
    let first_result = first_result.expect("first fold");
    let second_result = second_result.expect("second fold");
    assert_eq!(
        first_result.response.outcome,
        FoldWalOutcome::AlreadyCurrent
    );
    assert_eq!(second_result.response.outcome, FoldWalOutcome::Published);
    assert_eq!(
        first_result.response.manifest_no,
        second_result.response.manifest_no
    );
    let keys = content_puts(&store);
    assert_eq!(keys.len(), 2);
    assert_eq!(keys[0], keys[1]);
    assert_eq!(
        store
            .snapshot()
            .iter()
            .filter(
                |operation| matches!(operation, RecordedOperation::Put { .. })
                    && family(operation) == Some(DurableObjectFamily::MetadataManifest)
            )
            .count(),
        1
    );
}

#[tokio::test]
async fn an_existing_different_object_is_corruption_and_stops_manifest_publication() {
    let (_directory, store, mut engine, context) = setup().await;
    let value = inline(&engine.namespace_id, Bytes::from_static(b"right"));
    publish(
        &mut engine,
        &store,
        &context,
        candidate("conflict", vec![value.clone()]),
    )
    .await
    .expect("publish");
    let input = engine.wal_fold_input().expect("tail");
    let key = content_object_key_for_ref(value.content_ref()).expect("key");
    store
        .put(&key, Bytes::from_static(b"wrong"), PutMode::CreateIfAbsent)
        .await
        .expect("conflicting object");
    store.reset();
    let error = fold_wal(&store, &engine.namespace_id)
        .await
        .expect_err("different object");
    assert_eq!(error.code(), ErrorCode::NamespaceCorrupt);
    assert!(matches!(error, CoreError::NamespaceCorrupt(_)));
    assert_eq!(store.counts().puts, 1);
    assert_eq!(content_puts(&store), vec![key.clone()]);
    assert_eq!(
        load_current_manifest(&store, &engine.namespace_id)
            .await
            .expect("manifest")
            .state
            .manifest(),
        *input.basis.manifest()
    );
    assert_eq!(
        store
            .get(&key, None)
            .await
            .expect("get")
            .expect("object")
            .as_ref(),
        b"wrong"
    );
}

/// Publishes one inline value, then plants `existing` at its key before the
/// fold, as a fold that got further would have left a longer object.
async fn fold_over_longer_object(
    existing: &'static [u8],
) -> (
    tempfile::TempDir,
    std::sync::Arc<RecordingStore<LocalFsStore>>,
    std::result::Result<(), CoreError>,
    String,
    loonfs_types::format::control::ManifestRef,
    loonfs_types::NamespaceId,
) {
    let (directory, store, mut engine, context) = setup().await;
    let value = inline(&engine.namespace_id, Bytes::from_static(b"right"));
    publish(
        &mut engine,
        &store,
        &context,
        candidate("longer", vec![value.clone()]),
    )
    .await
    .expect("publish");
    let input = engine.wal_fold_input().expect("tail");
    let key = content_object_key_for_ref(value.content_ref()).expect("key");
    store
        .put(&key, Bytes::from_static(existing), PutMode::CreateIfAbsent)
        .await
        .expect("longer object");
    store.reset();
    let folded = fold_wal(&store, &engine.namespace_id).await.map(|_| ());
    let manifest_before = input.basis.manifest().clone();
    (
        directory,
        store,
        folded,
        key,
        manifest_before,
        engine.namespace_id.clone(),
    )
}

#[tokio::test]
async fn a_longer_object_with_the_pieces_in_place_is_another_folds_work() {
    let (_directory, store, folded, key, before, namespace_id) =
        fold_over_longer_object(b"right and longer").await;
    folded.expect("the object holds the pieces, so the fold publishes");
    assert_eq!(content_puts(&store), vec![key.clone()]);
    assert_eq!(
        store
            .get(&key, None)
            .await
            .expect("get")
            .expect("object")
            .as_ref(),
        b"right and longer",
        "nothing rewrote the longer object"
    );
    let published = load_current_manifest(&store, &namespace_id)
        .await
        .expect("manifest");
    assert_ne!(published.state.manifest(), before);
}

#[tokio::test]
async fn a_longer_object_with_other_bytes_under_the_pieces_is_corruption() {
    let (_directory, store, folded, key, before, namespace_id) =
        fold_over_longer_object(b"wrong and longer").await;
    let error = folded.expect_err("other bytes where the pieces belong");
    assert!(matches!(error, CoreError::NamespaceCorrupt(_)), "{error:?}");
    assert_eq!(content_puts(&store), vec![key.clone()]);
    assert_eq!(
        load_current_manifest(&store, &namespace_id)
            .await
            .expect("manifest")
            .state
            .manifest(),
        before,
        "the fold published nothing"
    );
}

#[tokio::test]
async fn a_materialization_transport_failure_remains_retryable() {
    let (_directory, store, mut engine, context) = setup().await;
    let value = inline(&engine.namespace_id, Bytes::from_static(b"right"));
    publish(
        &mut engine,
        &store,
        &context,
        candidate("transport", vec![value.clone()]),
    )
    .await
    .expect("publish");
    let input = engine.wal_fold_input().expect("tail");
    let key = content_object_key_for_ref(value.content_ref()).expect("key");
    store
        .put(&key, value.bytes().clone(), PutMode::CreateIfAbsent)
        .await
        .expect("existing object");
    let failing = FailStore::new(
        store.clone(),
        KeyPredicate::content_blob(),
        OperationClass::Head,
        InjectedError::Transport("attestation head failure".to_owned()),
    );
    failing.fail_all();
    store.reset();
    let error = fold_wal(&failing, &engine.namespace_id)
        .await
        .expect_err("attestation head failure");
    assert!(
        matches!(
            error,
            CoreError::Store {
                class: crate::error::StoreFailureClass::RetryableTransport,
                ..
            }
        ),
        "{error:?}"
    );
    assert_eq!(store.counts().puts, 1);
    assert_eq!(content_puts(&store), vec![key]);
    assert_eq!(
        load_current_manifest(&store, &engine.namespace_id)
            .await
            .expect("manifest")
            .state
            .manifest(),
        *input.basis.manifest()
    );
}

#[tokio::test]
async fn a_fold_reanchors_with_only_the_commits_published_since_it_began() {
    let (_directory, store, mut engine, context) = setup().await;
    let before = candidate(
        "before",
        vec![inline(&engine.namespace_id, Bytes::from_static(b"before"))],
    );
    publish(&mut engine, &store, &context, before)
        .await
        .expect("before fold");
    let input = engine.begin_wal_fold().expect("fold input");
    let mut expected = crate::wal::ProjectedWalTail::default();
    for name in ["during-one", "during-two"] {
        let value = inline(&engine.namespace_id, Bytes::from_static(b"during"));
        publish(
            &mut engine,
            &store,
            &context,
            candidate(name, vec![value.clone()]),
        )
        .await
        .expect("during fold");
        let key = store
            .snapshot()
            .into_iter()
            .rev()
            .find_map(|operation| match operation {
                RecordedOperation::Put { key, .. }
                    if key.starts_with(&wal_prefix(&engine.namespace_id)) =>
                {
                    Some(key)
                }
                _ => None,
            })
            .expect("published WAL key");
        let bytes = store
            .get(&key, None)
            .await
            .expect("WAL read")
            .expect("WAL object");
        let wal_object = decode_wal_object_envelope_zstd(&bytes).expect("WAL decode");
        expected
            .apply_commit(&engine.namespace_id, &wal_object.payload().records[0])
            .expect("later rows");
    }
    let folded = fold_wal_tail(
        &store,
        None,
        &engine.namespace_id,
        Some(input.clone()),
        &Deadline::start(Arc::new(StdMonotonicTimer::default())),
    )
    .await
    .expect("fold");
    assert_eq!(folded.response.outcome, FoldWalOutcome::Published);
    let observed = engine.projection_observed.clone();
    engine.record_wal_fold(Some(&folded));
    let position = engine.publish_tail.as_ref().expect("retained position");
    assert_eq!(position.basis(), &folded.basis);
    assert_eq!(position.wal_tail_objects, 2);
    assert_eq!(position.head.folded_wal_no, input.head.wal_no);
    assert_eq!(
        *engine
            .wal_fold_input()
            .expect("the reanchored tail is cached")
            .tail_state,
        expected
    );
    let retained = engine.projection_observed.as_ref().expect("observation");
    let observed = observed.expect("tip observation before the fold");
    assert_eq!(retained.age_at(&observed), 0);
    assert_eq!(observed.age_at(retained), 0);
    store.reset();
    let after = candidate(
        "after",
        vec![inline(&engine.namespace_id, Bytes::from_static(b"after"))],
    );
    publish(&mut engine, &store, &context, after)
        .await
        .expect("after fold");
    assert!(store.snapshot().iter().all(|operation| !matches!(operation,
        RecordedOperation::Get { key, .. } | RecordedOperation::GetWithMetadata { key, .. }
        if key.ends_with("hint.json") || key.starts_with(&wal_prefix(&engine.namespace_id))
    )));
}

#[tokio::test]
async fn a_fold_extends_the_object_with_its_unfolded_pieces() {
    let (_directory, store, mut engine, context) = setup().await;
    let namespace_id = engine.namespace_id.clone();
    let (inode_id, original) = folded_file(&store, &mut engine, &context, b"hello").await;
    let appended = commit_piece(
        &store,
        &namespace_id,
        (inode_id, RevisionNo(2)),
        &original.content_id,
        b"hello world",
        5,
        None,
    )
    .await;
    let key = content_object_key_for_ref(&original).expect("content key");

    store.reset();
    fold_wal(&store, &namespace_id).await.expect("fold");
    assert!(content_puts(&store).is_empty());
    assert!(store.snapshot().contains(&RecordedOperation::Extend {
        key: key.clone(),
        bytes: 6,
    }));
    let held = store
        .inner()
        .head(&key)
        .await
        .expect("head")
        .expect("object");
    assert_eq!(held.size_bytes, 11);
    assert_eq!(held.sha256, Some(appended.checksum));
    assert_eq!(
        read_file(&store, &namespace_id, RevisionNo(2)).await,
        b"hello world"
    );
}

#[tokio::test]
async fn a_second_fold_finds_the_object_at_its_target_and_verifies_it() {
    let (_directory, store, mut engine, context) = setup().await;
    let namespace_id = engine.namespace_id.clone();
    let (inode_id, original) = folded_file(&store, &mut engine, &context, b"hello").await;
    commit_piece(
        &store,
        &namespace_id,
        (inode_id, RevisionNo(2)),
        &original.content_id,
        b"hello world",
        5,
        None,
    )
    .await;
    let anchor = crate::namespace::read_anchor::load_read_anchor(&*store, &namespace_id)
        .await
        .expect("anchor");
    let tail = crate::namespace::read_anchor::project_anchor_tail(&*store, None, &anchor)
        .await
        .expect("tail");
    assert_eq!(tail.contents().len(), 1);
    let content = &tail.contents()[0];
    fold_wal(&store, &namespace_id).await.expect("first fold");

    store.reset();
    let pieces = assemble_tail_content(content).expect("pieces");
    write_tail_content(&*store, &tail, content, pieces.clone())
        .await
        .expect("a fold that lost the race finds the object extended");
    assert_no_writes(&store);
    assert_eq!(store.counts().heads, 1);

    let key = content_object_key_for_ref(&original).expect("content key");
    store
        .put_overwrite(&key, Bytes::from_static(b"hello WORLD"))
        .await
        .expect("replace the object");
    let error = write_tail_content(&*store, &tail, content, pieces)
        .await
        .expect_err("different bytes at the target length");
    assert_eq!(error.code(), ErrorCode::NamespaceCorrupt);
}

#[tokio::test]
async fn a_chain_from_a_base_materializes_under_its_own_id() {
    let (_directory, store, mut engine, context) = setup().await;
    let namespace_id = engine.namespace_id.clone();
    let (inode_id, original) = folded_file(&store, &mut engine, &context, b"hello").await;
    let chained = commit_piece(
        &store,
        &namespace_id,
        (inode_id, RevisionNo(2)),
        &ContentId::generate(),
        b"hello!!!",
        5,
        Some(ContentBase {
            owner_namespace_id: namespace_id.clone(),
            content_id: original.content_id.clone(),
        }),
    )
    .await;
    assert_eq!(
        read_file(&store, &namespace_id, RevisionNo(2)).await,
        b"hello!!!"
    );

    store.reset();
    fold_wal(&store, &namespace_id).await.expect("fold");
    let chain_key = content_object_key_for_ref(&chained).expect("chain key");
    assert_eq!(content_puts(&store), std::slice::from_ref(&chain_key));
    assert_eq!(
        store
            .inner()
            .get(&chain_key, None)
            .await
            .expect("get")
            .expect("chain object"),
        b"hello!!!".as_slice()
    );
    let base_key = content_object_key_for_ref(&original).expect("base key");
    let base = store
        .inner()
        .head(&base_key)
        .await
        .expect("head")
        .expect("base");
    assert_eq!(base.size_bytes, 5);
    assert_eq!(
        read_file(&store, &namespace_id, RevisionNo(2)).await,
        b"hello!!!"
    );
}

#[tokio::test]
async fn a_chain_from_a_base_longer_than_a_chunk_streams_the_prefix_in_chunks() {
    let (_directory, store, mut engine, context) = setup().await;
    let namespace_id = engine.namespace_id.clone();
    let (inode_id, _) = folded_file(&store, &mut engine, &context, b"hello").await;
    let base_id = ContentId::generate();
    let base_key = content_blob(&namespace_id, &base_id);
    let prefix_bytes = CONTENT_READ_CHUNK_BYTES + 5;
    let mut whole: Vec<u8> = (0..prefix_bytes).map(|index| (index % 251) as u8).collect();
    store
        .put(
            &base_key,
            Bytes::from(whole.clone()),
            PutMode::CreateIfAbsent,
        )
        .await
        .expect("base object");
    whole.extend_from_slice(b"!!!");
    let chained = commit_piece(
        &store,
        &namespace_id,
        (inode_id, RevisionNo(2)),
        &ContentId::generate(),
        &whole,
        prefix_bytes as usize,
        Some(ContentBase {
            owner_namespace_id: namespace_id.clone(),
            content_id: base_id,
        }),
    )
    .await;

    store.reset();
    fold_wal(&store, &namespace_id).await.expect("fold");
    let chain_key = content_object_key_for_ref(&chained).expect("chain key");
    assert_eq!(
        store
            .snapshot()
            .into_iter()
            .filter(creates_content)
            .collect::<Vec<_>>(),
        [RecordedOperation::PutImmutableStream {
            key: chain_key.clone(),
            sha256: Some(chained.checksum.clone()),
            bytes: Some(whole.len() as u64),
        }]
    );
    let base_gets: Vec<_> = store
        .take_gets()
        .into_iter()
        .filter(|(key, _)| *key == base_key)
        .collect();
    assert_eq!(
        base_gets,
        [
            (base_key.clone(), Some((0, CONTENT_READ_CHUNK_BYTES))),
            (base_key, Some((CONTENT_READ_CHUNK_BYTES, prefix_bytes))),
        ],
        "the prefix is read one chunk at a time, never whole"
    );
    assert_eq!(
        store
            .inner()
            .get(&chain_key, None)
            .await
            .expect("get")
            .expect("chain object"),
        whole
    );
}

#[tokio::test]
async fn a_chain_from_a_base_that_misses_its_reference_creates_nothing() {
    let (_directory, store, mut engine, context) = setup().await;
    let namespace_id = engine.namespace_id.clone();
    let (inode_id, original) = folded_file(&store, &mut engine, &context, b"hello").await;
    let chained = commit_piece(
        &store,
        &namespace_id,
        (inode_id, RevisionNo(2)),
        &ContentId::generate(),
        b"HELLO!!!",
        5,
        Some(ContentBase {
            owner_namespace_id: namespace_id.clone(),
            content_id: original.content_id.clone(),
        }),
    )
    .await;

    store.reset();
    let error = fold_wal(&store, &namespace_id)
        .await
        .expect_err("the base's bytes are not the reference's");
    assert_eq!(error.code(), ErrorCode::NamespaceCorrupt, "{error:?}");
    let chain_key = content_object_key_for_ref(&chained).expect("chain key");
    assert_eq!(content_puts(&store), std::slice::from_ref(&chain_key));
    assert_eq!(
        store.inner().head(&chain_key).await.expect("head"),
        None,
        "the store created nothing"
    );
}

#[tokio::test]
async fn a_crc_chain_from_a_base_creates_unattested_and_a_second_fold_reads_it_back() {
    let (_directory, store, mut engine, context) = setup().await;
    let namespace_id = engine.namespace_id.clone();
    let (inode_id, original) = folded_file(&store, &mut engine, &context, b"hello").await;
    let whole = b"hello!!!";
    let content_id = ContentId::generate();
    let chained = ContentRef {
        kind: ContentRefKind::BlobV1,
        owner_namespace_id: namespace_id.clone(),
        content_id: content_id.clone(),
        size_bytes: whole.len() as u64,
        checksum: Checksum::crc64nvme(whole),
    };
    crate::test_support::ops::append_wal_commit(
        &*store,
        &namespace_id,
        vec![WalDelta::AppendFileRevision {
            delta_index: 0,
            inode_id,
            revision_no: RevisionNo(2),
            content_ref: chained.clone(),
            hash_state: None,
            crc64nvme: Some(Checksum::crc64nvme(whole)),
        }],
        vec![WalInlineContent {
            content_id,
            offset: 5,
            bytes: whole[5..].to_vec(),
            base: Some(ContentBase {
                owner_namespace_id: namespace_id.clone(),
                content_id: original.content_id.clone(),
            }),
        }],
    )
    .await
    .expect("commit piece");
    let anchor = crate::namespace::read_anchor::load_read_anchor(&*store, &namespace_id)
        .await
        .expect("anchor");
    let tail = crate::namespace::read_anchor::project_anchor_tail(&*store, None, &anchor)
        .await
        .expect("tail");
    let content = &tail.contents()[0];

    store.reset();
    fold_wal(&store, &namespace_id).await.expect("first fold");
    let chain_key = content_object_key_for_ref(&chained).expect("chain key");
    assert!(store
        .snapshot()
        .contains(&RecordedOperation::PutImmutableStream {
            key: chain_key,
            sha256: None,
            bytes: Some(whole.len() as u64),
        }));

    store.reset();
    let pieces = assemble_tail_content(content).expect("pieces");
    write_tail_content(&*store, &tail, content, pieces)
        .await
        .expect("a second fold accepts the object");
    assert_no_writes(&store);
}
