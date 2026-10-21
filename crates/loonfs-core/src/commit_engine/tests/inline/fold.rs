//! Inline materialization ordering, failure, and concurrent fold contracts.

use super::*;
use crate::namespace::control::load_current_manifest;
use crate::storage::content::content_object_key_for_ref;
use loonfs_objectstore::layout::{parse_object_key, DurableObjectFamily};
use loonfs_objectstore::PutMode;
use loonfs_test_support::clock::ManualClock;
use loonfs_test_support::stores::{BlockingStore, FailStore, InjectedError, OperationSelector};
use loonfs_types::ErrorCode;
use std::assert_matches;

fn family(operation: &RecordedOperation) -> Option<DurableObjectFamily> {
    parse_object_key(operation.key()).map(|key| key.family())
}

fn assembles_content(operation: &RecordedOperation) -> bool {
    matches!(operation, RecordedOperation::Assemble { .. })
        && family(operation) == Some(DurableObjectFamily::ContentBlob)
}

fn content_assemblies(store: &RecordingStore<LocalFsStore>) -> Vec<String> {
    store
        .snapshot()
        .iter()
        .filter(|operation| assembles_content(operation))
        .map(|operation| operation.key().to_owned())
        .collect()
}

/// Checks that a session's fold moves a position that the session loaded
/// again while the fold ran, so the position keeps only the commit after the
/// fold, and a retried commit finds its receipt there without reading the WAL.
#[tokio::test]
async fn an_own_fold_moves_a_position_loaded_again_during_the_fold() {
    let content_writes = crate::test_support::content_writes();
    let (_directory, store, mut engine, context) = setup().await;
    let before = candidate(
        "before",
        vec![inline(&engine.namespace_id, Bytes::from_static(b"before"))],
    );
    publish(&mut engine, &store, &context, before)
        .await
        .expect("before");
    let input = engine.wal_fold_input().expect("fold input");
    engine.invalidate_projection();
    let during = candidate(
        "during",
        vec![inline(&engine.namespace_id, Bytes::from_static(b"during"))],
    );
    publish(&mut engine, &store, &context, during.clone())
        .await
        .expect("during");
    let folded = fold_wal_tail(
        &store,
        None,
        &engine.namespace_id,
        Some(input.clone()),
        &Deadline::start(Arc::new(StdMonotonicTimer::default())),
        crate::manifest::MetadataRunPolicy::default(),
        &content_writes,
    )
    .await
    .expect("fold");
    engine.record_manifest_published(folded.manifest.clone());
    let moved = engine.wal_fold_input().expect("the position moved");
    assert_eq!(moved.basis.0, folded.manifest.manifest);
    assert_eq!(moved.head.folded_wal_no, input.head.wal_no);
    assert_eq!(moved.wal_tail_objects, 1);
    store.reset();
    publish(&mut engine, &store, &context, during)
        .await
        .expect("retained receipt");
    let operations = store.take();
    assert!(
        !operations.iter().any(|operation| {
            matches!(operation, RecordedOperation::Put { .. })
                || operation
                    .key()
                    .starts_with(&wal_prefix(&engine.namespace_id))
        }),
        "{operations:?}"
    );
}

#[tokio::test]
async fn an_own_fold_discovers_later_commits_after_the_projection_is_dropped() {
    let content_writes = crate::test_support::content_writes();
    // A receipt replay does not land a put, so it also tests that discovery
    // itself refreshes the tip and basis observations.
    for replay_receipt in [false, true] {
        let (_directory, store, mut engine, context) = setup().await;
        let directory = |name: &str| {
            CommitCandidate::new(CommitIntent::single(
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
        let input = engine.wal_fold_input().expect("fold input");
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
            crate::manifest::MetadataRunPolicy::default(),
            &content_writes,
        )
        .await
        .expect("fold");
        assert_eq!(folded.response.outcome, FoldWalOutcome::Published);
        engine.record_manifest_published(folded.manifest);
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
    let content_writes = crate::test_support::content_writes();
    let (_directory, store, mut engine, context) = setup().await;
    let before = candidate(
        "before",
        vec![inline(&engine.namespace_id, Bytes::from_static(b"before"))],
    );
    publish(&mut engine, &store, &context, before)
        .await
        .expect("before fold");
    let input = engine.wal_fold_input().expect("fold input");
    engine.invalidate_projection();
    let folded = fold_wal_tail(
        &store,
        None,
        &engine.namespace_id,
        Some(input),
        &Deadline::start(Arc::new(StdMonotonicTimer::default())),
        crate::manifest::MetadataRunPolicy::default(),
        &content_writes,
    )
    .await
    .expect("fold");
    assert_eq!(folded.response.outcome, FoldWalOutcome::Published);
    engine.record_manifest_published(folded.manifest);
    NamespaceCommitEngine::new(
        engine.namespace_id.clone(),
        std::sync::Arc::new(crate::test_support::content_writes()),
    )
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
    assert_eq!(content_assemblies(store).len(), count);
    assert_eq!(
        operations[..first_metadata]
            .iter()
            .filter(|operation| assembles_content(operation))
            .count(),
        count
    );
}

#[tokio::test]
async fn failed_manifest_and_over_budget_retries_keep_materialized_content() {
    let content_writes = crate::test_support::content_writes();
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
        OperationSelector::Put,
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
            &crate::time::Deadline::start(Arc::new(StdMonotonicTimer::default())),
            crate::manifest::MetadataRunPolicy::default(),
            &content_writes,
        )
        .await,
        Err(CoreError::Store {
            kind: loonfs_objectstore::ObjectStoreErrorKind::PermissionDenied,
            ..
        })
    ));
    assert_content_before_metadata(&store, values.len());
    let keys = content_assemblies(&store);
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
    let timer = Arc::new(ManualClock::stepping(20 * 60 * 1000));
    assert!(matches!(
        fold_wal_tail(
            &store,
            None,
            &engine.namespace_id,
            Some(input.clone()),
            &crate::time::Deadline::start(timer.clone()),
            crate::manifest::MetadataRunPolicy::default(),
            &content_writes,
        )
        .await,
        Err(CoreError::MetadataPublicationBudgetExceeded { .. })
    ));
    assert_eq!(store.counts().puts, values.len());
    assert_eq!(store.counts().deletes, 0);
    assert_eq!(content_assemblies(&store).len(), values.len());
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
        crate::manifest::MetadataRunPolicy::default(),
        &content_writes,
    )
    .await
    .expect("retry");
    assert_eq!(folded.response.outcome, FoldWalOutcome::Published);
    assert_content_before_metadata(&store, values.len());
    let mut retry_keys = content_assemblies(&store);
    retry_keys.sort();
    let mut keys = keys;
    keys.sort();
    assert_eq!(retry_keys, keys);
}

/// Checks that two sessions' folds of the same tail write identical content
/// objects and publish one manifest, and that the fold of the older session
/// finds the tail folded after the second session's epoch claim.
#[tokio::test]
async fn competing_engines_materialize_identical_objects_and_publish_one_manifest() {
    let content_writes = crate::test_support::content_writes();
    let (_directory, store, mut first, context) = setup().await;
    let values = vec![inline(&first.namespace_id, Bytes::from_static(b"shared"))];
    let candidate = candidate("race", values);
    publish(&mut first, &store, &context, candidate.clone())
        .await
        .expect("publish");
    let mut second = NamespaceCommitEngine::new(
        first.namespace_id.clone(),
        std::sync::Arc::new(crate::test_support::content_writes()),
    );
    publish(&mut second, &store, &context, candidate)
        .await
        .expect("replay");
    let first_input = first.wal_fold_input().expect("first tail");
    let second_input = second.wal_fold_input().expect("second tail");
    assert_eq!(first_input.tail_state, second_input.tail_state);
    let basis = second_input.basis.manifest_no();
    let blocked = BlockingStore::new(
        store.clone(),
        KeyPredicate::content_blob(),
        OperationSelector::Put,
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
        crate::manifest::MetadataRunPolicy::default(),
        &content_writes,
    );
    let second_fold = async {
        blocked.wait_until_blocked().await;
        let result = fold_wal_tail(
            &store,
            None,
            &second.namespace_id,
            Some(second_input),
            &crate::time::Deadline::start(Arc::new(StdMonotonicTimer::default())),
            crate::manifest::MetadataRunPolicy::default(),
            &content_writes,
        )
        .await;
        blocked.release();
        result
    };
    let (first_result, second_result) = tokio::join!(first_fold, second_fold);
    assert_eq!(
        first_result.expect("first fold").response.outcome,
        FoldWalOutcome::AlreadyCurrent
    );
    assert_eq!(
        second_result.expect("second fold").response.outcome,
        FoldWalOutcome::Published
    );
    let keys = content_assemblies(&store);
    assert_eq!(keys.len(), 2);
    assert_eq!(keys[0], keys[1]);
    // The first fold's put loses to the second fold's manifest, and its next
    // attempt finds the tail folded.
    assert_eq!(
        store
            .snapshot()
            .iter()
            .filter(
                |operation| matches!(operation, RecordedOperation::Put { .. })
                    && family(operation) == Some(DurableObjectFamily::MetadataManifest)
            )
            .count(),
        2
    );
    assert_eq!(
        crate::namespace::control::load_current_manifest(&store, &first.namespace_id)
            .await
            .expect("current manifest")
            .state
            .manifest()
            .manifest_no,
        basis.successor().expect("next manifest number")
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
    assert_matches!(error, CoreError::NamespaceCorrupt(_));
    assert_eq!(store.counts().puts, 1);
    assert_eq!(content_assemblies(&store), vec![key.clone()]);
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
        OperationSelector::Put,
        InjectedError::Transport("assembly failure".to_owned()),
    )
    .apply_then_fail();
    failing.fail_all();
    store.reset();
    let error = fold_wal(&failing, &engine.namespace_id)
        .await
        .expect_err("assembly failure");
    assert_matches!(
        error,
        CoreError::Store {
            kind: loonfs_objectstore::ObjectStoreErrorKind::RetryableTransport,
            ..
        }
    );
    assert_eq!(store.counts().puts, 1);
    assert_eq!(content_assemblies(&store), vec![key]);
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
    let content_writes = crate::test_support::content_writes();
    let (_directory, store, mut engine, context) = setup().await;
    let before = candidate(
        "before",
        vec![inline(&engine.namespace_id, Bytes::from_static(b"before"))],
    );
    publish(&mut engine, &store, &context, before)
        .await
        .expect("before fold");
    let input = engine.wal_fold_input().expect("fold input");
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
            .push_object(&engine.namespace_id, &wal_object.payload().records)
            .expect("later rows");
    }
    let cached = engine
        .wal_fold_input()
        .expect("the tail with the commits since the fold began")
        .tail_state;
    let folded = fold_wal_tail(
        &store,
        None,
        &engine.namespace_id,
        Some(input.clone()),
        &Deadline::start(Arc::new(StdMonotonicTimer::default())),
        crate::manifest::MetadataRunPolicy::default(),
        &content_writes,
    )
    .await
    .expect("fold");
    assert_eq!(folded.response.outcome, FoldWalOutcome::Published);
    let observed = engine.projection_observed.clone();
    engine.record_manifest_published(folded.manifest.clone());
    let position = engine.publish_tail.as_ref().expect("retained position");
    assert_eq!(position.basis().0, folded.manifest.manifest);
    assert_eq!(position.head.unfolded_wal_objects(), 2);
    assert_eq!(position.head.folded_wal_no, input.head.wal_no);
    let reanchored = engine
        .wal_fold_input()
        .expect("the moved position keeps its tail")
        .tail_state;
    assert_eq!(reanchored.merged_rows(), expected.merged_rows());
    assert_eq!(reanchored.contents(), expected.contents());
    assert!(
        reanchored.chunks().len() == 2
            && reanchored
                .chunks()
                .iter()
                .zip(&cached.chunks()[cached.chunks().len() - 2..])
                .all(|(folded, cached)| Arc::ptr_eq(folded, cached)),
        "the fold tail shares the cached tail's chunks for the commits since the fold began"
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

/// Checks that a fold manifest that this runtime published moves the
/// engine's position together with its tail, with no cache involved: the
/// moved position keeps only the commit after the fold, its inline bytes
/// come from that tail, and reads and folds take the same tail.
#[tokio::test]
async fn a_published_fold_moves_the_position_with_its_tail() {
    let content_writes = crate::test_support::content_writes();
    let (_directory, store, mut engine, context) = setup().await;
    let folded_value = inline(&engine.namespace_id, Bytes::from_static(b"folded"));
    publish(
        &mut engine,
        &store,
        &context,
        candidate("folded", vec![folded_value]),
    )
    .await
    .expect("commit before the fold");
    let folded = fold_wal_tail(
        &store,
        None,
        &engine.namespace_id,
        None,
        &Deadline::start(Arc::new(StdMonotonicTimer::default())),
        crate::manifest::MetadataRunPolicy::default(),
        &content_writes,
    )
    .await
    .expect("a maintenance fold");
    let published = folded.published().expect("the fold publishes a manifest");
    let kept = inline(&engine.namespace_id, Bytes::from_static(b"kept"));
    publish(
        &mut engine,
        &store,
        &context,
        candidate("kept", vec![kept.clone()]),
    )
    .await
    .expect("commit after the fold");

    engine.record_manifest_published(published.clone());
    let moved = engine.read_position().expect("the position moved");
    assert_eq!(moved.basis.manifest(), &published.manifest);
    assert_eq!(moved.tail.chunks().len(), 1);
    assert_eq!(engine.wal_tail_objects(), Some(1));
    assert_eq!(engine.wal_tail_inline_bytes(), Some(kept.bytes().len()));
    let receipt =
        |commit_id| engine.retains_commit_receipt(&CommitId::parse(commit_id).expect("id"));
    assert!(receipt("kept") && !receipt("folded"));
    assert!(
        Arc::ptr_eq(
            &moved.tail,
            &engine.wal_fold_input().expect("a fold input").tail_state
        ),
        "reads and folds take the position's one tail"
    );
}
