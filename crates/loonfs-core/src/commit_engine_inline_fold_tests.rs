//! Inline materialization ordering, failure, and concurrent flush contracts.

use super::*;
use crate::namespace::control::load_current_manifest;
use crate::storage::content::content_object_key_for_ref;
use loonfs_api::ErrorCode;
use loonfs_objectstore::layout::{parse_object_key, DurableObjectFamily};
use loonfs_objectstore::PutMode;
use loonfs_test_support::stores::{BlockingStore, FailStore, InjectedError, OperationClass};
use std::sync::atomic::{AtomicU64, Ordering};

fn family(operation: &RecordedOperation) -> Option<DurableObjectFamily> {
    parse_object_key(operation.key()).map(|key| key.family())
}

fn content_puts(store: &RecordingStore<LocalFsStore>) -> Vec<String> {
    store
        .snapshot()
        .iter()
        .filter_map(|operation| match operation {
            RecordedOperation::Put { key, .. }
                if family(operation) == Some(DurableObjectFamily::ContentBlob) =>
            {
                Some(key.clone())
            }
            _ => None,
        })
        .collect()
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
            .filter(|operation| {
                matches!(operation, RecordedOperation::Put { .. })
                    && family(operation) == Some(DurableObjectFamily::ContentBlob)
            })
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
            &StdMonotonicTimer::default()
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
    let timer = SteppingTimer(AtomicU64::new(0));
    assert!(matches!(
        fold_wal_tail(
            &store,
            None,
            &engine.namespace_id,
            Some(input.clone()),
            &timer
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
    let flushed = fold_wal_tail(
        &store,
        None,
        &engine.namespace_id,
        Some(input),
        &StdMonotonicTimer::default(),
    )
    .await
    .expect("retry");
    assert_eq!(flushed.outcome, FlushWalOutcome::Published);
    assert_content_before_metadata(&store, values.len());
    let mut retry_keys = content_puts(&store);
    retry_keys.sort();
    let mut keys = keys;
    keys.sort();
    assert_eq!(retry_keys, keys);
    for key in &keys {
        assert!(store.snapshot().iter().any(|operation| matches!(operation, RecordedOperation::GetWithMetadata { key: actual, .. } if actual == key)));
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
    let mut second = NamespaceCommitEngine::new(first.namespace_id.clone());
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
    let first_timer = StdMonotonicTimer::default();
    let first_flush = fold_wal_tail(
        &blocked,
        None,
        &first.namespace_id,
        Some(first_input),
        &first_timer,
    );
    let second_flush = async {
        blocked.wait_until_blocked().await;
        let result = fold_wal_tail(
            &store,
            None,
            &second.namespace_id,
            Some(second_input),
            &StdMonotonicTimer::default(),
        )
        .await;
        blocked.release();
        result
    };
    let (first_result, second_result) = tokio::join!(first_flush, second_flush);
    let first_result = first_result.expect("first flush");
    let second_result = second_result.expect("second flush");
    assert_eq!(first_result.outcome, FlushWalOutcome::AlreadyCurrent);
    assert_eq!(second_result.outcome, FlushWalOutcome::Published);
    assert_eq!(first_result.manifest_no, second_result.manifest_no);
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
    let error = flush_wal(&store, &engine.namespace_id)
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
        OperationClass::GetWithMetadata,
        InjectedError::Transport("readback failure".to_owned()),
    );
    failing.fail_all();
    store.reset();
    let error = flush_wal(&failing, &engine.namespace_id)
        .await
        .expect_err("readback failure");
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
