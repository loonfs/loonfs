//! Merges in the publisher's queue: a merge's admission, merges that share
//! the merge allowance, and a merge's publication in a WAL object of its own.

use super::*;
use loonfs_test_support::stores::RecordedOperation;
use loonfs_types::format::wal::MergeRecord;
use loonfs_types::{InodeId, NamespaceId};
use std::collections::BTreeMap;
use std::num::NonZeroU32;

/// A merge under `commit_id` of the fork's commits above 3, through 7, which
/// creates `/{name}` under an inode ID from the fork.
fn merge(commit_id: &str, name: &str) -> CommitCandidate {
    merge_through(commit_id, name, 3, 7)
}

/// A merge under `commit_id` of the fork's commits above `after_seq`, through
/// `through_seq`, from a fork made at 3, which creates `/{name}` under an
/// inode ID that the fork's commit after `after_seq` made.
fn merge_through(commit_id: &str, name: &str, after_seq: u64, through_seq: u64) -> CommitCandidate {
    loonfs_core::test_support::merge_candidate(
        CommitCandidate::new(create_directory_request(commit_id, name)),
        MergeRecord {
            source_namespace_id: NamespaceId::parse("fork").expect("namespace"),
            source_after_seq: ChangeSeq(after_seq),
            source_through_seq: ChangeSeq(through_seq),
        },
        ChangeSeq(3),
        BTreeMap::from([(
            0,
            InodeId::generate(ChangeSeq(after_seq + 1), NonZeroU32::MIN),
        )]),
    )
}

/// Checks that a merge opens a batch of its own, that nothing joins it, and
/// that its WAL object contains the merge alone.
#[tokio::test]
async fn a_merge_is_published_alone_in_its_wal_object() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("main").expect("namespace");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::prefix(wal_prefix(&namespace_id)),
    ));
    let runtime = test_runtime(store.clone());
    create_namespace(&runtime, &namespace_id).await;
    let publisher = standalone_publisher_with(&namespace_id, &runtime, TestCore::unpaced());
    recv_commit(
        admit_commit(
            &publisher,
            &namespace_id,
            create_directory_request("warmup", "warmup"),
        ),
        "warmup",
    )
    .await;
    store.reset();

    let mut responses = Vec::new();
    for candidate in [
        CommitCandidate::new(create_directory_request("before-1", "before-1")),
        CommitCandidate::new(create_directory_request("before-2", "before-2")),
        merge("merge", "merged"),
        CommitCandidate::new(create_directory_request("after-1", "after-1")),
        CommitCandidate::new(create_directory_request("after-2", "after-2")),
    ] {
        responses.push(try_admit_candidate(&publisher, &namespace_id, candidate).expect("admit"));
    }
    let batches = publisher_state(&publisher)
        .queue
        .iter()
        .map(|item| match item {
            WorkItem::Batch(batch) => batch.candidates.len(),
            WorkItem::Delete(_) | WorkItem::RetentionAdvance(_) => {
                panic!("expected commit batch")
            }
        })
        .collect::<Vec<_>>();
    assert_eq!(batches, [2, 1, 2]);

    let mut seqs = Vec::new();
    for response in responses {
        seqs.push(
            response
                .await
                .expect("result")
                .expect("commit")
                .committed_seq
                .0,
        );
    }
    assert_eq!(seqs, [2, 3, 4, 5, 6]);
    let mut objects = Vec::new();
    for operation in store.snapshot() {
        if let RecordedOperation::Put { key, .. } = operation {
            let bytes = store.get(&key, None).await.expect("get WAL").expect("WAL");
            let payload = decode_wal_object_envelope_zstd(&bytes)
                .expect("decode WAL")
                .into_payload();
            objects.push(
                payload
                    .records
                    .iter()
                    .map(|record| (record.committed_seq.0, record.merge.is_some()))
                    .collect::<Vec<_>>(),
            );
        }
    }
    assert_eq!(
        objects,
        [
            vec![(2, false), (3, false)],
            vec![(4, true)],
            vec![(5, false), (6, false)]
        ]
    );
    publisher.wait_for_worker().await;
}

/// Checks that the publisher admits and publishes a merge whose estimated
/// peak, three times its planned WAL bytes, fits the merge allowance,
/// although the merge is above the budget's byte total and above its
/// namespace's byte allowance, which refuse a lighter ordinary commit with
/// `commit_queue_full`. Two merges whose reservations fit the allowance are
/// admitted at once, and both commit. A merge whose peak is above the
/// allowance fails with `merge_too_large`, which reports the peak and the
/// allowance.
#[tokio::test]
async fn merges_that_fit_the_allowance_are_admitted_together() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("main").expect("namespace");
    let store: SharedStore = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store"));
    let runtime = test_runtime(store);
    create_namespace(&runtime, &namespace_id).await;
    let prepared = PreparedCandidate::new(merge("merge", "merged"));
    let ordinary = CommitCandidate::new(create_directory_request("ordinary", "ordinary"));
    assert!(ordinary.estimated_retained_bytes() < prepared.estimated_retained_bytes);
    let ordinary_bytes = ordinary.estimated_retained_bytes() + 512 + namespace_id.as_str().len();
    let merge_peak = 3 * prepared.wal_record_bytes_upper_bound;
    let publisher_with_allowance = |max_merge_bytes: usize| {
        let mut core = TestCore::unpaced();
        let below_the_merge = NonZeroUsize::new(ordinary_bytes - 1).expect("limit");
        core.config.publication.max_estimated_bytes_per_namespace = below_the_merge;
        core.execution_budget = ExecutionBudget::builder()
            .max_admitted_bytes(below_the_merge)
            .max_merge_bytes(max_merge_bytes)
            .build();
        let budget = core.execution_budget.clone();
        (
            standalone_publisher_with(&namespace_id, &runtime, core),
            budget,
        )
    };

    let allowance = 2 * super::super::admission::MIN_MERGE_RESERVED_BYTES;
    let (publisher, budget) = publisher_with_allowance(allowance);
    assert_matches!(
        try_admit_commit(
            &publisher,
            &namespace_id,
            create_directory_request("ordinary", "ordinary")
        ),
        Err(CoreError::CommitQueueFull)
    );
    let first = try_admit_candidate(&publisher, &namespace_id, merge("merge", "merged"))
        .expect("admit a merge above the namespace allowance and the byte total");
    let second = try_admit_candidate(
        &publisher,
        &namespace_id,
        merge_through("later", "second", 7, 9),
    )
    .expect("admit a second merge beside the first");
    assert_eq!(budget.stats().admitted_merges, 2);
    assert_eq!(budget.stats().admitted_merge_bytes, allowance);
    assert!(recv_commit(first, "merge").await.merge.is_some());
    assert!(recv_commit(second, "later").await.merge.is_some());
    publisher.wait_for_worker().await;
    assert_eq!(budget.stats().admitted_merge_bytes, 0);

    let (smaller, _) = publisher_with_allowance(merge_peak - 1);
    assert_matches!(
        try_admit_candidate(&smaller, &namespace_id, merge("merge", "merged")),
        Err(CoreError::MergeTooLarge {
            bound: loonfs_core::MergeBound::MergeAllowance,
            estimated_bytes,
            max_bytes,
            split_at_seq: None,
        }) if estimated_bytes == merge_peak && max_bytes == merge_peak - 1
    );
}

/// Checks that a retry of a merge whose commit is still in flight is
/// admitted beside the merge and returns the merge's commit, and that a
/// merge of the same range under another commit ID fails the writer's range
/// check, so the range is applied once.
#[tokio::test]
async fn a_retry_of_a_merge_in_flight_returns_the_merge() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("main").expect("namespace");
    let store: SharedStore = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store"));
    let runtime = test_runtime(store);
    create_namespace(&runtime, &namespace_id).await;
    let publisher = standalone_publisher_with(&namespace_id, &runtime, TestCore::unpaced());

    let mut first = Box::pin(publisher.submit(merge("merge", "merged")));
    assert!(
        futures::poll!(first.as_mut()).is_pending(),
        "the merge is admitted"
    );
    let retry = publisher
        .submit(merge("merge", "merged"))
        .await
        .expect("the retry");
    let first = first.await.expect("the merge");
    assert_eq!(retry, first);
    let error = publisher
        .submit(merge("other", "merged"))
        .await
        .expect_err("the range was applied");
    assert_eq!(error.code(), crate::ErrorCode::MergeRangeConflict);
    publisher.wait_for_worker().await;
}
