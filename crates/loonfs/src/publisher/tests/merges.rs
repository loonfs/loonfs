//! Merges in the publisher's queue: a merge's admission, one merge at a
//! time, and its publication in a WAL object of its own.

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
    CommitCandidate::new(create_directory_request(commit_id, name)).with_merge(
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
            WorkItem::Delete(_) => panic!("expected commit batch"),
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

/// Checks that the publisher admits and publishes a merge that fits the
/// merge allowance, although the merge is above the budget's byte total and
/// above its namespace's byte allowance, which refuses an ordinary commit of
/// about the same weight with `commit_queue_full`. A second merge while the
/// first is admitted gets `commit_queue_full`, and it's admitted once the
/// first settles. A merge above the allowance fails with `merge_too_large`.
#[tokio::test]
async fn a_merge_is_held_to_the_merge_allowance_and_admitted_one_at_a_time() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("main").expect("namespace");
    let store: SharedStore = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store"));
    let runtime = test_runtime(store);
    create_namespace(&runtime, &namespace_id).await;
    let admission_bytes = |candidate: CommitCandidate| {
        PreparedCandidate::new(candidate)
            .expect("prepare")
            .estimated_retained_bytes
            + 512
            + namespace_id.as_str().len()
    };
    let merge_bytes = admission_bytes(merge("merge", "merged"));
    let publisher_with_allowance = |max_merge_bytes: usize| {
        let mut core = TestCore::unpaced();
        let below_the_merge = NonZeroUsize::new(merge_bytes - 1).expect("limit");
        core.config.publication.max_estimated_bytes_per_namespace = below_the_merge;
        core.execution_budget = ExecutionBudget::builder()
            .max_admitted_bytes(below_the_merge)
            .max_merge_bytes(NonZeroUsize::new(max_merge_bytes).expect("allowance"))
            .build();
        standalone_publisher_with(&namespace_id, &runtime, core)
    };

    let publisher = publisher_with_allowance(merge_bytes);
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
    let second = || merge_through("later", "second", 7, 9);
    assert_matches!(
        try_admit_candidate(&publisher, &namespace_id, second()),
        Err(CoreError::CommitQueueFull)
    );
    assert!(recv_commit(first, "merge").await.merge.is_some());
    publisher.wait_for_worker().await;
    let next = recv_commit(
        try_admit_candidate(&publisher, &namespace_id, second())
            .expect("admit the second merge once the first settles"),
        "later",
    )
    .await;
    assert!(next.merge.is_some());
    publisher.wait_for_worker().await;

    let smaller = publisher_with_allowance(merge_bytes - 1);
    assert_matches!(
        try_admit_candidate(&smaller, &namespace_id, merge("merge", "merged")),
        Err(CoreError::MergeTooLarge { estimated_bytes, max_bytes })
            if estimated_bytes == merge_bytes && max_bytes == merge_bytes - 1
    );
}
