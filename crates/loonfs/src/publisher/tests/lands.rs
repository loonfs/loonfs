//! Lands in the publisher's queue: each land is published in a WAL object of
//! its own, and a land queued behind another land from the same fork is
//! checked against the state that includes the first.

use super::*;
use loonfs_test_support::stores::RecordedOperation;
use loonfs_types::format::wal::LandRecord;
use loonfs_types::{InodeId, NamespaceId};
use std::collections::BTreeMap;
use std::num::NonZeroU32;

/// A land under `commit_id` of the fork's commits above 3, through 7, which
/// creates `/{name}` under an inode ID from the fork.
fn land(commit_id: &str, name: &str) -> CommitCandidate {
    CommitCandidate::new(create_directory_request(commit_id, name)).with_land(
        LandRecord {
            source_namespace_id: NamespaceId::parse("fork").expect("namespace"),
            source_after_seq: ChangeSeq(3),
            source_through_seq: ChangeSeq(7),
        },
        ChangeSeq(3),
        BTreeMap::from([(0, InodeId::generate(ChangeSeq(4), NonZeroU32::MIN))]),
    )
}

/// Checks that a land opens a batch of its own, that nothing joins it, and
/// that its WAL object contains the land alone. The second land repeats the
/// first's range, so it's refused against the state that includes the
/// first, and it writes no WAL object.
#[tokio::test]
async fn a_land_is_published_alone_in_its_wal_object() {
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
        land("land", "landed"),
        land("land-again", "landed-again"),
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
    assert_eq!(batches, [2, 1, 1, 2]);

    let mut results = Vec::new();
    for response in responses {
        results.push(response.await.expect("result"));
    }
    let seqs: Vec<_> = results
        .iter()
        .map(|result| result.as_ref().ok().map(|commit| commit.committed_seq.0))
        .collect();
    assert_eq!(seqs, [Some(2), Some(3), Some(4), None, Some(5), Some(6)]);
    assert_eq!(
        results[3]
            .as_ref()
            .map(|_| ())
            .expect_err("the same range again")
            .code(),
        ErrorCode::LandRangeConflict
    );
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
                    .map(|record| (record.committed_seq.0, record.land.is_some()))
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
