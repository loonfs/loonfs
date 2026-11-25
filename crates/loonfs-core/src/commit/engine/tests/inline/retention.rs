//! Receipt retention is independent of retained revisions and the history that snapshots keep.

use super::*;
use crate::authorize::{Authorizer, ReadAccess};
use crate::compaction::CompactionUnitOutcome;
use crate::test_support::WriterAt;
use loonfs_test_support::stores::MetadataMapStore;
use loonfs_types::format::control::SnapshotOwner;
use std::assert_matches;

fn replace(bytes: &'static [u8]) -> CommitCandidate {
    let value = inline(Bytes::from_static(bytes));
    CommitCandidate::with_inline_content(
        CommitIntent::single(
            CommitId::parse("reused").expect("commit id"),
            loonfs_test_support::test_actor(),
            None,
            FilesystemOperation::PutFile {
                path: AbsolutePath::parse("/file").expect("path"),
                content: loonfs_types::FileContent::Ref(value.content_ref().clone()),
                behavior: DestinationBehavior::Replace,
                expected_inode_id: None,
                expected_revision_no: None,
            },
        ),
        Vec::new(),
        vec![value],
    )
}

async fn compact_and_check_pair(
    store: &RecordingStore<LocalFsStore>,
    namespace_id: &NamespaceId,
    commit_id: &CommitId,
    seq: CommitSeq,
) {
    let mut fixture = crate::test_support::NamespaceFixture::new(
        store,
        namespace_id,
        &crate::test_support::writer_at("test-writer", 0),
    );
    fixture.namespace.run_policy = crate::test_support::one_delta_run_policy();
    let mut finished = false;
    for _ in 0..32 {
        let outcome = fixture
            .engine()
            .compact_from_current()
            .await
            .expect("compact a family group");
        let namespace = crate::test_support::namespace_store(store, namespace_id);
        let view = namespace
            .load_current_metadata_view()
            .await
            .expect("read each published manifest");
        let metadata = view.metadata_view();
        let receipt = metadata
            .find_commit_receipt(commit_id)
            .await
            .expect("receipt");
        let record = metadata.commit_at_seq(seq).await.expect("commit record");
        assert_eq!(receipt.is_some(), record.is_some());
        if matches!(outcome, CompactionUnitOutcome::NotNeeded) {
            finished = true;
            break;
        }
        assert_matches!(outcome, CompactionUnitOutcome::Published { .. });
    }
    assert!(finished, "small fixture did not finish compaction");
}

#[tokio::test]
async fn inline_receipt_retention_keeps_the_boundary_and_reuses_only_pruned_ids() {
    let (_directory, store, mut engine, context) = setup().await;
    let namespace_id = engine.namespace.namespace_id.clone();
    let filler = candidate("filler", vec![inline(Bytes::from_static(b"padding"))]);
    publish(&mut engine, &context, filler)
        .await
        .expect("first run");
    crate::test_support::namespace_store(&store, &namespace_id)
        .fold_wal()
        .await
        .expect("first fold");
    let request = replace(b"before");
    let old_reference = request.inline_content()[0].content_ref().clone();
    let original = publish(&mut engine, &context, request)
        .await
        .expect("original inline commit");
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let snapshot = namespace
        .create_snapshot_record(
            SnapshotOwner::User {
                name: Some("before reuse".into()),
                expires_at_ms: Some(u64::MAX),
                deleted_at_ms: None,
            },
            crate::test_support::folded_manifest(&namespace)
                .await
                .expect("fold the WAL tail"),
            &context.clock(),
        )
        .await
        .map(|(record, _)| record)
        .expect("take a snapshot of the original history");
    // The session advances the floor itself, through a floor object at its
    // tip, so it keeps its position for the replay below.
    let floor =
        crate::commit::engine::tests::advance_in_session(&mut engine, original.committed_seq).await;
    assert_eq!(
        floor.manifest().payload().retention_floor_seq,
        original.committed_seq
    );
    compact_and_check_pair(
        &store,
        &namespace_id,
        &original.commit_id,
        original.committed_seq,
    )
    .await;

    // A retry draws a new content ID but keeps the inline request identity.
    let retry = replace(b"before");
    assert_ne!(
        retry.inline_content()[0].content_ref().content_id,
        old_reference.content_id
    );
    engine.invalidate_projection();
    store.reset();
    assert_eq!(
        publish(&mut engine, &context, retry)
            .await
            .expect("boundary replay"),
        original
    );
    assert_no_writes(&store);

    let later = candidate("later", vec![inline(Bytes::from_static(b"later"))]);
    let later = publish(&mut engine, &context, later)
        .await
        .expect("advance history");
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    namespace.fold_wal().await.expect("fold later history");
    // Another process creates a floor object at the number after the
    // session's tip, so the session's own put finds the number taken, and
    // the session's advance ends as an advance without a session does.
    let planned = namespace.load_current_manifest().await.expect("planned");
    namespace
        .publish_floor_object(&planned, later.committed_seq)
        .await
        .expect("another process creates a floor object");
    assert_eq!(
        crate::commit::engine::tests::advance_in_session(&mut engine, later.committed_seq)
            .await
            .manifest()
            .payload()
            .retention_floor_seq,
        later.committed_seq
    );
    compact_and_check_pair(
        &store,
        &namespace_id,
        &original.commit_id,
        original.committed_seq,
    )
    .await;
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let current = namespace
        .load_current_metadata_view()
        .await
        .expect("compacted view");
    assert!(current
        .metadata_view()
        .find_commit_receipt(&original.commit_id)
        .await
        .expect("pruned receipt")
        .is_none());
    assert!(current
        .metadata_view()
        .commit_at_seq(original.committed_seq)
        .await
        .expect("pruned record")
        .is_none());
    assert_eq!(
        current
            .metadata_view()
            .content_layout(&old_reference.content_id)
            .await
            .expect("retained layout")
            .map(|head| head.committed_seq),
        Some(original.committed_seq)
    );
    drop(current);

    engine.invalidate_projection();
    let reused = publish(&mut engine, &context, replace(b"after"))
        .await
        .expect("a pruned ID can identify a new mutation");
    assert_eq!(reused.committed_seq, CommitSeq(later.committed_seq.0 + 1));
    assert_ne!(reused.events, original.events);
    crate::test_support::namespace_store(&store, &namespace_id)
        .fold_wal()
        .await
        .expect("fold reused ID");
    let aged = MetadataMapStore::aged(store.clone(), KeyPredicate::any());
    crate::test_support::namespace_store(&aged, &namespace_id)
        .gc(&WriterAt {
            now_ms: crate::limits::GC_GRACE_WINDOW_MS + 1,
            ..context.clone()
        }
        .clock())
        .await
        .expect("collect old WAL and segments that no snapshot keeps");
    assert!(crate::test_support::commit_wal_keys(&store, &namespace_id)
        .await
        .is_empty());

    engine.invalidate_projection();
    store.reset();
    assert_eq!(
        publish(&mut engine, &context, replace(b"after"))
            .await
            .expect("new receipt replay"),
        reused
    );
    assert_matches!(
        publish(&mut engine, &context, replace(b"before")).await,
        Err(CoreError::CommitIdReuseConflict {
            committed: Some(loonfs_types::CommittedReceipt { committed_seq: seq, .. }),
            ..
        }) if seq == reused.committed_seq
    );
    assert_no_writes(&store);

    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let current = namespace
        .load_current_metadata_view()
        .await
        .expect("fresh current view");
    let snapshot_basis =
        crate::test_support::NamespaceFixture::new(&store, &current.head().namespace_id, &context)
            .namespace
            .load_snapshot_read_basis(current.head(), &snapshot.snapshot_id)
            .await
            .expect("snapshot still keeps original history");
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let live = namespace
        .load_read_anchor(crate::test_support::observed_now())
        .await
        .expect("the live head");
    let old = namespace
        .load_metadata_view(&crate::RuntimeReadContext::at_snapshot(
            snapshot_basis,
            crate::RuntimeReadContext::new(&live.held_basis(), None).expect("a live namespace"),
        ))
        .await
        .expect("fresh snapshot view");
    let old_receipt = old
        .metadata_view()
        .find_commit_receipt(&original.commit_id)
        .await
        .expect("old receipt")
        .expect("retained by snapshot");
    assert_eq!(old_receipt.committed_seq, original.committed_seq);
    let old_record = old
        .metadata_view()
        .commit_at_seq(original.committed_seq)
        .await
        .expect("old record")
        .expect("retained with old receipt");
    assert!(
        old_record.inline_content.is_empty(),
        "folded records do not carry inline payloads"
    );
    let access = ReadAccess::live(Authorizer::Service);
    assert_eq!(
        old.read_file_bytes("/file", None, &access)
            .await
            .expect("snapshot bytes")
            .bytes
            .as_slice(),
        b"before"
    );
    assert_eq!(
        current
            .read_file_bytes("/file", None, &access)
            .await
            .expect("current bytes")
            .bytes
            .as_slice(),
        b"after"
    );
}
