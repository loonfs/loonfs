//! Receipt retention is independent of retained revisions and pinned history.

use super::*;
use crate::authorize::{Authorizer, ReadAccess};
use crate::gc::GcOptions;
use crate::manifest::{CompactionUnitOutcome, MetadataCompactionPolicy, RetentionTarget};
use crate::test_support::WriterAt;
use loonfs_test_support::stores::MetadataMapStore;
use loonfs_types::format::control::PinOwner;
use std::assert_matches;

fn replace(namespace_id: &NamespaceId, bytes: &'static [u8]) -> CommitCandidate {
    let value = inline(namespace_id, Bytes::from_static(bytes));
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
    seq: ChangeSeq,
) {
    let mut finished = false;
    for _ in 0..32 {
        let outcome = crate::test_support::NamespaceFixture::new(
            store,
            namespace_id,
            &crate::test_support::writer_at("test-writer", 0),
        )
        .engine()
        .compact_metadata_unit(
            None,
            MetadataCompactionPolicy::CompactImmediately,
            crate::manifest::CompactionAdmission::AnySize,
            loonfs_types::CompactorEpoch(0),
            &crate::manifest::MetadataCompactionCancellation::default(),
        )
        .await
        .expect("compact a family group");
        let namespace = crate::test_support::namespace_store(store, namespace_id);
        let view = namespace
            .load_current_metadata_view()
            .await
            .expect("read each published manifest");
        let metadata = view.projected_metadata_view();
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
    let filler = candidate(
        "filler",
        vec![inline(&namespace_id, Bytes::from_static(b"padding"))],
    );
    publish(&mut engine, &context, filler)
        .await
        .expect("first run");
    crate::test_support::namespace_store(&store, &namespace_id)
        .fold_wal()
        .await
        .expect("first fold");
    let request = replace(&namespace_id, b"before");
    let old_reference = request.inline_content()[0].content_ref().clone();
    let original = publish(&mut engine, &context, request)
        .await
        .expect("original inline commit");
    let snapshot = crate::test_support::namespace_store(&store, &namespace_id)
        .create_pin(
            PinOwner::User {
                name: "before reuse".into(),
                expires_at_ms: Some(u64::MAX),
            },
            &context.clock(),
        )
        .await
        .expect("pin the original history")
        .response;
    let floor = crate::test_support::NamespaceFixture::new(
        &store,
        &namespace_id,
        &crate::test_support::writer_at(
            "test-writer",
            crate::test_support::grace_elapsed_after(context.now_ms),
        ),
    )
    .engine()
    .advance_retention_floor(RetentionTarget::Head)
    .await
    .expect("floor exactly at original commit")
    .response;
    assert_eq!(floor.retention_floor_seq, original.committed_seq);
    compact_and_check_pair(
        &store,
        &namespace_id,
        &original.commit_id,
        original.committed_seq,
    )
    .await;

    // Staging changes the object ID but must preserve the inline request identity.
    let mut staged_retry = replace(&namespace_id, b"before");
    let identity = staged_retry.semantic_identity(&namespace_id);
    let inline_id = staged_retry.inline_content()[0]
        .content_ref()
        .content_id
        .clone();
    let stored = crate::test_support::namespace_store(&store, &namespace_id)
        .store_bytes_as_content(b"before")
        .await
        .expect("fallback content");
    staged_retry.stage_inline_content(
        &inline_id,
        PreparedContent::for_durable_content_write(stored.content_ref().clone()),
    );
    assert_eq!(staged_retry.semantic_identity(&namespace_id), identity);
    engine.invalidate_projection();
    store.reset();
    assert_eq!(
        publish(&mut engine, &context, staged_retry)
            .await
            .expect("boundary replay"),
        original
    );
    assert_no_writes(&store);

    let later = candidate(
        "later",
        vec![inline(&namespace_id, Bytes::from_static(b"later"))],
    );
    let later = publish(&mut engine, &context, later)
        .await
        .expect("advance history");
    crate::test_support::namespace_store(&store, &namespace_id)
        .fold_wal()
        .await
        .expect("fold later history");
    assert_eq!(
        crate::test_support::NamespaceFixture::new(
            &store,
            &namespace_id,
            &crate::test_support::writer_at(
                "test-writer",
                crate::test_support::grace_elapsed_after(context.now_ms)
            )
        )
        .engine()
        .advance_retention_floor(RetentionTarget::Head)
        .await
        .expect("advance floor")
        .response
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
        .projected_metadata_view()
        .find_commit_receipt(&original.commit_id)
        .await
        .expect("pruned receipt")
        .is_none());
    assert!(current
        .projected_metadata_view()
        .commit_at_seq(original.committed_seq)
        .await
        .expect("pruned record")
        .is_none());
    assert_eq!(
        current
            .projected_metadata_view()
            .content_layout(&old_reference.content_id)
            .await
            .expect("retained layout")
            .map(|head| head.committed_seq),
        Some(original.committed_seq)
    );
    drop(current);

    engine.invalidate_projection();
    let reused = publish(&mut engine, &context, replace(&namespace_id, b"after"))
        .await
        .expect("a pruned ID can identify a new mutation");
    assert_eq!(reused.committed_seq, ChangeSeq(later.committed_seq.0 + 1));
    assert_ne!(reused.events, original.events);
    crate::test_support::namespace_store(&store, &namespace_id)
        .fold_wal()
        .await
        .expect("fold reused ID");
    let aged = MetadataMapStore::aged(store.clone(), KeyPredicate::any());
    crate::test_support::namespace_store(&aged, &namespace_id)
        .gc(
            None,
            &GcOptions::default(),
            &WriterAt {
                now_ms: crate::limits::UNREFERENCED_SEGMENT_GRACE_WINDOW_MS + 1,
                ..context.clone()
            }
            .clock(),
        )
        .await
        .expect("collect old WAL and unpinned segments");
    assert!(store
        .list_prefix(&wal_prefix(&namespace_id))
        .await
        .expect("WAL listing")
        .is_empty());

    engine.invalidate_projection();
    store.reset();
    assert_eq!(
        publish(&mut engine, &context, replace(&namespace_id, b"after"))
            .await
            .expect("new receipt replay"),
        reused
    );
    assert_matches!(
        publish(&mut engine, &context, replace(&namespace_id, b"before")).await,
        Err(CoreError::CommitIdReuseConflict {
            committed: Some(loonfs_types::api::v0::CommittedReceipt { committed_seq: seq, .. }),
            ..
        }) if seq == reused.committed_seq
    );
    assert_no_writes(&store);

    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let current = namespace
        .load_current_metadata_view()
        .await
        .expect("fresh current view");
    let pinned =
        crate::test_support::NamespaceFixture::new(&store, &current.head().namespace_id, &context)
            .namespace
            .load_snapshot_read_basis(current.head(), &snapshot.pin_id)
            .await
            .expect("snapshot still pins original history");
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let old = namespace
        .load_metadata_view(&crate::RuntimeReadContext {
            read_basis: pinned,
            tail: None,
        })
        .await
        .expect("fresh snapshot view");
    let old_receipt = old
        .projected_metadata_view()
        .find_commit_receipt(&original.commit_id)
        .await
        .expect("old receipt")
        .expect("retained by snapshot");
    assert_eq!(old_receipt.committed_seq, original.committed_seq);
    let old_record = old
        .projected_metadata_view()
        .commit_at_seq(original.committed_seq)
        .await
        .expect("old record")
        .expect("retained with old receipt");
    assert!(
        old_record.inline_content.is_empty(),
        "folded records do not carry inline payloads"
    );
    let access = ReadAccess::live(Authorizer::Unrestricted);
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
