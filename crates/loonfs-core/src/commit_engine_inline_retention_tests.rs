//! Receipt retention is independent of retained revisions and pinned history.

use super::*;
use crate::authorize::{Authorizer, ReadAccess};
use crate::checkpoint::{
    advance_retention_floor, create_checkpoint, load_checkpoint_read_basis,
    reorganize_metadata_step, MetadataCompactionPolicy, MetadataLsmPolicy,
    MetadataReorganizeOutcome,
};
use crate::gc::{gc_namespace, GcConfig};
use crate::path::read::{load_metadata_view, ReadLoadContext};
use loonfs_api::wire::control::PinOwner;
use loonfs_test_support::stores::MetadataMapStore;

fn replace(namespace_id: &NamespaceId, bytes: &'static [u8]) -> CommitCandidate {
    let value = inline(namespace_id, Bytes::from_static(bytes));
    CommitCandidate::with_inline_content(
        CommitRequest::single(
            CommitId::parse("reused").expect("commit id"),
            loonfs_test_support::test_actor(),
            None,
            FilesystemOperation::PutFile {
                path: AbsolutePath::parse("/file").expect("path"),
                content_ref: Some(value.content_ref().clone()),
                inline_content: None,
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
        let outcome = reorganize_metadata_step(
            store,
            namespace_id,
            0,
            MetadataLsmPolicy::default(),
            MetadataCompactionPolicy::CompactImmediately,
        )
        .await
        .expect("compact a family group");
        let view = load_current_metadata_view(store, namespace_id)
            .await
            .expect("read each published manifest");
        let metadata = view.projected_metadata_view();
        let receipt = metadata
            .find_commit_receipt(commit_id)
            .await
            .expect("receipt");
        let record = metadata.commit_at_seq(seq).await.expect("commit record");
        assert_eq!(receipt.is_some(), record.is_some());
        if matches!(outcome, MetadataReorganizeOutcome::NotNeeded { .. }) {
            finished = true;
            break;
        }
        assert!(matches!(
            outcome,
            MetadataReorganizeOutcome::UnitPublished { .. }
        ));
    }
    assert!(finished, "small fixture did not finish compaction");
}

#[tokio::test]
async fn inline_receipt_retention_keeps_the_boundary_and_reuses_only_pruned_ids() {
    let (_directory, store, mut engine, context) = setup().await;
    let namespace_id = engine.namespace_id.clone();
    let filler = candidate(
        "filler",
        vec![inline(&namespace_id, Bytes::from_static(b"padding"))],
    );
    publish(&mut engine, &store, &context, filler)
        .await
        .expect("first run");
    flush_wal(&store, &namespace_id).await.expect("first fold");
    let request = replace(&namespace_id, b"before");
    let old_reference = request.inline_content()[0].content_ref().clone();
    let original = publish(&mut engine, &store, &context, request)
        .await
        .expect("original inline commit");
    let snapshot = create_checkpoint(
        &store,
        &namespace_id,
        PinOwner::Snapshot {
            name: "before reuse".into(),
            expires_at_ms: u64::MAX,
        },
        &context,
    )
    .await
    .expect("pin the original history");
    let floor = advance_retention_floor(&store, &namespace_id)
        .await
        .expect("floor exactly at original commit");
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
    let identity = staged_retry
        .semantic_identity(&namespace_id)
        .expect("identity");
    let inline_id = staged_retry.inline_content()[0]
        .content_ref()
        .content_id
        .clone();
    let stored = store_bytes_as_content(&store, &namespace_id, b"before")
        .await
        .expect("fallback content");
    staged_retry.stage_inline_content(
        &inline_id,
        PreparedContent::for_durable_content_write(stored.content_ref().clone()),
    );
    assert_eq!(
        staged_retry
            .semantic_identity(&namespace_id)
            .expect("staged identity"),
        identity
    );
    engine.invalidate_projection();
    store.reset();
    assert_eq!(
        publish(&mut engine, &store, &context, staged_retry)
            .await
            .expect("boundary replay"),
        original
    );
    assert_no_writes(&store);

    let later = candidate(
        "later",
        vec![inline(&namespace_id, Bytes::from_static(b"later"))],
    );
    let later = publish(&mut engine, &store, &context, later)
        .await
        .expect("advance history");
    flush_wal(&store, &namespace_id)
        .await
        .expect("fold later history");
    assert_eq!(
        advance_retention_floor(&store, &namespace_id)
            .await
            .expect("advance floor")
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
    let current = load_current_metadata_view(&store, &namespace_id)
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
            .find_content_publication(&old_reference.content_id)
            .await
            .expect("permanent publication"),
        Some(original.committed_seq)
    );
    drop(current);

    engine.invalidate_projection();
    let reused = publish(
        &mut engine,
        &store,
        &context,
        replace(&namespace_id, b"after"),
    )
    .await
    .expect("a pruned ID can identify a new mutation");
    assert_eq!(reused.committed_seq, ChangeSeq(later.committed_seq.0 + 1));
    assert_ne!(reused.events, original.events);
    flush_wal(&store, &namespace_id)
        .await
        .expect("fold reused ID");
    let aged = MetadataMapStore::aged(store.clone(), KeyPredicate::any());
    gc_namespace(
        &aged,
        &namespace_id,
        &GcConfig::default(),
        &MutationContext {
            now_ms: crate::limits::UNREFERENCED_SEGMENT_MIN_AGE_MS + 1,
            ..context.clone()
        },
    )
    .await
    .expect("collect old WAL and unpinned segments");
    assert!(store
        .list_prefix(&wal_segment_prefix(&namespace_id))
        .await
        .expect("WAL listing")
        .is_empty());

    engine.invalidate_projection();
    store.reset();
    assert_eq!(
        publish(
            &mut engine,
            &store,
            &context,
            replace(&namespace_id, b"after")
        )
        .await
        .expect("new receipt replay"),
        reused
    );
    assert!(matches!(
        publish(&mut engine, &store, &context, replace(&namespace_id, b"before")).await,
        Err(CoreError::CommitIdReuseConflict { committed_seq: Some(seq), .. }) if seq == reused.committed_seq
    ));
    assert_no_writes(&store);

    let current = load_current_metadata_view(&store, &namespace_id)
        .await
        .expect("fresh current view");
    let pinned = load_checkpoint_read_basis(&store, None, current.head(), &snapshot.pin_id)
        .await
        .expect("snapshot still pins original history");
    let old = load_metadata_view(
        &store,
        &namespace_id,
        ReadLoadContext::pinned_head(&pinned.head, &pinned.basis, None, None),
    )
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
        old.get_file_bytes(&store, "/file", None, &access)
            .await
            .expect("snapshot bytes")
            .bytes
            .as_slice(),
        b"before"
    );
    assert_eq!(
        current
            .get_file_bytes(&store, "/file", None, &access)
            .await
            .expect("current bytes")
            .bytes
            .as_slice(),
        b"after"
    );
}
