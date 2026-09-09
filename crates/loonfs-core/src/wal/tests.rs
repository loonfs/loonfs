//! Numbered WAL verification and fence reclamation contracts.

use crate::namespace::{
    bootstrap::bootstrap_namespace, control::load_current_manifest,
    writer_epoch::acquire_writer_epoch,
};
use crate::path::read::load_current_metadata_view;
use loonfs_api::wire::wal::{decode_wal_segment_envelope_zstd, encode_wal_segment_envelope_zstd};
use loonfs_api::{ChangeSeq, ErrorCode, InodeId, NamespaceId, WalNo, WriterEpoch, WriterId};
use loonfs_objectstore::{keys::wal_segment, local_fs_store::LocalFsStore, ObjectStore};
use loonfs_test_support::stores::{KeyPredicate, MetadataMapStore};

fn context(now_ms: u64) -> crate::MutationContext {
    crate::MutationContext {
        writer_id: WriterId::parse("writer").expect("writer"),
        now_ms,
    }
}

#[tokio::test]
async fn readers_reject_invalid_numbers_epochs_sequences_and_allocation_summaries() {
    let directory = tempfile::tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let namespace_id = NamespaceId::parse("verification").expect("namespace");
    bootstrap_namespace(&store, &namespace_id, &context(1_000), false)
        .await
        .expect("create");
    for _ in 0..2 {
        acquire_writer_epoch(&store, &namespace_id, &context(1_000))
            .await
            .expect("fence");
    }
    let key = wal_segment(&namespace_id, &WalNo(2));
    let original = decode_wal_segment_envelope_zstd(
        &store.get(&key, None).await.expect("get").expect("fence"),
    )
    .expect("decode");
    for changed in 0..5 {
        let mut payload = original.payload().clone();
        match changed {
            0 => payload.wal_no = WalNo(1),
            1 => payload.writer_epoch = WriterEpoch(0),
            2 => payload.writer_epoch = WriterEpoch(3),
            3 => payload.end_seq = ChangeSeq(1),
            _ => payload.next_inode_id = InodeId(3),
        }
        let bytes = encode_wal_segment_envelope_zstd(payload)
            .expect("encode")
            .into_bytes();
        store
            .put_overwrite(&key, bytes.into())
            .await
            .expect("corrupt fixture");
        let error = load_current_metadata_view(&store, &namespace_id)
            .await
            .err()
            .expect("corruption");
        assert_eq!(
            error.code(),
            ErrorCode::NamespaceCorrupt,
            "case {changed}: {error}"
        );
    }
}

#[tokio::test]
async fn fences_fold_and_reclaim_by_both_wal_numbers_without_advancing_sequence() {
    let directory = tempfile::tempdir().expect("directory");
    let store = MetadataMapStore::aged(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    let namespace_id = NamespaceId::parse("fences").expect("namespace");
    let setup = context(1_000);
    bootstrap_namespace(&store, &namespace_id, &setup, false)
        .await
        .expect("create");
    acquire_writer_epoch(&store, &namespace_id, &setup)
        .await
        .expect("first fence");
    crate::checkpoint::flush_wal(&store, &namespace_id, &setup)
        .await
        .expect("fold fence");
    crate::checkpoint::advance_retention_floor(&store, &namespace_id, &setup)
        .await
        .expect("advance WAL floor at sequence zero");
    acquire_writer_epoch(&store, &namespace_id, &setup)
        .await
        .expect("second fence");
    let current = load_current_manifest(&store, &namespace_id)
        .await
        .expect("manifest");
    assert_eq!(current.envelope.payload().head_seq, ChangeSeq(0));
    assert_eq!(current.envelope.payload().last_folded_wal_no, WalNo(1));
    assert_eq!(current.envelope.payload().retention_floor_wal_no, WalNo(1));
    let config = crate::gc::GcConfig {
        grace_window_ms: crate::limits::GC_MIN_GRACE_WINDOW_MS,
    };
    let aged = context(config.grace_window_ms + 1);
    let report = crate::gc::gc_namespace(&store, &namespace_id, &config, &aged)
        .await
        .expect("collect");
    assert_eq!(report.deleted.wal_segments, 1);
    assert!(store
        .head(&wal_segment(&namespace_id, &WalNo(1)))
        .await
        .expect("first")
        .is_none());
    assert!(store
        .head(&wal_segment(&namespace_id, &WalNo(2)))
        .await
        .expect("second")
        .is_some());
    crate::checkpoint::flush_wal(&store, &namespace_id, &setup)
        .await
        .expect("fold second fence");
    let retained = crate::gc::gc_namespace(&store, &namespace_id, &config, &aged)
        .await
        .expect("floor retains");
    assert_eq!(retained.deleted.wal_segments, 0);
    crate::checkpoint::advance_retention_floor(&store, &namespace_id, &setup)
        .await
        .expect("advance second floor");
    let reclaimed = crate::gc::gc_namespace(&store, &namespace_id, &config, &aged)
        .await
        .expect("reclaim second fence");
    assert_eq!(reclaimed.deleted.wal_segments, 1);
    load_current_metadata_view(&store, &namespace_id)
        .await
        .expect("genesis remains readable");
}

#[tokio::test]
async fn a_same_sequence_writer_acquisition_does_not_cover_a_fence_flush() {
    use loonfs_test_support::stores::{BlockingStore, OperationClass};
    let directory = tempfile::tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("flush-race").expect("namespace");
    let store = BlockingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::exact(loonfs_objectstore::keys::metadata_manifest_object(
            &namespace_id,
            &loonfs_api::ManifestNo(3),
        )),
        OperationClass::PutCreateIfAbsent,
    );
    let setup = context(1_000);
    bootstrap_namespace(&store, &namespace_id, &setup, false)
        .await
        .expect("create");
    acquire_writer_epoch(&store, &namespace_id, &setup)
        .await
        .expect("first fence");
    store.block_next();
    let (flushed, acquired) = futures::join!(
        crate::checkpoint::flush_wal(&store, &namespace_id, &setup),
        async {
            store.wait_until_blocked().await;
            let acquired = acquire_writer_epoch(store.inner(), &namespace_id, &setup).await;
            store.release();
            acquired
        }
    );
    acquired.expect("second fence");
    flushed.expect("flush retries");
    let current = load_current_manifest(&store, &namespace_id)
        .await
        .expect("manifest");
    assert_eq!(current.envelope.payload().head_seq, ChangeSeq(0));
    assert_eq!(current.envelope.payload().last_folded_wal_no, WalNo(2));
}
