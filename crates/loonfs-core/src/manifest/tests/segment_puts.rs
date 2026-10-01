//! Segment write concurrency, ordering, and publication failures.

use super::*;
use crate::manifest::block_load::DEFAULT_BLOCK_MEMO_BYTES;
use crate::manifest::row::manifest_rows_for_family;
use crate::manifest::runs::MANIFEST_ROW_FAMILIES;
use crate::namespace::control::load_current_manifest;
use crate::namespace::writer_epoch::acquire_writer_epoch;
use crate::store_waves::STORE_WRITE_WAVE;
use crate::test_support::ops::{create, write_file_bytes};
use crate::time::StdMonotonicTimer;
use crate::MutationContext;
use loonfs_api::wire::manifest::{
    decode_namespace_manifest_json, MetadataRow, MetadataRowFamily, MetadataSegmentRef,
    NamespaceManifestEnvelope,
};
use loonfs_api::wire::sst_blocks::{
    decode_data_block, decode_filter_block, decode_index_block, BlockHandle,
};
use loonfs_api::WriterId;
use loonfs_objectstore::keys::{
    metadata_manifest_object, metadata_manifest_prefix, metadata_segment_object_key,
    metadata_segment_prefix,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::stores::{
    BlockingStore, ConcurrencyWatchStore, FailStore, InjectedError, KeyPredicate, OperationKind,
    RecordedOperation, RecordingStore,
};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use tempfile::tempdir;

async fn seed_tail(store: &LocalFsStore, namespace_id: &NamespaceId) {
    let context = MutationContext {
        writer_id: WriterId::parse("segment-writes").expect("writer id"),
        now_ms: 1_000,
    };
    create(store, namespace_id, &context)
        .await
        .expect("namespace");
    acquire_writer_epoch(store, namespace_id, &context)
        .await
        .expect("writer");
    for index in 0..36 {
        write_file_bytes(
            store,
            namespace_id,
            &format!("/file-{index:03}"),
            b"content",
            &context,
            None,
        )
        .await
        .expect("file");
    }
}

fn section<'a>(bytes: &'a [u8], handle: &BlockHandle) -> &'a [u8] {
    let start = handle.offset as usize;
    &bytes[start..start + handle.stored_bytes as usize]
}

async fn verify_segments<S: ObjectStore>(
    store: &S,
    descriptors: &[MetadataSegmentRef],
    expected: &[(MetadataRowFamily, Vec<MetadataRow>)],
) {
    for (descriptor, (family, expected_rows)) in descriptors.iter().zip(expected) {
        assert_eq!(descriptor.family, *family);
        assert_eq!(descriptor.row_count, expected_rows.len() as u64);
        assert_eq!(
            descriptor.min_row_key,
            expected_rows[0].row_key_for_family(*family)
        );
        assert_eq!(
            descriptor.max_row_key,
            expected_rows
                .last()
                .expect("rows")
                .row_key_for_family(*family)
        );
        let bytes = store
            .get(&metadata_segment_object_key(descriptor), None)
            .await
            .expect("get segment")
            .expect("segment exists before publication");
        decode_filter_block(
            section(&bytes, &descriptor.filter_block),
            &descriptor.filter_block,
        )
        .expect("verified filter");
        let index = decode_index_block(
            section(&bytes, &descriptor.index_block),
            &descriptor.index_block,
        )
        .expect("verified index");
        let rows = index
            .iter()
            .flat_map(|entry| {
                decode_data_block(section(&bytes, &entry.block), &entry.block)
                    .expect("verified data")
                    .rows
            })
            .collect::<Vec<MetadataRow>>();
        assert_eq!(&rows, expected_rows);
    }
}

#[tokio::test]
async fn a_fold_puts_segments_in_a_bounded_wave_before_publishing_in_builder_order() {
    for rows_per_segment in [2, usize::MAX] {
        let directory = tempdir().expect("tempdir");
        let namespace_id = NamespaceId::parse("segment-wave").expect("namespace id");
        let local = LocalFsStore::new(directory.path()).expect("store");
        seed_tail(&local, &namespace_id).await;
        let captured = Arc::new(Mutex::new(None::<NamespaceManifestEnvelope>));
        let manifest_prefix = metadata_manifest_prefix(&namespace_id);
        let store = BlockingStore::matching(
            ConcurrencyWatchStore::new(
                RecordingStore::new(local, KeyPredicate::any()),
                KeyPredicate::metadata_segment(),
            ),
            {
                let captured = Arc::clone(&captured);
                move |operation| {
                    if operation.key().starts_with(&manifest_prefix) {
                        if let OperationKind::Put { bytes, .. } = operation.kind() {
                            *captured.lock().expect("capture lock") =
                                Some(decode_namespace_manifest_json(bytes).expect("manifest"));
                            return true;
                        }
                    }
                    false
                }
            },
        );
        let projection = load_manifest_projection(&store, &namespace_id, DEFAULT_BLOCK_MEMO_BYTES)
            .await
            .expect("projection");
        let expected = MANIFEST_ROW_FAMILIES
            .into_iter()
            .flat_map(|family| {
                manifest_rows_for_family(&projection.tail_state.rows, family)
                    .chunks(rows_per_segment)
                    .map(|rows| (family, rows.to_vec()))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        if rows_per_segment == 2 {
            assert!(expected.len() > STORE_WRITE_WAVE);
            for family in MANIFEST_ROW_FAMILIES {
                let count = expected
                    .iter()
                    .filter(|(actual, _)| *actual == family)
                    .count();
                assert!(count == 0 || count > STORE_WRITE_WAVE);
            }
        } else {
            assert_eq!(expected.len(), 7);
        }
        store.inner().inner().reset();
        store.block_next();
        let deadline = Deadline::start(Arc::new(StdMonotonicTimer::default()));
        let (result, verified_manifest) = tokio::try_join!(
            try_fold_wal_projection(
                &store,
                &namespace_id,
                &projection,
                &deadline,
                MetadataLsmPolicy {
                    max_rows_per_segment: NonZeroUsize::new(rows_per_segment).expect("row limit"),
                    ..MetadataLsmPolicy::default()
                }
            ),
            async {
                store.wait_until_blocked().await;
                let manifest = captured
                    .lock()
                    .expect("capture lock")
                    .clone()
                    .expect("manifest captured");
                let manifest_key =
                    metadata_manifest_object(&namespace_id, &manifest.payload().manifest_no);
                let descriptors = &manifest.payload().runs[0].segments;
                assert_eq!(descriptors.len(), expected.len());
                let concurrency = store.inner().puts();
                assert!(concurrency.peak_in_flight > 1);
                assert!(concurrency.peak_in_flight <= STORE_WRITE_WAVE);
                assert_eq!(concurrency.total, expected.len());
                assert!(!store.inner().inner().snapshot().iter().any(|operation| {
                    matches!(operation, RecordedOperation::Put { key, .. } if key == &manifest_key)
                }));
                verify_segments(&store, descriptors, &expected).await;
                store.release();
                Ok(manifest)
            }
        )
        .expect("fold");
        assert!(
            matches!(result, TryFoldWal::Settled(basis) if basis.outcome == FoldWalOutcome::Published)
        );
        let published = load_current_manifest(&store, &namespace_id)
            .await
            .expect("published manifest");
        assert_eq!(
            published.state.envelope.payload(),
            verified_manifest.payload()
        );
        let operations = store.inner().inner().snapshot();
        let manifest_key =
            metadata_manifest_object(&namespace_id, &verified_manifest.payload().manifest_no);
        let publication = operations.iter().position(|operation| matches!(operation, RecordedOperation::Put { key, .. } if key == &manifest_key)).expect("manifest put recorded");
        for descriptor in &verified_manifest.payload().runs[0].segments {
            let key = metadata_segment_object_key(descriptor);
            assert!(operations[..publication].iter().any(|operation| matches!(operation, RecordedOperation::Put { key: actual, .. } if actual == &key)));
            assert!(operations[..publication].iter().any(|operation| matches!(operation, RecordedOperation::Get { key: actual, .. } if actual == &key)));
        }
    }
}

#[tokio::test]
async fn a_failed_segment_put_prevents_fold_publication() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("failed-segment-wave").expect("namespace id");
    let local = LocalFsStore::new(directory.path()).expect("store");
    seed_tail(&local, &namespace_id).await;
    let before = load_current_manifest(&local, &namespace_id)
        .await
        .expect("current manifest");
    let segment_prefix = metadata_segment_prefix(&namespace_id);
    let puts = AtomicUsize::new(0);
    let failing = FailStore::matching(
        local,
        move |operation| {
            operation.key().starts_with(&segment_prefix)
                && matches!(operation.kind(), OperationKind::Put { .. })
                && puts.fetch_add(1, Ordering::SeqCst) == STORE_WRITE_WAVE
        },
        InjectedError::PermissionDenied("segment put refused".to_owned()),
    );
    failing.fail_next(1);
    let store = RecordingStore::new(
        ConcurrencyWatchStore::new(failing, KeyPredicate::metadata_segment()),
        KeyPredicate::any(),
    );
    let projection = load_manifest_projection(&store, &namespace_id, DEFAULT_BLOCK_MEMO_BYTES)
        .await
        .expect("projection");
    store.reset();
    let result = try_fold_wal_projection(
        &store,
        &namespace_id,
        &projection,
        &Deadline::start(Arc::new(StdMonotonicTimer::default())),
        MetadataLsmPolicy {
            max_rows_per_segment: NonZeroUsize::new(2).expect("row limit"),
            ..MetadataLsmPolicy::default()
        },
    )
    .await;
    assert!(matches!(
        result,
        Err(CoreError::Store {
            class: crate::error::StoreFailureClass::PermissionDenied,
            ..
        })
    ));
    assert_eq!(store.inner().inner().remaining(), 0);
    assert!(store.inner().puts().total > STORE_WRITE_WAVE);
    assert!(!store.snapshot().iter().any(|operation| matches!(operation, RecordedOperation::Put { key, .. } if !key.starts_with(&metadata_segment_prefix(&namespace_id)))));
    let after = load_current_manifest(&store, &namespace_id)
        .await
        .expect("unchanged manifest");
    assert_eq!(
        after.state.envelope.payload(),
        before.state.envelope.payload()
    );
}
