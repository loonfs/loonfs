//! Complete collection across snapshot and upload families.

use super::*;
use loonfs_objectstore::keys::{
    content_prefix, extensions_prefix, snapshot_record, temporary_prefix, upload_outcome,
    upload_session_prefix,
};
use loonfs_test_support::stores::RecordedOperation;
use loonfs_types::format::control::UploadOutcome;

#[tokio::test]
async fn one_collection_deletes_an_aged_upload_and_every_expired_snapshot_among_many_snapshots() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("many-snapshots").expect("namespace");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::new({
            let prefix = loonfs_objectstore::keys::namespace_prefix(&namespace_id);
            move |key| key.starts_with(&prefix) || key.starts_with("pools/")
        }),
    );
    let setup = context(1_000);
    let pool_id = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap")
        .pool_id;
    let permanent = create_snapshot(&store, &namespace_id, &setup)
        .await
        .expect("permanent snapshot");
    let basis = crate::test_support::namespace_store(&store, &namespace_id)
        .load_snapshot_record(&permanent.snapshot_id)
        .await
        .expect("snapshot")
        .expect("record")
        .state;
    for number in 0..1025 {
        let record = SnapshotPayload {
            snapshot_id: SnapshotId::parse(format!(
                "snp_{:020}-{number:026x}",
                basis.snapshot_id.manifest_no().0
            ))
            .expect("snapshot id"),
            owner: SnapshotOwner::User {
                name: "expired".to_owned(),
                expires_at_ms: Some(2_000),
            },
            ..basis.clone()
        };
        crate::snapshot::record::write_snapshot_record(&store, &record)
            .await
            .expect("expired snapshot");
    }
    let session = crate::test_support::proxied_upload_session(&namespace_id, 2_000);
    let session_key = crate::test_support::write_upload_records(
        &store,
        &pool_id,
        &session,
        Some(UploadOutcome::Aborted {
            aborted_at_ms: 2_000,
        }),
    )
    .await;
    store.reset();
    let now = context(2_000 + GRACE_MS);
    let report = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(&options(), &now.clock())
        .await
        .expect("complete collection");
    assert_eq!(report.deleted_snapshots_by_owner.user, 1025);
    assert_eq!(report.retained.snapshot_not_deletable, 1);
    assert_eq!(store.counts().deletes, 1025);
    let operations = store.take();
    assert_eq!(
        operations
            .iter()
            .filter(|operation| matches!(
                operation,
                RecordedOperation::GetWithMetadata { key, .. }
                    if key.starts_with(&snapshot_prefix(&namespace_id))
            ))
            .count(),
        1026
    );
    let listings: Vec<_> = operations
        .into_iter()
        .filter_map(|operation| match operation {
            RecordedOperation::List { prefix } => Some(prefix),
            _ => None,
        })
        .collect();
    assert_eq!(
        listings,
        vec![
            snapshot_prefix(&namespace_id),
            metadata_manifest_prefix(&namespace_id),
            wal_prefix(&namespace_id),
            metadata_segment_prefix(&namespace_id),
            extensions_prefix(&namespace_id),
            snapshot_prefix(&namespace_id),
        ]
    );
    store.reset();
    let report = collect_pool(&store, &pool_id, &now).await;
    assert_eq!(report.deleted.upload_sessions, 1);
    // The session's two records, and the cleanup of its unpublished content.
    assert_eq!(store.counts().deletes, 3);
    let listings: Vec<_> = store
        .take()
        .into_iter()
        .filter_map(|operation| match operation {
            RecordedOperation::List { prefix } => Some(prefix),
            _ => None,
        })
        .collect();
    assert_eq!(
        listings,
        vec![
            // Root content before reading the WAL tail, and sweep the sessions.
            upload_session_prefix(&pool_id),
            snapshot_prefix(&namespace_id),
            content_prefix(&pool_id),
            temporary_prefix(&pool_id),
        ]
    );
    for key in [session_key, upload_outcome(&pool_id, &session.upload_id)] {
        assert!(store.inner().head(&key).await.expect("head").is_none());
    }
    assert_eq!(
        store
            .inner()
            .list_prefix(&snapshot_prefix(&namespace_id))
            .await
            .expect("remaining snapshots"),
        vec![snapshot_record(&namespace_id, &permanent.snapshot_id)],
    );
}
