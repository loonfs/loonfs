//! Complete collection across pin and upload families.

use super::*;
use loonfs_objectstore::keys::{
    content_prefix, pin, temporary_prefix, upload_outcome, upload_session_prefix,
};
use loonfs_test_support::stores::RecordedOperation;
use loonfs_types::format::control::UploadOutcome;

#[tokio::test]
async fn one_pass_deletes_an_aged_upload_and_every_expired_snapshot_among_many_pins() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("many-pins").expect("namespace");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::prefix(loonfs_objectstore::keys::namespace_prefix(&namespace_id)),
    );
    let setup = context(1_000);
    create(&store, &namespace_id, &setup)
        .await
        .expect("bootstrap");
    let permanent = create_checkpoint(&store, &namespace_id, &setup)
        .await
        .expect("permanent pin");
    let basis = crate::pin::record::load_pin(&store, &namespace_id, &permanent.checkpoint_id)
        .await
        .expect("pin")
        .expect("record")
        .state;
    for number in 0..1025 {
        let record = PinPayload {
            pin_id: PinId::parse(format!(
                "pin_{:020}-{number:016x}",
                basis.pin_id.manifest_no().0
            ))
            .expect("pin id"),
            owner: PinOwner::User {
                name: "expired".to_owned(),
                expires_at_ms: Some(2_000),
            },
            ..basis.clone()
        };
        crate::pin::record::write_pin(&store, &record)
            .await
            .expect("expired pin");
    }
    let session = crate::test_support::proxied_upload_session(&namespace_id, 2_000);
    let session_key = crate::test_support::write_upload_records(
        &store,
        &session,
        Some(UploadOutcome::Aborted {
            aborted_at_ms: 2_000,
        }),
    )
    .await;
    store.reset();
    let report = gc_namespace(
        &store,
        None,
        None,
        &namespace_id,
        &options(),
        &context(2_000 + GRACE_MS),
        crate::test_support::std_timer(),
    )
    .await
    .expect("complete pass");
    assert_eq!(report.deleted.upload_sessions, 1);
    assert_eq!(report.deleted_checkpoints_by_owner.user, 1025);
    assert_eq!(report.retained.checkpoint_not_deletable, 1);
    // The pins, and the session's two records.
    assert_eq!(store.counts().deletes, 1028);
    let operations = store.take();
    assert_eq!(
        operations
            .iter()
            .filter(|operation| matches!(
                operation,
                RecordedOperation::GetWithMetadata { key, .. }
                    if key.starts_with(&pin_prefix(&namespace_id))
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
            upload_session_prefix(&namespace_id), // Root content before reading the WAL tail.
            pin_prefix(&namespace_id),
            metadata_manifest_prefix(&namespace_id), // Discover superseded roots before sweeping.
            metadata_manifest_prefix(&namespace_id),
            wal_prefix(&namespace_id),
            metadata_segment_prefix(&namespace_id),
            pin_prefix(&namespace_id),
            format!("{}con_", content_prefix(&namespace_id)),
            temporary_prefix(&namespace_id),
            upload_session_prefix(&namespace_id),
        ]
    );
    for key in [
        session_key,
        upload_outcome(&namespace_id, &session.upload_id),
    ] {
        assert!(store.inner().head(&key).await.expect("head").is_none());
    }
    assert_eq!(
        store
            .inner()
            .list_prefix(&pin_prefix(&namespace_id))
            .await
            .expect("remaining pins"),
        vec![pin(&namespace_id, &permanent.checkpoint_id)],
    );
}
