//! Complete collection across pin and upload families.

use super::*;
use loonfs_objectstore::keys::{checkpoint_record, upload_session, upload_session_prefix};
use loonfs_test_support::stores::RecordedOperation;

#[tokio::test]
async fn one_pass_deletes_an_aged_upload_and_every_expired_snapshot_among_many_pins() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("many-pins").expect("namespace");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::prefix(loonfs_objectstore::keys::namespace_prefix(&namespace_id)),
    );
    let setup = context(1_000);
    bootstrap_namespace(&store, &namespace_id, &setup, false)
        .await
        .expect("bootstrap");
    let permanent = create_checkpoint(&store, &namespace_id, &setup)
        .await
        .expect("permanent pin");
    let basis =
        crate::checkpoint::load_checkpoint_record(&store, &namespace_id, &permanent.checkpoint_id)
            .await
            .expect("pin")
            .expect("record")
            .state;
    for number in 0..1025 {
        let record = CheckpointRecordState {
            pin_id: CheckpointId::parse(format!("pin_{:020}-{number:016x}", basis.manifest_no.0))
                .expect("pin id"),
            owner: CheckpointOwner::Snapshot {
                name: "expired".to_owned(),
                expires_at_ms: 2_000,
            },
            ..basis.clone()
        };
        crate::checkpoint::record::write_checkpoint_record(&store, &record)
            .await
            .expect("snapshot pin");
    }
    let session = UploadSessionState {
        namespace_id: namespace_id.clone(),
        upload_id: UploadId::generate(),
        content_id: loonfs_api::ContentId::generate(),
        created_at_ms: setup.now_ms,
        mode: UploadSessionMode::ServiceProxied {
            staging: ProxiedStaging::Idle,
        },
        status: UploadSessionRecordStatus::Aborted {
            aborted_at_ms: 2_000,
        },
    };
    let session_key = upload_session(&namespace_id, &session.upload_id);
    store
        .put_if_absent(
            &session_key,
            Bytes::from(
                loonfs_api::wire::control::encode_control_state(
                    ControlObjectKind::UploadSession,
                    &session,
                )
                .expect("encode session"),
            ),
        )
        .await
        .expect("write aborted upload");
    store.reset();
    let report = gc_namespace(&store, &namespace_id, &config(), &context(2_000 + GRACE_MS))
        .await
        .expect("complete pass");
    assert_eq!(report.deleted.upload_sessions, 1);
    assert_eq!(report.deleted_checkpoints_by_owner.snapshot, 1025);
    assert_eq!(report.retained.checkpoint_not_deletable, 1);
    assert_eq!(store.counts().deletes, 1026);
    let operations = store.take();
    assert_eq!(
        operations
            .iter()
            .filter(|operation| matches!(
                operation,
                RecordedOperation::GetWithMetadata { key, .. }
                    if key.starts_with(&checkpoint_prefix(&namespace_id))
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
            checkpoint_prefix(&namespace_id),
            metadata_manifest_prefix(&namespace_id),
            wal_segment_prefix(&namespace_id),
            metadata_segment_prefix(&namespace_id),
            checkpoint_prefix(&namespace_id),
            upload_session_prefix(&namespace_id),
        ]
    );
    assert!(store
        .inner()
        .head(&session_key)
        .await
        .expect("session")
        .is_none());
    assert_eq!(
        store
            .inner()
            .list_prefix(&checkpoint_prefix(&namespace_id))
            .await
            .expect("remaining pins"),
        vec![checkpoint_record(&namespace_id, &permanent.checkpoint_id)],
    );
}
