use super::bootstrap::BootstrapNamespaceError;
use crate::checkpoint::{
    create_checkpoint, freshen_fork_checkpoint, load_verified_manifest_tables,
    read_checkpoint_record, write_namespace_manifest,
};
use crate::context::MutationContext;
use crate::error::MetadataProjectionLoadError;
use crate::error::{CoreError, Result};
use crate::namespace::catalog::{
    load_namespace_descriptor, namespace_initialization_state, NamespaceInitializationError,
    NamespaceInitializationState,
};
use crate::namespace::control::read_head_object;
use bytes::Bytes;
use loonfs_api::wire::control::{
    encode_control_object, CheckpointOwner, CheckpointRecordState, ControlObjectKind, HeadState,
    HeadStateEnvelope, MetadataRootEnvelope, MetadataRootState, NamespaceConfigEnvelope,
    NamespaceConfigState, NamespaceState, WalFloorBasis, WalFloorEnvelope, WalFloorState,
    WriterBlock,
};
use loonfs_api::wire::manifest::{
    NamespaceManifestEnvelope, NamespaceManifestFork, NamespaceManifestPayload,
};
use loonfs_api::{ManifestId, ManifestObjectId, NamespaceId, NamespaceSummary, WriterEpoch};
use loonfs_objectstore::keys::{metadata_root, namespace_config, wal_head};
use loonfs_objectstore::{ObjectStore, ObjectStoreError};

pub(crate) async fn fork_namespace<S: ObjectStore + ?Sized>(
    store: &S,
    source_namespace_id: &NamespaceId,
    new_namespace_id: &NamespaceId,
    context: &MutationContext,
) -> Result<NamespaceSummary> {
    match namespace_initialization_state(store, new_namespace_id)
        .await
        .map_err(map_namespace_initialization_error_to_core)?
    {
        NamespaceInitializationState::Absent => {}
        NamespaceInitializationState::Complete => {
            let head = read_head_object(store, new_namespace_id)
                .await
                .map_err(|error| CoreError::MetadataProjection(error.into()))?;
            if head.envelope.state.state == NamespaceState::Deleted {
                return Err(CoreError::NamespaceDeleted {
                    namespace_id: new_namespace_id.clone(),
                });
            }
            return Err(CoreError::NamespaceAlreadyExists {
                namespace_id: new_namespace_id.clone(),
            });
        }
        NamespaceInitializationState::Partial => {
            return Err(CoreError::NamespacePartiallyInitialized {
                namespace_id: new_namespace_id.clone(),
            });
        }
    }

    // Fork routes through a fork-owned source checkpoint: the record is the
    // reachability root protecting every source-owned metadata file the
    // target will reference, for as long as the target lives.
    let checkpoint = create_checkpoint(
        store,
        source_namespace_id,
        CheckpointOwner::Fork {
            target_namespace_id: new_namespace_id.clone(),
        },
        None,
        context,
    )
    .await?;
    let source_record =
        read_checkpoint_record(store, source_namespace_id, &checkpoint.checkpoint_id)
            .await?
            .ok_or_else(|| {
                CoreError::NamespaceCorrupt(format!(
                    "source checkpoint `{}` disappeared during fork",
                    checkpoint.checkpoint_id
                ))
            })?
            .state;
    let source_tables = load_verified_manifest_tables(
        store,
        source_namespace_id,
        &source_record.manifest_object_id,
    )
    .await
    .map_err(|err| CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(err)))?;
    let source_manifest = source_tables.manifest();
    let fork_seq = source_record.manifest_head_seq;
    let source_head_commit_id = source_record.head_commit_id.clone();
    let source_checkpoint_id = source_record.checkpoint_id.clone();
    let source_config = load_namespace_descriptor(store, source_namespace_id)
        .await
        .map_err(MetadataProjectionLoadError::from)?;
    let source_content_store_id = source_config.content_store_id.clone();
    let target_manifest_id = ManifestId(fork_seq.0);
    let target_manifest_object_id = ManifestObjectId::generate(target_manifest_id);

    let initial_head = HeadState {
        namespace_id: new_namespace_id.clone(),
        seq: fork_seq,
        head_commit_id: source_head_commit_id.clone(),
        writer_epoch: WriterEpoch(0),
        writer: Some(WriterBlock {
            writer_id: context.writer_id.clone(),
            writer_session_id: context.writer_session_id.clone(),
            acquired_at_ms: context.now_ms,
        }),
        next_inode_id: source_manifest.payload.next_inode_id,
        visible_wal_tip: None,
        recent_segments: Vec::new(),
        state: NamespaceState::Active,
    };
    let namespace_descriptor_envelope = NamespaceConfigEnvelope::from_state(
        ControlObjectKind::NamespaceConfig,
        &context.writer_version,
        NamespaceConfigState {
            namespace_id: new_namespace_id.clone(),
            content_store_id: source_content_store_id,
            name_policy: source_config.name_policy,
        },
    )
    .map_err(|err| CoreError::Internal(format!("failed to build fork control envelope: {err}")))?;
    let head = HeadStateEnvelope::from_state(
        ControlObjectKind::WalHead,
        &context.writer_version,
        initial_head,
    )
    .map_err(|err| CoreError::Internal(format!("failed to build fork control envelope: {err}")))?;
    let head_key = wal_head(new_namespace_id.as_str());
    let descriptor_key = namespace_config(new_namespace_id.as_str());
    let target_manifest = NamespaceManifestEnvelope::from_payload(
        &context.writer_version,
        fork_target_manifest_payload(
            new_namespace_id,
            target_manifest_id,
            target_manifest_object_id,
            source_namespace_id,
            source_manifest,
            &source_record,
        ),
    )
    .map_err(|err| CoreError::Internal(format!("failed to build fork control envelope: {err}")))?;
    // Freshen the record before the first target write: the compare-and-swap
    // serializes this fork against a concurrent GC release, and the fresh
    // provider timestamp keeps the abandoned-fork age rule from firing under
    // a live retry.
    freshen_fork_checkpoint(
        store,
        source_namespace_id,
        &source_checkpoint_id,
        new_namespace_id,
        &context.writer_version,
    )
    .await?;
    write_namespace_manifest(store, &target_manifest)
        .await
        .map_err(CoreError::MetadataProjection)?;
    let target_root = MetadataRootEnvelope::from_state(
        ControlObjectKind::MetadataRoot,
        &context.writer_version,
        MetadataRootState {
            namespace_id: new_namespace_id.clone(),
            manifest_id: target_manifest_id,
            manifest_object_id: target_manifest.payload.manifest_object_id.clone(),
            manifest_head_seq: fork_seq,
            manifest_payload_checksum: target_manifest.payload_checksum.clone(),
            updated_at_ms: context.now_ms,
        },
    )
    .map_err(|err| CoreError::Internal(format!("failed to build metadata root envelope: {err}")))?;
    put_target_namespace_control_object(
        store,
        new_namespace_id,
        &metadata_root(new_namespace_id.as_str()),
        &encode_control_object(&target_root).map_err(|err| {
            CoreError::Internal(format!("failed to encode metadata root object: {err}"))
        })?,
    )
    .await?;
    let target_floor = WalFloorEnvelope::from_state(
        ControlObjectKind::WalFloor,
        &context.writer_version,
        WalFloorState {
            namespace_id: new_namespace_id.clone(),
            floor_seq: fork_seq,
            basis: WalFloorBasis {
                manifest_id: target_manifest_id,
                manifest_object_id: target_manifest.payload.manifest_object_id.clone(),
                manifest_head_seq: fork_seq,
                manifest_payload_checksum: target_manifest.payload_checksum.clone(),
            },
            verified_at_ms: context.now_ms,
            updated_at_ms: context.now_ms,
        },
    )
    .map_err(|err| CoreError::Internal(format!("failed to build wal floor envelope: {err}")))?;
    put_target_namespace_control_object(
        store,
        new_namespace_id,
        &loonfs_objectstore::keys::wal_floor(new_namespace_id.as_str()),
        &encode_control_object(&target_floor).map_err(|err| {
            CoreError::Internal(format!("failed to encode wal floor object: {err}"))
        })?,
    )
    .await?;
    put_target_namespace_control_object(
        store,
        new_namespace_id,
        &head_key,
        &encode_control_object(&head)
            .map_err(|err| CoreError::Internal(format!("failed to encode head object: {err}")))?,
    )
    .await?;
    put_target_namespace_control_object(
        store,
        new_namespace_id,
        &descriptor_key,
        &encode_control_object(&namespace_descriptor_envelope).map_err(|err| {
            CoreError::Internal(format!(
                "failed to encode namespace descriptor object: {err}"
            ))
        })?,
    )
    .await?;

    Ok(NamespaceSummary {
        namespace_id: new_namespace_id.clone(),
    })
}

/// Builds the fork target's manifest payload from the pinned source
/// checkpoint. The target adopts the source's feature declarations and
/// metadata file references verbatim: it must be readable under exactly the
/// capabilities the source declared for those tables.
fn fork_target_manifest_payload(
    new_namespace_id: &NamespaceId,
    target_manifest_id: ManifestId,
    target_manifest_object_id: ManifestObjectId,
    source_namespace_id: &NamespaceId,
    source_manifest: &NamespaceManifestEnvelope,
    source_record: &CheckpointRecordState,
) -> NamespaceManifestPayload {
    let fork_seq = source_record.manifest_head_seq;
    NamespaceManifestPayload {
        namespace_id: new_namespace_id.clone(),
        manifest_id: target_manifest_id,
        manifest_object_id: target_manifest_object_id,
        head_seq: fork_seq,
        head_commit_id: source_record.head_commit_id.clone(),
        base_seq: source_manifest.payload.base_seq,
        writer_epoch: WriterEpoch(0),
        next_inode_id: source_manifest.payload.next_inode_id,
        retention_floor_seq: fork_seq,
        initialized: true,
        verified: true,
        fork: Some(NamespaceManifestFork {
            source_namespace_id: source_namespace_id.clone(),
            fork_seq,
            source_checkpoint_id: source_record.checkpoint_id.clone(),
            source_manifest_id: source_record.manifest_id,
            source_manifest_object_id: source_record.manifest_object_id.clone(),
            source_head_seq: source_record.manifest_head_seq,
        }),
        features: source_manifest.payload.features.clone(),
        metadata_files: source_manifest.payload.metadata_files.clone(),
    }
}

async fn put_target_namespace_control_object<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    object_key: &str,
    bytes: &[u8],
) -> Result<()> {
    match store
        .put_if_absent(object_key, Bytes::copy_from_slice(bytes))
        .await
    {
        Ok(_) => Ok(()),
        Err(ObjectStoreError::PreconditionFailed { .. } | ObjectStoreError::Conflict { .. }) => {
            match namespace_initialization_state(store, namespace_id)
                .await
                .map_err(map_namespace_initialization_error_to_core)?
            {
                NamespaceInitializationState::Complete => Err(CoreError::NamespaceAlreadyExists {
                    namespace_id: namespace_id.clone(),
                }),
                NamespaceInitializationState::Partial => {
                    Err(CoreError::NamespacePartiallyInitialized {
                        namespace_id: namespace_id.clone(),
                    })
                }
                NamespaceInitializationState::Absent => Err(CoreError::Store {
                    object_key: object_key.to_owned(),
                    message: "control object write failed, but namespace remains absent".to_owned(),
                }),
            }
        }
        Err(err) => Err(CoreError::store(object_key, &err)),
    }
}

fn map_namespace_initialization_error_to_core(error: NamespaceInitializationError) -> CoreError {
    match error {
        NamespaceInitializationError::InvalidNamespaceId(error) => {
            CoreError::InvalidNamespaceId(error)
        }
        other => CoreError::Internal(BootstrapNamespaceError::from(other).to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::fork_target_manifest_payload;
    use loonfs_api::wire::control::{
        CheckpointOwner, CheckpointRecordLifecycle, CheckpointRecordState,
    };
    use loonfs_api::wire::manifest::{NamespaceManifestEnvelope, NamespaceManifestPayload};
    use loonfs_api::{
        ChangeSeq, CheckpointId, CommitId, InodeId, ManifestId, ManifestObjectId, NamespaceId,
        WriterEpoch,
    };
    use std::collections::BTreeMap;

    fn manifest_object_id(manifest_id: ManifestId) -> ManifestObjectId {
        ManifestObjectId::parse(format!("{:020}-0123456789abcdef", manifest_id.0))
            .expect("valid manifest object id")
    }

    fn source_checkpoint_record(
        checkpoint_id: &str,
        manifest_id: ManifestId,
        head_seq: ChangeSeq,
        head_commit_id: &str,
    ) -> CheckpointRecordState {
        CheckpointRecordState {
            checkpoint_id: CheckpointId::parse(checkpoint_id).expect("checkpoint id"),
            namespace_id: NamespaceId::parse("source").expect("source namespace"),
            manifest_id,
            manifest_object_id: manifest_object_id(manifest_id),
            manifest_head_seq: head_seq,
            manifest_payload_checksum: "sha256:test".to_owned(),
            head_commit_id: CommitId::parse(head_commit_id).expect("commit id"),
            created_at_ms: 1_000,
            expires_at_ms: None,
            owner: CheckpointOwner::Fork {
                target_namespace_id: NamespaceId::parse("target").expect("target namespace"),
            },
            state: CheckpointRecordLifecycle::Active,
        }
    }

    fn source_namespace_manifest(
        manifest_id: ManifestId,
        head_seq: ChangeSeq,
        head_commit_id: &str,
    ) -> NamespaceManifestEnvelope {
        let namespace_id = NamespaceId::parse("source").expect("source namespace");
        let commit_id = CommitId::parse(head_commit_id).expect("commit id");
        NamespaceManifestEnvelope::from_payload(
            "test-writer/0.1.0",
            NamespaceManifestPayload {
                namespace_id,
                manifest_id,
                manifest_object_id: manifest_object_id(manifest_id),
                head_seq,
                head_commit_id: commit_id.clone(),
                base_seq: head_seq,
                writer_epoch: WriterEpoch(1),
                next_inode_id: InodeId(2),
                retention_floor_seq: head_seq,
                initialized: true,
                verified: true,
                fork: None,
                features: BTreeMap::new(),
                metadata_files: Vec::new(),
            },
        )
        .expect("manifest")
    }

    #[test]
    fn fork_target_manifest_preserves_source_features_and_tables() {
        let base = source_namespace_manifest(
            ManifestId(7),
            ChangeSeq(7),
            "c_00000000000000000000000000000009",
        );
        let mut payload = base.payload.clone();
        payload
            .features
            .insert("core.test-capability".to_owned(), serde_json::json!(true));
        let source = NamespaceManifestEnvelope::from_payload("test-writer/0.1.0", payload)
            .expect("source manifest");
        let source_checkpoint = source_checkpoint_record(
            "chk_00000000000000000000000000000002",
            ManifestId(7),
            ChangeSeq(7),
            "c_00000000000000000000000000000009",
        );

        let target = fork_target_manifest_payload(
            &NamespaceId::parse("target").expect("target namespace"),
            ManifestId(7),
            manifest_object_id(ManifestId(7)),
            &NamespaceId::parse("source").expect("source namespace"),
            &source,
            &source_checkpoint,
        );

        assert_eq!(target.features, source.payload.features);
        assert_eq!(target.metadata_files, source.payload.metadata_files);
        assert_eq!(target.head_seq, source_checkpoint.manifest_head_seq);
        let fork = target.fork.expect("fork provenance");
        assert_eq!(fork.source_checkpoint_id, source_checkpoint.checkpoint_id);
        assert_eq!(fork.source_manifest_id, source_checkpoint.manifest_id);
    }
}
