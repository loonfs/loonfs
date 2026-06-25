use super::bootstrap::BootstrapNamespaceError;
use crate::checkpoint::{
    create_checkpoint, load_verified_manifest_materialization, write_namespace_manifest,
};
use crate::context::MutationContext;
use crate::error::CoreError;
use crate::namespace::catalog::{
    namespace_initialization_state, NamespaceInitializationError, NamespaceInitializationState,
};
use crate::namespace::control::read_head_object;
use crate::namespace::full_materialization::{
    load_full_namespace_materialization, FullMaterializationLoadError, FullMaterializationPurpose,
};
use bytes::Bytes;
use loonfs_api::wire::control::{
    decode_control_object, encode_control_object, ControlObjectKind, HeadState, HeadStateEnvelope,
    LeaseState, LeaseStateEnvelope, NamespaceDescriptorEnvelope, NamespaceDescriptorState,
    NamespaceForkState, NamespaceForkStateEnvelope, NamespaceGcPinState,
    NamespaceGcPinStateEnvelope, NamespaceState,
};
use loonfs_api::wire::manifest::{
    NamespaceCheckpointRecord, NamespaceManifestEnvelope, NamespaceManifestFork,
    NamespaceManifestPayload,
};
use loonfs_api::{
    generate_checkpoint_id, sha256_digest, FenceToken, ManifestId, NamespaceId, NamespaceSummary,
};
use loonfs_objectstore::keys::{
    gc_pin, namespace_descriptor, namespace_fork_state, namespace_head, namespace_lease,
};
use loonfs_objectstore::{ObjectStore, ObjectStoreError};
use std::collections::BTreeMap;

pub(crate) async fn fork_namespace<S: ObjectStore + ?Sized>(
    store: &S,
    source_namespace_id: &NamespaceId,
    new_namespace_id: &NamespaceId,
    context: &MutationContext,
) -> Result<NamespaceSummary, CoreError> {
    match namespace_initialization_state(store, new_namespace_id)
        .await
        .map_err(map_namespace_initialization_error_to_core)?
    {
        NamespaceInitializationState::Absent => {}
        NamespaceInitializationState::Complete => {
            let head = read_head_object(store, new_namespace_id)
                .await
                .map_err(|error| CoreError::FullMaterialization(error.into()))?;
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

    let checkpoint = create_checkpoint(store, source_namespace_id, context).await?;
    let source_manifest =
        load_verified_manifest_materialization(store, source_namespace_id, checkpoint.manifest_id)
            .await
            .map_err(|err| {
                CoreError::FullMaterialization(FullMaterializationLoadError::ManifestLoad(err))
            })?;
    let source_checkpoint =
        checkpoint_record_by_id(&source_manifest.manifest, &checkpoint.checkpoint_id)?;
    let fork_seq = source_checkpoint.head_seq;
    let source_head_seq = source_checkpoint.head_seq;
    let source_head_commit_id = source_checkpoint.head_commit_id.clone();
    let source_manifest_id = source_checkpoint.manifest_id;
    let source_checkpoint_id = source_checkpoint.checkpoint_id.clone();
    let source_materialization = load_full_namespace_materialization(
        store,
        source_namespace_id,
        FullMaterializationPurpose::ForkInitializationTemporary,
    )
    .await?;
    let target_manifest_id = ManifestId(fork_seq.0);
    let target_checkpoint_id = generate_checkpoint_id();

    let initial_head = HeadState {
        namespace_id: new_namespace_id.clone(),
        seq: fork_seq,
        head_commit_id: source_head_commit_id.clone(),
        active_fence_token: FenceToken(0),
        next_inode_id: source_manifest.manifest.payload.next_inode_id,
        name_policy: source_manifest.manifest.payload.name_policy,
        current_manifest_id: Some(target_manifest_id),
        latest_checkpoint_id: Some(target_checkpoint_id.clone()),
        retention_floor_seq: fork_seq,
        visible_wal_tip: None,
        state: NamespaceState::Active,
    };
    let initial_lease = LeaseState {
        namespace_id: new_namespace_id.clone(),
        holder_id: context.writer_id.clone(),
        fence_token: initial_head.active_fence_token,
        lease_expires_at_ms: context.now_ms.saturating_add(context.lease_duration_ms),
    };
    let namespace_descriptor_envelope = NamespaceDescriptorEnvelope::from_state(
        ControlObjectKind::NamespaceDescriptor,
        &context.writer_version,
        NamespaceDescriptorState {
            namespace_id: new_namespace_id.clone(),
            content_store_id: source_materialization.content_store_id,
        },
    )
    .map_err(|err| CoreError::Store(err.to_string()))?;
    let head = HeadStateEnvelope::from_state(
        ControlObjectKind::NamespaceHead,
        &context.writer_version,
        initial_head,
    )
    .map_err(|err| CoreError::Store(err.to_string()))?;
    let lease = LeaseStateEnvelope::from_state(
        ControlObjectKind::NamespaceLease,
        &context.writer_version,
        initial_lease,
    )
    .map_err(|err| CoreError::Store(err.to_string()))?;
    let fork_state = NamespaceForkStateEnvelope::from_state(
        ControlObjectKind::NamespaceForkState,
        &context.writer_version,
        NamespaceForkState {
            namespace_id: new_namespace_id.clone(),
            source_namespace_id: source_namespace_id.clone(),
            fork_seq,
            source_checkpoint_id: source_checkpoint_id.clone(),
            source_manifest_id,
            source_head_seq,
            created_at_ms: context.now_ms,
        },
    )
    .map_err(|err| CoreError::Store(err.to_string()))?;

    let head_key = namespace_head(new_namespace_id.as_str());
    let fork_state_key = namespace_fork_state(new_namespace_id.as_str());
    let lease_key = namespace_lease(new_namespace_id.as_str());
    let descriptor_key = namespace_descriptor(new_namespace_id.as_str());
    let target_manifest = NamespaceManifestEnvelope::from_payload(
        &context.writer_version,
        NamespaceManifestPayload {
            namespace_id: new_namespace_id.clone(),
            manifest_id: target_manifest_id,
            head_seq: fork_seq,
            head_commit_id: source_head_commit_id.clone(),
            base_seq: source_manifest.manifest.payload.base_seq,
            active_fence_token: FenceToken(0),
            next_inode_id: source_manifest.manifest.payload.next_inode_id,
            name_policy: source_manifest.manifest.payload.name_policy,
            retention_floor_seq: fork_seq,
            initialized: true,
            verified: true,
            fork: Some(NamespaceManifestFork {
                source_namespace_id: source_namespace_id.clone(),
                fork_seq,
                source_checkpoint_id: source_checkpoint_id.clone(),
                source_manifest_id,
                source_head_seq,
            }),
            checkpoints: vec![NamespaceCheckpointRecord {
                checkpoint_id: target_checkpoint_id.clone(),
                manifest_id: target_manifest_id,
                head_seq: fork_seq,
                head_commit_id: source_head_commit_id.clone(),
                created_at_ms: context.now_ms,
                expires_at_ms: None,
                name: None,
            }],
            features: BTreeMap::new(),
            metadata_files: source_manifest.manifest.payload.metadata_files.clone(),
        },
    )
    .map_err(|err| CoreError::Store(err.to_string()))?;
    let gc_pin_envelope = NamespaceGcPinStateEnvelope::from_state(
        ControlObjectKind::NamespaceGcPinState,
        &context.writer_version,
        NamespaceGcPinState {
            pin_id: deterministic_gc_pin_id(
                source_namespace_id,
                new_namespace_id,
                &source_checkpoint_id,
                source_manifest_id,
                source_head_seq,
            ),
            source_namespace_id: source_namespace_id.clone(),
            target_namespace_id: new_namespace_id.clone(),
            source_checkpoint_id,
            source_manifest_id,
            source_head_seq,
            created_at_ms: context.now_ms,
        },
    )
    .map_err(|err| CoreError::Store(err.to_string()))?;
    let gc_pin_key = gc_pin(source_namespace_id.as_str(), &gc_pin_envelope.state.pin_id);
    write_source_gc_pin(store, &gc_pin_key, &gc_pin_envelope).await?;
    write_namespace_manifest(store, &target_manifest)
        .await
        .map_err(CoreError::FullMaterialization)?;
    put_target_namespace_control_object(
        store,
        new_namespace_id,
        &head_key,
        &encode_control_object(&head).map_err(|err| CoreError::Store(err.to_string()))?,
    )
    .await?;
    put_target_namespace_control_object(
        store,
        new_namespace_id,
        &fork_state_key,
        &encode_control_object(&fork_state).map_err(|err| CoreError::Store(err.to_string()))?,
    )
    .await?;
    put_target_namespace_control_object(
        store,
        new_namespace_id,
        &lease_key,
        &encode_control_object(&lease).map_err(|err| CoreError::Store(err.to_string()))?,
    )
    .await?;
    put_target_namespace_control_object(
        store,
        new_namespace_id,
        &descriptor_key,
        &encode_control_object(&namespace_descriptor_envelope)
            .map_err(|err| CoreError::Store(err.to_string()))?,
    )
    .await?;

    Ok(NamespaceSummary {
        namespace_id: new_namespace_id.clone(),
    })
}

async fn write_source_gc_pin<S: ObjectStore + ?Sized>(
    store: &S,
    object_key: &str,
    expected: &NamespaceGcPinStateEnvelope,
) -> Result<(), CoreError> {
    let bytes = encode_control_object(expected).map_err(|err| CoreError::Store(err.to_string()))?;
    match store.put_if_absent(object_key, Bytes::from(bytes)).await {
        Ok(_) => Ok(()),
        Err(ObjectStoreError::PreconditionFailed | ObjectStoreError::Conflict) => {
            verify_existing_gc_pin(store, object_key, expected).await
        }
        Err(err) => Err(CoreError::Store(err.to_string())),
    }
}

fn checkpoint_record_by_id<'a>(
    manifest: &'a NamespaceManifestEnvelope,
    checkpoint_id: &str,
) -> Result<&'a NamespaceCheckpointRecord, CoreError> {
    manifest
        .payload
        .checkpoints
        .iter()
        .find(|checkpoint| checkpoint.checkpoint_id == checkpoint_id)
        .ok_or_else(|| {
            CoreError::NamespaceCorrupt(format!(
                "manifest {:?} does not contain checkpoint `{checkpoint_id}`",
                manifest.payload.manifest_id
            ))
        })
}

fn deterministic_gc_pin_id(
    source_namespace_id: &NamespaceId,
    target_namespace_id: &NamespaceId,
    source_checkpoint_id: &str,
    source_manifest_id: ManifestId,
    source_head_seq: loonfs_api::ChangeSeq,
) -> String {
    let identity = format!(
        "loonfs.gc-pin.v0\0source={}\0target={}\0checkpoint={}\0manifest={}\0seq={}",
        source_namespace_id.as_str(),
        target_namespace_id.as_str(),
        source_checkpoint_id,
        source_manifest_id.0,
        source_head_seq.0,
    );
    let digest = sha256_digest(identity.as_bytes());
    let hex = digest
        .strip_prefix("sha256:")
        .expect("sha256_digest returns a sha256-prefixed digest");
    format!("pin_{}", &hex[..32])
}

async fn verify_existing_gc_pin<S: ObjectStore + ?Sized>(
    store: &S,
    object_key: &str,
    expected: &NamespaceGcPinStateEnvelope,
) -> Result<(), CoreError> {
    let Some(bytes) = store
        .get(object_key, None)
        .await
        .map_err(|err| CoreError::Store(err.to_string()))?
    else {
        return Err(CoreError::Store(format!(
            "GC pin `{object_key}` write conflicted, but the existing object is missing"
        )));
    };
    let existing: NamespaceGcPinStateEnvelope =
        decode_control_object(&bytes, ControlObjectKind::NamespaceGcPinState).map_err(|err| {
            CoreError::NamespaceCorrupt(format!("GC pin `{object_key}` is invalid: {err}"))
        })?;
    if gc_pin_matches(expected, &existing) {
        Ok(())
    } else {
        Err(CoreError::NamespaceCorrupt(format!(
            "GC pin `{object_key}` conflicts with expected fork provenance"
        )))
    }
}

fn gc_pin_matches(
    expected: &NamespaceGcPinStateEnvelope,
    existing: &NamespaceGcPinStateEnvelope,
) -> bool {
    existing.format_version == expected.format_version
        && existing.state.pin_id == expected.state.pin_id
        && existing.state.source_namespace_id == expected.state.source_namespace_id
        && existing.state.target_namespace_id == expected.state.target_namespace_id
        && existing.state.source_checkpoint_id == expected.state.source_checkpoint_id
        && existing.state.source_manifest_id == expected.state.source_manifest_id
        && existing.state.source_head_seq == expected.state.source_head_seq
}

async fn put_target_namespace_control_object<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    object_key: &str,
    bytes: &[u8],
) -> Result<(), CoreError> {
    match store
        .put_if_absent(object_key, Bytes::copy_from_slice(bytes))
        .await
    {
        Ok(_) => Ok(()),
        Err(ObjectStoreError::PreconditionFailed | ObjectStoreError::Conflict) => {
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
                NamespaceInitializationState::Absent => Err(CoreError::Store(format!(
                    "target namespace control object `{object_key}` write failed, but namespace remains absent"
                ))),
            }
        }
        Err(err) => Err(CoreError::Store(err.to_string())),
    }
}

fn map_namespace_initialization_error_to_core(error: NamespaceInitializationError) -> CoreError {
    match error {
        NamespaceInitializationError::InvalidNamespaceId(error) => {
            CoreError::InvalidNamespaceId(error)
        }
        other => CoreError::Store(BootstrapNamespaceError::from(other).to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::{checkpoint_record_by_id, deterministic_gc_pin_id, write_source_gc_pin};
    use crate::error::CoreError;
    use bytes::Bytes;
    use loonfs_api::wire::control::{
        ControlObjectKind, NamespaceGcPinState, NamespaceGcPinStateEnvelope,
    };
    use loonfs_api::wire::manifest::{
        NamespaceCheckpointRecord, NamespaceManifestEnvelope, NamespaceManifestPayload,
    };
    use loonfs_api::{
        validate_gc_pin_id, ChangeSeq, CommitId, FenceToken, InodeId, ManifestId, NamePolicy,
        NamespaceId,
    };
    use loonfs_objectstore::fs::LocalFsStore;
    use loonfs_objectstore::keys::gc_pin;
    use loonfs_objectstore::ObjectStore;
    use std::collections::BTreeMap;
    use tempfile::tempdir;

    #[tokio::test]
    async fn gc_pin_id_is_deterministic_for_same_source_target_checkpoint_manifest() {
        let source = NamespaceId::parse("source").expect("source namespace");
        let target = NamespaceId::parse("target").expect("target namespace");
        let first = deterministic_gc_pin_id(
            &source,
            &target,
            "chk_00000000000000000000000000000001",
            ManifestId(42),
            ChangeSeq(7),
        );
        let second = deterministic_gc_pin_id(
            &source,
            &target,
            "chk_00000000000000000000000000000001",
            ManifestId(42),
            ChangeSeq(7),
        );

        assert_eq!(first, second);
        validate_gc_pin_id(&first).expect("valid deterministic pin id");
    }

    #[tokio::test]
    async fn checkpoint_record_by_id_returns_fork_base_record() {
        let manifest = namespace_manifest_with_checkpoint(
            "chk_00000000000000000000000000000001",
            ManifestId(42),
            ChangeSeq(7),
            "c_00000000000000000000000000000007",
        );

        let checkpoint = checkpoint_record_by_id(&manifest, "chk_00000000000000000000000000000001")
            .expect("checkpoint record");

        assert_eq!(checkpoint.manifest_id, ManifestId(42));
        assert_eq!(checkpoint.head_seq, ChangeSeq(7));
        assert_eq!(
            checkpoint.head_commit_id,
            CommitId::parse("c_00000000000000000000000000000007").expect("commit id")
        );
    }

    #[tokio::test]
    async fn gc_pin_conflict_same_payload_is_idempotent() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let expected = gc_pin_envelope("source", "target", 1_000);
        let object_key = gc_pin("source", &expected.state.pin_id);

        write_source_gc_pin(&store, &object_key, &expected)
            .await
            .expect("first write");
        write_source_gc_pin(&store, &object_key, &expected)
            .await
            .expect("conflict is idempotent");
    }

    #[tokio::test]
    async fn gc_pin_conflict_different_payload_is_error() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let expected = gc_pin_envelope("source", "target", 1_000);
        let mut conflicting = gc_pin_envelope("source", "other-target", 1_000);
        conflicting.state.pin_id = expected.state.pin_id.clone();
        let conflicting = NamespaceGcPinStateEnvelope::from_state(
            ControlObjectKind::NamespaceGcPinState,
            "test-writer/0.1.0",
            conflicting.state,
        )
        .expect("conflicting envelope");
        let object_key = gc_pin("source", &expected.state.pin_id);
        store
            .put_if_absent(
                &object_key,
                Bytes::from(
                    super::encode_control_object(&conflicting).expect("encode conflicting pin"),
                ),
            )
            .await
            .expect("write conflicting pin");

        let error = write_source_gc_pin(&store, &object_key, &expected)
            .await
            .expect_err("conflicting pin should fail");

        assert!(matches!(error, CoreError::NamespaceCorrupt(_)));
    }

    fn gc_pin_envelope(
        source_namespace_id: &str,
        target_namespace_id: &str,
        created_at_ms: u64,
    ) -> NamespaceGcPinStateEnvelope {
        let source = NamespaceId::parse(source_namespace_id).expect("source namespace");
        let target = NamespaceId::parse(target_namespace_id).expect("target namespace");
        let checkpoint_id = "chk_00000000000000000000000000000001";
        let source_manifest_id = ManifestId(42);
        let source_head_seq = ChangeSeq(7);
        NamespaceGcPinStateEnvelope::from_state(
            ControlObjectKind::NamespaceGcPinState,
            "test-writer/0.1.0",
            NamespaceGcPinState {
                pin_id: deterministic_gc_pin_id(
                    &source,
                    &target,
                    checkpoint_id,
                    source_manifest_id,
                    source_head_seq,
                ),
                source_namespace_id: source,
                target_namespace_id: target,
                source_checkpoint_id: checkpoint_id.to_owned(),
                source_manifest_id,
                source_head_seq,
                created_at_ms,
            },
        )
        .expect("pin envelope")
    }

    fn namespace_manifest_with_checkpoint(
        checkpoint_id: &str,
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
                head_seq,
                head_commit_id: commit_id.clone(),
                base_seq: head_seq,
                active_fence_token: FenceToken(1),
                next_inode_id: InodeId(2),
                name_policy: NamePolicy::default(),
                retention_floor_seq: head_seq,
                initialized: true,
                verified: true,
                fork: None,
                checkpoints: vec![NamespaceCheckpointRecord {
                    checkpoint_id: checkpoint_id.to_owned(),
                    manifest_id,
                    head_seq,
                    head_commit_id: commit_id,
                    created_at_ms: 1_000,
                    expires_at_ms: None,
                    name: None,
                }],
                features: BTreeMap::new(),
                metadata_files: Vec::new(),
            },
        )
        .expect("manifest")
    }
}
