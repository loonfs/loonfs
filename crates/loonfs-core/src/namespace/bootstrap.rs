//! Namespace bootstrap: writes the initial durable tree, finishing with
//! `namespace.json` as the completion marker.

use crate::checkpoint::{
    build_initial_namespace_manifest, load_namespace_manifest_envelope, write_namespace_manifest,
    ManifestLoadFailureClass,
};
use crate::context::MutationContext;
use crate::error::{CoreError, MetadataProjectionLoadError};
#[cfg(test)]
use crate::limits::GC_MIN_GRACE_WINDOW_MS;
use crate::metadata::{InodeRecord, MetadataState};
use crate::namespace::catalog::{
    map_namespace_initialization_error_to_core, namespace_initialization_state,
    put_completion_descriptor, put_namespace_install_gate, put_target_namespace_control_object,
    NamespaceInitializationState, NamespaceInstallGate,
};
use crate::namespace::control::ControlObjectLoadError;
use crate::namespace::control::{read_head_object, read_metadata_root_object};
use bytes::Bytes;
use loonfs_api::wire::control::NamespaceState;
use loonfs_api::wire::control::{
    encode_control_object, ContentStoreDescriptorEnvelope, ContentStoreDescriptorState,
    ControlObjectKind, HeadState, HeadStateEnvelope, MetadataRootEnvelope, MetadataRootState,
    NamespaceConfigEnvelope, NamespaceConfigState, WalFloorEnvelope, WalFloorState, WriterBlock,
};
use loonfs_api::{
    ChangeSeq, ContentStoreId, ErrorCode, InodeId, InodeKind, NamePolicy, NamespaceId,
    NamespaceSummary,
};
#[cfg(test)]
use loonfs_objectstore::keys::wal_head;
use loonfs_objectstore::keys::{
    content_store_descriptor, metadata_root, namespace_config, wal_floor,
};
use loonfs_objectstore::{ObjectStore, ObjectStoreError};
use thiserror::Error;

#[derive(Debug, Clone, Error)]
pub enum BootstrapNamespaceError {
    #[error("holder id must not be empty")]
    EmptyHolderId,
    #[error("writer version must not be empty")]
    EmptyWriterVersion,
    #[error("namespace `{namespace_id}` already exists")]
    NamespaceAlreadyExists { namespace_id: NamespaceId },
    #[error("namespace `{namespace_id}` is partially initialized; run admin repair explicitly")]
    NamespacePartiallyInitialized { namespace_id: NamespaceId },
    #[error("namespace `{namespace_id}` is deleted and its id is retired")]
    NamespaceDeleted { namespace_id: NamespaceId },
    #[error(transparent)]
    Head(#[from] ControlObjectLoadError),
    /// A failure inside the installation protocol shared with fork — the
    /// classification probe, a control-object install, debris recovery — or
    /// engine plumbing such as assembling the mutation context. The wire
    /// code delegates to [`CoreError::code`](crate::Error::code), so store
    /// failures keep their failure class.
    #[error(transparent)]
    Core(#[from] CoreError),
}

impl BootstrapNamespaceError {
    /// Returns the stable machine-readable reason for this error.
    ///
    /// This is the single source of truth for the wire code every surface
    /// (HTTP server, CLI) reports for a bootstrap failure, mirroring
    /// [`CoreError::code`](crate::Error::code).
    pub fn code(&self) -> ErrorCode {
        match self {
            BootstrapNamespaceError::EmptyHolderId
            | BootstrapNamespaceError::EmptyWriterVersion => ErrorCode::InvalidRequest,
            BootstrapNamespaceError::NamespaceAlreadyExists { .. } => ErrorCode::NamespaceExists,
            BootstrapNamespaceError::NamespacePartiallyInitialized { .. } => {
                ErrorCode::NamespacePartial
            }
            BootstrapNamespaceError::NamespaceDeleted { .. } => ErrorCode::NamespaceDeleted,
            BootstrapNamespaceError::Head(_) => ErrorCode::ServerError,
            BootstrapNamespaceError::Core(error) => error.code(),
        }
    }
}

pub(crate) async fn bootstrap_namespace<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
    allow_existing: bool,
) -> Result<NamespaceSummary, BootstrapNamespaceError> {
    if context.writer_id.trim().is_empty() {
        return Err(BootstrapNamespaceError::EmptyHolderId);
    }
    if context.writer_version.trim().is_empty() {
        return Err(BootstrapNamespaceError::EmptyWriterVersion);
    }

    match namespace_initialization_state(store, namespace_id)
        .await
        .map_err(map_namespace_initialization_error_to_core)?
    {
        NamespaceInitializationState::Complete => {
            let head = read_head_object(store, namespace_id).await?;
            // A deleted namespace retires its id permanently; re-creation is
            // refused as deleted, not as existing.
            if head.envelope.state.state == NamespaceState::Deleted {
                return Err(BootstrapNamespaceError::NamespaceDeleted {
                    namespace_id: namespace_id.clone(),
                });
            }
            if allow_existing {
                return Ok(NamespaceSummary {
                    namespace_id: namespace_id.clone(),
                });
            }
            return Err(BootstrapNamespaceError::NamespaceAlreadyExists {
                namespace_id: namespace_id.clone(),
            });
        }
        NamespaceInitializationState::Partial | NamespaceInitializationState::PreHeadDebris => {
            return Err(BootstrapNamespaceError::NamespacePartiallyInitialized {
                namespace_id: namespace_id.clone(),
            });
        }
        NamespaceInitializationState::Absent => {}
    }

    let mut initial_head = HeadState::initial(namespace_id.clone());
    initial_head.writer = Some(WriterBlock {
        writer_id: context.writer_id.clone(),
        writer_session_id: context.writer_session_id.clone(),
        acquired_at_ms: context.now_ms,
    });
    let initial_manifest = build_initial_namespace_manifest(
        store,
        namespace_id,
        &initial_head,
        &context.writer_version,
    )
    .await?;
    write_namespace_manifest(store, &initial_manifest)
        .await
        .map_err(CoreError::MetadataProjection)?;

    // The head create-if-absent is the first fixed-key conditional install
    // write and therefore the admission gate. No fixed-key prerequisite is
    // written unless this attempt owns that state. Repair condemns this same
    // key and removes it last.
    let initial_seq = initial_head.seq;
    let head_envelope = HeadStateEnvelope::from_state(
        ControlObjectKind::WalHead,
        &context.writer_version,
        initial_head,
    )
    .map_err(|err| CoreError::Internal(format!("failed to build head envelope: {err}")))?;
    let head_bytes = encode_control_object(&head_envelope)
        .map_err(|err| CoreError::Internal(format!("failed to encode head object: {err}")))?;
    let gate = put_namespace_install_gate(store, namespace_id, &head_bytes).await?;

    let root_envelope = MetadataRootEnvelope::from_state(
        ControlObjectKind::MetadataRoot,
        &context.writer_version,
        MetadataRootState {
            namespace_id: namespace_id.clone(),
            manifest_id: initial_manifest.payload.manifest_id,
            manifest_object_id: initial_manifest.payload.manifest_object_id.clone(),
            manifest_head_seq: initial_seq,
            manifest_payload_checksum: initial_manifest.payload_checksum.clone(),
            updated_at_ms: context.now_ms,
        },
    )
    .map_err(|err| CoreError::Internal(format!("failed to build metadata root envelope: {err}")))?;
    put_target_namespace_control_object(
        store,
        namespace_id,
        &metadata_root(namespace_id.as_str()),
        &encode_control_object(&root_envelope).map_err(|err| {
            CoreError::Internal(format!("failed to encode metadata root object: {err}"))
        })?,
    )
    .await?;

    let floor_envelope = WalFloorEnvelope::from_state(
        ControlObjectKind::WalFloor,
        &context.writer_version,
        WalFloorState {
            namespace_id: namespace_id.clone(),
            floor_seq: initial_seq,
            verified_at_ms: context.now_ms,
            updated_at_ms: context.now_ms,
        },
    )
    .map_err(|err| CoreError::Internal(format!("failed to build wal floor envelope: {err}")))?;
    put_target_namespace_control_object(
        store,
        namespace_id,
        &wal_floor(namespace_id.as_str()),
        &encode_control_object(&floor_envelope).map_err(|err| {
            CoreError::Internal(format!("failed to encode wal floor object: {err}"))
        })?,
    )
    .await?;

    if gate == NamespaceInstallGate::ExistingPartial {
        return Err(CoreError::NamespacePartiallyInitialized {
            namespace_id: namespace_id.clone(),
        }
        .into());
    }

    let content_store_id = create_new_content_store(store, context).await?;
    let namespace_descriptor_envelope = NamespaceConfigEnvelope::from_state(
        ControlObjectKind::NamespaceConfig,
        &context.writer_version,
        NamespaceConfigState {
            namespace_id: namespace_id.clone(),
            content_store_id,
            name_policy: NamePolicy::default(),
        },
    )
    .map_err(|err| {
        CoreError::Internal(format!(
            "failed to build namespace descriptor envelope: {err}"
        ))
    })?;
    put_target_namespace_control_object(
        store,
        namespace_id,
        &namespace_config(namespace_id.as_str()),
        &encode_control_object(&namespace_descriptor_envelope).map_err(|err| {
            CoreError::Internal(format!(
                "failed to encode namespace descriptor object: {err}"
            ))
        })?,
    )
    .await?;

    Ok(NamespaceSummary {
        namespace_id: namespace_id.clone(),
    })
}

/// Finishes a create that crashed after its head write when explicit admin
/// repair invokes it. Guarded to genesis trees only — the head must be the
/// unmodified initial state and the root manifest must carry no fork block;
/// anything else answers partial. The crashed attempt's content-store
/// descriptor (written after the head, before the namespace descriptor)
/// may remain as a tiny orphan; nothing references it.
pub(super) async fn complete_post_head_bootstrap<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) -> Result<NamespaceSummary, BootstrapNamespaceError> {
    // Completion is strictly best-effort: a tree whose head, root, or
    // manifest cannot be loaded and validated is not completable, and the
    // honest answer stays `namespace_partial` so repair can apply the
    // non-completable debris policy.
    let partial = || BootstrapNamespaceError::NamespacePartiallyInitialized {
        namespace_id: namespace_id.clone(),
    };
    let head = read_head_object(store, namespace_id)
        .await
        .map_err(|error| match error {
            ControlObjectLoadError::Store { .. } => BootstrapNamespaceError::Core(
                CoreError::MetadataProjection(MetadataProjectionLoadError::LoadHead(error)),
            ),
            _ => partial(),
        })?
        .envelope
        .state;
    let genesis = head.state == NamespaceState::Active
        && head.seq == ChangeSeq(0)
        && head.visible_wal_tip.is_none();
    if !genesis {
        return Err(partial());
    }
    let root = read_metadata_root_object(store, namespace_id)
        .await
        .map_err(|error| match error {
            ControlObjectLoadError::Store { .. } => BootstrapNamespaceError::Core(
                CoreError::MetadataProjection(MetadataProjectionLoadError::LoadHead(error)),
            ),
            _ => partial(),
        })?
        .envelope
        .state;
    let manifest = load_namespace_manifest_envelope(store, namespace_id, &root.manifest_object_id)
        .await
        .map_err(|error| match error.failure_class() {
            ManifestLoadFailureClass::Store => BootstrapNamespaceError::Core(
                CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(error)),
            ),
            ManifestLoadFailureClass::Corrupt => partial(),
        })?;
    if manifest.payload.fork.is_some() {
        // A crashed fork target must be completed by the fork path, which
        // knows the source; a create finishing it would bind the wrong
        // content store.
        return Err(BootstrapNamespaceError::NamespacePartiallyInitialized {
            namespace_id: namespace_id.clone(),
        });
    }

    let content_store_id = create_new_content_store(store, context).await?;
    let namespace_descriptor_envelope = NamespaceConfigEnvelope::from_state(
        ControlObjectKind::NamespaceConfig,
        &context.writer_version,
        NamespaceConfigState {
            namespace_id: namespace_id.clone(),
            content_store_id,
            name_policy: NamePolicy::default(),
        },
    )
    .map_err(|err| {
        CoreError::Internal(format!(
            "failed to build namespace descriptor envelope: {err}"
        ))
    })?;
    let namespace_descriptor_bytes = encode_control_object(&namespace_descriptor_envelope)
        .map_err(|err| {
            CoreError::Internal(format!(
                "failed to encode namespace descriptor object: {err}"
            ))
        })?;
    put_completion_descriptor(store, namespace_id, &namespace_descriptor_bytes).await?;
    Ok(NamespaceSummary {
        namespace_id: namespace_id.clone(),
    })
}

const CONTENT_STORE_ID_RETRY_LIMIT: usize = 8;

async fn create_new_content_store<S: ObjectStore + ?Sized>(
    store: &S,
    context: &MutationContext,
) -> Result<ContentStoreId, CoreError> {
    for _attempt in 0..CONTENT_STORE_ID_RETRY_LIMIT {
        let content_store_id = ContentStoreId::generate();
        let descriptor = ContentStoreDescriptorEnvelope::from_state(
            ControlObjectKind::ContentStoreDescriptor,
            &context.writer_version,
            ContentStoreDescriptorState {
                content_store_id: content_store_id.clone(),
            },
        )
        .map_err(|err| {
            CoreError::Internal(format!(
                "failed to build content store descriptor envelope: {err}"
            ))
        })?;
        let bytes = encode_control_object(&descriptor).map_err(|err| {
            CoreError::Internal(format!(
                "failed to encode content store descriptor object: {err}"
            ))
        })?;
        let key = content_store_descriptor(content_store_id.as_str());
        match store.put_if_absent(&key, Bytes::from(bytes)).await {
            Ok(_) => return Ok(content_store_id),
            Err(
                ObjectStoreError::PreconditionFailed { .. } | ObjectStoreError::Conflict { .. },
            ) => continue,
            Err(err) => return Err(CoreError::store(&key, &err)),
        }
    }

    Err(CoreError::Internal(
        "content store id generation collided repeatedly".to_owned(),
    ))
}

pub(crate) fn bootstrap_metadata_state() -> MetadataState {
    MetadataState::from_rows(
        vec![InodeRecord {
            inode_id: InodeId(1),
            inode_kind: InodeKind::Directory,
            created_seq: ChangeSeq(0),
        }],
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit_engine::{NamespaceCommitEngine, NamespaceMutationCandidate};
    use crate::namespace::catalog::load_namespace_descriptor;
    use crate::namespace::fork::fork_namespace;
    use crate::namespace::repair::repair_namespace;
    use crate::namespace::status::load_namespace_head_summary;
    use futures::stream::BoxStream;
    use loonfs_api::v0::{CommitOp as ApiCommitOp, CommitRequest as ApiCommitRequest};
    use loonfs_api::wire::control::decode_control_object;
    use loonfs_api::{CommitId, ManifestId, ManifestObjectId, RepairNamespaceOutcome};
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use loonfs_objectstore::{ByteRange, ObjectBody, ObjectMetadata, ObjectStoreError, PutMode};
    use std::sync::atomic::{AtomicBool, Ordering};
    use tempfile::tempdir;
    use tokio::sync::Semaphore;

    const WRITER_VERSION: &str = "writer/0.1.0";

    #[derive(Debug)]
    struct BlockingRepairDeleteStore {
        inner: LocalFsStore,
        gate_key: String,
        install_blocked_once: AtomicBool,
        install_entered: Semaphore,
        install_release: Semaphore,
        install_finished: Semaphore,
        delete_blocked_once: AtomicBool,
        delete_entered: Semaphore,
        delete_release: Semaphore,
    }

    impl BlockingRepairDeleteStore {
        fn new(inner: LocalFsStore, gate_key: String) -> Self {
            Self {
                inner,
                gate_key,
                install_blocked_once: AtomicBool::new(false),
                install_entered: Semaphore::new(0),
                install_release: Semaphore::new(0),
                install_finished: Semaphore::new(0),
                delete_blocked_once: AtomicBool::new(false),
                delete_entered: Semaphore::new(0),
                delete_release: Semaphore::new(0),
            }
        }

        async fn wait_until_install_blocked(&self) {
            self.install_entered
                .acquire()
                .await
                .expect("blocking store remains open")
                .forget();
        }

        fn unblock_install(&self) {
            self.install_release.add_permits(1);
        }

        async fn wait_until_install_finished(&self) {
            self.install_finished
                .acquire()
                .await
                .expect("blocking store remains open")
                .forget();
        }

        async fn wait_until_delete_blocked(&self) {
            self.delete_entered
                .acquire()
                .await
                .expect("blocking store remains open")
                .forget();
        }

        fn unblock_delete(&self) {
            self.delete_release.add_permits(1);
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for BlockingRepairDeleteStore {
        async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
            self.inner.head(key).await
        }

        async fn get_with_metadata(
            &self,
            key: &str,
        ) -> Result<Option<ObjectBody>, ObjectStoreError> {
            self.inner.get_with_metadata(key).await
        }

        async fn get(
            &self,
            key: &str,
            range: Option<ByteRange>,
        ) -> Result<Option<Bytes>, ObjectStoreError> {
            self.inner.get(key, range).await
        }

        async fn put(
            &self,
            key: &str,
            bytes: Bytes,
            mode: PutMode,
        ) -> Result<ObjectMetadata, ObjectStoreError> {
            let installing_active_head = key == self.gate_key
                && matches!(&mode, PutMode::CreateIfAbsent)
                && decode_control_object::<HeadState>(&bytes, ControlObjectKind::WalHead)
                    .is_ok_and(|envelope| envelope.state.state == NamespaceState::Active)
                && !self.install_blocked_once.swap(true, Ordering::SeqCst);
            if installing_active_head {
                self.install_entered.add_permits(1);
                self.install_release
                    .acquire()
                    .await
                    .expect("blocking store remains open")
                    .forget();
                let result = self.inner.put(key, bytes, mode).await;
                self.install_finished.add_permits(1);
                return result;
            }
            self.inner.put(key, bytes, mode).await
        }

        async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
            if key != self.gate_key && !self.delete_blocked_once.swap(true, Ordering::SeqCst) {
                self.delete_entered.add_permits(1);
                self.delete_release
                    .acquire()
                    .await
                    .expect("blocking store remains open")
                    .forget();
            }
            self.inner.delete(key).await
        }

        fn list_prefix_stream(
            &self,
            prefix: &str,
        ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
            self.inner.list_prefix_stream(prefix)
        }
    }

    fn context(now_ms: u64) -> MutationContext {
        MutationContext {
            writer_id: "writer-a".to_owned(),
            writer_session_id: "wrs_test".to_owned(),
            writer_version: WRITER_VERSION.to_owned(),
            now_ms,
        }
    }

    /// Simulates pre-head create/fork debris with root and floor but no gating
    /// head. The root's manifest pointer is a dangling dummy: repair deletes
    /// this debris without reading through it.
    async fn write_pre_head_debris(store: &LocalFsStore, namespace_id: &NamespaceId) {
        let root = MetadataRootEnvelope::from_state(
            ControlObjectKind::MetadataRoot,
            WRITER_VERSION,
            MetadataRootState {
                namespace_id: namespace_id.clone(),
                manifest_id: ManifestId(0),
                manifest_object_id: ManifestObjectId::parse(
                    "00000000000000000000-0123456789abcdef",
                )
                .expect("valid manifest object id"),
                manifest_head_seq: ChangeSeq(0),
                manifest_payload_checksum:
                    "sha256:0000000000000000000000000000000000000000000000000000000000000000"
                        .to_owned(),
                updated_at_ms: 1_000,
            },
        )
        .expect("root envelope");
        store
            .put_if_absent(
                &metadata_root(namespace_id.as_str()),
                Bytes::from(encode_control_object(&root).expect("encode root")),
            )
            .await
            .expect("write root debris");
        let floor = WalFloorEnvelope::from_state(
            ControlObjectKind::WalFloor,
            WRITER_VERSION,
            WalFloorState {
                namespace_id: namespace_id.clone(),
                floor_seq: ChangeSeq(0),
                verified_at_ms: 1_000,
                updated_at_ms: 1_000,
            },
        )
        .expect("floor envelope");
        store
            .put_if_absent(
                &wal_floor(namespace_id.as_str()),
                Bytes::from(encode_control_object(&floor).expect("encode floor")),
            )
            .await
            .expect("write floor debris");
    }

    /// Derives "now" from durable object ages so the tests never touch a
    /// wall clock: `offset_ms` past the newest object under the namespace.
    async fn now_after_newest_object(
        store: &LocalFsStore,
        namespace_id: &NamespaceId,
        offset_ms: u64,
    ) -> u64 {
        let prefix = format!("namespaces/{}/", namespace_id.as_str());
        let mut newest = 0;
        for key in store.list_prefix(&prefix).await.expect("list namespace") {
            let modified = store
                .head(&key)
                .await
                .expect("head object")
                .expect("object exists")
                .last_modified_ms
                .expect("local fs provides timestamps");
            newest = newest.max(modified);
        }
        assert!(newest > 0, "namespace tree must not be empty");
        newest + offset_ms
    }

    async fn namespace_keys(store: &LocalFsStore, namespace_id: &NamespaceId) -> Vec<String> {
        store
            .list_prefix(&format!("namespaces/{}/", namespace_id.as_str()))
            .await
            .expect("list namespace")
    }

    async fn commit_directory(store: &LocalFsStore, namespace_id: &NamespaceId, name: &str) {
        let mut engine = NamespaceCommitEngine::new(namespace_id.clone());
        engine
            .publish_batch(
                store,
                vec![NamespaceMutationCandidate::commit(ApiCommitRequest {
                    commit_id: CommitId::generate(),
                    preconditions: Vec::new(),
                    ops: vec![ApiCommitOp::CreateDirectory {
                        parent_inode_id: loonfs_api::InodeId(1),
                        display_name: name.to_owned(),
                    }],
                    message: None,
                })],
                &context(2_000),
            )
            .await
            .results
            .pop()
            .expect("one commit result")
            .expect("commit lands");
    }

    #[tokio::test]
    async fn create_on_aged_pre_head_debris_stays_partial_until_repair_reaps() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        write_pre_head_debris(&store, &namespace_id).await;

        let aged = context(
            now_after_newest_object(&store, &namespace_id, GC_MIN_GRACE_WINDOW_MS + 1).await,
        );
        let keys_before = namespace_keys(&store, &namespace_id).await;
        let error = bootstrap_namespace(&store, &namespace_id, &aged, false)
            .await
            .expect_err("normal create never cleans aged debris");
        assert_eq!(error.code(), ErrorCode::NamespacePartial);
        assert_eq!(namespace_keys(&store, &namespace_id).await, keys_before);

        let report = repair_namespace(&store, &namespace_id, &aged)
            .await
            .expect("repair reaps aged debris");
        assert_eq!(report.outcome, RepairNamespaceOutcome::Reaped);
        bootstrap_namespace(&store, &namespace_id, &aged, false)
            .await
            .expect("create succeeds after explicit reap");
        assert_eq!(
            namespace_initialization_state(&store, &namespace_id)
                .await
                .expect("probe"),
            NamespaceInitializationState::Complete
        );
    }

    #[tokio::test]
    async fn young_debris_stays_partial_until_repair_safety_window_expires() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        write_pre_head_debris(&store, &namespace_id).await;

        let young = context(now_after_newest_object(&store, &namespace_id, 1).await);
        let error = bootstrap_namespace(&store, &namespace_id, &young, false)
            .await
            .expect_err("young debris may be a concurrent create");
        assert_eq!(error.code(), ErrorCode::NamespacePartial);
        let report = repair_namespace(&store, &namespace_id, &young)
            .await
            .expect("young repair is a typed refusal");
        assert_eq!(report.outcome, RepairNamespaceOutcome::InFlight);
        // The debris is untouched: repair never races a live attempt.
        assert!(store
            .head(&metadata_root(namespace_id.as_str()))
            .await
            .expect("head root")
            .is_some());
        assert!(store
            .head(&wal_floor(namespace_id.as_str()))
            .await
            .expect("head floor")
            .is_some());

        let aged = context(
            now_after_newest_object(&store, &namespace_id, GC_MIN_GRACE_WINDOW_MS + 1).await,
        );
        let report = repair_namespace(&store, &namespace_id, &aged)
            .await
            .expect("aged repair reaps");
        assert_eq!(report.outcome, RepairNamespaceOutcome::Reaped);
    }

    #[tokio::test]
    async fn create_reports_partial_until_repair_completes_the_headed_tree() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        bootstrap_namespace(&store, &namespace_id, &context(1_000), false)
            .await
            .expect("bootstrap");
        store
            .delete(&namespace_config(namespace_id.as_str()))
            .await
            .expect("simulate crash before the completion marker");

        let keys_before = namespace_keys(&store, &namespace_id).await;
        let error = bootstrap_namespace(&store, &namespace_id, &context(2_000), false)
            .await
            .expect_err("normal create leaves the partial tree untouched");
        assert_eq!(error.code(), ErrorCode::NamespacePartial);
        assert_eq!(namespace_keys(&store, &namespace_id).await, keys_before);
        let report = repair_namespace(&store, &namespace_id, &context(2_000))
            .await
            .expect("repair completes genesis tree");
        assert_eq!(report.outcome, RepairNamespaceOutcome::Completed);
        assert_eq!(
            namespace_initialization_state(&store, &namespace_id)
                .await
                .expect("probe"),
            NamespaceInitializationState::Complete
        );
        let error = bootstrap_namespace(&store, &namespace_id, &context(3_000), false)
            .await
            .expect_err("the completed namespace exists");
        assert_eq!(error.code(), ErrorCode::NamespaceExists);
        commit_directory(&store, &namespace_id, "docs").await;
        let status = load_namespace_head_summary(&store, &namespace_id)
            .await
            .expect("repaired namespace remains readable");
        assert_eq!(status.head_seq, ChangeSeq(1));
    }

    #[tokio::test]
    async fn non_completable_headed_tree_is_reaped_only_by_aged_repair() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        bootstrap_namespace(&store, &namespace_id, &context(1_000), false)
            .await
            .expect("bootstrap");
        commit_directory(&store, &namespace_id, "docs").await;
        store
            .delete(&namespace_config(namespace_id.as_str()))
            .await
            .expect("corrupt: drop the descriptor of a used namespace");

        let error = bootstrap_namespace(&store, &namespace_id, &context(2_000), false)
            .await
            .expect_err("a non-genesis tree is not a crashed create");
        assert_eq!(error.code(), ErrorCode::NamespacePartial);
        let young = context(now_after_newest_object(&store, &namespace_id, 1).await);
        let report = repair_namespace(&store, &namespace_id, &young)
            .await
            .expect("young repair refuses to reap");
        assert_eq!(report.outcome, RepairNamespaceOutcome::InFlight);

        let aged = context(
            now_after_newest_object(&store, &namespace_id, GC_MIN_GRACE_WINDOW_MS + 1).await,
        );
        let report = repair_namespace(&store, &namespace_id, &aged)
            .await
            .expect("aged repair reaps non-completable tree");
        assert_eq!(report.outcome, RepairNamespaceOutcome::Reaped);
        bootstrap_namespace(&store, &namespace_id, &aged, false)
            .await
            .expect("namespace is creatable after reap");
    }

    #[tokio::test]
    async fn repair_condemns_the_install_gate_before_a_racing_create() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let store = BlockingRepairDeleteStore::new(store, wal_head(namespace_id.as_str()));

        let create_context = context(1_000);
        let create = bootstrap_namespace(&store, &namespace_id, &create_context, false);
        let repair = async {
            store.wait_until_install_blocked().await;
            // The create passed its absent probe and prepared its immutable
            // manifest, but is paused at the first fixed-key conditional
            // install write.
            let repair_context = context(u64::MAX);
            let repair = repair_namespace(&store, &namespace_id, &repair_context);
            let coordinate = async {
                store.wait_until_delete_blocked().await;
                let head = read_head_object(&store, &namespace_id)
                    .await
                    .expect("condemned gate is durable before subtree deletion");
                assert_eq!(head.envelope.state.state, NamespaceState::Condemned);
                store.unblock_install();
                store.wait_until_install_finished().await;
                store.unblock_delete();
            };
            let (repair, ()) = tokio::join!(repair, coordinate);
            repair
        };
        let (create, repair) = tokio::join!(create, repair);

        assert_eq!(
            create.expect_err("racing create sees the gate").code(),
            ErrorCode::NamespacePartial
        );
        assert_eq!(
            repair.expect("repair completes").outcome,
            RepairNamespaceOutcome::Reaped
        );
        bootstrap_namespace(&store, &namespace_id, &context(2_000), false)
            .await
            .expect("fresh create succeeds after gate-last deletion");
    }

    #[tokio::test]
    async fn fork_reports_partial_until_repair_completes_its_headed_target() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let source = NamespaceId::parse("source").expect("namespace id");
        let clone = NamespaceId::parse("clone").expect("namespace id");
        bootstrap_namespace(&store, &source, &context(1_000), false)
            .await
            .expect("bootstrap source");
        commit_directory(&store, &source, "docs").await;
        fork_namespace(&store, &source, &clone, &context(2_000))
            .await
            .expect("fork");
        store
            .delete(&namespace_config(clone.as_str()))
            .await
            .expect("simulate crash before the completion marker");
        let keys_before = namespace_keys(&store, &clone).await;

        // A create must not adopt a fork tree: it would bind the wrong
        // content store.
        let error = bootstrap_namespace(&store, &clone, &context(3_000), false)
            .await
            .expect_err("create refuses the fork tree");
        assert_eq!(error.code(), ErrorCode::NamespacePartial);
        assert_eq!(namespace_keys(&store, &clone).await, keys_before);

        let error = fork_namespace(&store, &source, &clone, &context(3_000))
            .await
            .expect_err("normal fork leaves its partial target untouched");
        assert_eq!(error.code(), ErrorCode::NamespacePartial);
        assert_eq!(namespace_keys(&store, &clone).await, keys_before);
        let report = repair_namespace(&store, &clone, &context(3_000))
            .await
            .expect("repair completes fork target");
        assert_eq!(report.outcome, RepairNamespaceOutcome::Completed);
        let error = fork_namespace(&store, &source, &clone, &context(4_000))
            .await
            .expect_err("completed fork target exists");
        assert_eq!(error.code(), ErrorCode::NamespaceExists);
        let source_config = load_namespace_descriptor(&store, &source)
            .await
            .expect("source descriptor");
        let clone_config = load_namespace_descriptor(&store, &clone)
            .await
            .expect("clone descriptor");
        assert_eq!(
            clone_config.content_store_id, source_config.content_store_id,
            "the completed fork shares the source content store"
        );
    }

    #[tokio::test]
    async fn fork_on_aged_target_debris_stays_partial_until_repair_reaps() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let source = NamespaceId::parse("source").expect("namespace id");
        let clone = NamespaceId::parse("clone").expect("namespace id");
        bootstrap_namespace(&store, &source, &context(1_000), false)
            .await
            .expect("bootstrap source");
        commit_directory(&store, &source, "docs").await;
        write_pre_head_debris(&store, &clone).await;

        let aged =
            context(now_after_newest_object(&store, &clone, GC_MIN_GRACE_WINDOW_MS + 1).await);
        let keys_before = namespace_keys(&store, &clone).await;
        let error = fork_namespace(&store, &source, &clone, &aged)
            .await
            .expect_err("normal fork never cleans aged target debris");
        assert_eq!(error.code(), ErrorCode::NamespacePartial);
        assert_eq!(namespace_keys(&store, &clone).await, keys_before);
        let report = repair_namespace(&store, &clone, &aged)
            .await
            .expect("repair reaps aged target debris");
        assert_eq!(report.outcome, RepairNamespaceOutcome::Reaped);
        fork_namespace(&store, &source, &clone, &aged)
            .await
            .expect("fork succeeds after explicit reap");
        assert_eq!(
            namespace_initialization_state(&store, &clone)
                .await
                .expect("probe"),
            NamespaceInitializationState::Complete
        );
    }

    #[tokio::test]
    async fn repair_reports_already_complete_and_absent_is_not_found() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let complete = NamespaceId::parse("complete").expect("namespace id");
        let absent = NamespaceId::parse("absent").expect("namespace id");
        bootstrap_namespace(&store, &complete, &context(1_000), false)
            .await
            .expect("bootstrap");

        let report = repair_namespace(&store, &complete, &context(2_000))
            .await
            .expect("complete repair is a no-op");
        assert_eq!(report.outcome, RepairNamespaceOutcome::AlreadyComplete);
        let error = repair_namespace(&store, &absent, &context(2_000))
            .await
            .expect_err("absent namespace is not found");
        assert_eq!(error.code(), ErrorCode::NamespaceNotFound);
    }
}
