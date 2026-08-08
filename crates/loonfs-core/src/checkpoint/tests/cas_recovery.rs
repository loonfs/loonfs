//! Checkpoint publication CAS races, unknown outcomes, and recovery.

use super::*;

async fn build_manifest_from_projection<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    projection: &CurrentProjection,
    manifest_id: ManifestId,
) -> NamespaceManifestEnvelope {
    build_namespace_manifest_from_metadata_state(
        store,
        namespace_id,
        ManifestMetadataSource {
            head: &projection.head,
            basis_manifest_id: Some(projection.root.manifest_id),
            retention_floor_seq: read_floor_seq(store, namespace_id).await,
            metadata_state: &projection.metadata_state,
        },
        MetadataLsmPolicy::default(),
        manifest_id,
    )
    .await
    .expect("build manifest")
}

#[derive(Debug)]
struct ManifestSwapOnCasConflictStore {
    inner: LocalFsStore,
    namespace_id: NamespaceId,
    remaining_conflicts: std::sync::atomic::AtomicUsize,
}

impl ManifestSwapOnCasConflictStore {
    async fn install_competing_manifest_id(&self) {
        let loaded = read_metadata_root_object(&self.inner, &self.namespace_id)
            .await
            .expect("read root for swap");
        let mut root = loaded.envelope.state;
        // Same-seq replacement referencing a different manifest: the shape a
        // pure compaction publishes.
        root.manifest_id = ManifestId(root.manifest_id.0 + 1);
        let envelope = loonfs_api::wire::control::MetadataRootEnvelope::from_state(
            loonfs_api::wire::control::ControlObjectKind::MetadataRoot,
            root,
        )
        .expect("root envelope");
        let bytes =
            loonfs_api::wire::control::encode_control_object(&envelope).expect("root bytes");
        self.inner
            .put(
                &loonfs_objectstore::keys::metadata_root(self.namespace_id.as_str()),
                Bytes::from(bytes),
                PutMode::Overwrite,
            )
            .await
            .expect("install competing head");
    }
}

#[async_trait]
impl ObjectStore for ManifestSwapOnCasConflictStore {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.inner.head(key).await
    }

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        self.inner.get(key, range).await
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        self.inner.get_with_metadata(key).await
    }

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        self.inner.put(key, bytes, mode).await
    }

    async fn compare_and_swap(
        &self,
        key: &str,
        expected_etag: &str,
        bytes: Bytes,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        use std::sync::atomic::Ordering;
        if self.remaining_conflicts.load(Ordering::SeqCst) > 0 {
            self.remaining_conflicts.fetch_sub(1, Ordering::SeqCst);
            self.install_competing_manifest_id().await;
            return Err(ObjectStoreError::PreconditionFailed {
                object_key: key.to_owned(),
            });
        }
        self.inner.compare_and_swap(key, expected_etag, bytes).await
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.inner.delete(key).await
    }

    fn list_prefix_stream(
        &self,
        prefix: &str,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        self.inner.list_prefix_stream(prefix)
    }
}

#[derive(Debug)]
struct FloorRaiseOnCasConflictStore {
    inner: LocalFsStore,
    namespace_id: NamespaceId,
    remaining_conflicts: std::sync::atomic::AtomicUsize,
}

impl FloorRaiseOnCasConflictStore {
    async fn install_higher_floor(&self) {
        // The namespace may not have published a floor yet: create and fork
        // write none, so the competitor may be the first writer too.
        let mut floor = match read_wal_floor_object(&self.inner, &self.namespace_id).await {
            Ok(loaded) => loaded.envelope.state,
            Err(_) => loonfs_api::wire::control::WalFloorState {
                namespace_id: self.namespace_id.clone(),
                floor_seq: ChangeSeq(0),
                verified_at_ms: 1_000,
                updated_at_ms: 1_000,
            },
        };
        floor.floor_seq = ChangeSeq(5);
        let envelope = loonfs_api::wire::control::WalFloorEnvelope::from_state(
            loonfs_api::wire::control::ControlObjectKind::WalFloor,
            floor,
        )
        .expect("floor envelope");
        let bytes =
            loonfs_api::wire::control::encode_control_object(&envelope).expect("floor bytes");
        self.inner
            .put(
                &loonfs_objectstore::keys::wal_floor(self.namespace_id.as_str()),
                Bytes::from(bytes),
                PutMode::Overwrite,
            )
            .await
            .expect("write raised floor");
    }
}

#[async_trait]
impl ObjectStore for FloorRaiseOnCasConflictStore {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.inner.head(key).await
    }

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        self.inner.get(key, range).await
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        self.inner.get_with_metadata(key).await
    }

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        use std::sync::atomic::Ordering;
        // The first advance creates the floor object, so the competing
        // writer has to be able to win that race too.
        if mode == PutMode::CreateIfAbsent
            && key == loonfs_objectstore::keys::wal_floor(self.namespace_id.as_str())
            && self.remaining_conflicts.load(Ordering::SeqCst) > 0
        {
            self.remaining_conflicts.fetch_sub(1, Ordering::SeqCst);
            self.install_higher_floor().await;
            return Err(ObjectStoreError::PreconditionFailed {
                object_key: key.to_owned(),
            });
        }
        self.inner.put(key, bytes, mode).await
    }

    async fn compare_and_swap(
        &self,
        key: &str,
        expected_etag: &str,
        bytes: Bytes,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        use std::sync::atomic::Ordering;
        if key == loonfs_objectstore::keys::wal_floor(self.namespace_id.as_str())
            && self.remaining_conflicts.load(Ordering::SeqCst) > 0
        {
            self.remaining_conflicts.fetch_sub(1, Ordering::SeqCst);
            self.install_higher_floor().await;
            return Err(ObjectStoreError::PreconditionFailed {
                object_key: key.to_owned(),
            });
        }
        self.inner.compare_and_swap(key, expected_etag, bytes).await
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.inner.delete(key).await
    }

    fn list_prefix_stream(
        &self,
        prefix: &str,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        self.inner.list_prefix_stream(prefix)
    }
}

#[derive(Debug)]
struct StaleHeadOnceStore {
    inner: LocalFsStore,
    head_key: String,
    stale_head: std::sync::Mutex<Option<ObjectBody>>,
}

#[async_trait]
impl ObjectStore for StaleHeadOnceStore {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.inner.head(key).await
    }

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        self.inner.get(key, range).await
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        if key == self.head_key {
            if let Some(stale) = self.stale_head.lock().expect("stale head lock").take() {
                return Ok(Some(stale));
            }
        }
        self.inner.get_with_metadata(key).await
    }

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        self.inner.put(key, bytes, mode).await
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.inner.delete(key).await
    }

    fn list_prefix_stream(
        &self,
        prefix: &str,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        self.inner.list_prefix_stream(prefix)
    }
}

#[derive(Debug)]
struct RootCasTransportAfterApplyStore {
    inner: LocalFsStore,
    root_key: String,
    fail_next_root_cas: Mutex<bool>,
}

impl RootCasTransportAfterApplyStore {
    fn new(inner: LocalFsStore, root_key: String) -> Self {
        Self {
            inner,
            root_key,
            fail_next_root_cas: Mutex::new(false),
        }
    }

    fn fail_next_root_cas_after_apply(&self) {
        *self
            .fail_next_root_cas
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = true;
    }
}

#[async_trait]
impl ObjectStore for RootCasTransportAfterApplyStore {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.inner.head(key).await
    }

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        self.inner.get(key, range).await
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        self.inner.get_with_metadata(key).await
    }

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        if key == self.root_key && matches!(&mode, PutMode::CompareAndSwap { .. }) {
            let should_fail = {
                let mut fail_next = self
                    .fail_next_root_cas
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let should_fail = *fail_next;
                if should_fail {
                    *fail_next = false;
                }
                should_fail
            };
            if should_fail {
                self.inner.put(key, bytes, mode).await?;
                return Err(ObjectStoreError::transport(
                    key,
                    "simulated ambiguous root CAS transport error",
                ));
            }
        }
        self.inner.put(key, bytes, mode).await
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.inner.delete(key).await
    }

    fn list_prefix_stream(
        &self,
        prefix: &str,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        self.inner.list_prefix_stream(prefix)
    }
}

#[derive(Debug)]
struct RootCasTransportAfterCompetingRootStore {
    inner: LocalFsStore,
    root_key: String,
    competing_root: Mutex<Option<MetadataRootState>>,
}

impl RootCasTransportAfterCompetingRootStore {
    fn new(inner: LocalFsStore, root_key: String, competing_root: MetadataRootState) -> Self {
        Self {
            inner,
            root_key,
            competing_root: Mutex::new(Some(competing_root)),
        }
    }

    async fn install_competing_root(&self, root: MetadataRootState) {
        let envelope = loonfs_api::wire::control::MetadataRootEnvelope::from_state(
            loonfs_api::wire::control::ControlObjectKind::MetadataRoot,
            root,
        )
        .expect("metadata root envelope");
        let bytes = Bytes::from(
            loonfs_api::wire::control::encode_control_object(&envelope)
                .expect("encode metadata root"),
        );
        self.inner
            .put(&self.root_key, bytes, PutMode::Overwrite)
            .await
            .expect("install competing root");
    }
}

#[async_trait]
impl ObjectStore for RootCasTransportAfterCompetingRootStore {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.inner.head(key).await
    }

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        self.inner.get(key, range).await
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        self.inner.get_with_metadata(key).await
    }

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        if key == self.root_key && matches!(&mode, PutMode::CompareAndSwap { .. }) {
            let competing_root = {
                self.competing_root
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .take()
            };
            if let Some(root) = competing_root {
                self.install_competing_root(root).await;
                return Err(ObjectStoreError::transport(
                    key,
                    "simulated ambiguous root CAS transport error",
                ));
            }
        }
        self.inner.put(key, bytes, mode).await
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.inner.delete(key).await
    }

    fn list_prefix_stream(
        &self,
        prefix: &str,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        self.inner.list_prefix_stream(prefix)
    }
}

#[tokio::test]
async fn retention_advance_aborts_when_current_manifest_changes() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    let plain = LocalFsStore::new(temp_dir.path()).expect("store");
    bootstrap_namespace(&plain, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    write_file_bytes(
        &plain,
        &namespace_id,
        "/docs/hello.txt",
        b"hello\n",
        &context,
        None,
    )
    .await
    .expect("write hello");
    create_checkpoint(&plain, &namespace_id, &context)
        .await
        .expect("create checkpoint");

    // Lose the floor CAS once while a competing compactor swaps the root to
    // a same-seq replacement mid-flight. Root updates are monotonic, so the
    // replacement covers at least the derived floor and the retried publish
    // is safe to complete.
    let store = ManifestSwapOnCasConflictStore {
        inner: LocalFsStore::new(temp_dir.path()).expect("store"),
        namespace_id: namespace_id.clone(),
        remaining_conflicts: std::sync::atomic::AtomicUsize::new(1),
    };
    let response = advance_retention_floor(&store, &namespace_id, &context)
        .await
        .expect("floor advance survives a monotonic root swap");
    assert_eq!(response.retention_floor_seq, ChangeSeq(1));
    assert_eq!(
        read_floor_seq(&store.inner, &namespace_id).await,
        ChangeSeq(1)
    );
}

#[tokio::test]
async fn create_checkpoint_fails_when_its_manifest_key_holds_a_different_payload() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    let raw_store = LocalFsStore::new(temp_dir.path()).expect("raw store");
    bootstrap_namespace(&raw_store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    write_file_bytes(
        &raw_store,
        &namespace_id,
        "/docs/hello.txt",
        b"hello\n",
        &context,
        None,
    )
    .await
    .expect("write hello");

    // Every manifest object id ends in 16 random hex characters, so only this
    // publication proposes the key it writes. The store puts a different
    // payload there anyway, which is corruption, and the checkpoint must say
    // so rather than generate another id.
    let store = ConflictOnManifestCreateStore::mutate_next_inode(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        format!(
            "{}{:020}-",
            metadata_manifest_prefix(namespace_id.as_str()),
            2
        ),
    );

    let error = create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect_err("a conflicting manifest payload must fail the checkpoint");

    match error {
        CoreError::NamespaceCorrupt(message) => {
            assert!(
                message.contains(&metadata_manifest_prefix(namespace_id.as_str())),
                "the error must name the manifest key, got {message}"
            );
        }
        other => panic!("expected a namespace corruption error, got {other:?}"),
    }

    let materialization_after = load_current_projection(&store, &namespace_id)
        .await
        .expect("materialization");
    assert_eq!(materialization_after.root.manifest_id, ManifestId(1));
}

#[tokio::test]
async fn same_root_checkpoint_builders_write_distinct_manifest_objects_and_loser_is_superseded() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/hello.txt",
        b"hello\n",
        &context,
        None,
    )
    .await
    .expect("write hello");

    let materialization = load_current_projection(&store, &namespace_id)
        .await
        .expect("materialization");
    let first_manifest =
        build_manifest_from_projection(&store, &namespace_id, &materialization, ManifestId(1))
            .await;
    let second_manifest =
        build_manifest_from_projection(&store, &namespace_id, &materialization, ManifestId(1))
            .await;
    assert_eq!(
        first_manifest.payload.manifest_id,
        second_manifest.payload.manifest_id
    );
    assert_ne!(
        first_manifest.payload.manifest_object_id,
        second_manifest.payload.manifest_object_id
    );

    write_namespace_manifest(&store, &first_manifest)
        .await
        .expect("write first manifest");
    write_namespace_manifest(&store, &second_manifest)
        .await
        .expect("write second manifest");

    let first_outcome = publish_metadata_root(
        &store,
        &namespace_id,
        &first_manifest,
        Some(materialization.root.manifest_object_id.clone()),
        context.now_ms,
    )
    .await
    .expect("first publication succeeds");
    match first_outcome {
        ManifestPublicationOutcome::Published(root) => {
            assert_eq!(
                root.manifest_object_id,
                first_manifest.payload.manifest_object_id
            );
        }
        other => panic!("expected first publication to win, got {other:?}"),
    }

    let second_outcome = publish_metadata_root(
        &store,
        &namespace_id,
        &second_manifest,
        Some(materialization.root.manifest_object_id.clone()),
        context.now_ms + 1,
    )
    .await
    .expect("second publication should yield to the published root");
    match second_outcome {
        ManifestPublicationOutcome::Superseded(root) => {
            assert_eq!(
                root.manifest_object_id,
                first_manifest.payload.manifest_object_id
            );
        }
        other => panic!("expected second publication to be superseded, got {other:?}"),
    }

    let manifest_objects = store
        .list_prefix(&metadata_manifest_prefix(namespace_id.as_str()))
        .await
        .expect("list manifest objects");
    assert_eq!(
        manifest_objects.len(),
        3,
        "bootstrap and both race manifests should remain distinct immutable objects"
    );
}

#[tokio::test]
async fn lower_seq_root_publication_yields_to_the_newer_root() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/hello.txt",
        b"hello\n",
        &context,
        None,
    )
    .await
    .expect("write hello");

    let materialization_before = load_current_projection(&store, &namespace_id)
        .await
        .expect("materialization");
    let tables = build_manifest_tables(
        &store,
        &namespace_id,
        materialization_before.head.seq,
        CHECKPOINT_BASE_RUN_LEVEL,
        &materialization_before.metadata_state,
        MetadataLsmPolicy::default().max_rows_per_segment,
    )
    .await
    .expect("build metadata tables");
    let manifest = NamespaceManifestEnvelope::from_payload(NamespaceManifestPayload {
        namespace_id: namespace_id.clone(),
        manifest_id: ManifestId(materialization_before.head.seq.0),
        manifest_object_id: manifest_object_id(ManifestId(materialization_before.head.seq.0)),
        head_seq: materialization_before.head.seq,
        head_commit_id: materialization_before.head.head_commit_id.clone(),
        base_seq: materialization_before.head.seq,
        writer_epoch: materialization_before.head.writer_epoch,
        next_inode_id: materialization_before.head.next_inode_id,
        retention_floor_seq: read_floor_seq(&store, &namespace_id).await,
        metadata_files: flatten_manifest_tables(tables),
    })
    .expect("build manifest");
    write_namespace_manifest(&store, &manifest)
        .await
        .expect("write manifest");

    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/second.txt",
        b"second\n",
        &context,
        None,
    )
    .await
    .expect("write second");
    let later_checkpoint = create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("later checkpoint");
    assert!(later_checkpoint.checkpoint_seq > materialization_before.head.seq);

    let outcome = publish_metadata_root(
        &store,
        &namespace_id,
        &manifest,
        Some(materialization_before.root.manifest_object_id.clone()),
        context.now_ms,
    )
    .await
    .expect("manifest publication should classify the newer root");

    match outcome {
        ManifestPublicationOutcome::Superseded(current) => {
            assert_eq!(current.manifest_id, later_checkpoint.manifest_id);
        }
        other => panic!("expected superseded outcome, got {other:?}"),
    }
}

#[tokio::test]
async fn current_manifest_cas_retry_exhaustion_reports_head_race() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    let store = FailStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::metadata_root(namespace_id.as_str()),
        OperationClass::CompareAndSwap,
        InjectedError::PreconditionFailed,
    );
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/hello.txt",
        b"hello\n",
        &context,
        None,
    )
    .await
    .expect("write hello");
    let materialization = load_current_projection(&store, &namespace_id)
        .await
        .expect("materialization");
    let manifest = build_namespace_manifest_from_metadata_state(
        &store,
        &namespace_id,
        ManifestMetadataSource {
            head: &materialization.head,
            basis_manifest_id: Some(materialization.root.manifest_id),
            retention_floor_seq: read_floor_seq(&store, &namespace_id).await,
            metadata_state: &materialization.metadata_state,
        },
        MetadataLsmPolicy::default(),
        ManifestId(1),
    )
    .await
    .expect("build manifest");
    write_namespace_manifest(&store, &manifest)
        .await
        .expect("write manifest");

    store.fail_all();
    let outcome = publish_metadata_root(
        &store,
        &namespace_id,
        &manifest,
        Some(materialization.root.manifest_object_id.clone()),
        context.now_ms,
    )
    .await
    .expect("root publication should report CAS race");

    assert_eq!(outcome, ManifestPublicationOutcome::RootCasRaceLost);
}

#[tokio::test]
async fn root_cas_transport_error_recovers_when_candidate_was_published() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    let store = RootCasTransportAfterApplyStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        loonfs_objectstore::keys::metadata_root(namespace_id.as_str()),
    );
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/hello.txt",
        b"hello\n",
        &context,
        None,
    )
    .await
    .expect("write hello");
    let materialization = load_current_projection(&store, &namespace_id)
        .await
        .expect("materialization");
    let manifest = build_namespace_manifest_from_metadata_state(
        &store,
        &namespace_id,
        ManifestMetadataSource {
            head: &materialization.head,
            basis_manifest_id: Some(materialization.root.manifest_id),
            retention_floor_seq: read_floor_seq(&store, &namespace_id).await,
            metadata_state: &materialization.metadata_state,
        },
        MetadataLsmPolicy::default(),
        ManifestId(1),
    )
    .await
    .expect("build manifest");
    write_namespace_manifest(&store, &manifest)
        .await
        .expect("write manifest");

    store.fail_next_root_cas_after_apply();
    let outcome = publish_metadata_root(
        &store,
        &namespace_id,
        &manifest,
        Some(materialization.root.manifest_object_id.clone()),
        context.now_ms,
    )
    .await
    .expect("root publication should recover after ambiguous CAS");

    match outcome {
        ManifestPublicationOutcome::Published(root) => {
            assert_eq!(root.manifest_id, manifest.payload.manifest_id);
            assert_eq!(root.manifest_object_id, manifest.payload.manifest_object_id);
        }
        other => panic!("expected recovered published root, got {other:?}"),
    }
}

#[tokio::test]
async fn root_cas_transport_error_recovers_when_newer_root_was_published() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    let raw_store = LocalFsStore::new(temp_dir.path()).expect("raw store");
    bootstrap_namespace(&raw_store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    write_file_bytes(
        &raw_store,
        &namespace_id,
        "/docs/hello.txt",
        b"hello\n",
        &context,
        None,
    )
    .await
    .expect("write hello");

    let materialization = load_current_projection(&raw_store, &namespace_id)
        .await
        .expect("materialization");
    let candidate_manifest =
        build_manifest_from_projection(&raw_store, &namespace_id, &materialization, ManifestId(1))
            .await;
    let competing_manifest =
        build_manifest_from_projection(&raw_store, &namespace_id, &materialization, ManifestId(2))
            .await;
    write_namespace_manifest(&raw_store, &candidate_manifest)
        .await
        .expect("write candidate manifest");
    write_namespace_manifest(&raw_store, &competing_manifest)
        .await
        .expect("write competing manifest");

    let competing_root = MetadataRootState {
        namespace_id: namespace_id.clone(),
        manifest_id: competing_manifest.payload.manifest_id,
        manifest_object_id: competing_manifest.payload.manifest_object_id.clone(),
        manifest_head_seq: competing_manifest.payload.head_seq,
        manifest_payload_checksum: competing_manifest.payload_checksum.clone(),
        updated_at_ms: context.now_ms + 1,
    };
    let store = RootCasTransportAfterCompetingRootStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        loonfs_objectstore::keys::metadata_root(namespace_id.as_str()),
        competing_root,
    );

    let outcome = publish_metadata_root(
        &store,
        &namespace_id,
        &candidate_manifest,
        Some(materialization.root.manifest_object_id.clone()),
        context.now_ms,
    )
    .await
    .expect("root publication should recover by observing the competing root");

    match outcome {
        ManifestPublicationOutcome::Superseded(root) => {
            assert_eq!(root.manifest_id, competing_manifest.payload.manifest_id);
            assert_eq!(
                root.manifest_object_id,
                competing_manifest.payload.manifest_object_id
            );
        }
        other => panic!("expected superseded root after ambiguous CAS, got {other:?}"),
    }
}

#[tokio::test]
async fn floor_cas_never_regresses_under_a_competing_advancement() {
    // A competing GC actor lands a higher floor between our verification and
    // CAS. The retry observes it and yields: floors are monotonic.
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    let inner = LocalFsStore::new(temp_dir.path()).expect("store");
    bootstrap_namespace(&inner, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    write_file_bytes(
        &inner,
        &namespace_id,
        "/docs/hello.txt",
        b"hello\n",
        &context,
        None,
    )
    .await
    .expect("write hello");
    create_checkpoint(&inner, &namespace_id, &context)
        .await
        .expect("create checkpoint");

    let store = FloorRaiseOnCasConflictStore {
        inner,
        namespace_id: namespace_id.clone(),
        remaining_conflicts: std::sync::atomic::AtomicUsize::new(1),
    };
    let response = advance_retention_floor(&store, &namespace_id, &context)
        .await
        .expect("advance yields to the competing higher floor");
    // The competitor installed seq 5; our target of seq 1 must not clobber it.
    assert_eq!(response.retention_floor_seq, ChangeSeq(5));
    assert_eq!(
        read_floor_seq(&store.inner, &namespace_id).await,
        ChangeSeq(5)
    );
}

#[tokio::test]
async fn same_seq_root_replacement_publishes_a_compacted_manifest() {
    // A pure compaction publishes a different manifest for the same logical
    // seq. The monotonic root accepts it: seq must not decrease, the
    // manifest may change.
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/hello.txt",
        b"hello\n",
        &context,
        None,
    )
    .await
    .expect("write hello");
    let checkpoint = create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("create checkpoint");

    let materialization = load_current_projection(&store, &namespace_id)
        .await
        .expect("materialization");
    assert_eq!(materialization.root.manifest_id, checkpoint.manifest_id);
    let compacted = build_namespace_manifest_from_metadata_state(
        &store,
        &namespace_id,
        ManifestMetadataSource {
            head: &materialization.head,
            basis_manifest_id: Some(materialization.root.manifest_id),
            retention_floor_seq: read_floor_seq(&store, &namespace_id).await,
            metadata_state: &materialization.metadata_state,
        },
        MetadataLsmPolicy::default(),
        ManifestId(checkpoint.manifest_id.0 + 1),
    )
    .await
    .expect("build compacted manifest");
    assert_eq!(
        compacted.payload.head_seq,
        materialization.root.manifest_head_seq
    );
    write_namespace_manifest(&store, &compacted)
        .await
        .expect("write compacted manifest");

    let outcome = publish_metadata_root(
        &store,
        &namespace_id,
        &compacted,
        Some(materialization.root.manifest_object_id.clone()),
        context.now_ms,
    )
    .await
    .expect("same-seq replacement publishes");
    match outcome {
        ManifestPublicationOutcome::Published(root) => {
            assert_eq!(root.manifest_id, compacted.payload.manifest_id);
            assert_eq!(
                root.manifest_head_seq,
                materialization.root.manifest_head_seq
            );
        }
        other => panic!("expected published compacted root, got {other:?}"),
    }
    // Reads keep working against the replaced root.
    let after = load_current_projection(&store, &namespace_id)
        .await
        .expect("materialization after compaction");
    assert_eq!(after.root.manifest_id, compacted.payload.manifest_id);
    assert!(metadata_states_equivalent(
        &materialization.metadata_state,
        &after.metadata_state
    ));
}

#[tokio::test]
async fn read_anchor_reloads_the_head_when_the_root_is_ahead() {
    // A reader that loads a stale head next to a fresher root must reload
    // the head instead of treating the pair as corruption.
    let temp_dir = tempdir().expect("tempdir");
    let inner = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&inner, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    let stale_head = inner
        .get_with_metadata(&wal_head(namespace_id.as_str()))
        .await
        .expect("read bootstrap head")
        .expect("bootstrap head exists");

    write_file_bytes(
        &inner,
        &namespace_id,
        "/docs/hello.txt",
        b"hello\n",
        &context,
        None,
    )
    .await
    .expect("write hello");
    create_checkpoint(&inner, &namespace_id, &context)
        .await
        .expect("create checkpoint");

    let store = StaleHeadOnceStore {
        inner,
        head_key: wal_head(namespace_id.as_str()),
        stale_head: std::sync::Mutex::new(Some(stale_head)),
    };
    let projection = load_current_projection(&store, &namespace_id)
        .await
        .expect("read anchor resolves the stale-head race by reloading");
    assert_eq!(projection.head.seq, ChangeSeq(1));
    assert_eq!(projection.root.manifest_head_seq, ChangeSeq(1));
}

#[tokio::test]
async fn stale_basis_publication_cannot_clobber_a_sibling_root() {
    // The review's flush race: a publisher builds from root A while a
    // sibling publishes B from the same basis, then tries to win on a
    // higher head. Its carried-forward state predates B, so head ordering
    // must not decide the winner — the predecessor gate must.
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    write_file_bytes(&store, &namespace_id, "/one.txt", b"one\n", &context, None)
        .await
        .expect("write one");
    let sibling_basis = load_current_projection(&store, &namespace_id)
        .await
        .expect("sibling basis");

    // The stale publisher observes a newer head against the same root.
    write_file_bytes(&store, &namespace_id, "/two.txt", b"two\n", &context, None)
        .await
        .expect("write two");
    let stale_basis = load_current_projection(&store, &namespace_id)
        .await
        .expect("stale basis");
    assert_eq!(
        sibling_basis.root.manifest_object_id,
        stale_basis.root.manifest_object_id
    );
    assert!(stale_basis.head.seq > sibling_basis.head.seq);

    let sibling = build_manifest_from_projection(
        &store,
        &namespace_id,
        &sibling_basis,
        ManifestId(sibling_basis.root.manifest_id.0 + 1),
    )
    .await;
    let stale_higher_head = build_manifest_from_projection(
        &store,
        &namespace_id,
        &stale_basis,
        ManifestId(stale_basis.root.manifest_id.0 + 1),
    )
    .await;
    assert!(stale_higher_head.payload.head_seq > sibling.payload.head_seq);
    write_namespace_manifest(&store, &sibling)
        .await
        .expect("write sibling manifest");
    write_namespace_manifest(&store, &stale_higher_head)
        .await
        .expect("write stale manifest");

    let sibling_outcome = publish_metadata_root(
        &store,
        &namespace_id,
        &sibling,
        Some(sibling_basis.root.manifest_object_id.clone()),
        context.now_ms,
    )
    .await
    .expect("sibling publication");
    assert!(matches!(
        sibling_outcome,
        ManifestPublicationOutcome::Published(_)
    ));

    let stale_outcome = publish_metadata_root(
        &store,
        &namespace_id,
        &stale_higher_head,
        Some(stale_basis.root.manifest_object_id.clone()),
        context.now_ms + 1,
    )
    .await
    .expect("stale publication");
    match stale_outcome {
        ManifestPublicationOutcome::Superseded(root) => {
            assert_eq!(
                root.manifest_object_id, sibling.payload.manifest_object_id,
                "the sibling's acknowledged publication must survive"
            );
        }
        other => panic!("a stale-basis candidate must be superseded, got {other:?}"),
    }

    let root_after = read_metadata_root_object(&store, &namespace_id)
        .await
        .expect("read root")
        .envelope
        .state;
    assert_eq!(
        root_after.manifest_object_id,
        sibling.payload.manifest_object_id
    );
}
