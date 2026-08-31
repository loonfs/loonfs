//! Checkpoint publication CAS races, unknown outcomes, and recovery.

use super::*;

async fn build_manifest_from_projection<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    projection: &CurrentProjection,
    manifest_no: ManifestNo,
) -> NamespaceManifestEnvelope {
    build_namespace_manifest_from_metadata_state(
        store,
        namespace_id,
        ManifestMetadataSource {
            head: &projection.head,
            basis_manifest_no: Some(projection.root.manifest.manifest_no),
            retention_floor_seq: read_floor_seq(store, namespace_id).await,
            metadata_state: &projection.metadata_state,
        },
        MetadataLsmPolicy::default(),
        manifest_no,
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
    async fn install_competing_manifest_no(&self) {
        let loaded = load_metadata_root_object(&self.inner, &self.namespace_id)
            .await
            .expect("read root for swap");
        let mut root = loaded.state;
        // Same-seq replacement referencing a different manifest: the shape a
        // pure compaction publishes.
        root.manifest.manifest_no = ManifestNo(root.manifest.manifest_no.0 + 1);
        let envelope = loonfs_api::wire::control::MetadataRootEnvelope::from_state(
            loonfs_api::wire::control::ControlObjectKind::MetadataRoot,
            root,
        )
        .expect("root envelope");
        let bytes =
            loonfs_api::wire::control::encode_control_object(&envelope).expect("root bytes");
        self.inner
            .put(
                &loonfs_objectstore::keys::metadata_root(&self.namespace_id),
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
            self.install_competing_manifest_no().await;
            return Err(ObjectStoreError::PreconditionFailed {
                object_key: key.to_owned(),
            });
        }
        self.inner.compare_and_swap(key, expected_etag, bytes).await
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.inner.delete(key).await
    }

    fn list_prefix_from_stream(
        &self,
        prefix: &str,
        start_after: Option<&str>,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        self.inner.list_prefix_from_stream(prefix, start_after)
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
        let mut floor = match load_wal_floor_object(&self.inner, &self.namespace_id).await {
            Ok(loaded) => loaded.state,
            Err(_) => loonfs_api::wire::control::WalFloorState {
                namespace_id: self.namespace_id.clone(),
                floor_seq: ChangeSeq(0),
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
                &loonfs_objectstore::keys::wal_floor(&self.namespace_id),
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
            && key == loonfs_objectstore::keys::wal_floor(&self.namespace_id)
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
        if key == loonfs_objectstore::keys::wal_floor(&self.namespace_id)
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

    fn list_prefix_from_stream(
        &self,
        prefix: &str,
        start_after: Option<&str>,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        self.inner.list_prefix_from_stream(prefix, start_after)
    }
}

/// Returns one stale response for a selected key.
#[derive(Debug)]
struct StaleObjectOnceStore {
    inner: LocalFsStore,
    key: String,
    stale: std::sync::Mutex<Option<ObjectBody>>,
}

#[async_trait]
impl ObjectStore for StaleObjectOnceStore {
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
        if key == self.key {
            if let Some(stale) = self.stale.lock().expect("stale object lock").take() {
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

    fn list_prefix_from_stream(
        &self,
        prefix: &str,
        start_after: Option<&str>,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        self.inner.list_prefix_from_stream(prefix, start_after)
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

    fn list_prefix_from_stream(
        &self,
        prefix: &str,
        start_after: Option<&str>,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        self.inner.list_prefix_from_stream(prefix, start_after)
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

    fn list_prefix_from_stream(
        &self,
        prefix: &str,
        start_after: Option<&str>,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        self.inner.list_prefix_from_stream(prefix, start_after)
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
        format!("{}man_{:020}-", metadata_manifest_prefix(&namespace_id), 2),
    );

    let error = create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect_err("a conflicting manifest payload must fail the checkpoint");

    match error {
        CoreError::NamespaceCorrupt(message) => {
            assert!(
                message.contains(&metadata_manifest_prefix(&namespace_id)),
                "the error must name the manifest key, got {message}"
            );
        }
        other => panic!("expected a namespace corruption error, got {other:?}"),
    }

    let materialization_after = load_current_projection(&store, &namespace_id)
        .await
        .expect("materialization");
    assert_eq!(
        materialization_after.root.manifest.manifest_no,
        ManifestNo(1)
    );
}

#[tokio::test]
async fn same_root_publications_keep_the_first_candidate() {
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
    let sibling_manifest_no = ManifestNo(materialization.root.manifest.manifest_no.0 + 1);
    let first_manifest = build_manifest_from_projection(
        &store,
        &namespace_id,
        &materialization,
        sibling_manifest_no,
    )
    .await;
    let second_manifest = build_manifest_from_projection(
        &store,
        &namespace_id,
        &materialization,
        sibling_manifest_no,
    )
    .await;
    assert_eq!(
        first_manifest.payload.manifest_no,
        second_manifest.payload.manifest_no
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
        Some(materialization.root.manifest.manifest_object_id.clone()),
        context.now_ms,
    )
    .await
    .expect("first publication succeeds");
    match first_outcome {
        ManifestPublicationOutcome::Published(root) => {
            assert_eq!(
                root.manifest.manifest_object_id,
                first_manifest.payload.manifest_object_id
            );
        }
        other => panic!("expected first publication to win, got {other:?}"),
    }

    let second_outcome = publish_metadata_root(
        &store,
        &namespace_id,
        &second_manifest,
        Some(materialization.root.manifest.manifest_object_id.clone()),
        context.now_ms + 1,
    )
    .await
    .expect("second publication should yield to the published root");
    match second_outcome {
        ManifestPublicationOutcome::CoveredByCurrent(root) => {
            assert_eq!(
                root.manifest.manifest_object_id,
                first_manifest.payload.manifest_object_id
            );
        }
        other => panic!("expected the second publication to be covered, got {other:?}"),
    }

    let manifest_objects = store
        .list_prefix(&metadata_manifest_prefix(&namespace_id))
        .await
        .expect("list manifest objects");
    assert_eq!(
        manifest_objects.len(),
        3,
        "bootstrap and both race manifests should remain distinct immutable objects"
    );
}

#[tokio::test]
async fn flush_retries_when_a_same_seq_replacement_wins_behind_its_target() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    let inner = LocalFsStore::new(temp_dir.path()).expect("store");
    bootstrap_namespace(&inner, &namespace_id, &context, false)
        .await
        .expect("bootstrap");

    // Prepare the shape of a pure compaction: a logically equivalent
    // replacement at the root's existing sequence.
    let basis = load_current_projection(&inner, &namespace_id)
        .await
        .expect("load compaction basis");
    let replacement = build_manifest_from_projection(
        &inner,
        &namespace_id,
        &basis,
        ManifestNo(basis.root.manifest.manifest_no.0 + 1),
    )
    .await;
    write_namespace_manifest(&inner, &replacement)
        .await
        .expect("write replacement manifest");
    let competing_root = MetadataRootState {
        namespace_id: namespace_id.clone(),
        manifest: ManifestRef {
            owner_namespace_id: namespace_id.clone(),
            manifest_no: replacement.payload.manifest_no,
            manifest_object_id: replacement.payload.manifest_object_id.clone(),
            manifest_head_seq: replacement.payload.head_seq,
            manifest_payload_checksum: replacement.payload_checksum.clone(),
        },
        updated_at_ms: context.now_ms + 1,
    };

    // The flush observes a newer WAL head against the old root. Its first
    // root swap loses to the prepared same-sequence replacement.
    write_file_bytes(
        &inner,
        &namespace_id,
        "/later.txt",
        b"later\n",
        &context,
        None,
    )
    .await
    .expect("write later commit");
    let target = load_head_object(&inner, &namespace_id)
        .await
        .expect("load target head")
        .state
        .seq;
    assert!(replacement.payload.head_seq < target);

    let store = RootCasTransportAfterCompetingRootStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        loonfs_objectstore::keys::metadata_root(&namespace_id),
        competing_root,
    );
    let response = flush::flush_wal(&store, &namespace_id, &context)
        .await
        .expect("flush retries from the replacement root");

    assert_eq!(response.target_head_seq, target);
    assert_eq!(response.manifest_head_seq, target);
    assert_eq!(response.outcome, loonfs_api::FlushWalOutcome::Published);
    let root = load_metadata_root_object(&store, &namespace_id)
        .await
        .expect("load published root")
        .state;
    assert_eq!(root.manifest.manifest_head_seq, target);
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
    let segments = build_manifest_segments(
        &store,
        &namespace_id,
        RunNo(0),
        materialization_before.head.seq,
        CHECKPOINT_BASE_RUN_LEVEL,
        &materialization_before.metadata_state,
        MetadataLsmPolicy::default().max_rows_per_segment,
    )
    .await
    .expect("build metadata segments");
    let manifest = NamespaceManifestEnvelope::from_payload(NamespaceManifestPayload {
        namespace_id: namespace_id.clone(),
        manifest_no: ManifestNo(materialization_before.head.seq.0),
        manifest_object_id: manifest_object_id(ManifestNo(materialization_before.head.seq.0)),
        head_seq: materialization_before.head.seq,
        head_commit_id: materialization_before.head.head_commit_id.clone(),
        base_seq: materialization_before.head.seq,
        writer_epoch: materialization_before.head.writer_epoch,
        next_inode_id: materialization_before.head.next_inode_id,
        next_run_no: RunNo(1),
        retention_floor_seq: read_floor_seq(&store, &namespace_id).await,
        segments: flatten_manifest_segments(segments),
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
        Some(
            materialization_before
                .root
                .manifest
                .manifest_object_id
                .clone(),
        ),
        context.now_ms,
    )
    .await
    .expect("manifest publication should classify the newer root");

    match outcome {
        ManifestPublicationOutcome::CoveredByCurrent(current) => {
            assert_eq!(current.manifest.manifest_no, later_checkpoint.manifest_no);
        }
        other => panic!("expected a covering root, got {other:?}"),
    }
}

#[tokio::test]
async fn root_cas_conflict_reports_a_race() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    let store = FailStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::metadata_root(&namespace_id),
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
            basis_manifest_no: Some(materialization.root.manifest.manifest_no),
            retention_floor_seq: read_floor_seq(&store, &namespace_id).await,
            metadata_state: &materialization.metadata_state,
        },
        MetadataLsmPolicy::default(),
        ManifestNo(materialization.root.manifest.manifest_no.0 + 1),
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
        Some(materialization.root.manifest.manifest_object_id.clone()),
        context.now_ms,
    )
    .await
    .expect("root publication should report CAS contention");

    assert_eq!(outcome, ManifestPublicationOutcome::RootCasRaceLost);
}

#[tokio::test]
async fn root_cas_transport_error_recovers_when_candidate_was_published() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    let store = RootCasTransportAfterApplyStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        loonfs_objectstore::keys::metadata_root(&namespace_id),
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
            basis_manifest_no: Some(materialization.root.manifest.manifest_no),
            retention_floor_seq: read_floor_seq(&store, &namespace_id).await,
            metadata_state: &materialization.metadata_state,
        },
        MetadataLsmPolicy::default(),
        ManifestNo(materialization.root.manifest.manifest_no.0 + 1),
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
        Some(materialization.root.manifest.manifest_object_id.clone()),
        context.now_ms,
    )
    .await
    .expect("root publication should recover after ambiguous CAS");

    match outcome {
        ManifestPublicationOutcome::Published(root) => {
            assert_eq!(root.manifest.manifest_no, manifest.payload.manifest_no);
            assert_eq!(
                root.manifest.manifest_object_id,
                manifest.payload.manifest_object_id
            );
        }
        other => panic!("expected the candidate to be found current, got {other:?}"),
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
    let candidate_manifest_no = ManifestNo(materialization.root.manifest.manifest_no.0 + 1);
    let candidate_manifest = build_manifest_from_projection(
        &raw_store,
        &namespace_id,
        &materialization,
        candidate_manifest_no,
    )
    .await;
    let competing_manifest = build_manifest_from_projection(
        &raw_store,
        &namespace_id,
        &materialization,
        ManifestNo(candidate_manifest_no.0 + 1),
    )
    .await;
    write_namespace_manifest(&raw_store, &candidate_manifest)
        .await
        .expect("write candidate manifest");
    write_namespace_manifest(&raw_store, &competing_manifest)
        .await
        .expect("write competing manifest");

    let competing_root = MetadataRootState {
        namespace_id: namespace_id.clone(),
        manifest: ManifestRef {
            owner_namespace_id: namespace_id.clone(),
            manifest_no: competing_manifest.payload.manifest_no,
            manifest_object_id: competing_manifest.payload.manifest_object_id.clone(),
            manifest_head_seq: competing_manifest.payload.head_seq,
            manifest_payload_checksum: competing_manifest.payload_checksum.clone(),
        },
        updated_at_ms: context.now_ms + 1,
    };
    let store = RootCasTransportAfterCompetingRootStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        loonfs_objectstore::keys::metadata_root(&namespace_id),
        competing_root,
    );

    let outcome = publish_metadata_root(
        &store,
        &namespace_id,
        &candidate_manifest,
        Some(materialization.root.manifest.manifest_object_id.clone()),
        context.now_ms,
    )
    .await
    .expect("root publication should recover by observing the competing root");

    match outcome {
        ManifestPublicationOutcome::CoveredByCurrent(root) => {
            assert_eq!(
                root.manifest.manifest_no,
                competing_manifest.payload.manifest_no
            );
            assert_eq!(
                root.manifest.manifest_object_id,
                competing_manifest.payload.manifest_object_id
            );
        }
        other => panic!("expected a covering root after an ambiguous CAS, got {other:?}"),
    }
}

#[tokio::test]
async fn first_root_create_recovers_when_the_write_lands_ambiguously() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    let store = FailStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::exact(metadata_root(&namespace_id)),
        OperationClass::PutCreateIfAbsent,
        InjectedError::Transport("lost first-root acknowledgment".to_owned()),
    )
    .apply_then_fail();
    crate::namespace::bootstrap::bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap without a root");
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
    store.fail_next(1);

    let response = flush::flush_wal(&store, &namespace_id, &context)
        .await
        .expect("root read-back reconciles the landed create");

    assert_eq!(response.manifest_head_seq, ChangeSeq(1));
    assert_eq!(store.attempts(), 1);
    assert_eq!(
        load_metadata_root_object(&store, &namespace_id)
            .await
            .expect("load reconciled root")
            .state
            .manifest
            .manifest_head_seq,
        ChangeSeq(1)
    );
}

#[tokio::test]
async fn first_floor_create_recovers_when_the_write_lands_ambiguously() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    let store = FailStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::exact(wal_floor(&namespace_id)),
        OperationClass::PutCreateIfAbsent,
        InjectedError::Transport("lost first-floor acknowledgment".to_owned()),
    )
    .apply_then_fail();
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
    flush::flush_wal(&store, &namespace_id, &context)
        .await
        .expect("publish root at sequence one");
    store.fail_next(1);

    let response = advance_retention_floor(&store, &namespace_id, &context)
        .await
        .expect("floor read-back reconciles the landed create");

    assert_eq!(response.retention_floor_seq, ChangeSeq(1));
    assert_eq!(store.attempts(), 1);
    assert_eq!(read_floor_seq(&store, &namespace_id).await, ChangeSeq(1));
}

#[tokio::test]
async fn floor_cas_recovers_when_the_write_lands_ambiguously() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    let store = FailStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::exact(wal_floor(&namespace_id)),
        OperationClass::CompareAndSwap,
        InjectedError::Transport("lost floor-CAS acknowledgment".to_owned()),
    )
    .apply_then_fail();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");

    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/one.txt",
        b"one\n",
        &context,
        None,
    )
    .await
    .expect("write first file");
    flush::flush_wal(&store, &namespace_id, &context)
        .await
        .expect("publish root at sequence one");
    advance_retention_floor(&store, &namespace_id, &context)
        .await
        .expect("create the initial floor");

    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/two.txt",
        b"two\n",
        &context,
        None,
    )
    .await
    .expect("write second file");
    flush::flush_wal(&store, &namespace_id, &context)
        .await
        .expect("publish root at sequence two");
    assert_eq!(read_floor_seq(&store, &namespace_id).await, ChangeSeq(1));
    store.fail_next(1);

    let response = advance_retention_floor(&store, &namespace_id, &context)
        .await
        .expect("floor read-back reconciles the landed CAS");

    assert_eq!(response.retention_floor_seq, ChangeSeq(2));
    assert_eq!(store.attempts(), 1);
    assert_eq!(read_floor_seq(&store, &namespace_id).await, ChangeSeq(2));
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
    assert_eq!(
        materialization.root.manifest.manifest_no,
        checkpoint.manifest_no
    );
    let compacted = build_namespace_manifest_from_metadata_state(
        &store,
        &namespace_id,
        ManifestMetadataSource {
            head: &materialization.head,
            basis_manifest_no: Some(materialization.root.manifest.manifest_no),
            retention_floor_seq: read_floor_seq(&store, &namespace_id).await,
            metadata_state: &materialization.metadata_state,
        },
        MetadataLsmPolicy::default(),
        ManifestNo(checkpoint.manifest_no.0 + 1),
    )
    .await
    .expect("build compacted manifest");
    assert_eq!(
        compacted.payload.head_seq,
        materialization.root.manifest.manifest_head_seq
    );
    write_namespace_manifest(&store, &compacted)
        .await
        .expect("write compacted manifest");

    let outcome = publish_metadata_root(
        &store,
        &namespace_id,
        &compacted,
        Some(materialization.root.manifest.manifest_object_id.clone()),
        context.now_ms,
    )
    .await
    .expect("same-seq replacement publishes");
    match outcome {
        ManifestPublicationOutcome::Published(root) => {
            assert_eq!(root.manifest.manifest_no, compacted.payload.manifest_no);
            assert_eq!(
                root.manifest.manifest_head_seq,
                materialization.root.manifest.manifest_head_seq
            );
        }
        other => panic!("expected the compacted root to install, got {other:?}"),
    }
    // Reads keep working against the replaced root.
    let after = load_current_projection(&store, &namespace_id)
        .await
        .expect("materialization after compaction");
    assert_eq!(
        after.root.manifest.manifest_no,
        compacted.payload.manifest_no
    );
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
        .get_with_metadata(&wal_head(&namespace_id))
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

    let store = StaleObjectOnceStore {
        inner,
        key: wal_head(&namespace_id),
        stale: std::sync::Mutex::new(Some(stale_head)),
    };
    let projection = load_current_projection(&store, &namespace_id)
        .await
        .expect("read anchor resolves the stale-head race by reloading");
    assert_eq!(projection.head.seq, ChangeSeq(1));
    assert_eq!(projection.root.manifest.manifest_head_seq, ChangeSeq(1));
}

#[tokio::test]
async fn namespace_status_and_change_feed_reload_a_head_behind_the_floor() {
    // Retention publishes the floor independently of the head. A reader that
    // straddles those writes must not report a floor newer than its head.
    let temp_dir = tempdir().expect("tempdir");
    let inner = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&inner, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    let stale_head = inner
        .get_with_metadata(&wal_head(&namespace_id))
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
    advance_retention_floor(&inner, &namespace_id, &context)
        .await
        .expect("advance retention");

    let status_store = StaleObjectOnceStore {
        inner: LocalFsStore::new(temp_dir.path()).expect("status store"),
        key: wal_head(&namespace_id),
        stale: std::sync::Mutex::new(Some(stale_head.clone())),
    };
    let namespace = load_namespace(&status_store, &namespace_id)
        .await
        .expect("status reloads the stale head");
    assert_eq!(namespace.head_seq, ChangeSeq(1));
    assert_eq!(namespace.retention_floor_seq, ChangeSeq(1));

    let feed_store = StaleObjectOnceStore {
        inner: LocalFsStore::new(temp_dir.path()).expect("change-feed store"),
        key: wal_head(&namespace_id),
        stale: std::sync::Mutex::new(Some(stale_head)),
    };
    let changes = list_changes_after(
        &feed_store,
        &namespace_id,
        ChangeSeq(1),
        EffectiveLimit::new(NonZeroU32::new(10).expect("nonzero")),
    )
    .await
    .expect("change feed reloads the stale head");
    assert_eq!(changes.through_seq, ChangeSeq(1));
    assert!(changes.changes.is_empty());
}

#[tokio::test]
async fn diagnostics_reload_a_root_behind_the_floor() {
    // A floor above the observed root proves that the root read is stale.
    let temp_dir = tempdir().expect("tempdir");
    let inner = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&inner, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    write_file_bytes(&inner, &namespace_id, "/one.txt", b"one\n", &context, None)
        .await
        .expect("write one");
    create_checkpoint(&inner, &namespace_id, &context)
        .await
        .expect("first checkpoint");
    let stale_root = inner
        .get_with_metadata(&metadata_root(&namespace_id))
        .await
        .expect("read first root")
        .expect("first root exists");

    write_file_bytes(&inner, &namespace_id, "/two.txt", b"two\n", &context, None)
        .await
        .expect("write two");
    create_checkpoint(&inner, &namespace_id, &context)
        .await
        .expect("second checkpoint");
    advance_retention_floor(&inner, &namespace_id, &context)
        .await
        .expect("advance retention");
    let current_manifest_no = load_metadata_root_object(&inner, &namespace_id)
        .await
        .expect("read current root")
        .state
        .manifest
        .manifest_no;

    let store = StaleObjectOnceStore {
        inner,
        key: metadata_root(&namespace_id),
        stale: std::sync::Mutex::new(Some(stale_root)),
    };
    let diagnostics = load_namespace_diagnostics(&store, &namespace_id)
        .await
        .expect("diagnostics reload the stale root");
    assert_eq!(diagnostics.retention_floor_seq, ChangeSeq(2));
    assert_eq!(diagnostics.current_manifest_no, Some(current_manifest_no));
    assert_eq!(diagnostics.wal_tail_segments, 0);
}

#[tokio::test]
async fn steady_state_reads_stay_off_the_objects_they_do_not_need() {
    let temp_dir = tempdir().expect("tempdir");
    let inner = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&inner, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    write_file_bytes(&inner, &namespace_id, "/one.txt", b"one\n", &context, None)
        .await
        .expect("write one");
    create_checkpoint(&inner, &namespace_id, &context)
        .await
        .expect("create checkpoint");

    let store = CountingStore::new(inner, KeyPredicate::exact(metadata_root(&namespace_id)));
    load_namespace(&store, &namespace_id)
        .await
        .expect("load namespace");
    list_changes_after(
        &store,
        &namespace_id,
        ChangeSeq(0),
        EffectiveLimit::new(NonZeroU32::new(10).expect("nonzero")),
    )
    .await
    .expect("list changes");
    assert_eq!(
        store.snapshot().operations(OperationClass::Read),
        0,
        "status and the change feed resolve from head and floor alone"
    );

    let basis_store = CountingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("basis store"),
        KeyPredicate::exact(wal_floor(&namespace_id)),
    );
    load_current_metadata_view(&basis_store, &namespace_id)
        .await
        .expect("basis load");
    assert_eq!(
        basis_store.snapshot().operations(OperationClass::Read),
        0,
        "a basis load with a root present never reads the floor"
    );
}

#[tokio::test]
async fn stale_basis_publication_cannot_clobber_a_sibling_root() {
    // Build two candidates from the same root. The candidate with the newer
    // WAL head must not overwrite a sibling whose state it does not include.
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

    // The second candidate observes a newer head against the same root.
    write_file_bytes(&store, &namespace_id, "/two.txt", b"two\n", &context, None)
        .await
        .expect("write two");
    let stale_basis = load_current_projection(&store, &namespace_id)
        .await
        .expect("stale basis");
    assert_eq!(
        sibling_basis.root.manifest.manifest_object_id,
        stale_basis.root.manifest.manifest_object_id
    );
    assert!(stale_basis.head.seq > sibling_basis.head.seq);

    let sibling = build_manifest_from_projection(
        &store,
        &namespace_id,
        &sibling_basis,
        ManifestNo(sibling_basis.root.manifest.manifest_no.0 + 1),
    )
    .await;
    let stale_higher_head = build_manifest_from_projection(
        &store,
        &namespace_id,
        &stale_basis,
        ManifestNo(stale_basis.root.manifest.manifest_no.0 + 1),
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
        Some(sibling_basis.root.manifest.manifest_object_id.clone()),
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
        Some(stale_basis.root.manifest.manifest_object_id.clone()),
        context.now_ms + 1,
    )
    .await
    .expect("stale publication");
    match stale_outcome {
        ManifestPublicationOutcome::PredecessorChanged(root) => {
            assert_eq!(
                root.manifest.manifest_object_id, sibling.payload.manifest_object_id,
                "the sibling's acknowledged publication must survive"
            );
        }
        other => panic!("a stale-basis candidate must not install, got {other:?}"),
    }

    let root_after = load_metadata_root_object(&store, &namespace_id)
        .await
        .expect("read root")
        .state;
    assert_eq!(
        root_after.manifest.manifest_object_id,
        sibling.payload.manifest_object_id
    );
}
