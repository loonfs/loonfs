//! Checkpoint retention, reorganization, compaction, and publication budgets.

use super::*;
use loonfs_objectstore::keys::{checkpoint_prefix, wal_segment_prefix};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[derive(Debug)]
struct ManifestChecksumMismatchOnceStore {
    inner: LocalFsStore,
    checkpoint_prefix: String,
    manifest_key: String,
    mismatched_manifest: Bytes,
    mismatch_armed: AtomicBool,
    mismatch_injected: AtomicBool,
    checkpoint_writes: AtomicUsize,
}

impl ManifestChecksumMismatchOnceStore {
    fn new(
        inner: LocalFsStore,
        namespace_id: &NamespaceId,
        manifest_key: String,
        mismatched_manifest: Bytes,
    ) -> Self {
        Self {
            inner,
            checkpoint_prefix: checkpoint_prefix(namespace_id),
            manifest_key,
            mismatched_manifest,
            mismatch_armed: AtomicBool::new(false),
            mismatch_injected: AtomicBool::new(false),
            checkpoint_writes: AtomicUsize::new(0),
        }
    }

    fn inner(&self) -> &LocalFsStore {
        &self.inner
    }

    fn checkpoint_writes(&self) -> usize {
        self.checkpoint_writes.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl ObjectStore for ManifestChecksumMismatchOnceStore {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.inner.head(key).await
    }

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        let full_object = range.is_none();
        let stored = self.inner.get(key, range).await?;
        if key == self.manifest_key
            && full_object
            && self.mismatch_armed.swap(false, Ordering::SeqCst)
        {
            return Ok(Some(self.mismatched_manifest.clone()));
        }
        Ok(stored)
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
        let creates_checkpoint =
            key.starts_with(&self.checkpoint_prefix) && matches!(&mode, PutMode::CreateIfAbsent);
        let result = self.inner.put(key, bytes, mode).await;
        if result.is_ok() && creates_checkpoint {
            self.checkpoint_writes.fetch_add(1, Ordering::SeqCst);
            // Only the first verification observes the contradiction. If the
            // operation retries, the next basis is sound and would hide it.
            if !self.mismatch_injected.swap(true, Ordering::SeqCst) {
                self.mismatch_armed.store(true, Ordering::SeqCst);
            }
        }
        result
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

async fn current_manifest_no<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> ManifestNo {
    load_metadata_root_object(store, namespace_id)
        .await
        .expect("read metadata root")
        .state
        .manifest_no
}

fn run_segment_object_keys(manifest: &NamespaceManifestEnvelope) -> Vec<String> {
    runs_from_segments(&manifest.payload)
        .into_iter()
        .flat_map(|run| {
            run.segments.into_iter().flat_map(|family_segments| {
                family_segments
                    .segments
                    .into_iter()
                    .map(|descriptor| descriptor.object_key.clone())
            })
        })
        .collect()
}

/// Compares two states written by two independent runs of the same
/// operations.
///
/// Every such run stages its own content objects, so revision rows name
/// different ids by design — and because a commit's fingerprint covers which
/// object a put attached, the receipt rows differ too. Both differences come
/// from the seeding, not from the reorganization under test, so both are
/// normalized away here and only here. Comparisons within one store stay
/// exact.
fn metadata_states_equivalent_ignoring_content_identity(
    left: &MetadataState,
    right: &MetadataState,
) -> bool {
    let normalize = |state: &MetadataState| {
        CHECKPOINT_ROW_FAMILIES
            .into_iter()
            .map(|family| {
                let rows = manifest_rows_for_family(state, family)
                    .into_iter()
                    .map(|row| match row {
                        MetadataRow::Revision {
                            inode_id,
                            revision_no,
                            committed_seq,
                            commit_id,
                            committed_at_ms,
                            actor,
                            revision_delta_index,
                            content_ref,
                        } => MetadataRow::Revision {
                            inode_id,
                            revision_no,
                            committed_seq,
                            commit_id,
                            committed_at_ms,
                            actor,
                            revision_delta_index,
                            content_ref: loonfs_api::ContentRef {
                                content_id: loonfs_api::ContentId::parse(
                                    "con_00000000000000000000000000000000",
                                )
                                .expect("placeholder content id"),
                                ..content_ref
                            },
                        },
                        MetadataRow::CommitReceipt {
                            commit_id,
                            actor,
                            semantic_commit_fingerprint: _,
                            committed_seq,
                            committed_at_ms,
                            message,
                        } => MetadataRow::CommitReceipt {
                            commit_id,
                            actor,
                            semantic_commit_fingerprint: "<normalized>".to_owned(),
                            committed_seq,
                            committed_at_ms,
                            message,
                        },
                        other => other,
                    })
                    .collect::<Vec<_>>();
                (family, rows)
            })
            .collect::<Vec<_>>()
    };
    normalize(left) == normalize(right)
}

fn assert_manifest_rows_have_unique_keys(metadata_state: &MetadataState) {
    for family in CHECKPOINT_ROW_FAMILIES {
        let rows = manifest_rows_for_family(metadata_state, family);
        let mut seen = BTreeSet::new();
        for row in rows {
            let row_key = row.row_key_for_family(family);
            assert!(
                seen.insert(row_key.clone()),
                "duplicate metadata row key `{row_key}` in {family:?}"
            );
        }
    }
}

type FamilyRunShape = (ApiMetadataRowFamily, u64, usize);
type ManifestRunShape = (ChangeSeq, u32, Vec<FamilyRunShape>);

fn manifest_run_shape(manifest: &NamespaceManifestEnvelope) -> Vec<ManifestRunShape> {
    runs_from_segments(&manifest.payload)
        .into_iter()
        .map(|run| {
            let segments = run
                .segments
                .into_iter()
                .map(|family_segments| {
                    let row_count = family_segments
                        .segments
                        .iter()
                        .map(|descriptor| descriptor.row_count)
                        .sum();
                    (
                        family_segments.family,
                        row_count,
                        family_segments.segments.len(),
                    )
                })
                .collect();
            (run.run_seq, run.level, segments)
        })
        .collect()
}

async fn drain_reorganization_with_count<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
    policy: MetadataLsmPolicy,
) -> (ManifestNo, usize) {
    let mut published = 0usize;
    for _ in 0..128 {
        let report = super::reorganize_metadata_step(
            store,
            namespace_id,
            context,
            policy,
            MetadataCompactionView::default(),
        )
        .await
        .expect("reorganization step");
        match report.outcome {
            super::MetadataReorganizeOutcome::UnitPublished { .. } => published += 1,
            super::MetadataReorganizeOutcome::Superseded => {
                panic!("single-writer test must not be superseded")
            }
            super::MetadataReorganizeOutcome::CompactionPlanned { .. } => {
                panic!("test budget must admit a progress-making subset")
            }
            super::MetadataReorganizeOutcome::NotNeeded { .. } => {
                return (current_manifest_no(store, namespace_id).await, published);
            }
        }
    }
    panic!("reorganization did not converge")
}

/// Deterministic timer advancing a fixed step per reading, so publication
/// budgets are consumed by observations instead of wall time.
#[derive(Debug)]
struct SteppingTimer {
    now_ms: std::sync::atomic::AtomicU64,
    step_ms: u64,
}

impl SteppingTimer {
    fn new(step_ms: u64) -> Self {
        Self {
            now_ms: std::sync::atomic::AtomicU64::new(0),
            step_ms,
        }
    }
}

impl crate::time::MonotonicTimer for SteppingTimer {
    fn monotonic_now_ms(&self) -> u64 {
        self.now_ms
            .fetch_add(self.step_ms, std::sync::atomic::Ordering::SeqCst)
    }
}

#[tokio::test]
async fn retention_advancement_uses_published_manifest_and_updates_floor_only() {
    let temp_dir = tempdir().expect("tempdir");
    let store =
        CountingStore::metadata_segments(LocalFsStore::new(temp_dir.path()).expect("store"));
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");

    store.reset();
    let unchanged = advance_retention_floor(&store, &namespace_id, &context)
        .await
        .expect("initial manifest already covers floor zero");
    assert_eq!(unchanged.retention_floor_seq, ChangeSeq(0));
    assert_eq!(
        store.count(OperationClass::Read),
        0,
        "retention should validate manifest descriptors without loading metadata segment payloads"
    );

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
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("create checkpoint");
    store.reset();
    let advanced = advance_retention_floor(&store, &namespace_id, &context)
        .await
        .expect("advance retention");
    assert_eq!(advanced.retention_floor_seq, ChangeSeq(1));
    assert_eq!(
        store.count(OperationClass::Read),
        0,
        "retention should advance from manifest descriptors without materializing rows"
    );

    assert_eq!(read_floor_seq(&store, &namespace_id).await, ChangeSeq(1));
    assert_eq!(
        store
            .list_prefix(&format!(
                "namespaces/{}/wal/segments/",
                namespace_id.as_str()
            ))
            .await
            .expect("list wal")
            .len(),
        1
    );
    assert!(store
        .head(&current_manifest_key(&store, &namespace_id).await)
        .await
        .expect("manifest head")
        .is_some());
}

#[tokio::test]
async fn retention_floor_advancement_preserves_writer_identity() {
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
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("create checkpoint");
    let before = load_current_projection(&store, &namespace_id)
        .await
        .expect("load before retention")
        .head;

    advance_retention_floor(&store, &namespace_id, &context)
        .await
        .expect("advance retention");
    let after = load_current_projection(&store, &namespace_id)
        .await
        .expect("load after retention")
        .head;

    assert_eq!(read_floor_seq(&store, &namespace_id).await, ChangeSeq(1));
    // Floor advancement never touches the head.
    assert_eq!(after, before);
}

/// Reads the files one checkpoint pins, or the error that says it no longer
/// pins anything.
async fn read_checkpoint_files<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    checkpoint_id: &CheckpointId,
) -> crate::error::Result<Vec<InodeId>> {
    let page = crate::checkpoint::list_checkpoint_files_page(
        store,
        None,
        namespace_id,
        checkpoint_id,
        loonfs_api::PageRequest {
            cursor: None,
            limit: EffectiveLimit::new(NonZeroU32::new(64).expect("nonzero")),
        },
    )
    .await?;
    Ok(page.files.into_iter().map(|file| file.inode_id).collect())
}

#[tokio::test]
async fn release_is_terminal_and_the_next_pin_is_a_different_record() {
    // Nothing turns a released record back into a pin. A caller asking for
    // a pin again — even at the same instant, over the same basis, under the
    // same owner name — gets a brand new record, so the release can never be
    // undone by racing it.
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
        "/docs/file.txt",
        b"body\n",
        &context,
        None,
    )
    .await
    .expect("write");
    let first = create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("create checkpoint");
    assert!(
        !read_checkpoint_files(&store, &namespace_id, &first.checkpoint_id)
            .await
            .expect("the first pin serves")
            .is_empty()
    );

    let release = crate::checkpoint::release_checkpoint(
        &store,
        &namespace_id,
        &first.checkpoint_id,
        &context,
    );
    let recreate = create_checkpoint(&store, &namespace_id, &context);
    let (release, second) = tokio::join!(release, recreate);
    assert_eq!(release.expect("release").checkpoint_id, first.checkpoint_id);
    let second = second.expect("a concurrent create takes a fresh pin");
    assert_ne!(
        second.checkpoint_id, first.checkpoint_id,
        "a new pin never lands on a released record's key"
    );

    let released = load_checkpoint_record(&store, &namespace_id, &first.checkpoint_id)
        .await
        .expect("read checkpoint record")
        .expect("record exists")
        .state;
    assert_eq!(
        released.status,
        loonfs_api::wire::control::CheckpointStatus::Released {
            released_at_ms: context.now_ms
        }
    );
    let error = read_checkpoint_files(&store, &namespace_id, &first.checkpoint_id)
        .await
        .expect_err("a released record serves no read");
    assert_eq!(error.code(), ErrorCode::CheckpointUnavailable);
    assert!(
        !read_checkpoint_files(&store, &namespace_id, &second.checkpoint_id)
            .await
            .expect("the new pin serves")
            .is_empty()
    );

    // Releasing again is the same end state, not a revival and not an error.
    let again = crate::checkpoint::release_checkpoint(
        &store,
        &namespace_id,
        &first.checkpoint_id,
        &context,
    )
    .await
    .expect("repeat release");
    assert_eq!(again.checkpoint_id, first.checkpoint_id);
    assert_eq!(
        load_checkpoint_record(&store, &namespace_id, &first.checkpoint_id)
            .await
            .expect("read checkpoint record")
            .expect("record exists")
            .state
            .status,
        loonfs_api::wire::control::CheckpointStatus::Released {
            released_at_ms: context.now_ms
        }
    );
}

#[tokio::test]
async fn each_create_mints_its_own_record_and_carries_its_own_expiry() {
    // Re-creating a checkpoint is not a renewal of an earlier one. Every
    // call is its own pin with its own id, its own creation instant, and
    // exactly the expiry it asked for; earlier records are left alone.
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let owner = |expires_at_ms| loonfs_api::wire::control::CheckpointOwner::User {
        name: "test-pin".to_owned(),
        expires_at_ms,
    };
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/file.txt",
        b"body\n",
        &context,
        None,
    )
    .await
    .expect("write");

    let first =
        super::create::create_checkpoint(&store, &namespace_id, owner(Some(10_000)), &context)
            .await
            .expect("create checkpoint")
            .checkpoint;
    assert_eq!(first.expires_at_ms, Some(10_000));

    let mut later_context = test_context();
    later_context.now_ms = 2_000;
    let mut minted = BTreeSet::from([first.checkpoint_id.clone()]);
    for expiry in [Some(99_000), Some(5_000), None] {
        let next =
            super::create::create_checkpoint(&store, &namespace_id, owner(expiry), &later_context)
                .await
                .expect("create checkpoint")
                .checkpoint;
        assert!(
            minted.insert(next.checkpoint_id.clone()),
            "each pin gets an id of its own"
        );
        assert_eq!(next.expires_at_ms, expiry);
        let record = load_checkpoint_record(&store, &namespace_id, &next.checkpoint_id)
            .await
            .expect("read checkpoint record")
            .expect("record exists")
            .state;
        assert_eq!(record.owner.expires_at_ms(), expiry);
        assert_eq!(record.created_at_ms, 2_000);
        assert_eq!(
            record.status,
            loonfs_api::wire::control::CheckpointStatus::Active {}
        );
    }

    // The very first record is untouched by any of it: a pin taken without
    // an expiry, or with one, is held until something releases it.
    let original = load_checkpoint_record(&store, &namespace_id, &first.checkpoint_id)
        .await
        .expect("read checkpoint record")
        .expect("record exists")
        .state;
    assert_eq!(original.created_at_ms, 1_000, "creation instant is history");
    assert_eq!(original.owner.expires_at_ms(), Some(10_000));
    assert_eq!(
        original.status,
        loonfs_api::wire::control::CheckpointStatus::Active {}
    );
}

#[tokio::test]
async fn an_expired_but_unreleased_pin_still_enumerates_its_files() {
    // No clock reads on the checkpoint read path. Release is the whole
    // authority: until a pass turns the passed expiry into a release, the
    // record is still a garbage-collection root, so the state behind it is
    // provably still there and serving it is safe.
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
        "/docs/file.txt",
        b"body\n",
        &context,
        None,
    )
    .await
    .expect("write");
    let already_expired = super::create::create_checkpoint(
        &store,
        &namespace_id,
        loonfs_api::wire::control::CheckpointOwner::User {
            name: "test-pin".to_owned(),
            expires_at_ms: Some(context.now_ms),
        },
        &context,
    )
    .await
    .expect("create checkpoint whose expiry has already passed")
    .checkpoint;
    assert_eq!(
        load_checkpoint_record(&store, &namespace_id, &already_expired.checkpoint_id)
            .await
            .expect("read checkpoint record")
            .expect("record exists")
            .state
            .status,
        loonfs_api::wire::control::CheckpointStatus::Active {}
    );
    assert!(
        !read_checkpoint_files(&store, &namespace_id, &already_expired.checkpoint_id)
            .await
            .expect("an expired but unreleased pin still serves")
            .is_empty()
    );

    // The pass that releases it is what ends the reads.
    crate::gc::gc_namespace(
        &store,
        &namespace_id,
        &crate::gc::GcConfig::default(),
        &test_context(),
    )
    .await
    .expect("gc pass");
    let error = read_checkpoint_files(&store, &namespace_id, &already_expired.checkpoint_id)
        .await
        .expect_err("a released pin serves nothing");
    assert_eq!(error.code(), ErrorCode::CheckpointUnavailable);
}

#[tokio::test]
async fn a_pin_without_a_ttl_is_held_until_it_is_released() {
    // No expiry means no clock: the record stays a serving pin however far
    // the wall clock moves, and only an explicit release ends it.
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
        "/docs/file.txt",
        b"body\n",
        &context,
        None,
    )
    .await
    .expect("write");
    let pin = create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("create checkpoint");
    assert_eq!(pin.expires_at_ms, None);

    let mut distant = test_context();
    distant.now_ms = u64::MAX / 2;
    for _ in 0..3 {
        crate::gc::gc_namespace(
            &store,
            &namespace_id,
            &crate::gc::GcConfig::default(),
            &distant,
        )
        .await
        .expect("gc pass");
    }
    let record = load_checkpoint_record(&store, &namespace_id, &pin.checkpoint_id)
        .await
        .expect("read checkpoint record")
        .expect("an unexpiring pin survives every pass")
        .state;
    assert_eq!(
        record.status,
        loonfs_api::wire::control::CheckpointStatus::Active {}
    );
    assert!(
        !read_checkpoint_files(&store, &namespace_id, &pin.checkpoint_id)
            .await
            .expect("the pin still serves")
            .is_empty()
    );

    crate::checkpoint::release_checkpoint(&store, &namespace_id, &pin.checkpoint_id, &distant)
        .await
        .expect("release");
    let error = read_checkpoint_files(&store, &namespace_id, &pin.checkpoint_id)
        .await
        .expect_err("release ends it");
    assert_eq!(error.code(), ErrorCode::CheckpointUnavailable);
}

#[tokio::test]
async fn checkpoint_verification_rejects_a_basis_below_the_floor() {
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
        "/docs/file.txt",
        b"body\n",
        &context,
        None,
    )
    .await
    .expect("write");
    let checkpoint = create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("create checkpoint");
    advance_retention_floor(&store, &namespace_id, &context)
        .await
        .expect("advance floor");

    // The bootstrap basis (seq 0) now sits below the floor (seq 1): a
    // record pinned to it must fail post-write verification.
    let stale = loonfs_api::wire::control::CheckpointRecordState {
        checkpoint_id: checkpoint.checkpoint_id.clone(),
        namespace_id: namespace_id.clone(),
        manifest_no: ManifestNo(0),
        manifest_object_id: manifest_object_id(ManifestNo(0)),
        manifest_head_seq: ChangeSeq(0),
        manifest_payload_checksum: "sha256:stale".to_owned(),
        head_commit_id: CommitId::parse("c_00000000000000000000000000000000").expect("commit id"),
        created_at_ms: context.now_ms,
        owner: loonfs_api::wire::control::CheckpointOwner::User {
            name: "test-pin".to_owned(),
            expires_at_ms: None,
        },
        status: loonfs_api::wire::control::CheckpointStatus::Active {},
    };
    let verified = super::record::verify_checkpoint_basis(&store, &stale)
        .await
        .expect("verification runs");
    assert_eq!(
        verified,
        super::record::CheckpointBasisVerification::Invalid,
        "sub-floor basis must not verify"
    );
}

#[tokio::test]
async fn checkpoint_basis_verification_store_failure_surfaces_and_releases_record() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    let setup_store = LocalFsStore::new(temp_dir.path()).expect("setup store");
    bootstrap_namespace(&setup_store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    let manifest_key = current_manifest_key(&setup_store, &namespace_id).await;
    let manifest_reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let selected_reads = Arc::clone(&manifest_reads);
    let selected_manifest_key = manifest_key.clone();
    let store = FailStore::matching(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        move |operation| {
            operation.key() == selected_manifest_key
                && matches!(
                    operation.kind(),
                    loonfs_test_support::stores::OperationKind::Get { .. }
                )
                && selected_reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 1
        },
        InjectedError::Transport("injected basis verification failure".to_owned()),
    );
    store.fail_next(1);

    let error = create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect_err("basis verification store failure must surface");

    assert_eq!(error.code(), ErrorCode::ServerError);
    match error {
        CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(
            ManifestLoadError::ReadManifest {
                object_key,
                message,
            },
        )) => {
            assert_eq!(object_key, manifest_key);
            assert_eq!(
                message,
                loonfs_objectstore::ObjectStoreErrorClass::Other.public_message()
            );
        }
        other => panic!("expected a classified manifest store error, got {other:?}"),
    }
    assert_eq!(store.attempts(), 1, "only the verification read fails");
    assert_eq!(
        manifest_reads.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "the injected failure follows the projection's manifest read"
    );

    let record_keys = store
        .inner()
        .list_prefix(&checkpoint_prefix(&namespace_id))
        .await
        .expect("list checkpoint records");
    assert_eq!(record_keys.len(), 1, "the failed create wrote one record");
    let checkpoint_id = record_keys[0]
        .rsplit('/')
        .next()
        .and_then(|name| name.strip_suffix(".json"))
        .and_then(|id| CheckpointId::parse(id).ok())
        .expect("checkpoint record key");
    let record = load_checkpoint_record(store.inner(), &namespace_id, &checkpoint_id)
        .await
        .expect("read checkpoint record")
        .expect("record remains for cleanup inspection")
        .state;
    assert_eq!(
        record.status,
        loonfs_api::wire::control::CheckpointStatus::Released {
            released_at_ms: context.now_ms
        }
    );
}

#[tokio::test]
async fn checkpoint_checksum_disagreement_is_terminal_corruption_and_releases_record() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    let setup_store = LocalFsStore::new(temp_dir.path()).expect("setup store");
    bootstrap_namespace(&setup_store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");

    let manifest_key = current_manifest_key(&setup_store, &namespace_id).await;
    let manifest_bytes = setup_store
        .get(&manifest_key, None)
        .await
        .expect("read manifest")
        .expect("current manifest exists");
    let manifest = decode_namespace_manifest_json(&manifest_bytes).expect("decode manifest");
    let record_checksum = manifest.payload_checksum.clone();
    let mut mismatched_payload = manifest.payload;
    mismatched_payload.next_inode_id = InodeId(mismatched_payload.next_inode_id.0 + 1);
    let mismatched_manifest = NamespaceManifestEnvelope::from_payload(mismatched_payload)
        .expect("build mismatched manifest");
    let manifest_checksum = mismatched_manifest.payload_checksum.clone();
    assert_ne!(record_checksum, manifest_checksum);
    let mismatched_bytes = Bytes::from(
        encode_namespace_manifest_json(&mismatched_manifest).expect("encode mismatched manifest"),
    );
    let store = ManifestChecksumMismatchOnceStore::new(
        setup_store,
        &namespace_id,
        manifest_key,
        mismatched_bytes,
    );

    let error = create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect_err("a durable checksum contradiction must fail checkpoint creation");

    assert_eq!(error.code(), ErrorCode::NamespaceCorrupt);
    let CoreError::NamespaceCorrupt(message) = error else {
        panic!("expected namespace corruption, got {error:?}");
    };
    assert!(message.contains(&record_checksum));
    assert!(message.contains(&manifest_checksum));
    assert_eq!(
        store.checkpoint_writes(),
        1,
        "the checksum contradiction must not be retried under a new checkpoint id"
    );

    let record_keys = store
        .inner()
        .list_prefix(&checkpoint_prefix(&namespace_id))
        .await
        .expect("list checkpoint records");
    assert_eq!(record_keys.len(), 1, "the failed create wrote one record");
    let checkpoint_id = record_keys[0]
        .rsplit('/')
        .next()
        .and_then(|name| name.strip_suffix(".json"))
        .and_then(|id| CheckpointId::parse(id).ok())
        .expect("checkpoint record key");
    let record = load_checkpoint_record(store.inner(), &namespace_id, &checkpoint_id)
        .await
        .expect("read checkpoint record")
        .expect("record remains for cleanup inspection")
        .state;
    assert_eq!(
        record.status,
        loonfs_api::wire::control::CheckpointStatus::Released {
            released_at_ms: context.now_ms
        }
    );
}

async fn wal_segment_count(store: &LocalFsStore, namespace_id: &NamespaceId) -> usize {
    store
        .list_prefix(&wal_segment_prefix(namespace_id))
        .await
        .expect("list wal segments")
        .len()
}

#[tokio::test]
async fn publish_backpressure_rejects_at_the_longest_tail_the_head_describes() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    let boundary =
        usize::try_from(crate::commit_engine::WalTailPolicy::DEFAULT.reject_writes_at_segments)
            .expect("the write-stop bound fits a segment count");

    // Stop one short, so the next publish is the one that reaches the bound.
    for round in 0..boundary - 1 {
        write_file_bytes(
            &store,
            &namespace_id,
            &format!("/docs/file-{round}.txt"),
            b"body\n",
            &context,
            None,
        )
        .await
        .expect("write within backpressure window");
    }
    assert_eq!(wal_segment_count(&store, &namespace_id).await, boundary - 1);

    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/at-the-boundary.txt",
        b"body\n",
        &context,
        None,
    )
    .await
    .expect("the publish that lands exactly at the bound is still admitted");
    assert_eq!(wal_segment_count(&store, &namespace_id).await, boundary);

    let error = write_file_bytes(
        &store,
        &namespace_id,
        "/docs/one-too-many.txt",
        b"body\n",
        &context,
        None,
    )
    .await
    .expect_err("a publish against a tail at the bound must be rejected");
    assert_eq!(error.code(), ErrorCode::MaintenanceRequired);
    assert_eq!(
        wal_segment_count(&store, &namespace_id).await,
        boundary,
        "the rejection lands before the segment PUT, so nothing new is written"
    );

    // The whole legal tail is named by the tip plus its predecessor hints,
    // so no reader has to discover a key by walking an unhinted link.
    let head = load_head_object(&store, &namespace_id)
        .await
        .expect("read head")
        .state;
    assert_eq!(head.recent_segments.len(), boundary - 1);
    let described_tail = head
        .visible_wal_tip
        .iter()
        .chain(head.recent_segments.iter())
        .collect::<Vec<_>>();
    assert_eq!(described_tail.len(), boundary);
    for pair in described_tail.windows(2) {
        let [newer, older] = pair else {
            panic!("windows(2) yields pairs")
        };
        assert_eq!(
            older.end_seq.0 + 1,
            newer.start_seq.0,
            "hints must be newest-first and gap-free"
        );
    }

    // Reads never gate: the change feed still serves the whole tail.
    let changes = list_changes_after(
        &store,
        &namespace_id,
        ChangeSeq(0),
        EffectiveLimit::new(NonZeroU32::new(200).expect("nonzero")),
    )
    .await
    .expect("list changes");
    assert_eq!(
        changes.through_seq,
        ChangeSeq(u64::try_from(boundary).expect("one commit per segment"))
    );
}

#[tokio::test]
async fn checkpoints_append_past_the_threshold_and_reorganization_drains() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    let policy = MetadataLsmPolicy {
        max_l0_runs: NonZeroUsize::new(2).expect("test run limit should be nonzero"),
        ..MetadataLsmPolicy::default()
    };
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");

    for index in 1..=4 {
        write_file_and_checkpoint(&store, &namespace_id, &context, index).await;
    }

    // Checkpoints never compact: well past the policy threshold every
    // delta run is still chained and the base is still the seed's.
    let appended = load_manifest_materialization_for_inspection(
        &store,
        &namespace_id,
        current_manifest_no(&store, &namespace_id).await,
    )
    .await
    .expect("load appended manifest");
    assert_eq!(l0_runs(&appended.manifest).len(), 4);
    assert_eq!(appended.manifest.payload.base_seq, ChangeSeq(0));

    // Reorganization folds one family group per unit, each publishing its
    // own manifest — the manifest chain is the progress record.
    let mut units = 0usize;
    let mut last_manifest_no = None;
    loop {
        let report = super::reorganize_metadata_step(
            &store,
            &namespace_id,
            &context,
            policy,
            MetadataCompactionView::default(),
        )
        .await
        .expect("reorganization step");
        match report.outcome {
            super::MetadataReorganizeOutcome::UnitPublished { manifest_no, .. } => {
                units += 1;
                if let Some(previous) = last_manifest_no {
                    assert!(manifest_no > previous, "units must advance the manifest");
                }
                last_manifest_no = Some(manifest_no);
            }
            super::MetadataReorganizeOutcome::Superseded => {
                panic!("no concurrent publisher exists in this test")
            }
            super::MetadataReorganizeOutcome::NotNeeded { .. } => break,
            super::MetadataReorganizeOutcome::CompactionPlanned { .. } => {
                panic!("test reorganization budget should admit a progress-making subset")
            }
        }
    }
    assert!(units >= 2, "several family groups should fold, got {units}");

    let drained = load_manifest_materialization_for_inspection(
        &store,
        &namespace_id,
        current_manifest_no(&store, &namespace_id).await,
    )
    .await
    .expect("load drained manifest");
    let materialization_after = load_current_projection(&store, &namespace_id)
        .await
        .expect("materialization");
    assert!(l0_runs(&drained.manifest).is_empty());
    assert_eq!(drained.manifest.payload.base_seq, ChangeSeq(4));
    assert!(metadata_states_equivalent(
        &materialization_after.metadata_state,
        &drained.metadata_state
    ));
}

#[tokio::test]
async fn reorganization_step_honors_run_row_and_decoded_byte_budgets() {
    let temp_dir = tempdir().expect("tempdir");
    let store =
        CountingStore::metadata_segments(LocalFsStore::new(temp_dir.path()).expect("store"));
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    for index in 1..=5 {
        write_file_bytes(
            &store,
            &namespace_id,
            &format!("/docs/file-{index}.txt"),
            b"body\n",
            &context,
            None,
        )
        .await
        .expect("write file");
        create_checkpoint(&store, &namespace_id, &context)
            .await
            .expect("create checkpoint");
    }

    let root_before = current_manifest_no(&store, &namespace_id).await;
    let tiny_byte_policy = MetadataLsmPolicy {
        max_l0_runs: NonZeroUsize::MIN,
        max_input_runs_per_step: NonZeroUsize::new(2).expect("test run budget should be nonzero"),
        max_decoded_input_bytes_per_step: NonZeroUsize::MIN,
        ..MetadataLsmPolicy::default()
    };
    store.reset();
    let blocked = super::reorganize_metadata_step(
        &store,
        &namespace_id,
        &context,
        tiny_byte_policy,
        MetadataCompactionView::default(),
    )
    .await
    .expect("budgeted step");
    let super::MetadataReorganizeOutcome::CompactionPlanned { group, .. } = blocked.outcome else {
        panic!("one byte must not admit a metadata run");
    };
    assert_eq!(
        current_manifest_no(&store, &namespace_id).await,
        root_before,
        "a step that plans a compaction must not publish"
    );
    assert_eq!(store.count(OperationClass::Put), 0);
    assert!(
        store.count(OperationClass::Read) <= group.families().len(),
        "byte preflight should read only the oldest candidate's index sections"
    );

    let policy = MetadataLsmPolicy {
        max_l0_runs: NonZeroUsize::MIN,
        max_input_runs_per_step: NonZeroUsize::new(2).expect("test run budget should be nonzero"),
        max_decoded_input_rows_per_step: NonZeroUsize::new(6)
            .expect("test row budget should be nonzero"),
        ..MetadataLsmPolicy::default()
    };
    store.reset();
    let published = super::reorganize_metadata_step(
        &store,
        &namespace_id,
        &context,
        policy,
        MetadataCompactionView::default(),
    )
    .await
    .expect("bounded step");
    let super::MetadataReorganizeOutcome::UnitPublished {
        group,
        input_runs,
        decoded_input_rows,
        decoded_input_bytes,
        ..
    } = published.outcome
    else {
        panic!("two oldest runs should fit the test budgets");
    };
    assert_eq!(input_runs, 2);
    assert!(input_runs <= policy.max_input_runs_per_step.get());
    assert!(decoded_input_rows <= policy.max_decoded_input_rows_per_step.get() as u64);
    assert!(decoded_input_bytes <= policy.max_decoded_input_bytes_per_step.get() as u64);
    assert!(
        store.count(OperationClass::Read) <= input_runs * group.families().len() * 2,
        "the step should read index and data sections only for selected-run family segments"
    );
}

#[tokio::test]
async fn bounded_reorganization_converges_to_unbounded_shape_and_preserves_intermediate_reads() {
    let bounded_dir = tempdir().expect("bounded tempdir");
    let unbounded_dir = tempdir().expect("unbounded tempdir");
    let bounded_store = LocalFsStore::new(bounded_dir.path()).expect("bounded store");
    let unbounded_store = LocalFsStore::new(unbounded_dir.path()).expect("unbounded store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    for store in [&bounded_store, &unbounded_store] {
        bootstrap_namespace(store, &namespace_id, &context, false)
            .await
            .expect("bootstrap");
        for index in 1..=6 {
            let commit_id =
                CommitId::parse(format!("bounded-convergence-{index}")).expect("commit id");
            write_file_bytes(
                store,
                &namespace_id,
                &format!("/docs/file-{index}.txt"),
                format!("file {index}\n").as_bytes(),
                &context,
                Some(&commit_id),
            )
            .await
            .expect("write file");
            create_checkpoint(store, &namespace_id, &context)
                .await
                .expect("create checkpoint");
        }
    }

    let visible_before = load_current_projection(&bounded_store, &namespace_id)
        .await
        .expect("visible state before bounded reorganization");
    let unlimited = NonZeroUsize::new(usize::MAX).expect("usize max should be nonzero");
    let bounded_policy = MetadataLsmPolicy {
        max_l0_runs: NonZeroUsize::new(4).expect("test trigger should be nonzero"),
        max_input_runs_per_step: NonZeroUsize::new(2).expect("test run budget should be nonzero"),
        max_decoded_input_rows_per_step: unlimited,
        max_decoded_input_bytes_per_step: unlimited,
        ..MetadataLsmPolicy::default()
    };
    let first = super::reorganize_metadata_step(
        &bounded_store,
        &namespace_id,
        &context,
        bounded_policy,
        MetadataCompactionView::default(),
    )
    .await
    .expect("first bounded step");
    assert!(matches!(
        first.outcome,
        super::MetadataReorganizeOutcome::UnitPublished { input_runs: 2, .. }
    ));
    let visible_between = load_current_projection(&bounded_store, &namespace_id)
        .await
        .expect("visible state between bounded steps");
    assert!(metadata_states_equivalent(
        &visible_before.metadata_state,
        &visible_between.metadata_state
    ));

    let (bounded_manifest_no, bounded_steps_after_first) =
        drain_reorganization_with_count(&bounded_store, &namespace_id, &context, bounded_policy)
            .await;
    let unbounded_policy = MetadataLsmPolicy {
        max_l0_runs: NonZeroUsize::new(4).expect("test trigger should be nonzero"),
        max_input_runs_per_step: unlimited,
        max_decoded_input_rows_per_step: unlimited,
        max_decoded_input_bytes_per_step: unlimited,
        ..MetadataLsmPolicy::default()
    };
    let (unbounded_manifest_no, unbounded_steps) = drain_reorganization_with_count(
        &unbounded_store,
        &namespace_id,
        &context,
        unbounded_policy,
    )
    .await;
    assert!(bounded_steps_after_first + 1 > unbounded_steps);

    let bounded = load_manifest_materialization_for_inspection(
        &bounded_store,
        &namespace_id,
        bounded_manifest_no,
    )
    .await
    .expect("load bounded result");
    let unbounded = load_manifest_materialization_for_inspection(
        &unbounded_store,
        &namespace_id,
        unbounded_manifest_no,
    )
    .await
    .expect("load unbounded result");
    assert!(metadata_states_equivalent_ignoring_content_identity(
        &bounded.metadata_state,
        &unbounded.metadata_state
    ));
    assert_eq!(
        manifest_run_shape(&bounded.manifest),
        manifest_run_shape(&unbounded.manifest)
    );
    assert!(l0_runs(&bounded.manifest).is_empty());

    let later_commit = CommitId::parse("bounded-convergence-later").expect("commit id");
    write_file_bytes(
        &bounded_store,
        &namespace_id,
        "/docs/later.txt",
        b"later\n",
        &context,
        Some(&later_commit),
    )
    .await
    .expect("write later file");
    create_checkpoint(&bounded_store, &namespace_id, &context)
        .await
        .expect("checkpoint later file");
    let below_trigger = super::reorganize_metadata_step(
        &bounded_store,
        &namespace_id,
        &context,
        bounded_policy,
        MetadataCompactionView::default(),
    )
    .await
    .expect("below-trigger step");
    assert!(matches!(
        below_trigger.outcome,
        super::MetadataReorganizeOutcome::NotNeeded { l0_runs: 1 }
    ));
}

#[tokio::test]
async fn whole_run_compaction_rewrites_base_segments() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    let policy = MetadataLsmPolicy {
        max_l0_runs: NonZeroUsize::MIN,
        max_rows_per_segment: NonZeroUsize::new(2)
            .expect("test segment row budget should be nonzero"),
        ..MetadataLsmPolicy::default()
    };
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    write_file_bytes(
        &store,
        &namespace_id,
        "/hot/original.txt",
        b"hot\n",
        &context,
        None,
    )
    .await
    .expect("write hot");
    write_file_bytes(
        &store,
        &namespace_id,
        "/cold/original.txt",
        b"cold\n",
        &context,
        None,
    )
    .await
    .expect("write cold");

    let first_manifest_no =
        checkpoint_then_reorganize(&store, &namespace_id, &context, policy).await;
    let first_materialized =
        load_manifest_materialization_for_inspection(&store, &namespace_id, first_manifest_no)
            .await
            .expect("load first manifest");
    let first_run_keys = run_segment_object_keys(&first_materialized.manifest);

    write_file_bytes(
        &store,
        &namespace_id,
        "/hot/after-l0.txt",
        b"hot 2\n",
        &context,
        None,
    )
    .await
    .expect("write hot l0");
    checkpoint_then_reorganize(&store, &namespace_id, &context, policy).await;
    write_file_bytes(
        &store,
        &namespace_id,
        "/hot/after-compact.txt",
        b"hot 3\n",
        &context,
        None,
    )
    .await
    .expect("write hot compact");
    let compacted = create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("compacted checkpoint");
    let compacted_manifest_no = drain_reorganization(&store, &namespace_id, &context, policy).await;
    let compacted_materialized =
        load_manifest_materialization_for_inspection(&store, &namespace_id, compacted_manifest_no)
            .await
            .expect("load compacted manifest");
    let materialization_after = load_current_projection(&store, &namespace_id)
        .await
        .expect("materialization");
    let compacted_run_keys = run_segment_object_keys(&compacted_materialized.manifest);
    let compacted_run_prefix = format!(
        "namespaces/{}/metadata/segments/seg_",
        namespace_id.as_str()
    );

    assert_eq!(
        compacted_materialized.manifest.payload.base_seq,
        compacted.checkpoint_seq
    );
    assert!(l0_runs(&compacted_materialized.manifest).is_empty());
    assert_eq!(
        runs_from_segments(&compacted_materialized.manifest.payload).len(),
        1
    );
    assert!(!compacted_run_keys.is_empty());
    assert!(compacted_run_keys
        .iter()
        .all(|key| key.starts_with(&compacted_run_prefix)));
    assert!(compacted_run_keys
        .iter()
        .all(|key| !first_run_keys.contains(key)));
    assert_manifest_rows_have_unique_keys(&compacted_materialized.metadata_state);
    assert!(metadata_states_equivalent(
        &materialization_after.metadata_state,
        &compacted_materialized.metadata_state
    ));
}

#[tokio::test]
async fn whole_run_compaction_resegments_row_key_range_families_with_l0_runs() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    let policy = MetadataLsmPolicy {
        max_l0_runs: NonZeroUsize::MIN,
        max_rows_per_segment: NonZeroUsize::new(2)
            .expect("test segment row budget should be nonzero"),
        ..MetadataLsmPolicy::default()
    };
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    for index in 0..6 {
        let path = format!("/docs/file-{index}.txt");
        write_file_bytes(&store, &namespace_id, &path, b"initial\n", &context, None)
            .await
            .expect("write initial file");
    }

    let first_manifest_no =
        checkpoint_then_reorganize(&store, &namespace_id, &context, policy).await;
    let first_materialized =
        load_manifest_materialization_for_inspection(&store, &namespace_id, first_manifest_no)
            .await
            .expect("load first manifest");
    let revision_keys_before = base_segment_object_keys_for_family(
        &first_materialized.manifest,
        ApiMetadataRowFamily::Revisions,
    );
    assert!(revision_keys_before.len() >= 3);

    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/file-0.txt",
        b"l0\n",
        &context,
        None,
    )
    .await
    .expect("write l0 revision");
    checkpoint_then_reorganize(&store, &namespace_id, &context, policy).await;
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/file-0.txt",
        b"compact\n",
        &context,
        None,
    )
    .await
    .expect("write compaction revision");
    let _compacted = create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("compacted checkpoint");
    let compacted_manifest_no = drain_reorganization(&store, &namespace_id, &context, policy).await;
    let compacted_materialized =
        load_manifest_materialization_for_inspection(&store, &namespace_id, compacted_manifest_no)
            .await
            .expect("load compacted manifest");
    let revision_keys_after = base_segment_object_keys_for_family(
        &compacted_materialized.manifest,
        ApiMetadataRowFamily::Revisions,
    );
    let materialization_after = load_current_projection(&store, &namespace_id)
        .await
        .expect("materialization");

    assert!(l0_runs(&compacted_materialized.manifest).is_empty());
    // Groups untouched since the first fold keep their older base run, so
    // several base runs may coexist; what matters is that no delta remains.
    assert!(runs_from_segments(&compacted_materialized.manifest.payload)
        .iter()
        .all(|run| run.level == CHECKPOINT_BASE_RUN_LEVEL));
    assert!(revision_keys_after
        .iter()
        .all(|key| !revision_keys_before.contains(key)));
    assert!(metadata_states_equivalent(
        &materialization_after.metadata_state,
        &compacted_materialized.metadata_state
    ));
    assert_manifest_rows_have_unique_keys(&compacted_materialized.metadata_state);
}

#[tokio::test]
async fn reorganization_resumes_from_the_manifest_after_interruption() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    let policy = MetadataLsmPolicy {
        max_l0_runs: NonZeroUsize::MIN,
        ..MetadataLsmPolicy::default()
    };
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    for index in 1..=3 {
        write_file_and_checkpoint(&store, &namespace_id, &context, index).await;
    }

    // One unit runs, then the process "crashes": nothing is carried over
    // but the published manifest.
    let first_report = super::reorganize_metadata_step(
        &store,
        &namespace_id,
        &context,
        policy,
        MetadataCompactionView::default(),
    )
    .await
    .expect("first unit");
    let super::MetadataReorganizeOutcome::UnitPublished {
        group: first_group, ..
    } = first_report.outcome
    else {
        panic!("expected a published unit, got {:?}", first_report.outcome);
    };

    // A checkpoint lands in between, adding a fresh delta run.
    write_file_and_checkpoint(&store, &namespace_id, &context, 9).await;

    // A fresh sequence of steps reads the live manifest and finishes the
    // job, including the delta the interleaved checkpoint added.
    let mut folded_groups = vec![first_group];
    loop {
        let report = super::reorganize_metadata_step(
            &store,
            &namespace_id,
            &context,
            policy,
            MetadataCompactionView::default(),
        )
        .await
        .expect("resumed unit");
        match report.outcome {
            super::MetadataReorganizeOutcome::UnitPublished { group, .. } => {
                folded_groups.push(group);
            }
            super::MetadataReorganizeOutcome::Superseded => {
                panic!("no concurrent publisher exists in this test")
            }
            super::MetadataReorganizeOutcome::NotNeeded { .. } => break,
            super::MetadataReorganizeOutcome::CompactionPlanned { .. } => {
                panic!("test reorganization budget should admit a progress-making subset")
            }
        }
    }
    assert!(folded_groups.len() >= 2);

    let drained = load_manifest_materialization_for_inspection(
        &store,
        &namespace_id,
        current_manifest_no(&store, &namespace_id).await,
    )
    .await
    .expect("load drained manifest");
    let materialization_after = load_current_projection(&store, &namespace_id)
        .await
        .expect("materialization");
    assert!(l0_runs(&drained.manifest).is_empty());
    assert!(metadata_states_equivalent(
        &materialization_after.metadata_state,
        &drained.metadata_state
    ));
}

#[tokio::test]
async fn checkpoints_chain_l0_runs_past_the_default_cap() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    for index in 1..=10 {
        write_file_and_checkpoint(&store, &namespace_id, &context, index).await;
    }

    // The default cap is a reorganization trigger, never a checkpoint
    // behavior: publication keeps appending regardless.
    let appended = load_manifest_materialization_for_inspection(
        &store,
        &namespace_id,
        current_manifest_no(&store, &namespace_id).await,
    )
    .await
    .expect("load appended manifest");
    assert!(l0_runs(&appended.manifest).len() > DEFAULT_MAX_CHECKPOINT_L0_RUNS);
    assert_eq!(appended.manifest.payload.base_seq, ChangeSeq(0));
}

/// A created namespace writes no floor object at all, and its absence
/// reads as "retain from the namespace's birth sequence", which for a
/// created namespace is genesis.
#[tokio::test]
async fn a_missing_floor_reads_as_retain_everything() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    assert!(
        store
            .head(&loonfs_objectstore::keys::wal_floor(&namespace_id))
            .await
            .expect("probe floor")
            .is_none(),
        "creation writes no floor object"
    );

    let head = crate::namespace::control::load_head_object(&store, &namespace_id)
        .await
        .expect("head")
        .state;
    let floor = crate::namespace::basis::resolve_retention_floor_seq(&store, &head)
        .await
        .expect("missing floor defaults");
    assert_eq!(floor, ChangeSeq(0));
}

/// An over-budget WAL flush aborts before the root compare-and-swap:
/// the root keeps its previous basis, the orphan outputs are harmless GC
/// candidates, and an in-budget retry succeeds.
#[tokio::test]
async fn over_budget_wal_flush_aborts_without_publishing() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = MutationContext {
        writer_id: "budget-test".to_owned(),
        now_ms: 1_000,
    };
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/budget.txt",
        b"body",
        DestinationBehavior::NoReplace,
        &context,
        None,
    )
    .await
    .expect("seed file");
    let root_before = load_metadata_root_object(&store, &namespace_id)
        .await
        .expect("read root")
        .state;

    // Every reading advances 20 minutes against the 15-minute budget: the
    // pre-CAS check observes the publication as over budget.
    let overrun = SteppingTimer::new(20 * 60 * 1000);
    let error = super::flush::flush_wal_with_timer(&store, &namespace_id, &context, &overrun)
        .await
        .expect_err("over-budget publication must abort");
    assert!(
        matches!(error, CoreError::MetadataPublicationBudgetExceeded { .. }),
        "expected budget error, got {error:?}"
    );

    let root_after = load_metadata_root_object(&store, &namespace_id)
        .await
        .expect("read root")
        .state;
    assert_eq!(
        root_after, root_before,
        "an aborted publication must not move the root"
    );

    // The in-budget retry publishes normally over fresh outputs.
    let advanced = super::flush::flush_wal(&store, &namespace_id, &context)
        .await
        .expect("in-budget retry succeeds");
    assert_eq!(advanced.outcome, loonfs_api::FlushWalOutcome::Published);
    assert!(advanced.manifest_no > root_before.manifest_no);
}

/// An over-budget reorganization unit aborts the same way: no root motion,
/// orphan segments left for garbage collection.
#[tokio::test]
async fn over_budget_reorganization_aborts_without_publishing() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = MutationContext {
        writer_id: "budget-test".to_owned(),
        now_ms: 1_000,
    };
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    put_file_bytes(
        &store,
        &namespace_id,
        "/docs/fold.txt",
        b"body",
        DestinationBehavior::NoReplace,
        &context,
        None,
    )
    .await
    .expect("seed file");
    super::flush::flush_wal(&store, &namespace_id, &context)
        .await
        .expect("publish an L0 run to fold");
    let root_before = load_metadata_root_object(&store, &namespace_id)
        .await
        .expect("read root")
        .state;

    let fold_everything = MetadataLsmPolicy {
        max_l0_runs: NonZeroUsize::MIN,
        ..Default::default()
    };
    let overrun = SteppingTimer::new(20 * 60 * 1000);
    let error = super::reorganize::reorganize_metadata_step_with_timer(
        &store,
        &namespace_id,
        &context,
        fold_everything,
        MetadataCompactionView::default(),
        &overrun,
    )
    .await
    .expect_err("over-budget reorganization must abort");
    assert!(
        matches!(error, CoreError::MetadataPublicationBudgetExceeded { .. }),
        "expected budget error, got {error:?}"
    );

    let root_after = load_metadata_root_object(&store, &namespace_id)
        .await
        .expect("read root")
        .state;
    assert_eq!(root_after, root_before);

    let report = super::reorganize::reorganize_metadata_step(
        &store,
        &namespace_id,
        &context,
        fold_everything,
        MetadataCompactionView::default(),
    )
    .await
    .expect("in-budget retry folds the unit");
    assert!(matches!(
        report.outcome,
        super::MetadataReorganizeOutcome::UnitPublished { .. }
    ));
}

/// Runs the selector exactly as a step runs it — same family group, same
/// policy — and returns the group beside what the selector chose.
async fn select_reorganization_window<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    policy: MetadataLsmPolicy,
) -> (
    MetadataFamilyGroup,
    super::reorganize::ReorganizationSelection,
) {
    let manifest_object_id = current_manifest_object_id(store, namespace_id).await;
    let segments = load_verified_manifest_segments(store, namespace_id, &manifest_object_id)
        .await
        .expect("load manifest segments");
    let group = super::reorganize::select_family_group(&segments.manifest().payload, None)
        .expect("a family group with L0 rows to fold");
    let frozen_floor_seq = read_floor_seq(store, namespace_id).await;
    let selection = super::reorganize::select_reorganization_input(
        &segments,
        group,
        policy,
        frozen_floor_seq,
        &super::reorganize::FrozenBasePolicy::default(),
    )
    .await
    .expect("select a reorganization input");
    (group, selection)
}

/// The bounded merge a selection chose, or `None` when it chose a streaming
/// compaction instead.
fn bounded_merge(
    selection: super::reorganize::ReorganizationSelection,
) -> Option<super::reorganize::ReorganizationInput> {
    match selection.plan {
        Some(super::reorganize::ReorganizationPlan::BoundedMerge(input)) => Some(input),
        _ => None,
    }
}

/// Rows one run holds in one family group: the quantity the per-step row
/// budget is measured against.
fn group_run_rows(run: &MetadataRunManifest, group: &[ApiMetadataRowFamily]) -> u64 {
    run.segments
        .iter()
        .filter(|family_segments| group.contains(&family_segments.family))
        .flat_map(|family_segments| &family_segments.segments)
        .map(|descriptor| descriptor.row_count)
        .sum()
}

fn group_segment_object_keys(
    run: &MetadataRunManifest,
    group: &[ApiMetadataRowFamily],
) -> Vec<String> {
    run.segments
        .iter()
        .filter(|family_segments| group.contains(&family_segments.family))
        .flat_map(|family_segments| &family_segments.segments)
        .map(|descriptor| descriptor.object_key.clone())
        .collect()
}

/// A per-step row budget one row short of what the group's base run needs,
/// with the trigger forced so a single delta run still folds.
fn policy_that_cannot_fold_the_base(base_rows: u64) -> MetadataLsmPolicy {
    let row_budget = usize::try_from(base_rows).expect("test row counts are small") - 1;
    MetadataLsmPolicy {
        max_l0_runs: NonZeroUsize::MIN,
        max_decoded_input_rows_per_step: NonZeroUsize::new(row_budget)
            .expect("test row budget should be nonzero"),
        ..MetadataLsmPolicy::default()
    }
}

/// A group whose base run alone busts the per-step row budget used to park
/// immediately: selection broke on the first candidate, chose nothing, and
/// reported the group blocked on every step after that while delta runs
/// piled up behind it with nothing saying why.
///
/// The step skips the base run it cannot read whole, merges the delta runs
/// above it into one bigger delta run, and says out loud that the base has
/// outgrown the budget. The merged output is a delta run at its newest
/// input's identity, so the base stays exactly one run: minting a second
/// base run here is what hid the over-budget bottom from the report.
///
/// Once the delta runs are down to one the group has nothing left to merge,
/// and the step hands the whole group to a streaming compaction. That job
/// rebuilds the group under no budget, so the base is folded again, its
/// segments are replaced, and the group ends in one run. There is no state
/// where the group is stuck.
#[tokio::test]
async fn a_base_run_over_the_step_budget_merges_the_delta_runs_then_compacts_the_group() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");

    // A compacted base first, then delta runs stacked above it.
    for index in 1..=4 {
        write_file_and_checkpoint(&store, &namespace_id, &context, index).await;
    }
    drain_reorganization(
        &store,
        &namespace_id,
        &context,
        MetadataLsmPolicy::default(),
    )
    .await;
    for index in 5..=8 {
        write_file_and_checkpoint(&store, &namespace_id, &context, index).await;
    }

    let before = load_manifest_materialization_for_inspection(
        &store,
        &namespace_id,
        current_manifest_no(&store, &namespace_id).await,
    )
    .await
    .expect("load the parked manifest");
    let (group, _) =
        select_reorganization_window(&store, &namespace_id, MetadataLsmPolicy::default()).await;
    let base = base_run(&before.manifest);
    let base_rows = group_run_rows(&base, group.families());
    let base_segments = group_segment_object_keys(&base, group.families());
    assert!(base_rows > 1, "the base run must hold rows in this group");
    let policy = policy_that_cannot_fold_the_base(base_rows);

    let (_, selection) = select_reorganization_window(&store, &namespace_id, policy).await;
    let bottom = selection
        .group_bottom_over_budget
        .expect("a base run over the budget must raise the alarm");
    let input = bounded_merge(selection)
        .expect("a group must keep finding work above a base run it cannot fold");
    assert!(
        matches!(
            input.placement,
            super::reorganize::MergePlacement::Delta { .. }
        ),
        "a window that starts above the base run must write a delta run, got {:?}",
        input.placement
    );
    assert!(
        input
            .runs
            .iter()
            .all(|run| run.level == CHECKPOINT_L0_RUN_LEVEL),
        "skipping the base leaves a delta-only merge, got {:?}",
        input
            .runs
            .iter()
            .map(|run| (run.run_seq, run.level))
            .collect::<Vec<_>>()
    );
    assert!(
        input.runs.len() > 1,
        "a merge must consume more than one run to reduce the count"
    );
    assert_eq!(bottom.run_seq, base.run_seq);
    assert_eq!(bottom.level, CHECKPOINT_BASE_RUN_LEVEL);
    assert_eq!(bottom.rows, base_rows);

    // Repeated steps keep merging what fits. While they do, the base run
    // stays exactly where it is, and stays one run.
    let mut published = 0usize;
    let mut compactions = 0usize;
    let mut group_delta_run_counts =
        vec![group_delta_runs(&before.manifest, group.families()).len()];
    let mut delta_runs_when_the_compaction_was_planned = None;
    for _ in 0..64 {
        let report = super::reorganize_metadata_step(
            &store,
            &namespace_id,
            &context,
            policy,
            MetadataCompactionView::default(),
        )
        .await
        .expect("reorganization step");
        let current = load_manifest_materialization_for_inspection(
            &store,
            &namespace_id,
            current_manifest_no(&store, &namespace_id).await,
        )
        .await
        .expect("load the folded manifest");
        assert_eq!(
            group_base_runs(&current.manifest, group.families()).len(),
            1,
            "a merge above the base must not mint a second base run"
        );
        group_delta_run_counts.push(group_delta_runs(&current.manifest, group.families()).len());
        match &report.outcome {
            super::MetadataReorganizeOutcome::UnitPublished { .. } => published += 1,
            super::MetadataReorganizeOutcome::Superseded => {
                panic!("no concurrent publisher exists in this test")
            }
            // A group whose delta runs are down to one has nothing left to
            // merge, and merging one run into itself is not progress. Only a
            // rebuild of the whole group moves it on, and that is the job the
            // step plans. This budget starves more than one group, so the
            // others are run too and the loop ends with nothing left to fold.
            super::MetadataReorganizeOutcome::CompactionPlanned {
                group: planned_group,
                spec,
            } => {
                if *planned_group == group {
                    let referenced = run_segment_object_keys(&current.manifest);
                    assert!(
                        base_segments
                            .iter()
                            .all(|segment_key| referenced.contains(segment_key)),
                        "the base must still be untouched when the job is planned"
                    );
                    delta_runs_when_the_compaction_was_planned =
                        Some(group_delta_runs(&current.manifest, group.families()).len());
                    compactions += 1;
                }
                publish_planned_compaction(&store, &namespace_id, &context, policy, spec).await;
            }
            super::MetadataReorganizeOutcome::NotNeeded { .. } => break,
        }
    }
    assert!(published > 0, "at least one unit must publish");
    assert_eq!(
        compactions, 1,
        "the group must hand itself to one streaming compaction, ran {group_delta_run_counts:?}"
    );
    assert_eq!(
        delta_runs_when_the_compaction_was_planned,
        Some(1),
        "the delta runs must merge down to one before the job takes the group, ran \
         {group_delta_run_counts:?}"
    );

    let after = load_manifest_materialization_for_inspection(
        &store,
        &namespace_id,
        current_manifest_no(&store, &namespace_id).await,
    )
    .await
    .expect("load the drained manifest");
    // The job rebuilt the group, so it is one base run and the frozen base's
    // segments are gone. That is the whole point: they could not be rewritten
    // by any step, and a run nothing rewrites is a run nothing reclaims.
    assert_eq!(
        group_runs(&after.manifest, group.families()).len(),
        1,
        "the group must end in one run, ran {group_delta_run_counts:?}"
    );
    assert_eq!(group_base_runs(&after.manifest, group.families()).len(), 1);
    let remaining = run_segment_object_keys(&after.manifest);
    for segment_key in &base_segments {
        assert!(
            !remaining.contains(segment_key),
            "the frozen base's segments must be replaced by the rebuilt run"
        );
    }
    let projection = load_current_projection(&store, &namespace_id)
        .await
        .expect("materialization");
    assert!(metadata_states_equivalent(
        &projection.metadata_state,
        &after.metadata_state
    ));
}

/// A merge that skipped older runs must carry every row its inputs held, and
/// must leave the group's base alone.
///
/// The retention drop rules read across the merged rows: an unbind cancels
/// the bind it names, and both have to leave together. A window that starts
/// above the base cannot see the bind at all, so dropping the unbind there
/// would put a deleted file back. The step merges without dropping instead.
///
/// The output is a delta run at the newest input's sequence, so it stands
/// where that run stood and the group keeps exactly one base run. Writing it
/// at the base tier instead is what fragmented the base, and stamping it at
/// the manifest head is what made two merges of one quiet namespace collide
/// at one identity.
#[tokio::test]
async fn a_merge_above_the_base_keeps_the_rows_that_shadow_it() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");

    // The base run holds both bindings.
    for name in ["keep", "gone"] {
        write_file_bytes(
            &store,
            &namespace_id,
            &format!("/docs/{name}.txt"),
            b"body\n",
            &context,
            None,
        )
        .await
        .expect("seed file");
    }
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("checkpoint the seed");
    drain_reorganization(
        &store,
        &namespace_id,
        &context,
        MetadataLsmPolicy::default(),
    )
    .await;

    // One delta run cancels a binding the base holds, a second delta run
    // lands above it, and the floor moves past the cancellation.
    let deleted = delete_path(&store, &namespace_id, "/docs/gone.txt", &context, None)
        .await
        .expect("delete");
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("checkpoint the delete");
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/extra.txt",
        b"body\n",
        &context,
        None,
    )
    .await
    .expect("write extra");
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("checkpoint the extra file");
    advance_retention_floor(&store, &namespace_id, &context)
        .await
        .expect("advance the floor");
    assert!(
        read_floor_seq(&store, &namespace_id).await >= deleted.committed_seq,
        "the floor must sit past the cancelling row for this test to mean anything"
    );

    let before = load_manifest_materialization_for_inspection(
        &store,
        &namespace_id,
        current_manifest_no(&store, &namespace_id).await,
    )
    .await
    .expect("load the manifest before the fold");
    let (group, _) =
        select_reorganization_window(&store, &namespace_id, MetadataLsmPolicy::default()).await;
    assert!(
        group.contains(ApiMetadataRowFamily::DirentryUnbinds),
        "this test is about the binding families, got {group:?}"
    );
    let base_rows = group_run_rows(&base_run(&before.manifest), group.families());
    let policy = policy_that_cannot_fold_the_base(base_rows);

    let (_, selection) = select_reorganization_window(&store, &namespace_id, policy).await;
    let input = bounded_merge(selection).expect("the delta runs must still merge");
    let newest_input = input
        .runs
        .iter()
        .map(|run| run.run_seq)
        .max()
        .expect("the window holds runs");
    assert_eq!(
        input.placement,
        super::reorganize::MergePlacement::Delta {
            output_seq: newest_input
        },
        "a merge above the base writes a delta run at its newest input's sequence"
    );
    assert!(input.runs.len() > 1);
    let base_before = group_base_runs(&before.manifest, group.families());

    let report = super::reorganize_metadata_step(
        &store,
        &namespace_id,
        &context,
        policy,
        MetadataCompactionView::default(),
    )
    .await
    .expect("reorganization step");
    assert!(
        matches!(
            report.outcome,
            super::MetadataReorganizeOutcome::UnitPublished { .. }
        ),
        "expected a published unit, got {:?}",
        report.outcome
    );

    let after = load_manifest_materialization_for_inspection(
        &store,
        &namespace_id,
        current_manifest_no(&store, &namespace_id).await,
    )
    .await
    .expect("load the manifest after the fold");
    // The merge wrote a delta run, not a second base run: the group's base is
    // exactly the one it was, and the merged rows sit at the identity of the
    // newest run the window held, which is where that run stood.
    assert_eq!(
        group_base_runs(&after.manifest, group.families()),
        base_before,
        "a merge above the base must not mint a base run"
    );
    assert_eq!(
        group_delta_runs(&after.manifest, group.families()),
        vec![(newest_input, CHECKPOINT_L0_RUN_LEVEL)],
        "the merged delta runs must leave one delta run at the newest input's identity"
    );
    // A merge that skipped older runs drops nothing, so the row set is
    // exactly what it was: the cancelling row still shadows the base's
    // binding and the deleted file stays deleted.
    assert!(
        metadata_states_equivalent(&before.metadata_state, &after.metadata_state),
        "a merge above the base must be a pure rewrite"
    );
    assert!(
        !manifest_rows_for_family(&after.metadata_state, ApiMetadataRowFamily::DirentryUnbinds)
            .is_empty(),
        "the cancelling row must survive the merge"
    );
    let projection = load_current_projection(&store, &namespace_id)
        .await
        .expect("materialization");
    assert!(metadata_states_equivalent(
        &projection.metadata_state,
        &after.metadata_state
    ));
}

/// Skipping is only ever legal at the recency bottom.
///
/// A run in the middle that busts the budget stops the window. The selector
/// refuses to reach past it for a newer run that would have fit, because the
/// merged output stands where the window stood and reaching past a run would
/// move rows past it.
#[tokio::test]
async fn a_run_in_the_middle_over_the_budget_stops_the_window() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");

    write_file_and_checkpoint(&store, &namespace_id, &context, 1).await;
    drain_reorganization(
        &store,
        &namespace_id,
        &context,
        MetadataLsmPolicy::default(),
    )
    .await;
    // One wide delta run, then a narrow one above it.
    for index in 2..=7 {
        write_file_bytes(
            &store,
            &namespace_id,
            &format!("/docs/file-{index}.txt"),
            b"body\n",
            &context,
            None,
        )
        .await
        .expect("write file");
    }
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("checkpoint the wide run");
    write_file_and_checkpoint(&store, &namespace_id, &context, 8).await;

    let before = load_manifest_materialization_for_inspection(
        &store,
        &namespace_id,
        current_manifest_no(&store, &namespace_id).await,
    )
    .await
    .expect("load the manifest");
    let (group, _) =
        select_reorganization_window(&store, &namespace_id, MetadataLsmPolicy::default()).await;
    let base_rows = group_run_rows(&base_run(&before.manifest), group.families());
    let mut delta_rows = l0_runs(&before.manifest)
        .iter()
        .map(|run| group_run_rows(run, group.families()))
        .collect::<Vec<_>>();
    delta_rows.sort_unstable();
    let (narrow, wide) = (delta_rows[0], delta_rows[1]);

    // The budget admits the base run together with the narrow delta run,
    // and rules out the wide one that sits between them. Reaching past the
    // wide run is the illegal move this pins.
    let row_budget = base_rows + narrow;
    assert!(
        row_budget < base_rows + wide && row_budget < wide,
        "budget {row_budget} must exclude the wide run ({wide}) alone and beside the base ({base_rows})"
    );
    let policy = MetadataLsmPolicy {
        max_l0_runs: NonZeroUsize::MIN,
        max_decoded_input_rows_per_step: NonZeroUsize::new(
            usize::try_from(row_budget).expect("test row counts are small"),
        )
        .expect("test row budget should be nonzero"),
        ..MetadataLsmPolicy::default()
    };

    let (_, selection) = select_reorganization_window(&store, &namespace_id, policy).await;
    assert!(
        selection.group_bottom_over_budget.is_none(),
        "the base run fits here, so nothing should claim it does not"
    );
    assert!(
        matches!(
            selection.plan,
            Some(super::reorganize::ReorganizationPlan::FullCompaction(_))
        ),
        "no legal window exists; the selector must hand the group to a streaming compaction \
         rather than assemble one across the wide run"
    );

    let root_before = current_manifest_no(&store, &namespace_id).await;
    let report = super::reorganize_metadata_step(
        &store,
        &namespace_id,
        &context,
        policy,
        MetadataCompactionView::default(),
    )
    .await
    .expect("reorganization step");
    assert!(
        matches!(
            report.outcome,
            super::MetadataReorganizeOutcome::CompactionPlanned { .. }
        ),
        "expected a planned compaction, got {:?}",
        report.outcome
    );
    assert_eq!(
        current_manifest_no(&store, &namespace_id).await,
        root_before,
        "a step that plans a compaction must not publish"
    );
}

/// The soak's end state, as a test.
///
/// Four cycles of writes, deletions, and a retention floor that moves past
/// them, folded under budgets too small to take a group whole. The old level
/// rule turned every merge above the base into another base-tier run, so the
/// bases fragmented: a live S3 soak ended with six base-tier runs — direntry
/// binds split across three of them, revisions across four — and one run
/// identity carrying the output of several publications. Nothing then
/// reported any group as too large to fold from its oldest run, because every
/// fragment fit one step on its own, and the rows below the retention floor
/// could never be dropped.
///
/// What is pinned here is that neither happens. No group's base fragments,
/// whatever the budgets do; every step leaves reads answering exactly what
/// they answered before; a group whose bottom stops fitting one step is
/// handed to a streaming compaction rather than minting another base run;
/// and every cycle settles with nothing left to fold. There is no run of
/// steps that ends with a group nothing can fold.
#[tokio::test]
async fn repeated_churn_under_small_budgets_leaves_one_base_run_per_group() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    // Small segments so the folded base is many segments rather than one, and
    // a row budget the groups outgrow within a couple of cycles.
    let policy = MetadataLsmPolicy {
        max_l0_runs: NonZeroUsize::MIN,
        max_decoded_input_rows_per_step: NonZeroUsize::new(24).expect("nonzero"),
        max_rows_per_segment: NonZeroUsize::new(4).expect("nonzero"),
        ..MetadataLsmPolicy::default()
    };

    let group = group_containing(ApiMetadataRowFamily::DirentryUnbinds);
    let mut compacted_groups = 0usize;
    for cycle in 0..4u64 {
        for file in 0..4u64 {
            write_file_bytes(
                &store,
                &namespace_id,
                &format!("/d{cycle}/f{file}.txt"),
                format!("body {cycle}/{file}\n").as_bytes(),
                &context,
                None,
            )
            .await
            .expect("write file");
        }
        create_checkpoint(&store, &namespace_id, &context)
            .await
            .expect("checkpoint the writes");
        for file in 0..3u64 {
            delete_path(
                &store,
                &namespace_id,
                &format!("/d{cycle}/f{file}.txt"),
                &context,
                None,
            )
            .await
            .expect("delete a file");
        }
        create_checkpoint(&store, &namespace_id, &context)
            .await
            .expect("checkpoint the deletions");
        advance_retention_floor(&store, &namespace_id, &context)
            .await
            .expect("advance the floor past the deletions");
        let visible = visible_namespace(&store, &namespace_id).await;

        let mut settled = false;
        for _step in 0..512 {
            let report = super::reorganize_metadata_step(
                &store,
                &namespace_id,
                &context,
                policy,
                MetadataCompactionView::default(),
            )
            .await
            .expect("reorganization step");
            let current = load_manifest_materialization_for_inspection(
                &store,
                &namespace_id,
                current_manifest_no(&store, &namespace_id).await,
            )
            .await
            .expect("load the current manifest");
            for (folded, base_runs) in base_runs_per_family_group(&current.manifest) {
                assert!(
                    base_runs.len() <= 1,
                    "cycle {cycle}: {folded:?} holds base runs {base_runs:?}"
                );
            }
            assert_eq!(
                visible_namespace(&store, &namespace_id).await,
                visible,
                "cycle {cycle}: a step changed what a read answers"
            );
            match report.outcome {
                super::MetadataReorganizeOutcome::NotNeeded { .. } => {
                    settled = true;
                    break;
                }
                // The group's bottom no longer fits one step and its delta
                // runs are merged down to one. The step hands the group to a
                // streaming compaction, which is what folds it. The runner
                // runs that job in the background; here it runs inline, so
                // the steps that follow see what they would have seen once it
                // landed.
                super::MetadataReorganizeOutcome::CompactionPlanned { spec, .. } => {
                    publish_planned_compaction(&store, &namespace_id, &context, policy, &spec)
                        .await;
                    compacted_groups += 1;
                }
                super::MetadataReorganizeOutcome::Superseded => {
                    panic!("no concurrent publisher exists in this test")
                }
                super::MetadataReorganizeOutcome::UnitPublished { .. } => {}
            }
        }
        assert!(
            settled,
            "cycle {cycle}: reorganization did not settle with nothing left to fold"
        );
    }

    assert!(
        compacted_groups > 0,
        "a group must have outgrown one step and been rebuilt by a streaming compaction"
    );
    let final_manifest = load_manifest_materialization_for_inspection(
        &store,
        &namespace_id,
        current_manifest_no(&store, &namespace_id).await,
    )
    .await
    .expect("load the final manifest");
    assert_eq!(
        group_base_runs(&final_manifest.manifest, group).len(),
        1,
        "the bindings group must end in one base run"
    );
}
