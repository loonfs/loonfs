//! Checkpoint retention, reorganization, compaction, and publication budgets.

use super::*;

async fn current_manifest_id<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> ManifestId {
    read_metadata_root_object(store, namespace_id)
        .await
        .expect("read metadata root")
        .envelope
        .state
        .manifest_id
}

fn run_segment_object_keys(manifest: &NamespaceManifestEnvelope) -> Vec<String> {
    runs_from_metadata_files(&manifest.payload)
        .into_iter()
        .flat_map(|run| {
            run.tables.into_iter().flat_map(|table| {
                table
                    .segments
                    .into_iter()
                    .map(|descriptor| descriptor.object_key.clone())
            })
        })
        .collect()
}

fn assert_manifest_rows_have_unique_keys(metadata_state: &MetadataState) {
    for family in CHECKPOINT_TABLE_FAMILIES {
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

type TableRunShape = (ApiMetadataTableFamily, u64, usize);
type ManifestRunShape = (ChangeSeq, u32, Vec<TableRunShape>);

fn manifest_run_shape(manifest: &NamespaceManifestEnvelope) -> Vec<ManifestRunShape> {
    runs_from_metadata_files(&manifest.payload)
        .into_iter()
        .map(|run| {
            let tables = run
                .tables
                .into_iter()
                .map(|table| {
                    let row_count = table
                        .segments
                        .iter()
                        .map(|descriptor| descriptor.row_count)
                        .sum();
                    (table.family, row_count, table.segments.len())
                })
                .collect();
            (run.run_seq, run.level, tables)
        })
        .collect()
}

async fn drain_reorganization_with_count<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
    policy: MetadataLsmPolicy,
) -> (ManifestId, usize) {
    let mut published = 0usize;
    for _ in 0..128 {
        let report = super::reorganize_metadata_step(store, namespace_id, context, policy)
            .await
            .expect("reorganization step");
        match report.outcome {
            super::MetadataReorganizeOutcome::UnitPublished { .. } => published += 1,
            super::MetadataReorganizeOutcome::Superseded => {
                panic!("single-writer test must not be superseded")
            }
            super::MetadataReorganizeOutcome::BudgetExhausted { .. } => {
                panic!("test budget must admit a progress-making subset")
            }
            super::MetadataReorganizeOutcome::NotNeeded { .. } => {
                return (current_manifest_id(store, namespace_id).await, published);
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

impl crate::timing::MonotonicTimer for SteppingTimer {
    fn monotonic_now_ms(&self) -> u64 {
        self.now_ms
            .fetch_add(self.step_ms, std::sync::atomic::Ordering::SeqCst)
    }
}

#[tokio::test]
async fn retention_advancement_uses_published_manifest_and_updates_floor_only() {
    let temp_dir = tempdir().expect("tempdir");
    let store = CountingStore::metadata_tables(LocalFsStore::new(temp_dir.path()).expect("store"));
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
        "retention should validate manifest descriptors without loading metadata SST payloads"
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

#[tokio::test]
async fn retention_floor_does_not_advance_past_missing_metadata_segment() {
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
    let materialized =
        load_manifest_materialization_for_inspection(&store, &namespace_id, checkpoint.manifest_id)
            .await
            .expect("load manifest");
    let missing_key = materialized
        .manifest
        .payload
        .metadata_files
        .first()
        .expect("metadata file")
        .object_key
        .clone();
    store.delete(&missing_key).await.expect("delete segment");

    let error = advance_retention_floor(&store, &namespace_id, &context)
        .await
        .expect_err("floor must not advance past missing segment");
    assert!(
        matches!(error, CoreError::CheckpointUnavailable(_)),
        "unexpected error: {error:?}"
    );
    assert_eq!(read_floor_seq(&store, &namespace_id).await, ChangeSeq(0));
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
        loonfs_api::NamePolicy::default(),
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
    assert!(release.expect("release").was_active);
    let second = second.expect("a concurrent create takes a fresh pin");
    assert_ne!(
        second.checkpoint_id, first.checkpoint_id,
        "a new pin never lands on a released record's key"
    );

    let released = read_checkpoint_record(&store, &namespace_id, &first.checkpoint_id)
        .await
        .expect("read checkpoint record")
        .expect("record exists")
        .state;
    assert_eq!(
        released.state,
        loonfs_api::wire::control::CheckpointRecordLifecycle::Released {
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
    assert!(!again.was_active);
    assert_eq!(
        read_checkpoint_record(&store, &namespace_id, &first.checkpoint_id)
            .await
            .expect("read checkpoint record")
            .expect("record exists")
            .state
            .state,
        loonfs_api::wire::control::CheckpointRecordLifecycle::Released {
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
    let owner = || loonfs_api::wire::control::CheckpointOwner::User {
        name: "test-pin".to_owned(),
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
        super::create::create_checkpoint(&store, &namespace_id, owner(), Some(10_000), &context)
            .await
            .expect("create checkpoint");
    assert_eq!(first.expires_at_ms, Some(10_000));

    let mut later_context = test_context();
    later_context.now_ms = 2_000;
    let mut minted = BTreeSet::from([first.checkpoint_id.clone()]);
    for expiry in [Some(99_000), Some(5_000), None] {
        let next = super::create::create_checkpoint(
            &store,
            &namespace_id,
            owner(),
            expiry,
            &later_context,
        )
        .await
        .expect("create checkpoint");
        assert!(
            minted.insert(next.checkpoint_id.clone()),
            "each pin gets an id of its own"
        );
        assert_eq!(next.expires_at_ms, expiry);
        let record = read_checkpoint_record(&store, &namespace_id, &next.checkpoint_id)
            .await
            .expect("read checkpoint record")
            .expect("record exists")
            .state;
        assert_eq!(record.expires_at_ms, expiry);
        assert_eq!(record.created_at_ms, 2_000);
        assert_eq!(
            record.state,
            loonfs_api::wire::control::CheckpointRecordLifecycle::Active
        );
    }

    // The very first record is untouched by any of it: a pin taken without
    // an expiry, or with one, is held until something releases it.
    let original = read_checkpoint_record(&store, &namespace_id, &first.checkpoint_id)
        .await
        .expect("read checkpoint record")
        .expect("record exists")
        .state;
    assert_eq!(original.created_at_ms, 1_000, "creation instant is history");
    assert_eq!(original.expires_at_ms, Some(10_000));
    assert_eq!(
        original.state,
        loonfs_api::wire::control::CheckpointRecordLifecycle::Active
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
        },
        Some(context.now_ms),
        &context,
    )
    .await
    .expect("create checkpoint whose expiry has already passed");
    assert_eq!(
        read_checkpoint_record(&store, &namespace_id, &already_expired.checkpoint_id)
            .await
            .expect("read checkpoint record")
            .expect("record exists")
            .state
            .state,
        loonfs_api::wire::control::CheckpointRecordLifecycle::Active
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
    let record = read_checkpoint_record(&store, &namespace_id, &pin.checkpoint_id)
        .await
        .expect("read checkpoint record")
        .expect("an unexpiring pin survives every pass")
        .state;
    assert_eq!(
        record.state,
        loonfs_api::wire::control::CheckpointRecordLifecycle::Active
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
        manifest_id: ManifestId(0),
        manifest_object_id: manifest_object_id(ManifestId(0)),
        manifest_head_seq: ChangeSeq(0),
        manifest_payload_checksum: "sha256:stale".to_owned(),
        head_commit_id: CommitId::parse("c_00000000000000000000000000000000").expect("commit id"),
        created_at_ms: context.now_ms,
        expires_at_ms: None,
        owner: loonfs_api::wire::control::CheckpointOwner::User {
            name: "test-pin".to_owned(),
        },
        state: loonfs_api::wire::control::CheckpointRecordLifecycle::Active,
    };
    let verified = super::record::verify_checkpoint_basis(&store, &stale)
        .await
        .expect("verification runs");
    assert!(!verified, "sub-floor basis must not verify");
}

#[tokio::test]
async fn publish_backpressure_rejects_when_wal_tail_outruns_maintenance() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    let limit =
        u32::try_from(crate::commit_engine::WalTailPolicy::DEFAULT.reject_writes_at_segments)
            .expect("limit");
    for round in 0..=limit {
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

    let error = write_file_bytes(
        &store,
        &namespace_id,
        "/docs/one-too-many.txt",
        b"body\n",
        &context,
        None,
    )
    .await
    .expect_err("publish past the backpressure limit must be rejected");
    assert_eq!(error.code(), ErrorCode::MaintenanceRequired);

    // Reads never gate: the change feed still serves the whole tail.
    let changes = list_changes_after(
        &store,
        &namespace_id,
        ChangeSeq(0),
        EffectiveLimit::new(NonZeroU32::new(200).expect("nonzero")),
    )
    .await
    .expect("list changes");
    assert_eq!(changes.through_seq, ChangeSeq(129));
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
        current_manifest_id(&store, &namespace_id).await,
    )
    .await
    .expect("load appended manifest");
    assert_eq!(l0_runs(&appended.manifest).len(), 4);
    assert_eq!(appended.manifest.payload.base_seq, ChangeSeq(0));

    // Reorganization folds one family group per unit, each publishing its
    // own manifest — the manifest chain is the progress record.
    let mut units = 0usize;
    let mut last_manifest_id = None;
    loop {
        let report = super::reorganize_metadata_step(&store, &namespace_id, &context, policy)
            .await
            .expect("reorganization step");
        match report.outcome {
            super::MetadataReorganizeOutcome::UnitPublished { manifest_id, .. } => {
                units += 1;
                if let Some(previous) = last_manifest_id {
                    assert!(manifest_id > previous, "units must advance the manifest");
                }
                last_manifest_id = Some(manifest_id);
            }
            super::MetadataReorganizeOutcome::Superseded => {
                panic!("no concurrent publisher exists in this test")
            }
            super::MetadataReorganizeOutcome::NotNeeded { .. } => break,
            super::MetadataReorganizeOutcome::BudgetExhausted { .. } => {
                panic!("test reorganization budget should admit a progress-making subset")
            }
        }
    }
    assert!(units >= 2, "several family groups should fold, got {units}");

    let drained = load_manifest_materialization_for_inspection(
        &store,
        &namespace_id,
        current_manifest_id(&store, &namespace_id).await,
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
    let store = CountingStore::metadata_tables(LocalFsStore::new(temp_dir.path()).expect("store"));
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

    let root_before = current_manifest_id(&store, &namespace_id).await;
    let tiny_byte_policy = MetadataLsmPolicy {
        max_l0_runs: NonZeroUsize::MIN,
        max_input_runs_per_step: NonZeroUsize::new(2).expect("test run budget should be nonzero"),
        max_decoded_input_bytes_per_step: NonZeroUsize::MIN,
        ..MetadataLsmPolicy::default()
    };
    store.reset();
    let blocked =
        super::reorganize_metadata_step(&store, &namespace_id, &context, tiny_byte_policy)
            .await
            .expect("budgeted step");
    let super::MetadataReorganizeOutcome::BudgetExhausted { families, .. } = blocked.outcome else {
        panic!("one byte must not admit an SST run");
    };
    assert_eq!(
        current_manifest_id(&store, &namespace_id).await,
        root_before,
        "a budget-blocked step must not publish"
    );
    assert_eq!(store.count(OperationClass::Put), 0);
    assert!(
        store.count(OperationClass::Read) <= families.len(),
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
    let published = super::reorganize_metadata_step(&store, &namespace_id, &context, policy)
        .await
        .expect("bounded step");
    let super::MetadataReorganizeOutcome::UnitPublished {
        families,
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
        store.count(OperationClass::Read) <= input_runs * families.len() * 2,
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
    let first =
        super::reorganize_metadata_step(&bounded_store, &namespace_id, &context, bounded_policy)
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

    let (bounded_manifest_id, bounded_steps_after_first) =
        drain_reorganization_with_count(&bounded_store, &namespace_id, &context, bounded_policy)
            .await;
    let unbounded_policy = MetadataLsmPolicy {
        max_l0_runs: NonZeroUsize::new(4).expect("test trigger should be nonzero"),
        max_input_runs_per_step: unlimited,
        max_decoded_input_rows_per_step: unlimited,
        max_decoded_input_bytes_per_step: unlimited,
        ..MetadataLsmPolicy::default()
    };
    let (unbounded_manifest_id, unbounded_steps) = drain_reorganization_with_count(
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
        bounded_manifest_id,
    )
    .await
    .expect("load bounded result");
    let unbounded = load_manifest_materialization_for_inspection(
        &unbounded_store,
        &namespace_id,
        unbounded_manifest_id,
    )
    .await
    .expect("load unbounded result");
    assert!(metadata_states_equivalent(
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
    let below_trigger =
        super::reorganize_metadata_step(&bounded_store, &namespace_id, &context, bounded_policy)
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

    let first_manifest_id =
        checkpoint_then_reorganize(&store, &namespace_id, &context, policy).await;
    let first_materialized =
        load_manifest_materialization_for_inspection(&store, &namespace_id, first_manifest_id)
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
    let compacted_manifest_id = drain_reorganization(&store, &namespace_id, &context, policy).await;
    let compacted_materialized =
        load_manifest_materialization_for_inspection(&store, &namespace_id, compacted_manifest_id)
            .await
            .expect("load compacted manifest");
    let materialization_after = load_current_projection(&store, &namespace_id)
        .await
        .expect("materialization");
    let compacted_run_keys = run_segment_object_keys(&compacted_materialized.manifest);
    let compacted_run_prefix = format!("namespaces/{}/metadata/tables/tbl_", namespace_id.as_str());

    assert_eq!(
        compacted_materialized.manifest.payload.base_seq,
        compacted.checkpoint_seq
    );
    assert!(l0_runs(&compacted_materialized.manifest).is_empty());
    assert_eq!(
        runs_from_metadata_files(&compacted_materialized.manifest.payload).len(),
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

    let first_manifest_id =
        checkpoint_then_reorganize(&store, &namespace_id, &context, policy).await;
    let first_materialized =
        load_manifest_materialization_for_inspection(&store, &namespace_id, first_manifest_id)
            .await
            .expect("load first manifest");
    let revision_keys_before = base_segment_object_keys_for_family(
        &first_materialized.manifest,
        ApiMetadataTableFamily::Revisions,
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
    let compacted_manifest_id = drain_reorganization(&store, &namespace_id, &context, policy).await;
    let compacted_materialized =
        load_manifest_materialization_for_inspection(&store, &namespace_id, compacted_manifest_id)
            .await
            .expect("load compacted manifest");
    let revision_keys_after = base_segment_object_keys_for_family(
        &compacted_materialized.manifest,
        ApiMetadataTableFamily::Revisions,
    );
    let materialization_after = load_current_projection(&store, &namespace_id)
        .await
        .expect("materialization");

    assert!(l0_runs(&compacted_materialized.manifest).is_empty());
    // Groups untouched since the first fold keep their older base run, so
    // several base runs may coexist; what matters is that no delta remains.
    assert!(
        runs_from_metadata_files(&compacted_materialized.manifest.payload)
            .iter()
            .all(|run| run.level == CHECKPOINT_BASE_RUN_LEVEL)
    );
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
    let first_report = super::reorganize_metadata_step(&store, &namespace_id, &context, policy)
        .await
        .expect("first unit");
    let super::MetadataReorganizeOutcome::UnitPublished {
        families: first_families,
        ..
    } = first_report.outcome
    else {
        panic!("expected a published unit, got {:?}", first_report.outcome);
    };

    // A checkpoint lands in between, adding a fresh delta run.
    write_file_and_checkpoint(&store, &namespace_id, &context, 9).await;

    // A fresh sequence of steps reads the live manifest and finishes the
    // job, including the delta the interleaved checkpoint added.
    let mut folded_groups = vec![first_families];
    loop {
        let report = super::reorganize_metadata_step(&store, &namespace_id, &context, policy)
            .await
            .expect("resumed unit");
        match report.outcome {
            super::MetadataReorganizeOutcome::UnitPublished { families, .. } => {
                folded_groups.push(families);
            }
            super::MetadataReorganizeOutcome::Superseded => {
                panic!("no concurrent publisher exists in this test")
            }
            super::MetadataReorganizeOutcome::NotNeeded { .. } => break,
            super::MetadataReorganizeOutcome::BudgetExhausted { .. } => {
                panic!("test reorganization budget should admit a progress-making subset")
            }
        }
    }
    assert!(folded_groups.len() >= 2);

    let drained = load_manifest_materialization_for_inspection(
        &store,
        &namespace_id,
        current_manifest_id(&store, &namespace_id).await,
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
        current_manifest_id(&store, &namespace_id).await,
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
            .head(&loonfs_objectstore::keys::wal_floor(namespace_id.as_str()))
            .await
            .expect("probe floor")
            .is_none(),
        "creation writes no floor object"
    );

    let head = crate::namespace::control::read_head_object(&store, &namespace_id)
        .await
        .expect("head")
        .envelope
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
        writer_session_id: "wrs_budget_test".to_owned(),
        writer_version: "budget-test/0.1.0".to_owned(),
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
    let root_before = read_metadata_root_object(&store, &namespace_id)
        .await
        .expect("read root")
        .envelope
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

    let root_after = read_metadata_root_object(&store, &namespace_id)
        .await
        .expect("read root")
        .envelope
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
    assert!(advanced.manifest_id > root_before.manifest_id);
}

/// An over-budget reorganization unit aborts the same way: no root motion,
/// orphan tables left for garbage collection.
#[tokio::test]
async fn over_budget_reorganization_aborts_without_publishing() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = MutationContext {
        writer_id: "budget-test".to_owned(),
        writer_session_id: "wrs_budget_test".to_owned(),
        writer_version: "budget-test/0.1.0".to_owned(),
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
    let root_before = read_metadata_root_object(&store, &namespace_id)
        .await
        .expect("read root")
        .envelope
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
        &overrun,
    )
    .await
    .expect_err("over-budget reorganization must abort");
    assert!(
        matches!(error, CoreError::MetadataPublicationBudgetExceeded { .. }),
        "expected budget error, got {error:?}"
    );

    let root_after = read_metadata_root_object(&store, &namespace_id)
        .await
        .expect("read root")
        .envelope
        .state;
    assert_eq!(root_after, root_before);

    let report = super::reorganize::reorganize_metadata_step(
        &store,
        &namespace_id,
        &context,
        fold_everything,
    )
    .await
    .expect("in-budget retry folds the unit");
    assert!(matches!(
        report.outcome,
        super::MetadataReorganizeOutcome::UnitPublished { .. }
    ));
}
