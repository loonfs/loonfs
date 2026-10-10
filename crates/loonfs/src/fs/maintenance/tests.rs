//! The frozen-base policy over a live runtime: amortized bounded work and
//! explicit compaction.
//!
//! These tests need a family group whose base run no bounded step can compact,
//! with delta runs still arriving above it. The shipped row budget only
//! reaches that state at a scale no test can write, so the budget is narrowed
//! through [`Maintenance::starve_compaction_row_budget`]. Everything else —
//! planning, admission, the executor, the finalizer — is the shipped path.

use crate::metrics::{DefaultMetricsRecorder, MetricValue, MetricsSnapshot};
use crate::{
    CompactionStepOutcome, LoonFs, Maintenance, MetadataCompactionOutcome,
    MetadataCompactionPolicy, MetadataMaintenanceOptions, NamespaceId, SharedObjectStore, Writable,
};
use loonfs_core::MetadataFamilyGroup;
use loonfs_objectstore::keys::metadata_manifest_object;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::ObjectStore;
use loonfs_test_support::ids::namespace_id;
use loonfs_types::format::manifest::{
    decode_namespace_manifest_json, MetadataRowFamily, NamespaceManifestPayload, RunTier,
};
use std::collections::BTreeSet;
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;
use tempfile::tempdir;

/// The group these tests watch, because it is the one the planner selects:
/// selection takes the group holding the most delta rows, and every write and
/// every rename puts more rows there than anywhere else. Manifest
/// descriptors are per family, so its families are what they are filtered by.
const BINDINGS: MetadataFamilyGroup = MetadataFamilyGroup::Bindings;

/// A writer and two maintenance handles over one store.
///
/// This is the shape the explicit compaction path exists for. The second
/// maintenance is the contrast — it shares the writer's runtime core and caches.
async fn manual_deployment(
    root: &std::path::Path,
) -> (
    LoonFs<Writable>,
    Maintenance,
    Maintenance,
    Arc<DefaultMetricsRecorder>,
) {
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(root).expect("create local-fs store"));
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let writer = LoonFs::builder_with_store(Arc::clone(&store))
        .writer_id("manual-writer")
        .build()
        .await
        .expect("build the writer");
    let standalone = LoonFs::builder_with_store(Arc::clone(&store))
        .writer_id("standalone-maintenance")
        .metrics_recorder(recorder.clone())
        .build()
        .await
        .expect("build the standalone maintenance")
        .maintenance(loonfs_test_support::ids::writer_id(
            "standalone-maintenance",
        ));
    let scheduled =
        writer.maintenance(loonfs_test_support::ids::writer_id("scheduled-maintenance"));
    (writer, standalone, scheduled, recorder)
}

fn counter(snapshot: &MetricsSnapshot, name: &str, labels: &[(&str, &str)]) -> u64 {
    let entry = snapshot
        .by_name(name)
        .find(|entry| entry.labels == labels)
        .expect("the counter must be registered with these labels");
    assert!(
        matches!(entry.value, MetricValue::Counter(_)),
        "expected a counter, found {:?}",
        entry.value
    );
    let MetricValue::Counter(value) = entry.value else {
        return 0;
    };
    value
}

fn metadata_options(compaction_policy: MetadataCompactionPolicy) -> MetadataMaintenanceOptions {
    MetadataMaintenanceOptions {
        max_wal_tail_objects: NonZeroU64::MIN,
        compaction_policy,
        ..Default::default()
    }
}

#[tokio::test]
async fn a_maintenance_gc_step_records_the_pass_counters_once() {
    let temp_dir = tempdir().expect("tempdir");
    let (writer, maintenance, _scheduled, recorder) = manual_deployment(temp_dir.path()).await;
    let namespace = namespace_id("maintenance-gc-metrics");
    writer
        .create_namespace(&namespace, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let namespace_writer = writer.open_namespace(&namespace).expect("open namespace");
    namespace_writer
        .put_file("/live.txt", b"live", &loonfs_test_support::test_actor())
        .await
        .expect("write a live GC candidate");

    assert_eq!(counter(&recorder.snapshot(), "loonfs.gc.retained", &[]), 0);
    let gc = maintenance
        .gc(&namespace)
        .await
        .expect("run the maintenance GC step");
    assert!(
        gc.retained.total() > 0,
        "the live namespace gives the pass candidates to retain"
    );

    let snapshot = recorder.snapshot();
    assert_eq!(
        counter(&snapshot, "loonfs.gc.retained", &[]),
        gc.retained.total(),
        "the maintenance pass records its retained count exactly once"
    );
    for (category, reclaimed) in [
        ("deleted_wal_objects", gc.deleted.wal_objects),
        ("deleted_metadata_segments", gc.deleted.metadata_segments),
        ("deleted_manifests", gc.deleted.manifests),
        (
            "deleted_fork_checkpoints",
            gc.deleted_checkpoints_by_owner.fork,
        ),
        (
            "deleted_expired_checkpoints",
            gc.deleted_checkpoints_by_owner.user,
        ),
        ("deleted_upload_sessions", gc.deleted.upload_sessions),
        ("deleted_content_objects", gc.deleted.content_objects),
        (
            "deleted_snapshot_checkpoints",
            gc.deleted_checkpoints_by_owner.snapshot,
        ),
        ("deleted_temporary_objects", gc.deleted.temporary_objects),
        (
            "deleted_retired_content_objects",
            gc.deleted.retired_content_objects,
        ),
    ] {
        assert_eq!(
            counter(&snapshot, "loonfs.gc.reclaimed", &[("category", category)],),
            reclaimed,
            "the maintenance pass records `{category}` exactly once"
        );
    }
}

/// Writes one file and folds the tail, so each call leaves one more delta run.
async fn write_and_fold(
    writer: &LoonFs<Writable>,
    maintenance: &Maintenance,
    namespace_id: &NamespaceId,
    path: &str,
) {
    let namespace = writer.open_namespace(namespace_id).expect("open namespace");
    namespace
        .put_file(path, path.as_bytes(), &loonfs_test_support::test_actor())
        .await
        .expect("put a file");
    maintenance
        .fold_wal(namespace_id)
        .await
        .expect("fold the tail");
}

/// Builds a namespace whose bindings group holds one base run of real size,
/// with retention-eligible churn inside it, and then leaves delta runs piling
/// up above that base.
///
/// The order matters. The churn is merged into the base while the retention
/// floor is still at the bottom, so nothing drops on the way in and the rows a
/// rebuild may drop are in the base when the rebuild reads it. The floor is
/// advanced past that churn next, and the delta runs come last, because they
/// are what makes a delta-only merge available at the moment the tests ask for
/// a compaction.
async fn namespace_with_a_frozen_base(
    writer: &LoonFs<Writable>,
    maintenance: &Maintenance,
    namespace_id: &NamespaceId,
) {
    writer
        .create_namespace(namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create the namespace");
    let namespace = writer.open_namespace(namespace_id).expect("open namespace");
    for index in 0..24 {
        write_and_fold(
            writer,
            maintenance,
            namespace_id,
            &format!("/docs/file-{index}.txt"),
        )
        .await;
    }
    // Churn: every rename retires one binding and creates another, and a
    // bottom-anchored rebuild below the floor drops the retired pair.
    for index in 0..12 {
        namespace
            .move_path(
                &format!("/docs/file-{index}.txt"),
                &format!("/docs/moved-{index}.txt"),
                &loonfs_test_support::test_actor(),
            )
            .await
            .expect("rename a file");
    }
    maintenance
        .fold_wal(namespace_id)
        .await
        .expect("fold the tail");

    // Merge everything into one base run per group, under the shipped budgets
    // and with the floor still at the bottom, so the churn lands in the base.
    for _ in 0..64 {
        let response = maintenance
            .maintain_metadata_with_options(
                namespace_id,
                &metadata_options(MetadataCompactionPolicy::CompactImmediately),
            )
            .await
            .expect("merge a unit");
        if response.compaction == (CompactionStepOutcome::NotNeeded {}) {
            break;
        }
    }

    // Now the floor moves past that churn, so the next bottom-anchored rebuild
    // is the one that may drop it.
    maintenance
        .create_checkpoint(namespace_id, "retention")
        .await
        .expect("checkpoint the namespace");
    maintenance
        .advance_retention_floor(namespace_id)
        .await
        .expect("advance the retention floor past the churn");
}

/// Keeps writing while the group's base is frozen, so a delta-only merge is
/// always available above it. Each write is folded with no compaction step,
/// so each leaves one more delta run behind.
async fn sustained_writes(
    writer: &LoonFs<Writable>,
    maintenance: &Maintenance,
    namespace_id: &NamespaceId,
) {
    for index in 0..10 {
        write_and_fold(
            writer,
            maintenance,
            namespace_id,
            &format!("/arrivals/arrival-{index}.txt"),
        )
        .await;
    }
}

/// A per-step row budget the bindings group's base run does not fit, taken
/// from the namespace itself.
///
/// One short of that base starves the group's bottom-anchored window — no
/// window starting at the bottom makes progress — while still admitting the
/// far smaller delta runs above it, so a delta-only merge stays available.
async fn budget_that_starves_the_bindings_base<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> NonZeroUsize {
    let base_rows: u64 = manifest_runs(store, namespace_id)
        .await
        .into_iter()
        .filter(|run| run.tier == RunTier::Base && BINDINGS.families().contains(&run.family))
        .map(|run| run.rows)
        .sum();
    assert!(
        base_rows > 8,
        "the seed must leave the bindings group a base run with room for a budget above its delta \
         runs, got {base_rows}"
    );
    NonZeroUsize::new(usize::try_from(base_rows).expect("test row counts are small") - 1)
        .expect("nonzero")
}

/// One family's descriptors in one run of the current manifest.
struct ManifestRun {
    run_seq: u64,
    tier: RunTier,
    family: MetadataRowFamily,
    rows: u64,
}

async fn manifest_runs<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Vec<ManifestRun> {
    current_manifest_payload(store, namespace_id)
        .await
        .runs
        .iter()
        .flat_map(|run| {
            run.segments.iter().map(|descriptor| ManifestRun {
                run_seq: run.run_seq.0,
                tier: run.tier,
                family: descriptor.family,
                rows: descriptor.row_count,
            })
        })
        .collect()
}

async fn current_manifest_payload<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> NamespaceManifestPayload {
    let root = loonfs_core::control::load_namespace_current_manifest(store, namespace_id)
        .await
        .expect("read the metadata root");
    let key = metadata_manifest_object(namespace_id, &root.state.manifest().manifest_no);
    let bytes = store
        .get(&key, None)
        .await
        .expect("read the manifest")
        .expect("the manifest exists");
    decode_namespace_manifest_json(&bytes)
        .expect("decode the manifest")
        .into_payload()
}

/// The runs the bindings group holds right now, and the rows in them.
async fn bindings_runs<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> (BTreeSet<(u64, RunTier)>, u64) {
    manifest_runs(store, namespace_id)
        .await
        .into_iter()
        .filter(|run| BINDINGS.families().contains(&run.family))
        .fold((BTreeSet::new(), 0), |(mut runs, rows), run| {
            runs.insert((run.run_seq, run.tier));
            (runs, rows + run.rows)
        })
}

#[tokio::test]
async fn compaction_planning_survives_restart_and_explicit_work_has_bounded_fan_in() {
    let temp_dir = tempdir().expect("tempdir");
    let (writer, standalone, scheduled, recorder) = manual_deployment(temp_dir.path()).await;
    let explicit = namespace_id("explicit");
    let automatic = namespace_id("automatic");
    for namespace in [&explicit, &automatic] {
        namespace_with_a_frozen_base(&writer, &standalone, namespace).await;
    }
    let store = LocalFsStore::new(temp_dir.path()).expect("create local-fs store");
    let budget = budget_that_starves_the_bindings_base(&store, &explicit).await;
    let standalone = standalone.starve_compaction_row_budget(budget);
    let scheduled = scheduled.starve_compaction_row_budget(budget);
    for namespace in [&explicit, &automatic] {
        sustained_writes(&writer, &standalone, namespace).await;
    }

    // A small group is eligible immediately, even when its rows require a job.
    let metadata = scheduled
        .maintain_metadata_with_options(
            &automatic,
            &metadata_options(MetadataCompactionPolicy::SizeTiered),
        )
        .await
        .expect("plan automatic compaction");
    assert_eq!(
        metadata.compaction,
        CompactionStepOutcome::MetadataCompactionRequired {}
    );

    writer.shutdown().await.expect("shut down the first writer");
    let fresh_store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("create local-fs store"));
    let fresh_writer = LoonFs::builder_with_store(Arc::clone(&fresh_store))
        .writer_id("fresh-writer")
        .build()
        .await
        .expect("build a fresh writer");
    let fresh_scheduled = fresh_writer
        .maintenance(loonfs_test_support::ids::writer_id(
            "fresh-scheduled-maintenance",
        ))
        .starve_compaction_row_budget(budget);
    let metadata = fresh_scheduled
        .maintain_metadata_with_options(
            &automatic,
            &metadata_options(MetadataCompactionPolicy::SizeTiered),
        )
        .await
        .expect("replan after restart");
    assert_eq!(
        metadata.compaction,
        CompactionStepOutcome::MetadataCompactionRequired {},
        "the same durable run sizes produce the same plan after restart"
    );
    fresh_writer
        .shutdown()
        .await
        .expect("shut down the fresh writer");

    let (runs_before, rows_before) = bindings_runs(&store, &explicit).await;
    assert!(
        runs_before.len() > 1,
        "the group must hold a base run and delta runs above it, got {runs_before:?}"
    );

    let outcome = standalone
        .compact_metadata(&explicit)
        .await
        .expect("run the explicit compaction");
    assert!(
        matches!(
            outcome.compaction,
            MetadataCompactionOutcome::Published { .. }
        ),
        "the explicit call must run and publish the job rather than a delta merge, got {outcome:?}"
    );

    let (runs_after, rows_after) = bindings_runs(&store, &explicit).await;
    assert_eq!(
        runs_after.len(),
        runs_before.len() - 8 + 1,
        "one job replaces eight inputs and preserves the rest, got {runs_after:?}"
    );
    assert_eq!(
        runs_after
            .iter()
            .filter(|(_, tier)| *tier == RunTier::Base)
            .count(),
        1,
        "the group retains exactly one base"
    );
    assert!(
        rows_after < rows_before,
        "the rebuild drops the churn below the retention floor: {rows_before} rows became \
         {rows_after}"
    );

    let snapshot = recorder.snapshot();
    assert_eq!(
        counter(
            &snapshot,
            "loonfs.maintenance.compactions",
            &[("outcome", "completed")],
        ),
        1,
    );
    for (name, direction) in [
        ("loonfs.maintenance.compaction_rows", "input"),
        ("loonfs.maintenance.compaction_rows", "output"),
        ("loonfs.maintenance.compaction_bytes", "input"),
        ("loonfs.maintenance.compaction_bytes", "output"),
    ] {
        assert!(
            counter(&snapshot, name, &[("direction", direction)]) > 0,
            "`{name}` must report nonzero `{direction}` work"
        );
    }
}

#[tokio::test]
async fn an_immediate_step_reports_the_compaction_the_explicit_call_runs() {
    let temp_dir = tempdir().expect("tempdir");
    let (writer, standalone, _scheduled, _recorder) = manual_deployment(temp_dir.path()).await;
    let namespace = namespace_id("manual");
    namespace_with_a_frozen_base(&writer, &standalone, &namespace).await;
    let store = LocalFsStore::new(temp_dir.path()).expect("create local-fs store");
    let budget = budget_that_starves_the_bindings_base(&store, &namespace).await;
    let standalone = standalone.starve_compaction_row_budget(budget);
    sustained_writes(&writer, &standalone, &namespace).await;

    let response = standalone
        .maintain_metadata_with_options(
            &namespace,
            &crate::MetadataMaintenanceOptions {
                max_wal_tail_objects: std::num::NonZeroU64::MIN,
                compaction_policy: MetadataCompactionPolicy::CompactImmediately,
                ..Default::default()
            },
        )
        .await
        .expect("run an immediate step");
    assert_eq!(
        response.compaction,
        CompactionStepOutcome::MetadataCompactionRequired {},
        "an immediate step says the namespace needs a compaction job"
    );

    let outcome = standalone
        .compact_metadata(&namespace)
        .await
        .expect("run the explicit compaction");
    assert!(
        matches!(
            outcome.compaction,
            MetadataCompactionOutcome::Published { .. }
        ),
        "and the explicit call runs it, got {outcome:?}"
    );
}

#[tokio::test]
async fn explicit_compaction_merges_twenty_deltas_and_reads_the_large_base_once() {
    use crate::publish::{
        parse_mutation_path, CommitCandidate, CommitRequest, FilesystemOperation,
    };
    use crate::{AttributeChanges, CommitId};
    use loonfs_objectstore::keys::metadata_segment;
    use loonfs_test_support::ids::{attribute_key, attribute_text};
    use loonfs_test_support::stores::RecordingStore;
    use loonfs_types::Checksum;

    let directory = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::metadata_segments(
        LocalFsStore::new(directory.path()).expect("store"),
    ));
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id("writer")
        .build()
        .await
        .expect("writer");
    let maintenance = writer.maintenance(loonfs_test_support::ids::writer_id("maintenance"));
    let namespace = namespace_id("compact-deltas-first");
    writer
        .create_namespace(&namespace, &loonfs_test_support::test_actor())
        .await
        .expect("namespace");
    let namespace_writer = writer.open_namespace(&namespace).expect("open namespace");
    let keys: Vec<_> = (0..15)
        .map(|index| attribute_key(&format!("key-{index}")))
        .collect();
    namespace_writer
        .put_file("/file", b"content", &loonfs_test_support::test_actor())
        .await
        .expect("file");
    for batch in 0..10 {
        let operations = (batch * 32..(batch + 1) * 32)
            .map(|revision| FilesystemOperation::UpdateAttributes {
                path: parse_mutation_path("/file").expect("file path"),
                set: keys
                    .iter()
                    .enumerate()
                    .map(|(index, key)| {
                        let value: String = (0..64)
                            .map(|part| {
                                Checksum::sha256(format!("{revision}/{index}/{part}").as_bytes())
                                    .value
                            })
                            .collect();
                        (key.clone(), attribute_text(&value))
                    })
                    .collect(),
                remove: Vec::new(),
                expected_inode_id: None,
                expected_attributes_revision_no: None,
            })
            .collect();
        namespace_writer
            .commit_candidate(CommitCandidate::new(CommitRequest {
                commit_id: CommitId::generate(),
                actor_id: loonfs_test_support::test_actor(),
                subject: None,
                message: None,
                preconditions: Vec::new(),
                operations,
            }))
            .await
            .expect("write the base attribute history");
    }
    maintenance.fold_wal(&namespace).await.expect("fold base");
    let maintenance =
        maintenance.starve_compaction_row_budget(NonZeroUsize::new(16).expect("nonzero"));
    for revision in 0..20 {
        let mut changes = AttributeChanges {
            remove: keys.clone(),
            ..Default::default()
        };
        changes.set.insert(
            attribute_key("delta"),
            attribute_text(&revision.to_string()),
        );
        namespace_writer
            .update_attributes("/file", &loonfs_test_support::test_actor(), changes)
            .await
            .expect("write delta");
        maintenance.fold_wal(&namespace).await.expect("fold delta");
    }

    let attribute_runs = |manifest: &NamespaceManifestPayload| {
        manifest
            .runs
            .iter()
            .filter(|run| {
                run.segments
                    .iter()
                    .any(|segment| segment.family == MetadataRowFamily::Attributes)
            })
            .count()
    };
    let mut manifest = current_manifest_payload(store.as_ref(), &namespace).await;
    assert_eq!(attribute_runs(&manifest), 21);
    let base_bytes: u64 = manifest
        .runs
        .iter()
        .filter(|run| run.tier == RunTier::Base)
        .flat_map(|run| &run.segments)
        .filter(|segment| segment.family == MetadataRowFamily::Attributes)
        .map(|segment| segment.index_block.offset + u64::from(segment.index_block.stored_bytes))
        .sum();
    assert!(
        base_bytes > 8 * 1024 * 1024,
        "base has {base_bytes} stored bytes"
    );
    let mut base_reads = 0;
    let mut remaining_runs = vec![21];
    let mut completed = false;
    for _ in 0..32 {
        let base_keys: BTreeSet<_> = manifest
            .runs
            .iter()
            .filter(|run| run.tier == RunTier::Base)
            .flat_map(|run| &run.segments)
            .filter(|segment| segment.family == MetadataRowFamily::Attributes)
            .map(|segment| metadata_segment(&segment.owner_namespace_id, &segment.segment_id))
            .collect();
        store.reset();
        let compaction = maintenance
            .compact_metadata(&namespace)
            .await
            .expect("compact one unit")
            .compaction;
        let read_base = store
            .take_get_keys()
            .iter()
            .any(|key| base_keys.contains(key));
        base_reads += usize::from(read_base);
        let next = current_manifest_payload(store.as_ref(), &namespace).await;
        if attribute_runs(&next) != attribute_runs(&manifest) {
            remaining_runs.push(attribute_runs(&next));
            if read_base {
                assert!(
                    matches!(
                        compaction,
                        MetadataCompactionOutcome::Published {
                            rows_read: 340,
                            rows_written: 340,
                            ..
                        }
                    ),
                    "{compaction:?}"
                );
            } else {
                assert_eq!(compaction, MetadataCompactionOutcome::BoundedMergePublished);
            }
            if attribute_runs(&next) == 7 {
                let response = maintenance
                    .maintain_metadata_with_options(
                        &namespace,
                        &crate::MetadataMaintenanceOptions {
                            compaction_policy: MetadataCompactionPolicy::CompactImmediately,
                            ..Default::default()
                        },
                    )
                    .await
                    .expect("plan the base rebuild");
                assert_eq!(
                    response.compaction,
                    CompactionStepOutcome::MetadataCompactionRequired {}
                );
            }
        }
        manifest = next;
        if compaction == MetadataCompactionOutcome::NotNeeded {
            completed = true;
            break;
        }
    }
    assert!(completed, "compaction must finish");
    assert_eq!(remaining_runs, [21, 14, 7, 1]);
    assert_eq!(base_reads, 1);
    assert!(manifest.runs.iter().all(|run| run.tier == RunTier::Base));
    writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn maintenance_clones_share_one_claim_and_never_reclaim_after_fencing() {
    use loonfs_test_support::stores::{KeyPredicate, RecordingStore};
    let directory = tempdir().expect("tempdir");
    let namespace = namespace_id("shared-claim");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::prefix(loonfs_objectstore::keys::metadata_manifest_prefix(
            &namespace,
        )),
    ));
    let shared: SharedObjectStore = store.clone();
    let writer = LoonFs::builder_with_store(shared.clone())
        .writer_id("writer")
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace, &loonfs_test_support::test_actor())
        .await
        .expect("namespace");
    let maintenance = LoonFs::builder_with_store(shared.clone())
        .writer_id("maintenance")
        .build()
        .await
        .expect("maintenance")
        .maintenance(loonfs_test_support::ids::writer_id("maintenance"));
    maintenance
        .create_checkpoint(&namespace, "basis")
        .await
        .expect("checkpoint");
    let before = current_manifest_payload(store.as_ref(), &namespace).await;
    store.reset();
    assert!(matches!(
        maintenance
            .compact_once(&namespace, MetadataCompactionPolicy::SizeTiered, None)
            .await
            .expect("no work"),
        super::CompactionStep::Concluded(CompactionStepOutcome::NotNeeded {})
    ));
    assert_eq!(store.counts().puts, 0);
    let cloned = maintenance.clone();
    let (first, second) = tokio::join!(
        maintenance.compactor_epoch(&namespace),
        cloned.compactor_epoch(&namespace)
    );
    let epoch = first.expect("first claim");
    assert_eq!(second.expect("shared claim"), epoch);
    assert_eq!(store.counts().create_if_absent_puts, 1);
    let mut expected = before.clone();
    expected.manifest_no = before.manifest_no.successor().expect("next manifest");
    expected.compactor_epoch = loonfs_types::CompactorEpoch(before.compactor_epoch.0 + 1);
    assert_eq!(
        current_manifest_payload(store.as_ref(), &namespace).await,
        expected
    );
    let other = LoonFs::builder_with_store(shared)
        .writer_id("other")
        .build()
        .await
        .expect("other process")
        .maintenance(loonfs_test_support::ids::writer_id("other"));
    assert_eq!(
        other.compactor_epoch(&namespace).await.expect("new claim"),
        loonfs_types::CompactorEpoch(epoch.0 + 1)
    );
    store.reset();
    assert_eq!(
        maintenance
            .compactor_epoch(&namespace)
            .await
            .expect("remembered claim"),
        epoch
    );
    assert_eq!(
        maintenance
            .run_compaction_step(&namespace, MetadataCompactionPolicy::SizeTiered, None)
            .await
            .expect("fenced compaction"),
        CompactionStepOutcome::Fenced {}
    );
    assert_eq!(store.counts().puts, 0);
}

/// Lets a rival write one commit and fold it before each manifest write it
/// sees while it has races left, so the compaction step that reaches its
/// publication loses that race.
#[derive(Debug)]
struct RivalFoldStore {
    inner: LocalFsStore,
    namespace_id: NamespaceId,
    races_left: std::sync::atomic::AtomicUsize,
    races_run: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl ObjectStore for RivalFoldStore {
    loonfs_test_support::delegate_object_store!(self => self.inner; except put);

    // `fetch_update` is deprecated since Rust 1.99 for `try_update`, which the 1.88 MSRV lacks.
    #[allow(deprecated)]
    async fn put(
        &self,
        key: &str,
        bytes: bytes::Bytes,
        mode: loonfs_objectstore::PutMode,
    ) -> Result<loonfs_objectstore::ObjectMetadata, loonfs_objectstore::ObjectStoreError> {
        use std::sync::atomic::Ordering::SeqCst;
        if loonfs_objectstore::layout::manifest_no_of(key).is_some()
            && self
                .races_left
                .fetch_update(SeqCst, SeqCst, |left| left.checked_sub(1))
                .is_ok()
        {
            self.races_run.fetch_add(1, SeqCst);
            loonfs_core::test_support::append_wal_objects(
                &self.inner,
                &self.namespace_id,
                1,
                &loonfs_core::MutationContext {
                    writer_id: loonfs_test_support::ids::writer_id("rival"),
                    now_ms: 1_000,
                },
            )
            .await
            .expect("the rival writes");
            loonfs_core::fold_wal_tail(
                &self.inner,
                None,
                &self.namespace_id,
                None,
                &loonfs_core::time::Deadline::start(Arc::new(
                    loonfs_types::StdMonotonicTimer::default(),
                )),
                &tokio::sync::Semaphore::new(32 * 1024 * 1024),
            )
            .await
            .expect("the rival folds");
        }
        self.inner.put(key, bytes, mode).await
    }
}

#[tokio::test]
async fn the_metadata_loop_stops_after_three_lost_races_and_on_a_fenced_step() {
    use std::sync::atomic::Ordering::SeqCst;
    let temp_dir = tempdir().expect("tempdir");
    let namespace = namespace_id("losing-loop");
    let store = Arc::new(RivalFoldStore {
        inner: LocalFsStore::new(temp_dir.path()).expect("store"),
        namespace_id: namespace.clone(),
        races_left: 0.into(),
        races_run: 0.into(),
    });
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id("writer")
        .build()
        .await
        .expect("writer");
    let maintenance = writer.maintenance(loonfs_test_support::ids::writer_id("maintenance"));
    writer
        .create_namespace(&namespace, &loonfs_test_support::test_actor())
        .await
        .expect("create the namespace");
    for index in 0..loonfs_types::format::sst_blocks::DEFAULT_MAX_DELTA_RUNS + 2 {
        write_and_fold(&writer, &maintenance, &namespace, &format!("/file-{index}")).await;
    }
    maintenance
        .compactor_epoch(&namespace)
        .await
        .expect("claim the namespace before the races start");

    store.races_left.store(5, SeqCst);
    assert!(!maintenance
        .maintain_metadata_while_due(&namespace, &crate::MaintenanceCancellation::new())
        .await
        .expect("a lost race is an outcome, not an error"));
    assert_eq!(
        store.races_run.load(SeqCst),
        3,
        "the loop gives up after three lost races in a row"
    );

    store.races_left.store(0, SeqCst);
    LoonFs::builder_with_store(Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")))
        .writer_id("other")
        .build()
        .await
        .expect("another process")
        .maintenance(loonfs_test_support::ids::writer_id("other"))
        .compactor_epoch(&namespace)
        .await
        .expect("another process claims the namespace");
    let fenced = current_manifest_payload(store.as_ref(), &namespace).await;
    assert!(!maintenance
        .maintain_metadata_while_due(&namespace, &crate::MaintenanceCancellation::new())
        .await
        .expect("a fenced step is an outcome, not an error"));
    assert_eq!(
        current_manifest_payload(store.as_ref(), &namespace).await,
        fenced,
        "a fenced loop stops instead of claiming the namespace back"
    );
}

#[tokio::test]
async fn a_metadata_loop_call_stops_after_its_unit_cap() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store"));
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id("writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("writer");
    let maintenance = writer.maintenance(loonfs_test_support::ids::writer_id("maintenance"));
    let namespace = namespace_id("backlog");
    writer
        .create_namespace(&namespace, &loonfs_test_support::test_actor())
        .await
        .expect("create the namespace");
    // One unit merges at most eight runs of one family group, and each
    // folded write adds a run to several groups.
    for index in 0..3 * loonfs_types::format::sst_blocks::DEFAULT_MAX_DELTA_RUNS {
        write_and_fold(&writer, &maintenance, &namespace, &format!("/file-{index}")).await;
    }
    maintenance
        .compactor_epoch(&namespace)
        .await
        .expect("claim the namespace first, so each later manifest is one unit");

    let mut published = Vec::new();
    for _ in 0..2 {
        let before = current_manifest_payload(store.as_ref(), &namespace)
            .await
            .manifest_no;
        let caught_up = maintenance
            .maintain_metadata_while_due(&namespace, &crate::MaintenanceCancellation::new())
            .await
            .expect("run the metadata loop");
        if published.is_empty() {
            assert!(!caught_up, "the unit cap cannot establish completion");
        }
        let after = current_manifest_payload(store.as_ref(), &namespace)
            .await
            .manifest_no;
        published.push(after.0 - before.0);
    }
    assert_eq!(
        published[0],
        u64::from(super::MAX_COMPACTION_UNITS_PER_CALL),
        "one call publishes exactly the cap"
    );
    assert!(
        published[1] > 0,
        "a second call continues where the first stopped"
    );
    writer.shutdown().await.expect("shutdown");
}

/// A wall clock a week ahead, so a collection pass finds everything the test
/// wrote past every grace window.
#[derive(Debug)]
struct WeekAheadClock(u64);

impl crate::WallClock for WeekAheadClock {
    fn now_ms(&self) -> std::result::Result<u64, crate::CoreError> {
        Ok(self.0)
    }
}

#[tokio::test]
async fn an_idle_namespace_visit_costs_a_fixed_number_of_requests() {
    use loonfs_core::time::WallClock;
    use loonfs_test_support::stores::{KeyPredicate, RecordingStore, StoreCounts};

    /// GET, HEAD, and LIST requests, then every write.
    fn requests(counts: StoreCounts) -> [usize; 4] {
        [
            counts.gets + counts.gets_with_metadata,
            counts.heads,
            counts.lists,
            counts.puts + counts.deletes,
        ]
    }

    let directory = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let actor = loonfs_test_support::test_actor();
    let [idle, claimed, retired, missing] =
        ["idle", "claimed", "retired", "missing"].map(namespace_id);
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id("writer")
        .build()
        .await
        .expect("writer");
    for namespace in [&idle, &claimed, &retired] {
        writer
            .create_namespace(namespace, &actor)
            .await
            .expect("create the namespace");
        writer
            .open_namespace(namespace)
            .expect("open the namespace")
            .put_file("/file", b"body", &actor)
            .await
            .expect("put a file");
    }
    writer
        .open_namespace(&retired)
        .expect("open the namespace")
        .delete()
        .await
        .expect("delete the namespace");
    writer.shutdown().await.expect("shut down the writer");

    let week_ahead_ms = loonfs_core::time::SystemWallClock
        .now_ms()
        .expect("system clock")
        + 7 * 24 * 60 * 60 * 1000;
    let maintenance = LoonFs::builder_with_store(store.clone())
        .writer_id("maintenance")
        .wall_clock(Arc::new(WeekAheadClock(week_ahead_ms)))
        .build()
        .await
        .expect("maintenance")
        .maintenance(loonfs_test_support::ids::writer_id("maintenance"));
    for namespace in [&idle, &claimed] {
        maintenance
            .fold_wal(namespace)
            .await
            .expect("fold the tail");
    }
    maintenance
        .compactor_epoch(&claimed)
        .await
        .expect("claim the namespace");
    for namespace in [&idle, &claimed, &retired] {
        maintenance
            .gc(namespace)
            .await
            .expect("collect what the setup left behind");
    }

    store.reset();
    let cancelled = crate::MaintenanceCancellation::new();
    cancelled.cancel();
    assert!(!maintenance
        .maintain_metadata_while_due(&idle, &cancelled)
        .await
        .expect("cancellation is not completion"));
    assert_eq!(requests(store.counts()), [0; 4]);

    for (namespace, metadata_requests, metadata_error, gc_requests, gc_error) in [
        (&idle, [5, 1, 0, 0], None, [5, 1, 10, 0], None),
        (&claimed, [5, 1, 0, 0], None, [5, 1, 10, 0], None),
        (
            &retired,
            [2, 0, 0, 0],
            Some(crate::ErrorCode::NamespaceDeleted),
            [2, 0, 9, 0],
            None,
        ),
        (
            &missing,
            [1, 0, 0, 0],
            Some(crate::ErrorCode::NamespaceNotFound),
            [1, 0, 0, 0],
            Some(crate::ErrorCode::NamespaceNotFound),
        ),
    ] {
        store.reset();
        let metadata = maintenance
            .maintain_metadata_while_due(namespace, &crate::MaintenanceCancellation::new())
            .await;
        if metadata_error.is_none() {
            assert!(metadata.as_ref().expect("idle metadata is caught up"));
        }
        assert_eq!(metadata.err().map(|error| error.code()), metadata_error);
        assert_eq!(
            requests(store.counts()),
            metadata_requests,
            "the metadata loop on `{namespace}`"
        );
        store.reset();
        let gc = maintenance.gc(namespace).await;
        assert_eq!(
            requests(store.counts()),
            gc_requests,
            "collection of `{namespace}`"
        );
        assert_eq!(gc.as_ref().err().map(|error| error.code()), gc_error);
        if let Ok(report) = gc {
            assert_eq!(report.deleted, loonfs_types::DeletedObjectCounts::default());
        }
    }

    LoonFs::builder_with_store(store.clone())
        .writer_id("other")
        .build()
        .await
        .expect("another process")
        .maintenance(loonfs_test_support::ids::writer_id("other"))
        .compactor_epoch(&claimed)
        .await
        .expect("another process claims the namespace");
    store.reset();
    let fenced = maintenance
        .maintain_metadata(&claimed)
        .await
        .expect("a fenced step is an outcome, not an error");
    assert_eq!(fenced.compaction, CompactionStepOutcome::Fenced {});
    assert_eq!(
        requests(store.counts()),
        [5, 1, 0, 0],
        "the observed anchor already reports the fence"
    );
}

#[tokio::test]
async fn a_repeated_retirement_pass_lists_once_and_deletes_nothing() {
    use loonfs_core::limits::{GC_SAFETY_MARGIN_MS, NAMESPACE_RETIREMENT_GRACE_MS};
    use loonfs_test_support::stores::{KeyPredicate, RecordingStore, StoreCounts};

    let directory = tempdir().expect("directory");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::prefix("namespaces/retired/content/"),
    ));
    let namespace_id = namespace_id("retired");
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id("retirement-test")
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("namespace");
    for _ in 0..3 {
        let key = loonfs_objectstore::keys::content_blob(
            &namespace_id,
            &loonfs_types::ContentId::generate(),
        );
        store
            .put_if_absent(&key, bytes::Bytes::from_static(b"content"))
            .await
            .expect("content");
    }
    let mut context = loonfs_core::MutationContext {
        writer_id: loonfs_types::WriterId::parse("deleter").expect("writer id"),
        now_ms: 1_000,
    };
    loonfs_core::publish::NamespaceCommitEngine::new(
        namespace_id.clone(),
        std::sync::Arc::new(tokio::sync::Semaphore::new(32 * 1024 * 1024)),
    )
    .delete_namespace(store.as_ref(), Default::default(), &context)
    .await
    .expect("delete");
    context.now_ms += crate::GcOptions::default()
        .grace_window_ms
        .max(NAMESPACE_RETIREMENT_GRACE_MS)
        + GC_SAFETY_MARGIN_MS;
    store.reset();
    let first = loonfs_core::gc_namespace(
        store.as_ref(),
        None,
        &namespace_id,
        &crate::GcOptions::default(),
        &context,
    )
    .await
    .expect("first pass");
    assert_eq!(first.deleted.retired_content_objects, 3);
    assert_eq!(
        store.counts(),
        StoreCounts {
            lists: 1,
            deletes: 3,
            ..Default::default()
        }
    );
    store.reset();
    let repeated = loonfs_core::gc_namespace(
        store.as_ref(),
        None,
        &namespace_id,
        &crate::GcOptions::default(),
        &context,
    )
    .await
    .expect("repeat pass");
    assert_eq!(
        repeated.deleted,
        loonfs_types::DeletedObjectCounts::default()
    );
    assert_eq!(
        store.counts(),
        StoreCounts {
            lists: 1,
            ..Default::default()
        }
    );
    writer.shutdown().await.expect("shutdown");
}
