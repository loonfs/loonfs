//! The frozen-base policy over a live runtime: what an explicit compaction
//! does that a writer's own upkeep does not.
//!
//! These tests need a family group whose base run no bounded step can fold,
//! with delta runs still arriving above it. The shipped row budget only
//! reaches that state at a scale no test can write, so the budget is narrowed
//! through [`FsAdmin::starve_reorganization_row_budget`]. Everything else —
//! planning, admission, the executor, the finalizer — is the shipped path.

use crate::metrics::{DefaultMetricsRecorder, MetricValue, MetricsSnapshot};
use crate::{
    CreateCheckpointOptions, CreateNamespaceOptions, FsAdmin, FsBackgroundWork, FsWriter, GcConfig,
    MaintenancePlan, MetadataCompactionOutcome, MetadataMaintenanceOptions, MoveOptions,
    NamespaceId, PutFileOptions, ReorganizeStepOutcome, SharedObjectStore,
};
use loonfs_api::wire::manifest::{decode_namespace_manifest_json, MetadataRowFamily};
use loonfs_core::MetadataFamilyGroup;
use loonfs_objectstore::keys::metadata_manifest_object;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::ObjectStore;
use loonfs_test_support::ids::namespace_id;
use std::collections::BTreeSet;
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;
use tempfile::tempdir;

/// The level a bottom-anchored merge writes its output at, which is where a
/// rebuilt group's one run stands.
const BASE_RUN_LEVEL: u32 = 1;

/// The group these tests watch, because it is the one the planner selects:
/// selection takes the group holding the most delta rows, and every write and
/// every rename puts more rows there than anywhere else. Manifest
/// descriptors are per family, so its families are what they are filtered by.
const BINDINGS: MetadataFamilyGroup = MetadataFamilyGroup::Bindings;

/// A `ManualOnly` deployment: a writer that schedules nothing, and an admin
/// handle with no background work behind it, over one store.
///
/// This is the shape the explicit compaction path exists for. The second
/// admin is the contrast — the same store, but attached to the writer's
/// runner, so its steps plan under the amortizing policy a writer's own
/// upkeep runs under.
async fn manual_deployment(
    root: &std::path::Path,
) -> (FsWriter, FsAdmin, FsAdmin, Arc<DefaultMetricsRecorder>) {
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(root).expect("create local-fs store"));
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let writer = FsWriter::builder_with_store(Arc::clone(&store))
        .writer_id("manual-writer")
        .background_work(FsBackgroundWork::ManualOnly)
        .build()
        .await
        .expect("build the writer");
    let standalone = FsAdmin::builder_with_store(Arc::clone(&store))
        .actor_id("standalone-admin")
        .metrics_recorder(recorder.clone())
        .build()
        .await
        .expect("build the standalone admin");
    let scheduled = FsAdmin::builder_with_store(store)
        .actor_id("scheduled-admin")
        .background_maintenance(&writer)
        .build()
        .await
        .expect("build the writer-backed admin");
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

fn metadata_plan() -> MaintenancePlan {
    MaintenancePlan {
        metadata: Some(MetadataMaintenanceOptions {
            max_wal_tail_segments: NonZeroU64::MIN,
        }),
        ..MaintenancePlan::default()
    }
}

#[tokio::test]
async fn an_admin_gc_step_records_the_pass_counters_once() {
    let temp_dir = tempdir().expect("tempdir");
    let (writer, admin, _scheduled, recorder) = manual_deployment(temp_dir.path()).await;
    let namespace = namespace_id("admin-gc-metrics");
    writer
        .create_namespace(&namespace, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    writer
        .put_file_bytes(
            &namespace,
            "/live.txt",
            b"live",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("write a live GC candidate");

    assert_eq!(counter(&recorder.snapshot(), "loonfs.gc.retained", &[]), 0);
    let step = admin
        .run_maintenance(
            &namespace,
            MaintenancePlan {
                gc: Some(GcConfig::default()),
                ..MaintenancePlan::default()
            },
        )
        .await
        .expect("run the admin GC step");
    let gc = step.gc.expect("a GC plan reports its pass");
    assert!(
        gc.retained_candidates > 0,
        "the live namespace gives the pass candidates to retain"
    );

    let snapshot = recorder.snapshot();
    assert_eq!(
        counter(&snapshot, "loonfs.gc.retained", &[]),
        gc.retained_candidates,
        "the admin pass records its retained count exactly once"
    );
    for (category, reclaimed) in [
        ("deleted_wal_segments", gc.deleted.wal_segments),
        ("deleted_metadata_segments", gc.deleted.metadata_segments),
        ("deleted_manifests", gc.deleted.manifests),
        ("deleted_checkpoint_records", gc.deleted.checkpoint_records),
        ("released_fork_checkpoints", gc.released_checkpoints.fork),
        (
            "released_expired_checkpoints",
            gc.released_checkpoints.expired,
        ),
        ("deleted_upload_sessions", gc.deleted.upload_sessions),
        ("deleted_content_objects", gc.deleted.content_objects),
        (
            "released_missing_basis_checkpoints",
            gc.released_checkpoints.missing_basis,
        ),
    ] {
        assert_eq!(
            counter(&snapshot, "loonfs.gc.reclaimed", &[("category", category)],),
            reclaimed,
            "the admin pass records `{category}` exactly once"
        );
    }
}

/// Writes one file and folds the tail, so each call leaves one more delta run.
async fn write_and_flush(
    writer: &FsWriter,
    admin: &FsAdmin,
    namespace_id: &NamespaceId,
    path: &str,
) {
    writer
        .put_file_bytes(
            namespace_id,
            path,
            path.as_bytes(),
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("put a file");
    admin.flush_wal(namespace_id).await.expect("fold the tail");
}

/// Builds a namespace whose bindings group holds one base run of real size,
/// with retention-eligible churn inside it, and then leaves delta runs piling
/// up above that base.
///
/// The order matters. The churn is folded into the base while the retention
/// floor is still at the bottom, so nothing drops on the way in and the rows a
/// rebuild may drop are in the base when the rebuild reads it. The floor is
/// advanced past that churn next, and the delta runs come last, because they
/// are what makes a delta-only merge available at the moment the tests ask for
/// a compaction.
async fn namespace_with_a_frozen_base(
    writer: &FsWriter,
    admin: &FsAdmin,
    namespace_id: &NamespaceId,
) {
    writer
        .create_namespace(namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create the namespace");
    for index in 0..24 {
        write_and_flush(
            writer,
            admin,
            namespace_id,
            &format!("/docs/file-{index}.txt"),
        )
        .await;
    }
    // Churn: every rename retires one binding and creates another, and a
    // bottom-anchored rebuild below the floor drops the retired pair.
    for index in 0..12 {
        writer
            .move_path(
                namespace_id,
                &format!("/docs/file-{index}.txt"),
                &format!("/docs/moved-{index}.txt"),
                MoveOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect("rename a file");
    }
    admin.flush_wal(namespace_id).await.expect("fold the tail");

    // Fold everything into one base run per group, under the shipped budgets
    // and with the floor still at the bottom, so the churn lands in the base.
    for _ in 0..64 {
        let response = admin.flush_wal(namespace_id).await.expect("fold a unit");
        if response.reorganize == ReorganizeStepOutcome::NotNeeded {
            break;
        }
    }

    // Now the floor moves past that churn, so the next bottom-anchored rebuild
    // is the one that may drop it.
    admin
        .create_checkpoint(
            namespace_id,
            CreateCheckpointOptions {
                name: "retention".to_owned(),
                ttl_ms: None,
            },
        )
        .await
        .expect("checkpoint the namespace");
    admin
        .advance_retention_floor(namespace_id)
        .await
        .expect("advance the retention floor past the churn");
}

/// Keeps writing while the group's base is frozen, so a delta-only merge is
/// always available above it.
///
/// The admin here is the starved one, which is what makes these runs pile up:
/// its steps can no longer fold the group they land in, so each write leaves
/// one more delta run behind rather than being merged into the base.
async fn sustained_writes(writer: &FsWriter, admin: &FsAdmin, namespace_id: &NamespaceId) {
    for index in 0..10 {
        write_and_flush(
            writer,
            admin,
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
        .filter(|run| run.level == BASE_RUN_LEVEL && BINDINGS.families().contains(&run.family))
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
    level: u32,
    family: MetadataRowFamily,
    rows: u64,
}

async fn manifest_runs<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Vec<ManifestRun> {
    let root = loonfs_core::control::load_namespace_metadata_root_control(store, namespace_id)
        .await
        .expect("read the metadata root");
    let key = metadata_manifest_object(namespace_id, &root.state.manifest.manifest_object_id);
    let bytes = store
        .get(&key, None)
        .await
        .expect("read the manifest")
        .expect("the manifest exists");
    decode_namespace_manifest_json(&bytes)
        .expect("decode the manifest")
        .payload
        .segments
        .iter()
        .map(|descriptor| ManifestRun {
            run_seq: descriptor.run_seq.0,
            level: descriptor.level,
            family: descriptor.family,
            rows: descriptor.row_count,
        })
        .collect()
}

/// The runs the bindings group holds right now, and the rows in them.
async fn bindings_runs<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> (BTreeSet<(u64, u32)>, u64) {
    manifest_runs(store, namespace_id)
        .await
        .into_iter()
        .filter(|run| BINDINGS.families().contains(&run.family))
        .fold((BTreeSet::new(), 0), |(mut runs, rows), run| {
            runs.insert((run.run_seq, run.level));
            (runs, rows + run.rows)
        })
}

#[tokio::test]
async fn explicit_compaction_runs_the_job_while_a_delta_merge_is_still_available() {
    let temp_dir = tempdir().expect("tempdir");
    let (writer, standalone, scheduled, recorder) = manual_deployment(temp_dir.path()).await;
    let explicit = namespace_id("explicit");
    let automatic = namespace_id("automatic");
    for namespace in [&explicit, &automatic] {
        namespace_with_a_frozen_base(&writer, &standalone, namespace).await;
    }
    let store = LocalFsStore::new(temp_dir.path()).expect("create local-fs store");
    let budget = budget_that_starves_the_bindings_base(&store, &explicit).await;
    let standalone = standalone.starve_reorganization_row_budget(budget);
    let scheduled = scheduled.starve_reorganization_row_budget(budget);
    for namespace in [&explicit, &automatic] {
        sustained_writes(&writer, &standalone, namespace).await;
    }

    // A writer's own step, under the same budget and the same namespace
    // shape, publishes the delta merge. That is what proves the merge was
    // there to take.
    let step = scheduled
        .run_maintenance(&automatic, metadata_plan())
        .await
        .expect("run a writer-scheduled step");
    assert_eq!(
        step.metadata_maintenance
            .expect("a metadata plan reports its upkeep")
            .reorganize,
        ReorganizeStepOutcome::UnitPublished,
        "an amortizing planner takes the delta merge above the frozen base"
    );

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
            outcome,
            MetadataCompactionOutcome::Ran(
                loonfs_core::MetadataCompactionJobOutcome::Published { .. }
            )
        ),
        "the explicit call must run and publish the job rather than a delta merge, got {outcome:?}"
    );

    let (runs_after, rows_after) = bindings_runs(&store, &explicit).await;
    assert_eq!(
        runs_after.len(),
        1,
        "the rebuilt group is left with one run, got {runs_after:?}"
    );
    assert_eq!(
        runs_after.iter().next().expect("the group holds one run").1,
        BASE_RUN_LEVEL,
        "and that run is the group's base"
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
async fn a_standalone_step_reports_the_compaction_the_explicit_call_runs() {
    let temp_dir = tempdir().expect("tempdir");
    let (writer, standalone, _scheduled, _recorder) = manual_deployment(temp_dir.path()).await;
    let namespace = namespace_id("manual");
    namespace_with_a_frozen_base(&writer, &standalone, &namespace).await;
    let store = LocalFsStore::new(temp_dir.path()).expect("create local-fs store");
    let budget = budget_that_starves_the_bindings_base(&store, &namespace).await;
    let standalone = standalone.starve_reorganization_row_budget(budget);
    sustained_writes(&writer, &standalone, &namespace).await;

    let step = standalone
        .run_maintenance(&namespace, metadata_plan())
        .await
        .expect("run a standalone step");
    assert_eq!(
        step.metadata_maintenance
            .expect("a metadata plan reports its upkeep")
            .reorganize,
        ReorganizeStepOutcome::CompactionRequired,
        "a step with nowhere to run a job says the namespace needs one"
    );

    let outcome = standalone
        .compact_metadata(&namespace)
        .await
        .expect("run the explicit compaction");
    assert!(
        matches!(
            outcome,
            MetadataCompactionOutcome::Ran(
                loonfs_core::MetadataCompactionJobOutcome::Published { .. }
            )
        ),
        "and the explicit call runs it, got {outcome:?}"
    );
}
