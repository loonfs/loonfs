//! The frozen-base policy over a live runtime: `metadata` passes within the
//! row budget and explicit compaction.
//!
//! These tests need a family group whose window is too large for a `metadata`
//! pass, with delta runs still arriving above its base. The shipped row budget
//! only reaches that state at a scale no test can write, so the budget is
//! narrowed through [`Maintenance::starve_compaction_row_budget`]. Everything
//! else (planning, admission, the merge, the finalization) is the shipped
//! path.

use crate::metrics::{DefaultMetricsRecorder, MetricValue, MetricsSnapshot};
use crate::test_support::maintenance_past_the_grace_window;
use crate::{
    LoonFs, MaintainMetadataOptions, Maintenance, MetadataCompactionOutcome,
    MetadataCompactionPolicy, NamespaceId, SharedObjectStore, Writable,
};
use loonfs_core::MetadataFamilyGroup;
use loonfs_objectstore::keys::metadata_manifest_object;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::ObjectStore;
use loonfs_objectstore::ObjectStoreExtension;
use loonfs_test_support::clock::ManualClock;
use loonfs_test_support::ids::namespace_id;
use loonfs_test_support::stores::{
    Intercept, InterceptStore, Interceptor, OperationContext, OperationSelector,
};
use loonfs_types::format::manifest::{
    decode_namespace_manifest_json, MetadataRowFamily, NamespaceManifestPayload, RunTier,
};
use std::assert_matches;
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
        .writer_id(loonfs_test_support::ids::writer_id("manual-writer"))
        .build()
        .await
        .expect("build the writer");
    let standalone = LoonFs::builder_with_store(Arc::clone(&store))
        .writer_id(loonfs_test_support::ids::writer_id(
            "standalone-maintenance",
        ))
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
    assert_matches!(
        entry.value,
        MetricValue::Counter(_),
        "expected a counter, found {:?}",
        entry.value
    );
    let MetricValue::Counter(value) = entry.value else {
        return 0;
    };
    value
}

fn metadata_options(compaction_policy: MetadataCompactionPolicy) -> MaintainMetadataOptions {
    MaintainMetadataOptions {
        max_wal_tail_objects: NonZeroU64::MIN,
        compaction_policy,
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
    assert_eq!(snapshot.by_name("loonfs.gc.reclaimed").count(), 9);
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
    // The base stays within four times the size of one delta run of
    // `sustained_writes`, so a window over the base and those runs is one
    // size tier.
    for index in 0..16 {
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
    for index in 0..8 {
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
        if response.compaction == MetadataCompactionOutcome::NotNeeded {
            break;
        }
    }

    // Now the floor moves past that churn, so the next bottom-anchored rebuild
    // is the one that may drop it.
    namespace
        .create_snapshot("retention")
        .await
        .expect("snapshot the namespace");
    maintenance_past_the_grace_window(writer.object_store())
        .await
        .advance_retention_floor(namespace_id)
        .await
        .expect("advance the retention floor past the churn");
}

/// Keeps writing while the group's base is frozen, so a delta-only merge is
/// always available above it. Each write is folded with no compaction unit,
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
    let root = loonfs_core::test_support::namespace_store(store, namespace_id)
        .load_current_manifest()
        .await
        .expect("read the metadata root");
    let key = metadata_manifest_object(namespace_id, &root.manifest().manifest_no);
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
        MetadataCompactionOutcome::MetadataCompactionRequired
    );

    writer.shutdown().await.expect("shut down the first writer");
    let fresh_store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("create local-fs store"));
    let fresh_writer = LoonFs::builder_with_store(Arc::clone(&fresh_store))
        .writer_id(loonfs_test_support::ids::writer_id("fresh-writer"))
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
        MetadataCompactionOutcome::MetadataCompactionRequired,
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

    let completed_before = counter(
        &recorder.snapshot(),
        "loonfs.maintenance.compactions",
        &[("outcome", "completed")],
    );
    let outcome = standalone
        .compact_metadata(&explicit, &crate::MaintenanceCancellation::new())
        .await
        .expect("run the explicit compaction");
    assert_matches!(
        outcome.compaction,
        MetadataCompactionOutcome::Published { .. },
        "the explicit call must merge the whole window rather than a delta window, got \
         {outcome:?}"
    );

    let (runs_after, rows_after) = bindings_runs(&store, &explicit).await;
    assert_eq!(
        runs_after.len(),
        runs_before.len() - 8 + 1,
        "one unit replaces eight inputs and preserves the rest, got {runs_after:?}"
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
        completed_before + 1,
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
            &crate::MaintainMetadataOptions {
                max_wal_tail_objects: std::num::NonZeroU64::MIN,
                compaction_policy: MetadataCompactionPolicy::CompactImmediately,
            },
        )
        .await
        .expect("run an immediate step");
    assert_eq!(
        response.compaction,
        MetadataCompactionOutcome::MetadataCompactionRequired,
        "an immediate step says the namespace needs a compaction job"
    );

    let outcome = standalone
        .compact_metadata(&namespace, &crate::MaintenanceCancellation::new())
        .await
        .expect("run the explicit compaction");
    assert_matches!(
        outcome.compaction,
        MetadataCompactionOutcome::Published { .. },
        "and the explicit call runs it, got {outcome:?}"
    );
}

#[tokio::test]
async fn explicit_compaction_merges_twenty_deltas_and_reads_the_large_base_once() {
    use crate::publish::{parse_mutation_path, CommitCandidate, CommitIntent, FilesystemOperation};
    use crate::{AttributeChanges, CommitId};
    use loonfs_objectstore::keys::metadata_segment;
    use loonfs_test_support::ids::{attribute_key, attribute_text};
    use loonfs_test_support::stores::RecordingStore;

    let directory = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::metadata_segments(
        LocalFsStore::new(directory.path()).expect("store"),
    ));
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("writer"))
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
                                loonfs_types::sha256_hex(
                                    format!("{revision}/{index}/{part}").as_bytes(),
                                )
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
            .commit_candidate(CommitCandidate::new(CommitIntent {
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
            .compact_metadata(&namespace, &crate::MaintenanceCancellation::new())
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
                assert_matches!(
                    compaction,
                    MetadataCompactionOutcome::Published {
                        rows_read: 340,
                        rows_written: 340,
                        ..
                    }
                );
            } else {
                assert_matches!(compaction, MetadataCompactionOutcome::Published { .. });
            }
            if attribute_runs(&next) == 7 {
                let response = maintenance
                    .maintain_metadata_with_options(
                        &namespace,
                        &crate::MaintainMetadataOptions {
                            compaction_policy: MetadataCompactionPolicy::CompactImmediately,
                            ..Default::default()
                        },
                    )
                    .await
                    .expect("plan the base rebuild");
                assert_eq!(
                    response.compaction,
                    MetadataCompactionOutcome::MetadataCompactionRequired
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
        .writer_id(loonfs_test_support::ids::writer_id("writer"))
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace, &loonfs_test_support::test_actor())
        .await
        .expect("namespace");
    let maintenance_runtime = LoonFs::builder_with_store(shared.clone())
        .writer_id(loonfs_test_support::ids::writer_id("maintenance"))
        .build()
        .await
        .expect("maintenance");
    let maintenance =
        maintenance_runtime.maintenance(loonfs_test_support::ids::writer_id("maintenance"));
    maintenance_runtime
        .open_namespace(&namespace)
        .expect("open namespace")
        .create_snapshot("basis")
        .await
        .expect("snapshot");
    let before = current_manifest_payload(store.as_ref(), &namespace).await;
    store.reset();
    assert_eq!(
        maintenance
            .compactor
            .compact_once(
                &namespace,
                MetadataCompactionPolicy::SizeTiered,
                loonfs_core::CompactionAdmission::WithinRowBudget,
                None,
                &loonfs_core::MetadataCompactionCancellation::default(),
            )
            .await
            .expect("no work"),
        MetadataCompactionOutcome::NotNeeded
    );
    assert_eq!(store.counts().puts, 0);
    let cloned = maintenance.clone();
    let (first, second) = tokio::join!(
        maintenance.compactor.compactor_epoch(&namespace),
        cloned.compactor.compactor_epoch(&namespace)
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
        .writer_id(loonfs_test_support::ids::writer_id("other"))
        .build()
        .await
        .expect("other process")
        .maintenance(loonfs_test_support::ids::writer_id("other"));
    assert_eq!(
        other
            .compactor
            .compactor_epoch(&namespace)
            .await
            .expect("new claim"),
        loonfs_types::CompactorEpoch(epoch.0 + 1)
    );
    store.reset();
    assert_eq!(
        maintenance
            .compactor
            .compactor_epoch(&namespace)
            .await
            .expect("remembered claim"),
        epoch
    );
    assert_eq!(
        maintenance
            .compactor
            .compact_once(
                &namespace,
                MetadataCompactionPolicy::SizeTiered,
                loonfs_core::CompactionAdmission::WithinRowBudget,
                None,
                &loonfs_core::MetadataCompactionCancellation::default(),
            )
            .await
            .expect("fenced compaction"),
        MetadataCompactionOutcome::Fenced
    );
    assert_eq!(store.counts().puts, 0);
}

/// Lets a rival write one commit and fold it before each manifest write it
/// sees while it has races left, so the compaction unit that reaches its
/// publication loses that race.
#[derive(Debug)]
struct RivalFold {
    rival: LocalFsStore,
    namespace_id: NamespaceId,
    races_left: std::sync::atomic::AtomicUsize,
    races_run: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl Interceptor for RivalFold {
    type Guard = ();

    async fn before(&self, context: &OperationContext<'_>) -> (Intercept, ()) {
        use std::sync::atomic::Ordering::SeqCst;
        if OperationSelector::Put.matches(context.kind())
            && loonfs_objectstore::layout::manifest_no_of(context.key()).is_some()
            && self
                .races_left
                .try_update(SeqCst, SeqCst, |left| left.checked_sub(1))
                .is_ok()
        {
            self.races_run.fetch_add(1, SeqCst);
            loonfs_core::test_support::NamespaceFixture::new(
                &self.rival,
                &self.namespace_id,
                &loonfs_core::test_support::WriterAt {
                    writer_id: loonfs_test_support::ids::writer_id("rival"),
                    now_ms: 1_000,
                },
            )
            .append_wal_objects(1)
            .await
            .expect("the rival writes");
            loonfs_core::test_support::namespace_store(&self.rival, &self.namespace_id)
                .fold_wal_tail(
                    None,
                    &loonfs_core::time::OperationClock::start(
                        0,
                        Arc::new(loonfs_types::StdMonotonicTimer::default()),
                    ),
                )
                .await
                .expect("the rival folds");
        }
        (Intercept::Continue, ())
    }
}

/// Checks that a unit whose put loses to a fold publishes its output over the
/// fold's manifest, that the metadata loop counts only abandoned units as
/// lost races and stops after three, and that a fenced loop stops without
/// claiming the namespace back.
#[tokio::test]
async fn a_unit_retries_over_a_fold_and_the_loop_stops_after_three_abandoned_units() {
    use std::sync::atomic::Ordering::SeqCst;
    let temp_dir = tempdir().expect("tempdir");
    let namespace = namespace_id("losing-loop");
    let store = Arc::new(InterceptStore::with_interceptor(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        RivalFold {
            rival: LocalFsStore::new(temp_dir.path()).expect("store"),
            namespace_id: namespace.clone(),
            races_left: 0.into(),
            races_run: 0.into(),
        },
    ));
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("writer"))
        .build()
        .await
        .expect("writer");
    let maintenance = writer.maintenance(loonfs_test_support::ids::writer_id("maintenance"));
    writer
        .create_namespace(&namespace, &loonfs_test_support::test_actor())
        .await
        .expect("create the namespace");
    for index in 0..loonfs_types::format::segment_blocks::DEFAULT_MAX_DELTA_RUNS.get() + 2 {
        write_and_fold(&writer, &maintenance, &namespace, &format!("/file-{index}")).await;
    }
    maintenance
        .compactor
        .compactor_epoch(&namespace)
        .await
        .expect("claim the namespace before the races start");

    // One race: the unit's put loses to the rival's fold, and the next
    // attempt publishes over the fold's manifest.
    let before = current_manifest_payload(store.as_ref(), &namespace)
        .await
        .manifest_no;
    store.interceptor().races_left.store(1, SeqCst);
    let outcome = maintenance
        .compact_metadata(&namespace, &crate::MaintenanceCancellation::new())
        .await
        .expect("a lost put is retried, not an error");
    let MetadataCompactionOutcome::Published { manifest_no, .. } = outcome.compaction else {
        panic!("a unit that loses one put publishes, got {outcome:?}");
    };
    assert_eq!(store.interceptor().races_run.load(SeqCst), 1);
    // The rival's claim and fold took the numbers after `before`.
    assert!(
        manifest_no.0 > before.0 + 1,
        "the unit publishes over the rival's fold, {manifest_no} after {before}"
    );
    assert_eq!(
        current_manifest_payload(store.as_ref(), &namespace)
            .await
            .manifest_no,
        manifest_no
    );

    // Every put loses: each unit abandons after its four attempts, and the
    // loop stops after three abandoned units in a row.
    store.interceptor().races_run.store(0, SeqCst);
    store.interceptor().races_left.store(64, SeqCst);
    assert_eq!(
        maintenance
            .maintain_metadata_while_due(&namespace, &crate::MaintenanceCancellation::new())
            .await
            .expect("a lost race is an outcome, not an error")
            .outcome,
        crate::MetadataMaintenanceOutcome::Due
    );
    assert_eq!(
        store.interceptor().races_run.load(SeqCst),
        3 * 4,
        "the loop stops after three abandoned units of four attempts each"
    );

    store.interceptor().races_left.store(0, SeqCst);
    LoonFs::builder_with_store(Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")))
        .writer_id(loonfs_test_support::ids::writer_id("other"))
        .build()
        .await
        .expect("another process")
        .maintenance(loonfs_test_support::ids::writer_id("other"))
        .compactor
        .compactor_epoch(&namespace)
        .await
        .expect("another process claims the namespace");
    let fenced = current_manifest_payload(store.as_ref(), &namespace).await;
    assert_eq!(
        maintenance
            .maintain_metadata_while_due(&namespace, &crate::MaintenanceCancellation::new())
            .await
            .expect("a fenced unit is an outcome, not an error")
            .outcome,
        crate::MetadataMaintenanceOutcome::Due
    );
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
        .writer_id(loonfs_test_support::ids::writer_id("writer"))
        .config(crate::RuntimeConfig {
            min_publish_interval_ms: 0,
            ..crate::RuntimeConfig::default()
        })
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
    // folded write adds a run to several groups, so 32 folded writes need at
    // least four units in each of those groups.
    for index in 0..4 * loonfs_types::format::segment_blocks::DEFAULT_MAX_DELTA_RUNS.get() {
        write_and_fold(&writer, &maintenance, &namespace, &format!("/file-{index}")).await;
    }
    maintenance
        .compactor
        .compactor_epoch(&namespace)
        .await
        .expect("claim the namespace first, so each later manifest is one unit");

    let mut published = Vec::new();
    for _ in 0..2 {
        let before = current_manifest_payload(store.as_ref(), &namespace)
            .await
            .manifest_no;
        let response = maintenance
            .maintain_metadata_while_due(&namespace, &crate::MaintenanceCancellation::new())
            .await
            .expect("run the metadata loop");
        if published.is_empty() {
            assert_eq!(
                response.outcome,
                crate::MetadataMaintenanceOutcome::Due,
                "the unit cap cannot establish completion"
            );
        }
        let after = current_manifest_payload(store.as_ref(), &namespace)
            .await
            .manifest_no;
        assert_eq!(
            response.compaction_units,
            after.0 - before.0,
            "the response counts every unit the call published"
        );
        published.push(after.0 - before.0);
    }
    assert_eq!(
        published[0],
        u64::from(super::compactor::MAX_COMPACTION_UNITS_PER_CALL),
        "one call publishes exactly the cap"
    );
    assert!(
        published[1] > 0,
        "a second call continues where the first stopped"
    );
    writer.shutdown().await.expect("shutdown");
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
        .writer_id(loonfs_test_support::ids::writer_id("writer"))
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

    // A week ahead, a collection pass finds everything the test wrote past
    // every grace window.
    let week_ahead_ms = loonfs_core::time::SystemWallClock.now_ms() + 7 * 24 * 60 * 60 * 1000;
    let maintenance = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("maintenance"))
        .wall_clock(Arc::new(ManualClock::new(week_ahead_ms)))
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
        .compactor
        .compactor_epoch(&claimed)
        .await
        .expect("claim the namespace");
    // A runtime skips a hint raise less than a second after its last raise
    // of the same hint, so the test raises each hint to its current manifest
    // before the collection below and before it counts requests.
    for namespace in [&idle, &claimed, &retired] {
        loonfs_core::test_support::namespace_store(store.as_ref(), namespace)
            .raise_hint_to_current_manifest()
            .await;
    }
    for namespace in [&idle, &claimed, &retired] {
        maintenance
            .gc(namespace)
            .await
            .expect("collect what the setup left behind");
    }

    store.reset();
    let cancelled = crate::MaintenanceCancellation::new();
    cancelled.cancel();
    assert_eq!(
        maintenance
            .maintain_metadata_while_due(&idle, &cancelled)
            .await
            .expect("cancellation is not completion")
            .outcome,
        crate::MetadataMaintenanceOutcome::Due
    );
    assert_eq!(requests(store.counts()), [0; 4]);

    // A load of a live namespace sends a HEAD request for the missing
    // manifest after the current one in its discovery, and again in its
    // check of the manifest's successor.
    let heads = 2;
    for (namespace, metadata_requests, metadata_error, gc_requests, gc_error) in [
        (&idle, [4, heads, 0, 0], None, [4, heads, 10, 0], None),
        (&claimed, [4, heads, 0, 0], None, [4, heads, 10, 0], None),
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
            assert_eq!(
                metadata
                    .as_ref()
                    .expect("idle metadata is caught up")
                    .outcome,
                crate::MetadataMaintenanceOutcome::CaughtUp
            );
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

    let other = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("other"))
        .build()
        .await
        .expect("another process");
    other
        .maintenance(loonfs_test_support::ids::writer_id("other"))
        .compactor
        .compactor_epoch(&claimed)
        .await
        .expect("another process claims the namespace");
    store.reset();
    let fenced = maintenance
        .maintain_metadata(&claimed)
        .await
        .expect("a fenced step is an outcome, not an error");
    assert_eq!(fenced.compaction, MetadataCompactionOutcome::Fenced);
    assert_eq!(
        requests(store.counts()),
        [4, heads, 0, 0],
        "the observed anchor already reports the fence"
    );
}

#[tokio::test]
async fn an_advance_that_moves_no_floor_loads_the_manifest_once_and_writes_nothing() {
    use crate::{AdvanceRetentionFloorOptions, ChangeSeq, RetentionTarget};
    use loonfs_test_support::stores::{KeyPredicate, RecordedOperation, RecordingStore};

    let directory = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let actor = loonfs_test_support::test_actor();
    let namespace = namespace_id("idle");
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("writer"))
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace, &actor)
        .await
        .expect("create the namespace");
    for path in ["/a", "/b"] {
        writer
            .open_namespace(&namespace)
            .expect("open the namespace")
            .put_file(path, b"body", &actor)
            .await
            .expect("put a file");
    }
    writer
        .maintenance(loonfs_test_support::ids::writer_id("writer"))
        .fold_wal(&namespace)
        .await
        .expect("fold commits 1 and 2");
    writer.shutdown().await.expect("shut down the writer");

    // Each advance runs in a new runtime, so no cache has the segments that
    // the fold wrote.
    let maintenance = || maintenance_past_the_grace_window(store.clone());
    for (floor, advances) in [
        (
            ChangeSeq(1),
            [
                (RetentionTarget::Seq(ChangeSeq(0)), 3),
                (RetentionTarget::Seq(ChangeSeq(1)), 3),
                // The scan reads commit 2 from the commits segment, and
                // commit 2 is later than the cutoff.
                (RetentionTarget::Cutoff { cutoff_at_ms: 0 }, 4),
            ],
        ),
        (
            ChangeSeq(2),
            [
                (RetentionTarget::Head, 3),
                (RetentionTarget::Seq(ChangeSeq(u64::MAX)), 3),
                // No folded commit is above the head, so the scan reads no
                // segment.
                (
                    RetentionTarget::Cutoff {
                        cutoff_at_ms: u64::MAX,
                    },
                    3,
                ),
            ],
        ),
    ] {
        let advancing = maintenance().await;
        advancing
            .advance_retention_floor_with_options(
                &namespace,
                &AdvanceRetentionFloorOptions {
                    target: RetentionTarget::Seq(floor),
                },
            )
            .await
            .expect("advance the floor");
        let manifest = metadata_manifest_object(
            &namespace,
            &current_manifest_payload(store.as_ref(), &namespace)
                .await
                .manifest_no,
        );
        for (target, gets) in advances {
            let maintenance = maintenance().await;
            store.reset();
            let advanced = maintenance
                .advance_retention_floor_with_options(
                    &namespace,
                    &AdvanceRetentionFloorOptions { target },
                )
                .await
                .expect("an advance that moves nothing");
            assert_eq!(advanced.retention_floor_seq, floor, "{target:?}");
            let operations = store.take();
            let heads = operations
                .iter()
                .filter(|operation| matches!(operation, RecordedOperation::Head { .. }))
                .count();
            assert!(
                operations.iter().all(|operation| matches!(
                    operation,
                    RecordedOperation::Get { .. }
                        | RecordedOperation::GetWithMetadata { .. }
                        | RecordedOperation::Head { .. }
                )),
                "{target:?} sends only reads: {operations:?}"
            );
            let loads = operations
                .iter()
                .filter(|operation| {
                    matches!(operation, RecordedOperation::Get { key, .. } if *key == manifest)
                })
                .count();
            assert_eq!(
                (loads, operations.len() - heads, heads),
                (1, gets, 1),
                "{target:?} loads the current manifest once (the hint, the manifest, a HEAD \
                 request for the missing manifest after it, and the hint again): \
                 {operations:?}"
            );
        }
    }
}

#[tokio::test]
async fn a_repeated_retirement_pass_lists_once_and_deletes_nothing() {
    use loonfs_core::limits::{GC_SAFETY_MARGIN_MS, NAMESPACE_RETIREMENT_GRACE_WINDOW_MS};
    use loonfs_test_support::stores::{KeyPredicate, RecordingStore, StoreCounts};

    let directory = tempdir().expect("directory");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::prefix("namespaces/retired/content/"),
    ));
    let namespace_id = namespace_id("retired");
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("retirement-test"))
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
    let mut context = loonfs_core::test_support::writer_at("deleter", 1_000);
    loonfs_core::test_support::NamespaceFixture::new(store.as_ref(), &namespace_id, &context)
        .engine()
        .delete_namespace(Default::default())
        .await
        .expect("delete");
    context.now_ms += crate::GcOptions::default()
        .grace_window_ms
        .max(NAMESPACE_RETIREMENT_GRACE_WINDOW_MS)
        + GC_SAFETY_MARGIN_MS;
    store.reset();
    let first = loonfs_core::test_support::namespace_store(store.as_ref(), &namespace_id)
        .gc(None, &crate::GcOptions::default(), &context.clock())
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
    let repeated = loonfs_core::test_support::namespace_store(store.as_ref(), &namespace_id)
        .gc(None, &crate::GcOptions::default(), &context.clock())
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

/// The file under `/large` that the read-after-fold tests read.
const LARGE_FOLD_FILE: &str = "/large/f-02048.txt";

/// Commits 8,192 new files under `/large`, whose bytes are their paths, in
/// two commits, and folds them with one explicit fold. Checks that the fold's
/// revisions segment, which a read of one of the files looks in, is larger
/// than 256 KiB stored, the largest segment that LoonFS fetches whole.
async fn write_a_large_batch_and_fold<S: ObjectStore + ?Sized>(
    store: &S,
    namespace: &crate::Namespace<Writable>,
    maintenance: &Maintenance,
    namespace_id: &NamespaceId,
) {
    use crate::publish::{CommitIntent, FilesystemOperation};

    for commit in 0..2 {
        let mut operations = Vec::new();
        let mut prepared = Vec::new();
        for index in commit * 4096..(commit + 1) * 4096 {
            let path = format!("/large/f-{index:05}.txt");
            let content = namespace
                .prepare_content(path.as_bytes())
                .await
                .expect("prepare the content");
            operations.push(FilesystemOperation::PutFile {
                path: loonfs_types::AbsolutePath::parse(path).expect("path"),
                content: loonfs_types::FileContent::Ref(content.content_ref().clone()),
                behavior: crate::DestinationBehavior::NoReplace,
                expected_inode_id: None,
                expected_revision_no: None,
            });
            prepared.push(content);
        }
        let intent = CommitIntent {
            commit_id: loonfs_types::CommitId::generate(),
            actor_id: loonfs_test_support::test_actor(),
            subject: None,
            message: None,
            operations,
            preconditions: Vec::new(),
        };
        namespace
            .commit_prepared(intent, prepared)
            .await
            .expect("commit the batch");
    }
    maintenance
        .fold_wal(namespace_id)
        .await
        .expect("fold the batch");
    let manifest = current_manifest_payload(store, namespace_id).await;
    let folded = manifest.runs.last().expect("the fold's run");
    let revisions = folded
        .segments
        .iter()
        .find(|segment| segment.family == MetadataRowFamily::Revisions)
        .expect("the fold writes revisions");
    let stored = revisions.index_block.offset + u64::from(revisions.index_block.stored_bytes);
    assert!(
        stored > 256 * 1024,
        "the fold's revisions segment is {stored} bytes"
    );
}

/// Checks that the first read after an explicit fold, again after bounded
/// compaction units, and again after a fold whose segments are larger than a
/// segment that LoonFS fetches whole, finds the segments that maintenance
/// wrote in the runtime's segment cache and sends no segment GET.
#[tokio::test]
async fn a_read_after_a_fold_or_a_compaction_unit_sends_no_segment_get() {
    use loonfs_objectstore::layout::{parse_object_key, DurableObjectFamily};
    use loonfs_test_support::stores::{KeyPredicate, RecordingStore};
    use std::collections::HashMap;

    let directory = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("writer"))
        .build()
        .await
        .expect("writer");
    let maintenance = writer.maintenance(loonfs_test_support::ids::writer_id("maintenance"));
    let namespace_id = namespace_id("read-after-maintenance");
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create the namespace");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open the namespace");
    let read_gets_by_key_class = |path: &'static str| {
        let namespace = &namespace;
        let store = &store;
        async move {
            store.reset();
            let file = namespace.read_file(path).await.expect("read the file");
            assert_eq!(file.bytes, path.as_bytes());
            let mut gets = HashMap::<Option<DurableObjectFamily>, usize>::new();
            for key in store.take_get_keys() {
                *gets
                    .entry(parse_object_key(&key).map(|parsed| parsed.family()))
                    .or_default() += 1;
            }
            gets
        }
    };
    let segment = Some(DurableObjectFamily::MetadataSegment);

    write_and_fold(&writer, &maintenance, &namespace_id, "/docs/first.txt").await;
    let gets = read_gets_by_key_class("/docs/first.txt").await;
    assert_eq!(gets.get(&segment), None, "GETs after a fold: {gets:?}");

    write_and_fold(&writer, &maintenance, &namespace_id, "/docs/second.txt").await;
    let mut units = 0;
    loop {
        match maintenance
            .compact_metadata(&namespace_id, &crate::MaintenanceCancellation::new())
            .await
            .expect("compact one unit")
            .compaction
        {
            MetadataCompactionOutcome::Published { .. } => units += 1,
            MetadataCompactionOutcome::NotNeeded => break,
            other => panic!("expected a published unit, found {other:?}"),
        }
    }
    assert!(units > 0, "the second fold leaves runs to merge");
    let manifest = current_manifest_payload(store.as_ref(), &namespace_id).await;
    assert!(manifest.runs.iter().all(|run| run.tier == RunTier::Base));
    let gets = read_gets_by_key_class("/docs/second.txt").await;
    assert_eq!(gets.get(&segment), None, "GETs after compaction: {gets:?}");

    write_a_large_batch_and_fold(store.as_ref(), &namespace, &maintenance, &namespace_id).await;
    let gets = read_gets_by_key_class(LARGE_FOLD_FILE).await;
    assert_eq!(
        gets.get(&segment),
        None,
        "GETs after a large fold: {gets:?}"
    );
    writer.shutdown().await.expect("shutdown");
}

/// Checks that a fold on a runtime whose cache has an SSD tier writes every
/// block of its segments to disk, so a runtime started over the same SSD
/// directory reads the folded file from disk and sends no segment GET.
#[tokio::test]
async fn a_restarted_runtime_reads_a_folded_file_from_the_ssd_tier() {
    use crate::{MetadataCache, SsdSegmentBlocks};
    use loonfs_objectstore::layout::{parse_object_key, DurableObjectFamily};
    use loonfs_test_support::stores::{KeyPredicate, RecordingStore};

    let store_directory = tempdir().expect("tempdir");
    let ssd_directory = tempdir().expect("tempdir");
    let ssd_cache = || async {
        let budget = NonZeroUsize::new(1 << 30).expect("a nonzero budget");
        let ssd = SsdSegmentBlocks::open(ssd_directory.path(), budget)
            .await
            .expect("open the SSD tier");
        MetadataCache::builder().ssd_segment_blocks(ssd).build()
    };
    let namespace_id = namespace_id("ssd-after-fold");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(store_directory.path()).expect("store"));
    let cache = ssd_cache().await;
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("writer"))
        .metadata_cache(cache.clone())
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create the namespace");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open the namespace");
    let maintenance = writer.maintenance(loonfs_test_support::ids::writer_id("maintenance"));
    write_a_large_batch_and_fold(store.as_ref(), &namespace, &maintenance, &namespace_id).await;
    writer.shutdown().await.expect("shutdown");
    cache.close().await;

    let recording = Arc::new(RecordingStore::new(
        LocalFsStore::new(store_directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let cache = ssd_cache().await;
    let reader = LoonFs::builder_with_store(recording.clone())
        .metadata_cache(cache.clone())
        .read_only()
        .build()
        .await
        .expect("reader");
    let file = reader
        .namespace(&namespace_id)
        .read_file(LARGE_FOLD_FILE)
        .await
        .expect("read the file");
    assert_eq!(file.bytes, LARGE_FOLD_FILE.as_bytes());
    let segment_gets = recording
        .take_get_keys()
        .iter()
        .filter(|key| {
            parse_object_key(key)
                .is_some_and(|parsed| parsed.family() == DurableObjectFamily::MetadataSegment)
        })
        .count();
    assert_eq!(segment_gets, 0, "{:?}", cache.stats());
    assert!(cache.stats().segment_ssd.expect("the SSD tier").hits > 0);
}

/// Forwards every call to a local store, and holds the answer to one read of
/// a namespace's hint until the test releases it, as a slow store would. The
/// read itself runs when it's sent, so the held answer is the hint as it was
/// before anything the test does while the answer waits.
#[derive(Debug)]
struct HeldHintAnswer {
    inner: LocalFsStore,
    hint_key: String,
    /// Hint reads to let through before the held one, plus one. Zero holds
    /// none.
    reads_until_held: std::sync::atomic::AtomicUsize,
    held: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

#[async_trait::async_trait]
impl ObjectStore for HeldHintAnswer {
    loonfs_test_support::delegate_object_store!(self => self.inner;
        head,
        checksum_algorithm,
        identity,
        create_multipart_upload,
        complete_multipart_upload,
        abort_multipart_upload,
        get,
        put,
        put_streamed,
        put_immutable_verified,
        assemble,
        delete,
        list_entries_from_stream,
        list_child_prefixes,
    );

    async fn get_with_metadata(
        &self,
        key: &str,
    ) -> Result<Option<loonfs_objectstore::ObjectBody>, loonfs_objectstore::ObjectStoreError> {
        let answer = self.inner.get_with_metadata(key).await;
        if key == self.hint_key
            && self.reads_until_held.try_update(
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
                |reads| reads.checked_sub(1),
            ) == Ok(1)
        {
            self.held.notify_one();
            self.release.notified().await;
        }
        answer
    }
}

/// Checks that a session's compaction step reads the writer position before
/// the manifest.
///
/// The position's last check is past the revalidation bound, so the step
/// loads the current manifest through the hint. The step's manifest read
/// ends with a second read of the hint, and the answer to that read is held
/// while the session's fold publishes the next manifest and moves the
/// position onto it. The step must then plan from a manifest at least as new
/// as the position that it read, so the tail that it saw unfolded leaves
/// metadata due instead of caught up.
#[tokio::test]
async fn a_session_compaction_step_reads_the_position_before_the_manifest() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = namespace_id("position-before-manifest");
    let store = Arc::new(HeldHintAnswer {
        inner: LocalFsStore::new(directory.path()).expect("store"),
        hint_key: loonfs_objectstore::keys::hint(&namespace_id),
        reads_until_held: std::sync::atomic::AtomicUsize::new(0),
        held: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
    });
    let clock = Arc::new(ManualClock::new(0));
    // Each write leaves inline bytes in the tail, so the session folds each
    // write.
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("writer"))
        .monotonic_timer(clock.clone())
        .config(crate::RuntimeConfig {
            inline_content: crate::InlineContentPolicy {
                inline_content_fold_at_bytes: 1,
                ..crate::InlineContentPolicy::default()
            },
            ..crate::RuntimeConfig::default()
        })
        .build()
        .await
        .expect("writer");
    let maintenance = writer.maintenance(loonfs_test_support::ids::writer_id("maintenance"));
    let actor = loonfs_test_support::test_actor();
    writer
        .create_namespace(&namespace_id, &actor)
        .await
        .expect("create the namespace");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open the namespace");
    namespace
        .put_file("/folded", b"body", &actor)
        .await
        .expect("put a file");
    namespace.wait_for_fold().await.expect("the fold settles");
    writer.drain().await.expect("nothing else runs");

    // With the fold permits held, the next write waits unfolded in the
    // session's position.
    let mut fold_permits = Vec::new();
    for _ in 0..crate::DEFAULT_MAX_CONCURRENT_FOLDS.get() {
        fold_permits.push(writer.execution_budget().fold_permit().await);
    }
    namespace
        .put_file("/unfolded", b"body", &actor)
        .await
        .expect("put a file whose fold waits for a permit");
    let before = current_manifest_payload(store.as_ref(), &namespace_id)
        .await
        .manifest_no;
    // A step plans from a position checked within the revalidation bound
    // without loading the manifest, so the test ages the position past the
    // bound.
    clock.advance_ms(loonfs_core::limits::READ_REVALIDATION_BOUND_MS);

    store
        .reads_until_held
        .store(2, std::sync::atomic::Ordering::SeqCst);
    let cancellation = loonfs_core::MetadataCompactionCancellation::default();
    let step = maintenance.compactor.metadata_step(
        &namespace_id,
        super::TailFold::Never,
        MetadataCompactionPolicy::SizeTiered,
        &cancellation,
    );
    let fold_while_the_answer_waits = async {
        store.held.notified().await;
        drop(fold_permits);
        namespace.wait_for_fold().await.expect("the fold settles");
        store.release.notify_one();
    };
    let (step, ()) = tokio::join!(step, fold_while_the_answer_waits);
    let step = step
        .expect("the step runs")
        .expect("the step isn't cancelled");
    writer.drain().await.expect("the fold's compaction settles");

    assert!(
        current_manifest_payload(store.as_ref(), &namespace_id)
            .await
            .manifest_no
            > before,
        "the fold published while the step's manifest read waited"
    );
    assert_eq!(
        step.wal_fold,
        crate::MetadataMaintenanceOutcome::Due,
        "the step saw the tail unfolded, so it can't report metadata caught up"
    );
    writer.shutdown().await.expect("shutdown");
}

/// Forwards every call to a local store. While it has a view set, it answers
/// reads of a namespace's hint with an earlier copy of the hint, and hides
/// the manifests above the newest manifest of the view, so discovery finds
/// that older manifest current.
#[derive(Debug)]
struct EarlierManifestView {
    inner: LocalFsStore,
    hint_key: String,
    /// The earlier hint, and the newest manifest number that stays visible.
    view: std::sync::Mutex<Option<(loonfs_objectstore::ObjectBody, loonfs_types::ManifestNo)>>,
}

impl EarlierManifestView {
    /// The view's answer for `key`: `Some(None)` hides the object, and `None`
    /// forwards the read.
    fn answer(&self, key: &str) -> Option<Option<loonfs_objectstore::ObjectBody>> {
        let view = self.view.lock().expect("view lock");
        let (hint, newest) = view.as_ref()?;
        if key == self.hint_key {
            return Some(Some(hint.clone()));
        }
        loonfs_objectstore::layout::manifest_no_of(key)
            .filter(|manifest_no| manifest_no > newest)
            .map(|_| None)
    }
}

#[async_trait::async_trait]
impl ObjectStore for EarlierManifestView {
    loonfs_test_support::delegate_object_store!(self => self.inner;
        checksum_algorithm,
        identity,
        create_multipart_upload,
        complete_multipart_upload,
        abort_multipart_upload,
        put,
        put_streamed,
        put_immutable_verified,
        assemble,
        delete,
        list_entries_from_stream,
        list_child_prefixes,
    );

    async fn head(
        &self,
        key: &str,
    ) -> Result<Option<loonfs_objectstore::ObjectMetadata>, loonfs_objectstore::ObjectStoreError>
    {
        match self.answer(key) {
            Some(None) => Ok(None),
            _ => self.inner.head(key).await,
        }
    }

    async fn get_with_metadata(
        &self,
        key: &str,
    ) -> Result<Option<loonfs_objectstore::ObjectBody>, loonfs_objectstore::ObjectStoreError> {
        match self.answer(key) {
            Some(answer) => Ok(answer),
            None => self.inner.get_with_metadata(key).await,
        }
    }

    async fn get(
        &self,
        key: &str,
        range: Option<loonfs_objectstore::ByteRange>,
    ) -> Result<Option<bytes::Bytes>, loonfs_objectstore::ObjectStoreError> {
        match self.answer(key) {
            Some(answer) => Ok(answer.map(|body| body.bytes)),
            None => self.inner.get(key, range).await,
        }
    }
}

/// Checks that a session's compaction step that finds a current manifest
/// older than the writer position's manifest reports metadata due.
///
/// The publish order rules that out, so the store fakes it: it answers with
/// the hint from before the session's last fold, and hides the fold's
/// manifest. The position's last check is past the revalidation bound, so
/// the step loads the current manifest through the hint.
#[tokio::test]
async fn a_session_compaction_step_behind_the_position_reports_metadata_due() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = namespace_id("manifest-behind-position");
    let hint_key = loonfs_objectstore::keys::hint(&namespace_id);
    let store = Arc::new(EarlierManifestView {
        inner: LocalFsStore::new(directory.path()).expect("store"),
        hint_key: hint_key.clone(),
        view: std::sync::Mutex::new(None),
    });
    let clock = Arc::new(ManualClock::new(0));
    // Each write leaves inline bytes in the tail, so the session folds each
    // write.
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("writer"))
        .monotonic_timer(clock.clone())
        .config(crate::RuntimeConfig {
            inline_content: crate::InlineContentPolicy {
                inline_content_fold_at_bytes: 1,
                ..crate::InlineContentPolicy::default()
            },
            ..crate::RuntimeConfig::default()
        })
        .build()
        .await
        .expect("writer");
    let maintenance = writer.maintenance(loonfs_test_support::ids::writer_id("maintenance"));
    let actor = loonfs_test_support::test_actor();
    writer
        .create_namespace(&namespace_id, &actor)
        .await
        .expect("create the namespace");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open the namespace");
    namespace
        .put_file("/first", b"body", &actor)
        .await
        .expect("put a file");
    namespace.wait_for_fold().await.expect("the fold settles");
    writer.drain().await.expect("nothing else runs");
    let earlier_hint = store
        .inner
        .get_with_metadata(&hint_key)
        .await
        .expect("read the hint")
        .expect("the hint exists");
    let earlier = current_manifest_payload(store.as_ref(), &namespace_id)
        .await
        .manifest_no;
    namespace
        .put_file("/second", b"body", &actor)
        .await
        .expect("put a file");
    namespace.wait_for_fold().await.expect("the fold settles");
    writer.drain().await.expect("nothing else runs");

    // A step plans from a position checked within the revalidation bound
    // without loading the manifest, so the test ages the position past the
    // bound.
    clock.advance_ms(loonfs_core::limits::READ_REVALIDATION_BOUND_MS);
    *store.view.lock().expect("view lock") = Some((earlier_hint, earlier));
    let step = maintenance
        .compactor
        .metadata_step(
            &namespace_id,
            super::TailFold::Never,
            MetadataCompactionPolicy::SizeTiered,
            &loonfs_core::MetadataCompactionCancellation::default(),
        )
        .await;
    *store.view.lock().expect("view lock") = None;
    let step = step
        .expect("the step runs")
        .expect("the step isn't cancelled");
    assert_eq!(
        step.wal_fold,
        crate::MetadataMaintenanceOutcome::Due,
        "a manifest behind the position leaves metadata due"
    );
    writer.shutdown().await.expect("shutdown");
}
