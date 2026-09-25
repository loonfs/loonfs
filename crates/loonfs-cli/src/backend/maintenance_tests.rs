//! Embedded maintenance assignments and shutdown.

use super::*;
use crate::config::StoreConfig;
use crate::resolve::ResolvedTarget;
use loonfs::{CreateNamespaceOptions, MaintenanceConclusion, MetadataMaintenanceOptions};
use loonfs_api::NamespaceId;
use loonfs_core::test_support::append_wal_segments;
use loonfs_core::MutationContext;
use loonfs_grep::{GREP_GC_JOB, GREP_INDEX_JOB};
use tempfile::tempdir;

#[tokio::test]
async fn embedded_writes_drain_maintenance_before_the_wal_backpressure_cap() {
    let directory = tempdir().expect("store directory");
    let (config, _) = local_store(directory.path());
    let target = ResolvedTarget::embedded(&config, None, true)
        .await
        .expect("embedded profile");
    let namespace = namespace_id("demo");
    let actor = loonfs_test_support::test_actor();
    target
        .client
        .create_namespace(
            &namespace,
            &actor,
            loonfs_api::NamespaceAccess::unrestricted(),
        )
        .await
        .expect("namespace");
    for index in 0..140 {
        target
            .client
            .put_file_bytes(
                &loonfs_client::NamespacePath::parse("demo", &format!("/file-{index}"))
                    .expect("file path"),
                b"payload",
                &loonfs_client::PutFileOptions::new(actor.clone()),
            )
            .await
            .expect("write past the unmaintained WAL limit");
    }
}

fn namespace_id(value: &str) -> NamespaceId {
    NamespaceId::parse(value).expect("valid namespace id")
}

fn checkpoint_threshold() -> u64 {
    MetadataMaintenanceOptions::default()
        .max_wal_tail_segments
        .get()
}

fn every_job() -> [MaintenanceJobId; 5] {
    [
        MaintenanceJobId::METADATA,
        MaintenanceJobId::METADATA_COMPACTION,
        MaintenanceJobId::GC,
        GREP_INDEX_JOB,
        GREP_GC_JOB,
    ]
}

async fn seed_wal_backlog(store: &SharedObjectStore, namespace_id: &NamespaceId) {
    const PUBLISHES_PAST_THE_CHECKPOINT_THRESHOLD: u64 = 34;
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id(format!("{namespace_id}-backlog"))
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("build backlog writer");
    writer
        .create_namespace(
            namespace_id,
            CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("create namespace");
    append_wal_segments(
        store.as_ref(),
        namespace_id,
        PUBLISHES_PAST_THE_CHECKPOINT_THRESHOLD,
        &MutationContext {
            writer_id: loonfs_api::WriterId::parse(format!("{namespace_id}-tail"))
                .expect("writer id"),
            now_ms: 1_000,
        },
    )
    .await
    .expect("seed WAL backlog");
}

fn local_store(temp_dir: &std::path::Path) -> (StoreConfig, SharedObjectStore) {
    let config = StoreConfig::LocalFs {
        root: temp_dir.display().to_string(),
        key_prefix: None,
    };
    let store = config
        .configured_object_store()
        .expect("configure store")
        .into_shared();
    (config, store)
}

#[tokio::test]
async fn a_drain_settles_every_assigned_key_and_does_the_work_it_finds() {
    let temp_dir = tempdir().expect("create temp dir");
    let (store_config, store) = local_store(temp_dir.path());
    let indexed = namespace_id("alpha");
    let unindexed = namespace_id("beta");
    seed_wal_backlog(&store, &indexed).await;
    seed_wal_backlog(&store, &unindexed).await;

    let target = ResolvedTarget::embedded(&store_config, None, false)
        .await
        .expect("build embedded target");
    target
        .client
        .enable_grep_index(&indexed)
        .await
        .expect("enable the index without driving it");
    let head_seq = target
        .client
        .get_namespace(&indexed)
        .await
        .expect("status before the drain")
        .head_seq;

    let progress = target
        .maintenance
        .as_ref()
        .expect("embedded host")
        .drain_maintenance(
            &[indexed.clone(), unindexed.clone()],
            &every_job(),
            StepBudget::default(),
        )
        .await
        .expect("drain the assignment");

    assert!(
        !progress.budget_exhausted(),
        "an unbudgeted drain settles every key: {:?}",
        progress.keys
    );
    assert_eq!(progress.keys.len(), 10, "five jobs over two namespaces");
    assert!(progress.steps >= 10, "every key took at least one step");
    let unindexed_grep = progress
        .keys
        .iter()
        .find(|key| key.job == GREP_INDEX_JOB && key.namespace_id == unindexed)
        .expect("the unindexed namespace's grep key");
    assert_eq!(
        unindexed_grep.conclusion,
        Some(MaintenanceConclusion::NotEnabled)
    );

    for namespace_id in [&indexed, &unindexed] {
        let status = target
            .maintenance
            .as_ref()
            .expect("embedded host")
            .maintenance
            .get_namespace_diagnostics(namespace_id)
            .await
            .expect("diagnostics after the drain");
        assert!(
            status.wal_tail_segments < checkpoint_threshold(),
            "`{namespace_id}` kept a WAL tail of {} segments past the checkpoint threshold",
            status.wal_tail_segments
        );
        assert!(status.current_manifest_no.is_some(), "{namespace_id}");
    }
    let indexed_status = target
        .maintenance
        .as_ref()
        .expect("embedded host")
        .grep_worker
        .get_grep_index(&indexed)
        .await
        .expect("index status after the drain");
    assert!(
        indexed_status.lifecycle.is_built_through(head_seq),
        "the assigned index must reach the head it was behind: {:?}",
        indexed_status.lifecycle
    );
}

#[tokio::test]
async fn a_spent_drain_budget_reports_the_keys_it_left_unsettled() {
    let temp_dir = tempdir().expect("create temp dir");
    let (store_config, store) = local_store(temp_dir.path());
    let namespace = namespace_id("alpha");
    seed_wal_backlog(&store, &namespace).await;
    let target = ResolvedTarget::embedded(&store_config, None, false)
        .await
        .expect("build embedded target");

    let progress = target
        .maintenance
        .as_ref()
        .expect("embedded host")
        .drain_maintenance(
            std::slice::from_ref(&namespace),
            &[MaintenanceJobId::METADATA, MaintenanceJobId::GC],
            StepBudget {
                max_steps: Some(1),
                deadline_ms: None,
            },
        )
        .await
        .expect("drain within a budget");

    assert!(progress.budget_exhausted());
    assert_eq!(progress.steps, 1);
    let metadata = &progress.keys[0];
    assert_eq!(metadata.job, MaintenanceJobId::METADATA);
    assert_eq!(metadata.steps, 1);
    assert_eq!(
        metadata.conclusion,
        Some(MaintenanceConclusion::Progressed),
        "one step of a real backlog moves durable state and leaves more behind"
    );
    assert!(!metadata.settled());
    let collection = &progress.keys[1];
    assert_eq!(collection.job, MaintenanceJobId::GC);
    assert_eq!(collection.steps, 0);
    assert_eq!(
        collection.conclusion, None,
        "a key the budget never reached reports no conclusion rather than a made-up one"
    );
    assert!(!collection.settled());
}

#[tokio::test]
async fn hosting_an_assignment_maintains_a_cold_namespace_until_the_signal() {
    let temp_dir = tempdir().expect("create temp dir");
    let (store_config, store) = local_store(temp_dir.path());
    let namespace = namespace_id("alpha");
    seed_wal_backlog(&store, &namespace).await;
    let target = ResolvedTarget::embedded(&store_config, None, false)
        .await
        .expect("build embedded target");
    target
        .client
        .enable_grep_index(&namespace)
        .await
        .expect("enable the index without driving it");
    let head_seq = target
        .client
        .get_namespace(&namespace)
        .await
        .expect("status before hosting")
        .head_seq;

    let stop = async {
        wait_until(|| async {
            target
                .client
                .get_grep_index(&namespace)
                .await
                .expect("index status while hosting")
                .lifecycle
                .is_built_through(head_seq)
        })
        .await;
    };
    target
        .maintenance
        .as_ref()
        .expect("embedded host")
        .host_maintenance(std::slice::from_ref(&namespace), &every_job(), None, stop)
        .await
        .expect("the host shuts down cleanly on its signal");

    let status = target
        .maintenance
        .as_ref()
        .expect("embedded host")
        .maintenance
        .get_namespace_diagnostics(&namespace)
        .await
        .expect("diagnostics after hosting");
    assert!(
        status.wal_tail_segments < checkpoint_threshold(),
        "the hosted runner left a WAL tail of {} segments",
        status.wal_tail_segments
    );
}

#[allow(clippy::disallowed_methods)]
async fn wait_until<F, Fut>(condition: F)
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    // This test polls hosted work so protocol time remains unchanged.
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        while !condition().await {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the hosted runner never reached the state it was assigned");
}
