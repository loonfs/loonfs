#![allow(clippy::panic)]
//! The grep job under a real maintenance runner: what its probe answers,
//! what its steps conclude, and how a writer's one permit pool paces them.

use crate::common::is_content_object;
use bytes::Bytes;
use loonfs::{
    DeleteNamespaceOptions, FsAdmin, FsBackgroundWork, FsReader, FsWriter, MaintenanceJob,
    MaintenanceProbe, MaintenanceStepConclusion, SharedObjectStore,
};
use loonfs_api::{ChangeSeq, IndexSegmentId, NamespaceId};
use loonfs_grep::keyspace::{root_key, segment_key};
use loonfs_grep::root::load_grep_root;
use loonfs_grep::{
    GramIndexBuildPolicy, GrepGcJob, GrepMaintenanceJob, GrepWorker, GREP_GC_JOB, GREP_INDEX_JOB,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::ObjectStore;
use loonfs_test_support::stores::{
    BlockingStore, ConcurrencyWatchStore, KeyPredicate, MetadataMapStore, OperationClass,
};
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;

const WAIT: Duration = Duration::from_secs(10);

#[tokio::test]
async fn the_probe_reports_work_from_the_root_and_the_feed() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store"));
    let namespace_id = NamespaceId::parse("probe").expect("namespace id");
    let writer = seed(store.clone(), &namespace_id).await;
    let worker = worker(store.clone(), "probe-worker").await;
    let job = job(&worker);

    assert_eq!(
        job.probe(&namespace_id).await.expect("probe absent root"),
        MaintenanceProbe::Idle,
        "a namespace with no grep root has nothing to maintain"
    );

    worker.enable(&namespace_id).await.expect("enable grep");
    assert_eq!(
        job.probe(&namespace_id).await.expect("probe backfill"),
        MaintenanceProbe::Due,
        "a backfill always has its next page to walk"
    );

    catch_up(&job, &namespace_id).await;
    assert_eq!(
        job.probe(&namespace_id).await.expect("probe caught up"),
        MaintenanceProbe::Idle
    );

    put_file(&writer, &namespace_id, "probe-put").await;
    assert_eq!(
        job.probe(&namespace_id).await.expect("probe behind head"),
        MaintenanceProbe::Due,
        "one commit after the watermark is all the feed has to report"
    );

    catch_up(&job, &namespace_id).await;
    worker.disable(&namespace_id).await.expect("disable grep");
    assert_eq!(
        job.probe(&namespace_id).await.expect("probe disabled root"),
        MaintenanceProbe::Idle,
        "a disabled root is what the runner evicts the key on"
    );
}

#[tokio::test]
async fn a_tombstoned_namespace_concludes_not_enabled() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store"));
    let namespace_id = NamespaceId::parse("deleted").expect("namespace id");
    let writer = seed(store.clone(), &namespace_id).await;
    let worker = worker(store, "deleted-worker").await;
    worker.enable(&namespace_id).await.expect("enable grep");
    writer
        .delete_namespace(&namespace_id, DeleteNamespaceOptions::default())
        .await
        .expect("delete namespace");

    let job = job(&worker);
    assert_eq!(
        job.step(&namespace_id, None)
            .await
            .expect("step tombstone")
            .conclusion,
        MaintenanceStepConclusion::NotEnabled,
        "a deleted namespace is not a failure to retry; it is nothing to maintain"
    );
    assert_eq!(
        job.probe(&namespace_id).await.expect("probe tombstone"),
        MaintenanceProbe::Due,
        "the probe reads grep's own root and no more, so a root left behind by a deleted \
         namespace still looks like work — and the step is what settles it, once"
    );
}

/// Disabling the index is one durable compare-and-swap, and the next step
/// is where a scheduler finds out — with the conclusion that makes the
/// runner forget the namespace.
#[tokio::test]
async fn a_disabled_root_concludes_not_enabled_on_the_next_step() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store"));
    let namespace_id = NamespaceId::parse("disabled").expect("namespace id");
    let writer = seed(store.clone(), &namespace_id).await;
    put_file(&writer, &namespace_id, "disabled-put").await;
    let worker = worker(store, "disabled-worker").await;
    worker.enable(&namespace_id).await.expect("enable grep");
    let job = job(&worker);
    catch_up(&job, &namespace_id).await;

    worker.disable(&namespace_id).await.expect("disable grep");
    assert_eq!(
        job.step(&namespace_id, None)
            .await
            .expect("step disabled root")
            .conclusion,
        MaintenanceStepConclusion::NotEnabled
    );
    assert_eq!(
        job.probe(&namespace_id).await.expect("probe disabled root"),
        MaintenanceProbe::Idle
    );
}

/// The runner is what turns a nudge into an indexed namespace, and a
/// poisoned root must not stop the namespaces beside it.
#[tokio::test]
async fn a_nudge_indexes_a_namespace_while_a_poisoned_sibling_backs_off() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store"));
    let poisoned = NamespaceId::parse("poisoned").expect("namespace id");
    let healthy = NamespaceId::parse("healthy").expect("namespace id");
    let poisoned_writer = seed(store.clone(), &poisoned).await;
    let healthy_writer = seed(store.clone(), &healthy).await;
    put_file(&poisoned_writer, &poisoned, "poisoned-put").await;
    put_file(&healthy_writer, &healthy, "healthy-put").await;

    let worker = worker(store.clone(), "runner-test-worker").await;
    worker.enable(&poisoned).await.expect("enable poisoned");
    worker.enable(&healthy).await.expect("enable healthy");
    let orphan = segment_key(&healthy, &IndexSegmentId::generate());
    store
        .put_overwrite(&orphan, Bytes::from_static(b"orphan"))
        .await
        .expect("write orphan");
    store
        .put_overwrite(&root_key(&poisoned), Bytes::from_static(b"poison"))
        .await
        .expect("poison root");

    let host = host_writer(store.clone(), "runner-host", 2).await;
    host.register_maintenance_job(Arc::new(job(&worker)))
        .expect("register the grep job");
    for namespace_id in [&poisoned, &healthy] {
        host.maintenance().nudge(GREP_INDEX_JOB, namespace_id);
    }

    wait_for_watermark(&store, &healthy, ChangeSeq(1)).await;
    assert!(
        store.head(&orphan).await.expect("head orphan").is_some(),
        "catching up must never run garbage collection implicitly"
    );
    assert_eq!(
        store
            .get(&root_key(&poisoned), None)
            .await
            .expect("read poisoned root")
            .expect("poisoned root bytes"),
        Bytes::from_static(b"poison"),
        "a step that cannot read its root publishes nothing"
    );
    host.shutdown().await.expect("settle host maintenance");
}

/// One writer, one permit pool: the runner's cap is what bounds grep steps
/// now, so blocking each namespace's sole backfill content read makes the
/// number of simultaneously executing steps directly visible.
#[tokio::test]
async fn the_runners_one_permit_pool_caps_grep_steps_across_namespaces() {
    const NAMESPACES: usize = 8;
    const MAX_CONCURRENT_MAINTENANCE: usize = 2;

    let temp_dir = tempdir().expect("tempdir");
    let content_keys = KeyPredicate::new(is_content_object);
    let blocked_store = Arc::new(BlockingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        content_keys.clone(),
        OperationClass::Read,
    ));
    let store = Arc::new(ConcurrencyWatchStore::new(
        blocked_store.clone(),
        content_keys,
    ));
    let worker = worker(store.clone(), "concurrency-worker").await;
    let mut namespace_ids = Vec::new();
    for index in 0..NAMESPACES {
        let namespace_id =
            NamespaceId::parse(format!("concurrency-{index}")).expect("namespace id");
        let writer = seed(store.clone(), &namespace_id).await;
        put_file(&writer, &namespace_id, &format!("concurrency-put-{index}")).await;
        worker.enable(&namespace_id).await.expect("enable grep");
        namespace_ids.push(namespace_id);
    }

    let reads_before_steps = store.reads().total;
    blocked_store.arm();
    let host = host_writer(
        store.clone(),
        "concurrency-host",
        MAX_CONCURRENT_MAINTENANCE,
    )
    .await;
    host.register_maintenance_job(Arc::new(job(&worker)))
        .expect("register the grep job");
    for namespace_id in &namespace_ids {
        host.maintenance().nudge(GREP_INDEX_JOB, namespace_id);
    }

    tokio::time::timeout(WAIT, async {
        while store.reads().total < reads_before_steps + MAX_CONCURRENT_MAINTENANCE {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the permitted steps must enter their build steps");
    assert_eq!(store.reads().peak_in_flight, MAX_CONCURRENT_MAINTENANCE);

    blocked_store.release();
    for namespace_id in &namespace_ids {
        wait_for_watermark(&store, namespace_id, ChangeSeq(1)).await;
    }
    assert!(
        store.reads().peak_in_flight <= MAX_CONCURRENT_MAINTENANCE,
        "executing grep steps exceeded the runner's pool"
    );
    host.shutdown().await.expect("settle host maintenance");
}

fn job<S: ObjectStore + Clone>(worker: &GrepWorker<S>) -> GrepMaintenanceJob<S> {
    GrepMaintenanceJob::new(worker.clone(), GramIndexBuildPolicy::default())
}

/// Runs the job's steps the way a one-shot host does, so a test can set up
/// a caught-up index without a runner.
async fn catch_up<S: ObjectStore + Clone + Send + Sync + 'static>(
    job: &GrepMaintenanceJob<S>,
    namespace_id: &NamespaceId,
) {
    for _ in 0..64 {
        match job
            .step(namespace_id, None)
            .await
            .expect("grep step")
            .conclusion
        {
            MaintenanceStepConclusion::Progressed | MaintenanceStepConclusion::Superseded => {}
            MaintenanceStepConclusion::Idle => return,
            settled => panic!("grep step concluded {settled:?} while catching up"),
        }
    }
    panic!("the grep job did not catch up");
}

/// The collection job is the reclaiming half the index job deliberately
/// leaves undone, and a nudge is what asks for it.
#[tokio::test]
async fn a_nudge_collects_what_indexing_left_behind() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(MetadataMapStore::aged(
        LocalFsStore::new(temp_dir.path()).expect("local store"),
        KeyPredicate::any(),
    ));
    let namespace_id = NamespaceId::parse("collect").expect("namespace id");
    let writer = seed(store.clone(), &namespace_id).await;
    put_file(&writer, &namespace_id, "collect-put").await;
    let worker = worker(store.clone(), "collect-worker").await;
    worker.enable(&namespace_id).await.expect("enable grep");
    let orphan = segment_key(&namespace_id, &IndexSegmentId::generate());
    store
        .put_overwrite(&orphan, Bytes::from_static(b"orphan"))
        .await
        .expect("write orphan");

    let host = host_writer(store.clone(), "collect-host", 2).await;
    host.register_maintenance_job(Arc::new(GrepGcJob::new(worker.clone())))
        .expect("register the grep collection job");
    host.maintenance().nudge(GREP_GC_JOB, &namespace_id);

    wait_for_deletion(&store, &orphan).await;
    assert!(
        store
            .head(&root_key(&namespace_id))
            .await
            .expect("head root")
            .is_some(),
        "the pointer a live namespace still names is never a candidate"
    );
    host.shutdown().await.expect("settle host maintenance");
}

/// A resume position the collector refuses restarts the pass instead of
/// failing the key: every pass rebuilds its own safety proof, so starting
/// over is always sound.
#[tokio::test]
async fn a_refused_resume_position_restarts_the_collection_pass() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store"));
    let namespace_id = NamespaceId::parse("resume").expect("namespace id");
    let writer = seed(store.clone(), &namespace_id).await;
    put_file(&writer, &namespace_id, "resume-put").await;
    let worker = worker(store.clone(), "resume-worker").await;
    worker.enable(&namespace_id).await.expect("enable grep");
    let job = GrepGcJob::new(worker);

    assert_eq!(
        job.step(&namespace_id, Some("not-a-cursor"))
            .await
            .expect("a refused cursor is not a step failure")
            .conclusion,
        MaintenanceStepConclusion::Superseded
    );
    let fresh = job.step(&namespace_id, None).await.expect("fresh pass");
    assert_eq!(fresh.conclusion, MaintenanceStepConclusion::Idle);
    assert_eq!(fresh.continuation, None);
}

/// A writer that only hosts the runner: it serves no namespace of its own,
/// so every step its pool admits is a grep step.
async fn host_writer<S: ObjectStore + 'static>(
    store: Arc<S>,
    writer_id: &str,
    max_concurrent_maintenance: usize,
) -> FsWriter {
    let shared: SharedObjectStore = store;
    FsWriter::builder_with_store(shared)
        .writer_id(writer_id)
        .background_work(FsBackgroundWork::Enabled)
        .max_concurrent_maintenance(max_concurrent_maintenance)
        .build()
        .await
        .expect("build host writer")
}

/// A worker over one store: grep's own keyspace writes go straight to it,
/// and the runtime handles it reads and checkpoints through are opened on
/// the same client, so a fault-injecting store covers both.
async fn worker<S: ObjectStore + 'static>(store: Arc<S>, actor: &str) -> GrepWorker<Arc<S>> {
    let shared: SharedObjectStore = store.clone();
    let reader = FsReader::builder_with_store(shared.clone())
        .build()
        .await
        .expect("build reader");
    let admin = FsAdmin::builder_with_store(shared)
        .actor_id(actor)
        .build()
        .await
        .expect("build admin");
    GrepWorker::new(store, reader, admin)
}

async fn seed<S: ObjectStore + 'static>(store: Arc<S>, namespace_id: &NamespaceId) -> FsWriter {
    crate::test_seeding::writer(store, namespace_id, format!("seed-{namespace_id}")).await
}

async fn put_file(writer: &FsWriter, namespace_id: &NamespaceId, commit_id: &str) {
    crate::test_seeding::put_file(
        writer,
        namespace_id,
        b"maintenance needle\n",
        "/note.txt",
        commit_id,
    )
    .await;
}

/// Waits for the runner's collection steps to reclaim one key.
#[allow(clippy::disallowed_methods)]
async fn wait_for_deletion<S: ObjectStore + 'static>(store: &Arc<S>, key: &str) {
    // The pass reports what it reclaimed durably and nowhere else; this
    // polls that durable state under a bounded timeout.
    tokio::time::timeout(WAIT, async {
        while store.head(key).await.expect("head candidate").is_some() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("`{key}` was never collected"));
}

/// Waits for the runner's steps to publish a root at `built_through_seq`.
#[allow(clippy::disallowed_methods)]
async fn wait_for_watermark<S: ObjectStore + 'static>(
    store: &Arc<S>,
    namespace_id: &NamespaceId,
    built_through_seq: ChangeSeq,
) {
    // The runner reports its progress durably and nowhere else; this polls
    // that durable state under a bounded timeout.
    tokio::time::timeout(WAIT, async {
        loop {
            if let Some(root) = load_grep_root(&**store, namespace_id)
                .await
                .expect("load grep root")
            {
                if root
                    .manifest_state()
                    .status()
                    .active_watermark()
                    .is_some_and(|(reached, _)| reached >= built_through_seq)
                {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("`{namespace_id}` did not reach {built_through_seq:?}"));
}
