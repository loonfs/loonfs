//! How many grep steps hold file content or index segments at once.

use crate::common::GrepHost;
use loonfs::{InlineContentPolicy, LoonFs, NamespaceId, SharedObjectStore, Writable};
use loonfs_grep::keyspace::{parse_key, GrepKeyKind};
use loonfs_grep::{
    GramIndexBuildPolicy, GrepBuildOutcome, GrepReorganizeOutcome, GrepStepBudget,
    GrepStepBudgetStats, GrepWorker,
};
use loonfs_objectstore::layout::{parse_object_key, DurableObjectFamily};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::clock::bounded;
use loonfs_test_support::stores::{
    BlockingStore, KeyPredicate, NamespaceOverlapStore, OperationSelector,
};
use std::assert_matches;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;

/// Watches content reads and writes and grep segment reads and writes,
/// keyed by the namespace that owns them.
fn content_watch(inner: LocalFsStore) -> NamespaceOverlapStore<LocalFsStore> {
    NamespaceOverlapStore::new(inner, |context| {
        let kind = context.kind();
        (OperationSelector::Get.matches(kind) || OperationSelector::Put.matches(kind))
            .then(|| content_namespace(context.key()))
            .flatten()
    })
}

/// The namespace of a content object or a grep segment.
fn content_namespace(key: &str) -> Option<String> {
    if let Some(parsed) =
        parse_object_key(key).filter(|parsed| parsed.family() == DurableObjectFamily::ContentBlob)
    {
        return Some(parsed.owner_namespace_id().to_owned());
    }
    parse_key(key)
        .filter(|parsed| matches!(parsed.kind, GrepKeyKind::Segment { .. }))
        .map(|parsed| parsed.namespace_id.to_string())
}

/// A writer that stores every file as a content object, so a step's content
/// reads reach the store.
async fn content_object_writer(store: &SharedObjectStore) -> LoonFs<Writable> {
    LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id(
            "grep-step-permits-writer",
        ))
        .min_publish_interval_ms(0)
        .inline_content(InlineContentPolicy {
            inline_content_threshold_bytes: None,
            ..InlineContentPolicy::default()
        })
        .build()
        .await
        .expect("build writer")
}

async fn put_files(writer: &LoonFs<Writable>, namespace_id: &NamespaceId, prefix: &str) {
    let namespace = writer.open_namespace(namespace_id).expect("open namespace");
    for index in 0..2 {
        namespace
            .put_file(
                &format!("/{prefix}-{index}.txt"),
                format!("needle {prefix} {index}\n").as_bytes(),
                &loonfs_test_support::test_actor(),
            )
            .await
            .expect("put file");
    }
}

async fn namespace_with_files(writer: &LoonFs<Writable>, name: &str) -> NamespaceId {
    let namespace_id = NamespaceId::parse(name).expect("namespace id");
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    put_files(writer, &namespace_id, "first").await;
    namespace_id
}

fn worker_with(host: &GrepHost, budget: &GrepStepBudget) -> GrepWorker<SharedObjectStore> {
    GrepWorker::new(
        host.store.clone(),
        host.reader.clone(),
        host.writer.clone(),
        budget.clone(),
    )
}

async fn blocking_setup(
    directory: &tempfile::TempDir,
) -> (Arc<BlockingStore<LocalFsStore>>, LoonFs<Writable>, GrepHost) {
    let blocking = Arc::new(BlockingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::content_blob(),
        OperationSelector::Get,
    ));
    let store: SharedObjectStore = blocking.clone();
    let writer = content_object_writer(&store).await;
    let host = GrepHost::new(&store, "grep-step-permits").await;
    (blocking, writer, host)
}

/// Starts a build step and returns it parked at its first content read,
/// holding a step permit.
async fn parked_build_step<'a>(
    blocking: &BlockingStore<LocalFsStore>,
    worker: &'a GrepWorker<SharedObjectStore>,
    namespace_id: &'a NamespaceId,
) -> Pin<Box<impl Future<Output = loonfs_grep::Result<GrepBuildOutcome>> + 'a>> {
    blocking.block_next();
    let mut step = Box::pin(worker.build_step(namespace_id, GramIndexBuildPolicy::default()));
    tokio::select! {
        outcome = &mut step => panic!("the step finished while its read was held: {outcome:?}"),
        () = blocking.wait_until_blocked() => {}
    }
    step
}

/// Drives `step` until `budget` reports `expected`. The step must not finish
/// first.
async fn drive_until_reported(
    step: &mut (impl Future<Output = loonfs_grep::Result<GrepBuildOutcome>> + Unpin),
    budget: &GrepStepBudget,
    expected: GrepStepBudgetStats,
) {
    let reported = async {
        while budget.stats() != expected {
            tokio::task::yield_now().await;
        }
    };
    bounded(Duration::from_secs(10), async {
        tokio::select! {
            outcome = step => panic!("the step finished before the budget reported {expected:?}: {outcome:?}"),
            () = reported => {}
        }
    })
    .await
    .unwrap_or_else(|_| panic!("the budget reported {:?}, not {expected:?}", budget.stats()));
}

#[tokio::test]
async fn no_more_grep_steps_hold_content_than_the_limit() {
    let directory = tempdir().expect("directory");
    let watch = Arc::new(content_watch(
        LocalFsStore::new(directory.path()).expect("store"),
    ));
    let store: SharedObjectStore = watch.clone();
    let writer = content_object_writer(&store).await;
    let host = GrepHost::new(&store, "grep-step-permits").await;
    let worker = worker_with(&host, &GrepStepBudget::new(NonZeroUsize::MIN));
    let mut namespaces = Vec::new();
    for index in 0..3 {
        let namespace_id = namespace_with_files(&writer, &format!("permits-{index}")).await;
        worker.enable(&namespace_id).await.expect("enable grep");
        namespaces.push(namespace_id);
    }
    // The backfill run and the incremental run make a reorganize step due.
    let policy = GramIndexBuildPolicy {
        max_delta_runs: NonZeroUsize::new(2).expect("two delta runs"),
        ..GramIndexBuildPolicy::default()
    };
    watch.reset_peak();

    let backfills = futures::future::try_join_all(
        namespaces
            .iter()
            .map(|namespace_id| worker.build_step(namespace_id, policy)),
    )
    .await
    .expect("backfill steps");
    for namespace_id in &namespaces {
        put_files(&writer, namespace_id, "second").await;
    }
    let increments = futures::future::try_join_all(
        namespaces
            .iter()
            .map(|namespace_id| worker.build_step(namespace_id, policy)),
    )
    .await
    .expect("incremental steps");
    let reorganizations = futures::future::try_join_all(
        namespaces
            .iter()
            .map(|namespace_id| worker.reorganize_step(namespace_id, policy)),
    )
    .await
    .expect("reorganize steps");

    for outcome in backfills.iter().chain(&increments) {
        assert_matches!(outcome, GrepBuildOutcome::Published { .. });
    }
    for outcome in &reorganizations {
        assert_matches!(outcome, GrepReorganizeOutcome::UnitPublished { .. });
    }
    assert_eq!(
        watch.peak_namespaces(),
        1,
        "one step holds content at a time, across every namespace"
    );
}

#[tokio::test]
async fn workers_over_two_stores_share_one_step_permit() {
    let first_directory = tempdir().expect("directory");
    let second_directory = tempdir().expect("directory");
    let (blocking, first_writer, first_host) = blocking_setup(&first_directory).await;
    let (_, second_writer, second_host) = blocking_setup(&second_directory).await;
    let budget = GrepStepBudget::new(NonZeroUsize::MIN);
    let first_worker = worker_with(&first_host, &budget);
    let second_worker = worker_with(&second_host, &budget);
    let first = namespace_with_files(&first_writer, "first").await;
    let second = namespace_with_files(&second_writer, "second").await;
    first_worker.enable(&first).await.expect("enable grep");
    second_worker.enable(&second).await.expect("enable grep");

    let holding_step = parked_build_step(&blocking, &first_worker, &first).await;
    assert_eq!(
        budget.stats(),
        GrepStepBudgetStats {
            running: 1,
            waiting: 0
        }
    );
    let mut waiting_step =
        Box::pin(second_worker.build_step(&second, GramIndexBuildPolicy::default()));
    drive_until_reported(
        &mut waiting_step,
        &budget,
        GrepStepBudgetStats {
            running: 1,
            waiting: 1,
        },
    )
    .await;

    blocking.release();
    let outcome = holding_step.await.expect("holding step");
    assert_matches!(outcome, GrepBuildOutcome::Published { .. });
    let outcome = bounded(Duration::from_secs(10), waiting_step)
        .await
        .expect("the waiting step takes the permit the first step returned")
        .expect("waiting step");
    assert_matches!(outcome, GrepBuildOutcome::Published { .. });
    assert_eq!(budget.stats(), GrepStepBudgetStats::default());
}

#[tokio::test]
async fn an_up_to_date_step_on_either_worker_takes_no_permit() {
    let first_directory = tempdir().expect("directory");
    let second_directory = tempdir().expect("directory");
    let (blocking, first_writer, first_host) = blocking_setup(&first_directory).await;
    let (_, second_writer, second_host) = blocking_setup(&second_directory).await;
    let budget = GrepStepBudget::new(NonZeroUsize::MIN);
    let first_worker = worker_with(&first_host, &budget);
    let second_worker = worker_with(&second_host, &budget);
    let mut current = Vec::new();
    for (writer, host, worker) in [
        (&first_writer, &first_host, &first_worker),
        (&second_writer, &second_host, &second_worker),
    ] {
        let namespace_id = namespace_with_files(writer, "current").await;
        host.enable_grep_index(&namespace_id)
            .await
            .expect("index the current namespace");
        current.push((worker, namespace_id));
    }
    let busy = namespace_with_files(&first_writer, "busy").await;
    first_worker.enable(&busy).await.expect("enable grep");

    let busy_step = parked_build_step(&blocking, &first_worker, &busy).await;
    for (worker, namespace_id) in &current {
        let outcome = bounded(
            Duration::from_secs(10),
            worker.build_step(namespace_id, GramIndexBuildPolicy::default()),
        )
        .await
        .expect("an up-to-date step does not wait for the held permit")
        .expect("build step");
        assert_matches!(outcome, GrepBuildOutcome::UpToDate { .. });
    }
    assert_eq!(
        budget.stats(),
        GrepStepBudgetStats {
            running: 1,
            waiting: 0
        }
    );

    blocking.release();
    let outcome = busy_step.await.expect("busy step");
    assert_matches!(outcome, GrepBuildOutcome::Published { .. });
}

#[tokio::test]
async fn a_dropped_step_returns_its_permit() {
    let directory = tempdir().expect("directory");
    let (blocking, writer, host) = blocking_setup(&directory).await;
    let worker = worker_with(&host, &GrepStepBudget::new(NonZeroUsize::MIN));
    let holding = namespace_with_files(&writer, "holding").await;
    let waiting = namespace_with_files(&writer, "waiting").await;
    for namespace_id in [&holding, &waiting] {
        worker.enable(namespace_id).await.expect("enable grep");
    }
    let policy = GramIndexBuildPolicy::default();

    // One step takes the only permit and parks at its first content read.
    blocking.block_next();
    let mut holding_step = Box::pin(worker.build_step(&holding, policy));
    tokio::select! {
        outcome = &mut holding_step => panic!("the holding step finished while its read was held: {outcome:?}"),
        () = blocking.wait_until_blocked() => {}
    }
    // Another step with work waits for that permit, and is dropped waiting.
    bounded(
        Duration::from_millis(200),
        worker.build_step(&waiting, policy),
    )
    .await
    .expect_err("a step with work waits while the only permit is held");
    drop(holding_step);
    blocking.release();

    let outcome = bounded(Duration::from_secs(10), worker.build_step(&waiting, policy))
        .await
        .expect("both dropped steps returned the permit")
        .expect("build step");
    assert_matches!(outcome, GrepBuildOutcome::Published { .. });
}

#[tokio::test]
async fn a_step_dropped_while_waiting_leaves_the_counts_at_zero() {
    let directory = tempdir().expect("directory");
    let (blocking, writer, host) = blocking_setup(&directory).await;
    let budget = GrepStepBudget::new(NonZeroUsize::MIN);
    let worker = worker_with(&host, &budget);
    let holding = namespace_with_files(&writer, "holding").await;
    let waiting = namespace_with_files(&writer, "waiting").await;
    for namespace_id in [&holding, &waiting] {
        worker.enable(namespace_id).await.expect("enable grep");
    }

    let holding_step = parked_build_step(&blocking, &worker, &holding).await;
    let mut waiting_step = Box::pin(worker.build_step(&waiting, GramIndexBuildPolicy::default()));
    drive_until_reported(
        &mut waiting_step,
        &budget,
        GrepStepBudgetStats {
            running: 1,
            waiting: 1,
        },
    )
    .await;
    drop(waiting_step);
    assert_eq!(
        budget.stats(),
        GrepStepBudgetStats {
            running: 1,
            waiting: 0
        }
    );

    blocking.release();
    let outcome = holding_step.await.expect("holding step");
    assert_matches!(outcome, GrepBuildOutcome::Published { .. });
    assert_eq!(budget.stats(), GrepStepBudgetStats::default());
}
