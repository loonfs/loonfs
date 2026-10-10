//! One execution budget shared by runtimes over different stores: its
//! limits hold across runtimes, admission totals are shared while each
//! store's namespaces are charged on their own, idle runtimes hold nothing
//! back, a runtime that leaves gives everything back, and every wait can be
//! dropped.

#![allow(clippy::panic)]

use super::{ExecutionBudget, ExecutionBudgetStats};
use crate::metrics::{DefaultMetricsRecorder, MetricValue};
use crate::{
    CommitId, CreateDirectoryOptions, ErrorCode, LoonFs, LoonFsBuilder, Namespace, NamespaceId,
    PublicationLimits, Writable,
};
use loonfs_objectstore::layout::{parse_object_key, DurableObjectFamily};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::ids::{namespace_id, test_actor, writer_id};
use loonfs_test_support::stores::{BlockingStore, KeyPredicate, OperationClass};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tempfile::{tempdir, TempDir};
use tokio::task::JoinHandle;
use tokio::time::timeout;

const TWO: NonZeroUsize = NonZeroUsize::MIN.saturating_add(1);

/// A writable runtime over a local store of its own. While the store is
/// armed, every WAL object and metadata segment write parks, so a
/// publication, a fold, or a merge that starts holds its permit until the
/// store is released.
struct GatedRuntime {
    _root: TempDir,
    store: Arc<BlockingStore<LocalFsStore>>,
    runtime: LoonFs<Writable>,
}

async fn gated_runtime(
    configure: impl FnOnce(LoonFsBuilder<Writable>) -> LoonFsBuilder<Writable>,
) -> GatedRuntime {
    let root = tempdir().expect("tempdir");
    let store = Arc::new(BlockingStore::new(
        LocalFsStore::new(root.path()).expect("create local-fs store"),
        KeyPredicate::new(|key| {
            parse_object_key(key).is_some_and(|parsed| {
                matches!(
                    parsed.family(),
                    DurableObjectFamily::WalObject | DurableObjectFamily::MetadataSegment
                )
            })
        }),
        OperationClass::Put,
    ));
    let runtime = configure(LoonFs::builder_with_store(store.clone()).writer_id("budget-writer"))
        .build()
        .await
        .expect("build writer");
    GatedRuntime {
        _root: root,
        store,
        runtime,
    }
}

async fn sharing(budget: &ExecutionBudget) -> GatedRuntime {
    gated_runtime(|builder| builder.execution_budget(budget.clone())).await
}

impl GatedRuntime {
    async fn namespace(&self, name: &str) -> Namespace<Writable> {
        let namespace_id = namespace_id(name);
        self.runtime
            .create_namespace(&namespace_id, &test_actor())
            .await
            .expect("create namespace");
        self.runtime
            .open_namespace(&namespace_id)
            .expect("open namespace")
    }

    /// Creates `name` with two delta runs, so a merge is due, and one file in
    /// its WAL tail, so a fold is due.
    async fn namespace_with_work(&self, name: &str) -> NamespaceId {
        let namespace_id = namespace_id(name);
        self.runtime
            .create_namespace(&namespace_id, &test_actor())
            .await
            .expect("create namespace");
        let namespace = self
            .runtime
            .open_namespace(&namespace_id)
            .expect("open namespace");
        let maintenance = self.runtime.maintenance(writer_id("budget-setup"));
        for index in 0..2 {
            namespace
                .put_file(&format!("/folded-{index}"), b"body", &test_actor())
                .await
                .expect("put a file");
            maintenance
                .fold_wal(&namespace_id)
                .await
                .expect("fold the file into a delta run");
        }
        namespace
            .put_file("/tail", b"body", &test_actor())
            .await
            .expect("put the file that stays in the tail");
        namespace_id
    }

    fn start_fold(&self, namespace_id: &NamespaceId) -> JoinHandle<()> {
        let maintenance = self.runtime.maintenance(writer_id("budget-folds"));
        let namespace_id = namespace_id.clone();
        tokio::spawn(async move {
            maintenance
                .fold_wal(&namespace_id)
                .await
                .expect("fold the tail");
        })
    }

    fn start_merge(&self, namespace_id: &NamespaceId) -> JoinHandle<()> {
        let maintenance = self.runtime.maintenance(writer_id("budget-merges"));
        let namespace_id = namespace_id.clone();
        tokio::spawn(async move {
            maintenance
                .compact_metadata(&namespace_id)
                .await
                .expect("merge the delta runs");
        })
    }
}

/// Creates `/directory` under one fixed commit id, so every call admits a
/// request of the same estimated size.
async fn create_directory(namespace: &Namespace<Writable>) -> crate::Result<()> {
    let mut options = CreateDirectoryOptions::default();
    options.commit.commit_id = Some(CommitId::parse("directory").expect("valid commit id"));
    namespace
        .create_directory_with_options("/directory", &test_actor(), &options)
        .await
        .map(drop)
}

fn start_directory(namespace: &Namespace<Writable>) -> JoinHandle<()> {
    let namespace = namespace.clone();
    tokio::spawn(async move {
        create_directory(&namespace)
            .await
            .expect("create the directory");
    })
}

async fn wait_until(budget: &ExecutionBudget, done: impl Fn(ExecutionBudgetStats) -> bool) {
    if timeout(Duration::from_secs(10), async {
        while !done(budget.stats()) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_err()
    {
        panic!(
            "the budget never reached the expected state: {:?}",
            budget.stats()
        );
    }
}

async fn wait_for_stats(budget: &ExecutionBudget, expected: ExecutionBudgetStats) {
    wait_until(budget, |stats| stats == expected).await;
}

/// Waits until the budget matches `expected` in everything but the admitted
/// bytes, which depend on request encodings, and the read working bytes,
/// which a running merge reserves for its input blocks after it takes its
/// permit and keeps while its write is parked. Callers that finish their
/// work check that both return to zero with [`wait_for_stats`].
async fn wait_for_counts(budget: &ExecutionBudget, expected: ExecutionBudgetStats) {
    wait_until(budget, |stats| {
        ExecutionBudgetStats {
            admitted_bytes: expected.admitted_bytes,
            read_working_bytes: expected.read_working_bytes,
            ..stats
        } == expected
    })
    .await;
}

async fn finish(work: Vec<JoinHandle<()>>) {
    for task in work {
        timeout(Duration::from_secs(30), task)
            .await
            .expect("the work finishes")
            .expect("join the work");
    }
}

/// Samples the budget until the returned flag is set, and returns the most
/// running publications, folds, and merges it saw.
fn watch_peak_running(
    budget: &ExecutionBudget,
) -> (Arc<AtomicBool>, JoinHandle<(usize, usize, usize)>) {
    let stop = Arc::new(AtomicBool::new(false));
    let watcher = tokio::spawn({
        let budget = budget.clone();
        let stop = Arc::clone(&stop);
        async move {
            let mut peak = (0, 0, 0);
            while !stop.load(Ordering::SeqCst) {
                let stats = budget.stats();
                peak.0 = peak.0.max(stats.publications_running);
                peak.1 = peak.1.max(stats.folds_running);
                peak.2 = peak.2.max(stats.compactions_running);
                tokio::task::yield_now().await;
            }
            peak
        }
    });
    (stop, watcher)
}

pub(super) fn gauge(recorder: &DefaultMetricsRecorder, name: &str) -> i64 {
    let snapshot = recorder.snapshot();
    let entry = snapshot
        .by_name(name)
        .next()
        .unwrap_or_else(|| panic!("no `{name}` gauge registered"));
    match entry.value {
        MetricValue::Gauge(value) => value,
        ref other => panic!("expected a gauge, found {other:?}"),
    }
}

fn admission_rejections(recorder: &DefaultMetricsRecorder) -> u64 {
    let snapshot = recorder.snapshot();
    let entry = snapshot
        .by_name("loonfs.execution_budget.admission_rejections")
        .next()
        .expect("the admission refusal counter is registered");
    match entry.value {
        MetricValue::Counter(value) => value,
        ref other => panic!("expected a counter, found {other:?}"),
    }
}

/// The estimated bytes one [`create_directory`] request charges, read from a
/// budget of its own.
async fn admitted_bytes_of_one_directory() -> usize {
    let budget = ExecutionBudget::default();
    let probe = sharing(&budget).await;
    let documents = probe.namespace("documents").await;
    probe.store.arm();
    let work = start_directory(&documents);
    wait_until(&budget, |stats| stats.admitted_requests == 1).await;
    let bytes = budget.stats().admitted_bytes;
    probe.store.release();
    finish(vec![work]).await;
    probe.runtime.shutdown().await.expect("shut down");
    bytes
}

#[tokio::test]
async fn runtimes_sharing_a_budget_never_exceed_its_limits() {
    let budget = ExecutionBudget::builder()
        .max_concurrent_publications(TWO)
        .max_concurrent_folds(TWO)
        .max_concurrent_compactions(TWO)
        .build();
    let mut runtimes = Vec::new();
    for _ in 0..4 {
        runtimes.push(sharing(&budget).await);
    }
    let mut namespaces = Vec::new();
    let mut publishing = Vec::new();
    for (index, gated) in runtimes.iter().enumerate() {
        for name in ["a", "b"] {
            namespaces.push((
                gated,
                gated.namespace_with_work(&format!("{name}-{index}")).await,
            ));
            publishing.push(gated.namespace(&format!("published-{name}-{index}")).await);
        }
    }

    for gated in &runtimes {
        gated.store.arm();
    }
    let (stop, watcher) = watch_peak_running(&budget);
    let mut work = Vec::new();
    for (gated, namespace_id) in &namespaces {
        work.push(gated.start_fold(namespace_id));
        work.push(gated.start_merge(namespace_id));
    }
    for namespace in &publishing {
        work.push(start_directory(namespace));
    }
    wait_for_counts(
        &budget,
        ExecutionBudgetStats {
            admitted_requests: 8,
            publications_running: 2,
            publications_waiting: 6,
            folds_running: 2,
            folds_waiting: 6,
            compactions_running: 2,
            compactions_waiting: 6,
            ..ExecutionBudgetStats::default()
        },
    )
    .await;

    for gated in &runtimes {
        gated.store.release();
    }
    finish(work).await;
    stop.store(true, Ordering::SeqCst);
    let (peak_publications, peak_folds, peak_compactions) =
        watcher.await.expect("join the watcher");
    assert!(
        peak_publications <= 2,
        "{peak_publications} publications ran at once"
    );
    assert!(peak_folds <= 2, "{peak_folds} folds ran at once");
    assert!(
        peak_compactions <= 2,
        "{peak_compactions} merges ran at once"
    );
    wait_for_stats(&budget, ExecutionBudgetStats::default()).await;
    for gated in &runtimes {
        gated.runtime.shutdown().await.expect("shut down");
    }
}

#[tokio::test]
async fn an_idle_runtime_strands_no_capacity() {
    let budget = ExecutionBudget::builder()
        .max_concurrent_publications(TWO)
        .build();
    let mut idle = Vec::new();
    for index in 0..3 {
        let gated = sharing(&budget).await;
        let namespace_id = namespace_id(&format!("idle-{index}"));
        gated
            .runtime
            .create_namespace(&namespace_id, &test_actor())
            .await
            .expect("create namespace");
        let session = gated
            .runtime
            .open_namespace(&namespace_id)
            .expect("open namespace");
        session
            .put_file("/idle", b"body", &test_actor())
            .await
            .expect("put a file");
        idle.push((gated, session));
    }
    let busy = sharing(&budget).await;
    let first = busy.namespace_with_work("busy-a").await;
    let second = busy.namespace_with_work("busy-b").await;
    let publishing = [
        busy.namespace("busy-published-a").await,
        busy.namespace("busy-published-b").await,
    ];

    busy.store.arm();
    let mut work = vec![busy.start_fold(&first), busy.start_fold(&second)];
    work.extend(publishing.iter().map(start_directory));
    wait_for_counts(
        &budget,
        ExecutionBudgetStats {
            admitted_requests: 2,
            publications_running: 2,
            folds_running: 2,
            ..ExecutionBudgetStats::default()
        },
    )
    .await;

    busy.store.release();
    finish(work).await;
    wait_for_stats(&budget, ExecutionBudgetStats::default()).await;
    for (gated, _session) in &idle {
        gated.runtime.shutdown().await.expect("shut down");
    }
    busy.runtime.shutdown().await.expect("shut down");
}

#[tokio::test]
async fn shutting_down_one_runtime_leaves_the_budget_usable() {
    let budget = ExecutionBudget::default();
    let holder = sharing(&budget).await;
    let holding = [
        holder.namespace_with_work("held-a").await,
        holder.namespace_with_work("held-b").await,
    ];
    let leaving = sharing(&budget).await;
    let leaving_namespace = leaving.namespace_with_work("leaving").await;
    let other = sharing(&budget).await;
    let others = [
        other.namespace_with_work("other-a").await,
        other.namespace_with_work("other-b").await,
    ];

    holder.store.arm();
    let held = holding
        .iter()
        .map(|namespace_id| holder.start_fold(namespace_id))
        .collect::<Vec<_>>();
    wait_for_stats(
        &budget,
        ExecutionBudgetStats {
            folds_running: 2,
            ..ExecutionBudgetStats::default()
        },
    )
    .await;
    let waiting = leaving.start_fold(&leaving_namespace);
    wait_for_stats(
        &budget,
        ExecutionBudgetStats {
            folds_running: 2,
            folds_waiting: 1,
            ..ExecutionBudgetStats::default()
        },
    )
    .await;
    let GatedRuntime {
        _root: leaving_root,
        runtime: leaving_runtime,
        ..
    } = leaving;
    leaving_runtime.shutdown().await.expect("shut down");
    drop(leaving_runtime);

    holder.store.release();
    finish(held).await;
    finish(vec![waiting]).await;
    drop(leaving_root);

    other.store.arm();
    let work = others
        .iter()
        .map(|namespace_id| other.start_fold(namespace_id))
        .collect::<Vec<_>>();
    wait_for_stats(
        &budget,
        ExecutionBudgetStats {
            folds_running: 2,
            ..ExecutionBudgetStats::default()
        },
    )
    .await;
    other.store.release();
    finish(work).await;
    assert_eq!(budget.stats(), ExecutionBudgetStats::default());
    holder.runtime.shutdown().await.expect("shut down");
    other.runtime.shutdown().await.expect("shut down");
}

#[tokio::test]
async fn a_runtime_without_a_budget_reports_the_default_limits_to_its_recorder() {
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let gated = gated_runtime(|builder| builder.metrics_recorder(recorder.clone())).await;
    let mut namespaces = Vec::new();
    for index in 0..3 {
        namespaces.push(gated.namespace_with_work(&format!("private-{index}")).await);
    }

    gated.store.arm();
    let mut work = Vec::new();
    for namespace_id in &namespaces {
        work.push(gated.start_fold(namespace_id));
        work.push(gated.start_merge(namespace_id));
    }
    let expected = ExecutionBudgetStats {
        folds_running: crate::DEFAULT_MAX_CONCURRENT_FOLDS,
        folds_waiting: namespaces.len() - crate::DEFAULT_MAX_CONCURRENT_FOLDS,
        compactions_running: crate::DEFAULT_MAX_CONCURRENT_COMPACTIONS,
        compactions_waiting: namespaces.len() - crate::DEFAULT_MAX_CONCURRENT_COMPACTIONS,
        ..ExecutionBudgetStats::default()
    };
    wait_for_stats(gated.runtime.execution_budget(), expected).await;
    let gauges = || {
        [
            "loonfs.execution_budget.admitted_requests",
            "loonfs.execution_budget.admitted_bytes",
            "loonfs.execution_budget.publications_running",
            "loonfs.execution_budget.publications_waiting",
            "loonfs.execution_budget.folds_running",
            "loonfs.execution_budget.folds_waiting",
            "loonfs.execution_budget.compactions_running",
            "loonfs.execution_budget.compactions_waiting",
        ]
        .map(|name| gauge(&recorder, name))
    };
    assert_eq!(
        gauges(),
        [
            expected.admitted_requests,
            expected.admitted_bytes,
            expected.publications_running,
            expected.publications_waiting,
            expected.folds_running,
            expected.folds_waiting,
            expected.compactions_running,
            expected.compactions_waiting,
        ]
        .map(|count| i64::try_from(count).expect("a small count"))
    );
    assert_eq!(admission_rejections(&recorder), 0);

    gated.store.release();
    finish(work).await;
    assert_eq!(gauges(), [0; 8]);
    gated.runtime.shutdown().await.expect("shut down");
}

#[tokio::test]
async fn a_dropped_wait_gives_up_its_place_and_its_count() {
    let budget = ExecutionBudget::builder()
        .max_concurrent_folds(NonZeroUsize::MIN)
        .build();
    let held = budget.fold_permit().await;
    let mut dropped = Box::pin(budget.fold_permit());
    assert!(futures::poll!(dropped.as_mut()).is_pending());
    let mut kept = Box::pin(budget.fold_permit());
    assert!(futures::poll!(kept.as_mut()).is_pending());
    assert_eq!(
        budget.stats(),
        ExecutionBudgetStats {
            folds_running: 1,
            folds_waiting: 2,
            ..ExecutionBudgetStats::default()
        }
    );

    drop(dropped);
    assert_eq!(budget.stats().folds_waiting, 1);
    drop(held);
    let kept = timeout(Duration::from_secs(10), kept)
        .await
        .expect("the wait behind the dropped one takes the permit");
    assert_eq!(
        budget.stats(),
        ExecutionBudgetStats {
            folds_running: 1,
            ..ExecutionBudgetStats::default()
        }
    );
    drop(kept);
    assert_eq!(budget.stats(), ExecutionBudgetStats::default());
}

#[tokio::test]
async fn admitted_bytes_are_shared_and_refunded() {
    let charge = admitted_bytes_of_one_directory().await;
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let budget = ExecutionBudget::builder()
        .max_admitted_bytes(NonZeroUsize::new(charge).expect("a request charges bytes"))
        .metrics_recorder(recorder.clone())
        .build();
    let filling = sharing(&budget).await;
    let refused = sharing(&budget).await;
    let filling_documents = filling.namespace("documents").await;
    let refused_documents = refused.namespace("documents").await;

    filling.store.arm();
    let held = start_directory(&filling_documents);
    wait_for_counts(
        &budget,
        ExecutionBudgetStats {
            admitted_requests: 1,
            publications_running: 1,
            ..ExecutionBudgetStats::default()
        },
    )
    .await;
    assert_eq!(budget.stats().admitted_bytes, charge);
    let error = create_directory(&refused_documents)
        .await
        .expect_err("the other runtime filled the shared total");
    assert_eq!(error.code(), ErrorCode::CommitQueueFull);
    assert_eq!(admission_rejections(&recorder), 1);
    assert_eq!(budget.stats().admitted_requests, 1);

    filling.store.release();
    finish(vec![held]).await;
    wait_for_stats(&budget, ExecutionBudgetStats::default()).await;
    create_directory(&refused_documents)
        .await
        .expect("the refunded total admits the other runtime");
    wait_for_stats(&budget, ExecutionBudgetStats::default()).await;
    filling.runtime.shutdown().await.expect("shut down");
    refused.runtime.shutdown().await.expect("shut down");
}

#[tokio::test]
async fn one_namespace_id_in_two_stores_is_charged_separately() {
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let budget = ExecutionBudget::builder()
        .metrics_recorder(recorder.clone())
        .build();
    let one_request_per_namespace = || PublicationLimits {
        max_requests_per_namespace: NonZeroUsize::MIN,
        ..PublicationLimits::default()
    };
    let first = gated_runtime(|builder| {
        builder
            .execution_budget(budget.clone())
            .publication_limits(one_request_per_namespace())
    })
    .await;
    let second = gated_runtime(|builder| {
        builder
            .execution_budget(budget.clone())
            .publication_limits(one_request_per_namespace())
    })
    .await;
    let first_documents = first.namespace("documents").await;
    let second_documents = second.namespace("documents").await;

    first.store.arm();
    second.store.arm();
    let mut work = vec![start_directory(&first_documents)];
    wait_until(&budget, |stats| stats.publications_running == 1).await;
    let error = create_directory(&first_documents)
        .await
        .expect_err("the first store's namespace is at its limit");
    assert_eq!(error.code(), ErrorCode::CommitQueueFull);
    work.push(start_directory(&second_documents));
    wait_for_counts(
        &budget,
        ExecutionBudgetStats {
            admitted_requests: 2,
            publications_running: 2,
            ..ExecutionBudgetStats::default()
        },
    )
    .await;
    let error = create_directory(&second_documents)
        .await
        .expect_err("the second store's namespace is at its own limit");
    assert_eq!(error.code(), ErrorCode::CommitQueueFull);
    assert_eq!(budget.stats().admitted_requests, 2);
    assert_eq!(
        admission_rejections(&recorder),
        0,
        "a namespace refusal is not a refusal at the total"
    );

    first.store.release();
    second.store.release();
    finish(work).await;
    wait_for_stats(&budget, ExecutionBudgetStats::default()).await;
    first.runtime.shutdown().await.expect("shut down");
    second.runtime.shutdown().await.expect("shut down");
}

#[test]
fn content_merges_share_the_configured_pool() {
    let default = ExecutionBudget::default();
    assert_eq!(
        default.content_merge_memory().available_permits(),
        128 * 1024 * 1024
    );
    let budget = ExecutionBudget::builder()
        .max_content_merge_bytes(64 * 1024 * 1024)
        .build();
    let pool = budget.content_merge_memory();
    let shared = budget.clone().content_merge_memory();
    assert!(Arc::ptr_eq(&pool, &shared));
    let permits = pool.try_acquire_many(32 * 1024 * 1024).expect("one merge");
    assert_eq!(shared.available_permits(), 32 * 1024 * 1024);
    drop(permits);
    assert_eq!(shared.available_permits(), 64 * 1024 * 1024);
    assert_eq!(
        ExecutionBudget::builder()
            .max_content_merge_bytes(1)
            .build()
            .content_merge_memory()
            .available_permits(),
        32 * 1024 * 1024
    );
}
