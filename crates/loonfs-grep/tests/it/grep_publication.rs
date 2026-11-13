//! Grep's publications in the namespace manifest: the steps of one interval
//! publish once, a lost put is published over a manifest that copied grep's
//! entry and publishes nothing over another change of the entry, and an
//! unknown outcome keeps what the put may list.

use crate::common::{control, fold_wal_and_advance_retention, page_request, GrepHost};
use loonfs::{LoonFs, SharedObjectStore, Writable};
use loonfs_grep::entry::GrepIndexStatus;
use loonfs_grep::{
    GramIndexBuildPolicy, GrepBuildOutcome, GrepEnableOutcome, GrepError, GrepPublicationStats,
    GrepPublishOutcome,
};
use loonfs_objectstore::keys::metadata_manifest_prefix;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::ids::namespace_id;
use loonfs_test_support::stores::{
    BlockingStore, FailStore, InjectedError, KeyPredicate, OperationSelector,
};
use loonfs_types::{ErrorCode, GrepRequest, GrepScope, ManifestNo, NamespaceId};
use std::assert_matches;
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;

fn needle() -> GrepRequest {
    GrepRequest {
        pattern: "needle".to_owned(),
        case_insensitive: false,
        scope: GrepScope::Namespace,
        allow_stale: false,
        allow_scan: false,
    }
}

/// A build policy whose steps each examine one file.
fn one_file_steps() -> GramIndexBuildPolicy {
    GramIndexBuildPolicy {
        max_files_per_step: NonZeroUsize::MIN,
        ..GramIndexBuildPolicy::default()
    }
}

/// Creates `namespace_id` over `store` with a file for each of `paths`, and
/// returns the writer.
async fn seed(
    store: &SharedObjectStore,
    namespace_id: &NamespaceId,
    paths: &[&str],
) -> LoonFs<Writable> {
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("publication-seed"))
        .config(loonfs::RuntimeConfig {
            min_publish_interval_ms: 0,
            ..loonfs::RuntimeConfig::default()
        })
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("namespace");
    let namespace = writer.open_namespace(namespace_id).expect("open namespace");
    for path in paths {
        namespace
            .put_file(path, b"needle\n", &loonfs_test_support::test_actor())
            .await
            .expect("file");
    }
    writer
}

async fn manifest_no(store: &SharedObjectStore, namespace_id: &NamespaceId) -> ManifestNo {
    control::metadata_manifest(store, namespace_id)
        .await
        .payload()
        .manifest_no
}

/// Sets the namespace's retention window through another runtime, which
/// publishes the next manifest.
async fn publish_rival(store: &SharedObjectStore, namespace_id: &NamespaceId, window_ms: u64) {
    let rival = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("rival"))
        .build()
        .await
        .expect("rival");
    rival
        .maintenance(loonfs_test_support::ids::writer_id("rival"))
        .set_retention_window(namespace_id, NonZeroU64::new(window_ms))
        .await
        .expect("the rival publishes");
}

/// Checks that the steps of a run publish once, when the run publishes, and
/// that a query reads the index that its view's manifest lists, so the run's
/// pending steps aren't searchable until then.
#[tokio::test]
async fn the_steps_of_a_run_publish_once_and_a_query_reads_the_published_entry() {
    let directory = tempfile::tempdir().expect("directory");
    let store: SharedObjectStore = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
    let namespace_id = namespace_id("one-publication");
    let _writer = seed(&store, &namespace_id, &["/a", "/b", "/c"]).await;
    let host = GrepHost::new(&store, "one-publication").await;
    assert_matches!(
        host.worker.enable(&namespace_id).await,
        Ok(GrepEnableOutcome::Enabled { .. })
    );
    let enabled = manifest_no(&store, &namespace_id).await;

    let mut steps = host.worker.steps(&namespace_id, u64::MAX);
    let mut indexed = 0;
    loop {
        match steps.build_step(one_file_steps()).await.expect("step") {
            GrepBuildOutcome::Indexed { .. } => indexed += 1,
            GrepBuildOutcome::UpToDate { .. } => break,
            other => panic!("unexpected build outcome {other:?}"),
        }
    }
    assert!(indexed >= 3, "one step for each file");
    assert_matches!(steps.pending_status(), Some(GrepIndexStatus::Active { .. }));
    assert_eq!(manifest_no(&store, &namespace_id).await, enabled);
    assert_matches!(
        host.worker.lifecycle(&namespace_id).await,
        Ok(GrepIndexStatus::Backfilling { .. })
    );
    assert_matches!(
        host.grep(&namespace_id, &needle(), page_request(None, 10))
            .await,
        Err(GrepError::Backfilling)
    );

    assert_eq!(
        steps.publish().await.expect("publish"),
        GrepPublishOutcome::Published
    );
    assert_eq!(
        manifest_no(&store, &namespace_id).await,
        enabled.successor().expect("next")
    );
    assert_eq!(
        steps.publish().await.expect("publish nothing"),
        GrepPublishOutcome::NothingPending
    );
    assert_eq!(
        host.worker.publication_stats(),
        GrepPublicationStats {
            published: 2,
            ..GrepPublicationStats::default()
        },
        "the enable and the run published, and an empty publication isn't one"
    );
    let found = host
        .grep(&namespace_id, &needle(), page_request(None, 10))
        .await
        .expect("query the published index");
    assert_eq!(found.matches.len(), 3);
}

/// Checks that a rebootstrap and the backfill that it starts publish in one
/// manifest when the backfill finishes within the publish interval, so the
/// published entry follows its predecessor with a later build.
#[tokio::test]
async fn a_restart_and_its_finished_backfill_publish_together() {
    let directory = tempfile::tempdir().expect("directory");
    let store: SharedObjectStore = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
    let namespace_id = namespace_id("restart-in-one-change");
    let writer = seed(&store, &namespace_id, &["/a"]).await;
    let host = GrepHost::new(&store, "restart-in-one-change").await;
    host.enable_grep_index(&namespace_id).await.expect("enable");
    let GrepIndexStatus::Active {
        captured_seq: first_build,
        ..
    } = host.worker.lifecycle(&namespace_id).await.expect("status")
    else {
        panic!("the index is active");
    };
    writer
        .open_namespace(&namespace_id)
        .expect("open namespace")
        .put_file("/b", b"needle\n", &loonfs_test_support::test_actor())
        .await
        .expect("file");
    fold_wal_and_advance_retention(&store, &host.maintenance, &namespace_id).await;

    let mut steps = host.worker.steps(&namespace_id, u64::MAX);
    assert_matches!(
        steps.build_step(one_file_steps()).await,
        Ok(GrepBuildOutcome::BackfillRestarted { .. }),
        "the floor passed the watermark"
    );
    loop {
        match steps.build_step(one_file_steps()).await.expect("step") {
            GrepBuildOutcome::Indexed { .. } => {}
            GrepBuildOutcome::UpToDate { .. } => break,
            other => panic!("unexpected build outcome {other:?}"),
        }
    }
    assert_eq!(
        steps.publish().await.expect("publish"),
        GrepPublishOutcome::Published
    );
    let GrepIndexStatus::Active {
        captured_seq: second_build,
        ..
    } = host.worker.lifecycle(&namespace_id).await.expect("status")
    else {
        panic!("the restarted backfill finished");
    };
    assert!(second_build > first_build);
    let found = host
        .grep(&namespace_id, &needle(), page_request(None, 10))
        .await
        .expect("query the rebuilt index");
    assert_eq!(found.matches.len(), 2);
    assert_eq!(found.built_through_seq, found.head_seq);
}

/// Checks that a step whose publication loses its put to a manifest that
/// copied grep's entry, such as a retention window change, is published over
/// that manifest with the winner's other fields.
#[tokio::test]
async fn a_step_that_loses_its_put_to_a_copy_of_its_entry_is_published_over_it() {
    let directory = tempfile::tempdir().expect("directory");
    let base: SharedObjectStore = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
    let namespace_id = namespace_id("rebuilt-step");
    let _writer = seed(&base, &namespace_id, &["/a", "/b"]).await;
    let gate = Arc::new(BlockingStore::new(
        base.clone(),
        KeyPredicate::prefix(metadata_manifest_prefix(&namespace_id)),
        OperationSelector::PutCreateIfAbsent,
    ));
    let gated: SharedObjectStore = gate.clone();
    let host = GrepHost::new(&gated, "rebuilt-step").await;
    host.worker.enable(&namespace_id).await.expect("enable");

    gate.block_next();
    let (rebuilt, winner) = tokio::join!(
        async {
            host.worker
                .steps(&namespace_id, 0)
                .build_step(one_file_steps())
                .await
        },
        async {
            gate.wait_until_blocked().await;
            publish_rival(&base, &namespace_id, 60_000).await;
            let winner = manifest_no(&base, &namespace_id).await;
            gate.release();
            winner
        }
    );
    assert_matches!(rebuilt, Ok(GrepBuildOutcome::Indexed { .. }));
    assert_matches!(
        host.worker.lifecycle(&namespace_id).await,
        Ok(GrepIndexStatus::Backfilling {
            cursor_inode_id: Some(_),
            ..
        })
    );
    let current = control::metadata_manifest(&base, &namespace_id).await;
    assert_eq!(
        current.payload().manifest_no,
        winner.successor().expect("next")
    );
    assert_eq!(
        current.payload().retention_window_ms,
        NonZeroU64::new(60_000),
        "grep's publications copy the winner's other fields"
    );
    assert_eq!(
        host.worker.publication_stats(),
        GrepPublicationStats {
            published: 1,
            rebuilt: 1,
            superseded: 0,
        },
        "the enable published at its first put, and the step's change was rebuilt"
    );

    host.enable_grep_index(&namespace_id)
        .await
        .expect("finish the build");
    let found = host
        .grep(&namespace_id, &needle(), page_request(None, 10))
        .await
        .expect("query");
    assert_eq!(found.matches.len(), 2);
}

/// Checks that a step whose publication loses its put to another process's
/// change of grep's entry publishes nothing, and that the next step plans
/// from the manifest that won.
#[tokio::test]
async fn a_step_that_loses_its_put_to_another_change_of_its_entry_publishes_nothing() {
    let directory = tempfile::tempdir().expect("directory");
    let base: SharedObjectStore = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
    let namespace_id = namespace_id("lost-step");
    let _writer = seed(&base, &namespace_id, &["/a", "/b"]).await;
    let gate = Arc::new(BlockingStore::new(
        base.clone(),
        KeyPredicate::prefix(metadata_manifest_prefix(&namespace_id)),
        OperationSelector::PutCreateIfAbsent,
    ));
    let gated: SharedObjectStore = gate.clone();
    let host = GrepHost::new(&gated, "lost-step").await;
    let rival = GrepHost::new(&base, "lost-step-rival").await;
    host.worker.enable(&namespace_id).await.expect("enable");

    gate.block_next();
    let (lost, ()) = tokio::join!(
        async {
            host.worker
                .steps(&namespace_id, 0)
                .build_step(one_file_steps())
                .await
        },
        async {
            gate.wait_until_blocked().await;
            assert_matches!(
                rival
                    .worker
                    .steps(&namespace_id, 0)
                    .build_step(one_file_steps())
                    .await,
                Ok(GrepBuildOutcome::Indexed { .. })
            );
            gate.release();
        }
    );
    assert_matches!(lost, Ok(GrepBuildOutcome::Superseded));
    assert_eq!(
        host.worker.publication_stats(),
        GrepPublicationStats {
            published: 1,
            rebuilt: 0,
            superseded: 1,
        }
    );
    let winner = control::metadata_manifest(&base, &namespace_id).await;
    assert_eq!(
        host.worker.entry(&namespace_id).await.expect("entry"),
        control::grep_entry(&base, &namespace_id).await,
        "the step published nothing over the rival's change"
    );

    let built = host
        .enable_grep_index(&namespace_id)
        .await
        .expect("build from the winner");
    assert!(built.status.is_built_through(winner.payload().head_seq));
    let found = host
        .grep(&namespace_id, &needle(), page_request(None, 10))
        .await
        .expect("query");
    assert_eq!(found.matches.len(), 2);
}

/// Checks that an enable whose publication loses its put to another
/// process's enable deletes the backfill snapshot it created, because no
/// manifest lists it.
#[tokio::test]
async fn an_enable_that_loses_to_another_enable_deletes_its_snapshot() {
    let directory = tempfile::tempdir().expect("directory");
    let base: SharedObjectStore = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
    let namespace_id = namespace_id("lost-enable");
    let writer = seed(&base, &namespace_id, &["/a"]).await;
    writer
        .maintenance(loonfs_test_support::ids::writer_id("publication-seed"))
        .fold_wal(&namespace_id)
        .await
        .expect("fold, so the snapshot publishes no manifest");
    let gate = Arc::new(BlockingStore::new(
        base.clone(),
        KeyPredicate::prefix(metadata_manifest_prefix(&namespace_id)),
        OperationSelector::PutCreateIfAbsent,
    ));
    let gated: SharedObjectStore = gate.clone();
    let host = GrepHost::new(&gated, "lost-enable").await;
    let rival = GrepHost::new(&base, "lost-enable-rival").await;

    gate.block_next();
    let (lost, ()) = tokio::join!(host.worker.enable(&namespace_id), async {
        gate.wait_until_blocked().await;
        assert_matches!(
            rival.worker.enable(&namespace_id).await,
            Ok(GrepEnableOutcome::Enabled { .. })
        );
        gate.release();
    });
    assert_matches!(lost, Ok(GrepEnableOutcome::Superseded));
    let Ok(GrepIndexStatus::Backfilling {
        snapshot_id: rival_snapshot_id,
        ..
    }) = host.worker.lifecycle(&namespace_id).await
    else {
        panic!("the rival's enable is published");
    };
    let snapshots = host
        .maintenance
        .list_snapshot_records(&namespace_id)
        .page()
        .await
        .expect("list snapshots")
        .snapshots;
    assert_eq!(
        snapshots
            .iter()
            .map(|snapshot| &snapshot.snapshot_id)
            .collect::<Vec<_>>(),
        [&rival_snapshot_id],
        "the lost enable deleted its own snapshot"
    );
}

/// Checks that an enable whose put has an unknown outcome returns
/// `outcome_unknown` and keeps the backfill snapshot, which the put may
/// have listed.
#[tokio::test]
async fn an_enable_whose_put_has_an_unknown_outcome_fails_and_keeps_its_snapshot() {
    let directory = tempfile::tempdir().expect("directory");
    let base: SharedObjectStore = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
    let namespace_id = namespace_id("unknown-enable");
    let writer = seed(&base, &namespace_id, &[]).await;
    writer.shutdown().await.expect("shutdown");
    let failing = FailStore::new(
        base.clone(),
        KeyPredicate::prefix(metadata_manifest_prefix(&namespace_id)),
        OperationSelector::PutCreateIfAbsent,
        InjectedError::Transport("lost enable request".to_owned()),
    );
    failing.fail_next(1);
    let failing: SharedObjectStore = Arc::new(failing);
    let host = GrepHost::new(&failing, "unknown-enable").await;

    let outcome = host.worker.enable(&namespace_id).await;

    assert_matches!(outcome, Err(error) if error.code() == ErrorCode::OutcomeUnknown);
    assert_matches!(host.worker.entry(&namespace_id).await, Ok(None));
    let snapshots = host
        .maintenance
        .list_snapshot_records(&namespace_id)
        .page()
        .await
        .expect("list snapshots")
        .snapshots;
    assert_eq!(
        snapshots.len(),
        1,
        "an unknown outcome must keep the backfill snapshot the put may list"
    );
}
