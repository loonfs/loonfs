//! Grep's publications in the namespace manifest: the steps of one interval
//! publish once, a lost put is published over a manifest that copied grep's
//! entry and publishes nothing over another change of the entry, and an
//! unknown outcome keeps what the put may list.

use crate::common::{
    control, fold_wal_and_advance_retention, page_request, writable_namespace, writer_session,
    GrepHost,
};
use loonfs::{LoonFs, NamespaceSessionStatus, SharedObjectStore, Writable};
use loonfs_grep::entry::GrepIndexStatus;
use loonfs_grep::{
    GramIndexBuildPolicy, GrepBuildOutcome, GrepDisableOutcome, GrepEnableOutcome, GrepError,
    GrepPublicationStats, GrepPublishOutcome,
};
use loonfs_objectstore::keys::metadata_manifest_prefix;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::ids::namespace_id;
use loonfs_test_support::stores::{
    BlockingStore, FailStore, InjectedError, KeyPredicate, OperationSelector,
};
use loonfs_types::{ErrorCode, GrepRequest, GrepScope, ManifestNo, NamespaceId};
use std::assert_matches;
use std::num::NonZeroUsize;
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
    let (writer, namespace) = writable_namespace(store, namespace_id).await;
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

/// Checks that the steps of a run publish once, when the run publishes, and
/// that a query reads the index that its view's manifest lists, so the run's
/// pending steps aren't searchable until then.
#[tokio::test]
async fn the_steps_of_a_run_publish_once_and_a_query_reads_the_published_entry() {
    let directory = tempfile::tempdir().expect("directory");
    let store: SharedObjectStore = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
    let namespace_id = namespace_id("one-publication");
    let _writer = seed(&store, &namespace_id, &["/a", "/b", "/c"]).await;
    let host = GrepHost::new(&store).await;
    // The held session is the namespace's writer for the whole run, so only
    // the enable claims the namespace.
    let _session = host
        .writer
        .open_namespace(&namespace_id)
        .expect("hold the writer session");
    assert_matches!(
        host.enable_of(&namespace_id).await,
        Ok(GrepEnableOutcome::Enabled { .. })
    );
    let enabled = manifest_no(&store, &namespace_id).await;

    let mut steps = host.steps_of(&namespace_id, u64::MAX);
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
            rebuilt: 0,
            superseded: 0,
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
    let host = GrepHost::new(&store).await;
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
    fold_wal_and_advance_retention(&store, &namespace_id).await;

    let mut steps = host.steps_of(&namespace_id, u64::MAX);
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

/// Checks that an enable whose publication loses its put to another
/// writer's enable returns `writer_fenced`, which the server sends once
/// more in a new session, instead of `Superseded`, and that the other enable
/// is published. The session has its writer epoch before the enable, so the
/// put that loses is the enable's own.
#[tokio::test]
async fn an_enable_that_loses_to_another_writers_enable_is_fenced() {
    let directory = tempfile::tempdir().expect("directory");
    let base: SharedObjectStore = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
    let namespace_id = namespace_id("lost-enable");
    let _writer = seed(&base, &namespace_id, &["/a"]).await;
    let gate = Arc::new(BlockingStore::new(
        base.clone(),
        KeyPredicate::prefix(metadata_manifest_prefix(&namespace_id)),
        OperationSelector::PutCreateIfAbsent,
    ));
    let gated: SharedObjectStore = gate.clone();
    let host = GrepHost::new(&gated).await;
    let rival = GrepHost::new(&base).await;
    let held = writer_session(&gated, &namespace_id);
    held.put_file("/claim", b"claim", &loonfs_test_support::test_actor())
        .await
        .expect("the session claims the namespace with a commit");

    gate.block_next();
    let (lost, ()) = tokio::join!(host.enable_of(&namespace_id), async {
        gate.wait_until_blocked().await;
        assert_matches!(
            rival.enable_of(&namespace_id).await,
            Ok(GrepEnableOutcome::Enabled { .. })
        );
        gate.release();
    });
    let error = lost.expect_err("the rival's claim fences the host's session");
    assert_eq!(error.code(), ErrorCode::WriterFenced, "{error:?}");
    drop(held);
    assert_matches!(
        host.worker.lifecycle(&namespace_id).await,
        Ok(GrepIndexStatus::Backfilling { .. }),
        "the rival's enable is published"
    );
}

/// Checks that a run of steps on server A, after another server claimed the
/// namespace and published another grep entry with a backfill to build, ends
/// fenced before it builds
/// anything: A's first step loads its basis from A's position, the check of
/// that position finds the other server's manifests, and the writer's load
/// returns `writer_fenced`. The step writes no segment, A's session is
/// fenced, and an enable on it returns `writer_fenced`, which the server
/// sends once more in a new session, instead of `grep_publication_conflict`.
#[tokio::test]
async fn a_run_of_steps_that_another_writer_displaced_ends_fenced() {
    let directory = tempfile::tempdir().expect("directory");
    let base: SharedObjectStore = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
    let namespace_id = namespace_id("displaced-steps");
    let _writer = seed(&base, &namespace_id, &["/a", "/b"]).await;
    let host = GrepHost::new(&base).await;
    let held = writer_session(&base, &namespace_id);
    assert_matches!(
        host.worker.enable(&held).await,
        Ok(GrepEnableOutcome::Enabled { .. })
    );

    let rival_store: SharedObjectStore =
        Arc::new(LocalFsStore::new(directory.path()).expect("store"));
    let rival = GrepHost::new(&rival_store).await;
    assert_matches!(
        rival.disable_of(&namespace_id).await,
        Ok(GrepDisableOutcome::Disabled),
        "server B claims the namespace"
    );
    assert_matches!(
        rival.enable_of(&namespace_id).await,
        Ok(GrepEnableOutcome::Enabled { .. }),
        "server B publishes another entry, whose backfill has work to build"
    );
    let segments = directory
        .path()
        .join(loonfs_objectstore::keys::extension_segment_prefix(
            &namespace_id,
            &loonfs_grep::entry::grep_extension_name(),
        ));
    let before = file_count(&segments);
    let manifest = control::metadata_manifest(&base, &namespace_id).await;

    let mut steps = host.worker.steps(&held, u64::MAX);
    let error = steps
        .step(one_file_steps())
        .await
        .expect_err("A's step finds B's manifests above its position");
    assert_eq!(error.code(), ErrorCode::WriterFenced, "{error:?}");
    assert_eq!(held.session_status(), NamespaceSessionStatus::Fenced);
    assert_eq!(file_count(&segments), before, "the step writes no segment");
    assert_eq!(
        control::metadata_manifest(&base, &namespace_id)
            .await
            .payload(),
        manifest.payload()
    );
    let error = host
        .worker
        .enable(&held)
        .await
        .expect_err("an enable on the fenced session");
    assert_eq!(error.code(), ErrorCode::WriterFenced, "{error:?}");
}

/// Returns the number of files under `directory`, or 0 when it doesn't
/// exist.
fn file_count(directory: &std::path::Path) -> usize {
    let Ok(entries) = std::fs::read_dir(directory) else {
        return 0;
    };
    entries
        .map(|entry| {
            let entry = entry.expect("a directory entry");
            if entry.file_type().expect("a file type").is_dir() {
                file_count(&entry.path())
            } else {
                1
            }
        })
        .sum()
}

/// Checks that an enable whose put has an unknown outcome returns
/// `outcome_unknown`.
#[tokio::test]
async fn an_enable_whose_put_has_an_unknown_outcome_fails() {
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
    let failing = Arc::new(failing);
    let shared: SharedObjectStore = failing.clone();
    let host = GrepHost::new(&shared).await;
    // The session claims the namespace first, so the put that fails is the
    // enable's own.
    let held = writer_session(&shared, &namespace_id);
    held.put_file("/claim", b"claim", &loonfs_test_support::test_actor())
        .await
        .expect("the session claims the namespace with a commit");
    failing.fail_next(1);

    let outcome = host.enable_of(&namespace_id).await;
    drop(held);

    assert_matches!(outcome, Err(error) if error.code() == ErrorCode::OutcomeUnknown);
    assert_matches!(host.worker.entry(&namespace_id).await, Ok(None));
}

/// Checks that a step whose publication loses its put to the session's own
/// retention window change, a manifest that copied grep's entry, is
/// published over that manifest with the winner's other fields.
#[tokio::test]
async fn a_step_that_loses_its_put_to_the_sessions_copy_of_its_entry_is_published_over_it() {
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
    let host = GrepHost::new(&gated).await;
    // The test keeps the session open, so the enable's claim and the step's
    // publication share it.
    let session = host
        .writer
        .open_namespace(&namespace_id)
        .expect("open the session");
    host.worker.enable(&session).await.expect("enable");
    let window_ms = 2 * loonfs::GC_GRACE_WINDOW_MS;

    gate.block_next();
    let (rebuilt, winner) = tokio::join!(
        async {
            host.worker
                .steps(&session, 0)
                .build_step(one_file_steps())
                .await
        },
        async {
            gate.wait_until_blocked().await;
            session
                .set_retention_window(Some(
                    loonfs::RetentionWindow::new(window_ms).expect("a long enough window"),
                ))
                .await
                .expect("the session changes the window");
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
        }),
        "the rebuilt manifest has the step's progress"
    );
    let current = control::metadata_manifest(&base, &namespace_id).await;
    assert_eq!(
        current.payload().manifest_no,
        winner.successor().expect("next")
    );
    assert_eq!(
        current.payload().retention_window_ms,
        std::num::NonZeroU64::new(window_ms),
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

/// Checks that a step whose publication loses its put to the session's own
/// disable publishes nothing and returns `Superseded`, and that the next
/// enable builds from the manifest that won.
#[tokio::test]
async fn a_step_that_loses_its_put_to_the_sessions_disable_publishes_nothing() {
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
    let host = GrepHost::new(&gated).await;
    // The test keeps the session open, so the enable's claim, the step and the
    // disable share it.
    let _session = host
        .writer
        .open_namespace(&namespace_id)
        .expect("open the session");
    host.enable_of(&namespace_id).await.expect("enable");

    gate.block_next();
    let (lost, ()) = tokio::join!(
        async {
            host.steps_of(&namespace_id, 0)
                .build_step(one_file_steps())
                .await
        },
        async {
            gate.wait_until_blocked().await;
            assert_matches!(
                host.disable_of(&namespace_id).await,
                Ok(loonfs_grep::GrepDisableOutcome::Disabled)
            );
            gate.release();
        }
    );
    assert_matches!(lost, Ok(GrepBuildOutcome::Superseded));
    assert_eq!(
        host.worker.publication_stats(),
        GrepPublicationStats {
            published: 2,
            rebuilt: 0,
            superseded: 1,
        },
        "the enable and the disable published, and the step's change was superseded"
    );
    assert_matches!(
        host.worker.lifecycle(&namespace_id).await,
        Ok(GrepIndexStatus::Disabled {})
    );

    let built = host
        .enable_grep_index(&namespace_id)
        .await
        .expect("build from the winner");
    let winner = control::metadata_manifest(&base, &namespace_id).await;
    assert!(built.status.is_built_through(winner.payload().head_seq));
    let found = host
        .grep(&namespace_id, &needle(), page_request(None, 10))
        .await
        .expect("query");
    assert_eq!(found.matches.len(), 2);
}

/// Checks that an enable and a disable whose put loses its response read
/// the number back: they return `Enabled` and `Disabled` when the put was
/// written, and `writer_fenced` when another writer claimed the namespace
/// and published at that number first, which the server sends once more in
/// a new session.
#[tokio::test]
async fn an_enable_or_disable_whose_put_loses_its_response_reads_back_whose_manifest_it_is() {
    for rival in [false, true] {
        let directory = tempfile::tempdir().expect("directory");
        let base: SharedObjectStore = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
        let namespace_id = namespace_id("lost-response");
        let writer = seed(&base, &namespace_id, &["/a"]).await;
        writer.shutdown().await.expect("shutdown");
        let manifests = KeyPredicate::prefix(metadata_manifest_prefix(&namespace_id));
        let failing = FailStore::new(
            base.clone(),
            manifests.clone(),
            OperationSelector::PutCreateIfAbsent,
            InjectedError::Transport("lost response".to_owned()),
        );
        // Without a rival, the put is written and its response is lost. With
        // one, the put waits while the rival publishes, and then fails before
        // it reaches the store.
        let failing = if rival {
            failing
        } else {
            failing.apply_then_fail()
        };
        let gate = Arc::new(BlockingStore::new(
            failing,
            manifests,
            OperationSelector::PutCreateIfAbsent,
        ));
        let gated: SharedObjectStore = gate.clone();
        let host = GrepHost::new(&gated).await;
        let rival_store: SharedObjectStore =
            Arc::new(LocalFsStore::new(directory.path()).expect("store"));
        let other = GrepHost::new(&rival_store).await;
        // The host's session claims the namespace first, so the injected
        // failures hit the grep publications' puts.
        let held = writer_session(&gated, &namespace_id);
        held.put_file("/claim", b"claim", &loonfs_test_support::test_actor())
            .await
            .expect("the session claims the namespace with a commit");
        let arm = || {
            gate.inner().fail_next(1);
            if rival {
                gate.block_next();
            }
        };

        arm();
        let (enabled, ()) = tokio::join!(host.enable_of(&namespace_id), async {
            if rival {
                gate.wait_until_blocked().await;
                assert_matches!(
                    other.enable_of(&namespace_id).await,
                    Ok(GrepEnableOutcome::Enabled { .. })
                );
                gate.release();
            }
        });
        if rival {
            let error = enabled.expect_err("the rival's claim fences the session");
            assert_eq!(error.code(), ErrorCode::WriterFenced, "{error:?}");
            assert_eq!(gate.inner().remaining(), 0);
            continue;
        }
        assert_matches!(enabled, Ok(GrepEnableOutcome::Enabled { .. }));
        arm();
        assert_matches!(
            host.disable_of(&namespace_id).await,
            Ok(GrepDisableOutcome::Disabled)
        );
        assert_eq!(gate.inner().remaining(), 0);
        drop(held);
    }
}
