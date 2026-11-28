//! A writer session admits a host's maintenance as it admits a commit, and
//! confirms its position before it reports a change done without a put.

use loonfs::{
    ErrorCode, LoonFs, MaintainMetadataOptions, MaintenanceCancellation, Namespace,
    RetentionWindow, SharedObjectStore, Writable, GC_GRACE_WINDOW_MS,
};
use loonfs_core::limits::MAX_TAIL_COMMIT_AGE_MS;
use loonfs_objectstore::keys::metadata_manifest_prefix;
use loonfs_test_support::clock::{bounded, ManualClock};
use loonfs_test_support::ids::{namespace_id, writer_id};
use loonfs_test_support::stores::{
    BlockingStore, KeyPredicate, MemoryStore, OperationSelector, RecordingStore,
};
use loonfs_types::{ChecksumAlgorithm, NamespaceId};
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Duration;

const DAY_MS: u64 = 24 * 60 * 60 * 1000;

async fn runtime(store: &SharedObjectStore, name: &str) -> LoonFs<Writable> {
    LoonFs::builder_with_store(store.clone(), loonfs::Context::Service)
        .writer_id(writer_id(name))
        .config(loonfs::RuntimeConfig {
            min_publish_interval_ms: 0,
            ..loonfs::RuntimeConfig::default()
        })
        .build()
        .await
        .expect("runtime")
}

/// The store of a namespace that runtime `a` created and wrote one file to,
/// and `a`'s handle on its session, which keeps the session and its
/// position.
struct Created {
    recording: Arc<RecordingStore<MemoryStore>>,
    namespace_id: NamespaceId,
    a: LoonFs<Writable>,
    handle: Namespace<Writable>,
}

async fn created(name: &str) -> Created {
    let recording = Arc::new(RecordingStore::new(
        MemoryStore::new(ChecksumAlgorithm::Crc64nvme),
        KeyPredicate::any(),
    ));
    let store: SharedObjectStore = recording.clone();
    let namespace_id = namespace_id(name);
    let a = runtime(&store, &format!("{name}-a")).await;
    a.create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create");
    let handle = a.open_namespace(&namespace_id).expect("open");
    handle
        .put_file("/a", b"a", &loonfs_test_support::test_actor())
        .await
        .expect("write");
    Created {
        recording,
        namespace_id,
        a,
        handle,
    }
}

async fn current_window(store: &SharedObjectStore, namespace_id: &NamespaceId) -> Option<u64> {
    loonfs_core::test_support::namespace_store(store.clone(), namespace_id)
        .load_current_manifest()
        .await
        .expect("current manifest")
        .payload()
        .retention_window_ms
        .map(std::num::NonZeroU64::get)
}

/// Checks that a handle kept past the runtime's shutdown can't change the
/// retention window, fold or compact, and that the attempts send no request.
#[tokio::test]
async fn a_handle_kept_after_shutdown_publishes_nothing() {
    let Created {
        recording,
        a,
        handle: kept,
        ..
    } = created("kept-after-shutdown").await;
    a.shutdown().await.expect("shutdown");
    recording.reset();
    let window = RetentionWindow::new(DAY_MS).expect("window");
    let error = kept
        .set_retention_window(Some(window))
        .await
        .expect_err("a window change after shutdown");
    assert_eq!(error.code(), ErrorCode::ShuttingDown);
    let error = kept.fold_wal().await.expect_err("a fold after shutdown");
    assert_eq!(error.code(), ErrorCode::ShuttingDown);
    let error = kept
        .compact_metadata(&loonfs::MaintenanceCancellation::new())
        .await
        .expect_err("a compaction after shutdown");
    assert_eq!(error.code(), ErrorCode::ShuttingDown);
    assert!(recording.take().is_empty(), "nothing is sent to the store");
}

/// Checks that a clone of a closed session's handle can't publish or claim,
/// and that a new session opened after the close is the namespace's only
/// one: the clone neither fences it nor publishes beside it.
#[tokio::test]
async fn a_clone_of_a_closed_session_publishes_nothing_and_claims_nothing() {
    let Created {
        recording,
        namespace_id,
        a,
        handle,
    } = created("closed-clone").await;
    let clone = handle.clone();
    handle.close().await.expect("close");
    recording.reset();
    let window = RetentionWindow::new(DAY_MS).expect("window");
    let error = clone
        .set_retention_window(Some(window))
        .await
        .expect_err("a window change on a closed session");
    assert_eq!(error.code(), ErrorCode::WriterSessionClosed);
    let error = clone
        .fold_wal()
        .await
        .expect_err("a fold on a closed session");
    assert_eq!(error.code(), ErrorCode::WriterSessionClosed);
    assert!(recording.take().is_empty(), "nothing is sent to the store");

    let reopened = a.open_namespace(&namespace_id).expect("a new session");
    reopened
        .put_file("/b", b"b", &loonfs_test_support::test_actor())
        .await
        .expect("the new session claims and commits");
    let error = clone.fold_wal().await.expect_err("the clone stays closed");
    assert_eq!(error.code(), ErrorCode::WriterSessionClosed);
    reopened
        .put_file("/c", b"c", &loonfs_test_support::test_actor())
        .await
        .expect("nothing fenced the new session");
    a.shutdown().await.expect("shutdown");
}

/// Checks that a session whose position has no retention window, after
/// another writer took the namespace over and set a one-day window, doesn't
/// report a clear of the window done: the clear confirms the position,
/// finds the other writer's manifests, and returns `writer_fenced`, and the
/// clear that the server sends once more in a new session clears the
/// durable window.
#[tokio::test]
async fn a_clear_of_the_window_after_a_takeover_is_fenced_and_its_retry_clears_it() {
    let Created {
        recording,
        namespace_id,
        a,
        handle: a_session,
    } = created("window-takeover").await;
    let store: SharedObjectStore = recording.clone();
    assert!(a_session.has_current_writer_position());
    let b = runtime(&store, "window-takeover-b").await;
    b.open_namespace(&namespace_id)
        .expect("open on b")
        .set_retention_window(Some(RetentionWindow::new(DAY_MS).expect("window")))
        .await
        .expect("b claims and sets a window");
    assert_eq!(current_window(&store, &namespace_id).await, Some(DAY_MS));

    let error = a_session
        .set_retention_window(None)
        .await
        .expect_err("a's position is behind b's manifests");
    assert_eq!(error.code(), ErrorCode::WriterFenced, "{error:?}");
    assert_eq!(current_window(&store, &namespace_id).await, Some(DAY_MS));

    drop(a_session);
    a.wait_for_ended_session(&namespace_id).await;
    a.open_namespace(&namespace_id)
        .expect("a new session")
        .set_retention_window(None)
        .await
        .expect("the retry claims and clears the window");
    assert_eq!(current_window(&store, &namespace_id).await, None);
    b.shutdown().await.expect("shutdown b");
    a.shutdown().await.expect("shutdown a");
}

/// Checks that an extension's change whose basis another namespace's session
/// loaded is an invalid request that the call refuses before it claims
/// anything: it returns `invalid_request`, and nothing is sent for the other
/// namespace.
#[tokio::test]
async fn an_extension_basis_of_another_namespace_is_refused_before_a_claim() {
    let Created {
        recording,
        a,
        handle,
        ..
    } = created("basis-source").await;
    let other = namespace_id("basis-target");
    a.create_namespace(&other, &loonfs_test_support::test_actor())
        .await
        .expect("create the other namespace");
    let basis = handle
        .load_extension_basis()
        .await
        .expect("the source's basis");
    let target = a.open_namespace(&other).expect("open the other namespace");
    recording.reset();
    let name = loonfs::engine::ExtensionName::parse("grep").expect("name");
    let entry = loonfs::engine::ExtensionEntry::new(1, Vec::new(), serde_json::Map::new());
    let error = target
        .publish_extension(&basis, &name, entry)
        .await
        .expect_err("a basis of another namespace");
    assert_eq!(error.code(), ErrorCode::InvalidRequest, "{error:?}");
    let prefix = loonfs_objectstore::keys::namespace_prefix(&other);
    assert!(
        !recording
            .take()
            .iter()
            .any(|operation| operation.key().starts_with(&prefix)),
        "nothing is sent for the other namespace"
    );
    drop((target, handle));
    a.shutdown().await.expect("shutdown");
}

/// Checks that a retention advance of a session whose namespace another
/// runtime deleted returns `writer_fenced`, as a commit does, and writes
/// nothing: the advance checks its position first, as a strong read does,
/// the check finds the manifests that the other runtime's deletion
/// published under a higher writer epoch and drops the position, and the
/// writer's load of the position finds that epoch.
#[tokio::test]
async fn an_advance_after_another_runtimes_deletion_returns_writer_fenced() {
    let Created {
        recording,
        namespace_id,
        a,
        handle,
    } = created("advance-deleted").await;
    let store: SharedObjectStore = recording.clone();
    assert!(handle.has_current_writer_position());
    let b = runtime(&store, "advance-deleted-b").await;
    b.open_namespace(&namespace_id)
        .expect("open on b")
        .delete()
        .await
        .expect("b deletes the namespace");
    recording.reset();
    let error = handle
        .advance_retention_floor()
        .await
        .expect_err("the namespace is deleted");
    assert_eq!(error.code(), ErrorCode::WriterFenced, "{error:?}");
    assert!(
        recording.take().iter().all(|operation| !matches!(
            operation,
            loonfs_test_support::stores::RecordedOperation::Put { .. }
        )),
        "the advance writes nothing"
    );
    drop(handle);
    b.shutdown().await.expect("shutdown b");
    a.shutdown().await.expect("shutdown a");
}

/// A runtime over a store that can park the next put of a namespace's
/// manifest or WAL object, with a manual wall clock, a namespace that it
/// created, and its handle on the namespace's session, which has a writer
/// epoch and a position from a commit.
struct Gated {
    gate: Arc<BlockingStore<MemoryStore>>,
    store: SharedObjectStore,
    clock: Arc<ManualClock>,
    namespace_id: NamespaceId,
    a: LoonFs<Writable>,
    handle: Namespace<Writable>,
}

/// Builds a [`Gated`] whose gate parks puts of `family`, the manifest or the
/// WAL prefix of the namespace.
async fn gated(name: &str, family: fn(&NamespaceId) -> String) -> Gated {
    let namespace_id = namespace_id(name);
    let gate = Arc::new(BlockingStore::new(
        MemoryStore::new(ChecksumAlgorithm::Crc64nvme),
        KeyPredicate::prefix(family(&namespace_id)),
        OperationSelector::PutCreateIfAbsent,
    ));
    let store: SharedObjectStore = gate.clone();
    let clock = Arc::new(ManualClock::at_system_time());
    let a = clocked_runtime(&store, &clock, &format!("{name}-a")).await;
    a.create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create");
    let handle = a.open_namespace(&namespace_id).expect("open");
    handle
        .put_file("/a", b"a", &loonfs_test_support::test_actor())
        .await
        .expect("the session claims with a commit");
    Gated {
        gate,
        store,
        clock,
        namespace_id,
        a,
        handle,
    }
}

async fn clocked_runtime(
    store: &SharedObjectStore,
    clock: &Arc<ManualClock>,
    name: &str,
) -> LoonFs<Writable> {
    LoonFs::builder_with_store(store.clone(), loonfs::Context::Service)
        .writer_id(writer_id(name))
        .wall_clock(clock.clone())
        .build()
        .await
        .expect("runtime")
}

/// Polls `work` for a moment and asserts that it's still waiting.
async fn assert_waits<T: std::fmt::Debug>(
    work: &mut std::pin::Pin<&mut impl std::future::Future<Output = T>>,
    what: &str,
) {
    let early = bounded(Duration::from_millis(200), work.as_mut()).await;
    assert!(early.is_err(), "{what} waits, but it ended with {early:?}");
}

async fn current_manifest(
    store: &SharedObjectStore,
    namespace_id: &NamespaceId,
) -> loonfs_core::control::LoadedManifest {
    loonfs_core::test_support::namespace_store(store.clone(), namespace_id)
        .load_current_manifest()
        .await
        .expect("current manifest")
}

/// Checks that a close during a host's metadata call whose fold has its fold
/// permit and is in its manifest put waits for the put, that the fold's
/// manifest is published, and that the call then returns
/// `writer_session_closed`, the session's refusal, because the close's stop
/// ended the rest of the call.
#[tokio::test]
async fn a_close_during_a_hosts_fold_waits_for_its_put_and_the_fold_is_published() {
    let gated = gated("close-during-fold", metadata_manifest_prefix).await;
    let before = current_manifest(&gated.store, &gated.namespace_id).await;
    let cancellation = MaintenanceCancellation::new();
    let options = MaintainMetadataOptions {
        max_wal_tail_objects: NonZeroU64::MIN,
    };
    gated.gate.block_next();
    let call = gated
        .handle
        .maintain_metadata_while_due_with_options(&cancellation, &options);
    tokio::pin!(call);
    tokio::select! {
        ended = &mut call => panic!("the call ended before its fold's put: {ended:?}"),
        () = gated.gate.wait_until_blocked() => {}
    }
    let close = gated.handle.clone().close();
    tokio::pin!(close);
    assert_waits(&mut close, "the close").await;
    gated.gate.release();
    let (closed, maintained) = tokio::join!(close, call);
    closed.expect("close");
    let error = maintained.expect_err("the close stops the call after its fold");
    assert_eq!(error.code(), ErrorCode::WriterSessionClosed, "{error:?}");
    let after = current_manifest(&gated.store, &gated.namespace_id).await;
    assert!(
        after.payload().head_seq > before.payload().head_seq,
        "the fold's manifest is published"
    );
    gated.a.shutdown().await.expect("shutdown");
}

/// Checks that a deletion whose fold loses its put to another writer's
/// claim and fold, whose manifest already covers the deletion's tail, returns
/// `writer_fenced`, which the server sends once more in a new session: the
/// fold reports the position behind, and the deletion's next attempt loads
/// the writer's anchor, which finds the other writer's epoch.
#[tokio::test]
async fn a_deletion_whose_fold_loses_to_another_writers_fold_is_fenced() {
    let gated = gated("deletion-fold", metadata_manifest_prefix).await;
    let b = clocked_runtime(&gated.store, &gated.clock, "deletion-fold-b").await;
    gated.gate.block_next();
    let deletion = gated.handle.delete();
    tokio::pin!(deletion);
    tokio::select! {
        ended = &mut deletion => panic!("the deletion ended before its fold's put: {ended:?}"),
        () = gated.gate.wait_until_blocked() => {}
    }
    let b_session = b.open_namespace(&gated.namespace_id).expect("open on b");
    b_session
        .put_file("/b", b"b", &loonfs_test_support::test_actor())
        .await
        .expect("b claims with a commit");
    b_session.fold_wal().await.expect("b folds the tail");
    gated.gate.release();
    let error = deletion
        .await
        .expect_err("the other writer's claim fences the deletion");
    assert_eq!(error.code(), ErrorCode::WriterFenced, "{error:?}");
    assert!(
        current_manifest(&gated.store, &gated.namespace_id)
            .await
            .payload()
            .status
            .is_active(),
        "the namespace isn't deleted"
    );
    b.shutdown().await.expect("shutdown b");
    gated.a.shutdown().await.expect("shutdown a");
}

/// Checks that a retention advance whose put of the manifest that records
/// the floor waits while another runtime deletes the namespace returns
/// `writer_fenced`, as a commit does, and leaves the namespace deleted: the
/// advance checked its position and planned a move before the deletion, and
/// the deletion's claim then takes the number that the advance's manifest
/// put needs.
#[tokio::test]
async fn an_advance_whose_floor_manifest_put_follows_a_deletion_is_fenced() {
    let gated = gated("advance-then-deletion", metadata_manifest_prefix).await;
    gated.handle.fold_wal().await.expect("fold the commit");
    gated.clock.advance_ms(2 * GC_GRACE_WINDOW_MS);
    let b = clocked_runtime(&gated.store, &gated.clock, "advance-then-deletion-b").await;
    gated.gate.block_next();
    let advance = gated.handle.advance_retention_floor();
    tokio::pin!(advance);
    tokio::select! {
        ended = &mut advance => panic!("the advance ended before its manifest put: {ended:?}"),
        () = gated.gate.wait_until_blocked() => {}
    }
    b.open_namespace(&gated.namespace_id)
        .expect("open on b")
        .delete()
        .await
        .expect("b deletes the namespace");
    gated.gate.release();
    let error = advance
        .await
        .expect_err("the deletion's claim fences the advance");
    assert_eq!(error.code(), ErrorCode::WriterFenced, "{error:?}");
    assert!(
        !current_manifest(&gated.store, &gated.namespace_id)
            .await
            .payload()
            .status
            .is_active(),
        "the namespace stays deleted"
    );
    b.shutdown().await.expect("shutdown b");
    gated.a.shutdown().await.expect("shutdown a");
}

/// Checks that a fork of a namespace whose tail is too old to fork without
/// a fold waits for this runtime's session of the source when that session
/// is closed, or fenced, and still ending, because a host's fold is in its
/// manifest put, and then folds the source in a new session and forks,
/// instead of returning the old session's `writer_session_closed` or
/// `writer_fenced`, an error about another namespace than the fork's.
#[tokio::test]
async fn a_fork_waits_for_its_sources_ending_session_and_folds_in_a_new_one() {
    for fenced in [false, true] {
        let name = if fenced {
            "fenced-source"
        } else {
            "closed-source"
        };
        let gated = gated(name, metadata_manifest_prefix).await;
        gated.gate.block_next();
        let fold = gated.handle.fold_wal();
        tokio::pin!(fold);
        tokio::select! {
            ended = &mut fold => panic!("{name}: the fold ended before its put: {ended:?}"),
            () = gated.gate.wait_until_blocked() => {}
        }
        let b = clocked_runtime(&gated.store, &gated.clock, &format!("{name}-b")).await;
        let close = gated.handle.clone().close();
        tokio::pin!(close);
        if fenced {
            b.open_namespace(&gated.namespace_id)
                .expect("open on b")
                .put_file("/b", b"b", &loonfs_test_support::test_actor())
                .await
                .expect("b claims with a commit");
            let error = gated
                .handle
                .put_file("/c", b"c", &loonfs_test_support::test_actor())
                .await
                .expect_err("b's claim fences a's session");
            assert_eq!(error.code(), ErrorCode::WriterFenced, "{name}: {error:?}");
        } else {
            assert_waits(&mut close, "the close").await;
        }
        gated.clock.advance_ms(MAX_TAIL_COMMIT_AGE_MS + 1);
        let fork_id = namespace_id(&format!("{name}-fork"));
        let actor = loonfs_test_support::test_actor();
        let fork = gated
            .a
            .fork_namespace(&gated.namespace_id, &fork_id, &actor);
        tokio::pin!(fork);
        assert_waits(&mut fork, "the fork").await;
        gated.gate.release();
        let (forked, _, _) = tokio::join!(fork, fold, close);
        forked.unwrap_or_else(|error| panic!("{name}: {error:?}"));
        let file = gated
            .a
            .open_namespace(&fork_id)
            .expect("open the fork")
            .read_file("/a")
            .await
            .expect("read the fork");
        assert_eq!(file.bytes, b"a", "{name}");
        b.shutdown().await.expect("shutdown b");
        gated.a.shutdown().await.expect("shutdown a");
    }
}

/// Checks that a window change that changes nothing, on a session without a
/// position, reads the newest manifest, returns the window, and leaves the
/// store as it was: no claim's manifest, no fence. A change that does change
/// the window still claims the namespace and publishes it.
#[tokio::test]
async fn a_window_change_that_changes_nothing_claims_nothing_without_a_position() {
    let Created {
        recording,
        namespace_id,
        a,
        handle,
    } = created("unchanged-window").await;
    let store: SharedObjectStore = recording.clone();
    let before = current_manifest(&store, &namespace_id).await;
    let b = runtime(&store, "unchanged-window-b").await;
    let b_session = b.open_namespace(&namespace_id).expect("open on b");
    recording.reset();
    let unchanged = b_session
        .set_retention_window(None)
        .await
        .expect("an unchanged window");
    assert_eq!(unchanged.retention_window_ms, None);
    assert!(
        !recording.take().iter().any(|operation| matches!(
            operation,
            loonfs_test_support::stores::RecordedOperation::Put { .. }
        )),
        "the unchanged window puts nothing"
    );
    assert_eq!(
        current_manifest(&store, &namespace_id).await.payload(),
        before.payload()
    );
    b_session
        .set_retention_window(Some(RetentionWindow::new(DAY_MS).expect("window")))
        .await
        .expect("a window change");
    let after = current_manifest(&store, &namespace_id).await;
    assert_eq!(current_window(&store, &namespace_id).await, Some(DAY_MS));
    assert!(
        after.payload().writer_epoch > before.payload().writer_epoch,
        "the change claims the namespace"
    );
    drop((b_session, handle));
    b.shutdown().await.expect("shutdown b");
    a.shutdown().await.expect("shutdown a");
}

/// Builds a [`Gated`] whose gate parks manifest puts, after a fold of the
/// first commit and a second commit, with the clock two grace windows
/// ahead, so an advance plans a floor that moves and its ordinary fold
/// puts a manifest.
async fn gated_advance(name: &str) -> Gated {
    let gated = gated(name, metadata_manifest_prefix).await;
    gated.handle.fold_wal().await.expect("fold the commit");
    gated
        .handle
        .put_file("/b", b"b", &loonfs_test_support::test_actor())
        .await
        .expect("a commit for the advance's ordinary fold");
    gated.clock.advance_ms(2 * GC_GRACE_WINDOW_MS);
    gated
}

/// Checks that the session's close and the runtime's drain wait for a
/// retention advance whose ordinary fold is in its manifest put, and that
/// the advance then publishes its floor.
#[tokio::test]
async fn a_close_and_a_drain_wait_for_a_running_advance() {
    for drains in [false, true] {
        let gated = gated_advance(if drains {
            "drain-during-advance"
        } else {
            "close-during-advance"
        })
        .await;
        gated.gate.block_next();
        let advance = gated.handle.advance_retention_floor();
        tokio::pin!(advance);
        tokio::select! {
            ended = &mut advance => panic!("the advance ended before its fold's put: {ended:?}"),
            () = gated.gate.wait_until_blocked() => {}
        }
        let ending = async {
            if drains {
                gated.a.drain().await.map(drop)
            } else {
                gated.handle.clone().close().await.map(drop)
            }
        };
        tokio::pin!(ending);
        assert_waits(&mut ending, "the close or the drain").await;
        gated.gate.release();
        let (ended, advanced) = tokio::join!(ending, advance);
        ended.expect("the close or the drain");
        let advanced = advanced.expect("the advance publishes its floor");
        assert!(advanced.retention_floor_seq > loonfs_types::CommitSeq(0));
        assert_eq!(
            current_manifest(&gated.store, &gated.namespace_id)
                .await
                .payload()
                .retention_floor_seq,
            advanced.retention_floor_seq
        );
        gated.a.shutdown().await.expect("shutdown");
    }
}

/// Checks that the session's stop ends an advance's wait for the fold lock
/// with the session's refusal: a host's fold has the fold lock and is in its
/// manifest put, the advance waits for the lock, and the session's close
/// sets the stop, so the advance returns `writer_session_closed` and moves
/// no floor, while the close waits for the fold.
#[tokio::test]
async fn the_sessions_stop_ends_an_advances_wait_for_the_fold_lock() {
    let gated = gated_advance("stop-before-advance").await;
    gated.gate.block_next();
    let fold = gated.handle.fold_wal();
    tokio::pin!(fold);
    tokio::select! {
        ended = &mut fold => panic!("the fold ended before its put: {ended:?}"),
        () = gated.gate.wait_until_blocked() => {}
    }
    let advance = gated.handle.advance_retention_floor();
    tokio::pin!(advance);
    assert_waits(&mut advance, "the advance").await;
    let close = gated.handle.clone().close();
    tokio::pin!(close);
    assert_waits(&mut close, "the close").await;
    let refused = bounded(Duration::from_secs(10), advance)
        .await
        .expect("the stop ends the advance's wait")
        .expect_err("a closed session's advance");
    assert_eq!(
        refused.code(),
        ErrorCode::WriterSessionClosed,
        "{refused:?}"
    );
    gated.gate.release();
    let (closed, folded) = tokio::join!(close, fold);
    closed.expect("close");
    folded.expect("the fold that had its permit runs to its end");
    assert_eq!(
        current_manifest(&gated.store, &gated.namespace_id)
            .await
            .payload()
            .retention_floor_seq,
        loonfs_types::CommitSeq(0),
        "the floor didn't move"
    );
    gated.a.shutdown().await.expect("shutdown");
}

/// Checks that an advance whose ordinary fold's put waits while another
/// runtime claims the namespace returns `writer_fenced`, and that the floor
/// in the store is the other writer's.
#[tokio::test]
async fn an_advance_whose_fold_loses_to_another_writers_claim_is_fenced() {
    let gated = gated_advance("claim-during-advance").await;
    let before = current_manifest(&gated.store, &gated.namespace_id).await;
    gated.gate.block_next();
    let advance = gated.handle.advance_retention_floor();
    tokio::pin!(advance);
    tokio::select! {
        ended = &mut advance => panic!("the advance ended before its fold's put: {ended:?}"),
        () = gated.gate.wait_until_blocked() => {}
    }
    let b = clocked_runtime(&gated.store, &gated.clock, "claim-during-advance-b").await;
    b.open_namespace(&gated.namespace_id)
        .expect("open on b")
        .put_file("/c", b"c", &loonfs_test_support::test_actor())
        .await
        .expect("b claims with a commit");
    gated.gate.release();
    let error = advance
        .await
        .expect_err("the other writer's claim fences the advance");
    assert_eq!(error.code(), ErrorCode::WriterFenced, "{error:?}");
    let after = current_manifest(&gated.store, &gated.namespace_id).await;
    assert!(after.payload().writer_epoch > before.payload().writer_epoch);
    assert_eq!(
        after.payload().retention_floor_seq,
        before.payload().retention_floor_seq,
        "the floor is the other writer's"
    );
    b.shutdown().await.expect("shutdown b");
    gated.a.shutdown().await.expect("shutdown a");
}
