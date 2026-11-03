//! The metadata work that a host asks a writer session to do over its own
//! namespace, how a close of the session ends that work, and why the
//! session's own folds never wait for it.

use loonfs::{
    ActorId, InlineContentPolicy, LoonFs, MaintenanceCancellation, Namespace, RuntimeConfig,
    Writable,
};
use loonfs_test_support::clock::bounded;
use loonfs_test_support::ids::{namespace_id, writer_id};
use loonfs_test_support::stores::{BlockingStore, KeyPredicate, MemoryStore, OperationSelector};
use loonfs_types::format::segment_blocks::DEFAULT_MAX_DELTA_RUNS;
use loonfs_types::ChecksumAlgorithm;
use std::sync::Arc;
use std::time::Duration;

/// Inline bytes that make the session's fold due.
const FOLD_AT_BYTES: usize = 4096;

/// A runtime over a store that can park a metadata segment read, with a
/// held session whose namespace has more delta runs than one compaction
/// unit merges.
struct Backlog {
    store: Arc<BlockingStore<MemoryStore>>,
    runtime: LoonFs<Writable>,
    held: Namespace<Writable>,
    actor: ActorId,
}

async fn backlog(name: &str) -> Backlog {
    let store = Arc::new(BlockingStore::new(
        MemoryStore::new(ChecksumAlgorithm::Crc64nvme),
        KeyPredicate::metadata_segment(),
        OperationSelector::Read,
    ));
    let runtime = LoonFs::builder_with_store(store.clone())
        .writer_id(writer_id("session-maintenance"))
        .config(RuntimeConfig {
            inline_content: InlineContentPolicy {
                inline_content_fold_at_bytes: FOLD_AT_BYTES,
                ..InlineContentPolicy::default()
            },
            ..RuntimeConfig::default()
        })
        .build()
        .await
        .expect("runtime");
    let actor = loonfs_test_support::test_actor();
    let namespace_id = namespace_id(name);
    runtime
        .create_namespace(&namespace_id, &actor)
        .await
        .expect("create");
    let held = runtime.open_namespace(&namespace_id).expect("open");
    let maintenance = runtime.maintenance(writer_id("session-maintenance"));
    // One fold per commit leaves more delta runs than one compaction unit
    // merges.
    for index in 0..2 * DEFAULT_MAX_DELTA_RUNS.get() {
        held.put_file(&format!("/note-{index}"), b"data", &actor)
            .await
            .expect("publish");
        maintenance.fold_wal(&namespace_id).await.expect("fold");
    }
    Backlog {
        store,
        runtime,
        held,
        actor,
    }
}

impl Backlog {
    /// Commits a file whose inline bytes reach the fold trigger, so the
    /// session starts its fold, and waits for that fold.
    async fn publish_over_the_fold_trigger(&self) {
        self.held
            .put_file("/large", &[b'x'; FOLD_AT_BYTES], &self.actor)
            .await
            .expect("publish over the fold trigger");
        self.held.wait_for_fold().await.expect("the session's fold");
    }
}

/// Checks that a close of a session ends a host's metadata call through the
/// session at the compaction unit in progress, so the close doesn't wait for
/// the call's later units. The call is parked at its first metadata segment
/// read, and a fold of the session then starts the session's own
/// compaction, which a close cancels.
#[tokio::test]
async fn a_close_ends_a_hosts_metadata_call_at_its_unit_in_progress() {
    let backlog = backlog("closing").await;
    backlog.store.block_next();
    let cancellation = MaintenanceCancellation::new();
    let call = backlog.held.maintain_metadata_while_due(&cancellation);
    tokio::pin!(call);
    tokio::select! {
        _ = &mut call => panic!("the call finished without reading a segment"),
        () = backlog.store.wait_until_blocked() => {}
    }
    backlog.publish_over_the_fold_trigger().await;

    // The close cancels the session's compaction before its first wait.
    let close = backlog.held.clone().close();
    tokio::pin!(close);
    let _ = futures::poll!(close.as_mut());
    backlog.store.release();
    let (closed, maintained) = tokio::join!(close, call);
    closed.expect("close");
    let maintained = maintained.expect("the call");
    assert_eq!(
        maintained.compaction_units, 0,
        "the close stops the call at its unit in progress"
    );
    backlog.runtime.shutdown().await.expect("shutdown");
}

/// Checks that a session's fold never waits for a host's metadata call
/// through the session. The session has no position when the call starts,
/// because a fork of its namespace dropped it, and the call's compaction
/// unit is parked at its first metadata segment read. A commit that reaches
/// the fold trigger then completes, and the session's fold publishes, while
/// the call is still compacting.
#[tokio::test]
async fn a_sessions_fold_runs_while_a_hosts_metadata_call_compacts() {
    let backlog = backlog("folding").await;
    backlog
        .runtime
        .fork_namespace(
            backlog.held.id(),
            &namespace_id("folding-copy"),
            &backlog.actor,
        )
        .await
        .expect("fork");
    backlog.store.block_next();
    let cancellation = MaintenanceCancellation::new();
    let call = backlog.held.maintain_metadata_while_due(&cancellation);
    tokio::pin!(call);
    tokio::select! {
        _ = &mut call => panic!("the call finished without reading a segment"),
        () = backlog.store.wait_until_blocked() => {}
    }
    bounded(
        Duration::from_secs(10),
        backlog.publish_over_the_fold_trigger(),
    )
    .await
    .expect("the commit and the session's fold don't wait for the call");
    let diagnostics = backlog
        .runtime
        .maintenance(writer_id("session-maintenance"))
        .diagnostics(backlog.held.id())
        .await
        .expect("diagnostics");
    assert_eq!(
        diagnostics.wal_tail_objects, 0,
        "the session's fold published"
    );
    assert!(
        futures::poll!(call.as_mut()).is_pending(),
        "the call is still compacting"
    );
    backlog.store.release();
    call.await.expect("the call");
    backlog.runtime.shutdown().await.expect("shutdown");
}
