//! The turns of a writer session's publication lane: a namespace's
//! publication refuses another namespace's turn and writes, and the
//! session's folds and compaction units publish over each other.

use super::session_folds::{deadline, Writer};
use super::*;
use crate::commit::publish_view::WriterPosition;
use crate::{CompactionWriteOutcome, FoldWalOutcome, FoldWriteOutcome};

impl Writer {
    /// Returns the namespace's store components, without a segment cache.
    fn namespace(&self) -> crate::namespace::store::NamespaceStore<&RecordingStore<LocalFsStore>> {
        crate::test_support::namespace_store(&*self.store, &self.namespace_id)
    }

    /// Returns an engine of the session's writer, which writes compaction
    /// units from the session's position.
    fn compaction_engine(&self) -> crate::NamespaceWriterEngine<Arc<RecordingStore<LocalFsStore>>> {
        crate::NamespaceWriterEngine::writer(
            crate::test_support::namespace_store(Arc::clone(&self.store), &self.namespace_id),
            self.context.writer_id.clone(),
        )
    }

    /// Commits one file and folds it at a time, until a compaction unit is
    /// due on the session's newest manifest, and returns the session's
    /// position.
    async fn fold_until_compaction_due(&mut self) -> WriterPosition {
        let engine = self.compaction_engine();
        for index in 0..16 {
            let path = format!("/file-{index}");
            self.commit(&format!("file-{index}"), Vec::new(), &[&path])
                .await;
            let folded = self.engine.fold().await.expect("fold");
            if engine.metadata_compaction_due(folded.current.manifest()) {
                return self
                    .engine
                    .load_position_alone()
                    .await
                    .expect("the session's position");
            }
        }
        panic!("sixteen folds should make a compaction unit due");
    }

    /// Writes a compaction unit from `position`, which the run policy makes
    /// due.
    async fn write_unit(&self, position: &WriterPosition) -> CompactionWrites {
        match self
            .compaction_engine()
            .write_metadata_compaction_unit(position, &MaintenanceCancellation::default())
            .await
            .expect("write a compaction unit")
        {
            CompactionWriteOutcome::Written(writes) => writes,
            CompactionWriteOutcome::Ended(outcome) => {
                panic!("a unit is due, but it ended {outcome:?}")
            }
        }
    }

    /// Publishes `writes` in the session's publication lane.
    async fn publish_unit(&self, writes: CompactionWrites) -> CompactionUnitOutcome {
        let lane = self.engine.publication_lane();
        let guard = lane.lock().await;
        self.namespace()
            .publish_compaction_in_turn(
                guard.acquired_turn(),
                writes,
                &MaintenanceCancellation::default(),
            )
            .await
            .expect("publish a compaction unit")
    }
}

/// Returns the current manifest of `writer`'s namespace, with the manifest
/// boundary.
async fn current_basis(
    writer: &Writer,
) -> crate::namespace::Held<crate::namespace::control::LoadedManifest> {
    writer
        .namespace()
        .load_current_basis(crate::test_support::observed_now())
        .await
        .expect("the current manifest")
}

/// Checks that a publication refuses a turn of another namespace's lane,
/// here a window change built from that turn's basis, before any request.
#[tokio::test]
#[should_panic(expected = "a publication takes a turn of its own namespace's publication lane")]
async fn a_publication_refuses_a_turn_of_another_namespaces_lane() {
    let source = Writer::open_namespace("turn-source").await;
    let other = Writer::open_namespace("turn-other").await;
    let basis = current_basis(&source).await;
    let payload = NamespaceManifestPayload {
        retention_window_ms: std::num::NonZeroU64::new(2 * crate::limits::GC_GRACE_WINDOW_MS),
        ..basis.manifest().payload().clone()
    };
    let lane = crate::test_support::lane_at_manifest(basis);
    let guard = lane.lock().await;
    let _ = other
        .namespace()
        .publish_manifest_in_turn(guard.acquired_turn(), payload, &deadline())
        .await;
}

/// Checks that a fold's publication refuses the segments that a fold wrote
/// for another namespace's tail.
#[tokio::test]
#[should_panic(expected = "a fold publishes the segments that it wrote for its own namespace")]
async fn a_fold_publication_refuses_another_namespaces_writes() {
    let mut source = Writer::open_namespace("fold-source").await;
    source.commit("first", Vec::new(), &["/a"]).await;
    let position = source.engine.writer_position().expect("the source's tail");
    let FoldWriteOutcome::Written(writes) = source
        .namespace()
        .write_fold_segments(&position, &deadline())
        .await
        .expect("write the source's fold")
    else {
        panic!("the source's tail has a commit to fold");
    };
    let other = Writer::open_namespace("fold-other").await;
    let lane = crate::test_support::lane_at_manifest(current_basis(&other).await);
    let guard = lane.lock().await;
    let _ = other
        .namespace()
        .publish_fold_in_turn(guard.acquired_turn(), writes, &deadline())
        .await;
}

/// Checks that a compaction unit's publication refuses the output that a
/// unit wrote from another namespace's runs.
#[tokio::test]
#[should_panic(
    expected = "a compaction unit publishes the output that it wrote for its own namespace"
)]
async fn a_compaction_publication_refuses_another_namespaces_output() {
    let mut source = Writer::open_namespace("unit-source").await;
    let position = source.fold_until_compaction_due().await;
    let writes = source.write_unit(&position).await;
    let other = Writer::open_namespace("unit-other").await;
    let lane = crate::test_support::lane_at_manifest(current_basis(&other).await);
    let guard = lane.lock().await;
    let _ = other
        .namespace()
        .publish_compaction_in_turn(
            guard.acquired_turn(),
            writes,
            &MaintenanceCancellation::default(),
        )
        .await;
}

/// Checks that a compaction unit whose input runs a publication of its
/// session replaced before the unit's turn (here a unit of the same group,
/// planned from the same position) publishes nothing, sends no request, and
/// returns `Abandoned`.
#[tokio::test]
async fn a_unit_whose_inputs_moved_before_its_turn_is_abandoned_without_a_request() {
    let mut writer = Writer::open_namespace("moved-inputs").await;
    let position = writer.fold_until_compaction_due().await;
    let first = writer.write_unit(&position).await;
    let second = writer.write_unit(&position).await;
    assert_matches!(
        writer.publish_unit(first).await,
        CompactionUnitOutcome::Published { .. }
    );
    writer.store.reset();
    assert_eq!(
        writer.publish_unit(second).await,
        CompactionUnitOutcome::Abandoned
    );
    assert_eq!(
        loonfs_test_support::stores::request_lines(&writer.store.take()),
        Vec::<String>::new()
    );
}

/// Checks that a compaction unit publishes over a fold that its session
/// published after the unit planned, at the next manifest number, and that
/// the unit's manifest keeps the fold's run and head.
#[tokio::test]
async fn a_unit_publishes_over_a_fold_of_its_session_and_keeps_the_folds_run() {
    let mut writer = Writer::open_namespace("unit-over-fold").await;
    let position = writer.fold_until_compaction_due().await;
    let writes = writer.write_unit(&position).await;
    writer
        .commit("after-plan", Vec::new(), &["/after-plan"])
        .await;
    let folded = writer.engine.fold().await.expect("fold");
    assert_eq!(folded.response.outcome, FoldWalOutcome::Published);
    let fold = folded.current.manifest().payload();
    let fold_run = fold.runs.last().expect("the fold's run").clone();
    let CompactionUnitOutcome::Published { manifest, .. } = writer.publish_unit(writes).await
    else {
        panic!("the unit's inputs are still listed");
    };
    assert_eq!(
        manifest.manifest_no(),
        folded
            .current
            .manifest_no()
            .successor()
            .expect("next number")
    );
    let unit = manifest.manifest().payload();
    assert!(
        unit.runs.contains(&fold_run),
        "the unit's manifest lists the fold's run: {:?}",
        unit.runs
    );
    assert_eq!(
        (unit.head_seq, unit.folded_wal_no),
        (fold.head_seq, fold.folded_wal_no)
    );
}

/// Checks that a fold whose segments were written before a compaction unit
/// of its session published publishes over the unit's manifest, at the next
/// manifest number, and lists the unit's runs and then the fold's run.
#[tokio::test]
async fn a_fold_publishes_over_a_unit_of_its_session() {
    let mut writer = Writer::open_namespace("fold-over-unit").await;
    writer.fold_until_compaction_due().await;
    writer
        .commit("before-unit", Vec::new(), &["/before-unit"])
        .await;
    let position = writer
        .engine
        .load_position_alone()
        .await
        .expect("the session's position");
    let writes = writer.write_unit(&position).await;
    let FoldWriteOutcome::Written(fold) = writer
        .namespace()
        .write_fold_segments(&position, &deadline())
        .await
        .expect("write the fold")
    else {
        panic!("the tail has a commit to fold");
    };
    let CompactionUnitOutcome::Published { manifest, .. } = writer.publish_unit(writes).await
    else {
        panic!("nothing published since the unit planned");
    };
    let lane = writer.engine.publication_lane();
    let folded = writer
        .namespace()
        .publish_fold_in_turn(lane.lock().await.acquired_turn(), fold, &deadline())
        .await
        .expect("publish the fold");
    assert_eq!(folded.response.outcome, FoldWalOutcome::Published);
    assert_eq!(
        folded.current.manifest_no(),
        manifest.manifest_no().successor().expect("next number")
    );
    let unit = manifest.manifest().payload();
    let runs = &folded.current.manifest().payload().runs;
    assert_eq!(runs[..runs.len() - 1], unit.runs[..]);
    let fold_run = runs.last().expect("the fold's run");
    assert_eq!(
        (fold_run.run_no, fold_run.tier),
        (unit.next_run_no, RunTier::Delta)
    );
}
