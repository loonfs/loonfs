//! Behavior tests for the runtime core.

use crate::{
    ChangeSeq, CommitId, CommitOp, CommitPrecondition, CommitRequest, DisplayName, InodeId,
    NameKey, NamePolicy, RevisionNo,
};

/// Runs build steps until the index reports up to date, so each call
/// after one write lands that write's revisions as one delta run. Fails
/// the test by panicking when the step budget exhausts.
#[allow(clippy::panic)]
async fn drain_grams_builds(engine: &loonfs_core::NamespaceEngine<crate::SharedObjectStore>) {
    for _ in 0..8 {
        let report = engine
            .build_grams_index_step(loonfs_core::GramIndexBuildPolicy::default(), None)
            .await
            .expect("build step");
        match report.outcome {
            loonfs_core::GramIndexBuildOutcome::Published { .. } => {}
            loonfs_core::GramIndexBuildOutcome::UpToDate { .. } => return,
            other => unreachable!("unexpected build outcome: {other:?}"),
        }
    }
    panic!("the build backlog must drain");
}

// The bounded drive loops fail the test by panicking when they exhaust
// their step budget; that reachable failure is the point of the helper.
#[allow(clippy::panic)]
async fn drive_grams_fold_to_completion(
    engine: &loonfs_core::NamespaceEngine<crate::SharedObjectStore>,
    policy: loonfs_core::GramIndexBuildPolicy,
) {
    for _ in 0..64 {
        let report = engine
            .fold_grams_index_step(policy, None)
            .await
            .expect("fold step");
        match report.outcome {
            loonfs_core::GramIndexFoldOutcome::StepPublished { completed, .. } => {
                if completed {
                    return;
                }
            }
            other => unreachable!("expected a published fold step, got {other:?}"),
        }
    }
    panic!("the fold walk must terminate");
}

async fn grams_segment_levels(
    store: &crate::SharedObjectStore,
    namespace_id: &crate::NamespaceId,
) -> Vec<u32> {
    let root = loonfs_core::control::load_namespace_metadata_root_control(&**store, namespace_id)
        .await
        .expect("metadata root");
    let manifest_key = loonfs_objectstore::keys::metadata_manifest_object(
        namespace_id.as_str(),
        &root.state.manifest_object_id,
    );
    let manifest_bytes = store
        .get(&manifest_key, None)
        .await
        .expect("read namespace manifest")
        .expect("namespace manifest exists");
    let manifest = loonfs_api::wire::manifest::decode_namespace_manifest_json(&manifest_bytes)
        .expect("decode manifest");
    manifest
        .payload
        .index_files
        .iter()
        .filter(|descriptor| descriptor.family == "grams")
        .map(|descriptor| descriptor.level)
        .collect()
}

/// Review regression: a completed delta fold used to report no
/// continuing work, so a drain stopped at the tier transition and the
/// base fold its completion had just made eligible waited for the next
/// external trigger. The drain must keep going after every published
/// fold step until the trigger reports `NotNeeded`.
///
/// The scenario needs a fold whose completing step lands in a drain
/// iteration with no build publish, which the default budgets only
/// produce past ~131k index rows; the injected policy shapes the same
/// state at test scale, driving the exact drain the background path
/// runs.
#[tokio::test]
async fn grams_drain_continues_across_a_fold_tier_transition() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let store: crate::SharedObjectStore = std::sync::Arc::new(
        loonfs_objectstore::local_fs_store::LocalFsStore::new(temp_dir.path())
            .expect("local store"),
    );
    let namespace_id = crate::NamespaceId::parse("grams-drain-tiers").expect("namespace id");
    let writer = crate::FsWriter::builder_with_store(store.clone())
        .writer_id("grams-drain-writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("build writer");
    writer
        .create_namespace(&namespace_id, crate::CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    writer
        .put_file_bytes(
            &namespace_id,
            "/alpha.txt",
            b"alpha shared\n",
            crate::PutFileOptions::default(),
        )
        .await
        .expect("write alpha");
    writer
        .core()
        .enable_grams_index(&namespace_id)
        .await
        .expect("enable");

    // One existing mid run, so the in-flight delta fold's completion
    // is exactly what crosses the two-run mid threshold.
    let engine = writer.core().namespace_engine(&namespace_id);
    drain_grams_builds(&engine).await;
    drive_grams_fold_to_completion(
        &engine,
        loonfs_core::GramIndexBuildPolicy {
            max_l0_runs: 1,
            ..loonfs_core::GramIndexBuildPolicy::default()
        },
    )
    .await;

    // Two fresh delta runs, then a delta fold parked mid-walk by a
    // one-row step budget — and no build backlog left, so no drain
    // iteration after this publishes a build unit.
    for path in ["/bravo.txt", "/charlie.txt"] {
        writer
            .put_file_bytes(
                &namespace_id,
                path,
                format!("{path} shared\n").as_bytes(),
                crate::PutFileOptions::default(),
            )
            .await
            .expect("write file");
        drain_grams_builds(&engine).await;
    }
    let parked = engine
        .fold_grams_index_step(
            loonfs_core::GramIndexBuildPolicy {
                max_l0_runs: 2,
                max_fold_rows_per_step: 1,
                ..loonfs_core::GramIndexBuildPolicy::default()
            },
            None,
        )
        .await
        .expect("first fold step");
    assert!(
        matches!(
            parked.outcome,
            loonfs_core::GramIndexFoldOutcome::StepPublished {
                completed: false,
                ..
            }
        ),
        "a one-row budget must park the delta fold mid-walk, got {:?}",
        parked.outcome
    );
    let levels = grams_segment_levels(&store, &namespace_id).await;
    assert!(
        !levels.contains(&2),
        "no base may exist before the drain, got levels {levels:?}"
    );

    // One drain must finish the parked delta fold, discover that its
    // completion tipped the mid threshold, and run the base fold —
    // with no further write or explicit tick.
    writer
        .core()
        .drain_grams_index_backlog_with_policy(
            &namespace_id,
            loonfs_core::GramIndexBuildPolicy {
                max_l0_runs: 2,
                max_mid_runs: 2,
                ..loonfs_core::GramIndexBuildPolicy::default()
            },
        )
        .await
        .expect("drain");
    let levels = grams_segment_levels(&store, &namespace_id).await;
    assert!(
        !levels.is_empty() && levels.iter().all(|level| *level == 2),
        "the drain must cross the tier transition into the base fold, \
         got levels {levels:?}"
    );

    writer.shutdown_background().await.expect("writer shutdown");
}

#[test]
fn explicit_commit_facade_exports_constructor_types() {
    let display_name = DisplayName::parse("Report.txt").expect("valid display name");
    let name_key = NameKey::for_display_name(NamePolicy::default(), &display_name);
    let precondition = CommitPrecondition::BindingIs {
        parent_inode_id: InodeId(1),
        name_key,
        child_inode_id: InodeId(2),
        bind_seq: ChangeSeq(3),
        bind_delta_index: 4,
    };

    let request = CommitRequest {
        commit_id: CommitId::generate(),
        preconditions: vec![precondition],
        ops: vec![
            CommitOp::RestoreRevision {
                inode_id: InodeId(2),
                source_revision_no: RevisionNo(1),
                base_revision_no: RevisionNo(2),
            },
            CommitOp::Rename {
                inode_id: InodeId(2),
                new_parent_inode_id: InodeId(1),
                new_display_name: "report.txt".to_owned(),
            },
        ],
        message: None,
    };

    assert_eq!(request.preconditions.len(), 1);
    assert_eq!(request.ops.len(), 2);
}
