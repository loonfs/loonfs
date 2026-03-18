use loon_testkit::replay::run_replay_fixture;
use loon_testkit::seed::Seed;

#[test]
fn wal_tail_replay_fixture_matches_model_and_core() {
    run_replay_fixture("native/wal_tail_replay_advances_head.yaml", None).unwrap();
    run_replay_fixture(
        "native/wal_tail_replay_applies_delete_subtree_tombstone.yaml",
        None,
    )
    .unwrap();
    run_replay_fixture(
        "native/wal_tail_replay_applies_rename_direntry_rebind.yaml",
        None,
    )
    .unwrap();
    run_replay_fixture(
        "native/wal_tail_replay_applies_restore_revision_head.yaml",
        None,
    )
    .unwrap();
}

#[test]
fn checkpoint_plus_wal_tail_fixture_matches_model_and_core() {
    run_replay_fixture(
        "native/checkpoint_manifest_plus_wal_tail_reproduces_head.yaml",
        None,
    )
    .unwrap();
    run_replay_fixture(
        "native/checkpoint_manifest_plus_delete_subtree_wal_tail_hides_descendants.yaml",
        None,
    )
    .unwrap();
    run_replay_fixture(
        "native/checkpoint_manifest_reused_direntry_slot_restores_bind_history.yaml",
        None,
    )
    .unwrap();
    run_replay_fixture(
        "native/checkpoint_manifest_plus_rename_wal_tail_reproduces_head.yaml",
        None,
    )
    .unwrap();
    run_replay_fixture(
        "native/checkpoint_manifest_plus_restore_revision_wal_tail_reproduces_head.yaml",
        None,
    )
    .unwrap();
}

#[test]
fn wal_tail_replay_snapshot_matches_checked_in_artifact() {
    let report = run_replay_fixture(
        "native/wal_tail_replay_advances_head.yaml",
        Some(Seed(4242)),
    )
    .unwrap();
    assert_eq!(
        report.rendered_trace,
        include_str!(
            "../../../tests/snapshots/replay-differential/native/wal_tail_replay_advances_head.txt"
        )
    );
}

#[test]
fn checkpoint_plus_wal_tail_snapshot_matches_checked_in_artifact() {
    let report = run_replay_fixture(
        "native/checkpoint_manifest_plus_wal_tail_reproduces_head.yaml",
        Some(Seed(4242)),
    )
    .unwrap();
    assert_eq!(
        report.rendered_trace,
        include_str!(
            "../../../tests/snapshots/replay-differential/native/checkpoint_manifest_plus_wal_tail_reproduces_head.txt"
        )
    );
}
