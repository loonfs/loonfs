use loon_testkit::replay::run_replay_fixture;

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
