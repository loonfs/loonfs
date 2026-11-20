//! Argument grammar tests.

use super::*;
use clap::{CommandFactory, Parser};
use loonfs_types::NamespaceId;

#[test]
fn global_selectors_work_before_between_and_after_subcommands() {
    let before_command = Cli::try_parse_from([
        "loonfs",
        "--profile",
        "prod",
        "--namespace",
        "demo",
        "ls",
        "/",
    ])
    .expect("selectors before command");
    assert_selected_target(&before_command, "prod", "demo");

    let between_subcommands = Cli::try_parse_from([
        "loonfs",
        "maintenance",
        "--profile",
        "prod",
        "snapshot",
        "--namespace",
        "demo",
        "list",
    ])
    .expect("selectors between subcommands");
    assert_selected_target(&between_subcommands, "prod", "demo");

    let after_leaf_arguments = Cli::try_parse_from([
        "loonfs",
        "ls",
        "/",
        "--profile",
        "prod",
        "--namespace",
        "demo",
    ])
    .expect("selectors after leaf arguments");
    assert_selected_target(&after_leaf_arguments, "prod", "demo");
}

#[test]
fn stat_help_and_parser_show_both_exclusive_target_forms() {
    let mut command = Cli::command();
    let help = command
        .find_subcommand_mut("stat")
        .expect("stat command")
        .render_long_help()
        .to_string();
    assert!(help.contains("loonfs stat <PATH>"), "{help}");
    assert!(help.contains("loonfs stat --inode <INODE_ID>"), "{help}");

    let missing = Cli::try_parse_from(["loonfs", "stat"]).expect_err("target is required");
    assert_eq!(
        missing.kind(),
        clap::error::ErrorKind::MissingRequiredArgument
    );

    let both = Cli::try_parse_from([
        "loonfs",
        "stat",
        "/doc",
        "--inode",
        "ino_00000000000000000000000002",
    ])
    .expect_err("targets conflict");
    assert_eq!(both.kind(), clap::error::ErrorKind::ArgumentConflict);

    Cli::try_parse_from(["loonfs", "stat", "/doc"]).expect("path target");
    Cli::try_parse_from([
        "loonfs",
        "stat",
        "--inode",
        "ino_00000000000000000000000002",
    ])
    .expect("inode target");
}

#[test]
fn restore_takes_either_a_revision_or_a_deleted_item() {
    const INODE: &str = "ino_00000000000000000000000002";
    for arguments in [
        vec!["/doc", "--revision", "1"],
        vec!["--inode", INODE, "--deletion-seq", "3"],
        vec!["--inode", INODE, "--deletion-seq", "3", "--to", "/doc"],
    ] {
        let parsed =
            Cli::try_parse_from(["loonfs", "restore"].into_iter().chain(arguments.clone()));
        assert!(parsed.is_ok(), "{arguments:?}: {parsed:?}");
    }
    for arguments in [
        vec![],
        vec!["/doc"],
        vec!["--revision", "1"],
        vec!["--inode", INODE],
        vec!["--deletion-seq", "3"],
        vec!["/doc", "--inode", INODE, "--deletion-seq", "3"],
        vec!["/doc", "--revision", "1", "--to", "/other"],
        vec!["--revision", "1", "--inode", INODE, "--deletion-seq", "3"],
    ] {
        let parsed =
            Cli::try_parse_from(["loonfs", "restore"].into_iter().chain(arguments.clone()));
        assert!(parsed.is_err(), "{arguments:?} should be refused");
    }
}

#[test]
fn revision_preconditions_require_matching_inode_preconditions() {
    for arguments in [
        vec![
            "loonfs",
            "annotate",
            "/file.txt",
            "--set",
            "owner=ada",
            "--expected-attributes-revision",
            "1",
        ],
        vec![
            "loonfs",
            "cp",
            "/source.txt",
            "/destination.txt",
            "--expected-destination-revision",
            "1",
        ],
        vec![
            "loonfs",
            "put",
            "payload.txt",
            "/destination.txt",
            "--expected-revision",
            "1",
        ],
    ] {
        let error = Cli::try_parse_from(arguments).expect_err("inode precondition is required");
        assert_eq!(
            error.kind(),
            clap::error::ErrorKind::MissingRequiredArgument
        );
    }
}

#[test]
fn an_index_enable_that_does_not_wait_takes_no_wait_budget() {
    for budget in ["--max-steps", "--deadline-ms"] {
        let error = Cli::try_parse_from([
            "loonfs",
            "maintenance",
            "index",
            "enable",
            "--no-wait",
            budget,
            "3",
        ])
        .expect_err("a budget for a wait that does not happen");
        assert_eq!(
            error.kind(),
            clap::error::ErrorKind::ArgumentConflict,
            "{budget}"
        );
    }
}

fn assert_selected_target(cli: &Cli, profile: &str, namespace: &str) {
    let selectors = cli.selectors();
    assert_eq!(selectors.profile.as_deref(), Some(profile));
    assert_eq!(
        selectors.namespace.as_ref().map(NamespaceId::as_str),
        Some(namespace)
    );
}

#[test]
fn every_paginated_listing_accepts_the_shared_flags() {
    let cases: &[&[&str]] = &[
        &[
            "loonfs",
            "ls",
            "--limit",
            "5",
            "--page-size",
            "2",
            "--all",
            "--jsonl",
            "--cursor",
            "c",
        ],
        &[
            "loonfs",
            "revisions",
            "/f",
            "--limit",
            "5",
            "--page-size",
            "2",
            "--all",
            "--jsonl",
            "--cursor",
            "c",
        ],
        &[
            "loonfs",
            "trash",
            "--limit",
            "5",
            "--page-size",
            "2",
            "--all",
            "--jsonl",
            "--cursor",
            "c",
        ],
        &[
            "loonfs",
            "changes",
            "--limit",
            "5",
            "--page-size",
            "2",
            "--all",
            "--jsonl",
            "--after",
            "1",
        ],
        &[
            "loonfs",
            "grep",
            "x",
            "--limit",
            "5",
            "--page-size",
            "2",
            "--all",
            "--jsonl",
            "--cursor",
            "c",
        ],
        &[
            "loonfs",
            "namespace",
            "list",
            "--limit",
            "5",
            "--page-size",
            "2",
            "--all",
            "--jsonl",
            "--cursor",
            "c",
        ],
        &[
            "loonfs",
            "maintenance",
            "snapshot",
            "list",
            "--limit",
            "5",
            "--page-size",
            "2",
            "--all",
            "--jsonl",
            "--cursor",
            "c",
        ],
        &[
            "loonfs",
            "snapshot",
            "list",
            "demo",
            "--limit",
            "5",
            "--page-size",
            "2",
            "--all",
            "--jsonl",
            "--cursor",
            "c",
        ],
    ];
    for arguments in cases {
        let cli = Cli::try_parse_from(*arguments).expect("pagination arguments parse");
        let pagination = cli.command.pagination().expect("paginated command");
        assert!(pagination.all);
        assert!(pagination.jsonl);
    }
}

#[test]
fn all_json_is_rejected_for_every_paginated_listing() {
    let cases: &[&[&str]] = &[
        &["loonfs", "--json", "ls", "--all", "--limit", "5"],
        &[
            "loonfs",
            "--json",
            "revisions",
            "/f",
            "--all",
            "--limit",
            "5",
        ],
        &[
            "loonfs", "--json", "snapshot", "list", "demo", "--all", "--limit", "5",
        ],
        &["loonfs", "--json", "trash", "--all", "--limit", "5"],
        &[
            "loonfs",
            "--json",
            "namespace",
            "list",
            "--all",
            "--limit",
            "5",
        ],
        &["loonfs", "--json", "changes", "--all", "--limit", "5"],
        &["loonfs", "--json", "grep", "x", "--all", "--limit", "5"],
        &[
            "loonfs",
            "--json",
            "maintenance",
            "snapshot",
            "list",
            "--all",
            "--limit",
            "5",
        ],
    ];
    for arguments in cases {
        let cli = Cli::try_parse_from(*arguments).expect("arguments parse");
        let error = validate_cli(&cli).expect_err("--json --all must fail");
        assert!(error.to_string().contains(
            "`--all` cannot be used with `--json`; use `--json --limit <n>` for a bounded document or `--jsonl` to stream all results"
        ));
    }
}

#[test]
fn command_kind_envelope_values_are_pinned() {
    // The wire strings are written out, so renaming a variant or changing
    // its string fails this test.
    let table = [
        (CommandKind::Completion, "completion"),
        (CommandKind::Init, "init"),
        (CommandKind::ProfileCreate, "profile_create"),
        (CommandKind::ProfileList, "profile_list"),
        (CommandKind::ProfileShow, "profile_show"),
        (CommandKind::ProfileUpdate, "profile_update"),
        (CommandKind::ProfileDelete, "profile_delete"),
        (CommandKind::ProfileUse, "profile_use"),
        (CommandKind::NamespaceCreate, "namespace_create"),
        (CommandKind::NamespaceShow, "namespace_show"),
        (CommandKind::NamespaceList, "namespace_list"),
        (CommandKind::NamespaceDelete, "namespace_delete"),
        (CommandKind::NamespaceFork, "namespace_fork"),
        (CommandKind::NamespaceUse, "namespace_use"),
        (CommandKind::Merge, "merge"),
        (CommandKind::SnapshotCreate, "snapshot_create"),
        (CommandKind::SnapshotList, "snapshot_list"),
        (CommandKind::SnapshotExtend, "snapshot_extend"),
        (CommandKind::SnapshotDelete, "snapshot_delete"),
        (CommandKind::Current, "current"),
        (CommandKind::FilesystemLs, "filesystem_ls"),
        (CommandKind::FilesystemStat, "filesystem_stat"),
        (CommandKind::FilesystemAnnotate, "filesystem_annotate"),
        (CommandKind::AccessSet, "access_set"),
        (CommandKind::FilesystemCat, "filesystem_cat"),
        (CommandKind::FilesystemGrep, "filesystem_grep"),
        (CommandKind::FilesystemGet, "filesystem_get"),
        (CommandKind::FilesystemPut, "filesystem_put"),
        (CommandKind::FilesystemAppend, "filesystem_append"),
        (CommandKind::FilesystemRevisions, "filesystem_revisions"),
        (CommandKind::FilesystemTrash, "filesystem_trash"),
        (CommandKind::FilesystemRestore, "filesystem_restore"),
        (CommandKind::FilesystemMkdir, "filesystem_mkdir"),
        (CommandKind::FilesystemRm, "filesystem_rm"),
        (CommandKind::FilesystemMv, "filesystem_mv"),
        (CommandKind::FilesystemCp, "filesystem_cp"),
        (CommandKind::Changes, "changes"),
        (CommandKind::Capabilities, "capabilities"),
        (CommandKind::Doctor, "doctor"),
        (
            CommandKind::MaintenanceSnapshotList,
            "maintenance_snapshot_list",
        ),
        (CommandKind::MaintenanceFold, "maintenance_fold"),
        (
            CommandKind::MaintenanceRetentionAdvance,
            "maintenance_retention_advance",
        ),
        (
            CommandKind::MaintenanceRetentionWindow,
            "maintenance_retention_window",
        ),
        (CommandKind::MaintenanceMetadata, "maintenance_metadata"),
        (CommandKind::MaintenanceGc, "maintenance_gc"),
        (CommandKind::MaintenancePoolGc, "maintenance_pool_gc"),
        (
            CommandKind::MaintenanceStoreProbe,
            "maintenance_store_probe",
        ),
        (
            CommandKind::MaintenanceIndexEnable,
            "maintenance_index_enable",
        ),
        (
            CommandKind::MaintenanceIndexDisable,
            "maintenance_index_disable",
        ),
        (
            CommandKind::MaintenanceIndexStatus,
            "maintenance_index_status",
        ),
        (CommandKind::ConfigPath, "config_path"),
        (CommandKind::ConfigShow, "config_show"),
        (CommandKind::Version, "version"),
    ];
    for (kind, wire) in table {
        assert_eq!(kind.as_str(), wire);
    }
    assert_eq!(table.len(), CommandKind::ALL.len());
    for kind in CommandKind::ALL {
        assert!(
            table.iter().any(|(listed, _)| *listed == kind),
            "{kind:?} has no wire string in the table"
        );
    }
}

#[test]
fn management_command_model_uses_noun_families() {
    let command = Cli::command();
    let profile_create = subcommand(subcommand(&command, "profile"), "create");
    assert_subcommands(
        profile_create,
        &["s3", "r2", "gcs", "abs", "local", "remote"],
    );
    let profile_update = subcommand(subcommand(&command, "profile"), "update");
    assert_subcommands(
        profile_update,
        &["s3", "r2", "gcs", "abs", "local", "remote"],
    );

    let namespace = subcommand(&command, "namespace");
    assert_subcommands(namespace, &["create", "show", "list", "delete", "fork"]);

    let snapshot = subcommand(&command, "snapshot");
    assert_subcommands(snapshot, &["create", "list", "extend", "delete"]);

    let maintenance = subcommand(&command, "maintenance");
    assert_subcommands(
        maintenance,
        &[
            "metadata",
            "fold",
            "snapshot",
            "index",
            "retention",
            "gc",
            "pool",
            "store",
        ],
    );
    assert_subcommands(subcommand(maintenance, "snapshot"), &["list"]);
    assert_subcommands(
        subcommand(maintenance, "index"),
        &["enable", "disable", "status"],
    );
    assert_subcommands(subcommand(maintenance, "retention"), &["advance", "window"]);
    assert_subcommands(subcommand(maintenance, "pool"), &["gc"]);
    assert_subcommands(subcommand(maintenance, "store"), &["probe"]);
}

#[test]
fn provider_create_commands_only_expose_applicable_flags() {
    let command = Cli::command();
    let create = subcommand(subcommand(&command, "profile"), "create");
    let s3 = subcommand(create, "s3");
    assert!(has_argument(s3, "credential_source"));
    assert!(has_argument(s3, "session_token"));
    assert!(!has_argument(s3, "account_id"));

    let r2 = subcommand(create, "r2");
    assert!(has_argument(r2, "credential_source"));
    assert!(has_argument(r2, "account_id"));
    assert!(!has_argument(r2, "region"));

    let abs = subcommand(create, "abs");
    assert!(has_argument(abs, "account_name"));
    assert!(has_argument(abs, "account_key"));
    assert!(!has_argument(abs, "bucket"));

    let local = subcommand(create, "local");
    assert!(has_argument(local, "root"));
    assert!(!has_argument(local, "bucket"));

    let remote = subcommand(create, "remote");
    assert!(has_argument(remote, "server_url"));
    assert!(!has_argument(remote, "key_prefix"));

    let init = subcommand(&command, "init");
    assert!(!has_argument(init, "mode"));
    assert!(!has_argument(init, "store_kind"));
}

fn subcommand<'a>(command: &'a clap::Command, name: &str) -> &'a clap::Command {
    command
        .get_subcommands()
        .find(|subcommand| subcommand.get_name() == name)
        .expect("subcommand exists")
}

fn has_argument(command: &clap::Command, id: &str) -> bool {
    command
        .get_arguments()
        .any(|argument| argument.get_id() == id)
}

fn assert_subcommands(command: &clap::Command, expected: &[&str]) {
    let actual = command
        .get_subcommands()
        .filter(|subcommand| subcommand.get_name() != "help")
        .map(clap::Command::get_name)
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
}
