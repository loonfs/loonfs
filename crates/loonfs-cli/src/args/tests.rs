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
        "checkpoint",
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
            "maintenance",
            "checkpoint",
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
        &["loonfs", "--json", "changes", "--all", "--limit", "5"],
        &["loonfs", "--json", "grep", "x", "--all", "--limit", "5"],
        &[
            "loonfs",
            "--json",
            "maintenance",
            "checkpoint",
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
    for kind in CommandKind::ALL {
        assert_eq!(kind.as_str(), snake_case(&format!("{kind:?}")));
    }
}

fn snake_case(value: &str) -> String {
    let mut result = String::with_capacity(value.len());
    for (index, character) in value.chars().enumerate() {
        if index > 0 && character.is_ascii_uppercase() {
            result.push('_');
        }
        result.push(character.to_ascii_lowercase());
    }
    result
}

#[test]
fn management_command_model_uses_noun_families() {
    let command = Cli::command();
    let profile_create = subcommand(subcommand(&command, "profile"), "create");
    assert_subcommands(profile_create, &["s3", "r2", "gcs", "local", "remote"]);
    let profile_update = subcommand(subcommand(&command, "profile"), "update");
    assert_subcommands(profile_update, &["s3", "r2", "gcs", "local", "remote"]);

    let namespace = subcommand(&command, "namespace");
    assert_subcommands(namespace, &["create", "show", "delete", "fork"]);

    let snapshot = subcommand(&command, "snapshot");
    assert_subcommands(snapshot, &["create", "list", "extend", "delete"]);

    let maintenance = subcommand(&command, "maintenance");
    assert_subcommands(
        maintenance,
        &[
            "recover-administrator",
            "metadata",
            "fold",
            "checkpoint",
            "index",
            "retention",
            "gc",
            "store",
        ],
    );
    assert_subcommands(subcommand(maintenance, "checkpoint"), &["list"]);
    assert_subcommands(
        subcommand(maintenance, "index"),
        &["enable", "disable", "status"],
    );
    assert_subcommands(subcommand(maintenance, "retention"), &["advance", "window"]);
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
