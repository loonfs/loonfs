use serde_json::Value;
use std::env;
use std::fs;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;

/// A config file with no profiles: enough to load, for the tests that care
/// about which file was loaded rather than what was in it.
const MINIMAL_CONFIG: &str = "config_version = 1\n";

#[test]
fn profile_create_list_show_delete_work() {
    let harness = Harness::new();

    let add_embedded = harness.run(&[
        "--json",
        "profile",
        "create",
        "default",
        "--mode",
        "embedded",
        "--store-kind",
        "local-fs",
        "--root",
        harness.store_root("default").to_str().expect("utf-8 path"),
    ]);
    assert_success(&add_embedded);
    assert_eq!(json_data(&add_embedded)["mode"], "embedded");

    let external = harness
        .start_external_server(harness.write_server_config("remote", "profile-create-remote"));
    let add_remote = harness.run(&[
        "--json",
        "profile",
        "create",
        "prod",
        "--mode",
        "remote",
        "--server-url",
        &external.server_url,
        "--auth-token",
        "test-token",
    ]);
    assert_success(&add_remote);
    assert_eq!(json_data(&add_remote)["mode"], "remote");

    let list = harness.run(&["--json", "profile", "list"]);
    assert_success(&list);
    let list_data = json_data(&list);
    let profiles = list_data["profiles"].as_array().expect("json array");
    assert_eq!(profiles.len(), 2);
    assert_eq!(profiles[0]["name"], "default");
    assert_eq!(profiles[1]["name"], "prod");

    let show = harness.run(&["config", "show"]);
    assert_success(&show);
    let stdout = stdout_string(&show);
    assert!(stdout.contains("mode = \"remote\""));
    assert!(stdout.contains("<redacted>"));
    assert!(!stdout.contains("test-token"));

    let show_default = harness.run(&["--json", "profile", "show"]);
    assert_success(&show_default);
    assert_eq!(json_data(&show_default)["mode"], "embedded");

    let remove_default = harness.run(&["--json", "profile", "delete", "default"]);
    assert_success(&remove_default);
    assert_eq!(json_data(&remove_default)["name"], "default");

    let list_after_remove = harness.run(&["--json", "profile", "list"]);
    assert_success(&list_after_remove);
    assert!(json_data(&list_after_remove)["default_profile"].is_null());

    let show_without_default = harness.run(&["--json", "profile", "show"]);
    assert_failure(&show_without_default);
    assert_eq!(
        json_error(&show_without_default)["code"],
        "no_default_profile"
    );
}

#[test]
fn mutation_actor_precedence_is_flag_then_environment_then_profile() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    assert_success(&harness.run(&[
        "profile",
        "update",
        "default",
        "--actor-kind",
        "user",
        "--actor-id",
        "profile-user",
    ]));
    assert_success(&harness.run(&["namespace", "create", "demo"]));
    assert_success(&harness.run(&["use", "demo"]));
    let payload = harness.temp_dir.path().join("actor.txt");
    fs::write(&payload, b"attributed").expect("payload");
    let payload = payload.to_str().expect("utf-8 path");

    assert_success(&harness.run(&["put", payload, "/profile.txt"]));
    assert_success(&harness.run_with_env(
        &[
            ("LOONFS_ACTOR_KIND", "service"),
            ("LOONFS_ACTOR_ID", "environment-service"),
        ],
        &["put", payload, "/environment.txt"],
    ));
    assert_success(&harness.run_with_env(
        &[
            ("LOONFS_ACTOR_KIND", "service"),
            ("LOONFS_ACTOR_ID", "environment-service"),
        ],
        &[
            "put",
            payload,
            "/flag.txt",
            "--actor-kind",
            "system",
            "--actor-id",
            "flag-system",
        ],
    ));

    let changes = harness.run(&["--json", "changes"]);
    assert_success(&changes);
    let actors = json_data(&changes)["changes"]
        .as_array()
        .expect("changes")
        .iter()
        .map(|change| change["actor"].clone())
        .collect::<Vec<_>>();
    assert_eq!(
        actors,
        [
            serde_json::json!({"kind":"user","id":"profile-user"}),
            serde_json::json!({"kind":"service","id":"environment-service"}),
            serde_json::json!({"kind":"system","id":"flag-system"}),
        ]
    );
}

#[test]
fn broken_configs_stay_repairable_with_the_repair_commands() {
    let harness = Harness::new();
    harness.write_cli_config(format!(
        r#"config_version = 1
default_profile = "broken"

[profiles.broken]
mode = "remote"
server_url = "https://loonfs.example.com"
auth_token = "secret-degraded-token"
unknown_knob = true

[profiles.keeper]
mode = "embedded"

[profiles.keeper.store]
kind = "local-fs"
root = "{}"
"#,
        harness.store_root("keeper").display()
    ));

    // Ordinary commands reject the file, naming it and the offending key.
    let list = harness.run(&["profile", "list"]);
    assert_failure(&list);
    let message = stderr_string(&list);
    assert!(message.contains("config.toml"), "{message}");
    assert!(message.contains("unknown_knob"), "{message}");

    // The repair commands still work. Show renders the file as parsed, with
    // the failure on top and secrets masked.
    let show = harness.run(&["config", "show"]);
    assert_success(&show);
    let shown = stdout_string(&show);
    assert!(shown.contains("warning:"), "{shown}");
    assert!(shown.contains("unknown_knob"), "{shown}");
    assert!(shown.contains("<redacted>"), "{shown}");
    assert!(!shown.contains("secret-degraded-token"), "{shown}");

    // Use switches the default while the file is still degraded; delete then
    // removes the broken profile, which heals the file for every command.
    assert_success(&harness.run(&["profile", "use", "keeper"]));
    let delete = harness.run(&["--json", "profile", "delete", "broken"]);
    assert_success(&delete);
    assert_eq!(json_data(&delete)["name"], "broken");
    assert_eq!(json_data(&delete)["mode"], "remote");

    let healed = harness.run(&["--json", "profile", "list"]);
    assert_success(&healed);
    let healed_data = json_data(&healed);
    assert_eq!(healed_data["default_profile"], "keeper");
    assert_eq!(
        healed_data["profiles"]
            .as_array()
            .expect("json array")
            .len(),
        1
    );

    // A config for another version is a hard error, named before any
    // unknown-field noise; repair commands do not edit files written for a
    // different version.
    harness.write_cli_config("config_version = 2\nfuture_setting = true\n");
    let future = harness.run(&["config", "show"]);
    assert_failure(&future);
    let future_message = stderr_string(&future);
    assert!(
        future_message.contains("`config_version = 2`"),
        "{future_message}"
    );
    assert!(
        !future_message.contains("future_setting"),
        "{future_message}"
    );
}

/// The resolution chain, first match wins: `--config`, then
/// `LOONFS_CONFIG`, then `$XDG_CONFIG_HOME/loonfs/config.toml`, then
/// `~/.loonfs/config.toml`.
#[test]
fn config_resolution_prefers_the_flag_then_the_environment_then_xdg_then_legacy() {
    let harness = Harness::new();
    harness.write_cli_config(MINIMAL_CONFIG);
    let legacy_path = harness.config_path.display().to_string();

    let xdg_home = harness.temp_dir.path().join("xdg");
    let xdg_path = xdg_home.join("loonfs").join("config.toml");
    let named_path = harness.temp_dir.path().join("named.toml");
    let flagged_path = harness.temp_dir.path().join("flagged.toml");

    // Without XDG the legacy path is simply the default, with nothing to
    // migrate to.
    let default = json_data(&harness.run(&["--json", "config", "path"]));
    assert_eq!(default["path"], legacy_path);
    assert_eq!(default["source"], "legacy");
    assert!(default["preferred_path"].is_null(), "{default}");

    // XDG set but holding no config: the existing legacy file keeps
    // working, and the answer names where it belongs.
    let migrating = json_data(&harness.run_with_env(
        &[("XDG_CONFIG_HOME", &xdg_home)],
        &["--json", "config", "path"],
    ));
    assert_eq!(migrating["path"], legacy_path);
    assert_eq!(migrating["source"], "legacy");
    assert_eq!(migrating["preferred_path"], xdg_path.display().to_string());

    // A config at the preferred path wins as soon as one exists there.
    fs::create_dir_all(xdg_path.parent().expect("xdg config dir")).expect("create xdg config dir");
    fs::write(&xdg_path, MINIMAL_CONFIG).expect("write xdg config");
    let xdg = json_data(&harness.run_with_env(
        &[("XDG_CONFIG_HOME", &xdg_home)],
        &["--json", "config", "path"],
    ));
    assert_eq!(xdg["path"], xdg_path.display().to_string());
    assert_eq!(xdg["source"], "xdg");
    assert!(xdg["preferred_path"].is_null(), "{xdg}");

    // The environment beats both defaults, by being spelled rather than by
    // the file it names existing.
    let from_env = json_data(&harness.run_with_env(
        &[
            ("XDG_CONFIG_HOME", &xdg_home),
            ("LOONFS_CONFIG", &named_path),
        ],
        &["--json", "config", "path"],
    ));
    assert_eq!(from_env["path"], named_path.display().to_string());
    assert_eq!(from_env["source"], "env");
    assert!(!named_path.exists(), "the named file need not exist yet");

    // The flag beats everything, and being global it may follow the
    // subcommand as readily as precede it.
    for args in [
        vec![
            "--json",
            "--config",
            flagged_path.to_str().expect("utf-8 path"),
            "config",
            "path",
        ],
        vec![
            "--json",
            "config",
            "path",
            "--config",
            flagged_path.to_str().expect("utf-8 path"),
        ],
    ] {
        let from_flag = json_data(&harness.run_with_env(
            &[
                ("XDG_CONFIG_HOME", &xdg_home),
                ("LOONFS_CONFIG", &named_path),
            ],
            &args,
        ));
        assert_eq!(from_flag["path"], flagged_path.display().to_string());
        assert_eq!(from_flag["source"], "flag");
    }
}

/// `config path` is the command that answers "which file is this build even
/// looking at", so it never reads that file.
#[test]
fn config_path_answers_while_the_config_file_is_unreadable() {
    let harness = Harness::new();
    harness.write_cli_config("config_version = 1\nunknown_knob = true\n");

    assert_failure(&harness.run(&["profile", "list"]));

    let path = harness.run(&["--json", "config", "path"]);
    assert_success(&path);
    assert_eq!(
        json_data(&path)["path"],
        harness.config_path.display().to_string()
    );
    assert_eq!(json_data(&path)["source"], "legacy");

    // The human line carries both answers: the file, and why that file.
    let human = harness.run(&["config", "path"]);
    assert_success(&human);
    let shown = stdout_string(&human);
    assert!(
        shown.contains(&harness.config_path.display().to_string()),
        "{shown}"
    );
    assert!(shown.contains("default location"), "{shown}");
}

/// The recovery path: an override reaches a config file of its own while
/// the default file is one this build refuses to read.
#[test]
fn init_runs_through_an_override_while_the_default_config_is_unreadable() {
    let harness = Harness::new();
    harness.write_cli_config("config_version = 1\nunknown_knob = true\n");
    let broken = fs::read_to_string(&harness.config_path).expect("read broken config");

    let flagged_path = harness.temp_dir.path().join("recovery").join("config.toml");
    let flagged = flagged_path.to_str().expect("utf-8 path");
    let init = harness.run(&[
        "--json",
        "--config",
        flagged,
        "init",
        "rescue",
        "--mode",
        "embedded",
        "--store-kind",
        "local-fs",
        "--root",
        harness.store_root("rescue").to_str().expect("utf-8 path"),
    ]);
    assert_success(&init);
    assert_eq!(json_data(&init)["mode"], "embedded");
    assert!(
        flagged_path.exists(),
        "init creates the directories it needs"
    );

    let list = harness.run(&["--json", "--config", flagged, "profile", "list"]);
    assert_success(&list);
    assert_eq!(json_data(&list)["profiles"][0]["name"], "rescue");

    // The environment is the same way out.
    let env_path = harness.temp_dir.path().join("by-env").join("config.toml");
    let env_init = harness.run_with_env(
        &[("LOONFS_CONFIG", &env_path)],
        &[
            "--json",
            "init",
            "byenv",
            "--mode",
            "embedded",
            "--store-kind",
            "local-fs",
            "--root",
            harness.store_root("byenv").to_str().expect("utf-8 path"),
        ],
    );
    assert_success(&env_init);
    let env_list = harness.run_with_env(
        &[("LOONFS_CONFIG", &env_path)],
        &["--json", "profile", "list"],
    );
    assert_success(&env_list);
    assert_eq!(json_data(&env_list)["profiles"][0]["name"], "byenv");

    // Neither route read or rewrote the file that started this.
    assert_eq!(
        fs::read_to_string(&harness.config_path).expect("read unchanged config"),
        broken
    );
}

/// A file this build will not read names itself, what in it went wrong, and
/// the two ways past it.
#[test]
fn unreadable_config_errors_name_the_file_the_field_and_the_way_out() {
    let harness = Harness::new();
    harness.write_cli_config("config_version = 1\ndefault_profil = \"typo\"\n");

    let list = harness.run(&["--json", "profile", "list"]);
    assert_failure(&list);
    let error = json_error(&list);
    assert_eq!(error["code"], "invalid_config");
    let message = error["message"].as_str().expect("json string");
    assert!(
        message.contains(&harness.config_path.display().to_string()),
        "{message}"
    );
    assert!(message.contains("default_profil"), "{message}");
    assert!(message.contains("line 2"), "{message}");
    assert!(message.contains("--config"), "{message}");
    assert!(message.contains("LOONFS_CONFIG"), "{message}");

    // A semantic failure is just as much a wall, so it carries the same way
    // past it.
    harness.write_cli_config("config_version = 1\ndefault_profile = \"missing\"\n");
    let unresolvable = harness.run(&["--json", "profile", "list"]);
    assert_failure(&unresolvable);
    let message = json_error(&unresolvable)["message"]
        .as_str()
        .expect("json string")
        .to_owned();
    assert!(
        message.contains(&harness.config_path.display().to_string()),
        "{message}"
    );
    assert!(message.contains("--config"), "{message}");
    assert!(message.contains("LOONFS_CONFIG"), "{message}");
}

#[test]
fn unreachable_servers_are_named_with_their_url() {
    let harness = Harness::new();
    // A port that was just free with nothing listening: connection refused.
    let dead_url = format!("http://127.0.0.1:{}", available_port());
    let create = harness.run(&[
        "--json",
        "profile",
        "create",
        "dead",
        "--mode",
        "remote",
        "--server-url",
        &dead_url,
    ]);
    assert_success(&create);

    let attempt = harness.run(&["namespace", "create", "ghost"]);
    assert_failure(&attempt);
    let message = stderr_string(&attempt);
    assert!(message.contains("cannot connect to"), "{message}");
    assert!(message.contains(&dead_url), "{message}");
    assert!(message.contains("`server_url`"), "{message}");
}

#[test]
fn embedded_profile_filesystem_flow_works_end_to_end() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");

    let upload_path = harness.temp_dir.path().join("upload.txt");
    let update_path = harness.temp_dir.path().join("updated.txt");
    let download_path = harness.temp_dir.path().join("downloaded.txt");
    fs::write(&upload_path, b"hello from direct core\n").expect("upload payload");
    fs::write(&update_path, b"updated from direct core\n").expect("updated payload");

    assert_success(&harness.run(&["namespace", "create", "demo"]));
    assert_success(&harness.run(&["use", "demo"]));

    let mkdir = harness.run(&["--json", "mkdir", "/docs"]);
    assert_success(&mkdir);
    assert_eq!(json_data(&mkdir)["target"], "demo:/docs");

    let docs = harness.run(&["--json", "stat", "/docs"]);
    assert_success(&docs);
    assert_eq!(json_data(&docs)["inode_kind"], "dir");

    let put = harness.run(&[
        "--json",
        "put",
        upload_path.to_str().expect("utf-8 path"),
        "/docs/hello.txt",
    ]);
    assert_success(&put);
    assert_eq!(json_data(&put)["target"], "demo:/docs/hello.txt");

    let put_conflict = harness.run(&[
        "--json",
        "put",
        upload_path.to_str().expect("utf-8 path"),
        "/docs/hello.txt",
    ]);
    assert_failure(&put_conflict);
    assert_eq!(json_error(&put_conflict)["code"], "path_conflict");

    let put_force = harness.run(&[
        "--json",
        "put",
        update_path.to_str().expect("utf-8 path"),
        "/docs/hello.txt",
        "--force",
    ]);
    assert_success(&put_force);

    let revisions = harness.run(&["--json", "revisions", "/docs/hello.txt"]);
    assert_success(&revisions);
    assert_eq!(
        json_data(&revisions)["revisions"]
            .as_array()
            .expect("json array")
            .len(),
        2
    );

    let old_cat = harness.run(&["cat", "--revision", "1", "/docs/hello.txt"]);
    assert_success(&old_cat);
    assert_eq!(old_cat.stdout, b"hello from direct core\n");

    let cp = harness.run(&["--json", "cp", "/docs/hello.txt", "/docs/copy.txt"]);
    assert_success(&cp);

    let source = harness.run(&["--json", "stat", "/docs/hello.txt"]);
    let copy = harness.run(&["--json", "stat", "/docs/copy.txt"]);
    assert_success(&source);
    assert_success(&copy);
    assert_ne!(json_data(&source)["inode_id"], json_data(&copy)["inode_id"]);
    assert_eq!(
        json_data(&source)["content_ref"],
        json_data(&copy)["content_ref"]
    );

    let cat = harness.run(&["cat", "/docs/hello.txt"]);
    assert_success(&cat);
    assert_eq!(cat.stdout, b"updated from direct core\n");

    let get_stdout = harness.run(&["get", "/docs/hello.txt", "-"]);
    assert_success(&get_stdout);
    assert_eq!(get_stdout.stdout, b"updated from direct core\n");

    let get_old_stdout = harness.run(&["get", "--revision", "1", "/docs/hello.txt", "-"]);
    assert_success(&get_old_stdout);
    assert_eq!(get_old_stdout.stdout, b"hello from direct core\n");

    let get_file = harness.run(&[
        "--json",
        "get",
        "/docs/hello.txt",
        download_path.to_str().expect("utf-8 path"),
    ]);
    assert_success(&get_file);
    assert_eq!(
        fs::read(&download_path).expect("downloaded bytes"),
        b"updated from direct core\n"
    );

    let restore = harness.run(&["--json", "restore", "--revision", "1", "/docs/hello.txt"]);
    assert_success(&restore);
    let restored = harness.run(&["cat", "/docs/hello.txt"]);
    assert_success(&restored);
    assert_eq!(restored.stdout, b"hello from direct core\n");

    let mv = harness.run(&["--json", "mv", "/docs/copy.txt", "/docs/final.txt"]);
    assert_success(&mv);

    let rm_dir = harness.run(&["--json", "rm", "/docs"]);
    assert_failure(&rm_dir);
    assert_eq!(json_error(&rm_dir)["code"], "directory_not_empty");

    let rm = harness.run(&["--json", "rm", "/docs/final.txt"]);
    assert_success(&rm);
}

#[test]
fn put_expected_revision_replaces_only_the_observed_revision() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    assert_success(&harness.run(&["namespace", "create", "demo"]));
    assert_success(&harness.run(&["use", "demo"]));

    let payload = harness.temp_dir.path().join("doc.txt");
    fs::write(&payload, b"v1").expect("write payload");
    assert_success(&harness.run(&["put", payload.to_str().expect("utf-8 path"), "/doc.txt"]));

    // The guard implies --force: replacing the revision the caller observed
    // needs no second flag.
    fs::write(&payload, b"v2").expect("write payload");
    let guarded = harness.run(&[
        "--json",
        "put",
        payload.to_str().expect("utf-8 path"),
        "/doc.txt",
        "--expected-revision",
        "1",
    ]);
    assert_success(&guarded);
    let stat = harness.run(&["--json", "stat", "/doc.txt"]);
    assert_success(&stat);
    assert_eq!(json_data(&stat)["revision_no"], 2);

    // A raced write fails instead of stacking on it: the file has moved on
    // from revision 1, so the same guard now reports the stale revision.
    fs::write(&payload, b"v3").expect("write payload");
    let stale = harness.run(&[
        "--json",
        "put",
        payload.to_str().expect("utf-8 path"),
        "/doc.txt",
        "--expected-revision",
        "1",
    ]);
    assert_failure(&stale);
    assert_eq!(json_error(&stale)["code"], "stale_revision");
    // The rejection reads as a sentence, with both revisions in it and no
    // Rust formatting of the one that may be absent.
    let stale_error = json_error(&stale);
    let message = stale_error["message"].as_str().unwrap_or_default();
    assert!(
        message.ends_with("expected revision 1, found revision 2"),
        "{message}"
    );
    // The embedded backend carries the same structured details a server's
    // envelope would, so `--json` consumers read one contract from both
    // profiles.
    assert_eq!(stale_error["details"]["expected_revision_no"], 1);
    assert_eq!(stale_error["details"]["actual_revision_no"], 2);
    let cat = harness.run(&["cat", "/doc.txt"]);
    assert_success(&cat);
    assert_eq!(cat.stdout, b"v2");
}

#[test]
fn concurrent_embedded_puts_land_or_report_the_fence() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    assert_success(&harness.run(&["namespace", "create", "demo"]));
    assert_success(&harness.run(&["use", "demo"]));

    let mut payloads = Vec::new();
    for index in 0..4 {
        let path = harness.temp_dir.path().join(format!("payload-{index}.txt"));
        fs::write(&path, format!("payload {index}")).expect("write payload");
        payloads.push(path);
    }

    // Four simultaneous processes: each is its own writer session, so they
    // fence each other on acquisition. Fencing is terminal — no silent
    // reacquisition — so the contract is honesty, not recovery: every
    // process either lands its put or reports `writer_fenced`, the last
    // acquirer always lands, and a fenced put commits nothing.
    let children: Vec<Child> = payloads
        .iter()
        .enumerate()
        .map(|(index, path)| {
            Command::new(loon_binary_path())
                .env("HOME", &harness.home_dir)
                .args([
                    "--json",
                    "put",
                    path.to_str().expect("utf-8 path"),
                    &format!("/docs/file-{index}.txt"),
                ])
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .expect("spawn loonfs put")
        })
        .collect();
    let mut landed = Vec::new();
    for (index, child) in children.into_iter().enumerate() {
        let output = child.wait_with_output().expect("join loonfs put");
        if output.status.success() {
            landed.push(index);
        } else {
            assert_eq!(json_error(&output)["code"], "writer_fenced");
        }
    }
    assert!(
        !landed.is_empty(),
        "the last writer to acquire faces no later fence and must land"
    );

    for index in 0..4 {
        let stat = harness.run(&["--json", "stat", &format!("/docs/file-{index}.txt")]);
        if landed.contains(&index) {
            assert_success(&stat);
        } else {
            assert_failure(&stat);
            assert_eq!(json_error(&stat)["code"], "path_not_found");
        }
    }
}

#[test]
fn commit_messages_ride_the_feed_and_bind_identity() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    assert_success(&harness.run(&["namespace", "create", "demo"]));
    assert_success(&harness.run(&["use", "demo"]));

    let payload = harness.temp_dir.path().join("doc.txt");
    fs::write(&payload, b"v1").expect("write payload");
    assert_success(&harness.run(&[
        "put",
        payload.to_str().expect("utf-8 path"),
        "/doc.txt",
        "-m",
        "initial import",
    ]));
    // The audit's motivating case: a restore is indistinguishable from an
    // edit in the feed without a message.
    fs::write(&payload, b"v2").expect("write payload");
    assert_success(&harness.run(&[
        "put",
        payload.to_str().expect("utf-8 path"),
        "/doc.txt",
        "--force",
    ]));
    assert_success(&harness.run(&[
        "--json",
        "restore",
        "--revision",
        "1",
        "/doc.txt",
        "-m",
        "roll back to the imported copy",
    ]));

    let changes = harness.run(&["--json", "changes"]);
    assert_success(&changes);
    let rows = json_data(&changes)["changes"]
        .as_array()
        .expect("changes array")
        .clone();
    assert_eq!(rows[0]["message"], "initial import");
    assert!(rows[1].get("message").is_none());
    assert_eq!(rows[2]["message"], "roll back to the imported copy");

    // The message is part of the commit's identity: the same commit id with
    // a different message conflicts instead of silently replaying.
    let first = harness.run(&[
        "--json",
        "mkdir",
        "/pinned",
        "--commit-id",
        "pinned-mkdir",
        "--message",
        "one",
    ]);
    assert_success(&first);
    let replay = harness.run(&[
        "--json",
        "mkdir",
        "/pinned",
        "--commit-id",
        "pinned-mkdir",
        "--message",
        "one",
    ]);
    assert_success(&replay);
    assert_eq!(
        json_data(&first)["committed_seq"],
        json_data(&replay)["committed_seq"],
        "an identical retry replays the original commit"
    );
    let conflicted = harness.run(&[
        "--json",
        "mkdir",
        "/pinned",
        "--commit-id",
        "pinned-mkdir",
        "--message",
        "two",
    ]);
    assert_failure(&conflicted);
    assert_eq!(
        json_error(&conflicted)["code"],
        "commit_id_reuse_conflict",
        "{}",
        json_error(&conflicted)
    );

    // A put's identity includes *which* content object it attaches, and
    // `loonfs put` uploads its file every time it runs, so a rerun under the
    // same commit id is a different mutation as far as the server is
    // concerned. What makes rerunning safe anyway is the client comparing
    // the bytes it just sent against what that commit id actually
    // committed: identical bytes mean the command had already succeeded, so
    // it reports the same commit rather than a conflict.
    let local_payload = payload.to_str().expect("utf-8 path");
    let first = harness.run(&[
        "--json",
        "put",
        local_payload,
        "/pinned.txt",
        "--commit-id",
        "pinned-put",
    ]);
    assert_success(&first);
    let rerun = harness.run(&[
        "--json",
        "put",
        local_payload,
        "/pinned.txt",
        "--commit-id",
        "pinned-put",
    ]);
    assert_success(&rerun);
    assert_eq!(
        json_data(&rerun)["committed_seq"],
        json_data(&first)["committed_seq"],
        "rerunning an identical put must report the commit that already landed"
    );

    // Different bytes under that commit id are a different operation, and
    // the conflict stands.
    let changed = harness.temp_dir.path().join("changed.txt");
    fs::write(&changed, b"different pinned bytes\n").expect("write changed payload");
    let conflicting = harness.run(&[
        "--json",
        "put",
        changed.to_str().expect("utf-8 path"),
        "/pinned.txt",
        "--commit-id",
        "pinned-put",
    ]);
    assert_failure(&conflicting);
    assert_eq!(
        json_error(&conflicting)["code"],
        "commit_id_reuse_conflict",
        "{}",
        json_error(&conflicting)
    );
}

/// A file small enough that a tree holding it uploads it in one request,
/// beside one that is not.
const SMALL_TREE_FILE: &[u8] = b"small enough to hold";

/// A payload past the size at which a put stops holding its bytes whole.
/// It is the smallest payload that exercises the streaming path at all.
fn streaming_payload() -> Vec<u8> {
    let len = 8 * 1024 * 1024 + 1_024;
    (0..len).map(|offset| (offset % 251) as u8).collect()
}

/// Reads a remote file back through the CLI and returns its bytes.
fn download(harness: &Harness, remote_path: &str, name: &str) -> Vec<u8> {
    let local = harness.temp_dir.path().join(name);
    assert_success(&harness.run(&[
        "get",
        remote_path,
        local.to_str().expect("utf-8 path"),
        "--force",
    ]));
    fs::read(&local).expect("read downloaded file")
}

/// Events of one kind, in the order they were reported.
fn events_of_kind(output: &Output, kind: &str) -> Vec<Value> {
    json_progress_events(output)
        .into_iter()
        .filter(|event| event["kind"] == kind)
        .collect()
}

/// A download that takes real time says where it has got to, in events an
/// agent can tell apart from a hang, and says so without disturbing the
/// result document on standard output.
#[test]
fn a_download_reports_its_progress_to_an_agent() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    assert_success(&harness.run(&["namespace", "create", "demo"]));
    assert_success(&harness.run(&["use", "demo"]));

    let payload = streaming_payload();
    let local = harness.temp_dir.path().join("big.bin");
    fs::write(&local, &payload).expect("write payload");
    assert_success(&harness.run(&["put", local.to_str().expect("utf-8 path"), "/big.bin"]));

    let back = harness.temp_dir.path().join("back.bin");
    let get = harness.run(&[
        "--json",
        "get",
        "/big.bin",
        back.to_str().expect("utf-8 path"),
    ]);
    assert_success(&get);
    assert_eq!(json_data(&get)["bytes_written"], payload.len() as u64);

    let started = events_of_kind(&get, "file_started");
    assert_eq!(started.len(), 1, "one file, one start: {started:?}");
    assert_eq!(started[0]["op"], "get");
    assert_eq!(started[0]["path"], "/big.bin");
    assert_eq!(started[0]["bytes_total"], payload.len() as u64);

    let progress = events_of_kind(&get, "progress");
    assert!(
        !progress.is_empty(),
        "a download past one chunk reports as it lands"
    );
    let last = progress.last().expect("a progress event");
    assert_eq!(last["op"], "get");
    assert_eq!(last["bytes_done"], payload.len() as u64);
    assert_eq!(last["bytes_total"], payload.len() as u64);
    assert_eq!(last["files_total"], 1);
    assert!(last["rate_bps"].is_u64(), "a rate is always reported");
    assert!(last["elapsed_ms"].is_u64());

    let finished = events_of_kind(&get, "file_finished");
    assert_eq!(finished.len(), 1, "one file, one finish: {finished:?}");
    assert_eq!(finished[0]["bytes_done"], payload.len() as u64);
    assert_eq!(finished[0]["path"], "/big.bin");
}

/// An upload counts the payload as it is read and then names the commit,
/// which is the stretch where time passes and no bytes move.
#[test]
fn an_upload_reports_bytes_read_and_then_the_commit() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    assert_success(&harness.run(&["namespace", "create", "demo"]));
    assert_success(&harness.run(&["use", "demo"]));

    let payload = streaming_payload();
    let local = harness.temp_dir.path().join("big.bin");
    fs::write(&local, &payload).expect("write payload");
    let put = harness.run(&[
        "--json",
        "put",
        local.to_str().expect("utf-8 path"),
        "/big.bin",
    ]);
    assert_success(&put);

    let progress = events_of_kind(&put, "progress");
    let last = progress.last().expect("a progress event");
    assert_eq!(last["op"], "put");
    assert_eq!(last["path"], "/big.bin");
    assert_eq!(last["bytes_done"], payload.len() as u64);
    assert_eq!(last["bytes_total"], payload.len() as u64);

    let phases = events_of_kind(&put, "phase");
    assert_eq!(phases.len(), 1, "one transition to report: {phases:?}");
    assert_eq!(phases[0]["phase"], "committing");
    assert_eq!(phases[0]["op"], "put");

    // A payload with no knowable length still counts its bytes; it just
    // has no total to measure them against.
    let piped = harness.run_with_stdin(&["--json", "put", "-", "/piped.bin"], &payload);
    assert_success(&piped);
    let piped_progress = events_of_kind(&piped, "progress");
    let last = piped_progress.last().expect("a progress event");
    assert_eq!(last["bytes_done"], payload.len() as u64);
    assert!(
        last["bytes_total"].is_null(),
        "a pipe has no total: {last:?}"
    );
}

/// A tree is measured as a tree: one operation's bytes and files, plus a
/// start and a finish for every file inside it.
#[test]
fn a_recursive_transfer_counts_files_and_bytes_for_the_whole_tree() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    assert_success(&harness.run(&["namespace", "create", "demo"]));
    assert_success(&harness.run(&["use", "demo"]));

    let tree = harness.temp_dir.path().join("tree");
    fs::create_dir_all(tree.join("docs")).expect("create tree dirs");
    fs::write(tree.join("top.txt"), b"top").expect("write top");
    fs::write(tree.join("docs/a.txt"), b"alpha").expect("write a");
    fs::write(tree.join("docs/b.txt"), b"beta").expect("write b");
    let tree_bytes = (b"top".len() + b"alpha".len() + b"beta".len()) as u64;

    let put = harness.run(&[
        "--json",
        "put",
        "-r",
        tree.to_str().expect("utf-8 path"),
        "/up",
    ]);
    assert_success(&put);
    assert_eq!(events_of_kind(&put, "file_started").len(), 3);
    assert_eq!(events_of_kind(&put, "file_finished").len(), 3);
    let last = events_of_kind(&put, "progress")
        .pop()
        .expect("a progress event");
    assert_eq!(last["op"], "put");
    assert_eq!(last["path"], "demo:/up");
    assert_eq!(last["bytes_done"], tree_bytes);
    assert_eq!(last["bytes_total"], tree_bytes);
    assert_eq!(last["files_total"], 3);
    assert!(
        events_of_kind(&put, "phase").is_empty(),
        "several files of a tree are in flight at once, so no one file's \
         commit is the operation's: {:?}",
        events_of_kind(&put, "phase")
    );

    let back = harness.temp_dir.path().join("back");
    let get = harness.run(&[
        "--json",
        "get",
        "-r",
        "/up",
        back.to_str().expect("utf-8 path"),
    ]);
    assert_success(&get);
    assert_eq!(events_of_kind(&get, "file_started").len(), 3);
    assert_eq!(events_of_kind(&get, "file_finished").len(), 3);
    let last = events_of_kind(&get, "progress")
        .pop()
        .expect("a progress event");
    assert_eq!(last["op"], "get");
    assert_eq!(last["path"], "demo:/up");
    assert_eq!(last["bytes_done"], tree_bytes);
    assert_eq!(last["bytes_total"], tree_bytes);
    assert_eq!(last["files_done"], 3);
    assert_eq!(last["files_total"], 3);
}

/// A tree's large file is read once, a piece at a time, exactly as a single
/// `put` of it is. So the tree's byte count moves through that file rather
/// than jumping over it once the whole thing has been read — which is what
/// an upload that held each file whole could only ever report.
#[test]
fn a_recursive_put_counts_a_large_file_as_it_is_read() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    assert_success(&harness.run(&["namespace", "create", "demo"]));
    assert_success(&harness.run(&["use", "demo"]));

    let tree = harness.temp_dir.path().join("tree");
    fs::create_dir_all(tree.join("docs")).expect("create tree dirs");
    let payload = streaming_payload();
    fs::write(tree.join("docs/big.bin"), &payload).expect("write big");
    fs::write(tree.join("small.txt"), SMALL_TREE_FILE).expect("write small");
    let tree_bytes = (payload.len() + SMALL_TREE_FILE.len()) as u64;

    let put = harness.run(&[
        "--json",
        "put",
        "-r",
        tree.to_str().expect("utf-8 path"),
        "/up",
    ]);
    assert_success(&put);
    assert_eq!(json_data(&put)["files"], 2);

    let counts: Vec<u64> = events_of_kind(&put, "progress")
        .iter()
        .map(|event| event["bytes_done"].as_u64().expect("a byte count"))
        .collect();
    // The only counts a whole-file-at-a-time upload could ever report.
    let whole_files = [
        0,
        SMALL_TREE_FILE.len() as u64,
        payload.len() as u64,
        tree_bytes,
    ];
    assert!(
        counts.iter().any(|count| !whole_files.contains(count)),
        "a payload read in pieces reports counts taken part way through it: {counts:?}"
    );
    assert_eq!(counts.last(), Some(&tree_bytes), "{counts:?}");
    assert_eq!(
        download(&harness, "/up/docs/big.bin", "big-back.bin"),
        payload
    );
}

/// The same tree over the remote transport, where a large file is the one
/// upload with parts to lose. Each file keeps its own resume record, named
/// by the profile, namespace, remote path, and local file that decide which
/// upload it is — and a run that committed leaves none of them behind.
#[test]
fn a_recursive_put_streams_a_large_file_over_the_remote_transport() {
    let harness = Harness::new();
    let remote_server =
        harness.start_external_server(harness.write_server_config("remote", "recursive-remote"));
    assert_success(&harness.run(&[
        "--json",
        "profile",
        "create",
        "default",
        "--mode",
        "remote",
        "--server-url",
        &remote_server.server_url,
        "--auth-token",
        "test-token",
    ]));
    assert_success(&harness.run(&["namespace", "create", "demo"]));
    assert_success(&harness.run(&["use", "demo"]));

    let tree = harness.temp_dir.path().join("tree");
    fs::create_dir_all(tree.join("docs")).expect("create tree dirs");
    let payload = streaming_payload();
    fs::write(tree.join("docs/big.bin"), &payload).expect("write big");
    fs::write(tree.join("small.txt"), SMALL_TREE_FILE).expect("write small");

    let state_home = harness.temp_dir.path().join("state");
    let put = harness.run_with_env(
        &[("XDG_STATE_HOME", state_home.as_path())],
        &[
            "--json",
            "put",
            "-r",
            tree.to_str().expect("utf-8 path"),
            "/up",
        ],
    );
    assert_success(&put);
    assert_eq!(json_data(&put)["files"], 2);
    assert_eq!(
        download(&harness, "/up/docs/big.bin", "big-back.bin"),
        payload
    );
    assert_eq!(
        download(&harness, "/up/small.txt", "small-back.txt"),
        SMALL_TREE_FILE
    );

    let records: Vec<PathBuf> = fs::read_dir(state_home.join("loonfs").join("uploads"))
        .map(|entries| {
            entries
                .map(|entry| entry.expect("dir entry").path())
                .collect()
        })
        .unwrap_or_default();
    assert!(
        records.is_empty(),
        "an upload that committed keeps no record for a rerun to pick up: {records:?}"
    );
}

/// What a tree holds that is neither a file nor a directory is named and
/// skipped rather than silently dropped, and the walk carries on around it.
/// Unix only: nothing else in the suite can make one.
#[cfg(unix)]
#[test]
fn a_recursive_put_names_what_it_will_not_transfer() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    assert_success(&harness.run(&["namespace", "create", "demo"]));
    assert_success(&harness.run(&["use", "demo"]));

    let tree = harness.temp_dir.path().join("tree");
    fs::create_dir_all(&tree).expect("create tree dir");
    fs::write(tree.join("real.txt"), b"real").expect("write file");
    std::os::unix::fs::symlink(tree.join("real.txt"), tree.join("link.txt")).expect("make symlink");

    let put = harness.run(&[
        "--json",
        "put",
        "-r",
        tree.to_str().expect("utf-8 path"),
        "/up",
    ]);
    assert_failure(&put);
    let data = json_data(&put);
    assert_eq!(
        data["files"], 1,
        "the regular file still transferred: {data}"
    );
    let failures = data["failures"].as_array().expect("failures");
    assert_eq!(failures.len(), 1, "{data}");
    assert!(
        failures[0]["path"]
            .as_str()
            .expect("a path")
            .ends_with("link.txt"),
        "{data}"
    );
    assert!(
        failures[0]["error"]["message"]
            .as_str()
            .expect("a message")
            .contains("symlinks and special"),
        "{data}"
    );
    assert_success(&harness.run(&["--json", "stat", "/up/real.txt"]));
}

/// Nobody asked, so nobody is told: a run whose standard error is a pipe
/// and which asked for no events says nothing about the transfer, and
/// --no-progress silences the agent stream too.
#[test]
fn progress_is_silent_unless_someone_is_watching() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    assert_success(&harness.run(&["namespace", "create", "demo"]));
    assert_success(&harness.run(&["use", "demo"]));

    let local = harness.temp_dir.path().join("doc.txt");
    fs::write(&local, b"body").expect("write payload");
    let put = harness.run(&["put", local.to_str().expect("utf-8 path"), "/doc.txt"]);
    assert_success(&put);
    assert_eq!(
        stderr_string(&put),
        "",
        "a piped run draws no line, the way curl does not"
    );

    let quiet = harness.run(&[
        "--json",
        "--no-progress",
        "put",
        local.to_str().expect("utf-8 path"),
        "/quiet.txt",
    ]);
    assert_success(&quiet);
    assert_eq!(
        stderr_string(&quiet),
        "",
        "--no-progress silences the event stream"
    );

    // A failure still reports itself, and is still the last document on
    // standard error when progress preceded it.
    let clash = harness.run(&[
        "--json",
        "put",
        local.to_str().expect("utf-8 path"),
        "/doc.txt",
    ]);
    assert_failure(&clash);
    assert_eq!(json_error(&clash)["code"], "path_conflict");
}

#[test]
fn no_progress_silences_recursive_item_lines() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    assert_success(&harness.run(&["namespace", "create", "demo"]));
    assert_success(&harness.run(&["use", "demo"]));

    let tree = harness.temp_dir.path().join("quiet-tree");
    fs::create_dir_all(tree.join("empty")).expect("create tree dirs");
    fs::write(tree.join("doc.txt"), b"body").expect("write tree file");

    let put = harness.run(&[
        "--no-progress",
        "put",
        "-r",
        tree.to_str().expect("utf-8 path"),
        "/quiet-tree",
    ]);

    assert_success(&put);
    assert_eq!(stderr_string(&put), "");
}

/// A large file and a pipe both round-trip through an embedded profile,
/// and the retry contract holds for them exactly as it does for a payload
/// small enough to hold.
#[test]
fn large_and_piped_puts_round_trip_through_an_embedded_profile() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    assert_success(&harness.run(&["namespace", "create", "demo"]));
    assert_success(&harness.run(&["use", "demo"]));
    let payload = streaming_payload();

    let local = harness.temp_dir.path().join("big.bin");
    fs::write(&local, &payload).expect("write payload");
    let local_path = local.to_str().expect("utf-8 path");
    // `--force` on every run of this id, first included: the replacement
    // behavior is part of a commit's identity, so a rerun that adds the
    // flag is asking for a different mutation and conflicts.
    let first = harness.run(&[
        "--json",
        "put",
        local_path,
        "/big.bin",
        "--commit-id",
        "pinned-big",
        "--force",
    ]);
    assert_success(&first);
    assert_eq!(download(&harness, "/big.bin", "big-back.bin"), payload);

    // The same reconciliation the buffered path has: the rerun uploads
    // again, conflicts on identity, and resolves to the commit that landed.
    let rerun = harness.run(&[
        "--json",
        "put",
        local_path,
        "/big.bin",
        "--commit-id",
        "pinned-big",
        "--force",
    ]);
    assert_success(&rerun);
    assert_eq!(
        json_data(&rerun)["committed_seq"],
        json_data(&first)["committed_seq"],
        "rerunning an identical large put must report the commit that already landed"
    );

    let mut changed = payload.clone();
    changed[0] ^= 0xff;
    let changed_path = harness.temp_dir.path().join("changed.bin");
    fs::write(&changed_path, &changed).expect("write changed payload");
    let conflicting = harness.run(&[
        "--json",
        "put",
        changed_path.to_str().expect("utf-8 path"),
        "/big.bin",
        "--commit-id",
        "pinned-big",
        "--force",
    ]);
    assert_failure(&conflicting);
    assert_eq!(
        json_error(&conflicting)["code"],
        "commit_id_reuse_conflict",
        "{}",
        json_error(&conflicting)
    );

    // Standard input has no length to declare and no name to derive a
    // destination from, so it needs one spelled out and takes the same
    // read-once path.
    assert_success(&harness.run_with_stdin(&["--json", "put", "-", "/piped.bin"], &payload));
    assert_eq!(download(&harness, "/piped.bin", "piped-back.bin"), payload);

    let no_destination = harness.run_with_stdin(&["--json", "put", "-"], b"anything");
    assert_failure(&no_destination);
    assert_eq!(json_error(&no_destination)["code"], "invalid_input");
}

/// A payload of several download chunks, so a `get` that read the file whole
/// and one that reads it in chunks are told apart by what comes back rather
/// than by what a comment claims.
fn multi_chunk_payload() -> Vec<u8> {
    let len = 3 * loonfs::CONTENT_READ_CHUNK_BYTES as usize + 1_024;
    (0..len).map(|offset| (offset % 251) as u8).collect()
}

/// A file many chunks long round-trips through an embedded profile to a
/// local file and to standard output, byte for byte.
#[test]
fn a_multi_chunk_file_round_trips_to_a_file_and_to_stdout() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    assert_success(&harness.run(&["namespace", "create", "demo"]));
    assert_success(&harness.run(&["use", "demo"]));
    let payload = multi_chunk_payload();

    let local = harness.temp_dir.path().join("chunked.bin");
    fs::write(&local, &payload).expect("write payload");
    assert_success(&harness.run(&["put", local.to_str().expect("utf-8 path"), "/chunked.bin"]));

    assert_eq!(
        download(&harness, "/chunked.bin", "chunked-back.bin"),
        payload,
        "a downloaded file is the file that was uploaded"
    );

    let streamed = harness.run(&["get", "/chunked.bin", "-"]);
    assert_success(&streamed);
    assert_eq!(
        streamed.stdout, payload,
        "streaming to stdout writes the content and nothing else"
    );
}

/// Content that no longer matches its reference fails the download, and
/// fails it without leaving anything at the destination: no file, and no
/// partial file beside it. A verified-looking local copy of unverified bytes
/// is the outcome the temp-file-then-rename order exists to prevent.
#[test]
fn a_download_of_corrupted_content_leaves_nothing_at_the_destination() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    assert_success(&harness.run(&["namespace", "create", "demo"]));
    assert_success(&harness.run(&["use", "demo"]));

    let payload = b"the bytes that were committed".to_vec();
    let local = harness.temp_dir.path().join("source.bin");
    fs::write(&local, &payload).expect("write payload");
    assert_success(&harness.run(&["put", local.to_str().expect("utf-8 path"), "/doc.bin"]));

    // Same length, different bytes: the reference's digest is the only
    // thing that can tell, which is what the read has to notice.
    let object = content_object_path(&harness.store_root("default"), payload.len() as u64);
    let mut corrupted = payload.clone();
    corrupted[0] ^= 0xff;
    fs::write(&object, &corrupted).expect("corrupt content object");

    let destination = harness.temp_dir.path().join("downloads").join("doc.bin");
    fs::create_dir_all(destination.parent().expect("parent")).expect("create download dir");
    let failed = harness.run(&[
        "--json",
        "get",
        "/doc.bin",
        destination.to_str().expect("utf-8 path"),
    ]);
    assert_failure(&failed);
    assert_eq!(json_error(&failed)["code"], "namespace_corrupt");
    assert!(
        !destination.exists(),
        "a failed download must not install a file"
    );
    let leftovers: Vec<PathBuf> = fs::read_dir(destination.parent().expect("parent"))
        .expect("read download dir")
        .map(|entry| entry.expect("dir entry").path())
        .collect();
    assert!(
        leftovers.is_empty(),
        "the partial file must be cleaned up, found {leftovers:?}"
    );
}

/// The bytes and the note an interrupted download leaves beside its
/// destination, as this CLI names them.
fn partial_paths(destination: &Path) -> (PathBuf, PathBuf) {
    let name = destination
        .file_name()
        .expect("destination file name")
        .to_str()
        .expect("utf-8 name");
    let parent = destination.parent().expect("destination parent");
    (
        parent.join(format!(".{name}.loonfs-partial")),
        parent.join(format!(".{name}.loonfs-partial.meta")),
    )
}

/// Lays down the bytes and note an interrupted download of `remote_path`
/// would have left, having got `held` bytes in.
fn leave_a_partial_download(
    harness: &Harness,
    remote_path: &str,
    destination: &Path,
    payload: &[u8],
    held: usize,
) {
    let stat = harness.run(&["--json", "stat", remote_path]);
    assert_success(&stat);
    let content_ref = json_data(&stat)["content_ref"].clone();
    let (partial, meta) = partial_paths(destination);
    fs::write(&partial, &payload[..held]).expect("write partial bytes");
    let note = serde_json::json!({
        "content_id": content_ref["content_id"],
        "size_bytes": content_ref["size_bytes"],
        "checksum": content_ref["checksum"],
        "revision_no": null,
    });
    fs::write(&meta, serde_json::to_vec(&note).expect("encode note")).expect("write note");
}

/// A download that stopped part way is picked up rather than started over:
/// the bytes already on disk are fetched again by nobody, and the file that
/// lands is still verified as a whole.
#[test]
fn an_interrupted_download_resumes_from_what_it_already_has() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    assert_success(&harness.run(&["namespace", "create", "demo"]));
    assert_success(&harness.run(&["use", "demo"]));

    let payload = streaming_payload();
    let source = harness.temp_dir.path().join("source.bin");
    fs::write(&source, &payload).expect("write payload");
    assert_success(&harness.run(&["put", source.to_str().expect("utf-8 path"), "/big.bin"]));

    let destination = harness.temp_dir.path().join("big.bin");
    let held = 3 * 1024 * 1024;
    leave_a_partial_download(&harness, "/big.bin", &destination, &payload, held);

    let get = harness.run(&[
        "--json",
        "get",
        "/big.bin",
        destination.to_str().expect("utf-8 path"),
    ]);
    assert_success(&get);
    assert_eq!(
        fs::read(&destination).expect("read destination"),
        payload,
        "a resumed download still lands the whole verified file"
    );

    let resuming: Vec<Value> = events_of_kind(&get, "phase")
        .into_iter()
        .filter(|event| event["phase"] == "resuming")
        .collect();
    assert_eq!(resuming.len(), 1, "one resume to report: {resuming:?}");
    assert_eq!(
        resuming[0]["bytes_done"], held as u64,
        "the run started at what was already on disk, not at zero"
    );

    let (partial, meta) = partial_paths(&destination);
    assert!(!partial.exists(), "an installed download leaves no partial");
    assert!(!meta.exists(), "and takes its note with it");
}

/// The note is what makes leftover bytes resumable. Bytes belonging to
/// other content, and bytes with nothing vouching for them, are dropped
/// without a word and the download starts over.
#[test]
fn a_partial_that_does_not_describe_this_file_is_started_over() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    assert_success(&harness.run(&["namespace", "create", "demo"]));
    assert_success(&harness.run(&["use", "demo"]));

    let payload = streaming_payload();
    let source = harness.temp_dir.path().join("source.bin");
    fs::write(&source, &payload).expect("write payload");
    assert_success(&harness.run(&["put", source.to_str().expect("utf-8 path"), "/big.bin"]));
    assert_success(&harness.run(&["put", source.to_str().expect("utf-8 path"), "/other.bin"]));

    // The note of a different file: same length, same bytes, and a content
    // id that says it is not this object.
    let destination = harness.temp_dir.path().join("big.bin");
    let held = 3 * 1024 * 1024;
    leave_a_partial_download(&harness, "/other.bin", &destination, &payload, held);

    let get = harness.run(&[
        "--json",
        "get",
        "/big.bin",
        destination.to_str().expect("utf-8 path"),
    ]);
    assert_success(&get);
    assert_eq!(fs::read(&destination).expect("read destination"), payload);
    assert!(
        events_of_kind(&get, "phase").is_empty(),
        "a download that started over reports no resume"
    );

    // Bytes with no note beside them say nothing about themselves either.
    let elsewhere = harness.temp_dir.path().join("again.bin");
    let (partial, _) = partial_paths(&elsewhere);
    fs::write(&partial, &payload[..held]).expect("write orphan partial");
    let get = harness.run(&[
        "--json",
        "get",
        "/big.bin",
        elsewhere.to_str().expect("utf-8 path"),
    ]);
    assert_success(&get);
    assert_eq!(fs::read(&elsewhere).expect("read destination"), payload);
    assert!(events_of_kind(&get, "phase").is_empty());
}

/// The one content object of a given length under a store root. Content
/// objects live at
/// `content-stores/<store>/objects/<first-shard>/<second-shard>/<id>`, and the
/// tests that use this write one file whose length nothing else shares.
fn content_object_path(store_root: &Path, size_bytes: u64) -> PathBuf {
    let objects = walkdir::WalkDir::new(store_root.join("content-stores"))
        .into_iter()
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            entry.file_type().is_file()
                && entry
                    .metadata()
                    .is_ok_and(|metadata| metadata.len() == size_bytes)
        })
        .map(|entry| entry.path().to_path_buf())
        .collect::<Vec<_>>();
    assert_eq!(
        objects.len(),
        1,
        "expected exactly one content object of {size_bytes} bytes, found {objects:?}"
    );
    objects.into_iter().next().expect("one content object")
}

/// The same two payloads over the remote transport. This deployment stores
/// to a local filesystem, so it cannot authorize direct part uploads and
/// the payload streams through the server instead — the fallback the client
/// picks from the capability document rather than from a guess.
#[test]
fn large_and_piped_puts_round_trip_over_the_remote_transport() {
    let harness = Harness::new();
    let remote_server =
        harness.start_external_server(harness.write_server_config("remote", "streaming-remote"));
    assert_success(&harness.run(&[
        "--json",
        "profile",
        "create",
        "default",
        "--mode",
        "remote",
        "--server-url",
        &remote_server.server_url,
        "--auth-token",
        "test-token",
    ]));
    assert_success(&harness.run(&["namespace", "create", "demo"]));
    assert_success(&harness.run(&["use", "demo"]));
    let payload = streaming_payload();

    let local = harness.temp_dir.path().join("big.bin");
    fs::write(&local, &payload).expect("write payload");
    assert_success(&harness.run(&[
        "--json",
        "put",
        local.to_str().expect("utf-8 path"),
        "/big.bin",
    ]));
    assert_eq!(download(&harness, "/big.bin", "big-back.bin"), payload);

    // No `Content-Length` to send: this body is chunked, and the server's
    // own incremental accounting is what bounds it.
    assert_success(&harness.run_with_stdin(&["--json", "put", "-", "/piped.bin"], &payload));
    assert_eq!(download(&harness, "/piped.bin", "piped-back.bin"), payload);
}

/// Every mutating command takes `-m`, and each one lands its annotation on
/// its own commit. Reading the feed back through `loonfs changes` covers the
/// flag, the threading, and the rendering in a single pass.
#[test]
fn every_mutating_command_records_its_message() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    assert_success(&harness.run(&["namespace", "create", "demo"]));
    assert_success(&harness.run(&["use", "demo"]));

    let payload = harness.temp_dir.path().join("doc.txt");
    fs::write(&payload, b"body").expect("write payload");
    let local = payload.to_str().expect("utf-8 path");

    assert_success(&harness.run(&["mkdir", "/dir", "-m", "mkdir message"]));
    assert_success(&harness.run(&["put", local, "/dir/doc.txt", "-m", "put message"]));
    assert_success(&harness.run(&["cp", "/dir/doc.txt", "/dir/copy.txt", "-m", "cp message"]));
    assert_success(&harness.run(&["mv", "/dir/copy.txt", "/dir/moved.txt", "-m", "mv message"]));
    assert_success(&harness.run(&[
        "put",
        local,
        "/dir/doc.txt",
        "--force",
        "-m",
        "second put message",
    ]));
    assert_success(&harness.run(&[
        "restore",
        "--revision",
        "1",
        "/dir/doc.txt",
        "-m",
        "restore message",
    ]));
    let removed = harness.run(&["--json", "rm", "/dir/moved.txt", "-m", "rm message"]);
    assert_success(&removed);
    let inode_id = json_data(&removed)["inode_id"]
        .as_u64()
        .expect("rm reports the deleted inode id");
    let deletion_seq = json_data(&removed)["committed_seq"]
        .as_u64()
        .expect("rm reports the committed seq");
    assert_success(&harness.run(&[
        "undelete",
        "/dir/moved.txt",
        "--inode",
        &inode_id.to_string(),
        "--deletion-seq",
        &deletion_seq.to_string(),
        "-m",
        "undelete message",
    ]));

    assert_eq!(
        feed_messages(&harness),
        vec![
            "mkdir message",
            "put message",
            "cp message",
            "mv message",
            "second put message",
            "restore message",
            "rm message",
            "undelete message",
        ]
    );
}

/// The remote arm has to hand the message to the client's mutation options,
/// not just the embedded arm. Same flag, same feed rows, over HTTP.
#[test]
fn commit_messages_ride_the_feed_over_the_remote_transport() {
    let harness = Harness::new();
    let remote_server =
        harness.start_external_server(harness.write_server_config("remote", "message-remote"));
    let add_remote = harness.run(&[
        "--json",
        "profile",
        "create",
        "default",
        "--mode",
        "remote",
        "--server-url",
        &remote_server.server_url,
        "--auth-token",
        "test-token",
    ]);
    assert_success(&add_remote);
    assert_success(&harness.run(&["namespace", "create", "demo"]));
    assert_success(&harness.run(&["use", "demo"]));

    let payload = harness.temp_dir.path().join("doc.txt");
    fs::write(&payload, b"body").expect("write payload");
    assert_success(&harness.run(&[
        "put",
        payload.to_str().expect("utf-8 path"),
        "/doc.txt",
        "-m",
        "landed over http",
    ]));
    assert_success(&harness.run(&["mkdir", "/dir", "-m", "made over http"]));

    assert_eq!(
        feed_messages(&harness),
        vec!["landed over http", "made over http"]
    );
}

/// Reads the namespace's change feed through the CLI and returns the
/// annotations in commit order, dropping the rows that carry none.
fn feed_messages(harness: &Harness) -> Vec<String> {
    let changes = harness.run(&["--json", "changes"]);
    assert_success(&changes);
    json_data(&changes)["changes"]
        .as_array()
        .expect("changes array")
        .iter()
        .filter_map(|row| row["message"].as_str().map(ToOwned::to_owned))
        .collect()
}

#[test]
fn trash_lists_recoverable_deletions_with_their_handles() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    assert_success(&harness.run(&["namespace", "create", "demo"]));
    assert_success(&harness.run(&["use", "demo"]));

    let payload = harness.temp_dir.path().join("doc.txt");
    fs::write(&payload, b"body").expect("write payload");
    assert_success(&harness.run(&[
        "put",
        payload.to_str().expect("utf-8 path"),
        "/docs/Quarterly Report.PDF",
    ]));
    assert_success(&harness.run(&[
        "put",
        payload.to_str().expect("utf-8 path"),
        "/notes/scratch.txt",
    ]));
    assert_success(&harness.run(&["rm", "/docs/Quarterly Report.PDF"]));
    assert_success(&harness.run(&["rm", "-r", "/notes"]));

    let trash = harness.run(&["--json", "trash"]);
    assert_success(&trash);
    let data = json_data(&trash);
    let entries = data["entries"].as_array().expect("entries").clone();
    assert_eq!(entries.len(), 2, "{data}");
    let report = entries
        .iter()
        .find(|entry| entry["display_name"] == "Quarterly Report.PDF")
        .expect("report entry");
    assert!(report["deleted_at_ms"].as_u64().expect("ms") > 0);
    assert_eq!(
        report["deleted_by"],
        serde_json::json!({ "kind": "service", "id": "loonfs-cli" })
    );

    // The human table prints the exact undelete invocation.
    let human = harness.run(&["trash"]);
    assert_success(&human);
    let table = stdout_string(&human);
    assert!(
        table.contains("DELETED\tDELETED_BY\tNAME\tINODE\tSEQ\tRECOVER"),
        "{table}"
    );
    assert!(table.contains("Quarterly Report.PDF"), "{table}");
    assert!(table.contains("loonfs undelete "), "{table}");

    // Recovering through the listed handle empties that entry out of trash.
    let inode = report["inode_id"].as_u64().expect("inode");
    let seq = report["deletion_seq"].as_u64().expect("seq");
    assert_success(&harness.run(&[
        "undelete",
        "/docs/Quarterly Report.PDF",
        "--inode",
        &inode.to_string(),
        "--deletion-seq",
        &seq.to_string(),
    ]));
    let after = harness.run(&["--json", "trash"]);
    assert_success(&after);
    assert_eq!(
        json_data(&after)["entries"]
            .as_array()
            .expect("entries")
            .len(),
        1
    );

    // Pagination pins the cursor contract: a one-entry page of a one-entry
    // trash carries no next cursor.
    let page = harness.run(&["--json", "trash", "--limit", "1"]);
    assert_success(&page);
    assert!(json_data(&page)["next_cursor"].is_null());
}

/// The printed recovery commands are meant to be pasted, so they name the
/// namespace they belong to and quote a path a shell would otherwise split.
#[test]
fn recovery_hints_name_their_namespace_and_quote_the_path() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    assert_success(&harness.run(&["namespace", "create", "demo"]));
    assert_success(&harness.run(&["use", "demo"]));

    let payload = harness.temp_dir.path().join("doc.txt");
    fs::write(&payload, b"body").expect("write payload");
    assert_success(&harness.run(&[
        "put",
        payload.to_str().expect("utf-8 path"),
        "/docs/Quarterly Report.PDF",
    ]));
    let inode = json_data(&harness.run(&["--json", "stat", "/docs/Quarterly Report.PDF"]))
        ["inode_id"]
        .as_u64()
        .expect("the stored inode id");

    // Nothing here is spelled on the command line, so the namespace is the
    // only ambient value the hint has to pin down.
    let removed = harness.run(&["rm", "/docs/Quarterly Report.PDF"]);
    assert_success(&removed);
    let entry = json_data(&harness.run(&["--json", "trash"]))["entries"][0].clone();
    let deletion_seq = entry["deletion_seq"].as_u64().expect("the deletion seq");
    assert_eq!(
        hinted_recovery_command(&removed),
        format!("loonfs undelete --inode {inode} --deletion-seq {deletion_seq} --namespace demo")
    );

    // The trash table offers the same command for the same deletion. Neither
    // names a destination: a recorded binding restores in place, under the
    // parent and name the delete recorded.
    let listed = harness.run(&["trash"]);
    assert_success(&listed);
    assert_eq!(
        trash_recovery_command(&listed, "Quarterly Report.PDF"),
        format!("loonfs undelete --inode {inode} --deletion-seq {deletion_seq} --namespace demo")
    );

    // Pasting the hint into a shell recovers the file, which is the whole
    // reason the path is quoted.
    let replayed = harness.replay_in_shell(&hinted_recovery_command(&removed));
    assert_success(&replayed);
    let cat = harness.run(&["cat", "/docs/Quarterly Report.PDF"]);
    assert_success(&cat);
    assert_eq!(cat.stdout, b"body");
}

/// A hint that leaves out a profile or a config file a bare invocation would
/// not find again sends the paste at some other filesystem, so both are
/// spelled whenever this run did not reach them the default way.
#[test]
fn recovery_hints_name_a_profile_and_config_a_bare_invocation_would_miss() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    harness.add_embedded_profile("staging");
    assert_success(&harness.run(&["namespace", "create", "demo"]));
    assert_success(&harness.run(&["use", "demo"]));
    assert_success(&harness.run(&["namespace", "create", "release", "--profile", "staging"]));

    let payload = harness.temp_dir.path().join("doc.txt");
    fs::write(&payload, b"body").expect("write payload");
    let put = |extra: &[&str]| {
        let mut args = vec!["put", payload.to_str().expect("utf-8 path"), "/notes.txt"];
        args.extend_from_slice(extra);
        assert_success(&harness.run(&args));
    };

    // A spelled --profile means the config's default is some other profile,
    // so the hint has to say which one it meant.
    put(&["--profile", "staging", "--namespace", "release"]);
    let removed = harness.run(&[
        "rm",
        "/notes.txt",
        "--profile",
        "staging",
        "--namespace",
        "release",
    ]);
    assert_success(&removed);
    let command = hinted_recovery_command(&removed);
    assert!(
        command.ends_with(" --namespace release --profile staging"),
        "{command}"
    );

    // The config flag names a file a later paste would not look at.
    let elsewhere = harness.temp_dir.path().join("elsewhere.toml");
    let elsewhere_arg = elsewhere.to_str().expect("utf-8 config path");
    fs::copy(&harness.config_path, &elsewhere).expect("copy the config aside");
    put(&[]);
    let removed = harness.run(&["--config", elsewhere_arg, "rm", "/notes.txt"]);
    assert_success(&removed);
    let command = hinted_recovery_command(&removed);
    assert!(
        command.ends_with(&format!(" --namespace demo --config {elsewhere_arg}")),
        "{command}"
    );

    // So does the environment variable: the pasting shell need not still
    // export it, so the hint carries the path instead of relying on it.
    put(&[]);
    let removed = harness.run_with_env(&[("LOONFS_CONFIG", elsewhere_arg)], &["rm", "/notes.txt"]);
    assert_success(&removed);
    let command = hinted_recovery_command(&removed);
    assert!(
        command.ends_with(&format!(" --namespace demo --config {elsewhere_arg}")),
        "{command}"
    );

    // The default locations need no flag: they are what a bare invocation
    // reads.
    put(&[]);
    let removed = harness.run(&["rm", "/notes.txt"]);
    assert_success(&removed);
    let command = hinted_recovery_command(&removed);
    assert!(command.ends_with(" --namespace demo"), "{command}");
    assert!(!command.contains("--config"), "{command}");
    assert!(!command.contains("--profile"), "{command}");
}

#[test]
fn human_output_shows_dates_and_event_names() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    assert_success(&harness.run(&["namespace", "create", "demo"]));
    assert_success(&harness.run(&["use", "demo"]));

    let payload = harness.temp_dir.path().join("doc.txt");
    fs::write(&payload, b"v1").expect("write payload");
    assert_success(&harness.run(&[
        "put",
        payload.to_str().expect("utf-8 path"),
        "/docs/Report.PDF",
    ]));
    assert_success(&harness.run(&["rm", "/docs/Report.PDF"]));

    let changes = harness.run(&["changes"]);
    assert_success(&changes);
    let feed = stdout_string(&changes);
    assert!(feed.contains("SEQ\tDATE\tEVENTS\tMESSAGE"), "{feed}");
    assert!(feed.contains("create 'Report.PDF'"), "{feed}");
    assert!(feed.contains("delete 'Report.PDF'"), "{feed}");
    // Dates render as UTC wall-clock, not raw milliseconds.
    assert!(feed.contains("Z\t"), "{feed}");

    fs::write(&payload, b"v2").expect("write payload");
    assert_success(&harness.run(&["put", payload.to_str().expect("utf-8 path"), "/doc.txt"]));
    let revisions = harness.run(&["revisions", "/doc.txt"]);
    assert_success(&revisions);
    let table = stdout_string(&revisions);
    assert!(
        table.contains("REVISION\tDATE\tACTOR\tSEQ\tSIZE\tDIGEST"),
        "{table}"
    );

    let stat_file = harness.run(&["stat", "/doc.txt"]);
    assert_success(&stat_file);
    assert!(
        stdout_string(&stat_file).contains("modified: "),
        "{}",
        stdout_string(&stat_file)
    );
    let json_stat = harness.run(&["--json", "stat", "/doc.txt"]);
    assert_success(&json_stat);
    let entry = json_data(&json_stat);
    assert_eq!(
        entry["created_by"],
        serde_json::json!({ "kind": "service", "id": "loonfs-cli" })
    );
    assert_eq!(
        entry["revision_actor"],
        serde_json::json!({ "kind": "service", "id": "loonfs-cli" })
    );
    assert!(entry["created_at_ms"].as_u64().expect("creation time") > 0);
}

#[test]
fn naming_strictness_and_directory_intent_hold_end_to_end() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    assert_success(&harness.run(&["namespace", "create", "demo"]));
    assert_success(&harness.run(&["use", "demo"]));

    let payload = harness.temp_dir.path().join("report.pdf");
    fs::write(&payload, b"body").expect("write payload");

    // A trailing slash means into-directory, never a file named like the
    // directory; other noncanonical spellings fail like the wire.
    assert_success(&harness.run(&["mkdir", "/docs"]));
    assert_success(&harness.run(&["put", payload.to_str().expect("utf-8 path"), "/docs/"]));
    assert_success(&harness.run(&["--json", "stat", "/docs/report.pdf"]));
    let double_slash = harness.run(&["put", payload.to_str().expect("utf-8 path"), "//x.txt"]);
    assert_failure(&double_slash);

    // Case-only rename works in place — including without --force — and a
    // no-op respelling stays a conflict.
    assert_success(&harness.run(&["--json", "mv", "/docs/report.pdf", "/docs/REPORT.PDF"]));
    let stat = harness.run(&["--json", "stat", "/docs/REPORT.PDF"]);
    assert_success(&stat);
    assert_eq!(json_data(&stat)["display_name"], "REPORT.PDF");
    let noop = harness.run(&["--json", "mv", "/docs/REPORT.PDF", "/docs/REPORT.PDF"]);
    assert_failure(&noop);
    assert_eq!(json_error(&noop)["code"], "path_conflict");

    // A normalization-equal collision names the stored spelling, so two
    // visually identical names stop looking like the same one.
    let collision = harness.run(&[
        "put",
        payload.to_str().expect("utf-8 path"),
        "/docs/report.pdf",
    ]);
    assert_failure(&collision);
    let message = stderr_string(&collision);
    assert!(message.contains("stored as `REPORT.PDF`"), "{message}");
    assert!(message.contains("case folding"), "{message}");

    // The portability floor rejects what no target filesystem can hold.
    for name in ["/docs/CON", "/docs/notes.", "/docs/draft "] {
        let rejected = harness.run(&["put", payload.to_str().expect("utf-8 path"), name]);
        assert_failure(&rejected);
    }
}

#[test]
fn recursive_transfers_roundtrip_a_tree() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    assert_success(&harness.run(&["namespace", "create", "demo"]));
    assert_success(&harness.run(&["use", "demo"]));

    // A local tree with nesting, an empty directory chain, and a root file.
    let tree = harness.temp_dir.path().join("tree");
    fs::create_dir_all(tree.join("docs/nested")).expect("create tree dirs");
    fs::create_dir_all(tree.join("empty/inner")).expect("create empty chain");
    fs::write(tree.join("top.txt"), b"top").expect("write top");
    fs::write(tree.join("docs/a.txt"), b"alpha").expect("write a");
    fs::write(tree.join("docs/nested/b.txt"), b"beta").expect("write b");

    // A plain put on a directory names the recursive flag.
    let plain = harness.run(&["--json", "put", tree.to_str().expect("utf-8 path"), "/up"]);
    assert_failure(&plain);
    assert!(json_error(&plain)["message"]
        .as_str()
        .expect("error message")
        .contains("put -r"),);

    let put = harness.run(&[
        "--json",
        "put",
        "-r",
        tree.to_str().expect("utf-8 path"),
        "/up",
        "--actor-kind",
        "user",
        "--actor-id",
        "tree-actor",
    ]);
    assert_success(&put);
    let put_data = json_data(&put);
    assert_eq!(put_data["kind"], "tree_transfer");
    assert_eq!(put_data["files"], 3);
    assert_eq!(put_data["directories"], 1);
    assert_eq!(put_data["failures"].as_array().expect("failures").len(), 0);
    let changes = harness.run(&["--json", "changes"]);
    assert_success(&changes);
    for change in json_data(&changes)["changes"]
        .as_array()
        .expect("recursive put changes")
    {
        assert_eq!(
            change["actor"],
            serde_json::json!({"kind":"user","id":"tree-actor"})
        );
    }
    for path in ["/up/top.txt", "/up/docs/nested/b.txt", "/up/empty/inner"] {
        assert_success(&harness.run(&["--json", "stat", path]));
    }

    // Rerunning without --force reports per-file conflicts and exits
    // nonzero while the summary stays structured; --force replaces cleanly.
    let rerun = harness.run(&[
        "--json",
        "put",
        "-r",
        tree.to_str().expect("utf-8 path"),
        "/up",
    ]);
    assert_failure(&rerun);
    let rerun_data = json_data(&rerun);
    assert_eq!(rerun_data["files"], 0);
    assert_eq!(
        rerun_data["failures"].as_array().expect("failures").len(),
        4
    );
    assert_eq!(
        rerun_data["failures"][0]["error"]["code"], "path_conflict",
        "{rerun_data}"
    );
    let forced = harness.run(&[
        "--json",
        "put",
        "-r",
        tree.to_str().expect("utf-8 path"),
        "/up",
        "--force",
    ]);
    assert_failure(&forced);
    let forced_data = json_data(&forced);
    assert_eq!(forced_data["files"], 3);
    assert_eq!(
        forced_data["failures"].as_array().expect("failures").len(),
        1,
        "the empty directory still conflicts: {forced_data}"
    );

    // Download the tree and compare bytes; empty directories materialize.
    let downloaded = harness.temp_dir.path().join("downloaded");
    let get = harness.run(&[
        "--json",
        "get",
        "-r",
        "/up",
        downloaded.to_str().expect("utf-8 path"),
    ]);
    assert_success(&get);
    let get_data = json_data(&get);
    assert_eq!(get_data["files"], 3);
    assert_eq!(
        fs::read(downloaded.join("docs/nested/b.txt")).expect("downloaded bytes"),
        b"beta"
    );
    assert!(downloaded.join("empty/inner").is_dir());

    // Server-side copy: the tree lands without moving bytes — the copied
    // file shares its source's content reference.
    let cp = harness.run(&["--json", "cp", "-r", "/up", "/copy"]);
    assert_success(&cp);
    let cp_data = json_data(&cp);
    assert_eq!(cp_data["files"], 3);
    assert_eq!(cp_data["directories"], 5);
    let source = harness.run(&["--json", "stat", "/up/docs/a.txt"]);
    let copy = harness.run(&["--json", "stat", "/copy/docs/a.txt"]);
    assert_success(&source);
    assert_success(&copy);
    assert_eq!(
        json_data(&source)["content_ref"],
        json_data(&copy)["content_ref"]
    );
    assert_success(&harness.run(&["--json", "stat", "/copy/empty/inner"]));

    // mv still moves a directory in one commit, no flag involved.
    assert_success(&harness.run(&["--json", "mv", "/copy", "/moved"]));
    assert_success(&harness.run(&["--json", "stat", "/moved/docs/a.txt"]));
    let mv_recursive = harness.run(&["--json", "mv", "-r", "/moved", "/again"]);
    assert_failure(&mv_recursive);
    assert_eq!(json_error(&mv_recursive)["code"], "invalid_input");
}

/// A recursive get creates the destination it was handed, parents included,
/// the way `cp -r` creates its target. The shape that used to fail on every
/// file is a remote directory holding nothing but files: no subdirectory
/// existed to create the root as a side effect of its own `mkdir`.
#[test]
fn recursive_get_creates_an_absent_destination_root_in_both_modes() {
    let harness = Harness::new();
    harness.add_embedded_profile("embedded");
    let remote_server =
        harness.start_external_server(harness.write_server_config("remote", "get-destination"));
    let add_remote = harness.run(&[
        "--json",
        "profile",
        "create",
        "remote",
        "--mode",
        "remote",
        "--server-url",
        &remote_server.server_url,
        "--auth-token",
        "test-token",
    ]);
    assert_success(&add_remote);

    let flat = harness.temp_dir.path().join("flat");
    fs::create_dir_all(&flat).expect("create flat source");
    for name in ["a.txt", "b.txt"] {
        fs::write(flat.join(name), name.as_bytes()).expect("write source file");
    }

    for profile in ["embedded", "remote"] {
        assert_success(&harness.run(&["namespace", "create", "--profile", profile, "demo"]));
        assert_success(&harness.run(&["use", "--profile", profile, "demo"]));
        assert_success(&harness.run(&[
            "put",
            "-r",
            "--profile",
            profile,
            flat.to_str().expect("utf-8 path"),
            "/flat",
        ]));
        // A directory holding nothing, inside another holding nothing.
        assert_success(&harness.run(&["mkdir", "-p", "--profile", profile, "/nested/empty/inner"]));

        // Files with no subdirectory among them: nothing but the transfer
        // itself can bring the destination root into being. Neither the root
        // nor its parent exists here.
        let destination = harness.temp_dir.path().join(profile).join("dest");
        let get = harness.run(&[
            "--json",
            "get",
            "-r",
            "--profile",
            profile,
            "/flat",
            destination.to_str().expect("utf-8 path"),
        ]);
        assert_success(&get);
        let data = json_data(&get);
        assert_eq!(data["files"], 2, "{data}");
        // The destination root, counted the way `cp -r` counts its own.
        assert_eq!(data["directories"], 1, "{data}");
        assert_eq!(data["failures"].as_array().expect("failures").len(), 0);
        assert_eq!(
            fs::read(destination.join("a.txt")).expect("downloaded a.txt"),
            b"a.txt"
        );

        // Empty directories, nested, land as local directories.
        let nested = harness.temp_dir.path().join(profile).join("nested");
        let nested_get = harness.run(&[
            "--json",
            "get",
            "-r",
            "--profile",
            profile,
            "/nested",
            nested.to_str().expect("utf-8 path"),
        ]);
        assert_success(&nested_get);
        let nested_data = json_data(&nested_get);
        assert_eq!(nested_data["files"], 0, "{nested_data}");
        assert_eq!(nested_data["directories"], 3, "{nested_data}");
        assert!(nested.join("empty/inner").is_dir());

        // A tree with nothing in it at all still leaves the caller with the
        // directory they named.
        let empty_destination = harness.temp_dir.path().join(profile).join("only-empty");
        let empty = harness.run(&[
            "--json",
            "get",
            "-r",
            "--profile",
            profile,
            "/nested/empty/inner",
            empty_destination.to_str().expect("utf-8 path"),
        ]);
        assert_success(&empty);
        let empty_data = json_data(&empty);
        assert_eq!(empty_data["files"], 0, "{empty_data}");
        assert_eq!(empty_data["directories"], 1, "{empty_data}");
        assert!(empty_destination.is_dir());
    }
}

/// One unwritable destination path fails alone. A local file sitting where a
/// directory has to go takes down that directory and the files under it —
/// each named on its own — while every sibling still lands.
#[test]
fn recursive_get_names_only_the_paths_it_could_not_write_in_both_modes() {
    let harness = Harness::new();
    harness.add_embedded_profile("embedded");
    let remote_server =
        harness.start_external_server(harness.write_server_config("remote", "get-partial"));
    let add_remote = harness.run(&[
        "--json",
        "profile",
        "create",
        "remote",
        "--mode",
        "remote",
        "--server-url",
        &remote_server.server_url,
        "--auth-token",
        "test-token",
    ]);
    assert_success(&add_remote);

    let tree = harness.temp_dir.path().join("tree");
    fs::create_dir_all(tree.join("docs")).expect("create docs");
    fs::create_dir_all(tree.join("other")).expect("create other");
    fs::write(tree.join("top.txt"), b"top").expect("write top");
    fs::write(tree.join("docs/a.txt"), b"alpha").expect("write a");
    fs::write(tree.join("docs/b.txt"), b"beta").expect("write b");
    fs::write(tree.join("other/c.txt"), b"gamma").expect("write c");

    for profile in ["embedded", "remote"] {
        assert_success(&harness.run(&["namespace", "create", "--profile", profile, "demo"]));
        assert_success(&harness.run(&["use", "--profile", profile, "demo"]));
        assert_success(&harness.run(&[
            "put",
            "-r",
            "--profile",
            profile,
            tree.to_str().expect("utf-8 path"),
            "/src",
        ]));

        // A plain file occupies the place `docs` has to be.
        let destination = harness.temp_dir.path().join(profile);
        fs::create_dir_all(&destination).expect("create destination");
        fs::write(destination.join("docs"), b"in the way").expect("block docs");

        let get = harness.run(&[
            "--json",
            "get",
            "-r",
            "--profile",
            profile,
            "/src",
            destination.to_str().expect("utf-8 path"),
        ]);
        assert_failure(&get);
        let data = json_data(&get);
        assert_eq!(data["files"], 2, "{data}");
        // The destination root and `other`; `docs` is the one that failed.
        assert_eq!(data["directories"], 2, "{data}");

        // The blocked directory is named by the local path that failed, the
        // files under it by the remote paths that could not be written.
        let failed: Vec<&str> = data["failures"]
            .as_array()
            .expect("failures")
            .iter()
            .map(|failure| failure["path"].as_str().expect("failure path"))
            .collect();
        assert_eq!(failed.len(), 3, "{data}");
        assert!(
            failed.contains(&destination.join("docs").to_str().expect("utf-8 path")),
            "{data}"
        );
        assert!(failed.contains(&"/src/docs/a.txt"), "{data}");
        assert!(failed.contains(&"/src/docs/b.txt"), "{data}");
        for failure in data["failures"].as_array().expect("failures") {
            assert_eq!(failure["error"]["code"], "io_error", "{data}");
        }

        // Everything outside the blocked subtree downloaded.
        assert_eq!(
            fs::read(destination.join("top.txt")).expect("downloaded top.txt"),
            b"top"
        );
        assert_eq!(
            fs::read(destination.join("other/c.txt")).expect("downloaded c.txt"),
            b"gamma"
        );
    }
}

/// One file into a directory that does not exist fails, exactly as `cp` into
/// a missing parent fails: only `-r`, which owns the destination tree it is
/// asked to build, creates directories.
#[test]
fn single_file_get_refuses_a_missing_parent_directory() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    assert_success(&harness.run(&["namespace", "create", "demo"]));
    assert_success(&harness.run(&["use", "demo"]));

    let payload = harness.temp_dir.path().join("payload.txt");
    fs::write(&payload, b"body").expect("write payload");
    assert_success(&harness.run(&["put", payload.to_str().expect("utf-8 path"), "/docs/a.txt"]));

    let missing = harness.temp_dir.path().join("no-such-dir");
    let get = harness.run(&[
        "--json",
        "get",
        "/docs/a.txt",
        missing.join("a.txt").to_str().expect("utf-8 path"),
    ]);
    assert_failure(&get);
    let error = json_error(&get);
    assert_eq!(error["code"], "io_error");
    let message = error["message"].as_str().expect("error message");
    assert!(
        message.contains(missing.to_str().expect("utf-8 path")),
        "the message names the directory to create, got: {message}"
    );
    assert!(
        !message.contains(".loonfs-partial"),
        "the message keeps the CLI's own temporary file out of it, got: {message}"
    );
    assert!(!missing.exists(), "a failed get created no directory");
}

#[test]
fn rm_recursive_deletes_a_populated_directory_in_one_commit() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    assert_success(&harness.run(&["namespace", "create", "demo"]));
    assert_success(&harness.run(&["use", "demo"]));

    let payload = harness.temp_dir.path().join("payload.txt");
    fs::write(&payload, b"body").expect("write payload");
    for path in ["/docs/a.txt", "/docs/nested/b.txt"] {
        assert_success(&harness.run(&["put", payload.to_str().expect("utf-8 path"), path]));
    }

    // Without -r a populated directory still refuses, exactly as before.
    let refused = harness.run(&["--json", "rm", "/docs"]);
    assert_failure(&refused);
    assert_eq!(json_error(&refused)["code"], "directory_not_empty");

    let removed = harness.run(&["--json", "rm", "-r", "/docs"]);
    assert_success(&removed);
    assert_eq!(json_data(&removed)["target"], "demo:/docs");

    for path in ["/docs", "/docs/a.txt", "/docs/nested/b.txt"] {
        let stat = harness.run(&["--json", "stat", path]);
        assert_failure(&stat);
        assert_eq!(json_error(&stat)["code"], "path_not_found");
    }
}

#[test]
fn embedded_profile_namespace_fork_reads_shared_content_and_diverges() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");

    let upload_path = harness.temp_dir.path().join("upload.txt");
    let clone_upload_path = harness.temp_dir.path().join("clone-upload.txt");
    fs::write(&upload_path, b"base from cli\n").expect("upload payload");
    fs::write(&clone_upload_path, b"clone from cli\n").expect("clone upload payload");

    assert_success(&harness.run(&["namespace", "create", "demo"]));
    assert_success(&harness.run(&["use", "demo"]));
    assert_success(&harness.run(&[
        "put",
        upload_path.to_str().expect("utf-8 path"),
        "/docs/shared.txt",
    ]));

    let fork = harness.run(&["--json", "namespace", "fork", "demo", "clone"]);
    assert_success(&fork);
    assert_eq!(json_data(&fork)["namespace_id"], "clone");

    let source = harness.run(&["--json", "stat", "/docs/shared.txt"]);
    let clone = harness.run(&["--json", "stat", "--namespace", "clone", "/docs/shared.txt"]);
    assert_success(&source);
    assert_success(&clone);
    assert_eq!(
        json_data(&source)["content_ref"],
        json_data(&clone)["content_ref"]
    );

    assert_success(&harness.run(&[
        "put",
        "--namespace",
        "clone",
        clone_upload_path.to_str().expect("utf-8 path"),
        "/docs/shared.txt",
        "--force",
    ]));

    let source_cat = harness.run(&["cat", "/docs/shared.txt"]);
    assert_success(&source_cat);
    assert_eq!(source_cat.stdout, b"base from cli\n");

    let clone_cat = harness.run(&["cat", "--namespace", "clone", "/docs/shared.txt"]);
    assert_success(&clone_cat);
    assert_eq!(clone_cat.stdout, b"clone from cli\n");
}

#[test]
fn init_creates_embedded_profile_and_current_reports_namespace_unset() {
    let harness = Harness::new();

    let init = harness.run(&[
        "--json",
        "init",
        "mystore",
        "--mode",
        "embedded",
        "--store-kind",
        "local-fs",
        "--root",
        harness.store_root("mystore").to_str().expect("utf-8 path"),
    ]);
    assert_success(&init);
    assert_eq!(json_data(&init)["mode"], "embedded");

    let show = harness.run(&["--json", "profile", "show"]);
    assert_success(&show);
    assert_eq!(json_data(&show)["mode"], "embedded");

    let current = harness.run(&["--json", "current"]);
    assert_success(&current);
    assert_eq!(json_data(&current)["profile"], "mystore");
    assert!(json_data(&current)["namespace"].is_null());

    assert_success(&harness.run(&["namespace", "create", "demo"]));
    let use_namespace = harness.run(&["--json", "use", "demo"]);
    assert_success(&use_namespace);
    assert_eq!(json_data(&use_namespace)["namespace"], "demo");
}

#[test]
fn invalid_profile_mode_is_rejected() {
    let harness = Harness::new();

    let result = harness.run(&[
        "--json",
        "profile",
        "create",
        "default",
        "--mode",
        "not-a-mode",
        "--store-kind",
        "local-fs",
        "--root",
        harness.store_root("default").to_str().expect("utf-8 path"),
    ]);

    assert_failure(&result);
    let error = json_error(&result);
    assert_eq!(error["code"], "invalid_input");
    let message = error["message"].as_str().expect("json string");
    assert!(message.contains("expected embedded or remote"));
}

#[test]
fn removing_last_profile_leaves_empty_config() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");

    let remove = harness.run(&["--json", "--no-input", "profile", "delete", "default"]);
    assert_success(&remove);

    let list = harness.run(&["--json", "profile", "list"]);
    assert_success(&list);
    let data = json_data(&list);
    assert!(data["default_profile"].is_null());
    assert_eq!(data["profiles"].as_array().expect("json array").len(), 0);

    let show_config = harness.run(&["config", "show"]);
    assert_success(&show_config);
    assert!(!stdout_string(&show_config).contains("default_profile"));

    let show = harness.run(&["--json", "profile", "show"]);
    assert_failure(&show);
    assert_eq!(json_error(&show)["code"], "no_default_profile");
}

#[test]
fn removing_default_profile_requires_explicit_reselection() {
    let harness = Harness::new();
    harness.add_embedded_profile("alpha");
    harness.add_embedded_profile("beta");

    let remove = harness.run(&["--json", "--no-input", "profile", "delete", "alpha"]);
    assert_success(&remove);

    let list = harness.run(&["--json", "profile", "list"]);
    assert_success(&list);
    let data = json_data(&list);
    assert!(data["default_profile"].is_null());
    assert_eq!(data["profiles"].as_array().expect("json array").len(), 1);

    let current = harness.run(&["--json", "current"]);
    assert_failure(&current);
    assert_eq!(json_error(&current)["code"], "no_default_profile");

    let namespace = harness.run(&["--json", "namespace", "create", "new-ns"]);
    assert_failure(&namespace);
    assert_eq!(json_error(&namespace)["code"], "no_default_profile");

    let filesystem = harness.run(&["--json", "ls", "/"]);
    assert_failure(&filesystem);
    assert_eq!(json_error(&filesystem)["code"], "no_default_profile");

    let use_profile = harness.run(&["--json", "profile", "use", "beta"]);
    assert_success(&use_profile);

    let show_after = harness.run(&["--json", "profile", "show"]);
    assert_success(&show_after);
    assert_eq!(json_data(&show_after)["mode"], "embedded");
}

#[test]
fn profile_update_changes_fields() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");

    let new_root = harness.store_root("updated");
    let update = harness.run(&[
        "--json",
        "profile",
        "update",
        "default",
        "--root",
        new_root.to_str().expect("utf-8 path"),
    ]);
    assert_success(&update);

    let show = harness.run(&["--json", "profile", "show", "default"]);
    assert_success(&show);
    let store = &json_data(&show)["store"];
    assert_eq!(store["root"], new_root.to_str().expect("utf-8 path"));
}

#[test]
fn profile_update_with_only_service_account_key_path_applies() {
    let harness = Harness::new();
    let create = harness.run(&[
        "--json",
        "profile",
        "create",
        "gcp",
        "--mode",
        "embedded",
        "--store-kind",
        "gcp-gcs",
        "--bucket",
        "documents",
        "--service-account-key-path",
        "/old/service-account.json",
    ]);
    assert_success(&create);

    let update = harness.run(&[
        "--json",
        "--no-input",
        "profile",
        "update",
        "gcp",
        "--service-account-key-path",
        "/new/service-account.json",
    ]);
    assert_success(&update);

    let show = harness.run(&["--json", "profile", "show", "gcp"]);
    assert_success(&show);
    assert_eq!(
        json_data(&show)["store"]["service_account_key_path"],
        "/new/service-account.json"
    );
}

#[test]
fn profile_use_switches_default() {
    let harness = Harness::new();
    harness.add_embedded_profile("alpha");
    harness.add_embedded_profile("beta");

    let use_profile = harness.run(&["--json", "profile", "use", "beta"]);
    assert_success(&use_profile);
    assert_eq!(json_data(&use_profile)["name"], "beta");

    let current = harness.run(&["--json", "current"]);
    assert_success(&current);
    assert_eq!(json_data(&current)["profile"], "beta");
}

#[test]
fn profile_use_rejects_missing_profile() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");

    let result = harness.run(&["--json", "profile", "use", "nonexistent"]);
    assert_failure(&result);
    assert_eq!(json_error(&result)["code"], "profile_not_found");
}

#[test]
fn profile_names_matching_top_level_config_keys_are_allowed() {
    // Profiles nest under [profiles.<name>], so names that once collided
    // with top-level settings need no reservation.
    let harness = Harness::new();

    let init = harness.run(&[
        "--json",
        "init",
        "default_profile",
        "--mode",
        "embedded",
        "--store-kind",
        "local-fs",
        "--root",
        harness
            .store_root("default_profile")
            .to_str()
            .expect("utf-8 path"),
    ]);
    assert_success(&init);

    let create = harness.run(&[
        "--json",
        "profile",
        "create",
        "config_version",
        "--mode",
        "embedded",
        "--store-kind",
        "local-fs",
        "--root",
        harness
            .store_root("config_version")
            .to_str()
            .expect("utf-8 path"),
    ]);
    assert_success(&create);

    let list = harness.run(&["--json", "profile", "list"]);
    assert_success(&list);
    let names: Vec<_> = json_data(&list)["profiles"]
        .as_array()
        .expect("profiles array")
        .iter()
        .map(|profile| profile["name"].as_str().expect("profile name").to_owned())
        .collect();
    assert!(names.contains(&"default_profile".to_owned()));
    assert!(names.contains(&"config_version".to_owned()));
}

#[test]
fn ambient_provider_credentials_do_not_look_like_flags() {
    // An AWS key exported for something else is not `--access-key-id`
    // passed to this command, so a provider that cannot use it ignores it.
    let harness = Harness::new();
    let ambient = &[
        ("AWS_ACCESS_KEY_ID", "ambient-access"),
        ("AWS_SECRET_ACCESS_KEY", "ambient-secret"),
        ("AWS_SESSION_TOKEN", "ambient-session"),
        ("LOONFS_AUTH_TOKEN", "ambient-token"),
    ];

    let gcs = harness.run_with_env(
        ambient,
        &[
            "--json",
            "profile",
            "create",
            "gcs",
            "--mode",
            "embedded",
            "--store-kind",
            "gcp-gcs",
            "--bucket",
            "bucket",
            "--service-account-key-path",
            "/tmp/service-account.json",
        ],
    );
    assert_success(&gcs);
    assert_eq!(json_data(&gcs)["store"]["kind"], "gcp-gcs");

    let local_fs = harness.run_with_env(
        ambient,
        &[
            "--json",
            "profile",
            "create",
            "local",
            "--mode",
            "embedded",
            "--store-kind",
            "local-fs",
            "--root",
            harness.store_root("local").to_str().expect("utf-8 path"),
        ],
    );
    assert_success(&local_fs);

    // The same environment still fills the providers that use it.
    let s3 = harness.run_with_env(
        ambient,
        &[
            "--json",
            "profile",
            "create",
            "s3",
            "--mode",
            "embedded",
            "--store-kind",
            "aws-s3",
            "--bucket",
            "bucket",
            "--region",
            "us-east-1",
        ],
    );
    assert_success(&s3);
    assert_eq!(json_data(&s3)["store"]["access_key_id"], "<redacted>");

    // And a typed flag that does not apply is still rejected.
    let typed = harness.run_with_env(
        ambient,
        &[
            "--json",
            "profile",
            "create",
            "gcs-typed",
            "--mode",
            "embedded",
            "--store-kind",
            "gcp-gcs",
            "--bucket",
            "bucket",
            "--service-account-key-path",
            "/tmp/service-account.json",
            "--access-key-id",
            "typed-access",
        ],
    );
    assert_failure(&typed);
    let error = json_error(&typed);
    assert_eq!(error["code"], "invalid_input");
    assert!(error["message"]
        .as_str()
        .expect("json string")
        .contains("`--access-key-id` does not apply"));
}

/// A command line the parser rejected is a failure like any other under
/// `--json`, and keeps clap's own exit status so a script can still tell a
/// command that never ran from one that ran and failed.
#[test]
fn json_covers_command_lines_the_parser_rejects() {
    let harness = Harness::new();

    for arguments in [
        vec!["--json", "bogus-command"],
        vec!["--json", "mkdir"],
        vec![
            "--json",
            "admin",
            "run",
            "--namespace",
            "demo",
            "--drain",
            "--max-steps",
            "abc",
        ],
        // The flag is global, so it still counts after the subcommand.
        vec!["stat", "--json", "--nonexistent-flag"],
    ] {
        let output = harness.run(&arguments);
        assert_failure(&output);
        assert_eq!(
            output.status.code(),
            Some(2),
            "a parse failure keeps clap's usage status for {arguments:?}"
        );
        assert!(
            output.stdout.is_empty(),
            "the failure belongs on stderr for {arguments:?}"
        );
        let envelope = parse_json(&output.stderr);
        assert_eq!(envelope["kind"], "parse_error");
        assert_eq!(envelope["format_version"], 1);
        assert!(envelope["data"].is_null());
        assert_eq!(envelope["error"]["code"], "invalid_usage");
        assert!(!envelope["error"]["message"]
            .as_str()
            .expect("json string")
            .is_empty());
    }

    // Without --json the plain-text rendering and the status are unchanged.
    let plain = harness.run(&["bogus-command"]);
    assert_eq!(plain.status.code(), Some(2));
    assert!(stderr_string(&plain).contains("unrecognized subcommand"));
    assert!(!stderr_string(&plain).starts_with('{'));

    // Help and version are not failures, whatever else is on the line.
    let help = harness.run(&["--json", "--help"]);
    assert_success(&help);
    assert!(stdout_string(&help).contains("Usage:"));
    let version = harness.run(&["--json", "--version"]);
    assert_success(&version);
}

#[test]
fn init_rejects_existing_config_file() {
    let harness = Harness::new();
    harness.write_cli_config(format!(
        r#"
config_version = 1
default_profile = "default"

[profiles.default]
mode = "embedded"

[profiles.default.store]
kind = "local-fs"
root = "{}"
"#,
        harness.store_root("default").display()
    ));
    let existing = fs::read_to_string(&harness.config_path).expect("read existing config");

    let init = harness.run(&[
        "--json",
        "init",
        "mystore",
        "--mode",
        "embedded",
        "--store-kind",
        "local-fs",
        "--root",
        harness.store_root("mystore").to_str().expect("utf-8 path"),
    ]);
    assert_failure(&init);
    let error = json_error(&init);
    assert_eq!(error["code"], "config_already_exists");
    let message = error["message"].as_str().expect("json string");
    assert!(message.contains("loonfs profile create"));
    assert!(message.contains("loonfs profile update"));
    assert!(message.contains("loonfs profile use"));
    assert_eq!(
        fs::read_to_string(&harness.config_path).expect("read unchanged config"),
        existing
    );
}

#[test]
fn profiles_nest_under_their_own_table() {
    let harness = Harness::new();
    harness.write_cli_config(format!(
        r#"
config_version = 1

[profiles.default_profile]
mode = "embedded"

[profiles.default_profile.store]
kind = "local-fs"
root = "{}"
"#,
        harness.store_root("default_profile").display()
    ));

    let list = harness.run(&["--json", "profile", "list"]);
    assert_success(&list);
    assert_eq!(json_data(&list)["profiles"][0]["name"], "default_profile");
}

#[test]
fn empty_default_profile_in_config_is_rejected() {
    let harness = Harness::new();
    harness.write_cli_config(
        r#"
config_version = 1
default_profile = ""
"#,
    );

    let list = harness.run(&["--json", "profile", "list"]);
    assert_failure(&list);
    let error = json_error(&list);
    assert_eq!(error["code"], "invalid_config");
    assert!(error["message"]
        .as_str()
        .expect("json string")
        .contains("default_profile"));
}

#[test]
fn whitespace_default_profile_in_config_is_rejected() {
    let harness = Harness::new();
    harness.write_cli_config(
        r#"
config_version = 1
default_profile = "   "
"#,
    );

    let list = harness.run(&["--json", "profile", "list"]);
    assert_failure(&list);
    let error = json_error(&list);
    assert_eq!(error["code"], "invalid_config");
    assert!(error["message"]
        .as_str()
        .expect("json string")
        .contains("default_profile"));
}

#[test]
fn invalid_store_field_messages_use_flattened_paths() {
    let harness = Harness::new();
    harness.write_cli_config(
        r#"
config_version = 1
default_profile = "default"

[profiles.default]
mode = "embedded"

[profiles.default.store]
kind = "local-fs"
root = ""
"#,
    );

    let list = harness.run(&["--json", "profile", "list"]);
    assert_failure(&list);
    let error = json_error(&list);
    assert_eq!(error["code"], "invalid_config");
    assert!(error["message"]
        .as_str()
        .expect("json string")
        .contains("default.store.root"));
}

#[test]
fn invalid_default_namespace_in_config_is_rejected() {
    let harness = Harness::new();
    harness.write_cli_config(format!(
        r#"
config_version = 1
default_profile = "default"

[profiles.default]
mode = "embedded"
default_namespace = "bad/name"

[profiles.default.store]
kind = "local-fs"
root = "{}"
"#,
        harness.store_root("default").display()
    ));

    let current = harness.run(&["--json", "current"]);
    assert_failure(&current);
    let error = json_error(&current);
    assert_eq!(error["code"], "invalid_config");
    assert!(error["message"]
        .as_str()
        .expect("json string")
        .contains("default.default_namespace"));
}

#[test]
fn embedded_namespace_commands_reject_invalid_namespace_ids() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");

    let create = harness.run(&["--json", "namespace", "create", "bad/name"]);
    assert_failure(&create);
    assert_eq!(json_error(&create)["code"], "invalid_request");
    assert!(json_error(&create)["message"]
        .as_str()
        .expect("json string")
        .contains("invalid namespace_id"));

    assert_success(&harness.run(&["namespace", "create", "demo"]));
    let fork = harness.run(&["--json", "namespace", "fork", "demo", "bad/name"]);
    assert_failure(&fork);
    assert_eq!(json_error(&fork)["code"], "invalid_request");

    let use_namespace = harness.run(&["--json", "use", "bad/name"]);
    assert_failure(&use_namespace);
    assert_eq!(json_error(&use_namespace)["code"], "invalid_request");
}

#[test]
fn remote_namespace_commands_reject_invalid_namespace_ids_before_http() {
    let harness = Harness::new();
    let add_remote = harness.run(&[
        "--json",
        "profile",
        "create",
        "default",
        "--mode",
        "remote",
        "--server-url",
        "http://127.0.0.1:9",
    ]);
    assert_success(&add_remote);

    let create = harness.run(&["--json", "namespace", "create", "bad/name"]);
    assert_failure(&create);
    assert_eq!(json_error(&create)["code"], "invalid_request");

    let fork = harness.run(&["--json", "namespace", "fork", "demo", "bad/name"]);
    assert_failure(&fork);
    assert_eq!(json_error(&fork)["code"], "invalid_request");

    let use_namespace = harness.run(&["--json", "use", "bad/name"]);
    assert_failure(&use_namespace);
    assert_eq!(json_error(&use_namespace)["code"], "invalid_request");
}

#[test]
fn invalid_remote_urls_are_rejected() {
    let harness = Harness::new();

    let missing_host_http = harness.run(&[
        "--json",
        "profile",
        "create",
        "default",
        "--mode",
        "remote",
        "--server-url",
        "http://",
    ]);
    assert_failure(&missing_host_http);
    assert_eq!(json_error(&missing_host_http)["code"], "invalid_config");
    assert!(json_error(&missing_host_http)["message"]
        .as_str()
        .expect("json string")
        .contains("default.server_url"));

    let missing_host_https = harness.run(&[
        "--json",
        "profile",
        "create",
        "default",
        "--mode",
        "remote",
        "--server-url",
        "https://",
    ]);
    assert_failure(&missing_host_https);
    assert_eq!(json_error(&missing_host_https)["code"], "invalid_config");
}

#[test]
fn external_remote_profile_executes_through_http() {
    let harness = Harness::new();
    let remote_server =
        harness.start_external_server(harness.write_server_config("remote", "remote-exec"));
    let add_remote = harness.run(&[
        "--json",
        "profile",
        "create",
        "default",
        "--mode",
        "remote",
        "--server-url",
        &remote_server.server_url,
        "--auth-token",
        "test-token",
    ]);
    assert_success(&add_remote);

    let create = harness.run(&["--json", "namespace", "create", "demo"]);
    assert_success(&create);
    let fork = harness.run(&["--json", "namespace", "fork", "demo", "clone"]);
    assert_success(&fork);
    assert_eq!(json_data(&fork)["namespace_id"], "clone");

    let use_namespace = harness.run(&["--json", "use", "demo"]);
    assert_success(&use_namespace);

    let use_clone = harness.run(&["--json", "use", "clone"]);
    assert_success(&use_clone);
    assert_eq!(json_data(&use_clone)["namespace"], "clone");
}

/// Embedded and remote profiles must report the same `code` for the same
/// failure: registry codes pass through verbatim in both modes instead of
/// being rewritten to CLI-local codes on one side.
#[test]
fn embedded_and_remote_profiles_emit_the_same_error_codes() {
    let harness = Harness::new();
    harness.add_embedded_profile("embedded");
    let remote_server =
        harness.start_external_server(harness.write_server_config("remote", "error-parity"));
    let add_remote = harness.run(&[
        "--json",
        "profile",
        "create",
        "remote",
        "--mode",
        "remote",
        "--server-url",
        &remote_server.server_url,
        "--auth-token",
        "test-token",
    ]);
    assert_success(&add_remote);

    for profile in ["embedded", "remote"] {
        assert_success(&harness.run(&["namespace", "create", "--profile", profile, "demo"]));
    }

    // Creating a namespace that already exists.
    let embedded = harness.run(&[
        "--json",
        "namespace",
        "create",
        "--profile",
        "embedded",
        "demo",
    ]);
    let remote = harness.run(&[
        "--json",
        "namespace",
        "create",
        "--profile",
        "remote",
        "demo",
    ]);
    assert_failure(&embedded);
    assert_failure(&remote);
    assert_eq!(json_error(&embedded)["code"], "namespace_exists");
    assert_eq!(json_error(&embedded)["code"], json_error(&remote)["code"]);

    // A malformed namespace id.
    let embedded = harness.run(&[
        "--json",
        "namespace",
        "create",
        "--profile",
        "embedded",
        "bad/name",
    ]);
    let remote = harness.run(&[
        "--json",
        "namespace",
        "create",
        "--profile",
        "remote",
        "bad/name",
    ]);
    assert_failure(&embedded);
    assert_failure(&remote);
    assert_eq!(json_error(&embedded)["code"], "invalid_request");
    assert_eq!(json_error(&embedded)["code"], json_error(&remote)["code"]);
}

#[test]
fn stat_inode_has_embedded_and_remote_parity_and_tracks_renames() {
    let harness = Harness::new();
    harness.add_embedded_profile("embedded");
    let remote_server =
        harness.start_external_server(harness.write_server_config("remote", "stat-inode-parity"));
    assert_success(&harness.run(&[
        "--json",
        "profile",
        "create",
        "remote",
        "--mode",
        "remote",
        "--server-url",
        &remote_server.server_url,
        "--auth-token",
        "test-token",
    ]));
    let payload = harness.temp_dir.path().join("inode-stat.txt");
    fs::write(&payload, b"inode stat").expect("write payload");

    for profile in ["embedded", "remote"] {
        assert_success(&harness.run(&["namespace", "create", "--profile", profile, "demo"]));
        assert_success(&harness.run(&[
            "put",
            "--profile",
            profile,
            "--namespace",
            "demo",
            payload.to_str().expect("utf-8 path"),
            "/before.txt",
        ]));
        let by_path = harness.run(&[
            "--json",
            "stat",
            "--profile",
            profile,
            "--namespace",
            "demo",
            "/before.txt",
        ]);
        assert_success(&by_path);
        let inode_id = json_data(&by_path)["inode_id"]
            .as_u64()
            .expect("stat reports inode id");
        let by_inode = harness.run(&[
            "--json",
            "stat",
            "--profile",
            profile,
            "--namespace",
            "demo",
            "--inode",
            &inode_id.to_string(),
        ]);
        assert_success(&by_inode);
        assert_eq!(json_data(&by_inode), json_data(&by_path));

        assert_success(&harness.run(&[
            "mv",
            "--profile",
            profile,
            "--namespace",
            "demo",
            "/before.txt",
            "/after.txt",
        ]));
        let renamed = harness.run(&[
            "--json",
            "stat",
            "--profile",
            profile,
            "--namespace",
            "demo",
            "--inode",
            &inode_id.to_string(),
        ]);
        assert_success(&renamed);
        assert_eq!(json_data(&renamed)["inode_id"], inode_id);
        assert_eq!(json_data(&renamed)["path"], "/after.txt");

        let missing = harness.run(&[
            "--json",
            "stat",
            "--profile",
            profile,
            "--namespace",
            "demo",
            "--inode",
            &u64::MAX.to_string(),
        ]);
        assert_failure(&missing);
        assert_eq!(json_error(&missing)["code"], "inode_not_found");
    }

    let neither = harness.run(&["stat", "--profile", "embedded"]);
    assert_failure(&neither);
    let both = harness.run(&[
        "stat",
        "--profile",
        "embedded",
        "/after.txt",
        "--inode",
        "2",
    ]);
    assert_failure(&both);
}

#[test]
fn filesystem_requires_default_namespace_when_omitted() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");

    let output = harness.run(&["--json", "ls", "/"]);
    assert_failure(&output);
    let error = json_error(&output);
    assert_eq!(error["code"], "no_default_namespace");
}

#[test]
fn namespace_resolution_uses_environment_before_profile_default() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    assert_success(&harness.run(&["namespace", "create", "profile-default"]));
    assert_success(&harness.run(&["namespace", "create", "from-environment"]));
    assert_success(&harness.run(&["use", "profile-default"]));

    let output = harness.run_with_env(
        &[("LOONFS_NAMESPACE", "from-environment")],
        &["--json", "changes"],
    );

    assert_success(&output);
    assert_eq!(json_data(&output)["namespace_id"], "from-environment");
}

#[test]
fn namespace_resolution_uses_flag_before_environment() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    assert_success(&harness.run(&["namespace", "create", "from-environment"]));
    assert_success(&harness.run(&["namespace", "create", "from-flag"]));

    let output = harness.run_with_env(
        &[("LOONFS_NAMESPACE", "from-environment")],
        &["--json", "changes", "--namespace", "from-flag"],
    );

    assert_success(&output);
    assert_eq!(json_data(&output)["namespace_id"], "from-flag");
}

#[test]
fn namespace_resolution_gives_invalid_environment_the_flag_error_code() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");

    let from_environment =
        harness.run_with_env(&[("LOONFS_NAMESPACE", "bad/name")], &["--json", "changes"]);
    let from_flag = harness.run(&["--json", "changes", "--namespace", "bad/name"]);

    assert_failure(&from_environment);
    assert_failure(&from_flag);
    assert_eq!(
        json_error(&from_environment)["code"],
        json_error(&from_flag)["code"]
    );
}

#[test]
fn namespace_resolution_ignores_empty_environment() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    assert_success(&harness.run(&["namespace", "create", "profile-default"]));
    assert_success(&harness.run(&["use", "profile-default"]));

    let output = harness.run_with_env(&[("LOONFS_NAMESPACE", "")], &["--json", "changes"]);

    assert_success(&output);
    assert_eq!(json_data(&output)["namespace_id"], "profile-default");
}

#[test]
fn embedded_profile_missing_namespace_reports_user_facing_message() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");

    let output = harness.run(&["--json", "ls", "--namespace", "missing", "/"]);
    assert_failure(&output);
    let error = json_error(&output);
    assert_eq!(error["code"], "namespace_not_found");
    assert_eq!(error["message"], "namespace `missing` does not exist");
}

#[test]
fn remote_profile_missing_namespace_reports_user_facing_message() {
    let harness = Harness::new();
    let remote_server =
        harness.start_external_server(harness.write_server_config("remote", "remote-missing-ns"));
    let add_remote = harness.run(&[
        "--json",
        "profile",
        "create",
        "default",
        "--mode",
        "remote",
        "--server-url",
        &remote_server.server_url,
        "--auth-token",
        "test-token",
    ]);
    assert_success(&add_remote);

    let output = harness.run(&["--json", "ls", "--namespace", "missing", "/"]);
    assert_failure(&output);
    let error = json_error(&output);
    assert_eq!(error["code"], "namespace_not_found");
    assert_eq!(error["message"], "namespace `missing` does not exist");
}

#[test]
fn current_reports_profile_specific_namespace() {
    let harness = Harness::new();
    harness.add_embedded_profile("alpha");
    harness.add_embedded_profile("beta");

    assert_success(&harness.run(&["namespace", "create", "--profile", "alpha", "alpha-ns"]));
    assert_success(&harness.run(&["use", "--profile", "alpha", "alpha-ns"]));
    assert_success(&harness.run(&["namespace", "create", "--profile", "beta", "beta-ns"]));
    assert_success(&harness.run(&["use", "--profile", "beta", "beta-ns"]));
    assert_success(&harness.run(&["profile", "use", "beta"]));

    let current_default = harness.run(&["--json", "current"]);
    assert_success(&current_default);
    assert_eq!(json_data(&current_default)["profile"], "beta");
    assert_eq!(json_data(&current_default)["namespace"], "beta-ns");

    let current_alpha = harness.run(&["--json", "current", "--profile", "alpha"]);
    assert_success(&current_alpha);
    assert_eq!(json_data(&current_alpha)["profile"], "alpha");
    assert_eq!(json_data(&current_alpha)["namespace"], "alpha-ns");
}

#[test]
fn current_uses_namespace_from_environment() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    assert_success(&harness.run(&["namespace", "create", "profile-default"]));
    assert_success(&harness.run(&["use", "profile-default"]));

    let current = harness.run_with_env(
        &[("LOONFS_NAMESPACE", "from-environment")],
        &["--json", "current"],
    );

    assert_success(&current);
    assert_eq!(json_data(&current)["namespace"], "from-environment");
}

#[test]
fn current_uses_profile_default_without_namespace_environment() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    assert_success(&harness.run(&["namespace", "create", "profile-default"]));
    assert_success(&harness.run(&["use", "profile-default"]));

    // `Harness::run` spawns the command with `LOONFS_NAMESPACE` removed.
    let current = harness.run(&["--json", "current"]);

    assert_success(&current);
    assert_eq!(json_data(&current)["namespace"], "profile-default");
}

#[test]
fn current_does_not_require_backend_resolution() {
    let harness = Harness::new();
    harness.write_cli_config(format!(
        r#"config_version = 1
default_profile = "broken"

[profiles.broken]
mode = "embedded"
default_namespace = "demo"

[profiles.broken.store]
kind = "local-fs"
root = "{}"
key_prefix = "../bad"
"#,
        harness.store_root("broken").display()
    ));

    let current = harness.run(&["--json", "current"]);
    assert_success(&current);
    assert_eq!(json_data(&current)["profile"], "broken");
    assert_eq!(json_data(&current)["namespace"], "demo");
}

#[test]
fn rm_reports_the_inode_and_undelete_recovers_it() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    assert_success(&harness.run(&["namespace", "create", "demo"]));
    assert_success(&harness.run(&["use", "demo"]));

    let payload_one = harness.temp_dir.path().join("one.txt");
    let payload_two = harness.temp_dir.path().join("two.txt");
    fs::write(&payload_one, b"draft one").expect("payload one");
    fs::write(&payload_two, b"draft two").expect("payload two");
    assert_success(&harness.run(&[
        "put",
        payload_one.to_str().expect("utf-8 path"),
        "/docs/report.txt",
    ]));
    assert_success(&harness.run(&[
        "put",
        payload_two.to_str().expect("utf-8 path"),
        "/docs/report.txt",
        "--force",
    ]));

    // rm reports the inode id and the deletion's sequence — together the
    // recovery handle undelete needs.
    let removed = harness.run(&["--json", "rm", "/docs/report.txt"]);
    assert_success(&removed);
    let inode_id = json_data(&removed)["inode_id"]
        .as_u64()
        .expect("rm reports the deleted inode id");
    let deletion_seq = json_data(&removed)["committed_seq"]
        .as_u64()
        .expect("rm reports the deletion sequence");
    let gone = harness.run(&["--json", "revisions", "/docs/report.txt"]);
    assert_failure(&gone);
    assert_eq!(json_error(&gone)["code"], "path_not_found");

    let retired_flag = harness.run(&[
        "undelete",
        "/docs/report.txt",
        "--inode",
        &inode_id.to_string(),
        "--deleted-at",
        &deletion_seq.to_string(),
    ]);
    assert_failure(&retired_flag);
    let retired_flag_error = stderr_string(&retired_flag);
    assert!(retired_flag_error.contains("--deleted-at"));
    assert!(retired_flag_error.contains("--deletion-seq"));

    // Undelete brings back identity, content, and revision history.
    let recovered = harness.run(&[
        "--json",
        "undelete",
        "/docs/report.txt",
        "--inode",
        &inode_id.to_string(),
        "--deletion-seq",
        &deletion_seq.to_string(),
    ]);
    assert_success(&recovered);
    assert_eq!(json_data(&recovered)["target"], "demo:/docs/report.txt");
    let cat = harness.run(&["cat", "/docs/report.txt"]);
    assert_success(&cat);
    assert_eq!(cat.stdout, b"draft two");
    let revisions = harness.run(&["--json", "revisions", "/docs/report.txt"]);
    assert_success(&revisions);
    assert_eq!(
        json_data(&revisions)["revisions"]
            .as_array()
            .expect("json array")
            .len(),
        2
    );

    // A recovered inode is no longer deleted; the stale handle conflicts.
    let again = harness.run(&[
        "--json",
        "undelete",
        "/docs/report-copy.txt",
        "--inode",
        &inode_id.to_string(),
        "--deletion-seq",
        &deletion_seq.to_string(),
    ]);
    assert_failure(&again);
    assert_eq!(json_error(&again)["code"], "not_deleted");
}

/// A pathless undelete restores in place: it re-binds under the parent
/// inode and name the deletion recorded. Renaming the parent between the
/// delete and the recovery is the proof that the anchor is identity, not a
/// remembered path — the entry comes back inside the parent's new name.
#[test]
fn an_undelete_without_a_path_restores_in_place() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    assert_success(&harness.run(&["namespace", "create", "demo"]));
    assert_success(&harness.run(&["use", "demo"]));

    let payload = harness.temp_dir.path().join("report.txt");
    fs::write(&payload, b"quarterly numbers").expect("payload");
    assert_success(&harness.run(&[
        "put",
        payload.to_str().expect("utf-8 path"),
        "/docs/report.txt",
    ]));

    let removed = harness.run(&["rm", "/docs/report.txt"]);
    assert_success(&removed);
    // The printed recovery command names no destination: in place needs
    // none.
    let recovery = hinted_recovery_command(&removed);
    assert!(
        recovery.starts_with("loonfs undelete --inode "),
        "{recovery}"
    );
    let trash = harness.run(&["--json", "trash"]);
    assert_success(&trash);
    let entry = json_data(&trash)["entries"][0].clone();
    let inode_id = entry["inode_id"]
        .as_u64()
        .expect("trash reports the deleted inode id");
    let deletion_seq = entry["deletion_seq"]
        .as_u64()
        .expect("trash reports the deletion sequence");

    // The parent moves on while the file sits in the trash.
    assert_success(&harness.run(&["mv", "/docs", "/archive"]));

    let recovered = harness.run(&[
        "--json",
        "undelete",
        "--inode",
        &inode_id.to_string(),
        "--deletion-seq",
        &deletion_seq.to_string(),
    ]);
    assert_success(&recovered);
    assert_eq!(json_data(&recovered)["target"], "demo:(restored in place)");
    let cat = harness.run(&["cat", "/archive/report.txt"]);
    assert_success(&cat);
    assert_eq!(cat.stdout, b"quarterly numbers");

    // The trash listing offers the same pathless command.
    fs::write(&payload, b"second life").expect("payload");
    assert_success(&harness.run(&[
        "put",
        payload.to_str().expect("utf-8 path"),
        "/archive/notes.txt",
    ]));
    assert_success(&harness.run(&["rm", "/archive/notes.txt"]));
    let listed = harness.run(&["trash"]);
    assert_success(&listed);
    assert!(
        trash_recovery_command(&listed, "notes.txt").starts_with("loonfs undelete --inode "),
        "trash offers a pathless in-place command"
    );

    // A stale pathless handle answers the same code a pathed one does.
    let stale = harness.run(&[
        "--json",
        "undelete",
        "--inode",
        &inode_id.to_string(),
        "--deletion-seq",
        &deletion_seq.to_string(),
    ]);
    assert_failure(&stale);
    assert_eq!(json_error(&stale)["code"], "not_deleted");
}

#[test]
fn remote_undelete_recovers_through_http() {
    let harness = Harness::new();
    let remote_server =
        harness.start_external_server(harness.write_server_config("remote", "remote-undelete"));
    assert_success(&harness.run(&[
        "--json",
        "profile",
        "create",
        "default",
        "--mode",
        "remote",
        "--server-url",
        &remote_server.server_url,
        "--auth-token",
        "test-token",
    ]));
    assert_success(&harness.run(&["namespace", "create", "demo"]));
    assert_success(&harness.run(&["use", "demo"]));

    let payload = harness.temp_dir.path().join("wire.txt");
    fs::write(&payload, b"over the wire").expect("payload");
    assert_success(&harness.run(&["put", payload.to_str().expect("utf-8 path"), "/wire.txt"]));
    let removed = harness.run(&["--json", "rm", "/wire.txt"]);
    assert_success(&removed);
    let inode_id = json_data(&removed)["inode_id"]
        .as_u64()
        .expect("rm reports the deleted inode id");
    let deletion_seq = json_data(&removed)["committed_seq"]
        .as_u64()
        .expect("rm reports the deletion sequence");

    // The hint is built from the invocation, not the backend, so a remote
    // profile prints the same command an embedded one would.
    let listed = harness.run(&["trash"]);
    assert_success(&listed);
    assert_eq!(
        trash_recovery_command(&listed, "wire.txt"),
        format!(
            "loonfs undelete --inode {inode_id} \
             --deletion-seq {deletion_seq} --namespace demo"
        )
    );

    let recovered = harness.run(&[
        "--json",
        "undelete",
        "/wire.txt",
        "--inode",
        &inode_id.to_string(),
        "--deletion-seq",
        &deletion_seq.to_string(),
    ]);
    assert_success(&recovered);
    let cat = harness.run(&["cat", "/wire.txt"]);
    assert_success(&cat);
    assert_eq!(cat.stdout, b"over the wire");
}

#[test]
fn mkdir_parents_get_noclobber_and_version() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    assert_success(&harness.run(&["namespace", "create", "demo"]));
    assert_success(&harness.run(&["use", "demo"]));

    // mkdir still requires the parent by default; -p creates the chain.
    let missing_parent = harness.run(&["--json", "mkdir", "/a/b/c"]);
    assert_failure(&missing_parent);
    assert_eq!(json_error(&missing_parent)["code"], "path_not_found");
    let with_parents = harness.run(&["--json", "mkdir", "-p", "/a/b/c"]);
    assert_success(&with_parents);
    assert_eq!(json_data(&with_parents)["target"], "demo:/a/b/c");
    let created_ancestor = harness.run(&["--json", "stat", "/a/b"]);
    assert_success(&created_ancestor);
    assert_eq!(json_data(&created_ancestor)["inode_kind"], "dir");

    // get refuses to clobber a local file unless forced.
    let payload = harness.temp_dir.path().join("f.txt");
    fs::write(&payload, b"remote bytes").expect("payload");
    assert_success(&harness.run(&["put", payload.to_str().expect("utf-8 path"), "/f.txt"]));
    let dest = harness.temp_dir.path().join("dest.txt");
    fs::write(&dest, b"precious local bytes").expect("existing local file");
    let refused = harness.run(&[
        "--json",
        "get",
        "/f.txt",
        dest.to_str().expect("utf-8 path"),
    ]);
    assert_failure(&refused);
    assert_eq!(json_error(&refused)["code"], "destination_exists");
    assert_eq!(fs::read(&dest).expect("unchanged"), b"precious local bytes");
    let forced = harness.run(&[
        "--json",
        "get",
        "/f.txt",
        dest.to_str().expect("utf-8 path"),
        "--force",
    ]);
    assert_success(&forced);
    assert_eq!(fs::read(&dest).expect("replaced"), b"remote bytes");

    // Every version form reports the package version without build metadata.
    let version_flag = harness.run(&["--version"]);
    assert_success(&version_flag);
    assert_eq!(
        String::from_utf8_lossy(&version_flag.stdout),
        format!("loonfs {}\n", env!("CARGO_PKG_VERSION"))
    );
    let version_command = harness.run(&["version"]);
    assert_success(&version_command);
    assert_eq!(
        String::from_utf8_lossy(&version_command.stdout),
        format!("{}\n", env!("CARGO_PKG_VERSION"))
    );
    let version = harness.run(&["--json", "version"]);
    assert_success(&version);
    assert_eq!(
        json_data(&version),
        serde_json::json!({
            "kind": "version",
            "version": env!("CARGO_PKG_VERSION")
        })
    );
}

#[test]
fn namespace_delete_without_yes_fails_cleanly_when_not_interactive() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    assert_success(&harness.run(&["namespace", "create", "demo"]));

    // Piped stdin (no terminal): the command names the requirement instead
    // of surfacing the prompt machinery's i/o error.
    let refused = harness.run(&["--json", "namespace", "delete", "demo"]);
    assert_failure(&refused);
    assert_eq!(
        json_error(&refused)["code"],
        "non_interactive_input_required"
    );

    let deleted = harness.run(&["--json", "namespace", "delete", "demo", "--yes"]);
    assert_success(&deleted);
}

/// A refused precondition has to say what it wanted and what it found, or
/// the caller cannot tell a raced delete from a mistyped sequence.
#[test]
fn namespace_delete_reports_both_head_sequences_when_the_precondition_fails() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    assert_success(&harness.run(&["namespace", "create", "demo"]));
    assert_success(&harness.run(&["use", "demo"]));

    let payload = harness.temp_dir.path().join("doc.txt");
    fs::write(&payload, b"body").expect("write payload");
    assert_success(&harness.run(&["put", payload.to_str().expect("utf-8 path"), "/doc.txt"]));

    let stale = harness.run(&[
        "--json",
        "namespace",
        "delete",
        "demo",
        "--yes",
        "--expected-head-seq",
        "0",
    ]);
    assert_failure(&stale);
    let error = json_error(&stale);
    assert_eq!(error["code"], "stale_head");
    assert_eq!(error["message"], "expected head sequence 0, found 1");

    // The same sentence is what a human run prints, since the renderer
    // writes the message through unchanged.
    let human = harness.run(&[
        "namespace",
        "delete",
        "demo",
        "--yes",
        "--expected-head-seq",
        "0",
    ]);
    assert_failure(&human);
    assert_eq!(
        stderr_string(&human).trim_end(),
        "expected head sequence 0, found 1"
    );

    // Refusing deleted nothing, so the namespace is still readable.
    assert_success(&harness.run(&["--json", "ls", "/"]));
}

#[test]
fn admin_gc_reclaims_a_deleted_namespace_instead_of_refusing() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    assert_success(&harness.run(&["namespace", "create", "demo"]));
    assert_success(&harness.run(&["use", "demo"]));

    let payload = harness.temp_dir.path().join("payload.txt");
    fs::write(&payload, b"body").expect("write payload");
    assert_success(&harness.run(&["put", payload.to_str().expect("utf-8 path"), "/doc.txt"]));
    // Materialize derived state so the tombstone has something reclaimable.
    assert_success(&harness.run(&["admin", "flush"]));
    assert_success(&harness.run(&["--json", "namespace", "delete", "demo", "--yes"]));

    // GC is the reclamation path for a tombstoned namespace: it must run
    // and report, not refuse. (Fresh objects sit inside the grace window,
    // so this pins reachability, not byte counts.)
    let gc = harness.run(&["--json", "admin", "gc"]);
    assert_success(&gc);
    assert_eq!(json_data(&gc)["kind"], "garbage_collected");

    // Everything that is not the GC-only step still reports the deletion.
    let step = harness.run(&["--json", "admin", "step"]);
    assert_failure(&step);
    assert_eq!(json_error(&step)["code"], "namespace_deleted");
    let recreate = harness.run(&["--json", "namespace", "create", "demo"]);
    assert_failure(&recreate);
    assert_eq!(json_error(&recreate)["code"], "namespace_deleted");
}

#[test]
fn embedded_grep_works_after_index_enable() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    assert_success(&harness.run(&["namespace", "create", "code"]));
    assert_success(&harness.run(&["use", "code"]));

    let payload = harness.temp_dir.path().join("main.rs");
    fs::write(&payload, b"fn main() {}\n// TODO: expand\n").expect("write payload");
    assert_success(&harness.run(&["put", payload.to_str().expect("utf-8 path"), "/src/main.rs"]));

    // Before the index exists, grep names the missing feature.
    let before = harness.run(&["--json", "grep", "TODO"]);
    assert_failure(&before);
    assert_eq!(json_error(&before)["code"], "not_supported");

    // Enable waits for the backfill in-process: the one-shot CLI is its own
    // grep maintenance, so the query works immediately — no server, no
    // driver, no state that never resolves.
    let enabled = harness.run(&["--json", "admin", "index-enable"]);
    assert_success(&enabled);
    assert_eq!(json_data(&enabled)["status"], "active");
    assert_eq!(json_data(&enabled)["waited_for_seq"], 1);
    assert_eq!(json_data(&enabled)["budget_exhausted"], false);
    let found = harness.run(&["--json", "grep", "TODO"]);
    assert_success(&found);
    assert_eq!(
        json_data(&found)["matches"]
            .as_array()
            .expect("json array")
            .len(),
        1
    );

    // Later writes catch up when enable is re-run.
    let more = harness.temp_dir.path().join("lib.rs");
    fs::write(&more, b"// TODO: also here\n").expect("write payload");
    assert_success(&harness.run(&["put", more.to_str().expect("utf-8 path"), "/src/lib.rs"]));
    let recaught = harness.run(&["--json", "admin", "index-enable"]);
    assert_success(&recaught);
    assert_eq!(json_data(&recaught)["status"], "active");
    let found = harness.run(&["--json", "grep", "TODO"]);
    assert_success(&found);
    assert_eq!(
        json_data(&found)["matches"]
            .as_array()
            .expect("json array")
            .len(),
        2
    );
}

/// `--max-matches` caps the whole search, which `--limit` cannot: `--limit`
/// sizes one page and the deployment rejects a page larger than its own
/// maximum.
#[test]
fn grep_max_matches_caps_the_search_while_limit_sizes_a_page() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    assert_success(&harness.run(&["namespace", "create", "code"]));
    assert_success(&harness.run(&["use", "code"]));

    let payload = harness.temp_dir.path().join("notes.txt");
    fs::write(&payload, b"TODO one\nTODO two\nTODO three\nTODO four\n").expect("write payload");
    assert_success(&harness.run(&["put", payload.to_str().expect("utf-8 path"), "/notes.txt"]));
    assert_success(&harness.run(&["--json", "admin", "index-enable"]));

    let all = harness.run(&["--json", "grep", "TODO"]);
    assert_success(&all);
    let all_data = json_data(&all);
    assert_eq!(all_data["matches"].as_array().expect("json array").len(), 4);
    assert_eq!(all_data["truncated"], false);

    let capped = harness.run(&["--json", "grep", "TODO", "--max-matches", "2"]);
    assert_success(&capped);
    let capped_data = json_data(&capped);
    assert_eq!(
        capped_data["matches"].as_array().expect("json array").len(),
        2
    );
    assert_eq!(capped_data["truncated"], true);

    // A cap the search never reaches is not a truncation.
    let roomy = harness.run(&["--json", "grep", "TODO", "--max-matches", "99"]);
    assert_success(&roomy);
    assert_eq!(
        json_data(&roomy)["matches"]
            .as_array()
            .expect("json array")
            .len(),
        4
    );
    assert_eq!(json_data(&roomy)["truncated"], false);

    // A small page still returns every match, because it bounds a page and
    // the command follows the cursor.
    let paged = harness.run(&["--json", "grep", "TODO", "--limit", "1"]);
    assert_success(&paged);
    assert_eq!(
        json_data(&paged)["matches"]
            .as_array()
            .expect("json array")
            .len(),
        4
    );
    assert_eq!(json_data(&paged)["truncated"], false);

    // The two compose: pages of one, stopped after three.
    let both = harness.run(&[
        "--json",
        "grep",
        "TODO",
        "--limit",
        "1",
        "--max-matches",
        "3",
    ]);
    assert_success(&both);
    assert_eq!(
        json_data(&both)["matches"]
            .as_array()
            .expect("json array")
            .len(),
        3
    );
    assert_eq!(json_data(&both)["truncated"], true);

    // The human rendering says it stopped early.
    let human = harness.run(&["grep", "TODO", "--max-matches", "2"]);
    assert_success(&human);
    assert!(stdout_string(&human).contains("--max-matches"));
}

#[test]
fn index_enable_leaves_core_maintenance_decoupled() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    assert_success(&harness.run(&["namespace", "create", "demo"]));
    assert_success(&harness.run(&["use", "demo"]));

    let enabled = harness.run(&["--json", "admin", "index-enable"]);
    assert_success(&enabled);
    assert!(json_data(&enabled).get("backfill_step").is_none());

    let retried = harness.run(&["--json", "admin", "index-enable"]);
    assert_success(&retried);
    assert_eq!(json_data(&retried)["status"], "active");
    assert!(json_data(&retried).get("backfill_step").is_none());
}

/// The index reports its lifecycle status, and statuses do not share a
/// field: a backfill names its target, an active index names its watermark.
#[test]
fn index_status_reports_each_lifecycle_status_in_its_own_terms() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    assert_success(&harness.run(&["namespace", "create", "demo"]));
    assert_success(&harness.run(&["use", "demo"]));

    let disabled = harness.run(&["--json", "admin", "index-status"]);
    assert_success(&disabled);
    assert_eq!(json_data(&disabled)["status"], "disabled");
    assert!(json_data(&disabled).get("built_through_seq").is_none());

    // `--no-wait` returns with the root the enable published: a backfill,
    // naming the sequence it will walk to and nothing it has indexed.
    let enabled = harness.run(&["--json", "admin", "index-enable", "--no-wait"]);
    assert_success(&enabled);
    let data = json_data(&enabled);
    assert_eq!(data["status"], "backfilling");
    assert_eq!(data["target_seq"], 0);
    assert!(data.get("built_through_seq").is_none());
    assert!(json_data(&enabled).get("waited_for_seq").is_none());
    assert_eq!(json_data(&enabled)["steps"], 0);

    let backfilling = harness.run(&["--json", "admin", "index-status"]);
    assert_success(&backfilling);
    assert_eq!(json_data(&backfilling)["status"], "backfilling");
    assert_eq!(json_data(&backfilling)["reorganize_pending"], false);
    assert!(backfilling_text_names_no_watermark(&harness));

    // Waiting takes it active, and only then is there a watermark.
    assert_success(&harness.run(&["admin", "index-enable"]));
    let active = harness.run(&["--json", "admin", "index-status"]);
    assert_success(&active);
    assert_eq!(json_data(&active)["status"], "active");
    assert_eq!(json_data(&active)["built_through_seq"], 0);
    assert!(json_data(&active).get("target_seq").is_none());

    let disabled = harness.run(&["--json", "admin", "index-disable"]);
    assert_success(&disabled);
    let data = json_data(&disabled);
    assert_eq!(data["status"], "disabled");
    assert!(data.get("built_through_seq").is_none());
    assert!(data.get("next_run_ordinal").is_some());

    let disabled_again = harness.run(&["--json", "admin", "index-disable"]);
    assert_success(&disabled_again);
    assert_eq!(json_data(&disabled_again)["status"], "disabled");
}

fn backfilling_text_names_no_watermark(harness: &Harness) -> bool {
    let rendered = stdout_string(&harness.run(&["admin", "index-status"]));
    rendered.contains("backfilling toward seq") && !rendered.contains("built through")
}

/// The wait stops at the sequence it captured, even while a writer keeps
/// committing past it.
#[test]
fn index_enable_waits_to_its_captured_target_and_not_the_live_head() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    assert_success(&harness.run(&["namespace", "create", "demo"]));
    assert_success(&harness.run(&["use", "demo"]));
    let payload = harness.temp_dir.path().join("one.txt");
    fs::write(&payload, b"needle one\n").expect("write payload");
    assert_success(&harness.run(&["put", payload.to_str().expect("utf-8 path"), "/one.txt"]));

    let enabled = harness.run(&["--json", "admin", "index-enable"]);
    assert_success(&enabled);
    assert_eq!(json_data(&enabled)["waited_for_seq"], 1);

    // A commit that lands after the capture is not waited for: the next
    // enable is what picks it up.
    let more = harness.temp_dir.path().join("two.txt");
    fs::write(&more, b"needle two\n").expect("write payload");
    assert_success(&harness.run(&["put", more.to_str().expect("utf-8 path"), "/two.txt"]));
    let status = harness.run(&["--json", "admin", "index-status"]);
    assert_success(&status);
    assert_eq!(
        json_data(&status)["built_through_seq"],
        1,
        "the earlier wait stopped at the target it captured"
    );

    // An index already at the namespace head returns without stepping.
    assert_success(&harness.run(&["admin", "index-enable"]));
    let caught_up = harness.run(&["--json", "admin", "index-enable"]);
    assert_success(&caught_up);
    assert_eq!(json_data(&caught_up)["status"], "active");
    assert_eq!(json_data(&caught_up)["waited_for_seq"], 2);
    assert_eq!(
        json_data(&caught_up)["steps"],
        0,
        "an index already at the captured target takes no steps"
    );
}

/// A wait that runs out of budget reports where the index got to and exits
/// nonzero — real progress, not an error-shaped lie.
#[test]
fn index_enable_budgets_exit_nonzero_and_report_progress() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    assert_success(&harness.run(&["namespace", "create", "demo"]));
    assert_success(&harness.run(&["use", "demo"]));
    let payload = harness.temp_dir.path().join("one.txt");
    fs::write(&payload, b"needle\n").expect("write payload");
    assert_success(&harness.run(&["put", payload.to_str().expect("utf-8 path"), "/one.txt"]));

    for budget in [vec!["--max-steps", "0"], vec!["--deadline-ms", "0"]] {
        let mut args = vec!["--json", "admin", "index-enable"];
        args.extend(budget.iter().copied());
        let stopped = harness.run(&args);
        assert_failure(&stopped);
        let data = json_data(&stopped);
        assert_eq!(data["budget_exhausted"], true, "{budget:?}");
        assert_eq!(data["steps"], 0, "{budget:?}");
        assert_eq!(data["waited_for_seq"], 1, "{budget:?}");
        assert_eq!(
            data["status"], "backfilling",
            "the report must say where the index actually is: {budget:?}"
        );
    }

    // The index is untouched by the give-up, and a plain wait still lands.
    assert_success(&harness.run(&["admin", "index-enable"]));
    let found = harness.run(&["--json", "grep", "needle"]);
    assert_success(&found);
}

/// The remote arm answers the same questions the embedded one does, and
/// waits the same way: the server drives its own index, so the command only
/// watches the status endpoint until the captured target is reached.
#[test]
fn index_status_and_enable_answer_the_same_over_the_remote_transport() {
    let harness = Harness::new();
    // A config with no `[grep]` table composes no grep at all, so this
    // deployment says so explicitly.
    let remote_server = harness.start_external_server(harness.write_server_config_with(
        "remote",
        "index-remote",
        "\n[grep]\nmode = \"serve_and_maintain\"\n",
    ));
    assert_success(&harness.run(&[
        "--json",
        "profile",
        "create",
        "default",
        "--mode",
        "remote",
        "--server-url",
        &remote_server.server_url,
        "--auth-token",
        "test-token",
    ]));
    assert_success(&harness.run(&["namespace", "create", "demo"]));
    assert_success(&harness.run(&["use", "demo"]));
    let payload = harness.temp_dir.path().join("one.txt");
    fs::write(&payload, b"remote needle\n").expect("write payload");
    assert_success(&harness.run(&["put", payload.to_str().expect("utf-8 path"), "/one.txt"]));

    let disabled = harness.run(&["--json", "admin", "index-status"]);
    assert_success(&disabled);
    assert_eq!(json_data(&disabled)["status"], "disabled");

    let enabled = harness.run(&["--json", "admin", "index-enable"]);
    assert_success(&enabled);
    assert_eq!(json_data(&enabled)["waited_for_seq"], 1);
    assert_eq!(json_data(&enabled)["budget_exhausted"], false);
    assert_eq!(json_data(&enabled)["status"], "active");

    let active = harness.run(&["--json", "admin", "index-status"]);
    assert_success(&active);
    assert_eq!(json_data(&active)["built_through_seq"], 1);

    let found = harness.run(&["--json", "grep", "remote needle"]);
    assert_success(&found);
    assert_eq!(
        json_data(&found)["matches"]
            .as_array()
            .expect("json array")
            .len(),
        1
    );

    let collected = harness.run(&["--json", "admin", "index-gc"]);
    assert_success(&collected);
    assert_eq!(json_data(&collected)["namespace_reaped"], false);
}

/// `index-gc` loops the cursor like `admin gc`: one accumulated result out
/// of however many bounded passes it took.
#[test]
fn index_gc_loops_its_cursor_and_accumulates() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    assert_success(&harness.run(&["namespace", "create", "demo"]));
    assert_success(&harness.run(&["use", "demo"]));
    let payload = harness.temp_dir.path().join("one.txt");
    fs::write(&payload, b"needle\n").expect("write payload");
    assert_success(&harness.run(&["put", payload.to_str().expect("utf-8 path"), "/one.txt"]));
    assert_success(&harness.run(&["admin", "index-enable"]));

    // Nothing here is past its grace window, so a full loop retains what it
    // examines and, having walked to the end, carries no resume cursor.
    let collected = harness.run(&["--json", "admin", "index-gc"]);
    assert_success(&collected);
    let data = json_data(&collected);
    assert_eq!(data["deleted_segments"], 0);
    assert_eq!(data["namespace_reaped"], false);
    assert!(data.get("next_cursor").is_none(), "{data}");

    // One bounded pass stops early and hands back where to resume.
    let single = harness.run(&["--json", "admin", "index-gc", "--max-objects", "1"]);
    assert_success(&single);
    assert!(
        json_data(&single)["next_cursor"].is_string(),
        "{}",
        json_data(&single)
    );
}

/// `admin run --drain` is the assigned host's catch-up: every
/// `{job, namespace}` key it was given reaches a settled conclusion, it
/// exits zero, and the work it did is in durable state rather than in its
/// output.
#[test]
fn admin_run_drains_an_assignment_and_leaves_the_work_done() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    for namespace in ["alpha", "beta"] {
        assert_success(&harness.run(&["namespace", "create", namespace]));
        let payload = harness.temp_dir.path().join(format!("{namespace}.txt"));
        fs::write(&payload, b"assigned needle\n").expect("write payload");
        assert_success(&harness.run(&[
            "put",
            "--namespace",
            namespace,
            payload.to_str().expect("utf-8 path"),
            "/note.txt",
        ]));
        // Enabled and deliberately left behind: catching it up is what the
        // assignment is for.
        assert_success(&harness.run(&[
            "admin",
            "index-enable",
            "--namespace",
            namespace,
            "--no-wait",
        ]));
    }

    let drained = harness.run(&[
        "--json",
        "admin",
        "run",
        "--namespace",
        "alpha",
        "--namespace",
        "beta",
        "--drain",
    ]);
    assert_success(&drained);
    let data = json_data(&drained);
    assert_eq!(data["kind"], "maintenance_drained");
    assert!(data.get("drained").is_none());
    assert_eq!(data["budget_exhausted"], false);
    let keys = data["keys"].as_array().expect("json array");
    assert_eq!(keys.len(), 8, "four jobs over two namespaces: {data}");
    assert!(
        keys.iter().all(|key| key["settled"] == true),
        "an unbudgeted drain settles every key: {data}"
    );
    assert_eq!(
        data["jobs"],
        serde_json::json!(["metadata", "gc", "grep-index", "grep-gc"])
    );

    for namespace in ["alpha", "beta"] {
        let status = harness.run(&["--json", "admin", "index-status", "--namespace", namespace]);
        assert_success(&status);
        assert_eq!(
            json_data(&status)["built_through_seq"],
            1,
            "the assigned index must reach the head it was behind: {namespace}"
        );
    }
}

/// A drain that runs out of budget reports where every key got to — the one
/// it stopped inside and the ones it never reached — and exits nonzero.
#[test]
fn admin_run_budgets_exit_nonzero_and_report_per_key_progress() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    assert_success(&harness.run(&["namespace", "create", "alpha"]));

    let unstarted = harness.run(&[
        "--json",
        "admin",
        "run",
        "--namespace",
        "alpha",
        "--drain",
        "--max-steps",
        "0",
    ]);
    assert_failure(&unstarted);
    let data = json_data(&unstarted);
    assert_eq!(data["budget_exhausted"], true);
    assert_eq!(data["steps"], 0);
    for key in data["keys"].as_array().expect("json array") {
        assert_eq!(key["settled"], false, "{data}");
        assert_eq!(key["steps"], 0, "{data}");
        assert!(key.get("conclusion").is_none(), "{data}");
    }

    // One step is enough for the first key on a quiet namespace and reaches
    // no other, which is exactly what the report has to say.
    let partial = harness.run(&[
        "admin",
        "run",
        "--namespace",
        "alpha",
        "--drain",
        "--max-steps",
        "1",
    ]);
    assert_failure(&partial);
    let rendered = stdout_string(&partial);
    assert!(
        rendered.contains("alpha/metadata: idle after 1 step"),
        "{rendered}"
    );
    assert!(
        rendered.contains("alpha/gc: not started; the budget ran out first"),
        "{rendered}"
    );
    assert!(rendered.contains("gave up"), "{rendered}");
}

/// The assignment is explicit and the job names are a closed set: neither is
/// something the command guesses at.
#[test]
fn admin_run_requires_an_assignment_and_names_the_jobs_it_hosts() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");

    let unassigned = harness.run(&["admin", "run"]);
    assert_failure(&unassigned);
    assert!(
        stderr_string(&unassigned).contains("--namespace"),
        "{}",
        stderr_string(&unassigned)
    );

    let unknown_job = harness.run(&["admin", "run", "--namespace", "alpha", "--job", "bogus"]);
    assert_failure(&unknown_job);
    let message = stderr_string(&unknown_job);
    for job in ["metadata", "core-gc", "grep-index", "grep-gc"] {
        assert!(
            message.contains(job),
            "the valid set must be listed: {message}"
        );
    }
}

/// The re-assertion cadence is the one timer a hosted run owns, so an
/// operator may shorten it — down to a floor, below which a nudge per
/// assigned key only spends provider requests. A drain never rests between
/// keys, so the flag is inert there.
#[test]
fn admin_run_takes_a_poll_interval_with_a_floor_that_a_drain_ignores() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    for namespace in ["alpha", "beta"] {
        assert_success(&harness.run(&["namespace", "create", namespace]));
    }

    let too_fast = harness.run(&[
        "admin",
        "run",
        "--namespace",
        "alpha",
        "--poll-interval-ms",
        "99",
    ]);
    assert_failure(&too_fast);
    let message = stderr_string(&too_fast);
    assert!(
        message.contains("poll-interval-ms"),
        "the rejection must name the flag: {message}"
    );
    assert!(
        message.contains("100"),
        "the rejection must name the floor: {message}"
    );

    let plain = harness.run(&["--json", "admin", "run", "--namespace", "alpha", "--drain"]);
    assert_success(&plain);
    let paced = harness.run(&[
        "--json",
        "admin",
        "run",
        "--namespace",
        "beta",
        "--drain",
        "--poll-interval-ms",
        "100",
    ]);
    assert_success(&paced);
    let (plain, paced) = (json_data(&plain), json_data(&paced));
    for field in ["budget_exhausted", "jobs"] {
        assert_eq!(
            paced[field], plain[field],
            "a drain reports the same `{field}` with the cadence flag as without it"
        );
    }
    let keys = paced["keys"].as_array().expect("json array");
    assert_eq!(
        keys.len(),
        plain["keys"].as_array().expect("json array").len()
    );
    assert!(
        keys.iter().all(|key| key["settled"] == true),
        "the drain still settles every key: {paced}"
    );
}

/// The store probe is store-scoped: it needs no namespace, renders every
/// returned check, and exits zero only because they all passed.
#[test]
fn admin_store_probe_renders_the_profile_stores_report() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");

    let probe = harness.run(&["admin", "store-probe"]);
    assert_success(&probe);
    let text = stdout_string(&probe);

    let json = harness.run(&["--json", "admin", "store-probe"]);
    assert_success(&json);
    let data = json_data(&json);
    assert_eq!(data["kind"], "store_probed");
    let checks = data["checks"].as_array().expect("checks array");
    assert!(!checks.is_empty(), "the probe must report its work");
    let names: std::collections::BTreeSet<&str> = checks
        .iter()
        .map(|check| check["name"].as_str().expect("check name"))
        .collect();
    assert_eq!(names.len(), checks.len(), "check names must be unique");
    for check in checks {
        let name = check["name"].as_str().expect("check name");
        assert_eq!(check["outcome"], "passed", "{check}");
        assert!(text.contains(name), "missing `{name}` in: {text}");
    }
    assert!(
        text.contains(&format!("{} checks passed", checks.len())),
        "{text}"
    );

    // The probe cleans up after itself, so the store carries no probe keys.
    let probe_runs = harness.store_root("default").join("probe-runs");
    assert!(
        !probe_runs.exists() || fs::read_dir(&probe_runs).is_ok_and(|mut dir| dir.next().is_none()),
        "the probe left objects behind at {}",
        probe_runs.display()
    );
}

/// A remote profile's server hosts its own runner. Hosting another one here
/// would be a second scheduler over the same namespaces, so the command
/// refuses instead of pretending it can.
#[test]
fn admin_run_refuses_a_remote_profile() {
    let harness = Harness::new();
    assert_success(&harness.run(&[
        "--json",
        "profile",
        "create",
        "default",
        "--mode",
        "remote",
        "--server-url",
        "http://127.0.0.1:9",
        "--auth-token",
        "test-token",
    ]));

    let refused = harness.run(&["--json", "admin", "run", "--namespace", "demo", "--drain"]);
    assert_failure(&refused);
    let error = json_error(&refused);
    assert_eq!(error["code"], "not_supported");
    assert!(error["message"]
        .as_str()
        .expect("error message")
        .contains("embedded-only"));
}

#[test]
fn help_lists_the_context_commands() {
    let harness = Harness::new();
    let output = Command::new(loon_binary_path())
        .env("HOME", &harness.home_dir)
        .arg("--help")
        .output()
        .expect("run help");
    assert_success(&output);
    let stdout = stdout_string(&output);
    assert!(stdout.contains("current"));
    assert!(stdout.contains("use"));
}

/// `annotate` writes and removes attributes, and `stat` shows what the inode
/// holds — over both transports, from the same command line.
#[test]
fn annotate_writes_and_removes_attributes_in_both_modes() {
    let harness = Harness::new();
    harness.add_embedded_profile("embedded");
    let remote_server =
        harness.start_external_server(harness.write_server_config("remote", "annotate"));
    assert_success(&harness.run(&[
        "--json",
        "profile",
        "create",
        "remote",
        "--mode",
        "remote",
        "--server-url",
        &remote_server.server_url,
        "--auth-token",
        "test-token",
    ]));

    let payload = harness.temp_dir.path().join("file.txt");
    fs::write(&payload, b"bytes\n").expect("payload");

    for profile in ["embedded", "remote"] {
        assert_success(&harness.run(&["namespace", "create", "--profile", profile, "demo"]));
        assert_success(&harness.run(&["use", "--profile", profile, "demo"]));
        assert_success(&harness.run(&[
            "put",
            "--profile",
            profile,
            payload.to_str().expect("utf-8 path"),
            "/docs/file.txt",
        ]));

        // A fresh inode holds an empty map at revision 0, and the human
        // output says nothing extra about it.
        let bare = harness.run(&["stat", "--profile", profile, "/docs/file.txt"]);
        assert_success(&bare);
        assert!(
            !stdout_string(&bare).contains("attr."),
            "an inode with no attributes printed attribute lines: {}",
            stdout_string(&bare)
        );
        let bare_json = harness.run(&["--json", "stat", "--profile", profile, "/docs/file.txt"]);
        assert_success(&bare_json);
        let bare_entry = json_data(&bare_json);
        assert_eq!(bare_entry["attributes"], serde_json::json!({}));
        assert_eq!(bare_entry["attributes_revision_no"], 0);
        assert!(bare_entry.get("attributes_updated_by").is_none());
        assert!(bare_entry.get("attributes_updated_at_ms").is_none());

        let annotated = harness.run(&[
            "--json",
            "annotate",
            "--profile",
            profile,
            "/docs/file.txt",
            "--set",
            "owner=platform",
            "--set",
            "note=has=equals",
        ]);
        assert_success(&annotated);
        assert_eq!(json_data(&annotated)["kind"], "file_mutation");

        let stat = harness.run(&["stat", "--profile", profile, "/docs/file.txt"]);
        assert_success(&stat);
        let text = stdout_string(&stat);
        assert!(text.contains("attr.owner: platform"), "{text}");
        // The key ends at the first `=`, so the rest is the value.
        assert!(text.contains("attr.note: has=equals"), "{text}");

        // Scripts can pass the same plain string map through --attributes-json.
        assert_success(&harness.run(&[
            "annotate",
            "--profile",
            profile,
            "/docs/file.txt",
            "--attributes-json",
            r#"{"set": {"tags": "red,blue"}}"#,
        ]));
        let with_list = harness.run(&["stat", "--profile", profile, "/docs/file.txt"]);
        assert_success(&with_list);
        assert!(stdout_string(&with_list).contains("attr.tags: red,blue"));
        let with_list_json =
            harness.run(&["--json", "stat", "--profile", profile, "/docs/file.txt"]);
        assert_success(&with_list_json);
        assert_eq!(
            json_data(&with_list_json)["attributes"]["tags"],
            serde_json::json!("red,blue")
        );
        assert_eq!(
            json_data(&with_list_json)["attributes_updated_by"],
            serde_json::json!({ "kind": "service", "id": "loonfs-cli" })
        );
        assert!(
            json_data(&with_list_json)["attributes_updated_at_ms"]
                .as_u64()
                .expect("attribute update time")
                > 0
        );

        assert_success(&harness.run(&[
            "annotate",
            "--profile",
            profile,
            "/docs/file.txt",
            "--remove",
            "owner",
        ]));
        let removed = harness.run(&["--json", "stat", "--profile", profile, "/docs/file.txt"]);
        assert_success(&removed);
        let entry = json_data(&removed);
        assert!(entry["attributes"]["owner"].is_null());
        assert_eq!(entry["attributes"]["note"], "has=equals");
        // Three effective updates, three revisions.
        assert_eq!(entry["attributes_revision_no"], 3);
        assert!(entry.pointer("/attributes/attributes").is_none());

        // Attributes belong to the inode, so a directory takes them too.
        assert_success(&harness.run(&[
            "annotate",
            "--profile",
            profile,
            "/docs",
            "--set",
            "owner=docs-team",
        ]));
        let directory = harness.run(&["stat", "--profile", profile, "/docs"]);
        assert_success(&directory);
        assert!(stdout_string(&directory).contains("attr.owner: docs-team"));

        // A listing does not carry attributes, and the CLI offers no flag.
        let listing = harness.run(&["--json", "ls", "--profile", profile, "/docs"]);
        assert_success(&listing);
        let listed = json_data(&listing);
        assert!(listed["entries"][0]["attributes"].is_null());
    }
}

/// The argument errors `annotate` reports about its own flags, before it
/// commits anything.
#[test]
fn annotate_rejects_a_set_without_an_equals_and_a_document_beside_the_flags() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    assert_success(&harness.run(&["namespace", "create", "demo"]));
    assert_success(&harness.run(&["use", "demo"]));
    assert_success(&harness.run(&["mkdir", "/docs"]));

    let no_equals = harness.run(&["--json", "annotate", "/docs", "--set", "owner"]);
    assert_failure(&no_equals);
    assert_eq!(json_error(&no_equals)["code"], "invalid_input");
    assert!(json_error(&no_equals)["message"]
        .as_str()
        .expect("json string")
        .contains("--set"));

    // clap rejects the combination before the command runs.
    let both = harness.run(&[
        "--json",
        "annotate",
        "/docs",
        "--set",
        "owner=platform",
        "--attributes-json",
        r#"{"set": {}}"#,
    ]);
    assert_failure(&both);
    assert_eq!(both.status.code(), Some(2));
    assert_eq!(parse_json(&both.stderr)["error"]["code"], "invalid_usage");
}

/// `mkdir -p` on a directory that is already there succeeds, the way Unix
/// does, and still fails on a file — in both profile modes.
#[test]
fn mkdir_parents_is_idempotent_over_an_existing_directory_in_both_modes() {
    let harness = Harness::new();
    harness.add_embedded_profile("embedded");
    let remote_server =
        harness.start_external_server(harness.write_server_config("remote", "mkdir-idempotent"));
    assert_success(&harness.run(&[
        "--json",
        "profile",
        "create",
        "remote",
        "--mode",
        "remote",
        "--server-url",
        &remote_server.server_url,
        "--auth-token",
        "test-token",
    ]));

    let payload = harness.temp_dir.path().join("file.txt");
    fs::write(&payload, b"bytes\n").expect("payload");

    for profile in ["embedded", "remote"] {
        assert_success(&harness.run(&["namespace", "create", "--profile", profile, "demo"]));
        assert_success(&harness.run(&["use", "--profile", profile, "demo"]));

        let created = harness.run(&["--json", "mkdir", "-p", "/a/b", "--profile", profile]);
        assert_success(&created);
        assert_eq!(json_data(&created)["kind"], "file_mutation");

        // The second -p is a no-op, not a conflict: no commit, and the
        // output says what is already there.
        let again = harness.run(&["--json", "mkdir", "-p", "/a/b", "--profile", profile]);
        assert_success(&again);
        let again_data = json_data(&again);
        assert_eq!(again_data["kind"], "directory_already_exists");
        assert_eq!(again_data["target"], "demo:/a/b");
        assert!(again_data["inode_id"].is_number());

        // Without -p the conflict still surfaces.
        let strict = harness.run(&["--json", "mkdir", "/a/b", "--profile", profile]);
        assert_failure(&strict);
        assert_eq!(json_error(&strict)["code"], "path_conflict");

        // A file at the target is a conflict even with -p.
        assert_success(&harness.run(&[
            "put",
            "--profile",
            profile,
            payload.to_str().expect("utf-8 path"),
            "/a/file.txt",
        ]));
        let over_file =
            harness.run(&["--json", "mkdir", "-p", "/a/file.txt", "--profile", profile]);
        assert_failure(&over_file);
        assert_eq!(json_error(&over_file)["code"], "path_conflict");
    }
}

/// `cp` and `mv` land inside a destination that is already a directory, the
/// way Unix does; a destination that is a file keeps its overwrite rules.
#[test]
fn cp_and_mv_land_inside_an_existing_directory_in_both_modes() {
    let harness = Harness::new();
    harness.add_embedded_profile("embedded");
    let remote_server =
        harness.start_external_server(harness.write_server_config("remote", "transfer-into-dir"));
    assert_success(&harness.run(&[
        "--json",
        "profile",
        "create",
        "remote",
        "--mode",
        "remote",
        "--server-url",
        &remote_server.server_url,
        "--auth-token",
        "test-token",
    ]));

    let payload = harness.temp_dir.path().join("report.pdf");
    fs::write(&payload, b"report bytes\n").expect("payload");
    let other = harness.temp_dir.path().join("other.txt");
    fs::write(&other, b"other bytes\n").expect("other payload");

    for profile in ["embedded", "remote"] {
        assert_success(&harness.run(&["namespace", "create", "--profile", profile, "demo"]));
        assert_success(&harness.run(&["use", "--profile", profile, "demo"]));
        assert_success(&harness.run(&["mkdir", "/docs", "--profile", profile]));
        assert_success(&harness.run(&[
            "put",
            "--profile",
            profile,
            payload.to_str().expect("utf-8 path"),
            "/report.pdf",
        ]));

        // No trailing slash, and /docs is a directory: the file keeps its
        // own name inside it.
        let copied = harness.run(&["--json", "cp", "/report.pdf", "/docs", "--profile", profile]);
        assert_success(&copied);
        assert_eq!(json_data(&copied)["to"], "demo:/docs/report.pdf");

        let moved = harness.run(&["--json", "mv", "/report.pdf", "/docs", "--profile", profile]);
        assert_failure(&moved);
        assert_eq!(
            json_error(&moved)["code"],
            "path_conflict",
            "the copy already occupies /docs/report.pdf"
        );
        let forced = harness.run(&[
            "--json",
            "mv",
            "/report.pdf",
            "/docs",
            "--force",
            "--profile",
            profile,
        ]);
        assert_success(&forced);
        assert_eq!(json_data(&forced)["to"], "demo:/docs/report.pdf");

        // A destination that does not exist is still the exact path typed.
        let renamed = harness.run(&[
            "--json",
            "cp",
            "/docs/report.pdf",
            "/docs/renamed.pdf",
            "--profile",
            profile,
        ]);
        assert_success(&renamed);
        assert_eq!(json_data(&renamed)["to"], "demo:/docs/renamed.pdf");

        // A destination that is a file keeps the overwrite rules it had.
        assert_success(&harness.run(&[
            "put",
            "--profile",
            profile,
            other.to_str().expect("utf-8 path"),
            "/other.txt",
        ]));
        let onto_file = harness.run(&[
            "--json",
            "cp",
            "/other.txt",
            "/docs/renamed.pdf",
            "--profile",
            profile,
        ]);
        assert_failure(&onto_file);
        assert_eq!(json_error(&onto_file)["code"], "path_conflict");
        let onto_file_forced = harness.run(&[
            "--json",
            "cp",
            "/other.txt",
            "/docs/renamed.pdf",
            "--force",
            "--profile",
            profile,
        ]);
        assert_success(&onto_file_forced);
        assert_eq!(json_data(&onto_file_forced)["to"], "demo:/docs/renamed.pdf");

        // A directory tree lands inside an existing directory too.
        assert_success(&harness.run(&["mkdir", "/archive", "--profile", profile]));
        let tree = harness.run(&[
            "--json",
            "cp",
            "-r",
            "/docs",
            "/archive",
            "--profile",
            profile,
        ]);
        assert_success(&tree);
        assert_eq!(json_data(&tree)["destination"], "demo:/archive/docs");
    }
}

/// The default listing stops after one real server page, while the explicit
/// streaming modes cross that boundary and a cursor resumes at the next row.
#[test]
fn ls_default_all_jsonl_and_cursor_obey_page_boundaries() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    assert_success(&harness.run(&["namespace", "create", "demo"]));
    assert_success(&harness.run(&["use", "demo"]));

    // The pagination policy is fixed at 1,000 entries, so one extra file is
    // the smallest integration fixture that produces a continuation cursor.
    let local = harness.temp_dir.path().join("listing");
    fs::create_dir(&local).expect("create listing fixture");
    for index in 0..=loonfs_api::DEFAULT_PAGE_LIMIT {
        fs::write(local.join(format!("f{index:04}.txt")), b"x").expect("write listing entry");
    }
    let uploaded = harness.run(&[
        "--json",
        "--no-progress",
        "put",
        "-r",
        local.to_str().expect("utf-8 path"),
        "/listing",
    ]);
    assert_success(&uploaded);

    let default_human = harness.run(&["ls", "/listing"]);
    assert_success(&default_human);
    assert!(stderr_string(&default_human).is_empty());
    let default_human_stdout = stdout_string(&default_human);
    let human_lines = default_human_stdout.lines().collect::<Vec<_>>();
    assert_eq!(
        human_lines.len(),
        loonfs_api::DEFAULT_PAGE_LIMIT as usize + 1
    );
    assert!(human_lines
        .last()
        .expect("continuation line")
        .starts_with("more entries exist; continue with --cursor "));
    assert!(human_lines
        .last()
        .expect("continuation line")
        .ends_with(" or stream everything with --all"));

    let first = harness.run(&["--json", "ls", "/listing"]);
    assert_success(&first);
    let first_data = json_data(&first);
    assert_eq!(first_data["namespace_id"], "demo");
    assert_eq!(first_data["path"], "/listing");
    assert!(first_data["head_seq"].is_u64());
    assert!(first_data.get("head_drift").is_none());
    let first_entries = first_data["entries"].as_array().expect("first page");
    assert_eq!(first_entries.len(), loonfs_api::DEFAULT_PAGE_LIMIT as usize);
    let cursor = first_data["next_cursor"]
        .as_str()
        .expect("default JSON page carries a cursor");

    let one_page_jsonl = harness.run(&["ls", "/listing", "--jsonl"]);
    assert_success(&one_page_jsonl);
    assert_eq!(
        stdout_string(&one_page_jsonl).lines().count(),
        loonfs_api::DEFAULT_PAGE_LIMIT as usize
    );

    let all = harness.run(&["ls", "/listing", "--all"]);
    assert_success(&all);
    assert_eq!(
        stdout_string(&all).lines().count(),
        loonfs_api::DEFAULT_PAGE_LIMIT as usize + 1
    );
    assert!(!stdout_string(&all).contains("more entries exist"));

    let all_json = harness.run(&["--json", "ls", "/listing", "--all"]);
    assert_success(&all_json);
    let all_json_data = json_data(&all_json);
    assert_eq!(all_json_data["namespace_id"], "demo");
    assert_eq!(all_json_data["path"], "/listing");
    assert!(all_json_data["head_seq"].is_u64());
    assert!(all_json_data.get("head_drift").is_none());
    assert_eq!(
        all_json_data["entries"]
            .as_array()
            .expect("all entries")
            .len(),
        loonfs_api::DEFAULT_PAGE_LIMIT as usize + 1
    );
    assert!(all_json_data.get("next_cursor").is_none());

    let jsonl = harness.run(&["ls", "/listing", "--all", "--jsonl"]);
    assert_success(&jsonl);
    let jsonl_entries = stdout_string(&jsonl)
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).expect("one JSON entry per line"))
        .collect::<Vec<_>>();
    assert_eq!(
        jsonl_entries.len(),
        loonfs_api::DEFAULT_PAGE_LIMIT as usize + 1
    );
    assert!(jsonl_entries
        .iter()
        .all(|entry| entry.get("data").is_none()));
    let actual_paths = jsonl_entries
        .iter()
        .map(|entry| entry["path"].as_str().expect("entry path"))
        .collect::<Vec<_>>();
    let expected_paths = (0..=loonfs_api::DEFAULT_PAGE_LIMIT)
        .map(|index| format!("/listing/f{index:04}.txt"))
        .collect::<Vec<_>>();
    assert_eq!(actual_paths, expected_paths);

    let second = harness.run(&["--json", "ls", "/listing", "--cursor", cursor]);
    assert_success(&second);
    let second_data = json_data(&second);
    let second_entries = second_data["entries"].as_array().expect("second page");
    assert_eq!(second_entries.len(), 1);
    assert_eq!(
        first_entries.last().expect("last first-page entry")["path"],
        "/listing/f0999.txt"
    );
    assert_eq!(second_entries[0]["path"], "/listing/f1000.txt");
}

/// A two-page wire fixture makes the otherwise racy between-page commit
/// deterministic while still exercising the CLI process, client, command,
/// JSON envelope, and standard-error rendering together.
#[test]
fn ls_surfaces_head_drift_from_paged_responses() {
    let harness = Harness::new();

    let (server_url, server) = json_response_server(vec![
        serde_json::json!({
            "namespace_id": "demo",
            "path": "/docs",
            "head_seq": 5,
            "entries": [],
            "next_cursor": "resume-after-first-page",
        }),
        serde_json::json!({
            "namespace_id": "demo",
            "path": "/docs",
            "head_seq": 8,
            "entries": [],
        }),
    ]);
    harness.write_remote_listing_config(&server_url);
    let json = harness.run(&["--json", "ls", "/docs", "--all"]);
    assert_success(&json);
    assert!(json.stderr.is_empty());
    let data = json_data(&json);
    assert_eq!(data["namespace_id"], "demo");
    assert_eq!(data["path"], "/docs");
    assert_eq!(data["head_seq"], 8);
    assert_eq!(
        data["head_drift"],
        serde_json::json!({
            "first_head_seq": 5,
            "last_head_seq": 8,
        })
    );
    server.join().expect("listing server");

    let (server_url, server) = json_response_server(vec![
        serde_json::json!({
            "namespace_id": "demo",
            "path": "/docs",
            "head_seq": 11,
            "entries": [],
            "next_cursor": "resume-after-first-page",
        }),
        serde_json::json!({
            "namespace_id": "demo",
            "path": "/docs",
            "head_seq": 12,
            "entries": [],
        }),
    ]);
    harness.write_remote_listing_config(&server_url);
    let human = harness.run(&["ls", "/docs", "--all"]);
    assert_success(&human);
    let stderr = stderr_string(&human);
    assert_eq!(
        stderr,
        "warning: namespace advanced during the listing (head seq 11 to 12); entries may mix states; re-run for a settled view\n"
    );
    assert_eq!(
        stderr
            .matches("namespace advanced during the listing")
            .count(),
        1
    );
    server.join().expect("listing server");
}

#[test]
fn recursive_get_surfaces_drift_across_directory_listings() {
    let harness = Harness::new();
    let (server_url, server) = json_response_server(vec![
        serde_json::json!({
            "namespace_id": "demo",
            "path": "/docs",
            "inode_id": 2,
            "created_by": { "kind": "system", "id": "loonfs" },
            "created_at_ms": 1_752_624_000_000_u64,
            "inode_kind": "dir",
            "head_seq": 20,
            "parent_inode_id": 1,
            "display_name": "docs",
        }),
        serde_json::json!({
            "namespace_id": "demo",
            "path": "/docs",
            "head_seq": 20,
            "entries": [{
                "namespace_id": "demo",
                "path": "/docs/sub",
                "inode_id": 3,
                "created_by": { "kind": "system", "id": "loonfs" },
                "created_at_ms": 1_752_624_000_000_u64,
                "inode_kind": "dir",
                "head_seq": 20,
                "parent_inode_id": 2,
                "display_name": "sub",
            }],
        }),
        serde_json::json!({
            "namespace_id": "demo",
            "path": "/docs/sub",
            "head_seq": 21,
            "entries": [],
        }),
    ]);
    harness.write_remote_listing_config(&server_url);
    let destination = harness.temp_dir.path().join("drifted-download");
    let get = harness.run(&[
        "--json",
        "--no-progress",
        "get",
        "-r",
        "/docs",
        destination.to_str().expect("utf-8 destination"),
    ]);
    assert_success(&get);
    assert!(get.stderr.is_empty());
    let data = json_data(&get);
    assert_eq!(data["files"], 0);
    assert_eq!(data["directories"], 2);
    assert_eq!(
        data["head_drift"],
        serde_json::json!({
            "first_head_seq": 20,
            "last_head_seq": 21,
        })
    );
    server.join().expect("listing server");
}

/// `ls --limit` bounds the total, and incompatible output bounds fail in
/// clap before the command runs.
#[test]
fn ls_limit_bounds_the_whole_listing_and_rejects_all() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    assert_success(&harness.run(&["namespace", "create", "demo"]));
    assert_success(&harness.run(&["use", "demo"]));

    let payload = harness.temp_dir.path().join("entry.txt");
    fs::write(&payload, b"bytes\n").expect("payload");
    for index in 0..5 {
        assert_success(&harness.run(&[
            "put",
            payload.to_str().expect("utf-8 path"),
            &format!("/f{index}.txt"),
        ]));
    }

    // Explicitly unbounded JSON still prints everything in one document.
    let all = harness.run(&["--json", "ls", "--all"]);
    assert_success(&all);
    let all_data = json_data(&all);
    assert_eq!(all_data["entries"].as_array().expect("json array").len(), 5);
    assert!(all_data.get("next_cursor").is_none());

    // A bound stops at exactly that many entries and hands back a cursor.
    let first = harness.run(&["--json", "ls", "--limit", "2"]);
    assert_success(&first);
    let first_data = json_data(&first);
    let first_entries = first_data["entries"].as_array().expect("json array");
    assert_eq!(first_entries.len(), 2);
    let cursor = first_data["next_cursor"]
        .as_str()
        .expect("a truncated listing reports where it stopped")
        .to_owned();

    let second = harness.run(&["--json", "ls", "--limit", "2", "--cursor", &cursor]);
    assert_success(&second);
    let second_data = json_data(&second);
    let second_entries = second_data["entries"].as_array().expect("json array");
    assert_eq!(second_entries.len(), 2);
    assert_ne!(
        second_entries[0]["path"], first_entries[0]["path"],
        "the cursor resumes after the entries already printed"
    );

    // The last page fits inside the bound and reports no cursor.
    let rest = harness.run(&[
        "--json",
        "ls",
        "--limit",
        "10",
        "--cursor",
        second_data["next_cursor"].as_str().expect("second cursor"),
    ]);
    assert_success(&rest);
    let rest_data = json_data(&rest);
    assert_eq!(
        rest_data["entries"].as_array().expect("json array").len(),
        1
    );
    assert!(rest_data.get("next_cursor").is_none());

    // The human rendering says how to continue after the bound.
    let human = harness.run(&["ls", "--limit", "2"]);
    assert_success(&human);
    assert!(stdout_string(&human).contains("continue with --cursor"));

    let conflicting_bound = harness.run(&["ls", "--all", "--limit", "2"]);
    assert_failure(&conflicting_bound);
    assert_eq!(conflicting_bound.status.code(), Some(2));

    let conflicting_json = harness.run(&["--json", "ls", "--jsonl"]);
    assert_failure(&conflicting_json);
    assert_eq!(conflicting_json.status.code(), Some(2));
    assert!(conflicting_json.stdout.is_empty());
}

/// The admin plane and the change feed work end to end, and both profile
/// modes emit the same `--json` shapes and error codes for them.
#[test]
fn admin_and_changes_commands_report_the_same_shapes_in_both_modes() {
    let harness = Harness::new();
    harness.add_embedded_profile("embedded");
    let remote_server =
        harness.start_external_server(harness.write_server_config("remote", "admin-parity"));
    let add_remote = harness.run(&[
        "--json",
        "profile",
        "create",
        "remote",
        "--mode",
        "remote",
        "--server-url",
        &remote_server.server_url,
        "--auth-token",
        "test-token",
    ]);
    assert_success(&add_remote);

    let first_payload = harness.temp_dir.path().join("first.txt");
    let second_payload = harness.temp_dir.path().join("second.txt");
    fs::write(&first_payload, b"first change\n").expect("first payload");
    fs::write(&second_payload, b"second change\n").expect("second payload");

    let mut shapes_by_mode = Vec::new();
    for profile in ["embedded", "remote"] {
        assert_success(&harness.run(&["namespace", "create", "--profile", profile, "demo"]));
        assert_success(&harness.run(&["use", "--profile", profile, "demo"]));
        assert_success(&harness.run(&[
            "put",
            "--profile",
            profile,
            first_payload.to_str().expect("utf-8 path"),
            "/first.txt",
        ]));
        assert_success(&harness.run(&[
            "put",
            "--profile",
            profile,
            second_payload.to_str().expect("utf-8 path"),
            "/second.txt",
        ]));

        let changes = harness.run(&["--json", "changes", "--profile", profile]);
        assert_success(&changes);
        let changes_data = json_data(&changes);
        assert_eq!(changes_data["kind"], "changes");
        assert_eq!(changes_data["namespace_id"], "demo");
        assert_eq!(changes_data["after_seq"], 0);
        assert_eq!(changes_data["through_seq"], 2);
        assert!(changes_data["next_after_seq"].is_null());
        let listed = changes_data["changes"].as_array().expect("json array");
        assert_eq!(listed.len(), 2);
        assert_eq!(listed[0]["committed_seq"], 1);
        assert_eq!(listed[1]["committed_seq"], 2);
        assert!(listed[0]["commit_id"]
            .as_str()
            .expect("json string")
            .starts_with("c_"));
        assert_eq!(
            listed[0]["actor"],
            serde_json::json!({"kind":"service","id":"loonfs-cli"})
        );
        assert!(!listed[1]["events"]
            .as_array()
            .expect("json array")
            .is_empty());

        let paged = harness.run(&["--json", "changes", "--profile", profile, "--limit", "1"]);
        assert_success(&paged);
        let paged_data = json_data(&paged);
        assert_eq!(
            paged_data["changes"].as_array().expect("json array").len(),
            1
        );
        assert_eq!(paged_data["changes"][0]["committed_seq"], 1);
        assert_eq!(paged_data["next_after_seq"], 1);

        let resumed = harness.run(&["--json", "changes", "--profile", profile, "--after", "1"]);
        assert_success(&resumed);
        let resumed_data = json_data(&resumed);
        assert_eq!(resumed_data["after_seq"], 1);
        assert_eq!(
            resumed_data["changes"]
                .as_array()
                .expect("json array")
                .len(),
            1
        );
        assert_eq!(resumed_data["changes"][0]["committed_seq"], 2);

        let checkpoint = harness.run(&[
            "--json",
            "admin",
            "checkpoint",
            "--name",
            "nightly",
            "--profile",
            profile,
        ]);
        assert_success(&checkpoint);
        let checkpoint_data = json_data(&checkpoint);
        assert_eq!(checkpoint_data["kind"], "checkpoint_created");
        assert_eq!(checkpoint_data["namespace_id"], "demo");
        assert_eq!(checkpoint_data["checkpoint_seq"], 2);
        let checkpoint_id = checkpoint_data["checkpoint_id"]
            .as_str()
            .expect("json string")
            .to_owned();
        assert!(checkpoint_id.starts_with("chk_"));

        // A name is a label, not a key: the same one asked for twice mints a
        // second record, and the listing is how both ids are found again.
        let second_checkpoint = harness.run(&[
            "--json",
            "admin",
            "checkpoint",
            "--name",
            "nightly",
            "--profile",
            profile,
        ]);
        assert_success(&second_checkpoint);
        let second_checkpoint_id = json_data(&second_checkpoint)["checkpoint_id"]
            .as_str()
            .expect("json string")
            .to_owned();
        assert_ne!(second_checkpoint_id, checkpoint_id);

        let listed = harness.run(&["--json", "admin", "checkpoint-list", "--profile", profile]);
        assert_success(&listed);
        let listed_data = json_data(&listed);
        assert_eq!(listed_data["kind"], "checkpoints_listed");
        assert_eq!(listed_data["namespace_id"], "demo");
        let listed_ids = listed_data["checkpoints"]
            .as_array()
            .expect("json array")
            .iter()
            .map(|checkpoint| {
                assert_eq!(checkpoint["owner"]["kind"], "user");
                assert_eq!(checkpoint["owner"]["name"], "nightly");
                assert_eq!(checkpoint["checkpoint_seq"], 2);
                checkpoint["checkpoint_id"]
                    .as_str()
                    .expect("json string")
                    .to_owned()
            })
            .collect::<Vec<_>>();
        let mut expected_ids = vec![checkpoint_id.clone(), second_checkpoint_id.clone()];
        expected_ids.sort();
        assert_eq!(listed_ids, expected_ids);
        assert!(listed_data.get("next_cursor").is_none());

        // A page limit returns one page and a cursor that resumes at the next id.
        let first_page = harness.run(&[
            "--json",
            "admin",
            "checkpoint-list",
            "--profile",
            profile,
            "--limit",
            "1",
        ]);
        assert_success(&first_page);
        let first_page_data = json_data(&first_page);
        assert_eq!(
            first_page_data["checkpoints"]
                .as_array()
                .expect("json array")
                .len(),
            1
        );
        assert_eq!(
            first_page_data["checkpoints"][0]["checkpoint_id"],
            expected_ids[0].as_str()
        );
        let cursor = first_page_data["next_cursor"]
            .as_str()
            .expect("partial page cursor")
            .to_owned();

        let resumed_page = harness.run(&[
            "--json",
            "admin",
            "checkpoint-list",
            "--profile",
            profile,
            "--cursor",
            &cursor,
        ]);
        assert_success(&resumed_page);
        let resumed_page_data = json_data(&resumed_page);
        assert_eq!(
            resumed_page_data["checkpoints"][0]["checkpoint_id"],
            expected_ids[1].as_str()
        );
        assert!(resumed_page_data.get("next_cursor").is_none());

        let first_page_human = harness.run(&[
            "admin",
            "checkpoint-list",
            "--profile",
            profile,
            "--limit",
            "1",
        ]);
        assert_success(&first_page_human);
        assert!(stdout_string(&first_page_human).contains("next_cursor:"));

        // The human rendering is the same table in both modes, and it names
        // the id the release command takes.
        let listed_human = harness.run(&["admin", "checkpoint-list", "--profile", profile]);
        assert_success(&listed_human);
        let listed_text = stdout_string(&listed_human);
        assert!(listed_text.contains("CREATED\tEXPIRES\tSEQ\tOWNER\tCHECKPOINT"));
        assert!(listed_text.contains(&checkpoint_id));
        assert!(listed_text.contains("nightly"));

        // Releasing one leaves the other listed: a release is per record,
        // never per label.
        assert_success(&harness.run(&[
            "--json",
            "admin",
            "checkpoint-release",
            &second_checkpoint_id,
            "--profile",
            profile,
        ]));
        let after_release =
            harness.run(&["--json", "admin", "checkpoint-list", "--profile", profile]);
        assert_success(&after_release);
        let remaining = json_data(&after_release);
        let remaining = remaining["checkpoints"].as_array().expect("json array");
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0]["checkpoint_id"], checkpoint_id.as_str());

        // `admin flush` runs one metadata-upkeep pass at a threshold of one
        // segment, so it reports both halves and nothing else.
        let flush = harness.run(&["--json", "admin", "flush", "--profile", profile]);
        assert_success(&flush);
        let flush_data = json_data(&flush);
        assert_eq!(flush_data["kind"], "maintenance_stepped");
        assert_eq!(flush_data["namespace_id"], "demo");
        assert_eq!(flush_data["metadata"]["reorganize"]["kind"], "not_needed");
        assert!(flush_data["metadata"]["wal_flush"].is_object());
        assert!(flush_data.get("retention").is_none());
        assert!(flush_data.get("gc").is_none());

        let release = harness.run(&[
            "--json",
            "admin",
            "checkpoint-release",
            &checkpoint_id,
            "--profile",
            profile,
        ]);
        assert_success(&release);
        let release_data = json_data(&release);
        assert_eq!(release_data["kind"], "checkpoint_released");
        assert_eq!(release_data["checkpoint_id"], checkpoint_id.as_str());
        assert_eq!(release_data["was_active"], true);

        let release_again = harness.run(&[
            "--json",
            "admin",
            "checkpoint-release",
            &checkpoint_id,
            "--profile",
            profile,
        ]);
        assert_success(&release_again);
        assert_eq!(json_data(&release_again)["was_active"], false);

        let retention =
            harness.run(&["--json", "admin", "retention-advance", "--profile", profile]);
        assert_success(&retention);
        let retention_data = json_data(&retention);
        assert_eq!(retention_data["kind"], "maintenance_stepped");
        assert_eq!(retention_data["namespace_id"], "demo");
        assert_eq!(retention_data["retention"]["retention_floor_seq"], 2);
        // `retention-advance` names one action, so the report carries one.
        assert!(retention_data.get("metadata").is_none());

        // The checkpoint above already covers the head, so a step reports
        // not-needed identically in both modes.
        let step = harness.run(&["--json", "admin", "step", "--profile", profile]);
        assert_success(&step);
        let step_data = json_data(&step);
        assert_eq!(step_data["kind"], "maintenance_stepped");
        assert_eq!(step_data["namespace_id"], "demo");
        assert_eq!(step_data["metadata"]["wal_flush"]["kind"], "not_needed");
        assert_eq!(step_data["metadata"]["reorganize"]["kind"], "not_needed");
        // A step without `--retention` never advances the floor, and says
        // nothing about it: the floor is `status_before`'s to report.
        assert!(step_data.get("retention").is_none());
        assert_eq!(step_data["status_before"]["retention_floor_seq"], 2);
        assert_eq!(step_data["status_before"]["namespace_id"], "demo");
        assert!(step_data.get("gc").is_none());

        // A fresh namespace has nothing eligible to sweep.
        let gc = harness.run(&["--json", "admin", "gc", "--profile", profile]);
        assert_success(&gc);
        let gc_data = json_data(&gc);
        assert_eq!(gc_data["kind"], "garbage_collected");
        assert_eq!(gc_data["namespace_id"], "demo");
        assert_eq!(gc_data["deleted_wal_segments"], 0);
        assert_eq!(gc_data["deleted_manifests"], 0);
        assert_eq!(gc_data["degraded_retention"], false);
        assert!(gc_data.get("next_cursor").is_none());
        // Every retention reason is reported whether or not it happened, so
        // a consumer reads a field rather than probing for one, and the
        // breakdown accounts for exactly the total beside it.
        let retained = gc_data["retained"].as_object().expect("json object");
        let reason_total: u64 = retained
            .values()
            .map(|count| count.as_u64().expect("json number"))
            .sum();
        assert_eq!(
            reason_total,
            gc_data["retained_candidates"]
                .as_u64()
                .expect("json number")
        );
        assert!(retained.contains_key("checkpoint_not_releasable"));

        // A run that took one pass says nothing on the way: its summary on
        // standard output is the whole report.
        let quiet_gc = harness.run(&["admin", "gc", "--profile", profile]);
        assert_success(&quiet_gc);
        assert!(!stderr_string(&quiet_gc).contains("pass 1:"));

        // The budget covers marking too, so one object buys the head and
        // the root beside it and nothing else. That pass says it ran out
        // rather than reporting a clean sweep, and hands back no position
        // it never reached.
        let starved_gc = harness.run(&[
            "--json",
            "admin",
            "gc",
            "--max-objects",
            "1",
            "--profile",
            profile,
        ]);
        assert_success(&starved_gc);
        let starved_data = json_data(&starved_gc);
        assert_eq!(starved_data["budget_exhausted"], true);
        assert!(starved_data.get("next_cursor").is_none());

        // Supplying a budget with room for the roots and some candidates
        // requests exactly one pass and exposes the opaque cursor instead
        // of the CLI's default completion loop.
        let bounded_gc = harness.run(&[
            "--json",
            "admin",
            "gc",
            "--max-objects",
            "8",
            "--profile",
            profile,
        ]);
        assert_success(&bounded_gc);
        let bounded_data = json_data(&bounded_gc);
        let cursor = bounded_data["next_cursor"]
            .as_str()
            .filter(|cursor| !cursor.is_empty())
            .expect("bounded pass returns a cursor")
            .to_owned();

        // Resumption skips the candidates the first pass enumerated. A
        // restart over this unchanged keyspace would return the same token.
        let resumed_gc = harness.run(&[
            "--json",
            "admin",
            "gc",
            "--max-objects",
            "8",
            "--cursor",
            &cursor,
            "--profile",
            profile,
        ]);
        assert_success(&resumed_gc);
        assert_ne!(
            json_data(&resumed_gc)
                .get("next_cursor")
                .and_then(Value::as_str),
            Some(cursor.as_str())
        );

        // Admin failures surface the registry code in both modes.
        let missing = harness.run(&[
            "--json",
            "admin",
            "checkpoint",
            "--name",
            "nightly",
            "--profile",
            profile,
            "--namespace",
            "missing",
        ]);
        assert_failure(&missing);
        assert_eq!(json_error(&missing)["code"], "namespace_not_found");
        // Both modes name the namespace: the CLI mapper mirrors the server
        // handler's scoping.
        assert_eq!(
            json_error(&missing)["message"],
            "namespace `missing` does not exist"
        );

        // The store probe is store-scoped, so it is the one admin command
        // that names no namespace in either mode; both modes still answer
        // with the same report shape.
        let probe = harness.run(&["--json", "admin", "store-probe", "--profile", profile]);
        assert_success(&probe);
        let probe_data = json_data(&probe);
        assert_eq!(probe_data["kind"], "store_probed");
        assert_eq!(
            probe_data["checks"].as_array().expect("checks array").len(),
            14
        );

        shapes_by_mode.push((
            sorted_object_keys(&changes_data),
            sorted_object_keys(&checkpoint_data),
            sorted_object_keys(&retention_data),
            sorted_object_keys(&step_data),
            sorted_object_keys(&gc_data),
            sorted_object_keys(&probe_data),
        ));
    }

    assert_eq!(
        shapes_by_mode[0], shapes_by_mode[1],
        "embedded and remote --json payloads diverged in shape"
    );
}

struct Harness {
    temp_dir: TempDir,
    home_dir: PathBuf,
    config_path: PathBuf,
}

impl Harness {
    fn new() -> Self {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let home_dir = temp_dir.path().join("home");
        fs::create_dir_all(&home_dir).expect("create temp home");
        Self {
            config_path: home_dir.join(".loonfs").join("config.toml"),
            home_dir,
            temp_dir,
        }
    }

    fn run(&self, args: &[&str]) -> Output {
        self.command().args(args).output().expect("run loonfs")
    }

    /// Runs the CLI with `variables` in its environment, for the config
    /// resolution the environment takes part in.
    fn run_with_env<V: AsRef<std::ffi::OsStr>>(
        &self,
        variables: &[(&str, V)],
        args: &[&str],
    ) -> Output {
        let mut command = self.command();
        for (name, value) in variables {
            command.env(name, value);
        }
        command.args(args).output().expect("run loonfs")
    }

    /// Every invocation starts from the temp home with CLI selection
    /// environment cleared, so a developer's shell cannot affect a test.
    fn command(&self) -> Command {
        let mut command = Command::new(loon_binary_path());
        command
            .env("HOME", &self.home_dir)
            .env_remove("XDG_CONFIG_HOME")
            .env_remove("LOONFS_CONFIG")
            .env_remove("LOONFS_NAMESPACE")
            .env_remove("LOONFS_ACTOR_KIND")
            .env_remove("LOONFS_ACTOR_ID");
        command
    }

    /// Runs a command the CLI printed the way a user would: through a shell,
    /// which is what makes the quoting in it load-bearing. Only the `loonfs`
    /// the hint spells is swapped for the binary under test.
    fn replay_in_shell(&self, command: &str) -> Output {
        let arguments = command
            .strip_prefix("loonfs ")
            .expect("a printed command invokes loonfs");
        let binary = loon_binary_path();
        let script = format!("'{}' {arguments}", binary.display());
        Command::new("sh")
            .arg("-c")
            .arg(&script)
            .env("HOME", &self.home_dir)
            .env_remove("XDG_CONFIG_HOME")
            .env_remove("LOONFS_CONFIG")
            .env_remove("LOONFS_NAMESPACE")
            .env_remove("LOONFS_ACTOR_KIND")
            .env_remove("LOONFS_ACTOR_ID")
            .output()
            .expect("replay the printed command")
    }

    /// Runs the CLI with a payload on standard input, which is the one
    /// source whose length is not knowable before it is read.
    fn run_with_stdin(&self, args: &[&str], stdin: &[u8]) -> Output {
        let mut child = self
            .command()
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn loonfs");
        child
            .stdin
            .take()
            .expect("piped stdin")
            .write_all(stdin)
            .expect("write stdin");
        child.wait_with_output().expect("run loonfs")
    }

    fn store_root(&self, name: &str) -> PathBuf {
        self.temp_dir.path().join(format!("{name}-store"))
    }

    fn add_embedded_profile(&self, name: &str) {
        let output = self.run(&[
            "--json",
            "profile",
            "create",
            name,
            "--mode",
            "embedded",
            "--store-kind",
            "local-fs",
            "--root",
            self.store_root(name).to_str().expect("utf-8 path"),
        ]);
        assert_success(&output);
    }

    fn write_cli_config(&self, contents: impl AsRef<[u8]>) {
        fs::create_dir_all(self.config_path.parent().expect("config dir"))
            .expect("create config dir");
        fs::write(&self.config_path, contents).expect("write cli config");
    }

    fn write_remote_listing_config(&self, server_url: &str) {
        self.write_cli_config(format!(
            r#"config_version = 1
default_profile = "remote"

[profiles.remote]
mode = "remote"
server_url = "{server_url}"
default_namespace = "demo"
auth_token = "test-token"
"#,
        ));
    }

    fn write_server_config(&self, name: &str, key_prefix: &str) -> PathBuf {
        self.write_server_config_with(name, key_prefix, "")
    }

    /// A server config with `extra` appended, for tests that need a table
    /// the default deployment leaves out.
    fn write_server_config_with(&self, name: &str, key_prefix: &str, extra: &str) -> PathBuf {
        let bind = format!("127.0.0.1:{}", available_port());
        let path = self
            .temp_dir
            .path()
            .join(format!("{name}.loonfs-server.toml"));
        let store_root = self.store_root(name);
        let contents = format!(
            r#"
bind = "{bind}"
auth_token = "test-token"
content_token_secret = "test-content-token-secret"
writer_id = "{name}"

[store]
kind = "local-fs"
root = "{}"
key_prefix = "{key_prefix}"
{extra}"#,
            store_root.display()
        );
        fs::write(&path, contents).expect("write server config");
        path
    }

    fn start_external_server(&self, server_config_path: PathBuf) -> ExternalServer {
        for _ in 0..5 {
            let child = Command::new(loonfs_server_binary_path())
                .arg("--config")
                .arg(&server_config_path)
                .spawn()
                .expect("spawn loonfs-server");
            let server_url = server_url_from_config(&server_config_path);
            if wait_for_readiness(&server_url) {
                return ExternalServer { child, server_url };
            }

            let mut child = child;
            let _ = child.kill();
            let _ = child.wait();
            rewrite_server_bind(&server_config_path, available_port());
        }

        unreachable!(
            "timed out waiting for external server from {}",
            server_config_path.display()
        );
    }
}

fn json_response_server(responses: Vec<Value>) -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind JSON server");
    let address = listener.local_addr().expect("JSON server address");
    let server = thread::spawn(move || {
        for response in responses {
            let (mut stream, _) = listener.accept().expect("accept JSON request");
            let mut request = Vec::new();
            let mut chunk = [0_u8; 1024];
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                let read = stream.read(&mut chunk).expect("read JSON request");
                assert!(read > 0, "JSON request ended before its headers");
                request.extend_from_slice(&chunk[..read]);
            }

            let body = serde_json::to_vec(&response).expect("encode JSON response");
            write!(
                stream,
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                body.len()
            )
            .expect("write JSON response headers");
            stream.write_all(&body).expect("write JSON response");
        }
    });
    (format!("http://{address}"), server)
}

struct ExternalServer {
    child: Child,
    server_url: String,
}

impl Drop for ExternalServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn loon_binary_path() -> PathBuf {
    if let Some(path) = env::var_os("CARGO_BIN_EXE_loonfs") {
        return PathBuf::from(path);
    }

    let current_exe = env::current_exe().expect("current test binary path");
    let debug_dir = current_exe
        .parent()
        .and_then(|path| path.parent())
        .expect("target debug dir");
    let candidate = debug_dir.join(if cfg!(windows) {
        "loonfs.exe"
    } else {
        "loonfs"
    });
    assert!(
        candidate.exists(),
        "expected loonfs binary at {}",
        candidate.display()
    );
    candidate
}

fn loonfs_server_binary_path() -> PathBuf {
    if let Some(path) = env::var_os("CARGO_BIN_EXE_loonfs-server") {
        return PathBuf::from(path);
    }

    let current_exe = env::current_exe().expect("current test binary path");
    let debug_dir = current_exe
        .parent()
        .and_then(|path| path.parent())
        .expect("target debug dir");
    let candidate = debug_dir.join(if cfg!(windows) {
        "loonfs-server.exe"
    } else {
        "loonfs-server"
    });
    assert!(
        candidate.exists(),
        "expected loonfs-server binary at {}",
        candidate.display()
    );
    candidate
}

fn server_url_from_config(path: &Path) -> String {
    let config = fs::read_to_string(path).expect("read server config");
    let bind = config
        .lines()
        .find_map(|line| line.trim().strip_prefix("bind = "))
        .expect("bind line")
        .trim_matches('"')
        .to_owned();
    format!("http://{bind}")
}

// Polls a spawned server binary for readiness; a wall-clock deadline is the
// point, so the timer methods the workspace otherwise disallows are scoped
// to this helper.
#[allow(clippy::disallowed_methods)]
fn wait_for_readiness(server_url: &str) -> bool {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if ureq::get(&format!("{server_url}/health")).call().is_ok() {
            return true;
        }
        thread::sleep(Duration::from_millis(100));
    }
    false
}

fn rewrite_server_bind(path: &Path, port: u16) {
    let config = fs::read_to_string(path).expect("read server config for bind rewrite");
    let bind = format!("127.0.0.1:{port}");
    let rewritten = config
        .lines()
        .map(|line| {
            if line.trim().starts_with("bind = ") {
                format!("bind = \"{bind}\"")
            } else {
                line.to_owned()
            }
        })
        .collect::<Vec<_>>()
        .join("\n");
    fs::write(path, rewritten).expect("rewrite server bind");
}

fn available_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind port")
        .local_addr()
        .expect("local addr")
        .port()
}

fn assert_success(output: &Output) {
    assert!(
        output.status.success(),
        "expected success, got {:?}\nstdout:\n{}\nstderr:\n{}",
        output.status.code(),
        stdout_string(output),
        stderr_string(output)
    );
}

fn assert_failure(output: &Output) {
    assert!(
        !output.status.success(),
        "expected failure, got success\nstdout:\n{}\nstderr:\n{}",
        stdout_string(output),
        stderr_string(output)
    );
}

fn stdout_string(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

/// The command a `rm` hint spells, taken from between its backticks.
fn hinted_recovery_command(output: &Output) -> String {
    let text = stdout_string(output);
    let hint = text
        .split_once("recover with `")
        .and_then(|(_, rest)| rest.split_once('`'))
        .map(|(command, _)| command.to_owned());
    assert!(
        hint.is_some(),
        "expected a backtick-delimited recovery hint, got:\n{text}"
    );
    hint.expect("checked just above")
}

/// The `RECOVER` cell of the one trash row naming `display_name`.
fn trash_recovery_command(output: &Output, display_name: &str) -> String {
    let table = stdout_string(output);
    let cell = table
        .lines()
        .find(|line| line.split('\t').nth(2) == Some(display_name))
        .and_then(|row| row.split('\t').nth(5))
        .map(ToOwned::to_owned);
    assert!(
        cell.is_some(),
        "expected a trash row for `{display_name}`, got:\n{table}"
    );
    cell.expect("checked just above")
}

fn stderr_string(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn parse_json(bytes: &[u8]) -> Value {
    serde_json::from_slice(bytes).expect("parse json")
}

fn json_data(output: &Output) -> Value {
    parse_json(&output.stdout)["data"].clone()
}

/// The failure envelope, which is the last document on standard error.
///
/// Under `--json` standard error carries a stream of JSON documents, not
/// one: a transfer reports progress there while it runs, and the envelope
/// comes last.
fn json_error(output: &Output) -> Value {
    json_stderr_documents(output)
        .pop()
        .expect("a failure envelope on stderr")["error"]
        .clone()
}

/// Every JSON document standard error carried, in order.
fn json_stderr_documents(output: &Output) -> Vec<Value> {
    serde_json::Deserializer::from_slice(&output.stderr)
        .into_iter::<Value>()
        .collect::<Result<Vec<_>, _>>()
        .expect("parse the json documents on stderr")
}

/// The progress events of one run, in the order they were reported.
fn json_progress_events(output: &Output) -> Vec<Value> {
    json_stderr_documents(output)
        .into_iter()
        .filter(|document| document.get("error").is_none() && document.get("data").is_none())
        .collect()
}

fn sorted_object_keys(value: &Value) -> Vec<String> {
    let mut keys: Vec<String> = value
        .as_object()
        .expect("json object")
        .keys()
        .cloned()
        .collect();
    keys.sort();
    keys
}
