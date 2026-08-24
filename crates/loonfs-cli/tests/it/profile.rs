//! Profile creation, config resolution, and target selection.

use super::common::*;

#[test]
fn profile_create_list_show_delete_work() {
    let harness = Harness::new();

    let add_embedded = harness.run(&[
        "--json",
        "profile",
        "create",
        "local",
        "default",
        "--root",
        harness.store_root("default").to_str().expect("utf-8 path"),
    ]);
    assert_success(&add_embedded);
    assert_eq!(json_data(&add_embedded)["mode"], "embedded");

    let add_remote = harness.run(&[
        "--json",
        "profile",
        "create",
        "remote",
        "prod",
        "--server-url",
        "http://127.0.0.1:9400",
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
        .map(|change| change["committed_by"].clone())
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

#[test]
fn profile_create_runs_through_an_override_while_the_default_config_is_unreadable() {
    let harness = Harness::new();
    harness.write_cli_config("config_version = 1\nunknown_knob = true\n");
    let broken = fs::read_to_string(&harness.config_path).expect("read broken config");

    let flagged_path = harness.temp_dir.path().join("recovery").join("config.toml");
    let flagged = flagged_path.to_str().expect("utf-8 path");
    let create = harness.run(&[
        "--json",
        "--config",
        flagged,
        "profile",
        "create",
        "local",
        "rescue",
        "--root",
        harness.store_root("rescue").to_str().expect("utf-8 path"),
    ]);
    assert_success(&create);
    assert_eq!(json_data(&create)["mode"], "embedded");
    assert!(
        flagged_path.exists(),
        "profile create creates the directories it needs"
    );

    let list = harness.run(&["--json", "--config", flagged, "profile", "list"]);
    assert_success(&list);
    assert_eq!(json_data(&list)["profiles"][0]["name"], "rescue");

    // The environment is the same way out.
    let env_path = harness.temp_dir.path().join("by-env").join("config.toml");
    let env_create = harness.run_with_env(
        &[("LOONFS_CONFIG", &env_path)],
        &[
            "--json",
            "profile",
            "create",
            "local",
            "byenv",
            "--root",
            harness.store_root("byenv").to_str().expect("utf-8 path"),
        ],
    );
    assert_success(&env_create);
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
        "remote",
        "dead",
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
fn profile_create_embedded_and_current_reports_namespace_unset() {
    let harness = Harness::new();

    let create = harness.run(&[
        "--json",
        "profile",
        "create",
        "local",
        "mystore",
        "--root",
        harness.store_root("mystore").to_str().expect("utf-8 path"),
    ]);
    assert_success(&create);
    assert_eq!(json_data(&create)["mode"], "embedded");

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
fn init_is_interactive_only() {
    let harness = Harness::new();

    let result = harness.run(&["--json", "init"]);

    assert_failure(&result);
    let error = json_error(&result);
    assert_eq!(error["code"], "non_interactive_input_required");
    let message = error["message"].as_str().expect("json string");
    assert!(message.contains("interactive"));
    assert!(message.contains("profile create <provider>"));
    assert!(!harness.config_path.exists());
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
    assert_eq!(json_data(&use_profile)["name"], "beta");

    let show_after = harness.run(&["--json", "profile", "show"]);
    assert_success(&show_after);
    assert_eq!(json_data(&show_after)["mode"], "embedded");
}

#[test]
fn profile_update_with_only_service_account_key_path_applies() {
    let harness = Harness::new();
    let create = harness.run(&[
        "--json",
        "profile",
        "create",
        "gcs",
        "gcp",
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
        json_data(&show)["store"]["credentials"]["path"],
        "/new/service-account.json"
    );
}

#[test]
fn profile_resolution_is_flag_then_environment_then_config_default() {
    let harness = Harness::new();
    harness.add_embedded_profile("configured");
    harness.add_embedded_profile("environment");
    harness.add_embedded_profile("flagged");

    let from_environment =
        harness.run_with_env(&[("LOONFS_PROFILE", "environment")], &["--json", "current"]);
    assert_success(&from_environment);
    assert_eq!(json_data(&from_environment)["profile"], "environment");

    let from_flag = harness.run_with_env(
        &[("LOONFS_PROFILE", "environment")],
        &["--json", "current", "--profile", "flagged"],
    );
    assert_success(&from_flag);
    assert_eq!(json_data(&from_flag)["profile"], "flagged");

    let from_default = harness.run_with_env(&[("LOONFS_PROFILE", "")], &["--json", "current"]);
    assert_success(&from_default);
    assert_eq!(json_data(&from_default)["profile"], "configured");
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
        "profile",
        "create",
        "local",
        "default_profile",
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
        "local",
        "config_version",
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
            "gcs",
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
            "local",
            "--root",
            harness.store_root("local").to_str().expect("utf-8 path"),
        ],
    );
    assert_success(&local_fs);

    // Creating an ambient profile must not copy these values into the config.
    let s3 = harness.run_with_env(
        ambient,
        &[
            "--json",
            "profile",
            "create",
            "s3",
            "s3",
            "--bucket",
            "bucket",
            "--region",
            "us-east-1",
        ],
    );
    assert_success(&s3);
    assert_eq!(json_data(&s3)["store"]["credentials"]["kind"], "ambient");
    let persisted = fs::read_to_string(&harness.config_path).expect("read config");
    assert!(!persisted.contains("ambient-access"), "{persisted}");
    assert!(!persisted.contains("ambient-secret"), "{persisted}");
    assert!(!persisted.contains("ambient-session"), "{persisted}");
    assert!(!persisted.contains("ambient-token"), "{persisted}");

    // Provider-specific commands reject flags for other providers during
    // parsing.
    let typed = harness.run_with_env(
        ambient,
        &[
            "--json",
            "profile",
            "create",
            "gcs",
            "gcs-typed",
            "--bucket",
            "bucket",
            "--service-account-key-path",
            "/tmp/service-account.json",
            "--access-key-id",
            "typed-access",
        ],
    );
    assert_failure(&typed);
    let error = stderr_string(&typed);
    assert!(error.contains("unexpected argument '--access-key-id'"));
}

#[test]
fn remote_profile_creation_does_not_capture_the_environment_token() {
    let harness = Harness::new();
    let create = harness.run_with_env(
        &[("LOONFS_AUTH_TOKEN", "ambient-auth-token")],
        &[
            "--json",
            "profile",
            "create",
            "remote",
            "remote",
            "--server-url",
            "https://loonfs.example.com",
        ],
    );
    assert_success(&create);
    assert!(json_data(&create)["auth_token"].is_null());

    let persisted = fs::read_to_string(&harness.config_path).expect("read config");
    assert!(!persisted.contains("ambient-auth-token"), "{persisted}");
    assert!(!persisted.contains("auth_token"), "{persisted}");
}

#[test]
fn profile_update_switches_credential_source_atomically() {
    let harness = Harness::new();
    let create = harness.run(&[
        "--json",
        "profile",
        "create",
        "s3",
        "s3",
        "--bucket",
        "bucket",
        "--region",
        "us-east-1",
    ]);
    assert_success(&create);
    let before = fs::read_to_string(&harness.config_path).expect("read ambient config");

    let failed = harness.run(&[
        "--json",
        "--no-input",
        "profile",
        "update",
        "s3",
        "--credential-source",
        "static",
        "--access-key-id",
        "partial-access",
    ]);
    assert_failure(&failed);
    assert!(json_error(&failed)["message"]
        .as_str()
        .expect("message")
        .contains("secret-access-key"));
    assert_eq!(
        fs::read_to_string(&harness.config_path).expect("read unchanged config"),
        before
    );

    let switched = harness.run(&[
        "--json",
        "--no-input",
        "profile",
        "update",
        "s3",
        "--credential-source",
        "static",
        "--access-key-id",
        "static-access",
        "--secret-access-key",
        "static-secret",
        "--session-token",
        "static-session",
    ]);
    assert_success(&switched);
    let credentials = &json_data(&switched)["store"]["credentials"];
    assert_eq!(credentials["kind"], "static");
    assert_eq!(credentials["access_key_id"], "<redacted>");
    assert_eq!(credentials["secret_access_key"], "<redacted>");
    assert_eq!(credentials["session_token"], "<redacted>");

    let ambient = harness.run(&[
        "--json",
        "--no-input",
        "profile",
        "update",
        "s3",
        "--credential-source",
        "ambient",
    ]);
    assert_success(&ambient);
    assert_eq!(
        json_data(&ambient)["store"]["credentials"]["kind"],
        "ambient"
    );
    let persisted = fs::read_to_string(&harness.config_path).expect("read ambient config");
    assert!(!persisted.contains("static-access"), "{persisted}");
    assert!(!persisted.contains("static-secret"), "{persisted}");
    assert!(!persisted.contains("static-session"), "{persisted}");
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

    let init = harness.run(&["--json", "init"]);
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
fn blank_default_profile_in_config_is_rejected() {
    let harness = Harness::new();

    for value in ["", "   "] {
        harness.write_cli_config(format!(
            r#"
config_version = 1
default_profile = "{value}"
"#
        ));

        let list = harness.run(&["--json", "profile", "list"]);
        assert_failure(&list);
        let error = json_error(&list);
        assert_eq!(error["code"], "invalid_config");
        assert!(error["message"]
            .as_str()
            .expect("json string")
            .contains("default_profile"));
    }
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
fn invalid_remote_urls_are_rejected() {
    let harness = Harness::new();

    let missing_host_http = harness.run(&[
        "--json",
        "profile",
        "create",
        "remote",
        "default",
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
        "remote",
        "default",
        "--server-url",
        "https://",
    ]);
    assert_failure(&missing_host_https);
    assert_eq!(json_error(&missing_host_https)["code"], "invalid_config");
}

#[test]
fn profile_create_rejects_token_over_non_loopback_http_before_writing() {
    let harness = Harness::new();
    let create = harness.run(&[
        "--json",
        "profile",
        "create",
        "remote",
        "default",
        "--server-url",
        "http://example.internal",
        "--auth-token",
        "test-token",
    ]);

    assert_failure(&create);
    let error = json_error(&create);
    assert_eq!(error["code"], "invalid_config");
    assert!(error["message"]
        .as_str()
        .expect("json string")
        .contains("bearer tokens require https except for loopback http URLs"));
    assert!(
        !harness.config_path.exists(),
        "unsafe profile must be rejected before creating the config file"
    );

    // A token from the environment is the token every request would carry,
    // so creation judges the profile the same way.
    let ambient = harness.run_with_env(
        &[("LOONFS_AUTH_TOKEN", "ambient-auth-token")],
        &[
            "--json",
            "profile",
            "create",
            "remote",
            "default",
            "--server-url",
            "http://example.internal",
        ],
    );
    assert_failure(&ambient);
    let error = json_error(&ambient);
    assert_eq!(error["code"], "invalid_config");
    assert!(
        error["message"]
            .as_str()
            .expect("json string")
            .contains("bearer tokens require https except for loopback http URLs"),
        "{error}"
    );
    assert!(
        !harness.config_path.exists(),
        "a profile no request could use must not be written"
    );
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
        "remote",
        "default",
        "--server-url",
        &remote_server.server_url,
        "--auth-token",
        "test-token",
    ]);
    assert_success(&add_remote);

    let create = harness.run(&["--json", "namespace", "create", "demo"]);
    assert_success(&create);
    assert_eq!(
        json_data(&create),
        serde_json::json!({
            "kind": "namespace_status",
            "namespace_id": "demo",
            "head_seq": 0,
            "retention_floor_seq": 0
        })
    );
    let fork = harness.run(&["--json", "namespace", "fork", "demo", "clone"]);
    assert_success(&fork);
    assert_eq!(
        json_data(&fork),
        serde_json::json!({
            "kind": "namespace_status",
            "namespace_id": "clone",
            "head_seq": 0,
            "retention_floor_seq": 0
        })
    );

    let use_namespace = harness.run(&["--json", "use", "demo"]);
    assert_success(&use_namespace);

    let use_clone = harness.run(&["--json", "use", "clone"]);
    assert_success(&use_clone);
    assert_eq!(json_data(&use_clone)["namespace"], "clone");
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
fn namespace_show_reads_positional_and_default_namespaces() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    assert_success(&harness.run(&["namespace", "create", "demo"]));

    let positional = harness.run(&["--json", "namespace", "show", "demo"]);
    assert_success(&positional);
    assert_eq!(json_data(&positional)["kind"], "namespace_status");
    assert_eq!(json_data(&positional)["namespace_id"], "demo");

    assert_success(&harness.run(&["use", "demo"]));
    let selected = harness.run(&["--json", "namespace", "show"]);
    assert_success(&selected);
    assert_eq!(json_data(&selected), json_data(&positional));
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
        "remote",
        "default",
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
