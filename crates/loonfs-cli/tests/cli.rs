#![allow(clippy::unwrap_used, clippy::panic, clippy::disallowed_methods)]
// CLI integration tests use concise JSON/path assertions and explicit server polling.

use serde_json::Value;
use std::env;
use std::fs;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;

#[test]
fn profile_create_list_show_remove_work() {
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
        harness.store_root("default").to_str().unwrap(),
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
    let profiles = list_data["profiles"].as_array().unwrap();
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

    let remove_default = harness.run(&["--json", "profile", "remove", "default"]);
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
        upload_path.to_str().unwrap(),
        "/docs/hello.txt",
    ]);
    assert_success(&put);
    assert_eq!(json_data(&put)["target"], "demo:/docs/hello.txt");

    let put_conflict = harness.run(&[
        "--json",
        "put",
        upload_path.to_str().unwrap(),
        "/docs/hello.txt",
    ]);
    assert_failure(&put_conflict);
    assert_eq!(json_error(&put_conflict)["code"], "path_conflict");

    let put_force = harness.run(&[
        "--json",
        "put",
        update_path.to_str().unwrap(),
        "/docs/hello.txt",
        "--force",
    ]);
    assert_success(&put_force);

    let revisions = harness.run(&["--json", "revisions", "/docs/hello.txt"]);
    assert_success(&revisions);
    assert_eq!(
        json_data(&revisions)["revisions"].as_array().unwrap().len(),
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
        download_path.to_str().unwrap(),
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
fn embedded_profile_namespace_fork_reads_shared_content_and_diverges() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");

    let upload_path = harness.temp_dir.path().join("upload.txt");
    let clone_upload_path = harness.temp_dir.path().join("clone-upload.txt");
    fs::write(&upload_path, b"base from cli\n").expect("upload payload");
    fs::write(&clone_upload_path, b"clone from cli\n").expect("clone upload payload");

    assert_success(&harness.run(&["namespace", "create", "demo"]));
    assert_success(&harness.run(&["use", "demo"]));
    assert_success(&harness.run(&["put", upload_path.to_str().unwrap(), "/docs/shared.txt"]));

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
        clone_upload_path.to_str().unwrap(),
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
        harness.store_root("mystore").to_str().unwrap(),
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
fn legacy_local_mode_is_rejected() {
    let harness = Harness::new();

    let result = harness.run(&[
        "--json",
        "profile",
        "create",
        "default",
        "--mode",
        "local",
        "--store-kind",
        "local-fs",
        "--root",
        harness.store_root("default").to_str().unwrap(),
    ]);

    assert_failure(&result);
    let error = json_error(&result);
    assert_eq!(error["code"], "invalid_input");
    let message = error["message"].as_str().unwrap();
    assert!(message.contains("expected embedded or remote"));
}

#[test]
fn removing_last_profile_leaves_empty_config() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");

    let remove = harness.run(&["--json", "--no-input", "profile", "remove", "default"]);
    assert_success(&remove);

    let list = harness.run(&["--json", "profile", "list"]);
    assert_success(&list);
    let data = json_data(&list);
    assert!(data["default_profile"].is_null());
    assert_eq!(data["profiles"].as_array().unwrap().len(), 0);

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

    let remove = harness.run(&["--json", "--no-input", "profile", "remove", "alpha"]);
    assert_success(&remove);

    let list = harness.run(&["--json", "profile", "list"]);
    assert_success(&list);
    let data = json_data(&list);
    assert!(data["default_profile"].is_null());
    assert_eq!(data["profiles"].as_array().unwrap().len(), 1);

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
        new_root.to_str().unwrap(),
    ]);
    assert_success(&update);

    let show = harness.run(&["--json", "profile", "show", "default"]);
    assert_success(&show);
    let store = &json_data(&show)["store"];
    assert_eq!(store["root"], new_root.to_str().unwrap());
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
        harness.store_root("default_profile").to_str().unwrap(),
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
        harness.store_root("config_version").to_str().unwrap(),
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
        harness.store_root("mystore").to_str().unwrap(),
    ]);
    assert_failure(&init);
    let error = json_error(&init);
    assert_eq!(error["code"], "config_already_exists");
    let message = error["message"].as_str().unwrap();
    assert!(message.contains("loon profile create"));
    assert!(message.contains("loon profile update"));
    assert!(message.contains("loon profile use"));
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
    assert_eq!(error["code"], "invalid_request");
    assert!(error["message"]
        .as_str()
        .unwrap()
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
    assert_eq!(error["code"], "invalid_request");
    assert!(error["message"]
        .as_str()
        .unwrap()
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
    assert_eq!(error["code"], "invalid_request");
    assert!(error["message"]
        .as_str()
        .unwrap()
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
    assert_eq!(error["code"], "invalid_request");
    assert!(error["message"]
        .as_str()
        .unwrap()
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
        .unwrap()
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
    assert_eq!(json_error(&missing_host_http)["code"], "invalid_request");
    assert!(json_error(&missing_host_http)["message"]
        .as_str()
        .unwrap()
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
    assert_eq!(json_error(&missing_host_https)["code"], "invalid_request");
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
fn filesystem_requires_default_namespace_when_omitted() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");

    let output = harness.run(&["--json", "ls", "/"]);
    assert_failure(&output);
    let error = json_error(&output);
    assert_eq!(error["code"], "no_default_namespace");
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
fn legacy_commands_are_rejected() {
    let harness = Harness::new();

    let add = harness.run(&["profile", "add"]);
    assert_failure(&add);
    assert!(stderr_string(&add).contains("unrecognized"));

    let filesystem = harness.run(&["filesystem", "ls"]);
    assert_failure(&filesystem);
    assert!(stderr_string(&filesystem).contains("unrecognized"));
}

#[test]
fn help_omits_legacy_commands_and_config_flag() {
    let harness = Harness::new();
    let output = Command::new(loon_binary_path())
        .env("HOME", &harness.home_dir)
        .arg("--help")
        .output()
        .expect("run help");
    assert_success(&output);
    let stdout = stdout_string(&output);
    assert!(!stdout.contains("--config"));
    assert!(!stdout.contains("filesystem"));
    assert!(stdout.contains("current"));
    assert!(stdout.contains("use"));
}

/// Pins the capability registry to the CLI surface: every profile and every
/// feature key advertised by the embedded runtime must map to a CLI command
/// path that exercises it, so no advertised capability is unreachable from
/// this surface.
///
/// When `Fs::capabilities()` (crates/loonfs/src/fs.rs) grows a profile or a
/// feature key, this test fails until the tables below either name the CLI
/// command path that covers the new capability, or record a deliberately
/// deferred CLI gap with a comment (none today).
#[test]
fn every_advertised_capability_maps_to_a_cli_command_path() {
    // Advertised profile -> the CLI command paths exercising that plane.
    const PROFILE_COMMAND_PATHS: &[(&str, &[&[&str]])] = &[
        (
            "core/v0",
            &[
                &["namespace", "create"],
                &["namespace", "delete"],
                &["namespace", "fork"],
                &["use"],
                &["ls"],
                &["stat"],
                &["cat"],
                &["get"],
                &["put"],
                &["mkdir"],
                &["rm"],
                &["mv"],
                &["cp"],
                &["revisions"],
                &["restore"],
                &["changes"],
            ],
        ),
        (
            "admin/v0",
            &[
                &["admin", "checkpoint"],
                &["admin", "retention-advance"],
                &["admin", "tick"],
                &["admin", "gc"],
            ],
        ),
    ];
    // Advertised feature key -> the CLI command path exercising it. Keys are
    // listed whether the embedded build advertises them `true` or `false`;
    // gating is the backend's job, reachability is this surface's job.
    const FEATURE_COMMAND_PATHS: &[(&str, &[&str])] = &[
        ("core.namespaces.create", &["namespace", "create"]),
        ("core.namespaces.delete", &["namespace", "delete"]),
        ("core.namespaces.fork", &["namespace", "fork"]),
        // Direct-put is a transfer mode negotiated inside the same upload
        // staging flow `put` drives; it needs no separate verb.
        ("core.uploads.direct_put", &["put"]),
    ];

    let harness = Harness::new();
    let document = embedded_capability_document();

    for profile in &document.profiles {
        let (_, command_paths) = PROFILE_COMMAND_PATHS
            .iter()
            .find(|(advertised, _)| *advertised == profile.as_str())
            .unwrap_or_else(|| {
                panic!(
                    "capability profile `{profile}` has no CLI command mapping; \
                     add its command paths to PROFILE_COMMAND_PATHS"
                )
            });
        for command_path in *command_paths {
            assert_cli_command_path_exists(&harness, command_path);
        }
    }

    for feature in document.features.keys() {
        let (_, command_path) = FEATURE_COMMAND_PATHS
            .iter()
            .find(|(advertised, _)| *advertised == feature.as_str())
            .unwrap_or_else(|| {
                panic!(
                    "capability feature `{feature}` has no CLI command mapping; \
                     add its command path to FEATURE_COMMAND_PATHS"
                )
            });
        assert_cli_command_path_exists(&harness, command_path);
    }
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
            first_payload.to_str().unwrap(),
            "/first.txt",
        ]));
        assert_success(&harness.run(&[
            "put",
            "--profile",
            profile,
            second_payload.to_str().unwrap(),
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
        let listed = changes_data["changes"].as_array().unwrap();
        assert_eq!(listed.len(), 2);
        assert_eq!(listed[0]["seq"], 1);
        assert_eq!(listed[1]["seq"], 2);
        assert!(listed[0]["commit_id"].as_str().unwrap().starts_with("c_"));
        assert!(!listed[1]["deltas"].as_array().unwrap().is_empty());

        let paged = harness.run(&["--json", "changes", "--profile", profile, "--limit", "1"]);
        assert_success(&paged);
        let paged_data = json_data(&paged);
        assert_eq!(paged_data["changes"].as_array().unwrap().len(), 1);
        assert_eq!(paged_data["changes"][0]["seq"], 1);
        assert_eq!(paged_data["next_after_seq"], 1);

        let resumed = harness.run(&["--json", "changes", "--profile", profile, "--after", "1"]);
        assert_success(&resumed);
        let resumed_data = json_data(&resumed);
        assert_eq!(resumed_data["after_seq"], 1);
        assert_eq!(resumed_data["changes"].as_array().unwrap().len(), 1);
        assert_eq!(resumed_data["changes"][0]["seq"], 2);

        let checkpoint = harness.run(&["--json", "admin", "checkpoint", "--profile", profile]);
        assert_success(&checkpoint);
        let checkpoint_data = json_data(&checkpoint);
        assert_eq!(checkpoint_data["kind"], "checkpoint_created");
        assert_eq!(checkpoint_data["namespace_id"], "demo");
        assert_eq!(checkpoint_data["checkpoint_seq"], 2);
        assert!(checkpoint_data["checkpoint_id"]
            .as_str()
            .unwrap()
            .starts_with("chk_"));

        let retention =
            harness.run(&["--json", "admin", "retention-advance", "--profile", profile]);
        assert_success(&retention);
        let retention_data = json_data(&retention);
        assert_eq!(retention_data["kind"], "retention_advanced");
        assert_eq!(retention_data["namespace_id"], "demo");
        assert_eq!(retention_data["retention_floor_seq"], 2);

        // The checkpoint above already covers the head, so a tick reports
        // not-needed identically in both modes.
        let tick = harness.run(&["--json", "admin", "tick", "--profile", profile]);
        assert_success(&tick);
        let tick_data = json_data(&tick);
        assert_eq!(tick_data["kind"], "maintenance_ticked");
        assert_eq!(tick_data["namespace_id"], "demo");
        assert_eq!(tick_data["outcome"]["kind"], "not_needed");
        assert_eq!(tick_data["status_before"]["namespace_id"], "demo");
        assert!(tick_data.get("gc").is_none());

        // A fresh namespace has nothing eligible to sweep.
        let gc = harness.run(&["--json", "admin", "gc", "--profile", profile]);
        assert_success(&gc);
        let gc_data = json_data(&gc);
        assert_eq!(gc_data["kind"], "garbage_collected");
        assert_eq!(gc_data["namespace_id"], "demo");
        assert_eq!(gc_data["deleted_wal_segments"], 0);
        assert_eq!(gc_data["deleted_manifests"], 0);
        assert_eq!(gc_data["degraded_retention"], false);

        // Admin failures surface the registry code in both modes.
        let missing = harness.run(&[
            "--json",
            "admin",
            "checkpoint",
            "--profile",
            profile,
            "--namespace",
            "missing",
        ]);
        assert_failure(&missing);
        assert_eq!(json_error(&missing)["code"], "namespace_not_found");

        shapes_by_mode.push((
            sorted_object_keys(&changes_data),
            sorted_object_keys(&checkpoint_data),
            sorted_object_keys(&retention_data),
            sorted_object_keys(&tick_data),
            sorted_object_keys(&gc_data),
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
        Command::new(loon_binary_path())
            .env("HOME", &self.home_dir)
            .args(args)
            .output()
            .expect("run loon")
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
            self.store_root(name).to_str().unwrap(),
        ]);
        assert_success(&output);
    }

    fn write_cli_config(&self, contents: impl AsRef<[u8]>) {
        fs::create_dir_all(self.config_path.parent().expect("config dir"))
            .expect("create config dir");
        fs::write(&self.config_path, contents).expect("write cli config");
    }

    fn write_server_config(&self, name: &str, key_prefix: &str) -> PathBuf {
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
writer_version = "{name}/0.1.0"

[store]
kind = "local-fs"
root = "{}"
key_prefix = "{key_prefix}"
"#,
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
            if wait_for_health_ready(&server_url) {
                return ExternalServer { child, server_url };
            }

            let mut child = child;
            let _ = child.kill();
            let _ = child.wait();
            rewrite_server_bind(&server_config_path, available_port());
        }

        panic!(
            "timed out waiting for external server from {}",
            server_config_path.display()
        );
    }
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
    if let Some(path) = env::var_os("CARGO_BIN_EXE_loon") {
        return PathBuf::from(path);
    }

    let current_exe = env::current_exe().expect("current test binary path");
    let debug_dir = current_exe
        .parent()
        .and_then(|path| path.parent())
        .expect("target debug dir");
    let candidate = debug_dir.join(if cfg!(windows) { "loon.exe" } else { "loon" });
    assert!(
        candidate.exists(),
        "expected loon binary at {}",
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

fn wait_for_health_ready(server_url: &str) -> bool {
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

fn stderr_string(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn parse_json(bytes: &[u8]) -> Value {
    serde_json::from_slice(bytes).expect("parse json")
}

fn json_data(output: &Output) -> Value {
    parse_json(&output.stdout)["data"].clone()
}

fn json_error(output: &Output) -> Value {
    parse_json(&output.stderr)["error"].clone()
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

/// The embedded capability document: the registry of advertised profiles and
/// feature keys (`crates/loonfs/tests/capability_conformance.rs` pins it to
/// the spec text).
fn embedded_capability_document() -> loonfs::CapabilityDocument {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let store = std::sync::Arc::new(
        loonfs_objectstore::local_fs_store::LocalFsStore::new(temp_dir.path()).expect("store"),
    ) as loonfs::SharedObjectStore;
    let reader = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime")
        .block_on(loonfs::FsReader::builder_with_store(store).build())
        .expect("build reader");
    reader.capabilities()
}

fn assert_cli_command_path_exists(harness: &Harness, command_path: &[&str]) {
    let mut args = command_path.to_vec();
    args.push("--help");
    let output = harness.run(&args);
    assert!(
        output.status.success(),
        "no CLI command path `loon {}`:\n{}",
        command_path.join(" "),
        stderr_string(&output)
    );
}
