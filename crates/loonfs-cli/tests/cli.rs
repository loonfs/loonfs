use serde_json::Value;
use std::env;
use std::fs;
use std::io::Write;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;

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
    let first = harness.run(&[
        "--json",
        "put",
        local_path,
        "/big.bin",
        "--commit-id",
        "pinned-big",
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
    let deleted_at = json_data(&removed)["committed_seq"]
        .as_u64()
        .expect("rm reports the committed seq");
    assert_success(&harness.run(&[
        "undelete",
        "/dir/moved.txt",
        "--inode",
        &inode_id.to_string(),
        "--deleted-at",
        &deleted_at.to_string(),
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

    // The human table prints the exact undelete invocation.
    let human = harness.run(&["trash"]);
    assert_success(&human);
    let table = stdout_string(&human);
    assert!(
        table.contains("DELETED\tNAME\tINODE\tSEQ\tRECOVER"),
        "{table}"
    );
    assert!(table.contains("Quarterly Report.PDF"), "{table}");
    assert!(table.contains("loonfs undelete "), "{table}");

    // Recovering through the listed handle empties that entry out of trash.
    let inode = report["root_inode_id"].as_u64().expect("inode");
    let seq = report["deleted_at_seq"].as_u64().expect("seq");
    assert_success(&harness.run(&[
        "undelete",
        "/docs/Quarterly Report.PDF",
        "--inode",
        &inode.to_string(),
        "--deleted-at",
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
        table.contains("REVISION\tDATE\tSEQ\tSIZE\tDIGEST"),
        "{table}"
    );

    let stat_file = harness.run(&["stat", "/doc.txt"]);
    assert_success(&stat_file);
    assert!(
        stdout_string(&stat_file).contains("modified: "),
        "{}",
        stdout_string(&stat_file)
    );
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
    ]);
    assert_success(&put);
    let put_data = json_data(&put);
    assert_eq!(put_data["kind"], "tree_transfer");
    assert_eq!(put_data["files"], 3);
    assert_eq!(put_data["directories"], 1);
    assert_eq!(put_data["failures"].as_array().expect("failures").len(), 0);
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
    let deleted_at = json_data(&removed)["committed_seq"]
        .as_u64()
        .expect("rm reports the deletion sequence");
    let gone = harness.run(&["--json", "revisions", "/docs/report.txt"]);
    assert_failure(&gone);
    assert_eq!(json_error(&gone)["code"], "path_not_found");

    // Undelete brings back identity, content, and revision history.
    let recovered = harness.run(&[
        "--json",
        "undelete",
        "/docs/report.txt",
        "--inode",
        &inode_id.to_string(),
        "--deleted-at",
        &deleted_at.to_string(),
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
        "--deleted-at",
        &deleted_at.to_string(),
    ]);
    assert_failure(&again);
    assert_eq!(json_error(&again)["code"], "not_deleted");
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
    let deleted_at = json_data(&removed)["committed_seq"]
        .as_u64()
        .expect("rm reports the deletion sequence");

    let recovered = harness.run(&[
        "--json",
        "undelete",
        "/wire.txt",
        "--inode",
        &inode_id.to_string(),
        "--deleted-at",
        &deleted_at.to_string(),
    ]);
    assert_success(&recovered);
    let cat = harness.run(&["cat", "/wire.txt"]);
    assert_success(&cat);
    assert_eq!(cat.stdout, b"over the wire");
}

#[test]
fn mkdir_parents_get_noclobber_and_version_metadata() {
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

    // --version is a real flag now, and both forms carry build metadata.
    assert_success(&harness.run(&["--version"]));
    let version = harness.run(&["--json", "version"]);
    assert_success(&version);
    let data = json_data(&version);
    assert!(!data["commit"].as_str().expect("commit").is_empty());
    assert!(!data["commit_date"]
        .as_str()
        .expect("commit date")
        .is_empty());
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
    assert_eq!(json_data(&enabled)["state"]["phase"], "steady");
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
    assert_eq!(json_data(&recaught)["already_enabled"], true);
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
    assert_eq!(json_data(&retried)["already_enabled"], true);
    assert!(json_data(&retried).get("backfill_step").is_none());
}

/// The index reports the phase it is in, and the phases do not share a
/// field: a backfill names its target, a steady index names its watermark.
#[test]
fn index_status_reports_each_phase_in_its_own_terms() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    assert_success(&harness.run(&["namespace", "create", "demo"]));
    assert_success(&harness.run(&["use", "demo"]));

    let disabled = harness.run(&["--json", "admin", "index-status"]);
    assert_success(&disabled);
    assert_eq!(json_data(&disabled)["state"]["phase"], "disabled");
    assert!(json_data(&disabled)["state"]
        .get("built_through_seq")
        .is_none());

    // `--no-wait` returns with the root the enable published: a backfill,
    // naming the sequence it will walk to and nothing it has indexed.
    let enabled = harness.run(&["--json", "admin", "index-enable", "--no-wait"]);
    assert_success(&enabled);
    let state = &json_data(&enabled)["state"];
    assert_eq!(state["phase"], "backfilling");
    assert_eq!(state["target_seq"], 0);
    assert!(state.get("built_through_seq").is_none());
    assert!(json_data(&enabled).get("waited_for_seq").is_none());
    assert_eq!(json_data(&enabled)["steps"], 0);

    let backfilling = harness.run(&["--json", "admin", "index-status"]);
    assert_success(&backfilling);
    assert_eq!(json_data(&backfilling)["state"]["phase"], "backfilling");
    assert_eq!(json_data(&backfilling)["reorganize_pending"], false);
    assert!(backfilling_text_names_no_watermark(&harness));

    // Waiting takes it steady, and only then is there a watermark.
    assert_success(&harness.run(&["admin", "index-enable"]));
    let steady = harness.run(&["--json", "admin", "index-status"]);
    assert_success(&steady);
    assert_eq!(json_data(&steady)["state"]["phase"], "steady");
    assert_eq!(json_data(&steady)["state"]["built_through_seq"], 0);
    assert!(json_data(&steady)["state"].get("target_seq").is_none());
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
        json_data(&status)["state"]["built_through_seq"],
        1,
        "the earlier wait stopped at the target it captured"
    );

    // An index already at the namespace head returns without stepping.
    assert_success(&harness.run(&["admin", "index-enable"]));
    let caught_up = harness.run(&["--json", "admin", "index-enable"]);
    assert_success(&caught_up);
    assert_eq!(json_data(&caught_up)["already_enabled"], true);
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
            data["state"]["phase"], "backfilling",
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
    assert_eq!(json_data(&disabled)["state"]["phase"], "disabled");

    let enabled = harness.run(&["--json", "admin", "index-enable"]);
    assert_success(&enabled);
    assert_eq!(json_data(&enabled)["waited_for_seq"], 1);
    assert_eq!(json_data(&enabled)["budget_exhausted"], false);
    assert_eq!(json_data(&enabled)["state"]["phase"], "steady");

    let steady = harness.run(&["--json", "admin", "index-status"]);
    assert_success(&steady);
    assert_eq!(json_data(&steady)["state"]["built_through_seq"], 1);

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
    assert_eq!(data["drained"], true);
    assert_eq!(data["budget_exhausted"], false);
    let keys = data["keys"].as_array().expect("json array");
    assert_eq!(keys.len(), 6, "three jobs over two namespaces: {data}");
    assert!(
        keys.iter().all(|key| key["settled"] == true),
        "an unbudgeted drain settles every key: {data}"
    );
    assert_eq!(
        data["jobs"],
        serde_json::json!(["metadata", "gc", "grep-index"])
    );

    for namespace in ["alpha", "beta"] {
        let status = harness.run(&["--json", "admin", "index-status", "--namespace", namespace]);
        assert_success(&status);
        assert_eq!(
            json_data(&status)["state"]["built_through_seq"],
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
    for job in ["metadata", "core-gc", "grep-index"] {
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
    for field in ["drained", "budget_exhausted", "jobs"] {
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

/// A remote profile's server hosts its own runner. Stepping one from here
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
    assert!(
        error["message"]
            .as_str()
            .expect("message")
            .contains("embedded profile"),
        "{error}"
    );
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
        ("query/v0", &[&["grep"]]),
        (
            "admin/v0",
            &[
                &["admin", "checkpoint"],
                &["admin", "checkpoint-release"],
                &["admin", "flush"],
                &["admin", "retention-advance"],
                &["admin", "run"],
                &["admin", "step"],
                &["admin", "gc"],
                &["admin", "index-enable"],
                &["admin", "index-disable"],
                &["admin", "index-status"],
                &["admin", "index-gc"],
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
        // Both direct transports are modes negotiated inside the same upload
        // staging flow `put` drives; neither needs a separate verb.
        ("core.uploads.direct_put", &["put"]),
        ("core.uploads.direct_multipart", &["put"]),
        ("query.grep", &["grep"]),
    ];

    let harness = Harness::new();
    let document = embedded_capability_document();

    for profile in &document.profiles {
        let (_, command_paths) = PROFILE_COMMAND_PATHS
            .iter()
            .find(|(advertised, _)| *advertised == profile.as_str())
            .unwrap_or_else(|| {
                unreachable!(
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
                unreachable!(
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
        assert_eq!(listed[0]["seq"], 1);
        assert_eq!(listed[1]["seq"], 2);
        assert!(listed[0]["commit_id"]
            .as_str()
            .expect("json string")
            .starts_with("c_"));
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
        assert_eq!(paged_data["changes"][0]["seq"], 1);
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
        assert_eq!(resumed_data["changes"][0]["seq"], 2);

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

        // `admin flush` runs a maintenance step restricted to the WAL
        // flush, so it reports a step: the flush part acted, the parts `only`
        // excluded report `not_needed`.
        let flush = harness.run(&["--json", "admin", "flush", "--profile", profile]);
        assert_success(&flush);
        let flush_data = json_data(&flush);
        assert_eq!(flush_data["kind"], "maintenance_stepped");
        assert_eq!(flush_data["namespace_id"], "demo");
        assert_eq!(flush_data["reorganize"]["kind"], "not_needed");
        assert!(flush_data["gc"].is_null());

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
        assert_eq!(retention_data["retention_floor_seq"], 2);

        // The checkpoint above already covers the head, so a step reports
        // not-needed identically in both modes.
        let step = harness.run(&["--json", "admin", "step", "--profile", profile]);
        assert_success(&step);
        let step_data = json_data(&step);
        assert_eq!(step_data["kind"], "maintenance_stepped");
        assert_eq!(step_data["namespace_id"], "demo");
        assert_eq!(step_data["wal_flush"]["kind"], "not_needed");
        assert_eq!(step_data["reorganize"]["kind"], "not_needed");
        // An unrestricted step never advances the floor on its own; it
        // reports the floor the earlier retention-advance established.
        assert_eq!(step_data["retention_floor_seq"], 2);
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

        // Supplying a candidate budget requests exactly one pass and exposes
        // the opaque cursor instead of the CLI's default completion loop.
        let bounded_gc = harness.run(&[
            "--json",
            "admin",
            "gc",
            "--max-objects",
            "1",
            "--profile",
            profile,
        ]);
        assert_success(&bounded_gc);
        assert!(json_data(&bounded_gc)["next_cursor"]
            .as_str()
            .is_some_and(|cursor| !cursor.is_empty()));

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

        shapes_by_mode.push((
            sorted_object_keys(&changes_data),
            sorted_object_keys(&checkpoint_data),
            sorted_object_keys(&retention_data),
            sorted_object_keys(&step_data),
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
            .expect("run loonfs")
    }

    /// Runs the CLI with a payload on standard input, which is the one
    /// source whose length is not knowable before it is read.
    fn run_with_stdin(&self, args: &[&str], stdin: &[u8]) -> Output {
        let mut child = Command::new(loon_binary_path())
            .env("HOME", &self.home_dir)
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

/// What an embedded profile serves: the runtime's own capability document
/// (`crates/loonfs/tests/capability_conformance.rs` pins it to the spec
/// text) plus the query plane the CLI composes from `loonfs-grep`, which is
/// how `loonfs grep` and `loonfs admin index-*` reach a store at all.
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
    let mut document = reader.capabilities();
    document
        .profiles
        .push(loonfs_api::PROFILE_QUERY_V0.to_owned());
    document
        .features
        .insert(loonfs_api::FEATURE_QUERY_GREP.to_owned(), true);
    document
}

fn assert_cli_command_path_exists(harness: &Harness, command_path: &[&str]) {
    let mut args = command_path.to_vec();
    args.push("--help");
    let output = harness.run(&args);
    assert!(
        output.status.success(),
        "no CLI command path `loonfs {}`:\n{}",
        command_path.join(" "),
        stderr_string(&output)
    );
}
