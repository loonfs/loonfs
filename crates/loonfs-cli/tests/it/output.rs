//! Human, JSON, progress, warning, and pagination output contracts.

use super::common::*;

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
        .as_str()
        .expect("the stored inode ID")
        .to_owned();

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
