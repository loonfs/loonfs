//! Namespace and file operations across embedded and remote profiles.

use super::common::*;

#[test]
fn revisions_and_trash_use_the_shared_pagination_flags() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    assert_success(&harness.run(&["namespace", "create", "demo"]));
    assert_success(&harness.run(&["use", "demo"]));

    let payload = harness.temp_dir.path().join("payload.txt");
    fs::write(&payload, b"one\n").expect("payload");
    assert_success(&harness.run(&["put", payload.to_str().expect("utf-8 path"), "/report.txt"]));
    fs::write(&payload, b"two\n").expect("replacement payload");
    assert_success(&harness.run(&[
        "put",
        payload.to_str().expect("utf-8 path"),
        "/report.txt",
        "--force",
    ]));

    let first_revision = harness.run(&["--json", "revisions", "/report.txt", "--page-size", "1"]);
    assert_success(&first_revision);
    assert_eq!(
        json_data(&first_revision)["revisions"]
            .as_array()
            .expect("revision array")
            .len(),
        1
    );
    let revision_cursor = json_data(&first_revision)["next_cursor"]
        .as_str()
        .expect("revision cursor")
        .to_owned();
    let resumed_revision = harness.run(&[
        "--json",
        "revisions",
        "/report.txt",
        "--cursor",
        &revision_cursor,
    ]);
    assert_success(&resumed_revision);
    assert_eq!(
        json_data(&resumed_revision)["revisions"]
            .as_array()
            .expect("revision array")
            .len(),
        1
    );
    let revisions = harness.run(&["revisions", "/report.txt", "--page-size", "1", "--jsonl"]);
    assert_success(&revisions);
    assert_eq!(stdout_string(&revisions).lines().count(), 2);

    for index in 0..2 {
        let path = format!("/trash-{index}.txt");
        assert_success(&harness.run(&["put", payload.to_str().expect("utf-8 path"), &path]));
        assert_success(&harness.run(&["rm", &path]));
    }
    let bounded_trash = harness.run(&["--json", "trash", "--limit", "2", "--page-size", "1"]);
    assert_success(&bounded_trash);
    assert_eq!(
        json_data(&bounded_trash)["entries"]
            .as_array()
            .expect("trash array")
            .len(),
        2
    );
    let trash = harness.run(&["trash", "--page-size", "1", "--jsonl"]);
    assert_success(&trash);
    assert_eq!(stdout_string(&trash).lines().count(), 2);
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
    let revisions_data = json_data(&revisions);
    let revision_items = revisions_data["revisions"].as_array().expect("json array");
    assert_eq!(revision_items.len(), 2);
    assert_eq!(
        revision_items[0]["committed_by"],
        serde_json::json!({"kind":"service","id":"loonfs-cli"})
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
    let initial_stat = harness.run(&["--json", "stat", "/doc.txt"]);
    assert_success(&initial_stat);
    let inode_id = json_data(&initial_stat)["inode_id"]
        .as_str()
        .expect("inode id")
        .to_owned();

    // The guard implies --force: replacing the revision the caller observed
    // needs no second flag.
    fs::write(&payload, b"v2").expect("write payload");
    let guarded = harness.run(&[
        "--json",
        "put",
        payload.to_str().expect("utf-8 path"),
        "/doc.txt",
        "--expected-inode-id",
        &inode_id,
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
        "--expected-inode-id",
        &inode_id,
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
            harness
                .command()
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

    // Embedded PUTs upload a new object on each command invocation.
    // The original receipt remains authoritative even when the bytes match.
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
    assert_failure(&rerun);
    assert_eq!(json_error(&rerun)["code"], "commit_id_reuse_conflict");
    assert_eq!(
        json_error(&rerun)["details"]["committed_seq"],
        json_data(&first)["committed_seq"],
        "the conflict identifies the original publication"
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

    // A second embedded upload creates another object and must conflict.
    let rerun = harness.run(&[
        "--json",
        "put",
        local_path,
        "/big.bin",
        "--commit-id",
        "pinned-big",
        "--force",
    ]);
    assert_failure(&rerun);
    assert_eq!(json_error(&rerun)["code"], "commit_id_reuse_conflict");
    assert_eq!(
        json_error(&rerun)["details"]["committed_seq"],
        json_data(&first)["committed_seq"],
        "the conflict identifies the original publication"
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
    assert_eq!(json_error(&no_destination)["code"], "invalid_request");
}

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

#[test]
fn large_and_piped_puts_round_trip_over_the_remote_transport() {
    let harness = Harness::new();
    let remote_server =
        harness.start_external_server(harness.write_server_config("remote", "streaming-remote"));
    assert_success(&harness.run(&[
        "--json",
        "profile",
        "create",
        "remote",
        "default",
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
        .as_str()
        .expect("rm reports the deleted inode ID")
        .to_owned();
    let deletion_seq = json_data(&removed)["committed_seq"]
        .as_u64()
        .expect("rm reports the committed seq");
    assert_success(&harness.run(&[
        "undelete",
        "/dir/moved.txt",
        "--inode",
        &inode_id,
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

#[test]
fn commit_messages_ride_the_feed_over_the_remote_transport() {
    let harness = Harness::new();
    let remote_server =
        harness.start_external_server(harness.write_server_config("remote", "message-remote"));
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
        .find(|entry| entry["deleted_binding"]["display_name"] == "Quarterly Report.PDF")
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
    let inode = report["inode_id"].as_str().expect("inode");
    let seq = report["deletion_seq"].as_u64().expect("seq");
    assert_success(&harness.run(&[
        "undelete",
        "/docs/Quarterly Report.PDF",
        "--inode",
        inode,
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
    assert_eq!(
        json_data(&fork),
        serde_json::json!({
            "kind": "namespace_status",
            "namespace_id": "clone",
            "head_seq": 1,
            "retention_floor_seq": 1
        })
    );

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
fn embedded_commands_reject_out_of_range_ordinal_arguments() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");

    let changes = harness.run(&[
        "--json",
        "changes",
        "--profile",
        "default",
        "--namespace",
        "demo",
        "--after",
        "9007199254740992",
    ]);
    assert_failure(&changes);
    assert_eq!(json_error(&changes)["code"], "invalid_request");
    assert!(json_error(&changes)["message"]
        .as_str()
        .expect("json string")
        .contains("must be an integer from 0 through 9007199254740991"));
}

#[test]
fn remote_namespace_commands_reject_invalid_namespace_ids_before_http() {
    let harness = Harness::new();
    let add_remote = harness.run(&[
        "--json",
        "profile",
        "create",
        "remote",
        "default",
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
fn malformed_paths_are_invalid_request_in_both_modes_and_output_formats() {
    let harness = Harness::new();
    harness.add_embedded_profile("embedded");
    let add_remote = harness.run(&[
        "--json",
        "profile",
        "create",
        "remote",
        "remote",
        "--server-url",
        "http://127.0.0.1:9",
    ]);
    assert_success(&add_remote);

    for profile in ["embedded", "remote"] {
        let human = harness.run(&[
            "stat",
            "--profile",
            profile,
            "--namespace",
            "demo",
            "relative/path",
        ]);
        assert_failure(&human);
        assert!(stderr_string(&human).contains("absolute"), "{human:?}");
        assert!(stderr_string(&human).contains("param: path"), "{human:?}");

        let json = harness.run(&[
            "--json",
            "stat",
            "--profile",
            profile,
            "--namespace",
            "demo",
            "relative/path",
        ]);
        assert_failure(&json);
        let error = json_error(&json);
        assert_eq!(error["code"], "invalid_request");
        assert_eq!(error["param"], "path");
        assert!(error["message"]
            .as_str()
            .expect("error message")
            .contains("absolute"));
    }
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
            .as_str()
            .expect("stat reports inode ID")
            .to_owned();
        let by_inode = harness.run(&[
            "--json",
            "stat",
            "--profile",
            profile,
            "--namespace",
            "demo",
            "--inode",
            &inode_id,
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
            &inode_id,
        ]);
        assert_success(&renamed);
        assert_eq!(json_data(&renamed)["inode_id"], inode_id.as_str());
        assert_eq!(json_data(&renamed)["path"], "/after.txt");

        let missing = harness.run(&[
            "--json",
            "stat",
            "--profile",
            profile,
            "--namespace",
            "demo",
            "--inode",
            &format!("ino_{}", u64::MAX),
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
        "ino_2",
    ]);
    assert_failure(&both);
}

#[test]
fn inode_arguments_reject_bare_numbers() {
    let harness = Harness::new();
    let outputs = [
        harness.run(&["stat", "--inode", "27"]),
        harness.run(&["undelete", "--inode", "27", "--deletion-seq", "1"]),
        harness.run(&[
            "annotate",
            "/docs",
            "--expected-inode-id",
            "27",
            "--set",
            "owner=ada",
        ]),
    ];

    for output in outputs {
        assert_failure(&output);
        let stderr = stderr_string(&output);
        assert!(
            stderr.contains("must use `ino_` followed by a nonzero u64 without leading zeroes"),
            "{stderr}"
        );
    }
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
        .as_str()
        .expect("rm reports the deleted inode ID")
        .to_owned();
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
        &inode_id,
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
        &inode_id,
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
        &inode_id,
        "--deletion-seq",
        &deletion_seq.to_string(),
    ]);
    assert_failure(&again);
    assert_eq!(json_error(&again)["code"], "not_deleted");
}

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
        .as_str()
        .expect("trash reports the deleted inode ID")
        .to_owned();
    let deletion_seq = entry["deletion_seq"]
        .as_u64()
        .expect("trash reports the deletion sequence");

    // The parent moves on while the file sits in the trash.
    assert_success(&harness.run(&["mv", "/docs", "/archive"]));

    let recovered = harness.run(&[
        "--json",
        "undelete",
        "--inode",
        &inode_id,
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
        &inode_id,
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
        "remote",
        "default",
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
        .as_str()
        .expect("rm reports the deleted inode ID")
        .to_owned();
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
        &inode_id,
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

#[test]
fn annotate_rejects_a_set_without_an_equals_and_a_document_beside_the_flags() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    assert_success(&harness.run(&["namespace", "create", "demo"]));
    assert_success(&harness.run(&["use", "demo"]));
    assert_success(&harness.run(&["mkdir", "/docs"]));

    let no_equals = harness.run(&["--json", "annotate", "/docs", "--set", "owner"]);
    assert_failure(&no_equals);
    assert_eq!(json_error(&no_equals)["code"], "invalid_request");
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

    let string_value = harness.run(&[
        "--json",
        "annotate",
        "/docs",
        "--attributes-json",
        r#"{"set":{"tags":"[\"red\",\"blue\"]"}}"#,
    ]);
    assert_success(&string_value);
    let stat = harness.run(&["--json", "stat", "/docs"]);
    assert_success(&stat);
    assert_eq!(
        json_data(&stat)["attributes"]["tags"],
        serde_json::json!(r#"["red","blue"]"#)
    );

    let native_array = harness.run(&[
        "--json",
        "annotate",
        "/docs",
        "--attributes-json",
        r#"{"set":{"tags":["red","blue"]}}"#,
    ]);
    assert_failure(&native_array);
    let error = json_error(&native_array);
    assert_eq!(error["code"], "invalid_request");
    assert_eq!(error["param"], "--attributes-json");
}

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
        assert!(again_data["inode_id"]
            .as_str()
            .is_some_and(|inode_id| { loonfs_api::public_inode_id::decode(inode_id).is_ok() }));

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

#[test]
fn a_remote_put_replays_its_exact_request_after_the_upload_session_is_gone() {
    let harness = Harness::new();
    let server =
        harness.start_external_server(harness.write_server_config("remote", "put-recovery"));
    assert_success(&harness.run(&[
        "profile",
        "create",
        "remote",
        "default",
        "--server-url",
        &server.server_url,
        "--auth-token",
        "test-token",
    ]));
    assert_success(&harness.run(&["namespace", "create", "demo"]));
    assert_success(&harness.run(&["use", "demo"]));
    let payload = harness.temp_dir.path().join("payload");
    fs::write(&payload, b"retained request").expect("source");
    let args = [
        "--json",
        "put",
        payload.to_str().expect("path"),
        "/file",
        "--commit-id",
        "retained-put",
    ];
    let first = harness.run(&args);
    assert_success(&first);
    let uploads = harness
        .store_root("remote")
        .join("put-recovery/namespaces/demo/uploads");
    let records = fs::read_dir(&uploads)
        .expect("upload sessions")
        .collect::<Result<Vec<_>, _>>()
        .expect("records");
    assert!(!records.is_empty(), "the first call opened an upload");
    for entry in records {
        fs::remove_file(entry.path()).expect("expire session record");
    }
    let replay = harness.run(&args);
    assert_success(&replay);
    assert_eq!(
        json_data(&replay)["commit_id"],
        json_data(&first)["commit_id"]
    );
    assert_eq!(
        json_data(&replay)["committed_seq"],
        json_data(&first)["committed_seq"]
    );
    assert_eq!(
        fs::read_dir(&uploads).expect("upload directory").count(),
        0,
        "replay never creates or reads an upload session"
    );
    assert_eq!(download(&harness, "/file", "readback"), b"retained request");
}
