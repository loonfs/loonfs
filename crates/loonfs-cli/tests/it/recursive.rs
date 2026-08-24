//! Recursive transfer and deletion behavior.

use super::common::*;

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

#[test]
fn a_recursive_put_streams_a_large_file_over_the_remote_transport() {
    let harness = Harness::new();
    let remote_server =
        harness.start_external_server(harness.write_server_config("remote", "recursive-remote"));
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
            change["committed_by"],
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
        rerun_data["directories"], 0,
        "the empty directory was already there: {rerun_data}"
    );
    assert_eq!(
        rerun_data["failures"].as_array().expect("failures").len(),
        3
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
    assert_success(&forced);
    let forced_data = json_data(&forced);
    assert_eq!(forced_data["files"], 3);
    assert_eq!(
        forced_data["directories"], 0,
        "--force replaces files, and creates no directory that already exists: {forced_data}"
    );
    assert_eq!(
        forced_data["failures"].as_array().expect("failures").len(),
        0,
        "the existing empty directory is already ensured: {forced_data}"
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
    assert_eq!(json_error(&mv_recursive)["code"], "invalid_request");
}

#[test]
fn recursive_put_reuses_existing_directories_but_rejects_files() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    assert_success(&harness.run(&["namespace", "create", "demo"]));
    assert_success(&harness.run(&["use", "demo"]));

    let tree = harness.temp_dir.path().join("empty-tree");
    fs::create_dir_all(tree.join("empty")).expect("create empty directory");
    let local_tree = tree.to_str().expect("utf-8 path");
    assert_success(&harness.run(&["put", "-r", local_tree, "/tree"]));

    let rerun = harness.run(&["--no-progress", "put", "-r", "--force", local_tree, "/tree"]);
    assert_success(&rerun);
    assert!(
        stdout_string(&rerun).contains("stored 0 files and 0 directories"),
        "{}",
        stdout_string(&rerun)
    );

    let payload = harness.temp_dir.path().join("payload.txt");
    fs::write(&payload, b"file").expect("write payload");
    assert_success(&harness.run(&[
        "put",
        payload.to_str().expect("utf-8 path"),
        "/blocked/empty",
    ]));
    let blocked = harness.run(&["--json", "put", "-r", "--force", local_tree, "/blocked"]);
    assert_failure(&blocked);
    let data = json_data(&blocked);
    assert_eq!(data["directories"], 0);
    assert_eq!(data["failures"][0]["error"]["code"], "path_conflict");
}

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
        // `other` alone: the destination root was already there, and `docs`
        // is the one that failed.
        assert_eq!(data["directories"], 1, "{data}");

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
fn recursive_get_surfaces_drift_across_directory_listings() {
    let harness = Harness::new();
    let (server_url, server) = json_response_server(vec![
        serde_json::json!({
            "namespace_id": "demo",
            "path": "/docs",
            "inode_id": "ino_2",
            "created_by": { "kind": "system", "id": "loonfs" },
            "created_at_ms": 1_752_624_000_000_u64,
            "inode_kind": "dir",
            "head_seq": 20,
            "parent_inode_id": "ino_1",
            "display_name": "docs",
        }),
        serde_json::json!({
            "namespace_id": "demo",
            "path": "/docs",
            "head_seq": 20,
            "entries": [{
                "namespace_id": "demo",
                "path": "/docs/sub",
                "inode_id": "ino_3",
                "created_by": { "kind": "system", "id": "loonfs" },
                "created_at_ms": 1_752_624_000_000_u64,
                "inode_kind": "dir",
                "head_seq": 20,
                "parent_inode_id": "ino_2",
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
