//! Subject selection, access replacement, and administrator recovery.

use super::common::*;

#[test]
fn an_acl_namespace_is_created_shown_and_recovered() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    assert_success(&harness.run(&[
        "namespace",
        "create",
        "demo",
        "--access",
        "acl",
        "--principal-scope",
        "org",
        "--administrator",
        "prn_root",
    ]));
    assert_success(&harness.run(&["use", "demo"]));
    let shown = harness.run(&["namespace", "show"]);
    assert_success(&shown);
    assert!(stdout_string(&shown).contains("access: acl (scope org)"));
    assert_success(&harness.run(&["mkdir", "/team", "--principals", "prn_root"]));
    assert_success(&harness.run(&[
        "access",
        "set",
        "/",
        "--grant",
        "team=read,create",
        "--principals",
        "prn_root",
    ]));
    let removed = harness.run(&["--json", "mkdir", "/again", "--principals", "prn_root"]);
    assert_failure(&removed);
    assert_eq!(json_error(&removed)["code"], "path_not_found");
    assert_success(&harness.run(&["maintenance", "recover-administrator", "prn_root"]));
    assert_success(&harness.run(&["mkdir", "/again", "--principals", "prn_root"]));
    let hidden = harness.run(&["--json", "mkdir", "/x", "--principals", "nobody"]);
    assert_failure(&hidden);
    assert_eq!(json_error(&hidden)["code"], "path_not_found");
    let missing = harness.run(&["--json", "mkdir", "/y"]);
    assert_failure(&missing);
    assert_eq!(json_error(&missing)["code"], "invalid_request");
    assert_eq!(json_error(&missing)["param"], "Loonfs-Principals");
    let fork = harness.run(&[
        "--json",
        "namespace",
        "fork",
        "demo",
        "clone",
        "--principals",
        "nobody",
    ]);
    assert_failure(&fork);
    assert_eq!(json_error(&fork)["code"], "forbidden");
    let delete = harness.run(&[
        "--json",
        "namespace",
        "delete",
        "demo",
        "--yes",
        "--principals",
        "nobody",
    ]);
    assert_failure(&delete);
    assert_eq!(json_error(&delete)["code"], "forbidden");
    let payload = harness.temp_dir.path().join("note.txt");
    fs::write(&payload, b"needle\n").expect("payload");
    assert_success(&harness.run(&[
        "put",
        payload.to_str().expect("utf-8 path"),
        "/team/note.txt",
        "--principals",
        "prn_root",
    ]));
    assert_success(&harness.run(&["maintenance", "index", "enable"]));
    let found = harness.run(&["--json", "grep", "needle", "--principals", "team"]);
    assert_success(&found);
    assert_eq!(json_data(&found)["matches"][0]["path"], "/team/note.txt");
}

#[test]
fn subject_flags_and_profile_fields_resolve_in_order() {
    let harness = Harness::new();
    assert_success(&harness.run(&[
        "profile",
        "create",
        "local",
        "default",
        "--root",
        harness.store_root("default").to_str().expect("utf-8 path"),
        "--principals",
        "team",
    ]));
    assert_success(&harness.run(&[
        "namespace",
        "create",
        "demo",
        "--access",
        "acl",
        "--principal-scope",
        "org",
        "--administrator",
        "prn_root",
    ]));
    assert_success(&harness.run(&["use", "demo"]));
    assert_success(&harness.run(&[
        "access",
        "set",
        "/",
        "--grant",
        "team=read,create",
        "--principals",
        "prn_root",
    ]));
    assert_success(&harness.run(&["mkdir", "/a"]));
    let unshared = harness.run(&["--json", "access", "set", "/a", "--grant", "team=read"]);
    assert_failure(&unshared);
    assert_eq!(json_error(&unshared)["code"], "forbidden");
    let environment = [("LOONFS_PRINCIPALS", "nobody")];
    let hidden = harness.run_with_env(&environment, &["--json", "mkdir", "/b"]);
    assert_failure(&hidden);
    assert_eq!(json_error(&hidden)["code"], "path_not_found");
    assert_success(&harness.run_with_env(&environment, &["mkdir", "/b", "--principals", "team"]));
    let invalid = harness.run(&["--json", "mkdir", "/c", "--principals", "bad principal"]);
    assert_failure(&invalid);
    assert_eq!(json_error(&invalid)["code"], "invalid_request");
    assert_eq!(json_error(&invalid)["param"], "--principals");
}

#[test]
fn a_remote_profile_sends_the_subject_headers() {
    let harness = Harness::new();
    let server = harness.start_external_server(harness.write_server_config("remote", "access"));
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
    assert_success(&harness.run(&[
        "namespace",
        "create",
        "demo",
        "--access",
        "acl",
        "--principal-scope",
        "org",
        "--administrator",
        "prn_root",
    ]));
    assert_success(&harness.run(&["use", "demo"]));
    assert_success(&harness.run(&["mkdir", "/r", "--principals", "prn_root"]));
    let hidden = harness.run(&["--json", "mkdir", "/s", "--principals", "nobody"]);
    assert_failure(&hidden);
    assert_eq!(json_error(&hidden)["code"], "path_not_found");
}
