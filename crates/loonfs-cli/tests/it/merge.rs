//! `loonfs merge` against a local server.

use super::common::*;

#[test]
fn merge_merges_a_fork_and_abandons_a_conflicting_session_against_a_server() {
    let harness = Harness::new();
    let server = harness.start_external_server(harness.write_server_config("remote", "merge"));
    harness.write_remote_listing_config(&server.server_url);
    assert_success(&harness.run(&["namespace", "create", "demo"]));
    let put = |namespace: &str, path: &str, bytes: &[u8]| {
        let args = ["--namespace", namespace, "put", "-", path, "--force"];
        assert_success(&harness.run_with_stdin(&args, bytes));
    };
    put("demo", "/a.txt", b"base");
    assert_success(&harness.run(&["namespace", "fork", "demo", "sandbox"]));
    put("sandbox", "/b.txt", b"new");

    let merged = harness.run(&["--json", "merge", "demo", "--from", "sandbox"]);

    assert_success(&merged);
    let data = json_data(&merged);
    assert_eq!(data["session"]["status"]["status"], "merged", "{data}");
    assert_eq!(data["steps"][0]["outcome"], "merge_committed", "{data}");
    let record = data["record_path"]
        .as_str()
        .expect("record path")
        .to_owned();
    assert!(
        !Path::new(&record).exists(),
        "a merged session removes its record"
    );
    assert_eq!(harness.run(&["cat", "/b.txt"]).stdout, b"new");

    // Both namespaces change `/a.txt`, so the fork's next merge conflicts,
    // and the default resolver leaves the session failed.
    put("sandbox", "/a.txt", b"ours");
    put("demo", "/a.txt", b"theirs");
    let failed = harness.run(&["--json", "merge", "demo", "--from", "sandbox"]);

    assert_failure(&failed);
    let data = json_data(&failed);
    let status = &data["session"]["status"];
    assert_eq!(status["status"], "failed", "{data}");
    assert_eq!(status["failure"]["kind"], "unresolved", "{data}");
    assert_eq!(
        status["reports"][0]["conflicts"][0]["ours"]["path"],
        "/a.txt"
    );
    let new_fork_id = status["fork_id"]
        .as_str()
        .expect("fork to inspect")
        .to_owned();
    assert_eq!(data["session"]["new_fork_ids"][0], new_fork_id.as_str());
    assert!(
        Path::new(&record).exists(),
        "a failed session keeps its record"
    );
    let again = harness.run(&["--json", "merge", "demo", "--from", "sandbox"]);
    assert_failure(&again);
    assert_eq!(json_data(&again)["steps"], serde_json::json!([]));

    let abandoned = harness.run(&["--json", "merge", "demo", "--from", "sandbox", "--abandon"]);

    assert_success(&abandoned);
    let data = json_data(&abandoned);
    assert_eq!(data["session"]["status"]["status"], "abandoned", "{data}");
    assert_eq!(data["steps"][0]["outcome"], "deleted", "{data}");
    assert!(!Path::new(&record).exists());
    assert_failure(&harness.run(&["namespace", "show", &new_fork_id]));
    assert_eq!(harness.run(&["cat", "/a.txt"]).stdout, b"theirs");
}
