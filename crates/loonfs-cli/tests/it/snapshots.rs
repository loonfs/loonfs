//! Snapshot lifecycle and point-in-time CLI reads.

use super::common::*;

#[test]
fn snapshot_family_and_captured_read_work_end_to_end() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    assert_success(&harness.run(&["namespace", "create", "demo"]));
    assert_success(&harness.run(&["use", "demo"]));

    let payload = harness.temp_dir.path().join("report.txt");
    fs::write(&payload, b"captured\n").expect("write captured payload");
    assert_success(&harness.run(&["put", payload.to_str().expect("utf-8 path"), "/report.txt"]));

    let created = harness.run(&[
        "--json", "snapshot", "create", "demo", "--name", "report", "--ttl-ms", "5000",
    ]);
    assert_success(&created);
    let created_data = json_data(&created);
    assert_eq!(created_data["kind"], "snapshot_created");
    assert_eq!(created_data["namespace_id"], "demo");
    assert_eq!(created_data["name"], "report");
    assert_eq!(created_data["head_seq"], 1);
    let snapshot_id = created_data["snapshot_id"]
        .as_str()
        .expect("snapshot id")
        .to_owned();
    let original_expiry = created_data["expires_at_ms"]
        .as_u64()
        .expect("snapshot expiry");

    let listed = harness.run(&["--json", "snapshot", "list", "demo"]);
    assert_success(&listed);
    let listed_data = json_data(&listed);
    assert_eq!(listed_data["kind"], "snapshots_listed");
    assert_eq!(listed_data["snapshots"][0]["snapshot_id"], snapshot_id);

    let listed_human = harness.run(&["snapshot", "list", "demo"]);
    assert_success(&listed_human);
    let listed_text = stdout_string(&listed_human);
    assert!(listed_text.contains("SNAPSHOT\tNAME\tSEQ\tCREATED\tEXPIRES"));
    assert!(listed_text.contains(&snapshot_id));

    let extended = harness.run(&[
        "--json",
        "snapshot",
        "extend",
        "demo",
        &snapshot_id,
        "--ttl-ms",
        "60000",
    ]);
    assert_success(&extended);
    let extended_data = json_data(&extended);
    assert_eq!(extended_data["kind"], "snapshot_extended");
    assert!(
        extended_data["expires_at_ms"]
            .as_u64()
            .expect("extended expiry")
            > original_expiry
    );

    fs::write(&payload, b"current\n").expect("write current payload");
    assert_success(&harness.run(&[
        "put",
        payload.to_str().expect("utf-8 path"),
        "/report.txt",
        "--force",
    ]));
    let captured = harness.run(&["cat", "/report.txt", "--snapshot-id", &snapshot_id]);
    assert_success(&captured);
    assert_eq!(captured.stdout, b"captured\n");

    let released = harness.run(&["--json", "snapshot", "release", "demo", &snapshot_id]);
    assert_success(&released);
    assert_eq!(json_data(&released)["kind"], "snapshot_released");

    let empty = harness.run(&["--json", "snapshot", "list", "demo"]);
    assert_success(&empty);
    assert!(json_data(&empty)["snapshots"]
        .as_array()
        .expect("snapshot array")
        .is_empty());

    let gone = harness.run(&[
        "--json",
        "stat",
        "/report.txt",
        "--snapshot-id",
        &snapshot_id,
    ]);
    assert_failure(&gone);
    let error = json_error(&gone);
    assert_eq!(error["code"], "snapshot_gone");
    assert!(error["message"]
        .as_str()
        .expect("snapshot error message")
        .contains("released"));
}
