//! Integration tests for `capabilities` and `doctor`.

use super::common::*;
use loonfs_api::{CapabilityDocument, PROTOCOL_VERSION};
use std::collections::BTreeMap;

const CHECK_NAMES: [&str; 9] = [
    "config",
    "config_decode",
    "profile",
    "provider_config",
    "connectivity",
    "auth",
    "health",
    "capabilities",
    "namespace",
];

#[test]
fn embedded_capabilities_are_the_canonical_document() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");

    let output = harness.run(&["--json", "capabilities"]);
    assert_success(&output);
    let data = json_data(&output);
    assert_eq!(data["kind"], "capabilities");
    assert_eq!(data["protocol_version"], PROTOCOL_VERSION);
    assert!(data["profiles"]
        .as_array()
        .is_some_and(|rows| !rows.is_empty()));
    assert!(data["features"].is_object());
    assert!(data["limits"].is_object());

    let human = harness.run(&["capabilities"]);
    assert_success(&human);
    let rendered = stdout_string(&human);
    for heading in [
        "protocol version",
        "profiles",
        "enabled features",
        "disabled features",
        "limits",
    ] {
        assert!(rendered.lines().any(|line| line == heading), "{rendered}");
    }
}

#[test]
fn doctor_is_read_only_when_a_local_store_root_is_missing() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    let root = harness.store_root("default");
    assert!(!root.exists());

    let output = harness.run(&["--json", "doctor"]);
    assert_success(&output);
    assert!(!root.exists(), "doctor must not create the store root");

    let checks = json_data(&output)["checks"]
        .as_array()
        .expect("doctor checks")
        .clone();
    assert_eq!(check_names(&checks), CHECK_NAMES);
    assert_eq!(check_status(&checks, "health"), "skipped");
    assert_eq!(check_status(&checks, "capabilities"), "skipped");
    assert_eq!(check_status(&checks, "namespace"), "skipped");
}

#[test]
fn doctor_opens_an_existing_embedded_store_without_writing_it() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    let root = harness.store_root("default");
    fs::create_dir_all(&root).expect("create empty store root");

    let output = harness.run(&["--json", "doctor"]);
    assert_success(&output);
    let checks = json_data(&output)["checks"]
        .as_array()
        .expect("doctor checks")
        .clone();
    assert_eq!(check_status(&checks, "health"), "ok");
    assert_eq!(check_status(&checks, "capabilities"), "ok");
    assert_eq!(check_status(&checks, "namespace"), "skipped");
    assert_eq!(
        fs::read_dir(&root).expect("read empty store root").count(),
        0,
        "doctor must not create store objects"
    );
}

#[test]
fn doctor_renders_every_check_before_exiting_for_a_bad_config() {
    let harness = Harness::new();

    let output = harness.run(&["--json", "doctor"]);
    assert_failure(&output);
    assert!(
        output.stderr.is_empty(),
        "doctor should write results to stdout"
    );
    let checks = json_data(&output)["checks"]
        .as_array()
        .expect("doctor checks")
        .clone();
    assert_eq!(check_names(&checks), CHECK_NAMES);
    assert_eq!(check_status(&checks, "config"), "ok");
    assert_eq!(check_status(&checks, "config_decode"), "failed");
    assert_eq!(check_status(&checks, "profile"), "skipped");
}

#[test]
fn doctor_write_check_appends_the_existing_fourteen_check_probe() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");

    let output = harness.run(&["--json", "doctor", "--write-check"]);
    assert_success(&output);
    let checks = json_data(&output)["checks"]
        .as_array()
        .expect("doctor checks")
        .clone();
    assert_eq!(checks.len(), 10);
    assert_eq!(checks[9]["name"], "store_probe");
    assert_eq!(
        checks[9]["store_probe"]["checks"]
            .as_array()
            .expect("store probe checks")
            .len(),
        14
    );
}

#[test]
fn remote_doctor_checks_transport_auth_health_and_capabilities() {
    let harness = Harness::new();
    let document = CapabilityDocument {
        protocol_version: PROTOCOL_VERSION.to_owned(),
        profiles: vec!["core/v0".to_owned()],
        features: BTreeMap::new(),
        limits: BTreeMap::new(),
    };
    let (server_url, server) = json_response_server(vec![
        serde_json::json!({}),
        serde_json::to_value(document).expect("capabilities JSON"),
        serde_json::json!({}),
        serde_json::json!({
            "namespace_id": "demo",
            "head_seq": 0,
            "wal_tail_segments": 0,
            "retention_floor_seq": 0
        }),
    ]);
    harness.write_remote_listing_config(&server_url);

    let output = harness.run(&["--json", "doctor"]);
    server.join().expect("JSON server");
    assert_success(&output);
    let checks = json_data(&output)["checks"]
        .as_array()
        .expect("doctor checks")
        .clone();
    for name in ["connectivity", "auth", "health", "capabilities"] {
        assert_eq!(check_status(&checks, name), "ok", "{name}: {checks:?}");
    }
    assert_eq!(check_status(&checks, "namespace"), "ok");
}

fn check_names(checks: &[Value]) -> Vec<&str> {
    checks
        .iter()
        .map(|check| check["name"].as_str().expect("check name"))
        .collect()
}

fn check_status<'a>(checks: &'a [Value], name: &str) -> &'a str {
    checks
        .iter()
        .find(|check| check["name"] == name)
        .and_then(|check| check["status"].as_str())
        .expect("named check status")
}
