//! Contract comparisons between embedded handles and the HTTP surface.

use super::common::*;

/// The `[grep]` table a server needs before it serves or maintains grep.
const GREP_SERVED: &str = "\n[grep]\nmode = \"serve_and_maintain\"\n";

fn backends(schedule_maintenance: bool, server_extra: &str) -> (Harness, ExternalServer) {
    let harness = Harness::new();
    harness.add_embedded_profile("embedded");
    let config = harness.write_server_config_with("remote", "contract", server_extra);
    if !schedule_maintenance {
        let contents = fs::read_to_string(&config).expect("server config");
        fs::write(&config, format!("maintenance = \"serve_only\"\n{contents}"))
            .expect("disable scheduled maintenance");
    }
    let server = harness.start_external_server(config);
    assert_success(&harness.run(&[
        "profile",
        "create",
        "remote",
        "remote",
        "--server-url",
        &server.server_url,
        "--auth-token",
        "test-token",
    ]));
    for profile in ["embedded", "remote"] {
        assert_success(&harness.run(&["namespace", "create", "--profile", profile, "demo"]));
        assert_success(&harness.run(&["use", "--profile", profile, "demo"]));
    }
    (harness, server)
}

fn error_fields(error: &Value) -> Value {
    serde_json::json!({ "code": error["code"], "param": error["param"], "feature": error["feature"] })
}

fn compare_errors(
    harness: &Harness,
    args: &[&str],
    code: &str,
    param: Option<&str>,
    feature: Option<&str>,
) {
    let errors: Vec<_> = ["embedded", "remote"]
        .into_iter()
        .map(|profile| {
            let mut command = vec!["--json", "--profile", profile];
            command.extend_from_slice(args);
            let output = harness.run(&command);
            assert_failure(&output);
            error_fields(&json_error(&output))
        })
        .collect();
    assert_eq!(errors[0], errors[1]);
    assert_eq!(
        errors[0],
        serde_json::json!({"code": code, "param": param, "feature": feature})
    );
}

#[test]
fn maintenance_configuration_errors_have_the_same_code_and_parameter() {
    let (harness, _server) = backends(true, "");
    compare_errors(
        &harness,
        &["maintenance", "metadata", "--max-wal-tail-segments", "0"],
        "invalid_request",
        Some("/max_wal_tail_segments"),
        None,
    );
}

#[test]
fn unavailable_grep_identifies_the_same_feature() {
    let (harness, _server) = backends(true, "");
    compare_errors(
        &harness,
        &["grep", "needle"],
        "not_supported",
        None,
        Some("query.grep"),
    );
}

#[test]
fn grep_pattern_limits_match_before_index_access() {
    let (harness, _server) = backends(true, GREP_SERVED);
    compare_errors(
        &harness,
        &["grep", &"x".repeat(1025)],
        "invalid_request",
        Some("pattern"),
        None,
    );
}

#[test]
fn grep_cursor_errors_identify_the_same_parameter() {
    let (harness, _server) = backends(true, GREP_SERVED);
    for profile in ["embedded", "remote"] {
        assert_success(&harness.run(&["maintenance", "index", "enable", "--profile", profile]));
    }
    compare_errors(
        &harness,
        &["grep", "needle", "--cursor", "invalid"],
        "invalid_request",
        Some("cursor"),
        None,
    );
}

#[test]
fn snapshot_ttl_errors_match_without_creating_pins() {
    let (harness, _server) = backends(true, "");
    for ttl in ["0", "86400001"] {
        compare_errors(
            &harness,
            &[
                "snapshot", "create", "demo", "--name", "invalid", "--ttl-ms", ttl,
            ],
            "invalid_request",
            Some("/ttl_ms"),
            None,
        );
    }
    for profile in ["embedded", "remote"] {
        let listed = harness.run(&["--json", "snapshot", "list", "demo", "--profile", profile]);
        assert_success(&listed);
        assert_eq!(json_data(&listed)["snapshots"], serde_json::json!([]));
    }
}

#[test]
fn snapshot_quota_errors_match() {
    let (harness, _server) = backends(true, "");
    for profile in ["embedded", "remote"] {
        for _ in 0..loonfs::SnapshotPolicy::default().max_live_per_namespace {
            assert_success(&harness.run(&[
                "snapshot",
                "create",
                "demo",
                "--profile",
                profile,
                "--name",
                "pin",
                "--ttl-ms",
                "600000",
            ]));
        }
    }
    compare_errors(
        &harness,
        &[
            "snapshot", "create", "demo", "--name", "overflow", "--ttl-ms", "600000",
        ],
        "snapshot_quota_exceeded",
        None,
        None,
    );
}

#[test]
fn snapshot_change_pages_report_the_same_through_sequence() {
    let (harness, _server) = backends(true, "");
    let mut pages = Vec::new();
    let mut errors = Vec::new();
    for profile in ["embedded", "remote"] {
        for path in ["/one", "/two", "/three"] {
            assert_success(&harness.run(&["mkdir", "--profile", profile, path]));
        }
        let created = harness.run(&[
            "--json",
            "snapshot",
            "create",
            "demo",
            "--profile",
            profile,
            "--name",
            "pin",
            "--ttl-ms",
            "600000",
        ]);
        assert_success(&created);
        let snapshot = json_data(&created)["snapshot_id"]
            .as_str()
            .expect("snapshot id")
            .to_owned();
        assert_success(&harness.run(&["mkdir", "--profile", profile, "/later"]));
        let page = harness.run(&[
            "--json",
            "changes",
            "--profile",
            profile,
            "--snapshot-id",
            &snapshot,
            "--limit",
            "1",
        ]);
        assert_success(&page);
        let page = json_data(&page);
        pages.push(serde_json::json!({ "through_seq": page["through_seq"], "next_after_seq": page["next_after_seq"], "count": page["changes"].as_array().expect("changes").len() }));
        let invalid = harness.run(&[
            "--json",
            "changes",
            "--profile",
            profile,
            "--snapshot-id",
            &snapshot,
            "--after",
            "4",
        ]);
        assert_failure(&invalid);
        errors.push(error_fields(&json_error(&invalid)));
    }
    assert_eq!(pages[0], pages[1]);
    assert_eq!(
        pages[0],
        serde_json::json!({"through_seq": 1, "next_after_seq": 1, "count": 1})
    );
    assert_eq!(errors[0], errors[1]);
    assert_eq!(errors[0]["param"], "after_seq");
}

#[test]
fn journaled_puts_replay_and_refuse_changed_subjects_in_both_modes() {
    let (harness, _server) = backends(true, "");
    let payload = harness.temp_dir.path().join("payload");
    fs::write(&payload, vec![b'x'; 512 * 1024]).expect("payload");
    for profile in ["embedded", "remote"] {
        let mut command = vec![
            "--json",
            "put",
            "--profile",
            profile,
            payload.to_str().expect("path"),
            "/file",
            "--commit-id",
            "c_journal",
            "--subject-id",
            "alice",
            "--principal-scope",
            "org",
            "--principals",
            "member",
        ];
        let first = harness.run(&command);
        assert_success(&first);
        let repeated = harness.run(&command);
        assert_success(&repeated);
        assert_eq!(
            json_data(&first)["committed_seq"],
            json_data(&repeated)["committed_seq"]
        );
        let subject = command
            .iter()
            .position(|value| *value == "alice")
            .expect("subject");
        command[subject] = "bob";
        let refused = harness.run(&command);
        assert_failure(&refused);
        assert!(json_error(&refused)["message"]
            .as_str()
            .expect("message")
            .contains("PUT options changed"));
    }
}

#[tokio::test]
async fn commit_shape_errors_preserve_json_pointers_across_surfaces() {
    let (harness, server) = backends(true, "");
    let namespace_id = loonfs_api::NamespaceId::parse("demo").expect("namespace");
    let actor_id = loonfs_test_support::test_actor();
    let operation = loonfs_api::FilesystemOperation::CopyPath {
        source_path: loonfs_api::AbsolutePath::parse("/source").expect("path"),
        destination_path: loonfs_api::AbsolutePath::parse("/target").expect("path"),
        precondition: loonfs_api::DestinationPrecondition {
            behavior: loonfs_api::DestinationBehavior::Replace,
            expected_inode_id: None,
            expected_revision_no: Some(loonfs_api::RevisionNo(1)),
        },
    };
    let request =
        loonfs_api::CommitRequest::single(loonfs_api::CommitId::generate(), None, operation);
    let writer = loonfs::FsWriter::builder(loonfs_objectstore::StoreConfig::LocalFs {
        root: harness.store_root("embedded").display().to_string(),
        key_prefix: None,
    })
    .writer_id("contract")
    .build()
    .await
    .expect("writer");
    let embedded = writer
        .create_commit(
            &namespace_id,
            loonfs::publish::CommitRequest {
                commit_id: request.commit_id.clone(),
                actor_id: actor_id.clone(),
                subject: None,
                message: request.message.clone(),
                preconditions: request.preconditions.clone(),
                operations: request.operations.clone(),
            },
        )
        .await
        .expect_err("malformed commit")
        .to_api_error();
    let client = loonfs_client::Client::new(loonfs_client::ClientConfig {
        server_url: server.server_url.clone(),
        auth_token: Some("test-token".into()),
        request_timeout_ms: None,
        disable_transient_retry: true,
        ca_cert_path: None,
    })
    .expect("client");
    let remote = client
        .create_commit(&namespace_id, &request, &actor_id)
        .await
        .expect_err("malformed commit");
    let loonfs_client::ClientError::Api {
        code,
        param,
        feature,
        ..
    } = remote
    else {
        panic!("expected API error: {remote:?}")
    };
    assert_eq!(embedded.code, code);
    assert_eq!(embedded.param, param);
    assert_eq!(embedded.feature, feature);
    assert_eq!(
        embedded.param.as_deref(),
        Some("/operations/0/expected_destination_revision_no")
    );
}

#[tokio::test]
async fn maintenance_required_is_returned_without_a_cli_retry() {
    let (harness, _server) = backends(false, "");
    let namespace_id = loonfs_api::NamespaceId::parse("demo").expect("namespace");
    for profile in ["embedded", "remote"] {
        let store = loonfs_objectstore::StoreConfig::LocalFs {
            root: harness.store_root(profile).display().to_string(),
            key_prefix: (profile == "remote").then(|| "contract".to_owned()),
        }
        .configured_object_store()
        .expect("store")
        .into_shared();
        loonfs_core::test_support::append_wal_segments(
            store.as_ref(),
            &namespace_id,
            loonfs_core::limits::MAX_UNFLUSHED_WAL_SEGMENTS - 1,
            &loonfs_core::MutationContext {
                writer_id: loonfs_api::WriterId::parse("contract").expect("writer"),
                now_ms: 1_000,
            },
        )
        .await
        .expect("seed WAL debt");
    }
    compare_errors(
        &harness,
        &["mkdir", "/gated"],
        "maintenance_required",
        None,
        None,
    );
}
