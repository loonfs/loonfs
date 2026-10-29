//! Rendering tests for human and JSON output.

use super::human::human_success;
use super::json::{json_error, json_success};
use super::output::human_error;
use crate::args::CommandKind;
use crate::commands::{CommandData, CommandFailure, CommandOutput, DoctorCheck, DoctorStatus};
use crate::config::{ProfileConfig, ProfileConnection, ProfileIdentity, ProfileMode, StoreConfig};
use crate::error::CliError;
use crate::profiles::ProfileSummary;
use loonfs_client::ClientError;
use loonfs_objectstore::local_fs_store::LocalFsStoreConfig;
use loonfs_test_support::provider_detail::{
    assert_provider_markers_absent, POISON_PROVIDER_DETAIL,
};
use loonfs_types::{
    AbsolutePath, AttributeValue, AttributesProjection, ChangeSeq, DisplayName, InodeId,
    NamespaceId, PathEntry, PathEntryKind, StoreProbeCheck, StoreProbeCheckOutcome,
    StoreProbeResponse,
};

fn poison_permission_error() -> loonfs::Error {
    loonfs::Error::Core(loonfs::CoreError::Store {
        object_key: "namespaces/customer-a/hint.json".to_owned(),
        message: POISON_PROVIDER_DETAIL.to_owned(),
        kind: loonfs::ObjectStoreErrorKind::PermissionDenied,
    })
}

fn rendered_remote_error(server_boundary: &str) -> serde_json::Value {
    let body: loonfs_types::ErrorResponse =
        serde_json::from_str(server_boundary).expect("server boundary is valid JSON");
    let error = CliError::from(ClientError::from_error_response(400, body));
    let failure = CommandFailure {
        kind: CommandKind::ConfigShow,
        profile: Some("remote".to_owned()),
        mode: Some(ProfileMode::Remote),
        error: Box::new(error),
    };
    serde_json::from_str(&json_error(&failure).expect("JSON error renders"))
        .expect("rendered error is valid JSON")
}

fn assert_server_fields_survive(server_boundary: &str, rendered: &serde_json::Value) {
    let boundary: serde_json::Value =
        serde_json::from_str(server_boundary).expect("server boundary is valid JSON");
    for (field, value) in boundary
        .as_object()
        .expect("server error boundary is an object")
    {
        assert_eq!(
            rendered.pointer(&format!("/error/{field}")),
            Some(value),
            "server field `{field}` was lost before CLI JSON"
        );
    }
}

#[test]
fn provider_failure_is_safe_on_embedded_remote_and_doctor_surfaces() {
    let public_message = loonfs::ObjectStoreError::PermissionDenied {
        object_key: "namespaces/customer-a/hint.json".to_owned(),
        message: POISON_PROVIDER_DETAIL.to_owned(),
    }
    .public_message()
    .into_owned();

    let embedded_failure = CommandFailure {
        kind: CommandKind::ConfigShow,
        profile: Some("embedded".to_owned()),
        mode: Some(ProfileMode::Embedded),
        error: Box::new(crate::error::CliError::from(poison_permission_error())),
    };
    let embedded_human = human_error(&embedded_failure.error, embedded_failure.mode);
    let embedded_json = json_error(&embedded_failure).expect("embedded JSON error renders");
    for rendered in [&embedded_human, &embedded_json] {
        assert_provider_markers_absent("rendered output", rendered);
        assert!(rendered.contains(&public_message), "{rendered}");
    }

    let remote_boundary = serde_json::to_string(&loonfs_types::ErrorResponse {
        code: loonfs_types::ErrorCode::StoragePermissionDenied
            .as_str()
            .to_owned(),
        feature: None,
        message: public_message.clone(),
        param: None,
        request_id: Some("req_provider_hygiene".to_owned()),
        details: None,
    })
    .expect("remote boundary serializes");
    let body: loonfs_types::ErrorResponse =
        serde_json::from_str(&remote_boundary).expect("remote boundary is valid JSON");
    let remote_failure = CommandFailure {
        kind: CommandKind::ConfigShow,
        profile: Some("remote".to_owned()),
        mode: Some(ProfileMode::Remote),
        error: Box::new(CliError::from(ClientError::from_error_response(503, body))),
    };
    let remote_human = human_error(&remote_failure.error, remote_failure.mode);
    let remote_json = json_error(&remote_failure).expect("remote JSON error renders");
    for rendered in [&remote_human, &remote_json] {
        assert_provider_markers_absent("rendered output", rendered);
        assert!(rendered.contains(&public_message), "{rendered}");
        assert!(rendered.contains("req_provider_hygiene"), "{rendered}");
    }

    let failed = |name: &str, message: String| StoreProbeCheck {
        name: name.to_owned(),
        outcome: StoreProbeCheckOutcome::Failed { message },
    };
    let checks = vec![
        failed(
            "create_if_absent_enforced",
            format!("create failed: {public_message}"),
        ),
        failed(
            "compare_and_swap_rejects_stale",
            format!(" create   failed:  {public_message}\n"),
        ),
        failed(
            "stored_checksum_readback",
            format!("checksum head failed: {public_message}"),
        ),
        failed(
            "range_reads",
            "range read failed: object-store is temporarily unavailable; retry the operation"
                .to_owned(),
        ),
    ];
    let doctor = CommandOutput {
        kind: CommandKind::Doctor,
        profile: Some("embedded".to_owned()),
        mode: Some(ProfileMode::Embedded),
        data: CommandData::Doctor {
            checks: vec![DoctorCheck {
                name: "store_probe".to_owned(),
                status: DoctorStatus::Failed,
                message: "store probe probe_00000000000000000000000001: 4 of 4 checks failed"
                    .to_owned(),
                request_id: None,
                store_probe: Some(StoreProbeResponse {
                    run_id: loonfs_types::ProbeRunId::parse("probe_00000000000000000000000001")
                        .expect("probe run id"),
                    checks: checks.clone(),
                }),
            }],
        },
    };
    let doctor_human = human_success(&doctor);
    let doctor_json = json_success(&doctor).expect("doctor JSON renders");
    for rendered in [&doctor_human, &doctor_json] {
        assert_provider_markers_absent("rendered output", rendered);
        assert!(rendered.contains(&public_message), "{rendered}");
    }
    assert!(
        doctor_human.contains("FAILED  store_probe"),
        "{doctor_human}"
    );
    assert!(
        doctor_human.contains("3 checks failed: object-store permission denied;"),
        "{doctor_human}"
    );
    assert!(
        doctor_human.contains(
            "affected: create_if_absent_enforced, compare_and_swap_rejects_stale, stored_checksum_readback"
        ),
        "{doctor_human}"
    );
    assert!(
        doctor_human.contains(
            "1 check failed: object-store is temporarily unavailable; retry the operation"
        ),
        "{doctor_human}"
    );
    assert!(
        doctor_human.contains("affected: range_reads"),
        "{doctor_human}"
    );
    let doctor_value: serde_json::Value =
        serde_json::from_str(&doctor_json).expect("doctor JSON is valid");
    assert_eq!(
        doctor_value["data"]["checks"][0]["store_probe"]["checks"],
        serde_json::to_value(checks).expect("probe checks serialize")
    );
}

fn path_entry(path: &str, display_name: Option<&str>) -> PathEntry {
    PathEntry {
        namespace_id: NamespaceId::parse("demo").expect("namespace id"),
        path: AbsolutePath::parse(path).expect("absolute path"),
        inode_id: InodeId(if display_name.is_some() { 2 } else { 1 }),
        created_by: loonfs_types::ActorId::loonfs(),
        created_at_ms: 1_752_624_000_000,
        kind: PathEntryKind::Directory {},
        head_seq: ChangeSeq(3),
        parent_inode_id: display_name.map(|_| InodeId(1)),
        display_name: display_name.map(|name| DisplayName::parse(name).expect("display name")),
        binding_version: None,
        attributes: None,
    }
}

fn stat_output(entry: PathEntry) -> CommandOutput {
    CommandOutput {
        kind: CommandKind::FilesystemStat,
        profile: Some("default".to_owned()),
        mode: Some(ProfileMode::Embedded),
        data: CommandData::PathEntry(entry),
    }
}

#[test]
fn human_stat_omits_the_absent_root_name() {
    let root = human_success(&stat_output(path_entry("/", None)));
    for line in [
        "path: /",
        "inode: ino_1",
        "kind: dir",
        "seq: 3",
        "created_by: loonfs",
        "created: 2025-07-16 00:00:00Z",
    ] {
        assert!(root.contains(line), "{root}");
    }
    assert!(
        !root.contains("\nname:"),
        "the root has no name to print: {root}"
    );

    let named = human_success(&stat_output(path_entry("/docs", Some("docs"))));
    assert!(named.contains("path: /docs"), "{named}");
    assert!(named.contains("\nname: docs"), "{named}");
}

/// Builds a stat answer carrying one attribute, so the value's rendering
/// can be read off the last line.
fn stat_with_attribute(value: AttributeValue) -> CommandOutput {
    let mut entry = path_entry("/docs", Some("docs"));
    entry.attributes = Some(AttributesProjection {
        attributes_revision_no: loonfs_types::AttributesRevisionNo(1),
        attributes_updated_by: Some(loonfs_types::ActorId::loonfs()),
        attributes_updated_at_ms: Some(1_752_624_000_000),
        attributes: loonfs_types::Attributes::new(std::collections::BTreeMap::from([(
            loonfs_types::AttributeKey::parse("note").expect("attribute key"),
            value,
        )]))
        .expect("attribute map"),
    });
    stat_output(entry)
}

fn attribute_line(output: &CommandOutput) -> String {
    human_success(output)
        .lines()
        .last()
        .expect("stat output has lines")
        .to_owned()
}

#[test]
fn human_stat_escapes_control_characters_in_attribute_values() {
    let newline = stat_with_attribute(
        AttributeValue::parse("first\nattr.inode: 99").expect("attribute value"),
    );
    let rendered = human_success(&newline);
    assert!(
        rendered.ends_with("attr.note: first\\nattr.inode: 99"),
        "the forged line should stay on one line: {rendered}"
    );
    assert_eq!(
        rendered.lines().count(),
        10,
        "the base entry, attribution, attribute update, and one attribute line: {rendered}"
    );

    let escape =
        stat_with_attribute(AttributeValue::parse("\u{1b}[31mred").expect("attribute value"));
    assert_eq!(attribute_line(&escape), "attr.note: \\u{1b}[31mred");

    let tabbed = stat_with_attribute(AttributeValue::parse("a\tb\rc").expect("attribute value"));
    assert_eq!(attribute_line(&tabbed), "attr.note: a\\tb\\rc");
}

#[test]
fn human_stat_leaves_ordinary_unicode_alone() {
    let unicode =
        stat_with_attribute(AttributeValue::parse("café ☃ 日本語 🙂").expect("attribute value"));
    assert_eq!(attribute_line(&unicode), "attr.note: café ☃ 日本語 🙂");
}

#[test]
fn json_stat_flattens_the_attribute_projection_and_preserves_values() {
    let newline =
        stat_with_attribute(AttributeValue::parse("first\nsecond").expect("attribute value"));
    let json: serde_json::Value =
        serde_json::from_str(&json_success(&newline).expect("JSON stat should render"))
            .expect("rendered stat is JSON");
    assert_eq!(json["data"]["attributes"]["note"], "first\nsecond");
    assert_eq!(json["data"]["attributes_revision_no"], 1);
    assert!(json.pointer("/data/attributes/attributes").is_none());
}

#[test]
fn json_stat_preserves_the_absent_root_name() {
    let root = stat_output(path_entry("/", None));
    let json: serde_json::Value =
        serde_json::from_str(&json_success(&root).expect("JSON stat should render"))
            .expect("rendered stat is JSON");

    assert!(json["data"].get("display_name").is_none());
}

#[test]
fn human_profile_list_renders_default_marker() {
    let output = CommandOutput {
        kind: CommandKind::ProfileList,
        profile: None,
        mode: None,
        data: CommandData::ProfileList {
            default_profile: Some("default".to_owned()),
            profiles: vec![
                ProfileSummary {
                    name: "default".to_owned(),
                    mode: "embedded".to_owned(),
                    store_kind: Some("local_fs".to_owned()),
                },
                ProfileSummary {
                    name: "prod".to_owned(),
                    mode: "remote".to_owned(),
                    store_kind: None,
                },
            ],
        },
    };
    assert_eq!(
        human_success(&output),
        "NAME\tMODE\tSTORE\tDEFAULT\n\
         default\tembedded\tlocal_fs\t*\n\
         prod\tremote\t-\t"
    );
}

#[test]
fn human_profile_show_includes_name() {
    let output = CommandOutput {
        kind: CommandKind::ProfileShow,
        profile: Some("default".to_owned()),
        mode: Some(ProfileMode::Embedded),
        data: CommandData::Profile {
            profile: ProfileConfig {
                identity: ProfileIdentity::default(),
                default_namespace: Some("demo".to_owned()),
                connection: ProfileConnection::Embedded {
                    store: StoreConfig::LocalFs(LocalFsStoreConfig {
                        root: "/tmp/store".to_owned(),
                        key_prefix: None,
                    }),
                    writer_id: None,
                },
            },
            default_profile: None,
        },
    };
    assert_eq!(
        human_success(&output),
        "name = \"default\"\n\
         mode = \"embedded\"\n\
         default_namespace = \"demo\"\n\
         \n\
         [store]\n\
         kind = \"local_fs\"\n\
         root = \"/tmp/store\"\n"
    );
}

#[test]
fn profile_updates_show_store_locations_without_credentials() {
    for (store, location) in [
        (
            serde_json::json!({
                "kind": "aws_s3", "bucket": "files", "region": "us-east-1", "force_path_style": false,
                "key_prefix": "team", "credentials": {
                    "kind": "static", "access_key_id": "secret-id", "secret_access_key": "secret-key"
                }
            }),
            "aws_s3, bucket files, key prefix team",
        ),
        (
            serde_json::json!({
                "kind": "cloudflare_r2", "bucket": "files", "account_id": "account",
                "endpoint_url": "https://account.r2.cloudflarestorage.com",
                "credentials": { "kind": "static", "access_key_id": "secret-id", "secret_access_key": "secret-key" }
            }),
            "cloudflare_r2, bucket files",
        ),
        (
            serde_json::json!({
                "kind": "gcp_gcs", "bucket": "files", "key_prefix": "team",
                "credentials": { "kind": "service_account_file", "path": "/private/key.json" }
            }),
            "gcp_gcs, bucket files, key prefix team",
        ),
    ] {
        let profile = serde_json::json!({ "mode": "embedded", "store": store });
        let output = CommandOutput {
            kind: CommandKind::ProfileUpdate,
            profile: Some("dev".to_owned()),
            mode: Some(ProfileMode::Embedded),
            data: CommandData::Profile {
                profile: serde_json::from_value(profile.clone()).expect("profile"),
                default_profile: None,
            },
        };
        assert_eq!(
            human_success(&output),
            format!("updated profile `dev` ({location})")
        );
        let mut expected = profile;
        expected["kind"] = serde_json::json!("profile");
        let json: serde_json::Value =
            serde_json::from_str(&json_success(&output).expect("render JSON")).expect("JSON");
        assert_eq!(json["data"], expected);
    }
}

#[test]
fn json_error_carries_profile_and_code() {
    let failure = CommandFailure {
        kind: CommandKind::ConfigShow,
        profile: Some("default".to_owned()),
        mode: Some(ProfileMode::Remote),
        error: Box::new(CliError::client_error("connection refused")),
    };
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(
            &json_error(&failure).expect("json error renders")
        )
        .expect("rendered error is valid json"),
        serde_json::json!({
            "error": {
                "code": "client_error",
                "message": "connection refused"
            },
            "format_version": 1,
            "kind": "config_show",
            "mode": "remote",
            "profile": "default"
        })
    );
}

#[test]
fn server_feature_survives_client_and_cli_json() {
    let boundary = r#"{
        "code": "not_supported",
        "feature": "query.grep",
        "message": "grep is not served",
        "request_id": "req_feature"
    }"#;
    let rendered = rendered_remote_error(boundary);
    let expected: serde_json::Value = serde_json::from_str(
        r#"{
            "kind": "config_show",
            "format_version": 1,
            "profile": "remote",
            "mode": "remote",
            "error": {
                "code": "not_supported",
                "feature": "query.grep",
                "message": "grep is not served",
                "request_id": "req_feature"
            }
        }"#,
    )
    .expect("pinned JSON is valid");
    assert_eq!(rendered, expected);
    assert_server_fields_survive(boundary, &rendered);
}

#[test]
fn malformed_request_param_survives_client_and_cli_json() {
    let boundary = r#"{
        "code": "invalid_request",
        "message": "path must be absolute",
        "param": "/operations/0/path",
        "request_id": "req_param",
        "details": { "operation_index": 0 }
    }"#;
    let rendered = rendered_remote_error(boundary);
    let expected: serde_json::Value = serde_json::from_str(
        r#"{
            "kind": "config_show",
            "format_version": 1,
            "profile": "remote",
            "mode": "remote",
            "error": {
                "code": "invalid_request",
                "message": "path must be absolute",
                "param": "/operations/0/path",
                "request_id": "req_param",
                "details": { "operation_index": 0 }
            }
        }"#,
    )
    .expect("pinned JSON is valid");
    assert_eq!(rendered, expected);
    assert_server_fields_survive(boundary, &rendered);
}

#[test]
fn future_error_code_survives_client_and_cli_json() {
    let boundary = r#"{
        "code": "newer_server_code",
        "message": "a newer server failure",
        "param": "limit",
        "request_id": "req_future"
    }"#;
    let rendered = rendered_remote_error(boundary);
    let expected: serde_json::Value = serde_json::from_str(
        r#"{
            "kind": "config_show",
            "format_version": 1,
            "profile": "remote",
            "mode": "remote",
            "error": {
                "code": "newer_server_code",
                "message": "a newer server failure",
                "param": "limit",
                "request_id": "req_future"
            }
        }"#,
    )
    .expect("pinned JSON is valid");
    assert_eq!(rendered, expected);
    assert_server_fields_survive(boundary, &rendered);
}

#[test]
fn cli_parse_error_json_names_the_flag_param() {
    use clap::Parser;

    let clap_error =
        crate::args::Cli::try_parse_from(["loonfs", "--json", "ls", "--limit", "not-a-number"])
            .expect_err("invalid --limit must fail parsing");
    let failure = crate::parse_failure_error(&clap_error);
    let rendered: serde_json::Value = serde_json::from_str(
        &super::json::json_parse_error(&failure).expect("parse error JSON renders"),
    )
    .expect("parse error is valid JSON");

    assert_eq!(
        rendered["error"],
        serde_json::json!({
            "code": "invalid_usage",
            "message": failure.message,
            "param": "--limit"
        })
    );
    assert_eq!(rendered["kind"], "parse_error");
    assert_eq!(rendered["format_version"], 1);
}

#[test]
fn errors_keep_diagnostics_and_show_request_ids_in_json_and_remote_human_output() {
    let mut error = CliError::new("not_supported", "grep is not served");
    error.feature = Some("query.grep".to_owned());
    error.param = Some("/pattern".to_owned());
    error.request_id = Some("req_human".to_owned());

    for mode in [ProfileMode::Remote, ProfileMode::Embedded] {
        let failure = CommandFailure {
            kind: CommandKind::ConfigShow,
            profile: Some("default".to_owned()),
            mode: Some(mode),
            error: Box::new(error.clone()),
        };
        let rendered = human_error(&failure.error, failure.mode);
        let suffix = if mode == ProfileMode::Remote {
            " (request id: req_human)"
        } else {
            ""
        };
        assert_eq!(
            rendered,
            format!("grep is not served{suffix}\nfeature: query.grep\nparam: /pattern")
        );
        let json: serde_json::Value =
            serde_json::from_str(&json_error(&failure).expect("JSON error should render"))
                .expect("rendered error should be valid JSON");
        assert_eq!(json["mode"], mode.as_str());
        assert_eq!(json["error"]["request_id"], "req_human");
    }
}

#[test]
fn doctor_failures_show_request_ids_in_json_and_remote_human_output() {
    let mut output = CommandOutput {
        kind: CommandKind::Doctor,
        profile: Some("remote".to_owned()),
        mode: Some(ProfileMode::Remote),
        data: CommandData::Doctor {
            checks: vec![DoctorCheck {
                name: "auth".to_owned(),
                status: DoctorStatus::Failed,
                message: "token rejected\ncheck the profile".to_owned(),
                request_id: Some("req_doctor".to_owned()),
                store_probe: None,
            }],
        },
    };

    for mode in [ProfileMode::Remote, ProfileMode::Embedded] {
        output.mode = Some(mode);
        let rendered = human_success(&output);
        let suffix = if mode == ProfileMode::Remote {
            " (request id: req_doctor)"
        } else {
            ""
        };
        assert_eq!(
            rendered,
            format!("auth: failed\n  detail: token rejected | check the profile{suffix}")
        );
        let json: serde_json::Value =
            serde_json::from_str(&json_success(&output).expect("doctor JSON should render"))
                .expect("doctor output should be valid JSON");
        assert_eq!(json["mode"], mode.as_str());
        assert_eq!(json["data"]["checks"][0]["request_id"], "req_doctor");
    }
}
#[test]
fn gc_summaries_report_counts_retention_and_namespace_retirement() {
    use super::summaries::gc_summary;
    use loonfs_types::{GcResponse, GrepGcReport, RetainedReason};

    let mut pass = GcResponse::empty(NamespaceId::parse("demo").expect("namespace id"));
    pass.deleted.wal_objects = 2;
    pass.deleted.upload_sessions = 3;
    pass.deleted.content_objects = 1;
    pass.deleted.retired_content_objects = 7;
    pass.deleted_checkpoints_by_owner.fork = 2;
    for _ in 0..4 {
        pass.retain(RetainedReason::WithinGraceWindow);
    }
    pass.retain(RetainedReason::UploadSessionWindow);
    pass.next_reclamation_at_ms = Some(1_700_000_000_000);

    let summary = gc_summary(&pass);
    assert!(
        summary.contains("1 content objects, 7 retired content objects"),
        "{summary}"
    );
    assert!(summary.contains("2 fork checkpoints"), "{summary}");
    assert!(!summary.contains("namespace can be reclaimed"), "{summary}");
    assert!(!summary.contains("grep index"), "{summary}");

    pass.extensions.grep = Some(GrepGcReport {
        deleted_segments: 3,
        deleted_other_objects: 1,
        namespace_reaped: true,
        retained_candidates: 4,
        listed_bytes: 0,
    });
    let summary = gc_summary(&pass);
    assert!(
        summary.ends_with(
            "\ngrep index gc deleted 3 segments, 1 other objects (4 retained); the namespace's grep state was reaped"
        ),
        "{summary}"
    );

    pass.reclaimable_at_ms = Some(1_700_000_000_000);
    for next in [
        Some(1_699_999_999_000),
        Some(1_700_000_000_000),
        None,
        Some(1_700_000_001_000),
    ] {
        pass.next_reclamation_at_ms = next;
        assert_eq!(
            gc_summary(&pass).lines().last(),
            Some("namespace can be reclaimed at or after 2023-11-14 22:13:20Z after pin cleanup; only fork pins can hold it beyond this deadline")
        );
    }
    let empty = gc_summary(&GcResponse::empty(
        NamespaceId::parse("demo").expect("namespace id"),
    ));
    assert!(
        empty.contains("0 content objects, 0 retired content objects"),
        "{empty}"
    );
    assert!(!empty.contains("namespace is retired"), "{empty}");
    assert!(empty.contains("0 fork checkpoints, 0 snapshots"), "{empty}");
}
