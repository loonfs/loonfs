//! Renders command outcomes as human-readable text or `--json`.

mod human;
mod json;
mod summaries;

use crate::args::CommandKind;
use crate::commands::{
    CommandData, CommandFailure, CommandOutput, DoctorCheck, DoctorStatus, ListingHeadDrift,
    MaintenanceKeyReport,
};
use crate::config::ConfigSource;
use crate::error::CliError;
use loonfs_api::v0::{GrepIndexLifecycle, StoreProbeCheckOutcome, StoreProbeCheckResult};
use loonfs_api::{
    AttributeValue, CheckpointOwnerSummary, GcResponse, NamespaceId, ReorganizeStepOutcome,
    WalFlushStepOutcome,
};
use serde::Serialize;
use std::io::{self, Write};

pub(crate) use human::{human_path_entry, human_success};
pub(crate) use json::{json_error, json_success, render_parse_error};
pub(crate) use summaries::{
    gc_pass_line, store_probe_summary_line, store_probe_verdict, StoreProbeVerdict,
};

pub(crate) fn render_success(output: &CommandOutput, json_mode: bool) -> io::Result<()> {
    if json_mode {
        let body = json_success(output)?;
        let mut stdout = io::stdout().lock();
        stdout.write_all(body.as_bytes())?;
        stdout.write_all(b"\n")?;
        return Ok(());
    }

    match &output.data {
        CommandData::CompletionScript(bytes) | CommandData::StreamBytes(bytes) => {
            let mut stdout = io::stdout().lock();
            stdout.write_all(bytes)?;
        }
        // Already written, chunk by chunk, by the command itself.
        CommandData::StreamedToStdout => {}
        _ => {
            let rendered = human_success(output);
            let mut stdout = io::stdout().lock();
            stdout.write_all(rendered.as_bytes())?;
            if !rendered.ends_with('\n') {
                stdout.write_all(b"\n")?;
            }
        }
    }
    Ok(())
}

pub(crate) fn render_error(failure: &CommandFailure, json_mode: bool) -> io::Result<()> {
    let mut stderr = io::stderr().lock();
    if json_mode {
        let body = json_error(failure)?;
        stderr.write_all(body.as_bytes())?;
        stderr.write_all(b"\n")?;
    } else {
        stderr.write_all(human_error(&failure.error).as_bytes())?;
        stderr.write_all(b"\n")?;
    }
    Ok(())
}

fn human_error(error: &CliError) -> String {
    let mut rendered = error.message.clone();
    rendered.push_str(&request_id_suffix(error.request_id.as_deref()));
    if let Some(feature) = &error.feature {
        rendered.push_str(&format!("\nfeature: {feature}"));
    }
    if let Some(param) = &error.param {
        rendered.push_str(&format!("\nparam: {param}"));
    }
    rendered
}

fn request_id_suffix(request_id: Option<&str>) -> String {
    request_id.map_or_else(String::new, |request_id| {
        format!(" (request id: {request_id})")
    })
}

pub(crate) fn write_stderr_warning(message: impl std::fmt::Display) {
    let _ = writeln!(io::stderr().lock(), "warning: {message}");
}

/// Per-item progress for long-running recursive transfers, on stderr so
/// stdout stays the machine-readable summary.
pub(crate) fn write_stderr_progress(message: impl std::fmt::Display) {
    let _ = writeln!(io::stderr().lock(), "{message}");
}

pub(crate) fn listing_drift_warning(drift: &ListingHeadDrift) -> String {
    format!(
        "namespace advanced during the listing (head seq {} to {}); entries may mix states; re-run for a settled view",
        drift.first_head_seq.0, drift.last_head_seq.0
    )
}

pub(crate) fn write_listing_drift_warning(drift: &ListingHeadDrift) {
    write_stderr_warning(listing_drift_warning(drift));
}

pub(crate) fn more_entries_hint(cursor: &str) -> String {
    format!("more entries exist; continue with --cursor {cursor} or stream everything with --all")
}

#[cfg(test)]
mod tests {
    use super::{human_error, human_success, json_error, json_success, AttributeValue};
    use crate::args::CommandKind;
    use crate::commands::{CommandData, CommandFailure, CommandOutput, DoctorCheck, DoctorStatus};
    use crate::config::{ProfileConfig, StoreConfig};
    use crate::error::CliError;
    use crate::profiles::ProfileSummary;
    use insta::{assert_json_snapshot, assert_snapshot};
    use loonfs_api::v0::{StoreProbeCheckOutcome, StoreProbeCheckResult, StoreProbeResponse};
    use loonfs_api::{
        AbsolutePath, AttributesProjection, ChangeSeq, DisplayName, InodeId, NamespaceId,
        PathEntry, PathEntryKind,
    };
    use loonfs_client::ClientError;

    const POISON_PROVIDER_DETAIL: &str = "<Error>AccessDenied</Error> \
        arn:aws:iam::123456789012:role/private-role private-bucket \
        namespaces/customer-a/head.json x-amz-request-id=provider-request \
        x-amz-id-2=provider-host-id AKIAEXAMPLE";

    fn assert_provider_markers_absent(rendered: &str) {
        for marker in [
            "<Error>AccessDenied</Error>",
            "arn:aws:iam::123456789012:role/private-role",
            "private-bucket",
            "namespaces/customer-a/head.json",
            "x-amz-request-id=provider-request",
            "x-amz-id-2=provider-host-id",
            "AKIAEXAMPLE",
        ] {
            assert!(
                !rendered.contains(marker),
                "rendered output leaked {marker}: {rendered}"
            );
        }
    }

    fn poison_permission_runtime_error() -> loonfs::RuntimeError {
        loonfs::RuntimeError::Core(loonfs::CoreError::Store {
            object_key: "namespaces/customer-a/head.json".to_owned(),
            message: POISON_PROVIDER_DETAIL.to_owned(),
            class: loonfs::StoreFailureClass::PermissionDenied,
        })
    }

    fn rendered_remote_error(server_boundary: &str) -> serde_json::Value {
        let body: loonfs_api::ApiError =
            serde_json::from_str(server_boundary).expect("server boundary is valid JSON");
        let error = CliError::from(crate::backend_error::BackendError::from(
            ClientError::from_api_error(400, body),
        ));
        let failure = CommandFailure {
            kind: CommandKind::ConfigShow,
            profile: Some("remote".to_owned()),
            mode: Some("remote".to_owned()),
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
            object_key: "namespaces/customer-a/head.json".to_owned(),
            message: POISON_PROVIDER_DETAIL.to_owned(),
        }
        .public_message()
        .into_owned();

        let embedded_failure = CommandFailure {
            kind: CommandKind::ConfigShow,
            profile: Some("embedded".to_owned()),
            mode: Some("embedded".to_owned()),
            error: Box::new(CliError::from(crate::backend_error::map_runtime_error(
                poison_permission_runtime_error(),
            ))),
        };
        let embedded_human = human_error(&embedded_failure.error);
        let embedded_json = json_error(&embedded_failure).expect("embedded JSON error renders");
        for rendered in [&embedded_human, &embedded_json] {
            assert_provider_markers_absent(rendered);
            assert!(rendered.contains(&public_message), "{rendered}");
        }

        let remote_boundary = serde_json::to_string(&loonfs_api::ApiError {
            code: loonfs_api::ErrorCode::StoragePermissionDenied
                .as_str()
                .to_owned(),
            feature: None,
            message: public_message.clone(),
            param: None,
            request_id: Some("req_provider_hygiene".to_owned()),
            details: None,
        })
        .expect("remote boundary serializes");
        let body: loonfs_api::ApiError =
            serde_json::from_str(&remote_boundary).expect("remote boundary is valid JSON");
        let remote_failure = CommandFailure {
            kind: CommandKind::ConfigShow,
            profile: Some("remote".to_owned()),
            mode: Some("remote".to_owned()),
            error: Box::new(CliError::from(crate::backend_error::BackendError::from(
                ClientError::from_api_error(503, body),
            ))),
        };
        let remote_human = human_error(&remote_failure.error);
        let remote_json = json_error(&remote_failure).expect("remote JSON error renders");
        for rendered in [&remote_human, &remote_json] {
            assert_provider_markers_absent(rendered);
            assert!(rendered.contains(&public_message), "{rendered}");
            assert!(rendered.contains("req_provider_hygiene"), "{rendered}");
        }

        let checks = vec![
            StoreProbeCheckResult {
                name: "create_if_absent_enforced".to_owned(),
                outcome: StoreProbeCheckOutcome::Failed,
                message: Some(format!("create failed: {public_message}")),
            },
            StoreProbeCheckResult {
                name: "compare_and_swap_rejects_stale".to_owned(),
                outcome: StoreProbeCheckOutcome::Failed,
                message: Some(format!(" create   failed:  {public_message}\n")),
            },
            StoreProbeCheckResult {
                name: "stored_checksum_readback".to_owned(),
                outcome: StoreProbeCheckOutcome::Failed,
                message: Some(format!("checksum head failed: {public_message}")),
            },
            StoreProbeCheckResult {
                name: "range_reads".to_owned(),
                outcome: StoreProbeCheckOutcome::Failed,
                message: Some(
                    "range read failed: object-store is temporarily unavailable; retry the operation"
                        .to_owned(),
                ),
            },
        ];
        let doctor = CommandOutput {
            kind: CommandKind::Doctor,
            profile: Some("embedded".to_owned()),
            mode: Some("embedded".to_owned()),
            data: CommandData::Doctor {
                checks: vec![DoctorCheck {
                    name: "store_probe".to_owned(),
                    status: DoctorStatus::Failed,
                    message: "store probe probe_poison: 4 of 4 checks failed".to_owned(),
                    request_id: None,
                    store_probe: Some(StoreProbeResponse {
                        run_id: "probe_poison".to_owned(),
                        checks: checks.clone(),
                    }),
                }],
            },
        };
        let doctor_human = human_success(&doctor);
        let doctor_json = json_success(&doctor).expect("doctor JSON renders");
        for rendered in [&doctor_human, &doctor_json] {
            assert_provider_markers_absent(rendered);
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
            created_by: loonfs_api::ActorRef::loonfs_system(),
            created_at_ms: 1_752_624_000_000,
            kind: PathEntryKind::Directory {},
            head_seq: ChangeSeq(3),
            parent_inode_id: display_name.map(|_| InodeId(1)),
            display_name: display_name.map(|name| DisplayName::parse(name).expect("display name")),
            binding_generation: None,
            attributes: None,
        }
    }

    fn stat_output(entry: PathEntry) -> CommandOutput {
        CommandOutput {
            kind: CommandKind::FilesystemStat,
            profile: Some("default".to_owned()),
            mode: Some("embedded".to_owned()),
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
            "created_by: system:loonfs",
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
            attributes_revision_no: loonfs_api::AttributeRevisionNo(1),
            attributes_updated_by: Some(loonfs_api::ActorRef::loonfs_system()),
            attributes_updated_at_ms: Some(1_752_624_000_000),
            attributes: loonfs_api::Attributes::new(std::collections::BTreeMap::from([(
                loonfs_api::AttributeKey::parse("note").expect("attribute key"),
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

        let tabbed =
            stat_with_attribute(AttributeValue::parse("a\tb\rc").expect("attribute value"));
        assert_eq!(attribute_line(&tabbed), "attr.note: a\\tb\\rc");
    }

    #[test]
    fn human_stat_leaves_ordinary_unicode_alone() {
        let unicode = stat_with_attribute(
            AttributeValue::parse("café ☃ 日本語 🙂").expect("attribute value"),
        );
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
                        store_kind: Some("local-fs".to_owned()),
                    },
                    ProfileSummary {
                        name: "prod".to_owned(),
                        mode: "remote".to_owned(),
                        store_kind: None,
                    },
                ],
            },
        };
        assert_snapshot!(human_success(&output));
    }

    #[test]
    fn human_profile_show_includes_name() {
        let output = CommandOutput {
            kind: CommandKind::ProfileShow,
            profile: Some("default".to_owned()),
            mode: Some("embedded".to_owned()),
            data: CommandData::Profile(ProfileConfig::Embedded {
                store: StoreConfig::LocalFs {
                    root: "/tmp/store".to_owned(),
                    key_prefix: None,
                },
                actor: crate::config::ProfileActorConfig::default(),
                default_namespace: Some("demo".to_owned()),
                writer_id: None,
            }),
        };
        assert_snapshot!(human_success(&output));
    }

    #[test]
    fn json_error_carries_profile_and_code() {
        let failure = CommandFailure {
            kind: CommandKind::ConfigShow,
            profile: Some("default".to_owned()),
            mode: Some("remote".to_owned()),
            error: Box::new(CliError::from(
                crate::backend_error::BackendError::client_error("connection refused"),
            )),
        };
        assert_json_snapshot!(serde_json::from_str::<serde_json::Value>(
            &json_error(&failure).expect("json error renders")
        )
        .expect("rendered error is valid json"));
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
    fn remote_request_id_is_absent_from_equivalent_embedded_error() {
        let remote_boundary = r#"{
            "code": "invalid_request",
            "message": "limit must be greater than zero",
            "param": "limit",
            "request_id": "req_remote"
        }"#;
        let remote = rendered_remote_error(remote_boundary);
        let embedded_error = CliError::from(
            crate::backend_error::BackendError::new(
                loonfs_api::ErrorCode::InvalidRequest.as_str(),
                "limit must be greater than zero",
            )
            .with_param("limit"),
        );
        let embedded_failure = CommandFailure {
            kind: CommandKind::ConfigShow,
            profile: Some("embedded".to_owned()),
            mode: Some("embedded".to_owned()),
            error: Box::new(embedded_error),
        };
        let embedded: serde_json::Value = serde_json::from_str(
            &json_error(&embedded_failure).expect("embedded JSON error renders"),
        )
        .expect("embedded error is valid JSON");
        let expected_embedded: serde_json::Value = serde_json::from_str(
            r#"{
                "kind": "config_show",
                "format_version": 1,
                "profile": "embedded",
                "mode": "embedded",
                "error": {
                    "code": "invalid_request",
                    "message": "limit must be greater than zero",
                    "param": "limit"
                }
            }"#,
        )
        .expect("pinned JSON is valid");

        assert_eq!(remote["error"]["request_id"], "req_remote");
        assert_eq!(embedded, expected_embedded);
        assert!(embedded["error"].get("request_id").is_none());
        assert_server_fields_survive(remote_boundary, &remote);
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
    fn human_errors_render_feature_and_param_diagnostics() {
        let mut error = CliError::new("not_supported", "grep is not served");
        error.feature = Some("query.grep".to_owned());
        error.param = Some("/pattern".to_owned());
        error.request_id = Some("req_human".to_owned());

        let rendered = human_error(&error);
        assert!(rendered.contains("grep is not served"), "{rendered}");
        assert!(rendered.contains("request id: req_human"), "{rendered}");
        assert!(rendered.contains("\nfeature: query.grep"), "{rendered}");
        assert!(rendered.contains("\nparam: /pattern"), "{rendered}");
    }

    #[test]
    fn human_doctor_failure_has_one_detail_line_with_the_request_id() {
        let output = CommandOutput {
            kind: CommandKind::Doctor,
            profile: Some("remote".to_owned()),
            mode: Some("remote".to_owned()),
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

        let rendered = human_success(&output);
        assert_eq!(rendered.lines().count(), 2, "{rendered}");
        assert!(rendered.contains("auth: failed"), "{rendered}");
        assert!(
            rendered.contains("token rejected | check the profile"),
            "{rendered}"
        );
        assert!(rendered.contains("request id: req_doctor"), "{rendered}");
    }
}
