//! Profile-scoped capability discovery and read-only diagnostics.

use super::context::{fail, fail_for};
use super::output::{CommandData, CommandFailure, CommandOutput, DoctorCheck, DoctorStatus};
use crate::args::{CapabilitiesArgs, CommandKind, DoctorArgs};
use crate::backend_error::BackendError;
use crate::config::{
    load_config, non_empty_env, resolve_config_location, ConfigSource, ProfileConfig, StoreConfig,
    PROFILE_ENV,
};
use crate::error::CliError;
use crate::profiles::resolve_profile;
use crate::resolve::{resolve_namespace, resolve_target_profile, ResolvedTarget};
use loonfs_api::v0::{StoreProbeCheckOutcome, StoreProbeResponse};
use loonfs_api::{CapabilityDocument, PROTOCOL_VERSION};
use std::path::Path;

pub(crate) const DOCTOR_CHECK_NAMES: [&str; 9] = [
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
pub(crate) const WRITE_CHECK_NAME: &str = "store_probe";

pub(crate) async fn run_capabilities(
    kind: CommandKind,
    config_path: &Path,
    args: CapabilitiesArgs,
) -> Result<CommandOutput, CommandFailure> {
    let explicit_profile = args.profile.profile.as_deref();
    let resolved = resolve_target_profile(config_path, explicit_profile, args.request.no_retry)
        .await
        .map_err(|error| fail(kind, explicit_profile.map(ToOwned::to_owned), None, error))?;
    let mode = resolved.target.mode_str().to_owned();
    let document = resolved
        .target
        .capabilities()
        .await
        .map_err(|error| fail_for(kind, &resolved.profile_name, &mode, error))?;

    Ok(CommandOutput {
        kind,
        profile: Some(resolved.profile_name),
        mode: Some(mode),
        data: CommandData::Capabilities(document),
    })
}

/// Runs every diagnostic it can reach and returns the checks as data even
/// when one failed. [`CommandData::reports_failures`] turns that data into a
/// nonzero process status after the renderer has printed all of it.
pub(crate) async fn run_doctor(
    kind: CommandKind,
    config_flag: Option<&Path>,
    args: &DoctorArgs,
) -> Result<CommandOutput, CommandFailure> {
    let mut checks = Vec::with_capacity(if args.write_check { 10 } else { 9 });

    let location = match resolve_config_location(config_flag) {
        Ok(location) => {
            checks.push(ok(
                DOCTOR_CHECK_NAMES[0],
                config_resolution_message(&location),
            ));
            location
        }
        Err(error) => {
            checks.push(failed(DOCTOR_CHECK_NAMES[0], error));
            skip_remaining(
                &mut checks,
                1,
                args.write_check,
                "config path resolution failed",
            );
            return Ok(doctor_output(kind, None, None, checks));
        }
    };

    let config = match load_config(&location.path) {
        Ok(config) => {
            checks.push(ok(
                DOCTOR_CHECK_NAMES[1],
                format!("strictly decoded and validated {}", location.path.display()),
            ));
            config
        }
        Err(error) => {
            checks.push(failed(DOCTOR_CHECK_NAMES[1], error));
            skip_remaining(&mut checks, 2, args.write_check, "config did not decode");
            return Ok(doctor_output(kind, None, None, checks));
        }
    };

    let explicit_profile = args.target.profile.profile.as_deref();
    let (profile_name, profile) = match resolve_profile(&config, explicit_profile) {
        Ok((name, profile)) => {
            checks.push(ok(
                DOCTOR_CHECK_NAMES[2],
                format!(
                    "profile `{name}` selected via {}",
                    profile_selection_source(explicit_profile)
                ),
            ));
            (name.to_owned(), profile.clone())
        }
        Err(error) => {
            checks.push(failed(DOCTOR_CHECK_NAMES[2], error));
            skip_remaining(&mut checks, 3, args.write_check, "profile selection failed");
            return Ok(doctor_output(
                kind,
                explicit_profile.map(ToOwned::to_owned),
                None,
                checks,
            ));
        }
    };
    let mode = profile.mode_str().to_owned();

    let mut target = match provider_check(&profile, args.target.request.no_retry).await {
        Ok((message, target)) => {
            checks.push(ok(DOCTOR_CHECK_NAMES[3], message));
            target
        }
        Err(error) => {
            checks.push(failed(DOCTOR_CHECK_NAMES[3], error));
            skip_remaining(
                &mut checks,
                4,
                args.write_check,
                "provider configuration is unusable",
            );
            return Ok(doctor_output(kind, Some(profile_name), Some(mode), checks));
        }
    };

    let remote = matches!(&profile, ProfileConfig::Remote { .. });
    if remote {
        let result = target
            .as_ref()
            .expect("remote provider validation constructs a target")
            .remote_connectivity()
            .await;
        checks.push(check_from_backend_result(
            DOCTOR_CHECK_NAMES[4],
            "DNS, TCP, and TLS connectivity succeeded",
            result,
        ));
    } else {
        checks.push(skipped(
            DOCTOR_CHECK_NAMES[4],
            "embedded profiles do not use a network connection",
        ));
    }

    let mut remote_capabilities = None;
    if remote {
        let result = target
            .as_ref()
            .expect("remote provider validation constructs a target")
            .capabilities()
            .await;
        checks.push(match &result {
            Ok(_) => ok(
                DOCTOR_CHECK_NAMES[5],
                "authenticated capabilities request succeeded",
            ),
            Err(error) => failed(DOCTOR_CHECK_NAMES[5], CliError::from(error.clone())),
        });
        remote_capabilities = Some(result);
    } else {
        checks.push(skipped(
            DOCTOR_CHECK_NAMES[5],
            "embedded profiles do not authenticate to a server",
        ));
    }

    if remote {
        let result = target
            .as_ref()
            .expect("remote provider validation constructs a target")
            .remote_health()
            .await;
        checks.push(check_from_backend_result(
            DOCTOR_CHECK_NAMES[6],
            "server health endpoint answered successfully",
            result,
        ));
    } else if local_root_is_missing(&profile) && !args.write_check {
        checks.push(skipped(
            DOCTOR_CHECK_NAMES[6],
            "opening this local store would create its missing root; read-only doctor left it untouched",
        ));
    } else {
        match ResolvedTarget::resolve(&profile, args.target.request.no_retry).await {
            Ok(resolved) => {
                target = Some(resolved);
                checks.push(ok(
                    DOCTOR_CHECK_NAMES[6],
                    "embedded object store opened without an object write",
                ));
            }
            Err(error) => checks.push(failed(DOCTOR_CHECK_NAMES[6], error)),
        }
    }

    let capability_result = if remote {
        remote_capabilities.expect("remote auth check records its capability result")
    } else if let Some(target) = &target {
        target.capabilities().await
    } else {
        checks.push(skipped(
            DOCTOR_CHECK_NAMES[7],
            "embedded store was not opened read-only",
        ));
        namespace_check(
            &mut checks,
            &config,
            explicit_profile,
            args,
            target.as_ref(),
        )
        .await;
        append_write_check(&mut checks, args.write_check, target.as_ref()).await;
        return Ok(doctor_output(kind, Some(profile_name), Some(mode), checks));
    };
    checks.push(capability_document_check(capability_result));

    namespace_check(
        &mut checks,
        &config,
        explicit_profile,
        args,
        target.as_ref(),
    )
    .await;
    append_write_check(&mut checks, args.write_check, target.as_ref()).await;

    Ok(doctor_output(kind, Some(profile_name), Some(mode), checks))
}

async fn provider_check(
    profile: &ProfileConfig,
    no_retry: bool,
) -> Result<(String, Option<ResolvedTarget>), CliError> {
    match profile {
        ProfileConfig::Embedded { store, .. } => {
            store.validate().map_err(|error| {
                CliError::invalid_config(format!("invalid embedded store config: {error}"))
            })?;
            Ok((
                format!("{} store configuration is valid", store.kind().as_str()),
                None,
            ))
        }
        ProfileConfig::Remote { server_url, .. } => {
            let target = ResolvedTarget::resolve(profile, no_retry).await?;
            Ok((
                format!("server URL and TLS configuration parsed for {server_url}"),
                Some(target),
            ))
        }
    }
}

async fn namespace_check(
    checks: &mut Vec<DoctorCheck>,
    config: &crate::config::CliConfig,
    explicit_profile: Option<&str>,
    args: &DoctorArgs,
    target: Option<&ResolvedTarget>,
) {
    let namespace =
        match resolve_namespace(config, explicit_profile, args.target.namespace.as_deref()) {
            Ok(resolved) => resolved.namespace,
            Err(error) if error.is_no_default_namespace() => {
                checks.push(skipped(DOCTOR_CHECK_NAMES[8], "no namespace is selected"));
                return;
            }
            Err(error) => {
                checks.push(failed(DOCTOR_CHECK_NAMES[8], error));
                return;
            }
        };
    let Some(target) = target else {
        checks.push(skipped(
            DOCTOR_CHECK_NAMES[8],
            "the selected provider is unavailable",
        ));
        return;
    };
    let result = target.namespace_status(&namespace).await;
    checks.push(check_from_backend_result(
        DOCTOR_CHECK_NAMES[8],
        format!("namespace `{namespace}` is reachable"),
        result.map(|_| ()),
    ));
}

async fn append_write_check(
    checks: &mut Vec<DoctorCheck>,
    requested: bool,
    target: Option<&ResolvedTarget>,
) {
    if !requested {
        return;
    }
    let Some(target) = target else {
        checks.push(skipped(
            WRITE_CHECK_NAME,
            "the selected provider is unavailable",
        ));
        return;
    };
    match target.probe_store().await {
        Ok(response) => checks.push(store_probe_check(response)),
        Err(error) => checks.push(failed(WRITE_CHECK_NAME, CliError::from(error))),
    }
}

fn capability_document_check(result: Result<CapabilityDocument, BackendError>) -> DoctorCheck {
    let document = match result {
        Ok(document) => document,
        Err(error) => return failed(DOCTOR_CHECK_NAMES[7], CliError::from(error)),
    };
    if let Err(error) = document.validate() {
        return failed_message(
            DOCTOR_CHECK_NAMES[7],
            format!("capability document is not well-formed: {error}"),
        );
    }
    if document.protocol_version != PROTOCOL_VERSION {
        return failed_message(
            DOCTOR_CHECK_NAMES[7],
            format!(
                "server protocol `{}` is not supported by this CLI (speaks `{PROTOCOL_VERSION}`)",
                document.protocol_version
            ),
        );
    }
    ok(
        DOCTOR_CHECK_NAMES[7],
        format!("well-formed capability document speaks `{PROTOCOL_VERSION}`"),
    )
}

fn store_probe_check(response: StoreProbeResponse) -> DoctorCheck {
    let failed_count = response
        .checks
        .iter()
        .filter(|check| check.outcome == StoreProbeCheckOutcome::Failed)
        .count();
    let unsupported_count = response
        .checks
        .iter()
        .filter(|check| check.outcome == StoreProbeCheckOutcome::Unsupported)
        .count();
    let status = if failed_count > 0 {
        DoctorStatus::Failed
    } else if unsupported_count > 0 {
        DoctorStatus::Warning
    } else {
        DoctorStatus::Ok
    };
    let message = if failed_count > 0 {
        format!(
            "store probe {}: {failed_count} of {} checks failed",
            response.run_id,
            response.checks.len()
        )
    } else if unsupported_count > 0 {
        format!(
            "store probe {}: {unsupported_count} of {} checks unsupported",
            response.run_id,
            response.checks.len()
        )
    } else {
        format!(
            "store probe {}: {} checks passed",
            response.run_id,
            response.checks.len()
        )
    };
    DoctorCheck {
        name: WRITE_CHECK_NAME.to_owned(),
        status,
        message,
        request_id: None,
        store_probe: Some(response),
    }
}

fn local_root_is_missing(profile: &ProfileConfig) -> bool {
    matches!(
        profile,
        ProfileConfig::Embedded {
            store: StoreConfig::LocalFs { root, .. },
            ..
        } if !Path::new(root).exists()
    )
}

fn config_resolution_message(location: &crate::config::ConfigLocation) -> String {
    match location.source {
        ConfigSource::Flag => format!("resolved {} from --config", location.path.display()),
        ConfigSource::Env => format!("resolved {} from LOONFS_CONFIG", location.path.display()),
        ConfigSource::Xdg | ConfigSource::Legacy => {
            format!("defaulted to {}", location.path.display())
        }
    }
}

fn profile_selection_source(explicit_profile: Option<&str>) -> &'static str {
    if explicit_profile.is_some() {
        "--profile"
    } else if non_empty_env(PROFILE_ENV).is_some() {
        "LOONFS_PROFILE"
    } else {
        "the configured default"
    }
}

fn check_from_backend_result<T>(
    name: &str,
    success_message: impl Into<String>,
    result: Result<T, BackendError>,
) -> DoctorCheck {
    match result {
        Ok(_) => ok(name, success_message),
        Err(error) => failed(name, CliError::from(error)),
    }
}

fn ok(name: &str, message: impl Into<String>) -> DoctorCheck {
    DoctorCheck {
        name: name.to_owned(),
        status: DoctorStatus::Ok,
        message: message.into(),
        request_id: None,
        store_probe: None,
    }
}

fn skipped(name: &str, message: impl Into<String>) -> DoctorCheck {
    DoctorCheck {
        name: name.to_owned(),
        status: DoctorStatus::Skipped,
        message: message.into(),
        request_id: None,
        store_probe: None,
    }
}

fn failed(name: &str, error: CliError) -> DoctorCheck {
    DoctorCheck {
        name: name.to_owned(),
        status: DoctorStatus::Failed,
        message: error.message,
        request_id: error.request_id,
        store_probe: None,
    }
}

fn failed_message(name: &str, message: impl Into<String>) -> DoctorCheck {
    DoctorCheck {
        name: name.to_owned(),
        status: DoctorStatus::Failed,
        message: message.into(),
        request_id: None,
        store_probe: None,
    }
}

fn skip_remaining(checks: &mut Vec<DoctorCheck>, first: usize, write_check: bool, reason: &str) {
    checks.extend(
        DOCTOR_CHECK_NAMES[first..]
            .iter()
            .map(|name| skipped(name, reason)),
    );
    if write_check {
        checks.push(skipped(WRITE_CHECK_NAME, reason));
    }
}

fn doctor_output(
    kind: CommandKind,
    profile: Option<String>,
    mode: Option<String>,
    checks: Vec<DoctorCheck>,
) -> CommandOutput {
    CommandOutput {
        kind,
        profile,
        mode,
        data: CommandData::Doctor { checks },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use loonfs_api::v0::StoreProbeCheckResult;
    use std::collections::BTreeMap;

    fn capability_document(protocol_version: &str) -> CapabilityDocument {
        CapabilityDocument {
            protocol_version: protocol_version.to_owned(),
            profiles: vec!["core/v0".to_owned()],
            features: BTreeMap::new(),
            limits: BTreeMap::new(),
        }
    }

    #[test]
    fn doctor_check_names_are_pinned_in_execution_order() {
        assert_eq!(
            DOCTOR_CHECK_NAMES,
            [
                "config",
                "config_decode",
                "profile",
                "provider_config",
                "connectivity",
                "auth",
                "health",
                "capabilities",
                "namespace",
            ]
        );
        assert_eq!(WRITE_CHECK_NAME, "store_probe");
    }

    #[test]
    fn capabilities_check_pins_the_protocol_version() {
        assert_eq!(
            capability_document_check(Ok(capability_document(PROTOCOL_VERSION))).status,
            DoctorStatus::Ok
        );
        let failed = capability_document_check(Ok(capability_document("v-next")));
        assert_eq!(failed.status, DoctorStatus::Failed);
        assert!(failed.message.contains("not supported"));
    }

    #[test]
    fn store_probe_failure_remains_nested_data_and_fails_doctor() {
        let response = StoreProbeResponse {
            run_id: "probe_test".to_owned(),
            checks: vec![StoreProbeCheckResult {
                name: "compare_and_swap".to_owned(),
                outcome: StoreProbeCheckOutcome::Failed,
                message: Some("replacement was not atomic".to_owned()),
            }],
        };
        let check = store_probe_check(response.clone());
        assert_eq!(check.status, DoctorStatus::Failed);
        assert_eq!(check.store_probe, Some(response));
    }
}
