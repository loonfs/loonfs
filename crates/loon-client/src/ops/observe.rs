use crate::local_fs::{
    observe_delete_path as client_observe_delete_path,
    observe_local_path as client_observe_local_path, observe_move_path as client_observe_move_path,
    observe_subtree_path as client_observe_subtree_path,
};
use crate::ops::OpsConfig;
use anyhow::Result;
use loon_types::NamespaceId;
use std::path::Path;

pub use crate::local_fs::{
    ObserveDeleteError, ObserveDeleteReport, ObserveLocalError, ObserveLocalReport,
    ObserveMoveError, ObserveMoveReport, ObserveSubtreeError, ObserveSubtreeReport,
    ObservedPathKind,
};

pub fn observe_local_path(
    config: &OpsConfig,
    namespace_id: &NamespaceId,
    path: &Path,
) -> Result<ObserveLocalReport, ObserveLocalError> {
    let cwd = std::env::current_dir()?;
    client_observe_local_path(
        &config.client.state_db_path,
        namespace_id,
        &config.client.mirror_root,
        &cwd,
        path,
        config.now_ms(),
    )
}

pub fn observe_delete_path(
    config: &OpsConfig,
    namespace_id: &NamespaceId,
    path: &Path,
) -> Result<ObserveDeleteReport, ObserveDeleteError> {
    let cwd = std::env::current_dir()?;
    client_observe_delete_path(
        &config.client.state_db_path,
        namespace_id,
        &config.client.mirror_root,
        &cwd,
        path,
        config.now_ms(),
    )
}

pub fn observe_move_path(
    config: &OpsConfig,
    namespace_id: &NamespaceId,
    from: &Path,
    to: &Path,
) -> Result<ObserveMoveReport, ObserveMoveError> {
    let cwd = std::env::current_dir()?;
    client_observe_move_path(
        &config.client.state_db_path,
        namespace_id,
        &config.client.mirror_root,
        &cwd,
        from,
        to,
        config.now_ms(),
    )
}

pub fn observe_subtree_path(
    config: &OpsConfig,
    namespace_id: &NamespaceId,
    path: &Path,
) -> Result<ObserveSubtreeReport, ObserveSubtreeError> {
    let cwd = std::env::current_dir()?;
    client_observe_subtree_path(
        &config.client.state_db_path,
        namespace_id,
        &config.client.mirror_root,
        &cwd,
        path,
        config.now_ms(),
    )
}

pub(crate) fn render_observe_local_report(report: &ObserveLocalReport) -> Result<String> {
    let yaml = serde_yaml::to_string(report)?;
    Ok(format!(
        "command=ops/observe-local\nnamespace={}\nrelative_path={}\nobservation_kind={}\nplanned_decision={}\n---\n{}",
        report.namespace_id.as_str(),
        report.relative_path,
        report.observation_kind.as_str(),
        report.planned_decision,
        yaml
    ))
}

pub(crate) fn render_observe_delete_report(report: &ObserveDeleteReport) -> Result<String> {
    let yaml = serde_yaml::to_string(report)?;
    Ok(format!(
        "command=ops/observe-delete\nnamespace={}\nrelative_path={}\nobservation_kind={}\nplanned_decision={}\n---\n{}",
        report.namespace_id.as_str(),
        report.relative_path,
        report.observation_kind.as_str(),
        report.planned_decision,
        yaml
    ))
}

pub(crate) fn render_observe_move_report(report: &ObserveMoveReport) -> Result<String> {
    let yaml = serde_yaml::to_string(report)?;
    Ok(format!(
        "command=ops/observe-move\nnamespace={}\nfrom_relative_path={}\nto_relative_path={}\nobservation_kind={}\nplanned_decision={}\n---\n{}",
        report.namespace_id.as_str(),
        report.from_relative_path,
        report.to_relative_path,
        report.observation_kind.as_str(),
        report.planned_decision,
        yaml
    ))
}

pub(crate) fn render_observe_subtree_report(report: &ObserveSubtreeReport) -> Result<String> {
    let yaml = serde_yaml::to_string(report)?;
    Ok(format!(
        "command=ops/observe-subtree\nnamespace={}\nrelative_path={}\nscanned_file_count={}\nscanned_dir_count={}\napplied_operation_count={}\n---\n{}",
        report.namespace_id.as_str(),
        report.relative_path,
        report.scanned_file_count,
        report.scanned_dir_count,
        report.applied_operation_count,
        yaml
    ))
}
