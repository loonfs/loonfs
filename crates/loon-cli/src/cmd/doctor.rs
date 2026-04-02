use crate::cmd::config::resolve_config_path;
use anyhow::{anyhow, Result};
use clap::Args;
use loon_client::ops::OpsConfig;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DoctorCommand {
    pub config_path: Option<PathBuf>,
}

#[derive(Debug, Clone, Args)]
#[command(after_help = "Examples:\n  loon doctor\n  loon doctor --config ./loondb-demo.local.toml")]
pub struct DoctorArgs {
    #[arg(
        long,
        value_name = "PATH",
        help = "Optional path to the ops TOML config file. If omitted, the same discovery rules as `loon config` are used."
    )]
    config: Option<PathBuf>,
}

impl DoctorArgs {
    pub(crate) fn into_command(self) -> DoctorCommand {
        DoctorCommand {
            config_path: self.config,
        }
    }
}

pub(crate) fn render_doctor(command: DoctorCommand) -> Result<String> {
    let resolved = resolve_config_path(command.config_path)?;
    let mut lines = vec![
        "command=doctor".to_string(),
        format!("config_path={}", resolved.path.display()),
        format!("config_source={}", resolved.source.as_str()),
    ];
    let mut failed = false;

    if !resolved.path.is_file() {
        lines.push("config_file=missing".to_string());
        lines.push("status=failed".to_string());
        return Err(anyhow!(lines.join("\n")));
    }
    lines.push("config_file=ok".to_string());

    let config = match OpsConfig::load(&resolved.path) {
        Ok(config) => {
            lines.push("config_parse=ok".to_string());
            config
        }
        Err(error) => {
            lines.push("config_parse=error".to_string());
            lines.push(format!("error={}", sanitize_error(&error.to_string())));
            lines.push("status=failed".to_string());
            return Err(anyhow!(lines.join("\n")));
        }
    };

    lines.push(format!(
        "object_store_kind={}",
        object_store_kind_as_str(&config)
    ));
    match crate::transport::open_store(&config) {
        Ok(_) => lines.push("object_store=ok".to_string()),
        Err(error) => {
            failed = true;
            lines.push("object_store=error".to_string());
            lines.push(format!(
                "object_store_error={}",
                sanitize_error(&error.to_string())
            ));
        }
    }

    let state_db_path = &config.client.state_db_path;
    lines.push(format!("client_state_db_path={}", state_db_path.display()));
    if state_db_path.is_file() {
        lines.push("client_state_db=exists".to_string());
    } else if state_db_path.exists() {
        failed = true;
        lines.push("client_state_db=not-file".to_string());
    } else {
        lines.push("client_state_db=missing".to_string());
    }

    match describe_parent_path(state_db_path) {
        ParentPathStatus::ExistsDirectory => {
            lines.push("client_state_db_parent=exists-directory".to_string())
        }
        ParentPathStatus::Missing => {
            failed = true;
            lines.push("client_state_db_parent=missing".to_string());
        }
        ParentPathStatus::NotDirectory => {
            failed = true;
            lines.push("client_state_db_parent=not-directory".to_string());
        }
        ParentPathStatus::WorkspaceRoot => {
            lines.push("client_state_db_parent=workspace-root".to_string())
        }
    }

    let mirror_root = &config.client.mirror_root;
    lines.push(format!("mirror_root_path={}", mirror_root.display()));
    if mirror_root.is_dir() {
        lines.push("mirror_root=exists-directory".to_string());
    } else if mirror_root.exists() {
        failed = true;
        lines.push("mirror_root=not-directory".to_string());
    } else {
        failed = true;
        lines.push("mirror_root=missing".to_string());
    }

    match example_config_status() {
        Ok(status) => lines.push(status),
        Err(status) => {
            failed = true;
            lines.push(status);
        }
    }

    lines.push(format!("status={}", if failed { "failed" } else { "ok" }));
    let report = lines.join("\n");
    if failed {
        Err(anyhow!(report))
    } else {
        Ok(report)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParentPathStatus {
    ExistsDirectory,
    Missing,
    NotDirectory,
    WorkspaceRoot,
}

fn describe_parent_path(path: &Path) -> ParentPathStatus {
    match path.parent() {
        None => ParentPathStatus::WorkspaceRoot,
        Some(parent) if parent.as_os_str().is_empty() => ParentPathStatus::WorkspaceRoot,
        Some(parent) if parent.is_dir() => ParentPathStatus::ExistsDirectory,
        Some(parent) if parent.exists() => ParentPathStatus::NotDirectory,
        Some(_) => ParentPathStatus::Missing,
    }
}

fn object_store_kind_as_str(config: &OpsConfig) -> &'static str {
    match config.object_store {
        loon_client::ops::OpsObjectStoreSpec::LocalFs { .. } => "local-fs",
        loon_client::ops::OpsObjectStoreSpec::AwsS3 { .. } => "aws-s3",
        loon_client::ops::OpsObjectStoreSpec::CloudflareR2 { .. } => "cloudflare-r2",
    }
}

fn example_config_status() -> std::result::Result<String, String> {
    let workspace_root = workspace_root();
    let expected = [
        "configs/loondb-demo.local-fs.example.toml",
        "configs/loondb-demo.aws-s3.example.toml",
        "configs/loondb-demo.cloudflare-r2.example.toml",
    ];
    let missing: Vec<String> = expected
        .iter()
        .filter_map(|relative| {
            let path = workspace_root.join(relative);
            (!path.is_file()).then(|| relative.to_string())
        })
        .collect();
    if missing.is_empty() {
        Ok("example_configs=ok".to_string())
    } else {
        Err(format!("example_configs=missing:{}", missing.join(",")))
    }
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace crates dir")
        .parent()
        .expect("workspace root")
        .to_path_buf()
}

fn sanitize_error(error: &str) -> String {
    error.replace('\n', " | ")
}

#[cfg(test)]
mod tests {
    use super::{describe_parent_path, ParentPathStatus};
    use std::path::Path;

    #[test]
    fn empty_parent_is_treated_as_workspace_root() {
        assert_eq!(
            describe_parent_path(Path::new("client.sqlite3")),
            ParentPathStatus::WorkspaceRoot
        );
    }
}
