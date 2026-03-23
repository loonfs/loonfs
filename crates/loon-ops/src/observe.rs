use crate::paths::NamespacePathIndex;
use crate::{require_existing_file, OpsConfig};
use anyhow::Result;
use loon_client::planner::{PlannedActionRecord, PlannedLocalOnlyActionRecord};
use loon_client::state_db::{
    ClientFileId, ObservedBoundInode, ObservedLocalOnlyInode, SqliteStateDb, StateDbError,
};
use loon_types::{sha256_digest, InodeId, NamespaceId};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObserveLocalKind {
    BoundFile,
    LocalOnlyFile,
}

impl ObserveLocalKind {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            Self::BoundFile => "bound_file",
            Self::LocalOnlyFile => "local_only_file",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObserveLocalReport {
    pub namespace_id: NamespaceId,
    pub relative_path: String,
    pub observation_kind: ObserveLocalKind,
    pub content_digest: String,
    pub planned_decision: String,
    pub planned_reason: String,
    pub inode_id: Option<InodeId>,
    pub client_file_id: Option<ClientFileId>,
    pub reused_existing_identity: Option<bool>,
}

#[derive(Debug, Error)]
pub enum ObserveLocalError {
    #[error(transparent)]
    StateDb(#[from] StateDbError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
    #[error(
        "observe-local path is outside mirror_root: path `{path}` mirror_root `{mirror_root}`"
    )]
    PathOutsideMirrorRoot { path: String, mirror_root: String },
    #[error("observe-local requires an existing file path, got directory `{path}`")]
    DirectoryPath { path: String },
    #[error(
        "observe-local path is ambiguous in namespace `{namespace_id}` at relative path `{relative_path}`"
    )]
    AmbiguousMatch {
        namespace_id: String,
        relative_path: String,
    },
    #[error(
        "observe-local parent is not a bound directory in namespace `{namespace_id}` for relative path `{relative_path}`"
    )]
    UnboundParent {
        namespace_id: String,
        relative_path: String,
    },
}

pub fn observe_local_path(
    config: &OpsConfig,
    namespace_id: &NamespaceId,
    path: &Path,
) -> Result<ObserveLocalReport, ObserveLocalError> {
    require_existing_file(&config.client.state_db_path, "client state db")?;

    let cwd = std::env::current_dir()?;
    let requested_path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        cwd.join(path)
    };
    let canonical_path = fs::canonicalize(&requested_path)?;
    if canonical_path.is_dir() {
        return Err(ObserveLocalError::DirectoryPath {
            path: canonical_path.display().to_string(),
        });
    }

    let mirror_root = fs::canonicalize(&config.client.mirror_root)?;
    let relative_path = canonical_path.strip_prefix(&mirror_root).map_err(|_| {
        ObserveLocalError::PathOutsideMirrorRoot {
            path: canonical_path.display().to_string(),
            mirror_root: mirror_root.display().to_string(),
        }
    })?;
    let relative_path = normalize_relative_path(relative_path);
    let parent_relative_path = std::path::Path::new(&relative_path)
        .parent()
        .map(normalize_relative_path)
        .unwrap_or_default();
    let display_name = canonical_path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    let content_digest = sha256_digest(&fs::read(&canonical_path)?);

    let now_ms = config.now_ms();
    let mut db = SqliteStateDb::open(&config.client.state_db_path)?;
    let summary = db.load_namespace_state_summary(namespace_id)?;
    let path_index = NamespacePathIndex::build(&summary);

    let local_only_matches = path_index.local_only_file_matches(&relative_path);
    let bound_file_matches = path_index.bound_file_matches(&relative_path);
    if !local_only_matches.is_empty() && !bound_file_matches.is_empty() {
        return Err(ObserveLocalError::AmbiguousMatch {
            namespace_id: namespace_id.as_str().to_owned(),
            relative_path,
        });
    }

    if local_only_matches.len() > 1 || bound_file_matches.len() > 1 {
        return Err(ObserveLocalError::AmbiguousMatch {
            namespace_id: namespace_id.as_str().to_owned(),
            relative_path,
        });
    }

    if let Some(existing) = local_only_matches.first() {
        let observed = ObservedLocalOnlyInode {
            namespace_id: namespace_id.clone(),
            inode_kind: existing.inode_kind.clone(),
            parent_inode_id: existing
                .parent_inode_id
                .expect("local-only path index only includes rows with parent"),
            display_name: existing.display_name.clone(),
            content_digest: Some(content_digest.clone()),
            exists_on_disk: true,
            dirty: true,
            last_local_change_ms: now_ms,
        };
        let result = db.observe_local_only_inode_under_parent_and_plan(&observed, now_ms)?;
        return Ok(report_local_only(
            namespace_id,
            &relative_path,
            &content_digest,
            &result.planned_action,
            &result.local_only.client_file_id,
            result.reused_existing_identity,
        ));
    }

    if let Some(bound) = bound_file_matches.first() {
        let planned = db.observe_bound_inode_and_plan(
            &ObservedBoundInode {
                namespace_id: namespace_id.clone(),
                inode_id: bound.inode_id,
                inode_kind: bound.inode_kind.clone(),
                content_digest: Some(content_digest.clone()),
                parent_inode_id: bound.parent_inode_id,
                display_name: bound.display_name.clone(),
                exists_on_disk: true,
                dirty: true,
                last_local_change_ms: now_ms,
            },
            now_ms,
        )?;
        return Ok(report_bound(
            namespace_id,
            &relative_path,
            &content_digest,
            &planned,
            bound.inode_id,
        ));
    }

    let parent_matches = path_index.bound_dir_matches(&parent_relative_path);
    if parent_matches.len() != 1 {
        return Err(ObserveLocalError::UnboundParent {
            namespace_id: namespace_id.as_str().to_owned(),
            relative_path,
        });
    }
    let parent = &parent_matches[0];
    let result = db.observe_local_only_inode_under_parent_and_plan(
        &ObservedLocalOnlyInode {
            namespace_id: namespace_id.clone(),
            inode_kind: loon_types::InodeKind::File,
            parent_inode_id: parent.inode_id,
            display_name,
            content_digest: Some(content_digest.clone()),
            exists_on_disk: true,
            dirty: true,
            last_local_change_ms: now_ms,
        },
        now_ms,
    )?;

    Ok(report_local_only(
        namespace_id,
        &relative_path,
        &content_digest,
        &result.planned_action,
        &result.local_only.client_file_id,
        result.reused_existing_identity,
    ))
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

fn normalize_relative_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn report_bound(
    namespace_id: &NamespaceId,
    relative_path: &str,
    content_digest: &str,
    planned: &PlannedActionRecord,
    inode_id: InodeId,
) -> ObserveLocalReport {
    ObserveLocalReport {
        namespace_id: namespace_id.clone(),
        relative_path: relative_path.to_owned(),
        observation_kind: ObserveLocalKind::BoundFile,
        content_digest: content_digest.to_owned(),
        planned_decision: planned.decision.as_str().to_owned(),
        planned_reason: planned.reason.as_str().to_owned(),
        inode_id: Some(inode_id),
        client_file_id: None,
        reused_existing_identity: None,
    }
}

fn report_local_only(
    namespace_id: &NamespaceId,
    relative_path: &str,
    content_digest: &str,
    planned: &PlannedLocalOnlyActionRecord,
    client_file_id: &ClientFileId,
    reused_existing_identity: bool,
) -> ObserveLocalReport {
    ObserveLocalReport {
        namespace_id: namespace_id.clone(),
        relative_path: relative_path.to_owned(),
        observation_kind: ObserveLocalKind::LocalOnlyFile,
        content_digest: content_digest.to_owned(),
        planned_decision: planned.decision.as_str().to_owned(),
        planned_reason: planned.reason.as_str().to_owned(),
        inode_id: None,
        client_file_id: Some(client_file_id.clone()),
        reused_existing_identity: Some(reused_existing_identity),
    }
}
