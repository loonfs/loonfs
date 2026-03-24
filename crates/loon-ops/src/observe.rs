use crate::paths::NamespacePathIndex;
use crate::{require_existing_file, OpsConfig};
use anyhow::Result;
use loon_client::planner::{PlannedActionRecord, PlannedLocalOnlyActionRecord};
use loon_client::state_db::{
    ClientFileId, LocalFileStateRow, LocalOnlyFileStateRow, LocalOnlyParentRef, ObservedBoundInode,
    ObservedLocalOnlyDeleteResult, ObservedLocalOnlyInode, SqliteStateDb, StateDbError,
};
use loon_types::{sha256_digest, InodeId, InodeKind, NamespaceId};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObservedPathKind {
    BoundFile,
    BoundDir,
    LocalOnlyFile,
    LocalOnlyDir,
}

impl ObservedPathKind {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            Self::BoundFile => "bound_file",
            Self::BoundDir => "bound_dir",
            Self::LocalOnlyFile => "local_only_file",
            Self::LocalOnlyDir => "local_only_dir",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObserveLocalReport {
    pub namespace_id: NamespaceId,
    pub relative_path: String,
    pub observation_kind: ObservedPathKind,
    pub content_digest: String,
    pub planned_decision: String,
    pub planned_reason: String,
    pub inode_id: Option<InodeId>,
    pub client_file_id: Option<ClientFileId>,
    pub reused_existing_identity: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObserveDeleteReport {
    pub namespace_id: NamespaceId,
    pub relative_path: String,
    pub observation_kind: ObservedPathKind,
    pub planned_decision: String,
    pub inode_id: Option<InodeId>,
    pub client_file_id: Option<ClientFileId>,
    pub removed_client_file_count: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObserveMoveReport {
    pub namespace_id: NamespaceId,
    pub from_relative_path: String,
    pub to_relative_path: String,
    pub observation_kind: ObservedPathKind,
    pub planned_decision: String,
    pub inode_id: Option<InodeId>,
    pub client_file_id: Option<ClientFileId>,
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

#[derive(Debug, Error)]
pub enum ObserveDeleteError {
    #[error(transparent)]
    StateDb(#[from] StateDbError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
    #[error(
        "observe-delete path is outside mirror_root: path `{path}` mirror_root `{mirror_root}`"
    )]
    PathOutsideMirrorRoot { path: String, mirror_root: String },
    #[error("observe-delete requires an absent path, got existing path `{path}`")]
    PathMustBeAbsent { path: String },
    #[error(
        "observe-delete path is ambiguous in namespace `{namespace_id}` at relative path `{relative_path}`"
    )]
    AmbiguousMatch {
        namespace_id: String,
        relative_path: String,
    },
    #[error(
        "observe-delete path is not tracked in namespace `{namespace_id}` at relative path `{relative_path}`"
    )]
    SourceNotTracked {
        namespace_id: String,
        relative_path: String,
    },
}

#[derive(Debug, Error)]
pub enum ObserveMoveError {
    #[error(transparent)]
    StateDb(#[from] StateDbError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
    #[error("observe-move path is outside mirror_root: path `{path}` mirror_root `{mirror_root}`")]
    PathOutsideMirrorRoot { path: String, mirror_root: String },
    #[error("observe-move requires `from` to be absent on disk, got `{path}`")]
    FromPathMustBeAbsent { path: String },
    #[error("observe-move requires `to` to exist on disk, got missing path `{path}`")]
    ToPathMissing { path: String },
    #[error(
        "observe-move path is ambiguous in namespace `{namespace_id}` at relative path `{relative_path}`"
    )]
    AmbiguousMatch {
        namespace_id: String,
        relative_path: String,
    },
    #[error(
        "observe-move source is not tracked in namespace `{namespace_id}` at relative path `{relative_path}`"
    )]
    SourceNotTracked {
        namespace_id: String,
        relative_path: String,
    },
    #[error(
        "observe-move parent is not a tracked directory in namespace `{namespace_id}` for relative path `{relative_path}`"
    )]
    UnboundParent {
        namespace_id: String,
        relative_path: String,
    },
    #[error(
        "observe-move bound source requires a bound destination parent in namespace `{namespace_id}` for relative path `{relative_path}`"
    )]
    BoundDestinationParentNotBound {
        namespace_id: String,
        relative_path: String,
    },
    #[error(
        "observe-move target is already occupied in namespace `{namespace_id}` at relative path `{relative_path}`"
    )]
    TargetOccupied {
        namespace_id: String,
        relative_path: String,
    },
    #[error("observe-move rejects cross-kind move from `{from_path}` to `{to_path}`")]
    CrossKindMove { from_path: String, to_path: String },
}

#[derive(Debug, Clone)]
enum PathMatch {
    Bound(LocalFileStateRow),
    LocalOnly(LocalOnlyFileStateRow),
}

impl PathMatch {
    fn inode_kind(&self) -> InodeKind {
        match self {
            Self::Bound(row) => row.inode_kind.clone(),
            Self::LocalOnly(row) => row.inode_kind.clone(),
        }
    }

    fn matches_identity(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Bound(left), Self::Bound(right)) => left.inode_id == right.inode_id,
            (Self::LocalOnly(left), Self::LocalOnly(right)) => {
                left.client_file_id == right.client_file_id
            }
            _ => false,
        }
    }
}

#[derive(Debug, Clone)]
enum ParentMatch {
    Bound(InodeId),
    LocalOnly(ClientFileId),
}

pub fn observe_local_path(
    config: &OpsConfig,
    namespace_id: &NamespaceId,
    path: &Path,
) -> Result<ObserveLocalReport, ObserveLocalError> {
    require_existing_file(&config.client.state_db_path, "client state db")?;

    let cwd = std::env::current_dir()?;
    let requested_path = resolve_requested_path(&cwd, path);
    let canonical_path = fs::canonicalize(&requested_path)?;
    if canonical_path.is_dir() {
        return Err(ObserveLocalError::DirectoryPath {
            path: canonical_path.display().to_string(),
        });
    }

    let mirror_root = fs::canonicalize(&config.client.mirror_root)?;
    let relative_path = canonical_path
        .strip_prefix(&mirror_root)
        .map(normalize_relative_path)
        .map_err(|_| ObserveLocalError::PathOutsideMirrorRoot {
            path: canonical_path.display().to_string(),
            mirror_root: mirror_root.display().to_string(),
        })?;
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
    let path_index = load_namespace_path_index(&db, namespace_id)?;

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
                .expect("local-only file path should remain under a bound parent"),
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
            inode_kind: InodeKind::File,
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

pub fn observe_delete_path(
    config: &OpsConfig,
    namespace_id: &NamespaceId,
    path: &Path,
) -> Result<ObserveDeleteReport, ObserveDeleteError> {
    require_existing_file(&config.client.state_db_path, "client state db")?;

    let cwd = std::env::current_dir()?;
    let requested_path = resolve_requested_path(&cwd, path);
    if requested_path.exists() {
        return Err(ObserveDeleteError::PathMustBeAbsent {
            path: requested_path.display().to_string(),
        });
    }

    let mirror_root = fs::canonicalize(&config.client.mirror_root)?;
    let relative_path =
        relative_absent_path_under_root(&requested_path, &mirror_root).map_err(|outside| {
            ObserveDeleteError::PathOutsideMirrorRoot {
                path: outside.0,
                mirror_root: outside.1,
            }
        })?;
    let now_ms = config.now_ms();
    let mut db = SqliteStateDb::open(&config.client.state_db_path)?;
    let path_index = load_namespace_path_index(&db, namespace_id)?;
    let source = classify_exact_path(&path_index, namespace_id, &relative_path)
        .map_err(|_| ObserveDeleteError::AmbiguousMatch {
            namespace_id: namespace_id.as_str().to_owned(),
            relative_path: relative_path.clone(),
        })?
        .ok_or_else(|| ObserveDeleteError::SourceNotTracked {
            namespace_id: namespace_id.as_str().to_owned(),
            relative_path: relative_path.clone(),
        })?;

    match source {
        PathMatch::LocalOnly(row) => {
            let deleted = db.observe_local_only_delete(&row.client_file_id)?;
            Ok(report_local_only_delete(
                namespace_id,
                &relative_path,
                &row,
                deleted,
            ))
        }
        PathMatch::Bound(row) => {
            let planned = db.observe_bound_inode_and_plan(
                &ObservedBoundInode {
                    namespace_id: namespace_id.clone(),
                    inode_id: row.inode_id,
                    inode_kind: row.inode_kind.clone(),
                    content_digest: row.content_digest.clone(),
                    parent_inode_id: row.parent_inode_id,
                    display_name: row.display_name.clone(),
                    exists_on_disk: false,
                    dirty: true,
                    last_local_change_ms: now_ms,
                },
                now_ms,
            )?;
            Ok(report_bound_delete(
                namespace_id,
                &relative_path,
                &row,
                &planned,
            ))
        }
    }
}

pub fn observe_move_path(
    config: &OpsConfig,
    namespace_id: &NamespaceId,
    from: &Path,
    to: &Path,
) -> Result<ObserveMoveReport, ObserveMoveError> {
    require_existing_file(&config.client.state_db_path, "client state db")?;

    let cwd = std::env::current_dir()?;
    let requested_from = resolve_requested_path(&cwd, from);
    if requested_from.exists() {
        return Err(ObserveMoveError::FromPathMustBeAbsent {
            path: requested_from.display().to_string(),
        });
    }
    let requested_to = resolve_requested_path(&cwd, to);
    if !requested_to.exists() {
        return Err(ObserveMoveError::ToPathMissing {
            path: requested_to.display().to_string(),
        });
    }

    let mirror_root = fs::canonicalize(&config.client.mirror_root)?;
    let from_relative_path = relative_absent_path_under_root(&requested_from, &mirror_root)
        .map_err(|outside| ObserveMoveError::PathOutsideMirrorRoot {
            path: outside.0,
            mirror_root: outside.1,
        })?;
    let canonical_to = fs::canonicalize(&requested_to)?;
    let to_relative_path = canonical_to
        .strip_prefix(&mirror_root)
        .map(normalize_relative_path)
        .map_err(|_| ObserveMoveError::PathOutsideMirrorRoot {
            path: canonical_to.display().to_string(),
            mirror_root: mirror_root.display().to_string(),
        })?;

    let mut db = SqliteStateDb::open(&config.client.state_db_path)?;
    let path_index = load_namespace_path_index(&db, namespace_id)?;
    let source = classify_exact_path(&path_index, namespace_id, &from_relative_path)
        .map_err(|_| ObserveMoveError::AmbiguousMatch {
            namespace_id: namespace_id.as_str().to_owned(),
            relative_path: from_relative_path.clone(),
        })?
        .ok_or_else(|| ObserveMoveError::SourceNotTracked {
            namespace_id: namespace_id.as_str().to_owned(),
            relative_path: from_relative_path.clone(),
        })?;
    let to_is_dir = canonical_to.is_dir();
    match source.inode_kind() {
        InodeKind::File if to_is_dir => {
            return Err(ObserveMoveError::CrossKindMove {
                from_path: from_relative_path,
                to_path: to_relative_path,
            });
        }
        InodeKind::Dir if !to_is_dir => {
            return Err(ObserveMoveError::CrossKindMove {
                from_path: from_relative_path,
                to_path: to_relative_path,
            });
        }
        InodeKind::Symlink | InodeKind::Mount => {
            return Err(ObserveMoveError::CrossKindMove {
                from_path: from_relative_path,
                to_path: to_relative_path,
            });
        }
        _ => {}
    }

    if target_owned_by_other(&path_index, &to_relative_path, &source) {
        return Err(ObserveMoveError::TargetOccupied {
            namespace_id: namespace_id.as_str().to_owned(),
            relative_path: to_relative_path,
        });
    }

    let parent_relative_path = std::path::Path::new(&to_relative_path)
        .parent()
        .map(normalize_relative_path)
        .unwrap_or_default();
    let destination_parent =
        classify_destination_parent(&path_index, namespace_id, &parent_relative_path).map_err(
            |error| match error {
                DestinationParentError::Ambiguous => ObserveMoveError::AmbiguousMatch {
                    namespace_id: namespace_id.as_str().to_owned(),
                    relative_path: parent_relative_path.clone(),
                },
                DestinationParentError::Missing => ObserveMoveError::UnboundParent {
                    namespace_id: namespace_id.as_str().to_owned(),
                    relative_path: parent_relative_path.clone(),
                },
            },
        )?;
    let target_display_name = canonical_to
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    let now_ms = config.now_ms();

    match source {
        PathMatch::Bound(row) => {
            let ParentMatch::Bound(parent_inode_id) = destination_parent else {
                return Err(ObserveMoveError::BoundDestinationParentNotBound {
                    namespace_id: namespace_id.as_str().to_owned(),
                    relative_path: parent_relative_path,
                });
            };
            let content_digest = if row.inode_kind == InodeKind::File {
                Some(sha256_digest(&fs::read(&canonical_to)?))
            } else {
                None
            };
            let planned = db.observe_bound_inode_and_plan(
                &ObservedBoundInode {
                    namespace_id: namespace_id.clone(),
                    inode_id: row.inode_id,
                    inode_kind: row.inode_kind.clone(),
                    content_digest,
                    parent_inode_id: Some(parent_inode_id),
                    display_name: target_display_name,
                    exists_on_disk: true,
                    dirty: true,
                    last_local_change_ms: now_ms,
                },
                now_ms,
            )?;
            Ok(report_bound_move(
                namespace_id,
                &from_relative_path,
                &to_relative_path,
                &row,
                &planned,
            ))
        }
        PathMatch::LocalOnly(row) => {
            let new_parent = match destination_parent {
                ParentMatch::Bound(parent_inode_id) => {
                    LocalOnlyParentRef::Bound { parent_inode_id }
                }
                ParentMatch::LocalOnly(parent_client_file_id) => LocalOnlyParentRef::LocalOnly {
                    parent_client_file_id,
                },
            };
            let content_digest = if row.inode_kind == InodeKind::File {
                Some(sha256_digest(&fs::read(&canonical_to)?))
            } else {
                None
            };
            let result = db.observe_local_only_move_and_plan(
                &row.client_file_id,
                &new_parent,
                row.inode_kind.clone(),
                &target_display_name,
                content_digest,
                true,
                true,
                now_ms,
                now_ms,
            )?;
            Ok(report_local_only_move(
                namespace_id,
                &from_relative_path,
                &to_relative_path,
                &result.planned_action,
                &result.local_only.client_file_id,
                row.inode_kind,
            ))
        }
    }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DestinationParentError {
    Ambiguous,
    Missing,
}

fn load_namespace_path_index(
    db: &SqliteStateDb,
    namespace_id: &NamespaceId,
) -> Result<NamespacePathIndex, StateDbError> {
    let summary = db.load_namespace_state_summary(namespace_id)?;
    let parent_links = db.load_local_only_parent_links_for_namespace(namespace_id)?;
    Ok(NamespacePathIndex::build(&summary, &parent_links))
}

fn classify_exact_path(
    path_index: &NamespacePathIndex,
    namespace_id: &NamespaceId,
    relative_path: &str,
) -> std::result::Result<Option<PathMatch>, ()> {
    let local_only_files = path_index.local_only_file_matches(relative_path);
    let local_only_dirs = path_index.local_only_dir_matches(relative_path);
    let bound_files = path_index.bound_file_matches(relative_path);
    let bound_dirs = path_index.bound_dir_matches(relative_path);
    let total =
        local_only_files.len() + local_only_dirs.len() + bound_files.len() + bound_dirs.len();
    if total == 0 {
        return Ok(None);
    }
    if total > 1
        || local_only_files.len() > 1
        || local_only_dirs.len() > 1
        || bound_files.len() > 1
        || bound_dirs.len() > 1
    {
        let _ = namespace_id;
        return Err(());
    }
    if let Some(row) = local_only_files.first() {
        return Ok(Some(PathMatch::LocalOnly(row.clone())));
    }
    if let Some(row) = local_only_dirs.first() {
        return Ok(Some(PathMatch::LocalOnly(row.clone())));
    }
    if let Some(row) = bound_files.first() {
        return Ok(Some(PathMatch::Bound(row.clone())));
    }
    if let Some(row) = bound_dirs.first() {
        return Ok(Some(PathMatch::Bound(row.clone())));
    }
    Ok(None)
}

fn classify_destination_parent(
    path_index: &NamespacePathIndex,
    _namespace_id: &NamespaceId,
    relative_path: &str,
) -> std::result::Result<ParentMatch, DestinationParentError> {
    let bound_dirs = path_index.bound_dir_matches(relative_path);
    let local_only_dirs = path_index.local_only_dir_matches(relative_path);
    let total = bound_dirs.len() + local_only_dirs.len();
    if total == 0 {
        return Err(DestinationParentError::Missing);
    }
    if total > 1 || bound_dirs.len() > 1 || local_only_dirs.len() > 1 {
        return Err(DestinationParentError::Ambiguous);
    }
    if let Some(row) = bound_dirs.first() {
        return Ok(ParentMatch::Bound(row.inode_id));
    }
    if let Some(row) = local_only_dirs.first() {
        return Ok(ParentMatch::LocalOnly(row.client_file_id.clone()));
    }
    Err(DestinationParentError::Missing)
}

fn target_owned_by_other(
    path_index: &NamespacePathIndex,
    relative_path: &str,
    source: &PathMatch,
) -> bool {
    let mut matches = Vec::new();
    matches.extend(
        path_index
            .local_only_file_matches(relative_path)
            .iter()
            .cloned()
            .map(PathMatch::LocalOnly),
    );
    matches.extend(
        path_index
            .local_only_dir_matches(relative_path)
            .iter()
            .cloned()
            .map(PathMatch::LocalOnly),
    );
    matches.extend(
        path_index
            .bound_file_matches(relative_path)
            .iter()
            .cloned()
            .map(PathMatch::Bound),
    );
    matches.extend(
        path_index
            .bound_dir_matches(relative_path)
            .iter()
            .cloned()
            .map(PathMatch::Bound),
    );
    matches
        .into_iter()
        .any(|matched| !matched.matches_identity(source))
}

fn resolve_requested_path(cwd: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        cwd.join(path)
    }
}

fn relative_absent_path_under_root(
    requested_path: &Path,
    mirror_root: &Path,
) -> std::result::Result<String, (String, String)> {
    let parent = requested_path.parent().unwrap_or(requested_path);
    let canonical_parent = fs::canonicalize(parent).map_err(|_| {
        (
            requested_path.display().to_string(),
            mirror_root.display().to_string(),
        )
    })?;
    let parent_relative = canonical_parent.strip_prefix(mirror_root).map_err(|_| {
        (
            requested_path.display().to_string(),
            mirror_root.display().to_string(),
        )
    })?;
    let file_name = requested_path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    let parent_relative = normalize_relative_path(parent_relative);
    Ok(if parent_relative.is_empty() {
        file_name
    } else {
        format!("{parent_relative}/{file_name}")
    })
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
        observation_kind: ObservedPathKind::BoundFile,
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
        observation_kind: ObservedPathKind::LocalOnlyFile,
        content_digest: content_digest.to_owned(),
        planned_decision: planned.decision.as_str().to_owned(),
        planned_reason: planned.reason.as_str().to_owned(),
        inode_id: None,
        client_file_id: Some(client_file_id.clone()),
        reused_existing_identity: Some(reused_existing_identity),
    }
}

fn report_bound_delete(
    namespace_id: &NamespaceId,
    relative_path: &str,
    row: &LocalFileStateRow,
    planned: &PlannedActionRecord,
) -> ObserveDeleteReport {
    ObserveDeleteReport {
        namespace_id: namespace_id.clone(),
        relative_path: relative_path.to_owned(),
        observation_kind: match row.inode_kind {
            InodeKind::File => ObservedPathKind::BoundFile,
            InodeKind::Dir => ObservedPathKind::BoundDir,
            InodeKind::Symlink | InodeKind::Mount => ObservedPathKind::BoundFile,
        },
        planned_decision: planned.decision.as_str().to_owned(),
        inode_id: Some(row.inode_id),
        client_file_id: None,
        removed_client_file_count: None,
    }
}

fn report_local_only_delete(
    namespace_id: &NamespaceId,
    relative_path: &str,
    row: &LocalOnlyFileStateRow,
    deleted: ObservedLocalOnlyDeleteResult,
) -> ObserveDeleteReport {
    ObserveDeleteReport {
        namespace_id: namespace_id.clone(),
        relative_path: relative_path.to_owned(),
        observation_kind: match row.inode_kind {
            InodeKind::File => ObservedPathKind::LocalOnlyFile,
            InodeKind::Dir => ObservedPathKind::LocalOnlyDir,
            InodeKind::Symlink | InodeKind::Mount => ObservedPathKind::LocalOnlyFile,
        },
        planned_decision: "no_op".to_owned(),
        inode_id: None,
        client_file_id: Some(deleted.root_client_file_id),
        removed_client_file_count: Some(deleted.removed_client_file_ids.len()),
    }
}

fn report_bound_move(
    namespace_id: &NamespaceId,
    from_relative_path: &str,
    to_relative_path: &str,
    row: &LocalFileStateRow,
    planned: &PlannedActionRecord,
) -> ObserveMoveReport {
    ObserveMoveReport {
        namespace_id: namespace_id.clone(),
        from_relative_path: from_relative_path.to_owned(),
        to_relative_path: to_relative_path.to_owned(),
        observation_kind: match row.inode_kind {
            InodeKind::File => ObservedPathKind::BoundFile,
            InodeKind::Dir => ObservedPathKind::BoundDir,
            InodeKind::Symlink | InodeKind::Mount => ObservedPathKind::BoundFile,
        },
        planned_decision: planned.decision.as_str().to_owned(),
        inode_id: Some(row.inode_id),
        client_file_id: None,
    }
}

fn report_local_only_move(
    namespace_id: &NamespaceId,
    from_relative_path: &str,
    to_relative_path: &str,
    planned: &PlannedLocalOnlyActionRecord,
    client_file_id: &ClientFileId,
    inode_kind: InodeKind,
) -> ObserveMoveReport {
    ObserveMoveReport {
        namespace_id: namespace_id.clone(),
        from_relative_path: from_relative_path.to_owned(),
        to_relative_path: to_relative_path.to_owned(),
        observation_kind: match inode_kind {
            InodeKind::File => ObservedPathKind::LocalOnlyFile,
            InodeKind::Dir => ObservedPathKind::LocalOnlyDir,
            InodeKind::Symlink | InodeKind::Mount => ObservedPathKind::LocalOnlyFile,
        },
        planned_decision: planned.decision.as_str().to_owned(),
        inode_id: None,
        client_file_id: Some(client_file_id.clone()),
    }
}
