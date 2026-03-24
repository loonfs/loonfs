use crate::paths::{relative_path_is_under_prefix, NamespacePathIndex};
use crate::{require_existing_file, OpsConfig};
use anyhow::Result;
use loon_client::planner::{PlannedActionRecord, PlannedLocalOnlyActionRecord};
use loon_client::state_db::{
    ClientFileId, LocalFileStateRow, LocalOnlyFileStateRow, LocalOnlyParentRef,
    ObservedBoundDelete, ObservedBoundInode, ObservedLocalOnlyDeleteResult, ObservedLocalOnlyInode,
    ObservedLocalOnlySubtreeInode, ObservedLocalOnlySubtreeMove, SqliteStateDb, StateDbError,
    SubtreeLocalOnlyParentRef, SubtreeObservationOp, SubtreeObservationOutcome,
};
use loon_types::{sha256_digest, InodeId, InodeKind, NamespaceId};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObserveSubtreeReport {
    pub namespace_id: NamespaceId,
    pub relative_path: String,
    pub scanned_file_count: usize,
    pub scanned_dir_count: usize,
    pub applied_operation_count: usize,
    pub bound_observe_count: usize,
    pub local_only_observe_count: usize,
    pub paired_bound_move_count: usize,
    pub paired_local_only_move_count: usize,
    pub bound_delete_count: usize,
    pub local_only_delete_count: usize,
    pub planned_decision_counts: BTreeMap<String, usize>,
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

#[derive(Debug, Error)]
pub enum ObserveSubtreeError {
    #[error(transparent)]
    StateDb(#[from] StateDbError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
    #[error(
        "observe-subtree path is outside mirror_root: path `{path}` mirror_root `{mirror_root}`"
    )]
    PathOutsideMirrorRoot { path: String, mirror_root: String },
    #[error("observe-subtree requires an existing directory path, got file `{path}`")]
    FilePath { path: String },
    #[error(
        "observe-subtree path is ambiguous in namespace `{namespace_id}` at relative path `{relative_path}`"
    )]
    AmbiguousMatch {
        namespace_id: String,
        relative_path: String,
    },
    #[error(
        "observe-subtree parent is not a tracked directory in namespace `{namespace_id}` for relative path `{relative_path}`"
    )]
    UntrackedParent {
        namespace_id: String,
        relative_path: String,
    },
    #[error(
        "observe-subtree tracked kind mismatch in namespace `{namespace_id}` at relative path `{relative_path}`"
    )]
    KindMismatch {
        namespace_id: String,
        relative_path: String,
    },
    #[error(
        "observe-subtree move pairing is ambiguous in namespace `{namespace_id}` at relative path `{relative_path}`"
    )]
    AmbiguousMovePairing {
        namespace_id: String,
        relative_path: String,
    },
    #[error("observe-subtree unsupported filesystem entry `{path}`")]
    UnsupportedFilesystemEntry { path: String },
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

#[derive(Debug, Clone, PartialEq, Eq)]
enum SubtreeDestinationParent {
    Bound(InodeId),
    ExistingLocalOnly(ClientFileId),
    BatchLocalOnly { parent_relative_path: String },
}

#[derive(Debug, Clone)]
struct UnmatchedPresentFile {
    relative_path: String,
    parent: SubtreeDestinationParent,
    display_name: String,
    content_digest: String,
}

#[derive(Debug, Clone)]
struct MissingTrackedFile {
    relative_path: String,
    source: PathMatch,
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

pub fn observe_subtree_path(
    config: &OpsConfig,
    namespace_id: &NamespaceId,
    path: &Path,
) -> Result<ObserveSubtreeReport, ObserveSubtreeError> {
    require_existing_file(&config.client.state_db_path, "client state db")?;

    let cwd = std::env::current_dir()?;
    let requested_path = resolve_requested_path(&cwd, path);
    let canonical_path = fs::canonicalize(&requested_path)?;
    if canonical_path.is_file() {
        return Err(ObserveSubtreeError::FilePath {
            path: canonical_path.display().to_string(),
        });
    }

    let mirror_root = fs::canonicalize(&config.client.mirror_root)?;
    let relative_path = canonical_path
        .strip_prefix(&mirror_root)
        .map(normalize_relative_path)
        .map_err(|_| ObserveSubtreeError::PathOutsideMirrorRoot {
            path: canonical_path.display().to_string(),
            mirror_root: mirror_root.display().to_string(),
        })?;
    let scanned_entries = scan_subtree_entries(&canonical_path, &relative_path)?;
    let scanned_file_count = scanned_entries
        .iter()
        .filter(|entry| entry.kind == ScannedEntryKind::File)
        .count();
    let scanned_dir_count = scanned_entries
        .iter()
        .filter(|entry| entry.kind == ScannedEntryKind::Dir)
        .count();

    let now_ms = config.now_ms();
    let mut db = SqliteStateDb::open(&config.client.state_db_path)?;
    let path_index = load_namespace_path_index(&db, namespace_id)?;
    let operations = build_subtree_operations(
        &path_index,
        namespace_id,
        &scanned_entries,
        &relative_path,
        now_ms,
    )?;
    let outcomes = db.observe_subtree_and_plan(&operations, now_ms)?;

    Ok(summarize_observe_subtree(
        namespace_id,
        &relative_path,
        scanned_file_count,
        scanned_dir_count,
        &outcomes,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScannedEntryKind {
    File,
    Dir,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ScannedEntry {
    relative_path: String,
    kind: ScannedEntryKind,
    content_digest: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DestinationParentError {
    Ambiguous,
    Missing,
}

fn scan_subtree_entries(
    canonical_root: &Path,
    root_relative_path: &str,
) -> Result<Vec<ScannedEntry>, ObserveSubtreeError> {
    let mut entries = Vec::new();
    scan_subtree_entries_recursive(canonical_root, root_relative_path, &mut entries)?;
    Ok(entries)
}

fn scan_subtree_entries_recursive(
    current_path: &Path,
    current_relative_path: &str,
    entries: &mut Vec<ScannedEntry>,
) -> Result<(), ObserveSubtreeError> {
    let metadata = fs::symlink_metadata(current_path)?;
    let file_type = metadata.file_type();
    if file_type.is_dir() {
        entries.push(ScannedEntry {
            relative_path: current_relative_path.to_owned(),
            kind: ScannedEntryKind::Dir,
            content_digest: None,
        });

        let mut children = fs::read_dir(current_path)?
            .map(|entry| entry.map(|entry| (entry.file_name(), entry.path())))
            .collect::<Result<Vec<_>, _>>()?;
        children.sort_by(|left, right| left.0.cmp(&right.0));

        for (_, child_path) in children {
            let child_name = child_path
                .file_name()
                .map(|name| name.to_string_lossy().into_owned())
                .unwrap_or_default();
            let child_relative_path = if current_relative_path.is_empty() {
                child_name
            } else {
                format!("{current_relative_path}/{child_name}")
            };
            scan_subtree_entries_recursive(&child_path, &child_relative_path, entries)?;
        }
        return Ok(());
    }

    if file_type.is_file() {
        entries.push(ScannedEntry {
            relative_path: current_relative_path.to_owned(),
            kind: ScannedEntryKind::File,
            content_digest: Some(sha256_digest(&fs::read(current_path)?)),
        });
        return Ok(());
    }

    Err(ObserveSubtreeError::UnsupportedFilesystemEntry {
        path: current_path.display().to_string(),
    })
}

fn build_subtree_operations(
    path_index: &NamespacePathIndex,
    namespace_id: &NamespaceId,
    scanned_entries: &[ScannedEntry],
    subtree_relative_path: &str,
    now_ms: u64,
) -> Result<Vec<SubtreeObservationOp>, ObserveSubtreeError> {
    let mut operations = Vec::new();
    let mut present_paths = BTreeSet::new();
    let mut new_local_only_dirs = BTreeSet::new();
    let mut unmatched_present_files = Vec::new();

    for entry in scanned_entries {
        present_paths.insert(entry.relative_path.clone());
        let exact =
            classify_exact_path(path_index, namespace_id, &entry.relative_path).map_err(|_| {
                ObserveSubtreeError::AmbiguousMatch {
                    namespace_id: namespace_id.as_str().to_owned(),
                    relative_path: entry.relative_path.clone(),
                }
            })?;

        match exact {
            Some(PathMatch::Bound(row)) => {
                if !matches_scanned_kind(row.inode_kind.clone(), entry.kind) {
                    return Err(ObserveSubtreeError::KindMismatch {
                        namespace_id: namespace_id.as_str().to_owned(),
                        relative_path: entry.relative_path.clone(),
                    });
                }
                if row.inode_kind == InodeKind::File {
                    operations.push(SubtreeObservationOp::ObserveBound {
                        observed: ObservedBoundInode {
                            namespace_id: namespace_id.clone(),
                            inode_id: row.inode_id,
                            inode_kind: row.inode_kind,
                            content_digest: entry.content_digest.clone(),
                            parent_inode_id: row.parent_inode_id,
                            display_name: row.display_name,
                            exists_on_disk: true,
                            dirty: true,
                            last_local_change_ms: now_ms,
                        },
                    });
                }
            }
            Some(PathMatch::LocalOnly(row)) => {
                if !matches_scanned_kind(row.inode_kind.clone(), entry.kind) {
                    return Err(ObserveSubtreeError::KindMismatch {
                        namespace_id: namespace_id.as_str().to_owned(),
                        relative_path: entry.relative_path.clone(),
                    });
                }
                let parent = path_index.local_only_parent_ref_for(&row).ok_or_else(|| {
                    ObserveSubtreeError::UntrackedParent {
                        namespace_id: namespace_id.as_str().to_owned(),
                        relative_path: entry.relative_path.clone(),
                    }
                })?;
                operations.push(SubtreeObservationOp::ObserveLocalOnly {
                    observed: ObservedLocalOnlySubtreeInode {
                        relative_path: entry.relative_path.clone(),
                        namespace_id: namespace_id.clone(),
                        inode_kind: row.inode_kind.clone(),
                        parent: subtree_parent_ref_from_existing(parent),
                        display_name: row.display_name.clone(),
                        content_digest: entry.content_digest.clone(),
                        exists_on_disk: true,
                        dirty: true,
                        last_local_change_ms: now_ms,
                    },
                });
            }
            None => {
                let parent_relative_path = Path::new(&entry.relative_path)
                    .parent()
                    .map(normalize_relative_path)
                    .unwrap_or_default();
                let parent = if new_local_only_dirs.contains(&parent_relative_path) {
                    SubtreeLocalOnlyParentRef::BatchLocalOnly {
                        parent_relative_path: parent_relative_path.clone(),
                    }
                } else {
                    match classify_destination_parent(
                        path_index,
                        namespace_id,
                        &parent_relative_path,
                    ) {
                        Ok(ParentMatch::Bound(parent_inode_id)) => {
                            SubtreeLocalOnlyParentRef::Bound { parent_inode_id }
                        }
                        Ok(ParentMatch::LocalOnly(parent_client_file_id)) => {
                            SubtreeLocalOnlyParentRef::ExistingLocalOnly {
                                parent_client_file_id,
                            }
                        }
                        Err(_) => {
                            return Err(ObserveSubtreeError::UntrackedParent {
                                namespace_id: namespace_id.as_str().to_owned(),
                                relative_path: entry.relative_path.clone(),
                            });
                        }
                    }
                };
                let display_name = Path::new(&entry.relative_path)
                    .file_name()
                    .map(|name| name.to_string_lossy().into_owned())
                    .unwrap_or_default();
                match entry.kind {
                    ScannedEntryKind::Dir => {
                        operations.push(SubtreeObservationOp::ObserveLocalOnly {
                            observed: ObservedLocalOnlySubtreeInode {
                                relative_path: entry.relative_path.clone(),
                                namespace_id: namespace_id.clone(),
                                inode_kind: InodeKind::Dir,
                                parent,
                                display_name,
                                content_digest: None,
                                exists_on_disk: true,
                                dirty: true,
                                last_local_change_ms: now_ms,
                            },
                        });
                        new_local_only_dirs.insert(entry.relative_path.clone());
                    }
                    ScannedEntryKind::File => {
                        unmatched_present_files.push(UnmatchedPresentFile {
                            relative_path: entry.relative_path.clone(),
                            parent: destination_parent_from_subtree_parent_ref(&parent),
                            display_name,
                            content_digest: entry.content_digest.clone().ok_or_else(|| {
                                ObserveSubtreeError::UnsupportedFilesystemEntry {
                                    path: entry.relative_path.clone(),
                                }
                            })?,
                        });
                    }
                }
            }
        }
    }

    let mut missing_roots: BTreeSet<String> = BTreeSet::new();
    for tracked_relative_path in
        path_index.tracked_relative_paths_under_prefix(subtree_relative_path)
    {
        if present_paths.contains(&tracked_relative_path) {
            continue;
        }
        if missing_roots
            .iter()
            .any(|root| relative_path_is_under_prefix(&tracked_relative_path, root))
        {
            continue;
        }
        missing_roots.insert(tracked_relative_path);
    }

    let mut missing_tracked_files = Vec::new();
    let mut missing_delete_ops = BTreeMap::new();

    for missing_relative_path in &missing_roots {
        let source =
            classify_exact_path(path_index, namespace_id, missing_relative_path).map_err(|_| {
                ObserveSubtreeError::AmbiguousMatch {
                    namespace_id: namespace_id.as_str().to_owned(),
                    relative_path: missing_relative_path.clone(),
                }
            })?;
        let Some(source) = source else {
            continue;
        };
        match source {
            PathMatch::Bound(row) if row.inode_kind == InodeKind::File => {
                missing_tracked_files.push(MissingTrackedFile {
                    relative_path: missing_relative_path.clone(),
                    source: PathMatch::Bound(row),
                });
            }
            PathMatch::LocalOnly(row) if row.inode_kind == InodeKind::File => {
                missing_tracked_files.push(MissingTrackedFile {
                    relative_path: missing_relative_path.clone(),
                    source: PathMatch::LocalOnly(row),
                });
            }
            PathMatch::Bound(row) => {
                missing_delete_ops.insert(
                    missing_relative_path.clone(),
                    SubtreeObservationOp::DeleteBound {
                        observed: ObservedBoundDelete {
                            namespace_id: namespace_id.clone(),
                            inode_id: row.inode_id,
                            inode_kind: row.inode_kind,
                            content_digest: row.content_digest,
                            parent_inode_id: row.parent_inode_id,
                            display_name: row.display_name,
                            last_local_change_ms: now_ms,
                        },
                    },
                );
            }
            PathMatch::LocalOnly(row) => {
                missing_delete_ops.insert(
                    missing_relative_path.clone(),
                    SubtreeObservationOp::DeleteLocalOnly {
                        client_file_id: row.client_file_id,
                    },
                );
            }
        }
    }

    let mut candidate_targets_by_source = BTreeMap::<String, Vec<String>>::new();
    let mut candidate_sources_by_target = BTreeMap::<String, Vec<String>>::new();
    for source in &missing_tracked_files {
        let Some(source_content_digest) = source_path_content_digest(&source.source) else {
            continue;
        };
        for target in &unmatched_present_files {
            if source_content_digest != target.content_digest {
                continue;
            }
            if !destination_parent_is_valid_for_source(&source.source, &target.parent) {
                continue;
            }
            candidate_targets_by_source
                .entry(source.relative_path.clone())
                .or_default()
                .push(target.relative_path.clone());
            candidate_sources_by_target
                .entry(target.relative_path.clone())
                .or_default()
                .push(source.relative_path.clone());
        }
    }

    for (source_relative_path, target_relative_paths) in &candidate_targets_by_source {
        if target_relative_paths.len() > 1 {
            return Err(ObserveSubtreeError::AmbiguousMovePairing {
                namespace_id: namespace_id.as_str().to_owned(),
                relative_path: source_relative_path.clone(),
            });
        }
    }
    for (target_relative_path, source_relative_paths) in &candidate_sources_by_target {
        if source_relative_paths.len() > 1 {
            return Err(ObserveSubtreeError::AmbiguousMovePairing {
                namespace_id: namespace_id.as_str().to_owned(),
                relative_path: target_relative_path.clone(),
            });
        }
    }

    let paired_source_to_target = candidate_targets_by_source
        .into_iter()
        .filter_map(|(source_relative_path, mut target_relative_paths)| {
            let target_relative_path = target_relative_paths.pop()?;
            Some((source_relative_path, target_relative_path))
        })
        .collect::<BTreeMap<_, _>>();
    let paired_target_to_source = candidate_sources_by_target
        .into_iter()
        .filter_map(|(target_relative_path, mut source_relative_paths)| {
            let source_relative_path = source_relative_paths.pop()?;
            Some((target_relative_path, source_relative_path))
        })
        .collect::<BTreeMap<_, _>>();

    let missing_file_sources = missing_tracked_files
        .into_iter()
        .map(|source| (source.relative_path.clone(), source.source))
        .collect::<BTreeMap<_, _>>();

    for target in unmatched_present_files {
        let Some(source_relative_path) = paired_target_to_source.get(&target.relative_path) else {
            operations.push(SubtreeObservationOp::ObserveLocalOnly {
                observed: ObservedLocalOnlySubtreeInode {
                    relative_path: target.relative_path,
                    namespace_id: namespace_id.clone(),
                    inode_kind: InodeKind::File,
                    parent: subtree_parent_ref_from_destination_parent(&target.parent),
                    display_name: target.display_name,
                    content_digest: Some(target.content_digest),
                    exists_on_disk: true,
                    dirty: true,
                    last_local_change_ms: now_ms,
                },
            });
            continue;
        };

        let source = missing_file_sources
            .get(source_relative_path)
            .expect("paired source path should always resolve to a missing tracked file source");
        match source {
            PathMatch::Bound(row) => {
                let SubtreeDestinationParent::Bound(parent_inode_id) = target.parent.clone() else {
                    continue;
                };
                operations.push(SubtreeObservationOp::MoveBound {
                    from_relative_path: source_relative_path.clone(),
                    observed: ObservedBoundInode {
                        namespace_id: namespace_id.clone(),
                        inode_id: row.inode_id,
                        inode_kind: row.inode_kind.clone(),
                        content_digest: Some(target.content_digest),
                        parent_inode_id: Some(parent_inode_id),
                        display_name: target.display_name,
                        exists_on_disk: true,
                        dirty: true,
                        last_local_change_ms: now_ms,
                    },
                });
            }
            PathMatch::LocalOnly(row) => {
                operations.push(SubtreeObservationOp::MoveLocalOnly {
                    observed: ObservedLocalOnlySubtreeMove {
                        from_relative_path: source_relative_path.clone(),
                        relative_path: target.relative_path,
                        client_file_id: row.client_file_id.clone(),
                        namespace_id: namespace_id.clone(),
                        inode_kind: row.inode_kind.clone(),
                        parent: subtree_parent_ref_from_destination_parent(&target.parent),
                        display_name: target.display_name,
                        content_digest: Some(target.content_digest),
                        exists_on_disk: true,
                        dirty: true,
                        last_local_change_ms: now_ms,
                    },
                });
            }
        }
    }

    for missing_relative_path in missing_roots {
        if paired_source_to_target.contains_key(&missing_relative_path) {
            continue;
        }
        if let Some(operation) = missing_delete_ops.get(&missing_relative_path) {
            operations.push(operation.clone());
            continue;
        }

        if let Some(source) = missing_file_sources.get(&missing_relative_path) {
            match source {
                PathMatch::Bound(row) => operations.push(SubtreeObservationOp::DeleteBound {
                    observed: ObservedBoundDelete {
                        namespace_id: namespace_id.clone(),
                        inode_id: row.inode_id,
                        inode_kind: row.inode_kind.clone(),
                        content_digest: row.content_digest.clone(),
                        parent_inode_id: row.parent_inode_id,
                        display_name: row.display_name.clone(),
                        last_local_change_ms: now_ms,
                    },
                }),
                PathMatch::LocalOnly(row) => {
                    operations.push(SubtreeObservationOp::DeleteLocalOnly {
                        client_file_id: row.client_file_id.clone(),
                    });
                }
            }
        }
    }

    Ok(operations)
}

fn summarize_observe_subtree(
    namespace_id: &NamespaceId,
    relative_path: &str,
    scanned_file_count: usize,
    scanned_dir_count: usize,
    outcomes: &[SubtreeObservationOutcome],
) -> ObserveSubtreeReport {
    let mut report = ObserveSubtreeReport {
        namespace_id: namespace_id.clone(),
        relative_path: relative_path.to_owned(),
        scanned_file_count,
        scanned_dir_count,
        applied_operation_count: outcomes.len(),
        bound_observe_count: 0,
        local_only_observe_count: 0,
        paired_bound_move_count: 0,
        paired_local_only_move_count: 0,
        bound_delete_count: 0,
        local_only_delete_count: 0,
        planned_decision_counts: BTreeMap::new(),
    };

    for outcome in outcomes {
        match outcome {
            SubtreeObservationOutcome::ObservedBound { planned_action, .. } => {
                report.bound_observe_count += 1;
                *report
                    .planned_decision_counts
                    .entry(planned_action.decision.as_str().to_owned())
                    .or_insert(0) += 1;
            }
            SubtreeObservationOutcome::ObservedLocalOnly { result, .. } => {
                report.local_only_observe_count += 1;
                *report
                    .planned_decision_counts
                    .entry(result.planned_action.decision.as_str().to_owned())
                    .or_insert(0) += 1;
            }
            SubtreeObservationOutcome::MovedBound { planned_action, .. } => {
                report.paired_bound_move_count += 1;
                *report
                    .planned_decision_counts
                    .entry(planned_action.decision.as_str().to_owned())
                    .or_insert(0) += 1;
            }
            SubtreeObservationOutcome::MovedLocalOnly { result, .. } => {
                report.paired_local_only_move_count += 1;
                *report
                    .planned_decision_counts
                    .entry(result.planned_action.decision.as_str().to_owned())
                    .or_insert(0) += 1;
            }
            SubtreeObservationOutcome::DeletedBound { planned_action, .. } => {
                report.bound_delete_count += 1;
                *report
                    .planned_decision_counts
                    .entry(planned_action.decision.as_str().to_owned())
                    .or_insert(0) += 1;
            }
            SubtreeObservationOutcome::DeletedLocalOnly { .. } => {
                report.local_only_delete_count += 1;
                *report
                    .planned_decision_counts
                    .entry("no_op".to_owned())
                    .or_insert(0) += 1;
            }
        }
    }

    report
}

fn subtree_parent_ref_from_existing(parent: LocalOnlyParentRef) -> SubtreeLocalOnlyParentRef {
    match parent {
        LocalOnlyParentRef::Bound { parent_inode_id } => {
            SubtreeLocalOnlyParentRef::Bound { parent_inode_id }
        }
        LocalOnlyParentRef::LocalOnly {
            parent_client_file_id,
        } => SubtreeLocalOnlyParentRef::ExistingLocalOnly {
            parent_client_file_id,
        },
    }
}

fn subtree_parent_ref_from_destination_parent(
    parent: &SubtreeDestinationParent,
) -> SubtreeLocalOnlyParentRef {
    match parent {
        SubtreeDestinationParent::Bound(parent_inode_id) => SubtreeLocalOnlyParentRef::Bound {
            parent_inode_id: *parent_inode_id,
        },
        SubtreeDestinationParent::ExistingLocalOnly(parent_client_file_id) => {
            SubtreeLocalOnlyParentRef::ExistingLocalOnly {
                parent_client_file_id: parent_client_file_id.clone(),
            }
        }
        SubtreeDestinationParent::BatchLocalOnly {
            parent_relative_path,
        } => SubtreeLocalOnlyParentRef::BatchLocalOnly {
            parent_relative_path: parent_relative_path.clone(),
        },
    }
}

fn destination_parent_from_subtree_parent_ref(
    parent: &SubtreeLocalOnlyParentRef,
) -> SubtreeDestinationParent {
    match parent {
        SubtreeLocalOnlyParentRef::Bound { parent_inode_id } => {
            SubtreeDestinationParent::Bound(*parent_inode_id)
        }
        SubtreeLocalOnlyParentRef::ExistingLocalOnly {
            parent_client_file_id,
        } => SubtreeDestinationParent::ExistingLocalOnly(parent_client_file_id.clone()),
        SubtreeLocalOnlyParentRef::BatchLocalOnly {
            parent_relative_path,
        } => SubtreeDestinationParent::BatchLocalOnly {
            parent_relative_path: parent_relative_path.clone(),
        },
    }
}

fn source_path_content_digest(source: &PathMatch) -> Option<&str> {
    match source {
        PathMatch::Bound(row) => row.content_digest.as_deref(),
        PathMatch::LocalOnly(row) => row.content_digest.as_deref(),
    }
}

fn destination_parent_is_valid_for_source(
    source: &PathMatch,
    parent: &SubtreeDestinationParent,
) -> bool {
    match source {
        PathMatch::Bound(_) => matches!(parent, SubtreeDestinationParent::Bound(_)),
        PathMatch::LocalOnly(_) => true,
    }
}

fn matches_scanned_kind(inode_kind: InodeKind, scanned_kind: ScannedEntryKind) -> bool {
    matches!(
        (inode_kind, scanned_kind),
        (InodeKind::File, ScannedEntryKind::File) | (InodeKind::Dir, ScannedEntryKind::Dir)
    )
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
