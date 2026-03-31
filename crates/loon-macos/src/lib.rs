#![forbid(unsafe_code)]

use loon_client::local_fs::{join_under_mirror_root, NamespacePathIndex};
use loon_client::provider::{
    materialize_inode_to_mirror_root, materialize_local_only_to_mirror_root,
    ProviderMaterializedPath, ProviderTargetedMaterializeError,
};
use loon_client::state_db::{
    ClientFileId, ClientNamespaceStateSummary, FileSyncViews, LocalOnlyParentLinkRow,
    LocalOnlyParentRef, SqliteStateDb, StateDbError,
};
use loon_ops::OpsConfig;
use loon_types::{InodeId, InodeKind, NamespaceId, RevisionNo};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileProviderSpikeConfig {
    pub ops_config: OpsConfig,
    pub exposed_namespaces: BTreeSet<NamespaceId>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ProviderItemId {
    Root,
    NamespaceRoot {
        namespace_id: NamespaceId,
    },
    BoundInode {
        namespace_id: NamespaceId,
        inode_id: InodeId,
    },
    LocalOnly {
        namespace_id: NamespaceId,
        client_file_id: ClientFileId,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderMaterializationState {
    SyntheticDir,
    Materialized,
    Placeholder,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderProjectionWarning {
    pub namespace_id: NamespaceId,
    pub relative_path: String,
    pub inode_kind: InodeKind,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderItemSnapshot {
    pub item_id: ProviderItemId,
    pub parent_item_id: Option<ProviderItemId>,
    pub display_name: String,
    pub inode_kind: InodeKind,
    pub materialization_state: ProviderMaterializationState,
    pub current_relative_path: Option<String>,
    pub size_bytes: Option<u64>,
    pub revision_no: Option<RevisionNo>,
    pub content_digest: Option<String>,
    pub content_manifest_digest: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderListing {
    pub items: Vec<ProviderItemSnapshot>,
    pub warnings: Vec<ProviderProjectionWarning>,
}

#[derive(Debug, Error)]
pub enum FileProviderBridgeError {
    #[error(transparent)]
    StateDb(#[from] StateDbError),
    #[error(transparent)]
    TargetedMaterialize(#[from] ProviderTargetedMaterializeError),
    #[error(transparent)]
    OpsConfig(#[from] anyhow::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("provider_state_db_missing: {0}")]
    StateDbMissing(String),
    #[error("provider_namespace_not_exposed: `{0}`")]
    NamespaceNotExposed(String),
}

pub struct FileProviderBridge {
    config: FileProviderSpikeConfig,
}

impl FileProviderBridge {
    pub fn new(config: FileProviderSpikeConfig) -> Result<Self, FileProviderBridgeError> {
        config.ops_config.open_store()?;
        Ok(Self { config })
    }

    pub fn list_root(&self) -> Result<ProviderListing, FileProviderBridgeError> {
        Ok(ProviderListing {
            items: self
                .config
                .exposed_namespaces
                .iter()
                .cloned()
                .map(|namespace_id| ProviderItemSnapshot {
                    item_id: ProviderItemId::NamespaceRoot {
                        namespace_id: namespace_id.clone(),
                    },
                    parent_item_id: Some(ProviderItemId::Root),
                    display_name: namespace_id.as_str().to_owned(),
                    inode_kind: InodeKind::Dir,
                    materialization_state: ProviderMaterializationState::SyntheticDir,
                    current_relative_path: None,
                    size_bytes: None,
                    revision_no: None,
                    content_digest: None,
                    content_manifest_digest: None,
                })
                .collect(),
            warnings: Vec::new(),
        })
    }

    pub fn lookup_item(
        &self,
        item_id: &ProviderItemId,
    ) -> Result<Option<ProviderItemSnapshot>, FileProviderBridgeError> {
        match item_id {
            ProviderItemId::Root => Ok(Some(self.root_snapshot())),
            ProviderItemId::NamespaceRoot { namespace_id } => {
                if !self.config.exposed_namespaces.contains(namespace_id) {
                    return Ok(None);
                }
                Ok(Some(self.namespace_root_snapshot(namespace_id)))
            }
            ProviderItemId::BoundInode { namespace_id, .. }
            | ProviderItemId::LocalOnly { namespace_id, .. } => {
                self.ensure_namespace_exposed(namespace_id)?;
                let projection = self.build_namespace_projection(namespace_id)?;
                Ok(projection.items.get(item_id).cloned())
            }
        }
    }

    pub fn list_children(
        &self,
        parent_id: &ProviderItemId,
    ) -> Result<ProviderListing, FileProviderBridgeError> {
        match parent_id {
            ProviderItemId::Root => self.list_root(),
            ProviderItemId::NamespaceRoot { namespace_id } => {
                self.ensure_namespace_exposed(namespace_id)?;
                let projection = self.build_namespace_projection(namespace_id)?;
                Ok(ProviderListing {
                    items: projection
                        .children
                        .get(parent_id)
                        .cloned()
                        .unwrap_or_default(),
                    warnings: projection.warnings,
                })
            }
            ProviderItemId::BoundInode { namespace_id, .. }
            | ProviderItemId::LocalOnly { namespace_id, .. } => {
                self.ensure_namespace_exposed(namespace_id)?;
                let projection = self.build_namespace_projection(namespace_id)?;
                Ok(ProviderListing {
                    items: projection
                        .children
                        .get(parent_id)
                        .cloned()
                        .unwrap_or_default(),
                    warnings: projection.warnings,
                })
            }
        }
    }

    pub fn materialize_item(
        &self,
        item_id: &ProviderItemId,
        now_ms: u64,
    ) -> Result<ProviderMaterializedPath, FileProviderBridgeError> {
        match item_id {
            ProviderItemId::Root => {
                fs::create_dir_all(&self.config.ops_config.client.mirror_root)?;
                Ok(ProviderMaterializedPath {
                    absolute_path: self.config.ops_config.client.mirror_root.clone(),
                    relative_path: String::new(),
                })
            }
            ProviderItemId::NamespaceRoot { namespace_id } => {
                self.ensure_namespace_exposed(namespace_id)?;
                let namespace_root = self.namespace_cache_root(namespace_id);
                fs::create_dir_all(&namespace_root)?;
                Ok(ProviderMaterializedPath {
                    absolute_path: namespace_root,
                    relative_path: String::new(),
                })
            }
            ProviderItemId::BoundInode {
                namespace_id,
                inode_id,
            } => {
                self.ensure_namespace_exposed(namespace_id)?;
                fs::create_dir_all(&self.config.ops_config.client.mirror_root)?;
                let mut db = self.open_db()?;
                let store = self.config.ops_config.open_store()?;
                materialize_inode_to_mirror_root(
                    &mut db,
                    &store,
                    namespace_id,
                    *inode_id,
                    &self.namespace_cache_root(namespace_id),
                    now_ms,
                )
                .map_err(FileProviderBridgeError::from)
            }
            ProviderItemId::LocalOnly {
                namespace_id,
                client_file_id,
            } => {
                self.ensure_namespace_exposed(namespace_id)?;
                fs::create_dir_all(&self.config.ops_config.client.mirror_root)?;
                let mut db = self.open_db()?;
                materialize_local_only_to_mirror_root(
                    &mut db,
                    client_file_id,
                    &self.namespace_cache_root(namespace_id),
                )
                .map_err(FileProviderBridgeError::from)
            }
        }
    }

    fn root_snapshot(&self) -> ProviderItemSnapshot {
        ProviderItemSnapshot {
            item_id: ProviderItemId::Root,
            parent_item_id: None,
            display_name: String::new(),
            inode_kind: InodeKind::Dir,
            materialization_state: ProviderMaterializationState::SyntheticDir,
            current_relative_path: None,
            size_bytes: None,
            revision_no: None,
            content_digest: None,
            content_manifest_digest: None,
        }
    }

    fn namespace_root_snapshot(&self, namespace_id: &NamespaceId) -> ProviderItemSnapshot {
        ProviderItemSnapshot {
            item_id: ProviderItemId::NamespaceRoot {
                namespace_id: namespace_id.clone(),
            },
            parent_item_id: Some(ProviderItemId::Root),
            display_name: namespace_id.as_str().to_owned(),
            inode_kind: InodeKind::Dir,
            materialization_state: ProviderMaterializationState::SyntheticDir,
            current_relative_path: None,
            size_bytes: None,
            revision_no: None,
            content_digest: None,
            content_manifest_digest: None,
        }
    }

    fn namespace_cache_root(&self, namespace_id: &NamespaceId) -> PathBuf {
        self.config
            .ops_config
            .client
            .mirror_root
            .join(namespace_id.as_str())
    }

    fn ensure_namespace_exposed(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<(), FileProviderBridgeError> {
        if self.config.exposed_namespaces.contains(namespace_id) {
            Ok(())
        } else {
            Err(FileProviderBridgeError::NamespaceNotExposed(
                namespace_id.as_str().to_owned(),
            ))
        }
    }

    fn open_db(&self) -> Result<SqliteStateDb, FileProviderBridgeError> {
        let db_path = &self.config.ops_config.client.state_db_path;
        if !db_path.is_file() {
            return Err(FileProviderBridgeError::StateDbMissing(
                db_path.display().to_string(),
            ));
        }
        SqliteStateDb::open(db_path).map_err(FileProviderBridgeError::from)
    }

    fn build_namespace_projection(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<NamespaceProjection, FileProviderBridgeError> {
        let db = self.open_db()?;
        let summary = db.load_namespace_state_summary(namespace_id)?;
        let parent_links = db.load_local_only_parent_links_for_namespace(namespace_id)?;
        Ok(NamespaceProjection::build(
            namespace_id,
            &self.namespace_cache_root(namespace_id),
            &summary,
            &parent_links,
        ))
    }
}

struct NamespaceProjection {
    items: BTreeMap<ProviderItemId, ProviderItemSnapshot>,
    children: BTreeMap<ProviderItemId, Vec<ProviderItemSnapshot>>,
    warnings: Vec<ProviderProjectionWarning>,
}

impl NamespaceProjection {
    fn build(
        namespace_id: &NamespaceId,
        namespace_root: &std::path::Path,
        summary: &ClientNamespaceStateSummary,
        parent_links: &[LocalOnlyParentLinkRow],
    ) -> Self {
        let path_index = NamespacePathIndex::build(summary, parent_links);
        let mut items = BTreeMap::new();
        let mut warnings = Vec::new();

        let inode_ids = summary
            .local_state
            .iter()
            .map(|row| row.inode_id)
            .chain(summary.remote_state.iter().map(|row| row.inode_id))
            .chain(summary.sync_anchors.iter().map(|row| row.inode_id))
            .collect::<BTreeSet<_>>();

        for inode_id in inode_ids {
            let views = FileSyncViews {
                namespace_id: namespace_id.clone(),
                inode_id,
                remote: summary
                    .remote_state
                    .iter()
                    .find(|row| row.inode_id == inode_id)
                    .cloned(),
                local: summary
                    .local_state
                    .iter()
                    .find(|row| row.inode_id == inode_id)
                    .cloned(),
                sync_anchor: summary
                    .sync_anchors
                    .iter()
                    .find(|row| row.inode_id == inode_id)
                    .cloned(),
            };

            if is_root_inode(&views) {
                continue;
            }

            let inode_kind = match choose_inode_kind(&views) {
                Some(kind) => kind,
                None => continue,
            };
            let relative_path = resolve_bound_relative_path(&path_index, &views, inode_id);
            if inode_kind == InodeKind::Symlink || inode_kind == InodeKind::Mount {
                warnings.push(ProviderProjectionWarning {
                    namespace_id: namespace_id.clone(),
                    relative_path: relative_path.unwrap_or_default(),
                    inode_kind: inode_kind.clone(),
                    reason: "unsupported_inode_kind".to_owned(),
                });
                continue;
            }
            if views.remote.as_ref().is_some_and(|row| row.is_deleted) {
                continue;
            }

            let Some(relative_path) = relative_path else {
                continue;
            };
            let item_id = ProviderItemId::BoundInode {
                namespace_id: namespace_id.clone(),
                inode_id,
            };
            let parent_item_id = bound_parent_item_id(namespace_id, &views);
            let materialization_state =
                bound_materialization_state(summary, namespace_id, inode_id, &views);
            items.insert(
                item_id.clone(),
                ProviderItemSnapshot {
                    item_id,
                    parent_item_id,
                    display_name: display_name_for_bound(&views),
                    inode_kind,
                    materialization_state,
                    current_relative_path: Some(relative_path.clone()),
                    size_bytes: materialized_size_bytes(namespace_root, &relative_path),
                    revision_no: bound_revision_no(&views),
                    content_digest: bound_content_digest(&views),
                    content_manifest_digest: bound_content_manifest_digest(&views),
                },
            );
        }

        for row in &summary.local_only_state {
            let relative_path = path_index
                .resolve_local_only_source_relative_path(&row.client_file_id)
                .map(str::to_owned);
            let Some(relative_path) = relative_path else {
                continue;
            };
            if row.inode_kind == InodeKind::Symlink || row.inode_kind == InodeKind::Mount {
                warnings.push(ProviderProjectionWarning {
                    namespace_id: namespace_id.clone(),
                    relative_path,
                    inode_kind: row.inode_kind.clone(),
                    reason: "unsupported_inode_kind".to_owned(),
                });
                continue;
            }

            let item_id = ProviderItemId::LocalOnly {
                namespace_id: namespace_id.clone(),
                client_file_id: row.client_file_id.clone(),
            };
            let parent_item_id = match path_index.local_only_parent_ref_for(row) {
                Some(LocalOnlyParentRef::Bound { parent_inode_id }) if parent_inode_id.0 == 1 => {
                    Some(ProviderItemId::NamespaceRoot {
                        namespace_id: namespace_id.clone(),
                    })
                }
                Some(LocalOnlyParentRef::Bound { parent_inode_id }) => {
                    Some(ProviderItemId::BoundInode {
                        namespace_id: namespace_id.clone(),
                        inode_id: parent_inode_id,
                    })
                }
                Some(LocalOnlyParentRef::LocalOnly {
                    parent_client_file_id,
                }) => Some(ProviderItemId::LocalOnly {
                    namespace_id: namespace_id.clone(),
                    client_file_id: parent_client_file_id,
                }),
                None => Some(ProviderItemId::NamespaceRoot {
                    namespace_id: namespace_id.clone(),
                }),
            };
            items.insert(
                item_id.clone(),
                ProviderItemSnapshot {
                    item_id,
                    parent_item_id,
                    display_name: row.display_name.clone(),
                    inode_kind: row.inode_kind.clone(),
                    materialization_state: if row.exists_on_disk {
                        ProviderMaterializationState::Materialized
                    } else {
                        ProviderMaterializationState::Unavailable
                    },
                    current_relative_path: Some(relative_path.clone()),
                    size_bytes: materialized_size_bytes(namespace_root, &relative_path),
                    revision_no: None,
                    content_digest: row.content_digest.clone(),
                    content_manifest_digest: None,
                },
            );
        }

        let mut children = BTreeMap::<ProviderItemId, Vec<ProviderItemSnapshot>>::new();
        for item in items.values().cloned() {
            if let Some(parent) = item.parent_item_id.clone() {
                children.entry(parent).or_default().push(item);
            }
        }
        for snapshots in children.values_mut() {
            snapshots.sort_by(|left, right| {
                left.display_name
                    .cmp(&right.display_name)
                    .then_with(|| left.item_id.cmp(&right.item_id))
            });
        }

        warnings.sort_by(|left, right| {
            left.relative_path
                .cmp(&right.relative_path)
                .then_with(|| left.inode_kind_text().cmp(right.inode_kind_text()))
        });

        Self {
            items,
            children,
            warnings,
        }
    }
}

impl ProviderProjectionWarning {
    fn inode_kind_text(&self) -> &'static str {
        match self.inode_kind {
            InodeKind::File => "file",
            InodeKind::Dir => "dir",
            InodeKind::Symlink => "symlink",
            InodeKind::Mount => "mount",
        }
    }
}

fn choose_inode_kind(views: &FileSyncViews) -> Option<InodeKind> {
    views
        .local
        .as_ref()
        .map(|row| row.inode_kind.clone())
        .or_else(|| views.remote.as_ref().map(|row| row.inode_kind.clone()))
        .or_else(|| views.sync_anchor.as_ref().map(|row| row.inode_kind.clone()))
}

fn is_root_inode(views: &FileSyncViews) -> bool {
    views.inode_id.0 == 1
        && views
            .local
            .as_ref()
            .map(|row| row.parent_inode_id.is_none())
            .unwrap_or_else(|| {
                views
                    .remote
                    .as_ref()
                    .map(|row| row.parent_inode_id.is_none())
                    .or_else(|| {
                        views
                            .sync_anchor
                            .as_ref()
                            .map(|row| row.parent_inode_id.is_none())
                    })
                    .unwrap_or(false)
            })
}

fn resolve_bound_relative_path(
    path_index: &NamespacePathIndex,
    views: &FileSyncViews,
    inode_id: InodeId,
) -> Option<String> {
    if let Some(path) = path_index.resolve_current_inode_relative_path(inode_id) {
        return Some(path.to_owned());
    }
    if let Some(remote) = &views.remote {
        return path_index.resolve_target_inode_relative_path(
            inode_id,
            remote.parent_inode_id,
            &remote.display_name,
        );
    }
    if let Some(anchor) = &views.sync_anchor {
        return path_index.resolve_target_inode_relative_path(
            inode_id,
            anchor.parent_inode_id,
            &anchor.display_name,
        );
    }
    if let Some(local) = &views.local {
        return path_index.resolve_target_inode_relative_path(
            inode_id,
            local.parent_inode_id,
            &local.display_name,
        );
    }
    None
}

fn display_name_for_bound(views: &FileSyncViews) -> String {
    views
        .local
        .as_ref()
        .map(|row| row.display_name.clone())
        .or_else(|| views.remote.as_ref().map(|row| row.display_name.clone()))
        .or_else(|| {
            views
                .sync_anchor
                .as_ref()
                .map(|row| row.display_name.clone())
        })
        .unwrap_or_default()
}

fn bound_parent_item_id(
    namespace_id: &NamespaceId,
    views: &FileSyncViews,
) -> Option<ProviderItemId> {
    let parent_inode_id = views
        .local
        .as_ref()
        .and_then(|row| row.parent_inode_id)
        .or_else(|| views.remote.as_ref().and_then(|row| row.parent_inode_id))
        .or_else(|| {
            views
                .sync_anchor
                .as_ref()
                .and_then(|row| row.parent_inode_id)
        });

    match parent_inode_id {
        None => None,
        Some(parent_inode_id) if parent_inode_id.0 == 1 => Some(ProviderItemId::NamespaceRoot {
            namespace_id: namespace_id.clone(),
        }),
        Some(parent_inode_id) => Some(ProviderItemId::BoundInode {
            namespace_id: namespace_id.clone(),
            inode_id: parent_inode_id,
        }),
    }
}

fn bound_materialization_state(
    summary: &ClientNamespaceStateSummary,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    views: &FileSyncViews,
) -> ProviderMaterializationState {
    if views.local.as_ref().is_some_and(|row| row.exists_on_disk) {
        return ProviderMaterializationState::Materialized;
    }
    if summary
        .conflicts_and_errors
        .iter()
        .any(|row| row.inode_id == inode_id)
        || summary
            .pending_inode_mutations
            .iter()
            .any(|row| row.inode_id == inode_id)
    {
        return ProviderMaterializationState::Unavailable;
    }
    if let Some(planned) = summary
        .planned_actions
        .iter()
        .find(|row| row.inode_id == inode_id)
    {
        if planned.decision == "download_remote_edit"
            || planned.decision == "materialize_remote_dir"
        {
            return ProviderMaterializationState::Placeholder;
        }
        return ProviderMaterializationState::Unavailable;
    }
    if bound_is_remote_placeholder(views) {
        return ProviderMaterializationState::Placeholder;
    }
    if views.remote.is_some() && views.local.is_none() {
        return ProviderMaterializationState::Placeholder;
    }
    let _ = namespace_id;
    ProviderMaterializationState::Unavailable
}

fn bound_is_remote_placeholder(views: &FileSyncViews) -> bool {
    let Some(local) = views.local.as_ref() else {
        return false;
    };
    let Some(remote) = views.remote.as_ref() else {
        return false;
    };
    !local.exists_on_disk
        && !local.dirty
        && local.inode_kind == remote.inode_kind
        && local.parent_inode_id == remote.parent_inode_id
        && local.display_name == remote.display_name
}

fn materialized_size_bytes(namespace_root: &std::path::Path, relative_path: &str) -> Option<u64> {
    let absolute_path = join_under_mirror_root(namespace_root, relative_path);
    fs::metadata(absolute_path)
        .ok()
        .and_then(|metadata| metadata.is_file().then_some(metadata.len()))
}

fn bound_revision_no(views: &FileSyncViews) -> Option<RevisionNo> {
    views
        .remote
        .as_ref()
        .map(|row| row.revision_no)
        .or_else(|| views.sync_anchor.as_ref().map(|row| row.revision_no))
}

fn bound_content_digest(views: &FileSyncViews) -> Option<String> {
    views
        .local
        .as_ref()
        .and_then(|row| row.content_digest.clone())
        .or_else(|| {
            views
                .remote
                .as_ref()
                .and_then(|row| row.content_digest.clone())
        })
        .or_else(|| {
            views
                .sync_anchor
                .as_ref()
                .and_then(|row| row.content_digest.clone())
        })
}

fn bound_content_manifest_digest(views: &FileSyncViews) -> Option<String> {
    views
        .remote
        .as_ref()
        .and_then(|row| row.content_manifest_digest.clone())
        .or_else(|| {
            views
                .sync_anchor
                .as_ref()
                .and_then(|row| row.content_manifest_digest.clone())
        })
}
