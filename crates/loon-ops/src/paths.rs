use loon_client::state_db::{
    ClientFileId, ClientNamespaceStateSummary, LocalFileStateRow, LocalOnlyFileStateRow,
    LocalOnlyParentLinkRow, RemoteFileStateRow, SyncAnchorRow,
};
use loon_types::InodeId;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NamespacePathIndex {
    local_paths_by_inode: BTreeMap<InodeId, String>,
    anchor_paths_by_inode: BTreeMap<InodeId, String>,
    remote_paths_by_inode: BTreeMap<InodeId, String>,
    local_only_paths_by_client_file_id: BTreeMap<String, String>,
    bound_files_by_path: BTreeMap<String, Vec<LocalFileStateRow>>,
    bound_dirs_by_path: BTreeMap<String, Vec<LocalFileStateRow>>,
    local_only_files_by_path: BTreeMap<String, Vec<LocalOnlyFileStateRow>>,
    local_only_dirs_by_path: BTreeMap<String, Vec<LocalOnlyFileStateRow>>,
}

impl NamespacePathIndex {
    pub(crate) fn build(
        summary: &ClientNamespaceStateSummary,
        local_only_parent_links: &[LocalOnlyParentLinkRow],
    ) -> Self {
        let local_paths_by_inode = build_local_paths(&summary.local_state);
        let anchor_paths_by_inode = build_anchor_paths(&summary.sync_anchors);
        let remote_paths_by_inode = build_remote_paths(&summary.remote_state);
        let local_only_paths_by_client_file_id = build_local_only_paths(
            &summary.local_only_state,
            local_only_parent_links,
            &local_paths_by_inode,
        );

        let mut bound_files_by_path = BTreeMap::new();
        let mut bound_dirs_by_path = BTreeMap::new();
        let mut local_only_files_by_path = BTreeMap::new();
        let mut local_only_dirs_by_path = BTreeMap::new();

        for row in &summary.local_state {
            let Some(path) = local_paths_by_inode.get(&row.inode_id) else {
                continue;
            };
            match row.inode_kind {
                loon_types::InodeKind::File => {
                    bound_files_by_path
                        .entry(path.clone())
                        .or_insert_with(Vec::new)
                        .push(row.clone());
                }
                loon_types::InodeKind::Dir => {
                    bound_dirs_by_path
                        .entry(path.clone())
                        .or_insert_with(Vec::new)
                        .push(row.clone());
                }
                loon_types::InodeKind::Symlink | loon_types::InodeKind::Mount => {}
            }
        }

        for row in &summary.local_only_state {
            let Some(path) = local_only_paths_by_client_file_id.get(row.client_file_id.as_str())
            else {
                continue;
            };
            match row.inode_kind {
                loon_types::InodeKind::File => {
                    local_only_files_by_path
                        .entry(path.clone())
                        .or_insert_with(Vec::new)
                        .push(row.clone());
                }
                loon_types::InodeKind::Dir => {
                    local_only_dirs_by_path
                        .entry(path.clone())
                        .or_insert_with(Vec::new)
                        .push(row.clone());
                }
                loon_types::InodeKind::Symlink | loon_types::InodeKind::Mount => {}
            }
        }

        Self {
            local_paths_by_inode,
            anchor_paths_by_inode,
            remote_paths_by_inode,
            local_only_paths_by_client_file_id,
            bound_files_by_path,
            bound_dirs_by_path,
            local_only_files_by_path,
            local_only_dirs_by_path,
        }
    }

    pub(crate) fn bound_file_matches(&self, relative_path: &str) -> &[LocalFileStateRow] {
        self.bound_files_by_path
            .get(relative_path)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    pub(crate) fn bound_dir_matches(&self, relative_path: &str) -> &[LocalFileStateRow] {
        self.bound_dirs_by_path
            .get(relative_path)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    pub(crate) fn local_only_file_matches(&self, relative_path: &str) -> &[LocalOnlyFileStateRow] {
        self.local_only_files_by_path
            .get(relative_path)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    pub(crate) fn local_only_dir_matches(&self, relative_path: &str) -> &[LocalOnlyFileStateRow] {
        self.local_only_dirs_by_path
            .get(relative_path)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    pub(crate) fn resolve_local_only_source_relative_path(
        &self,
        client_file_id: &ClientFileId,
    ) -> Option<&str> {
        self.local_only_paths_by_client_file_id
            .get(client_file_id.as_str())
            .map(String::as_str)
    }

    pub(crate) fn resolve_current_inode_relative_path(&self, inode_id: InodeId) -> Option<&str> {
        self.local_paths_by_inode
            .get(&inode_id)
            .or_else(|| self.anchor_paths_by_inode.get(&inode_id))
            .or_else(|| self.remote_paths_by_inode.get(&inode_id))
            .map(String::as_str)
    }

    pub(crate) fn resolve_target_inode_relative_path(
        &self,
        inode_id: InodeId,
        parent_inode_id: Option<InodeId>,
        display_name: &str,
    ) -> Option<String> {
        if let Some(path) = self.remote_paths_by_inode.get(&inode_id) {
            return Some(path.clone());
        }
        if let Some(path) = self.anchor_paths_by_inode.get(&inode_id) {
            return Some(path.clone());
        }
        if let Some(path) = self.local_paths_by_inode.get(&inode_id) {
            return Some(path.clone());
        }

        match parent_inode_id {
            None => Some(String::new()),
            Some(parent_inode_id) => self
                .remote_paths_by_inode
                .get(&parent_inode_id)
                .or_else(|| self.anchor_paths_by_inode.get(&parent_inode_id))
                .or_else(|| self.local_paths_by_inode.get(&parent_inode_id))
                .map(|parent| join_relative_path(parent, display_name)),
        }
    }
}

pub(crate) fn join_under_mirror_root(
    mirror_root: &std::path::Path,
    relative_path: &str,
) -> std::path::PathBuf {
    if relative_path.is_empty() {
        mirror_root.to_path_buf()
    } else {
        mirror_root.join(relative_path)
    }
}

fn build_local_paths(rows: &[LocalFileStateRow]) -> BTreeMap<InodeId, String> {
    let nodes = rows
        .iter()
        .map(|row| {
            (
                row.inode_id,
                PathNode {
                    parent_inode_id: row.parent_inode_id,
                    display_name: row.display_name.clone(),
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    build_relative_paths(&nodes)
}

fn build_anchor_paths(rows: &[SyncAnchorRow]) -> BTreeMap<InodeId, String> {
    let nodes = rows
        .iter()
        .map(|row| {
            (
                row.inode_id,
                PathNode {
                    parent_inode_id: row.parent_inode_id,
                    display_name: row.display_name.clone(),
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    build_relative_paths(&nodes)
}

fn build_remote_paths(rows: &[RemoteFileStateRow]) -> BTreeMap<InodeId, String> {
    let nodes = rows
        .iter()
        .map(|row| {
            (
                row.inode_id,
                PathNode {
                    parent_inode_id: row.parent_inode_id,
                    display_name: row.display_name.clone(),
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    build_relative_paths(&nodes)
}

fn build_local_only_paths(
    rows: &[LocalOnlyFileStateRow],
    parent_links: &[LocalOnlyParentLinkRow],
    local_paths_by_inode: &BTreeMap<InodeId, String>,
) -> BTreeMap<String, String> {
    fn resolve(
        client_file_id: &str,
        rows_by_id: &BTreeMap<String, &LocalOnlyFileStateRow>,
        parent_by_child: &BTreeMap<String, String>,
        local_paths_by_inode: &BTreeMap<InodeId, String>,
        resolved: &mut BTreeMap<String, String>,
        visiting: &mut BTreeSet<String>,
    ) -> Option<String> {
        if let Some(path) = resolved.get(client_file_id) {
            return Some(path.clone());
        }
        let row = rows_by_id.get(client_file_id)?;
        if !visiting.insert(client_file_id.to_owned()) {
            return None;
        }
        let path = if let Some(parent_inode_id) = row.parent_inode_id {
            let parent_path = local_paths_by_inode.get(&parent_inode_id)?;
            join_relative_path(parent_path, &row.display_name)
        } else if let Some(parent_client_file_id) = parent_by_child.get(client_file_id) {
            let parent_path = resolve(
                parent_client_file_id,
                rows_by_id,
                parent_by_child,
                local_paths_by_inode,
                resolved,
                visiting,
            )?;
            join_relative_path(&parent_path, &row.display_name)
        } else {
            join_relative_path("", &row.display_name)
        };
        visiting.remove(client_file_id);
        resolved.insert(client_file_id.to_owned(), path.clone());
        Some(path)
    }

    let rows_by_id = rows
        .iter()
        .map(|row| (row.client_file_id.as_str().to_owned(), row))
        .collect::<BTreeMap<_, _>>();
    let parent_by_child = parent_links
        .iter()
        .map(|link| {
            (
                link.client_file_id.as_str().to_owned(),
                link.parent_client_file_id.as_str().to_owned(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut resolved = BTreeMap::new();
    for client_file_id in rows_by_id.keys() {
        let mut visiting = BTreeSet::new();
        let _ = resolve(
            client_file_id,
            &rows_by_id,
            &parent_by_child,
            local_paths_by_inode,
            &mut resolved,
            &mut visiting,
        );
    }
    resolved
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PathNode {
    parent_inode_id: Option<InodeId>,
    display_name: String,
}

fn build_relative_paths(nodes: &BTreeMap<InodeId, PathNode>) -> BTreeMap<InodeId, String> {
    fn resolve(
        inode_id: InodeId,
        nodes: &BTreeMap<InodeId, PathNode>,
        resolved: &mut BTreeMap<InodeId, String>,
        visiting: &mut BTreeSet<InodeId>,
    ) -> Option<String> {
        if let Some(path) = resolved.get(&inode_id) {
            return Some(path.clone());
        }
        let node = nodes.get(&inode_id)?;
        if !visiting.insert(inode_id) {
            return None;
        }
        let path = match node.parent_inode_id {
            None => String::new(),
            Some(parent_inode_id) => {
                let parent_path = resolve(parent_inode_id, nodes, resolved, visiting)?;
                join_relative_path(&parent_path, &node.display_name)
            }
        };
        visiting.remove(&inode_id);
        resolved.insert(inode_id, path.clone());
        Some(path)
    }

    let mut resolved = BTreeMap::new();
    for inode_id in nodes.keys().copied().collect::<Vec<_>>() {
        let mut visiting = BTreeSet::new();
        let _ = resolve(inode_id, nodes, &mut resolved, &mut visiting);
    }
    resolved
}

fn join_relative_path(parent_relative_path: &str, display_name: &str) -> String {
    if parent_relative_path.is_empty() {
        display_name.to_owned()
    } else {
        format!("{parent_relative_path}/{display_name}")
    }
}
