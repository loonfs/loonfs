use super::{manifest_index, row_decode::unbind_matches_binding};
use crate::checkpoint::VerifiedMetadataTables;
use crate::error::CoreError;
use crate::metadata::{
    CommitReceiptRecord, DirentryBindRecord, DirentryUnbindRecord, InodeRecord, MetadataState,
    ResolvedVisiblePath, RevisionRecord, SubtreeTombstoneRecord, VisiblePathError,
};
use loonfs_api::wire::control::HeadState;
use loonfs_api::{AbsolutePath, CommitId, InodeId, InodeKind, NameKey, RevisionNo};
use loonfs_objectstore::ObjectStore;

pub(crate) struct CurrentManifestTailView<'a, S: ObjectStore + ?Sized> {
    head: &'a HeadState,
    tables: &'a VerifiedMetadataTables<'a, S>,
    wal_tail_rows: &'a MetadataState,
}

impl<'a, S: ObjectStore + ?Sized> CurrentManifestTailView<'a, S> {
    pub(crate) fn new(
        head: &'a HeadState,
        tables: &'a VerifiedMetadataTables<'a, S>,
        wal_tail_rows: &'a MetadataState,
    ) -> Self {
        Self {
            head,
            tables,
            wal_tail_rows,
        }
    }

    pub(crate) async fn visible_children(
        &self,
        parent_inode_id: InodeId,
    ) -> Result<Vec<DirentryBindRecord>, CoreError> {
        let Some(parent) = self.visible_inode(parent_inode_id).await? else {
            return Ok(Vec::new());
        };
        if parent.inode_kind != InodeKind::Dir {
            return Ok(Vec::new());
        }

        let mut candidates = self.direntry_binds_for_parent(parent_inode_id).await?;
        candidates.extend(
            self.wal_tail_rows
                .direntry_binds()
                .iter()
                .filter(|direntry| direntry.parent_inode_id == parent_inode_id)
                .cloned(),
        );
        let mut children = Vec::new();
        for direntry in candidates {
            let active = self
                .visible_child(parent_inode_id, &direntry.name_key)
                .await?;
            if active
                .as_ref()
                .map(|active| {
                    active.child_inode_id == direntry.child_inode_id
                        && active.bind_seq == direntry.bind_seq
                        && active.bind_delta_index == direntry.bind_delta_index
                })
                .unwrap_or(false)
                && self.visible_inode(direntry.child_inode_id).await?.is_some()
            {
                children.push(direntry);
            }
        }
        children.sort_by(|left, right| {
            left.display_name
                .cmp(&right.display_name)
                .then(left.child_inode_id.0.cmp(&right.child_inode_id.0))
        });
        Ok(children)
    }

    pub(crate) async fn resolve_visible_path(
        &self,
        absolute_path: &AbsolutePath,
    ) -> Result<ResolvedVisiblePath, CoreError> {
        let root_inode_id = InodeId(1);
        let root = self
            .visible_inode(root_inode_id)
            .await?
            .ok_or(VisiblePathError::RootMissing)?;
        if absolute_path.is_root() {
            return Ok(ResolvedVisiblePath {
                absolute_path: absolute_path.as_str().to_owned(),
                inode_id: root_inode_id,
                inode_kind: root.inode_kind,
                parent_inode_id: None,
                display_name: String::new(),
            });
        }

        let mut current_inode_id = root_inode_id;
        let mut current_absolute_path = String::from("/");
        let mut current_parent_inode_id = None;
        let mut current_display_name = String::new();

        for component in absolute_path.components() {
            let current_inode = self.visible_inode(current_inode_id).await?.ok_or_else(|| {
                VisiblePathError::PathNotFound {
                    absolute_path: current_absolute_path.clone(),
                }
            })?;
            if current_inode.inode_kind != InodeKind::Dir {
                return Err(VisiblePathError::PathComponentNotDirectory {
                    absolute_path: current_absolute_path,
                    inode_id: current_inode_id,
                    inode_kind: current_inode.inode_kind,
                }
                .into());
            }

            let requested_absolute_path = if current_absolute_path == "/" {
                format!("/{}", component.as_str())
            } else {
                format!("{}/{}", current_absolute_path, component.as_str())
            };
            let display_name = component.to_display_name();
            let name_key = NameKey::for_display_name(self.head.name_policy, &display_name);
            let direntry = self
                .visible_child(current_inode_id, name_key.as_str())
                .await?
                .ok_or(VisiblePathError::PathNotFound {
                    absolute_path: requested_absolute_path,
                })?;

            current_parent_inode_id = Some(current_inode_id);
            current_inode_id = direntry.child_inode_id;
            current_absolute_path =
                absolute_path_prefix(&current_absolute_path, &direntry.display_name);
            current_display_name = direntry.display_name;
        }

        let inode = self.visible_inode(current_inode_id).await?.ok_or_else(|| {
            VisiblePathError::PathNotFound {
                absolute_path: current_absolute_path.clone(),
            }
        })?;
        Ok(ResolvedVisiblePath {
            absolute_path: current_absolute_path,
            inode_id: current_inode_id,
            inode_kind: inode.inode_kind,
            parent_inode_id: current_parent_inode_id,
            display_name: current_display_name,
        })
    }

    pub(crate) async fn visible_children_page_by_name_key(
        &self,
        parent_inode_id: InodeId,
        start_after_name_key: Option<&str>,
        limit: usize,
    ) -> Result<Vec<DirentryBindRecord>, CoreError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let Some(parent) = self.visible_inode(parent_inode_id).await? else {
            return Ok(Vec::new());
        };
        if parent.inode_kind != InodeKind::Dir {
            return Ok(Vec::new());
        }

        let mut candidates = self.direntry_binds_for_parent(parent_inode_id).await?;
        candidates.extend(
            self.wal_tail_rows
                .direntry_binds()
                .iter()
                .filter(|direntry| direntry.parent_inode_id == parent_inode_id)
                .cloned(),
        );
        candidates.sort_by(|left, right| left.name_key.cmp(&right.name_key));

        let mut seen_name_keys = std::collections::BTreeSet::new();
        let mut children = Vec::with_capacity(limit);
        for candidate in candidates {
            if start_after_name_key
                .map(|last_name_key| candidate.name_key.as_str() <= last_name_key)
                .unwrap_or(false)
            {
                continue;
            }
            if !seen_name_keys.insert(candidate.name_key.clone()) {
                continue;
            }
            let Some(active) = self
                .visible_child(parent_inode_id, &candidate.name_key)
                .await?
            else {
                continue;
            };
            if self.visible_inode(active.child_inode_id).await?.is_none() {
                continue;
            }
            children.push(active);
            if children.len() == limit {
                break;
            }
        }
        Ok(children)
    }

    pub(crate) async fn visible_child(
        &self,
        parent_inode_id: InodeId,
        name_key: &str,
    ) -> Result<Option<DirentryBindRecord>, CoreError> {
        let Some(parent) = self.visible_inode(parent_inode_id).await? else {
            return Ok(None);
        };
        if parent.inode_kind != InodeKind::Dir {
            return Ok(None);
        }

        let Some(direntry) = self.bound_child(parent_inode_id, name_key).await? else {
            return Ok(None);
        };
        if self.is_direntry_unbound(&direntry).await? {
            return Ok(None);
        }
        let Some(latest_binding) = self
            .current_parent_binding_for_child(direntry.child_inode_id)
            .await?
        else {
            return Ok(None);
        };
        if latest_binding.parent_inode_id != direntry.parent_inode_id
            || latest_binding.name_key != direntry.name_key
            || latest_binding.bind_seq != direntry.bind_seq
            || latest_binding.bind_delta_index != direntry.bind_delta_index
            || self.is_direntry_unbound(&latest_binding).await?
        {
            return Ok(None);
        }

        Ok(Some(direntry))
    }

    pub(crate) async fn visible_inode(
        &self,
        inode_id: InodeId,
    ) -> Result<Option<InodeRecord>, CoreError> {
        let Some(inode) = self.inode_at_seq(inode_id).await? else {
            return Ok(None);
        };
        if self.covering_subtree_tombstone(inode_id).await?.is_some() {
            return Ok(None);
        }
        Ok(Some(inode))
    }

    pub(crate) async fn inode_at_seq(
        &self,
        inode_id: InodeId,
    ) -> Result<Option<InodeRecord>, CoreError> {
        if let Some(inode) = self
            .wal_tail_rows
            .inodes()
            .iter()
            .find(|inode| inode.inode_id == inode_id && inode.created_seq <= self.head.seq)
            .cloned()
        {
            return Ok(Some(inode));
        }
        manifest_index::inode_at_seq(self.tables, inode_id).await
    }

    pub(crate) async fn latest_revision_head(
        &self,
        inode_id: InodeId,
    ) -> Result<Option<RevisionRecord>, CoreError> {
        if self.visible_inode(inode_id).await?.is_none() {
            return Ok(None);
        }
        Ok(self
            .revisions_for_inode_at_head(inode_id)
            .await?
            .into_iter()
            .max_by_key(|revision| (revision.committed_seq, revision.revision_delta_index)))
    }

    pub(crate) async fn revisions_for_inode_at_head(
        &self,
        inode_id: InodeId,
    ) -> Result<Vec<RevisionRecord>, CoreError> {
        let mut revisions = self.revisions_for_inode(inode_id).await?;
        revisions.extend(
            self.wal_tail_rows
                .revisions()
                .iter()
                .filter(|revision| revision.inode_id == inode_id)
                .cloned(),
        );
        Ok(revisions
            .into_iter()
            .filter(|revision| revision.committed_seq <= self.head.seq)
            .collect())
    }

    pub(crate) async fn revision_for_inode(
        &self,
        inode_id: InodeId,
        revision_no: RevisionNo,
    ) -> Result<RevisionRecord, CoreError> {
        let inode = self
            .inode_at_seq(inode_id)
            .await?
            .ok_or_else(|| CoreError::MissingPath(inode_id.to_string()))?;
        if inode.inode_kind != InodeKind::File {
            return Err(CoreError::ExpectedFile {
                path: inode_id.to_string(),
                kind: inode.inode_kind,
            });
        }
        self.revisions_for_inode_at_head(inode_id)
            .await?
            .into_iter()
            .filter(|revision| revision.revision_no == revision_no)
            .max_by_key(|revision| (revision.committed_seq, revision.revision_delta_index))
            .ok_or(CoreError::MissingRevision {
                inode_id,
                revision_no,
            })
    }

    pub(crate) async fn find_commit_receipt(
        &self,
        commit_id: &CommitId,
    ) -> Result<Option<CommitReceiptRecord>, CoreError> {
        let tail_receipt = self
            .wal_tail_rows
            .commit_receipts()
            .iter()
            .filter(|receipt| receipt.commit_id == *commit_id)
            .max_by_key(|receipt| receipt.committed_seq)
            .cloned();
        let manifest_receipt = manifest_index::commit_receipt(self.tables, commit_id).await?;
        Ok(tail_receipt
            .into_iter()
            .chain(manifest_receipt)
            .max_by_key(|receipt| receipt.committed_seq))
    }

    pub(crate) async fn current_parent_binding_for_child(
        &self,
        child_inode_id: InodeId,
    ) -> Result<Option<DirentryBindRecord>, CoreError> {
        let mut bindings = self.direntry_binds_for_child(child_inode_id).await?;
        bindings.extend(
            self.wal_tail_rows
                .direntry_binds()
                .iter()
                .filter(|direntry| direntry.child_inode_id == child_inode_id)
                .cloned(),
        );
        let Some(binding) = bindings
            .into_iter()
            .filter(|direntry| direntry.bind_seq <= self.head.seq)
            .max_by_key(|direntry| (direntry.bind_seq, direntry.bind_delta_index))
        else {
            return Ok(None);
        };
        if self.is_direntry_unbound(&binding).await? {
            return Ok(None);
        }
        Ok(Some(binding))
    }

    pub(crate) async fn covering_subtree_tombstone(
        &self,
        inode_id: InodeId,
    ) -> Result<Option<SubtreeTombstoneRecord>, CoreError> {
        let mut current = Some(inode_id);
        let mut visited = std::collections::BTreeSet::new();
        while let Some(candidate_inode_id) = current {
            if !visited.insert(candidate_inode_id.0) {
                break;
            }
            if let Some(tombstone) = self.active_subtree_tombstone(candidate_inode_id).await? {
                return Ok(Some(tombstone));
            }
            current = self
                .current_parent_binding_for_child(candidate_inode_id)
                .await?
                .map(|direntry| direntry.parent_inode_id);
        }
        Ok(None)
    }

    async fn bound_child(
        &self,
        parent_inode_id: InodeId,
        name_key: &str,
    ) -> Result<Option<DirentryBindRecord>, CoreError> {
        let mut bindings = self
            .direntry_binds_for_parent_name(parent_inode_id, name_key)
            .await?;
        bindings.extend(
            self.wal_tail_rows
                .direntry_binds()
                .iter()
                .filter(|direntry| {
                    direntry.parent_inode_id == parent_inode_id && direntry.name_key == name_key
                })
                .cloned(),
        );
        Ok(bindings
            .into_iter()
            .filter(|direntry| direntry.bind_seq <= self.head.seq)
            .max_by_key(|direntry| (direntry.bind_seq, direntry.bind_delta_index)))
    }

    async fn is_direntry_unbound(&self, direntry: &DirentryBindRecord) -> Result<bool, CoreError> {
        let mut unbinds = self.direntry_unbinds_for_binding(direntry).await?;
        unbinds.extend(
            self.wal_tail_rows
                .direntry_unbinds()
                .iter()
                .filter(|unbind| unbind_matches_binding(unbind, direntry))
                .cloned(),
        );
        Ok(unbinds
            .into_iter()
            .any(|unbind| unbind.unbind_seq <= self.head.seq))
    }

    async fn active_subtree_tombstone(
        &self,
        root_inode_id: InodeId,
    ) -> Result<Option<SubtreeTombstoneRecord>, CoreError> {
        let mut tombstones = self.tombstones_for_root(root_inode_id).await?;
        tombstones.extend(
            self.wal_tail_rows
                .subtree_tombstones()
                .iter()
                .filter(|tombstone| tombstone.root_inode_id == root_inode_id)
                .cloned(),
        );
        Ok(tombstones
            .into_iter()
            .filter(|tombstone| tombstone.tombstone_seq <= self.head.seq)
            .max_by_key(|tombstone| (tombstone.tombstone_seq, tombstone.tombstone_delta_index)))
    }

    async fn direntry_binds_for_parent(
        &self,
        parent_inode_id: InodeId,
    ) -> Result<Vec<DirentryBindRecord>, CoreError> {
        manifest_index::direntry_binds_for_parent(self.tables, parent_inode_id).await
    }

    async fn direntry_binds_for_parent_name(
        &self,
        parent_inode_id: InodeId,
        name_key: &str,
    ) -> Result<Vec<DirentryBindRecord>, CoreError> {
        manifest_index::direntry_binds_for_parent_name(self.tables, parent_inode_id, name_key).await
    }

    async fn direntry_binds_for_child(
        &self,
        child_inode_id: InodeId,
    ) -> Result<Vec<DirentryBindRecord>, CoreError> {
        manifest_index::direntry_binds_for_child(self.tables, child_inode_id).await
    }

    async fn direntry_unbinds_for_binding(
        &self,
        direntry: &DirentryBindRecord,
    ) -> Result<Vec<DirentryUnbindRecord>, CoreError> {
        manifest_index::direntry_unbinds_for_binding(self.tables, direntry).await
    }

    async fn revisions_for_inode(
        &self,
        inode_id: InodeId,
    ) -> Result<Vec<RevisionRecord>, CoreError> {
        manifest_index::revisions_for_inode(self.tables, inode_id).await
    }

    async fn tombstones_for_root(
        &self,
        root_inode_id: InodeId,
    ) -> Result<Vec<SubtreeTombstoneRecord>, CoreError> {
        manifest_index::tombstones_for_root(self.tables, root_inode_id).await
    }
}

fn absolute_path_prefix(current: &str, component: &str) -> String {
    if current == "/" {
        format!("/{component}")
    } else {
        format!("{current}/{component}")
    }
}
