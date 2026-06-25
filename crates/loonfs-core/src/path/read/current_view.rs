use super::{manifest_index, row_decode::unbind_matches_binding};
use crate::checkpoint::VerifiedMetadataTables;
use crate::error::CoreError;
use crate::metadata::{
    CommitReceiptRecord, DirentryBindRecord, DirentryUnbindRecord, InodeRecord, MetadataState,
    ResolvedVisiblePath, RevisionRecord, SubtreeTombstoneRecord, VisiblePathError,
};
use loonfs_api::wire::control::HeadState;
use loonfs_api::wire::manifest::{MetadataRow, MetadataTableFamily};
use loonfs_api::{AbsolutePath, CommitId, InodeId, InodeKind, NameKey, RevisionNo};
use loonfs_objectstore::ObjectStore;
use std::collections::{BTreeSet, HashMap, VecDeque};

const DIRECTORY_PAGE_RAW_SCAN_LIMIT: usize = 64;

pub(crate) struct CurrentManifestTailView<'a, S: ObjectStore + ?Sized> {
    head: &'a HeadState,
    tables: &'a VerifiedMetadataTables<'a, S>,
    wal_tail_rows: &'a MetadataState,
}

impl<S: ObjectStore + ?Sized> Copy for CurrentManifestTailView<'_, S> {}

impl<S: ObjectStore + ?Sized> Clone for CurrentManifestTailView<'_, S> {
    fn clone(&self) -> Self {
        *self
    }
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

    pub(crate) fn session(self) -> MetadataReadSession<'a, S> {
        MetadataReadSession::new(self)
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
        let tail_revision = self.tail_latest_revision_for_inode(inode_id);
        let manifest_revision =
            manifest_index::latest_revision_for_inode(self.tables, inode_id).await?;
        Ok(tail_revision
            .into_iter()
            .chain(manifest_revision)
            .max_by_key(revision_order_key))
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
        self.revision_at_head(inode_id, revision_no)
            .await?
            .ok_or(CoreError::MissingRevision {
                inode_id,
                revision_no,
            })
    }

    pub(crate) async fn revision_at_head(
        &self,
        inode_id: InodeId,
        revision_no: RevisionNo,
    ) -> Result<Option<RevisionRecord>, CoreError> {
        let tail_revision = self.tail_revision_for_inode_no(inode_id, revision_no);
        let manifest_revision =
            manifest_index::revision_for_inode_no(self.tables, inode_id, revision_no).await?;
        Ok(tail_revision
            .into_iter()
            .chain(manifest_revision)
            .max_by_key(revision_order_key))
    }

    pub(crate) async fn revisions_for_inode_page_desc(
        &self,
        inode_id: InodeId,
        start_after: Option<manifest_index::RevisionPagePosition>,
        limit: usize,
    ) -> Result<Vec<RevisionRecord>, CoreError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut revisions = manifest_index::revisions_for_inode_page_desc(
            self.tables,
            inode_id,
            start_after,
            limit,
        )
        .await?;
        revisions.extend(self.tail_revisions_for_inode_page_desc(inode_id, start_after));
        revisions.retain(|revision| revision.committed_seq <= self.head.seq);
        revisions.sort_by_key(|revision| std::cmp::Reverse(revision_order_key(revision)));
        revisions.truncate(limit);
        Ok(revisions)
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

    pub(crate) async fn bound_child(
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

    pub(crate) async fn is_direntry_unbound(
        &self,
        direntry: &DirentryBindRecord,
    ) -> Result<bool, CoreError> {
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

    pub(crate) async fn active_subtree_tombstone(
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

    fn tail_latest_revision_for_inode(&self, inode_id: InodeId) -> Option<RevisionRecord> {
        self.wal_tail_rows
            .revisions()
            .iter()
            .filter(|revision| {
                revision.inode_id == inode_id && revision.committed_seq <= self.head.seq
            })
            .max_by_key(|revision| revision_order_key(revision))
            .cloned()
    }

    fn tail_revision_for_inode_no(
        &self,
        inode_id: InodeId,
        revision_no: RevisionNo,
    ) -> Option<RevisionRecord> {
        self.wal_tail_rows
            .revisions()
            .iter()
            .filter(|revision| {
                revision.inode_id == inode_id
                    && revision.revision_no == revision_no
                    && revision.committed_seq <= self.head.seq
            })
            .max_by_key(|revision| revision_order_key(revision))
            .cloned()
    }

    fn tail_revisions_for_inode_page_desc(
        &self,
        inode_id: InodeId,
        start_after: Option<manifest_index::RevisionPagePosition>,
    ) -> Vec<RevisionRecord> {
        let mut revisions = self
            .wal_tail_rows
            .revisions()
            .iter()
            .filter(|revision| {
                revision.inode_id == inode_id
                    && revision.committed_seq <= self.head.seq
                    && start_after
                        .map(|position| revision_is_after_position_desc(revision, position))
                        .unwrap_or(true)
            })
            .cloned()
            .collect::<Vec<_>>();
        revisions.sort_by_key(|revision| std::cmp::Reverse(revision_order_key(revision)));
        revisions
    }

    async fn tombstones_for_root(
        &self,
        root_inode_id: InodeId,
    ) -> Result<Vec<SubtreeTombstoneRecord>, CoreError> {
        manifest_index::tombstones_for_root(self.tables, root_inode_id).await
    }

    fn tail_direntry_bind_page_candidates(
        &self,
        parent_inode_id: InodeId,
        start_after_name_key: Option<&str>,
    ) -> Vec<DirentryBindPageCandidate> {
        let mut candidates = self
            .wal_tail_rows
            .direntry_binds()
            .iter()
            .filter(|direntry| {
                direntry.parent_inode_id == parent_inode_id
                    && start_after_name_key
                        .map(|last_name_key| direntry.name_key.as_str() > last_name_key)
                        .unwrap_or(true)
            })
            .map(|record| DirentryBindPageCandidate {
                row_key: direntry_bind_row_key(record),
                record: record.clone(),
            })
            .collect::<Vec<_>>();
        candidates.sort_by(|left, right| left.row_key.cmp(&right.row_key));
        candidates
    }
}

#[derive(Debug, Clone)]
pub(crate) struct VisibleChildEntry {
    pub(crate) binding: DirentryBindRecord,
    pub(crate) inode: InodeRecord,
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct MetadataReadSessionCounters {
    pub(crate) visible_child_calls: u64,
    pub(crate) visible_inode_calls: u64,
    pub(crate) current_parent_binding_calls: u64,
    pub(crate) covering_tombstone_calls: u64,
    pub(crate) latest_revision_calls: u64,
    pub(crate) direntry_child_scan_calls: u64,
    pub(crate) scan_prefix_calls: u64,
    pub(crate) scan_range_page_calls: u64,
}

pub(crate) struct MetadataReadSession<'a, S: ObjectStore + ?Sized> {
    base: CurrentManifestTailView<'a, S>,
    inode_at_seq_cache: HashMap<InodeId, Option<InodeRecord>>,
    visible_inode_cache: HashMap<InodeId, Option<InodeRecord>>,
    bound_child_cache: HashMap<ParentNameCacheKey, Option<DirentryBindRecord>>,
    current_parent_binding_cache: HashMap<InodeId, Option<DirentryBindRecord>>,
    latest_revision_head_cache: HashMap<InodeId, Option<RevisionRecord>>,
    active_tombstone_cache: HashMap<InodeId, Option<SubtreeTombstoneRecord>>,
    covering_tombstone_cache: HashMap<InodeId, Option<SubtreeTombstoneRecord>>,
    unbind_cache: HashMap<BindingCacheKey, bool>,
    counters: MetadataReadSessionCounters,
}

impl<'a, S: ObjectStore + ?Sized> MetadataReadSession<'a, S> {
    fn new(base: CurrentManifestTailView<'a, S>) -> Self {
        Self {
            base,
            inode_at_seq_cache: HashMap::new(),
            visible_inode_cache: HashMap::new(),
            bound_child_cache: HashMap::new(),
            current_parent_binding_cache: HashMap::new(),
            latest_revision_head_cache: HashMap::new(),
            active_tombstone_cache: HashMap::new(),
            covering_tombstone_cache: HashMap::new(),
            unbind_cache: HashMap::new(),
            counters: MetadataReadSessionCounters::default(),
        }
    }

    pub(crate) fn counters(&self) -> MetadataReadSessionCounters {
        self.counters
    }

    pub(crate) async fn visible_children_page_by_name_key(
        &mut self,
        parent_inode_id: InodeId,
        start_after_name_key: Option<&str>,
        limit: usize,
    ) -> Result<Vec<VisibleChildEntry>, CoreError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let Some(parent) = self.visible_inode(parent_inode_id).await? else {
            return Ok(Vec::new());
        };
        if parent.inode_kind != InodeKind::Dir {
            return Ok(Vec::new());
        }

        let raw_scan_limit = limit.max(DIRECTORY_PAGE_RAW_SCAN_LIMIT);
        let mut manifest_after_row_key = None;
        let mut manifest_exhausted = false;
        let mut manifest_candidates = VecDeque::<DirentryBindPageCandidate>::new();
        let tail_candidates = self
            .base
            .tail_direntry_bind_page_candidates(parent_inode_id, start_after_name_key);
        let mut tail_index = 0;
        let mut seen_name_keys = BTreeSet::new();
        let mut children = Vec::with_capacity(limit);
        while children.len() < limit {
            if manifest_candidates.is_empty() && !manifest_exhausted {
                self.counters.scan_range_page_calls =
                    self.counters.scan_range_page_calls.saturating_add(1);
                let page = manifest_index::direntry_binds_for_parent_name_key_page(
                    self.base.tables,
                    parent_inode_id,
                    start_after_name_key,
                    manifest_after_row_key.as_deref(),
                    raw_scan_limit,
                )
                .await?;
                if page.is_empty() {
                    manifest_exhausted = true;
                } else {
                    manifest_after_row_key = page.last().map(|candidate| candidate.row_key.clone());
                    manifest_candidates.extend(page.into_iter().map(|candidate| {
                        DirentryBindPageCandidate {
                            row_key: candidate.row_key,
                            record: candidate.record,
                        }
                    }));
                }
            }

            let next_manifest = manifest_candidates.front();
            let next_tail = tail_candidates.get(tail_index);
            let take_tail = match (next_manifest, next_tail) {
                (Some(manifest), Some(tail)) => tail.row_key < manifest.row_key,
                (None, Some(_)) => true,
                (Some(_), None) => false,
                (None, None) => break,
            };
            let candidate = if take_tail {
                let candidate = tail_candidates[tail_index].clone();
                tail_index += 1;
                candidate
            } else {
                manifest_candidates
                    .pop_front()
                    .expect("manifest candidate should exist")
            };

            if !seen_name_keys.insert(candidate.record.name_key.clone()) {
                continue;
            }
            let Some(active) = self
                .visible_child(parent_inode_id, &candidate.record.name_key)
                .await?
            else {
                continue;
            };
            let Some(inode) = self.visible_inode(active.child_inode_id).await? else {
                continue;
            };
            children.push(VisibleChildEntry {
                binding: active,
                inode,
            });
        }
        Ok(children)
    }

    pub(crate) async fn visible_child(
        &mut self,
        parent_inode_id: InodeId,
        name_key: &str,
    ) -> Result<Option<DirentryBindRecord>, CoreError> {
        self.counters.visible_child_calls = self.counters.visible_child_calls.saturating_add(1);

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
        &mut self,
        inode_id: InodeId,
    ) -> Result<Option<InodeRecord>, CoreError> {
        self.counters.visible_inode_calls = self.counters.visible_inode_calls.saturating_add(1);
        if let Some(cached) = self.visible_inode_cache.get(&inode_id).cloned() {
            return Ok(cached);
        }

        let visible = if let Some(inode) = self.inode_at_seq(inode_id).await? {
            if self.covering_subtree_tombstone(inode_id).await?.is_some() {
                None
            } else {
                Some(inode)
            }
        } else {
            None
        };
        self.visible_inode_cache.insert(inode_id, visible.clone());
        Ok(visible)
    }

    pub(crate) async fn inode_at_seq(
        &mut self,
        inode_id: InodeId,
    ) -> Result<Option<InodeRecord>, CoreError> {
        if let Some(cached) = self.inode_at_seq_cache.get(&inode_id).cloned() {
            return Ok(cached);
        }
        let inode = self.base.inode_at_seq(inode_id).await?;
        self.inode_at_seq_cache.insert(inode_id, inode.clone());
        Ok(inode)
    }

    pub(crate) async fn latest_revision_head(
        &mut self,
        inode_id: InodeId,
    ) -> Result<Option<RevisionRecord>, CoreError> {
        self.counters.latest_revision_calls = self.counters.latest_revision_calls.saturating_add(1);
        if let Some(cached) = self.latest_revision_head_cache.get(&inode_id).cloned() {
            return Ok(cached);
        }
        let revision = self.base.latest_revision_head(inode_id).await?;
        self.latest_revision_head_cache
            .insert(inode_id, revision.clone());
        Ok(revision)
    }

    pub(crate) async fn revisions_for_inode_page_desc(
        &mut self,
        inode_id: InodeId,
        start_after: Option<manifest_index::RevisionPagePosition>,
        limit: usize,
    ) -> Result<Vec<RevisionRecord>, CoreError> {
        self.counters.scan_range_page_calls = self.counters.scan_range_page_calls.saturating_add(1);
        self.base
            .revisions_for_inode_page_desc(inode_id, start_after, limit)
            .await
    }

    pub(crate) async fn current_parent_binding_for_child(
        &mut self,
        child_inode_id: InodeId,
    ) -> Result<Option<DirentryBindRecord>, CoreError> {
        self.counters.current_parent_binding_calls =
            self.counters.current_parent_binding_calls.saturating_add(1);
        if let Some(cached) = self
            .current_parent_binding_cache
            .get(&child_inode_id)
            .cloned()
        {
            return Ok(cached);
        }

        self.counters.direntry_child_scan_calls =
            self.counters.direntry_child_scan_calls.saturating_add(1);
        self.counters.scan_prefix_calls = self.counters.scan_prefix_calls.saturating_add(1);
        let mut bindings = self.base.direntry_binds_for_child(child_inode_id).await?;
        bindings.extend(
            self.base
                .wal_tail_rows
                .direntry_binds()
                .iter()
                .filter(|direntry| direntry.child_inode_id == child_inode_id)
                .cloned(),
        );
        let binding = bindings
            .into_iter()
            .filter(|direntry| direntry.bind_seq <= self.base.head.seq)
            .max_by_key(|direntry| (direntry.bind_seq, direntry.bind_delta_index));
        let binding = if let Some(binding) = binding {
            if self.is_direntry_unbound(&binding).await? {
                None
            } else {
                Some(binding)
            }
        } else {
            None
        };
        self.current_parent_binding_cache
            .insert(child_inode_id, binding.clone());
        Ok(binding)
    }

    pub(crate) async fn covering_subtree_tombstone(
        &mut self,
        inode_id: InodeId,
    ) -> Result<Option<SubtreeTombstoneRecord>, CoreError> {
        self.counters.covering_tombstone_calls =
            self.counters.covering_tombstone_calls.saturating_add(1);
        if let Some(cached) = self.covering_tombstone_cache.get(&inode_id).cloned() {
            return Ok(cached);
        }

        let mut current = Some(inode_id);
        let mut visited = BTreeSet::new();
        let mut tombstone = None;
        while let Some(candidate_inode_id) = current {
            if !visited.insert(candidate_inode_id.0) {
                break;
            }
            if let Some(active) = self.active_subtree_tombstone(candidate_inode_id).await? {
                tombstone = Some(active);
                break;
            }
            current = self
                .current_parent_binding_for_child(candidate_inode_id)
                .await?
                .map(|direntry| direntry.parent_inode_id);
        }
        self.covering_tombstone_cache
            .insert(inode_id, tombstone.clone());
        Ok(tombstone)
    }

    pub(crate) async fn active_subtree_tombstone(
        &mut self,
        root_inode_id: InodeId,
    ) -> Result<Option<SubtreeTombstoneRecord>, CoreError> {
        if let Some(cached) = self.active_tombstone_cache.get(&root_inode_id).cloned() {
            return Ok(cached);
        }
        self.counters.scan_prefix_calls = self.counters.scan_prefix_calls.saturating_add(1);
        let mut tombstones = self.base.tombstones_for_root(root_inode_id).await?;
        tombstones.extend(
            self.base
                .wal_tail_rows
                .subtree_tombstones()
                .iter()
                .filter(|tombstone| tombstone.root_inode_id == root_inode_id)
                .cloned(),
        );
        let tombstone = tombstones
            .into_iter()
            .filter(|tombstone| tombstone.tombstone_seq <= self.base.head.seq)
            .max_by_key(|tombstone| (tombstone.tombstone_seq, tombstone.tombstone_delta_index));
        self.active_tombstone_cache
            .insert(root_inode_id, tombstone.clone());
        Ok(tombstone)
    }

    async fn bound_child(
        &mut self,
        parent_inode_id: InodeId,
        name_key: &str,
    ) -> Result<Option<DirentryBindRecord>, CoreError> {
        let cache_key = ParentNameCacheKey {
            parent_inode_id,
            name_key: name_key.to_owned(),
        };
        if let Some(cached) = self.bound_child_cache.get(&cache_key).cloned() {
            return Ok(cached);
        }
        self.counters.scan_prefix_calls = self.counters.scan_prefix_calls.saturating_add(1);
        let mut bindings = self
            .base
            .direntry_binds_for_parent_name(parent_inode_id, name_key)
            .await?;
        bindings.extend(
            self.base
                .wal_tail_rows
                .direntry_binds()
                .iter()
                .filter(|direntry| {
                    direntry.parent_inode_id == parent_inode_id && direntry.name_key == name_key
                })
                .cloned(),
        );
        let binding = bindings
            .into_iter()
            .filter(|direntry| direntry.bind_seq <= self.base.head.seq)
            .max_by_key(|direntry| (direntry.bind_seq, direntry.bind_delta_index));
        self.bound_child_cache.insert(cache_key, binding.clone());
        Ok(binding)
    }

    async fn is_direntry_unbound(
        &mut self,
        direntry: &DirentryBindRecord,
    ) -> Result<bool, CoreError> {
        let cache_key = BindingCacheKey::from(direntry);
        if let Some(cached) = self.unbind_cache.get(&cache_key).copied() {
            return Ok(cached);
        }
        self.counters.scan_prefix_calls = self.counters.scan_prefix_calls.saturating_add(1);
        let mut unbinds = self.base.direntry_unbinds_for_binding(direntry).await?;
        unbinds.extend(
            self.base
                .wal_tail_rows
                .direntry_unbinds()
                .iter()
                .filter(|unbind| unbind_matches_binding(unbind, direntry))
                .cloned(),
        );
        let unbound = unbinds
            .into_iter()
            .any(|unbind| unbind.unbind_seq <= self.base.head.seq);
        self.unbind_cache.insert(cache_key, unbound);
        Ok(unbound)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ParentNameCacheKey {
    parent_inode_id: InodeId,
    name_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct BindingCacheKey {
    parent_inode_id: InodeId,
    name_key: String,
    child_inode_id: InodeId,
    bind_seq: loonfs_api::ChangeSeq,
    bind_delta_index: u32,
}

impl From<&DirentryBindRecord> for BindingCacheKey {
    fn from(record: &DirentryBindRecord) -> Self {
        Self {
            parent_inode_id: record.parent_inode_id,
            name_key: record.name_key.clone(),
            child_inode_id: record.child_inode_id,
            bind_seq: record.bind_seq,
            bind_delta_index: record.bind_delta_index,
        }
    }
}

#[derive(Clone)]
struct DirentryBindPageCandidate {
    row_key: String,
    record: DirentryBindRecord,
}

fn direntry_bind_row_key(record: &DirentryBindRecord) -> String {
    MetadataRow::DirentryBind {
        parent_inode_id: record.parent_inode_id,
        name_key: record.name_key.clone(),
        display_name: record.display_name.clone(),
        child_inode_id: record.child_inode_id,
        bind_seq: record.bind_seq,
        bind_delta_index: record.bind_delta_index,
    }
    .row_key_for_family(MetadataTableFamily::DirentryBinds)
}

fn revision_order_key(record: &RevisionRecord) -> (RevisionNo, loonfs_api::ChangeSeq, u32) {
    (
        record.revision_no,
        record.committed_seq,
        record.revision_delta_index,
    )
}

fn revision_is_after_position_desc(
    record: &RevisionRecord,
    position: manifest_index::RevisionPagePosition,
) -> bool {
    revision_order_key(record)
        < (
            position.revision_no,
            position.committed_seq,
            position.revision_delta_index,
        )
}

fn absolute_path_prefix(current: &str, component: &str) -> String {
    if current == "/" {
        format!("/{component}")
    } else {
        format!("{current}/{component}")
    }
}
