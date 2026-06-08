use super::helpers::{map_path_error_to_core, parse_absolute_path_for_core};
use crate::basis::{load_verified_namespace_basis, BasisLoadError, VerifiedNamespaceBasis};
use crate::checkpoint::{
    checkpoint_basis_head, load_verified_checkpoint_tables_with_cache, CheckpointLoadError,
    MetadataTableCache, VerifiedCheckpointTables,
};
use crate::content::read_durable_content_bytes;
use crate::error::CoreError;
use crate::loading::read_head_object;
use crate::metadata::{
    DirentryBindRecord, DirentryUnbindRecord, InodeRecord, MetadataState, ResolvedVisiblePath,
    RevisionRecord, SubtreeTombstoneRecord,
};
use crate::namespace::catalog::load_namespace_catalog_entry;
use crate::wal::{
    load_validated_wal_chain, replay_validated_wal_tail_with_metadata, WalChainLoadRequest,
};
use loon_api::wire::checkpoint::{
    hex_encode_row_key_component, CheckpointRow, CheckpointTableFamily,
};
use loon_api::wire::control::HeadState;
use loon_api::{
    AbsolutePath, AuthoritativeFileBytes, AuthoritativePathEntry, ChangeSeq, DisplayName,
    FileRevision, InodeId, InodeKind, ListFileRevisionsResponse, NameKey, NamespaceId, RevisionNo,
};
use loon_objectstore::ObjectStore;

pub(crate) fn resolve_path<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
) -> Result<AuthoritativePathEntry, CoreError> {
    if let Some(entry) = resolve_path_from_materialized_tables(store, namespace_id, absolute_path)?
    {
        return Ok(entry);
    }
    let basis = load_verified_namespace_basis(store, namespace_id)?;
    resolve_path_from_basis(&basis, absolute_path)
}

#[tracing::instrument(
    level = "info",
    name = "loon.phase",
    err,
    skip_all,
    fields(phase = "walk_path")
)]
pub(crate) fn resolve_path_from_basis(
    basis: &VerifiedNamespaceBasis,
    absolute_path: &str,
) -> Result<AuthoritativePathEntry, CoreError> {
    let absolute_path = parse_absolute_path_for_core(absolute_path)?;
    let resolved = basis.metadata_state.resolve_visible_path(
        &absolute_path,
        basis.head.name_policy,
        basis.head.seq,
    )?;
    build_authoritative_path_entry(
        &basis.head.namespace_id,
        basis.head.seq,
        &basis.metadata_state,
        &resolved,
    )
}

pub(crate) fn list_path<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
) -> Result<Vec<AuthoritativePathEntry>, CoreError> {
    if let Some(entries) = list_path_from_materialized_tables(store, namespace_id, absolute_path)? {
        return Ok(entries);
    }
    let basis = load_verified_namespace_basis(store, namespace_id)?;
    list_path_from_basis(&basis, absolute_path)
}

#[tracing::instrument(
    level = "info",
    name = "loon.phase",
    err,
    skip_all,
    fields(phase = "walk_path")
)]
pub(crate) fn list_path_from_basis(
    basis: &VerifiedNamespaceBasis,
    absolute_path: &str,
) -> Result<Vec<AuthoritativePathEntry>, CoreError> {
    let absolute_path = parse_absolute_path_for_core(absolute_path)?;
    let resolved = basis.metadata_state.resolve_visible_path(
        &absolute_path,
        basis.head.name_policy,
        basis.head.seq,
    )?;
    if resolved.inode_kind == InodeKind::File {
        return Ok(vec![build_authoritative_path_entry(
            &basis.head.namespace_id,
            basis.head.seq,
            &basis.metadata_state,
            &resolved,
        )?]);
    }
    if resolved.inode_kind != InodeKind::Dir {
        return Err(CoreError::ExpectedDirectory {
            path: resolved.absolute_path,
            kind: resolved.inode_kind,
        });
    }

    basis
        .metadata_state
        .visible_children(resolved.inode_id, basis.head.seq)
        .into_iter()
        .map(|direntry| {
            let child = basis
                .metadata_state
                .visible_inode(direntry.child_inode_id, basis.head.seq)
                .expect("visible child listing should resolve inode");
            let child_path = AbsolutePath::parse(&resolved.absolute_path)
                .map_err(map_path_error_to_core)?
                .join(&DisplayName::parse(&direntry.display_name).map_err(map_path_error_to_core)?);
            build_authoritative_path_entry(
                &basis.head.namespace_id,
                basis.head.seq,
                &basis.metadata_state,
                &ResolvedVisiblePath {
                    absolute_path: child_path.as_str().to_owned(),
                    inode_id: direntry.child_inode_id,
                    inode_kind: child.inode_kind,
                    parent_inode_id: Some(direntry.parent_inode_id),
                    display_name: direntry.display_name,
                },
            )
        })
        .collect()
}

pub(crate) fn resolve_path_from_materialized_tables<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
) -> Result<Option<AuthoritativePathEntry>, CoreError> {
    let Some(view) = MaterializedLatestView::load(store, namespace_id)? else {
        return Ok(None);
    };
    Ok(Some(view.resolve_path(absolute_path)?))
}

pub(crate) fn resolve_path_from_materialized_tables_at_head_with_cache<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    head: &HeadState,
    absolute_path: &str,
    table_cache: Option<&MetadataTableCache>,
) -> Result<Option<AuthoritativePathEntry>, CoreError> {
    let Some(view) = MaterializedLatestView::load_at_head_with_cache(
        store,
        namespace_id,
        head.clone(),
        table_cache,
    )?
    else {
        return Ok(None);
    };
    Ok(Some(view.resolve_path(absolute_path)?))
}

pub(crate) fn list_path_from_materialized_tables<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
) -> Result<Option<Vec<AuthoritativePathEntry>>, CoreError> {
    let Some(view) = MaterializedLatestView::load(store, namespace_id)? else {
        return Ok(None);
    };
    Ok(Some(view.list_path(absolute_path)?))
}

pub(crate) fn list_path_from_materialized_tables_at_head_with_cache<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    head: &HeadState,
    absolute_path: &str,
    table_cache: Option<&MetadataTableCache>,
) -> Result<Option<Vec<AuthoritativePathEntry>>, CoreError> {
    let Some(view) = MaterializedLatestView::load_at_head_with_cache(
        store,
        namespace_id,
        head.clone(),
        table_cache,
    )?
    else {
        return Ok(None);
    };
    Ok(Some(view.list_path(absolute_path)?))
}

struct MaterializedLatestView<'a, S: ObjectStore + ?Sized> {
    namespace_id: NamespaceId,
    head: HeadState,
    tables: VerifiedCheckpointTables<'a, S>,
    wal_tail_rows: MetadataState,
}

impl<'a, S: ObjectStore + ?Sized> MaterializedLatestView<'a, S> {
    fn load(store: &'a S, namespace_id: &NamespaceId) -> Result<Option<Self>, CoreError> {
        let loaded_head =
            read_head_object(store, namespace_id).map_err(BasisLoadError::LoadHead)?;
        Self::load_at_head(store, namespace_id, loaded_head.envelope.state)
    }

    fn load_at_head(
        store: &'a S,
        namespace_id: &NamespaceId,
        head: HeadState,
    ) -> Result<Option<Self>, CoreError> {
        Self::load_at_head_with_cache(store, namespace_id, head, None)
    }

    fn load_at_head_with_cache(
        store: &'a S,
        namespace_id: &NamespaceId,
        head: HeadState,
        table_cache: Option<&'a MetadataTableCache>,
    ) -> Result<Option<Self>, CoreError> {
        if &head.namespace_id != namespace_id {
            return Err(CoreError::NamespaceCorrupt(format!(
                "head namespace `{}` does not match requested namespace `{}`",
                head.namespace_id, namespace_id
            )));
        }
        let Some(checkpoint_seq) = head.checkpoint_hint_seq else {
            return Ok(None);
        };
        let _catalog_entry =
            load_namespace_catalog_entry(store, namespace_id).map_err(BasisLoadError::from)?;
        let tables = load_verified_checkpoint_tables_with_cache(
            store,
            table_cache,
            namespace_id,
            checkpoint_seq,
        )
        .map_err(|error| CoreError::Basis(BasisLoadError::CheckpointLoad(error)))?;
        let checkpoint_head = checkpoint_basis_head(&head, tables.manifest());
        let wal_chain = load_validated_wal_chain(
            store,
            WalChainLoadRequest {
                namespace_id,
                chain_base_seq: checkpoint_head.seq,
                head_seq: head.seq,
                visible_tip: head.visible_wal_tip.clone(),
                stop_after_seq: None,
            },
        )
        .map_err(|error| CoreError::Basis(BasisLoadError::WalChainLoad(error)))?;
        let replayed = {
            let _span =
                tracing::info_span!("loon.phase", phase = "project_metadata_state").entered();
            replay_validated_wal_tail_with_metadata(
                &checkpoint_head,
                &MetadataState::default(),
                wal_chain.segments(),
            )
            .map_err(|error| CoreError::Basis(BasisLoadError::WalReplay(error)))
        }?;
        Ok(Some(Self {
            namespace_id: namespace_id.clone(),
            head,
            tables,
            wal_tail_rows: replayed.resulting_metadata_state,
        }))
    }

    #[tracing::instrument(
        level = "info",
        name = "loon.phase",
        err,
        skip_all,
        fields(phase = "walk_path")
    )]
    fn resolve_path(&self, absolute_path: &str) -> Result<AuthoritativePathEntry, CoreError> {
        let absolute_path = parse_absolute_path_for_core(absolute_path)?;
        let resolved = self.resolve_visible_path(&absolute_path)?;
        self.build_authoritative_path_entry(&resolved)
    }

    #[tracing::instrument(
        level = "info",
        name = "loon.phase",
        err,
        skip_all,
        fields(phase = "walk_path")
    )]
    fn list_path(&self, absolute_path: &str) -> Result<Vec<AuthoritativePathEntry>, CoreError> {
        let absolute_path = parse_absolute_path_for_core(absolute_path)?;
        let resolved = self.resolve_visible_path(&absolute_path)?;
        if resolved.inode_kind == InodeKind::File {
            return Ok(vec![self.build_authoritative_path_entry(&resolved)?]);
        }
        if resolved.inode_kind != InodeKind::Dir {
            return Err(CoreError::ExpectedDirectory {
                path: resolved.absolute_path,
                kind: resolved.inode_kind,
            });
        }

        self.visible_children(resolved.inode_id)?
            .into_iter()
            .map(|direntry| {
                let child = self
                    .visible_inode(direntry.child_inode_id)?
                    .expect("visible child listing should resolve inode");
                let child_path = AbsolutePath::parse(&resolved.absolute_path)
                    .map_err(map_path_error_to_core)?
                    .join(
                        &DisplayName::parse(&direntry.display_name)
                            .map_err(map_path_error_to_core)?,
                    );
                self.build_authoritative_path_entry(&ResolvedVisiblePath {
                    absolute_path: child_path.as_str().to_owned(),
                    inode_id: direntry.child_inode_id,
                    inode_kind: child.inode_kind,
                    parent_inode_id: Some(direntry.parent_inode_id),
                    display_name: direntry.display_name,
                })
            })
            .collect()
    }

    fn resolve_visible_path(
        &self,
        absolute_path: &AbsolutePath,
    ) -> Result<ResolvedVisiblePath, CoreError> {
        let root_inode_id = InodeId(1);
        let root = self
            .visible_inode(root_inode_id)?
            .ok_or(crate::metadata::VisiblePathError::RootMissing)?;
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
            let current_inode = self.visible_inode(current_inode_id)?.ok_or_else(|| {
                crate::metadata::VisiblePathError::PathNotFound {
                    absolute_path: current_absolute_path.clone(),
                }
            })?;
            if current_inode.inode_kind != InodeKind::Dir {
                return Err(
                    crate::metadata::VisiblePathError::PathComponentNotDirectory {
                        absolute_path: current_absolute_path,
                        inode_id: current_inode_id,
                        inode_kind: current_inode.inode_kind,
                    }
                    .into(),
                );
            }

            let requested_absolute_path = if current_absolute_path == "/" {
                format!("/{}", component.as_str())
            } else {
                format!("{}/{}", current_absolute_path, component.as_str())
            };
            let display_name = component.to_display_name();
            let name_key = NameKey::for_display_name(self.head.name_policy, &display_name);
            let direntry = self
                .visible_child(current_inode_id, name_key.as_str())?
                .ok_or(crate::metadata::VisiblePathError::PathNotFound {
                    absolute_path: requested_absolute_path,
                })?;

            current_parent_inode_id = Some(current_inode_id);
            current_inode_id = direntry.child_inode_id;
            current_absolute_path =
                absolute_path_prefix(&current_absolute_path, &direntry.display_name);
            current_display_name = direntry.display_name;
        }

        let inode = self.visible_inode(current_inode_id)?.ok_or_else(|| {
            crate::metadata::VisiblePathError::PathNotFound {
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

    fn build_authoritative_path_entry(
        &self,
        resolved: &ResolvedVisiblePath,
    ) -> Result<AuthoritativePathEntry, CoreError> {
        let revision = self.latest_revision_head(resolved.inode_id)?;
        let content_ref = revision
            .as_ref()
            .map(|revision| revision.content_ref.clone());
        let size_bytes = content_ref
            .as_ref()
            .map(|content_ref| content_ref.size_bytes);

        Ok(AuthoritativePathEntry {
            namespace_id: self.namespace_id.clone(),
            absolute_path: resolved.absolute_path.clone(),
            inode_id: resolved.inode_id,
            inode_kind: resolved.inode_kind.clone(),
            authoritative_head_seq: self.head.seq,
            parent_inode_id: resolved.parent_inode_id,
            display_name: resolved.display_name.clone(),
            revision_no: revision.as_ref().map(|revision| revision.revision_no),
            size_bytes,
            content_ref,
        })
    }

    fn visible_children(
        &self,
        parent_inode_id: InodeId,
    ) -> Result<Vec<DirentryBindRecord>, CoreError> {
        let Some(parent) = self.visible_inode(parent_inode_id)? else {
            return Ok(Vec::new());
        };
        if parent.inode_kind != InodeKind::Dir {
            return Ok(Vec::new());
        }

        let mut candidates = self.direntry_binds_for_parent(parent_inode_id)?;
        candidates.extend(
            self.wal_tail_rows
                .direntry_binds()
                .iter()
                .filter(|direntry| direntry.parent_inode_id == parent_inode_id)
                .cloned(),
        );
        let mut children = Vec::new();
        for direntry in candidates {
            let active = self.visible_child(parent_inode_id, &direntry.name_key)?;
            if active
                .as_ref()
                .map(|active| {
                    active.child_inode_id == direntry.child_inode_id
                        && active.bind_seq == direntry.bind_seq
                        && active.bind_delta_index == direntry.bind_delta_index
                })
                .unwrap_or(false)
                && self.visible_inode(direntry.child_inode_id)?.is_some()
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

    fn visible_child(
        &self,
        parent_inode_id: InodeId,
        name_key: &str,
    ) -> Result<Option<DirentryBindRecord>, CoreError> {
        let Some(parent) = self.visible_inode(parent_inode_id)? else {
            return Ok(None);
        };
        if parent.inode_kind != InodeKind::Dir {
            return Ok(None);
        }

        let Some(direntry) = self.bound_child(parent_inode_id, name_key)? else {
            return Ok(None);
        };
        if self.is_direntry_unbound(&direntry)? {
            return Ok(None);
        }
        let Some(latest_binding) =
            self.current_parent_binding_for_child(direntry.child_inode_id)?
        else {
            return Ok(None);
        };
        if latest_binding.parent_inode_id != direntry.parent_inode_id
            || latest_binding.name_key != direntry.name_key
            || latest_binding.bind_seq != direntry.bind_seq
            || latest_binding.bind_delta_index != direntry.bind_delta_index
            || self.is_direntry_unbound(&latest_binding)?
        {
            return Ok(None);
        }

        Ok(Some(direntry))
    }

    fn visible_inode(&self, inode_id: InodeId) -> Result<Option<InodeRecord>, CoreError> {
        let Some(inode) = self.inode_at_seq(inode_id)? else {
            return Ok(None);
        };
        if self.covering_subtree_tombstone(inode_id)?.is_some() {
            return Ok(None);
        }
        Ok(Some(inode))
    }

    fn inode_at_seq(&self, inode_id: InodeId) -> Result<Option<InodeRecord>, CoreError> {
        if let Some(inode) = self
            .wal_tail_rows
            .inodes()
            .iter()
            .find(|inode| inode.inode_id == inode_id && inode.created_seq <= self.head.seq)
            .cloned()
        {
            return Ok(Some(inode));
        }
        let key = format!("inode-{:020}", inode_id.0);
        Ok(self
            .tables
            .get(CheckpointTableFamily::Inodes, &key)
            .map_err(checkpoint_error_to_core)?
            .and_then(inode_from_checkpoint_row))
    }

    fn latest_revision_head(&self, inode_id: InodeId) -> Result<Option<RevisionRecord>, CoreError> {
        if self.visible_inode(inode_id)?.is_none() {
            return Ok(None);
        }
        let mut revisions = self.revisions_for_inode(inode_id)?;
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
            .max_by_key(|revision| (revision.committed_seq, revision.revision_delta_index)))
    }

    fn bound_child(
        &self,
        parent_inode_id: InodeId,
        name_key: &str,
    ) -> Result<Option<DirentryBindRecord>, CoreError> {
        let mut bindings = self.direntry_binds_for_parent_name(parent_inode_id, name_key)?;
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

    fn current_parent_binding_for_child(
        &self,
        child_inode_id: InodeId,
    ) -> Result<Option<DirentryBindRecord>, CoreError> {
        let mut bindings = self.direntry_binds_for_child(child_inode_id)?;
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
        if self.is_direntry_unbound(&binding)? {
            return Ok(None);
        }
        Ok(Some(binding))
    }

    fn is_direntry_unbound(&self, direntry: &DirentryBindRecord) -> Result<bool, CoreError> {
        let mut unbinds = self.direntry_unbinds_for_binding(direntry)?;
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

    fn active_subtree_tombstone(
        &self,
        root_inode_id: InodeId,
    ) -> Result<Option<SubtreeTombstoneRecord>, CoreError> {
        let mut tombstones = self.tombstones_for_root(root_inode_id)?;
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

    fn covering_subtree_tombstone(
        &self,
        inode_id: InodeId,
    ) -> Result<Option<SubtreeTombstoneRecord>, CoreError> {
        let mut current = Some(inode_id);
        let mut visited = std::collections::BTreeSet::new();
        while let Some(candidate_inode_id) = current {
            if !visited.insert(candidate_inode_id.0) {
                break;
            }
            if let Some(tombstone) = self.active_subtree_tombstone(candidate_inode_id)? {
                return Ok(Some(tombstone));
            }
            current = self
                .current_parent_binding_for_child(candidate_inode_id)?
                .map(|direntry| direntry.parent_inode_id);
        }
        Ok(None)
    }

    fn direntry_binds_for_parent(
        &self,
        parent_inode_id: InodeId,
    ) -> Result<Vec<DirentryBindRecord>, CoreError> {
        let prefix = format!("direntry-{:020}-", parent_inode_id.0);
        Ok(self
            .tables
            .scan_prefix(CheckpointTableFamily::DirentryBinds, &prefix)
            .map_err(checkpoint_error_to_core)?
            .into_iter()
            .filter_map(direntry_bind_from_checkpoint_row)
            .collect())
    }

    fn direntry_binds_for_parent_name(
        &self,
        parent_inode_id: InodeId,
        name_key: &str,
    ) -> Result<Vec<DirentryBindRecord>, CoreError> {
        let encoded_name_key = hex_encode_row_key_component(name_key);
        let prefix = format!("direntry-{:020}-{encoded_name_key}-", parent_inode_id.0);
        Ok(self
            .tables
            .scan_prefix(CheckpointTableFamily::DirentryBinds, &prefix)
            .map_err(checkpoint_error_to_core)?
            .into_iter()
            .filter_map(direntry_bind_from_checkpoint_row)
            .collect())
    }

    fn direntry_binds_for_child(
        &self,
        child_inode_id: InodeId,
    ) -> Result<Vec<DirentryBindRecord>, CoreError> {
        let prefix = format!("direntry-child-{:020}-", child_inode_id.0);
        Ok(self
            .tables
            .scan_prefix(CheckpointTableFamily::DirentryChildBinds, &prefix)
            .map_err(checkpoint_error_to_core)?
            .into_iter()
            .filter_map(direntry_bind_from_checkpoint_row)
            .collect())
    }

    fn direntry_unbinds_for_binding(
        &self,
        direntry: &DirentryBindRecord,
    ) -> Result<Vec<DirentryUnbindRecord>, CoreError> {
        let encoded_name_key = hex_encode_row_key_component(&direntry.name_key);
        let prefix = format!(
            "direntry-unbind-{:020}-{}-{:020}-{:010}-",
            direntry.parent_inode_id.0,
            encoded_name_key,
            direntry.bind_seq.0,
            direntry.bind_delta_index
        );
        Ok(self
            .tables
            .scan_prefix(CheckpointTableFamily::DirentryUnbinds, &prefix)
            .map_err(checkpoint_error_to_core)?
            .into_iter()
            .filter_map(direntry_unbind_from_checkpoint_row)
            .filter(|unbind| unbind_matches_binding(unbind, direntry))
            .collect())
    }

    fn revisions_for_inode(&self, inode_id: InodeId) -> Result<Vec<RevisionRecord>, CoreError> {
        let prefix = format!("revision-{:020}-", inode_id.0);
        Ok(self
            .tables
            .scan_prefix(CheckpointTableFamily::Revisions, &prefix)
            .map_err(checkpoint_error_to_core)?
            .into_iter()
            .filter_map(revision_from_checkpoint_row)
            .collect())
    }

    fn tombstones_for_root(
        &self,
        root_inode_id: InodeId,
    ) -> Result<Vec<SubtreeTombstoneRecord>, CoreError> {
        let prefix = format!("tombstone-{:020}-", root_inode_id.0);
        Ok(self
            .tables
            .scan_prefix(CheckpointTableFamily::Tombstones, &prefix)
            .map_err(checkpoint_error_to_core)?
            .into_iter()
            .filter_map(tombstone_from_checkpoint_row)
            .collect())
    }
}

fn checkpoint_error_to_core(error: CheckpointLoadError) -> CoreError {
    CoreError::Basis(BasisLoadError::CheckpointLoad(error))
}

fn absolute_path_prefix(current: &str, component: &str) -> String {
    if current == "/" {
        format!("/{component}")
    } else {
        format!("{current}/{component}")
    }
}

fn inode_from_checkpoint_row(row: CheckpointRow) -> Option<InodeRecord> {
    match row {
        CheckpointRow::Inode {
            inode_id,
            inode_kind,
            created_seq,
        } => Some(InodeRecord {
            inode_id,
            inode_kind,
            created_seq,
        }),
        _ => None,
    }
}

fn direntry_bind_from_checkpoint_row(row: CheckpointRow) -> Option<DirentryBindRecord> {
    match row {
        CheckpointRow::DirentryBind {
            parent_inode_id,
            name_key,
            display_name,
            child_inode_id,
            bind_seq,
            bind_delta_index,
        } => Some(DirentryBindRecord {
            parent_inode_id,
            name_key,
            display_name,
            child_inode_id,
            bind_seq,
            bind_delta_index,
        }),
        _ => None,
    }
}

fn direntry_unbind_from_checkpoint_row(row: CheckpointRow) -> Option<DirentryUnbindRecord> {
    match row {
        CheckpointRow::DirentryUnbind {
            parent_inode_id,
            name_key,
            child_inode_id,
            bind_seq,
            bind_delta_index,
            unbind_seq,
            unbind_delta_index,
        } => Some(DirentryUnbindRecord {
            parent_inode_id,
            name_key,
            child_inode_id,
            bind_seq,
            bind_delta_index,
            unbind_seq,
            unbind_delta_index,
        }),
        _ => None,
    }
}

fn revision_from_checkpoint_row(row: CheckpointRow) -> Option<RevisionRecord> {
    match row {
        CheckpointRow::Revision {
            inode_id,
            revision_no,
            committed_seq,
            revision_delta_index,
            content_ref,
        } => Some(RevisionRecord {
            inode_id,
            revision_no,
            committed_seq,
            revision_delta_index,
            content_ref,
        }),
        _ => None,
    }
}

fn tombstone_from_checkpoint_row(row: CheckpointRow) -> Option<SubtreeTombstoneRecord> {
    match row {
        CheckpointRow::Tombstone {
            root_inode_id,
            tombstone_seq,
            tombstone_delta_index,
        } => Some(SubtreeTombstoneRecord {
            root_inode_id,
            tombstone_seq,
            tombstone_delta_index,
        }),
        _ => None,
    }
}

fn unbind_matches_binding(unbind: &DirentryUnbindRecord, direntry: &DirentryBindRecord) -> bool {
    unbind.parent_inode_id == direntry.parent_inode_id
        && unbind.name_key == direntry.name_key
        && unbind.child_inode_id == direntry.child_inode_id
        && unbind.bind_seq == direntry.bind_seq
        && unbind.bind_delta_index == direntry.bind_delta_index
}

pub(crate) fn read_file_bytes<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
) -> Result<AuthoritativeFileBytes, CoreError> {
    let basis = load_verified_namespace_basis(store, namespace_id)?;
    read_file_bytes_from_basis(store, &basis, absolute_path)
}

pub(crate) fn read_file_bytes_from_basis<S: ObjectStore + ?Sized>(
    store: &S,
    basis: &VerifiedNamespaceBasis,
    absolute_path: &str,
) -> Result<AuthoritativeFileBytes, CoreError> {
    let entry = resolve_path_from_basis(basis, absolute_path)?;
    if entry.inode_kind != InodeKind::File {
        return Err(CoreError::ExpectedFile {
            path: entry.absolute_path,
            kind: entry.inode_kind,
        });
    }
    let content_ref = entry
        .content_ref
        .clone()
        .ok_or_else(|| CoreError::MissingPath(absolute_path.to_owned()))?;
    let read = read_durable_content_bytes(store, &basis.content_store_id, &content_ref)?;
    Ok(AuthoritativeFileBytes {
        entry,
        bytes: read.bytes,
    })
}

pub(crate) fn list_file_revisions<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
) -> Result<ListFileRevisionsResponse, CoreError> {
    let basis = load_verified_namespace_basis(store, namespace_id)?;
    list_file_revisions_from_basis(&basis, absolute_path)
}

pub(crate) fn list_file_revisions_from_basis(
    basis: &VerifiedNamespaceBasis,
    absolute_path: &str,
) -> Result<ListFileRevisionsResponse, CoreError> {
    let entry = resolve_path_from_basis(basis, absolute_path)?;
    if entry.inode_kind != InodeKind::File {
        return Err(CoreError::ExpectedFile {
            path: entry.absolute_path,
            kind: entry.inode_kind,
        });
    }
    list_file_revisions_for_inode_from_basis(basis, entry.inode_id)
}

pub(crate) fn list_file_revisions_for_inode<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
) -> Result<ListFileRevisionsResponse, CoreError> {
    let basis = load_verified_namespace_basis(store, namespace_id)?;
    list_file_revisions_for_inode_from_basis(&basis, inode_id)
}

pub(crate) fn list_file_revisions_for_inode_from_basis(
    basis: &VerifiedNamespaceBasis,
    inode_id: InodeId,
) -> Result<ListFileRevisionsResponse, CoreError> {
    let inode = basis
        .metadata_state
        .inode_at_seq(inode_id, basis.head.seq)
        .ok_or_else(|| CoreError::MissingPath(inode_id.to_string()))?;
    if inode.inode_kind != InodeKind::File {
        return Err(CoreError::ExpectedFile {
            path: inode_id.to_string(),
            kind: inode.inode_kind,
        });
    }

    let mut revisions = basis
        .metadata_state
        .revisions()
        .iter()
        .filter(|revision| {
            revision.inode_id == inode_id && revision.committed_seq <= basis.head.seq
        })
        .map(|revision| FileRevision {
            inode_id: revision.inode_id,
            revision_no: revision.revision_no,
            committed_seq: revision.committed_seq,
            content_ref: revision.content_ref.clone(),
        })
        .collect::<Vec<_>>();
    revisions.sort_by_key(|revision| revision.revision_no);

    Ok(ListFileRevisionsResponse {
        namespace_id: basis.head.namespace_id.clone(),
        inode_id,
        head_seq: basis.head.seq,
        revisions,
    })
}

pub(crate) fn read_file_revision_bytes<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    revision_no: RevisionNo,
) -> Result<AuthoritativeFileBytes, CoreError> {
    let basis = load_verified_namespace_basis(store, namespace_id)?;
    read_file_revision_bytes_from_basis(store, &basis, absolute_path, revision_no)
}

pub(crate) fn read_file_revision_bytes_from_basis<S: ObjectStore + ?Sized>(
    store: &S,
    basis: &VerifiedNamespaceBasis,
    absolute_path: &str,
    revision_no: RevisionNo,
) -> Result<AuthoritativeFileBytes, CoreError> {
    let mut entry = resolve_path_from_basis(basis, absolute_path)?;
    if entry.inode_kind != InodeKind::File {
        return Err(CoreError::ExpectedFile {
            path: entry.absolute_path,
            kind: entry.inode_kind,
        });
    }
    let revision = revision_for_inode(basis, entry.inode_id, revision_no)?;
    entry.revision_no = Some(revision.revision_no);
    entry.size_bytes = Some(revision.content_ref.size_bytes);
    entry.content_ref = Some(revision.content_ref.clone());
    let read = read_durable_content_bytes(store, &basis.content_store_id, &revision.content_ref)?;
    Ok(AuthoritativeFileBytes {
        entry,
        bytes: read.bytes,
    })
}

pub(crate) fn read_file_revision_bytes_for_inode<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    revision_no: RevisionNo,
) -> Result<Vec<u8>, CoreError> {
    let basis = load_verified_namespace_basis(store, namespace_id)?;
    read_file_revision_bytes_for_inode_from_basis(store, &basis, inode_id, revision_no)
}

pub(crate) fn read_file_revision_bytes_for_inode_from_basis<S: ObjectStore + ?Sized>(
    store: &S,
    basis: &VerifiedNamespaceBasis,
    inode_id: InodeId,
    revision_no: RevisionNo,
) -> Result<Vec<u8>, CoreError> {
    let revision = revision_for_inode(basis, inode_id, revision_no)?;
    let read = read_durable_content_bytes(store, &basis.content_store_id, &revision.content_ref)?;
    Ok(read.bytes)
}

fn revision_for_inode(
    basis: &VerifiedNamespaceBasis,
    inode_id: InodeId,
    revision_no: RevisionNo,
) -> Result<crate::metadata::RevisionRecord, CoreError> {
    let inode = basis
        .metadata_state
        .inode_at_seq(inode_id, basis.head.seq)
        .ok_or_else(|| CoreError::MissingPath(inode_id.to_string()))?;
    if inode.inode_kind != InodeKind::File {
        return Err(CoreError::ExpectedFile {
            path: inode_id.to_string(),
            kind: inode.inode_kind,
        });
    }
    basis
        .metadata_state
        .revision_at_seq(inode_id, revision_no, basis.head.seq)
        .ok_or(CoreError::MissingRevision {
            inode_id,
            revision_no,
        })
}

fn build_authoritative_path_entry(
    namespace_id: &NamespaceId,
    head_seq: ChangeSeq,
    metadata_state: &MetadataState,
    resolved: &ResolvedVisiblePath,
) -> Result<AuthoritativePathEntry, CoreError> {
    let revision = metadata_state.latest_revision_head_at_seq(resolved.inode_id, head_seq);
    let content_ref = revision
        .as_ref()
        .map(|revision| revision.content_ref.clone());
    let size_bytes = content_ref
        .as_ref()
        .map(|content_ref| content_ref.size_bytes);

    Ok(AuthoritativePathEntry {
        namespace_id: namespace_id.clone(),
        absolute_path: resolved.absolute_path.clone(),
        inode_id: resolved.inode_id,
        inode_kind: resolved.inode_kind.clone(),
        authoritative_head_seq: head_seq,
        parent_inode_id: resolved.parent_inode_id,
        display_name: resolved.display_name.clone(),
        revision_no: revision.as_ref().map(|revision| revision.revision_no),
        size_bytes,
        content_ref,
    })
}
