//! [`LoadedMetadataView`]: a verified, seq-pinned view of one namespace
//! that answers every read — stat, list, revisions, content; the grep read
//! lives with its machinery in [`super::grep`].

use super::listing::{invalid_cursor, validate_cursor_head, validate_directory_cursor};
use crate::checkpoint::{
    head_from_manifest, load_verified_manifest_tables_with_cache, MetadataTableCache,
    VerifiedMetadataTables, WalTailProjectionCache, WalTailProjectionCacheKey,
};
use crate::error::MetadataProjectionLoadError;
use crate::error::{CoreError, MetadataViewError};
use crate::metadata::{
    MetadataState, MetadataView, MetadataViewSession, ResolvedVisiblePath, RevisionRecord,
    VisibleChildEntry,
};
use crate::namespace::catalog::{load_namespace_catalog_entry, VerifiedNamespaceCatalogEntry};
#[cfg(test)]
use crate::namespace::control::read_head_and_metadata_root;
use crate::path::helpers::{map_path_error_to_core, parse_absolute_path_for_core};
use crate::storage::content::read_durable_content_bytes;
use crate::wal::{load_validated_wal_chain, project_validated_wal_tail, WalChainLoadRequest};
use loonfs_api::wire::control::{HeadState, NamespaceState};
use loonfs_api::ManifestObjectId;
use loonfs_api::{
    AbsolutePath, AuthoritativeFileBytes, AuthoritativePathEntry, ContentStoreId,
    DirectoryPageCursor, DisplayName, FileRevision, FileRevisionsPageCursor, InodeId, InodeKind,
    ManifestId, NamePolicy, NamespaceId, Page, PageRequest, RevisionNo,
};
use loonfs_objectstore::ObjectStore;
use std::sync::Arc;
use tracing::Instrument;

#[derive(Clone, Copy)]
pub(crate) enum ReadViewContext<'a> {
    /// Fresh head+root read with no caches: the shape embedded unit tests
    /// exercise; production reads always pin an anchor.
    #[cfg(test)]
    Latest,
    PinnedHead {
        head: &'a HeadState,
        head_etag: Option<&'a str>,
        /// Manifest pinned together with the head when the snapshot was
        /// taken. The live root may have moved past a pinned head; the
        /// pinned pair stays consistent (any manifest at or below the
        /// pinned seq serves it, with WAL replay covering the rest).
        manifest_id: ManifestId,
        manifest_object_id: &'a ManifestObjectId,
    },
}

#[derive(Clone, Copy)]
pub(crate) struct ReadLoadContext<'a> {
    view: ReadViewContext<'a>,
    table_cache: Option<&'a MetadataTableCache>,
    tail_cache: Option<&'a WalTailProjectionCache>,
    /// The namespace's immutable catalog pair, when the caller has already
    /// loaded it; a view load without one reads it from the store.
    catalog: Option<&'a VerifiedNamespaceCatalogEntry>,
}

impl<'a> ReadLoadContext<'a> {
    #[cfg(test)]
    pub(crate) fn latest() -> Self {
        Self {
            view: ReadViewContext::Latest,
            table_cache: None,
            tail_cache: None,
            catalog: None,
        }
    }

    pub(crate) fn pinned_head(
        head: &'a HeadState,
        head_etag: Option<&'a str>,
        manifest_id: ManifestId,
        manifest_object_id: &'a ManifestObjectId,
        table_cache: Option<&'a MetadataTableCache>,
        tail_cache: Option<&'a WalTailProjectionCache>,
        catalog: Option<&'a VerifiedNamespaceCatalogEntry>,
    ) -> Self {
        Self {
            view: ReadViewContext::PinnedHead {
                head,
                head_etag,
                manifest_id,
                manifest_object_id,
            },
            table_cache,
            tail_cache,
            catalog,
        }
    }
}

pub(crate) async fn load_metadata_view<'a, S: ObjectStore + ?Sized>(
    store: &'a S,
    namespace_id: &NamespaceId,
    context: ReadLoadContext<'a>,
) -> Result<LoadedMetadataView<'a, S>, CoreError> {
    match context.view {
        #[cfg(test)]
        ReadViewContext::Latest => {
            let (loaded_head, loaded_root) = read_head_and_metadata_root(store, namespace_id)
                .await
                .map_err(MetadataProjectionLoadError::LoadHead)?;
            LoadedMetadataView::load_at_head(
                store,
                namespace_id,
                loaded_head.envelope.state,
                loaded_root.envelope.state.manifest_id,
                loaded_root.envelope.state.manifest_object_id,
                context,
            )
            .await
        }
        ReadViewContext::PinnedHead {
            head,
            manifest_id,
            manifest_object_id,
            ..
        } => {
            LoadedMetadataView::load_at_head(
                store,
                namespace_id,
                head.clone(),
                manifest_id,
                manifest_object_id.clone(),
                context,
            )
            .await
        }
    }
}

pub(crate) struct LoadedMetadataView<'a, S: ObjectStore + ?Sized> {
    pub(super) namespace_id: NamespaceId,
    pub(super) content_store_id: ContentStoreId,
    name_policy: NamePolicy,
    pub(super) head: HeadState,
    pub(super) tables: VerifiedMetadataTables<'a, S>,
    wal_tail_rows: Arc<MetadataState>,
}

impl<'a, S: ObjectStore + ?Sized> LoadedMetadataView<'a, S> {
    async fn load_at_head(
        store: &'a S,
        namespace_id: &NamespaceId,
        head: HeadState,
        manifest_id: ManifestId,
        manifest_object_id: ManifestObjectId,
        load_context: ReadLoadContext<'a>,
    ) -> Result<Self, CoreError> {
        if &head.namespace_id != namespace_id {
            return Err(CoreError::NamespaceCorrupt(format!(
                "head namespace `{}` does not match requested namespace `{}`",
                head.namespace_id, namespace_id
            )));
        }
        if head.state == NamespaceState::Deleted {
            return Err(CoreError::NamespaceDeleted {
                namespace_id: namespace_id.clone(),
            });
        }
        let catalog_entry = match load_context.catalog {
            Some(entry) => entry.clone(),
            None => load_namespace_catalog_entry(store, namespace_id)
                .await
                .map_err(MetadataProjectionLoadError::from)?,
        };
        let tables = load_verified_manifest_tables_with_cache(
            store,
            load_context.table_cache,
            namespace_id,
            &manifest_object_id,
        )
        .await
        .map_err(|error| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(error))
        })?;
        let manifest_head = head_from_manifest(&head, tables.manifest());
        let cache_key = match load_context.view {
            #[cfg(test)]
            ReadViewContext::Latest => None,
            ReadViewContext::PinnedHead { head_etag, .. } => {
                head_etag.map(|etag| WalTailProjectionCacheKey {
                    namespace_id: namespace_id.clone(),
                    manifest_id,
                    manifest_head_seq: manifest_head.seq,
                    head_seq: head.seq,
                    head_etag: etag.to_owned(),
                })
            }
        };
        if let (Some(cache), Some(key)) = (load_context.tail_cache, cache_key.as_ref()) {
            if let Some(wal_tail_rows) = cache.get(key) {
                return Ok(Self {
                    namespace_id: namespace_id.clone(),
                    content_store_id: catalog_entry.content_store_id,
                    name_policy: catalog_entry.namespace_config.name_policy,
                    head,
                    tables,
                    wal_tail_rows,
                });
            }
        }
        let wal_chain = load_validated_wal_chain(
            store,
            WalChainLoadRequest {
                namespace_id,
                chain_base_seq: manifest_head.seq,
                head_seq: head.seq,
                visible_tip: head.visible_wal_tip.clone(),
                stop_after_seq: None,
                recent_segments: &head.recent_segments,
            },
        )
        .await
        .map_err(|error| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::WalChainLoad(error))
        })?;
        let replayed = {
            let _span =
                tracing::info_span!("loon.phase", phase = "project_metadata_state").entered();
            project_validated_wal_tail(
                &manifest_head,
                &MetadataState::default(),
                Some(head.writer_epoch),
                &wal_chain,
            )
            .map_err(|error| {
                CoreError::MetadataProjection(MetadataProjectionLoadError::WalReplay(error))
            })
        }?;
        let wal_tail_rows = Arc::new(replayed.resulting_metadata_state);
        if let (Some(cache), Some(key)) = (load_context.tail_cache, cache_key) {
            cache.insert(key, Arc::clone(&wal_tail_rows));
        }
        Ok(Self {
            namespace_id: namespace_id.clone(),
            content_store_id: catalog_entry.content_store_id,
            name_policy: catalog_entry.namespace_config.name_policy,
            head,
            tables,
            wal_tail_rows,
        })
    }

    #[tracing::instrument(
        level = "info",
        name = "loon.phase",
        err,
        skip_all,
        fields(phase = "walk_path")
    )]
    pub(crate) async fn resolve_path(
        &self,
        absolute_path: &str,
    ) -> Result<AuthoritativePathEntry, CoreError> {
        let absolute_path = parse_absolute_path_for_core(absolute_path)?;
        // One session serves the resolution and the entry build: the walk's
        // preloaded probes (including the leaf's head revision) are exactly
        // what the entry build reads back as cache hits.
        let mut session = self.metadata_view().session();
        let resolved = session.resolve_visible_path(&absolute_path, true).await?;
        self.build_authoritative_path_entry_with_session(&mut session, &resolved)
            .await
    }

    pub(crate) async fn read_file_bytes(
        &self,
        store: &S,
        absolute_path: &str,
        max_content_bytes: Option<u64>,
    ) -> Result<AuthoritativeFileBytes, CoreError> {
        let entry = self.resolve_path(absolute_path).await?;
        if entry.inode_kind != InodeKind::File {
            return Err(CoreError::ExpectedFile {
                path: entry.absolute_path,
                kind: entry.inode_kind,
            });
        }
        let content_ref = entry
            .content_ref
            .clone()
            .ok_or_else(|| CoreError::PathNotFound(absolute_path.to_owned()))?;
        ensure_within_read_limit(content_ref.size_bytes, max_content_bytes)?;
        let read = read_durable_content_bytes(store, &self.content_store_id, &content_ref).await?;
        Ok(AuthoritativeFileBytes {
            entry,
            bytes: read.bytes,
        })
    }

    pub(crate) async fn list_file_revisions_page(
        &self,
        absolute_path: &str,
        request: PageRequest<FileRevisionsPageCursor>,
    ) -> Result<Page<FileRevision, FileRevisionsPageCursor>, CoreError> {
        let entry = self.resolve_path(absolute_path).await?;
        if entry.inode_kind != InodeKind::File {
            return Err(CoreError::ExpectedFile {
                path: entry.absolute_path,
                kind: entry.inode_kind,
            });
        }
        self.list_file_revisions_for_inode_page(entry.inode_id, request)
            .await
    }

    pub(crate) async fn list_file_revisions_for_inode_page(
        &self,
        inode_id: InodeId,
        request: PageRequest<FileRevisionsPageCursor>,
    ) -> Result<Page<FileRevision, FileRevisionsPageCursor>, CoreError> {
        let inode = self
            .metadata_view()
            .inode_at_seq(inode_id)
            .await?
            .ok_or_else(|| CoreError::PathNotFound(inode_id.to_string()))?;
        if inode.inode_kind != InodeKind::File {
            return Err(CoreError::ExpectedFile {
                path: inode_id.to_string(),
                kind: inode.inode_kind,
            });
        }
        if let Some(cursor) = request.cursor.as_ref() {
            validate_file_revisions_cursor(cursor, self.head.seq, inode_id)?;
        }

        let start_after = request.cursor.as_ref().map(|cursor| {
            crate::metadata::manifest_index::RevisionPagePosition::after(
                cursor.last_revision_no,
                cursor.last_committed_seq,
                cursor.last_revision_delta_index,
            )
        });
        let mut revision_records = self
            .metadata_view()
            .session()
            .revisions_for_inode_page_desc(inode_id, start_after, request.limit.limit_plus_one())
            .await?;
        let has_more = revision_records.len() > request.limit.as_usize();
        if has_more {
            revision_records.truncate(request.limit.as_usize());
        }
        let next_cursor = if has_more {
            let last = revision_records
                .last()
                .expect("non-zero page limit with more revisions must return an item");
            Some(FileRevisionsPageCursor {
                head_seq: self.head.seq,
                inode_id,
                last_revision_no: last.revision_no,
                last_committed_seq: last.committed_seq,
                last_revision_delta_index: last.revision_delta_index,
            })
        } else {
            None
        };
        let revisions = revision_records
            .into_iter()
            .map(|revision| FileRevision {
                inode_id: revision.inode_id,
                revision_no: revision.revision_no,
                committed_seq: revision.committed_seq,
                committed_at_ms: revision.committed_at_ms,
                content_ref: revision.content_ref,
            })
            .collect();

        Ok(Page {
            items: revisions,
            next_cursor,
        })
    }

    pub(crate) async fn read_file_revision_bytes(
        &self,
        store: &S,
        absolute_path: &str,
        revision_no: RevisionNo,
        max_content_bytes: Option<u64>,
    ) -> Result<AuthoritativeFileBytes, CoreError> {
        let mut entry = self.resolve_path(absolute_path).await?;
        if entry.inode_kind != InodeKind::File {
            return Err(CoreError::ExpectedFile {
                path: entry.absolute_path,
                kind: entry.inode_kind,
            });
        }
        let revision = self.revision_for_inode(entry.inode_id, revision_no).await?;
        entry.revision_no = Some(revision.revision_no);
        entry.size_bytes = Some(revision.content_ref.size_bytes);
        entry.content_ref = Some(revision.content_ref.clone());
        ensure_within_read_limit(revision.content_ref.size_bytes, max_content_bytes)?;
        let read = read_durable_content_bytes(store, &self.content_store_id, &revision.content_ref)
            .await?;
        Ok(AuthoritativeFileBytes {
            entry,
            bytes: read.bytes,
        })
    }

    pub(crate) async fn read_file_revision_bytes_for_inode(
        &self,
        store: &S,
        inode_id: InodeId,
        revision_no: RevisionNo,
        max_content_bytes: Option<u64>,
    ) -> Result<Vec<u8>, CoreError> {
        let revision = self.revision_for_inode(inode_id, revision_no).await?;
        ensure_within_read_limit(revision.content_ref.size_bytes, max_content_bytes)?;
        let read = read_durable_content_bytes(store, &self.content_store_id, &revision.content_ref)
            .await?;
        Ok(read.bytes)
    }

    #[tracing::instrument(
        level = "info",
        name = "loon.phase",
        err,
        skip_all,
        fields(phase = "walk_path")
    )]
    pub(crate) async fn list_path_page(
        &self,
        absolute_path: &str,
        request: PageRequest<DirectoryPageCursor>,
    ) -> Result<Page<AuthoritativePathEntry, DirectoryPageCursor>, CoreError> {
        validate_cursor_head(self.head.seq, request.cursor.as_ref())?;

        let absolute_path = parse_absolute_path_for_core(absolute_path)?;
        let mut session = self.metadata_view().session();
        let resolved = session.resolve_visible_path(&absolute_path, false).await?;
        if let Some(cursor) = request.cursor.as_ref() {
            validate_directory_cursor(cursor, &resolved)?;
        }

        if resolved.inode_kind == InodeKind::File {
            if request.cursor.is_some() {
                return Err(invalid_cursor(
                    "directory cursor cannot resume a file listing",
                ));
            }
            return Ok(Page {
                items: vec![
                    self.build_authoritative_path_entry_with_session(&mut session, &resolved)
                        .await?,
                ],
                next_cursor: None,
            });
        }
        if resolved.inode_kind != InodeKind::Directory {
            return Err(CoreError::ExpectedDirectory {
                path: resolved.absolute_path,
                kind: resolved.inode_kind,
            });
        }

        let start_after = request
            .cursor
            .as_ref()
            .map(|cursor| cursor.last_name_key.as_str());
        let select_span = tracing::info_span!(
            "loon.phase",
            phase = "list_page_select_children",
            list_page_requested_limit = request.limit.as_usize() as u64,
            list_page_children_returned = tracing::field::Empty,
        );
        let mut children = session
            .visible_children_page_by_name_key(
                resolved.inode_id,
                start_after,
                request.limit.limit_plus_one(),
            )
            .instrument(select_span.clone())
            .await?;
        select_span.record("list_page_children_returned", children.len() as u64);
        let has_more = children.len() > request.limit.as_usize();
        if has_more {
            children.truncate(request.limit.as_usize());
        }

        let next_cursor = if has_more {
            let last = children
                .last()
                .expect("non-zero page limit with more children must return an item");
            Some(DirectoryPageCursor {
                head_seq: self.head.seq,
                dir_inode_id: resolved.inode_id,
                last_name_key: last.binding.name_key.clone(),
            })
        } else {
            None
        };

        let build_span = tracing::info_span!(
            "loon.phase",
            phase = "list_page_build_entries",
            list_page_children_returned = children.len() as u64,
            list_page_visible_child_calls = tracing::field::Empty,
            list_page_visible_inode_calls = tracing::field::Empty,
            list_page_current_parent_binding_calls = tracing::field::Empty,
            list_page_covering_tombstone_calls = tracing::field::Empty,
            list_page_latest_revision_calls = tracing::field::Empty,
            list_page_direntry_child_scan_calls = tracing::field::Empty,
            list_page_scan_prefix_calls = tracing::field::Empty,
            list_page_scan_range_page_calls = tracing::field::Empty,
            list_page_preload_unbind_range_scans = tracing::field::Empty,
            list_page_preload_child_lookups = tracing::field::Empty,
        );
        let entries = async {
            let mut entries = Vec::with_capacity(children.len());
            for child in children {
                entries.push(
                    self.build_authoritative_path_entry_from_visible_child(
                        &mut session,
                        &resolved,
                        child,
                    )
                    .await?,
                );
            }
            Ok::<_, CoreError>(entries)
        }
        .instrument(build_span.clone())
        .await?;
        let counters = session.counters();
        build_span.record(
            "list_page_visible_child_calls",
            counters.visible_child_calls,
        );
        build_span.record(
            "list_page_visible_inode_calls",
            counters.visible_inode_calls,
        );
        build_span.record(
            "list_page_current_parent_binding_calls",
            counters.current_parent_binding_calls,
        );
        build_span.record(
            "list_page_covering_tombstone_calls",
            counters.covering_tombstone_calls,
        );
        build_span.record(
            "list_page_latest_revision_calls",
            counters.latest_revision_calls,
        );
        build_span.record(
            "list_page_direntry_child_scan_calls",
            counters.direntry_child_scan_calls,
        );
        build_span.record("list_page_scan_prefix_calls", counters.scan_prefix_calls);
        build_span.record(
            "list_page_scan_range_page_calls",
            counters.scan_range_page_calls,
        );
        build_span.record(
            "list_page_preload_unbind_range_scans",
            counters.list_preload_unbind_range_scans,
        );
        build_span.record(
            "list_page_preload_child_lookups",
            counters.list_preload_child_lookups,
        );

        Ok(Page {
            items: entries,
            next_cursor,
        })
    }

    /// `resolved` must come from visible resolution or enumeration at this
    /// session's seq (both callers do), so the revision lookup does not
    /// re-derive the inode's visibility.
    async fn build_authoritative_path_entry_with_session(
        &self,
        session: &mut MetadataViewSession<'_, '_, S>,
        resolved: &ResolvedVisiblePath,
    ) -> Result<AuthoritativePathEntry, CoreError> {
        let revision = if resolved.inode_kind == InodeKind::File {
            session
                .latest_revision_head_of_visible(resolved.inode_id)
                .await?
        } else {
            None
        };
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
            inode_kind: resolved.inode_kind,
            head_seq: self.head.seq,
            parent_inode_id: resolved.parent_inode_id,
            display_name: resolved.display_name.clone(),
            revision_no: revision.as_ref().map(|revision| revision.revision_no),
            size_bytes,
            content_ref,
            committed_at_ms: revision.as_ref().map(|revision| revision.committed_at_ms),
        })
    }

    async fn build_authoritative_path_entry_from_visible_child(
        &self,
        session: &mut MetadataViewSession<'_, '_, S>,
        resolved_dir: &ResolvedVisiblePath,
        child: VisibleChildEntry,
    ) -> Result<AuthoritativePathEntry, CoreError> {
        let child_path = AbsolutePath::parse(&resolved_dir.absolute_path)
            .map_err(map_path_error_to_core)?
            .join(
                &DisplayName::parse(&child.binding.display_name).map_err(map_path_error_to_core)?,
            );
        self.build_authoritative_path_entry_with_session(
            session,
            &ResolvedVisiblePath {
                absolute_path: child_path.as_str().to_owned(),
                inode_id: child.binding.child_inode_id,
                inode_kind: child.inode.inode_kind,
                parent_inode_id: Some(child.binding.parent_inode_id),
                display_name: child.binding.display_name,
            },
        )
        .await
    }

    async fn revision_for_inode(
        &self,
        inode_id: InodeId,
        revision_no: RevisionNo,
    ) -> Result<RevisionRecord, CoreError> {
        self.metadata_view()
            .revision_for_inode(inode_id, revision_no)
            .await
    }

    pub(super) fn metadata_view(&self) -> MetadataView<'_, '_, S> {
        MetadataView::from_loaded_head(
            &self.head,
            self.name_policy,
            &self.tables,
            self.wal_tail_rows.as_ref(),
        )
    }
}

/// Refuses a buffered content read whose resolved size exceeds the caller's
/// budget. The check runs on metadata, before any content fetch, so an
/// over-limit read costs no object-store traffic and allocates nothing.
fn ensure_within_read_limit(
    size_bytes: u64,
    max_content_bytes: Option<u64>,
) -> Result<(), CoreError> {
    match max_content_bytes {
        Some(max_bytes) if size_bytes > max_bytes => Err(CoreError::ContentTooLarge {
            size_bytes,
            max_bytes,
        }),
        _ => Ok(()),
    }
}

fn validate_file_revisions_cursor(
    cursor: &FileRevisionsPageCursor,
    head_seq: loonfs_api::ChangeSeq,
    inode_id: InodeId,
) -> Result<(), CoreError> {
    if cursor.head_seq != head_seq {
        // A stale snapshot is the same restart-from-fresh-state condition
        // directory listing reports, not a malformed request: both answer
        // `rebootstrap_required` (API spec, "Pagination").
        return Err(MetadataViewError::UnsupportedHistoricalRead {
            requested_seq: cursor.head_seq,
            head_seq,
        }
        .into());
    }
    if cursor.inode_id != inode_id {
        return Err(invalid_cursor(
            "file revisions cursor inode does not match the requested file",
        ));
    }
    Ok(())
}
