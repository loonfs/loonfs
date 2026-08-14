//! [`LoadedMetadataView`]: a verified, seq-pinned view of one namespace
//! that answers core reads.

use super::current_files::resolve_visible_inode;
use super::listing::{invalid_cursor, validate_cursor_head, validate_directory_cursor};
use crate::checkpoint::{
    head_from_manifest, load_basis_metadata_tables, MetadataTableCache, VerifiedMetadataTables,
    WalTailProjectionCache, WalTailProjectionCacheKey,
};
use crate::error::MetadataProjectionLoadError;
use crate::error::{CoreError, MetadataViewError, Result};
use crate::metadata::{
    LeafRevisionPrefetch, MetadataState, MetadataView, MetadataViewSession, ResolvedVisiblePath,
    RevisionRecord, VisibleChildEntry, METADATA_VIEW_SESSION_COUNTER_FIELDS,
};
#[cfg(test)]
use crate::namespace::basis::read_head_and_metadata_basis;
use crate::namespace::basis::MetadataBasis;
use crate::namespace::catalog::VerifiedNamespaceCatalogEntry;
use crate::path::helpers::{map_path_error_to_core, parse_absolute_path_for_core};
use crate::storage::content::{content_object_key_for_ref, read_durable_content_bytes};
use crate::wal::{load_validated_wal_chain, project_validated_wal_tail, WalChainLoadRequest};
use loonfs_api::wire::control::{HeadState, NamespaceState};
use loonfs_api::{
    AbsolutePath, AttributesProjection, AuthoritativeFileBytes, AuthoritativePathEntry,
    AuthoritativePathEntryKind, ChangeSeq, ContentRef, ContentStoreId, DirectoryPageCursor,
    DisplayName, FileRevision, FileRevisionsPageCursor, InodeId, InodeKind, ManifestId,
    NamespaceId, Page, PageRequest, RevisionNo, TrashEntry, TrashPageCursor,
};
use loonfs_objectstore::ObjectStore;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::Instrument;

/// Whether an entry build reads the inode's attributes.
///
/// Attributes are the one part of an entry a read pays for per inode, so the
/// caller states it rather than the builder guessing: a stat includes them,
/// a listing omits them unless asked, and every other read that resolves a
/// path on its way to content omits them.
///
/// A second projectable field should grow this into a projection struct
/// rather than add a second parameter beside it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AttributeProjection {
    Include,
    Omit,
}

impl From<bool> for AttributeProjection {
    fn from(include: bool) -> Self {
        match include {
            true => Self::Include,
            false => Self::Omit,
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum ReadViewContext<'a> {
    /// Fresh head+root read with no caches: the shape embedded unit tests
    /// exercise; production reads always pin an anchor.
    #[cfg(test)]
    Latest,
    PinnedHead {
        head: &'a HeadState,
        head_etag: Option<&'a str>,
        /// Basis pinned together with the head when the snapshot was
        /// taken. The live root may have moved past a pinned head; the
        /// pinned pair stays consistent (any manifest at or below the
        /// pinned seq serves it, with WAL replay covering the rest).
        basis: &'a MetadataBasis,
    },
}

#[derive(Clone, Copy)]
pub(crate) struct ReadLoadContext<'a> {
    view: ReadViewContext<'a>,
    table_cache: Option<&'a MetadataTableCache>,
    tail_cache: Option<&'a WalTailProjectionCache>,
}

impl<'a> ReadLoadContext<'a> {
    #[cfg(test)]
    pub(crate) fn latest() -> Self {
        Self {
            view: ReadViewContext::Latest,
            table_cache: None,
            tail_cache: None,
        }
    }

    pub(crate) fn pinned_head(
        head: &'a HeadState,
        head_etag: Option<&'a str>,
        basis: &'a MetadataBasis,
        table_cache: Option<&'a MetadataTableCache>,
        tail_cache: Option<&'a WalTailProjectionCache>,
    ) -> Self {
        Self {
            view: ReadViewContext::PinnedHead {
                head,
                head_etag,
                basis,
            },
            table_cache,
            tail_cache,
        }
    }
}

/// The metadata anchor one read is pinned to.
///
/// Three numbers say where a read's answer came from: the head sequence it
/// answers at, the manifest its verified tables were loaded from, and the
/// head sequence that manifest was published at. A read that answers
/// not-found for a path a writer had already committed is either anchored
/// behind that write or reading tables that do not hold it, and the two
/// cases are told apart by these three numbers alone.
///
/// They are the same three the WAL-tail projection cache keys a projection
/// by, so a read span names its anchor with the words that cache already
/// uses. All three are plain numbers, so recording them costs no allocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ReadAnchor {
    head_seq: ChangeSeq,
    manifest_id: ManifestId,
    manifest_head_seq: ChangeSeq,
}

pub(crate) async fn load_metadata_view<'a, S: ObjectStore + ?Sized>(
    store: &'a S,
    namespace_id: &NamespaceId,
    context: ReadLoadContext<'a>,
) -> Result<LoadedMetadataView<'a, S>> {
    match context.view {
        #[cfg(test)]
        ReadViewContext::Latest => {
            let loaded = read_head_and_metadata_basis(store, namespace_id)
                .await
                .map_err(MetadataProjectionLoadError::LoadHead)?;
            LoadedMetadataView::load_at_head(
                store,
                namespace_id,
                loaded.head.state,
                &loaded.basis,
                context,
            )
            .await
        }
        ReadViewContext::PinnedHead { head, basis, .. } => {
            LoadedMetadataView::load_at_head(store, namespace_id, head.clone(), basis, context)
                .await
        }
    }
}

/// Where one file's bytes actually live, for a host that is about to
/// authorize a client to read them without passing them through itself.
///
/// It names one immutable content object, so it does not go stale when the
/// path moves on: a commit that replaces the file writes a new object and
/// leaves this one where it is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectDownloadTarget {
    /// Absolute path as rendered from stored display names.
    pub absolute_path: AbsolutePath,
    /// Revision this target reads, resolved from the request.
    pub revision_no: RevisionNo,
    /// Identity, byte length, and checksum evidence for those bytes.
    pub content_ref: ContentRef,
    /// Logical unscoped object key an issuer signs a read of.
    pub object_key: String,
}

/// Content information needed to authorize an inode download.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectDownloadByInodeTarget {
    /// File inode that owns this revision.
    pub inode_id: InodeId,
    /// Revision being read.
    pub revision_no: RevisionNo,
    /// Content identity, size, and checksum.
    pub content_ref: ContentRef,
    /// Object key used to sign the read.
    pub object_key: String,
}

/// A coherent, seq-pinned namespace read view.
///
/// Every read the engine answers for one pinned context runs over one of
/// these; the view is crate-internal, and consumers reach it through the
/// engine's read methods.
pub(crate) struct LoadedMetadataView<'a, S: ObjectStore + ?Sized> {
    pub(super) namespace_id: NamespaceId,
    pub(super) content_store_id: ContentStoreId,
    pub(super) head: HeadState,
    pub(super) tables: VerifiedMetadataTables<'a, S>,
    wal_tail_rows: Arc<MetadataState>,
    anchor: ReadAnchor,
}

impl<'a, S: ObjectStore + ?Sized> LoadedMetadataView<'a, S> {
    #[cfg(test)]
    pub(crate) fn head(&self) -> &HeadState {
        &self.head
    }

    #[cfg(test)]
    pub(crate) fn projected_metadata_view(&self) -> MetadataView<'_, '_, S> {
        self.metadata_view()
    }

    async fn load_at_head(
        store: &'a S,
        namespace_id: &NamespaceId,
        head: HeadState,
        basis: &MetadataBasis,
        load_context: ReadLoadContext<'a>,
    ) -> Result<Self> {
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
        let catalog_entry = VerifiedNamespaceCatalogEntry::from_head(&head);
        let manifest_id = basis.manifest_id();
        let loaded_basis = load_basis_metadata_tables(
            store,
            load_context.table_cache,
            namespace_id,
            basis,
            head.created_at_ms,
        )
        .await?;
        let tables = loaded_basis.tables;
        let manifest_head = head_from_manifest(&head, tables.manifest());
        let anchor = ReadAnchor {
            head_seq: head.seq,
            manifest_id,
            manifest_head_seq: manifest_head.seq,
        };
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
                    content_store_id: catalog_entry.content_store_id().clone(),
                    head,
                    tables,
                    wal_tail_rows,
                    anchor,
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
                tracing::debug_span!("loonfs.phase", phase = "project_metadata_state").entered();
            project_validated_wal_tail(
                &manifest_head,
                &loaded_basis.base_state,
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
            content_store_id: catalog_entry.content_store_id().clone(),
            head,
            tables,
            wal_tail_rows,
            anchor,
        })
    }

    #[tracing::instrument(
        level = "debug",
        name = "loonfs.phase",
        err(level = "debug"),
        skip_all,
        fields(
            phase = "walk_path",
            head_seq = self.anchor.head_seq.0,
            manifest_id = self.anchor.manifest_id.0,
            manifest_head_seq = self.anchor.manifest_head_seq.0,
        )
    )]
    pub(crate) async fn resolve_path(
        &self,
        absolute_path: &str,
        attributes: AttributeProjection,
    ) -> Result<AuthoritativePathEntry> {
        let absolute_path = parse_absolute_path_for_core(absolute_path)?;
        // One session serves the resolution and the entry build: the walk's
        // preloaded probes (including the leaf's head revision) are exactly
        // what the entry build reads back as cache hits.
        let mut session = self.metadata_view().session();
        let resolved = session
            .resolve_visible_path(&absolute_path, LeafRevisionPrefetch::Prefetch)
            .await?;
        self.build_authoritative_path_entry_with_session(&mut session, &resolved, attributes)
            .await
    }

    /// Returns the current entry for a visible inode without listing a
    /// directory.
    pub(crate) async fn stat_inode(
        &self,
        inode_id: InodeId,
        attributes: AttributeProjection,
    ) -> Result<AuthoritativePathEntry> {
        let mut session = self.metadata_view().session();
        let mut ancestor_paths = HashMap::new();
        let resolved = resolve_visible_inode(&mut session, &mut ancestor_paths, inode_id)
            .await?
            .ok_or(CoreError::InodeNotFound(inode_id))?;
        self.build_authoritative_path_entry_with_session(&mut session, &resolved, attributes)
            .await
    }

    pub(crate) async fn read_file_bytes(
        &self,
        store: &S,
        absolute_path: &str,
        max_content_bytes: Option<u64>,
    ) -> Result<AuthoritativeFileBytes> {
        let (entry, content_ref) = self.resolve_file_content(absolute_path).await?;
        ensure_within_read_limit(content_ref.size_bytes, max_content_bytes)?;
        let read = read_durable_content_bytes(store, &self.content_store_id, &content_ref).await?;
        Ok(AuthoritativeFileBytes {
            entry,
            bytes: read.bytes,
        })
    }

    /// Resolves a path to the file it names and the content reference its
    /// current revision points at: the metadata half both the buffered read
    /// and the streaming one run, so neither can resolve a path the other
    /// would refuse.
    pub(crate) async fn resolve_file_content(
        &self,
        absolute_path: &str,
    ) -> Result<(AuthoritativePathEntry, ContentRef)> {
        let entry = self
            .resolve_path(absolute_path, AttributeProjection::Omit)
            .await?;
        let content_ref = match &entry.kind {
            AuthoritativePathEntryKind::File { content_ref, .. } => content_ref.clone(),
            AuthoritativePathEntryKind::Directory {} => {
                return Err(CoreError::ExpectedFile {
                    path: entry.path.to_string(),
                    kind: InodeKind::Directory,
                });
            }
        };
        Ok((entry, content_ref))
    }

    /// The content store this view's namespace is bound to.
    pub(crate) fn content_store_id(&self) -> &ContentStoreId {
        &self.content_store_id
    }

    /// Resolves a path to the content object a direct read would fetch:
    /// the reference that names those bytes, and the key that addresses
    /// them.
    ///
    /// No bytes move and no read limit applies. The limit bounds what this
    /// server buffers for one response, and this is the transport that
    /// buffers nothing — which is the whole reason it exists, because a
    /// deployment that allowed a direct upload past that limit has an
    /// object it could not otherwise hand back.
    pub(crate) async fn direct_download_target(
        &self,
        absolute_path: &str,
        revision_no: Option<RevisionNo>,
    ) -> Result<DirectDownloadTarget> {
        let entry = self
            .resolve_path(absolute_path, AttributeProjection::Omit)
            .await?;
        let current_revision = match &entry.kind {
            AuthoritativePathEntryKind::File {
                revision_no,
                content_ref,
                ..
            } => (*revision_no, content_ref.clone()),
            AuthoritativePathEntryKind::Directory {} => {
                return Err(CoreError::ExpectedFile {
                    path: entry.path.to_string(),
                    kind: InodeKind::Directory,
                });
            }
        };
        let (revision_no, content_ref) = match revision_no {
            Some(requested) => {
                let revision = self.revision_for_inode(entry.inode_id, requested).await?;
                (revision.revision_no, revision.content_ref)
            }
            None => current_revision,
        };
        let object_key = content_object_key_for_ref(&self.content_store_id, &content_ref)?;

        Ok(DirectDownloadTarget {
            absolute_path: entry.path,
            revision_no,
            content_ref,
            object_key,
        })
    }

    /// Resolves retained inode content without requiring a current path.
    pub(crate) async fn direct_download_target_by_inode(
        &self,
        inode_id: InodeId,
        revision_no: RevisionNo,
    ) -> Result<DirectDownloadByInodeTarget> {
        let revision = self.revision_for_inode(inode_id, revision_no).await?;
        let object_key = content_object_key_for_ref(&self.content_store_id, &revision.content_ref)?;
        Ok(DirectDownloadByInodeTarget {
            inode_id,
            revision_no: revision.revision_no,
            content_ref: revision.content_ref,
            object_key,
        })
    }

    pub(crate) async fn list_file_revisions_page(
        &self,
        absolute_path: &str,
        request: PageRequest<FileRevisionsPageCursor>,
    ) -> Result<Page<FileRevision, FileRevisionsPageCursor>> {
        let entry = self
            .resolve_path(absolute_path, AttributeProjection::Omit)
            .await?;
        if matches!(entry.kind, AuthoritativePathEntryKind::Directory {}) {
            return Err(CoreError::ExpectedFile {
                path: entry.path.to_string(),
                kind: InodeKind::Directory,
            });
        }
        self.list_file_revisions_for_inode_page(entry.inode_id, request)
            .await
    }

    /// One page of the namespace's recoverable deletions, oldest deletion
    /// first, as one bounded range scan over the derived active-deletion
    /// family.
    ///
    /// The materializer keeps that family in step with the tombstone family —
    /// a delete adds the row, an undelete removes it — so the page reads only
    /// the deletions it returns, never the namespace's whole deletion
    /// history. Rows are never dropped at the retention floor, so a deletion
    /// stays listed and recoverable however far the floor advances.
    pub(crate) async fn list_trash_page(
        &self,
        request: PageRequest<TrashPageCursor>,
    ) -> Result<Page<TrashEntry, TrashPageCursor>> {
        if let Some(cursor) = request.cursor.as_ref() {
            if cursor.head_seq > self.head.seq {
                // Forward-only drift, the same rule as every other cursor.
                return Err(MetadataViewError::SnapshotUnavailable {
                    requested_seq: cursor.head_seq,
                    head_seq: self.head.seq,
                }
                .into());
            }
        }
        let start_after = request
            .cursor
            .as_ref()
            .map(|cursor| (cursor.last_deleted_at_seq, cursor.last_root_inode_id));
        let mut deletions = self
            .metadata_view()
            .session()
            .active_deletions_page(start_after, request.limit.limit_plus_one())
            .await?;
        let has_more = deletions.len() > request.limit.as_usize();
        if has_more {
            deletions.truncate(request.limit.as_usize());
        }
        let next_cursor = if has_more {
            let last = deletions
                .last()
                .expect("non-zero page limit with more entries must return an item");
            Some(TrashPageCursor {
                head_seq: self.head.seq,
                last_deleted_at_seq: last.deleted_at_seq,
                last_root_inode_id: last.root_inode_id,
            })
        } else {
            None
        };
        // The entry keeps parent, key, and name as separate optional fields,
        // so it projects all three out of the one binding the deletion
        // recorded.
        let entries = deletions
            .into_iter()
            .map(|deletion| TrashEntry {
                inode_id: deletion.root_inode_id,
                deletion_seq: deletion.deleted_at_seq,
                deleted_at_ms: deletion.deleted_at_ms,
                deleted_by: deletion.deleted_by,
                parent_inode_id: deletion
                    .deleted_direntry
                    .as_ref()
                    .map(|direntry| direntry.parent_inode_id),
                name_key: deletion
                    .deleted_direntry
                    .as_ref()
                    .map(|direntry| direntry.name_key.clone()),
                display_name: deletion
                    .deleted_direntry
                    .map(|direntry| direntry.display_name),
            })
            .collect();
        Ok(Page {
            items: entries,
            next_cursor,
        })
    }

    pub(crate) async fn list_file_revisions_for_inode_page(
        &self,
        inode_id: InodeId,
        request: PageRequest<FileRevisionsPageCursor>,
    ) -> Result<Page<FileRevision, FileRevisionsPageCursor>> {
        let inode = self
            .metadata_view()
            .inode_at_seq(inode_id)
            .await?
            .ok_or(CoreError::InodeNotFound(inode_id))?;
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
                actor: revision.actor,
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
    ) -> Result<AuthoritativeFileBytes> {
        let mut entry = self
            .resolve_path(absolute_path, AttributeProjection::Omit)
            .await?;
        if matches!(entry.kind, AuthoritativePathEntryKind::Directory {}) {
            return Err(CoreError::ExpectedFile {
                path: entry.path.to_string(),
                kind: InodeKind::Directory,
            });
        }
        let revision = self.revision_for_inode(entry.inode_id, revision_no).await?;
        entry.kind = AuthoritativePathEntryKind::File {
            revision_no: revision.revision_no,
            size_bytes: revision.content_ref.size_bytes,
            content_ref: revision.content_ref.clone(),
            revision_actor: revision.actor,
            committed_at_ms: revision.committed_at_ms,
        };
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
    ) -> Result<Vec<u8>> {
        let revision = self.revision_for_inode(inode_id, revision_no).await?;
        ensure_within_read_limit(revision.content_ref.size_bytes, max_content_bytes)?;
        let read = read_durable_content_bytes(store, &self.content_store_id, &revision.content_ref)
            .await?;
        Ok(read.bytes)
    }

    #[tracing::instrument(
        level = "debug",
        name = "loonfs.phase",
        err(level = "debug"),
        skip_all,
        fields(
            phase = "walk_path",
            head_seq = self.anchor.head_seq.0,
            manifest_id = self.anchor.manifest_id.0,
            manifest_head_seq = self.anchor.manifest_head_seq.0,
        )
    )]
    pub(crate) async fn list_path_page(
        &self,
        absolute_path: &str,
        request: PageRequest<DirectoryPageCursor>,
        attributes: AttributeProjection,
    ) -> Result<Page<AuthoritativePathEntry, DirectoryPageCursor>> {
        validate_cursor_head(self.head.seq, request.cursor.as_ref())?;

        let absolute_path = parse_absolute_path_for_core(absolute_path)?;
        let mut session = self.metadata_view().session();
        let resolved = session
            .resolve_visible_path(&absolute_path, LeafRevisionPrefetch::Skip)
            .await?;
        if let Some(cursor) = request.cursor.as_ref() {
            validate_directory_cursor(cursor, &resolved)?;
        }

        match resolved.inode_kind {
            InodeKind::File => {
                if request.cursor.is_some() {
                    return Err(invalid_cursor(
                        "directory cursor cannot resume a file listing",
                    ));
                }
                return Ok(Page {
                    items: vec![
                        self.build_authoritative_path_entry_with_session(
                            &mut session,
                            &resolved,
                            attributes,
                        )
                        .await?,
                    ],
                    next_cursor: None,
                });
            }
            InodeKind::Directory => {}
        }

        let start_after = request
            .cursor
            .as_ref()
            .map(|cursor| cursor.last_name_key.as_str());
        let select_span = tracing::debug_span!(
            "loonfs.phase",
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
                directory_inode_id: resolved.inode_id,
                last_name_key: last.binding.name_key.clone(),
            })
        } else {
            None
        };

        let build_span = tracing::debug_span!(
            "loonfs.phase",
            phase = "list_page_build_entries",
            list_page_children_returned = children.len() as u64,
            { METADATA_VIEW_SESSION_COUNTER_FIELDS[0] } = tracing::field::Empty,
            { METADATA_VIEW_SESSION_COUNTER_FIELDS[1] } = tracing::field::Empty,
            { METADATA_VIEW_SESSION_COUNTER_FIELDS[2] } = tracing::field::Empty,
            { METADATA_VIEW_SESSION_COUNTER_FIELDS[3] } = tracing::field::Empty,
            { METADATA_VIEW_SESSION_COUNTER_FIELDS[4] } = tracing::field::Empty,
            { METADATA_VIEW_SESSION_COUNTER_FIELDS[5] } = tracing::field::Empty,
            { METADATA_VIEW_SESSION_COUNTER_FIELDS[6] } = tracing::field::Empty,
            { METADATA_VIEW_SESSION_COUNTER_FIELDS[7] } = tracing::field::Empty,
            { METADATA_VIEW_SESSION_COUNTER_FIELDS[8] } = tracing::field::Empty,
            { METADATA_VIEW_SESSION_COUNTER_FIELDS[9] } = tracing::field::Empty,
            { METADATA_VIEW_SESSION_COUNTER_FIELDS[10] } = tracing::field::Empty,
            { METADATA_VIEW_SESSION_COUNTER_FIELDS[11] } = tracing::field::Empty,
        );
        let entries = async {
            // A projected page reads every child's attributes as one wave,
            // so the build loop below runs over cache hits instead of a
            // round trip per entry.
            if attributes == AttributeProjection::Include {
                let child_inode_ids: Vec<InodeId> = children
                    .iter()
                    .map(|child| child.binding.child_inode_id)
                    .collect();
                session.preload_attributes(&child_inode_ids).await?;
            }
            let mut entries = Vec::with_capacity(children.len());
            for child in children {
                entries.push(
                    self.build_authoritative_path_entry_from_visible_child(
                        &mut session,
                        &resolved,
                        child,
                        attributes,
                    )
                    .await?,
                );
            }
            Ok::<_, CoreError>(entries)
        }
        .instrument(build_span.clone())
        .await?;
        session.counters().record_on(&build_span);

        Ok(Page {
            items: entries,
            next_cursor,
        })
    }

    /// `resolved` must come from visible resolution or enumeration at this
    /// session's seq (both callers do), so the revision and attribute lookups
    /// do not re-derive the inode's visibility.
    ///
    /// An included attribute projection carries the map and its revision even
    /// when the map is empty; an omitted one carries neither.
    async fn build_authoritative_path_entry_with_session(
        &self,
        session: &mut MetadataViewSession<'_, '_, S>,
        resolved: &ResolvedVisiblePath,
        attributes: AttributeProjection,
    ) -> Result<AuthoritativePathEntry> {
        let kind = match resolved.inode_kind {
            InodeKind::Directory => AuthoritativePathEntryKind::Directory {},
            InodeKind::File => {
                let revision = session
                    .latest_revision_head_of_visible(resolved.inode_id)
                    .await?
                    .ok_or_else(|| CoreError::PathNotFound(resolved.absolute_path.clone()))?;
                AuthoritativePathEntryKind::File {
                    revision_no: revision.revision_no,
                    size_bytes: revision.content_ref.size_bytes,
                    content_ref: revision.content_ref,
                    revision_actor: revision.actor,
                    committed_at_ms: revision.committed_at_ms,
                }
            }
        };
        // Attributes belong to the inode, so a directory answers as readily
        // as a file; only the projection decides whether the read happens.
        let attributes = match attributes {
            AttributeProjection::Include => {
                let (revision_no, attributes, updated_by, updated_at_ms) =
                    session.attributes_of_visible(resolved.inode_id).await?;
                Some(AttributesProjection {
                    attributes_revision_no: revision_no,
                    attributes_updated_by: updated_by,
                    attributes_updated_at_ms: updated_at_ms,
                    attributes,
                })
            }
            AttributeProjection::Omit => None,
        };
        let absolute_path = AbsolutePath::parse(&resolved.absolute_path).map_err(|error| {
            CoreError::NamespaceCorrupt(format!(
                "resolved visible path `{}` is not a valid absolute path: {error}",
                resolved.absolute_path
            ))
        })?;
        let display_name = resolved
            .parent_inode_id
            .map(|_| {
                DisplayName::parse(&resolved.display_name).map_err(|error| {
                    CoreError::NamespaceCorrupt(format!(
                        "stored display name for inode `{}` is invalid: {error}",
                        resolved.inode_id
                    ))
                })
            })
            .transpose()?;
        Ok(AuthoritativePathEntry {
            namespace_id: self.namespace_id.clone(),
            path: absolute_path,
            inode_id: resolved.inode_id,
            created_by: resolved.created_by.clone(),
            created_at_ms: resolved.created_at_ms,
            kind,
            head_seq: self.head.seq,
            parent_inode_id: resolved.parent_inode_id,
            display_name,
            attributes,
        })
    }

    async fn build_authoritative_path_entry_from_visible_child(
        &self,
        session: &mut MetadataViewSession<'_, '_, S>,
        resolved_dir: &ResolvedVisiblePath,
        child: VisibleChildEntry,
        attributes: AttributeProjection,
    ) -> Result<AuthoritativePathEntry> {
        let child_path = AbsolutePath::parse(&resolved_dir.absolute_path)
            .map_err(map_path_error_to_core)?
            .join(&child.binding.display_name);
        self.build_authoritative_path_entry_with_session(
            session,
            &ResolvedVisiblePath {
                absolute_path: child_path.as_str().to_owned(),
                inode_id: child.binding.child_inode_id,
                inode_kind: child.inode.inode_kind,
                created_by: child.inode.created_by,
                created_at_ms: child.inode.created_at_ms,
                parent_inode_id: Some(child.binding.parent_inode_id),
                display_name: child.binding.display_name.to_string(),
            },
            attributes,
        )
        .await
    }

    async fn revision_for_inode(
        &self,
        inode_id: InodeId,
        revision_no: RevisionNo,
    ) -> Result<RevisionRecord> {
        self.metadata_view()
            .revision_for_inode(inode_id, revision_no)
            .await
    }

    pub(super) fn metadata_view(&self) -> MetadataView<'_, '_, S> {
        MetadataView::from_loaded_head(&self.head, &self.tables, self.wal_tail_rows.as_ref())
    }
}

/// Refuses a buffered content read whose resolved size exceeds the caller's
/// budget. The check runs on metadata, before any content fetch, so an
/// over-limit read costs no object-store traffic and allocates nothing.
pub(crate) fn ensure_within_read_limit(
    size_bytes: u64,
    max_content_bytes: Option<u64>,
) -> Result<()> {
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
    head_seq: ChangeSeq,
    inode_id: InodeId,
) -> Result<()> {
    if cursor.head_seq > head_seq {
        // Forward-only drift, the same rule as directory listing and grep:
        // an older cursor resumes strictly after its last returned row at
        // whatever head is loaded now; only a cursor from the future is
        // unanswerable (`rebootstrap_required`).
        return Err(MetadataViewError::SnapshotUnavailable {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit_engine::{publish_namespace_commits_batch, CommitCandidate};
    use crate::context::MutationContext;
    use crate::namespace::bootstrap::bootstrap_namespace;
    use crate::path::write::{CommitRequest, FilesystemOperation};
    use loonfs_api::{AttributeKey, AttributeRevisionNo, AttributeValue, CommitId};
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use std::collections::BTreeMap;
    use tempfile::TempDir;

    fn context() -> MutationContext {
        MutationContext {
            writer_id: "reader-tests".to_owned(),
            now_ms: 1,
        }
    }

    fn attribute_key(key: &str) -> AttributeKey {
        AttributeKey::parse(key).expect("attribute key")
    }

    /// Bootstraps a namespace holding `/docs` with one annotated child
    /// directory and one bare child directory, so an entry build has both an
    /// inode with attributes and an inode without.
    async fn namespace_with_annotated_children() -> (TempDir, LocalFsStore, NamespaceId) {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let context = context();
        bootstrap_namespace(&store, &namespace_id, &context, false)
            .await
            .expect("bootstrap");

        for (commit_id, operation) in [
            (
                "create-docs",
                FilesystemOperation::CreateDirectory {
                    path: AbsolutePath::parse("/docs").expect("path"),
                    parents: false,
                },
            ),
            (
                "create-annotated",
                FilesystemOperation::CreateDirectory {
                    path: AbsolutePath::parse("/docs/annotated").expect("path"),
                    parents: false,
                },
            ),
            (
                "create-bare",
                FilesystemOperation::CreateDirectory {
                    path: AbsolutePath::parse("/docs/bare").expect("path"),
                    parents: false,
                },
            ),
            (
                "annotate",
                FilesystemOperation::UpdateAttributes {
                    path: AbsolutePath::parse("/docs/annotated").expect("path"),
                    set: BTreeMap::from([(
                        attribute_key("owner"),
                        AttributeValue::parse("ops").expect("valid attribute value"),
                    )]),
                    remove: Vec::new(),
                    expected_inode_id: None,
                    expected_attributes_revision_no: None,
                },
            ),
        ] {
            publish_namespace_commits_batch(
                &store,
                &namespace_id,
                vec![CommitCandidate::new(CommitRequest::single(
                    CommitId::parse(commit_id).expect("commit id"),
                    loonfs_test_support::test_actor(),
                    None,
                    operation,
                ))],
                &context,
            )
            .await
            .into_iter()
            .next()
            .expect("one result")
            .expect("commit lands");
        }
        (temp_dir, store, namespace_id)
    }

    /// A stat-shaped build with the projection on carries the grouped
    /// attribute state; with it off it carries none. The empty map is a
    /// projected answer, not an absent one.
    #[tokio::test]
    async fn an_entry_build_projects_grouped_attributes_or_none() {
        let (_temp_dir, store, namespace_id) = namespace_with_annotated_children().await;
        let view = load_metadata_view(&store, &namespace_id, ReadLoadContext::latest())
            .await
            .expect("load view");

        let annotated = view
            .resolve_path("/docs/annotated", AttributeProjection::Include)
            .await
            .expect("stat annotated");
        assert_eq!(
            annotated
                .attributes
                .as_ref()
                .map(|projection| projection.attributes_revision_no),
            Some(AttributeRevisionNo(1))
        );
        assert_eq!(
            annotated
                .attributes
                .as_ref()
                .and_then(|projection| projection.attributes.get(&attribute_key("owner")))
                .cloned(),
            Some(AttributeValue::parse("ops").expect("valid attribute value"))
        );

        let bare = view
            .resolve_path("/docs/bare", AttributeProjection::Include)
            .await
            .expect("stat bare");
        assert_eq!(
            bare.attributes.as_ref().map(|projection| {
                (
                    projection.attributes_revision_no,
                    projection.attributes.len(),
                )
            }),
            Some((AttributeRevisionNo(0), 0))
        );

        let omitted = view
            .resolve_path("/docs/annotated", AttributeProjection::Omit)
            .await
            .expect("stat without attributes");
        assert!(omitted.attributes.is_none());
    }

    /// The listing pays for attributes only when it was asked to. The session
    /// counter is the evidence: an omitted projection never calls the lookup,
    /// so a wide listing cannot quietly grow a per-entry read.
    #[tokio::test]
    async fn a_listing_reads_attributes_only_when_it_projects_them() {
        let (_temp_dir, store, namespace_id) = namespace_with_annotated_children().await;
        let view = load_metadata_view(&store, &namespace_id, ReadLoadContext::latest())
            .await
            .expect("load view");
        let directory = AbsolutePath::parse("/docs").expect("path");

        for (projection, expected_calls, expect_projected) in [
            (AttributeProjection::Omit, 0, false),
            (AttributeProjection::Include, 2, true),
        ] {
            let mut session = view.metadata_view().session();
            let resolved = session
                .resolve_visible_path(&directory, LeafRevisionPrefetch::Skip)
                .await
                .expect("resolve directory");
            let children = session
                .visible_children_page_by_name_key(resolved.inode_id, None, 16)
                .await
                .expect("children");
            assert_eq!(children.len(), 2);
            for child in children {
                let entry = view
                    .build_authoritative_path_entry_from_visible_child(
                        &mut session,
                        &resolved,
                        child,
                        projection,
                    )
                    .await
                    .expect("build entry");
                assert_eq!(entry.attributes.is_some(), expect_projected);
            }
            assert_eq!(
                session.counters().latest_attributes_calls,
                expected_calls,
                "{projection:?} made the wrong number of attribute lookups"
            );
        }
    }

    /// A projected listing reads every entry's attributes in one wave, so the
    /// per-entry build runs over cache hits and the page costs one round of
    /// lookups rather than one per entry.
    #[tokio::test]
    async fn a_projected_listing_reads_its_attributes_in_one_wave() {
        let (_temp_dir, store, namespace_id) = namespace_with_annotated_children().await;
        let view = load_metadata_view(&store, &namespace_id, ReadLoadContext::latest())
            .await
            .expect("load view");

        for (projection, expected_preloads) in [
            (AttributeProjection::Omit, 0),
            (AttributeProjection::Include, 2),
        ] {
            let mut session = view.metadata_view().session();
            let directory = AbsolutePath::parse("/docs").expect("path");
            let resolved = session
                .resolve_visible_path(&directory, LeafRevisionPrefetch::Skip)
                .await
                .expect("resolve directory");
            let children = session
                .visible_children_page_by_name_key(resolved.inode_id, None, 16)
                .await
                .expect("children");
            let child_inode_ids: Vec<InodeId> = children
                .iter()
                .map(|child| child.binding.child_inode_id)
                .collect();
            if projection == AttributeProjection::Include {
                session
                    .preload_attributes(&child_inode_ids)
                    .await
                    .expect("preload");
            }
            assert_eq!(
                session.counters().preload_attribute_lookups,
                expected_preloads
            );

            for child in children {
                view.build_authoritative_path_entry_from_visible_child(
                    &mut session,
                    &resolved,
                    child,
                    projection,
                )
                .await
                .expect("build entry");
            }
            // Nothing beyond the wave: every per-entry lookup was seeded.
            assert_eq!(
                session.counters().preload_attribute_lookups,
                expected_preloads
            );
        }
    }
}
