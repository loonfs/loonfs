//! Directory listings over a loaded metadata view: the pages, the entries
//! they list, and cursor validation and page framing.

use super::current_files::resolve_visible_inode;
use super::tree_listing::MAX_TREE_LISTING_DEPTH;
use crate::authorize::{Absence, ReadAccess};
use crate::error::{CoreError, Result};
use crate::metadata::{
    DirentryBindingRecord, LeafRevisionPreload, MetadataViewSession, ResolvedVisiblePath,
    VisibleChildEntry, METADATA_VIEW_SESSION_COUNTER_FIELDS,
};
use crate::path::mutation_path::parse_absolute_path_for_core;
use crate::path::read::loaded_view::LoadedMetadataView;
use crate::phase::Phase;
use loonfs_objectstore::ObjectStore;
use loonfs_types::options::{AttributeInclusion, ListOptions};
use loonfs_types::{
    AccessRight, ChangeSeq, DirectoryPageCursor, DirectoryTreePageCursor, DisplayName, InodeId,
    InodeKind, ListPageCursor, NameKey, Page, PageRequest, PathEntry, PathEntryKind,
};
use std::collections::HashMap;
use tracing::Instrument;

/// A cursor is an ordering resume: any head at or past the one that minted
/// it serves the next page, resuming strictly after the last returned key,
/// the same forward-only drift grep and change-feed cursors tolerate. Only a
/// cursor from a head ahead of this view's is refused, as an invalid cursor:
/// this view did not issue it.
pub(super) fn validate_cursor_head(
    current_head_seq: ChangeSeq,
    cursor_head_seq: Option<ChangeSeq>,
) -> Result<()> {
    let Some(cursor_head_seq) = cursor_head_seq else {
        return Ok(());
    };
    if cursor_head_seq > current_head_seq {
        return Err(invalid_cursor(format!(
            "the cursor was minted at seq `{cursor_head_seq}`, ahead of the serving head `{current_head_seq}`"
        )));
    }
    Ok(())
}

/// One page of a directory listing: of the directory's children, or of its
/// whole subtree.
pub(super) enum DirectoryListingRequest {
    Directory(PageRequest<DirectoryPageCursor>),
    DirectoryTree(PageRequest<DirectoryTreePageCursor>),
}

impl DirectoryListingRequest {
    /// Pairs a page request with the listing that `options` selects, and
    /// validates its cursor against the listed target.
    ///
    /// Each kind of listing accepts only its own kind of cursor.
    pub(super) fn new(
        request: PageRequest<ListPageCursor>,
        options: &ListOptions,
        resolved: &ResolvedVisiblePath,
    ) -> Result<Self> {
        let PageRequest { limit, cursor } = request;
        match (options.recursive, cursor) {
            (false, None) => Ok(Self::Directory(PageRequest {
                limit,
                cursor: None,
            })),
            (false, Some(ListPageCursor::Directory(cursor))) => {
                validate_cursor_directory(cursor.directory_inode_id, resolved)?;
                Ok(Self::Directory(PageRequest {
                    limit,
                    cursor: Some(cursor),
                }))
            }
            (true, None) => Ok(Self::DirectoryTree(PageRequest {
                limit,
                cursor: None,
            })),
            (true, Some(ListPageCursor::DirectoryTree(cursor))) => {
                validate_tree_cursor(&cursor, resolved)?;
                Ok(Self::DirectoryTree(PageRequest {
                    limit,
                    cursor: Some(cursor),
                }))
            }
            (false, Some(ListPageCursor::DirectoryTree(_))) => Err(invalid_cursor(
                "cursor kind `directory_tree` cannot resume a listing that is not recursive",
            )),
            (true, Some(ListPageCursor::Directory(_))) => Err(invalid_cursor(
                "cursor kind `directory` cannot resume a recursive listing",
            )),
        }
    }
}

/// Checks a recursive listing cursor's shape: one frame for each directory
/// from the listed directory down, at most the walk's depth, and a name key
/// in every frame but the last.
fn validate_tree_cursor(
    cursor: &DirectoryTreePageCursor,
    resolved: &ResolvedVisiblePath,
) -> Result<()> {
    let Some((last, levels)) = cursor.frames.split_last() else {
        return Err(invalid_cursor("recursive listing cursor has no frames"));
    };
    if cursor.frames.len() > MAX_TREE_LISTING_DEPTH {
        return Err(invalid_cursor(format!(
            "recursive listing cursor has `{}` frames, more than the walk's depth `{MAX_TREE_LISTING_DEPTH}`",
            cursor.frames.len()
        )));
    }
    if levels.iter().any(|frame| frame.last_name_key.is_none()) {
        return Err(invalid_cursor(
            "only the last frame of a recursive listing cursor can omit its name key",
        ));
    }
    let root = levels.first().unwrap_or(last);
    validate_cursor_directory(root.directory_inode_id, resolved)
}

fn validate_cursor_directory(
    cursor_directory_inode_id: InodeId,
    resolved: &ResolvedVisiblePath,
) -> Result<()> {
    if resolved.inode_kind != InodeKind::Directory {
        return Err(invalid_cursor(
            "directory cursor resolved to a non-directory target",
        ));
    }
    if resolved.inode_id != cursor_directory_inode_id {
        return Err(invalid_cursor(format!(
            "cursor directory inode `{cursor_directory_inode_id}` does not match the requested \
             directory inode `{}`",
            resolved.inode_id
        )));
    }
    Ok(())
}

pub(super) fn invalid_cursor(message: impl Into<String>) -> CoreError {
    CoreError::InvalidCursor(message.into())
}

impl<'a, S: ObjectStore + ?Sized> LoadedMetadataView<'a, S> {
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.phase",
        err(level = "debug"),
        skip_all,
        fields(
            phase = Phase::WalkPath.as_str(),
            head_seq = self.anchor.head_seq.0,
            manifest_no = self.anchor.manifest_no.0,
            manifest_head_seq = self.anchor.manifest_head_seq.0,
        )
    )]
    pub(crate) async fn list_path_page(
        &self,
        absolute_path: &str,
        request: PageRequest<ListPageCursor>,
        options: &ListOptions,
        access: &ReadAccess<'_, S>,
    ) -> Result<Page<PathEntry, ListPageCursor>> {
        validate_cursor_head(
            self.head.seq,
            request.cursor.as_ref().map(ListPageCursor::head_seq),
        )?;

        let absolute_path = parse_absolute_path_for_core(absolute_path)?;
        let mut session = self.metadata_view().session();
        let resolved = session
            .resolve_visible_path(&absolute_path, LeafRevisionPreload::Skip)
            .await?;
        access
            .require(
                &mut session,
                resolved.inode_id,
                AccessRight::Read.into(),
                Absence::Path(absolute_path.as_str()),
            )
            .await?;
        let request = DirectoryListingRequest::new(request, options, &resolved)?;

        match resolved.inode_kind {
            InodeKind::File => {
                return Ok(Page {
                    items: vec![
                        self.build_authoritative_path_entry_with_session(
                            &mut session,
                            &resolved,
                            options.include_attributes,
                        )
                        .await?,
                    ],
                    next_cursor: None,
                });
            }
            InodeKind::Directory => {}
        }

        self.directory_listing_page(
            &mut session,
            &resolved,
            request,
            options.include_attributes,
            access,
        )
        .await
    }

    /// Lists one page of a directory's children addressed by inode.
    ///
    /// Unknown or hidden inodes return [`CoreError::InodeNotFound`]. A
    /// non-directory target is a kind conflict rather than a single-entry
    /// listing: an inode-addressed caller asked for children, not for the
    /// entry itself.
    pub(crate) async fn list_inode_children_page(
        &self,
        inode_id: InodeId,
        request: PageRequest<ListPageCursor>,
        options: &ListOptions,
        access: &ReadAccess<'_, S>,
    ) -> Result<Page<PathEntry, ListPageCursor>> {
        validate_cursor_head(
            self.head.seq,
            request.cursor.as_ref().map(ListPageCursor::head_seq),
        )?;

        let mut session = self.metadata_view().session();
        let mut ancestor_paths = HashMap::new();
        let resolved = resolve_visible_inode(&mut session, &mut ancestor_paths, inode_id)
            .await?
            .ok_or(CoreError::InodeNotFound(inode_id))?;
        access
            .require(
                &mut session,
                inode_id,
                AccessRight::Read.into(),
                Absence::Inode,
            )
            .await?;
        if resolved.inode_kind != InodeKind::Directory {
            return Err(CoreError::ExpectedDirectory {
                target: resolved.absolute_path.to_string(),
                kind: resolved.inode_kind,
            });
        }
        let request = DirectoryListingRequest::new(request, options, &resolved)?;
        self.directory_listing_page(
            &mut session,
            &resolved,
            request,
            options.include_attributes,
            access,
        )
        .await
    }

    /// Lists one page of the directory `resolved`: of its children, or of
    /// its whole subtree.
    async fn directory_listing_page(
        &self,
        session: &mut MetadataViewSession<'_, '_, S>,
        resolved: &ResolvedVisiblePath,
        request: DirectoryListingRequest,
        attributes: AttributeInclusion,
        access: &ReadAccess<'_, S>,
    ) -> Result<Page<PathEntry, ListPageCursor>> {
        match request {
            DirectoryListingRequest::Directory(request) => {
                let page = self
                    .directory_children_page(session, resolved, request, attributes, access)
                    .await?;
                Ok(Page {
                    items: page.items,
                    next_cursor: page.next_cursor.map(ListPageCursor::Directory),
                })
            }
            DirectoryListingRequest::DirectoryTree(request) => {
                let page = self
                    .directory_tree_page(session, resolved, request, attributes, access)
                    .await?;
                Ok(Page {
                    items: page.items,
                    next_cursor: page.next_cursor.map(ListPageCursor::DirectoryTree),
                })
            }
        }
    }

    /// Scans one page of `resolved`'s visible children and builds their
    /// entries. The caller has established that `resolved` is a directory
    /// and validated any cursor against it.
    async fn directory_children_page(
        &self,
        session: &mut MetadataViewSession<'_, '_, S>,
        resolved: &ResolvedVisiblePath,
        request: PageRequest<DirectoryPageCursor>,
        attributes: AttributeInclusion,
        access: &ReadAccess<'_, S>,
    ) -> Result<Page<PathEntry, DirectoryPageCursor>> {
        let start_after = request.cursor.as_ref().map(|cursor| &cursor.last_name_key);
        let mut children = self
            .select_children(
                session,
                resolved.inode_id,
                start_after,
                request.limit.as_usize(),
            )
            .await?;
        let next_cursor = request
            .limit
            .finish_page(&mut children, |last| DirectoryPageCursor {
                head_seq: self.head.seq,
                pin_id: None,
                directory_inode_id: resolved.inode_id,
                last_name_key: last.binding.name_key.clone(),
            });
        let children = self
            .readable_children(session, children, access)
            .await?
            .into_iter()
            .map(|child| ListedChild::new(resolved, child))
            .collect();
        let entries = self
            .build_listed_entries(session, children, attributes)
            .await?;

        Ok(Page {
            items: entries,
            next_cursor,
        })
    }

    /// Scans a directory's visible children after `start_after`, in name-key
    /// order, up to `limit` and one more, so a full scan shows whether more
    /// children follow.
    pub(super) async fn select_children(
        &self,
        session: &mut MetadataViewSession<'_, '_, S>,
        directory_inode_id: InodeId,
        start_after: Option<&NameKey>,
        limit: usize,
    ) -> Result<Vec<VisibleChildEntry>> {
        let select_span = crate::phase_span!(
            Phase::ListPageSelectChildren,
            list_page_requested_limit = limit as u64,
            list_page_children_returned = tracing::field::Empty,
        );
        let children = session
            .visible_children_page_by_name_key(
                directory_inode_id,
                start_after,
                limit.saturating_add(1),
            )
            .instrument(select_span.clone())
            .await?;
        select_span.record("list_page_children_returned", children.len() as u64);
        Ok(children)
    }

    /// Pairs each scanned child with whether the subject can read it.
    pub(super) async fn readable_children(
        &self,
        session: &mut MetadataViewSession<'_, '_, S>,
        children: Vec<VisibleChildEntry>,
        access: &ReadAccess<'_, S>,
    ) -> Result<Vec<ScannedChild>> {
        let mut readable = vec![true; children.len()];
        async {
            if !access.is_unrestricted() {
                session.preload_access_rows(&children).await?;
                for (child, readable) in children.iter().zip(&mut readable) {
                    *readable = access
                        .can_read(session, child.binding.child_inode_id)
                        .await?;
                }
            }
            Ok::<_, CoreError>(())
        }
        .instrument(crate::phase_span!(Phase::ChildAccess))
        .await?;
        Ok(children
            .into_iter()
            .zip(readable)
            .map(|(child, readable)| ScannedChild { child, readable })
            .collect())
    }

    /// Builds the entries of listed children, and includes the attributes of
    /// the readable ones when `attributes` asks for them.
    pub(super) async fn build_listed_entries(
        &self,
        session: &mut MetadataViewSession<'_, '_, S>,
        children: Vec<ListedChild>,
        attributes: AttributeInclusion,
    ) -> Result<Vec<PathEntry>> {
        let build_span = crate::phase_span!(
            Phase::ListPageBuildEntries,
            list_page_children_returned = children.len() as u64,
            { METADATA_VIEW_SESSION_COUNTER_FIELDS[0].0 } = tracing::field::Empty,
            { METADATA_VIEW_SESSION_COUNTER_FIELDS[1].0 } = tracing::field::Empty,
            { METADATA_VIEW_SESSION_COUNTER_FIELDS[2].0 } = tracing::field::Empty,
            { METADATA_VIEW_SESSION_COUNTER_FIELDS[3].0 } = tracing::field::Empty,
            { METADATA_VIEW_SESSION_COUNTER_FIELDS[4].0 } = tracing::field::Empty,
            { METADATA_VIEW_SESSION_COUNTER_FIELDS[5].0 } = tracing::field::Empty,
            { METADATA_VIEW_SESSION_COUNTER_FIELDS[6].0 } = tracing::field::Empty,
            { METADATA_VIEW_SESSION_COUNTER_FIELDS[7].0 } = tracing::field::Empty,
            { METADATA_VIEW_SESSION_COUNTER_FIELDS[8].0 } = tracing::field::Empty,
            { METADATA_VIEW_SESSION_COUNTER_FIELDS[9].0 } = tracing::field::Empty,
            { METADATA_VIEW_SESSION_COUNTER_FIELDS[10].0 } = tracing::field::Empty,
        );
        let entries = async {
            let file_inode_ids: Vec<InodeId> = children
                .iter()
                .filter(|child| child.resolved.inode_kind == InodeKind::File)
                .map(|child| child.resolved.inode_id)
                .collect();
            session.preload_revision_heads(&file_inode_ids).await?;
            if attributes == AttributeInclusion::Include {
                let readable_inode_ids: Vec<InodeId> = children
                    .iter()
                    .filter_map(|child| child.readable.then_some(child.resolved.inode_id))
                    .collect();
                session.preload_attributes(&readable_inode_ids).await?;
            }
            let mut entries = Vec::with_capacity(children.len());
            for child in children {
                let attributes = if child.readable {
                    attributes
                } else {
                    AttributeInclusion::Omit
                };
                entries.push(
                    self.build_authoritative_path_entry_with_session(
                        session,
                        &child.resolved,
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
        Ok(entries)
    }

    /// `resolved` must come from visible resolution or enumeration at this
    /// session's seq (both callers do), so the revision and attribute lookups
    /// do not re-derive the inode's visibility.
    ///
    /// An included attribute projection includes the map and its revision even
    /// when the map is empty; an omitted projection includes neither.
    pub(crate) async fn build_authoritative_path_entry_with_session(
        &self,
        session: &mut MetadataViewSession<'_, '_, S>,
        resolved: &ResolvedVisiblePath,
        attributes: AttributeInclusion,
    ) -> Result<PathEntry> {
        let kind = match resolved.inode_kind {
            InodeKind::Directory => PathEntryKind::Directory {},
            InodeKind::File => {
                let revision = session
                    .latest_revision_head_of_visible(resolved.inode_id)
                    .await?
                    .ok_or_else(|| CoreError::PathNotFound(resolved.absolute_path.to_string()))?;
                PathEntryKind::File {
                    revision_no: revision.revision_no,
                    size_bytes: revision.content_ref.size_bytes,
                    content_ref: revision.content_ref,
                    revision_committed_by: revision.committed_by,
                    revision_committed_at_ms: revision.committed_at_ms,
                }
            }
        };
        // Attributes belong to the inode, so a directory returns attributes as readily
        // as a file; only the projection determines whether the read happens.
        let attributes = match attributes {
            AttributeInclusion::Include => {
                Some(session.attributes_of_visible(resolved.inode_id).await?)
            }
            AttributeInclusion::Omit => None,
        };
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
        let binding_version = resolved
            .binding_version
            .map(|position| crate::binding_version::encode(position, &self.head.namespace_id));
        Ok(PathEntry {
            namespace_id: self.head.namespace_id.clone(),
            path: resolved.absolute_path.clone(),
            inode_id: resolved.inode_id,
            created_by: resolved.created_by.clone(),
            created_at_ms: resolved.created_at_ms,
            kind,
            head_seq: self.head.seq,
            parent_inode_id: resolved.parent_inode_id,
            display_name,
            binding_version,
            attributes,
        })
    }
}

/// A scanned child of one directory, and whether the subject can read it.
pub(super) struct ScannedChild {
    pub(super) child: VisibleChildEntry,
    pub(super) readable: bool,
}

/// A child that a listing page returns, resolved to its path, and whether
/// the subject can read the child.
pub(super) struct ListedChild {
    pub(super) resolved: ResolvedVisiblePath,
    pub(super) readable: bool,
}

impl ListedChild {
    /// Resolves a scanned child of `directory` to its path.
    pub(super) fn new(directory: &ResolvedVisiblePath, scanned: ScannedChild) -> Self {
        Self {
            resolved: resolve_visible_child(directory, &scanned.child.binding),
            readable: scanned.readable,
        }
    }
}

/// Resolves the path of `binding`'s child, a visible child of `directory`.
pub(super) fn resolve_visible_child(
    directory: &ResolvedVisiblePath,
    binding: &DirentryBindingRecord,
) -> ResolvedVisiblePath {
    let display_name = binding
        .display_name()
        .expect("visible binding should be bound");
    ResolvedVisiblePath {
        absolute_path: directory.absolute_path.join(display_name),
        inode_id: binding.child_inode_id,
        inode_kind: binding.child_kind,
        created_by: binding.child_created_by.clone(),
        created_at_ms: binding.child_created_at_ms,
        parent_inode_id: Some(binding.parent_inode_id),
        display_name: display_name.to_string(),
        binding_version: Some(binding.position()),
    }
}
