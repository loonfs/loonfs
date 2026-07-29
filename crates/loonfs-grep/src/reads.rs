//! Every filesystem read grep performs, pinned to one namespace snapshot.
//!
//! Grep owns an index, not a filesystem: it learns what to index from the
//! checkpointed file enumeration and the semantic change feed, and it
//! verifies query candidates against current state — resolving inodes,
//! paths, directories, and content by reference. Every one of those is a
//! supported read any consumer could make, and this module is the one place
//! grep calls them, so the surface it depends on stays countable.
//!
//! Reads of grep's own durable state (segments, manifests, the root
//! pointer, all under `namespaces/{namespace}/extensions/grep/`) do not come
//! through here: that keyspace is grep's, and it reads it directly.
//!
//! Pinning is the one thing this module does that a consumer of the runtime
//! handles would not: a detached worker has no request to borrow a snapshot
//! from, so [`NamespaceReads::pin`] takes its own read anchor.

use crate::{GrepError, Result};
use loonfs_api::v0::{ChangesResponse, FilesystemChange};
use loonfs_api::{
    AbsolutePath, AuthoritativePathEntry, ChangeSeq, CheckpointId, ContentRef, DirectoryPageCursor,
    EffectiveLimit, InodeId, NamespaceId, Page, PageRequest, RevisionNo, DEFAULT_MAX_PAGE_LIMIT,
};
use loonfs_core::cache::{
    MetadataTableCache, MetadataTableCacheConfig, WalTailProjectionCache,
    WalTailProjectionCacheConfig, DEFAULT_WAL_TAIL_PROJECTION_DECODED_BYTES,
    DEFAULT_WAL_TAIL_PROJECTION_ROWS,
};
use loonfs_core::control::load_namespace_read_anchor;
use loonfs_core::{
    CheckpointFilesPage, CheckpointFilesPageCursor, CurrentFileState, Error as CoreError,
    MetadataProjectionLoadError, NamespaceEngine, RuntimeReadContext, MAX_RESOLVE_CURRENT_FILES,
};
use loonfs_objectstore::ObjectStore;
use std::num::NonZeroU32;
use std::sync::Arc;

/// Snapshots one grep worker keeps in its own read caches. A worker step
/// pins one snapshot and never revisits an earlier one, so a handful of
/// entries covers a step with room for the manifest it is walking.
const WORKER_CACHED_WAL_TAIL_PROJECTIONS: usize = 4;

/// One namespace's reads, all answered from one pinned snapshot.
///
/// The snapshot is the coherence guarantee: a page of checkpoint files, a
/// batch of resolved inodes, and the content read that follows them observe
/// the same namespace state, so a commit landing mid-query cannot make a
/// page contradict itself.
pub struct NamespaceReads<'a, S: ObjectStore> {
    engine: &'a NamespaceEngine<S>,
    context: RuntimeReadContext,
}

impl<'a, S: ObjectStore> NamespaceReads<'a, S> {
    /// Adopts a snapshot the caller already pinned, sharing its read caches.
    pub fn pinned(engine: &'a NamespaceEngine<S>, context: RuntimeReadContext) -> Self {
        Self { engine, context }
    }

    /// Pins a fresh snapshot for one bounded unit of grep work.
    ///
    /// A worker running outside a serving runtime has no pinned request to
    /// borrow, so it takes its own anchor and its own caches; the caches
    /// live exactly as long as the step that pinned them.
    pub async fn pin(store: &S, engine: &'a NamespaceEngine<S>) -> Result<Self> {
        let (head, basis) = load_namespace_read_anchor(store, engine.namespace_id())
            .await
            .map_err(|error| {
                GrepError::Core(CoreError::MetadataProjection(
                    MetadataProjectionLoadError::LoadHead(error),
                ))
            })?;
        Ok(Self {
            engine,
            context: RuntimeReadContext {
                head: head.state,
                head_etag: head.identity.etag,
                basis,
                table_cache: Arc::new(MetadataTableCache::new(MetadataTableCacheConfig::default())),
                tail_cache: Arc::new(WalTailProjectionCache::new(WalTailProjectionCacheConfig {
                    max_entries: WORKER_CACHED_WAL_TAIL_PROJECTIONS,
                    max_rows: DEFAULT_WAL_TAIL_PROJECTION_ROWS,
                    max_decoded_bytes: DEFAULT_WAL_TAIL_PROJECTION_DECODED_BYTES,
                })),
            },
        })
    }

    /// The namespace every read here answers for.
    pub fn namespace_id(&self) -> &NamespaceId {
        self.engine.namespace_id()
    }

    /// The sequence this snapshot is pinned to.
    pub fn head_seq(&self) -> ChangeSeq {
        self.context.head.seq
    }

    /// Reads one page of the files a checkpoint pins, in ascending inode-id
    /// order.
    ///
    /// A checkpoint that no longer pins its basis — released, expired, or
    /// reaped — answers `checkpoint_unavailable` instead of falling back to
    /// current state, which is what turns a lost backfill basis into an
    /// explicit rebootstrap.
    pub async fn list_checkpoint_files_page(
        &self,
        checkpoint_id: &CheckpointId,
        cursor: Option<CheckpointFilesPageCursor>,
        limit: usize,
    ) -> Result<CheckpointFilesPage> {
        Ok(self
            .engine
            .list_checkpoint_files_page(
                checkpoint_id,
                PageRequest {
                    limit: page_limit(limit),
                    cursor,
                },
                &self.context,
            )
            .await?)
    }

    /// Reads committed changes after `after_seq` as semantic events.
    ///
    /// A cursor below the namespace's retention floor answers
    /// `rebootstrap_required`: the history grep would need to catch up is
    /// gone, so it rebuilds from a fresh checkpoint instead.
    pub async fn list_changes_after(
        &self,
        after_seq: ChangeSeq,
        limit: usize,
    ) -> Result<ChangesResponse> {
        Ok(self
            .engine
            .list_changes_after(after_seq, page_limit(limit))
            .await?)
    }

    /// Answers what each inode looks like now: visible, its current
    /// revision, and its current path, in input order.
    ///
    /// At most [`MAX_RESOLVE_CURRENT_FILES`] ids per call; callers chunk
    /// with [`resolve_batch_size`].
    pub async fn resolve_current_files(
        &self,
        inode_ids: &[InodeId],
    ) -> Result<Vec<CurrentFileState>> {
        Ok(self
            .engine
            .resolve_current_files(inode_ids, &self.context)
            .await?)
    }

    /// Resolves one path to its authoritative entry at the pinned head.
    pub async fn resolve_path(
        &self,
        absolute_path: &AbsolutePath,
    ) -> Result<AuthoritativePathEntry> {
        Ok(self
            .engine
            .resolve_path(absolute_path.as_str(), &self.context)
            .await?)
    }

    /// Lists one page of a directory at the pinned head.
    pub async fn list_path_page(
        &self,
        absolute_path: &AbsolutePath,
        cursor: Option<DirectoryPageCursor>,
        limit: usize,
    ) -> Result<Page<AuthoritativePathEntry, DirectoryPageCursor>> {
        Ok(self
            .engine
            .list_path_page(
                absolute_path.as_str(),
                PageRequest {
                    limit: page_limit(limit),
                    cursor,
                },
                &self.context,
            )
            .await?)
    }

    /// Reads one immutable content object by reference, under grep's own
    /// byte budget rather than any deployment download limit.
    pub async fn read_content_ref(
        &self,
        content_ref: &ContentRef,
        max_bytes: u64,
    ) -> Result<Vec<u8>> {
        Ok(self
            .engine
            .read_content_ref(content_ref, max_bytes, &self.context)
            .await?)
    }
}

/// Ids one [`NamespaceReads::resolve_current_files`] call answers.
pub(crate) fn resolve_batch_size(wanted: usize) -> usize {
    wanted.clamp(1, MAX_RESOLVE_CURRENT_FILES)
}

/// The revision one change event published.
pub(crate) struct PublishedRevision<'a> {
    pub(crate) inode_id: InodeId,
    pub(crate) revision_no: RevisionNo,
    pub(crate) content_ref: &'a ContentRef,
}

/// The revision a change event published, or `None` when the event changed
/// no file content.
///
/// `Moved`, `Deleted`, and `Undeleted` are deliberately nothing to index:
/// the index is keyed by durable `(inode_id, revision_no)`, and every query
/// verifies its candidates against current state before emitting a match.
/// So a moved file is found at its new path, a deleted one stops verifying,
/// and an undeleted one verifies again — none of it touches a posting.
/// Re-indexing them would rewrite postings that are already correct.
pub(crate) fn published_revision(event: &FilesystemChange) -> Option<PublishedRevision<'_>> {
    match event {
        FilesystemChange::Created {
            inode_id,
            revision_no: Some(revision_no),
            content_ref: Some(content_ref),
            ..
        } => Some(PublishedRevision {
            inode_id: *inode_id,
            revision_no: *revision_no,
            content_ref,
        }),
        FilesystemChange::ContentChanged {
            inode_id,
            revision_no,
            content_ref,
        } => Some(PublishedRevision {
            inode_id: *inode_id,
            revision_no: *revision_no,
            content_ref,
        }),
        // A created directory publishes no content.
        FilesystemChange::Created { .. }
        | FilesystemChange::Moved { .. }
        | FilesystemChange::Deleted { .. }
        | FilesystemChange::Undeleted { .. } => None,
    }
}

/// Clamps a caller's page size to what a page request can carry.
fn page_limit(limit: usize) -> EffectiveLimit {
    let limit = u32::try_from(limit)
        .unwrap_or(DEFAULT_MAX_PAGE_LIMIT)
        .clamp(1, DEFAULT_MAX_PAGE_LIMIT);
    EffectiveLimit::new(NonZeroU32::new(limit).expect("a clamped page limit is non-zero"))
}

#[cfg(test)]
mod tests {
    use super::{page_limit, resolve_batch_size};
    use loonfs_api::DEFAULT_MAX_PAGE_LIMIT;
    use loonfs_core::MAX_RESOLVE_CURRENT_FILES;

    #[test]
    fn page_limits_clamp_into_the_pagination_policy() {
        assert_eq!(page_limit(0).get(), 1);
        assert_eq!(page_limit(7).get(), 7);
        assert_eq!(page_limit(usize::MAX).get(), DEFAULT_MAX_PAGE_LIMIT);
    }

    #[test]
    fn resolve_batches_stay_within_the_core_batch_cap() {
        assert_eq!(resolve_batch_size(0), 1);
        assert_eq!(resolve_batch_size(9), 9);
        assert_eq!(
            resolve_batch_size(MAX_RESOLVE_CURRENT_FILES * 2),
            MAX_RESOLVE_CURRENT_FILES
        );
    }
}
