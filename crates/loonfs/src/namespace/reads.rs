//! Path-addressed reads on a [`Namespace`] handle in any mode, the read
//! views and change feed that every read family uses, and the diff.

use crate::downloads::DirectDownloadTarget;
use crate::pagers::{
    decode_list_page_request, decode_page_request, encode_next_cursor, ChangesPager,
    PathEntriesPager, TrashPager,
};
use crate::pinned_read::PinnedRead;
use crate::read_view::{ReadAdmission, ReadSource, ReadView, SnapshotBoundCursor};
use crate::trace::{self, operation_span};
use crate::{
    CommitSeq, ContentRef, CoreError, DiffOptions, DownloadOptions, FileBytes, FileContentStream,
    ListChangesOptions, ListChangesResponse, ListDiffResponse, ListOptions,
    ListPathEntriesResponse, Namespace, PathEntry, ReadFileStreamOptions, Result, RevisionNo,
    SharedObjectStore, SnapshotId, StatOptions,
};
use loonfs_types::options::CommitSplitting;
use loonfs_types::{
    AbsolutePath, ChangeFeedPosition, EffectiveLimit, ListPageCursor, PageRequest, TrashPageCursor,
};
use std::future::Future;
use std::sync::Arc;

#[cfg(test)]
mod tests;

/// Rejects a cursor that a listing of a snapshot minted, because a listing
/// of the live head can't resume it.
pub(crate) fn reject_snapshot_cursor<C: SnapshotBoundCursor>(cursor: Option<&C>) -> Result<()> {
    let Some(snapshot_id) = cursor.and_then(SnapshotBoundCursor::snapshot_id) else {
        return Ok(());
    };
    Err(CoreError::InvalidCursor(format!(
        "{} cursor is bound to snapshot `{snapshot_id}`, so resume it from the same snapshot",
        C::NAME
    ))
    .into())
}

impl<M: crate::Mode> Namespace<M> {
    /// Runs `read` at the live head, confirmed as this handle's consistency
    /// asks. The read runs again from the newest manifest each time a newer
    /// manifest replaced the read's basis, up to `MAX_CONTENTION_ATTEMPTS`
    /// runs in all.
    pub(crate) async fn read_head<T, Read, ReadFuture>(&self, read: Read) -> Result<T>
    where
        Read: Fn(PinnedRead) -> ReadFuture,
        ReadFuture: Future<Output = Result<T>>,
    {
        self.read_head_at_least(None, read).await
    }

    /// Runs `read` as [`Self::read_head`] does. A read whose request includes
    /// `min_head_seq` reads a head at least that new when the namespace has
    /// reached it (see [`RuntimeCore::read_at_least`](crate::runtime_core::RuntimeCore::read_at_least)).
    pub(crate) async fn read_head_at_least<T, Read, ReadFuture>(
        &self,
        min_head_seq: Option<CommitSeq>,
        read: Read,
    ) -> Result<T>
    where
        Read: Fn(PinnedRead) -> ReadFuture,
        ReadFuture: Future<Output = Result<T>>,
    {
        self.core
            .read_at_least(&self.namespace_id, self.consistency, min_head_seq, read)
            .await
    }

    /// Captures the current namespace state for a group of related reads.
    ///
    /// The returned view keeps path lookup, directory listing, inode
    /// resolution, and content selection on the same head even if commits
    /// publish concurrently. It retains nothing in the store and is meant to
    /// live for one request or unit of work.
    ///
    /// The view captures the head as a read through this handle confirms it,
    /// so a view captured through [`Self::strong`] confirms the head first.
    pub async fn read_view(&self) -> Result<ReadView> {
        trace::in_operation(
            operation_span!(self.core, "read_view", self.namespace_id),
            async {
                let _permit = self.core.read_permit().await;
                let pinned = self
                    .core
                    .pinned_read(&self.namespace_id, self.consistency)
                    .await?;
                Ok(ReadView::new(
                    self.core.clone(),
                    pinned,
                    ReadSource::Head,
                    ReadAdmission::EachRead,
                ))
            },
        )
        .await
    }

    /// Runs `read`, which captures its own read views of this namespace and
    /// reads through them. A read through a view returns `ManifestReplaced`
    /// when a newer manifest replaced the view's manifest and a collection
    /// removed that manifest's segments. Each time `read` returns that error,
    /// this method drops the runtime's view of the namespace whose manifest
    /// was replaced and runs `read` again, so the next run's views load that
    /// namespace's head from the store, up to `MAX_CONTENTION_ATTEMPTS` runs
    /// in all. An ordinary read of the namespace runs again the same way. If
    /// every run returns `ManifestReplaced`, this method returns
    /// `store_contention`.
    pub async fn read_through_views<T, F, Fut>(&self, read: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        trace::in_operation(
            operation_span!(self.core, "read_through_views", self.namespace_id),
            self.core.run_again_after_replaced_manifest(read),
        )
        .await
    }

    /// Captures the namespace state an unexpired snapshot, or one of the
    /// namespace's fork snapshots, preserves. A missing snapshot or an expired snapshot
    /// returns `snapshot_not_found`.
    ///
    /// The view is for several reads of one moment: capturing it takes a
    /// read slot and returns it, and each read through it takes a slot of
    /// its own, so a caller can keep the view for as long as it needs it. To
    /// read a snapshot once, use [`Self::read_at_snapshot`], which takes one
    /// slot for the capture and the read.
    ///
    /// Reads through the view check the subject's rights at the live head,
    /// which this method confirms as a `strong` read does, whatever this
    /// handle's consistency. A view has no rerun of its own, so a caller
    /// that reads a snapshot through a view captures the view inside
    /// [`Self::read_through_views`] to run again when a newer manifest
    /// replaced the live head's manifest.
    pub async fn read_view_at_snapshot(&self, snapshot_id: &SnapshotId) -> Result<ReadView> {
        let span = operation_span!(
            self.core,
            "read_view_at_snapshot",
            self.namespace_id,
            snapshot_id = %snapshot_id
        );
        trace::in_operation(span, async {
            let _permit = self.core.read_permit().await;
            let pinned = self
                .core
                .pinned_read_at_snapshot(&self.namespace_id, snapshot_id)
                .await?;
            Ok(ReadView::new(
                self.core.clone(),
                pinned,
                ReadSource::Snapshot(snapshot_id.clone()),
                ReadAdmission::EachRead,
            ))
        })
        .await
    }

    /// Reads the snapshot `snapshot_id`, or one of the namespace's fork
    /// snapshots, once: captures a view of it and runs `read` on the view,
    /// under one read slot for the whole call.
    ///
    /// The slot is taken before the capture and returned when the call
    /// ends, so a read that waits for the budget waits once, before it
    /// captures anything. Each time `read` returns
    /// `ManifestReplaced`, because a newer manifest replaced the live head's
    /// manifest that the read checks rights at and a collection removed its
    /// segments, the call captures the view again and runs `read` again,
    /// under the same slot, as [`Self::read_through_views`] does. A missing
    /// or expired snapshot returns `snapshot_not_found`.
    ///
    /// Reads through the view take no slot of their own. A read through this
    /// handle inside `read` waits for a slot of its own while the call keeps
    /// one, so `read` reads only through the view. A copy of the view that
    /// `read` keeps, or returns, keeps the slot taken until the last copy is
    /// dropped. Use [`Self::read_view_at_snapshot`] for a view that serves
    /// several reads over time.
    pub async fn read_at_snapshot<T, F, Fut>(&self, snapshot_id: &SnapshotId, read: F) -> Result<T>
    where
        F: Fn(ReadView) -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let span = operation_span!(
            self.core,
            "read_at_snapshot",
            self.namespace_id,
            snapshot_id = %snapshot_id
        );
        trace::in_operation(span, async {
            let permit = Arc::new(self.core.owned_read_permit().await);
            // Boxed for the same reason as `RuntimeCore::read_at_least`:
            // without the box, a crate that awaits a read fails to compile
            // with "queries overflow the depth limit!".
            Box::pin(self.core.run_again_after_replaced_manifest(|| async {
                let pinned = self
                    .core
                    .pinned_read_at_snapshot(&self.namespace_id, snapshot_id)
                    .await?;
                read(ReadView::new(
                    self.core.clone(),
                    pinned,
                    ReadSource::Snapshot(snapshot_id.clone()),
                    ReadAdmission::Shared {
                        _slot: Arc::clone(&permit),
                    },
                ))
                .await
            }))
            .await
        })
        .await
    }

    /// Returns this namespace's metadata: its access mode, creation, fork
    /// basis, current head, and retention floor, from a discovery of the
    /// namespace in the store.
    ///
    /// The discovery reads the manifest boundary and the newest manifest, so
    /// it sees a deletion by another process at once. A cached head, even
    /// one confirmed as a `strong` read confirms it, sees a new manifest only
    /// at its manifest check, which runs at most once per
    /// `manifest_revalidation_interval_ms`. A host checks that a namespace
    /// exists with this read before it opens a writer session. Use
    /// [`Self::head_metadata`] to read through the runtime's cached head at
    /// this handle's consistency.
    pub async fn metadata(&self) -> Result<crate::NamespaceMetadata> {
        trace::in_operation(
            operation_span!(self.core, "metadata", self.namespace_id),
            async {
                let _permit = self.core.read_permit().await;
                Ok(self
                    .core
                    .namespace_store(&self.namespace_id)
                    .load_metadata()
                    .await?)
            },
        )
        .await
    }

    /// Returns this namespace's metadata, as [`Self::metadata`] does, at the
    /// head that a read through this handle confirms.
    pub async fn head_metadata(&self) -> Result<crate::NamespaceMetadata> {
        trace::in_operation(
            operation_span!(self.core, "metadata", self.namespace_id),
            async {
                let _permit = self.core.read_permit().await;
                self.read_head(|pinned| async move {
                    Ok(pinned.engine.namespace_metadata(&pinned.context).await?)
                })
                .await
            },
        )
        .await
    }

    /// Resolves an absolute path to its authoritative entry at the current
    /// head.
    pub async fn stat(&self, absolute_path: &str) -> Result<PathEntry> {
        self.stat_with_options(absolute_path, &StatOptions::default())
            .await
    }

    /// Resolves an absolute path to its authoritative entry at the current
    /// head, projecting what `options` asks for.
    pub async fn stat_with_options(
        &self,
        absolute_path: &str,
        options: &StatOptions,
    ) -> Result<PathEntry> {
        trace::in_operation(
            operation_span!(self.core, "stat", self.namespace_id),
            async {
                let _permit = self.core.read_permit().await;
                self.read_head(|pinned| async move {
                    Ok(pinned
                        .engine
                        .resolve_path(absolute_path, options.clone(), &pinned.context)
                        .await?)
                })
                .await
            },
        )
        .await
    }

    /// Lists a directory. Each page reads the head that is current when the
    /// page is fetched.
    pub fn list(&self, absolute_path: &str) -> PathEntriesPager {
        self.list_with_options(absolute_path, &ListOptions::default())
    }

    /// Lists a directory, projecting what `options` asks for.
    ///
    /// Asking for attributes costs one lookup per entry and adds an unbounded
    /// number of bytes to each page, so a caller that turns the projection on
    /// should also size its pages for the maps it expects back.
    pub fn list_with_options(
        &self,
        absolute_path: &str,
        options: &ListOptions,
    ) -> PathEntriesPager {
        loonfs_types::Pager::with_state(
            (self.read_only(), absolute_path.to_owned(), options.clone()),
            |(reader, absolute_path, options), request| async move {
                let request = decode_list_page_request(request, &options)?;
                reader
                    .path_entries_page(&absolute_path, request, options)
                    .await
            },
        )
    }

    async fn path_entries_page(
        &self,
        absolute_path: &str,
        request: PageRequest<ListPageCursor>,
        options: ListOptions,
    ) -> Result<ListPathEntriesResponse> {
        trace::in_operation(
            operation_span!(self.core, "list", self.namespace_id),
            async {
                let _permit = self.core.read_permit().await;
                reject_snapshot_cursor(request.cursor.as_ref())?;
                let listed_path = AbsolutePath::parse(absolute_path).map_err(CoreError::from)?;
                let cursor_head_seq = request.cursor.as_ref().map(ListPageCursor::head_seq);
                self.read_head_at_least(cursor_head_seq, |pinned| {
                    let request = request.clone();
                    let options = options.clone();
                    let listed_path = listed_path.clone();
                    async move {
                        // Give awakened validation waiters a turn before
                        // synchronous page work. The head is already
                        // captured and the validation guard has been
                        // released.
                        tokio::task::yield_now().await;
                        pinned
                            .path_entries_page(listed_path, request, options, None)
                            .await
                    }
                })
                .await
            },
        )
        .await
    }

    /// Reads a file's current content plus the metadata entry it came from.
    pub async fn read_file(&self, absolute_path: &str) -> Result<FileBytes> {
        trace::in_operation(
            operation_span!(self.core, "read_file", self.namespace_id),
            async {
                let _permit = self.core.read_permit().await;
                let max_bytes = self.core.max_read_content_bytes();
                self.read_head(|pinned| async move {
                    Ok(pinned
                        .engine
                        .read_file(absolute_path, &pinned.context, Some(max_bytes))
                        .await?)
                })
                .await
            },
        )
        .await
    }

    /// Reads a file's current content as bounded chunks instead of one buffer.
    ///
    /// See [`Self::read_file_stream_with_options`] for how the stream is
    /// verified.
    pub async fn read_file_stream(
        &self,
        absolute_path: &str,
    ) -> Result<FileContentStream<SharedObjectStore>> {
        self.read_file_stream_with_options(absolute_path, &ReadFileStreamOptions::default())
            .await
    }

    /// Reads a file as bounded chunks, as `options` asks.
    ///
    /// The stream reads one chunk at a time, so it uses bounded memory. Size
    /// and checksum verification complete when [`FileContentStream::next_chunk`]
    /// returns `None`. Stopping early leaves the content unverified. The
    /// buffered-read size limit does not apply.
    ///
    /// [`ReadFileStreamOptions::range`] can resume a read, in which case the
    /// caller must supply earlier bytes for whole-object verification through
    /// [`FileContentStream::fold_resumed_prefix`]. It can also select one
    /// byte range of the file, which the stream returns without verification.
    pub async fn read_file_stream_with_options(
        &self,
        absolute_path: &str,
        options: &ReadFileStreamOptions,
    ) -> Result<FileContentStream<SharedObjectStore>> {
        trace::in_operation(
            operation_span!(self.core, "read_file_stream", self.namespace_id),
            self.file_stream(absolute_path, None, options),
        )
        .await
    }

    pub(crate) async fn file_stream(
        &self,
        absolute_path: &str,
        revision_no: Option<RevisionNo>,
        options: &ReadFileStreamOptions,
    ) -> Result<FileContentStream<SharedObjectStore>> {
        let _permit = self.core.read_permit().await;
        self.read_head(|pinned| async move {
            Ok(pinned
                .engine
                .read_file_stream(
                    absolute_path,
                    &pinned.context,
                    revision_no,
                    options.chunk_bytes,
                    options.range,
                )
                .await?)
        })
        .await
    }

    /// Prepares the content object of a file's current revision for a direct
    /// download.
    ///
    /// See the API specification's download transport contract. The handle's
    /// `max_read_content_bytes` does not apply to direct downloads.
    pub async fn create_download(&self, absolute_path: &str) -> Result<DirectDownloadTarget> {
        self.create_download_with_options(absolute_path, &DownloadOptions::default())
            .await
    }

    /// Prepares the content object of a file's current revision for a direct
    /// download, as `options` asks. See [`Self::create_download`].
    pub async fn create_download_with_options(
        &self,
        absolute_path: &str,
        options: &DownloadOptions,
    ) -> Result<DirectDownloadTarget> {
        trace::in_operation(
            operation_span!(self.core, "create_download", self.namespace_id),
            self.direct_download(absolute_path, None, options),
        )
        .await
    }

    pub(crate) async fn direct_download(
        &self,
        absolute_path: &str,
        revision_no: Option<RevisionNo>,
        options: &DownloadOptions,
    ) -> Result<DirectDownloadTarget> {
        let _permit = self.core.read_permit().await;
        self.read_head(|pinned| async move {
            Ok(pinned
                .engine
                .direct_download_target(
                    absolute_path,
                    revision_no,
                    options.start_offset,
                    &pinned.context,
                )
                .await?)
        })
        .await
    }

    /// Reads one immutable content object by reference.
    ///
    /// Requires namespace administrator access. Content must be published in
    /// the namespace's read view, including through a fork. Otherwise returns
    /// `path_not_found` without reading content bytes. A published reference
    /// whose object collection reclaimed also returns `path_not_found`.
    ///
    /// `max_bytes` is checked against the declared size before fetching. It is
    /// independent of the deployment's download limit so callers can apply a
    /// smaller memory budget. The read fails if the returned size or digest
    /// does not match the reference.
    pub async fn read_content(&self, content_ref: &ContentRef, max_bytes: u64) -> Result<Vec<u8>> {
        trace::in_operation(
            operation_span!(self.core, "read_content", self.namespace_id),
            async {
                let _permit = self.core.read_permit().await;
                self.read_head(|pinned| async move {
                    Ok(pinned
                        .engine
                        .read_content_ref(content_ref, max_bytes, &pinned.context)
                        .await?)
                })
                .await
            },
        )
        .await
    }

    /// Lists the namespace's trash, oldest deletion first.
    /// A deletion stays in the trash until it's restored, or until the retention
    /// floor reaches it and it expires.
    pub fn list_trash(&self) -> TrashPager {
        loonfs_types::Pager::with_state(self.read_only(), |reader, request| async move {
            reader.trash_page(decode_page_request(request)?).await
        })
    }

    async fn trash_page(
        &self,
        request: PageRequest<TrashPageCursor>,
    ) -> Result<loonfs_types::ListTrashResponse> {
        trace::in_operation(
            operation_span!(self.core, "list_trash", self.namespace_id),
            async {
                let _permit = self.core.read_permit().await;
                let cursor_head_seq = request.cursor.as_ref().map(|cursor| cursor.head_seq);
                self.read_head_at_least(cursor_head_seq, |pinned| {
                    let request = request.clone();
                    async move {
                        let page = pinned
                            .engine
                            .list_trash_page(request, &pinned.context)
                            .await?;
                        let next_cursor = encode_next_cursor(page.next_cursor.as_ref())?;
                        Ok(loonfs_types::ListTrashResponse {
                            namespace_id: self.namespace_id.clone(),
                            head_seq: pinned.head_seq(),
                            entries: page.items,
                            next_cursor,
                        })
                    }
                })
                .await
            },
        )
        .await
    }

    /// Lists the ordered change feed after `after_seq`.
    pub fn list_changes(&self, after_seq: CommitSeq) -> ChangesPager {
        self.list_changes_with_options(after_seq, &ListChangesOptions::default())
    }

    /// Lists the ordered change feed after `after_seq`, as `options` asks.
    ///
    /// An eventual page whose `after_seq` is above the head that the page
    /// would read confirms the head as a `strong` page does,
    /// because the caller learned `after_seq` from a fresher read. An
    /// `after_seq` that is still above the confirmed head returns
    /// `invalid_request`.
    pub fn list_changes_with_options(
        &self,
        after_seq: CommitSeq,
        options: &ListChangesOptions,
    ) -> ChangesPager {
        let start = ChangeFeedPosition {
            after_seq,
            event_index: options.event_index,
        };
        loonfs_types::Pager::with_state(
            (self.read_only(), start, options.commit_splitting),
            |(reader, start, splitting), request: PageRequest<ChangeFeedPosition>| async move {
                reader
                    .changes_page(request.cursor.unwrap_or(start), request.limit, splitting)
                    .await
            },
        )
    }

    async fn changes_page(
        &self,
        position: ChangeFeedPosition,
        limit: EffectiveLimit,
        splitting: CommitSplitting,
    ) -> Result<ListChangesResponse> {
        trace::in_operation(
            operation_span!(self.core, "list_changes", self.namespace_id),
            async {
                let _permit = self.core.read_permit().await;
                self.read_head_at_least(Some(position.after_seq), |pinned| async move {
                    pinned.changes_page(position, limit, splitting).await
                })
                .await
            },
        )
        .await
    }

    /// Returns the first page of the diff of the whole namespace from
    /// `from_seq` to the head that the page reads.
    pub async fn diff(&self, from_seq: CommitSeq) -> Result<ListDiffResponse> {
        self.diff_with_options(from_seq, &DiffOptions::default())
            .await
    }

    /// Returns one page of the diff from `from_seq`, as `options` asks
    /// ([API section 6.12](https://github.com/loonfs/loonfs/blob/main/docs/specs/api.md#612-get-diff)).
    ///
    /// The page reports each inode under `options.path` whose visible state
    /// at the diff's end differs from its state at `from_seq`. To read the
    /// next page, pass the returned `to_seq` and `next_after_seq` as
    /// `options.to_seq` and `options.after_seq`. A `from_seq` below the
    /// retention floor of the head that a page reads returns
    /// `below_retention_floor`. An eventual page whose `from_seq` or `to_seq`
    /// is above that head confirms the head as a `strong` page does, and a
    /// sequence that is still above the confirmed head returns
    /// `invalid_request`.
    pub async fn diff_with_options(
        &self,
        from_seq: CommitSeq,
        options: &DiffOptions,
    ) -> Result<ListDiffResponse> {
        trace::in_operation(
            operation_span!(self.core, "diff", self.namespace_id),
            async {
                let _permit = self.core.read_permit().await;
                let page = self
                    .read_head_at_least(
                        Some(options.to_seq.unwrap_or(from_seq)),
                        |pinned| async move {
                            Ok(pinned
                                .engine
                                .diff_page(from_seq, options, &pinned.context)
                                .await?)
                        },
                    )
                    .await?;
                self.core.instruments().diff_page(&page);
                Ok(page.response)
            },
        )
        .await
    }
}
