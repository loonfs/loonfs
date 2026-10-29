//! Path-addressed reads on a [`Namespace`] handle in any mode, and the read
//! views and change feed that every read family uses.

use crate::downloads::DirectDownloadTarget;
use crate::execution_budget::BudgetPermit;
use crate::pagers::{
    decode_list_page_request, decode_page_request, encode_next_cursor, ChangesPager,
    PathEntriesPager, TrashPager,
};
use crate::pinned_read::PinnedRead;
use crate::read_view::{ReadSource, ReadView};
use crate::trace::{self, operation_span};
use crate::{
    ChangeSeq, Consistency, ContentRef, CoreError, DownloadOptions, Error, FileBytes,
    FileContentStream, ListChangesOptions, ListChangesResponse, ListOptions,
    ListPathEntriesResponse, ListTrashOptions, Namespace, PathEntry, PinId, ReadAt,
    ReadFileOptions, ReadFileStreamOptions, Result, RevisionNo, SharedObjectStore, StatOptions,
};
use loonfs_types::{AbsolutePath, EffectiveLimit, ListPageCursor, PageRequest, TrashPageCursor};
use std::future::Future;

#[cfg(test)]
mod tests;

/// Returns the error for a read of a numbered revision at a snapshot. A
/// snapshot captures one revision of each file, so a read of a numbered
/// revision reads the live head.
fn revision_at_snapshot() -> Error {
    Error::InvalidRequest {
        message: "revision_no cannot be combined with snapshot_id".to_owned(),
        param: "snapshot_id",
    }
}

/// Rejects a numbered revision read whose options read a snapshot.
pub(crate) fn ensure_revision_read_at(at: &ReadAt, revision_no: Option<RevisionNo>) -> Result<()> {
    match (at, revision_no) {
        (ReadAt::Snapshot(_), Some(_)) => Err(revision_at_snapshot()),
        _ => Ok(()),
    }
}

pub(crate) fn reject_pinned_directory_cursor(cursor: Option<&ListPageCursor>) -> Result<()> {
    let Some(pin_id) = cursor.and_then(ListPageCursor::pin_id) else {
        return Ok(());
    };
    Err(CoreError::InvalidCursor(format!(
        "directory cursor is bound to pin `{pin_id}`, so resume it from the same snapshot"
    ))
    .into())
}

impl<M: crate::Mode> Namespace<M> {
    /// Runs a read at the state that `at` selects: `live` at the live head,
    /// confirmed as `at` asks, or `snapshot` through a read view of the
    /// snapshot that `at` selects, under the caller's `permit`.
    pub(crate) async fn read_at<'p, 'b, T, Live, LiveFuture, Snapshot, SnapshotFuture>(
        &self,
        at: &ReadAt,
        permit: &'p BudgetPermit<'b>,
        live: Live,
        snapshot: Snapshot,
    ) -> Result<T>
    where
        Live: Fn(PinnedRead) -> LiveFuture,
        LiveFuture: Future<Output = Result<T>>,
        Snapshot: FnOnce(ReadView, &'p BudgetPermit<'b>) -> SnapshotFuture,
        SnapshotFuture: Future<Output = Result<T>>,
    {
        match at {
            ReadAt::Live(consistency) => {
                self.core.read(&self.namespace_id, *consistency, live).await
            }
            ReadAt::Snapshot(snapshot_id) => {
                let view = self
                    .read_view_at_snapshot_with_permit(snapshot_id, permit)
                    .await?;
                snapshot(view, permit).await
            }
        }
    }

    /// Captures the current namespace state for a group of related reads.
    ///
    /// The returned view keeps path lookup, directory listing, inode
    /// resolution, and content selection on the same head even if commits
    /// publish concurrently. It retains nothing in the store and is meant to
    /// live for one request or unit of work.
    pub async fn read_view(&self) -> Result<ReadView> {
        trace::in_operation(
            operation_span!(self.core, "read_view", self.namespace_id),
            async {
                let _permit = self.core.read_permit().await;
                let pinned = self
                    .core
                    .pinned_read(&self.namespace_id, Consistency::Strong)
                    .await?;
                Ok(ReadView::new(self.core.clone(), pinned, ReadSource::Head))
            },
        )
        .await
    }

    /// Captures the namespace state an unexpired snapshot preserves. A
    /// missing or expired snapshot returns `snapshot_not_found`.
    pub async fn read_view_at_snapshot(&self, snapshot_id: &PinId) -> Result<ReadView> {
        let span = operation_span!(
            self.core,
            "read_view_at_snapshot",
            self.namespace_id,
            snapshot_id = %snapshot_id
        );
        trace::in_operation(span, async {
            let permit = self.core.read_permit().await;
            self.read_view_at_snapshot_with_permit(snapshot_id, &permit)
                .await
        })
        .await
    }

    async fn read_view_at_snapshot_with_permit(
        &self,
        snapshot_id: &PinId,
        _permit: &BudgetPermit<'_>,
    ) -> Result<ReadView> {
        let pinned = self
            .core
            .pinned_read_at_snapshot(&self.namespace_id, snapshot_id)
            .await?;
        Ok(ReadView::new(
            self.core.clone(),
            pinned,
            ReadSource::Snapshot(snapshot_id.clone()),
        ))
    }

    /// Returns this namespace's metadata: its access mode, creation, fork
    /// basis, current head, and retention floor.
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
                let permit = self.core.read_permit().await;
                self.read_at(
                    &options.at,
                    &permit,
                    |pinned| async move { pinned.stat(absolute_path, options).await },
                    |view, permit| async move {
                        view.stat_with_permit(absolute_path, options, permit).await
                    },
                )
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
                let permit = self.core.read_permit().await;
                if matches!(options.at, ReadAt::Live(_)) {
                    reject_pinned_directory_cursor(request.cursor.as_ref())?;
                }
                let listed_path = AbsolutePath::parse(absolute_path).map_err(CoreError::from)?;
                self.read_at(
                    &options.at,
                    &permit,
                    |pinned| {
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
                    },
                    |view, permit| {
                        let request = request.clone();
                        let options = options.clone();
                        async move {
                            view.path_entries_page(absolute_path, request, options, permit)
                                .await
                        }
                    },
                )
                .await
            },
        )
        .await
    }

    /// Reads a file's current content plus the metadata entry it came from.
    pub async fn read_file(&self, absolute_path: &str) -> Result<FileBytes> {
        self.read_file_with_options(absolute_path, &ReadFileOptions::default())
            .await
    }

    /// Reads a file's current content plus the metadata entry it came from,
    /// as `options` asks.
    pub async fn read_file_with_options(
        &self,
        absolute_path: &str,
        options: &ReadFileOptions,
    ) -> Result<FileBytes> {
        trace::in_operation(
            operation_span!(self.core, "read_file", self.namespace_id),
            async {
                let permit = self.core.read_permit().await;
                let max_bytes = self.core.max_read_content_bytes();
                self.read_at(
                    &options.at,
                    &permit,
                    |pinned| async move { pinned.read_file(absolute_path, max_bytes).await },
                    |view, permit| async move {
                        view.read_file_with_permit(absolute_path, permit).await
                    },
                )
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
        let permit = self.core.read_permit().await;
        ensure_revision_read_at(&options.at, revision_no)?;
        self.read_at(
            &options.at,
            &permit,
            |pinned| async move {
                pinned
                    .read_file_stream(absolute_path, revision_no, options)
                    .await
            },
            |view, permit| async move {
                view.read_file_stream_with_permit(absolute_path, options, permit)
                    .await
            },
        )
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
        let permit = self.core.read_permit().await;
        ensure_revision_read_at(&options.at, revision_no)?;
        self.read_at(
            &options.at,
            &permit,
            |pinned| async move {
                pinned
                    .create_download(absolute_path, revision_no, options)
                    .await
            },
            |view, permit| async move {
                view.create_download_with_permit(absolute_path, options, permit)
                    .await
            },
        )
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
                self.core
                    .read(&self.namespace_id, None, |pinned| async move {
                        pinned.read_content(content_ref, max_bytes).await
                    })
                    .await
            },
        )
        .await
    }

    /// Lists the namespace's recoverable deletions, oldest deletion first.
    /// A deletion stays listed until it's undeleted, or until the retention
    /// floor reaches it and it expires.
    pub fn list_trash(&self) -> TrashPager {
        self.list_trash_with_options(&ListTrashOptions::default())
    }

    /// Lists the namespace's recoverable deletions, oldest deletion first,
    /// as `options` asks.
    pub fn list_trash_with_options(&self, options: &ListTrashOptions) -> TrashPager {
        loonfs_types::Pager::with_state(
            (self.read_only(), options.consistency),
            |(reader, consistency), request| async move {
                reader
                    .trash_page(decode_page_request(request)?, consistency)
                    .await
            },
        )
    }

    async fn trash_page(
        &self,
        request: PageRequest<TrashPageCursor>,
        consistency: Option<Consistency>,
    ) -> Result<loonfs_types::ListTrashResponse> {
        trace::in_operation(
            operation_span!(self.core, "list_trash", self.namespace_id),
            async {
                let _permit = self.core.read_permit().await;
                self.core
                    .read(&self.namespace_id, consistency, |pinned| {
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
    pub fn list_changes(&self, after_seq: ChangeSeq) -> ChangesPager {
        self.list_changes_with_options(after_seq, &ListChangesOptions::default())
    }

    /// Lists the ordered change feed after `after_seq`, as `options` asks.
    ///
    /// A page whose `after_seq` is above the head it reads returns
    /// `invalid_request`. An eventual page can read an older head than the
    /// read that the caller learned `after_seq` from, so a caller that gets
    /// this error from an eventual page reads the page again with
    /// [`Consistency::Strong`].
    pub fn list_changes_with_options(
        &self,
        after_seq: ChangeSeq,
        options: &ListChangesOptions,
    ) -> ChangesPager {
        loonfs_types::Pager::with_state(
            (self.read_only(), after_seq, options.at.clone()),
            |(reader, after_seq, at), request: PageRequest<ChangeSeq>| async move {
                reader
                    .changes_page(request.cursor.unwrap_or(after_seq), request.limit, &at)
                    .await
            },
        )
    }

    async fn changes_page(
        &self,
        after_seq: ChangeSeq,
        limit: EffectiveLimit,
        at: &ReadAt,
    ) -> Result<ListChangesResponse> {
        trace::in_operation(
            operation_span!(self.core, "list_changes", self.namespace_id),
            async {
                let permit = self.core.read_permit().await;
                self.read_at(
                    at,
                    &permit,
                    |pinned| async move { pinned.changes_page(after_seq, limit).await },
                    |view, permit| async move { view.changes_page(after_seq, limit, permit).await },
                )
                .await
            },
        )
        .await
    }
}
