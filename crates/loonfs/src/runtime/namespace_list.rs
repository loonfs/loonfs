//! The namespace listing: one page of namespace IDs from the store, and the
//! metadata of each namespace that is live.

use crate::pagers::NamespacesPager;
use crate::trace::{self, operation_span};
use crate::{
    Error, ErrorCode, ListNamespacesResponse, LiveReadOptions, LoonFs, Mode, ReadOnly, Result,
};
use futures::{StreamExt as _, TryStreamExt as _};
use loonfs_core::CoreError;
use loonfs_objectstore::layout::{list_namespace_ids, NamespacePageCursor, NAMESPACES_PREFIX};
use loonfs_types::{EffectiveLimit, NamespaceListPageCursor, PageRequest};

/// How many namespaces' metadata one page reads at once.
///
/// Each read takes a read permit from the runtime's execution budget as
/// well, so one page can't take every permit.
const MAX_CONCURRENT_METADATA_READS: usize = 16;

impl<M: Mode> LoonFs<M> {
    /// Lists the store's namespaces in ascending namespace ID order, with
    /// each namespace's metadata as [`crate::Namespace::metadata`] returns it.
    ///
    /// A deleted or retired namespace, and a namespace whose creation hasn't
    /// finished, are left out, so a page can have fewer namespaces than its
    /// limit, even none, and still have a next cursor. Each page lists one
    /// page of namespace IDs from the store and reads each listed namespace's
    /// metadata, [`MAX_CONCURRENT_METADATA_READS`] at a time. Without a page
    /// limit, a page lists [`DEFAULT_NAMESPACE_PAGE_LIMIT`] IDs.
    ///
    /// [`DEFAULT_NAMESPACE_PAGE_LIMIT`]: crate::DEFAULT_NAMESPACE_PAGE_LIMIT
    pub fn list_namespaces(&self) -> NamespacesPager {
        loonfs_types::Pager::with_state(self.read_only(), |runtime, request| async move {
            runtime
                .namespaces_page(crate::pagers::decode_page_request(request)?)
                .await
        })
        .page_limit(EffectiveLimit::NAMESPACE_DEFAULT)
    }
}

impl LoonFs<ReadOnly> {
    async fn namespaces_page(
        &self,
        request: PageRequest<NamespaceListPageCursor>,
    ) -> Result<ListNamespacesResponse> {
        trace::in_operation(operation_span!(self.core, "list_namespaces"), async {
            let mut start_after = request
                .cursor
                .as_ref()
                .map(|cursor| NamespacePageCursor::after(&cursor.last_namespace_id));
            let page = loop {
                let page =
                    list_namespace_ids(&*self.object_store(), start_after.as_ref(), request.limit)
                        .await
                        .map_err(|error| {
                            Error::from(CoreError::store(NAMESPACES_PREFIX, &error))
                        })?;
                // A page of children that aren't namespace IDs has no ID to
                // resume after, so the listing continues from the store's
                // own position instead of returning a cursor.
                if page.namespace_ids.is_empty() && page.next_cursor.is_some() {
                    start_after = page.next_cursor;
                    continue;
                }
                break page;
            };
            let next_cursor = page.next_cursor.and(page.namespace_ids.last()).map(|last| {
                NamespaceListPageCursor {
                    last_namespace_id: last.clone(),
                }
            });
            let next_cursor = crate::pagers::encode_next_cursor(next_cursor.as_ref())?;
            let options = LiveReadOptions::default();
            let namespaces = futures::stream::iter(page.namespace_ids)
                .map(|namespace_id| {
                    let options = &options;
                    async move {
                        match self
                            .namespace(&namespace_id)
                            .metadata_with_options(options)
                            .await
                        {
                            Ok(metadata) => Ok(Some(metadata)),
                            Err(error)
                                if matches!(
                                    error.code(),
                                    ErrorCode::NamespaceDeleted | ErrorCode::NamespaceNotFound
                                ) =>
                            {
                                Ok(None)
                            }
                            Err(error) => Err(error),
                        }
                    }
                })
                .buffered(MAX_CONCURRENT_METADATA_READS)
                .try_filter_map(|metadata| async move { Ok(metadata) })
                .try_collect::<Vec<_>>()
                .await?;
            Ok(ListNamespacesResponse {
                namespaces,
                next_cursor,
            })
        })
        .await
    }
}
