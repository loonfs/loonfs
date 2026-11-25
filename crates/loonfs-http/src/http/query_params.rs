//! Shared query-parameter checks for HTTP handlers.

use super::error::ApiResponseError;
use loonfs::{
    Consistency, CoreError, ErrorCode, ListOptions, Namespace, ReadOnly, ReadView, SnapshotId,
};
use loonfs_types::{AbsolutePath, FileRevisionsPageCursor, LimitError, ListPageCursor};
use std::future::Future;

// The `limit` query parameter declares these bounds to OpenAPI as literals,
// because the `IntoParams` derive takes only a literal bound.
const _: () = assert!(loonfs_types::MAX_PAGE_LIMIT == 1000);
const _: () = assert!(loonfs_types::DEFAULT_PAGE_LIMIT == 1000);

pub(super) fn required_query_param<T>(value: Option<T>, name: &str) -> Result<T, ApiResponseError> {
    value.ok_or_else(|| {
        ApiResponseError::new(
            ErrorCode::InvalidRequest,
            &format!("missing required query parameter `{name}`"),
        )
        .with_param(name)
    })
}

/// Returns `namespace` at a live read's decoded `consistency`, whether the
/// request sets it as a query parameter or as a body field. A request that
/// sets no consistency reads `eventual`, as a new handle does.
pub(super) fn at_consistency(
    namespace: Namespace<ReadOnly>,
    consistency: Option<Consistency>,
) -> Namespace<ReadOnly> {
    match consistency {
        Some(Consistency::Strong) => namespace.strong(),
        Some(Consistency::Eventual) | None => namespace,
    }
}

/// The state that a read request reads: the live head through a namespace
/// handle at the request's consistency, or a snapshot through a read view.
pub(super) enum ReadThrough {
    Head(Namespace<ReadOnly>),
    Snapshot(Namespace<ReadOnly>, SnapshotId),
}

/// Returns what a read reads through, from its `snapshot_id` and its decoded
/// `consistency`, whether the request sets them as query parameters or as
/// body fields. A read with a `snapshot_id` has no consistency, so a request
/// can't set `consistency` with one.
pub(super) fn read_through(
    namespace: Namespace<ReadOnly>,
    snapshot_id: Option<SnapshotId>,
    consistency: Option<Consistency>,
) -> Result<ReadThrough, ApiResponseError> {
    match (snapshot_id, consistency) {
        (None, consistency) => Ok(ReadThrough::Head(at_consistency(namespace, consistency))),
        (Some(snapshot_id), None) => Ok(ReadThrough::Snapshot(namespace, snapshot_id)),
        (Some(_), Some(_)) => Err(ApiResponseError::new(
            ErrorCode::InvalidRequest,
            "consistency cannot be combined with snapshot_id, because a snapshot read has no \
             consistency",
        )
        .with_param("consistency")),
    }
}

impl ReadThrough {
    /// Runs `head` on the namespace handle, or `snapshot` on a view of the
    /// snapshot through [`Namespace::read_at_snapshot`]. Either read takes
    /// one read slot, and runs again when a newer manifest replaced the live
    /// head's manifest.
    pub(super) async fn read<T, Head, HeadFuture, Snapshot, SnapshotFuture>(
        self,
        head: Head,
        snapshot: Snapshot,
    ) -> loonfs::Result<T>
    where
        Head: FnOnce(Namespace<ReadOnly>) -> HeadFuture,
        HeadFuture: Future<Output = loonfs::Result<T>>,
        Snapshot: Fn(ReadView) -> SnapshotFuture,
        SnapshotFuture: Future<Output = loonfs::Result<T>>,
    {
        // Boxed, because without the box, the crate fails to compile with
        // "queries overflow the depth limit!" while the compiler computes the
        // layout of a handler's future.
        match self {
            Self::Head(namespace) => Box::pin(head(namespace)).await,
            Self::Snapshot(namespace, snapshot_id) => {
                Box::pin(namespace.read_at_snapshot(&snapshot_id, snapshot)).await
            }
        }
    }
}

/// Checks the request's directory listing cursor, as a listing with
/// `options` issues it. A handler runs the checks of its request's
/// parameters before it reads, so a malformed request is refused before the
/// read looks up a snapshot or sends a store request, whatever the request
/// reads. The listing decodes the cursor again when it reads a page.
pub(super) fn check_list_cursor(cursor: Option<&str>, options: &ListOptions) -> loonfs::Result<()> {
    if let Some(cursor) = cursor {
        ListPageCursor::decode(cursor, options)
            .map_err(|error| CoreError::InvalidCursor(error.to_string()))?;
    }
    Ok(())
}

/// Checks the request's revision listing cursor, as [`check_list_cursor`]
/// checks a directory listing's.
pub(super) fn check_revisions_cursor(cursor: Option<&str>) -> loonfs::Result<()> {
    if let Some(cursor) = cursor {
        loonfs_types::decode_cursor::<FileRevisionsPageCursor>(cursor)
            .map_err(|error| CoreError::InvalidCursor(error.to_string()))?;
    }
    Ok(())
}

/// Checks that the request's `path` is an absolute path, as
/// [`check_list_cursor`] checks a cursor. The read parses the path again.
pub(super) fn check_path(path: &str) -> loonfs::Result<()> {
    AbsolutePath::parse(path).map_err(CoreError::from)?;
    Ok(())
}

pub(super) fn effective_page_limit(
    limit: Option<u32>,
) -> Result<loonfs_types::EffectiveLimit, ApiResponseError> {
    loonfs_types::EffectiveLimit::from_requested(limit).map_err(limit_response_error)
}

fn limit_response_error(error: LimitError) -> ApiResponseError {
    ApiResponseError::new(ErrorCode::InvalidRequest, &error.to_string()).with_param("limit")
}
