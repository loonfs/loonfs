//! Shared query-parameter checks for HTTP handlers.

use super::error::ApiResponseError;
use loonfs::{Consistency, ErrorCode, ReadAt, SnapshotId};
use loonfs_types::LimitError;

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

/// Returns what a read reads at, from its `snapshot_id` and its decoded
/// `consistency`, whether the request sets them as query parameters or as
/// body fields. A live read that sets no consistency is `eventual`. A read
/// with a `snapshot_id` has no consistency, so a request can't set
/// `consistency` with one.
pub(super) fn read_at(
    snapshot_id: Option<SnapshotId>,
    consistency: Option<Consistency>,
) -> Result<ReadAt, ApiResponseError> {
    match (snapshot_id, consistency) {
        (None, consistency) => Ok(ReadAt::Live(consistency.unwrap_or_default())),
        (Some(snapshot_id), None) => Ok(ReadAt::Snapshot(snapshot_id)),
        (Some(_), Some(_)) => Err(ApiResponseError::new(
            ErrorCode::InvalidRequest,
            "consistency cannot be combined with snapshot_id, because a snapshot read has no \
             consistency",
        )
        .with_param("consistency")),
    }
}

pub(super) fn effective_page_limit(
    limit: Option<u32>,
) -> Result<loonfs_types::EffectiveLimit, ApiResponseError> {
    loonfs_types::EffectiveLimit::from_requested(limit).map_err(limit_response_error)
}

fn limit_response_error(error: LimitError) -> ApiResponseError {
    ApiResponseError::new(ErrorCode::InvalidRequest, &error.to_string()).with_param("limit")
}
