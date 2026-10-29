//! Shared query-parameter checks for HTTP handlers.

use super::error::ApiResponseError;
use loonfs::engine::READ_REVALIDATION_BOUND_MS;
use loonfs::{Consistency, ErrorCode, ListOptions, PinId, ReadAt, ReadConsistency};
use loonfs_types::{decode_cursor, LimitError, ListPageCursor, OpaqueTokenError};

// The `limit` and `max_staleness_ms` query parameters declare these bounds
// to OpenAPI as literals, because the `IntoParams` derive takes only a
// literal bound.
const _: () = assert!(loonfs_types::MAX_PAGE_LIMIT == 1000);
const _: () = assert!(loonfs_types::DEFAULT_PAGE_LIMIT == 1000);
const _: () = assert!(READ_REVALIDATION_BOUND_MS == 1_155_000);

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
/// `consistency` and `max_staleness_ms`, whether the request sets them as
/// query parameters or as body fields. A read with a `snapshot_id` has no
/// consistency setting, so a request can't set `consistency` with one.
pub(super) fn read_at(
    snapshot_id: Option<PinId>,
    consistency: Option<ReadConsistency>,
    max_staleness_ms: Option<u64>,
) -> Result<ReadAt, ApiResponseError> {
    let Some(snapshot_id) = snapshot_id else {
        return live_consistency(consistency, max_staleness_ms).map(ReadAt::Live);
    };
    if consistency.is_some() {
        return Err(ApiResponseError::new(
            ErrorCode::InvalidRequest,
            "consistency cannot be combined with snapshot_id, because a snapshot read has no \
             consistency setting",
        )
        .with_param("consistency"));
    }
    live_consistency(None, max_staleness_ms)?;
    Ok(ReadAt::Snapshot(snapshot_id))
}

/// Returns the consistency of a live read from its decoded `consistency`
/// and `max_staleness_ms`, or `None`, the runtime's default, when the
/// request sets neither.
pub(super) fn live_consistency(
    consistency: Option<ReadConsistency>,
    max_staleness_ms: Option<u64>,
) -> Result<Option<Consistency>, ApiResponseError> {
    if let Some(max_staleness_ms) = max_staleness_ms.filter(|ms| *ms > READ_REVALIDATION_BOUND_MS) {
        return Err(ApiResponseError::new(
            ErrorCode::InvalidRequest,
            &format!(
                "invalid max_staleness_ms `{max_staleness_ms}`: must not exceed \
                 {READ_REVALIDATION_BOUND_MS}, the revalidation bound"
            ),
        )
        .with_param("max_staleness_ms"));
    }
    match consistency {
        Some(ReadConsistency::Eventual) => Ok(Some(Consistency::Eventual { max_staleness_ms })),
        None | Some(ReadConsistency::Strong) if max_staleness_ms.is_some() => {
            Err(ApiResponseError::new(
                ErrorCode::InvalidRequest,
                "max_staleness_ms requires consistency=eventual",
            )
            .with_param("max_staleness_ms"))
        }
        None => Ok(None),
        Some(ReadConsistency::Strong) => Ok(Some(Consistency::Strong)),
    }
}

pub(super) fn invalid_path_id_error(name: &str, value: &str, reason: &str) -> ApiResponseError {
    ApiResponseError::new(
        ErrorCode::InvalidRequest,
        &format!("invalid {name} {value:?}: {reason}"),
    )
    .with_param(name)
}

pub(super) fn effective_page_limit(
    limit: Option<u32>,
) -> Result<loonfs_types::EffectiveLimit, ApiResponseError> {
    loonfs_types::EffectiveLimit::from_requested(limit).map_err(limit_response_error)
}

/// Checks that a cursor was minted by this endpoint before any read, and
/// returns it unchanged for the pager, which decodes it again.
pub(super) fn checked_cursor<C: loonfs_types::PageCursor>(
    cursor: Option<String>,
) -> Result<Option<String>, ApiResponseError> {
    if let Some(cursor) = cursor.as_deref() {
        decode_cursor::<C>(cursor).map_err(page_cursor_response_error)?;
    }
    Ok(cursor)
}

/// Checks that a directory listing's cursor is the kind that a listing with
/// `options` issues, and returns it unchanged for the pager.
pub(super) fn checked_list_cursor(
    cursor: Option<String>,
    options: &ListOptions,
) -> Result<Option<String>, ApiResponseError> {
    if let Some(cursor) = cursor.as_deref() {
        ListPageCursor::decode(cursor, options).map_err(page_cursor_response_error)?;
    }
    Ok(cursor)
}

fn limit_response_error(error: LimitError) -> ApiResponseError {
    ApiResponseError::new(ErrorCode::InvalidRequest, &error.to_string()).with_param("limit")
}

fn page_cursor_response_error(error: OpaqueTokenError) -> ApiResponseError {
    ApiResponseError::new(ErrorCode::InvalidRequest, &error.to_string()).with_param("cursor")
}
