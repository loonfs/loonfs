//! Shared query-parameter and path-id parsing for HTTP handlers.

use super::error::ApiResponseError;
use loonfs::engine::READ_REVALIDATION_BOUND_MS;
use loonfs::{Consistency, ErrorCode, ListOptions, PinId, ReadAt, ReadConsistency};
use loonfs_types::{
    decode_cursor, GeneratedIdValidationError, LimitError, ListPageCursor, OpaqueTokenError,
    PaginationPolicy, PublicOrdinalRangeError, RevisionNo,
};
use std::str::FromStr;

pub(super) fn required_query_param(
    value: Option<String>,
    name: &str,
) -> Result<String, ApiResponseError> {
    value.ok_or_else(|| {
        ApiResponseError::new(
            ErrorCode::InvalidRequest,
            &format!("missing required query parameter `{name}`"),
        )
        .with_param(name)
    })
}

pub(super) fn parse_include_attributes(
    value: &str,
) -> Result<loonfs_types::AttributeInclusion, ApiResponseError> {
    parse_boolean_query_param(value, "include_attributes").map(|include| match include {
        true => loonfs_types::AttributeInclusion::Include,
        false => loonfs_types::AttributeInclusion::Omit,
    })
}

pub(super) fn parse_boolean_query_param(value: &str, name: &str) -> Result<bool, ApiResponseError> {
    match value {
        "true" => Ok(true),
        "false" => Ok(false),
        other => Err(ApiResponseError::new(
            ErrorCode::InvalidRequest,
            &format!("invalid {name} `{other}`: expected `true` or `false`"),
        )
        .with_param(name)),
    }
}

/// Returns what a read reads at, from its parsed `snapshot_id` and its
/// `consistency` and `max_staleness_ms` parameters. A read with a
/// `snapshot_id` has no consistency setting, so a request can't set
/// `consistency` with one.
pub(super) fn parse_read_at(
    snapshot_id: Option<PinId>,
    consistency: Option<&str>,
    max_staleness_ms: Option<&str>,
) -> Result<ReadAt, ApiResponseError> {
    let max_staleness_ms = max_staleness_ms.map(parse_max_staleness_ms).transpose()?;
    let consistency = consistency.map(parse_read_consistency).transpose()?;
    read_at(snapshot_id, consistency, max_staleness_ms)
}

/// Parses the `consistency` and `max_staleness_ms` parameters of a live
/// read, or returns `None`, the runtime's default, when the request sets
/// neither.
pub(super) fn parse_consistency(
    consistency: Option<&str>,
    max_staleness_ms: Option<&str>,
) -> Result<Option<Consistency>, ApiResponseError> {
    let max_staleness_ms = max_staleness_ms.map(parse_max_staleness_ms).transpose()?;
    let consistency = consistency.map(parse_read_consistency).transpose()?;
    live_consistency(consistency, max_staleness_ms)
}

/// Returns what a read reads at, from its `snapshot_id` and its decoded
/// `consistency` and `max_staleness_ms`, whether the request sets them as
/// query parameters or as body fields.
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

fn parse_read_consistency(value: &str) -> Result<ReadConsistency, ApiResponseError> {
    match value {
        "eventual" => Ok(ReadConsistency::Eventual),
        "strong" => Ok(ReadConsistency::Strong),
        other => Err(ApiResponseError::new(
            ErrorCode::InvalidRequest,
            &format!("invalid consistency `{other}`: expected `strong` or `eventual`"),
        )
        .with_param("consistency")),
    }
}

fn parse_max_staleness_ms(value: &str) -> Result<u64, ApiResponseError> {
    value.parse::<u64>().map_err(|_| {
        ApiResponseError::new(
            ErrorCode::InvalidRequest,
            &format!(
                "invalid max_staleness_ms `{value}`: expected an integer number of milliseconds"
            ),
        )
        .with_param("max_staleness_ms")
    })
}

pub(super) fn parse_revision_no(value: &str) -> Result<RevisionNo, ApiResponseError> {
    parse_public_ordinal("revision_no", value, RevisionNo::parse)
}

pub(super) fn parse_public_ordinal<T>(
    name: &str,
    value: &str,
    constructor: impl FnOnce(u64) -> Result<T, PublicOrdinalRangeError>,
) -> Result<T, ApiResponseError> {
    let parsed = value
        .parse::<u64>()
        .map_err(|_| public_ordinal_response_error(name, value, PublicOrdinalRangeError))?;
    constructor(parsed).map_err(|error| public_ordinal_response_error(name, value, error))
}

fn public_ordinal_response_error(
    name: &str,
    value: &str,
    error: PublicOrdinalRangeError,
) -> ApiResponseError {
    ApiResponseError::new(
        ErrorCode::InvalidRequest,
        &format!("invalid {name} `{value}`: {error}"),
    )
    .with_param(name)
}

pub(super) fn invalid_path_id_error(name: &str, value: &str, reason: &str) -> ApiResponseError {
    ApiResponseError::new(
        ErrorCode::InvalidRequest,
        &format!("invalid {name} {value:?}: {reason}"),
    )
    .with_param(name)
}

pub(super) fn parse_path_id<T>(name: &'static str, value: &str) -> Result<T, ApiResponseError>
where
    T: FromStr<Err = GeneratedIdValidationError>,
{
    value.parse().map_err(|error: GeneratedIdValidationError| {
        invalid_path_id_error(name, value, error.reason())
    })
}

pub(super) fn parse_snapshot_id(value: &str) -> Result<loonfs_types::PinId, ApiResponseError> {
    loonfs_types::PinId::parse(value)
        .map_err(|error| invalid_path_id_error("snapshot_id", value, error.reason()))
}

pub(super) fn effective_page_limit(
    limit: Option<String>,
) -> Result<loonfs_types::EffectiveLimit, ApiResponseError> {
    let requested = limit.as_deref().map(parse_page_limit).transpose()?;
    PaginationPolicy::default()
        .effective_limit(requested)
        .map_err(limit_response_error)
}

fn parse_page_limit(value: &str) -> Result<u32, ApiResponseError> {
    value.parse::<u32>().map_err(|error| {
        ApiResponseError::new(
            ErrorCode::InvalidRequest,
            &format!("invalid limit `{value}`: {error}"),
        )
        .with_param("limit")
    })
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

/// Schema-only override for the shared page-limit query contract.
#[cfg(feature = "openapi")]
pub(super) struct OpenApiPageLimit;

#[cfg(feature = "openapi")]
impl utoipa::PartialSchema for OpenApiPageLimit {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        utoipa::openapi::schema::Object::builder()
            .schema_type(utoipa::openapi::schema::Type::Integer)
            .format(Some(utoipa::openapi::SchemaFormat::KnownFormat(
                utoipa::openapi::KnownFormat::Int32,
            )))
            .minimum(Some(1u32))
            .maximum(Some(loonfs_types::DEFAULT_MAX_PAGE_LIMIT))
            .default(Some(serde_json::json!(loonfs_types::DEFAULT_PAGE_LIMIT)))
            .into()
    }
}

#[cfg(feature = "openapi")]
impl utoipa::ToSchema for OpenApiPageLimit {}

/// Schema-only override for the `consistency` query parameter.
///
/// The schema declares no default, because the default is the server's
/// `consistency` setting, and a client generator that sends declared defaults
/// would pair `consistency` with `snapshot_id`, which the server rejects.
#[cfg(feature = "openapi")]
pub(super) struct OpenApiConsistency;

#[cfg(feature = "openapi")]
impl utoipa::PartialSchema for OpenApiConsistency {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        utoipa::openapi::schema::Object::builder()
            .schema_type(utoipa::openapi::schema::Type::String)
            .enum_values(Some(["strong", "eventual"]))
            .into()
    }
}

#[cfg(feature = "openapi")]
impl utoipa::ToSchema for OpenApiConsistency {}

/// Schema-only override for the `max_staleness_ms` query parameter,
/// which can't exceed the revalidation bound.
#[cfg(feature = "openapi")]
pub(super) struct OpenApiMaxStalenessMs;

#[cfg(feature = "openapi")]
impl utoipa::PartialSchema for OpenApiMaxStalenessMs {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        utoipa::openapi::schema::Object::builder()
            .schema_type(utoipa::openapi::schema::Type::Integer)
            .format(Some(utoipa::openapi::SchemaFormat::KnownFormat(
                utoipa::openapi::KnownFormat::Int64,
            )))
            .minimum(Some(0u64))
            .maximum(Some(READ_REVALIDATION_BOUND_MS))
            .into()
    }
}

#[cfg(feature = "openapi")]
impl utoipa::ToSchema for OpenApiMaxStalenessMs {}

/// Schema-only override for an optional boolean that defaults to true.
#[cfg(feature = "openapi")]
pub(super) struct OpenApiDefaultTrueBoolean;

#[cfg(feature = "openapi")]
impl utoipa::PartialSchema for OpenApiDefaultTrueBoolean {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        utoipa::openapi::schema::Object::builder()
            .schema_type(utoipa::openapi::schema::Type::Boolean)
            .default(Some(serde_json::json!(true)))
            .into()
    }
}

#[cfg(feature = "openapi")]
impl utoipa::ToSchema for OpenApiDefaultTrueBoolean {}

/// Schema-only override for an optional boolean that defaults to false.
#[cfg(feature = "openapi")]
pub(super) struct OpenApiDefaultFalseBoolean;

#[cfg(feature = "openapi")]
impl utoipa::PartialSchema for OpenApiDefaultFalseBoolean {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        utoipa::openapi::schema::Object::builder()
            .schema_type(utoipa::openapi::schema::Type::Boolean)
            .default(Some(serde_json::json!(false)))
            .into()
    }
}

#[cfg(feature = "openapi")]
impl utoipa::ToSchema for OpenApiDefaultFalseBoolean {}
