//! Shared query-parameter and path-id parsing for HTTP handlers.

use super::error::ApiResponseError;
use loonfs::ErrorCode;
use loonfs_api::{
    decode_cursor, GeneratedIdValidationError, LimitError, PageCursorError, PaginationPolicy,
    PublicOrdinalRangeError, RevisionNo,
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
) -> Result<loonfs_api::AttributeInclusion, ApiResponseError> {
    parse_boolean_query_param(value, "include_attributes").map(|include| match include {
        true => loonfs_api::AttributeInclusion::Include,
        false => loonfs_api::AttributeInclusion::Omit,
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

pub(super) fn resolve_page_limit(
    limit: Option<String>,
) -> Result<loonfs_api::EffectiveLimit, ApiResponseError> {
    let requested = limit.as_deref().map(parse_page_limit).transpose()?;
    PaginationPolicy::default()
        .resolve_limit(requested)
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

pub(super) fn decode_optional_cursor<C: loonfs_api::PageCursor>(
    cursor: Option<String>,
) -> Result<Option<C>, ApiResponseError> {
    cursor
        .as_deref()
        .map(decode_cursor)
        .transpose()
        .map_err(page_cursor_response_error)
}

fn limit_response_error(error: LimitError) -> ApiResponseError {
    ApiResponseError::new(ErrorCode::InvalidRequest, &error.to_string()).with_param("limit")
}

fn page_cursor_response_error(error: PageCursorError) -> ApiResponseError {
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
            .maximum(Some(loonfs_api::DEFAULT_MAX_PAGE_LIMIT))
            .default(Some(serde_json::json!(loonfs_api::DEFAULT_PAGE_LIMIT)))
            .into()
    }
}

#[cfg(feature = "openapi")]
impl utoipa::ToSchema for OpenApiPageLimit {}

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
