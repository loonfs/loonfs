//! Authorized path, query, JSON, and payload extractors for HTTP handlers.

use super::error::ApiResponseError;
use super::serve::AppState;
use crate::config::ServerConfig;
use axum::async_trait;
use axum::body::Bytes;
use axum::extract::rejection::PathRejection;
use axum::extract::{FromRequest, FromRequestParts, Path as AxumPath, Query};
use axum::http::request::Parts;
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use loonfs::ErrorCode;
use loonfs_api::NamespaceId;
use std::convert::Infallible;
use tokio::sync::OwnedSemaphorePermit;

/// One admission cap answered in-envelope: 503 `server_busy`, distinct from
/// `shutting_down` (drain) and `commit_queue_full` (publisher backpressure).
pub(super) fn server_busy_error(what: &str) -> ApiResponseError {
    ApiResponseError::new(
        StatusCode::SERVICE_UNAVAILABLE,
        ErrorCode::ServerBusy,
        &format!("the server is at its concurrency limit for {what}; retry shortly"),
    )
    // Transfer slots clear in fractions of a second; one second is the
    // smallest Retry-After HTTP can express.
    .with_retry_after(1)
}

pub(super) fn authorize(
    config: &ServerConfig,
    headers: &HeaderMap,
) -> Result<(), ApiResponseError> {
    let Some(expected) = &config.auth_token else {
        return Ok(());
    };
    let actual = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    let expected = format!("Bearer {}", expected.expose());
    if constant_time_eq(actual.as_bytes(), expected.as_bytes()) {
        Ok(())
    } else {
        Err(ApiResponseError::new(
            StatusCode::UNAUTHORIZED,
            ErrorCode::Unauthorized,
            "missing or invalid bearer token",
        ))
    }
}

/// Compares token bytes without short-circuiting on the first mismatch, so
/// response timing does not narrow down the token byte by byte. Length is
/// still observable; token values are not.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

fn parse_namespace_id(value: String) -> Result<NamespaceId, ApiResponseError> {
    NamespaceId::parse(&value).map_err(ApiResponseError::invalid_namespace_id)
}

/// The decoded `namespace` path segment, deserialized by name so routes with
/// additional path parameters can share the extractor.
#[derive(Debug, serde::Deserialize)]
struct NamespaceSegment {
    namespace: String,
}

/// Extractor for the `:namespace` path segment of namespace-scoped routes.
///
/// The segment is parsed into a [`NamespaceId`] at extraction time, but the
/// outcome is surfaced through [`NamespaceIdPath::into_id`] inside the
/// handler body rather than as an extractor rejection: every handler
/// authorizes before validating the namespace id, and rejecting during
/// extraction would let a malformed id short-circuit `authorize` and turn
/// today's 401 into a 400 for unauthorized requests.
pub(super) struct NamespaceIdPath(Result<NamespaceId, ApiResponseError>);

impl NamespaceIdPath {
    /// Returns the parsed namespace id, or the same 400
    /// `invalid_namespace_id` response [`parse_namespace_id`] produces.
    pub(super) fn into_id(self) -> Result<NamespaceId, ApiResponseError> {
        self.0
    }
}

#[async_trait]
impl<S> FromRequestParts<S> for NamespaceIdPath
where
    S: Send + Sync,
{
    type Rejection = Infallible;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        match AxumPath::<NamespaceSegment>::from_request_parts(parts, state).await {
            Ok(AxumPath(NamespaceSegment { namespace })) => Ok(Self(parse_namespace_id(namespace))),
            Err(rejection) => Ok(Self(Err(invalid_path_params(&rejection)))),
        }
    }
}

fn invalid_path_params(rejection: &PathRejection) -> ApiResponseError {
    ApiResponseError::new(
        StatusCode::BAD_REQUEST,
        ErrorCode::InvalidRequest,
        &format!("invalid path parameters: {rejection}"),
    )
}

/// Path extractor that never rejects at extraction: the parse outcome is
/// surfaced through [`AppPath::into_params`] inside the handler, after
/// `authorize`, so malformed path parameters answer inside the JSON error
/// envelope and never turn an unauthorized request's 401 into a 400.
pub(super) struct AppPath<T>(Result<T, ApiResponseError>);

impl<T> AppPath<T> {
    pub(super) fn into_params(self) -> Result<T, ApiResponseError> {
        self.0
    }
}

#[async_trait]
impl<S, T> FromRequestParts<S> for AppPath<T>
where
    S: Send + Sync,
    T: serde::de::DeserializeOwned + Send,
{
    type Rejection = Infallible;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        match AxumPath::<T>::from_request_parts(parts, state).await {
            Ok(AxumPath(value)) => Ok(Self(Ok(value))),
            Err(rejection) => Ok(Self(Err(invalid_path_params(&rejection)))),
        }
    }
}

/// [`AppPath`]'s query-string twin: missing required parameters, values that
/// fail their field types (`after_seq=abc`), and undecodable query strings
/// all surface through [`AppQuery::into_params`] after `authorize`, inside
/// the envelope.
pub(super) struct AppQuery<T>(Result<T, ApiResponseError>);

impl<T> AppQuery<T> {
    pub(super) fn into_params(self) -> Result<T, ApiResponseError> {
        self.0
    }
}

#[async_trait]
impl<S, T> FromRequestParts<S> for AppQuery<T>
where
    S: Send + Sync,
    T: serde::de::DeserializeOwned,
{
    type Rejection = Infallible;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        match Query::<T>::from_request_parts(parts, state).await {
            Ok(Query(value)) => Ok(Self(Ok(value))),
            Err(rejection) => Ok(Self(Err(ApiResponseError::new(
                StatusCode::BAD_REQUEST,
                ErrorCode::InvalidRequest,
                &format!("invalid query parameters: {rejection}"),
            )))),
        }
    }
}

/// A `Json` extractor whose rejections stay inside the error contract:
/// malformed bodies answer 400 with an `invalid_request` `ApiError` body
/// instead of the raw framework rejection, and authorization runs before
/// the body is read so a malformed body never turns an unauthorized
/// request's 401 into a 400.
pub(super) struct AppJson<T>(pub(super) T);

async fn extract_json<S, T>(
    req: axum::extract::Request,
    state: &S,
    body_too_large: fn() -> ApiResponseError,
) -> Result<T, ApiResponseError>
where
    T: serde::de::DeserializeOwned,
    S: Send + Sync,
{
    match Json::<T>::from_request(req, state).await {
        Ok(Json(value)) => Ok(value),
        Err(rejection) if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE => {
            Err(body_too_large())
        }
        Err(rejection) => Err(ApiResponseError::new(
            StatusCode::BAD_REQUEST,
            ErrorCode::InvalidRequest,
            &rejection.body_text(),
        )),
    }
}

#[async_trait]
impl<T> FromRequest<AppState> for AppJson<T>
where
    T: serde::de::DeserializeOwned,
{
    type Rejection = ApiResponseError;

    async fn from_request(
        req: axum::extract::Request,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        authorize(&state.config, req.headers())?;
        extract_json(req, state, json_body_too_large_error)
            .await
            .map(AppJson)
    }
}

/// Commit JSON is both larger than the framework default and potentially
/// expensive to buffer, so authenticate before reading it and give its 413
/// the route-specific recovery guidance.
pub(super) struct CommitAppJson<T>(pub(super) T);

#[async_trait]
impl<T> FromRequest<AppState> for CommitAppJson<T>
where
    T: serde::de::DeserializeOwned,
{
    type Rejection = ApiResponseError;

    async fn from_request(
        req: axum::extract::Request,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        authorize(&state.config, req.headers())?;
        extract_json(req, state, commit_body_too_large_error)
            .await
            .map(CommitAppJson)
    }
}

/// The proxied-upload body plus the admission permit that bounds how many
/// such bodies the server buffers at once.
///
/// Extraction runs the admission sequence in bounded-cost order:
/// authorization first (an unauthenticated caller must not occupy a
/// buffering slot), then a permit — or 503 `server_busy` — and only then is
/// the body buffered, so the permit covers the buffering itself, not just
/// the handler. The permit rides with the bytes and frees its slot when the
/// handler drops them. Rejections stay inside the error contract: a body
/// over the route's limit answers 413 `content_too_large`, other unreadable
/// bodies 400.
pub(super) struct UploadBodyBytes {
    pub(super) bytes: Bytes,
    _permit: OwnedSemaphorePermit,
}

#[async_trait]
impl FromRequest<AppState> for UploadBodyBytes {
    type Rejection = ApiResponseError;

    async fn from_request(
        req: axum::extract::Request,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        authorize(&state.config, req.headers())?;
        let permit = state
            .upload_permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| server_busy_error("proxied uploads"))?;
        match Bytes::from_request(req, state).await {
            Ok(bytes) => Ok(UploadBodyBytes {
                bytes,
                _permit: permit,
            }),
            Err(rejection) if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE => {
                Err(upload_body_too_large_error())
            }
            Err(rejection) => Err(ApiResponseError::new(
                StatusCode::BAD_REQUEST,
                ErrorCode::InvalidRequest,
                &rejection.body_text(),
            )),
        }
    }
}

/// 413 for over-limit upload bodies: the guidance names the upload byte cap
/// and the optional `direct_put` path that bypasses proxied buffering.
fn upload_body_too_large_error() -> ApiResponseError {
    ApiResponseError::new(
        StatusCode::PAYLOAD_TOO_LARGE,
        ErrorCode::ContentTooLarge,
        "request body exceeds this deployment's limit; check the \
         `upload.max_content_bytes` capability limit, and use `direct_put` \
         for large content when `core.uploads.direct_put` is advertised",
    )
}

/// 413 for ordinary JSON routes that retain the framework's default bound.
fn json_body_too_large_error() -> ApiResponseError {
    ApiResponseError::new(
        StatusCode::PAYLOAD_TOO_LARGE,
        ErrorCode::ContentTooLarge,
        "JSON request body exceeds this route's body limit",
    )
}

/// 413 for over-limit commit JSON. Commit bodies are metadata, so the fix
/// is splitting the batch rather than routing content through `direct_put`.
fn commit_body_too_large_error() -> ApiResponseError {
    ApiResponseError::new(
        StatusCode::PAYLOAD_TOO_LARGE,
        ErrorCode::ContentTooLarge,
        "commit request body exceeds this deployment's \
         `commit.max_body_bytes` capability limit — split the commit into \
         smaller batches",
    )
}

/// Like [`AppJson`], but an absent (empty) body is `None` rather than an
/// error, while a present-but-malformed body still answers 400 in-envelope.
pub(super) struct OptionalAppJson<T>(pub(super) Option<T>);

const MAX_OPTIONAL_JSON_BODY_BYTES: usize = 1024 * 1024;

#[async_trait]
impl<T> FromRequest<AppState> for OptionalAppJson<T>
where
    T: serde::de::DeserializeOwned,
{
    type Rejection = ApiResponseError;

    async fn from_request(
        req: axum::extract::Request,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        authorize(&state.config, req.headers())?;
        let body = axum::body::to_bytes(req.into_body(), MAX_OPTIONAL_JSON_BODY_BYTES)
            .await
            .map_err(|error| {
                ApiResponseError::new(
                    StatusCode::BAD_REQUEST,
                    ErrorCode::InvalidRequest,
                    &format!("request body unreadable: {error}"),
                )
            })?;
        if body.is_empty() {
            return Ok(OptionalAppJson(None));
        }
        let value = serde_json::from_slice(&body).map_err(|error| {
            ApiResponseError::new(
                StatusCode::BAD_REQUEST,
                ErrorCode::InvalidRequest,
                &format!("request body is not valid JSON for this operation: {error}"),
            )
        })?;
        Ok(OptionalAppJson(Some(value)))
    }
}
