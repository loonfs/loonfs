//! Authorized path, query, JSON, and payload extractors for HTTP handlers.

use super::error::ApiResponseError;
use super::serve::AppState;
use crate::config::AuthPolicy;
use axum::extract::rejection::PathRejection;
use axum::extract::{FromRequest, FromRequestParts, Path as AxumPath, Query};
use axum::http::request::Parts;
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use bytes::Bytes;
use futures::StreamExt;
use loonfs::{ByteStream, ErrorCode};
use loonfs_api::NamespaceId;
use std::convert::Infallible;
use std::sync::{Arc, Mutex};
use tokio::sync::OwnedSemaphorePermit;

/// One admission cap answered in-envelope: 503 `server_busy`, distinct from
/// `shutting_down` (drain) and `commit_queue_full` (publisher backpressure).
pub(super) fn server_busy_error(what: &str) -> ApiResponseError {
    ApiResponseError::new(
        StatusCode::SERVICE_UNAVAILABLE,
        ErrorCode::ServerBusy,
        &format!("the server is at its concurrency limit for {what}; retry shortly"),
    )
}

pub(super) fn authorize(
    policy: AuthPolicy<'_>,
    headers: &HeaderMap,
) -> Result<(), ApiResponseError> {
    let expected = match policy {
        AuthPolicy::Unauthenticated => return Ok(()),
        AuthPolicy::BearerToken(expected) => expected,
    };
    let actual = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    let expected = format!("Bearer {expected}");
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

/// The `namespace_id` path segment shared by namespace-scoped routes.
#[derive(Debug, serde::Deserialize)]
struct NamespaceSegment {
    namespace_id: String,
}

/// Parses the `{namespace_id}` path segment.
///
/// The handler reads the result after authorization. This keeps malformed
/// namespace ids from changing an unauthorized response from 401 to 400.
pub(super) struct NamespaceIdPath(Result<NamespaceId, ApiResponseError>);

impl NamespaceIdPath {
    /// Returns the parsed namespace id, or the same 400
    /// `invalid_namespace_id` response [`parse_namespace_id`] produces.
    pub(super) fn into_id(self) -> Result<NamespaceId, ApiResponseError> {
        self.0
    }
}

impl<S> FromRequestParts<S> for NamespaceIdPath
where
    S: Send + Sync,
{
    type Rejection = Infallible;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        match AxumPath::<NamespaceSegment>::from_request_parts(parts, state).await {
            Ok(AxumPath(NamespaceSegment { namespace_id })) => {
                Ok(Self(parse_namespace_id(namespace_id)))
            }
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

impl<T> FromRequest<AppState> for AppJson<T>
where
    T: serde::de::DeserializeOwned,
{
    type Rejection = ApiResponseError;

    async fn from_request(
        req: axum::extract::Request,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        authorize(state.config.auth_policy(), req.headers())?;
        extract_json(req, state, json_body_too_large_error)
            .await
            .map(AppJson)
    }
}

/// An authorized request body decoded after the upload mode is loaded.
pub(super) struct UploadBodyBytes(Bytes);

impl UploadBodyBytes {
    pub(super) fn into_bytes(self) -> Bytes {
        self.0
    }
}

const MAX_UPLOAD_CONTROL_BODY_BYTES: usize = 1024 * 1024;

impl FromRequest<AppState> for UploadBodyBytes {
    type Rejection = ApiResponseError;

    async fn from_request(
        req: axum::extract::Request,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        authorize(state.config.auth_policy(), req.headers())?;
        axum::body::to_bytes(req.into_body(), MAX_UPLOAD_CONTROL_BODY_BYTES)
            .await
            .map(Self)
            .map_err(|error| {
                ApiResponseError::new(
                    StatusCode::BAD_REQUEST,
                    ErrorCode::InvalidRequest,
                    &format!("request body unreadable: {error}"),
                )
            })
    }
}

/// The proxied-upload body as a stream, plus the admission permit that
/// bounds how many such transfers run at once.
///
/// Extraction runs the admission sequence in bounded-cost order:
/// authorization first (an unauthenticated caller must not occupy a
/// transfer slot), then a permit — or 503 `server_busy`. The body itself is
/// not read here at all. It is handed on as a stream so the write path can
/// hash it and forward it a piece at a time, which is what keeps a large
/// upload's memory cost independent of its size. The permit rides with the
/// stream and frees its slot when the handler drops it.
///
/// Two things end a transfer early, and neither can be reported through the
/// stream itself — a store sees only "the payload stopped". So the reason
/// is recorded beside it and read back by [`Self::into_rejection`] once the
/// write has failed: a body past `upload.max_content_bytes` answers 413
/// `content_too_large`, an unreadable one 400.
pub(super) struct UploadBodyStream {
    body: axum::body::Body,
    max_bytes: u64,
    abort: Arc<Mutex<Option<UploadStreamAbort>>>,
    _permit: OwnedSemaphorePermit,
}

/// Why the server stopped reading an upload body.
enum UploadStreamAbort {
    /// The payload ran past the deployment's upload limit.
    TooLarge,
    /// The connection failed or the client stopped sending.
    Unreadable(String),
}

impl UploadBodyStream {
    /// Consumes the body as a byte stream, counting as it goes and cutting
    /// it off past the limit.
    pub(super) fn into_stream(self) -> (ByteStream, UploadStreamOutcome) {
        let Self {
            body,
            max_bytes,
            abort,
            _permit,
        } = self;
        let outcome = UploadStreamOutcome {
            abort: Arc::clone(&abort),
            _permit,
        };
        let mut read_bytes = 0u64;
        let stream = body
            .into_data_stream()
            .map(move |chunk| match chunk {
                Ok(chunk) => {
                    read_bytes += chunk.len() as u64;
                    if read_bytes > max_bytes {
                        return Err(record_abort(&abort, UploadStreamAbort::TooLarge));
                    }
                    Ok(chunk)
                }
                Err(error) => Err(record_abort(
                    &abort,
                    UploadStreamAbort::Unreadable(error.to_string()),
                )),
            })
            .boxed();
        (stream, outcome)
    }
}

/// Records why the body stopped and reports it to the store as a transport
/// failure, which is all a store can act on.
fn record_abort(
    abort: &Arc<Mutex<Option<UploadStreamAbort>>>,
    reason: UploadStreamAbort,
) -> loonfs::ObjectStoreError {
    let message = match &reason {
        UploadStreamAbort::TooLarge => "upload body exceeded this deployment's limit".to_owned(),
        UploadStreamAbort::Unreadable(error) => format!("upload body unreadable: {error}"),
    };
    *abort.lock().unwrap_or_else(|err| err.into_inner()) = Some(reason);
    loonfs::ObjectStoreError::transport("upload body", message)
}

/// Holds the transfer permit for as long as the write runs, and remembers
/// why the body stopped when it did.
pub(super) struct UploadStreamOutcome {
    abort: Arc<Mutex<Option<UploadStreamAbort>>>,
    _permit: OwnedSemaphorePermit,
}

impl UploadStreamOutcome {
    /// The response a failed write owes the client, when the body — not the
    /// store — is what failed.
    pub(super) fn into_rejection(self) -> Option<ApiResponseError> {
        match self
            .abort
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .take()
        {
            Some(UploadStreamAbort::TooLarge) => Some(upload_body_too_large_error()),
            Some(UploadStreamAbort::Unreadable(error)) => Some(ApiResponseError::new(
                StatusCode::BAD_REQUEST,
                ErrorCode::InvalidRequest,
                &format!("request body unreadable: {error}"),
            )),
            None => None,
        }
    }
}

impl FromRequest<AppState> for UploadBodyStream {
    type Rejection = ApiResponseError;

    async fn from_request(
        req: axum::extract::Request,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        authorize(state.config.auth_policy(), req.headers())?;
        let permit = state
            .upload_permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| {
                state.metrics.upload_rejected_as_busy();
                server_busy_error("proxied uploads")
            })?;
        let max_bytes = state.config.max_upload_bytes;
        // A declared length past the limit is refused before a byte moves.
        // The incremental count still runs: a chunked body declares nothing,
        // and a declared length is a claim rather than a measurement.
        if declared_content_length(req.headers()).is_some_and(|length| length > max_bytes) {
            return Err(upload_body_too_large_error());
        }
        Ok(UploadBodyStream {
            body: req.into_body(),
            max_bytes,
            abort: Arc::new(Mutex::new(None)),
            _permit: permit,
        })
    }
}

fn declared_content_length(headers: &HeaderMap) -> Option<u64> {
    headers
        .get(axum::http::header::CONTENT_LENGTH)?
        .to_str()
        .ok()?
        .parse()
        .ok()
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

/// Like [`AppJson`], but an absent (empty) body is `None` rather than an
/// error, while a present-but-malformed body still answers 400 in-envelope.
pub(super) struct OptionalAppJson<T>(pub(super) Option<T>);

const MAX_OPTIONAL_JSON_BODY_BYTES: usize = 1024 * 1024;

impl<T> FromRequest<AppState> for OptionalAppJson<T>
where
    T: serde::de::DeserializeOwned,
{
    type Rejection = ApiResponseError;

    async fn from_request(
        req: axum::extract::Request,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        authorize(state.config.auth_policy(), req.headers())?;
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
