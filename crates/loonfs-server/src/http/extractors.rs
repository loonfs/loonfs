//! Authorized path, query, JSON, and payload extractors for HTTP handlers.

use super::error::ApiResponseError;
use super::serve::AppState;
use crate::config::AuthPolicy;
use axum::extract::rejection::PathRejection;
use axum::extract::{DefaultBodyLimit, FromRequest, FromRequestParts, Path as AxumPath, Query};
use axum::http::request::Parts;
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use bytes::Bytes;
use futures::StreamExt;
use loonfs::{ByteStream, ErrorCode, MAX_MULTIPART_PARTS, MAX_SIGNED_PARTS_PER_REQUEST};
use loonfs_api::{AbsolutePath, NamespaceId};
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
    body_too_large: ApiResponseError,
) -> Result<T, ApiResponseError>
where
    T: serde::de::DeserializeOwned,
    S: Send + Sync,
{
    let body = match Json::<Box<serde_json::value::RawValue>>::from_request(req, state).await {
        Ok(Json(body)) => body,
        Err(rejection) if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE => {
            return Err(body_too_large);
        }
        Err(rejection) => {
            return Err(ApiResponseError::new(
                StatusCode::BAD_REQUEST,
                ErrorCode::InvalidRequest,
                &rejection.body_text(),
            ));
        }
    };
    decode_json(body.get().as_bytes())
}

fn decode_json<T>(body: &[u8]) -> Result<T, ApiResponseError>
where
    T: serde::de::DeserializeOwned,
{
    let mut deserializer = serde_json::Deserializer::from_slice(body);
    let decoded = serde_path_to_error::deserialize(&mut deserializer).map_err(|error| {
        let param = json_pointer(error.path())
            .and_then(|pointer| refine_internally_tagged_path(body, pointer));
        let response = ApiResponseError::new(
            StatusCode::BAD_REQUEST,
            ErrorCode::InvalidRequest,
            &format!("invalid JSON request body: {}", error.inner()),
        );
        match param {
            Some(param) => response.with_param(param),
            None => response,
        }
    })?;
    // One value is the whole body: `end` rejects trailing data, which no
    // single field can be blamed for, so this arm carries no param.
    deserializer.end().map_err(|error| {
        ApiResponseError::new(
            StatusCode::BAD_REQUEST,
            ErrorCode::InvalidRequest,
            &format!("invalid JSON request body: {error}"),
        )
    })?;
    Ok(decoded)
}

fn refine_internally_tagged_path(body: &[u8], pointer: String) -> Option<String> {
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(body) else {
        return Some(pointer);
    };
    let Some(object) = value
        .pointer(&pointer)
        .and_then(serde_json::Value::as_object)
    else {
        return Some(pointer);
    };
    if !object.contains_key("kind") {
        return Some(pointer);
    }
    let invalid_path_fields = object
        .iter()
        .filter(|(name, value)| {
            matches!(name.as_str(), "path" | "from_path" | "to_path")
                && serde_json::from_value::<AbsolutePath>((*value).clone()).is_err()
        })
        .map(|(name, _)| name.as_str())
        .collect::<Vec<_>>();
    if let [field] = invalid_path_fields.as_slice() {
        return Some(format!("{pointer}/{}", escape_json_pointer_segment(field)));
    }
    None
}

fn escape_json_pointer_segment(segment: &str) -> String {
    segment.replace('~', "~0").replace('/', "~1")
}

fn json_pointer(path: &serde_path_to_error::Path) -> Option<String> {
    let mut pointer = String::new();
    for segment in path {
        pointer.push('/');
        match segment {
            serde_path_to_error::Segment::Seq { index } => pointer.push_str(&index.to_string()),
            serde_path_to_error::Segment::Map { key } => {
                pointer.push_str(&escape_json_pointer_segment(key));
            }
            serde_path_to_error::Segment::Enum { .. } | serde_path_to_error::Segment::Unknown => {
                return None
            }
        }
    }
    (!pointer.is_empty()).then_some(pointer)
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
        extract_json(req, state, json_body_too_large_error())
            .await
            .map(AppJson)
    }
}

/// Maximum UTF-8 byte length accepted for a grep pattern query parameter.
pub(super) const MAX_GREP_PATTERN_BYTES: usize = 1024;

/// Maximum body size for starting an upload or signing multipart parts.
/// A request with 1,000 part claims is at most 131,011 bytes, so 1 MiB leaves
/// ample room for every valid request.
pub(super) const MAX_UPLOAD_CONTROL_BODY_BYTES: usize = 1024 * 1024;

const MAX_SERIALIZED_SIGNING_PART_BYTES: usize = 130;
const MAX_SIGNING_ENVELOPE_BYTES: usize = 12;
const _: () = assert!(
    MAX_SIGNED_PARTS_PER_REQUEST * MAX_SERIALIZED_SIGNING_PART_BYTES
        + (MAX_SIGNED_PARTS_PER_REQUEST - 1)
        + MAX_SIGNING_ENVELOPE_BYTES
        <= MAX_UPLOAD_CONTROL_BODY_BYTES
);

/// The largest completion body accepted by the multipart completion route.
///
/// A completion may contain 10,000 parts. The calculation allows 398 bytes
/// per part: a five-digit part number, a 256-byte quoted ETag, and a full
/// SHA-256 checksum. The remaining JSON fields use at most 167 bytes. This
/// puts the largest expected request at:
///
/// `10_000 × 398 + 9_999 commas + 167 = 3_990_166 bytes`.
///
/// The 8 MiB limit leaves room for future fields. Recalculate it if the part
/// limit or expected ETag size changes.
pub(super) const MAX_COMPLETION_BODY_BYTES: usize = 8 * 1024 * 1024;

const MAX_SERIALIZED_COMPLETION_PART_BYTES: usize = 398;
const MAX_COMPLETION_ENVELOPE_BYTES: usize = 167;
const _: () = assert!(
    (MAX_MULTIPART_PARTS as usize) * MAX_SERIALIZED_COMPLETION_PART_BYTES
        + (MAX_MULTIPART_PARTS as usize - 1)
        + MAX_COMPLETION_ENVELOPE_BYTES
        <= MAX_COMPLETION_BODY_BYTES
);

/// JSON from an upload route with an explicit body-size limit.
pub(super) struct UploadControlJson<T, const MAX_BYTES: usize>(pub(super) T);

impl<T, const MAX_BYTES: usize> FromRequest<AppState> for UploadControlJson<T, MAX_BYTES>
where
    T: serde::de::DeserializeOwned,
{
    type Rejection = ApiResponseError;

    async fn from_request(
        mut req: axum::extract::Request,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        authorize(state.config.auth_policy(), req.headers())?;
        DefaultBodyLimit::max(MAX_BYTES).apply(&mut req);
        extract_json(req, state, upload_control_body_too_large_error(MAX_BYTES))
            .await
            .map(Self)
    }
}

async fn extract_body_bytes<S>(
    mut req: axum::extract::Request,
    state: &S,
    max_bytes: usize,
    body_too_large: ApiResponseError,
) -> Result<Bytes, ApiResponseError>
where
    S: Send + Sync,
{
    DefaultBodyLimit::max(max_bytes).apply(&mut req);
    match Bytes::from_request(req, state).await {
        Ok(body) => Ok(body),
        Err(rejection) if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE => {
            Err(body_too_large)
        }
        Err(rejection) => Err(ApiResponseError::new(
            StatusCode::BAD_REQUEST,
            ErrorCode::InvalidRequest,
            &format!("request body unreadable: {}", rejection.body_text()),
        )),
    }
}

/// An authorized request body decoded after the upload mode is loaded.
pub(super) struct UploadBodyBytes<const MAX_BYTES: usize>(Bytes);

impl<const MAX_BYTES: usize> UploadBodyBytes<MAX_BYTES> {
    pub(super) fn into_bytes(self) -> Bytes {
        self.0
    }
}

impl<const MAX_BYTES: usize> FromRequest<AppState> for UploadBodyBytes<MAX_BYTES> {
    type Rejection = ApiResponseError;

    async fn from_request(
        req: axum::extract::Request,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        authorize(state.config.auth_policy(), req.headers())?;
        extract_body_bytes(
            req,
            state,
            MAX_BYTES,
            upload_control_body_too_large_error(MAX_BYTES),
        )
        .await
        .map(Self)
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

/// Returns a 413 error for an upload request that exceeds its route's limit.
fn upload_control_body_too_large_error(max_bytes: usize) -> ApiResponseError {
    ApiResponseError::new(
        StatusCode::PAYLOAD_TOO_LARGE,
        ErrorCode::ContentTooLarge,
        &format!("upload-control request body exceeds this route's limit of {max_bytes} bytes"),
    )
}

/// Returns a 413 error when optional JSON exceeds its body-size limit.
fn optional_json_body_too_large_error() -> ApiResponseError {
    ApiResponseError::new(
        StatusCode::PAYLOAD_TOO_LARGE,
        ErrorCode::ContentTooLarge,
        &format!(
            "JSON request body exceeds this route's limit of {MAX_OPTIONAL_JSON_BODY_BYTES} bytes"
        ),
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
        let body = extract_body_bytes(
            req,
            state,
            MAX_OPTIONAL_JSON_BODY_BYTES,
            optional_json_body_too_large_error(),
        )
        .await?;
        if body.is_empty() {
            return Ok(OptionalAppJson(None));
        }
        let value = decode_json(&body)?;
        Ok(OptionalAppJson(Some(value)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[allow(dead_code)]
    #[derive(Debug, serde::Deserialize)]
    struct Request {
        operations: Vec<Operation>,
    }

    #[allow(dead_code)]
    #[derive(Debug, serde::Deserialize)]
    struct Operation {
        path: u64,
    }

    #[test]
    fn typed_json_decode_reports_json_pointer() {
        let error = decode_json::<Request>(
            br#"{
            "operations": [{ "path": "relative" }]
        }"#,
        )
        .expect_err("path has the wrong type");
        assert_eq!(error.param(), Some("/operations/0/path"));
    }

    #[test]
    fn json_decode_rejects_trailing_data_after_the_value() {
        let error = decode_json::<Request>(br#"{"operations": []}{"operations": []}"#)
            .expect_err("trailing data is not part of the request");
        assert_eq!(error.param(), None);

        assert!(
            decode_json::<Request>(br#"{"operations": []}  "#).is_ok(),
            "trailing whitespace is not data"
        );
    }

    #[test]
    fn api_commit_decode_reports_operation_field_pointer() {
        let error = decode_json::<loonfs_api::CommitRequest>(
            br#"{
                "commit_id": "invalid-path",
                "actor": { "kind": "service", "id": "test-service" },
                "operations": [{ "kind": "create_directory", "path": "relative" }]
            }"#,
        )
        .expect_err("relative operation path is invalid");
        assert_eq!(error.param(), Some("/operations/0/path"));
    }

    #[test]
    fn ambiguous_operation_fields_do_not_report_a_param() {
        let error = decode_json::<loonfs_api::CommitRequest>(
            br#"{
                "commit_id": "invalid-paths",
                "actor": { "kind": "service", "id": "test-service" },
                "operations": [{
                    "kind": "move_path",
                    "from_path": "relative",
                    "to_path": "also-relative"
                }]
            }"#,
        )
        .expect_err("two relative operation paths are ambiguous");
        assert_eq!(error.param(), None);
    }

    #[test]
    fn json_pointer_escapes_object_keys() {
        #[allow(dead_code)]
        #[derive(Debug, serde::Deserialize)]
        struct MapRequest {
            values: std::collections::BTreeMap<String, u64>,
        }

        let error = decode_json::<MapRequest>(
            br#"{
            "values": { "a/b~c": false }
        }"#,
        )
        .expect_err("value has the wrong type");
        assert_eq!(error.param(), Some("/values/a~1b~0c"));
    }
}
