//! Builds HTTP requests, applies retry limits, and converts responses into
//! client errors.

use crate::operations::Operation;
use crate::transport_body::{timeout_error, wait_for_timeout, RequestActivity, TimedBody};
use crate::{Body, Client, ClientError, PayloadStream, Result, TransportError};
use bytes::Bytes;
use futures::{StreamExt as _, TryStreamExt as _};
use http::{Method, Request, Response, StatusCode};
use http_body_util::BodyExt as _;
use loonfs_types::{
    headers, Context, ErrorResponse, NamespaceId, OperationDeadline, PageRequest,
    TransportRetryOutcome,
};
pub(crate) use loonfs_types::{MonotonicTimer, StdMonotonicTimer, TransportRetryPolicy};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

pub(crate) type Transport =
    tower::util::BoxCloneSyncService<Request<Body>, Response<Body>, TransportError>;
use tower::ServiceExt as _;

/// Retry limits for a request that is safe to send more than once.
pub(crate) const DEFAULT: TransportRetryPolicy = TransportRetryPolicy {
    max_retries: 3,
    initial_backoff_ms: 250,
    max_backoff_ms: 2_000,
    operation_deadline_ms: 90_000,
};

pub(crate) const IO_INACTIVITY_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SendPolicy {
    Once,
    Retry,
    RetryWithoutDeadline,
}

pub(crate) struct WireRequest {
    method: Method,
    url: String,
    /// Extra headers beyond authorization, in insertion order.
    headers: Vec<(String, String)>,
    /// Whether the configured bearer token is attached. False only for
    /// presigned provider URLs, which include their own signature.
    authenticate: bool,
    /// Whether a failed attempt is sent again.
    policy: SendPolicy,
    /// The body, serialized once so that every attempt sends the same bytes.
    body: Option<Bytes>,
}

impl Client {
    /// Starts the request for `operation` at `url`, with the header of the
    /// client's context.
    pub(crate) fn request(&self, operation: Operation, url: &str) -> WireRequest {
        let request = self.request_without_context(operation, url);
        match &self.context {
            Context::Service => request.header(headers::CONTEXT, headers::SERVICE_CONTEXT),
            Context::Principals(principals) => {
                request.header(headers::PRINCIPALS, principals.to_string())
            }
        }
    }

    /// Starts an authenticated request without `Loonfs-Context` or
    /// `Loonfs-Principals`, for deployment endpoints (`/health`,
    /// `/v0/capabilities`) that don't take a principal context.
    pub(crate) fn request_without_context(&self, operation: Operation, url: &str) -> WireRequest {
        WireRequest {
            method: operation.method,
            url: url.to_owned(),
            headers: Vec::new(),
            authenticate: true,
            policy: operation.policy,
            body: None,
        }
    }

    /// Returns the URL of `suffix` under a namespace, where `suffix` is empty
    /// or starts with `/`.
    pub(crate) fn namespace_url(&self, namespace_id: &NamespaceId, suffix: &str) -> String {
        format!("{}/v0/namespaces/{namespace_id}{suffix}", self.base_url)
    }
}

impl WireRequest {
    /// A presigned provider URL: the signature authorizes it, so the
    /// deployment's bearer token must not be attached.
    pub(crate) fn presigned(method: Method, url: &str, policy: SendPolicy) -> Self {
        Self {
            method,
            url: url.to_owned(),
            headers: Vec::new(),
            authenticate: false,
            policy,
            body: None,
        }
    }

    pub(crate) fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }

    pub(crate) fn body(mut self, body: Bytes) -> Self {
        self.body = Some(body);
        self
    }

    /// Sets `body` as the JSON body.
    pub(crate) fn json(self, body: &impl serde::Serialize) -> Result<Self> {
        let body = serde_json::to_vec(body).map_err(|err| ClientError::Json(err.to_string()))?;
        Ok(self
            .header("content-type", "application/json")
            .body(Bytes::from(body)))
    }
}

/// Decodes a JSON response body.
pub(crate) fn decode_json<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    serde_json::from_slice(bytes).map_err(|err| ClientError::Json(err.to_string()))
}

impl Client {
    /// Sends `request` and decodes its JSON response.
    pub(crate) async fn send_json<T: serde::de::DeserializeOwned>(
        &self,
        request: WireRequest,
    ) -> Result<T> {
        decode_json(&self.call(&request).await?.bytes)
    }

    /// Sends a streaming request body exactly once.
    ///
    /// A stream cannot be replayed after an attempt consumes it, so failures are
    /// returned to the caller without retry. A stream without `size_bytes` is
    /// sent with chunked transfer encoding.
    pub(crate) async fn call_streamed_once(
        &self,
        request: &WireRequest,
        body: PayloadStream,
        size_bytes: Option<u64>,
    ) -> Result<Vec<u8>> {
        // A failed read of the caller's source fails the request, and the
        // transport reports it like a network failure. The failure is kept
        // here so that the request returns it as the local failure it is.
        let source_failure = Arc::new(OnceLock::new());
        let body = body
            .inspect_err({
                let source_failure = source_failure.clone();
                move |error| {
                    let _ = source_failure.set(error.to_string());
                }
            })
            .boxed();
        let request_body = self.build(request, Body::from_stream(body), size_bytes)?;
        let sent = match self.send(request_body, None).await {
            Ok(response) => response.collect(&request.url).await,
            Err(attempt) => Err(attempt),
        };
        sent.map(|response| response.bytes)
            .map_err(|attempt| match source_failure.get() {
                Some(message) => ClientError::Io(format!("reading the payload failed: {message}")),
                None => attempt.error,
            })
    }

    /// Sends a request and returns a successful response with its body
    /// unread.
    ///
    /// A failed attempt is sent again as the request's policy allows, until a
    /// successful response head arrives. No byte of the body has been read
    /// before then, so a resend can't repeat bytes the caller already has.
    pub(crate) async fn call_for_streamed_response(
        &self,
        request: &WireRequest,
    ) -> Result<StreamedResponse> {
        self.send_by_policy(request.policy, |attempt_timeout| {
            self.open_streamed(request, attempt_timeout)
        })
        .await
    }

    async fn open_streamed(
        &self,
        request: &WireRequest,
        attempt_timeout: Option<Duration>,
    ) -> std::result::Result<StreamedResponse, FailedAttempt> {
        let built = self
            .build(request, Body::empty(), Some(0))
            .map_err(|error| FailedAttempt {
                error,
                retry_after_ms: None,
            })?;
        let response = self.send(built, attempt_timeout).await?;
        if !response.status.is_success() {
            let retry_after_ms = parse_retry_after_ms(&response.headers);
            let bytes = response
                .body
                .collect()
                .await
                .map_err(|error| failed_attempt(&request.url, error))?
                .to_bytes();
            return Err(FailedAttempt {
                error: map_status_error(response.status.as_u16(), &bytes),
                retry_after_ms,
            });
        }
        Ok(StreamedResponse {
            status: response.status,
            headers: response.headers,
            body: response
                .body
                .into_data_stream()
                .map(|chunk| {
                    chunk.map_err(|error| {
                        std::io::Error::other(format!("response body ended early: {error}"))
                    })
                })
                .boxed(),
        })
    }

    /// Sends `request` with its body and buffers the response, sending a
    /// failed attempt again as the request's policy allows.
    pub(crate) async fn call(&self, request: &WireRequest) -> Result<BufferedResponse> {
        self.send_by_policy(request.policy, |attempt_timeout| {
            self.send_buffered(request, attempt_timeout)
        })
        .await
    }

    /// Runs `attempt` once, or until it succeeds or fails in a way that a
    /// retry can't fix, as `policy` allows.
    async fn send_by_policy<T, Attempt, Fut>(
        &self,
        policy: SendPolicy,
        attempt: Attempt,
    ) -> Result<T>
    where
        Attempt: FnMut(Option<Duration>) -> Fut,
        Fut: std::future::Future<Output = std::result::Result<T, FailedAttempt>>,
    {
        let mut attempt = attempt;
        match policy {
            SendPolicy::Once => attempt(None).await.map_err(|failed| failed.error),
            SendPolicy::Retry => {
                let deadline = OperationDeadline::start(
                    self.timer.as_ref(),
                    self.transport_retry.operation_deadline_ms,
                );
                self.send_with_retry(Some(deadline), attempt).await
            }
            SendPolicy::RetryWithoutDeadline => self.send_with_retry(None, attempt).await,
        }
    }

    async fn send_with_retry<T, Attempt, Fut>(
        &self,
        deadline: Option<OperationDeadline<'_>>,
        mut attempt: Attempt,
    ) -> Result<T>
    where
        Attempt: FnMut(Option<Duration>) -> Fut,
        Fut: std::future::Future<Output = std::result::Result<T, FailedAttempt>>,
    {
        let mut retries = 0;
        let mut attempt_timeout = deadline
            .as_ref()
            .map(|deadline| Duration::from_millis(deadline.deadline_ms()));
        loop {
            let failed = match attempt(attempt_timeout).await {
                Ok(response) => return Ok(response),
                Err(failed) => failed,
            };
            if !self.transport_retry_enabled || !failed.error.retryable_without_operator_action() {
                return Err(failed.error);
            }
            // The policy waits the whole `Retry-After`, or stops when the deadline can't fit it.
            let TransportRetryOutcome::Wait { backoff_ms } = self.transport_retry.next_backoff(
                &mut retries,
                deadline.as_ref(),
                failed.retry_after_ms,
            ) else {
                return Err(failed.error);
            };
            transport_delay(Duration::from_millis(backoff_ms)).await;
            attempt_timeout = match deadline.as_ref() {
                Some(deadline) => match deadline.remaining_ms() {
                    Some(remaining_ms) => Some(Duration::from_millis(remaining_ms)),
                    None => return Err(failed.error),
                },
                None => None,
            };
        }
    }

    async fn send_buffered(
        &self,
        request: &WireRequest,
        attempt_timeout: Option<Duration>,
    ) -> std::result::Result<BufferedResponse, FailedAttempt> {
        let body = request.body.clone().unwrap_or_default();
        let length = body.len() as u64;
        let built = self
            .build(request, Body::from(body), Some(length))
            .map_err(|error| FailedAttempt {
                error,
                retry_after_ms: None,
            })?;
        self.send(built, attempt_timeout)
            .await?
            .collect(&request.url)
            .await
    }

    /// Builds an HTTP request from `request` and `body`. `length` is the
    /// body's size in bytes, or `None` for a stream of unknown size, which is
    /// sent chunked.
    fn build(
        &self,
        request: &WireRequest,
        body: Body,
        length: Option<u64>,
    ) -> Result<Request<Body>> {
        let mut builder = Request::builder()
            .method(request.method.clone())
            .uri(&request.url);
        if request.authenticate {
            if let Some(token) = &self.auth_token {
                let mut value = crate::config::authorization_header(token)
                    .expect("the client validates its token when it is built");
                value.set_sensitive(true);
                builder = builder.header(http::header::AUTHORIZATION, value);
            }
        }
        for (name, value) in &request.headers {
            builder = builder.header(name.as_str(), value.as_str());
        }
        // The URL and the headers that remain come from the server, such as a
        // presigned URL, or from the caller, so a request that can't be built
        // breaks the API's contract and would fail the same way again. No
        // transport can send a request to a URL that isn't an absolute http or
        // https URL with a host, so the client refuses that request here,
        // whichever transport it uses.
        let cant_be_built = |reason: &dyn std::fmt::Display| {
            ClientError::Protocol(format!(
                "request to `{}` can't be built: {reason}",
                request.url
            ))
        };
        let mut built = builder.body(body).map_err(|error| cant_be_built(&error))?;
        if !crate::config::is_sendable_url(built.uri()) {
            return Err(cant_be_built(
                &"the URL isn't an absolute http or https URL with a host",
            ));
        }
        // HTTP asks a request whose method defines a body to send the body's
        // length, even when the body is empty (RFC 9110 section 8.6). The
        // HTTP library leaves the header off an empty body, and some load
        // balancers refuse that request with `411 Length Required`.
        if let Some(length) = length {
            if [Method::POST, Method::PUT, Method::PATCH].contains(&request.method) {
                built
                    .headers_mut()
                    .insert(http::header::CONTENT_LENGTH, length.into());
            }
        }
        Ok(built)
    }

    async fn send(
        &self,
        request: Request<Body>,
        attempt_timeout: Option<Duration>,
    ) -> std::result::Result<WireResponse, FailedAttempt> {
        let url = request.uri().to_string();
        let timeout = match (self.request_timeout, attempt_timeout) {
            (Some(configured), Some(remaining)) => Some(configured.min(remaining)),
            (configured, remaining) => configured.or(remaining),
        };
        let mut total_timeout = timeout.map(transport_delay);
        let activity = Arc::new(RequestActivity::new(self.timer.clone()));
        let mut read_timeout = Box::pin(activity.clone().wait_for_inactivity());
        let request = request.map(|body| body.track_activity(activity.clone()));
        let service = self.transport.clone();
        let response = tokio::select! {
            biased;
            () = wait_for_timeout(&mut total_timeout) => Err(timeout_error()),
            () = &mut read_timeout => Err(timeout_error()),
            response = service.oneshot(request) => response,
        }
        .map_err(|error| failed_attempt(&url, error))?;
        activity.touch();
        Ok(WireResponse::from(response.map(|body| {
            Body::new(TimedBody::new(body, total_timeout, read_timeout, activity))
        })))
    }
}

struct WireResponse {
    status: StatusCode,
    headers: http::HeaderMap,
    body: Body,
}

/// A successful response whose body is read as a stream.
pub(crate) struct StreamedResponse {
    pub(crate) status: StatusCode,
    pub(crate) headers: http::HeaderMap,
    pub(crate) body: PayloadStream,
}

impl From<Response<Body>> for WireResponse {
    fn from(response: Response<Body>) -> Self {
        let (parts, body) = response.into_parts();
        Self {
            status: parts.status,
            headers: parts.headers,
            body,
        }
    }
}

impl WireResponse {
    async fn collect(self, url: &str) -> std::result::Result<BufferedResponse, FailedAttempt> {
        let bytes = self
            .body
            .collect()
            .await
            .map_err(|error| failed_attempt(url, error))?
            .to_bytes();
        if self.status.is_success() {
            Ok(BufferedResponse {
                headers: self.headers,
                bytes: bytes.to_vec(),
            })
        } else {
            let retry_after_ms = parse_retry_after_ms(&self.headers);
            Err(FailedAttempt {
                error: map_status_error(self.status.as_u16(), &bytes),
                retry_after_ms,
            })
        }
    }
}

/// Parses an HTTP `Retry-After` delta-seconds header into milliseconds.
fn parse_retry_after_ms(headers: &http::HeaderMap) -> Option<u64> {
    let raw = headers
        .get(http::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim();
    let seconds: u64 = raw.parse().ok()?;
    Some(seconds.saturating_mul(1_000))
}

#[derive(Debug)]
pub(crate) struct BufferedResponse {
    headers: http::HeaderMap,
    pub(crate) bytes: Vec<u8>,
}

impl BufferedResponse {
    pub(crate) fn get(&self, name: http::header::HeaderName) -> Option<&http::HeaderValue> {
        self.headers.get(name)
    }
}

fn failed_attempt(url: &str, error: TransportError) -> FailedAttempt {
    let message = render_send_error(url, &error, error.is_connect(), error.is_timeout());
    FailedAttempt {
        // A request that the transport refuses to send fails the same way on
        // every attempt, like a request that the client can't build.
        error: if error.is_invalid_request() {
            ClientError::Protocol(message)
        } else {
            ClientError::Http(message)
        },
        retry_after_ms: None,
    }
}

/// One failed attempt.
pub(crate) struct FailedAttempt {
    pub(crate) error: ClientError,
    /// The wait that the failed response's `Retry-After` header asks for, in
    /// milliseconds, when the header is in delta seconds.
    pub(crate) retry_after_ms: Option<u64>,
}

pub(crate) fn map_status_error(status: u16, body: &[u8]) -> ClientError {
    match serde_json::from_slice::<ErrorResponse>(body) {
        Ok(body) => ClientError::from_error_response(status, body),
        // A status with a non-envelope body is most commonly an intermediary
        // responding for the server (a load balancer's HTML 502). Keep the
        // status, because it is the only signal in the response.
        Err(err) => {
            let provider_code = provider_error_code(body);
            ClientError::HttpStatus {
                status,
                detail: match &provider_code {
                    Some(code) => format!("provider error code {code}"),
                    None => err.to_string(),
                },
                provider_code,
            }
        }
    }
}

/// The `<Code>` of a storage provider's XML error body, which S3, GCS and
/// Azure all write, or `None` when `body` has no such element.
fn provider_error_code(body: &[u8]) -> Option<String> {
    let text = std::str::from_utf8(body).ok()?;
    let start = text.find("<Code>")? + "<Code>".len();
    let length = text[start..].find("</Code>")?;
    Some(text[start..start + length].to_owned())
}

fn render_send_error(
    url: &str,
    error: &(dyn std::error::Error + 'static),
    connect_failure: bool,
    timed_out: bool,
) -> String {
    let mut detail = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        let rendered = cause.to_string();
        // Wrapper layers usually restate their child; keep layers that add text.
        if !detail.contains(&rendered) {
            detail.push_str(": ");
            detail.push_str(&rendered);
        }
        source = cause.source();
    }
    if connect_failure {
        format!(
            "cannot connect to `{url}`: {detail}; check that the server is running and that \
             `server_url` points at it"
        )
    } else if timed_out {
        format!("request to `{url}` timed out: {detail}")
    } else {
        format!("request to `{url}` failed: {detail}")
    }
}

#[allow(
    clippy::disallowed_methods,
    reason = "this transport timer bounds local I/O and retries without changing protocol time"
)]
pub(crate) fn transport_delay(duration: Duration) -> std::pin::Pin<Box<tokio::time::Sleep>> {
    Box::pin(tokio::time::sleep(duration))
}

pub(crate) struct QueryBuilder {
    url: String,
    has_query: bool,
}

impl QueryBuilder {
    pub(crate) fn new(url: String) -> Self {
        Self {
            url,
            has_query: false,
        }
    }

    pub(crate) fn push(&mut self, name: &str, value: impl std::fmt::Display) {
        self.url.push(if self.has_query { '&' } else { '?' });
        self.has_query = true;
        self.url.push_str(name);
        self.url.push('=');
        self.url.extend(form_urlencoded::byte_serialize(
            value.to_string().as_bytes(),
        ));
    }

    pub(crate) fn page(&mut self, request: &PageRequest<String>) {
        self.push("limit", request.limit.get());
        if let Some(cursor) = &request.cursor {
            self.push("cursor", cursor);
        }
    }

    pub(crate) fn build(self) -> String {
        self.url
    }
}

#[cfg(test)]
pub(crate) mod tests;
