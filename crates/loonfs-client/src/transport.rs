//! Builds HTTP requests, applies retry limits, and converts responses into
//! client errors.

use crate::transport_body::{timeout_error, wait_for_timeout, RequestActivity, TimedBody};
use crate::{Body, Client, ClientError, PayloadStream, Result, TransportError};
use bytes::Bytes;
use futures::StreamExt as _;
use http::{Method, Request, Response, StatusCode};
use http_body_util::BodyExt as _;
use loonfs_api::{transport_retry_backoff, ApiError, ErrorCode, OperationDeadline};
pub(crate) use loonfs_api::{MonotonicTimer, StdMonotonicTimer, TransportRetryPolicy};
use std::sync::Arc;
use std::time::Duration;

pub(crate) type Transport =
    tower::util::BoxCloneSyncService<Request<Body>, Response<Body>, TransportError>;
use tower::ServiceExt as _;

/// Retry limits for a request that is safe to send more than once.
pub(crate) const DEFAULT: TransportRetryPolicy = TransportRetryPolicy {
    max_retries: 3,
    initial_backoff: Duration::from_millis(250),
    max_backoff: Duration::from_secs(2),
    operation_deadline: Duration::from_secs(90),
};

pub(crate) const IO_INACTIVITY_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SendPolicy {
    Once,
    Retry,
    RetryUnbounded,
}

pub(crate) struct WireRequest {
    method: Method,
    url: String,
    /// Extra headers beyond authorization, in insertion order.
    headers: Vec<(String, String)>,
    /// Whether the configured bearer token is attached. False only for
    /// presigned provider URLs, which carry their own signature.
    authenticate: bool,
}

impl Client {
    fn with_subject_headers(&self, request: WireRequest) -> WireRequest {
        let Some(subject) = &self.subject else {
            return request;
        };
        request
            .header("Loonfs-Principal-Scope", subject.principal_scope.as_str())
            .header("Loonfs-Subject", subject.subject_id.as_str())
            .header(
                "Loonfs-Principals",
                subject
                    .principals
                    .iter()
                    .map(|principal| principal.as_str())
                    .collect::<Vec<_>>()
                    .join(","),
            )
    }

    pub(crate) fn get(&self, url: &str) -> WireRequest {
        self.with_subject_headers(WireRequest::to_server(Method::GET, url))
    }

    pub(crate) fn post(&self, url: &str) -> WireRequest {
        self.with_subject_headers(WireRequest::to_server(Method::POST, url))
    }

    pub(crate) fn put(&self, url: &str) -> WireRequest {
        self.with_subject_headers(WireRequest::to_server(Method::PUT, url))
    }

    pub(crate) fn delete(&self, url: &str) -> WireRequest {
        self.with_subject_headers(WireRequest::to_server(Method::DELETE, url))
    }
}

impl WireRequest {
    fn to_server(method: Method, url: &str) -> Self {
        Self {
            method,
            url: url.to_owned(),
            headers: Vec::new(),
            authenticate: true,
        }
    }

    /// A presigned provider URL: the signature authorizes it, so the
    /// deployment's bearer token must not be attached.
    pub(crate) fn presigned(method: Method, url: &str) -> Self {
        Self {
            method,
            url: url.to_owned(),
            headers: Vec::new(),
            authenticate: false,
        }
    }

    pub(crate) fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }
}

impl Client {
    pub(crate) async fn request_json<Req, Resp>(
        &self,
        request: WireRequest,
        body: Option<&Req>,
        policy: SendPolicy,
    ) -> Result<Resp>
    where
        Req: serde::Serialize,
        Resp: serde::de::DeserializeOwned,
    {
        let body = match body {
            Some(body) => {
                // Serialized once: every retry attempt resends identical
                // bytes when this call site permits a resend.
                Some(Bytes::from(
                    serde_json::to_vec(body).map_err(|err| ClientError::Json(err.to_string()))?,
                ))
            }
            None => None,
        };
        let request = match body {
            Some(_) => request.header("content-type", "application/json"),
            None => request,
        };
        let bytes = self.call(&request, body.as_ref(), policy).await?.bytes;
        serde_json::from_slice(&bytes).map_err(|err| ClientError::Json(err.to_string()))
    }

    pub(crate) async fn request_bytes(&self, url: &str) -> Result<Vec<u8>> {
        let request = self.get(url);
        Ok(self
            .call(&request, None, SendPolicy::RetryUnbounded)
            .await?
            .bytes)
    }

    /// Sends a streaming request body exactly once.
    ///
    /// A stream cannot be replayed after an attempt consumes it, so failures are
    /// returned to the caller without retry. `size_bytes` sets `Content-Length`;
    /// when absent, the request uses chunked transfer encoding.
    pub(crate) async fn call_streamed_once(
        &self,
        request: &WireRequest,
        body: PayloadStream,
        size_bytes: Option<u64>,
    ) -> Result<Vec<u8>> {
        let mut request_body = self.build(request, Body::from_stream(body))?;
        if let Some(size_bytes) = size_bytes {
            request_body
                .headers_mut()
                .append(http::header::CONTENT_LENGTH, size_bytes.into());
        }
        self.send(request_body, None)
            .await
            .map_err(|attempt| attempt.error)?
            .collect(&request.url)
            .await
            .map(|response| response.bytes)
            .map_err(|attempt| attempt.error)
    }

    pub(crate) async fn call_for_response_stream(
        &self,
        request: &WireRequest,
    ) -> Result<PayloadStream> {
        let response = self
            .send(self.build(request, Body::empty())?, None)
            .await
            .map_err(|error| error.error)?;
        if !response.status.is_success() {
            let bytes = response
                .body
                .collect()
                .await
                .map_err(|error| failed_attempt(&request.url, error).error)?
                .to_bytes();
            return Err(map_status_error(response.status.as_u16(), &bytes));
        }
        Ok(response
            .body
            .into_data_stream()
            .map(|chunk| {
                chunk.map_err(|error| {
                    std::io::Error::other(format!("response body ended early: {error}"))
                })
            })
            .boxed())
    }

    pub(crate) async fn call(
        &self,
        request: &WireRequest,
        body: Option<&Bytes>,
        policy: SendPolicy,
    ) -> Result<BufferedResponse> {
        match policy {
            SendPolicy::Once => self
                .send_buffered(request, body, None)
                .await
                .map_err(|attempt| attempt.error),
            SendPolicy::Retry => {
                let deadline = OperationDeadline::start(
                    self.timer.as_ref(),
                    self.transport_retry.operation_deadline,
                );
                self.send_with_retry(request, body, Some(deadline)).await
            }
            SendPolicy::RetryUnbounded => self.send_with_retry(request, body, None).await,
        }
    }

    async fn send_with_retry(
        &self,
        request: &WireRequest,
        body: Option<&Bytes>,
        deadline: Option<OperationDeadline<'_>>,
    ) -> Result<BufferedResponse> {
        let mut retries = 0;
        let mut attempt_timeout = deadline.as_ref().map(OperationDeadline::deadline);
        loop {
            let attempt = match self.send_buffered(request, body, attempt_timeout).await {
                Ok(response) => return Ok(response),
                Err(attempt) => attempt,
            };
            let retryable = retryable_transport_failure(attempt.transport, &attempt.error);
            if !self.transport_retry_enabled || !retryable {
                return Err(attempt.error);
            }
            let Some(backoff) = next_transport_retry_backoff(
                &self.transport_retry,
                &mut retries,
                deadline.as_ref(),
            ) else {
                return Err(attempt.error);
            };
            transport_delay(backoff).await;
            attempt_timeout = match deadline.as_ref() {
                Some(deadline) => match deadline.remaining() {
                    Some(remaining) => Some(remaining),
                    None => return Err(attempt.error),
                },
                None => None,
            };
        }
    }

    async fn send_buffered(
        &self,
        request: &WireRequest,
        body: Option<&Bytes>,
        attempt_timeout: Option<Duration>,
    ) -> std::result::Result<BufferedResponse, FailedAttempt> {
        let body = body.cloned().map(Body::from).unwrap_or_else(Body::empty);
        let built = self.build(request, body).map_err(|error| FailedAttempt {
            transport: true,
            error,
        })?;
        self.send(built, attempt_timeout)
            .await?
            .collect(&request.url)
            .await
    }

    fn build(&self, request: &WireRequest, body: Body) -> Result<Request<Body>> {
        let mut builder = Request::builder()
            .method(request.method.clone())
            .uri(&request.url);
        if request.authenticate {
            if let Some(token) = &self.auth_token {
                let mut value = http::HeaderValue::from_str(&format!("Bearer {}", token.expose()))
                    .map_err(|error| {
                        failed_attempt(&request.url, TransportError::new(error)).error
                    })?;
                value.set_sensitive(true);
                builder = builder.header(http::header::AUTHORIZATION, value);
            }
        }
        for (name, value) in &request.headers {
            builder = builder.header(name.as_str(), value.as_str());
        }
        builder
            .body(body)
            .map_err(|error| failed_attempt(&request.url, TransportError::new(error)).error)
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
            Err(FailedAttempt {
                transport: false,
                error: map_status_error(self.status.as_u16(), &bytes),
            })
        }
    }
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
    FailedAttempt {
        transport: true,
        error: ClientError::Http(render_send_error(
            url,
            &error,
            error.is_connect(),
            error.is_timeout(),
        )),
    }
}

/// One failed attempt, carrying whether the network layer itself reported it.
pub(crate) struct FailedAttempt {
    /// True when no complete response was served. Classified here, before the
    /// error is flattened, because the retry policy keys on it: a served
    /// status with a non-envelope body (a load balancer's HTML 502) is not a
    /// transport failure.
    pub(crate) transport: bool,
    pub(crate) error: ClientError,
}

pub(crate) fn map_status_error(status: u16, body: &[u8]) -> ClientError {
    match serde_json::from_slice::<ApiError>(body) {
        Ok(body) => ClientError::from_api_error(status, body),
        // A status with a non-envelope body is most commonly an intermediary
        // answering for the server (a load balancer's HTML 502): keep the
        // status — it is the only signal the response carried.
        Err(err) => ClientError::Http(format!(
            "http status {status} with a non-envelope body: {err}"
        )),
    }
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
            "cannot connect to `{url}`: {detail}; check that the server is running and that the \
             profile's `server_url` points at it"
        )
    } else if timed_out {
        format!("request to `{url}` timed out: {detail}")
    } else {
        format!("request to `{url}` failed: {detail}")
    }
}

/// Returns whether a failed request is eligible for an automatic retry.
///
/// Network failures are retryable. Server responses are retryable only when
/// their error code identifies a temporary condition that requires no
/// operator action.
pub(crate) fn retryable_transport_failure(transport: bool, error: &ClientError) -> bool {
    transport
        || error
            .code()
            .is_some_and(ErrorCode::retryable_without_operator_action)
}

/// Returns the delay before the next retry, or `None` when a limit is reached.
fn next_transport_retry_backoff(
    policy: &TransportRetryPolicy,
    retries: &mut u32,
    deadline: Option<&OperationDeadline<'_>>,
) -> Option<Duration> {
    if *retries >= policy.max_retries {
        return None;
    }
    let remaining = match deadline {
        Some(deadline) => deadline.remaining()?,
        None => Duration::MAX,
    };
    *retries += 1;
    Some(transport_retry_backoff(policy, *retries).min(remaining))
}

// This transport timer bounds local I/O and retries without changing protocol time.
#[allow(clippy::disallowed_methods)]
pub(crate) fn transport_delay(duration: Duration) -> std::pin::Pin<Box<tokio::time::Sleep>> {
    Box::pin(tokio::time::sleep(duration))
}

pub(crate) struct QueryBuilder {
    url: String,
    has_query: bool,
}

impl QueryBuilder {
    pub(crate) fn new(url: String) -> Self {
        let has_query = url.contains('?');
        Self { url, has_query }
    }

    pub(crate) fn push(&mut self, name: &str, value: impl std::fmt::Display) {
        self.url.push(if self.has_query { '&' } else { '?' });
        self.has_query = true;
        self.url.push_str(name);
        self.url.push('=');
        self.url.push_str(&urlencoding::encode(&value.to_string()));
    }

    pub(crate) fn pagination(&mut self, limit: Option<u32>, cursor: Option<&str>) {
        if let Some(limit) = limit {
            self.push("limit", limit);
        }
        if let Some(cursor) = cursor {
            self.push("cursor", cursor);
        }
    }

    pub(crate) fn finish(self) -> String {
        self.url
    }
}

#[cfg(test)]
mod tests;
