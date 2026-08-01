//! The HTTP transport: request building, the bounded transient retry, and
//! response-to-error mapping.

use crate::{Client, ClientError, PayloadStream, Result};
use bytes::Bytes;
use futures::StreamExt as _;
use loonfs_api::{ApiError, ErrorCode};
use reqwest::Method;
use std::time::Duration;

/// Cap on attempts for transient-error retry: one initial try plus three
/// retries, sleeping with doubling backoff in between.
pub(crate) const MAX_TRANSIENT_ATTEMPTS: u32 = 4;
/// First transient-retry sleep; doubles per retry up to
/// [`MAX_TRANSIENT_RETRY_DELAY`].
pub(crate) const INITIAL_TRANSIENT_RETRY_DELAY: Duration = Duration::from_millis(250);
/// Ceiling for one transient-retry sleep.
pub(crate) const MAX_TRANSIENT_RETRY_DELAY: Duration = Duration::from_secs(2);

/// Socket read/write inactivity timeout applied to every request. A
/// connection that makes no progress for this long fails instead of hanging
/// the caller forever.
pub(crate) const IO_INACTIVITY_TIMEOUT: Duration = Duration::from_secs(60);

/// One outbound request, described rather than built.
///
/// A retry must resend byte-identical content, and a `reqwest::RequestBuilder`
/// is consumed by sending. Keeping the description lets every attempt build a
/// fresh builder from the same parts.
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
    pub(crate) fn get(&self, url: &str) -> WireRequest {
        WireRequest::to_server(Method::GET, url)
    }

    pub(crate) fn post(&self, url: &str) -> WireRequest {
        WireRequest::to_server(Method::POST, url)
    }

    pub(crate) fn put(&self, url: &str) -> WireRequest {
        WireRequest::to_server(Method::PUT, url)
    }

    pub(crate) fn delete(&self, url: &str) -> WireRequest {
        WireRequest::to_server(Method::DELETE, url)
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
    ) -> Result<Resp>
    where
        Req: serde::Serialize,
        Resp: serde::de::DeserializeOwned,
    {
        self.request_json_inner(request, body, true).await
    }

    pub(crate) async fn request_json_once<Req, Resp>(
        &self,
        request: WireRequest,
        body: Option<&Req>,
    ) -> Result<Resp>
    where
        Req: serde::Serialize,
        Resp: serde::de::DeserializeOwned,
    {
        self.request_json_inner(request, body, false).await
    }

    async fn request_json_inner<Req, Resp>(
        &self,
        request: WireRequest,
        body: Option<&Req>,
        retry: bool,
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
        let bytes = if retry {
            self.call_with_transient_retry(&request, body.as_ref())
                .await?
        } else {
            self.call_once(&request, body.as_ref()).await?
        };
        serde_json::from_slice(&bytes).map_err(|err| ClientError::Json(err.to_string()))
    }

    pub(crate) async fn request_bytes(&self, url: &str) -> Result<Vec<u8>> {
        let request = self.get(url);
        self.call_with_transient_retry(&request, None).await
    }

    /// Sends one request without retrying. Callers use this path when an
    /// ambiguous success cannot be reconciled through durable replay state.
    pub(crate) async fn call_once(
        &self,
        request: &WireRequest,
        body: Option<&Bytes>,
    ) -> Result<Vec<u8>> {
        self.send(request, body)
            .await
            .map(|response| response.bytes)
            .map_err(|attempt| attempt.error)
    }

    /// Sends one request whose body arrives in pieces, and never resends it.
    ///
    /// A stream is consumed by the attempt that reads it, so there is no
    /// second attempt to make: this is the one call whose failure is always
    /// the caller's to handle. `size_bytes` declares the length when the
    /// source knows it, and its absence is what puts the request on chunked
    /// transfer encoding — which is the honest framing for a payload whose
    /// length nobody knows yet.
    pub(crate) async fn call_streamed_once(
        &self,
        request: &WireRequest,
        body: PayloadStream,
        size_bytes: Option<u64>,
    ) -> Result<Vec<u8>> {
        self.send_streamed(request, body, size_bytes)
            .await
            .map(|response| response.bytes)
            .map_err(|attempt| attempt.error)
    }

    /// Sends one request and hands its response body back as a stream, so
    /// the answer is never held whole.
    ///
    /// This is the read counterpart of [`Self::call_streamed_once`], and it
    /// does not retry for the same reason: the caller consumes what arrives
    /// as it arrives, so only the caller knows what it already did with the
    /// first half. A non-success status is read and mapped here instead —
    /// an error body is small, and it is the one response worth holding.
    pub(crate) async fn call_for_response_stream(
        &self,
        request: &WireRequest,
    ) -> Result<PayloadStream> {
        #[cfg(test)]
        if let Some(outcome) = test_transport::next() {
            return match outcome {
                Ok(response) => {
                    Ok(
                        futures::stream::once(async move { Ok(Bytes::from(response.bytes)) })
                            .boxed(),
                    )
                }
                Err(attempt) => Err(attempt.error),
            };
        }
        let response = self
            .build(request)
            .send()
            .await
            .map_err(|err| ClientError::Http(describe_send_error(&request.url, &err)))?;
        let status = response.status();
        if !status.is_success() {
            let bytes = response
                .bytes()
                .await
                .map_err(|err| ClientError::Http(describe_send_error(&request.url, &err)))?;
            return Err(map_status_error(status.as_u16(), &bytes));
        }
        Ok(response
            .bytes_stream()
            .map(|chunk| {
                chunk.map_err(|err| {
                    std::io::Error::other(format!("response body ended early: {err}"))
                })
            })
            .boxed())
    }

    /// Sends one request, resending on quick-clearing transient failures —
    /// the retryable-unavailability codes (`server_busy`,
    /// `commit_queue_full`, `shutting_down`) and network-level transport
    /// errors — with doubling backoff, bounded by
    /// [`MAX_TRANSIENT_ATTEMPTS`]. Call sites opt into this path only for
    /// reads, commits (which carry a durable replay identity), and operations whose
    /// repeat semantics are idempotent. Lifecycle mutations and upload
    /// session creation use [`Self::call_once`] so ambiguous success remains
    /// visible to the caller. Other unavailability codes are excluded on
    /// purpose — `maintenance_required` and `index_lagging` clear on a
    /// maintenance step, not on a resend — and a served status with a
    /// non-envelope body is not retried: only failures the network layer
    /// itself reported count as transport.
    pub(crate) async fn call_with_transient_retry(
        &self,
        request: &WireRequest,
        body: Option<&Bytes>,
    ) -> Result<Vec<u8>> {
        self.call_with_transient_retry_headers(request, body)
            .await
            .map(|response| response.bytes)
    }

    /// [`Self::call_with_transient_retry`], keeping the response headers.
    ///
    /// A multipart part upload is the one call whose answer lives in a
    /// header: the provider's etag for the accepted part, which the client
    /// carries to completion.
    pub(crate) async fn call_with_transient_retry_headers(
        &self,
        request: &WireRequest,
        body: Option<&Bytes>,
    ) -> Result<WireResponse> {
        let mut attempts = 0;
        loop {
            let attempt = match self.send(request, body).await {
                Ok(response) => return Ok(response),
                Err(attempt) => attempt,
            };
            attempts += 1;
            let transient = transient_failure(attempt.transport, &attempt.error);
            if !self.transient_retry || !transient || attempts >= MAX_TRANSIENT_ATTEMPTS {
                return Err(attempt.error);
            }
            transient_retry_pause(transient_retry_backoff(attempts)).await;
        }
    }

    /// One attempt: build, send, and read the body. A served non-success
    /// status is an error but not a transport failure — that distinction is
    /// what the retry policy keys on.
    async fn send(
        &self,
        request: &WireRequest,
        body: Option<&Bytes>,
    ) -> std::result::Result<WireResponse, FailedAttempt> {
        #[cfg(test)]
        if let Some(outcome) = test_transport::next() {
            return outcome;
        }
        let mut builder = self.build(request);
        if let Some(bytes) = body {
            // Cloning a `Bytes` shares the allocation rather than copying
            // it. That is what keeps a part upload's memory equal to the
            // part the caller is already holding, instead of twice it.
            builder = builder.body(bytes.clone());
        }
        self.dispatch(request, builder).await
    }

    /// [`Self::send`] for a body that arrives in pieces.
    async fn send_streamed(
        &self,
        request: &WireRequest,
        body: PayloadStream,
        size_bytes: Option<u64>,
    ) -> std::result::Result<WireResponse, FailedAttempt> {
        #[cfg(test)]
        if let Some(outcome) = test_transport::next() {
            // Drain the body so a scripted test still observes the source
            // being read exactly once, as a real send would.
            let mut body = body;
            while futures::StreamExt::next(&mut body).await.is_some() {}
            return outcome;
        }
        let mut builder = self.build(request).body(reqwest::Body::wrap_stream(body));
        if let Some(size_bytes) = size_bytes {
            builder = builder.header(http::header::CONTENT_LENGTH, size_bytes);
        }
        self.dispatch(request, builder).await
    }

    /// The parts of a request that do not depend on its body.
    fn build(&self, request: &WireRequest) -> reqwest::RequestBuilder {
        let mut builder = self.http.request(request.method.clone(), &request.url);
        if request.authenticate {
            if let Some(token) = &self.auth_token {
                builder = builder.bearer_auth(token);
            }
        }
        for (name, value) in &request.headers {
            builder = builder.header(name.as_str(), value.as_str());
        }
        builder
    }

    /// Sends a built request and reads its response.
    async fn dispatch(
        &self,
        request: &WireRequest,
        builder: reqwest::RequestBuilder,
    ) -> std::result::Result<WireResponse, FailedAttempt> {
        let response = builder.send().await.map_err(|err| FailedAttempt {
            transport: true,
            error: ClientError::Http(describe_send_error(&request.url, &err)),
        })?;
        let status = response.status();
        let headers = response.headers().clone();
        let bytes = response.bytes().await.map_err(|err| FailedAttempt {
            // The status arrived but the body did not: the connection failed
            // mid-response, which is a transport failure like any other.
            transport: true,
            error: ClientError::Http(describe_send_error(&request.url, &err)),
        })?;
        if status.is_success() {
            return Ok(WireResponse {
                headers,
                bytes: bytes.to_vec(),
            });
        }
        Err(FailedAttempt {
            transport: false,
            error: map_status_error(status.as_u16(), &bytes),
        })
    }
}

/// One served response: the body, plus the headers for the call that needs
/// them.
pub(crate) struct WireResponse {
    headers: reqwest::header::HeaderMap,
    pub(crate) bytes: Vec<u8>,
}

impl WireResponse {
    pub(crate) fn get(
        &self,
        name: reqwest::header::HeaderName,
    ) -> Option<&reqwest::header::HeaderValue> {
        self.headers.get(name)
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
        Ok(body) => ClientError::Api {
            status,
            code: body.code,
            feature: body.feature,
            message: body.message,
            request_id: body.request_id,
            details: body.details,
        },
        // A status with a non-envelope body is most commonly an intermediary
        // answering for the server (a load balancer's HTML 502): keep the
        // status — it is the only signal the response carried.
        Err(err) => ClientError::Http(format!(
            "http status {status} with a non-envelope body: {err}"
        )),
    }
}

/// Renders a request-send failure with its root cause visible. Reqwest's
/// own `Display` stops at "error sending request", hiding the
/// connection-refused or DNS cause underneath — the one line that tells an
/// operator whether the server is down or `server_url` points at the wrong
/// place.
fn describe_send_error(url: &str, error: &reqwest::Error) -> String {
    render_send_error(url, error, error.is_connect(), error.is_timeout())
}

/// [`describe_send_error`] with the reqwest classification lifted out, so
/// the composition is testable without manufacturing reqwest errors.
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

/// The retry policy for one failed attempt: `transport` is whether the
/// network layer itself reported the failure (classified before the error
/// is flattened by `map_status_error`), and served envelopes retry only on
/// the retryable-unavailability codes.
pub(crate) fn transient_failure(transport: bool, error: &ClientError) -> bool {
    transport
        || matches!(
            error,
            ClientError::Api { code, .. }
                if code == ErrorCode::ServerBusy.as_str()
                    || code == ErrorCode::CommitQueueFull.as_str()
                    || code == ErrorCode::ShuttingDown.as_str()
        )
}

/// Deterministic doubling, the same shape as the object-store transport
/// retry: workspace policy avoids ambient randomness, and a bounded
/// per-request retry does not need jitter.
fn transient_retry_backoff(attempt: u32) -> Duration {
    let doublings = attempt.saturating_sub(1).min(16);
    INITIAL_TRANSIENT_RETRY_DELAY
        .saturating_mul(1u32 << doublings)
        .min(MAX_TRANSIENT_RETRY_DELAY)
}

#[allow(clippy::disallowed_methods)]
// The client's own retry pacing between bounded attempts of one request; no
// protocol time depends on it and replay never observes it.
async fn transient_retry_pause(backoff: Duration) {
    tokio::time::sleep(backoff).await;
}

#[cfg(test)]
pub(crate) mod test_transport {
    use super::{FailedAttempt, WireResponse};
    use crate::ClientError;
    use std::cell::RefCell;
    use std::collections::VecDeque;

    /// One response a scripted transport serves, in order.
    pub(crate) enum Outcome {
        TransportFailure,
        Success(Vec<u8>),
        /// A provider's answer to a part upload, whose result is its etag
        /// header rather than its body.
        PartAccepted(String),
    }

    struct State {
        outcomes: VecDeque<Outcome>,
        attempts: usize,
    }

    // A thread-local is sound here because client tests run on the default
    // current-thread runtime: the future is polled only on the thread that
    // installed the seam, so no attempt can observe another test's script.
    thread_local! {
        static STATE: RefCell<Option<State>> = const { RefCell::new(None) };
    }

    pub(crate) struct Guard;

    impl Guard {
        pub(crate) fn attempts(&self) -> usize {
            STATE.with(|state| {
                state
                    .borrow()
                    .as_ref()
                    .expect("test transport should be installed")
                    .attempts
            })
        }
    }

    impl Drop for Guard {
        fn drop(&mut self) {
            STATE.with(|state| *state.borrow_mut() = None);
        }
    }

    pub(crate) fn failures(count: usize) -> Guard {
        install(std::iter::repeat_with(|| Outcome::TransportFailure).take(count))
    }

    pub(crate) fn failure_then_success(body: Vec<u8>) -> Guard {
        install([Outcome::TransportFailure, Outcome::Success(body)])
    }

    /// Serves a scripted conversation: one outcome per request the client
    /// makes, in the order it makes them.
    pub(crate) fn script(outcomes: impl IntoIterator<Item = Outcome>) -> Guard {
        install(outcomes)
    }

    fn install(outcomes: impl IntoIterator<Item = Outcome>) -> Guard {
        STATE.with(|state| {
            let replaced = state.borrow_mut().replace(State {
                outcomes: outcomes.into_iter().collect(),
                attempts: 0,
            });
            assert!(replaced.is_none(), "test transport already installed");
        });
        Guard
    }

    pub(super) fn next() -> Option<Result<WireResponse, FailedAttempt>> {
        STATE.with(|state| {
            let mut state = state.borrow_mut();
            let state = state.as_mut()?;
            state.attempts += 1;
            let outcome = state
                .outcomes
                .pop_front()
                .expect("test transport exhausted before client stopped sending");
            Some(match outcome {
                Outcome::TransportFailure => Err(FailedAttempt {
                    transport: true,
                    error: ClientError::Http("injected transport failure".to_owned()),
                }),
                Outcome::Success(bytes) => Ok(WireResponse {
                    headers: reqwest::header::HeaderMap::new(),
                    bytes,
                }),
                Outcome::PartAccepted(etag) => {
                    let mut headers = reqwest::header::HeaderMap::new();
                    headers.insert(
                        reqwest::header::ETAG,
                        etag.parse().expect("etag is a valid header value"),
                    );
                    Ok(WireResponse {
                        headers,
                        bytes: Vec::new(),
                    })
                }
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::render_send_error;

    #[derive(Debug)]
    struct Layered {
        message: &'static str,
        cause: Option<Box<Layered>>,
    }

    impl std::fmt::Display for Layered {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(self.message)
        }
    }

    impl std::error::Error for Layered {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            self.cause
                .as_deref()
                .map(|cause| cause as &(dyn std::error::Error + 'static))
        }
    }

    #[test]
    fn send_errors_surface_the_root_cause_and_the_url() {
        let error = Layered {
            message: "error sending request",
            cause: Some(Box::new(Layered {
                message: "client error (Connect)",
                cause: Some(Box::new(Layered {
                    message: "tcp connect error: Connection refused (os error 61)",
                    cause: None,
                })),
            })),
        };

        let connect = render_send_error("http://127.0.0.1:9/v0/namespaces", &error, true, false);
        assert!(
            connect.contains("cannot connect to `http://127.0.0.1:9/v0/namespaces`"),
            "{connect}"
        );
        assert!(connect.contains("Connection refused"), "{connect}");
        assert!(connect.contains("`server_url`"), "{connect}");

        let timeout = render_send_error("http://h/v0", &error, false, true);
        assert!(timeout.contains("timed out"), "{timeout}");

        // A layer that restates its child is not repeated.
        let repeated = Layered {
            message: "outer: inner detail",
            cause: Some(Box::new(Layered {
                message: "inner detail",
                cause: None,
            })),
        };
        let rendered = render_send_error("http://h/v0", &repeated, false, false);
        assert_eq!(
            rendered,
            "request to `http://h/v0` failed: outer: inner detail"
        );
    }
}
