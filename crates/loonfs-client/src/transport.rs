//! Builds HTTP requests, applies retry limits, and converts responses into
//! client errors.

use crate::{Client, ClientError, PayloadStream, Result};
use bytes::Bytes;
use futures::StreamExt as _;
use loonfs_api::{transport_retry_backoff, ApiError, ErrorCode, OperationDeadline};
pub(crate) use loonfs_api::{MonotonicTimer, StdMonotonicTimer, TransportRetryPolicy};
use reqwest::Method;
use std::time::Duration;

/// Retry limits for a request that is safe to send more than once.
pub(crate) const DEFAULT: TransportRetryPolicy = TransportRetryPolicy {
    max_retries: 3,
    initial_backoff: Duration::from_millis(250),
    max_backoff: Duration::from_secs(2),
    operation_deadline: Duration::from_secs(90),
};

/// Socket read/write inactivity timeout applied to every request. A
/// connection that makes no progress for this long fails instead of hanging
/// the caller forever.
pub(crate) const IO_INACTIVITY_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SendPolicy {
    Once,
    Retry,
    RetryUnbounded,
}

/// Reusable description of an outbound request.
///
/// `reqwest::RequestBuilder` is consumed when sent. Storing the method, URL,
/// and headers lets each retry build a fresh request with identical bytes.
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
        self.send_streamed(request, body, size_bytes)
            .await
            .map(|response| response.bytes)
            .map_err(|attempt| attempt.error)
    }

    /// Sends one request and returns a streamed response body.
    ///
    /// The method does not retry because the caller may already have consumed
    /// part of the response. Non-success responses are buffered only long enough
    /// to decode the small error body.
    pub(crate) async fn call_for_response_stream(
        &self,
        request: &WireRequest,
    ) -> Result<PayloadStream> {
        #[cfg(test)]
        if let Some(outcome) = test_transport::next(request) {
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

    pub(crate) async fn call(
        &self,
        request: &WireRequest,
        body: Option<&Bytes>,
        policy: SendPolicy,
    ) -> Result<WireResponse> {
        match policy {
            SendPolicy::Once => self
                .send(request, body, None)
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
    ) -> Result<WireResponse> {
        let mut retries = 0;
        let mut attempt_timeout = deadline.as_ref().map(OperationDeadline::deadline);
        loop {
            let attempt = match self.send(request, body, attempt_timeout).await {
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
            transport_retry_pause(backoff).await;
            attempt_timeout = match deadline.as_ref() {
                Some(deadline) => match deadline.remaining() {
                    Some(remaining) => Some(remaining),
                    None => return Err(attempt.error),
                },
                None => None,
            };
        }
    }

    /// Sends one request and reads its response body.
    ///
    /// A non-success HTTP status is a server response, not a network failure.
    /// The retry policy handles those two failure types differently.
    async fn send(
        &self,
        request: &WireRequest,
        body: Option<&Bytes>,
        attempt_timeout: Option<Duration>,
    ) -> std::result::Result<WireResponse, FailedAttempt> {
        #[cfg(test)]
        if let Some(outcome) = test_transport::next(request) {
            return outcome;
        }
        let mut builder = self.build(request);
        if let Some(bytes) = body {
            // Cloning a `Bytes` shares the allocation rather than copying
            // it. That is what keeps a part upload's memory equal to the
            // part the caller is already holding, instead of twice it.
            builder = builder.body(bytes.clone());
        }
        if let Some(attempt_timeout) = attempt_timeout {
            let attempt_timeout = self.request_timeout.map_or(attempt_timeout, |configured| {
                configured.min(attempt_timeout)
            });
            builder = builder.timeout(attempt_timeout);
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
        if let Some(outcome) = test_transport::next(request) {
            // Drain the body so a scripted test still observes the source
            // being read exactly once, as a real send would, and record the
            // pieces it arrived in so a test can hold the client to sending
            // a payload it never assembled.
            let mut body = body;
            let mut chunks = Vec::new();
            while let Some(chunk) = futures::StreamExt::next(&mut body).await {
                chunks.push(chunk.map_or(0, |bytes| bytes.len()));
            }
            test_transport::record_streamed_body(chunks);
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
                builder = builder.bearer_auth(token.expose());
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
#[derive(Debug)]
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
        Ok(body) => ClientError::from_api_error(status, body),
        // A status with a non-envelope body is most commonly an intermediary
        // answering for the server (a load balancer's HTML 502): keep the
        // status — it is the only signal the response carried.
        Err(err) => ClientError::Http(format!(
            "http status {status} with a non-envelope body: {err}"
        )),
    }
}

/// Formats a request failure with its underlying connection or DNS error.
/// Reqwest's default message omits this detail, which operators need to
/// distinguish an unavailable server from an incorrect `server_url`.
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

// This delay affects only local retry scheduling. It is not stored or exposed
// through the protocol.
#[allow(clippy::disallowed_methods)]
async fn transport_retry_pause(backoff: Duration) {
    tokio::time::sleep(backoff).await;
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
pub(crate) mod test_transport {
    use super::{FailedAttempt, WireRequest, WireResponse};
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
        /// What each attempt asked for, so a test can hold the client to the
        /// request it made and not only to the answer it accepted.
        sent: Vec<SentRequest>,
    }

    /// One request a scripted attempt carried.
    #[derive(Debug, Clone)]
    pub(crate) struct SentRequest {
        pub url: String,
        pub headers: Vec<(String, String)>,
        /// Sizes of the body pieces a streamed send read, in order. Empty
        /// for a bodyless or buffered request.
        ///
        /// This is how a test holds the client to *how* it sent a payload
        /// rather than only that it sent one: a body that arrived in many
        /// bounded pieces was never assembled whole anywhere.
        pub body_chunks: Vec<usize>,
    }

    impl SentRequest {
        pub(crate) fn header(&self, name: &str) -> Option<&str> {
            self.headers
                .iter()
                .find(|(header, _)| header.eq_ignore_ascii_case(name))
                .map(|(_, value)| value.as_str())
        }

        /// Total bytes the streamed body carried.
        pub(crate) fn body_bytes(&self) -> usize {
            self.body_chunks.iter().sum()
        }

        /// The largest single piece the body arrived in — the upper bound on
        /// what any one allocation held.
        pub(crate) fn largest_body_chunk(&self) -> usize {
            self.body_chunks.iter().copied().max().unwrap_or(0)
        }
    }

    /// Records the body a streamed send read, against the request that
    /// carried it.
    pub(super) fn record_streamed_body(chunks: Vec<usize>) {
        STATE.with(|state| {
            if let Some(state) = state.borrow_mut().as_mut() {
                if let Some(sent) = state.sent.last_mut() {
                    sent.body_chunks = chunks;
                }
            }
        });
    }

    // A thread-local is sound here because client tests run on the default
    // current-thread runtime: the future is polled only on the thread that
    // installed the scripted transport, so no attempt can observe another
    // test's script.
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

        /// Every request the client sent, in order.
        pub(crate) fn sent(&self) -> Vec<SentRequest> {
            STATE.with(|state| {
                state
                    .borrow()
                    .as_ref()
                    .expect("test transport should be installed")
                    .sent
                    .clone()
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
                sent: Vec::new(),
            });
            assert!(replaced.is_none(), "test transport already installed");
        });
        Guard
    }

    pub(super) fn next(request: &WireRequest) -> Option<Result<WireResponse, FailedAttempt>> {
        STATE.with(|state| {
            let mut state = state.borrow_mut();
            let state = state.as_mut()?;
            state.attempts += 1;
            state.sent.push(SentRequest {
                url: request.url.clone(),
                headers: request.headers.clone(),
                body_chunks: Vec::new(),
            });
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
    use super::*;
    use crate::ClientConfig;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

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

    /// Test clock that reports an expired deadline after its first read.
    #[derive(Debug, Default)]
    struct ExpiredAfterFirstRead {
        reads: AtomicU64,
    }

    impl MonotonicTimer for ExpiredAfterFirstRead {
        fn monotonic_now_ms(&self) -> u64 {
            if self.reads.fetch_add(1, Ordering::SeqCst) == 0 {
                0
            } else {
                100
            }
        }
    }

    fn deadline_client() -> Client {
        let mut client = Client::new(ClientConfig {
            server_url: "http://example.invalid".to_owned(),
            auth_token: None,
            request_timeout_ms: None,
            disable_transient_retry: false,
            ca_cert_path: None,
        })
        .expect("valid client config");
        client.transport_retry = TransportRetryPolicy {
            max_retries: 1,
            initial_backoff: Duration::ZERO,
            max_backoff: Duration::ZERO,
            operation_deadline: Duration::from_millis(100),
        };
        client.timer = Arc::new(ExpiredAfterFirstRead::default());
        client
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
            connect.contains("http://127.0.0.1:9/v0/namespaces"),
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
        assert!(rendered.contains("http://h/v0"), "{rendered}");
        assert!(rendered.contains("outer: inner detail"), "{rendered}");
        assert_eq!(rendered.matches("inner detail").count(), 1, "{rendered}");
    }

    #[tokio::test]
    async fn transport_retries_stop_when_the_operation_deadline_is_spent() {
        let client = deadline_client();
        let transport = test_transport::failures(1);

        client
            .call(
                &client.get("http://example.invalid/control"),
                None,
                SendPolicy::Retry,
            )
            .await
            .expect_err("the spent deadline must preserve the first failure");

        assert_eq!(transport.attempts(), 1);
    }

    #[tokio::test]
    async fn content_transfer_retries_are_exempt_from_the_operation_deadline() {
        let client = deadline_client();
        let transport = test_transport::failure_then_success(b"content".to_vec());

        let response = client
            .call(
                &client.get("http://example.invalid/content"),
                None,
                SendPolicy::RetryUnbounded,
            )
            .await
            .expect("the count-bounded content retry succeeds");

        assert_eq!(response.bytes, b"content");
        assert!(transport.attempts() >= 2, "{}", transport.attempts());
    }
}
