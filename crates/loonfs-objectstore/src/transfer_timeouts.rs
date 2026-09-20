//! Progress-aware timeouts for provider HTTP requests.
//!
//! A total-request timeout makes large transfers fail solely because of their
//! size. This connector instead applies:
//!
//! - a fixed request-phase timeout selected by body size; and
//! - an idle timeout between response-body frames.
//!
//! Large downloads may continue indefinitely while frames keep arriving.
//! Stalls are reported as [`HttpErrorKind::Timeout`] so the provider client
//! can apply its normal retry policy.

use crate::content_timing::{ProviderAttempt, ProviderBodyTimer};
use crate::provider_object_store::{request_phase_bound, PROVIDER_ATTEMPT_TIMEOUT};
use async_trait::async_trait;
use bytes::Bytes;
use http_body::{Body, Frame, SizeHint};
use object_store::client::{
    HttpClient, HttpConnector, HttpError, HttpErrorKind, HttpRequest, HttpResponse,
    HttpResponseBody, HttpService, SpawnedReqwestConnector,
};
use object_store::ClientOptions;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use thiserror::Error;
use tokio::runtime::Handle;

/// Diagnostic for a synthesized timeout, including the transfer phase,
/// byte count, and enforced limit.
#[derive(Debug, Error)]
pub(crate) enum TransferTimeoutError {
    #[error(
        "request phase exceeded its transfer bound of {bound_secs}s \
         (request body {request_body_bytes} bytes)"
    )]
    RequestPhase {
        request_body_bytes: u64,
        bound_secs: u64,
    },
    #[error(
        "response body stalled for {idle_secs}s after {received_bytes} received bytes; \
         aborting the transfer"
    )]
    ResponseBodyIdle { received_bytes: u64, idle_secs: u64 },
}

/// [`HttpConnector`] for provider clients: routes IO onto the store's own
/// runtime (the [`SpawnedReqwestConnector`] behavior) and applies the
/// transfer-aware timeout scheme above instead of the client options'
/// total-request timeout.
#[derive(Debug)]
pub(crate) struct TransferTimeoutConnector {
    runtime_handle: Handle,
}

impl TransferTimeoutConnector {
    pub(crate) fn new(runtime_handle: Handle) -> Self {
        Self { runtime_handle }
    }
}

impl HttpConnector for TransferTimeoutConnector {
    fn connect(&self, options: &ClientOptions) -> object_store::Result<HttpClient> {
        // The total-request timeout is stripped, never merely left unset:
        // this connector owns attempt bounding, and a second total clock
        // underneath it would reintroduce the payload-size failure mode.
        let options = options.clone().with_timeout_disabled();
        let inner = SpawnedReqwestConnector::new(self.runtime_handle.clone()).connect(&options)?;
        Ok(HttpClient::new(TransferTimeoutService { inner }))
    }
}

#[derive(Debug)]
struct TransferTimeoutService {
    inner: HttpClient,
}

#[async_trait]
impl HttpService for TransferTimeoutService {
    async fn call(&self, req: HttpRequest) -> Result<HttpResponse, HttpError> {
        // This boundary is invoked again by the provider client's own retries.
        // Capture before the inner connector crosses onto its IO runtime.
        let mut attempt = ProviderAttempt::start(req.method());
        let request_body_bytes = req.body().content_length() as u64;
        let bound = request_phase_bound(request_body_bytes);
        let response = request_phase_timeout(bound, self.inner.execute(req))
            .await
            .unwrap_or_else(|| {
                Err(HttpError::new(
                    HttpErrorKind::Timeout,
                    TransferTimeoutError::RequestPhase {
                        request_body_bytes,
                        bound_secs: bound.as_secs(),
                    },
                ))
            });
        let timing = attempt.headers(response.as_ref().map(|r| r.status().as_u16()));
        let response = response?;

        let (parts, body) = response.into_parts();
        let body = IdleDeadlineBody::new(body, PROVIDER_ATTEMPT_TIMEOUT, timing);
        Ok(HttpResponse::from_parts(parts, HttpResponseBody::new(body)))
    }
}

async fn request_phase_timeout<T>(bound: Duration, request: impl Future<Output = T>) -> Option<T> {
    // Dropping the timed-out future aborts the spawned provider request; the
    // caller's retry layers own what happens next.
    tokio::time::timeout(bound, request).await.ok()
}

/// Response body with an idle timeout between frames.
///
/// Each received frame resets the timer. There is no total download timeout,
/// so a large response remains valid while data continues to arrive.
struct IdleDeadlineBody {
    inner: HttpResponseBody,
    idle_bound: Duration,
    idle_sleep: Pin<Box<tokio::time::Sleep>>,
    received_bytes: u64,
    timed_out: bool,
    timing: ProviderBodyTimer,
}

impl IdleDeadlineBody {
    #[allow(clippy::disallowed_methods)]
    fn new(inner: HttpResponseBody, idle_bound: Duration, mut timing: ProviderBodyTimer) -> Self {
        // The idle clock intentionally uses an isolated async timer; it is
        // armed here and re-armed on every frame, so it only ever measures
        // the gap since the last observed progress.
        if inner.is_end_stream() {
            timing.finish(None);
        }
        Self {
            inner,
            idle_bound,
            idle_sleep: Box::pin(tokio::time::sleep(idle_bound)),
            received_bytes: 0,
            timed_out: false,
            timing,
        }
    }
}

impl Body for IdleDeadlineBody {
    type Data = Bytes;
    type Error = HttpError;

    #[allow(clippy::disallowed_methods)]
    // Monotonic time is used only to enforce the response-body idle timeout.
    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        if this.timed_out {
            return Poll::Ready(None);
        }
        match Pin::new(&mut this.inner).poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(data) = frame.data_ref() {
                    this.received_bytes += data.len() as u64;
                }
                let deadline = tokio::time::Instant::now() + this.idle_bound;
                this.idle_sleep.as_mut().reset(deadline);
                if this.inner.is_end_stream() {
                    this.timing.finish(None);
                }
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Ready(other) => {
                this.timing
                    .finish(other.as_ref().and_then(|r| r.as_ref().err()));
                Poll::Ready(other)
            }
            Poll::Pending => {
                if this.idle_sleep.as_mut().poll(cx).is_ready() {
                    this.timed_out = true;
                    let error = HttpError::new(
                        HttpErrorKind::Timeout,
                        TransferTimeoutError::ResponseBodyIdle {
                            received_bytes: this.received_bytes,
                            idle_secs: this.idle_bound.as_secs(),
                        },
                    );
                    this.timing.finish(Some(&error));
                    return Poll::Ready(Some(Err(error)));
                }
                Poll::Pending
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        self.timed_out || self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider_object_store::{
        PROVIDER_TRANSFER_ATTEMPT_TIMEOUT, PROVIDER_TRANSFER_BODY_MIN_BYTES,
    };
    use http::Response;
    use std::collections::VecDeque;
    use std::sync::Mutex;

    /// Service double producing a response after a scripted delay, with a
    /// body whose frames each arrive after their own scripted delay.
    #[derive(Debug)]
    struct ScriptedService {
        response_delay: Duration,
        frames: Mutex<Option<VecDeque<(Duration, Bytes)>>>,
    }

    #[async_trait]
    impl HttpService for ScriptedService {
        async fn call(&self, _req: HttpRequest) -> Result<HttpResponse, HttpError> {
            #[allow(clippy::disallowed_methods)]
            tokio::time::sleep(self.response_delay).await;
            let frames = self
                .frames
                .lock()
                .expect("scripted response frames lock should not be poisoned")
                .take()
                .expect("one response per scripted service");
            let body = DelayedFrames {
                frames,
                armed: None,
            };
            Ok(Response::new(HttpResponseBody::new(body)))
        }
    }

    /// Body emitting scripted frames, each once its delay elapses.
    struct DelayedFrames {
        frames: VecDeque<(Duration, Bytes)>,
        armed: Option<(Pin<Box<tokio::time::Sleep>>, Bytes)>,
    }

    impl Body for DelayedFrames {
        type Data = Bytes;
        type Error = HttpError;

        fn poll_frame(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Bytes>, HttpError>>> {
            let this = self.get_mut();
            if this.armed.is_none() {
                let Some((delay, bytes)) = this.frames.pop_front() else {
                    return Poll::Ready(None);
                };
                #[allow(clippy::disallowed_methods)]
                let sleep = Box::pin(tokio::time::sleep(delay));
                this.armed = Some((sleep, bytes));
            }
            let (sleep, _) = this.armed.as_mut().expect("pending frame should be armed");
            match sleep.as_mut().poll(cx) {
                Poll::Ready(()) => {
                    let (_, bytes) = this.armed.take().expect("armed frame");
                    Poll::Ready(Some(Ok(Frame::data(bytes))))
                }
                Poll::Pending => Poll::Pending,
            }
        }
    }

    fn service_with(
        response_delay: Duration,
        frames: Vec<(Duration, Bytes)>,
    ) -> TransferTimeoutService {
        TransferTimeoutService {
            inner: HttpClient::new(ScriptedService {
                response_delay,
                frames: Mutex::new(Some(frames.into())),
            }),
        }
    }

    fn request_with_body(body: impl Into<object_store::client::HttpRequestBody>) -> HttpRequest {
        http::Request::builder()
            .uri("http://provider.invalid/object")
            .body(body.into())
            .expect("request")
    }

    #[tokio::test(start_paused = true)]
    async fn small_request_is_bounded_by_the_base_attempt_timeout() {
        let service = service_with(
            PROVIDER_ATTEMPT_TIMEOUT + Duration::from_secs(1),
            Vec::new(),
        );

        let error = service
            .call(request_with_body(Bytes::from_static(b"tiny")))
            .await
            .expect_err("headers past the base bound must time out");

        assert_eq!(error.kind(), HttpErrorKind::Timeout);
        assert!(error.to_string().contains("transfer bound"));
    }

    #[tokio::test(start_paused = true)]
    async fn payload_request_gets_the_transfer_bound() {
        // A payload-bearing request that needs longer than the control-plane
        // bound must survive up to the transfer bound...
        let service = service_with(
            PROVIDER_ATTEMPT_TIMEOUT + Duration::from_secs(60),
            vec![(Duration::ZERO, Bytes::from_static(b"done"))],
        );
        let response = service
            .call(request_with_body(vec![0u8; 8 * 1024 * 1024]))
            .await
            .expect("transfer bound admits the slow payload request");
        let body = response.into_body().bytes().await.expect("body");
        assert_eq!(body, "done");

        // ...and be cut once the transfer bound itself is exceeded.
        let service = service_with(
            PROVIDER_TRANSFER_ATTEMPT_TIMEOUT + Duration::from_secs(1),
            Vec::new(),
        );
        let error = service
            .call(request_with_body(vec![0u8; 8 * 1024 * 1024]))
            .await
            .expect_err("payload request past the transfer bound must time out");
        assert_eq!(error.kind(), HttpErrorKind::Timeout);
    }

    #[tokio::test(start_paused = true)]
    async fn sub_payload_body_keeps_the_control_plane_bound() {
        let service = service_with(
            PROVIDER_ATTEMPT_TIMEOUT + Duration::from_secs(1),
            Vec::new(),
        );

        let error = service
            .call(request_with_body(vec![
                0u8;
                (PROVIDER_TRANSFER_BODY_MIN_BYTES - 1)
                    as usize
            ]))
            .await
            .expect_err("bodies below the payload cutoff keep the base bound");

        assert_eq!(error.kind(), HttpErrorKind::Timeout);
    }

    #[tokio::test(start_paused = true)]
    async fn response_body_survives_on_progress_without_a_total_clock() {
        // Five frames each arriving just inside the idle bound: total body
        // time far exceeds the base attempt timeout, and the transfer still
        // completes because progress keeps resetting the idle clock.
        let frame_gap = PROVIDER_ATTEMPT_TIMEOUT - Duration::from_secs(1);
        let frames = (0..5)
            .map(|_| (frame_gap, Bytes::from_static(b"chunk")))
            .collect();
        let service = service_with(Duration::ZERO, frames);

        let response = service
            .call(request_with_body(Bytes::new()))
            .await
            .expect("headers are prompt");
        let body = response
            .into_body()
            .bytes()
            .await
            .expect("progressing body");
        assert_eq!(body.len(), 5 * "chunk".len());
    }

    #[tokio::test(start_paused = true)]
    async fn response_body_stall_is_cut_with_a_diagnostic() {
        let frames = vec![
            (Duration::ZERO, Bytes::from_static(b"chunk")),
            (
                PROVIDER_ATTEMPT_TIMEOUT + Duration::from_secs(1),
                Bytes::from_static(b"late"),
            ),
        ];
        let service = service_with(Duration::ZERO, frames);

        let response = service
            .call(request_with_body(Bytes::new()))
            .await
            .expect("headers are prompt");
        let error = response
            .into_body()
            .bytes()
            .await
            .expect_err("stalled body must be cut");

        assert_eq!(error.kind(), HttpErrorKind::Timeout);
        let message = error.to_string();
        assert!(message.contains("stalled"), "unexpected message {message}");
        assert!(
            message.contains("5 received bytes"),
            "diagnostic should carry progress, got {message}"
        );
    }

    #[tokio::test]
    #[allow(clippy::disallowed_methods)]
    async fn attribution_separates_headers_and_body_across_runtime_boundaries() {
        use crate::content_timing::{ContentIo, ContentReadTiming};
        let timing = ContentReadTiming::new();
        let service = service_with(
            Duration::from_millis(20),
            vec![(Duration::from_millis(30), Bytes::from_static(b"chunk"))],
        );
        let other = ContentReadTiming::new();
        timing
            .io(ContentIo::Get, async {
                let response = service
                    .call(request_with_body(Bytes::new()))
                    .await
                    .expect("headers");
                // The observer is carried by the response, not the consuming task's
                // task-local scope (which deliberately belongs to another request).
                let body = tokio::task::spawn_blocking(move || {
                    tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("consumer runtime")
                        .block_on(other.scope(response.into_body().bytes()))
                        .expect("body")
                })
                .await
                .expect("consumer");
                assert_eq!(body, "chunk");
            })
            .await;
        let stats = timing.snapshot().get;
        assert_eq!((stats.calls, stats.attempts, stats.get_attempts), (1, 1, 1));
        assert!(stats.headers_us >= 20_000);
        assert!(stats.body_us >= 30_000);
        assert!(stats.elapsed_us >= stats.headers_us + stats.body_us);
        assert_eq!(stats.bodies_completed, 1);
        assert_eq!(stats.attempts_cancelled, 0);
    }

    #[tokio::test]
    async fn attribution_counts_provider_internal_read_retries() {
        use crate::content_timing::{ContentIo, ContentReadTiming};
        use object_store::ObjectStore as _;
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        };

        #[derive(Debug, Clone)]
        struct RetryService(Arc<AtomicUsize>);
        #[async_trait]
        impl HttpService for RetryService {
            async fn call(&self, req: HttpRequest) -> Result<HttpResponse, HttpError> {
                assert_eq!(req.method(), http::Method::HEAD);
                let status = if self.0.fetch_add(1, Ordering::Relaxed) == 0 {
                    503
                } else {
                    200
                };
                Ok(Response::builder()
                    .status(status)
                    .header("content-length", "5")
                    .header("last-modified", "Wed, 21 Oct 2015 07:28:00 GMT")
                    .header("etag", "test")
                    .body(HttpResponseBody::new(DelayedFrames {
                        frames: VecDeque::new(),
                        armed: None,
                    }))
                    .expect("response"))
            }
        }
        #[derive(Debug)]
        struct Connector(RetryService);
        impl HttpConnector for Connector {
            fn connect(&self, _: &ClientOptions) -> object_store::Result<HttpClient> {
                Ok(HttpClient::new(TransferTimeoutService {
                    inner: HttpClient::new(self.0.clone()),
                }))
            }
        }
        let attempts = Arc::new(AtomicUsize::new(0));
        let store = object_store::aws::AmazonS3Builder::new()
            .with_region("test")
            .with_bucket_name("test")
            .with_access_key_id("test")
            .with_secret_access_key("test")
            .with_endpoint("http://provider.invalid")
            .with_allow_http(true)
            .with_http_connector(Connector(RetryService(attempts.clone())))
            .build()
            .expect("test provider");
        let timing = ContentReadTiming::new();
        let metadata = timing
            .io(ContentIo::Head, store.head(&"fixture".into()))
            .await
            .expect("retried HEAD");
        assert_eq!(metadata.size, 5);
        assert_eq!(attempts.load(Ordering::Relaxed), 2);
        let stats = timing.snapshot().head;
        assert_eq!(
            (stats.calls, stats.attempts, stats.head_attempts),
            (1, 2, 2)
        );
        assert_eq!(stats.failures.http_5xx, 1);
        assert_eq!(stats.attempts_cancelled, 0);
    }
}
