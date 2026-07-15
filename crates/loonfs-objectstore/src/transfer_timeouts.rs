//! Transfer-aware timeouts for provider HTTP requests.
//!
//! A blanket total-request timeout is the wrong shape for payload transfers:
//! it turns "the payload is larger than the window allows at this link speed"
//! into a deterministic failure that retries cannot fix. The connector here
//! replaces the provider client's total-request timeout with two
//! progress-shaped bounds:
//!
//! - The request phase (connect, request-body upload, response headers) gets
//!   [`request_phase_bound`]: the base attempt timeout plus time for the
//!   request body at the per-request transfer floor. Control-plane requests
//!   carry no or tiny bodies, so their bound stays the base attempt timeout;
//!   payload-bearing requests get a bound proportional to what they carry.
//! - Response bodies get an idle bound: every body frame must arrive within
//!   the base attempt timeout of the previous one. Bytes moving means the
//!   transfer is alive; a body that stalls is cut without putting a total
//!   clock on how long a large download may take.
//!
//! Timeouts surface as [`HttpErrorKind::Timeout`], which the provider client
//! retries exactly where it would retry its own request timeouts.

use crate::provider_object_store::{
    request_phase_bound, PROVIDER_ATTEMPT_TIMEOUT, PROVIDER_REQUEST_TRANSFER_FLOOR_BYTES_PER_SEC,
};
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

/// The stalled-transfer diagnostic carried by synthesized timeout errors: it
/// names the phase, the bytes involved, and the enforced bound, so a timeout
/// on a payload transfer is attributable to size and link speed instead of
/// reading as an anonymous request failure.
#[derive(Debug, Error)]
pub(crate) enum TransferTimeoutError {
    #[error(
        "request phase exceeded its transfer bound of {bound_secs}s \
         (request body {request_body_bytes} bytes, floor {floor_bytes_per_sec} bytes/s)"
    )]
    RequestPhase {
        request_body_bytes: u64,
        bound_secs: u64,
        floor_bytes_per_sec: u64,
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
                        floor_bytes_per_sec: PROVIDER_REQUEST_TRANSFER_FLOOR_BYTES_PER_SEC,
                    },
                ))
            })?;

        let (parts, body) = response.into_parts();
        let body = IdleDeadlineBody::new(body, PROVIDER_ATTEMPT_TIMEOUT);
        Ok(HttpResponse::from_parts(parts, HttpResponseBody::new(body)))
    }
}

async fn request_phase_timeout<T>(bound: Duration, request: impl Future<Output = T>) -> Option<T> {
    // Dropping the timed-out future aborts the spawned provider request; the
    // caller's retry layers own what happens next.
    tokio::time::timeout(bound, request).await.ok()
}

/// Response body enforcing an idle bound between frames: progress resets the
/// clock, a stall past the bound fails the body with a retry-classifiable
/// timeout. There is deliberately no total clock here — a large download is
/// healthy for as long as bytes keep arriving.
struct IdleDeadlineBody {
    inner: HttpResponseBody,
    idle_bound: Duration,
    idle_sleep: Pin<Box<tokio::time::Sleep>>,
    received_bytes: u64,
    timed_out: bool,
}

impl IdleDeadlineBody {
    #[allow(clippy::disallowed_methods)]
    fn new(inner: HttpResponseBody, idle_bound: Duration) -> Self {
        // The idle clock intentionally uses an isolated async timer; it is
        // armed here and re-armed on every frame, so it only ever measures
        // the gap since the last observed progress.
        Self {
            inner,
            idle_bound,
            idle_sleep: Box::pin(tokio::time::sleep(idle_bound)),
            received_bytes: 0,
            timed_out: false,
        }
    }
}

impl Body for IdleDeadlineBody {
    type Data = Bytes;
    type Error = HttpError;

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
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Ready(other) => Poll::Ready(other),
            Poll::Pending => {
                if this.idle_sleep.as_mut().poll(cx).is_ready() {
                    this.timed_out = true;
                    return Poll::Ready(Some(Err(HttpError::new(
                        HttpErrorKind::Timeout,
                        TransferTimeoutError::ResponseBodyIdle {
                            received_bytes: this.received_bytes,
                            idle_secs: this.idle_bound.as_secs(),
                        },
                    ))));
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
                .expect("frames")
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
            let (sleep, _) = this.armed.as_mut().expect("armed frame");
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
    async fn large_request_bound_scales_with_the_request_body() {
        // 64 MiB at the request floor adds 1024s to the base bound; a
        // response arriving after the base bound alone must succeed.
        let service = service_with(
            PROVIDER_ATTEMPT_TIMEOUT + Duration::from_secs(60),
            vec![(Duration::ZERO, Bytes::from_static(b"done"))],
        );

        let response = service
            .call(request_with_body(vec![0u8; 64 * 1024 * 1024]))
            .await
            .expect("scaled bound admits the slow response");
        let body = response.into_body().bytes().await.expect("body");
        assert_eq!(body, "done");
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
}
