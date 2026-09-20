//! Opt-in, fixed-size content-read attribution, consumed by the host's existing
//! request logger. No identities, keys, URLs, payloads or error text are retained.
//! Timings are wall time at the adapter boundary, not provider service time.
//! Logical-call, chunk and provider intervals overlap; do not add them together.
//! An attempt is a connector invocation, not a TCP connection/retransmission.
//! Method counts distinguish range-fallback HEADs from repeated GETs. Additional
//! attempts alone do not prove retry backoff, throttling or rate limiting.

use object_store::client::{HttpError, HttpErrorKind};
use serde::Serialize;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

tokio::task_local! {
    static REQUEST: ContentReadTiming;
    static IO: IoContext;
}

/// The physical source selected by the read view, not inferred from file size.
#[derive(Clone, Copy, Debug, Default, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ContentSource {
    /// No content source was resolved.
    #[default]
    Unresolved,
    /// Bytes retained in the selected WAL view.
    Inline,
    /// An immutable content object.
    Object,
}

/// One logical content-store operation; provider retries remain inside it.
#[derive(Clone, Copy)]
pub enum ContentIo {
    /// Existence/size validation before response creation.
    Head,
    /// A ranged content fetch during stream consumption.
    Get,
}

/// Bounded numeric diagnostics. Durations are microseconds, summed unless
/// named `max`. Counts describe observed boundaries, not inferred retries.
#[derive(Clone, Debug, Default, Serialize)]
pub struct ContentIoTiming {
    /// Logical store calls, including cancelled calls.
    pub calls: u64,
    /// Time inside logical calls, including internal retries/backoff.
    pub elapsed_us: u64,
    /// Logical calls abandoned before returning.
    pub cancelled: u64,
    /// Actual HTTP connector invocations, including provider-client retries.
    pub attempts: u64,
    /// Connector invocations whose HTTP method was HEAD.
    pub head_attempts: u64,
    /// Connector invocations whose HTTP method was GET.
    pub get_attempts: u64,
    /// Attempt start to headers/error/cancellation, including runtime handoff.
    pub headers_us: u64,
    /// Longest individual request-to-headers interval.
    pub max_headers_us: u64,
    /// GET response headers to body completion/error/drop at this boundary.
    pub body_us: u64,
    /// Longest individual GET response-body interval.
    pub max_body_us: u64,
    /// GET bodies consumed to completion.
    pub bodies_completed: u64,
    /// Cancelled HTTP requests or GET bodies dropped before completion.
    pub attempts_cancelled: u64,
    /// Sanitized failure counters; never contains a provider message.
    pub failures: ContentFailures,
}

/// Fixed failure categories. HTTP status counters describe observed responses,
/// not an inference that the provider throttled a request.
#[derive(Clone, Debug, Default, Serialize)]
pub struct ContentFailures {
    /// Connection errors.
    pub connect: u64,
    /// Request transport errors.
    pub request: u64,
    /// Timeout errors (including the unchanged adapter timeouts).
    pub timeout: u64,
    /// Interrupted transport operations.
    pub interrupted: u64,
    /// Response decoding errors.
    pub decode: u64,
    /// Other transport errors.
    pub other: u64,
    /// HTTP 429 responses.
    pub http_429: u64,
    /// Other HTTP 4xx responses.
    pub http_4xx: u64,
    /// HTTP 5xx responses.
    pub http_5xx: u64,
}

/// A snapshot to append to one existing, sanitized request-ID timing record.
#[derive(Clone, Debug, Default, Serialize)]
pub struct ContentReadSnapshot {
    /// Selected physical source.
    pub source: ContentSource,
    /// Resolution time, excluding content HEAD and consumption; absent if unentered.
    pub resolution_us: Option<u64>,
    /// Content HEAD observations.
    pub head: ContentIoTiming,
    /// Logical content GET observations (may include a provider range fallback HEAD).
    pub get: ContentIoTiming,
    /// Calls awaiting the next verified chunk, including the final EOF check.
    pub chunk_calls: u64,
    /// Total elapsed time awaiting chunks; includes GET and verification time.
    pub chunk_wait_us: u64,
    /// Number of final checksum computations, including failed verification.
    pub verification_calls: u64,
    /// Final checksum computation time, not time hashing earlier chunks.
    pub verification_us: u64,
    /// Response-body polls observed by the host.
    pub body_polls: u64,
    /// Largest interval between host body polls (first interval starts at handoff).
    pub max_body_poll_gap_us: u64,
}

#[derive(Default)]
struct State {
    snapshot: ContentReadSnapshot,
    resolution_started: Option<Instant>,
}

/// Optional per-request accumulator. A disabled/default handle allocates nothing.
/// Scope the request future, and capture the handle before spawning work or
/// returning a lazy body. Captured handles remain valid on another runtime.
#[derive(Clone, Default)]
pub struct ContentReadTiming(Option<Arc<Mutex<State>>>);

impl ContentReadTiming {
    /// Enables bounded attribution for one request.
    pub fn new() -> Self {
        Self(Some(Arc::new(Mutex::new(State::default()))))
    }

    /// Captures the request being polled; disabled outside an opt-in scope.
    pub fn current() -> Self {
        REQUEST.try_with(Clone::clone).unwrap_or_default()
    }

    /// Carries this handle through a future's polls, without a global subscriber.
    pub async fn scope<F: Future>(&self, future: F) -> F::Output {
        REQUEST.scope(self.clone(), future).await
    }

    /// Copies only fixed-size counters and timings.
    pub fn snapshot(&self) -> ContentReadSnapshot {
        self.0
            .as_ref()
            .map_or_else(ContentReadSnapshot::default, |state| {
                state.lock().expect("content timing").snapshot.clone()
            })
    }

    fn update(&self, f: impl FnOnce(&mut State)) {
        if let Some(state) = &self.0 {
            f(&mut state.lock().expect("content timing"));
        }
    }

    /// Begins view/content resolution; the stream ends this before its HEAD.
    /// Dropping the guard also records failed/cancelled resolution.
    pub fn resolving(&self) -> ResolutionTimer {
        self.update(|state| state.resolution_started = Some(now()));
        ResolutionTimer(self.clone())
    }

    /// Records the actual source and ends the resolution interval.
    pub fn resolved(&self, source: ContentSource) {
        self.finish_resolution();
        self.update(|state| state.snapshot.source = source);
    }

    fn finish_resolution(&self) {
        self.update(|state| {
            if let Some(started) = state.resolution_started.take() {
                state.snapshot.resolution_us = Some(micros(started.elapsed()));
            }
        });
    }

    /// Times a logical call and carries its identity to every provider attempt.
    pub async fn io<F: Future>(&self, kind: ContentIo, future: F) -> F::Output {
        if self.0.is_none() {
            return future.await;
        }
        let context = IoContext {
            timing: self.clone(),
            kind,
        };
        context.update(|stats| stats.calls += 1);
        let mut timer = IoTimer {
            context: context.clone(),
            started: now(),
            complete: false,
        };
        let output = IO.scope(context, future).await;
        timer.complete = true;
        output
    }

    /// Times one chunk wait, including cancellation and EOF/error verification.
    pub fn chunk(&self) -> ContentTimer {
        self.timer(false)
    }

    /// Times the final checksum computation without changing verification.
    pub fn verification(&self) -> ContentTimer {
        self.timer(true)
    }

    fn timer(&self, verification: bool) -> ContentTimer {
        ContentTimer {
            timing: self.clone(),
            started: self.0.as_ref().map(|_| now()),
            verification,
        }
    }

    /// Records a host poll gap; includes demand/backpressure and scheduling.
    pub fn body_poll(&self, gap: Duration) {
        self.update(|state| {
            state.snapshot.body_polls += 1;
            state.snapshot.max_body_poll_gap_us =
                state.snapshot.max_body_poll_gap_us.max(micros(gap));
        });
    }
}

/// Records resolution even when response creation is cancelled.
pub struct ResolutionTimer(ContentReadTiming);
impl Drop for ResolutionTimer {
    fn drop(&mut self) {
        self.0.finish_resolution();
    }
}

/// A bounded elapsed-time guard, including early return/cancellation.
pub struct ContentTimer {
    timing: ContentReadTiming,
    started: Option<Instant>,
    verification: bool,
}
impl Drop for ContentTimer {
    fn drop(&mut self) {
        if let Some(started) = self.started {
            self.timing.update(|state| {
                if self.verification {
                    state.snapshot.verification_calls += 1;
                    state.snapshot.verification_us += micros(started.elapsed());
                } else {
                    state.snapshot.chunk_calls += 1;
                    state.snapshot.chunk_wait_us += micros(started.elapsed());
                }
            });
        }
    }
}

#[derive(Clone)]
struct IoContext {
    timing: ContentReadTiming,
    kind: ContentIo,
}
impl IoContext {
    fn update(&self, f: impl FnOnce(&mut ContentIoTiming)) {
        self.timing.update(|state| {
            f(match self.kind {
                ContentIo::Head => &mut state.snapshot.head,
                ContentIo::Get => &mut state.snapshot.get,
            })
        });
    }
}

struct IoTimer {
    context: IoContext,
    started: Instant,
    complete: bool,
}
impl Drop for IoTimer {
    fn drop(&mut self) {
        self.context.update(|stats| {
            stats.elapsed_us += micros(self.started.elapsed());
            stats.cancelled += u64::from(!self.complete);
        });
    }
}

pub(crate) struct ProviderAttempt {
    context: Option<IoContext>,
    started: Instant,
    get: bool,
    finished: bool,
}
impl ProviderAttempt {
    pub(crate) fn start(method: &http::Method) -> Self {
        let context = IO.try_with(Clone::clone).ok();
        if let Some(context) = &context {
            context.update(|stats| {
                stats.attempts += 1;
                stats.head_attempts += u64::from(method == http::Method::HEAD);
                stats.get_attempts += u64::from(method == http::Method::GET);
            });
        }
        Self {
            context,
            started: now(),
            get: method == http::Method::GET,
            finished: false,
        }
    }

    pub(crate) fn headers(&mut self, result: Result<u16, &HttpError>) -> ProviderBodyTimer {
        self.finished = true;
        if let Some(context) = &self.context {
            context.update(|stats| {
                let elapsed = micros(self.started.elapsed());
                stats.headers_us += elapsed;
                stats.max_headers_us = stats.max_headers_us.max(elapsed);
                match result {
                    Ok(429) => stats.failures.http_429 += 1,
                    Ok(400..=499) => stats.failures.http_4xx += 1,
                    Ok(500..=599) => stats.failures.http_5xx += 1,
                    Err(error) => failure(&mut stats.failures, error),
                    _ => {}
                }
            });
        }
        ProviderBodyTimer {
            context: if self.get && result.is_ok() {
                self.context.clone()
            } else {
                None
            },
            started: now(),
            finished: false,
        }
    }
}
impl Drop for ProviderAttempt {
    fn drop(&mut self) {
        if !self.finished {
            if let Some(context) = &self.context {
                context.update(|stats| {
                    let elapsed = micros(self.started.elapsed());
                    stats.headers_us += elapsed;
                    stats.max_headers_us = stats.max_headers_us.max(elapsed);
                    stats.attempts_cancelled += 1;
                });
            }
        }
    }
}

pub(crate) struct ProviderBodyTimer {
    context: Option<IoContext>,
    started: Instant,
    finished: bool,
}
impl ProviderBodyTimer {
    pub(crate) fn finish(&mut self, error: Option<&HttpError>) {
        if self.finished {
            return;
        }
        self.finished = true;
        if let Some(context) = &self.context {
            context.update(|stats| {
                if let Some(error) = error {
                    failure(&mut stats.failures, error);
                }
                stats.bodies_completed += u64::from(error.is_none());
            });
        }
        self.record_elapsed();
    }
    fn record_elapsed(&self) {
        if let Some(context) = &self.context {
            context.update(|stats| {
                let elapsed = micros(self.started.elapsed());
                stats.body_us += elapsed;
                stats.max_body_us = stats.max_body_us.max(elapsed);
            });
        }
    }
}
impl Drop for ProviderBodyTimer {
    fn drop(&mut self) {
        if !self.finished {
            self.record_elapsed();
            if let Some(context) = &self.context {
                context.update(|stats| stats.attempts_cancelled += 1);
            }
        }
    }
}

fn failure(counts: &mut ContentFailures, error: &HttpError) {
    match error.kind() {
        HttpErrorKind::Connect => counts.connect += 1,
        HttpErrorKind::Request => counts.request += 1,
        HttpErrorKind::Timeout => counts.timeout += 1,
        HttpErrorKind::Interrupted => counts.interrupted += 1,
        HttpErrorKind::Decode => counts.decode += 1,
        _ => counts.other += 1,
    }
}

#[allow(clippy::disallowed_methods)]
fn now() -> Instant {
    Instant::now()
}
fn micros(duration: Duration) -> u64 {
    duration.as_micros().min(u64::MAX as u128) as u64
}

#[cfg(test)]
#[allow(clippy::disallowed_methods)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn response_body_completion_failure_and_cancellation_are_recorded_once() {
        let timing = ContentReadTiming::new();
        timing
            .io(ContentIo::Get, async {
                let mut attempt = ProviderAttempt::start(&http::Method::GET);
                let mut body = attempt.headers(Ok(200));
                body.finish(None);
                body.finish(None);

                let mut attempt = ProviderAttempt::start(&http::Method::GET);
                let mut body = attempt.headers(Ok(200));
                body.finish(Some(&HttpError::new(
                    HttpErrorKind::Decode,
                    std::io::Error::other("private-error"),
                )));

                let mut attempt = ProviderAttempt::start(&http::Method::GET);
                drop(attempt.headers(Ok(200)));
            })
            .await;
        let stats = timing.snapshot().get;
        assert_eq!(stats.attempts, 3);
        assert_eq!(stats.bodies_completed, 1);
        assert_eq!(stats.failures.decode, 1);
        assert_eq!(stats.attempts_cancelled, 1);
        assert!(!serde_json::to_string(&stats)
            .expect("snapshot")
            .contains("private"));
    }

    #[tokio::test]
    async fn resolution_finishes_before_head_and_scopes_do_not_leak() {
        let timing = ContentReadTiming::new();
        timing
            .scope(async {
                let _resolution = ContentReadTiming::current().resolving();
                tokio::time::sleep(Duration::from_millis(10)).await;
                ContentReadTiming::current().resolved(ContentSource::Object);
                let resolved = timing.snapshot().resolution_us;
                timing
                    .io(
                        ContentIo::Head,
                        tokio::time::sleep(Duration::from_millis(20)),
                    )
                    .await;
                assert_eq!(timing.snapshot().resolution_us, resolved);
            })
            .await;
        let snapshot = timing.snapshot();
        assert!(snapshot.resolution_us.expect("resolution") >= 10_000);
        assert!(snapshot.head.elapsed_us >= 20_000);
        assert_eq!(snapshot.head.calls, 1);
        assert_eq!(
            snapshot.head.attempts, 0,
            "a logical call is not an HTTP attempt"
        );
        assert!(ContentReadTiming::current().0.is_none());
        assert!(IO.try_with(Clone::clone).is_err());
    }

    #[tokio::test]
    async fn cancellation_records_partial_work_without_retaining_private_errors() {
        let timing = ContentReadTiming::new();
        let future = timing.io(ContentIo::Get, async {
            let _attempt = ProviderAttempt::start(&http::Method::GET);
            std::future::pending::<()>().await;
        });
        let mut future = Box::pin(future);
        assert!(futures::poll!(&mut future).is_pending());
        drop(future);
        let snapshot = timing.snapshot();
        assert_eq!(snapshot.get.calls, 1);
        assert_eq!(snapshot.get.attempts, 1);
        assert_eq!(snapshot.get.cancelled, 1);
        assert_eq!(snapshot.get.attempts_cancelled, 1);

        timing
            .io(ContentIo::Head, async {
                let mut attempt = ProviderAttempt::start(&http::Method::HEAD);
                attempt.headers(Err(&HttpError::new(
                    HttpErrorKind::Timeout,
                    std::io::Error::other("private-url-and-secret"),
                )));
            })
            .await;
        assert_eq!(timing.snapshot().head.failures.timeout, 1);
        assert!(!serde_json::to_string(&timing.snapshot())
            .expect("snapshot")
            .contains("private"));
    }

    #[tokio::test]
    async fn failed_or_cancelled_resolution_is_not_reported_as_a_resolved_source() {
        let timing = ContentReadTiming::new();
        let guard = timing.resolving();
        drop(guard);
        assert!(timing.snapshot().resolution_us.is_some());
        assert!(matches!(
            timing.snapshot().source,
            ContentSource::Unresolved
        ));
    }
}
