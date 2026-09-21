//! Opt-in, bounded attribution for a single-candidate publication batch.
//!
//! The host scopes a request; admission binds it to publisher-owned work. Exact
//! in-flight duplicates share that work without retaining request IDs or waiter
//! lists here. Counters never affect publication, retries, or cancellation.
//! Timings include nested stages and must not be added together.

use crate::Result;
use bytes::Bytes;
use serde::Serialize;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Instant;

tokio::task_local! {
    static REQUEST: CommitTiming;
    static WORK: CommitWork;
    static IO: Arc<Mutex<[u64; 3]>>;
}

/// Fixed operation boundaries; these labels contain no user input.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
#[repr(usize)]
pub enum Stage {
    /// Admission to batch selection, including pacing and publication capacity.
    Queue,
    /// Waiting for the shared publication semaphore (a subset of queue time).
    PublicationPermit,
    /// Waiting for the namespace engine mutex.
    EngineLock,
    /// Loading the publish metadata view, including a possible tail replay.
    MetadataView,
    /// Rebuilding the metadata tail after its last materialized basis.
    MetadataReplay,
    /// Looking up the durable commit receipt.
    ReceiptLookup,
    /// Reconstructing original events from retained change history.
    ResponseHistory,
    /// The retained WAL walk within response reconstruction, excluding head discovery.
    RetainedHistory,
    /// Reading one WAL object, including provider retries and body consumption.
    WalRead,
    /// Decoding and decompressing a WAL object.
    WalDecode,
    /// Validating a decoded WAL segment.
    WalValidate,
    /// Projecting validated WAL records into metadata.
    WalProject,
    /// Entire publication, including all nested stages and existing retries.
    Publish,
}
const STAGES: [Stage; 13] = [
    Stage::Queue,
    Stage::PublicationPermit,
    Stage::EngineLock,
    Stage::MetadataView,
    Stage::MetadataReplay,
    Stage::ReceiptLookup,
    Stage::ResponseHistory,
    Stage::RetainedHistory,
    Stage::WalRead,
    Stage::WalDecode,
    Stage::WalValidate,
    Stage::WalProject,
    Stage::Publish,
];

/// One stage's inclusive elapsed time, including its currently active interval.
#[derive(Clone, Debug, Serialize)]
pub struct StageSnapshot {
    /// Fixed stage label.
    pub stage: Stage,
    /// Entries into this stage.
    pub calls: u64,
    /// Completed plus currently running wall time in microseconds.
    pub elapsed_us: Option<u64>,
    /// Whether a stage is still in progress at this snapshot.
    pub active: bool,
}

/// WAL reads from two distinct paths; no keys, sequence numbers, or payloads.
#[derive(Clone, Debug, Default, Serialize)]
pub struct WalSnapshot {
    /// Logical WAL reads started, not HTTP dispatches.
    pub reads: u64,
    /// Successful reads that returned a segment.
    pub segments: u64,
    /// Compressed object bytes returned by those reads.
    pub bytes: u64,
}

/// Actual HTTP connector invocations inside the observed work.
#[derive(Clone, Debug, Default, Serialize)]
pub struct HttpSnapshot {
    /// Invocations at the provider's transport boundary, not logical calls.
    pub dispatches: u64,
    /// GET invocations.
    pub get_dispatches: u64,
    /// HEAD invocations.
    pub head_dispatches: u64,
    /// Other method invocations.
    pub other_dispatches: u64,
    /// Repeated same-method invocations within one logical adapter read.
    /// This observes retries; it does not infer backoff or its cause.
    pub repeat_dispatches: u64,
    /// Transport errors returned by the connector, without error text.
    pub errors: u64,
    /// Attempts dropped before receiving headers or a transport error.
    pub cancelled: u64,
    /// Actual HTTP 429 responses.
    pub http_429: u64,
    /// Actual HTTP 5xx responses.
    pub http_5xx: u64,
}

/// Work completion classification; never a raw error.
#[derive(Clone, Copy, Debug, Default, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    /// Publisher still owns this work.
    #[default]
    Pending,
    /// Publication/replay returned success.
    Ok,
    /// Publication/replay returned an error.
    Error,
    /// Publisher aborted before returning an ordinary result.
    Aborted,
}

/// Why inner-stage attribution is unavailable; never derived from input or errors.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Unavailable {
    /// Admission has not bound this request to publisher work.
    NotAdmitted,
    /// A batch has not been selected yet.
    NotSelected,
    /// Shared batch stages cannot be assigned to individual candidates.
    CoalescedBatch,
    /// Work did not enter its explicit observation scope.
    MissingContext,
    /// A contended request bound to more than one work item.
    Rebound,
}

/// Fixed-size work data exported at HTTP completion and at publisher completion.
#[derive(Clone, Debug, Serialize)]
pub struct WorkSnapshot {
    /// Server-generated opaque correlation ID; unrelated to a commit or tenant ID.
    pub work_id: String,
    /// True only after the publisher settled the result.
    pub completed: bool,
    /// Fixed completion category.
    pub outcome: Outcome,
    /// Only single-candidate batches receive inner-stage attribution.
    pub attributed: bool,
    /// Bounded explanation when attribution is unavailable.
    pub unavailable: Option<Unavailable>,
    /// Zero before selection; a larger batch explicitly limits attribution.
    pub batch_size: u64,
    /// Actual projection reuse decision, absent until made.
    pub projection_cache_hit: Option<bool>,
    /// Actual receipt lookup result, absent until made.
    pub receipt_found: Option<bool>,
    /// Innermost active stage at this snapshot, including after HTTP cancellation.
    pub active_stage: Option<Stage>,
    /// Fixed list of inclusive stage timings.
    pub stages: Option<[StageSnapshot; 13]>,
    /// WAL consumed while loading/replaying the metadata view.
    pub metadata_wal: Option<WalSnapshot>,
    /// Retained WAL consumed to reconstruct a replay's original events.
    pub response_wal: Option<WalSnapshot>,
    /// Actual transport observations across all stages of this work.
    pub http: Option<HttpSnapshot>,
    /// Transport observations during all response reconstruction, including head discovery.
    pub response_http: Option<HttpSnapshot>,
}

/// An HTTP observation binds the actual response request ID to this fixed snapshot.
#[derive(Clone, Debug, Serialize)]
pub struct CommitSnapshot {
    /// Whether this request attached to work already in flight.
    pub joined_in_flight: bool,
    /// A value greater than one means only the latest binding is represented.
    pub bindings: u64,
    /// Publisher work shared by exact duplicates.
    pub work: Option<WorkSnapshot>,
    /// Whether this request has exactly one fully attributed work binding.
    pub attributed: bool,
    /// Bounded explanation for missing or partial request attribution.
    pub unavailable: Option<Unavailable>,
}

#[derive(Default)]
struct Binding {
    work: CommitWork,
    joined: bool,
    count: u64,
}

/// Disabled by default, with no allocation. The host explicitly opts in.
#[derive(Clone, Default)]
pub struct CommitTiming(Option<Arc<Mutex<Binding>>>);
impl CommitTiming {
    /// Allocates one bounded request binding, without recording an identity.
    pub fn new() -> Self {
        Self(Some(Arc::new(Mutex::new(Binding::default()))))
    }
    /// Captures the request scope before publisher ownership or spawning.
    pub fn current() -> Self {
        REQUEST.try_with(Clone::clone).unwrap_or_default()
    }
    /// Whether the host selected this request for attribution.
    pub fn enabled(&self) -> bool {
        self.0.is_some()
    }
    /// Carries the request binding through the core HTTP future.
    pub async fn scope<F: Future>(&self, future: F) -> F::Output {
        if self.enabled() {
            REQUEST.scope(self.clone(), future).await
        } else {
            future.await
        }
    }
    /// Binds a newly admitted commit to independently owned work.
    pub fn admit(&self) -> CommitWork {
        if !self.enabled() {
            return CommitWork::default();
        }
        let work = CommitWork::new();
        self.bind(&work, false);
        work
    }
    /// Binds a duplicate to the original work; no request-ID list is retained.
    pub fn join(&self, work: &CommitWork) {
        self.bind(work, true);
    }
    fn bind(&self, work: &CommitWork, joined: bool) {
        if let Some(binding) = &self.0 {
            let mut binding = binding.lock().expect("commit timing binding");
            binding.work = work.clone();
            binding.joined = joined;
            binding.count += 1;
        }
    }
    /// Snapshot at response creation so a timeout preserves its active stage.
    pub fn snapshot(&self) -> Option<CommitSnapshot> {
        let binding = self.0.as_ref()?.lock().expect("commit timing binding");
        let work = binding.work.snapshot();
        let unavailable = if binding.count > 1 {
            Some(Unavailable::Rebound)
        } else if binding.count == 0 {
            Some(Unavailable::NotAdmitted)
        } else {
            work.as_ref()
                .map_or(Some(Unavailable::MissingContext), |w| w.unavailable)
        };
        Some(CommitSnapshot {
            joined_in_flight: binding.joined,
            bindings: binding.count,
            work,
            attributed: unavailable.is_none(),
            unavailable,
        })
    }
}

#[derive(Clone, Copy, Default)]
struct StageState {
    calls: u64,
    elapsed_us: u64,
    started: Option<Instant>,
}
struct State {
    scoped: bool,
    work_id: String,
    completed: bool,
    outcome: Outcome,
    batch_size: u64,
    projection_cache_hit: Option<bool>,
    receipt_found: Option<bool>,
    active: Option<Stage>,
    stages: [StageState; 13],
    metadata_wal: WalSnapshot,
    response_wal: WalSnapshot,
    http: HttpSnapshot,
    response_http: HttpSnapshot,
}

/// A publisher-owned handle. It remains alive after the HTTP future is cancelled.
#[derive(Clone, Default)]
pub struct CommitWork(Option<Arc<Mutex<State>>>);
impl CommitWork {
    fn new() -> Self {
        let mut stages = [StageState::default(); 13];
        stages[Stage::Queue as usize] = StageState {
            calls: 1,
            elapsed_us: 0,
            started: Some(now()),
        };
        Self(Some(Arc::new(Mutex::new(State {
            scoped: false,
            work_id: loonfs_api::generated_id("work"),
            completed: false,
            outcome: Outcome::Pending,
            batch_size: 0,
            projection_cache_hit: None,
            receipt_found: None,
            active: Some(Stage::Queue),
            stages,
            metadata_wal: WalSnapshot::default(),
            response_wal: WalSnapshot::default(),
            http: HttpSnapshot::default(),
            response_http: HttpSnapshot::default(),
        }))))
    }
    /// Captures the publisher's scope; background maintenance has no scope.
    pub fn current() -> Self {
        WORK.try_with(Clone::clone).unwrap_or_default()
    }
    /// Explicitly propagates work into the publisher future and its awaits.
    pub async fn scope<F: Future>(&self, future: F) -> F::Output {
        self.update(|s| s.scoped = true);
        WORK.scope(self.clone(), future).await
    }
    fn update(&self, f: impl FnOnce(&mut State)) {
        if let Some(state) = &self.0 {
            f(&mut state.lock().expect("commit timing work"));
        }
    }
    /// Marks selection. Queue time includes the publication-permit subset.
    pub fn selected(&self, batch_size: usize) {
        self.update(|state| {
            finish_stage(state, Stage::Queue);
            state.batch_size = batch_size as u64;
            state.active = None;
        });
    }
    /// Starts an inclusive stage timer. Guards change no execution policy.
    pub fn stage(&self, stage: Stage) -> StageTimer {
        let mut previous = None;
        self.update(|state| {
            previous = state.active;
            state.active = Some(stage);
            let timing = &mut state.stages[stage as usize];
            timing.calls += 1;
            timing.started = Some(now());
        });
        StageTimer {
            work: self.clone(),
            stage,
            previous,
        }
    }
    /// Records the actual metadata projection reuse decision.
    pub fn projection_reused(&self, hit: bool) {
        self.update(|s| s.projection_cache_hit = Some(hit));
    }
    /// Records the actual durable receipt lookup outcome.
    pub fn receipt_found(&self, found: bool) {
        self.update(|s| s.receipt_found = Some(found));
    }
    /// Observes a WAL read in its actual parent path, without retaining its key.
    pub async fn read_wal<F: Future<Output = Result<Option<Bytes>>>>(
        &self,
        read: F,
    ) -> Result<Option<Bytes>> {
        let path = self.0.as_ref().and_then(|state| {
            let state = state.lock().expect("commit timing work");
            if state.stages[Stage::RetainedHistory as usize]
                .started
                .is_some()
                && state.stages[Stage::ResponseHistory as usize]
                    .started
                    .is_some()
            {
                Some(true)
            } else if state.stages[Stage::MetadataReplay as usize]
                .started
                .is_some()
            {
                Some(false)
            } else {
                None
            }
        });
        if let Some(path) = path {
            self.update(|state| wal(state, path).reads += 1);
        }
        let _read = self.stage(Stage::WalRead);
        let result = read.await;
        if let (Some(path), Ok(Some(bytes))) = (path, &result) {
            self.update(|state| {
                let stats = wal(state, path);
                stats.segments += 1;
                stats.bytes += bytes.len() as u64;
            });
        }
        result
    }
    /// Marks work settled and emits one sanitized summary, even without waiters.
    pub fn finish(&self, outcome: Outcome) {
        let mut emit = false;
        self.update(|state| {
            if !state.completed {
                for stage in STAGES {
                    finish_stage(state, stage);
                }
                state.active = None;
                state.outcome = outcome;
                state.completed = true;
                emit = true;
            }
        });
        if emit {
            if let Some(snapshot) = self.snapshot() {
                // No diagnostic span fields. Exporters must discard ambient formatter context.
                tracing::info!(target: "loonfs::commit_timing", parent: None,
                    diagnostic = %serde_json::to_string(&snapshot).expect("numeric commit timing"),
                    "commit_work_finished");
            }
        }
    }
    /// Returns only fixed-size counters and an opaque server-generated work ID.
    pub fn snapshot(&self) -> Option<WorkSnapshot> {
        let state = self.0.as_ref()?.lock().expect("commit timing work");
        let unavailable = match state.batch_size {
            0 => Some(Unavailable::NotSelected),
            1 if state.scoped => None,
            1 => Some(Unavailable::MissingContext),
            _ => Some(Unavailable::CoalescedBatch),
        };
        let attributed = unavailable.is_none();
        Some(WorkSnapshot {
            work_id: state.work_id.clone(),
            completed: state.completed,
            outcome: state.outcome,
            attributed,
            unavailable,
            batch_size: state.batch_size,
            projection_cache_hit: state.projection_cache_hit,
            receipt_found: state.receipt_found,
            active_stage: state.active,
            stages: attributed.then(|| {
                STAGES.map(|stage| {
                    let timing = state.stages[stage as usize];
                    StageSnapshot {
                        stage,
                        calls: timing.calls,
                        elapsed_us: (timing.calls > 0).then(|| {
                            timing
                                .elapsed_us
                                .saturating_add(timing.started.map_or(0, elapsed))
                        }),
                        active: timing.started.is_some(),
                    }
                })
            }),
            metadata_wal: attributed.then(|| state.metadata_wal.clone()),
            response_wal: attributed.then(|| state.response_wal.clone()),
            http: attributed.then(|| state.http.clone()),
            response_http: attributed.then(|| state.response_http.clone()),
        })
    }
}
fn wal(state: &mut State, response: bool) -> &mut WalSnapshot {
    if response {
        &mut state.response_wal
    } else {
        &mut state.metadata_wal
    }
}
fn finish_stage(state: &mut State, stage: Stage) {
    let timing = &mut state.stages[stage as usize];
    if let Some(started) = timing.started.take() {
        timing.elapsed_us = timing.elapsed_us.saturating_add(elapsed(started));
    }
}
/// Records partial intervals on errors or cancellation as well as success.
pub struct StageTimer {
    work: CommitWork,
    stage: Stage,
    previous: Option<Stage>,
}
impl Drop for StageTimer {
    fn drop(&mut self) {
        self.work.update(|state| {
            finish_stage(state, self.stage);
            if !state.completed {
                state.active = self.previous;
            }
        });
    }
}

/// Scopes actual same-method dispatch counts to a logical provider read.
pub(crate) async fn provider_read<F: Future>(read: F) -> F::Output {
    if CommitWork::current().0.is_some() {
        IO.scope(Arc::new(Mutex::new([0; 3])), read).await
    } else {
        read.await
    }
}
/// Counts at the HTTP transport boundary, never at a retry-policy decision.
pub(crate) struct HttpAttempt {
    work: CommitWork,
    finished: bool,
    response: bool,
}
impl HttpAttempt {
    pub(crate) fn start(method: &http::Method) -> Self {
        let work = CommitWork::current();
        let index = if method == http::Method::GET {
            0
        } else if method == http::Method::HEAD {
            1
        } else {
            2
        };
        let repeated = IO
            .try_with(|io| {
                let mut counts = io.lock().expect("commit HTTP attempts");
                let repeated = counts[index] != 0;
                counts[index] += 1;
                repeated
            })
            .unwrap_or(false);
        let mut response = false;
        work.update(|state| {
            response = state.stages[Stage::ResponseHistory as usize]
                .started
                .is_some();
        });
        let attempt = Self {
            work,
            finished: false,
            response,
        };
        attempt.update(|http| {
            http.dispatches += 1;
            match index {
                0 => http.get_dispatches += 1,
                1 => http.head_dispatches += 1,
                _ => http.other_dispatches += 1,
            }
            http.repeat_dispatches += u64::from(repeated);
        });
        attempt
    }
    fn update(&self, mut record: impl FnMut(&mut HttpSnapshot)) {
        self.work.update(|state| {
            record(&mut state.http);
            if self.response {
                record(&mut state.response_http);
            }
        });
    }
    pub(crate) fn finish(mut self, status: Option<u16>) {
        self.finished = true;
        self.update(|http| match status {
            None => http.errors += 1,
            Some(429) => http.http_429 += 1,
            Some(500..=599) => http.http_5xx += 1,
            _ => {}
        });
    }
}
impl Drop for HttpAttempt {
    fn drop(&mut self) {
        if !self.finished {
            self.update(|http| http.cancelled += 1);
        }
    }
}
#[allow(clippy::disallowed_methods)]
fn now() -> Instant {
    Instant::now()
}
fn elapsed(started: Instant) -> u64 {
    started.elapsed().as_micros().min(u64::MAX as u128) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn disabled_unbound_and_lost_context_are_not_zero_measurements() {
        let disabled = CommitTiming::default();
        assert!(!disabled.enabled());
        assert!(disabled.admit().snapshot().is_none());
        assert!(disabled.snapshot().is_none());
        let request = CommitTiming::new();
        assert_eq!(
            request.snapshot().expect("request").unavailable,
            Some(Unavailable::NotAdmitted)
        );
        request.join(&CommitWork::default());
        assert_eq!(
            request.snapshot().expect("request").unavailable,
            Some(Unavailable::MissingContext)
        );
        let work = CommitTiming::new().admit();
        work.selected(1);
        let snapshot = work.snapshot().expect("work");
        assert_eq!(snapshot.unavailable, Some(Unavailable::MissingContext));
        assert!(snapshot.stages.is_none() && snapshot.http.is_none());
        // An explicitly disabled batch cannot accidentally inherit another work scope.
        work.scope(CommitWork::default().scope(async {
            assert!(CommitWork::current().snapshot().is_none());
        }))
        .await;
    }

    #[tokio::test]
    async fn wal_paths_are_separate_and_stage_totals_are_inclusive() {
        let work = CommitTiming::new().admit();
        work.selected(1);
        work.scope(async {
            let _publish = work.stage(Stage::Publish);
            {
                let _view = work.stage(Stage::MetadataView);
                let _replay = work.stage(Stage::MetadataReplay);
                work.read_wal(async { Ok(Some(Bytes::from_static(b"meta"))) })
                    .await
                    .expect("read");
            }
            {
                let _response = work.stage(Stage::ResponseHistory);
                let _retained = work.stage(Stage::RetainedHistory);
                work.read_wal(async { Ok(Some(Bytes::from_static(b"history"))) })
                    .await
                    .expect("read");
            }
            // Unclassified reads must not be claimed as metadata replay.
            work.read_wal(async { Ok(Some(Bytes::from_static(b"other"))) })
                .await
                .expect("read");
        })
        .await;
        let snapshot = work.snapshot().expect("work");
        let metadata = snapshot.metadata_wal.expect("metadata");
        let response = snapshot.response_wal.expect("response");
        assert_eq!(
            (metadata.reads, metadata.segments, metadata.bytes),
            (1, 1, 4)
        );
        assert_eq!(
            (response.reads, response.segments, response.bytes),
            (1, 1, 7)
        );
        let stages = snapshot.stages.expect("attributed");
        assert_eq!(stages[Stage::WalRead as usize].calls, 3);
        assert!(
            stages[Stage::Publish as usize].elapsed_us
                >= stages[Stage::WalRead as usize].elapsed_us
        );
        assert_eq!(stages[Stage::EngineLock as usize].elapsed_us, None);
    }

    #[tokio::test]
    async fn cancelled_transport_and_repeat_dispatches_are_actual_observations() {
        let work = CommitTiming::new().admit();
        work.selected(1);
        work.scope(provider_read(async {
            let history = work.stage(Stage::ResponseHistory);
            HttpAttempt::start(&http::Method::GET).finish(Some(503));
            HttpAttempt::start(&http::Method::GET).finish(Some(200));
            drop(history);
            drop(HttpAttempt::start(&http::Method::HEAD));
        }))
        .await;
        let response = work
            .snapshot()
            .expect("work")
            .response_http
            .expect("response");
        assert_eq!(
            (
                response.dispatches,
                response.repeat_dispatches,
                response.http_5xx
            ),
            (2, 1, 1)
        );
        let http = work.snapshot().expect("work").http.expect("attributed");
        assert_eq!(
            (
                http.dispatches,
                http.repeat_dispatches,
                http.http_5xx,
                http.cancelled
            ),
            (3, 1, 1, 1)
        );
    }
}
