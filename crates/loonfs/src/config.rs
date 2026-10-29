//! The settings of a runtime: its read limits, read policy, publication
//! policy, and inline content policy.

use crate::trace::TraceMode;
use crate::{Error, ReadConsistency, Result};
use loonfs_core::limits::READ_REVALIDATION_BOUND_MS;
use serde::{Deserialize, Serialize};
use std::num::{NonZeroU64, NonZeroUsize};

/// Default minimum interval, in milliseconds, between checks for a successor
/// to a cached manifest.
const DEFAULT_MANIFEST_REVALIDATION_INTERVAL_MS: u64 = 1000;
/// Default maximum staleness, in milliseconds, of an eventual read: how old the
/// reader's last confirmation of the head can be for the read to start from
/// that head without a request to confirm it.
const DEFAULT_MAX_STALENESS_MS: u64 = 1000;
/// Default largest file content that one buffered read materializes.
///
/// With the default of
/// [`DEFAULT_MAX_CONCURRENT_READS`](crate::DEFAULT_MAX_CONCURRENT_READS) reads
/// at once, the buffered reads under one budget keep at most 16 GiB (64 reads
/// of 256 MiB).
pub const DEFAULT_MAX_READ_CONTENT_BYTES: u64 = 256 * 1024 * 1024;
/// Default minimum interval, in milliseconds, between publication starts
/// for one namespace (see [`crate::publisher`]). A request to an idle
/// namespace publishes immediately; the interval only paces requests that
/// queued behind a publish, so concurrent submissions amortize into fewer,
/// larger WAL objects. Zero
/// keeps only the batching that in-flight publications force.
const DEFAULT_MIN_PUBLISH_INTERVAL_MS: u64 = 15;
/// Default age, in milliseconds, of a WAL tail's newest commit at which
/// maintenance folds a tail that is below the fold thresholds (15 minutes).
pub const DEFAULT_IDLE_FOLD_AFTER_MS: NonZeroU64 = NonZeroU64::new(15 * 60 * 1_000).unwrap();

/// The admission limits one writable runtime applies to each namespace's
/// publication requests, and how long a commit waits at the write stop.
///
/// Every admitted caller counts, including duplicate commits and namespace
/// deletes. Counts and bytes remain charged until the work settles, even if
/// the caller disconnects. Reaching either admission limit returns
/// `commit_queue_full`. The totals across namespaces, and across the runtimes
/// that share an execution budget, are limits of the
/// [`ExecutionBudget`](crate::ExecutionBudget).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PublicationPolicy {
    /// Maximum admitted requests for one namespace. Defaults to 1,024.
    pub max_requests_per_namespace: NonZeroUsize,
    /// Approximate retained request bytes for one namespace. Defaults to 8 MiB.
    pub max_estimated_bytes_per_namespace: NonZeroUsize,
    /// How long a new commit at the write stop waits in the queue for the
    /// session's fold, measured from when it entered the queue, before it
    /// returns `maintenance_required`. Defaults to 30,000 ms. See
    /// `docs/specs/api.md`, section 5.2.
    ///
    /// Not a key of the serde form: a host sets it from its own request
    /// deadline, so a commit that waits for the whole wait returns
    /// `maintenance_required` before the request's deadline passes. The
    /// reference server uses half of its `request_deadline_ms`.
    #[serde(skip)]
    pub write_stop_wait_budget_ms: u64,
}

impl Default for PublicationPolicy {
    fn default() -> Self {
        Self {
            max_requests_per_namespace: const { NonZeroUsize::new(1024).unwrap() },
            max_estimated_bytes_per_namespace: const { NonZeroUsize::new(8 * 1024 * 1024).unwrap() },
            write_stop_wait_budget_ms: 30_000,
        }
    }
}

/// Writer policy for content stored in WAL objects.
///
/// Retrying a completed inline commit does not upload its content again while
/// the commit receipt is retained. See `docs/specs/api.md`, section 5.2.
///
/// The bytes an append adds always travel in the WAL and never stage. They
/// count toward the WAL object budget and the unfolded tail, but neither
/// limit keeps an append back: a commit whose appends pass the budget
/// publishes in a WAL object of its own.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct InlineContentPolicy {
    /// Maximum size prepared inline; defaults to 64 KiB. `None` disables
    /// inline preparation, and serializes as `false`.
    #[serde(with = "inline_content_threshold")]
    pub inline_content_threshold_bytes: Option<usize>,
    /// Maximum inline bytes in one WAL object; defaults to 1 MiB.
    pub inline_content_wal_object_budget_bytes: usize,
    /// Unfolded inline bytes that make a fold due; defaults to 2 MiB.
    pub inline_content_fold_at_bytes: usize,
    /// Limit on unfolded and admitted inline bytes; defaults to 32 MiB.
    /// A session without a position (before its first publish, or after its
    /// position is dropped) admits at most the WAL object budget until its
    /// next publish reads the tail, so the tail can exceed the limit by the
    /// WAL object budget each time the position is dropped before a fold.
    /// `MAX_UNFOLDED_WAL_OBJECTS` stops new commits regardless of this limit.
    pub inline_content_tail_limit_bytes: usize,
}

impl Default for InlineContentPolicy {
    fn default() -> Self {
        Self {
            inline_content_threshold_bytes: Some(64 * 1024),
            inline_content_wal_object_budget_bytes: 1024 * 1024,
            inline_content_fold_at_bytes: 2 * 1024 * 1024,
            inline_content_tail_limit_bytes: 32 * 1024 * 1024,
        }
    }
}

/// The serde form of `inline_content_threshold_bytes`: a byte count, or
/// `false` for no inline preparation.
mod inline_content_threshold {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub(super) fn serialize<S: Serializer>(
        threshold: &Option<usize>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match threshold {
            Some(bytes) => bytes.serialize(serializer),
            None => serializer.serialize_bool(false),
        }
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<usize>, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Threshold {
            Bytes(usize),
            Enabled(bool),
        }

        match Threshold::deserialize(deserializer)? {
            Threshold::Bytes(bytes) => Ok(Some(bytes)),
            Threshold::Enabled(false) => Ok(None),
            Threshold::Enabled(true) => Err(serde::de::Error::custom(
                "`inline_content_threshold_bytes` must be a byte count or false",
            )),
        }
    }
}

impl InlineContentPolicy {
    fn validate(&self) -> Result<()> {
        use loonfs_types::format::wal::{
            MAX_WAL_INLINE_CONTENT_BYTES, MAX_WAL_OBJECT_INLINE_CONTENT_BYTES,
        };
        if self
            .inline_content_threshold_bytes
            .is_some_and(|value| value > MAX_WAL_INLINE_CONTENT_BYTES)
        {
            return Err(Error::Config(format!(
                "`inline_content_threshold_bytes` must not exceed {MAX_WAL_INLINE_CONTENT_BYTES}"
            )));
        }
        if self.inline_content_wal_object_budget_bytes == 0
            || self.inline_content_wal_object_budget_bytes > MAX_WAL_OBJECT_INLINE_CONTENT_BYTES
        {
            return Err(Error::Config(format!(
                "`inline_content_wal_object_budget_bytes` must be between 1 and {MAX_WAL_OBJECT_INLINE_CONTENT_BYTES}"
            )));
        }
        if self.inline_content_fold_at_bytes == 0 || self.inline_content_tail_limit_bytes == 0 {
            return Err(Error::Config(
                "`inline_content_fold_at_bytes` and `inline_content_tail_limit_bytes` must be greater than zero".to_owned()
            ));
        }
        if self.inline_content_fold_at_bytes > self.inline_content_tail_limit_bytes {
            return Err(Error::Config(
                "`inline_content_fold_at_bytes` must not exceed `inline_content_tail_limit_bytes`"
                    .to_owned(),
            ));
        }
        Ok(())
    }
}

/// The settings of a runtime.
///
/// A host sets them with [`LoonFsBuilder::config`](crate::LoonFsBuilder::config),
/// and sets the runtime's components (the store, the caches, the clocks, the
/// execution budget, and the metrics recorders) with the builder's other
/// methods. A read-only runtime reads the read settings and ignores the
/// publication, inline content, and fold settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RuntimeConfig {
    /// Largest file content that the buffered read APIs materialize for one
    /// call, checked against resolved metadata before any content fetch.
    /// An over-limit read fails with `content_too_large`. Defaults to
    /// [`DEFAULT_MAX_READ_CONTENT_BYTES`], 256 MiB. A caller that reads larger
    /// files through these APIs raises it, and a server sets it to its
    /// download limit.
    pub max_read_content_bytes: u64,
    /// Minimum interval, in milliseconds, between checks for a successor to
    /// the manifest of a cached head. Defaults to 1000, and zero checks on
    /// every read. A value above
    /// [`READ_REVALIDATION_BOUND_MS`](crate::engine::READ_REVALIDATION_BOUND_MS)
    /// fails the build with [`Error::Config`].
    pub manifest_revalidation_interval_ms: u64,
    /// Maximum staleness, in milliseconds, of an eventual read that doesn't
    /// set its own (see [`Consistency::Eventual`](crate::Consistency::Eventual)).
    ///
    /// An eventual read reads at the head that the runtime confirmed last
    /// (the position of the namespace's writer session, or else the cached
    /// head), without a request, while that confirmation is younger than the
    /// maximum staleness. Defaults to 1000, and zero confirms the head on
    /// every read. A value above
    /// [`READ_REVALIDATION_BOUND_MS`](crate::engine::READ_REVALIDATION_BOUND_MS)
    /// fails the build with [`Error::Config`].
    pub max_staleness_ms: u64,
    /// Consistency of a read that doesn't set its own, including every read
    /// that takes no options. Defaults to [`ReadConsistency::Eventual`],
    /// whose reads take [`Self::max_staleness_ms`]. A host whose readers
    /// follow writes made in other processes sets [`ReadConsistency::Strong`].
    pub consistency: ReadConsistency,
    /// Mode label of the runtime's trace spans. Defaults to
    /// [`TraceMode::Embedded`].
    pub trace_mode: TraceMode,
    /// Minimum interval, in milliseconds, between publication starts for one
    /// namespace.
    ///
    /// A request to an idle namespace publishes immediately. The interval
    /// paces the requests that queued behind a publish, so concurrent
    /// publishes share fewer, larger WAL objects, and each caller still
    /// waits for its own durable result. Defaults to 15 ms, and zero keeps
    /// only the batching that publications in flight cause.
    pub min_publish_interval_ms: u64,
    /// Admission limits and write-stop wait that a writable runtime applies
    /// to each namespace's publication requests.
    ///
    /// The totals across namespaces, and the publications running at once,
    /// are limits of the [`ExecutionBudget`](crate::ExecutionBudget).
    pub publication: PublicationPolicy,
    /// Inline preparation, WAL object, fold, and tail limits of a writable
    /// runtime's commits.
    pub inline_content: InlineContentPolicy,
    /// How old a WAL tail's newest commit must be, in milliseconds, before
    /// maintenance folds a tail that is below the fold thresholds, or `None`
    /// to turn that rule off.
    ///
    /// Defaults to [`DEFAULT_IDLE_FOLD_AFTER_MS`], 15 minutes.
    /// [`Maintenance`](crate::Maintenance) measures the age on the runtime's
    /// wall clock from the newest commit's `committed_at_ms`. A writer
    /// session doesn't fold on age. A host that sweeps its sessions reads
    /// when a session reaches the age with
    /// [`Namespace::idle_fold_due_at_ms`](crate::Namespace::idle_fold_due_at_ms).
    pub idle_fold_after_ms: Option<NonZeroU64>,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            max_read_content_bytes: DEFAULT_MAX_READ_CONTENT_BYTES,
            manifest_revalidation_interval_ms: DEFAULT_MANIFEST_REVALIDATION_INTERVAL_MS,
            max_staleness_ms: DEFAULT_MAX_STALENESS_MS,
            consistency: ReadConsistency::Eventual,
            trace_mode: TraceMode::Embedded,
            min_publish_interval_ms: DEFAULT_MIN_PUBLISH_INTERVAL_MS,
            publication: PublicationPolicy::default(),
            inline_content: InlineContentPolicy::default(),
            idle_fold_after_ms: Some(DEFAULT_IDLE_FOLD_AFTER_MS),
        }
    }
}

impl RuntimeConfig {
    /// Checks the settings against the limits of the runtime, and returns
    /// [`Error::Config`] for the first setting outside them.
    pub fn validate(&self) -> Result<()> {
        // A reader reloads a view whose manifest check is as old as the
        // revalidation bound, whatever the setting, so a longer interval or
        // maximum staleness would promise something that the runtime doesn't
        // do.
        for (setting, interval_ms) in [
            (
                "manifest_revalidation_interval_ms",
                self.manifest_revalidation_interval_ms,
            ),
            ("max_staleness_ms", self.max_staleness_ms),
        ] {
            if interval_ms > READ_REVALIDATION_BOUND_MS {
                return Err(Error::Config(format!(
                    "`{setting}` is {interval_ms} ms, which exceeds the revalidation bound of \
                     {READ_REVALIDATION_BOUND_MS} ms"
                )));
            }
        }
        self.inline_content.validate()
    }
}
