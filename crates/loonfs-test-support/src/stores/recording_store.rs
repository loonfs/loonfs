//! Exact operation logs and derived counts for selected object keys.

use super::{
    Intercept, InterceptStore, Interceptor, KeyPredicate, OperationClass, OperationContext,
    Outcome, RecordedOperation,
};
use async_trait::async_trait;
use loonfs_objectstore::PutMode;
use std::sync::Mutex;

/// An owned get record containing its key and optional `(start, end)` byte range.
pub type RecordedGet = (String, Option<(u64, u64)>);

/// Snapshot of request counts and transferred bytes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StoreCounts {
    /// Metadata-only read calls.
    pub heads: usize,
    /// Byte `get` calls.
    pub gets: usize,
    /// Full-object `get_with_metadata` calls.
    pub gets_with_metadata: usize,
    /// All put calls, including CAS-mode puts.
    pub puts: usize,
    /// Overwrite puts.
    pub overwrite_puts: usize,
    /// Create-if-absent puts.
    pub create_if_absent_puts: usize,
    /// Compare-and-swap calls and CAS-mode puts.
    pub compare_and_swaps: usize,
    /// Delete calls.
    pub deletes: usize,
    /// Prefix-list calls.
    pub lists: usize,
    /// Bytes returned by successful byte reads.
    pub read_bytes: u64,
    /// Bytes supplied to put calls.
    pub written_bytes: u64,
}

/// Intercepts operations and appends matching entries to a log.
#[derive(Debug)]
pub struct RecordingInterceptor {
    keys: KeyPredicate,
    operations: Mutex<Vec<RecordedOperation>>,
}

/// Records selected object-store operations.
pub type RecordingStore<S> = InterceptStore<S, RecordingInterceptor>;

impl<S> InterceptStore<S, RecordingInterceptor> {
    /// Records every operation whose key matches `keys`.
    pub fn new(inner: S, keys: KeyPredicate) -> Self {
        Self::with_interceptor(
            inner,
            RecordingInterceptor {
                keys,
                operations: Mutex::new(Vec::new()),
            },
        )
    }

    /// Records operations on metadata segments.
    pub fn metadata_segments(inner: S) -> Self {
        Self::new(inner, KeyPredicate::metadata_segment())
    }

    /// Returns a snapshot without clearing the log.
    pub fn snapshot(&self) -> Vec<RecordedOperation> {
        self.interceptor()
            .operations
            .lock()
            .expect("operation log lock should not be poisoned")
            .clone()
    }

    /// Returns counts derived from the operation log.
    pub fn counts(&self) -> StoreCounts {
        self.interceptor()
            .operations
            .lock()
            .expect("operation log lock should not be poisoned")
            .iter()
            .fold(StoreCounts::default(), fold_count)
    }

    /// Returns the current request count for `operation`.
    pub fn count(&self, operation: OperationClass) -> usize {
        self.interceptor()
            .operations
            .lock()
            .expect("operation log lock should not be poisoned")
            .iter()
            .filter(|recorded| recorded.matches(operation))
            .count()
    }

    /// Takes and clears the operation log.
    pub fn take(&self) -> Vec<RecordedOperation> {
        std::mem::take(
            &mut *self
                .interceptor()
                .operations
                .lock()
                .expect("operation log lock should not be poisoned"),
        )
    }

    /// Takes all operations and returns only byte-read records.
    pub fn take_gets(&self) -> Vec<RecordedGet> {
        self.take()
            .into_iter()
            .filter_map(|operation| match operation {
                RecordedOperation::Get { key, range, .. } => Some((
                    key,
                    range.map(|range| (range.start_inclusive, range.end_exclusive)),
                )),
                RecordedOperation::GetWithMetadata { key, .. } => Some((key, None)),
                _ => None,
            })
            .collect()
    }

    /// Takes all operations and returns only keys read through either get form.
    pub fn take_get_keys(&self) -> Vec<String> {
        self.take_gets().into_iter().map(|(key, _)| key).collect()
    }

    /// Clears the operation log.
    pub fn reset(&self) {
        self.interceptor()
            .operations
            .lock()
            .expect("operation log lock should not be poisoned")
            .clear();
    }
}

#[async_trait]
impl Interceptor for RecordingInterceptor {
    async fn before(&self, _context: &OperationContext<'_>) -> Intercept {
        Intercept::ContinueWithAfter
    }

    fn after(&self, context: &OperationContext<'_>, outcome: &Outcome) {
        if self.keys.matches(context.key()) {
            self.operations
                .lock()
                .expect("operation log lock should not be poisoned")
                .push(context.to_owned(outcome));
        }
    }
}

fn fold_count(mut counts: StoreCounts, operation: &RecordedOperation) -> StoreCounts {
    match operation {
        RecordedOperation::Head { .. } => counts.heads += 1,
        RecordedOperation::Get { result_bytes, .. } => {
            counts.gets += 1;
            counts.read_bytes +=
                u64::try_from(*result_bytes).expect("buffered read length should fit in u64");
        }
        RecordedOperation::GetWithMetadata { result_bytes, .. } => {
            counts.gets_with_metadata += 1;
            counts.read_bytes +=
                u64::try_from(*result_bytes).expect("buffered read length should fit in u64");
        }
        RecordedOperation::Put { mode, bytes, .. } => {
            counts.puts += 1;
            counts.written_bytes +=
                u64::try_from(*bytes).expect("buffered write length should fit in u64");
            fold_put_mode(&mut counts, mode);
        }
        RecordedOperation::PutStreamed { mode, bytes, .. } => {
            counts.puts += 1;
            counts.written_bytes += bytes.unwrap_or(0);
            fold_put_mode(&mut counts, mode);
        }
        RecordedOperation::CompareAndSwap { bytes, .. } => {
            counts.puts += 1;
            counts.compare_and_swaps += 1;
            counts.written_bytes +=
                u64::try_from(*bytes).expect("buffered write length should fit in u64");
        }
        RecordedOperation::Delete { .. } => counts.deletes += 1,
        RecordedOperation::List { .. } => counts.lists += 1,
    }
    counts
}

fn fold_put_mode(counts: &mut StoreCounts, mode: &PutMode) {
    match mode {
        PutMode::Overwrite => counts.overwrite_puts += 1,
        PutMode::CreateIfAbsent => counts.create_if_absent_puts += 1,
        PutMode::CompareAndSwap { .. } => counts.compare_and_swaps += 1,
    }
}
