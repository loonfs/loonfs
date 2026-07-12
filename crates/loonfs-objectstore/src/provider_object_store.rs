use crate::keyspace::{
    normalize_key_prefix, scope_list_prefix, scope_object_key, unscope_listed_key,
};
use crate::timing::{MonotonicTimer, StdMonotonicTimer};
use crate::{ByteRange, ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, BoxStream, StreamExt};
use loonfs_api::sha256_digest;
use object_store as provider_store;
use provider_store::path::Path;
use provider_store::{
    GetOptions, GetRange, ObjectMeta, PutOptions, PutPayload, PutResult, UpdateVersion,
};
use std::fmt;
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderObjectStoreConfig {
    pub key_prefix: Option<String>,
    pub sha256_checksum_metadata: bool,
}

/// One HTTP attempt's request timeout, applied through the provider client's
/// options. An attempt that makes no progress for this long fails and counts
/// against the operation deadline instead of consuming it invisibly.
pub const PROVIDER_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(30);

/// One HTTP attempt's connect timeout.
pub const PROVIDER_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Hard deadline for one logical object-store operation, consumed across
/// every retry of that operation rather than restarting per attempt. Reads
/// get it as the provider client's retry timeout; writes and deletes get it
/// through `TransportRetryPolicy`. The deadline gates starting another
/// attempt, so one operation's total wall time is bounded by
/// `PROVIDER_OP_DEADLINE + PROVIDER_ATTEMPT_TIMEOUT`. The GC grace window is
/// derived above this bound (format spec, "Garbage collection", rule 1).
pub const PROVIDER_OP_DEADLINE: Duration = Duration::from_secs(120);

/// Client options every provider builder applies: explicit per-attempt
/// request and connect timeouts, so attempt time is bounded by these named
/// constants instead of an upstream default.
pub(crate) fn provider_client_options() -> provider_store::ClientOptions {
    provider_store::ClientOptions::new()
        .with_timeout(PROVIDER_ATTEMPT_TIMEOUT)
        .with_connect_timeout(PROVIDER_CONNECT_TIMEOUT)
}

/// Retry configuration every provider builder applies: the client's internal
/// read retries consume [`PROVIDER_OP_DEADLINE`] as one per-operation budget,
/// matching the write loops above.
pub(crate) fn provider_retry_config() -> provider_store::RetryConfig {
    provider_store::RetryConfig {
        retry_timeout: PROVIDER_OP_DEADLINE,
        ..Default::default()
    }
}

/// Bounded retry for transient write failures, mirroring the retry budget the
/// provider client already applies to reads.
///
/// The provider client retries transport errors on reads because GET is
/// idempotent by method, but it refuses to re-send a PUT or DELETE that may
/// already have reached the store. LoonFS knows more than the HTTP layer:
/// overwrite puts, create-if-absent puts, and deletes are idempotent-safe at
/// this contract's semantics, so they get the same bounded retry reads have.
/// Both attempt count and elapsed time bound the loop: `op_deadline` is one
/// deadline for the whole logical operation, never restarted per attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TransportRetryPolicy {
    max_retries: u32,
    initial_backoff: Duration,
    max_backoff: Duration,
    op_deadline: Duration,
}

impl TransportRetryPolicy {
    /// Matches the provider client's read retry budget: up to 10 retries with
    /// exponential backoff from 100ms capped at 15s, all inside one
    /// operation deadline.
    const DEFAULT: Self = Self {
        max_retries: 10,
        initial_backoff: Duration::from_millis(100),
        max_backoff: Duration::from_secs(15),
        op_deadline: PROVIDER_OP_DEADLINE,
    };
}

fn transport_retry_backoff(policy: &TransportRetryPolicy, retry: u32) -> Duration {
    // Deterministic doubling: workspace policy avoids ambient randomness, and
    // a bounded per-operation retry does not need jitter.
    let doublings = retry.saturating_sub(1).min(16);
    policy
        .initial_backoff
        .saturating_mul(1u32 << doublings)
        .min(policy.max_backoff)
}

/// Elapsed-time state for one logical operation's retry loop: refuses new
/// attempts once the deadline is consumed and never sleeps past it.
struct OperationDeadline<'timer> {
    timer: &'timer dyn MonotonicTimer,
    started_ms: u64,
    deadline: Duration,
}

impl<'timer> OperationDeadline<'timer> {
    fn start(timer: &'timer dyn MonotonicTimer, deadline: Duration) -> Self {
        Self {
            timer,
            started_ms: timer.monotonic_now_ms(),
            deadline,
        }
    }

    fn remaining(&self) -> Option<Duration> {
        let elapsed_ms = self
            .timer
            .monotonic_now_ms()
            .saturating_sub(self.started_ms);
        let deadline_ms = u64::try_from(self.deadline.as_millis()).unwrap_or(u64::MAX);
        if elapsed_ms >= deadline_ms {
            return None;
        }
        Some(Duration::from_millis(deadline_ms - elapsed_ms))
    }
}

#[allow(clippy::disallowed_methods)]
async fn transport_retry_pause(backoff: Duration) {
    // Write retries intentionally wait an isolated async timer between
    // attempts, mirroring the backoff the provider client applies to reads.
    tokio::time::sleep(backoff).await;
}

#[derive(Clone)]
pub struct ProviderObjectStore {
    inner: Arc<dyn provider_store::ObjectStore>,
    key_prefix: Option<String>,
    sha256_checksum_metadata: bool,
    transport_retry: TransportRetryPolicy,
    timer: Arc<dyn MonotonicTimer>,
}

impl fmt::Debug for ProviderObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProviderObjectStore")
            .field("key_prefix", &self.key_prefix)
            .field("sha256_checksum_metadata", &self.sha256_checksum_metadata)
            .finish_non_exhaustive()
    }
}

impl ProviderObjectStore {
    pub fn new(
        inner: Arc<dyn provider_store::ObjectStore>,
        config: ProviderObjectStoreConfig,
    ) -> Result<Self, ObjectStoreError> {
        Ok(Self {
            inner,
            key_prefix: normalize_key_prefix(config.key_prefix.as_deref())?,
            sha256_checksum_metadata: config.sha256_checksum_metadata,
            transport_retry: TransportRetryPolicy::DEFAULT,
            timer: Arc::new(StdMonotonicTimer::default()),
        })
    }

    #[cfg(test)]
    fn with_transport_retry(mut self, transport_retry: TransportRetryPolicy) -> Self {
        self.transport_retry = transport_retry;
        self
    }

    #[cfg(test)]
    fn with_monotonic_timer(mut self, timer: Arc<dyn MonotonicTimer>) -> Self {
        self.timer = timer;
        self
    }

    fn to_path(&self, key: &str) -> Result<Path, ObjectStoreError> {
        let scoped = scope_object_key(self.key_prefix.as_deref(), key)?;
        Path::parse(scoped).map_err(|err| ObjectStoreError::InvalidKey {
            object_key: key.to_owned(),
            message: err.to_string(),
        })
    }

    pub(crate) fn validate_key(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.to_path(key).map(|_| ())
    }

    fn list_path(&self, prefix: &str) -> Result<Option<Path>, ObjectStoreError> {
        let scoped = scope_list_prefix(self.key_prefix.as_deref(), prefix)?;
        if scoped.is_empty() {
            return Ok(None);
        }
        Path::parse(scoped)
            .map(Some)
            .map_err(|err| ObjectStoreError::InvalidKey {
                object_key: prefix.to_owned(),
                message: err.to_string(),
            })
    }

    fn from_meta(meta: ObjectMeta, checksum_sha256: Option<String>) -> ObjectMetadata {
        ObjectMetadata {
            etag: meta.e_tag,
            version: meta.version,
            size_bytes: meta.size,
            checksum_sha256,
            last_modified_ms: u64::try_from(meta.last_modified.timestamp_millis()).ok(),
        }
    }

    fn from_put_result(
        result: PutResult,
        size_bytes: u64,
        checksum_sha256: Option<String>,
    ) -> ObjectMetadata {
        ObjectMetadata {
            etag: result.e_tag,
            version: result.version,
            size_bytes,
            checksum_sha256,
            last_modified_ms: None,
        }
    }

    async fn provider_range(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<ProviderRange>, ObjectStoreError> {
        let Some(range) = range else {
            return Ok(None);
        };
        let metadata = match self.head(key).await? {
            Some(metadata) => metadata,
            None => {
                return Err(ObjectStoreError::NotFound {
                    object_key: key.to_owned(),
                })
            }
        };
        if range.end_exclusive < range.start_inclusive
            || range.start_inclusive > metadata.size_bytes
        {
            return Err(ObjectStoreError::InvalidRange {
                object_key: key.to_owned(),
            });
        }

        let bounded_end = range.end_exclusive.min(metadata.size_bytes);
        if bounded_end == range.start_inclusive {
            return Ok(Some(ProviderRange::Empty));
        }
        Ok(Some(ProviderRange::Bounded(GetRange::Bounded(Range {
            start: range.start_inclusive,
            end: bounded_end,
        }))))
    }
}

#[async_trait]
impl ObjectStore for ProviderObjectStore {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        let path = self.to_path(key)?;
        match self.inner.head(&path).await {
            Ok(meta) => Ok(Some(Self::from_meta(meta, None))),
            Err(err) if provider_not_found(&err) => Ok(None),
            Err(err) => Err(map_provider_error(key, err)),
        }
    }

    async fn head_with_checksum(
        &self,
        key: &str,
    ) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.head(key).await
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        let path = self.to_path(key)?;
        match self.inner.get(&path).await {
            Ok(result) => {
                let metadata = Self::from_meta(result.meta.clone(), None);
                let bytes = result
                    .bytes()
                    .await
                    .map_err(|err| map_provider_error(key, err))?;
                Ok(Some(ObjectBody {
                    metadata,
                    bytes: bytes.to_vec(),
                }))
            }
            Err(err) if provider_not_found(&err) => Ok(None),
            Err(err) => Err(map_provider_error(key, err)),
        }
    }

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        let path = self.to_path(key)?;
        let Some(provider_range) = self.provider_range(key, range).await? else {
            return match self.inner.get(&path).await {
                Ok(result) => result
                    .bytes()
                    .await
                    .map(Some)
                    .map_err(|err| map_provider_error(key, err)),
                Err(err) if provider_not_found(&err) => Ok(None),
                Err(err) => Err(map_provider_error(key, err)),
            };
        };

        let ProviderRange::Bounded(provider_range) = provider_range else {
            return Ok(Some(Bytes::new()));
        };

        let options = GetOptions {
            range: Some(provider_range),
            ..Default::default()
        };
        match self.inner.get_opts(&path, options).await {
            Ok(result) => result
                .bytes()
                .await
                .map(Some)
                .map_err(|err| map_provider_error(key, err)),
            Err(err) if provider_not_found(&err) => Ok(None),
            Err(err) => Err(map_provider_error(key, err)),
        }
    }

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        let path = self.to_path(key)?;
        let checksum_sha256 = self.sha256_checksum_metadata.then(|| sha256_digest(&bytes));
        let size_bytes = bytes.len() as u64;

        if matches!(mode, PutMode::CompareAndSwap { .. }) {
            // Compare-and-swap is deliberately a single attempt: after an
            // ambiguous transport failure the first attempt may have landed
            // and changed the etag, so a blind re-send would report a false
            // conflict for our own write. Callers own precondition semantics
            // and recover by re-reading.
            let options = PutOptions {
                mode: map_put_mode(mode),
                ..Default::default()
            };
            return match self
                .inner
                .put_opts(&path, PutPayload::from(bytes), options)
                .await
            {
                Ok(result) => Ok(Self::from_put_result(result, size_bytes, checksum_sha256)),
                Err(err) if provider_not_found(&err) => Err(ObjectStoreError::PreconditionFailed {
                    object_key: key.to_owned(),
                }),
                Err(err) => Err(map_provider_error(key, err)),
            };
        }

        let create_if_absent = matches!(mode, PutMode::CreateIfAbsent);
        let deadline =
            OperationDeadline::start(self.timer.as_ref(), self.transport_retry.op_deadline);
        let mut retries: u32 = 0;
        // True once an attempt failed after possibly reaching the store, so a
        // later "already exists" may be our own first attempt having landed.
        let mut ambiguous_outcome = false;
        loop {
            let options = PutOptions {
                mode: map_put_mode(mode.clone()),
                ..Default::default()
            };
            let err = match self
                .inner
                .put_opts(&path, PutPayload::from(bytes.clone()), options)
                .await
            {
                Ok(result) => {
                    return Ok(Self::from_put_result(result, size_bytes, checksum_sha256))
                }
                Err(err) => err,
            };

            if create_if_absent && ambiguous_outcome && provider_precondition(&err) {
                match self.get_with_metadata(key).await? {
                    Some(body) if body.bytes.as_slice() == bytes.as_ref() => {
                        // Our earlier attempt landed: report it as the
                        // successful write it was, not a conflict.
                        let mut metadata = body.metadata;
                        metadata.checksum_sha256 = checksum_sha256;
                        return Ok(metadata);
                    }
                    Some(_) => {
                        return Err(ObjectStoreError::PreconditionFailed {
                            object_key: key.to_owned(),
                        })
                    }
                    // The conflicting object vanished, so the create can be
                    // re-attempted within the remaining retry budget.
                    None => {}
                }
            } else if provider_transport_retryable(&err) {
                ambiguous_outcome = true;
            } else {
                return Err(map_provider_error(key, err));
            }

            if retries >= self.transport_retry.max_retries {
                return Err(map_provider_error(key, err));
            }
            // The operation deadline is consumed across attempts, never
            // restarted: no new attempt starts once it is spent, and the
            // backoff never sleeps past it.
            let Some(remaining) = deadline.remaining() else {
                tracing::warn!(
                    object_key = key,
                    operation = "put",
                    retry = retries,
                    error = %err,
                    "object store operation deadline exhausted; not retrying",
                );
                return Err(map_provider_error(key, err));
            };
            retries += 1;
            let backoff = transport_retry_backoff(&self.transport_retry, retries).min(remaining);
            tracing::info!(
                object_key = key,
                operation = "put",
                retry = retries,
                max_retries = self.transport_retry.max_retries,
                backoff_ms = u64::try_from(backoff.as_millis()).unwrap_or(u64::MAX),
                error = %err,
                "transient object store write failure, backing off before retry",
            );
            transport_retry_pause(backoff).await;
        }
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        let path = self.to_path(key)?;
        let deadline =
            OperationDeadline::start(self.timer.as_ref(), self.transport_retry.op_deadline);
        let mut retries: u32 = 0;
        loop {
            // Delete is idempotent under this contract: not-found already
            // reports success, so a retry after an attempt that landed
            // converges to the same outcome.
            let err = match self.inner.delete(&path).await {
                Ok(()) => return Ok(()),
                Err(err) if provider_not_found(&err) => return Ok(()),
                Err(err) => err,
            };
            if !provider_transport_retryable(&err) || retries >= self.transport_retry.max_retries {
                return Err(map_provider_error(key, err));
            }
            let Some(remaining) = deadline.remaining() else {
                tracing::warn!(
                    object_key = key,
                    operation = "delete",
                    retry = retries,
                    error = %err,
                    "object store operation deadline exhausted; not retrying",
                );
                return Err(map_provider_error(key, err));
            };
            retries += 1;
            let backoff = transport_retry_backoff(&self.transport_retry, retries).min(remaining);
            tracing::info!(
                object_key = key,
                operation = "delete",
                retry = retries,
                max_retries = self.transport_retry.max_retries,
                backoff_ms = u64::try_from(backoff.as_millis()).unwrap_or(u64::MAX),
                error = %err,
                "transient object store write failure, backing off before retry",
            );
            transport_retry_pause(backoff).await;
        }
    }

    fn list_prefix_stream(
        &self,
        prefix: &str,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        let prefix_path = match self.list_path(prefix) {
            Ok(prefix_path) => prefix_path,
            Err(err) => return stream::once(async { Err(err) }).boxed(),
        };
        let key_prefix = self.key_prefix.clone();
        let listed_prefix = prefix.to_owned();
        self.inner
            .list(prefix_path.as_ref())
            .filter_map(move |result| {
                let key_prefix = key_prefix.clone();
                let listed_prefix = listed_prefix.clone();
                async move {
                    match result {
                        Ok(meta) => {
                            let key = meta.location.as_ref();
                            match key_prefix.as_deref() {
                                Some(prefix) => unscope_listed_key(Some(prefix), key).map(Ok),
                                None => Some(Ok(key.to_owned())),
                            }
                        }
                        Err(err) => Some(Err(map_provider_error(&listed_prefix, err))),
                    }
                }
            })
            .boxed()
    }
}

fn map_put_mode(mode: PutMode) -> provider_store::PutMode {
    match mode {
        PutMode::Overwrite => provider_store::PutMode::Overwrite,
        PutMode::CreateIfAbsent => provider_store::PutMode::Create,
        PutMode::CompareAndSwap { expected_etag } => {
            // The compare token is opaque and provider-issued: S3-family
            // backends condition on `e_tag`, GCS conditions on `version`
            // (its generation). Populate both so each backend reads the
            // field it understands.
            provider_store::PutMode::Update(UpdateVersion {
                e_tag: Some(expected_etag.clone()),
                version: Some(expected_etag),
            })
        }
    }
}

enum ProviderRange {
    Empty,
    Bounded(GetRange),
}

fn provider_not_found(err: &provider_store::Error) -> bool {
    matches!(err, provider_store::Error::NotFound { .. })
}

fn provider_precondition(err: &provider_store::Error) -> bool {
    matches!(
        err,
        provider_store::Error::AlreadyExists { .. } | provider_store::Error::Precondition { .. }
    )
}

fn provider_transport_retryable(err: &provider_store::Error) -> bool {
    // `Generic` is where the provider client surfaces request failures after
    // its own retry policy gives up: for writes that includes mid-flight
    // transport errors it refuses to re-send because a non-idempotent HTTP
    // request may already have reached the store. The remaining variants are
    // definite outcomes (not-found, already-exists, precondition), hard
    // rejections (auth, invalid path, unsupported), or the store IO runtime
    // shutting down (join error); re-sending those is wrong or futile.
    matches!(err, provider_store::Error::Generic { .. })
}

fn map_provider_error(object_key: &str, err: provider_store::Error) -> ObjectStoreError {
    match err {
        provider_store::Error::NotFound { .. } => ObjectStoreError::NotFound {
            object_key: object_key.to_owned(),
        },
        provider_store::Error::AlreadyExists { .. }
        | provider_store::Error::Precondition { .. }
        | provider_store::Error::NotModified { .. } => ObjectStoreError::PreconditionFailed {
            object_key: object_key.to_owned(),
        },
        provider_store::Error::InvalidPath { source } => ObjectStoreError::InvalidKey {
            object_key: object_key.to_owned(),
            message: source.to_string(),
        },
        provider_store::Error::NotSupported { .. } | provider_store::Error::NotImplemented => {
            ObjectStoreError::Unsupported("provider object store operation")
        }
        provider_store::Error::UnknownConfigurationKey { key, store } => {
            ObjectStoreError::transport(
                object_key,
                format!("unknown {store} configuration key `{key}`"),
            )
        }
        provider_store::Error::Generic { source, .. } => {
            ObjectStoreError::transport(object_key, source.to_string())
        }
        provider_store::Error::JoinError { source } => {
            ObjectStoreError::transport(object_key, source.to_string())
        }
        provider_store::Error::PermissionDenied { source, .. }
        | provider_store::Error::Unauthenticated { source, .. } => {
            ObjectStoreError::transport(object_key, source.to_string())
        }
        other => ObjectStoreError::transport(object_key, other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use object_store::memory::InMemory;

    fn memory_store() -> ProviderObjectStore {
        ProviderObjectStore::new(
            Arc::new(InMemory::default()),
            ProviderObjectStoreConfig {
                key_prefix: Some("tenant-a".to_owned()),
                sha256_checksum_metadata: true,
            },
        )
        .expect("provider store")
    }

    #[tokio::test]
    async fn provider_store_preserves_put_get_head_and_prefix_scoping() {
        let store = memory_store();
        let key = "namespaces/demo/wal/head.json";

        let metadata = store
            .put_if_absent(key, Bytes::from_static(b"head"))
            .await
            .expect("put");
        assert_eq!(metadata.size_bytes, 4);
        assert!(metadata.etag.is_some());
        assert_eq!(metadata.checksum_sha256, Some(sha256_digest(b"head")));

        let head = store.head(key).await.expect("head").expect("head exists");
        assert_eq!(head.size_bytes, 4);
        assert_eq!(
            store.get(key, None).await.expect("get"),
            Some(Bytes::from_static(b"head"))
        );
        assert_eq!(
            store.list_prefix("namespaces/demo/").await.expect("list"),
            vec![key.to_owned()]
        );
    }

    #[tokio::test]
    async fn provider_store_enforces_create_and_cas_preconditions() {
        let store = memory_store();
        let key = "namespaces/demo/wal/head.json";
        let first = store
            .put_if_absent(key, Bytes::from_static(b"one"))
            .await
            .expect("first put");

        assert!(matches!(
            store.put_if_absent(key, Bytes::from_static(b"two")).await,
            Err(ObjectStoreError::PreconditionFailed { .. })
        ));
        assert!(matches!(
            store
                .compare_and_swap(key, "stale", Bytes::from_static(b"two"))
                .await,
            Err(ObjectStoreError::PreconditionFailed { .. })
        ));
        assert!(matches!(
            store
                .compare_and_swap(
                    "namespaces/demo/control/missing-head.json",
                    "missing",
                    Bytes::from_static(b"two")
                )
                .await,
            Err(ObjectStoreError::PreconditionFailed { .. })
        ));
        let etag = first.etag.expect("etag");
        store
            .compare_and_swap(key, &etag, Bytes::from_static(b"two"))
            .await
            .expect("cas");
        assert_eq!(
            store.get(key, None).await.expect("get"),
            Some(Bytes::from_static(b"two"))
        );
    }

    #[tokio::test]
    async fn provider_store_range_semantics_match_blocking_contract() {
        let store = memory_store();
        let key = "content-stores/cs_0123456789abcdef0123456789abcdef/blobs/sha256/ab/cd/abcdef";
        store
            .put_overwrite(key, Bytes::from_static(b"abcdef"))
            .await
            .expect("put");

        assert_eq!(
            store
                .get(
                    key,
                    Some(ByteRange {
                        start_inclusive: 2,
                        end_exclusive: 4,
                    }),
                )
                .await
                .expect("range"),
            Some(Bytes::from_static(b"cd"))
        );
        assert_eq!(
            store
                .get(
                    key,
                    Some(ByteRange {
                        start_inclusive: 6,
                        end_exclusive: 10,
                    }),
                )
                .await
                .expect("empty"),
            Some(Bytes::new())
        );
        assert!(matches!(
            store
                .get(
                    key,
                    Some(ByteRange {
                        start_inclusive: 7,
                        end_exclusive: 8,
                    }),
                )
                .await,
            Err(ObjectStoreError::InvalidRange { .. })
        ));
    }

    #[tokio::test]
    async fn provider_stream_reports_invalid_prefix() {
        let store = memory_store();
        let mut stream = store.list_prefix_stream("../");
        assert!(matches!(
            stream.next().await,
            Some(Err(ObjectStoreError::InvalidKey { .. }))
        ));
    }

    use provider_store::{GetResult, ListResult, MultipartUpload, PutMultipartOptions};
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    #[derive(Debug)]
    enum WriteScript {
        FailWithoutLanding,
        LandThenFail,
        FailAuth,
    }

    #[derive(Debug)]
    enum ReadScript {
        NotFound,
    }

    /// Provider double that fails scripted attempts before delegating to an
    /// in-memory store, so retry behavior is observable per attempt.
    #[derive(Default)]
    struct FlakyStore {
        inner: InMemory,
        put_script: Mutex<VecDeque<WriteScript>>,
        get_script: Mutex<VecDeque<ReadScript>>,
        delete_script: Mutex<VecDeque<WriteScript>>,
        puts: AtomicUsize,
        gets: AtomicUsize,
        deletes: AtomicUsize,
    }

    impl fmt::Debug for FlakyStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("FlakyStore").finish_non_exhaustive()
        }
    }

    impl fmt::Display for FlakyStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "FlakyStore")
        }
    }

    fn transport_glitch() -> provider_store::Error {
        provider_store::Error::Generic {
            store: "flaky",
            source: "error sending request".into(),
        }
    }

    fn auth_rejection(location: &Path) -> provider_store::Error {
        provider_store::Error::PermissionDenied {
            path: location.to_string(),
            source: "access denied".into(),
        }
    }

    #[async_trait]
    impl provider_store::ObjectStore for FlakyStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> provider_store::Result<PutResult> {
            self.puts.fetch_add(1, Ordering::SeqCst);
            let script = self.put_script.lock().expect("put script").pop_front();
            match script {
                Some(WriteScript::FailWithoutLanding) => Err(transport_glitch()),
                Some(WriteScript::LandThenFail) => {
                    self.inner.put_opts(location, payload, opts).await?;
                    Err(transport_glitch())
                }
                Some(WriteScript::FailAuth) => Err(auth_rejection(location)),
                None => self.inner.put_opts(location, payload, opts).await,
            }
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> provider_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> provider_store::Result<GetResult> {
            self.gets.fetch_add(1, Ordering::SeqCst);
            let script = self.get_script.lock().expect("get script").pop_front();
            match script {
                Some(ReadScript::NotFound) => Err(provider_store::Error::NotFound {
                    path: location.to_string(),
                    source: "scripted not found".into(),
                }),
                None => self.inner.get_opts(location, options).await,
            }
        }

        async fn delete(&self, location: &Path) -> provider_store::Result<()> {
            self.deletes.fetch_add(1, Ordering::SeqCst);
            let script = self
                .delete_script
                .lock()
                .expect("delete script")
                .pop_front();
            match script {
                Some(WriteScript::FailWithoutLanding) => Err(transport_glitch()),
                Some(WriteScript::LandThenFail) => {
                    self.inner.delete(location).await?;
                    Err(transport_glitch())
                }
                Some(WriteScript::FailAuth) => Err(auth_rejection(location)),
                None => self.inner.delete(location).await,
            }
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, provider_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> provider_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy(&self, from: &Path, to: &Path) -> provider_store::Result<()> {
            self.inner.copy(from, to).await
        }

        async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> provider_store::Result<()> {
            self.inner.copy_if_not_exists(from, to).await
        }
    }

    fn retrying_store(flaky: Arc<FlakyStore>) -> ProviderObjectStore {
        ProviderObjectStore::new(
            flaky,
            ProviderObjectStoreConfig {
                key_prefix: Some("tenant-a".to_owned()),
                sha256_checksum_metadata: true,
            },
        )
        .expect("provider store")
        .with_transport_retry(TransportRetryPolicy {
            max_retries: 4,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(1),
            op_deadline: PROVIDER_OP_DEADLINE,
        })
    }

    /// Deterministic timer that advances a fixed step per reading, so a
    /// deadline is consumed by observations instead of wall time.
    #[derive(Debug)]
    struct SteppingTimer {
        now_ms: std::sync::atomic::AtomicU64,
        step_ms: u64,
    }

    impl SteppingTimer {
        fn new(step_ms: u64) -> Self {
            Self {
                now_ms: std::sync::atomic::AtomicU64::new(0),
                step_ms,
            }
        }
    }

    impl MonotonicTimer for SteppingTimer {
        fn monotonic_now_ms(&self) -> u64 {
            self.now_ms.fetch_add(self.step_ms, Ordering::SeqCst)
        }
    }

    fn script_puts(flaky: &FlakyStore, script: impl IntoIterator<Item = WriteScript>) {
        flaky.put_script.lock().expect("put script").extend(script);
    }

    #[test]
    fn transport_retry_backoff_doubles_and_caps() {
        let policy = TransportRetryPolicy::DEFAULT;
        assert_eq!(
            transport_retry_backoff(&policy, 1),
            Duration::from_millis(100)
        );
        assert_eq!(
            transport_retry_backoff(&policy, 2),
            Duration::from_millis(200)
        );
        assert_eq!(
            transport_retry_backoff(&policy, 8),
            Duration::from_millis(12_800)
        );
        assert_eq!(transport_retry_backoff(&policy, 9), Duration::from_secs(15));
        assert_eq!(
            transport_retry_backoff(&policy, 10),
            Duration::from_secs(15)
        );
    }

    #[tokio::test]
    async fn overwrite_put_retries_transient_transport_failures() {
        let flaky = Arc::new(FlakyStore::default());
        let store = retrying_store(Arc::clone(&flaky));
        script_puts(
            &flaky,
            [
                WriteScript::FailWithoutLanding,
                WriteScript::FailWithoutLanding,
            ],
        );
        let key = "namespaces/demo/uploads/upl_1.json";

        let metadata = store
            .put_overwrite(key, Bytes::from_static(b"session"))
            .await
            .expect("put succeeds after transient failures");

        assert_eq!(flaky.puts.load(Ordering::SeqCst), 3);
        assert!(metadata.etag.is_some());
        assert_eq!(
            store.get(key, None).await.expect("get"),
            Some(Bytes::from_static(b"session"))
        );
    }

    #[tokio::test]
    async fn create_put_resolves_landed_first_attempt_as_success() {
        let flaky = Arc::new(FlakyStore::default());
        let store = retrying_store(Arc::clone(&flaky));
        script_puts(&flaky, [WriteScript::LandThenFail]);
        let key = "namespaces/demo/uploads/upl_2.json";

        let metadata = store
            .put_if_absent(key, Bytes::from_static(b"upload session"))
            .await
            .expect("landed first attempt reported as success");

        assert_eq!(flaky.puts.load(Ordering::SeqCst), 2);
        assert_eq!(flaky.gets.load(Ordering::SeqCst), 1);
        assert_eq!(
            metadata.checksum_sha256,
            Some(sha256_digest(b"upload session"))
        );
        let head = store.head(key).await.expect("head").expect("object exists");
        assert_eq!(metadata.etag, head.etag);
    }

    #[tokio::test]
    async fn create_put_reports_real_conflict_after_transport_failure() {
        let flaky = Arc::new(FlakyStore::default());
        let store = retrying_store(Arc::clone(&flaky));
        let key = "namespaces/demo/wal/00000001.json";
        store
            .put_overwrite(key, Bytes::from_static(b"theirs"))
            .await
            .expect("seed conflicting object");
        script_puts(&flaky, [WriteScript::FailWithoutLanding]);

        let error = store
            .put_if_absent(key, Bytes::from_static(b"mine"))
            .await
            .expect_err("conflicting object is still a conflict");

        assert!(matches!(
            error,
            ObjectStoreError::PreconditionFailed { object_key } if object_key == key
        ));
        assert_eq!(flaky.puts.load(Ordering::SeqCst), 3);
        assert_eq!(flaky.gets.load(Ordering::SeqCst), 1);
        assert_eq!(
            store.get(key, None).await.expect("get"),
            Some(Bytes::from_static(b"theirs"))
        );
    }

    #[tokio::test]
    async fn create_put_without_ambiguity_keeps_precondition_semantics() {
        let flaky = Arc::new(FlakyStore::default());
        let store = retrying_store(Arc::clone(&flaky));
        let key = "namespaces/demo/wal/00000002.json";
        store
            .put_overwrite(key, Bytes::from_static(b"theirs"))
            .await
            .expect("seed existing object");

        let error = store
            .put_if_absent(key, Bytes::from_static(b"mine"))
            .await
            .expect_err("existing object fails the create precondition");

        assert!(matches!(error, ObjectStoreError::PreconditionFailed { .. }));
        assert_eq!(flaky.puts.load(Ordering::SeqCst), 2);
        assert_eq!(flaky.gets.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn create_put_retries_when_conflicting_object_vanishes() {
        let flaky = Arc::new(FlakyStore::default());
        let store = retrying_store(Arc::clone(&flaky));
        script_puts(&flaky, [WriteScript::LandThenFail]);
        flaky
            .get_script
            .lock()
            .expect("get script")
            .push_back(ReadScript::NotFound);
        let key = "namespaces/demo/uploads/upl_3.json";

        let metadata = store
            .put_if_absent(key, Bytes::from_static(b"payload"))
            .await
            .expect("create succeeds once the conflicting object vanishes");

        assert_eq!(flaky.puts.load(Ordering::SeqCst), 3);
        assert_eq!(flaky.gets.load(Ordering::SeqCst), 2);
        assert!(metadata.etag.is_some());
    }

    #[tokio::test]
    async fn compare_and_swap_never_retries_transport_failures() {
        let flaky = Arc::new(FlakyStore::default());
        let store = retrying_store(Arc::clone(&flaky));
        let key = "namespaces/demo/wal/head.json";
        let seeded = store
            .put_overwrite(key, Bytes::from_static(b"one"))
            .await
            .expect("seed head");
        let etag = seeded.etag.expect("etag");
        script_puts(&flaky, [WriteScript::FailWithoutLanding]);

        let error = store
            .compare_and_swap(key, &etag, Bytes::from_static(b"two"))
            .await
            .expect_err("compare-and-swap surfaces the transport failure");

        assert!(matches!(error, ObjectStoreError::Transport { .. }));
        assert_eq!(flaky.puts.load(Ordering::SeqCst), 2);
        assert_eq!(
            store.get(key, None).await.expect("get"),
            Some(Bytes::from_static(b"one"))
        );
    }

    #[tokio::test]
    async fn write_retries_are_bounded_by_policy() {
        let flaky = Arc::new(FlakyStore::default());
        let store = retrying_store(Arc::clone(&flaky));
        script_puts(&flaky, (0..6).map(|_| WriteScript::FailWithoutLanding));
        let key = "namespaces/demo/uploads/upl_4.json";

        let error = store
            .put_overwrite(key, Bytes::from_static(b"payload"))
            .await
            .expect_err("persistent transport failure surfaces after the retry budget");

        assert!(matches!(error, ObjectStoreError::Transport { .. }));
        assert_eq!(flaky.puts.load(Ordering::SeqCst), 5);
    }

    /// The operation deadline is one budget across every retry: once the
    /// elapsed observations consume it, the loop refuses further attempts
    /// even though the count budget has room.
    #[tokio::test]
    async fn write_retries_stop_once_the_operation_deadline_is_spent() {
        let flaky = Arc::new(FlakyStore::default());
        // Each timer reading advances 45s against a 120s deadline: the
        // deadline check after the second failed attempt finds it spent.
        let store = retrying_store(Arc::clone(&flaky))
            .with_monotonic_timer(Arc::new(SteppingTimer::new(45_000)));
        script_puts(&flaky, (0..6).map(|_| WriteScript::FailWithoutLanding));
        let key = "namespaces/demo/uploads/upl_9.json";

        let error = store
            .put_overwrite(key, Bytes::from_static(b"payload"))
            .await
            .expect_err("deadline exhaustion surfaces the transport failure");

        assert!(matches!(error, ObjectStoreError::Transport { .. }));
        let attempts = flaky.puts.load(Ordering::SeqCst);
        assert!(
            attempts < 5,
            "deadline must stop the loop before the count budget ({attempts} attempts)"
        );
    }

    #[tokio::test]
    async fn delete_retries_stop_once_the_operation_deadline_is_spent() {
        let flaky = Arc::new(FlakyStore::default());
        let store = retrying_store(Arc::clone(&flaky))
            .with_monotonic_timer(Arc::new(SteppingTimer::new(45_000)));
        let key = "namespaces/demo/uploads/upl_10.json";
        store
            .put_overwrite(key, Bytes::from_static(b"payload"))
            .await
            .expect("seed object");
        for _ in 0..6 {
            flaky
                .delete_script
                .lock()
                .expect("delete script")
                .push_back(WriteScript::FailWithoutLanding);
        }

        let error = store
            .delete(key)
            .await
            .expect_err("deadline exhaustion surfaces the transport failure");

        assert!(matches!(error, ObjectStoreError::Transport { .. }));
        let attempts = flaky.deletes.load(Ordering::SeqCst);
        assert!(
            attempts < 5,
            "deadline must stop the loop before the count budget ({attempts} attempts)"
        );
    }

    #[test]
    fn operation_deadline_remaining_caps_at_zero() {
        let timer = SteppingTimer::new(70_000);
        let deadline = OperationDeadline::start(&timer, Duration::from_secs(120));
        // First reading after start: 70s elapsed, 50s left.
        assert_eq!(deadline.remaining(), Some(Duration::from_secs(50)));
        // Second reading: 140s elapsed, spent.
        assert_eq!(deadline.remaining(), None);
    }

    #[tokio::test]
    async fn non_transport_provider_errors_are_not_retried() {
        let flaky = Arc::new(FlakyStore::default());
        let store = retrying_store(Arc::clone(&flaky));
        script_puts(&flaky, [WriteScript::FailAuth]);
        let key = "namespaces/demo/uploads/upl_5.json";

        let error = store
            .put_overwrite(key, Bytes::from_static(b"payload"))
            .await
            .expect_err("auth rejection surfaces immediately");

        assert!(matches!(error, ObjectStoreError::Transport { .. }));
        assert_eq!(flaky.puts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn delete_retries_transient_failures_and_landed_deletes() {
        let flaky = Arc::new(FlakyStore::default());
        let store = retrying_store(Arc::clone(&flaky));

        let landed_key = "namespaces/demo/uploads/upl_6.json";
        store
            .put_overwrite(landed_key, Bytes::from_static(b"payload"))
            .await
            .expect("seed object");
        flaky
            .delete_script
            .lock()
            .expect("delete script")
            .push_back(WriteScript::LandThenFail);
        store
            .delete(landed_key)
            .await
            .expect("landed delete converges to success");
        assert_eq!(flaky.deletes.load(Ordering::SeqCst), 2);
        assert!(store.head(landed_key).await.expect("head").is_none());

        let transient_key = "namespaces/demo/uploads/upl_7.json";
        store
            .put_overwrite(transient_key, Bytes::from_static(b"payload"))
            .await
            .expect("seed object");
        flaky
            .delete_script
            .lock()
            .expect("delete script")
            .push_back(WriteScript::FailWithoutLanding);
        store
            .delete(transient_key)
            .await
            .expect("transient delete failure is retried");
        assert_eq!(flaky.deletes.load(Ordering::SeqCst), 4);
        assert!(store.head(transient_key).await.expect("head").is_none());
    }
}
