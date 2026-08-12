//! Verified writes for objects whose keys name immutable bytes.

use crate::retry::{
    transport_retry_backoff, transport_retry_pause, OperationDeadline, TransportRetryPolicy,
};
use crate::timing::StdMonotonicTimer;
use crate::{
    ObjectMetadata, ObjectStore, ObjectStoreError, PutMode, PROVIDER_MULTIPART_THRESHOLD_BYTES,
};
use bytes::Bytes;
use thiserror::Error;

/// Failure to verify that an immutable key contains the requested bytes.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ImmutableWriteError {
    /// The key names bytes other than the immutable payload supplied here.
    ///
    /// This is a corruption tripwire, not a provider failure: callers must
    /// preserve that classification at their public error boundary.
    #[error("immutable object `{object_key}` already exists with different bytes")]
    DifferentObject {
        /// Durable key whose existing bytes violated immutability.
        object_key: String,
    },
    /// The storage boundary failed before byte identity could be established.
    #[error("immutable write transport failed for `{object_key}`: {source}")]
    Transport {
        /// Durable key whose byte identity could not be established.
        object_key: String,
        /// Final storage failure after retry and read-back reconciliation.
        #[source]
        source: ObjectStoreError,
    },
}

impl ImmutableWriteError {
    /// Returns the immutable object key whose byte identity was not established.
    pub fn object_key(&self) -> &str {
        match self {
            Self::DifferentObject { object_key } | Self::Transport { object_key, .. } => object_key,
        }
    }
}

pub(crate) async fn put<S: ObjectStore + ?Sized>(
    store: &S,
    key: &str,
    bytes: Bytes,
) -> std::result::Result<(), ImmutableWriteError> {
    let timer = StdMonotonicTimer::default();
    let retry_policy = TransportRetryPolicy::DEFAULT;
    let mode = if bytes.len() as u64 >= PROVIDER_MULTIPART_THRESHOLD_BYTES {
        PutMode::Overwrite
    } else {
        PutMode::CreateIfAbsent
    };
    let deadline = OperationDeadline::start(&timer, retry_policy.operation_deadline);
    let mut retries = 0;
    let mut ambiguous_transport = None;

    loop {
        match store.put(key, bytes.clone(), mode.clone()).await {
            Ok(_) => return Ok(()),
            Err(error @ ObjectStoreError::PreconditionFailed { .. }) => {
                return resolve_readback(store, key, &bytes, ambiguous_transport.unwrap_or(error))
                    .await;
            }
            Err(error @ ObjectStoreError::Transport { .. }) => {
                let Some(remaining) = deadline.remaining() else {
                    return resolve_readback(store, key, &bytes, error).await;
                };
                if retries >= retry_policy.max_retries {
                    return resolve_readback(store, key, &bytes, error).await;
                }
                retries += 1;
                let backoff = transport_retry_backoff(&retry_policy, retries).min(remaining);
                tracing::info!(
                    object_key = key,
                    operation = "put_immutable_verified",
                    retry = retries,
                    max_retries = retry_policy.max_retries,
                    backoff_ms = u64::try_from(backoff.as_millis()).unwrap_or(u64::MAX),
                    error = %error,
                    "transient immutable write failure, backing off before retry",
                );
                ambiguous_transport = Some(error);
                transport_retry_pause(backoff).await;
            }
            Err(error) if ambiguous_transport.is_some() => {
                return resolve_readback(store, key, &bytes, error).await;
            }
            Err(error) => return Err(transport(key, error)),
        }
    }
}

async fn resolve_readback<S: ObjectStore + ?Sized>(
    store: &S,
    key: &str,
    expected: &Bytes,
    original: ObjectStoreError,
) -> std::result::Result<(), ImmutableWriteError> {
    match readback(store, key, expected).await {
        Ok(ImmutableReadback::Identical(_)) => Ok(()),
        Ok(ImmutableReadback::Different) => Err(ImmutableWriteError::DifferentObject {
            object_key: key.to_owned(),
        }),
        Ok(ImmutableReadback::Missing) => Err(transport(key, original)),
        Err(verify_error @ ObjectStoreError::Transport { .. }) => {
            let source = ObjectStoreError::transport(
                key,
                format!(
                    "{}; failed to verify immutable write outcome: {verify_error}",
                    original.message()
                ),
            );
            Err(transport(key, source))
        }
        Err(verify_error) => Err(transport(key, verify_error)),
    }
}

pub(crate) enum ImmutableReadback {
    Identical(ObjectMetadata),
    Different,
    Missing,
}

pub(crate) async fn readback<S: ObjectStore + ?Sized>(
    store: &S,
    key: &str,
    expected: &Bytes,
) -> crate::Result<ImmutableReadback> {
    match store.get_with_metadata(key).await? {
        Some(body) if body.bytes.as_slice() == expected.as_ref() => {
            Ok(ImmutableReadback::Identical(body.metadata))
        }
        Some(_) => Ok(ImmutableReadback::Different),
        None => Ok(ImmutableReadback::Missing),
    }
}

fn transport(key: &str, source: ObjectStoreError) -> ImmutableWriteError {
    ImmutableWriteError::Transport {
        object_key: key.to_owned(),
        source,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::SteppingTimer;
    use std::time::Duration;

    #[test]
    fn immutable_retry_deadline_is_one_budget_across_attempts() {
        let timer = SteppingTimer::new(70_000);
        let deadline = OperationDeadline::start(&timer, Duration::from_secs(120));
        assert_eq!(deadline.remaining(), Some(Duration::from_secs(50)));
        assert_eq!(deadline.remaining(), None);
    }
}
