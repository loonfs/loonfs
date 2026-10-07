//! Verified writes for objects whose keys name immutable bytes.

use crate::retry::{next_retry_backoff, transport_retry_pause, DEFAULT};
use crate::timing::StdMonotonicTimer;
use crate::{
    ByteRange, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode,
    PROVIDER_MULTIPART_THRESHOLD_BYTES,
};
use bytes::Bytes;
use loonfs_types::OperationDeadline;
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
) -> std::result::Result<ObjectMetadata, ImmutableWriteError> {
    let timer = StdMonotonicTimer::default();
    let retry_policy = DEFAULT;
    let overwrite = bytes.len() as u64 >= PROVIDER_MULTIPART_THRESHOLD_BYTES;
    let deadline = OperationDeadline::start(&timer, retry_policy.operation_deadline);
    let mut retries = 0;
    let mut ambiguous_transport = None;

    loop {
        let attempt = if overwrite {
            // An overwrite cannot refuse an occupied key, so every attempt,
            // retries included, compares the key before writing.
            match compare_existing(store, key, &bytes).await {
                Ok(ImmutableReadback::Missing) => {
                    store.put(key, bytes.clone(), PutMode::Overwrite).await
                }
                Ok(ImmutableReadback::Identical(metadata)) => return Ok(metadata),
                Ok(ImmutableReadback::Different) => return Err(different_object(key)),
                Err(error) => Err(error),
            }
        } else {
            store.put(key, bytes.clone(), PutMode::CreateIfAbsent).await
        };
        match attempt {
            Ok(metadata) => return Ok(metadata),
            Err(error @ ObjectStoreError::PreconditionFailed { .. }) => {
                return resolve_readback(store, key, &bytes, ambiguous_transport.unwrap_or(error))
                    .await;
            }
            Err(error @ ObjectStoreError::Transport { .. }) => {
                let Some(backoff) = next_retry_backoff(
                    &retry_policy,
                    key,
                    "put_immutable_verified",
                    bytes.len() as u64,
                    &mut retries,
                    Some(&deadline),
                ) else {
                    return resolve_readback(store, key, &bytes, error).await;
                };
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
) -> std::result::Result<ObjectMetadata, ImmutableWriteError> {
    match readback(store, key, expected).await {
        Ok(ImmutableReadback::Identical(metadata)) => Ok(metadata),
        Ok(ImmutableReadback::Different) => Err(different_object(key)),
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

/// Compares what `key` holds with `expected` without writing.
///
/// A stored checksum decides without a download. Without one, the object is
/// read, but never more than one byte past the length of `expected`. An
/// identical object reports only its size.
async fn compare_existing<S: ObjectStore + ?Sized>(
    store: &S,
    key: &str,
    expected: &Bytes,
) -> crate::Result<ImmutableReadback> {
    let size_bytes = expected.len() as u64;
    let identical = match store.head_stored_checksum(key).await {
        Ok(None) => return Ok(ImmutableReadback::Missing),
        Ok(Some(stored)) => stored.size_bytes == size_bytes && stored.checksum.matches(expected),
        Err(ObjectStoreError::StoredChecksumMissing { .. } | ObjectStoreError::Unsupported(_)) => {
            // The extra byte tells a longer object apart from this payload.
            let range = ByteRange {
                start_inclusive: 0,
                end_exclusive: size_bytes + 1,
            };
            match store.get(key, Some(range)).await? {
                Some(existing) => existing == *expected,
                None => return Ok(ImmutableReadback::Missing),
            }
        }
        Err(error) => return Err(error),
    };
    Ok(if identical {
        ImmutableReadback::Identical(ObjectMetadata {
            etag: None,
            version: None,
            size_bytes,
            last_modified_ms: None,
        })
    } else {
        ImmutableReadback::Different
    })
}

fn different_object(key: &str) -> ImmutableWriteError {
    ImmutableWriteError::DifferentObject {
        object_key: key.to_owned(),
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
