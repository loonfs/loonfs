//! Verified writes for objects whose keys name immutable bytes.

use crate::retry::{with_transport_retry, DEFAULT};
use crate::timing::StdMonotonicTimer;
use crate::{ObjectMetadata, ObjectStore, ObjectStoreError, PutMode};
use bytes::Bytes;
use loonfs_types::{Checksum, OperationDeadline};
use std::future::Future;
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
    /// The provider reports no stored checksum for the occupied key.
    #[error("immutable object `{object_key}` already exists without a stored checksum")]
    StoredChecksumMissing {
        /// Durable key whose existing object has no stored checksum.
        object_key: String,
    },
    /// The storage boundary failed before byte identity could be established.
    #[error("immutable write transport failed for `{object_key}`: {source}")]
    Transport {
        /// Durable key whose byte identity could not be established.
        object_key: String,
        /// Final storage failure after retries.
        #[source]
        source: ObjectStoreError,
    },
}

impl ImmutableWriteError {
    /// Returns the immutable object key whose byte identity was not established.
    pub fn object_key(&self) -> &str {
        match self {
            Self::DifferentObject { object_key }
            | Self::StoredChecksumMissing { object_key }
            | Self::Transport { object_key, .. } => object_key,
        }
    }
}

pub(crate) async fn put<S: ObjectStore + ?Sized>(
    store: &S,
    key: &str,
    bytes: Bytes,
) -> std::result::Result<ObjectMetadata, ImmutableWriteError> {
    put_with(store, key, &bytes, || {
        store.put(key, bytes.clone(), PutMode::CreateIfAbsent)
    })
    .await
}

pub(crate) async fn put_with<S, F, Fut>(
    store: &S,
    key: &str,
    bytes: &Bytes,
    create: F,
) -> std::result::Result<ObjectMetadata, ImmutableWriteError>
where
    S: ObjectStore + ?Sized,
    F: FnMut() -> Fut,
    Fut: Future<Output = crate::object_store::Result<ObjectMetadata>>,
{
    let timer = StdMonotonicTimer::default();
    let deadline = OperationDeadline::start(&timer, DEFAULT.operation_deadline);
    let written = with_transport_retry(
        &DEFAULT,
        key,
        "put_immutable_verified",
        bytes.len() as u64,
        Some(&deadline),
        |error: &ObjectStoreError| matches!(error, ObjectStoreError::Transport { .. }),
        create,
    )
    .await;
    match written {
        Ok(metadata) => Ok(metadata),
        Err(conflict @ ObjectStoreError::PreconditionFailed { .. }) => {
            decide_occupied(
                store,
                key,
                &Checksum::compute(store.checksum_algorithm(), bytes),
                conflict,
            )
            .await
        }
        Err(error) => Err(transport(key, error)),
    }
}

pub(crate) async fn decide_created<S: ObjectStore + ?Sized>(
    store: &S,
    key: &str,
    expected: &Checksum,
    created: crate::object_store::Result<ObjectMetadata>,
) -> std::result::Result<ObjectMetadata, ImmutableWriteError> {
    match created {
        Ok(metadata) => Ok(metadata),
        Err(conflict @ ObjectStoreError::PreconditionFailed { .. }) => {
            decide_occupied(store, key, expected, conflict).await
        }
        Err(error) => Err(transport(key, error)),
    }
}

pub(crate) async fn decide_occupied<S: ObjectStore + ?Sized>(
    store: &S,
    key: &str,
    expected: &Checksum,
    conflict: ObjectStoreError,
) -> std::result::Result<ObjectMetadata, ImmutableWriteError> {
    match store.head(key).await {
        Ok(Some(existing)) => check_stored_checksum(key, expected, existing),
        Ok(None) => Err(transport(key, conflict)),
        Err(error) => Err(transport(key, error)),
    }
}

pub(crate) fn check_stored_checksum(
    key: &str,
    expected: &Checksum,
    existing: ObjectMetadata,
) -> std::result::Result<ObjectMetadata, ImmutableWriteError> {
    match &existing.checksum {
        Some(actual) if actual == expected => Ok(existing),
        Some(_) => Err(ImmutableWriteError::DifferentObject {
            object_key: key.to_owned(),
        }),
        None => Err(ImmutableWriteError::StoredChecksumMissing {
            object_key: key.to_owned(),
        }),
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
    fn occupied_keys_require_an_equal_stored_checksum_in_the_same_algorithm() {
        let expected = Checksum::crc64nvme(b"bytes");
        let metadata = |checksum| ObjectMetadata {
            etag: None,
            version: None,
            size_bytes: 5,
            last_modified_ms: None,
            checksum,
        };
        assert!(check_stored_checksum("key", &expected, metadata(Some(expected.clone()))).is_ok());
        for actual in [Checksum::crc64nvme(b"other"), Checksum::crc32c(b"bytes")] {
            assert!(matches!(
                check_stored_checksum("key", &expected, metadata(Some(actual))),
                Err(ImmutableWriteError::DifferentObject { .. })
            ));
        }
        assert!(matches!(
            check_stored_checksum("key", &expected, metadata(None)),
            Err(ImmutableWriteError::StoredChecksumMissing { .. })
        ));
    }

    #[test]
    fn immutable_retry_deadline_is_one_budget_across_attempts() {
        let timer = SteppingTimer::new(70_000);
        let deadline = OperationDeadline::start(&timer, Duration::from_secs(120));
        assert_eq!(deadline.remaining(), Some(Duration::from_secs(50)));
        assert_eq!(deadline.remaining(), None);
    }
}
