//! Verified writes for objects whose keys name immutable bytes.

use crate::object_store::Result;
use crate::retry::TransportRetry;
use crate::{ObjectMetadata, ObjectStore, ObjectStoreError};
use bytes::Bytes;
use loonfs_types::Checksum;
use std::future::Future;

/// Creates an immutable object through `create`, sent again under `retry`
/// while it fails with a retryable transport error, and accepts an occupied
/// key only when its stored checksum equals the checksum of `bytes`.
pub(crate) async fn put_with<S, F, Fut>(
    store: &S,
    retry: &TransportRetry,
    key: &str,
    bytes: &Bytes,
    create: F,
) -> Result<ObjectMetadata>
where
    S: ObjectStore + ?Sized,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<ObjectMetadata>>,
{
    let written = retry
        .run(key, "put_immutable_verified", bytes.len() as u64, create)
        .await;
    match written {
        Err(conflict @ ObjectStoreError::PreconditionFailed { .. }) => {
            verify_occupied(
                store,
                key,
                &Checksum::compute(store.checksum_algorithm(), bytes),
                conflict,
            )
            .await
        }
        written => written,
    }
}

pub(crate) async fn verify_created<S: ObjectStore + ?Sized>(
    store: &S,
    key: &str,
    expected: &Checksum,
    created: Result<ObjectMetadata>,
) -> Result<ObjectMetadata> {
    match created {
        Err(conflict @ ObjectStoreError::PreconditionFailed { .. }) => {
            verify_occupied(store, key, expected, conflict).await
        }
        created => created,
    }
}

pub(crate) async fn verify_occupied<S: ObjectStore + ?Sized>(
    store: &S,
    key: &str,
    expected: &Checksum,
    conflict: ObjectStoreError,
) -> Result<ObjectMetadata> {
    match store.head(key).await? {
        Some(existing) => verify_stored_checksum(key, expected, existing),
        None => Err(conflict),
    }
}

pub(crate) fn verify_stored_checksum(
    key: &str,
    expected: &Checksum,
    existing: ObjectMetadata,
) -> Result<ObjectMetadata> {
    match &existing.checksum {
        Some(actual) if actual == expected => Ok(existing),
        Some(_) => Err(ObjectStoreError::DifferentObject {
            object_key: key.to_owned(),
        }),
        None => Err(ObjectStoreError::StoredChecksumMissing {
            object_key: key.to_owned(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use loonfs_test_support::clock::ManualClock;
    use loonfs_types::OperationDeadline;
    use std::assert_matches;

    #[test]
    fn occupied_keys_require_an_equal_stored_checksum_in_the_same_algorithm() {
        let expected = Checksum::crc64nvme(b"bytes");
        let metadata = |checksum| ObjectMetadata {
            etag: None,
            version: None,
            size_bytes: 5,
            last_modified_at_ms: None,
            checksum,
        };
        assert!(verify_stored_checksum("key", &expected, metadata(Some(expected.clone()))).is_ok());
        for actual in [Checksum::crc64nvme(b"other"), Checksum::crc32c(b"bytes")] {
            assert_matches!(
                verify_stored_checksum("key", &expected, metadata(Some(actual))),
                Err(ObjectStoreError::DifferentObject { .. })
            );
        }
        assert_matches!(
            verify_stored_checksum("key", &expected, metadata(None)),
            Err(ObjectStoreError::StoredChecksumMissing { .. })
        );
    }

    #[test]
    fn immutable_retry_deadline_is_one_budget_across_attempts() {
        let timer = ManualClock::stepping(70_000);
        let deadline = OperationDeadline::start(&timer, 120_000);
        assert_eq!(deadline.remaining_ms(), Some(50_000));
        assert_eq!(deadline.remaining_ms(), None);
    }
}
