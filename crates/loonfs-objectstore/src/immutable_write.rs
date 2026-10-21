//! Verified writes for objects whose keys name immutable bytes.

use crate::object_store::Result;
use crate::retry::{with_transport_retry, RetryWarnings};
use crate::{ObjectMetadata, ObjectStore, ObjectStoreError, PutMode};
use bytes::Bytes;
use loonfs_types::Checksum;
use std::future::Future;

pub(crate) async fn put<S: ObjectStore + ?Sized>(
    store: &S,
    warnings: &RetryWarnings,
    key: &str,
    bytes: Bytes,
) -> Result<ObjectMetadata> {
    put_with(store, warnings, key, &bytes, || {
        store.put(key, bytes.clone(), PutMode::CreateIfAbsent)
    })
    .await
}

pub(crate) async fn put_with<S, F, Fut>(
    store: &S,
    warnings: &RetryWarnings,
    key: &str,
    bytes: &Bytes,
    create: F,
) -> Result<ObjectMetadata>
where
    S: ObjectStore + ?Sized,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<ObjectMetadata>>,
{
    let written = with_transport_retry(
        warnings,
        key,
        "put_immutable_verified",
        bytes.len() as u64,
        create,
    )
    .await;
    match written {
        Err(conflict @ ObjectStoreError::PreconditionFailed { .. }) => {
            decide_occupied(
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

pub(crate) async fn decide_created<S: ObjectStore + ?Sized>(
    store: &S,
    key: &str,
    expected: &Checksum,
    created: Result<ObjectMetadata>,
) -> Result<ObjectMetadata> {
    match created {
        Err(conflict @ ObjectStoreError::PreconditionFailed { .. }) => {
            decide_occupied(store, key, expected, conflict).await
        }
        created => created,
    }
}

pub(crate) async fn decide_occupied<S: ObjectStore + ?Sized>(
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
    use crate::test_support::SteppingTimer;
    use loonfs_types::OperationDeadline;
    use std::assert_matches;
    use std::time::Duration;

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
        let timer = SteppingTimer::new(70_000);
        let deadline = OperationDeadline::start(&timer, Duration::from_secs(120));
        assert_eq!(deadline.remaining(), Some(Duration::from_secs(50)));
        assert_eq!(deadline.remaining(), None);
    }
}
