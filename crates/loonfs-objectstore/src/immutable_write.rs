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
    /// The key holds an object whose writer attested no SHA-256, so whether
    /// it holds the supplied bytes is undecided.
    #[error("immutable object `{object_key}` already exists without a sha256 attestation")]
    Unattested {
        /// Durable key whose existing object carries no attestation.
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
            | Self::Unattested { object_key }
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

/// Runs `create`, a create-if-absent of `bytes` under `key` that attests
/// their SHA-256, under one retry deadline, and decides an occupied key by
/// one `head` of its attestation.
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
            match store.head(key).await {
                Ok(Some(existing)) => match &existing.sha256 {
                    Some(attested) if *attested == Checksum::sha256(bytes) => Ok(existing),
                    Some(_) => Err(ImmutableWriteError::DifferentObject {
                        object_key: key.to_owned(),
                    }),
                    None => Err(ImmutableWriteError::Unattested {
                        object_key: key.to_owned(),
                    }),
                },
                Ok(None) => Err(transport(key, conflict)),
                Err(error) => Err(transport(key, error)),
            }
        }
        Err(error) => Err(transport(key, error)),
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
