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
    /// Whether the key holds the supplied bytes is undecided: the object
    /// there carries no SHA-256 attestation, or the write had none to
    /// compare with it.
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
            decide_occupied(store, key, &Checksum::sha256(bytes), conflict).await
        }
        Err(error) => Err(transport(key, error)),
    }
}

/// Decides a create that attested `sha256` when given: a key found
/// occupied holds this object only when its attestation equals `sha256`.
pub(crate) async fn decide_created<S: ObjectStore + ?Sized>(
    store: &S,
    key: &str,
    sha256: Option<&Checksum>,
    created: crate::object_store::Result<ObjectMetadata>,
) -> std::result::Result<ObjectMetadata, ImmutableWriteError> {
    match created {
        Ok(metadata) => Ok(metadata),
        Err(conflict @ ObjectStoreError::PreconditionFailed { .. }) => match sha256 {
            Some(sha256) => decide_occupied(store, key, sha256, conflict).await,
            None => Err(ImmutableWriteError::Unattested {
                object_key: key.to_owned(),
            }),
        },
        Err(error) => Err(transport(key, error)),
    }
}

async fn decide_occupied<S: ObjectStore + ?Sized>(
    store: &S,
    key: &str,
    sha256: &Checksum,
    conflict: ObjectStoreError,
) -> std::result::Result<ObjectMetadata, ImmutableWriteError> {
    match store.head(key).await {
        Ok(Some(existing)) => match &existing.sha256 {
            Some(attested) if attested == sha256 => Ok(existing),
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
