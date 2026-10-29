//! Shared creates and compare-and-swap retry loops for control objects.

use crate::error::CoreError;
use crate::limits::MAX_CONTENTION_ATTEMPTS;
use bytes::Bytes;
use loonfs_objectstore::ObjectStoreExtension;
use loonfs_objectstore::{ObjectMetadata, ObjectStore, ObjectStoreError};
use std::future::Future;

/// An attempt whose write may have taken effect reads it back before it returns.
#[derive(Debug)]
pub(crate) enum CasOutcome<T, R> {
    Settled(T),
    Contended(R),
}

/// Runs up to [`MAX_CONTENTION_ATTEMPTS`] attempts.
pub(crate) async fn retry_while_contended<T, R, E, F, Fut>(
    mut attempt: F,
) -> std::result::Result<std::result::Result<T, R>, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = std::result::Result<CasOutcome<T, R>, E>>,
{
    let mut last_reason = None;
    for _attempt in 0..MAX_CONTENTION_ATTEMPTS {
        match attempt().await? {
            CasOutcome::Settled(outcome) => return Ok(Ok(outcome)),
            CasOutcome::Contended(reason) => last_reason = Some(reason),
        }
    }
    Ok(Err(last_reason.expect(
        "the positive contention retry limit should produce a contention reason",
    )))
}

/// Creates a control object under a freshly generated id.
///
/// The id is the object's identity: an object under it is this creation's
/// own put, whatever its bytes contain now, because another caller can change
/// a mutable record (such as a user pin) as soon as it's written. A
/// precondition failure is read back like
/// a transport failure: an earlier attempt's put can take effect after its own
/// read-back found nothing.
pub(crate) async fn create_control_object_under_generated_id<S: ObjectStore + ?Sized>(
    store: &S,
    object_key: &str,
    encoded: Bytes,
) -> crate::error::Result<ObjectMetadata> {
    retry_while_contended(|| async {
        match store.put_if_absent(object_key, encoded.clone()).await {
            Ok(metadata) => Ok(CasOutcome::Settled(metadata)),
            Err(
                error @ (ObjectStoreError::PreconditionFailed { .. }
                | ObjectStoreError::Transport { .. }),
            ) => match store.get_with_metadata(object_key).await {
                Ok(Some(stored)) => Ok(CasOutcome::Settled(stored.metadata)),
                Ok(None) => Ok(CasOutcome::Contended(CoreError::store(object_key, &error))),
                Err(error) => Err(CoreError::store(object_key, &error)),
            },
            Err(error) => Err(CoreError::store(object_key, &error)),
        }
    })
    .await?
}

#[cfg(test)]
mod tests {
    use super::*;
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use loonfs_test_support::stores::{
        BlockingStore, FailStore, InjectedError, KeyPredicate, OperationSelector,
    };
    use tempfile::tempdir;

    #[tokio::test]
    async fn generated_id_create_recovers_when_the_first_write_lands_ambiguously() {
        let temp_dir = tempdir().expect("tempdir");
        let inner = LocalFsStore::new(temp_dir.path()).expect("store");
        let object_key = "namespaces/demo/pins/pin_00000000000000000001-0000000000000001.json";
        let payload = Bytes::from_static(b"generated control record");
        let store = FailStore::new(
            inner,
            KeyPredicate::exact(object_key),
            OperationSelector::PutCreateIfAbsent,
            InjectedError::Transport("lost write acknowledgement".to_owned()),
        )
        .apply_then_fail();
        store.fail_next(1);

        let metadata =
            create_control_object_under_generated_id(&store, object_key, payload.clone())
                .await
                .expect("exact read-back reconciles the landed write");

        assert_eq!(store.attempts(), 1);
        assert!(metadata.etag.is_some());
        assert_eq!(
            store.get(object_key, None).await.expect("read record"),
            Some(payload)
        );
    }

    #[tokio::test]
    async fn generated_id_create_adopts_its_own_put_that_lands_after_the_read_back() {
        let temp_dir = tempdir().expect("tempdir");
        let object_key = "namespaces/demo/pins/pin_00000000000000000001-0000000000000001.json";
        let payload = Bytes::from_static(b"generated control record");
        let store = FailStore::new(
            BlockingStore::new(
                LocalFsStore::new(temp_dir.path()).expect("store"),
                KeyPredicate::exact(object_key),
                OperationSelector::PutCreateIfAbsent,
            ),
            KeyPredicate::exact(object_key),
            OperationSelector::PutCreateIfAbsent,
            InjectedError::Transport("lost write request".to_owned()),
        );
        store.fail_next(1);
        let gate = store.inner();
        gate.block_next();

        let (created, ()) = tokio::join!(
            create_control_object_under_generated_id(&store, object_key, payload.clone()),
            async {
                gate.wait_until_blocked().await;
                gate.inner()
                    .put_if_absent(object_key, payload.clone())
                    .await
                    .expect("the first put lands late");
                gate.release();
            },
        );

        let metadata = created.expect("the create adopts its own late-landed put");
        assert_eq!(store.attempts(), 2);
        let stored = store
            .get_with_metadata(object_key)
            .await
            .expect("read record")
            .expect("record exists");
        assert_eq!(stored.bytes, payload);
        assert_eq!(metadata, stored.metadata);
    }

    #[tokio::test]
    async fn generated_id_create_adopts_its_record_after_another_caller_changed_it() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let object_key = "namespaces/demo/pins/pin_00000000000000000001-0000000000000001.json";
        // The first put landed and lost its answer; a renewal changed the
        // record before this attempt reads it back.
        let renewed = Bytes::from_static(b"the record with its expiry extended");
        store
            .put_if_absent(object_key, renewed.clone())
            .await
            .expect("the landed record, as another caller changed it");

        create_control_object_under_generated_id(
            &store,
            object_key,
            Bytes::from_static(b"the record as written"),
        )
        .await
        .expect("the record under the generated id is this creation's own");

        assert_eq!(
            store.get(object_key, None).await.expect("read record"),
            Some(renewed)
        );
    }

    #[tokio::test]
    async fn retry_exhaustion_returns_the_last_contention_reason() {
        let attempt = std::cell::Cell::new(0);
        let exhausted = retry_while_contended(|| {
            let reason = attempt.get();
            attempt.set(reason + 1);
            async move { Ok::<CasOutcome<(), usize>, ()>(CasOutcome::Contended(reason)) }
        })
        .await
        .expect("contention attempts should not fail");

        assert_eq!(exhausted, Err(MAX_CONTENTION_ATTEMPTS - 1));
    }
}
