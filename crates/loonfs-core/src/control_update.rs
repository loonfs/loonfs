//! Shared creates and compare-and-swap retry loops for control objects.

use crate::error::CoreError;
use crate::limits::MAX_CONTENTION_ATTEMPTS;
use crate::time::OperationClock;
use bytes::Bytes;
use loonfs_objectstore::ObjectStoreExtension;
use loonfs_objectstore::{ObjectMetadata, ObjectStore, ObjectStoreError};
use loonfs_types::NamespaceId;
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
/// a mutable record (such as a user snapshot) as soon as it's written. A
/// precondition failure is read back like
/// a transport failure: an earlier attempt's put can take effect after its own
/// read-back found nothing.
///
/// Every put starts inside `clock`'s publication budget, as a manifest put
/// does (format spec, "Clock and operation assumptions"). A read-back isn't
/// checked, so a put that was applied is confirmed after the budget too.
/// When the budget is spent before the first put, the call returns the budget
/// error and has sent nothing. When it's spent before a later put, the call
/// stops and returns the error of the last put it sent, as it does when it
/// runs out of attempts. So the budget error means that no put was sent, and
/// any other error means that one may have been.
pub(crate) async fn create_control_object_under_generated_id<S: ObjectStore + ?Sized>(
    store: &S,
    object_key: &str,
    encoded: Bytes,
    namespace_id: &NamespaceId,
    clock: &OperationClock,
) -> crate::error::Result<ObjectMetadata> {
    let mut last_error = None;
    for _attempt in 0..MAX_CONTENTION_ATTEMPTS {
        if let Err(spent) = clock.ensure_metadata_publication_budget(namespace_id) {
            return Err(last_error.unwrap_or(spent));
        }
        match store.put_if_absent(object_key, encoded.clone()).await {
            Ok(metadata) => return Ok(metadata),
            Err(
                error @ (ObjectStoreError::PreconditionFailed { .. }
                | ObjectStoreError::Transport { .. }),
            ) => match store.get_with_metadata(object_key).await {
                Ok(Some(stored)) => return Ok(stored.metadata),
                Ok(None) => last_error = Some(CoreError::store(object_key, &error)),
                Err(error) => return Err(CoreError::store(object_key, &error)),
            },
            Err(error) => return Err(CoreError::store(object_key, &error)),
        }
    }
    Err(last_error.expect("the positive attempt limit should leave the error of a put"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use loonfs_test_support::stores::{
        BlockingStore, FailStore, InjectedError, KeyPredicate, OperationSelector,
    };
    use tempfile::tempdir;

    fn demo() -> NamespaceId {
        NamespaceId::parse("demo").expect("namespace id")
    }

    fn fresh_clock() -> OperationClock {
        OperationClock::start(1_000, crate::test_support::std_timer())
    }

    #[tokio::test]
    async fn generated_id_create_recovers_when_the_first_write_has_an_ambiguous_outcome() {
        let temp_dir = tempdir().expect("tempdir");
        let inner = LocalFsStore::new(temp_dir.path()).expect("store");
        let object_key = "namespaces/demo/snapshots/snp_0000000000000g000000000001.json";
        let payload = Bytes::from_static(b"generated control record");
        let store = FailStore::new(
            inner,
            KeyPredicate::exact(object_key),
            OperationSelector::PutCreateIfAbsent,
            InjectedError::Transport("lost write acknowledgement".to_owned()),
        )
        .apply_then_fail();
        store.fail_next(1);

        let metadata = create_control_object_under_generated_id(
            &store,
            object_key,
            payload.clone(),
            &demo(),
            &fresh_clock(),
        )
        .await
        .expect("exact read-back reconciles the write that succeeded");

        assert_eq!(store.attempts(), 1);
        assert!(metadata.etag.is_some());
        assert_eq!(
            store.get(object_key, None).await.expect("read record"),
            Some(payload)
        );
    }

    #[tokio::test]
    async fn generated_id_create_adopts_its_own_put_that_is_written_after_the_read_back() {
        let temp_dir = tempdir().expect("tempdir");
        let object_key = "namespaces/demo/snapshots/snp_0000000000000g000000000001.json";
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
        let (namespace_id, clock) = (demo(), fresh_clock());

        let (created, ()) = tokio::join!(
            create_control_object_under_generated_id(
                &store,
                object_key,
                payload.clone(),
                &namespace_id,
                &clock,
            ),
            async {
                gate.wait_until_blocked().await;
                gate.inner()
                    .put_if_absent(object_key, payload.clone())
                    .await
                    .expect("the first put is written late");
                gate.release();
            },
        );

        let metadata = created.expect("the create adopts its own late put");
        assert_eq!(store.attempts(), 2);
        let stored = store
            .get_with_metadata(object_key)
            .await
            .expect("read record")
            .expect("record exists");
        assert_eq!(stored.bytes, payload);
        assert_eq!(metadata, stored.metadata);
    }

    /// Checks that a put that was applied and lost its answer is confirmed by
    /// its read-back even when the budget passed during the read-back.
    #[tokio::test]
    async fn generated_id_create_confirms_a_written_put_after_its_budget() {
        let temp_dir = tempdir().expect("tempdir");
        let object_key = "namespaces/demo/snapshots/snp_0000000000000g000000000001.json";
        let payload = Bytes::from_static(b"generated control record");
        let store = BlockingStore::new(
            FailStore::new(
                LocalFsStore::new(temp_dir.path()).expect("store"),
                KeyPredicate::exact(object_key),
                OperationSelector::PutCreateIfAbsent,
                InjectedError::Transport("lost write acknowledgement".to_owned()),
            )
            .apply_then_fail(),
            KeyPredicate::exact(object_key),
            OperationSelector::GetWithMetadata,
        );
        store.inner().fail_next(1);
        store.block_next();
        let timer = std::sync::Arc::new(loonfs_test_support::clock::ManualClock::new(0));
        let clock = OperationClock::start(1_000, timer.clone());
        let namespace_id = demo();

        let (created, ()) = tokio::join!(
            create_control_object_under_generated_id(
                &store,
                object_key,
                payload.clone(),
                &namespace_id,
                &clock,
            ),
            async {
                store.wait_until_blocked().await;
                timer.advance_ms(crate::limits::METADATA_PUBLICATION_BUDGET_MS + 1);
                store.release();
            },
        );

        let metadata = created.expect("the read-back confirms the written put");
        assert_eq!(store.inner().attempts(), 1);
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
        let object_key = "namespaces/demo/snapshots/snp_0000000000000g000000000001.json";
        // The first put was written and lost its answer; a renewal changed the
        // record before this attempt reads it back.
        let renewed = Bytes::from_static(b"the record with its expiry extended");
        store
            .put_if_absent(object_key, renewed.clone())
            .await
            .expect("the written record, as another caller changed it");

        create_control_object_under_generated_id(
            &store,
            object_key,
            Bytes::from_static(b"the record as written"),
            &demo(),
            &fresh_clock(),
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
