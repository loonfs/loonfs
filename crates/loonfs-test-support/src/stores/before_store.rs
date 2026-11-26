//! A store that runs a test's action before selected operations, as another
//! process does in the moment before an operation reaches the store.

use super::{Intercept, InterceptStore, Interceptor, OperationContext};
use async_trait::async_trait;
use futures::future::BoxFuture;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};

type Predicate = dyn for<'a> Fn(&OperationContext<'a>) -> bool + Send + Sync;
type Action = dyn Fn(usize) -> BoxFuture<'static, ()> + Send + Sync;

/// Runs an action before each selected operation (see [`BeforeStore`]).
pub struct BeforeInterceptor {
    predicate: Box<Predicate>,
    action: Box<Action>,
    selected: AtomicUsize,
}

impl fmt::Debug for BeforeInterceptor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BeforeInterceptor")
            .field("selected", &self.selected)
            .finish_non_exhaustive()
    }
}

/// A store that runs a test's action before each operation that a predicate
/// selects, and forwards the operation once the action finishes.
///
/// The action gets the number of selected operations before this one, so a
/// test can take a WAL number with a commit before the first put of the
/// operation under test and fold the namespace before the second.
pub type BeforeStore<S> = InterceptStore<S, BeforeInterceptor>;

impl<S> InterceptStore<S, BeforeInterceptor> {
    /// Runs `action` before each operation that `predicate` selects.
    pub fn before(
        inner: S,
        predicate: impl for<'a> Fn(&OperationContext<'a>) -> bool + Send + Sync + 'static,
        action: impl Fn(usize) -> BoxFuture<'static, ()> + Send + Sync + 'static,
    ) -> Self {
        Self::with_interceptor(
            inner,
            BeforeInterceptor {
                predicate: Box::new(predicate),
                action: Box::new(action),
                selected: AtomicUsize::new(0),
            },
        )
    }
}

#[async_trait]
impl Interceptor for BeforeInterceptor {
    type Guard = ();

    async fn before(&self, context: &OperationContext<'_>) -> (Intercept, ()) {
        if (self.predicate)(context) {
            let index = self.selected.fetch_add(1, Ordering::SeqCst);
            (self.action)(index).await;
        }
        (Intercept::Continue, ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stores::OperationKind;
    use bytes::Bytes;
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use loonfs_objectstore::{ObjectStore, ObjectStoreError, ObjectStoreExtension};
    use std::assert_matches;
    use std::sync::Arc;
    use tempfile::tempdir;

    /// Checks that the action finishes before the selected operation reaches
    /// the store, with the count of the selected operations before it, so a
    /// create that the action makes first takes the key.
    #[tokio::test]
    async fn the_action_runs_before_each_selected_operation() {
        let temp_dir = tempdir().expect("tempdir");
        let rival = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store"));
        let store = BeforeStore::before(
            LocalFsStore::new(temp_dir.path()).expect("store"),
            |context| matches!(context.kind(), OperationKind::Put { .. }),
            move |index| {
                let rival = Arc::clone(&rival);
                Box::pin(async move {
                    if index == 0 {
                        rival
                            .put_if_absent("taken", Bytes::from_static(b"rival"))
                            .await
                            .expect("the rival's create");
                    }
                })
            },
        );
        let lost = store
            .put_if_absent("taken", Bytes::from_static(b"mine"))
            .await
            .expect_err("the rival took the key first");
        assert_matches!(lost, ObjectStoreError::PreconditionFailed { .. });
        store
            .put_if_absent("free", Bytes::from_static(b"mine"))
            .await
            .expect("the second put runs no rival create");
        assert_eq!(
            store.get("taken", None).await.expect("get").as_deref(),
            Some(&b"rival"[..])
        );
    }
}
