//! The writer's copy of each namespace hint, raised before every
//! acknowledgement so readers polling the hint see the commit at once.

use crate::NamespaceId;
use loonfs_api::WalNo;
use loonfs_core::control::LoadedHint;
use loonfs_objectstore::ObjectStore;
use std::collections::BTreeMap;
use std::sync::Mutex;

#[derive(Default)]
pub(crate) struct DiscoveryHints {
    known: Mutex<BTreeMap<NamespaceId, LoadedHint>>,
}

impl DiscoveryHints {
    /// Raises the hint's WAL number from the token of the last raise, so the
    /// steady state is one request, and returns the hint as written. A
    /// failed raise never fails the commit: readers probe forward from
    /// whatever the hint says.
    pub(crate) async fn raise<S: ObjectStore + ?Sized>(
        &self,
        store: &S,
        namespace_id: &NamespaceId,
        wal_no: WalNo,
    ) -> Option<LoadedHint> {
        let known = self
            .known
            .lock()
            .expect("hint token lock should be healthy")
            .remove(namespace_id);
        match loonfs_core::control::raise_namespace_hint(store, namespace_id, wal_no, known).await {
            Ok(hint) => {
                self.known
                    .lock()
                    .expect("hint token lock should be healthy")
                    .insert(namespace_id.clone(), hint.clone());
                Some(hint)
            }
            Err(error) => {
                tracing::warn!(%namespace_id, %error, "namespace hint raise failed");
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use loonfs_api::wire::control::{decode_control_object, ControlObjectKind, HintState};
    use loonfs_objectstore::{keys::hint, local_fs_store::LocalFsStore, ObjectStore};
    use loonfs_test_support::stores::{KeyPredicate, OperationClass, RecordingStore};
    use std::sync::Arc;

    #[tokio::test]
    async fn each_batch_raises_the_hint_from_its_last_token() {
        let directory = tempfile::tempdir().expect("directory");
        let namespace_id = NamespaceId::parse("hints").expect("namespace");
        let store = Arc::new(RecordingStore::new(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::any(),
        ));
        let writer = crate::FsWriter::builder_with_store(store.clone())
            .writer_id("writer")
            .build()
            .await
            .expect("writer");
        writer
            .create_namespace(&namespace_id, crate::CreateNamespaceOptions::default())
            .await
            .expect("create");
        let mut engine = loonfs_core::publish::NamespaceCommitEngine::new(namespace_id.clone());
        let context = loonfs_core::MutationContext {
            writer_id: loonfs_api::WriterId::parse("writer").expect("writer"),
            now_ms: 1_000,
        };
        let mut states = Vec::new();
        for index in 0..3 {
            let request = loonfs_core::publish::CommitRequest::single(
                loonfs_api::CommitId::parse(format!("commit-{index}")).expect("commit"),
                loonfs_test_support::test_actor(),
                None,
                loonfs_core::publish::FilesystemOperation::CreateDirectory {
                    path: loonfs_api::AbsolutePath::parse(format!("/directory-{index}"))
                        .expect("path"),
                    parents: false,
                },
            );
            let result = engine
                .publish_batch(
                    store.as_ref(),
                    vec![loonfs_core::publish::CommitCandidate::new(request)],
                    &context,
                    &Default::default(),
                )
                .await;
            result.results[0].as_ref().expect("commit");
            states.push(result.resulting_read_state.expect("read state").head);
        }
        let hints = DiscoveryHints::default();
        store.reset();
        for state in &states {
            hints
                .raise(store.as_ref(), &namespace_id, state.wal_no)
                .await;
        }
        assert_eq!(store.counts().compare_and_swaps, states.len());
        assert_eq!(
            store.count(OperationClass::Read),
            1,
            "only the first raise reads the hint"
        );
        let bytes = store
            .get(&hint(&namespace_id), None)
            .await
            .expect("read hint")
            .expect("hint");
        let current = decode_control_object::<HintState>(&bytes, ControlObjectKind::Hint)
            .expect("decode hint");
        assert_eq!(current.payload().wal_no, WalNo(4));
        store.reset();
        hints.raise(store.as_ref(), &namespace_id, WalNo(2)).await;
        assert_eq!(store.counts().compare_and_swaps, 0);
        assert_eq!(store.count(OperationClass::Put), 0);
        writer.shutdown().await.expect("shutdown");
    }
}
