//! Composable object-store wrappers for test fault injection and observation.

mod blocking_store;
mod buffer_watch_store;
mod concurrency_watch_store;
mod counting_store;
mod fail_store;
mod key_predicate;
mod metadata_map_store;
mod multipart_store;
mod operation;
mod recording_store;

pub use blocking_store::BlockingStore;
pub use buffer_watch_store::{BufferPeaks, BufferWatchStore};
pub use concurrency_watch_store::{ConcurrencyWatchStore, ReadConcurrency};
pub use counting_store::{CountingStore, StoreCounts};
pub use fail_store::{FailStore, FailureMode, InjectedError};
pub use key_predicate::KeyPredicate;
pub use metadata_map_store::MetadataMapStore;
pub use multipart_store::{MultipartChecksumEnforcement, MultipartStore};
pub use operation::{OperationClass, OperationContext, OperationKind};
pub use recording_store::{RecordedGet, RecordedOperation, RecordingStore};

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use futures::TryStreamExt;
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use loonfs_objectstore::ObjectStore;

    #[tokio::test]
    async fn wrappers_preserve_the_start_after_contract() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let store = LocalFsStore::new(temp_dir.path()).expect("local store");
        let store = MultipartStore::new(store);
        let store = MetadataMapStore::new(store, KeyPredicate::any(), |metadata| metadata);
        let store = FailStore::new(
            store,
            KeyPredicate::any(),
            OperationClass::List,
            InjectedError::Transport("unused".to_owned()),
        );
        let store = BlockingStore::new(store, KeyPredicate::any(), OperationClass::List);
        let store = BufferWatchStore::new(store, KeyPredicate::any());
        let store = ConcurrencyWatchStore::new(store, KeyPredicate::any());
        let store = CountingStore::new(store, KeyPredicate::any());
        let store = RecordingStore::new(store, KeyPredicate::any());

        let prefix = "contract/start-after/";
        let keys = [
            format!("{prefix}a"),
            format!("{prefix}c"),
            format!("{prefix}e"),
        ];
        for key in &keys {
            store
                .put_overwrite(key, Bytes::from_static(b"fixture"))
                .await
                .expect("write fixture");
        }

        let exact = store
            .list_prefix_from_stream(prefix, Some(&keys[0]))
            .try_collect::<Vec<_>>()
            .await
            .expect("list after exact key");
        assert_eq!(exact, keys[1..]);

        let absent = format!("{prefix}d");
        let gap = store
            .list_prefix_from_stream(prefix, Some(&absent))
            .try_collect::<Vec<_>>()
            .await
            .expect("list after absent key");
        assert_eq!(gap, keys[2..]);

        let past_end = format!("{prefix}z");
        let complete = store
            .list_prefix_from_stream(prefix, Some(&past_end))
            .try_collect::<Vec<_>>()
            .await
            .expect("list after end");
        assert!(complete.is_empty());
    }
}
