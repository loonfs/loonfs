//! Composable object-store wrappers for test fault injection and observation.

mod blocking_store;
mod buffer_watch_store;
mod concurrency_watch_store;
mod delegate;
mod fail_store;
mod fake_multipart_store;
mod intercept_store;
mod key_predicate;
mod latency_store;
mod lost_create_store;
mod memory_store;
mod metadata_map_store;
mod namespace_overlap_store;
mod operation;
mod recording_store;
mod replace;

pub use crate::delegate_object_store;
pub use blocking_store::{BlockingInterceptor, BlockingStore};
pub use buffer_watch_store::{BufferPeaks, BufferWatchStore};
pub use concurrency_watch_store::{
    ConcurrencyWatchInterceptor, ConcurrencyWatchStore, InFlightGuard, StoreConcurrency,
};
pub use fail_store::{FailInterceptor, FailStore, FailureMode, InjectedError};
pub use fake_multipart_store::{FakeMultipartStore, MultipartChecksumEnforcement};
pub use intercept_store::{Intercept, InterceptStore, Interceptor, Outcome};
pub use key_predicate::KeyPredicate;
pub use latency_store::{LatencyInterceptor, LatencyStore};
pub use lost_create_store::LostCreateStore;
pub use memory_store::MemoryStore;
pub use metadata_map_store::MetadataMapStore;
pub use namespace_overlap_store::{
    NamespaceInFlightGuard, NamespaceOverlapInterceptor, NamespaceOverlapStore,
};
pub use operation::{
    sort_head_runs, OperationContext, OperationKind, OperationSelector, RecordedOperation,
};
pub use recording_store::{RecordedGet, RecordingInterceptor, RecordingStore, StoreCounts};
pub use replace::replace_object;

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use futures::TryStreamExt;
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use loonfs_objectstore::ObjectStore;
    use loonfs_types::Checksum;

    #[tokio::test]
    async fn wrappers_preserve_the_start_after_contract() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let store = LocalFsStore::new(temp_dir.path()).expect("local store");
        let store = FakeMultipartStore::new(store);
        let store = MetadataMapStore::new(store, KeyPredicate::any(), |metadata| metadata);
        let store = FailStore::new(
            store,
            KeyPredicate::any(),
            OperationSelector::List,
            InjectedError::Transport("unused".to_owned()),
        );
        let store = BlockingStore::new(store, KeyPredicate::any(), OperationSelector::List);
        let store = BufferWatchStore::new(store, KeyPredicate::any());
        let store = ConcurrencyWatchStore::new(store, KeyPredicate::any());
        let store = RecordingStore::new(store, KeyPredicate::any());

        let prefix = "contract/start-after/";
        let keys = [
            format!("{prefix}a"),
            format!("{prefix}c"),
            format!("{prefix}e"),
        ];
        for key in &keys {
            store
                .put_if_absent(key, Bytes::from_static(b"fixture"))
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

        let multipart_key = "contract/multipart";
        let provider_upload_id = store
            .create_multipart_upload(multipart_key)
            .await
            .expect("create multipart upload");
        let checksum = Checksum::crc64nvme(&[]);
        store
            .complete_multipart_upload(multipart_key, &provider_upload_id, &[], &checksum)
            .await
            .expect("complete multipart upload");
        replace_object(&store, multipart_key, Bytes::from_static(b"replaced"))
            .await
            .expect("replace the assembled object");
        let replaced = store
            .head(multipart_key)
            .await
            .expect("head the replaced object")
            .expect("the replaced object exists");
        assert_eq!(replaced.checksum, Some(Checksum::crc64nvme(b"replaced")));
    }
}
