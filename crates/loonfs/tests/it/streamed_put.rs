//! What an embedded streamed put costs in memory.
//!
//! The claim is that a put's memory follows the transfer's part size and
//! not the payload's length, and the proof is retention at the store
//! boundary: every payload buffer handed to the store reports when it is
//! released, so the peak is exact rather than sampled. No resident-size
//! reading and no timing are involved.

use crate::common::{open_runtime_async, TestRuntime};
use bytes::Bytes;
use futures::StreamExt;
use loonfs::{
    ByteStream, CreateNamespaceOptions, DestinationBehavior, NamespaceId, PutFileOptions,
    SharedObjectStore,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::PROVIDER_MULTIPART_PART_BYTES;
use loonfs_test_support::stores::BufferWatchStore;
use std::sync::Arc;
use tempfile::tempdir;

const PATH: &str = "/big.bin";
/// Three parts and a bit: enough that holding the payload whole would show
/// up plainly against holding one part of it.
const PAYLOAD_BYTES: usize = 3 * PROVIDER_MULTIPART_PART_BYTES as usize + 4_096;
/// Chunks the size of a source's reads, not the store's parts, so the
/// store's own regrouping is what the bound has to survive.
const CHUNK_BYTES: usize = 64 * 1024;

fn payload(len: usize) -> Vec<u8> {
    (0..len).map(|offset| (offset % 251) as u8).collect()
}

fn streamed(payload: &[u8]) -> ByteStream {
    let chunks: Vec<Bytes> = payload
        .chunks(CHUNK_BYTES)
        .map(Bytes::copy_from_slice)
        .collect();
    futures::stream::iter(chunks.into_iter().map(Ok)).boxed()
}

async fn namespace(runtime: &TestRuntime) -> NamespaceId {
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    runtime
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    namespace_id
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_streamed_put_holds_one_part_of_its_payload() {
    let temp_dir = tempdir().expect("tempdir");
    let watched = Arc::new(BufferWatchStore::watching_content(
        LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
    ));
    let store: SharedObjectStore = watched.clone();
    let runtime = open_runtime_async(store, "writer-a").await;
    let namespace_id = namespace(&runtime).await;
    let payload = payload(PAYLOAD_BYTES);

    runtime
        .writer
        .put_file_stream(
            &namespace_id,
            PATH,
            streamed(&payload),
            PutFileOptions {
                behavior: DestinationBehavior::Replace,
                ..PutFileOptions::new(loonfs_test_support::test_actor())
            },
        )
        .await
        .expect("streamed put");

    let peaks = watched.peaks();
    assert_eq!(
        peaks.total_bytes, PAYLOAD_BYTES as u64,
        "every payload byte crossed the store boundary exactly once"
    );
    assert!(
        peaks.largest_buffer_bytes <= PROVIDER_MULTIPART_PART_BYTES,
        "no single buffer may exceed one part: largest was {}",
        peaks.largest_buffer_bytes
    );
    assert!(
        peaks.peak_live_bytes <= PROVIDER_MULTIPART_PART_BYTES,
        "the put held {} bytes at once, past its one-part window",
        peaks.peak_live_bytes
    );
    assert_eq!(
        runtime
            .reader
            .get_file_bytes(&namespace_id, PATH)
            .await
            .expect("read file")
            .bytes,
        payload,
        "the file that landed is the payload that was streamed"
    );
}
