//! What an embedded streaming read costs in memory, and what it verifies.
//!
//! The claim is that a read's memory follows its chunk size and not the
//! file's length, and the proof is retention at the store boundary: every
//! buffer the store hands back reports when it is released, so the peak is
//! exact rather than sampled. The buffered read is measured beside it as the
//! control — it is supposed to hold the whole file, and the difference
//! between the two numbers is the whole point of the streaming one.

use crate::common::{open_runtime_async, store, TestRuntime};
use loonfs::{
    CreateNamespaceOptions, DestinationBehavior, ErrorCode, FsReader, NamespaceId, PutFileOptions,
    ReadFileStreamOptions, SharedObjectStore,
};
use loonfs_objectstore::keys::content_blob;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::ObjectStore;
use loonfs_test_support::stores::BufferWatchStore;
use std::num::NonZeroU64;
use std::path::Path;
use std::sync::Arc;
use tempfile::tempdir;

const PATH: &str = "/big.bin";
/// Small enough to keep the test in kilobytes, and read in enough chunks
/// that a read holding the file whole could not hide inside one.
const CHUNK_BYTES: u64 = 64 * 1024;
const CHUNKS: u64 = 10;
const PAYLOAD_BYTES: usize = (CHUNK_BYTES * CHUNKS) as usize + 17;

fn payload(len: usize) -> Vec<u8> {
    (0..len).map(|offset| (offset % 251) as u8).collect()
}

fn chunked() -> ReadFileStreamOptions {
    ReadFileStreamOptions {
        chunk_bytes: NonZeroU64::new(CHUNK_BYTES).expect("non-zero chunk size"),
        ..ReadFileStreamOptions::default()
    }
}

async fn namespace(runtime: &TestRuntime) -> NamespaceId {
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    runtime
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    namespace_id
}

/// Writes the payload through a plain store, so only the read that follows
/// is measured, and returns a reader over a watched view of the same root.
async fn written_file(
    root: &Path,
    bytes: &[u8],
) -> (NamespaceId, Arc<BufferWatchStore<LocalFsStore>>, FsReader) {
    let runtime = open_runtime_async(store(root), "writer-a").await;
    let namespace_id = namespace(&runtime).await;
    runtime
        .writer
        .put_file_bytes(
            &namespace_id,
            PATH,
            bytes,
            PutFileOptions {
                behavior: DestinationBehavior::Replace,
                ..PutFileOptions::new(loonfs_test_support::test_actor())
            },
        )
        .await
        .expect("put file");

    let watched = Arc::new(BufferWatchStore::watching_content(
        LocalFsStore::new(root).expect("create local-fs store"),
    ));
    let store: SharedObjectStore = watched.clone();
    let reader = FsReader::builder_with_store(store)
        .build()
        .await
        .expect("build reader");
    (namespace_id, watched, reader)
}

/// A streamed read never holds more of the file than one chunk, and every
/// byte crosses the store boundary exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_streamed_read_holds_one_chunk_of_its_file() {
    let temp_dir = tempdir().expect("tempdir");
    let payload = payload(PAYLOAD_BYTES);
    let (namespace_id, watched, reader) = written_file(temp_dir.path(), &payload).await;

    let mut stream = reader
        .read_file_stream(&namespace_id, PATH, chunked())
        .await
        .expect("open stream");
    assert_eq!(stream.size_bytes(), PAYLOAD_BYTES as u64);
    assert_eq!(stream.entry().path.as_str(), PATH);

    let mut read = Vec::with_capacity(PAYLOAD_BYTES);
    let mut chunks = 0u64;
    // Each chunk is dropped before the next is asked for, which is what a
    // consumer writing to a file or a pipe does, and what the liveness
    // number below is about.
    while let Some(chunk) = stream.next_chunk().await.expect("chunk") {
        assert!(chunk.len() as u64 <= CHUNK_BYTES);
        read.extend_from_slice(&chunk);
        chunks += 1;
        let peaks = watched.peaks();
        assert!(
            peaks.peak_live_bytes <= CHUNK_BYTES,
            "the read held {} bytes at once, past its one-chunk window",
            peaks.peak_live_bytes
        );
    }

    assert_eq!(
        read, payload,
        "the file that arrives is the file that landed"
    );
    assert_eq!(chunks, CHUNKS + 1, "ten full chunks and the remainder");
    let peaks = watched.peaks();
    assert_eq!(
        peaks.total_bytes, PAYLOAD_BYTES as u64,
        "every byte crossed the store boundary exactly once"
    );
    assert!(
        peaks.largest_buffer_bytes <= CHUNK_BYTES,
        "no single buffer may exceed one chunk: largest was {}",
        peaks.largest_buffer_bytes
    );
}

/// The control the number above is measured against: the buffered read is
/// supposed to hold the whole file, and does.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_buffered_read_holds_the_whole_file() {
    let temp_dir = tempdir().expect("tempdir");
    let payload = payload(PAYLOAD_BYTES);
    let (namespace_id, watched, reader) = written_file(temp_dir.path(), &payload).await;

    let read = reader
        .get_file_bytes(&namespace_id, PATH)
        .await
        .expect("buffered read");

    assert_eq!(read.bytes, payload);
    assert_eq!(
        watched.peaks().largest_buffer_bytes,
        PAYLOAD_BYTES as u64,
        "the buffered read materializes the file in one buffer"
    );
}

/// Content that disagrees with its reference fails the read, and fails it at
/// the end — after the chunks are out, which is exactly why a caller that
/// installs a file installs it only once the stream has ended cleanly.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_streamed_read_rejects_content_that_stopped_matching_its_reference() {
    let temp_dir = tempdir().expect("tempdir");
    let payload = payload(PAYLOAD_BYTES);
    let (namespace_id, _watched, reader) = written_file(temp_dir.path(), &payload).await;

    // Same length, different bytes: nothing but the digest can tell, which
    // is the case the read exists to catch.
    let entry = reader
        .stat_path(&namespace_id, PATH, Default::default())
        .await
        .expect("stat file");
    let content_ref = entry.content_ref().cloned().expect("a file has content");
    let catalog =
        loonfs::control::load_namespace_catalog_entry(&store(temp_dir.path()), &namespace_id)
            .await
            .expect("load catalog");
    let key = content_blob(catalog.content_store_id(), &content_ref.content_id);
    let mut corrupted = payload.clone();
    corrupted[0] ^= 0xff;
    store(temp_dir.path())
        .put_overwrite(&key, corrupted.into())
        .await
        .expect("overwrite content object");

    let mut stream = reader
        .read_file_stream(&namespace_id, PATH, chunked())
        .await
        .expect("open stream");
    let error = loop {
        match stream.next_chunk().await {
            Ok(Some(_)) => continue,
            Ok(None) => break None,
            Err(error) => break Some(error),
        }
    }
    .expect("corrupted content must not report a verified end");
    assert_eq!(error.code(), ErrorCode::NamespaceCorrupt);
}
