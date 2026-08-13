//! Namespace seeding for this crate's integration tests, through the same
//! public writer any embedded host uses.

use loonfs::{CreateNamespaceOptions, FsWriter, PutFileOptions, SharedObjectStore};
use loonfs_api::{CommitId, NamespaceId};

pub(crate) async fn writer(
    store: SharedObjectStore,
    namespace_id: &NamespaceId,
    writer_id: String,
) -> FsWriter {
    let writer = FsWriter::builder_with_store(store)
        .writer_id(writer_id)
        // A seeded namespace is published one file at a time and read back
        // immediately, so the commit window only adds delay.
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("build seeding writer");
    writer
        .create_namespace(namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    writer
}

pub(crate) async fn put_file(
    writer: &FsWriter,
    namespace_id: &NamespaceId,
    bytes: &'static [u8],
    path: &str,
    commit_id: &str,
) {
    writer
        .put_file_bytes(namespace_id, path, bytes, {
            let mut options = PutFileOptions::new(loonfs_test_support::test_actor());
            options.commit.commit_id = Some(CommitId::parse(commit_id).expect("commit id"));
            options
        })
        .await
        .expect("publish file");
}
