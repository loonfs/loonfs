//! Namespace seeding for this crate's integration tests, through the same
//! public writer any embedded host uses.

use loonfs::{CommitOptions, LoonFs, PutFileOptions, SharedObjectStore, Writable};
use loonfs_types::{CommitId, NamespaceId};

pub(crate) async fn writer(
    store: SharedObjectStore,
    namespace_id: &NamespaceId,
    writer_id: String,
) -> LoonFs<Writable> {
    let writer = LoonFs::builder_with_store(store)
        .writer_id(writer_id)
        // A seeded namespace is published one file at a time and read back
        // immediately, so the commit window only adds delay.
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("build seeding writer");
    writer
        .create_namespace(namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    writer
}

pub(crate) async fn put_file(
    writer: &LoonFs<Writable>,
    namespace_id: &NamespaceId,
    bytes: &'static [u8],
    path: &str,
    commit_id: &str,
) {
    let namespace = writer.open_namespace(namespace_id).expect("open namespace");
    namespace
        .put_file_with_options(
            path,
            bytes,
            &loonfs_test_support::test_actor(),
            &PutFileOptions {
                commit: CommitOptions {
                    commit_id: Some(CommitId::parse(commit_id).expect("commit id")),
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await
        .expect("publish file");
}
