use loonfs_api::{AbsolutePath, CommitId, DestinationBehavior, NamespaceId};
use loonfs_core::content::{prepare_stored_content, store_bytes_as_content};
use loonfs_core::publish::{NamespaceMutationCandidate, PathMutationIntent};
use loonfs_core::{BootstrapOptions, NamespaceEngine};
use loonfs_objectstore::ObjectStore;

pub(crate) async fn bootstrap<S: ObjectStore + Clone>(
    store: S,
    namespace_id: &NamespaceId,
    writer_id: String,
    writer_session_id: String,
    writer_version: &str,
) -> NamespaceEngine<S> {
    let engine = NamespaceEngine::builder(store)
        .namespace_id(namespace_id.clone())
        .writer_id(writer_id)
        .writer_session_id(writer_session_id)
        .writer_version(writer_version)
        .build()
        .expect("engine");
    engine
        .bootstrap_namespace(BootstrapOptions::default())
        .await
        .expect("bootstrap namespace");
    engine
}

pub(crate) async fn put_file<S: ObjectStore + Clone>(
    store: &S,
    engine: &NamespaceEngine<S>,
    namespace_id: &NamespaceId,
    bytes: &'static [u8],
    path: &str,
    commit_id: &str,
) {
    let stored = store_bytes_as_content(store, namespace_id, bytes)
        .await
        .expect("store content");
    let content_ref = stored.content_ref.clone();
    let catalog = loonfs_core::control::load_namespace_catalog_entry(store, namespace_id)
        .await
        .expect("load namespace catalog");
    let prepared = prepare_stored_content(&catalog, stored).expect("prepare stored content");
    engine
        .publish_namespace_mutations_batch(vec![NamespaceMutationCandidate::path_prepared(
            PathMutationIntent::PutFile {
                commit_id: CommitId::parse(commit_id).expect("commit id"),
                message: None,
                absolute_path: AbsolutePath::parse(path).expect("path"),
                content_ref,
                behavior: DestinationBehavior::NoReplace,
                expected_revision_no: None,
            },
            vec![prepared],
        )])
        .await
        .pop()
        .expect("one result")
        .expect("publish file");
}
