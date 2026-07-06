use crate::error::CoreError;
use crate::path::read::{load_metadata_view, LoadedMetadataView, ReadLoadContext};
use loonfs_api::{AuthoritativePathEntry, InodeKind, NamespaceId};
use loonfs_objectstore::ObjectStore;
use sha2::{Digest, Sha256};

/// Lists every visible path in the current manifest-plus-tail namespace view.
pub async fn list_visible_paths<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<Vec<AuthoritativePathEntry>, CoreError> {
    let view = load_metadata_view(store, namespace_id, ReadLoadContext::latest()).await?;
    let mut entries = vec![view.resolve_path("/").await?];
    visit_visible_tree(&view, "/", &mut entries).await?;
    entries.sort_by(|left, right| left.absolute_path.cmp(&right.absolute_path));
    Ok(entries)
}

/// Computes a deterministic hash of the current manifest-plus-tail visible tree.
pub async fn visible_tree_hash<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<String, CoreError> {
    let entries = list_visible_paths(store, namespace_id).await?;
    let bytes = serde_json::to_vec(&entries).map_err(|error| {
        CoreError::Internal(format!("failed to encode visible tree hash: {error}"))
    })?;
    let digest = Sha256::digest(bytes);
    Ok(format!("sha256:{digest:x}"))
}

async fn visit_visible_tree<S: ObjectStore + ?Sized>(
    view: &LoadedMetadataView<'_, S>,
    absolute_path: &str,
    entries: &mut Vec<AuthoritativePathEntry>,
) -> Result<(), CoreError> {
    let mut pending = vec![absolute_path.to_owned()];
    while let Some(path) = pending.pop() {
        for entry in view.list_path(&path).await? {
            let is_dir = entry.inode_kind == InodeKind::Directory;
            let child_path = entry.absolute_path.clone();
            entries.push(entry);
            if is_dir {
                pending.push(child_path);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BootstrapOptions, NamespaceEngine, WriteOptions};
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use std::sync::Arc;

    #[tokio::test]
    async fn visible_paths_are_sorted() {
        let (_temp_dir, store, namespace) = populated_namespace().await;

        let paths = list_visible_paths(store.as_ref(), &namespace)
            .await
            .expect("visible paths");
        let actual: Vec<_> = paths.into_iter().map(|entry| entry.absolute_path).collect();

        assert_eq!(actual, vec!["/", "/a", "/a/file.txt", "/z"]);
    }

    #[tokio::test]
    async fn visible_tree_hash_is_deterministic() {
        let (_temp_dir, store, namespace) = populated_namespace().await;

        assert_eq!(
            visible_tree_hash(store.as_ref(), &namespace)
                .await
                .expect("left hash"),
            visible_tree_hash(store.as_ref(), &namespace)
                .await
                .expect("right hash")
        );
    }

    async fn populated_namespace() -> (tempfile::TempDir, Arc<LocalFsStore>, NamespaceId) {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
        let namespace = NamespaceId::parse("inspection").expect("valid namespace id");
        let engine = NamespaceEngine::builder(store.clone())
            .namespace_id(namespace.clone())
            .writer_id("inspection-test")
            .build()
            .expect("engine");

        engine
            .bootstrap_namespace(BootstrapOptions::default())
            .await
            .expect("bootstrap");
        engine
            .create_directory("/z", WriteOptions::default())
            .await
            .expect("create z");
        engine
            .create_directory("/a", WriteOptions::default())
            .await
            .expect("create a");
        engine
            .put_file("/a/file.txt", b"content", WriteOptions::default())
            .await
            .expect("put file");

        (temp_dir, store, namespace)
    }
}
