use crate::error::CoreError;
use crate::namespace::basis::VerifiedNamespaceBasis;
use loon_api::{AuthoritativePathEntry, InodeKind};
use sha2::{Digest, Sha256};

/// Lists every visible path in a verified namespace basis.
pub fn list_visible_paths_from_basis(
    basis: &VerifiedNamespaceBasis,
) -> Result<Vec<AuthoritativePathEntry>, CoreError> {
    let mut entries = vec![crate::path::query::resolve_path_from_basis(basis, "/")?];
    visit_visible_tree(basis, "/", &mut entries)?;
    entries.sort_by(|left, right| left.absolute_path.cmp(&right.absolute_path));
    Ok(entries)
}

/// Computes a deterministic hash of the visible path tree in a verified basis.
pub fn visible_tree_hash_from_basis(basis: &VerifiedNamespaceBasis) -> Result<String, CoreError> {
    let entries = list_visible_paths_from_basis(basis)?;
    let bytes = serde_json::to_vec(&entries).map_err(|error| {
        CoreError::Store(format!("failed to encode visible tree hash: {error}"))
    })?;
    let digest = Sha256::digest(bytes);
    Ok(format!("sha256:{digest:x}"))
}

fn visit_visible_tree(
    basis: &VerifiedNamespaceBasis,
    absolute_path: &str,
    entries: &mut Vec<AuthoritativePathEntry>,
) -> Result<(), CoreError> {
    for entry in crate::path::query::list_path_from_basis(basis, absolute_path)? {
        let is_dir = entry.inode_kind == InodeKind::Dir;
        let child_path = entry.absolute_path.clone();
        entries.push(entry);
        if is_dir {
            visit_visible_tree(basis, &child_path, entries)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::load_verified_namespace_basis;
    use crate::{BootstrapOptions, NamespaceEngine, WriteOptions};
    use loon_api::NamespaceId;
    use loon_objectstore::fs::LocalFsStore;
    use std::sync::Arc;

    #[tokio::test]
    async fn visible_paths_from_basis_are_sorted() {
        let (_temp_dir, store, namespace) = populated_namespace().await;
        let basis = load_verified_namespace_basis(&store, &namespace)
            .await
            .expect("load basis");

        let paths = list_visible_paths_from_basis(&basis).expect("visible paths");
        let actual: Vec<_> = paths.into_iter().map(|entry| entry.absolute_path).collect();

        assert_eq!(actual, vec!["/", "/a", "/a/file.txt", "/z"]);
    }

    #[tokio::test]
    async fn visible_tree_hash_from_basis_is_deterministic() {
        let (_temp_dir, store, namespace) = populated_namespace().await;
        let left = load_verified_namespace_basis(&store, &namespace)
            .await
            .expect("load left basis");
        let right = load_verified_namespace_basis(&store, &namespace)
            .await
            .expect("load right basis");

        assert_eq!(
            visible_tree_hash_from_basis(&left).expect("left hash"),
            visible_tree_hash_from_basis(&right).expect("right hash")
        );
    }

    async fn populated_namespace() -> (tempfile::TempDir, Arc<LocalFsStore>, NamespaceId) {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
        let namespace = NamespaceId::parse("inspection").expect("valid namespace id");
        let engine = NamespaceEngine::builder(store.clone())
            .namespace(namespace.clone())
            .writer("inspection-test")
            .build()
            .expect("engine");

        engine
            .bootstrap_namespace(BootstrapOptions::default())
            .await
            .expect("bootstrap");
        engine
            .create_dir("/z", WriteOptions::default())
            .await
            .expect("create z");
        engine
            .create_dir("/a", WriteOptions::default())
            .await
            .expect("create a");
        engine
            .put_file("/a/file.txt", b"content", WriteOptions::default())
            .await
            .expect("put file");

        (temp_dir, store, namespace)
    }
}
