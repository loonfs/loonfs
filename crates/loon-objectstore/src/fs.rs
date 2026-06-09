use crate::checksum;
use crate::keyspace::validate_segments;
use crate::{ByteRange, ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, BoxStream};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug)]
pub struct LocalFsStore {
    root: PathBuf,
    write_lock: Mutex<()>,
}

impl LocalFsStore {
    pub fn new(root: impl Into<PathBuf>) -> Result<Self, ObjectStoreError> {
        let root = root.into();
        fs::create_dir_all(&root).map_err(io_error)?;
        Ok(Self {
            root,
            write_lock: Mutex::new(()),
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    fn resolve_key(&self, key: &str) -> Result<PathBuf, ObjectStoreError> {
        let segments = validate_segments(key, false)?;
        let mut path = self.root.clone();
        for segment in segments {
            path.push(segment);
        }
        Ok(path)
    }

    fn metadata_for_path(
        path: &Path,
        include_checksum: bool,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        let metadata = fs::metadata(path).map_err(io_error)?;
        let checksum_sha256 = include_checksum
            .then(|| fs::read(path).map(|bytes| checksum::sha256_digest(&bytes)))
            .transpose()
            .map_err(io_error)?;
        Self::metadata_from_fs_metadata(&metadata, checksum_sha256, path)
    }

    fn metadata_from_fs_metadata(
        metadata: &fs::Metadata,
        checksum_sha256: Option<String>,
        path: &Path,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        if !metadata.is_file() {
            return Err(ObjectStoreError::Transport(format!(
                "object path is not a file: {}",
                path.display()
            )));
        }

        let modified_nanos = metadata
            .modified()
            .map_err(io_error)?
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();

        Ok(ObjectMetadata {
            etag: Some(format!("{:x}-{:x}", metadata.len(), modified_nanos)),
            version: None,
            size_bytes: metadata.len(),
            checksum_sha256,
        })
    }

    fn create_new_object(path: &Path, bytes: &[u8]) -> Result<(), ObjectStoreError> {
        ensure_parent_dir(path)?;
        let mut file = match OpenOptions::new().write(true).create_new(true).open(path) {
            Ok(file) => file,
            Err(err) => return Err(map_create_error(err)),
        };

        let result = (|| {
            file.write_all(bytes).map_err(io_error)?;
            file.sync_all().map_err(io_error)
        })();

        if result.is_err() {
            let _ = fs::remove_file(path);
        }

        result
    }

    fn replace_object(path: &Path, bytes: &[u8]) -> Result<(), ObjectStoreError> {
        ensure_parent_dir(path)?;
        let temp_path = temp_path(path);

        let result = (|| {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temp_path)
                .map_err(io_error)?;
            file.write_all(bytes).map_err(io_error)?;
            file.sync_all().map_err(io_error)?;

            match fs::rename(&temp_path, path) {
                Ok(()) => Ok(()),
                Err(rename_error)
                    if path.exists()
                        && matches!(
                            rename_error.kind(),
                            std::io::ErrorKind::AlreadyExists
                                | std::io::ErrorKind::PermissionDenied
                        ) =>
                {
                    fs::remove_file(path).map_err(io_error)?;
                    fs::rename(&temp_path, path).map_err(io_error)
                }
                Err(rename_error) => Err(io_error(rename_error)),
            }
        })();

        if result.is_err() {
            let _ = fs::remove_file(&temp_path);
        }

        result
    }
}

impl LocalFsStore {
    fn head_sync(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        let path = self.resolve_key(key)?;
        if !path.exists() {
            return Ok(None);
        }

        Self::metadata_for_path(&path, false).map(Some)
    }

    fn head_with_checksum_sync(
        &self,
        key: &str,
    ) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        let path = self.resolve_key(key)?;
        if !path.exists() {
            return Ok(None);
        }

        Self::metadata_for_path(&path, true).map(Some)
    }

    fn get_with_metadata_sync(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        let path = self.resolve_key(key)?;
        let mut file = match File::open(&path) {
            Ok(file) => file,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(io_error(err)),
        };
        let fs_metadata = file.metadata().map_err(io_error)?;

        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).map_err(io_error)?;

        let metadata = Self::metadata_from_fs_metadata(
            &fs_metadata,
            Some(checksum::sha256_digest(&bytes)),
            &path,
        )?;
        Ok(Some(ObjectBody { metadata, bytes }))
    }

    fn get_sync(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Vec<u8>>, ObjectStoreError> {
        let path = self.resolve_key(key)?;
        let mut file = match File::open(&path) {
            Ok(file) => file,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(io_error(err)),
        };

        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).map_err(io_error)?;

        match range {
            None => Ok(Some(bytes)),
            Some(range) => {
                let start = usize::try_from(range.start_inclusive)
                    .map_err(|_| ObjectStoreError::InvalidRange)?;
                let mut end = usize::try_from(range.end_exclusive)
                    .map_err(|_| ObjectStoreError::InvalidRange)?;

                if end < start || start > bytes.len() {
                    return Err(ObjectStoreError::InvalidRange);
                }

                end = end.min(bytes.len());
                Ok(Some(bytes[start..end].to_vec()))
            }
        }
    }

    fn put_sync(
        &self,
        key: &str,
        bytes: &[u8],
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        let path = self.resolve_key(key)?;
        let _guard = self
            .write_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        match mode {
            PutMode::Overwrite => Self::replace_object(&path, bytes)?,
            PutMode::CreateIfAbsent => {
                if path.exists() {
                    return Err(ObjectStoreError::PreconditionFailed);
                }
                Self::create_new_object(&path, bytes)?;
            }
            PutMode::CompareAndSwap { expected_etag } => {
                let current = self
                    .head_sync(key)?
                    .ok_or(ObjectStoreError::PreconditionFailed)?;
                if current.etag.as_deref() != Some(expected_etag.as_str()) {
                    return Err(ObjectStoreError::PreconditionFailed);
                }
                Self::replace_object(&path, bytes)?;
            }
        }

        Self::metadata_for_path(&path, true)
    }

    fn delete_sync(&self, key: &str) -> Result<(), ObjectStoreError> {
        let path = self.resolve_key(key)?;
        let _guard = self
            .write_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        match fs::remove_file(&path) {
            Ok(()) => {
                prune_empty_parent_dirs(path.parent(), &self.root)?;
                Ok(())
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(io_error(err)),
        }
    }

    fn list_prefix_sync(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
        validate_segments(prefix, true)?;

        if !self.root.exists() {
            return Ok(Vec::new());
        }

        let mut keys = Vec::new();
        collect_keys(&self.root, &self.root, &mut keys)?;
        keys.retain(|key| key.starts_with(prefix));
        keys.sort();
        Ok(keys)
    }
}

#[async_trait]
impl ObjectStore for LocalFsStore {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.head_sync(key)
    }

    async fn head_with_checksum(
        &self,
        key: &str,
    ) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.head_with_checksum_sync(key)
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        self.get_with_metadata_sync(key)
    }

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        self.get_sync(key, range)
            .map(|maybe| maybe.map(Bytes::from))
    }

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        self.put_sync(key, &bytes, mode)
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.delete_sync(key)
    }

    fn list_prefix_stream(
        &self,
        prefix: &str,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        match self.list_prefix_sync(prefix) {
            Ok(keys) => Box::pin(stream::iter(keys.into_iter().map(Ok))),
            Err(err) => Box::pin(stream::once(async { Err(err) })),
        }
    }
}

fn ensure_parent_dir(path: &Path) -> Result<(), ObjectStoreError> {
    match path.parent() {
        Some(parent) => fs::create_dir_all(parent).map_err(io_error),
        None => Err(ObjectStoreError::InvalidKey(path.display().to_string())),
    }
}

fn collect_keys(
    root: &Path,
    current: &Path,
    keys: &mut Vec<String>,
) -> Result<(), ObjectStoreError> {
    let mut entries = fs::read_dir(current)
        .map_err(io_error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(io_error)?;
    entries.sort_by_key(|entry| entry.path());

    for entry in entries {
        let path = entry.path();
        if path.is_dir() {
            collect_keys(root, &path, keys)?;
            continue;
        }

        if path.is_file() {
            keys.push(relative_key(root, &path)?);
        }
    }

    Ok(())
}

fn relative_key(root: &Path, path: &Path) -> Result<String, ObjectStoreError> {
    let relative = path.strip_prefix(root).map_err(|err| {
        ObjectStoreError::Transport(format!(
            "failed to strip object-store root from path {}: {err}",
            path.display()
        ))
    })?;

    let mut parts = Vec::new();
    for component in relative.components() {
        match component {
            Component::Normal(part) => parts.push(part.to_string_lossy().into_owned()),
            other => {
                return Err(ObjectStoreError::Transport(format!(
                    "unsupported relative path component {other:?} under local object store"
                )))
            }
        }
    }

    Ok(parts.join("/"))
}

fn prune_empty_parent_dirs(
    mut current: Option<&Path>,
    root: &Path,
) -> Result<(), ObjectStoreError> {
    while let Some(dir) = current {
        if dir == root {
            break;
        }

        match fs::remove_dir(dir) {
            Ok(()) => current = dir.parent(),
            Err(err) if err.kind() == std::io::ErrorKind::DirectoryNotEmpty => break,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => break,
            Err(err) => return Err(io_error(err)),
        }
    }

    Ok(())
}

#[allow(clippy::disallowed_methods)]
fn temp_path(path: &Path) -> PathBuf {
    // Local atomic writes need a unique sibling name; this timestamp is not durable state.
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("object");
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();

    path.with_file_name(format!(".{file_name}.tmp-{}-{stamp}", std::process::id()))
}

fn map_create_error(err: std::io::Error) -> ObjectStoreError {
    if err.kind() == std::io::ErrorKind::AlreadyExists {
        ObjectStoreError::PreconditionFailed
    } else {
        io_error(err)
    }
}

fn io_error(err: std::io::Error) -> ObjectStoreError {
    ObjectStoreError::Transport(err.to_string())
}

#[cfg(test)]
mod tests {
    use super::LocalFsStore;
    use super::{ObjectStore, PutMode};
    use crate::keys::{namespace_head, namespace_lease};
    use bytes::Bytes;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[tokio::test]
    async fn overwrite_refreshes_head_and_visible_bytes() {
        let temp_dir = TestDir::new("overwrite");
        let store = LocalFsStore::new(temp_dir.path()).expect("create local fs store");
        let key = namespace_head("ns-1");

        let first = store
            .put(
                &key,
                Bytes::from_static(br#"{"seq":1}"#),
                PutMode::Overwrite,
            )
            .await
            .expect("seed first object");
        let second = store
            .put(
                &key,
                Bytes::from_static(br#"{"seq":2}"#),
                PutMode::Overwrite,
            )
            .await
            .expect("overwrite object");

        assert_eq!(
            store.get(&key, None).await.expect("get object"),
            Some(Bytes::from_static(br#"{"seq":2}"#))
        );
        let head = store
            .head(&key)
            .await
            .expect("head object")
            .expect("head exists");
        assert_eq!(head.etag, second.etag);
        assert_eq!(head.size_bytes, second.size_bytes);
        assert_ne!(first, second);
    }

    #[tokio::test]
    async fn delete_is_idempotent_and_head_reflects_removal() {
        let temp_dir = TestDir::new("delete");
        let store = LocalFsStore::new(temp_dir.path()).expect("create local fs store");
        let key = namespace_lease("ns-1");

        store
            .put_if_absent(&key, Bytes::from_static(br#"{"holder":"writer-a"}"#))
            .await
            .expect("seed lease object");
        assert!(store
            .head(&key)
            .await
            .expect("head before delete")
            .is_some());
        store.delete(&key).await.expect("delete existing object");
        store.delete(&key).await.expect("delete missing object");
        assert_eq!(store.head(&key).await.expect("head after delete"), None);
    }

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        #[allow(clippy::disallowed_methods)]
        fn new(label: &str) -> Self {
            // Test-only unique paths are an entropy boundary, not protocol time.
            let stamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "loondb-local-fs-{label}-{}-{stamp}",
                std::process::id()
            ));
            fs::create_dir_all(&path).expect("create temp dir");
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}
