use crate::keyspace::validate_segments;
use crate::{ByteRange, ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, BoxStream, StreamExt};
use loon_api::sha256_digest;
use std::path::{Component, Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct LocalFsStore {
    root: PathBuf,
    write_lock: Mutex<()>,
}

impl LocalFsStore {
    pub fn new(root: impl Into<PathBuf>) -> Result<Self, ObjectStoreError> {
        let root = root.into();
        std::fs::create_dir_all(&root).map_err(io_error)?;
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

    async fn metadata_for_path(
        path: &Path,
        include_checksum: bool,
    ) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        let metadata = match fs::metadata(path).await {
            Ok(metadata) => metadata,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(io_error(err)),
        };
        let content_digest = sha256_digest(&fs::read(path).await.map_err(io_error)?);
        let checksum_sha256 = include_checksum.then_some(content_digest.clone());
        Self::metadata_from_fs_metadata(&metadata, &content_digest, checksum_sha256, path).map(Some)
    }

    fn metadata_from_fs_metadata(
        metadata: &std::fs::Metadata,
        content_digest: &str,
        checksum_sha256: Option<String>,
        path: &Path,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        if !metadata.is_file() {
            return Err(ObjectStoreError::Transport(format!(
                "object path is not a file: {}",
                path.display()
            )));
        }

        Ok(ObjectMetadata {
            etag: Some(format!("local-fs-v1:{content_digest}")),
            version: None,
            size_bytes: metadata.len(),
            checksum_sha256,
        })
    }

    async fn create_new_object(path: &Path, bytes: &[u8]) -> Result<(), ObjectStoreError> {
        ensure_parent_dir(path).await?;
        let mut file = match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .await
        {
            Ok(file) => file,
            Err(err) => return Err(map_create_error(err)),
        };

        let result: Result<(), ObjectStoreError> = async {
            file.write_all(bytes).await.map_err(io_error)?;
            file.sync_all().await.map_err(io_error)
        }
        .await;

        if result.is_err() {
            let _ = fs::remove_file(path).await;
        }

        result
    }

    async fn replace_object(path: &Path, bytes: &[u8]) -> Result<(), ObjectStoreError> {
        ensure_parent_dir(path).await?;
        let temp_path = temp_path(path);

        let result: Result<(), ObjectStoreError> = async {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temp_path)
                .await
                .map_err(io_error)?;
            file.write_all(bytes).await.map_err(io_error)?;
            file.sync_all().await.map_err(io_error)?;

            match fs::rename(&temp_path, path).await {
                Ok(()) => Ok(()),
                Err(rename_error)
                    if fs::try_exists(path).await.unwrap_or(false)
                        && matches!(
                            rename_error.kind(),
                            std::io::ErrorKind::AlreadyExists
                                | std::io::ErrorKind::PermissionDenied
                        ) =>
                {
                    fs::remove_file(path).await.map_err(io_error)?;
                    fs::rename(&temp_path, path).await.map_err(io_error)
                }
                Err(rename_error) => Err(io_error(rename_error)),
            }
        }
        .await;

        if result.is_err() {
            let _ = fs::remove_file(&temp_path).await;
        }

        result
    }
}

impl LocalFsStore {
    async fn head_object(
        &self,
        key: &str,
        include_checksum: bool,
    ) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        let path = self.resolve_key(key)?;
        Self::metadata_for_path(&path, include_checksum).await
    }

    async fn get_with_metadata_object(
        &self,
        key: &str,
    ) -> Result<Option<ObjectBody>, ObjectStoreError> {
        let path = self.resolve_key(key)?;
        let mut file = match File::open(&path).await {
            Ok(file) => file,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(io_error(err)),
        };
        let fs_metadata = file.metadata().await.map_err(io_error)?;

        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).await.map_err(io_error)?;

        let content_digest = sha256_digest(&bytes);
        let metadata = Self::metadata_from_fs_metadata(
            &fs_metadata,
            &content_digest,
            Some(content_digest.clone()),
            &path,
        )?;
        Ok(Some(ObjectBody { metadata, bytes }))
    }

    async fn get_object(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Vec<u8>>, ObjectStoreError> {
        let path = self.resolve_key(key)?;
        let mut file = match File::open(&path).await {
            Ok(file) => file,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(io_error(err)),
        };

        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).await.map_err(io_error)?;

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

    async fn put_object(
        &self,
        key: &str,
        bytes: &[u8],
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        let path = self.resolve_key(key)?;
        let _guard = self.write_lock.lock().await;

        match mode {
            PutMode::Overwrite => Self::replace_object(&path, bytes).await?,
            PutMode::CreateIfAbsent => {
                if fs::try_exists(&path).await.map_err(io_error)? {
                    return Err(ObjectStoreError::PreconditionFailed);
                }
                Self::create_new_object(&path, bytes).await?;
            }
            PutMode::CompareAndSwap { expected_etag } => {
                let current = Self::metadata_for_path(&path, false)
                    .await?
                    .ok_or(ObjectStoreError::PreconditionFailed)?;
                if current.etag.as_deref() != Some(expected_etag.as_str()) {
                    return Err(ObjectStoreError::PreconditionFailed);
                }
                Self::replace_object(&path, bytes).await?;
            }
        }

        Self::metadata_for_path(&path, true)
            .await?
            .ok_or_else(|| ObjectStoreError::Transport(format!("object disappeared: {key}")))
    }

    async fn delete_object(&self, key: &str) -> Result<(), ObjectStoreError> {
        let path = self.resolve_key(key)?;
        let _guard = self.write_lock.lock().await;

        match fs::remove_file(&path).await {
            Ok(()) => {
                prune_empty_parent_dirs(path.parent(), &self.root).await?;
                Ok(())
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(io_error(err)),
        }
    }
}

#[async_trait]
impl ObjectStore for LocalFsStore {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.head_object(key, false).await
    }

    async fn head_with_checksum(
        &self,
        key: &str,
    ) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.head_object(key, true).await
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        self.get_with_metadata_object(key).await
    }

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        self.get_object(key, range)
            .await
            .map(|maybe| maybe.map(Bytes::from))
    }

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        self.put_object(key, &bytes, mode).await
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.delete_object(key).await
    }

    fn list_prefix_stream(
        &self,
        prefix: &str,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        let root = self.root.clone();
        let prefix = prefix.to_owned();
        Box::pin(
            stream::once(async move { list_prefix_for_root(root, prefix).await }).flat_map(
                |result| match result {
                    Ok(keys) => stream::iter(keys.into_iter().map(Ok)).boxed(),
                    Err(err) => stream::once(async { Err(err) }).boxed(),
                },
            ),
        )
    }
}

async fn list_prefix_for_root(
    root: PathBuf,
    prefix: String,
) -> Result<Vec<String>, ObjectStoreError> {
    validate_segments(&prefix, true)?;

    if !fs::try_exists(&root).await.map_err(io_error)? {
        return Ok(Vec::new());
    }

    let mut keys = collect_keys(root).await?;
    keys.retain(|key| key.starts_with(&prefix));
    keys.sort();
    Ok(keys)
}

async fn ensure_parent_dir(path: &Path) -> Result<(), ObjectStoreError> {
    match path.parent() {
        Some(parent) => fs::create_dir_all(parent).await.map_err(io_error),
        None => Err(ObjectStoreError::InvalidKey(path.display().to_string())),
    }
}

async fn collect_keys(root: PathBuf) -> Result<Vec<String>, ObjectStoreError> {
    let mut keys = Vec::new();
    let mut dirs = vec![root.clone()];

    while let Some(current) = dirs.pop() {
        let mut entries = Vec::new();
        let mut reader = fs::read_dir(&current).await.map_err(io_error)?;
        while let Some(entry) = reader.next_entry().await.map_err(io_error)? {
            entries.push(entry.path());
        }
        entries.sort();

        for path in entries.into_iter().rev() {
            let metadata = fs::metadata(&path).await.map_err(io_error)?;
            if metadata.is_dir() {
                dirs.push(path);
                continue;
            }

            if metadata.is_file() {
                keys.push(relative_key(&root, &path)?);
            }
        }
    }

    keys.sort();
    Ok(keys)
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

async fn prune_empty_parent_dirs(
    mut current: Option<&Path>,
    root: &Path,
) -> Result<(), ObjectStoreError> {
    while let Some(dir) = current {
        if dir == root {
            break;
        }

        match fs::remove_dir(dir).await {
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
