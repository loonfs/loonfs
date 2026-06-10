use crate::keyspace::validate_segments;
use crate::{ByteRange, ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, BoxStream, StreamExt};
use loonfs_api::sha256_digest;
use std::path::{Component, Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;

/// Lock file serializing mutations across processes sharing one store root.
/// Held only for the duration of each write; the OS releases it if the
/// holding process dies. Never listed as an object (see `is_scratch_name`).
const STORE_LOCK_FILE_NAME: &str = ".loonfs-store.lock";

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

    /// Extends the in-process write mutex across processes with an advisory
    /// file lock on the store root. Check-then-act writes (compare-and-swap,
    /// delete-then-prune) are only safe while both are held.
    async fn acquire_cross_process_write_lock(&self) -> Result<std::fs::File, ObjectStoreError> {
        let lock_path = self.root.join(STORE_LOCK_FILE_NAME);
        tokio::task::spawn_blocking(move || {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&lock_path)
                .map_err(io_error)?;
            fs4::fs_std::FileExt::lock_exclusive(&file).map_err(io_error)?;
            Ok(file)
        })
        .await
        .map_err(|err| ObjectStoreError::Transport(format!("store lock task failed: {err}")))?
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    fn resolve_key(&self, key: &str) -> Result<PathBuf, ObjectStoreError> {
        let segments = validate_segments(key, false)?;
        // Scratch-shaped names are reserved by this store: they are hidden
        // from listings, so accepting them as keys would create objects a
        // listing can never report.
        if segments.iter().any(|segment| is_scratch_name(segment)) {
            return Err(ObjectStoreError::InvalidKey(key.to_owned()));
        }
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

    async fn create_new_object(
        root: &Path,
        path: &Path,
        bytes: &[u8],
    ) -> Result<(), ObjectStoreError> {
        let created_dirs = ensure_parent_dir(path).await?;
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
            return result;
        }

        if created_dirs {
            sync_dir_chain(path, root).await?;
        }
        sync_parent_dir(path).await
    }

    async fn replace_object(
        root: &Path,
        path: &Path,
        bytes: &[u8],
    ) -> Result<(), ObjectStoreError> {
        let created_dirs = ensure_parent_dir(path).await?;
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
            return result;
        }

        if created_dirs {
            sync_dir_chain(path, root).await?;
        }
        sync_parent_dir(path).await
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
        let _cross_process_guard = self.acquire_cross_process_write_lock().await?;

        match mode {
            PutMode::Overwrite => Self::replace_object(&self.root, &path, bytes).await?,
            PutMode::CreateIfAbsent => {
                if fs::try_exists(&path).await.map_err(io_error)? {
                    return Err(ObjectStoreError::PreconditionFailed);
                }
                Self::create_new_object(&self.root, &path, bytes).await?;
            }
            PutMode::CompareAndSwap { expected_etag } => {
                let current = Self::metadata_for_path(&path, false)
                    .await?
                    .ok_or(ObjectStoreError::PreconditionFailed)?;
                if current.etag.as_deref() != Some(expected_etag.as_str()) {
                    return Err(ObjectStoreError::PreconditionFailed);
                }
                Self::replace_object(&self.root, &path, bytes).await?;
            }
        }

        Self::metadata_for_path(&path, true)
            .await?
            .ok_or_else(|| ObjectStoreError::Transport(format!("object disappeared: {key}")))
    }

    async fn delete_object(&self, key: &str) -> Result<(), ObjectStoreError> {
        let path = self.resolve_key(key)?;
        let _guard = self.write_lock.lock().await;
        let _cross_process_guard = self.acquire_cross_process_write_lock().await?;

        match fs::remove_file(&path).await {
            Ok(()) => {
                sync_parent_dir(&path).await?;
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

/// Creates the parent directory chain. Returns whether anything was created,
/// so callers know the new directory entries also need to be made durable.
async fn ensure_parent_dir(path: &Path) -> Result<bool, ObjectStoreError> {
    match path.parent() {
        Some(parent) => {
            if fs::try_exists(parent).await.map_err(io_error)? {
                return Ok(false);
            }
            fs::create_dir_all(parent).await.map_err(io_error)?;
            Ok(true)
        }
        None => Err(ObjectStoreError::InvalidKey(path.display().to_string())),
    }
}

/// Fsyncs the directory holding `path`, making a rename, create, or unlink
/// of that entry durable. Without this, the file data can survive a crash
/// while the directory entry pointing at it does not.
async fn sync_parent_dir(path: &Path) -> Result<(), ObjectStoreError> {
    #[cfg(unix)]
    {
        if let Some(parent) = path.parent() {
            let dir = File::open(parent).await.map_err(io_error)?;
            dir.sync_all().await.map_err(io_error)?;
        }
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}

/// Fsyncs every directory from `path`'s parent up to and including the
/// store root, so a freshly created directory chain survives a crash. The
/// root itself pre-exists, so the chain never needs to go past it.
async fn sync_dir_chain(path: &Path, root: &Path) -> Result<(), ObjectStoreError> {
    #[cfg(unix)]
    {
        let mut current = path.parent();
        while let Some(dir) = current {
            let handle = File::open(dir).await.map_err(io_error)?;
            handle.sync_all().await.map_err(io_error)?;
            if dir == root {
                break;
            }
            current = dir.parent();
        }
    }
    #[cfg(not(unix))]
    {
        let _ = (path, root);
    }
    Ok(())
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
                if path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(is_scratch_name)
                {
                    continue;
                }
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

/// Private scratch names (the store lock and in-flight temp writes) are
/// never objects: listings hide them and `resolve_key` rejects them, so the
/// hidden set and the unaddressable set are identical. The match is
/// deliberately narrow — key segments are otherwise allowed to start with a
/// dot.
fn is_scratch_name(name: &str) -> bool {
    name == STORE_LOCK_FILE_NAME || (name.starts_with('.') && name.contains(".tmp-"))
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
    #![allow(clippy::panic)]
    // Tests panic in unexpected match arms for precise diagnostics.

    use super::LocalFsStore;
    use super::{ObjectStore, ObjectStoreError, PutMode};
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn compare_and_swap_is_safe_across_store_instances() {
        let temp_dir = TestDir::new("cross-instance-cas");
        let key = namespace_head("ns-cas");
        let seed = LocalFsStore::new(temp_dir.path()).expect("create local fs store");
        seed.put(&key, Bytes::from_static(b"0"), PutMode::CreateIfAbsent)
            .await
            .expect("seed counter");

        let mut writers = Vec::new();
        for _ in 0..2 {
            let root = temp_dir.path().to_path_buf();
            let key = key.clone();
            writers.push(tokio::spawn(async move {
                // A separate instance models a separate process: it shares
                // no in-process mutex with its sibling, only the file lock.
                let store = LocalFsStore::new(&root).expect("create local fs store");
                for _ in 0..20 {
                    loop {
                        let body = store
                            .get_with_metadata(&key)
                            .await
                            .expect("read counter")
                            .expect("counter exists");
                        let value: u64 = std::str::from_utf8(&body.bytes)
                            .expect("utf8 counter")
                            .parse()
                            .expect("numeric counter");
                        let expected_etag = body.metadata.etag.expect("counter etag");
                        let put = store
                            .put(
                                &key,
                                Bytes::from((value + 1).to_string()),
                                PutMode::CompareAndSwap { expected_etag },
                            )
                            .await;
                        match put {
                            Ok(_) => break,
                            Err(ObjectStoreError::PreconditionFailed) => continue,
                            Err(err) => panic!("unexpected CAS error: {err}"),
                        }
                    }
                }
            }));
        }
        for writer in writers {
            writer.await.expect("writer task");
        }

        let bytes = seed
            .get(&key, None)
            .await
            .expect("read final counter")
            .expect("counter exists");
        assert_eq!(std::str::from_utf8(&bytes).expect("utf8"), "40");
    }

    #[tokio::test]
    async fn listings_hide_scratch_files_and_reject_scratch_keys() {
        let temp_dir = TestDir::new("scratch");
        let store = LocalFsStore::new(temp_dir.path()).expect("create local fs store");
        let key = namespace_head("ns-scratch");
        store
            .put(&key, Bytes::from_static(b"{}"), PutMode::Overwrite)
            .await
            .expect("put object");

        // The write above created the store lock; fake an in-flight temp
        // write next to the real object as well.
        assert!(temp_dir.path().join(super::STORE_LOCK_FILE_NAME).exists());
        let control_dir = temp_dir.path().join("namespaces/ns-scratch/control");
        fs::write(control_dir.join(".head.json.tmp-123-456"), b"partial")
            .expect("write fake temp file");

        let keys = store.list_prefix("").await.expect("list all");
        assert_eq!(keys, vec![key]);

        let reserved = store.get(super::STORE_LOCK_FILE_NAME, None).await;
        assert!(matches!(reserved, Err(ObjectStoreError::InvalidKey(_))));
        let temp_shaped = store
            .put(
                "namespaces/ns-scratch/control/.head.json.tmp-9-9",
                Bytes::from_static(b"x"),
                PutMode::Overwrite,
            )
            .await;
        assert!(matches!(temp_shaped, Err(ObjectStoreError::InvalidKey(_))));
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
                "loonfs-local-fs-{label}-{}-{stamp}",
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
