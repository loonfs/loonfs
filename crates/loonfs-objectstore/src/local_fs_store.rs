//! [`LocalFsStore`]: the local-filesystem [`ObjectStore`] provider.
//!
//! This is the dev and test provider. On Unix-family platforms, sibling
//! staging files and atomic rename-replace give it the same replacement
//! visibility contract as the cloud providers. Construction fails on other
//! platforms rather than exposing a weaker contract. Its performance shapes
//! are deliberately relaxed (content-hash etags, whole-file reads,
//! whole-tree listings) and are not optimization targets.

use crate::keyspace::{
    normalize_key_prefix, scope_list_prefix, scope_object_key, unscope_listed_key,
    validate_segments,
};
use crate::object_store::Result;
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

/// Implements the object-store contract on a Unix-family local directory.
///
/// Replacements are atomic: a concurrent reader observes either the complete
/// prior object or the complete replacement, never a missing or partial
/// object.
#[derive(Debug)]
pub struct LocalFsStore {
    root: PathBuf,
    /// Logical prefix every key is confined beneath, or `None` for the root.
    key_prefix: Option<String>,
    write_lock: Mutex<()>,
}

impl LocalFsStore {
    /// Opens a local store, creating its root directory when necessary.
    ///
    /// Construction fails outside Unix-family platforms, where the provider
    /// does not claim atomic rename-replace support, or when the root cannot
    /// be created.
    pub fn new(root: impl Into<PathBuf>) -> Result<Self> {
        Self::with_key_prefix(root, None)
    }

    /// Opens a local store whose keys are confined beneath `key_prefix`,
    /// matching how the provider adapters scope theirs.
    pub fn with_key_prefix(root: impl Into<PathBuf>, key_prefix: Option<&str>) -> Result<Self> {
        require_atomic_rename_replace()?;
        let root = root.into();
        std::fs::create_dir_all(&root).map_err(|err| {
            ObjectStoreError::Configuration(format!(
                "failed to create store root `{}`: {err}",
                root.display()
            ))
        })?;
        Ok(Self {
            root,
            key_prefix: normalize_key_prefix(key_prefix)?,
            write_lock: Mutex::new(()),
        })
    }

    /// Extends the in-process write mutex across processes with an advisory
    /// file lock on the store root. Check-then-act writes (compare-and-swap,
    /// delete-then-prune) are only safe while both are held.
    async fn acquire_cross_process_write_lock(&self, key: &str) -> Result<std::fs::File> {
        let lock_path = self.root.join(STORE_LOCK_FILE_NAME);
        let lock_key = key.to_owned();
        tokio::task::spawn_blocking(move || {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&lock_path)
                .map_err(|err| io_error(&lock_key, err))?;
            fs4::fs_std::FileExt::lock_exclusive(&file).map_err(|err| io_error(&lock_key, err))?;
            Ok(file)
        })
        .await
        .map_err(|err| ObjectStoreError::transport(key, format!("store lock task failed: {err}")))?
    }

    /// Returns the filesystem directory beneath which validated object keys are resolved.
    pub fn root(&self) -> &Path {
        &self.root
    }

    fn resolve_key(&self, key: &str) -> Result<PathBuf> {
        let segments = validate_segments(key, false)?;
        // Scratch-shaped names are reserved by this store: they are hidden
        // from listings, so accepting them as keys would create objects a
        // listing can never report.
        if segments.iter().any(|segment| is_scratch_name(segment)) {
            return Err(ObjectStoreError::InvalidKey {
                object_key: key.to_owned(),
                message: "key uses a segment name reserved for store scratch files".to_owned(),
            });
        }
        let mut path = self.root.clone();
        for segment in segments {
            path.push(segment);
        }
        Ok(path)
    }

    async fn metadata_for_path(key: &str, path: &Path) -> Result<Option<ObjectMetadata>> {
        let metadata = match fs::metadata(path).await {
            Ok(metadata) => metadata,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(io_error(key, err)),
        };
        let content_digest =
            sha256_digest(&fs::read(path).await.map_err(|err| io_error(key, err))?);
        Self::metadata_from_fs_metadata(key, &metadata, &content_digest, path).map(Some)
    }

    fn metadata_from_fs_metadata(
        key: &str,
        metadata: &std::fs::Metadata,
        content_digest: &str,
        path: &Path,
    ) -> Result<ObjectMetadata> {
        if !metadata.is_file() {
            return Err(ObjectStoreError::transport(
                key,
                format!("object path is not a file: {}", path.display()),
            ));
        }

        let last_modified_ms = metadata
            .modified()
            .ok()
            .and_then(|modified| modified.duration_since(std::time::UNIX_EPOCH).ok())
            .and_then(|elapsed| u64::try_from(elapsed.as_millis()).ok());
        Ok(ObjectMetadata {
            etag: Some(format!("local-fs-v1:{content_digest}")),
            version: None,
            size_bytes: metadata.len(),
            last_modified_ms,
        })
    }

    async fn create_new_object(key: &str, root: &Path, path: &Path, bytes: &[u8]) -> Result<()> {
        // Stage to a temp file and link into place, like `replace_object`:
        // the bytes become visible at the final key atomically, so a reader
        // never observes a partial object and a crash never leaves a torn
        // file wedging the key. `hard_link` fails if the key exists, which
        // is exactly the create-if-absent precondition.
        let created_dirs = ensure_parent_dir(key, path).await?;
        let temp_path = temp_path(path);
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp_path)
            .await
            .map_err(|err| io_error(key, err))?;

        let staged: Result<()> = async {
            file.write_all(bytes)
                .await
                .map_err(|err| io_error(key, err))?;
            file.sync_all().await.map_err(|err| io_error(key, err))
        }
        .await;
        if staged.is_err() {
            let _ = fs::remove_file(&temp_path).await;
            return staged;
        }

        let linked = fs::hard_link(&temp_path, path)
            .await
            .map_err(|err| map_create_error(key, err));
        let _ = fs::remove_file(&temp_path).await;
        linked?;

        if created_dirs {
            sync_dir_chain(key, path, root).await?;
        }
        sync_parent_dir(key, path).await
    }

    async fn replace_object(key: &str, root: &Path, path: &Path, bytes: &[u8]) -> Result<()> {
        let created_dirs = ensure_parent_dir(key, path).await?;
        let temp_path = temp_path(path);

        let result: Result<()> = async {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temp_path)
                .await
                .map_err(|err| io_error(key, err))?;
            file.write_all(bytes)
                .await
                .map_err(|err| io_error(key, err))?;
            file.sync_all().await.map_err(|err| io_error(key, err))?;

            fs::rename(&temp_path, path)
                .await
                .map_err(|err| io_error(key, err))
        }
        .await;

        if result.is_err() {
            let _ = fs::remove_file(&temp_path).await;
            return result;
        }

        if created_dirs {
            sync_dir_chain(key, path, root).await?;
        }
        sync_parent_dir(key, path).await
    }
}

impl LocalFsStore {
    async fn head_object(&self, key: &str) -> Result<Option<ObjectMetadata>> {
        let path = self.resolve_key(key)?;
        Self::metadata_for_path(key, &path).await
    }

    async fn get_with_metadata_object(&self, key: &str) -> Result<Option<ObjectBody>> {
        let path = self.resolve_key(key)?;
        let mut file = match File::open(&path).await {
            Ok(file) => file,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(io_error(key, err)),
        };
        let fs_metadata = file.metadata().await.map_err(|err| io_error(key, err))?;

        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)
            .await
            .map_err(|err| io_error(key, err))?;

        let content_digest = sha256_digest(&bytes);
        let metadata = Self::metadata_from_fs_metadata(key, &fs_metadata, &content_digest, &path)?;
        Ok(Some(ObjectBody { metadata, bytes }))
    }

    async fn get_object(&self, key: &str, range: Option<ByteRange>) -> Result<Option<Vec<u8>>> {
        let path = self.resolve_key(key)?;
        let mut file = match File::open(&path).await {
            Ok(file) => file,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(io_error(key, err)),
        };

        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)
            .await
            .map_err(|err| io_error(key, err))?;

        match range {
            None => Ok(Some(bytes)),
            Some(range) => {
                let invalid_range = || ObjectStoreError::InvalidRange {
                    object_key: key.to_owned(),
                };
                let start = usize::try_from(range.start_inclusive).map_err(|_| invalid_range())?;
                let mut end = usize::try_from(range.end_exclusive).map_err(|_| invalid_range())?;

                if end < start || start > bytes.len() {
                    return Err(invalid_range());
                }

                end = end.min(bytes.len());
                Ok(Some(bytes[start..end].to_vec()))
            }
        }
    }

    async fn put_object(&self, key: &str, bytes: &[u8], mode: PutMode) -> Result<ObjectMetadata> {
        let path = self.resolve_key(key)?;
        let _guard = self.write_lock.lock().await;
        let _cross_process_guard = self.acquire_cross_process_write_lock(key).await?;
        let precondition_failed = || ObjectStoreError::PreconditionFailed {
            object_key: key.to_owned(),
        };

        match mode {
            PutMode::Overwrite => Self::replace_object(key, &self.root, &path, bytes).await?,
            PutMode::CreateIfAbsent => {
                if fs::try_exists(&path)
                    .await
                    .map_err(|err| io_error(key, err))?
                {
                    return Err(precondition_failed());
                }
                Self::create_new_object(key, &self.root, &path, bytes).await?;
            }
            PutMode::CompareAndSwap { expected_etag } => {
                let current = Self::metadata_for_path(key, &path)
                    .await?
                    .ok_or_else(precondition_failed)?;
                if current.etag.as_deref() != Some(expected_etag.as_str()) {
                    return Err(precondition_failed());
                }
                Self::replace_object(key, &self.root, &path, bytes).await?;
            }
        }

        Self::metadata_for_path(key, &path)
            .await?
            .ok_or_else(|| ObjectStoreError::transport(key, "object disappeared after write"))
    }

    async fn delete_object(&self, key: &str) -> Result<()> {
        let path = self.resolve_key(key)?;
        let _guard = self.write_lock.lock().await;
        let _cross_process_guard = self.acquire_cross_process_write_lock(key).await?;

        match fs::remove_file(&path).await {
            Ok(()) => {
                sync_parent_dir(key, &path).await?;
                prune_empty_parent_dirs(key, path.parent(), &self.root).await?;
                Ok(())
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(io_error(key, err)),
        }
    }
}

impl LocalFsStore {
    /// Confines one caller key beneath the configured prefix.
    fn scoped(&self, key: &str) -> Result<String> {
        scope_object_key(self.key_prefix.as_deref(), key)
    }
}

#[async_trait]
impl ObjectStore for LocalFsStore {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>> {
        self.head_object(&self.scoped(key)?).await
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>> {
        self.get_with_metadata_object(&self.scoped(key)?).await
    }

    async fn get(&self, key: &str, range: Option<ByteRange>) -> Result<Option<Bytes>> {
        self.get_object(&self.scoped(key)?, range)
            .await
            .map(|maybe| maybe.map(Bytes::from))
    }

    async fn put(&self, key: &str, bytes: Bytes, mode: PutMode) -> Result<ObjectMetadata> {
        self.put_object(&self.scoped(key)?, &bytes, mode).await
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.delete_object(&self.scoped(key)?).await
    }

    fn list_prefix_stream(&self, prefix: &str) -> BoxStream<'static, Result<String>> {
        let scoped = match scope_list_prefix(self.key_prefix.as_deref(), prefix) {
            Ok(scoped) => scoped,
            Err(err) => return stream::once(async { Err(err) }).boxed(),
        };
        let root = self.root.clone();
        let key_prefix = self.key_prefix.clone();
        Box::pin(
            stream::once(async move { list_prefix_for_root(root, scoped).await })
                .flat_map(|result| match result {
                    Ok(keys) => stream::iter(keys.into_iter().map(Ok)).boxed(),
                    Err(err) => stream::once(async { Err(err) }).boxed(),
                })
                .filter_map(move |result| {
                    let key_prefix = key_prefix.clone();
                    async move {
                        match result {
                            Ok(key) => match key_prefix.as_deref() {
                                Some(prefix) => unscope_listed_key(Some(prefix), &key).map(Ok),
                                None => Some(Ok(key)),
                            },
                            Err(err) => Some(Err(err)),
                        }
                    }
                }),
        )
    }
}

#[cfg(unix)]
fn require_atomic_rename_replace() -> Result<()> {
    Ok(())
}

#[cfg(not(unix))]
fn require_atomic_rename_replace() -> Result<()> {
    Err(ObjectStoreError::Configuration(
        "local filesystem provider requires atomic rename-replace and is supported only on \
         Unix-family platforms"
            .to_owned(),
    ))
}

async fn list_prefix_for_root(root: PathBuf, prefix: String) -> Result<Vec<String>> {
    validate_segments(&prefix, true)?;

    if !fs::try_exists(&root)
        .await
        .map_err(|err| io_error(&prefix, err))?
    {
        return Ok(Vec::new());
    }

    let mut keys = collect_keys(&prefix, root).await?;
    keys.retain(|key| key.starts_with(&prefix));
    keys.sort();
    Ok(keys)
}

/// Creates the parent directory chain. Returns whether anything was created,
/// so callers know the new directory entries also need to be made durable.
async fn ensure_parent_dir(key: &str, path: &Path) -> Result<bool> {
    match path.parent() {
        Some(parent) => {
            if fs::try_exists(parent)
                .await
                .map_err(|err| io_error(key, err))?
            {
                return Ok(false);
            }
            fs::create_dir_all(parent)
                .await
                .map_err(|err| io_error(key, err))?;
            Ok(true)
        }
        None => Err(ObjectStoreError::InvalidKey {
            object_key: key.to_owned(),
            message: format!("object path `{}` has no parent directory", path.display()),
        }),
    }
}

/// Fsyncs the directory holding `path`, making a rename, create, or unlink
/// of that entry durable. Without this, the file data can survive a crash
/// while the directory entry pointing at it does not.
async fn sync_parent_dir(key: &str, path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        if let Some(parent) = path.parent() {
            let dir = File::open(parent).await.map_err(|err| io_error(key, err))?;
            dir.sync_all().await.map_err(|err| io_error(key, err))?;
        }
    }
    #[cfg(not(unix))]
    {
        let _ = (key, path);
    }
    Ok(())
}

/// Fsyncs every directory from `path`'s parent up to and including the
/// store root, so a freshly created directory chain survives a crash. The
/// root itself pre-exists, so the chain never needs to go past it.
async fn sync_dir_chain(key: &str, path: &Path, root: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        let mut current = path.parent();
        while let Some(dir) = current {
            let handle = File::open(dir).await.map_err(|err| io_error(key, err))?;
            handle.sync_all().await.map_err(|err| io_error(key, err))?;
            if dir == root {
                break;
            }
            current = dir.parent();
        }
    }
    #[cfg(not(unix))]
    {
        let _ = (key, path, root);
    }
    Ok(())
}

async fn collect_keys(prefix: &str, root: PathBuf) -> Result<Vec<String>> {
    let mut keys = Vec::new();
    let mut dirs = vec![root.clone()];

    while let Some(current) = dirs.pop() {
        let mut entries = Vec::new();
        let mut reader = fs::read_dir(&current)
            .await
            .map_err(|err| io_error(prefix, err))?;
        while let Some(entry) = reader
            .next_entry()
            .await
            .map_err(|err| io_error(prefix, err))?
        {
            entries.push(entry.path());
        }
        entries.sort();

        for path in entries.into_iter().rev() {
            let metadata = fs::metadata(&path)
                .await
                .map_err(|err| io_error(prefix, err))?;
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
                keys.push(relative_key(prefix, &root, &path)?);
            }
        }
    }

    keys.sort();
    Ok(keys)
}

fn relative_key(prefix: &str, root: &Path, path: &Path) -> Result<String> {
    let relative = path.strip_prefix(root).map_err(|err| {
        ObjectStoreError::transport(
            prefix,
            format!(
                "failed to strip object-store root from path {}: {err}",
                path.display()
            ),
        )
    })?;

    let mut parts = Vec::new();
    for component in relative.components() {
        match component {
            Component::Normal(part) => parts.push(part.to_string_lossy().into_owned()),
            other => {
                return Err(ObjectStoreError::transport(
                    prefix,
                    format!(
                        "unsupported relative path component {other:?} under local object store"
                    ),
                ))
            }
        }
    }

    Ok(parts.join("/"))
}

async fn prune_empty_parent_dirs(key: &str, mut current: Option<&Path>, root: &Path) -> Result<()> {
    while let Some(dir) = current {
        if dir == root {
            break;
        }

        match fs::remove_dir(dir).await {
            Ok(()) => current = dir.parent(),
            Err(err) if err.kind() == std::io::ErrorKind::DirectoryNotEmpty => break,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => break,
            Err(err) => return Err(io_error(key, err)),
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

fn map_create_error(key: &str, err: std::io::Error) -> ObjectStoreError {
    if err.kind() == std::io::ErrorKind::AlreadyExists {
        ObjectStoreError::PreconditionFailed {
            object_key: key.to_owned(),
        }
    } else {
        io_error(key, err)
    }
}

fn io_error(key: &str, err: std::io::Error) -> ObjectStoreError {
    if err.kind() == std::io::ErrorKind::PermissionDenied {
        return ObjectStoreError::PermissionDenied {
            object_key: key.to_owned(),
            message: err.to_string(),
        };
    }
    ObjectStoreError::transport(key, err.to_string())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::panic)]
    // Tests panic in unexpected match arms for precise diagnostics.

    use super::LocalFsStore;
    use super::{ObjectStore, ObjectStoreError, PutMode};
    use crate::keys::{upload_session, wal_head};
    use bytes::Bytes;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::sync::Barrier;

    #[test]
    fn construction_requires_atomic_rename_replace_support() {
        let temp_dir = TestDir::new("construction-gate");
        let result = LocalFsStore::new(temp_dir.path());

        #[cfg(unix)]
        assert!(result.is_ok());

        #[cfg(not(unix))]
        assert!(matches!(
            result,
            Err(ObjectStoreError::Configuration(message))
                if message.contains("requires atomic rename-replace")
                    && message.contains("Unix-family")
        ));
    }

    #[tokio::test]
    async fn overwrite_refreshes_head_and_visible_bytes() {
        let temp_dir = TestDir::new("overwrite");
        let store = LocalFsStore::new(temp_dir.path()).expect("create local fs store");
        let key = wal_head("ns-1");

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

    /// Readers race each overwrite rename to exercise replacement visibility across runtime workers.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_readers_observe_complete_replacement_generations() {
        const PAYLOAD_BYTES: usize = 16 * 1024;
        const READER_COUNT: usize = 4;
        const REPLACEMENT_COUNT: u8 = 32;

        let temp_dir = TestDir::new("atomic-replacement");
        let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("create local fs store"));
        let key = wal_head("ns-atomic-replacement");
        store
            .put(
                &key,
                Bytes::from(vec![0; PAYLOAD_BYTES]),
                PutMode::Overwrite,
            )
            .await
            .expect("seed replacement object");

        let round_barrier = Arc::new(Barrier::new(READER_COUNT + 1));
        let mut readers = Vec::new();
        for _ in 0..READER_COUNT {
            let reader = Arc::clone(&store);
            let reader_key = key.clone();
            let reader_barrier = Arc::clone(&round_barrier);
            readers.push(tokio::spawn(async move {
                for _ in 0..REPLACEMENT_COUNT {
                    reader_barrier.wait().await;
                    let bytes = reader
                        .get(&reader_key, None)
                        .await
                        .expect("read during replacement")
                        .expect("replacement key remains present");
                    assert_eq!(bytes.len(), PAYLOAD_BYTES);
                    let generation = bytes[0];
                    assert!(generation <= REPLACEMENT_COUNT);
                    assert!(bytes.iter().all(|byte| *byte == generation));
                    reader_barrier.wait().await;
                }
            }));
        }

        for generation in 1..=REPLACEMENT_COUNT {
            round_barrier.wait().await;
            store
                .put(
                    &key,
                    Bytes::from(vec![generation; PAYLOAD_BYTES]),
                    PutMode::Overwrite,
                )
                .await
                .expect("replace object");
            round_barrier.wait().await;
        }

        for reader in readers {
            reader.await.expect("reader task");
        }
    }

    #[tokio::test]
    async fn delete_is_idempotent_and_head_reflects_removal() {
        let temp_dir = TestDir::new("delete");
        let store = LocalFsStore::new(temp_dir.path()).expect("create local fs store");
        let key = upload_session("ns-1", "upl_00000000000000000000000000000001");

        store
            .put_if_absent(&key, Bytes::from_static(br#"{"created":true}"#))
            .await
            .expect("seed upload object");
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
        let key = wal_head("ns-cas");
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
                            Err(ObjectStoreError::PreconditionFailed { .. }) => continue,
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
        let key = wal_head("ns-scratch");
        store
            .put(&key, Bytes::from_static(b"{}"), PutMode::Overwrite)
            .await
            .expect("put object");

        // The write above created the store lock; fake an in-flight temp
        // write next to the real object as well.
        assert!(temp_dir.path().join(super::STORE_LOCK_FILE_NAME).exists());
        let wal_dir = temp_dir.path().join("namespaces/ns-scratch/wal");
        fs::write(wal_dir.join(".head.json.tmp-123-456"), b"partial")
            .expect("write fake temp file");

        let keys = store.list_prefix("").await.expect("list all");
        assert_eq!(keys, vec![key]);

        let reserved = store.get(super::STORE_LOCK_FILE_NAME, None).await;
        assert!(matches!(
            reserved,
            Err(ObjectStoreError::InvalidKey { object_key, .. })
                if object_key == super::STORE_LOCK_FILE_NAME
        ));
        let temp_shaped = store
            .put(
                "namespaces/ns-scratch/control/.head.json.tmp-9-9",
                Bytes::from_static(b"x"),
                PutMode::Overwrite,
            )
            .await;
        assert!(matches!(
            temp_shaped,
            Err(ObjectStoreError::InvalidKey { .. })
        ));
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
