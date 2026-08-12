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
use crate::{
    ByteRange, ByteStream, ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode,
    StoredObjectChecksum,
};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, BoxStream, StreamExt};
use loonfs_api::{sha256_digest, StorageChecksum};
use std::io::SeekFrom;
use std::path::{Component, Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
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
        let Some(content_bytes) = Self::read_for_digest(key, path).await? else {
            return Ok(None);
        };
        let content_digest = sha256_digest(&content_bytes);
        Self::metadata_from_fs_metadata(key, &metadata, &content_digest, path).map(Some)
    }

    /// Reads the object's bytes for the digest, answering `None` when the
    /// object is gone. The stat above and this read are separate syscalls,
    /// so a concurrent delete can land between them; a head that races a
    /// delete reports "gone" — the answer a provider's head gives — never
    /// a transport error.
    async fn read_for_digest(key: &str, path: &Path) -> Result<Option<Vec<u8>>> {
        match fs::read(path).await {
            Ok(bytes) => Ok(Some(bytes)),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(io_error(key, err)),
        }
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

    /// Writes a streamed payload to a sibling staging file, then links or
    /// renames it into place exactly as the buffered writes do.
    ///
    /// Nothing but the current chunk is ever held: the staging file is the
    /// buffer. The payload is fully written and fsynced before the mode's
    /// precondition is evaluated, which is the contract a caller folding a
    /// digest over the same stream depends on.
    async fn put_streamed_object(
        &self,
        key: &str,
        mut body: ByteStream,
        mode: PutMode,
    ) -> Result<u64> {
        let path = self.resolve_key(key)?;
        let created_dirs = ensure_parent_dir(key, &path).await?;
        let temp_path = temp_path(&path);
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp_path)
            .await
            .map_err(|err| io_error(key, err))?;

        let staged: Result<u64> = async {
            let mut size_bytes = 0u64;
            while let Some(chunk) = body.next().await {
                let chunk = chunk?;
                file.write_all(&chunk)
                    .await
                    .map_err(|err| io_error(key, err))?;
                size_bytes += chunk.len() as u64;
            }
            file.sync_all().await.map_err(|err| io_error(key, err))?;
            Ok(size_bytes)
        }
        .await;
        let size_bytes = match staged {
            Ok(size_bytes) => size_bytes,
            Err(err) => {
                let _ = fs::remove_file(&temp_path).await;
                return Err(err);
            }
        };

        let published = self
            .publish_staged_object(key, &path, &temp_path, mode)
            .await;
        let _ = fs::remove_file(&temp_path).await;
        published?;

        if created_dirs {
            sync_dir_chain(key, &path, &self.root).await?;
        }
        sync_parent_dir(key, &path).await?;
        Ok(size_bytes)
    }

    /// Moves a fully written staging file to its key under `mode`'s
    /// precondition, holding both write locks while it checks and acts.
    async fn publish_staged_object(
        &self,
        key: &str,
        path: &Path,
        temp_path: &Path,
        mode: PutMode,
    ) -> Result<()> {
        let _guard = self.write_lock.lock().await;
        let _cross_process_guard = self.acquire_cross_process_write_lock(key).await?;
        let precondition_failed = || ObjectStoreError::PreconditionFailed {
            object_key: key.to_owned(),
        };
        match mode {
            PutMode::Overwrite => fs::rename(temp_path, path)
                .await
                .map_err(|err| io_error(key, err)),
            // `hard_link` fails if the key exists, which is exactly the
            // create-if-absent precondition.
            PutMode::CreateIfAbsent => fs::hard_link(temp_path, path)
                .await
                .map_err(|err| map_create_error(key, err)),
            PutMode::CompareAndSwap { expected_etag } => {
                let current = Self::metadata_for_path(key, path)
                    .await?
                    .ok_or_else(precondition_failed)?;
                if current.etag.as_deref() != Some(expected_etag.as_str()) {
                    return Err(precondition_failed());
                }
                fs::rename(temp_path, path)
                    .await
                    .map_err(|err| io_error(key, err))
            }
        }
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

    /// Reads the whole object, or exactly one range of it.
    ///
    /// A ranged read seeks to its start and reads only its length, so it
    /// holds the range and never the object. That is what a caller reading a
    /// large object in bounded chunks is promised, and a store that
    /// materialized the whole object to answer each chunk would break the
    /// promise on this provider alone.
    async fn get_object(&self, key: &str, range: Option<ByteRange>) -> Result<Option<Vec<u8>>> {
        let path = self.resolve_key(key)?;
        let mut file = match File::open(&path).await {
            Ok(file) => file,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(io_error(key, err)),
        };

        let Some(range) = range else {
            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes)
                .await
                .map_err(|err| io_error(key, err))?;
            return Ok(Some(bytes));
        };

        let invalid_range = || ObjectStoreError::InvalidRange {
            object_key: key.to_owned(),
        };
        let size_bytes = file
            .metadata()
            .await
            .map_err(|err| io_error(key, err))?
            .len();
        if range.end_exclusive < range.start_inclusive || range.start_inclusive > size_bytes {
            return Err(invalid_range());
        }
        // A range ending past the object is truncated, never refused.
        let end_exclusive = range.end_exclusive.min(size_bytes);
        file.seek(SeekFrom::Start(range.start_inclusive))
            .await
            .map_err(|err| io_error(key, err))?;
        let mut bytes = Vec::new();
        file.take(end_exclusive - range.start_inclusive)
            .read_to_end(&mut bytes)
            .await
            .map_err(|err| io_error(key, err))?;
        Ok(Some(bytes))
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

    /// The reference provider stores no checksum beside an object, so it
    /// computes one from the object it holds. That is the same guarantee a
    /// cloud provider's stored checksum gives — the provider attesting to
    /// the bytes it actually has — and it never crosses a network.
    async fn head_stored_checksum(&self, key: &str) -> Result<Option<StoredObjectChecksum>> {
        let scoped = self.scoped(key)?;
        let Some(bytes) = self.get_object(&scoped, None).await? else {
            return Ok(None);
        };
        Ok(Some(StoredObjectChecksum {
            size_bytes: bytes.len() as u64,
            storage_checksum: StorageChecksum::sha256(&bytes),
        }))
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

    async fn put_streamed(&self, key: &str, body: ByteStream, mode: PutMode) -> Result<u64> {
        self.put_streamed_object(&self.scoped(key)?, body, mode)
            .await
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
        // Concurrent writers race this walk by design: deletes prune empty
        // parent directories and publishes write-then-rename scratch files.
        // An entry that vanishes between enumeration and inspection is an
        // absent key, not a listing failure — exactly what a cloud
        // provider's list reports for it.
        let mut reader = match fs::read_dir(&current).await {
            Ok(reader) => reader,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => return Err(io_error(prefix, err)),
        };
        let mut entries = Vec::new();
        loop {
            match reader.next_entry().await {
                Ok(Some(entry)) => entries.push(entry.path()),
                Ok(None) => break,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => break,
                Err(err) => return Err(io_error(prefix, err)),
            }
        }
        entries.sort();

        for path in entries.into_iter().rev() {
            // Filter in-flight scratch names before the stat: a scratch
            // file is renamed away mid-publish, so inspecting it first
            // would turn the normal rename race into a listing error.
            if path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(is_scratch_name)
            {
                continue;
            }
            let metadata = match fs::metadata(&path).await {
                Ok(metadata) => metadata,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => return Err(io_error(prefix, err)),
            };
            if metadata.is_dir() {
                dirs.push(path);
                continue;
            }

            if metadata.is_file() {
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
    use super::{ByteRange, ObjectStore, ObjectStoreError, PutMode};
    use crate::keys::{upload_session, wal_head};
    use bytes::Bytes;
    use std::fs;
    use std::sync::Arc;
    use tokio::sync::Barrier;

    #[test]
    fn construction_requires_atomic_rename_replace_support() {
        let temp_dir = test_dir("construction-gate");
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

    #[cfg(unix)]
    #[tokio::test]
    async fn listing_tolerates_entries_that_vanish_mid_walk() {
        let temp_dir = test_dir("listing-races");
        let store = LocalFsStore::new(temp_dir.path()).expect("create local fs store");
        let key = wal_head(&loonfs_api::NamespaceId::parse("ns-1").expect("valid namespace id"));
        store
            .put(&key, Bytes::from_static(b"{}"), PutMode::Overwrite)
            .await
            .expect("seed object");

        // A dangling path is exactly what the walk sees when a concurrent
        // publish renames its scratch file away, or a concurrent delete
        // removes an object, between enumeration and inspection: read_dir
        // listed the entry but the stat answers NotFound. Broken symlinks
        // reproduce that window deterministically.
        let dir = temp_dir.path().join("namespaces/ns-1/wal");
        std::os::unix::fs::symlink(dir.join("missing"), dir.join(".head.json.tmp-1-2"))
            .expect("dangling scratch entry");
        std::os::unix::fs::symlink(dir.join("missing"), dir.join("vanished.json"))
            .expect("dangling plain entry");

        let keys = store
            .list_prefix("namespaces/ns-1/")
            .await
            .expect("listing succeeds despite dangling entries");
        assert_eq!(keys, vec![key]);
    }

    #[tokio::test]
    async fn a_head_of_a_missing_object_answers_gone() {
        let temp_dir = test_dir("head-missing");
        let store = LocalFsStore::new(temp_dir.path()).expect("create local fs store");

        let answer = store
            .head(&wal_head(
                &loonfs_api::NamespaceId::parse("ns-1").expect("valid namespace id"),
            ))
            .await
            .expect("head succeeds");
        assert!(answer.is_none());
    }

    // A delete can land between `metadata_for_path`'s stat and its digest
    // read; no single filesystem state makes the stat succeed and the read
    // answer NotFound (both follow the same path resolution), so the race
    // window cannot be staged whole. Each arm of the read is pinned
    // directly instead.
    #[tokio::test]
    async fn a_digest_read_of_a_vanished_object_answers_gone() {
        let temp_dir = test_dir("digest-vanished");
        let vanished = temp_dir.path().join("vanished.json");

        let answer = LocalFsStore::read_for_digest("namespaces/ns-1/wal/head.json", &vanished)
            .await
            .expect("a vanished object is an answer, not an error");
        assert!(answer.is_none());
    }

    #[tokio::test]
    async fn a_digest_read_that_fails_for_another_reason_stays_an_error() {
        let temp_dir = test_dir("digest-error");
        let directory = temp_dir.path().join("dir");
        fs::create_dir(&directory).expect("create directory");

        let error = LocalFsStore::read_for_digest("namespaces/ns-1/wal/head.json", &directory)
            .await
            .expect_err("reading a directory is not a missing object");
        assert!(matches!(error, ObjectStoreError::Transport { .. }));
    }

    #[tokio::test]
    async fn overwrite_refreshes_head_and_visible_bytes() {
        let temp_dir = test_dir("overwrite");
        let store = LocalFsStore::new(temp_dir.path()).expect("create local fs store");
        let key = wal_head(&loonfs_api::NamespaceId::parse("ns-1").expect("valid namespace id"));

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

    /// The range contract a chunked reader depends on: ranges answer their
    /// own bytes, an end past the object is truncated rather than refused,
    /// and a start past the object or a descending range is refused.
    #[tokio::test]
    async fn ranged_reads_answer_their_range_and_refuse_impossible_ones() {
        let temp_dir = test_dir("ranged-reads");
        let store = LocalFsStore::new(temp_dir.path()).expect("create local fs store");
        let key = wal_head(&loonfs_api::NamespaceId::parse("ns-1").expect("valid namespace id"));
        let payload = Bytes::from_static(b"0123456789");
        store
            .put(&key, payload.clone(), PutMode::Overwrite)
            .await
            .expect("seed object");

        let range = |start_inclusive, end_exclusive| {
            Some(ByteRange {
                start_inclusive,
                end_exclusive,
            })
        };
        assert_eq!(
            store.get(&key, range(0, 4)).await.expect("leading range"),
            Some(Bytes::from_static(b"0123"))
        );
        assert_eq!(
            store.get(&key, range(4, 7)).await.expect("middle range"),
            Some(Bytes::from_static(b"456"))
        );
        assert_eq!(
            store
                .get(&key, range(7, 99))
                .await
                .expect("range past the end is truncated"),
            Some(Bytes::from_static(b"789"))
        );
        assert_eq!(
            store
                .get(&key, range(10, 10))
                .await
                .expect("a range at the end is empty, not missing"),
            Some(Bytes::new())
        );
        assert!(matches!(
            store.get(&key, range(11, 12)).await,
            Err(ObjectStoreError::InvalidRange { .. })
        ));
        assert!(matches!(
            store.get(&key, range(6, 2)).await,
            Err(ObjectStoreError::InvalidRange { .. })
        ));
        assert!(store
            .get(
                &wal_head(
                    &loonfs_api::NamespaceId::parse("ns-missing").expect("valid namespace id")
                ),
                range(0, 4)
            )
            .await
            .expect("a ranged read of a missing object is absent, not an error")
            .is_none());
    }

    /// Readers race each overwrite rename to exercise replacement visibility across runtime workers.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_readers_observe_complete_replacement_generations() {
        const PAYLOAD_BYTES: usize = 16 * 1024;
        const READER_COUNT: usize = 4;
        const REPLACEMENT_COUNT: u8 = 32;

        let temp_dir = test_dir("atomic-replacement");
        let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("create local fs store"));
        let key = wal_head(
            &loonfs_api::NamespaceId::parse("ns-atomic-replacement").expect("valid namespace id"),
        );
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
        let temp_dir = test_dir("delete");
        let store = LocalFsStore::new(temp_dir.path()).expect("create local fs store");
        let key = upload_session(
            &loonfs_api::NamespaceId::parse("ns-1").expect("valid namespace id"),
            &loonfs_api::UploadId::parse("upl_00000000000000000000000000000001")
                .expect("valid upload id"),
        );

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
        let temp_dir = test_dir("cross-instance-cas");
        let key = wal_head(&loonfs_api::NamespaceId::parse("ns-cas").expect("valid namespace id"));
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
        let temp_dir = test_dir("scratch");
        let store = LocalFsStore::new(temp_dir.path()).expect("create local fs store");
        let key =
            wal_head(&loonfs_api::NamespaceId::parse("ns-scratch").expect("valid namespace id"));
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

    fn test_dir(label: &str) -> tempfile::TempDir {
        tempfile::Builder::new()
            .prefix(&format!("loonfs-local-fs-{label}-"))
            .tempdir()
            .expect("create temp dir")
    }
}
