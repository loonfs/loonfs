use super::{ByteRange, ObjectMetadata, ObjectStore, PutMode};
use crate::objectstore::keyspace::validate_segments;
use crate::objectstore::ObjectStoreError;
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

    fn metadata_for_path(path: &Path) -> Result<ObjectMetadata, ObjectStoreError> {
        let metadata = fs::metadata(path).map_err(io_error)?;
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
            size_bytes: metadata.len(),
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

impl ObjectStore for LocalFsStore {
    fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        let path = self.resolve_key(key)?;
        if !path.exists() {
            return Ok(None);
        }

        Self::metadata_for_path(&path).map(Some)
    }

    fn get(
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

    fn put(
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
                    .head(key)?
                    .ok_or(ObjectStoreError::PreconditionFailed)?;
                if current.etag.as_deref() != Some(expected_etag.as_str()) {
                    return Err(ObjectStoreError::PreconditionFailed);
                }
                Self::replace_object(&path, bytes)?;
            }
        }

        Self::metadata_for_path(&path)
    }

    fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
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

    fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
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

fn temp_path(path: &Path) -> PathBuf {
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
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn overwrite_refreshes_head_and_visible_bytes() {
        let temp_dir = TestDir::new("overwrite");
        let store = LocalFsStore::new(temp_dir.path()).expect("create local fs store");
        let key = "namespaces/ns-1/head.json";

        let first = store
            .put(key, br#"{"seq":1}"#, PutMode::Overwrite)
            .expect("seed first object");
        let second = store
            .put(key, br#"{"seq":2}"#, PutMode::Overwrite)
            .expect("overwrite object");

        assert_eq!(
            store.get(key, None).expect("get object"),
            Some(br#"{"seq":2}"#.to_vec())
        );
        assert_eq!(store.head(key).expect("head object"), Some(second.clone()));
        assert_ne!(first, second);
    }

    #[test]
    fn delete_is_idempotent_and_head_reflects_removal() {
        let temp_dir = TestDir::new("delete");
        let store = LocalFsStore::new(temp_dir.path()).expect("create local fs store");
        let key = "namespaces/ns-1/lease.json";

        store
            .put_if_absent(key, br#"{"holder":"writer-a"}"#)
            .expect("seed lease object");
        assert!(store.head(key).expect("head before delete").is_some());
        store.delete(key).expect("delete existing object");
        store.delete(key).expect("delete missing object");
        assert_eq!(store.head(key).expect("head after delete"), None);
    }

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new(label: &str) -> Self {
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
