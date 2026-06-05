use crate::options::{CommitOptions, ReadOptions, WriteOptions};
use loon_api::NamespaceId;
use loon_objectstore::ObjectStore;
use thiserror::Error;

const DEFAULT_LEASE_DURATION_MS: u64 = 5_000;

#[derive(Debug)]
pub struct NamespaceEngine<S> {
    store: S,
    namespace_id: NamespaceId,
    writer_id: String,
    writer_version: String,
    lease_duration_ms: u64,
    read_options: ReadOptions,
    write_options: WriteOptions,
    commit_options: CommitOptions,
}

impl<S: ObjectStore> NamespaceEngine<S> {
    pub fn builder(store: S) -> NamespaceEngineBuilder<S> {
        NamespaceEngineBuilder {
            store,
            namespace_id: None,
            writer_id: None,
            writer_version: default_writer_version(),
            lease_duration_ms: DEFAULT_LEASE_DURATION_MS,
            read_options: ReadOptions::default(),
            write_options: WriteOptions::default(),
            commit_options: CommitOptions::default(),
        }
    }

    pub fn namespace_id(&self) -> &NamespaceId {
        &self.namespace_id
    }

    pub fn writer_id(&self) -> &str {
        &self.writer_id
    }

    pub fn writer_version(&self) -> &str {
        &self.writer_version
    }

    pub fn lease_duration_ms(&self) -> u64 {
        self.lease_duration_ms
    }

    pub fn read_options(&self) -> &ReadOptions {
        &self.read_options
    }

    pub fn write_options(&self) -> &WriteOptions {
        &self.write_options
    }

    pub fn commit_options(&self) -> &CommitOptions {
        &self.commit_options
    }

    pub fn into_store(self) -> S {
        self.store
    }
}

#[derive(Debug)]
pub struct NamespaceEngineBuilder<S> {
    store: S,
    namespace_id: Option<NamespaceId>,
    writer_id: Option<String>,
    writer_version: String,
    lease_duration_ms: u64,
    read_options: ReadOptions,
    write_options: WriteOptions,
    commit_options: CommitOptions,
}

impl<S: ObjectStore> NamespaceEngineBuilder<S> {
    pub fn namespace(mut self, namespace_id: NamespaceId) -> Self {
        self.namespace_id = Some(namespace_id);
        self
    }

    pub fn writer(mut self, writer_id: impl Into<String>) -> Self {
        self.writer_id = Some(writer_id.into());
        self
    }

    pub fn writer_version(mut self, writer_version: impl Into<String>) -> Self {
        self.writer_version = writer_version.into();
        self
    }

    pub fn lease_duration_ms(mut self, lease_duration_ms: u64) -> Self {
        self.lease_duration_ms = lease_duration_ms;
        self
    }

    pub fn read_options(mut self, options: ReadOptions) -> Self {
        self.read_options = options;
        self
    }

    pub fn write_options(mut self, options: WriteOptions) -> Self {
        self.write_options = options;
        self
    }

    pub fn commit_options(mut self, options: CommitOptions) -> Self {
        self.commit_options = options;
        self
    }

    pub fn build(self) -> Result<NamespaceEngine<S>, NamespaceEngineBuildError> {
        let namespace_id = self
            .namespace_id
            .ok_or(NamespaceEngineBuildError::MissingNamespace)?;
        let writer_id = self
            .writer_id
            .ok_or(NamespaceEngineBuildError::MissingWriter)?;
        if writer_id.trim().is_empty() {
            return Err(NamespaceEngineBuildError::EmptyWriter);
        }
        if self.writer_version.trim().is_empty() {
            return Err(NamespaceEngineBuildError::EmptyWriterVersion);
        }

        Ok(NamespaceEngine {
            store: self.store,
            namespace_id,
            writer_id,
            writer_version: self.writer_version,
            lease_duration_ms: self.lease_duration_ms,
            read_options: self.read_options,
            write_options: self.write_options,
            commit_options: self.commit_options,
        })
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum NamespaceEngineBuildError {
    #[error("namespace is required")]
    MissingNamespace,
    #[error("writer identity is required")]
    MissingWriter,
    #[error("writer identity must not be empty")]
    EmptyWriter,
    #[error("writer version must not be empty")]
    EmptyWriterVersion,
}

fn default_writer_version() -> String {
    format!("loon-core/{}", env!("CARGO_PKG_VERSION"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use loon_objectstore::fs::LocalFsStore;
    use tempfile::tempdir;

    #[test]
    fn namespace_engine_builds_with_required_identity() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");

        let engine = NamespaceEngine::builder(store)
            .namespace(namespace_id.clone())
            .writer("writer-a")
            .build()
            .expect("engine builds");

        assert_eq!(engine.namespace_id(), &namespace_id);
        assert_eq!(engine.writer_id(), "writer-a");
        assert!(!engine.writer_version().is_empty());
        assert_eq!(engine.lease_duration_ms(), DEFAULT_LEASE_DURATION_MS);
        assert_eq!(engine.read_options(), &ReadOptions::default());
        assert_eq!(engine.write_options(), &WriteOptions::default());
        assert_eq!(engine.commit_options(), &CommitOptions::default());
    }

    #[test]
    fn namespace_engine_builder_rejects_missing_required_fields() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let err = NamespaceEngine::builder(store)
            .build()
            .expect_err("missing namespace");
        assert_eq!(err, NamespaceEngineBuildError::MissingNamespace);

        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let err = NamespaceEngine::builder(store)
            .namespace(NamespaceId::parse("demo").expect("valid namespace id"))
            .build()
            .expect_err("missing writer");
        assert_eq!(err, NamespaceEngineBuildError::MissingWriter);
    }
}
