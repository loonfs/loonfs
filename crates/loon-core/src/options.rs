use crate::{
    basis::VerifiedNamespaceBasis, checkpoint::MetadataTableCache, path::intent::PutFileBehavior,
};
use loon_api::wire::control::HeadState;
use loon_api::CommitId;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BootstrapOptions {
    pub allow_existing: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ForkOptions {}

#[derive(Debug, Clone)]
pub struct ReadOptions {
    source: ReadSource,
}

impl ReadOptions {
    pub fn prefer_materialized() -> Self {
        Self {
            source: ReadSource::PreferMaterialized,
        }
    }

    pub fn full_basis() -> Self {
        Self {
            source: ReadSource::FullBasis,
        }
    }

    pub fn verified_basis(basis: Arc<VerifiedNamespaceBasis>) -> Self {
        Self {
            source: ReadSource::VerifiedBasis(basis),
        }
    }

    pub fn materialized_tables_at_head(
        head: HeadState,
        table_cache: Option<Arc<MetadataTableCache>>,
    ) -> Self {
        Self {
            source: ReadSource::MaterializedTablesAtHead { head, table_cache },
        }
    }

    pub fn source(&self) -> &ReadSource {
        &self.source
    }

    pub(crate) fn into_source(self) -> ReadSource {
        self.source
    }
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self::prefer_materialized()
    }
}

#[derive(Debug, Clone)]
pub enum ReadSource {
    PreferMaterialized,
    FullBasis,
    VerifiedBasis(Arc<VerifiedNamespaceBasis>),
    MaterializedTablesAtHead {
        head: HeadState,
        table_cache: Option<Arc<MetadataTableCache>>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteOptions {
    pub commit_id: Option<CommitId>,
    pub put_file_behavior: PutFileBehavior,
    pub recursive_delete: bool,
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self {
            commit_id: None,
            put_file_behavior: PutFileBehavior::CreateOnly,
            recursive_delete: true,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CommitOptions {}
