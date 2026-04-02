use loon_types::{Identity, NamespaceId};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResolveError {
    NotFound,
    NotDirectory,
    MountLoop,
    CoveredBySubtreeTombstone,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolveRequest {
    pub start_namespace_id: NamespaceId,
    pub path: String,
}

pub type ResolveResult = Result<Identity, ResolveError>;
