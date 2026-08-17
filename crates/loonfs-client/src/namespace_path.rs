//! Validated namespace-qualified filesystem paths.

use super::*;

/// A path qualified by namespace.
///
/// Both parts are validated at construction — [`NamespacePath::parse`] for
/// strings, [`NamespacePath::new`] for already-typed parts — so a value of
/// this type always names a well-formed target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespacePath {
    namespace: NamespaceId,
    absolute_path: AbsolutePath,
}

impl NamespacePath {
    /// Parses and validates both parts of a namespace-qualified path.
    pub fn parse(namespace: &str, absolute_path: &str) -> Result<Self> {
        let namespace = NamespaceId::parse(namespace)
            .map_err(|error| ClientError::InvalidNamespacePath(error.to_string()))?;
        let absolute_path = AbsolutePath::parse(absolute_path)
            .map_err(|error| ClientError::InvalidNamespacePath(error.to_string()))?;
        Ok(Self {
            namespace,
            absolute_path,
        })
    }

    /// Pairs already-validated parts without re-parsing.
    pub fn new(namespace: NamespaceId, absolute_path: AbsolutePath) -> Self {
        Self {
            namespace,
            absolute_path,
        }
    }

    /// Namespace the path is scoped to.
    pub fn namespace(&self) -> &NamespaceId {
        &self.namespace
    }

    /// Absolute path inside the namespace.
    pub fn absolute_path(&self) -> &AbsolutePath {
        &self.absolute_path
    }
}
