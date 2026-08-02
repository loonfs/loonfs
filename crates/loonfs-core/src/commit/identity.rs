//! Commit identity fingerprints (format spec, "Commit identity
//! fingerprints"): a stable digest over a mutation's semantic content, used
//! to decide whether a reused commit id carries the same mutation or a
//! conflicting one.
//!
//! The digest itself is computed by
//! [`loonfs_api::semantic_commit_fingerprint`], because the HTTP client has
//! to compute the same value and does not depend on this crate. What stays
//! here is core's name for one: a value that reached this crate through
//! [`crate::path::write::commit_fingerprint`] and is therefore comparable
//! against a stored receipt.

use serde::{Deserialize, Serialize};

/// The semantic identity of one mutation request: what a reused commit id is
/// compared against.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CommitFingerprint(String);

impl CommitFingerprint {
    pub(crate) fn new_unchecked(value: String) -> Self {
        Self(value)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}
