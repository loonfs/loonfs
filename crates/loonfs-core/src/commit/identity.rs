//! Core representation of a commit identity fingerprint.
//!
//! [`loonfs_api::semantic_commit_fingerprint`] computes the shared client and
//! runtime digest. This module wraps a validated fingerprint for comparison
//! with stored commit receipts.

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
