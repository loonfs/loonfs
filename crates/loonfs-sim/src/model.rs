//! Stable model-state hashing and comparison helpers.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub trait ModelState {
    fn stable_hash(&self) -> String;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelComparison {
    pub model_hash: String,
    pub core_hash: String,
}

impl ModelComparison {
    pub fn matches(&self) -> bool {
        self.model_hash == self.core_hash
    }
}

pub fn stable_hash_bytes(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    format!("sha256:{digest:x}")
}
