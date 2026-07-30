//! Commit identity fingerprints (format spec, "Commit identity
//! fingerprints"): a stable digest over a mutation's semantic content, used
//! to decide whether a reused commit id carries the same mutation or a
//! conflicting one.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt::Write as _;

/// Domain separator for the one mutation fingerprint preimage.
pub(crate) const COMMIT_FINGERPRINT_DOMAIN: &str = "loonfs.commit.semantic.v0";

/// Scheme-and-algorithm tag carried by every stored fingerprint value.
///
/// `v0` names the canonicalization rules (domain string plus the frozen v0
/// preimage encoding; format spec, "Commit identity fingerprints") and
/// `sha256` the digest algorithm, so either can change later without
/// re-interpreting values already stored in WAL records and commit receipts.
const FINGERPRINT_SCHEME: &str = "v0:sha256";

/// Computes a stored fingerprint value (`v0:sha256:<64 lowercase hex>`) from
/// a canonical preimage.
///
/// The preimage's compact JSON encoding is the durable contract: a
/// pinned-value test in `path::write::planner` fails if it drifts.
pub(crate) fn fingerprint_digest<T>(preimage: &T) -> Result<String, serde_json::Error>
where
    T: Serialize,
{
    let bytes = serde_json::to_vec(preimage)?;
    Ok(fingerprint_bytes(&bytes))
}

fn fingerprint_bytes(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut value = String::with_capacity(FINGERPRINT_SCHEME.len() + 1 + digest.len() * 2);
    value.push_str(FINGERPRINT_SCHEME);
    value.push(':');
    for byte in digest {
        write!(&mut value, "{byte:02x}").expect("writing to a String should not fail");
    }
    value
}

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
