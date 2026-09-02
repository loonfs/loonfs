//! [`MutationContext`]: the writer identity and request timestamp every
//! mutation carries.

use loonfs_api::WriterId;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MutationContext {
    pub writer_id: WriterId,
    pub now_ms: u64,
}
