//! [`MutationContext`]: the writer identity and request timestamp every
//! mutation carries.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MutationContext {
    pub writer_id: String,
    pub writer_session_id: String,
    pub now_ms: u64,
}
