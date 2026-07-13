//! Durable content storage: blob reads and writes, plus the admission
//! tokens that vouch for already-validated direct-put content.

pub(crate) mod content;
pub(crate) mod content_admission;
