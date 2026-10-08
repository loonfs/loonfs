//! Durable content storage: blob reads and writes, inline values and the WAL
//! tail's pieces, plus the admission tokens that vouch for already-validated
//! direct-put content.

pub(crate) mod content;
pub(crate) mod content_admission;
pub(crate) mod content_location;
pub(crate) mod inline_content;
pub(crate) mod tail_content;
