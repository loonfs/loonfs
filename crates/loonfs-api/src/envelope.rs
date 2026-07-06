//! Shared durable envelope layout.
//!
//! Every durable object stores common metadata plus opaque payload bytes. The
//! payload checksum covers the stored bytes exactly, not a re-encoding.

use serde::Deserialize;

/// Identifying prefix of a durable envelope document.
#[derive(Debug, Deserialize)]
pub(crate) struct EnvelopeProbe {
    pub(crate) kind: String,
    pub(crate) format_version: u32,
}
