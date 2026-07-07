//! Shared durable envelope layout.
//!
//! Every durable object stores common metadata plus opaque payload bytes. The
//! payload checksum covers the stored bytes exactly, not a re-encoding.

use serde::Deserialize;

/// Identifying prefix of a durable envelope document.
///
/// Decoded before the full document so unknown kinds and unsupported format
/// versions fail with a precise error; `kind` stays a string rather than an
/// enum so future kinds remain reportable.
#[derive(Debug, Deserialize)]
pub(crate) struct EnvelopeProbe {
    pub(crate) kind: String,
    pub(crate) format_version: u32,
}
