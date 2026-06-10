//! Shared pieces of the durable envelope layout.
//!
//! Every durable LoonFS object is an envelope document with the same leading
//! fields — `kind`, `format_version`, `writer_version`, `payload_checksum` —
//! followed by the payload as an opaque sub-document (a CBOR byte string or a
//! raw JSON fragment). `payload_checksum` is always computed over the exact
//! payload bytes as stored, never over a re-encoding, so checksum failures
//! mean corruption and version skew surfaces as a version error.

use serde::Deserialize;

/// Identifying prefix of a durable envelope document.
///
/// Readers decode this probe before the full document so that objects written
/// with an unknown kind or an unsupported format version fail with a precise
/// error instead of a generic decode error. `kind` is deliberately a string
/// (not an enum) so future kinds remain reportable.
#[derive(Debug, Deserialize)]
pub(crate) struct EnvelopeProbe {
    pub(crate) kind: String,
    pub(crate) format_version: u32,
}
