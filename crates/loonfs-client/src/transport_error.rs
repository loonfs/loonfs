//! Transport failures before a complete HTTP response is received.

use loonfs_api::ErrorCode;

type Cause = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug)]
enum Kind {
    Other,
    Connect,
    Timeout,
    Body,
}

/// A failed HTTP exchange, distinct from a served API error response.
#[derive(Debug, thiserror::Error)]
#[error("transport failure: {source}")]
pub struct TransportError {
    kind: Kind,
    #[source]
    source: Cause,
}

impl TransportError {
    /// Preserves a transport cause without a more specific classification.
    pub fn new(source: impl Into<Cause>) -> Self {
        Self {
            kind: Kind::Other,
            source: source.into(),
        }
    }

    /// Marks a failure to establish a connection.
    pub fn connect(source: impl Into<Cause>) -> Self {
        Self {
            kind: Kind::Connect,
            source: source.into(),
        }
    }

    /// Marks an expired request or inactivity limit.
    pub fn timeout(source: impl Into<Cause>) -> Self {
        Self {
            kind: Kind::Timeout,
            source: source.into(),
        }
    }

    /// Marks an interrupted request or response body.
    pub fn body(source: impl Into<Cause>) -> Self {
        Self {
            kind: Kind::Body,
            source: source.into(),
        }
    }

    /// Reports whether establishing the connection failed.
    pub fn is_connect(&self) -> bool {
        matches!(self.kind, Kind::Connect)
    }

    /// Reports whether a configured time limit expired.
    pub fn is_timeout(&self) -> bool {
        matches!(self.kind, Kind::Timeout)
    }

    /// Reports whether a body ended with an error.
    pub fn is_body(&self) -> bool {
        matches!(self.kind, Kind::Body)
    }

    /// Returns `None` because transport failures carry no server error code.
    pub fn code(&self) -> Option<ErrorCode> {
        None
    }
}
