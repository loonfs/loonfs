//! The server's one error type, for loading its config, serving, and
//! shutting down.

use loonfs_types::config::ConfigFieldError;
use std::io;
use std::net::SocketAddr;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, ServerError>;

/// Failure loading the server's config, starting the server, serving, or
/// settling the server's runtime. A TLS variant identifies the file that
/// caused the failure and never includes key material.
#[derive(Debug, Error)]
pub enum ServerError {
    #[error("failed to read config: {0}")]
    ConfigRead(String),
    #[error("failed to decode config: {0}")]
    ConfigDecode(String),
    #[error("invalid server config: {0}")]
    Config(#[from] ConfigFieldError),
    #[error("failed to read TLS file `{path}`: {source}")]
    TlsRead {
        path: String,
        #[source]
        source: io::Error,
    },
    #[error("`{path}` is not valid PEM: {reason}")]
    TlsPem { path: String, reason: String },
    #[error(
        "`{path}` and the certificate in `{cert_path}` do not form a usable identity: {reason}"
    )]
    TlsIdentity {
        path: String,
        cert_path: String,
        reason: String,
    },
    #[error("failed to bind `{addr}`: {source}")]
    Bind {
        addr: SocketAddr,
        #[source]
        source: io::Error,
    },
    #[error("failed to install the shutdown signal handlers: {0}")]
    Signal(#[source] io::Error),
    #[error("server failed while serving requests: {0}")]
    Serve(#[source] io::Error),
    #[error("runtime or maintenance shutdown did not settle: {0}")]
    Shutdown(#[source] loonfs::Error),
    #[error("unsupported LOONFS_TRACE value `{0}`; expected `json` or `off`")]
    TraceMode(String),
    #[error("invalid RUST_LOG filter: {0}")]
    TraceFilter(String),
    #[error("failed to initialize tracing subscriber: {0}")]
    TraceSubscriber(String),
}
