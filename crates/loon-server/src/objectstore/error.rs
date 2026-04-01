use thiserror::Error;

#[derive(Debug, Error)]
pub enum ObjectStoreError {
    #[error("object not found")]
    NotFound,
    #[error("invalid object key: {0}")]
    InvalidKey(String),
    #[error("invalid byte range")]
    InvalidRange,
    #[error("precondition failed")]
    PreconditionFailed,
    #[error("conflict")]
    Conflict,
    #[error("unsupported capability: {0}")]
    Unsupported(&'static str),
    #[error("transport error: {0}")]
    Transport(String),
}
