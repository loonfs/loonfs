//! Defines [`DaemonError`], returned when the daemon can't start or a command
//! can't talk to it.

use std::path::Path;
use thiserror::Error;

/// Error returned by the daemon's setup and by its client.
///
/// A request that the daemon refuses isn't one of these: the daemon returns
/// it as a [`DaemonFailure`](super::protocol::DaemonFailure) response.
#[derive(Debug, Clone, Error)]
#[non_exhaustive]
pub enum DaemonError {
    /// Neither a state directory nor a home directory is set.
    #[error(
        "daemon has no state directory: pass `--state-dir`, or set `XDG_STATE_HOME` or `HOME`"
    )]
    NoStateDirectory,
    /// Another daemon keeps the state directory's lock.
    #[error("another daemon serves state directory `{path}`")]
    AlreadyRunning {
        /// The state directory.
        path: String,
    },
    /// A file or socket of the daemon couldn't be used.
    #[error("daemon i/o on `{path}` failed: {message}")]
    Io {
        /// The file, directory, or socket.
        path: String,
        /// The operating system's error.
        message: String,
    },
    /// The other side sent a line that isn't a message of the protocol.
    #[error("daemon protocol error: {message}")]
    Protocol {
        /// What was wrong with the line.
        message: String,
    },
}

impl DaemonError {
    pub(crate) fn io(path: &Path, error: &std::io::Error) -> Self {
        Self::Io {
            path: path.display().to_string(),
            message: error.to_string(),
        }
    }

    pub(crate) fn protocol(message: impl Into<String>) -> Self {
        Self::Protocol {
            message: message.into(),
        }
    }
}
