//! Where the daemon keeps its socket, its lock, its log, and each mount's
//! state.

use super::error::DaemonError;
use std::ffi::OsString;
use std::path::{Path, PathBuf};

const SOCKET_FILE_NAME: &str = "daemon.sock";
const LOCK_FILE_NAME: &str = "daemon.lock";
const LOG_FILE_NAME: &str = "daemon.log";
const MOUNTS_DIRECTORY_NAME: &str = "mounts";

/// The daemon's files for one state directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaemonPaths {
    state_directory: PathBuf,
    socket: PathBuf,
}

impl DaemonPaths {
    /// Resolves the daemon's files from the process's environment.
    ///
    /// The state directory is `state_directory` when it's set, and otherwise
    /// `$XDG_STATE_HOME/loonfs` or `~/.local/state/loonfs`. The socket is
    /// `$XDG_RUNTIME_DIR/loonfs/daemon.sock`, or `daemon.sock` in the state
    /// directory when that variable isn't set. A relative state directory is
    /// taken from the current directory.
    pub fn resolve(state_directory: Option<&Path>) -> Result<Self, DaemonError> {
        Self::resolve_from(state_directory, |name| std::env::var_os(name))
    }

    fn resolve_from(
        state_directory: Option<&Path>,
        variable: impl Fn(&str) -> Option<OsString>,
    ) -> Result<Self, DaemonError> {
        let set = |name: &str| variable(name).filter(|value| !value.is_empty());
        let state_directory = match state_directory {
            Some(directory) => directory.to_path_buf(),
            None => match (set("XDG_STATE_HOME"), set("HOME")) {
                (Some(state_home), _) => PathBuf::from(state_home).join("loonfs"),
                (None, Some(home)) => PathBuf::from(home).join(".local/state/loonfs"),
                (None, None) => {
                    return Err(DaemonError::NoStateDirectory);
                }
            },
        };
        let state_directory = std::path::absolute(&state_directory)
            .map_err(|error| DaemonError::io(&state_directory, &error))?;
        let socket = match set("XDG_RUNTIME_DIR") {
            Some(runtime) => PathBuf::from(runtime).join("loonfs").join(SOCKET_FILE_NAME),
            None => state_directory.join(SOCKET_FILE_NAME),
        };
        Ok(Self {
            state_directory,
            socket,
        })
    }

    /// Returns the state directory.
    pub fn state_directory(&self) -> &Path {
        &self.state_directory
    }

    /// Returns the daemon's socket.
    pub fn socket(&self) -> &Path {
        &self.socket
    }

    /// Returns whether `path` is the state directory, once both are
    /// resolved through their symbolic links.
    pub(crate) fn is_state_directory(&self, path: &Path) -> bool {
        let canonical =
            |path: &Path| std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
        canonical(path) == canonical(&self.state_directory)
    }

    /// Returns the file that the daemon writes its log to.
    pub fn log_file(&self) -> PathBuf {
        self.state_directory.join(LOG_FILE_NAME)
    }

    /// Returns the file whose lock the running daemon keeps.
    pub(crate) fn lock_file(&self) -> PathBuf {
        self.state_directory.join(LOCK_FILE_NAME)
    }

    /// Returns the state directory of the mount at `directory`, which is the
    /// same for every daemon, so a restarted daemon resumes the state.
    pub fn mount_state_directory(&self, directory: &Path) -> PathBuf {
        let key = loonfs_types::sha256_hex(directory.as_os_str().as_encoded_bytes());
        self.state_directory.join(MOUNTS_DIRECTORY_NAME).join(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn environment<'a>(pairs: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<OsString> + 'a {
        move |name| {
            pairs
                .iter()
                .find(|(key, _)| *key == name)
                .map(|(_, value)| OsString::from(value))
        }
    }

    /// Checks the state directory's and the socket's defaults, and that each
    /// mount directory has its own state directory.
    #[test]
    fn paths_follow_the_flag_then_the_environment() {
        let paths =
            DaemonPaths::resolve_from(None, environment(&[("HOME", "/home/ada")])).expect("paths");
        assert_eq!(
            paths.state_directory(),
            Path::new("/home/ada/.local/state/loonfs")
        );
        assert_eq!(
            paths.socket(),
            Path::new("/home/ada/.local/state/loonfs/daemon.sock")
        );

        let paths = DaemonPaths::resolve_from(
            None,
            environment(&[
                ("HOME", "/home/ada"),
                ("XDG_STATE_HOME", "/state"),
                ("XDG_RUNTIME_DIR", "/run/user/1000"),
            ]),
        )
        .expect("paths");
        assert_eq!(paths.state_directory(), Path::new("/state/loonfs"));
        assert_eq!(
            paths.socket(),
            Path::new("/run/user/1000/loonfs/daemon.sock")
        );

        let paths = DaemonPaths::resolve_from(
            Some(Path::new("/srv/loonfs")),
            environment(&[("XDG_STATE_HOME", "/state"), ("XDG_RUNTIME_DIR", "")]),
        )
        .expect("paths");
        assert_eq!(paths.state_directory(), Path::new("/srv/loonfs"));
        assert_eq!(paths.socket(), Path::new("/srv/loonfs/daemon.sock"));
        let first = paths.mount_state_directory(Path::new("/mnt/a"));
        assert!(first.starts_with("/srv/loonfs/mounts"));
        assert_eq!(first, paths.mount_state_directory(Path::new("/mnt/a")));
        assert_ne!(first, paths.mount_state_directory(Path::new("/mnt/b")));

        assert!(matches!(
            DaemonPaths::resolve_from(None, environment(&[])),
            Err(DaemonError::NoStateDirectory)
        ));
    }
}
