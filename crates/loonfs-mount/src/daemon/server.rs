//! The daemon's socket: it takes the state directory's lock, accepts one
//! request on each connection, and stops once it has been idle long enough.

use super::error::DaemonError;
use super::kernel::Kernel;
use super::mounts::Mounts;
use super::paths::DaemonPaths;
use super::protocol::{
    decode_request, encode_line, DaemonErrorCode, DaemonFailure, DaemonRequest, DaemonResponse,
    StatusRequest, MAX_REQUEST_BYTES,
};
use crate::clock;
use std::fs::{DirBuilder, File, OpenOptions};
use std::future::Future;
use std::os::unix::fs::DirBuilderExt as _;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;

/// How long the daemon keeps running without a mount before it stops.
const IDLE_EXIT_AFTER_MS: u64 = 60_000;

/// How long a stopping daemon waits for the responses that its connections
/// are still writing.
const STOP_WAIT_BUDGET_MS: u64 = 5_000;

/// The daemon of one host user: it serves every mount of one state
/// directory through a [`Kernel`].
pub struct Daemon {
    paths: DaemonPaths,
    mounts: Arc<Mounts>,
    listener: UnixListener,
    /// The open lock file, whose exclusive lock lasts as long as the daemon.
    _lock: File,
}

impl std::fmt::Debug for Daemon {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Daemon")
            .field("paths", &self.paths)
            .finish_non_exhaustive()
    }
}

impl Daemon {
    /// Takes the state directory's lock and listens on the socket.
    ///
    /// A second daemon for the same state directory fails with
    /// [`DaemonError::AlreadyRunning`]. It must be called from a Tokio
    /// runtime.
    pub fn bind(paths: DaemonPaths, kernel: Arc<dyn Kernel>) -> Result<Self, DaemonError> {
        create_private_directory(paths.state_directory())?;
        let state_directory = std::fs::canonicalize(paths.state_directory())
            .map_err(|error| DaemonError::io(paths.state_directory(), &error))?;
        let paths = DaemonPaths::resolve(Some(&state_directory))?;
        let lock_path = paths.lock_file();
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(|error| DaemonError::io(&lock_path, &error))?;
        if !fs4::fs_std::FileExt::try_lock_exclusive(&lock)
            .map_err(|error| DaemonError::io(&lock_path, &error))?
        {
            return Err(DaemonError::AlreadyRunning {
                path: paths.state_directory().display().to_string(),
            });
        }
        let socket = paths.socket();
        if let Some(parent) = socket.parent() {
            create_private_directory(parent)?;
        }
        // The lock is this daemon's, so a socket file left here belongs to a
        // daemon that stopped.
        match std::fs::remove_file(socket) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(DaemonError::io(socket, &error)),
        }
        let listener =
            UnixListener::bind(socket).map_err(|error| DaemonError::io(socket, &error))?;
        Ok(Self {
            mounts: Arc::new(Mounts::new(kernel, paths.clone())),
            paths,
            listener,
            _lock: lock,
        })
    }

    /// Returns the daemon's files.
    pub fn paths(&self) -> &DaemonPaths {
        &self.paths
    }

    /// Serves requests until a `shutdown` request, or until the daemon has had
    /// no mount for a minute.
    pub async fn run(self) -> Result<(), DaemonError> {
        let (stop, mut stopped) = watch::channel(false);
        let mut directories = self.mounts.watch_directories();
        let mut connections = JoinSet::new();
        // The daemon starts without a mount, so its idle timer runs from the
        // start, and restarts each time its last mount is removed.
        let mut idle = Some(idle_timer());
        tracing::info!(socket = %self.paths.socket().display(), "the daemon started");
        loop {
            tokio::select! {
                accepted = self.listener.accept() => match accepted {
                    Ok((stream, _)) => {
                        connections.spawn(serve(stream, self.mounts.clone(), stop.clone()));
                    }
                    Err(error) => tracing::warn!(%error, "accepting a connection failed"),
                },
                Some(_) = connections.join_next(), if !connections.is_empty() => {}
                _ = directories.changed() => {
                    idle = (*directories.borrow_and_update() == 0).then(idle_timer);
                }
                _ = stopped.changed() => break,
                () = async {
                    if let Some(timer) = idle.as_mut() {
                        timer.await;
                    }
                }, if idle.is_some() => {
                    tracing::info!("the daemon has had no mount for a minute");
                    break;
                }
            }
        }
        let _ = std::fs::remove_file(self.paths.socket());
        tokio::select! {
            () = async { while connections.join_next().await.is_some() {} } => {}
            () = clock::delay(STOP_WAIT_BUDGET_MS) => {}
        }
        tracing::info!("the daemon stopped");
        Ok(())
    }
}

/// Returns a timer that ends once the daemon has been idle long enough to
/// stop.
fn idle_timer() -> Pin<Box<dyn Future<Output = ()> + Send>> {
    Box::pin(clock::delay(IDLE_EXIT_AFTER_MS))
}

/// Creates a directory, and its parents, that only the daemon's user can
/// read, because the socket accepts credentials.
fn create_private_directory(path: &Path) -> Result<(), DaemonError> {
    DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(path)
        .map_err(|error| DaemonError::io(path, &error))
}

/// Reads one request from a connection, and writes its response lines.
async fn serve(stream: UnixStream, mounts: Arc<Mounts>, stop: watch::Sender<bool>) {
    let (reader, mut writer) = stream.into_split();
    let request = read_request(reader).await;
    let (sender, mut responses) = mpsc::unbounded_channel();
    // The request runs in a task of its own, so a command that disconnects
    // doesn't stop a mount halfway.
    tokio::spawn(async move {
        let response = match request {
            Ok(request) => respond(request, &mounts, &stop, &sender).await,
            Err(failure) => DaemonResponse::Error(failure),
        };
        let _ = sender.send(response);
    });
    while let Some(response) = responses.recv().await {
        let last = !matches!(response, DaemonResponse::Progress { .. });
        if let Err(error) = writer.write_all(&encode_line(&response)).await {
            tracing::debug!(%error, "a command disconnected before its response");
            return;
        }
        if last {
            return;
        }
    }
}

async fn read_request(
    reader: tokio::net::unix::OwnedReadHalf,
) -> Result<DaemonRequest, DaemonFailure> {
    let limit = u64::try_from(MAX_REQUEST_BYTES).expect("the request limit fits in a u64");
    let mut reader = BufReader::new(reader.take(limit));
    let mut line = Vec::new();
    reader.read_until(b'\n', &mut line).await.map_err(|error| {
        DaemonFailure::new(
            DaemonErrorCode::InvalidRequest,
            format!("reading the request failed: {error}"),
        )
    })?;
    if line.last() != Some(&b'\n') {
        return Err(DaemonFailure::new(
            DaemonErrorCode::InvalidRequest,
            format!("a request is one line of at most {MAX_REQUEST_BYTES} bytes"),
        ));
    }
    decode_request(&line)
}

async fn respond(
    request: DaemonRequest,
    mounts: &Arc<Mounts>,
    stop: &watch::Sender<bool>,
    progress: &mpsc::UnboundedSender<DaemonResponse>,
) -> DaemonResponse {
    if let Err(failure) = mounts.check_state_directory(request.state_directory()) {
        return DaemonResponse::Error(failure);
    }
    let result = match request {
        DaemonRequest::Mount(request) => {
            let progress = |step| {
                let _ = progress.send(DaemonResponse::Progress { step });
            };
            mounts
                .mount(*request, &progress)
                .await
                .map(|report| DaemonResponse::Mounted(Box::new(report)))
        }
        DaemonRequest::Unmount(request) => {
            mounts.unmount(request).await.map(DaemonResponse::Unmounted)
        }
        DaemonRequest::Sync(_) => Err(DaemonFailure::new(
            DaemonErrorCode::NotSupported,
            "`sync` isn't available yet, because mounts are read-only",
        )),
        DaemonRequest::Refresh(request) => mounts
            .refresh(request)
            .await
            .map(|applied_seq| DaemonResponse::Refreshed { applied_seq }),
        DaemonRequest::Status(StatusRequest { directory, .. }) => mounts
            .status(directory.as_deref())
            .map(|mounts| DaemonResponse::Statuses { mounts }),
        DaemonRequest::List(_) => Ok(DaemonResponse::Mounts {
            mounts: mounts.list(),
        }),
        DaemonRequest::Shutdown(_) => {
            mounts.detach_all().await;
            stop.send_replace(true);
            Ok(DaemonResponse::ShuttingDown)
        }
    };
    result.unwrap_or_else(DaemonResponse::Error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::daemon::kernel::{KernelMount, StaleMountOutcome};
    use crate::daemon::protocol::DaemonScope;
    use crate::engine::Engine;

    /// A kernel that mounts nothing, for requests that need no mount.
    struct NoKernel;

    impl Kernel for NoKernel {
        fn check_host(&self) -> std::io::Result<()> {
            Ok(())
        }

        fn detach_stale(&self, _directory: &Path) -> std::io::Result<StaleMountOutcome> {
            Ok(StaleMountOutcome::NotMounted)
        }

        fn mount(
            &self,
            _engine: Arc<Engine>,
            _directory: &Path,
        ) -> std::io::Result<Box<dyn KernelMount>> {
            Err(std::io::Error::other("this kernel mounts nothing"))
        }
    }

    /// Checks that the daemon refuses every request for another state
    /// directory, so a command that resolved another `--state-dir` never
    /// lists, stops, or unmounts this daemon's mounts.
    #[tokio::test]
    async fn a_request_for_another_state_directory_is_refused() {
        let state = tempfile::tempdir().expect("a state directory");
        let other = tempfile::tempdir().expect("another state directory");
        let paths = DaemonPaths::resolve(Some(state.path())).expect("paths");
        let mounts = Arc::new(Mounts::new(Arc::new(NoKernel), paths));
        let (stop, _stopped) = watch::channel(false);
        let (progress, _progress) = mpsc::unbounded_channel();
        let scope = |state_directory: &Path| DaemonScope {
            state_directory: state_directory.to_path_buf(),
        };

        for request in [
            DaemonRequest::List(scope(other.path())),
            DaemonRequest::Shutdown(scope(other.path())),
        ] {
            let refused = respond(request, &mounts, &stop, &progress).await;
            assert!(
                matches!(
                    &refused,
                    DaemonResponse::Error(failure)
                        if failure.code == DaemonErrorCode::StateDirectoryMismatch
                ),
                "{refused:?}"
            );
        }
        assert!(
            !*stop.borrow(),
            "the refused shutdown doesn't stop the daemon"
        );
        let listed = respond(
            DaemonRequest::List(scope(state.path())),
            &mounts,
            &stop,
            &progress,
        )
        .await;
        assert_eq!(listed, DaemonResponse::Mounts { mounts: Vec::new() });
    }
}
