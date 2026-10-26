//! Unmounting: detaching a directory, and releasing its mount once the
//! kernel does.

use super::{blocking, busy, Mounts, ReadyMount, Slot};
use crate::clock;
use crate::daemon::kernel::KernelMount;
use crate::daemon::protocol::{
    DaemonErrorCode, DaemonFailure, StateDisposal, UnmountOutcome, UnmountRequest, UnmountedReport,
};
use crate::engine::Engine;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::oneshot;

/// How long an `unmount` waits for the kernel to release a detached mount
/// before it reports that programs still have files open in it.
const RELEASE_WAIT_BUDGET_MS: u64 = 5_000;

impl Mounts {
    /// Unmounts a directory: detaches it, and releases the mount once the
    /// kernel does. A directory without a mount is left as it is, apart
    /// from detaching a mount that a stopped daemon left.
    pub(crate) async fn unmount(
        self: &Arc<Self>,
        request: UnmountRequest,
    ) -> Result<UnmountedReport, DaemonFailure> {
        let UnmountRequest {
            directory, force, ..
        } = request;
        let state_directory = self.paths.mount_state_directory(&directory);
        let unmounted = |state| UnmountedReport {
            directory: directory.clone(),
            outcome: UnmountOutcome::Unmounted { state },
            state_directory: state_directory.clone(),
        };
        let state = if force {
            StateDisposal::Kept
        } else {
            StateDisposal::Removed
        };
        let taken = self.update(|table| match table.remove(&directory) {
            Some(Slot::Ready(ready)) => {
                table.insert(
                    directory.clone(),
                    Slot::Releasing {
                        identity: ready.identity.clone(),
                        state,
                    },
                );
                Ok(Some(ready))
            }
            Some(Slot::Releasing { identity, state }) => {
                table.insert(directory.clone(), Slot::Releasing { identity, state });
                Err(Ok(state))
            }
            Some(slot) => {
                let failure = busy(&directory, &slot);
                table.insert(directory.clone(), slot);
                Err(Err(failure))
            }
            None => Ok(None),
        });
        let ready = match taken {
            Ok(Some(ready)) => ready,
            Ok(None) => {
                let kernel = self.kernel.clone();
                let stale_directory = directory.clone();
                let outcome = blocking(move || kernel.detach_stale(&stale_directory))
                    .await
                    .map_err(|error| unmount_failed(&directory, &error))?;
                return Ok(UnmountedReport {
                    directory: directory.clone(),
                    outcome: outcome.into(),
                    state_directory: state_directory.clone(),
                });
            }
            Err(Ok(state)) => return Ok(unmounted(state)),
            Err(Err(failure)) => return Err(failure),
        };
        let ReadyMount {
            identity,
            engine,
            mut kernel_mount,
        } = *ready;
        let (kernel_mount, detached) = blocking(move || {
            let detached = kernel_mount.detach();
            (kernel_mount, detached)
        })
        .await;
        if let Err(error) = detached {
            self.update(|table| {
                table.insert(
                    directory.clone(),
                    Slot::Ready(Box::new(ReadyMount {
                        identity,
                        engine,
                        kernel_mount,
                    })),
                )
            });
            return Err(unmount_failed(&directory, &error));
        }
        let released = self.release(directory.clone(), engine, kernel_mount, state);
        let state = match (state, wait_for_release(released).await) {
            (StateDisposal::Removed, Release::Pending) => StateDisposal::RemovedAfterRelease,
            (state, _) => state,
        };
        Ok(unmounted(state))
    }

    /// Starts a thread that waits for the kernel to release a detached
    /// mount, then stops its engine, removes its state unless `state` keeps
    /// it, and removes the directory from the table.
    ///
    /// The thread isn't one of the runtime's blocking threads, because the
    /// release can wait for as long as a program keeps a file open, and the
    /// runtime waits for its blocking threads when it stops.
    fn release(
        self: &Arc<Self>,
        directory: PathBuf,
        engine: Arc<Engine>,
        kernel_mount: Box<dyn KernelMount>,
        state: StateDisposal,
    ) -> oneshot::Receiver<()> {
        let (released, receiver) = oneshot::channel();
        let mounts = self.clone();
        let state_directory = self.paths.mount_state_directory(&directory);
        let released_directory = directory.clone();
        let release = move || {
            kernel_mount.wait();
            // A request that's still running can keep a reference to the
            // engine, so the engine is stopped before its state is removed.
            engine.stop();
            drop(engine);
            if state != StateDisposal::Kept {
                remove_state(&state_directory);
            }
            mounts.update(|table| table.remove(&released_directory));
            let _ = released.send(());
        };
        if let Err(error) = std::thread::Builder::new()
            .name("loonfs-mount-release".to_owned())
            .spawn(release)
        {
            // The mount stays detached, its session keeps serving the files
            // that programs have open, and its state stays on disk.
            tracing::error!(%error, "starting a mount's release thread failed");
            self.update(|table| table.remove(&directory));
        }
        receiver
    }

    /// Detaches every mount and keeps its state, as the daemon stops.
    pub(crate) async fn detach_all(self: &Arc<Self>) {
        let directories: Vec<PathBuf> = self.table().keys().cloned().collect();
        for directory in directories {
            let request = UnmountRequest {
                directory: directory.clone(),
                state_directory: self.paths.state_directory().to_path_buf(),
                force: true,
            };
            if let Err(failure) = self.unmount(request).await {
                tracing::warn!(
                    directory = %directory.display(),
                    message = %failure.message,
                    "detaching a mount as the daemon stops failed"
                );
            }
        }
    }
}

/// Whether the kernel released a detached mount within the wait budget.
enum Release {
    Done,
    Pending,
}

async fn wait_for_release(released: oneshot::Receiver<()>) -> Release {
    tokio::select! {
        _ = released => Release::Done,
        () = clock::delay(RELEASE_WAIT_BUDGET_MS) => Release::Pending,
    }
}

fn unmount_failed(directory: &Path, error: &std::io::Error) -> DaemonFailure {
    DaemonFailure::new(
        DaemonErrorCode::UnmountFailed,
        format!("detaching `{}` failed: {error}", directory.display()),
    )
}

fn remove_state(state_directory: &Path) {
    match std::fs::remove_dir_all(state_directory) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => tracing::warn!(
            path = %state_directory.display(),
            %error,
            "removing an unmounted mount's state failed"
        ),
    }
}
