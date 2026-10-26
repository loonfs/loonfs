//! The daemon's mounts, by directory: admitting and mounting them, reporting
//! their state, and releasing them.

mod unmount;

use super::kernel::{Kernel, KernelMount};
use super::paths::DaemonPaths;
use super::protocol::{
    DaemonErrorCode, DaemonFailure, MountListing, MountOutcome, MountPhase, MountRequest,
    MountStatusReport, MountStep, MountedReport, RefreshRequest, StateDisposal,
};
use crate::config::{MountAccess, MountConfig};
use crate::engine::Engine;
use crate::error::MountError;
use loonfs_client::{Client, ClientConfig};
use loonfs_types::{AbsolutePath, ChangeSeq, NamespaceId};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, watch};

/// What a mount shows, which a repeated `mount` request has to match.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Identity {
    namespace_id: NamespaceId,
    path: AbsolutePath,
    access: MountAccess,
}

/// One directory's entry in the table.
enum Slot {
    /// A `mount` request is admitting the mount.
    Starting(Identity),
    /// The directory is mounted.
    Ready(Box<ReadyMount>),
    /// The directory is detached, and a release thread waits for the kernel
    /// to release it.
    Releasing {
        identity: Identity,
        state: StateDisposal,
    },
}

impl Slot {
    fn identity(&self) -> &Identity {
        match self {
            Self::Starting(identity) | Self::Releasing { identity, .. } => identity,
            Self::Ready(ready) => &ready.identity,
        }
    }

    fn phase(&self) -> MountPhase {
        match self {
            Self::Starting(_) => MountPhase::Starting,
            Self::Ready(_) => MountPhase::Ready,
            Self::Releasing { .. } => MountPhase::Releasing,
        }
    }
}

struct ReadyMount {
    identity: Identity,
    engine: Arc<Engine>,
    kernel_mount: Box<dyn KernelMount>,
}

/// The daemon's mounts.
pub(crate) struct Mounts {
    kernel: Arc<dyn Kernel>,
    paths: DaemonPaths,
    table: Mutex<BTreeMap<PathBuf, Slot>>,
    /// The number of directories in the table, which the server watches to
    /// stop once the daemon has been idle long enough.
    directories: watch::Sender<usize>,
}

impl Mounts {
    pub(crate) fn new(kernel: Arc<dyn Kernel>, paths: DaemonPaths) -> Self {
        Self {
            kernel,
            paths,
            table: Mutex::new(BTreeMap::new()),
            directories: watch::Sender::new(0),
        }
    }

    /// Refuses a request for another state directory than the daemon's.
    pub(crate) fn check_state_directory(&self, requested: &Path) -> Result<(), DaemonFailure> {
        if self.paths.is_state_directory(requested) {
            return Ok(());
        }
        Err(DaemonFailure::new(
            DaemonErrorCode::StateDirectoryMismatch,
            format!(
                "the daemon keeps its state in `{}`, not `{}`, so pass the same `--state-dir`",
                self.paths.state_directory().display(),
                requested.display()
            ),
        ))
    }

    /// Returns a receiver of the number of directories in the table.
    pub(crate) fn watch_directories(&self) -> watch::Receiver<usize> {
        self.directories.subscribe()
    }

    fn table(&self) -> std::sync::MutexGuard<'_, BTreeMap<PathBuf, Slot>> {
        self.table
            .lock()
            .expect("the mount table lock should not be poisoned")
    }

    fn update<T>(&self, change: impl FnOnce(&mut BTreeMap<PathBuf, Slot>) -> T) -> T {
        let mut table = self.table();
        let result = change(&mut table);
        self.directories.send_replace(table.len());
        result
    }

    /// Admits and mounts the mount that `request` describes, or confirms that
    /// the directory already has it. `progress` receives each step as it
    /// starts.
    pub(crate) async fn mount(
        &self,
        request: MountRequest,
        progress: &(dyn Fn(MountStep) + Send + Sync),
    ) -> Result<MountedReport, DaemonFailure> {
        let identity = Identity {
            namespace_id: request.namespace_id.clone(),
            path: request.path.clone(),
            access: request.access,
        };
        let directory = request.directory.clone();
        let existing = self.update(|table| match table.get(&directory) {
            Some(Slot::Ready(ready)) if ready.identity == identity => {
                Some(Ok(report(&directory, &ready.engine)))
            }
            Some(slot) => Some(Err(busy(&directory, slot))),
            None => {
                table.insert(directory.clone(), Slot::Starting(identity.clone()));
                None
            }
        });
        if let Some(existing) = existing {
            return existing.map(|mount| MountedReport {
                outcome: MountOutcome::AlreadyMounted,
                mount,
            });
        }
        match self.start(request, progress).await {
            Ok((engine, kernel_mount)) => {
                let mount = report(&directory, &engine);
                self.update(|table| {
                    table.insert(
                        directory,
                        Slot::Ready(Box::new(ReadyMount {
                            identity,
                            engine,
                            kernel_mount,
                        })),
                    )
                });
                Ok(MountedReport {
                    outcome: MountOutcome::Mounted,
                    mount,
                })
            }
            Err(failure) => {
                self.update(|table| table.remove(&directory));
                Err(failure)
            }
        }
    }

    async fn start(
        &self,
        request: MountRequest,
        progress: &(dyn Fn(MountStep) + Send + Sync),
    ) -> Result<(Arc<Engine>, Box<dyn KernelMount>), DaemonFailure> {
        let directory = request.directory.clone();
        progress(MountStep::CheckingKernel);
        let kernel = self.kernel.clone();
        let checked_directory = directory.clone();
        blocking(move || {
            kernel.check_host()?;
            kernel.detach_stale(&checked_directory).map(drop)
        })
        .await
        .map_err(|error| mount_failed(format!("host can't serve the mount: {error}")))?;

        let client = client(&request)?;
        let mut config = MountConfig::new(
            request.namespace_id,
            request.path,
            request.access,
            self.paths.mount_state_directory(&directory),
        );
        request.options.apply(&mut config);
        let engine = Arc::new(admit(client, config, progress).await?);

        progress(MountStep::Mounting);
        let kernel = self.kernel.clone();
        let mounted_engine = engine.clone();
        let mounted_directory = directory.clone();
        let mounted = blocking(move || kernel.mount(mounted_engine, &mounted_directory)).await;
        match mounted {
            Ok(kernel_mount) => Ok((engine, kernel_mount)),
            Err(error) => {
                // Stopping the engine joins its control thread.
                blocking(move || drop(engine)).await;
                Err(mount_failed(format!(
                    "mounting `{}` failed: {error}",
                    directory.display()
                )))
            }
        }
    }

    /// Reads a mount's change feed now, and waits until it has applied
    /// `at_least`, or one poll when that isn't set, for at most the mount's
    /// sync timeout.
    pub(crate) async fn refresh(
        &self,
        request: RefreshRequest,
    ) -> Result<ChangeSeq, DaemonFailure> {
        let engine = self.ready_engine(&request.directory)?;
        let timeout = Duration::from_millis(engine.config().sync_timeout_ms);
        engine
            .refresh_at_least(request.at_least.unwrap_or(ChangeSeq(0)), timeout)
            .await
            .map_err(|error| refresh_failed(&request.directory, error))?;
        Ok(engine.status().applied_seq)
    }

    /// Returns the state of the mount at `directory`, or of every ready
    /// mount.
    pub(crate) fn status(
        &self,
        directory: Option<&Path>,
    ) -> Result<Vec<MountStatusReport>, DaemonFailure> {
        if let Some(directory) = directory {
            let engine = self.ready_engine(directory)?;
            return Ok(vec![report(directory, &engine)]);
        }
        let engines: Vec<(PathBuf, Arc<Engine>)> = self
            .table()
            .iter()
            .filter_map(|(directory, slot)| match slot {
                Slot::Ready(ready) => Some((directory.clone(), ready.engine.clone())),
                Slot::Starting(_) | Slot::Releasing { .. } => None,
            })
            .collect();
        Ok(engines
            .iter()
            .map(|(directory, engine)| report(directory, engine))
            .collect())
    }

    /// Lists every directory in the table.
    pub(crate) fn list(&self) -> Vec<MountListing> {
        self.table()
            .iter()
            .map(|(directory, slot)| MountListing {
                directory: directory.clone(),
                namespace_id: slot.identity().namespace_id.clone(),
                path: slot.identity().path.clone(),
                phase: slot.phase(),
            })
            .collect()
    }

    fn ready_engine(&self, directory: &Path) -> Result<Arc<Engine>, DaemonFailure> {
        match self.table().get(directory) {
            Some(Slot::Ready(ready)) => Ok(ready.engine.clone()),
            Some(slot) => Err(busy(directory, slot)),
            None => Err(DaemonFailure::new(
                DaemonErrorCode::NotMounted,
                format!("`{}` has no mount", directory.display()),
            )),
        }
    }
}

fn report(directory: &Path, engine: &Engine) -> MountStatusReport {
    MountStatusReport {
        directory: directory.to_path_buf(),
        status: engine.status(),
    }
}

fn busy(directory: &Path, slot: &Slot) -> DaemonFailure {
    let identity = slot.identity();
    let what = match slot.phase() {
        MountPhase::Starting => "is being mounted",
        MountPhase::Ready => "is mounted",
        MountPhase::Releasing => "is being released, because programs still have files open in it",
    };
    DaemonFailure::new(
        DaemonErrorCode::DirectoryBusy,
        format!(
            "`{}` {what} with `{}:{}`",
            directory.display(),
            identity.namespace_id,
            identity.path
        ),
    )
}

/// Admits a mount with the engine, in a task of its own, and passes each
/// step that the engine reports to `progress` as it arrives.
async fn admit(
    client: Client,
    config: MountConfig,
    progress: &(dyn Fn(MountStep) + Send + Sync),
) -> Result<Engine, DaemonFailure> {
    let (sender, mut steps) = mpsc::unbounded_channel();
    let mut admission = tokio::spawn(Engine::mount(client, config, Some(sender)));
    let admitted = loop {
        tokio::select! {
            biased;
            Some(step) = steps.recv() => progress(MountStep::Engine(step)),
            admitted = &mut admission => break admitted,
        }
    };
    while let Ok(step) = steps.try_recv() {
        progress(MountStep::Engine(step));
    }
    admitted
        .expect("a mount's admission should not panic")
        .map_err(|error| mount_failed(error.to_string()))
}

/// Returns why a refresh failed, with a code of its own for a timeout and
/// for a sequence that the namespace hasn't committed.
fn refresh_failed(directory: &Path, error: MountError) -> DaemonFailure {
    let directory = directory.display();
    match error {
        MountError::RefreshTimedOut {
            seq,
            applied_seq,
            timeout_ms,
        } => DaemonFailure::new(
            DaemonErrorCode::RefreshTimedOut,
            format!(
                "`{directory}` didn't apply sequence {} within its sync timeout of {timeout_ms} \
                 ms, and has applied sequence {}",
                seq.0, applied_seq.0
            ),
        ),
        MountError::SequenceNotCommitted { seq, head_seq } => DaemonFailure::new(
            DaemonErrorCode::SequenceNotCommitted,
            format!(
                "sequence {} isn't committed: the namespace's head is sequence {}",
                seq.0, head_seq.0
            ),
        ),
        other => DaemonFailure::new(DaemonErrorCode::EngineFailed, other.to_string()),
    }
}

fn mount_failed(message: String) -> DaemonFailure {
    DaemonFailure::new(DaemonErrorCode::MountFailed, message)
}

/// Builds the client that a mount's engine uses, from the profile that the
/// request sent.
fn client(request: &MountRequest) -> Result<Client, DaemonFailure> {
    let profile = &request.profile;
    let client = Client::new(ClientConfig {
        server_url: profile.server_url.clone(),
        auth_token: profile.auth_token.clone(),
        request_timeout_ms: None,
        disable_transient_retry: false,
        ca_cert_path: profile.ca_cert_path.clone(),
    })
    .map_err(|error| {
        DaemonFailure::new(
            DaemonErrorCode::InvalidRequest,
            format!("the mount's profile is invalid: {error}"),
        )
    })?;
    Ok(match &profile.subject {
        Some(subject) => client.with_subject(subject.clone()),
        None => client,
    })
}

/// Runs `work` on one of the runtime's blocking threads.
async fn blocking<T: Send + 'static>(work: impl FnOnce() -> T + Send + 'static) -> T {
    tokio::task::spawn_blocking(work)
        .await
        .expect("the daemon's blocking work should not panic")
}
