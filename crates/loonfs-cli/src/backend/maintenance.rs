//! Runtime maintenance owned by an embedded CLI invocation.

use super::{MaintenanceDrainProgress, MaintenanceKeyProgress, StepBudget};
use crate::error::CliError;
use crate::resolve::ResolvedTarget;
use loonfs::{
    FsMaintenance, FsWriter, MaintenanceAssignment, MaintenanceHandle, MaintenanceJob,
    MaintenanceJobId, MaintenanceRegistry, MaintenanceRunner, SharedObjectStore,
};
use loonfs_api::NamespaceId;
use loonfs_grep::GrepWorker;
use loonfs_objectstore::timing::{MonotonicTimer, StdMonotonicTimer};
use std::sync::Arc;

pub(crate) struct MaintenanceHost {
    pub(crate) writer: FsWriter,
    pub(crate) maintenance: FsMaintenance,
    pub(crate) jobs: MaintenanceRegistry,
    pub(crate) runner: MaintenanceRunner,
    pub(crate) grep_worker: GrepWorker<SharedObjectStore>,
}

type HostedJob = (MaintenanceJobId, Arc<dyn MaintenanceJob>);

impl ResolvedTarget {
    fn maintenance_host(&self) -> Result<&MaintenanceHost, CliError> {
        self.maintenance.as_ref().ok_or_else(|| CliError::new(
            loonfs_api::ErrorCode::NotSupported.as_str(),
            "`maintenance loop` requires an embedded profile because remote servers run their \
             own maintenance; use `loonfs maintenance metadata` for one pass or `loonfs maintenance \
             index status` to inspect the index",
        ))
    }

    pub(crate) async fn host_maintenance(
        &self,
        namespaces: &[NamespaceId],
        jobs: &[MaintenanceJobId],
        poll_interval_ms: Option<u64>,
        shutdown: impl std::future::Future<Output = ()>,
    ) -> Result<(), CliError> {
        self.maintenance_host()?
            .host_maintenance(namespaces, jobs, poll_interval_ms, shutdown)
            .await
    }

    pub(crate) async fn drain_maintenance(
        &self,
        namespaces: &[NamespaceId],
        jobs: &[MaintenanceJobId],
        budget: StepBudget,
    ) -> Result<MaintenanceDrainProgress, CliError> {
        self.maintenance_host()?
            .drain_maintenance(namespaces, jobs, budget)
            .await
    }
}

impl MaintenanceHost {
    fn hosted_jobs(&self, jobs: &[MaintenanceJobId]) -> Result<Vec<HostedJob>, CliError> {
        jobs.iter()
            .map(|job| {
                let executor = self.jobs.get(*job).ok_or_else(|| {
                    CliError::runtime_error(format!(
                        "no maintenance job is registered under `{job}`"
                    ))
                })?;
                Ok((*job, executor))
            })
            .collect()
    }

    pub(super) async fn host_maintenance(
        &self,
        namespaces: &[NamespaceId],
        jobs: &[MaintenanceJobId],
        poll_interval_ms: Option<u64>,
        shutdown: impl std::future::Future<Output = ()>,
    ) -> Result<(), CliError> {
        let hosted = self.hosted_jobs(jobs)?;
        let interval_ms = poll_interval_ms.unwrap_or(ASSIGNMENT_INTERVAL_MS);
        let maintenance = self.runner.handle();
        assign(&maintenance, &hosted, namespaces);
        let mut shutdown = std::pin::pin!(shutdown);
        loop {
            tokio::select! {
                () = &mut shutdown => break,
                () = rest_between_assignments(interval_ms) => {
                    assign(&maintenance, &hosted, namespaces);
                }
            }
        }
        let writer = self.writer.shutdown().await;
        let runner = self.runner.shutdown().await;
        writer.and(runner).map_err(CliError::from)
    }

    pub(super) async fn drain_maintenance(
        &self,
        namespaces: &[NamespaceId],
        jobs: &[MaintenanceJobId],
        budget: StepBudget,
    ) -> Result<MaintenanceDrainProgress, CliError> {
        let hosted = self.hosted_jobs(jobs)?;
        self.runner.shutdown().await.map_err(CliError::from)?;
        self.writer.shutdown().await.map_err(CliError::from)?;
        let timer = StdMonotonicTimer::default();
        let started_ms = timer.monotonic_now_ms();
        let mut steps = 0;
        let mut keys = Vec::with_capacity(hosted.len() * namespaces.len());
        for (job, _executor) in &hosted {
            for namespace_id in namespaces {
                let mut key = MaintenanceKeyProgress {
                    job: *job,
                    namespace_id: namespace_id.clone(),
                    steps: 0,
                    conclusion: None,
                };
                while !budget.spent(steps, timer.monotonic_now_ms().saturating_sub(started_ms)) {
                    let result = self
                        .jobs
                        .execute(MaintenanceAssignment {
                            namespace_id: namespace_id.clone(),
                            job: *job,
                        })
                        .await
                        .map_err(CliError::from)?;
                    steps += 1;
                    key.steps += 1;
                    key.conclusion = Some(result.conclusion);
                    if key.settled() {
                        break;
                    }
                }
                keys.push(key);
            }
        }
        Ok(MaintenanceDrainProgress { keys, steps })
    }
}

const ASSIGNMENT_INTERVAL_MS: u64 = 60_000;

fn assign(maintenance: &MaintenanceHandle, jobs: &[HostedJob], namespaces: &[NamespaceId]) {
    for (job, _) in jobs {
        for namespace_id in namespaces {
            maintenance.nudge(*job, namespace_id);
        }
    }
}

#[allow(clippy::disallowed_methods)]
async fn rest_between_assignments(interval_ms: u64) {
    // This host timer schedules observations without changing durable validity.
    tokio::time::sleep(std::time::Duration::from_millis(interval_ms)).await;
}

#[cfg(test)]
#[path = "maintenance_tests.rs"]
mod tests;
