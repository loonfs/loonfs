//! Server ownership and shutdown for event-driven per-namespace grep drivers.

use loonfs::{NamespaceId, RuntimeError, SharedObjectStore};
use loonfs_grep::{
    GramIndexBuildPolicy, GrepDriver, GrepDriverHandle, GrepDriverParked, GrepDriverTask,
    GrepStepLimiter, GrepWorker,
};
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub(crate) struct GrepDrivers {
    inner: Arc<GrepDriversInner>,
}

struct GrepDriversInner {
    worker: GrepWorker<SharedObjectStore>,
    policy: GramIndexBuildPolicy,
    step_limiter: GrepStepLimiter,
    runtime: tokio::runtime::Handle,
    tasks: Mutex<HashMap<NamespaceId, GrepDriverTask>>,
    finished: Mutex<Vec<GrepDriverTask>>,
}

impl GrepDrivers {
    pub(crate) fn new(
        worker: GrepWorker<SharedObjectStore>,
        policy: GramIndexBuildPolicy,
        max_concurrent_steps: NonZeroUsize,
    ) -> Result<Self, crate::ServerConfigError> {
        let runtime = tokio::runtime::Handle::try_current().map_err(|error| {
            crate::ServerConfigError::InvalidField {
                field: "runtime",
                reason: format!("server app must be built inside a Tokio runtime: {error}"),
            }
        })?;
        Ok(Self {
            inner: Arc::new(GrepDriversInner {
                worker,
                policy,
                step_limiter: GrepStepLimiter::new(max_concurrent_steps),
                runtime,
                tasks: Mutex::new(HashMap::new()),
                finished: Mutex::new(Vec::new()),
            }),
        })
    }

    pub(crate) fn start(&self, namespace_id: &NamespaceId) -> GrepDriverHandle {
        let mut tasks = self.inner.tasks.lock().expect("grep drivers lock poisoned");
        if let Some(task) = tasks.get(namespace_id) {
            if !task.is_finished() {
                let handle = task.handle();
                handle.nudge();
                return handle;
            }
        }
        if let Some(finished) = tasks.remove(namespace_id) {
            self.inner
                .finished
                .lock()
                .expect("finished grep drivers lock poisoned")
                .push(finished);
        }
        let task = GrepDriver::new(
            self.inner.worker.clone(),
            namespace_id.clone(),
            self.inner.policy,
            self.inner.step_limiter.clone(),
        )
        .spawn_on(&self.inner.runtime);
        let handle = task.handle();
        tasks.insert(namespace_id.clone(), task);
        handle
    }

    /// Nudge is deliberately synchronous and non-blocking so the runtime's
    /// publish observer can call it inline.
    pub(crate) fn nudge_existing(&self, namespace_id: &NamespaceId) {
        if let Some(task) = self
            .inner
            .tasks
            .lock()
            .expect("grep drivers lock poisoned")
            .get(namespace_id)
            .filter(|task| !task.is_finished())
        {
            task.handle().nudge();
        }
    }

    pub(crate) async fn stop(&self, namespace_id: &NamespaceId) -> Result<(), RuntimeError> {
        let task = self
            .inner
            .tasks
            .lock()
            .expect("grep drivers lock poisoned")
            .remove(namespace_id);
        if let Some(task) = task {
            task.shutdown().await.map_err(driver_join_error)?;
        }
        Ok(())
    }

    pub(crate) fn is_running(&self, namespace_id: &NamespaceId) -> bool {
        self.inner
            .tasks
            .lock()
            .expect("grep drivers lock poisoned")
            .get(namespace_id)
            .is_some_and(|task| !task.is_finished())
    }

    pub(crate) async fn wait_for_quiescence(
        &self,
        namespace_id: &NamespaceId,
    ) -> Option<GrepDriverParked> {
        let handle = self
            .inner
            .tasks
            .lock()
            .expect("grep drivers lock poisoned")
            .get(namespace_id)
            .map(GrepDriverTask::handle)?;
        handle.wait_for_quiescence().await
    }

    pub(crate) async fn shutdown(&self) -> Result<(), RuntimeError> {
        let tasks =
            std::mem::take(&mut *self.inner.tasks.lock().expect("grep drivers lock poisoned"));
        for (_, task) in tasks {
            task.shutdown().await.map_err(driver_join_error)?;
        }
        let finished = std::mem::take(
            &mut *self
                .inner
                .finished
                .lock()
                .expect("finished grep drivers lock poisoned"),
        );
        for task in finished {
            task.shutdown().await.map_err(driver_join_error)?;
        }
        Ok(())
    }
}

fn driver_join_error(error: tokio::task::JoinError) -> RuntimeError {
    RuntimeError::RuntimeTask(format!("grep namespace driver failed: {error}"))
}
