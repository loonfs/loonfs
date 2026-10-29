//! The `gc` maintenance job, which runs the core collector and then the grep index's collector.

use loonfs::{GcOptions, Maintenance, SharedObjectStore};
use loonfs_grep::{GrepError, GrepWorker};
use loonfs_types::{GcResponse, NamespaceId};

/// Collects one namespace with the core collector, and then with the grep index's collector when
/// the host maintains a grep index.
///
/// The two collectors delete separate objects, so the grep collector runs even when the core
/// collector fails, and a call where both fail returns the core collector's error. That way a
/// namespace that the core collector reports as missing still has its grep objects collected.
/// The `gc` maintenance job and the server's sweep both collect through this function, so they
/// run the same collectors.
pub async fn gc_namespace(
    maintenance: &Maintenance,
    grep_worker: Option<&GrepWorker<SharedObjectStore>>,
    namespace_id: &NamespaceId,
    options: &GcOptions,
) -> Result<GcResponse, GrepError> {
    let core = maintenance.gc_with_options(namespace_id, options).await;
    let Some(worker) = grep_worker else {
        return Ok(core?);
    };
    let grep = worker.gc_namespace(namespace_id).await;
    let mut response = core?;
    response.extensions.grep = Some(grep?);
    Ok(response)
}
