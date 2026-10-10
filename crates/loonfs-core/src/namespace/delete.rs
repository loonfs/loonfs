//! Namespace deletion status published as successive manifests.

use crate::error::{CoreError, Result};
use crate::manifest::publish::{update_manifest, ManifestChange};
use crate::namespace::read_anchor::load_read_anchor;
use crate::namespace::writer_epoch::ensure_writer_not_fenced;
use crate::options::DeleteNamespaceOptions;
use crate::time::Deadline;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::control::{AcquiredWriter, NamespaceStatus};
use loonfs_types::{DeleteNamespaceResponse, NamespaceId};

#[allow(
    clippy::too_many_arguments,
    reason = "deletion carries both shared memory pools through its fold"
)]
pub(crate) async fn delete_namespace<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    options: DeleteNamespaceOptions,
    acquired_writer: AcquiredWriter,
    context: &crate::context::MutationContext,
    deadline: &Deadline,
    pool: std::sync::Arc<crate::cache::ReadWorkingMemory>,
    merge_memory: &tokio::sync::Semaphore,
) -> Result<DeleteNamespaceResponse> {
    update_manifest(store, namespace_id, deadline, |mut payload| {
        let acquired_writer = &acquired_writer;
        let pool = std::sync::Arc::clone(&pool);
        async move {
            let anchor = load_read_anchor(store, namespace_id).await?;
            let head = &anchor.read_state;
            super::control::ensure_namespace_live(head)?;
            ensure_writer_not_fenced(head, acquired_writer)?;
            if let Some(expected) = options.expected_head_seq {
                if head.seq != expected {
                    return Err(CoreError::StaleHeadPrecondition {
                        precondition_index: None,
                        expected,
                        actual: head.seq,
                    });
                }
            }
            if payload.folded_wal_no < head.wal_no {
                deadline.ensure_metadata_publication_budget(namespace_id)?;
                crate::manifest::fold_wal_with_deadline(
                    store,
                    namespace_id,
                    deadline,
                    crate::manifest::MetadataLsmPolicy::default(),
                    pool,
                    merge_memory,
                )
                .await?;
                return Ok(ManifestChange::Again);
            }
            payload.status = NamespaceStatus::Deleted {
                deleted_at_ms: context.now_ms,
            };
            let response = DeleteNamespaceResponse {
                namespace_id: namespace_id.clone(),
                head_seq: payload.head_seq,
            };
            Ok(ManifestChange::Next(Box::new(payload), response))
        }
    })
    .await
    .map(|(result, _, _)| result)
}
