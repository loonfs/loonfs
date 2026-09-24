//! Namespace deletion status published as successive manifests.

use crate::checkpoint::publish::{update_manifest, ManifestChange};
use crate::error::{CoreError, Result};
use crate::namespace::read_anchor::load_read_anchor;
use crate::namespace::writer_epoch::ensure_writer_not_fenced;
use crate::options::DeleteNamespaceOptions;
use crate::time::MonotonicTimer;
use loonfs_api::wire::control::{AcquiredWriter, NamespaceStatus};
use loonfs_api::{DeleteNamespaceResponse, NamespaceId};
use loonfs_objectstore::ObjectStore;

enum DeleteManifest {
    FoldFirst,
    Deleted(DeleteNamespaceResponse),
}

pub(crate) async fn delete_namespace<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    options: DeleteNamespaceOptions,
    acquired_writer: AcquiredWriter,
    context: &crate::context::MutationContext,
    timer: &dyn MonotonicTimer,
    started_ms: u64,
) -> Result<DeleteNamespaceResponse> {
    loop {
        let anchor = load_read_anchor(store, namespace_id).await?;
        let head = &anchor.read_state;
        super::control::ensure_namespace_live(head)?;
        ensure_writer_not_fenced(head, &acquired_writer)?;
        if let Some(expected) = options.expected_head_seq {
            if head.seq != expected {
                return Err(CoreError::StaleHeadPrecondition {
                    precondition_index: None,
                    expected,
                    actual: head.seq,
                });
            }
        }
        let result = update_manifest(store, namespace_id, timer, started_ms, |mut payload| {
            let acquired_writer = &acquired_writer;
            async move {
                let current = super::state::NamespaceReadState::from(&payload);
                super::control::ensure_namespace_live(&current)?;
                ensure_writer_not_fenced(&current, acquired_writer)?;
                if payload.folded_wal_no < head.wal_no {
                    return Ok(ManifestChange::Finished(DeleteManifest::FoldFirst));
                }
                if let Some(expected) = options.expected_head_seq {
                    if payload.head_seq != expected {
                        return Err(CoreError::StaleHeadPrecondition {
                            precondition_index: None,
                            expected,
                            actual: payload.head_seq,
                        });
                    }
                }
                payload.status = NamespaceStatus::Deleted {
                    deleted_at_ms: context.now_ms,
                };
                let response = DeleteNamespaceResponse {
                    namespace_id: namespace_id.clone(),
                    head_seq: payload.head_seq,
                };
                Ok(ManifestChange::Next(
                    Box::new(payload),
                    DeleteManifest::Deleted(response),
                ))
            }
        })
        .await?;
        if let DeleteManifest::Deleted(response) = result {
            return Ok(response);
        }
        crate::checkpoint::ensure_metadata_publication_budget(timer, started_ms, namespace_id)?;
        crate::checkpoint::flush_wal(store, namespace_id).await?;
    }
}
