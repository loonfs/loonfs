use super::request::{
    build_client_mutation_request_from_state, build_inode_mutation_request_from_state,
};
use super::*;
use crate::state_db::SqliteStateDb;
use loon_types::{ClientMutationRequest, ClientMutationResponse, InodeId, NamespaceId};

pub fn dispatch_client_mutation_from_state<F>(
    db: &mut SqliteStateDb,
    client_file_id: &ClientFileId,
    created_at_ms: u64,
    dispatch: F,
) -> Result<DispatchedClientMutation, DispatchClientMutationError>
where
    F: FnOnce(&ClientMutationRequest) -> Result<ClientMutationResponse, String>,
{
    let pending = match db.load_pending_client_mutation_for_client_file(client_file_id)? {
        Some(existing) => existing,
        None => {
            let client_request_id = db.allocate_client_request_id()?;
            let request =
                build_client_mutation_request_from_state(db, &client_request_id, client_file_id)?;
            db.record_pending_client_mutation(client_file_id, &request, created_at_ms)?
        }
    };
    let request = pending.request.clone();
    let response =
        dispatch(&request).map_err(|message| DispatchClientMutationError::DispatchFailed {
            client_request_id: request.client_request_id.clone(),
            message,
        })?;
    let bound_identity = db.apply_client_mutation_response(&response)?;

    Ok(DispatchedClientMutation {
        pending,
        request,
        response,
        bound_identity,
    })
}

pub fn dispatch_inode_mutation_from_state<F>(
    db: &mut SqliteStateDb,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    created_at_ms: u64,
    dispatch: F,
) -> Result<DispatchedInodeMutation, DispatchInodeMutationError>
where
    F: FnOnce(&ClientMutationRequest) -> Result<ClientMutationResponse, String>,
{
    let pending = match db.load_pending_inode_mutation_for_inode(namespace_id, inode_id)? {
        Some(existing) => existing,
        None => {
            let client_request_id = db.allocate_client_request_id()?;
            let request = build_inode_mutation_request_from_state(
                db,
                &client_request_id,
                namespace_id,
                inode_id,
            )?;
            db.record_pending_inode_mutation(namespace_id, inode_id, &request, created_at_ms)?
        }
    };
    let request = pending.request.clone();
    let response =
        dispatch(&request).map_err(|message| DispatchInodeMutationError::DispatchFailed {
            client_request_id: request.client_request_id.clone(),
            message,
        })?;
    let applied = db.apply_inode_mutation_response(&response)?;

    Ok(DispatchedInodeMutation {
        pending,
        request,
        response,
        applied,
    })
}
