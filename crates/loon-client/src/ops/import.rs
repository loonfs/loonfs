use crate::state_db::{AppliedRemoteObservation, SqliteStateDb, StateDbError};
use loon_types::server::ServerTransport;
use loon_types::{ChangeSeq, NamespaceId, ObservedRemoteInode};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthoritativeObservationImportReport {
    pub namespace_id: NamespaceId,
    pub authoritative_head_seq: ChangeSeq,
    pub translated_observation_count: usize,
    pub bound_local_only_count: usize,
    pub converged_bound_inode_count: usize,
    pub discovered_remote_only_count: usize,
    pub recorded_conflict_or_error_count: usize,
    pub updated_bound_remote_state_count: usize,
    pub ignored_stale_count: usize,
    pub ignored_unmatched_count: usize,
}

#[derive(Debug, Error)]
pub enum AuthoritativeObservationImportError {
    #[error("failed to load authoritative namespace state summary: {0}")]
    LoadSummary(anyhow::Error),
    #[error("failed to translate authoritative namespace state into remote observations: {0}")]
    Translate(anyhow::Error),
    #[error("failed to apply authoritative remote observations batch: {0}")]
    Apply(#[from] StateDbError),
}

pub fn import_authoritative_remote_observations<T: ServerTransport>(
    db: &mut SqliteStateDb,
    transport: &T,
    namespace_id: &NamespaceId,
    applied_at_ms: u64,
) -> Result<AuthoritativeObservationImportReport, AuthoritativeObservationImportError> {
    let summary = transport
        .load_namespace_state_summary(namespace_id)
        .map_err(|e| AuthoritativeObservationImportError::LoadSummary(e.into()))?;
    let (_, observations) = transport
        .load_remote_observations(namespace_id)
        .map_err(|e| AuthoritativeObservationImportError::Translate(e.into()))?;
    Ok(apply_translated_authoritative_remote_observations(
        db,
        namespace_id,
        summary.head.seq,
        &observations,
        applied_at_ms,
    )?)
}

fn apply_translated_authoritative_remote_observations(
    db: &mut SqliteStateDb,
    namespace_id: &NamespaceId,
    authoritative_head_seq: ChangeSeq,
    observations: &[ObservedRemoteInode],
    applied_at_ms: u64,
) -> Result<AuthoritativeObservationImportReport, StateDbError> {
    let outcomes = db.apply_remote_observations_batch(observations, applied_at_ms)?;
    Ok(summarize_authoritative_import_outcomes(
        namespace_id,
        authoritative_head_seq,
        observations.len(),
        &outcomes,
    ))
}

fn summarize_authoritative_import_outcomes(
    namespace_id: &NamespaceId,
    authoritative_head_seq: ChangeSeq,
    translated_observation_count: usize,
    outcomes: &[AppliedRemoteObservation],
) -> AuthoritativeObservationImportReport {
    let mut report = AuthoritativeObservationImportReport {
        namespace_id: namespace_id.clone(),
        authoritative_head_seq,
        translated_observation_count,
        bound_local_only_count: 0,
        converged_bound_inode_count: 0,
        discovered_remote_only_count: 0,
        recorded_conflict_or_error_count: 0,
        updated_bound_remote_state_count: 0,
        ignored_stale_count: 0,
        ignored_unmatched_count: 0,
    };
    for outcome in outcomes {
        match outcome {
            AppliedRemoteObservation::BoundLocalOnly(_) => report.bound_local_only_count += 1,
            AppliedRemoteObservation::ConvergedBoundInode(_) => {
                report.converged_bound_inode_count += 1;
            }
            AppliedRemoteObservation::DiscoveredRemoteOnly { .. } => {
                report.discovered_remote_only_count += 1;
            }
            AppliedRemoteObservation::RecordedConflictOrError { .. } => {
                report.recorded_conflict_or_error_count += 1;
            }
            AppliedRemoteObservation::UpdatedBoundRemoteState { .. } => {
                report.updated_bound_remote_state_count += 1;
            }
            AppliedRemoteObservation::IgnoredStale { .. } => report.ignored_stale_count += 1,
            AppliedRemoteObservation::IgnoredUnmatched { .. } => {
                report.ignored_unmatched_count += 1;
            }
        }
    }
    report
}
