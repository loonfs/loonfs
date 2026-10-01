//! Complete collection passes over one namespace's grep objects.

use crate::keyspace::{
    grep_prefix, manifest_key, manifests_prefix, parse_key, segment_key, segments_prefix,
    GrepKeyKind,
};
use crate::manifest::{
    load_current_grep_manifest, load_grep_manifest, GrepManifestEnvelope, GrepManifestState,
};
use crate::{GrepError, GrepWorker, Result};
use futures::StreamExt as _;
use loonfs::engine::{
    delete_if_aged, grace_age, GraceAge, Observation, METADATA_PUBLICATION_BUDGET_MS,
    UNREFERENCED_SEGMENT_MIN_AGE_MS,
};
use loonfs::{GC_DEFAULT_GRACE_WINDOW_MS, GC_MIN_GRACE_WINDOW_MS};
use loonfs_objectstore::timing::StdMonotonicTimer;
use loonfs_objectstore::ObjectStore;
use loonfs_types::{ErrorCode, ManifestNo, NamespaceId, RunMaintenanceResponse};
use std::collections::BTreeSet;
use std::sync::Arc;

pub const GREP_GC_GRACE_WINDOW_MS: u64 = GC_DEFAULT_GRACE_WINDOW_MS;
const _: () = assert!(GREP_GC_GRACE_WINDOW_MS >= GC_MIN_GRACE_WINDOW_MS);
const _: () = assert!(
    METADATA_PUBLICATION_BUDGET_MS + GC_MIN_GRACE_WINDOW_MS <= UNREFERENCED_SEGMENT_MIN_AGE_MS
);

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GrepGcReport {
    pub deleted_segments: u64,
    pub deleted_other_objects: u64,
    pub namespace_reaped: bool,
    pub retained_candidates: u64,
}

pub async fn run_grep_gc<S: ObjectStore + Clone>(
    worker: &GrepWorker<S>,
    namespace_id: &NamespaceId,
    now_ms: u64,
) -> Result<RunMaintenanceResponse> {
    let report = worker
        .garbage_collect_namespace(namespace_id, now_ms)
        .await?;
    Ok(RunMaintenanceResponse::GrepGc {
        namespace_id: namespace_id.clone(),
        deleted_segments: report.deleted_segments,
        deleted_other_objects: report.deleted_other_objects,
        namespace_reaped: report.namespace_reaped,
        retained_candidates: report.retained_candidates,
    })
}

impl<S: ObjectStore + Clone> GrepWorker<S> {
    pub async fn garbage_collect_namespace(
        &self,
        namespace_id: &NamespaceId,
        now_ms: u64,
    ) -> Result<GrepGcReport> {
        let gone = match self.reads(namespace_id).head().await {
            Ok(_) => false,
            Err(error)
                if matches!(
                    error.code(),
                    ErrorCode::NamespaceNotFound | ErrorCode::NamespaceDeleted
                ) =>
            {
                true
            }
            Err(error) => return Err(error),
        };
        let mut report = GrepGcReport::default();
        if gone {
            let prefix = grep_prefix(namespace_id);
            let mut keys = self.store().list_prefix_stream(&prefix);
            while let Some(key) = keys.next().await {
                let key = key.map_err(|error| GrepError::store(&prefix, &error))?;
                collect_candidate(
                    self.store(),
                    &key,
                    GREP_GC_GRACE_WINDOW_MS,
                    now_ms,
                    &mut report,
                )
                .await?;
            }
            report.namespace_reaped =
                report.deleted_segments > 0 || report.deleted_other_objects > 0;
            return Ok(report);
        }
        let current = load_current_grep_manifest(
            self.store(),
            namespace_id,
            Observation::now(Arc::new(StdMonotonicTimer::default())),
        )
        .await?;
        let observed_hint = current
            .as_ref()
            .map_or(ManifestNo(1), |current| current.hint.state.manifest_no);
        let mut live_segments = BTreeSet::new();
        let mut kept_manifests = BTreeSet::new();
        let mut root = current.map(|current| current.manifest_state().clone());
        while let Some(state) = root.take() {
            live_segments.extend(
                state
                    .segments()
                    .iter()
                    .map(|segment| segment_key(namespace_id, &segment.segment_id)),
            );
            if state.manifest_no() <= ManifestNo(1) {
                break;
            }
            let successor = manifest_key(namespace_id, &state.manifest_no());
            let age = grace_age(self.store(), &successor, GREP_GC_GRACE_WINDOW_MS, now_ms)
                .await
                .map_err(|error| GrepError::store(&successor, &error))?;
            if matches!(age, GraceAge::Young | GraceAge::Unknown) {
                let predecessor = ManifestNo(state.manifest_no().0 - 1);
                root = load_grep_manifest(self.store(), namespace_id, predecessor)
                    .await?
                    .map(GrepManifestEnvelope::into_payload);
                kept_manifests.extend(root.as_ref().map(GrepManifestState::manifest_no));
            }
        }
        let prefix = manifests_prefix(namespace_id);
        let mut keys = self.store().list_prefix_stream(&prefix);
        while let Some(key) = keys.next().await {
            let key = key.map_err(|error| GrepError::store(&prefix, &error))?;
            let Some(parsed) = parse_key(&key) else {
                report.retained_candidates += 1;
                continue;
            };
            let GrepKeyKind::Manifest { manifest_no } = parsed.kind else {
                continue;
            };
            if manifest_no >= observed_hint || kept_manifests.contains(&manifest_no) {
                report.retained_candidates += 1;
                continue;
            }
            collect_candidate(
                self.store(),
                &key,
                GREP_GC_GRACE_WINDOW_MS,
                now_ms,
                &mut report,
            )
            .await?;
        }
        let prefix = segments_prefix(namespace_id);
        let mut keys = self.store().list_prefix_stream(&prefix);
        while let Some(key) = keys.next().await {
            let key = key.map_err(|error| GrepError::store(&prefix, &error))?;
            if live_segments.contains(&key) || parse_key(&key).is_none() {
                report.retained_candidates += 1;
                continue;
            }
            collect_candidate(
                self.store(),
                &key,
                UNREFERENCED_SEGMENT_MIN_AGE_MS + 1,
                now_ms,
                &mut report,
            )
            .await?;
        }
        Ok(report)
    }
}

async fn collect_candidate<S: ObjectStore + ?Sized>(
    store: &S,
    key: &str,
    minimum_age_ms: u64,
    now_ms: u64,
    report: &mut GrepGcReport,
) -> Result<()> {
    let age = delete_if_aged(store, key, minimum_age_ms, now_ms)
        .await
        .map_err(|error| GrepError::store(key, &error))?;
    match age {
        GraceAge::Aged => {
            if parse_key(key)
                .is_some_and(|parsed| matches!(parsed.kind, GrepKeyKind::Segment { .. }))
            {
                report.deleted_segments += 1;
            } else {
                report.deleted_other_objects += 1;
            }
        }
        GraceAge::Young | GraceAge::Unknown => report.retained_candidates += 1,
        GraceAge::Gone => {}
    }
    Ok(())
}
