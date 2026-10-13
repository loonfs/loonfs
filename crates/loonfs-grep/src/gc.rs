//! Complete collection passes over one namespace's grep objects.

use crate::keyspace::{
    grep_prefix, manifests_prefix, parse_key, segment_key, segments_prefix, GrepKeyKind,
};
use crate::manifest::{
    load_current_grep_manifest, load_grep_manifest, GrepManifestEnvelope, GrepManifestState,
};
use crate::{GrepError, GrepWorker, Result};
use futures::StreamExt as _;
use loonfs::engine::{
    GraceAge, Observation, METADATA_PUBLICATION_BUDGET_MS, UNREFERENCED_SEGMENT_GRACE_WINDOW_MS,
};
use loonfs::{GC_DEFAULT_GRACE_WINDOW_MS, GC_MIN_GRACE_WINDOW_MS};
use loonfs_objectstore::{ListedObject, ObjectStore};
use loonfs_types::{ErrorCode, ManifestNo, NamespaceId, RunMaintenanceResponse};
use std::collections::{BTreeMap, BTreeSet};

pub const GREP_GC_GRACE_WINDOW_MS: u64 = GC_DEFAULT_GRACE_WINDOW_MS;
const _: () = assert!(GREP_GC_GRACE_WINDOW_MS >= GC_MIN_GRACE_WINDOW_MS);
const _: () = assert!(
    METADATA_PUBLICATION_BUDGET_MS + GC_MIN_GRACE_WINDOW_MS <= UNREFERENCED_SEGMENT_GRACE_WINDOW_MS
);

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GrepGcReport {
    pub deleted_segments: u64,
    pub deleted_other_objects: u64,
    pub namespace_reaped: bool,
    pub retained_candidates: u64,
}

pub async fn gc_grep<S: ObjectStore + Clone>(
    worker: &GrepWorker<S>,
    namespace_id: &NamespaceId,
    now_ms: u64,
) -> Result<RunMaintenanceResponse> {
    let report = worker.gc_namespace(namespace_id, now_ms).await?;
    Ok(RunMaintenanceResponse::GrepGc {
        namespace_id: namespace_id.clone(),
        deleted_segments: report.deleted_segments,
        deleted_other_objects: report.deleted_other_objects,
        namespace_reaped: report.namespace_reaped,
        retained_candidates: report.retained_candidates,
    })
}

impl<S: ObjectStore + Clone> GrepWorker<S> {
    pub async fn gc_namespace(
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
            let mut keys = self.store().list_entries_from_stream(&prefix, None);
            while let Some(entry) = keys.next().await {
                let entry = entry.map_err(|error| GrepError::store(&prefix, &error))?;
                collect_candidate(
                    self.store(),
                    &entry,
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
        let current =
            load_current_grep_manifest(self.store(), namespace_id, Observation::now(self.timer()))
                .await?;
        let observed_hint = current
            .as_ref()
            .map_or(ManifestNo(1), |current| current.hint.state.manifest_no);
        let prefix = manifests_prefix(namespace_id);
        let mut manifest_entries = Vec::new();
        let mut keys = self.store().list_entries_from_stream(&prefix, None);
        while let Some(entry) = keys.next().await {
            manifest_entries.push(entry.map_err(|error| GrepError::store(&prefix, &error))?);
        }
        let listed_manifests: BTreeMap<ManifestNo, Option<u64>> = manifest_entries
            .iter()
            .filter_map(|entry| match parse_key(&entry.key)?.kind {
                GrepKeyKind::Manifest { manifest_no } => {
                    Some((manifest_no, entry.last_modified_at_ms))
                }
                _ => None,
            })
            .collect();
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
            let age = listed_manifests.get(&state.manifest_no()).map_or(
                GraceAge::Gone,
                |&last_modified_at_ms| {
                    GraceAge::of(last_modified_at_ms, GREP_GC_GRACE_WINDOW_MS, now_ms)
                },
            );
            if matches!(age, GraceAge::Young | GraceAge::Unknown) {
                let predecessor = ManifestNo(state.manifest_no().0 - 1);
                root = load_grep_manifest(self.store(), namespace_id, predecessor)
                    .await?
                    .map(GrepManifestEnvelope::into_payload);
                kept_manifests.extend(root.as_ref().map(GrepManifestState::manifest_no));
            }
        }
        for entry in &manifest_entries {
            let Some(parsed) = parse_key(&entry.key) else {
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
                entry,
                GREP_GC_GRACE_WINDOW_MS,
                now_ms,
                &mut report,
            )
            .await?;
        }
        let prefix = segments_prefix(namespace_id);
        let mut keys = self.store().list_entries_from_stream(&prefix, None);
        while let Some(entry) = keys.next().await {
            let entry = entry.map_err(|error| GrepError::store(&prefix, &error))?;
            let key = &entry.key;
            if live_segments.contains(key) || parse_key(key).is_none() {
                report.retained_candidates += 1;
                continue;
            }
            collect_candidate(
                self.store(),
                &entry,
                UNREFERENCED_SEGMENT_GRACE_WINDOW_MS + 1,
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
    entry: &ListedObject,
    minimum_age_ms: u64,
    now_ms: u64,
    report: &mut GrepGcReport,
) -> Result<()> {
    let key = &entry.key;
    let age = GraceAge::of(entry.last_modified_at_ms, minimum_age_ms, now_ms);
    match age {
        GraceAge::Aged => {
            store
                .delete(key)
                .await
                .map_err(|error| GrepError::store(key, &error))?;
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
