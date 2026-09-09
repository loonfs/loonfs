//! Complete collection passes over one namespace's grep objects.

use crate::keyspace::{
    grep_prefix, manifest_key, manifests_prefix, parse_key, segment_key, segments_prefix,
    GrepKeyKind,
};
use crate::root::load_current_grep_manifest;
use crate::{GrepError, GrepWorker, Result};
use futures::StreamExt as _;
use loonfs::{
    delete_if_aged, GraceAge, StoreFailureClass, GC_DEFAULT_GRACE_WINDOW_MS,
    GC_MIN_GRACE_WINDOW_MS, METADATA_PUBLICATION_BUDGET_MS, UNREFERENCED_SEGMENT_MIN_AGE_MS,
};
use loonfs_api::{ErrorCode, ManifestNo, NamespaceId};
use loonfs_objectstore::{ObjectStore, ObjectStoreError};
use std::collections::BTreeSet;

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

impl<S: ObjectStore + Clone> GrepWorker<S> {
    pub async fn garbage_collect_namespace(
        &self,
        namespace_id: &NamespaceId,
        now_ms: u64,
    ) -> Result<GrepGcReport> {
        let gone = match self.reads(namespace_id).head_seq().await {
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
                let key = key.map_err(|error| store_error(&prefix, &error))?;
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
        let current = load_current_grep_manifest(self.store(), namespace_id).await?;
        let observed_hint = current
            .as_ref()
            .map_or(ManifestNo(1), |current| current.hint.state.manifest_no);
        let live_segments: BTreeSet<_> = current
            .as_ref()
            .into_iter()
            .flat_map(|current| current.manifest_state().segments())
            .map(|segment| segment_key(namespace_id, &segment.segment_id))
            .collect();
        let prefix = manifests_prefix(namespace_id);
        let mut keys = self.store().list_prefix_stream(&prefix);
        while let Some(key) = keys.next().await {
            let key = key.map_err(|error| store_error(&prefix, &error))?;
            let Some(parsed) = parse_key(&key) else {
                report.retained_candidates += 1;
                continue;
            };
            let GrepKeyKind::Manifest { manifest_no } = parsed.kind else {
                continue;
            };
            if manifest_no >= observed_hint {
                report.retained_candidates += 1;
                continue;
            }
            let successor = manifest_no
                .successor()
                .expect("a manifest below the hint should have a successor");
            let successor_key = manifest_key(namespace_id, &successor);
            let metadata = self
                .store()
                .head(&successor_key)
                .await
                .map_err(|error| store_error(&successor_key, &error))?;
            if metadata.is_some_and(|metadata| {
                metadata.last_modified_ms.is_none_or(|modified| {
                    now_ms.saturating_sub(modified) < GREP_GC_GRACE_WINDOW_MS
                })
            }) {
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
            let key = key.map_err(|error| store_error(&prefix, &error))?;
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
        .map_err(|error| store_error(key, &error))?;
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

fn store_error(object_key: &str, error: &ObjectStoreError) -> GrepError {
    GrepError::StoreUnavailable {
        object_key: object_key.to_owned(),
        message: error.public_message().into_owned(),
        class: StoreFailureClass::of(error),
    }
}
