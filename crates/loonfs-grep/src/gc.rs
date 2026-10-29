//! Complete collection passes over one namespace's grep objects.

use crate::keyspace::{
    grep_prefix, manifest_key, manifests_prefix, parse_key, segment_key, segments_prefix,
    GrepKeyKind,
};
use crate::manifest::{
    load_current_grep_manifest, load_grep_manifest, GrepManifestEnvelope, GrepManifestState,
};
use crate::{GrepError, GrepWorker, Result};
use futures::TryStreamExt as _;
use loonfs::engine::{
    grace_age, GraceAge, Observation, METADATA_PUBLICATION_BUDGET_MS,
    UNREFERENCED_SEGMENT_GRACE_WINDOW_MS,
};
use loonfs::{GC_DEFAULT_GRACE_WINDOW_MS, GC_MIN_GRACE_WINDOW_MS};
use loonfs_objectstore::{ListedObject, ObjectStore};
use loonfs_types::{ErrorCode, GrepGcReport, ManifestNo, NamespaceId};
use std::collections::BTreeSet;
use std::pin::pin;

pub const GREP_GC_GRACE_WINDOW_MS: u64 = GC_DEFAULT_GRACE_WINDOW_MS;
const _: () = assert!(GREP_GC_GRACE_WINDOW_MS >= GC_MIN_GRACE_WINDOW_MS);
const _: () = assert!(
    METADATA_PUBLICATION_BUDGET_MS + GC_MIN_GRACE_WINDOW_MS <= UNREFERENCED_SEGMENT_GRACE_WINDOW_MS
);

/// The most deletes that one listing of a collection pass keeps in flight.
const GREP_GC_DELETES_IN_FLIGHT: usize = 16;

/// What a collection pass did with one listed grep object.
enum ListedObjectOutcome {
    Retained,
    DeletedSegment,
    DeletedOther,
    Gone,
}

/// The grep objects that a collection pass keeps because a manifest it
/// keeps references them.
struct LiveGrepObjects {
    segment_keys: BTreeSet<String>,
    manifest_nos: BTreeSet<ManifestNo>,
}

impl<S: ObjectStore + Clone> GrepWorker<S> {
    pub async fn gc_namespace(&self, namespace_id: &NamespaceId) -> Result<GrepGcReport> {
        let now_ms = self.now_ms();
        let mut report = GrepGcReport::default();
        if self.namespace_is_gone(namespace_id).await? {
            let prefix = grep_prefix(namespace_id);
            self.collect_listed(
                &prefix,
                |_| false,
                GREP_GC_GRACE_WINDOW_MS,
                now_ms,
                &mut report,
            )
            .await?;
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
        let root = current.map(|current| current.manifest_state().clone());
        let live = self.live_objects(namespace_id, root, now_ms).await?;
        let keeps_manifest = |key: &str| match parse_key(key).map(|parsed| parsed.kind) {
            Some(GrepKeyKind::Manifest { manifest_no }) => {
                manifest_no >= observed_hint || live.manifest_nos.contains(&manifest_no)
            }
            _ => true,
        };
        let manifests = manifests_prefix(namespace_id);
        self.collect_listed(
            &manifests,
            keeps_manifest,
            GREP_GC_GRACE_WINDOW_MS,
            now_ms,
            &mut report,
        )
        .await?;
        let keeps_segment = |key: &str| live.segment_keys.contains(key) || parse_key(key).is_none();
        // A reorganization may publish output that is exactly the minimum age
        // old, so a segment must be strictly older before it goes.
        let segments = segments_prefix(namespace_id);
        self.collect_listed(
            &segments,
            keeps_segment,
            UNREFERENCED_SEGMENT_GRACE_WINDOW_MS + 1,
            now_ms,
            &mut report,
        )
        .await?;
        Ok(report)
    }

    async fn namespace_is_gone(&self, namespace_id: &NamespaceId) -> Result<bool> {
        match self.reads(namespace_id).head().await {
            Ok(_) => Ok(false),
            Err(error)
                if matches!(
                    error.code(),
                    ErrorCode::NamespaceNotFound | ErrorCode::NamespaceDeleted
                ) =>
            {
                Ok(true)
            }
            Err(error) => Err(error),
        }
    }

    /// Walks back from `root` through each manifest whose successor is still
    /// inside the grace window, and returns what those manifests reference.
    async fn live_objects(
        &self,
        namespace_id: &NamespaceId,
        mut root: Option<GrepManifestState>,
        now_ms: u64,
    ) -> Result<LiveGrepObjects> {
        let mut live = LiveGrepObjects {
            segment_keys: BTreeSet::new(),
            manifest_nos: BTreeSet::new(),
        };
        while let Some(state) = root.take() {
            live.segment_keys.extend(
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
                live.manifest_nos
                    .extend(root.as_ref().map(GrepManifestState::manifest_no));
            }
        }
        Ok(live)
    }

    /// Deletes each object under `prefix` that `keeps` doesn't keep and that
    /// is at least `minimum_age_ms` old at `now_ms`, with at most
    /// `GREP_GC_DELETES_IN_FLIGHT` deletes in flight, and records every
    /// listed object in `report`.
    async fn collect_listed(
        &self,
        prefix: &str,
        keeps: impl Fn(&str) -> bool,
        minimum_age_ms: u64,
        now_ms: u64,
        report: &mut GrepGcReport,
    ) -> Result<()> {
        let mut outcomes = pin!(self
            .store()
            .list_entries_from_stream(prefix, None)
            .map_err(|error| GrepError::store(prefix, &error))
            .map_ok(|entry| {
                let kept = keeps(&entry.key);
                async move {
                    let outcome = if kept {
                        ListedObjectOutcome::Retained
                    } else {
                        collect_entry(self.store(), &entry, minimum_age_ms, now_ms).await?
                    };
                    Ok::<_, GrepError>((entry.size_bytes, outcome))
                }
            })
            .try_buffer_unordered(GREP_GC_DELETES_IN_FLIGHT));
        while let Some((size_bytes, outcome)) = outcomes.try_next().await? {
            report.listed_bytes += size_bytes;
            match outcome {
                ListedObjectOutcome::Retained => report.retained_candidates += 1,
                ListedObjectOutcome::DeletedSegment => report.deleted_segments += 1,
                ListedObjectOutcome::DeletedOther => report.deleted_other_objects += 1,
                ListedObjectOutcome::Gone => {}
            }
        }
        Ok(())
    }
}

/// Deletes `entry` when it is at least `minimum_age_ms` old at `now_ms`.
async fn collect_entry<S: ObjectStore + ?Sized>(
    store: &S,
    entry: &ListedObject,
    minimum_age_ms: u64,
    now_ms: u64,
) -> Result<ListedObjectOutcome> {
    let key = &entry.key;
    match GraceAge::of(entry.last_modified_at_ms, minimum_age_ms, now_ms) {
        GraceAge::Aged => {
            store
                .delete(key)
                .await
                .map_err(|error| GrepError::store(key, &error))?;
            if parse_key(key)
                .is_some_and(|parsed| matches!(parsed.kind, GrepKeyKind::Segment { .. }))
            {
                Ok(ListedObjectOutcome::DeletedSegment)
            } else {
                Ok(ListedObjectOutcome::DeletedOther)
            }
        }
        GraceAge::Young | GraceAge::Unknown => Ok(ListedObjectOutcome::Retained),
        GraceAge::Gone => Ok(ListedObjectOutcome::Gone),
    }
}
