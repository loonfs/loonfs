//! One durable marking step: a source object, a revision block, or a merge page.

use super::fork_checkpoints::{classify_fork_checkpoint, ForkCheckpointReachability};
use super::mark_index;
use super::mark_table::MarkTables;
use super::reap::lease_expired;
use crate::checkpoint::record::load_checkpoint_record_at_key;
use crate::checkpoint::{load_namespace_manifest_envelope_if_present, ManifestLoadFailureClass};
use crate::context::MutationContext;
use crate::control_object::ControlObjectLoadError;
use crate::error::{CoreError, MetadataProjectionLoadError, Result};
use futures::StreamExt;
use loonfs_api::wire::control::{CheckpointOwner, CheckpointStatus};
use loonfs_api::wire::gc::*;
use loonfs_api::wire::manifest::MetadataRowFamily;
use loonfs_api::wire::wal::WalDelta;
use loonfs_api::{
    manifest_object_id_manifest_no, ChangeSeq, ContentId, GcMarkTableId, ManifestObjectId,
    NamespaceId,
};
use loonfs_objectstore::keys::{
    checkpoint_prefix, metadata_manifest_object, metadata_manifest_prefix,
    metadata_segment_object_key,
};
use loonfs_objectstore::layout::manifest_object_id_of;
use loonfs_objectstore::ObjectStore;

pub(super) fn object(key: &str) -> GcMarkEntry {
    GcMarkEntry {
        key: format!("object/{key}"),
        value: GcMarkValue::Object {},
    }
}
pub(super) fn content(id: &ContentId) -> GcMarkEntry {
    GcMarkEntry {
        key: format!("content/{id}"),
        value: GcMarkValue::Content {},
    }
}
fn missing_manifest(key: &str) -> GcMarkEntry {
    GcMarkEntry {
        key: format!("missing-manifest/{key}"),
        value: GcMarkValue::MissingManifest {},
    }
}

/// Reuses the provider's paged stream within a call. On a CAS race the
/// confirmed durable position may jump; that invalidates the cached stream.
#[derive(Default)]
pub(super) struct Scan {
    stream: Option<futures::stream::BoxStream<'static, loonfs_objectstore::Result<String>>>,
    prefix: String,
    after: Option<String>,
}
impl Scan {
    pub(super) async fn next<S: ObjectStore + ?Sized>(
        &mut self,
        store: &S,
        prefix: &str,
        after: Option<&str>,
    ) -> Result<Option<String>> {
        if self.stream.is_none() || self.prefix != prefix || self.after.as_deref() != after {
            self.stream = Some(store.list_prefix_from_stream(prefix, after));
            self.prefix = prefix.to_owned();
        }
        let next = self
            .stream
            .as_mut()
            .expect("initialized stream")
            .next()
            .await
            .transpose()
            .map_err(|error| CoreError::store(prefix, &error))?;
        self.after.clone_from(&next);
        Ok(next)
    }
}

async fn append<S: ObjectStore + ?Sized>(
    tables: &MarkTables<'_, S>,
    index: &mut GcMarkIndex,
    entries: Vec<GcMarkEntry>,
) -> Result<()> {
    let table = mark_index::write_sorted(tables, entries).await?;
    mark_index::insert(index, table, 0)
}

/// Reads a source once; marks distinguish a missing manifest from a verified
/// one so every checkpoint sharing a missing basis can be repaired.
async fn manifest<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    tables: &mut MarkTables<'_, S>,
    work: &mut GcMarkWork,
    id: &ManifestObjectId,
    expected: Option<&loonfs_api::wire::control::ManifestRef>,
    entries: &mut Vec<GcMarkEntry>,
) -> Result<Option<ChangeSeq>> {
    let key = metadata_manifest_object(namespace_id, id);
    if mark_index::lookup(tables, &work.index, &missing_manifest(&key).key)
        .await?
        .is_some()
    {
        return Ok(None);
    }
    if let Some(entry) = mark_index::lookup(tables, &work.index, &object(&key).key).await? {
        let GcMarkValue::Manifest { manifest } = entry.value else {
            return Err(CoreError::NamespaceCorrupt(
                "GC manifest mark has a different meaning".to_owned(),
            ));
        };
        verify_manifest_ref(expected, &manifest)?;
        return Ok(Some(manifest.manifest_head_seq));
    }
    let loaded = load_namespace_manifest_envelope_if_present(store, namespace_id, id, &key).await;
    let envelope = match loaded {
        Ok(Some(envelope)) => envelope,
        Ok(None) => {
            entries.push(missing_manifest(&key));
            return Ok(None);
        }
        Err(error) if error.failure_class() == ManifestLoadFailureClass::Store => {
            work.roots.degraded = true;
            return Ok(None);
        }
        Err(error) => {
            return Err(CoreError::NamespaceCorrupt(format!(
                "GC root manifest does not load: {error}"
            )))
        }
    };
    let payload = envelope.payload();
    let reference = loonfs_api::wire::control::ManifestRef {
        owner_namespace_id: namespace_id.clone(),
        manifest_no: payload.manifest_no,
        manifest_object_id: id.clone(),
        manifest_head_seq: payload.head_seq,
        manifest_payload_checksum: envelope.payload_checksum().to_owned(),
    };
    verify_manifest_ref(expected, &reference)?;
    entries.push(GcMarkEntry {
        key: object(&key).key,
        value: GcMarkValue::Manifest {
            manifest: reference,
        },
    });
    for segment in payload.runs.iter().flat_map(|run| &run.segments) {
        let key = metadata_segment_object_key(segment);
        entries.push(object(&key));
        if segment.family == MetadataRowFamily::Revisions {
            // Full scans do not use bloom filters. Omitting the optional inline
            // copy also makes otherwise identical references deduplicate.
            let mut scan_segment = segment.clone();
            scan_segment.filter_inline = None;
            entries.push(GcMarkEntry {
                key: format!("revision/{key}"),
                value: GcMarkValue::RevisionSegment {
                    segment: scan_segment,
                    max_seq: payload.head_seq,
                },
            });
        }
    }
    Ok(Some(payload.head_seq))
}

fn select_anchor(work: &mut GcMarkWork, range: Option<GcManifestRange>, candidate_seen: bool) {
    match range {
        Some(range) => {
            work.roots.anchor = GcReferenceAnchor::Manifest {
                head_seq: ChangeSeq(loonfs_api::MAX_PUBLIC_INTEGER),
            };
            work.source = GcMarkSource::AnchorManifests {
                range,
                last_key: None,
            };
        }
        None => {
            work.roots.anchor = if candidate_seen {
                GcReferenceAnchor::Missing {}
            } else {
                GcReferenceAnchor::NotNeeded {}
            };
            work.source = GcMarkSource::Wal {};
        }
    }
}

pub(super) async fn step<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    tables: &mut MarkTables<'_, S>,
    work: &mut GcMarkWork,
    scan: &mut Scan,
    grace_window_ms: u64,
    context: &MutationContext,
) -> Result<Option<GcMarkTable>> {
    if work.index.merge.is_some() {
        mark_index::step(tables, &mut work.index).await?;
        return Ok(None);
    }
    let mut entries = Vec::new();
    match work.source.clone() {
        GcMarkSource::Root { manifest: root } => {
            if let Some(root) = root {
                if manifest(
                    store,
                    namespace_id,
                    tables,
                    work,
                    &root.manifest_object_id,
                    Some(&root),
                    &mut entries,
                )
                .await?
                .is_none()
                {
                    work.roots.degraded = true;
                }
            }
            work.source = GcMarkSource::Checkpoints { last_key: None };
        }
        GcMarkSource::Checkpoints { last_key } => {
            let prefix = checkpoint_prefix(namespace_id);
            if let Some(key) = scan.next(store, &prefix, last_key.as_deref()).await? {
                let loaded = load_checkpoint_record_at_key(store, &key).await;
                let record = match loaded {
                    Ok(loaded) => Some(loaded.state),
                    Err(ControlObjectLoadError::MissingObject { .. }) => None,
                    Err(error) => return Err(CoreError::ControlObjectLoad(error)),
                };
                if let Some(record) = record {
                    let candidate = match &record.owner {
                        CheckpointOwner::User { .. } | CheckpointOwner::Snapshot { .. } => {
                            record.status != (CheckpointStatus::Active {})
                                || lease_expired(&record, context.now_ms)
                        }
                        CheckpointOwner::Fork {
                            target_namespace_id,
                            expires_at_ms,
                        } => matches!(
                            classify_fork_checkpoint(
                                store,
                                &record,
                                target_namespace_id,
                                *expires_at_ms,
                                context
                            )
                            .await?,
                            ForkCheckpointReachability::Reclaimable
                        ),
                    };
                    let protects = !work.roots.namespace_deleted
                        || (!candidate && matches!(record.owner, CheckpointOwner::Fork { .. }));
                    if protects {
                        let present = manifest(
                            store,
                            namespace_id,
                            tables,
                            work,
                            &record.manifest.manifest_object_id,
                            Some(&record.manifest),
                            &mut entries,
                        )
                        .await?
                        .is_some();
                        if !candidate {
                            entries.push(object(&key));
                            if !present && !work.roots.degraded {
                                entries.push(GcMarkEntry {
                                    key: format!("missing-basis/{key}"),
                                    value: GcMarkValue::MissingBasisCheckpoint {},
                                });
                            }
                        }
                    }
                }
                work.source = GcMarkSource::Checkpoints {
                    last_key: Some(key),
                };
            } else {
                work.source = if work.roots.namespace_deleted {
                    GcMarkSource::Wal {}
                } else {
                    GcMarkSource::AnchorDiscovery {
                        last_key: None,
                        candidate_seen: false,
                        current: None,
                        aged: None,
                    }
                };
            }
        }
        GcMarkSource::AnchorDiscovery {
            last_key,
            mut candidate_seen,
            mut current,
            mut aged,
        } => {
            let prefix = metadata_manifest_prefix(namespace_id);
            match scan.next(store, &prefix, last_key.as_deref()).await? {
                None => select_anchor(work, current.or(aged), candidate_seen),
                Some(key) => {
                    if let Some(Ok(id)) = manifest_object_id_of(&key) {
                        candidate_seen = true;
                        let manifest_no = manifest_object_id_manifest_no(id.as_str())
                            .expect("parsed manifest generation");
                        if current
                            .as_ref()
                            .is_some_and(|range| range.manifest_no != manifest_no)
                        {
                            aged = current.take();
                        }
                        if let Some(metadata) = store
                            .head(&key)
                            .await
                            .map_err(|error| CoreError::store(&key, &error))?
                        {
                            if !metadata.last_modified_ms.is_some_and(|stamp| {
                                context.now_ms.saturating_sub(stamp) >= grace_window_ms
                            }) {
                                select_anchor(work, aged, candidate_seen);
                                return Ok(None);
                            }
                            match &mut current {
                                Some(range) => range.last_key.clone_from(&key),
                                None => {
                                    current = Some(GcManifestRange {
                                        manifest_no,
                                        first_key: key.clone(),
                                        last_key: key.clone(),
                                    })
                                }
                            }
                        }
                    }
                    work.source = GcMarkSource::AnchorDiscovery {
                        last_key: Some(key),
                        candidate_seen,
                        current,
                        aged,
                    };
                }
            }
        }
        GcMarkSource::AnchorManifests { range, last_key } => {
            let next = match last_key.as_deref() {
                None => Some(range.first_key.clone()),
                Some(key) if key == range.last_key => None,
                Some(key) => {
                    scan.next(store, &metadata_manifest_prefix(namespace_id), Some(key))
                        .await?
                }
            };
            match next.filter(|key| key <= &range.last_key) {
                Some(key) => {
                    if let Some(Ok(id)) = manifest_object_id_of(&key) {
                        match manifest(store, namespace_id, tables, work, &id, None, &mut entries)
                            .await?
                        {
                            Some(seq) => {
                                if let GcReferenceAnchor::Manifest { head_seq } =
                                    &mut work.roots.anchor
                                {
                                    *head_seq = (*head_seq).min(seq);
                                }
                            }
                            None => work.roots.anchor = GcReferenceAnchor::Missing {},
                        }
                    }
                    work.source = GcMarkSource::AnchorManifests {
                        range,
                        last_key: Some(key),
                    };
                }
                None => work.source = GcMarkSource::Wal {},
            }
        }
        GcMarkSource::Wal {} => {
            if let Some(pointer) = work.wal_tip.clone() {
                let segment = crate::wal::load_retained_segment(store, namespace_id, &pointer)
                    .await
                    .map_err(|error| {
                        CoreError::MetadataProjection(MetadataProjectionLoadError::WalChainLoad(
                            error,
                        ))
                    })?;
                let payload = segment.envelope().payload();
                if payload.base_head_seq < work.floor_seq {
                    return Err(CoreError::NamespaceCorrupt(
                        "GC WAL chain crosses its frozen floor".to_owned(),
                    ));
                }
                work.wal_tip = if payload.base_head_seq == work.floor_seq {
                    None
                } else {
                    Some(
                        payload
                            .prev_visible_segment
                            .clone()
                            .filter(|prev| prev.end_seq == payload.base_head_seq)
                            .ok_or_else(|| {
                                CoreError::NamespaceCorrupt(
                                    "GC WAL chain has a missing or noncontiguous predecessor"
                                        .to_owned(),
                                )
                            })?,
                    )
                };
                entries.push(object(segment.object_key()));
                for record in segment.records() {
                    for delta in &record.deltas {
                        if let WalDelta::AppendFileRevision { content_ref, .. } = &delta.delta {
                            entries.push(content(&content_ref.content_id));
                        }
                    }
                }
            } else {
                work.source = GcMarkSource::Done {};
            }
        }
        GcMarkSource::Done {} => {
            if !mark_index::seal(&mut work.index)? {
                return Ok(Some(
                    work.index
                        .levels
                        .iter()
                        .flatten()
                        .next()
                        .cloned()
                        .unwrap_or_else(empty_table),
                ));
            }
        }
    }
    if !entries.is_empty() {
        append(tables, &mut work.index, entries).await?;
    }
    Ok(None)
}

pub(super) fn empty_table() -> GcMarkTable {
    GcMarkTable {
        table_id: GcMarkTableId::generate(),
        page_count: 0,
        entry_count: 0,
    }
}

fn verify_manifest_ref(
    expected: Option<&loonfs_api::wire::control::ManifestRef>,
    actual: &loonfs_api::wire::control::ManifestRef,
) -> Result<()> {
    if expected.is_some_and(|expected| expected != actual) {
        return Err(CoreError::NamespaceCorrupt(
            "GC manifest does not match its complete root or checkpoint reference".to_owned(),
        ));
    }
    Ok(())
}
