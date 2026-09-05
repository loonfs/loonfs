//! Server-owned GC progress. Every worker joins the same run and advances it
//! by CAS; a client token carries identity, never deletion evidence.
use super::budget::PassBudget;
use super::compaction_staging::CompactionLeases;
use super::config::GcConfig;
use super::cursor::CandidateFamilyExt;
use super::mark_table::MarkTables;
use super::references::References;
use super::sweep::Sweep;
use super::uploads::UploadSweepContext;
use super::{mark, mark_index};
use crate::context::MutationContext;
use crate::control_object::{
    expect_namespace, load_control_object, ControlObjectLoadError, LoadedControl,
};
use crate::error::{CoreError, Result};
use crate::namespace::control_snapshot::load_control_snapshot;
use bytes::Bytes;
use loonfs_api::wire::control::{encode_control_state, ControlObjectKind};
use loonfs_api::wire::gc::*;
use loonfs_api::{decode_cursor, encode_cursor, GcResponse, GcRunId, NamespaceId, PageCursor};
use loonfs_objectstore::{ObjectStore, ObjectStoreError};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RunCursor {
    namespace_id: NamespaceId,
    gc_run_id: GcRunId,
    // Informational only. The singleton record selects the next step.
    step_no: u64,
}
impl PageCursor for RunCursor {
    const KIND: &'static str = "namespace_gc_run";
}

pub(super) fn run_key(namespace_id: &NamespaceId) -> String {
    loonfs_objectstore::keys::gc_run(namespace_id)
}
fn scratch_prefix(namespace_id: &NamespaceId) -> String {
    loonfs_objectstore::keys::gc_runs_prefix(namespace_id)
}

pub(super) async fn load_run<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<Option<LoadedControl<GcRunState>>> {
    let loaded = load_control_object(
        store,
        run_key(namespace_id),
        ControlObjectKind::GcRun,
        |state: &GcRunState| expect_namespace(namespace_id, &state.namespace_id),
    )
    .await;
    match loaded {
        Ok(loaded) => {
            super::validate::run(&loaded.state)?;
            Ok(Some(loaded))
        }
        Err(ControlObjectLoadError::MissingObject { .. }) => Ok(None),
        Err(error) => Err(CoreError::ControlObjectLoad(error)),
    }
}

fn encoded(state: &GcRunState) -> Result<Bytes> {
    encode_control_state(ControlObjectKind::GcRun, state)
        .map(Bytes::from)
        .map_err(|error| CoreError::NamespaceCorrupt(format!("cannot encode GC progress: {error}")))
}

/// Conditional writes are settled only by re-reading durable progress. A
/// transport failure never licenses sweeping an uncommitted partial mark set.
async fn save<S: ObjectStore + ?Sized>(
    store: &S,
    previous: Option<&LoadedControl<GcRunState>>,
    state: &GcRunState,
) -> Result<LoadedControl<GcRunState>> {
    let key = run_key(&state.namespace_id);
    let bytes = encoded(state)?;
    let result = match previous {
        Some(previous) => store.compare_and_swap(&key, &previous.etag, bytes).await,
        None => store.put_if_absent(&key, bytes).await,
    };
    match result {
        Ok(_) | Err(ObjectStoreError::PreconditionFailed { .. }) => {}
        Err(error @ ObjectStoreError::Transport { .. }) => {
            let current = load_run(store, &state.namespace_id).await?;
            return match current {
                Some(current) if current.state == *state => Ok(current),
                _ => Err(CoreError::store(&key, &error)),
            };
        }
        Err(error) => return Err(CoreError::store(&key, &error)),
    }
    load_run(store, &state.namespace_id)
        .await?
        .ok_or_else(|| CoreError::NamespaceCorrupt("GC progress disappeared after CAS".to_owned()))
}

pub async fn gc_namespace<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    config: &GcConfig,
    context: &MutationContext,
) -> Result<GcResponse> {
    config.validate()?;
    let requested = config
        .cursor
        .as_deref()
        .map(|token| {
            decode_cursor::<RunCursor>(token)
                .map_err(|error| CoreError::InvalidGcConfig(error.to_string()))
        })
        .transpose()?;
    if requested
        .as_ref()
        .is_some_and(|cursor| cursor.namespace_id != *namespace_id)
    {
        return Err(CoreError::InvalidGcConfig(
            "GC cursor belongs to another namespace".to_owned(),
        ));
    }
    let mut report = GcResponse::empty(namespace_id.clone());
    let current = load_run(store, namespace_id).await?;
    if let Some(requested) = &requested {
        if current
            .as_ref()
            .is_none_or(|current| current.state.gc_run_id != requested.gc_run_id)
        {
            return Err(CoreError::InvalidGcConfig(
                "GC cursor does not identify the namespace's current run".to_owned(),
            ));
        }
    }
    let mut loaded = if current.as_ref().is_none_or(|current| {
        matches!(current.state.phase, GcPhase::Complete {}) && requested.is_none()
    }) {
        // Read the format gate before reserving progress. Old collectors must
        // reject the head version used by this GC protocol.
        match crate::namespace::control::load_head_object(store, namespace_id).await {
            Ok(_) => {}
            Err(ControlObjectLoadError::MissingObject { .. }) => return Ok(report),
            Err(error) => return Err(CoreError::ControlObjectLoad(error)),
        }
        let state = GcRunState {
            namespace_id: namespace_id.clone(),
            gc_run_id: GcRunId::generate(),
            step_no: 0,
            started_at_ms: context.now_ms,
            grace_window_ms: config.grace_window_ms,
            phase: GcPhase::Starting {},
        };
        save(store, current.as_ref(), &state).await?
    } else {
        current.expect("existing run")
    };
    let run_id = loaded.state.gc_run_id.clone();
    let fixed_context = MutationContext {
        writer_id: context.writer_id.clone(),
        now_ms: loaded.state.started_at_ms,
    };
    let mut pass = Pass::new(store, namespace_id, &run_id, &fixed_context);
    let mut budget = PassBudget::new(config.max_objects);
    while !matches!(loaded.state.phase, GcPhase::Complete {}) && budget.try_charge() {
        let mut next = loaded.state.clone();
        pass.step(&mut next, &mut report).await?;
        next.step_no = next
            .step_no
            .checked_add(1)
            .ok_or_else(|| CoreError::NamespaceCorrupt("GC step number overflow".to_owned()))?;
        let confirmed = save(store, Some(&loaded), &next).await?;
        if confirmed.state.gc_run_id != run_id {
            // Another caller completed our run and reserved the next. Our
            // token cannot become permission to work on that newer run.
            return Ok(report);
        }
        loaded = confirmed;
    }
    report.retention_degraded |= match &loaded.state.phase {
        GcPhase::Marking { work } => work.roots.degraded,
        GcPhase::Revisions { roots, .. }
        | GcPhase::Sealing { roots, .. }
        | GcPhase::Sweeping { roots, .. } => roots.degraded,
        _ => false,
    };
    if !matches!(loaded.state.phase, GcPhase::Complete {}) {
        report.budget_exhausted = true;
        report.content_reclamation_deferred = !matches!(
            loaded.state.phase,
            GcPhase::Sweeping { .. } | GcPhase::Cleaning { .. }
        );
        report.next_cursor = Some(
            encode_cursor(&RunCursor {
                namespace_id: namespace_id.clone(),
                gc_run_id: run_id,
                step_no: loaded.state.step_no,
            })
            .map_err(|error| CoreError::Internal(format!("cannot encode GC cursor: {error}")))?,
        );
    }
    Ok(report)
}

/// Per-invocation I/O state. Durable positions remain entirely in GcRunState.
pub(super) struct Pass<'a, S: ?Sized> {
    store: &'a S,
    namespace_id: &'a NamespaceId,
    context: &'a MutationContext,
    tables: MarkTables<'a, S>,
    leases: CompactionLeases,
    scan: mark::Scan,
}

impl<'a, S: ObjectStore + ?Sized> Pass<'a, S> {
    pub(super) fn new(
        store: &'a S,
        namespace_id: &'a NamespaceId,
        run_id: &'a GcRunId,
        context: &'a MutationContext,
    ) -> Self {
        Self {
            store,
            namespace_id,
            context,
            tables: MarkTables::new(store, namespace_id, run_id),
            leases: CompactionLeases::default(),
            scan: mark::Scan::default(),
        }
    }
    pub(super) async fn step(
        &mut self,
        state: &mut GcRunState,
        report: &mut GcResponse,
    ) -> Result<()> {
        let store = self.store;
        let namespace_id = self.namespace_id;
        let context = self.context;
        let tables = &mut self.tables;
        let leases = &mut self.leases;
        let scan = &mut self.scan;
        match &mut state.phase {
            GcPhase::Starting {} => {
                let snapshot = load_control_snapshot(store, namespace_id)
                    .await
                    .map_err(CoreError::ControlObjectLoad)?;
                let head = &snapshot.head.state;
                let deleted = head.status.is_deleted();
                let basis = snapshot.basis();
                let root = (!deleted && basis.is_owned_by(namespace_id))
                    .then(|| basis.manifest().expect("owned basis").clone());
                let wal_tip = if deleted || head.seq == snapshot.retention_floor_seq {
                    None
                } else {
                    Some(
                        head.visible_wal_tip
                            .clone()
                            .filter(|tip| tip.end_seq == head.seq)
                            .ok_or_else(|| {
                                CoreError::NamespaceCorrupt(
                                    "GC snapshot has no matching WAL tip".to_owned(),
                                )
                            })?,
                    )
                };
                state.phase = GcPhase::Marking {
                    work: Box::new(GcMarkWork {
                        roots: GcRoots {
                            content_store_id: head.content_store_id.clone(),
                            namespace_deleted: deleted,
                            degraded: false,
                            anchor: GcReferenceAnchor::NotNeeded {},
                        },
                        index: GcMarkIndex::default(),
                        source: GcMarkSource::Root { manifest: root },
                        floor_seq: snapshot.retention_floor_seq,
                        wal_tip,
                    }),
                };
            }
            GcPhase::Marking { work } => {
                if let Some(objects) = mark::step(
                    store,
                    namespace_id,
                    tables,
                    work,
                    scan,
                    state.grace_window_ms,
                    context,
                )
                .await?
                {
                    state.phase = GcPhase::Revisions {
                        roots: work.roots.clone(),
                        objects,
                        position: GcMarkPosition::default(),
                        block_no: 0,
                        content: GcMarkIndex::default(),
                    };
                }
            }
            GcPhase::Revisions {
                roots,
                objects,
                position,
                block_no,
                content,
            } => {
                if content.merge.is_some() {
                    return mark_index::step(tables, content).await;
                }
                if roots.degraded || roots.namespace_deleted {
                    state.phase = GcPhase::Sweeping {
                        roots: roots.clone(),
                        table: objects.clone(),
                        family: GcCandidateFamily::WalSegments,
                        last_key: None,
                    };
                    return Ok(());
                }
                for _ in 0..GC_MARK_PAGE_ENTRIES {
                    let Some(entry) = tables.peek(objects, *position).await? else {
                        let mut index = content.clone();
                        mark_index::insert(&mut index, objects.clone(), 0)?;
                        state.phase = GcPhase::Sealing {
                            roots: roots.clone(),
                            index,
                        };
                        return Ok(());
                    };
                    match entry.value {
                        GcMarkValue::RevisionSegment { segment, max_seq } => {
                            match crate::checkpoint::revision_content_block(
                                store, &segment, max_seq, *block_no,
                            )
                            .await?
                            {
                                Some(ids) => {
                                    let table = mark_index::write_sorted(
                                        tables,
                                        ids.iter().map(mark::content).collect(),
                                    )
                                    .await?;
                                    mark_index::insert(content, table, 0)?;
                                    *block_no = block_no.checked_add(1).ok_or_else(|| {
                                        CoreError::NamespaceCorrupt(
                                            "GC revision block position overflow".to_owned(),
                                        )
                                    })?;
                                }
                                None => {
                                    MarkTables::<S>::advance(objects, position);
                                    *block_no = 0;
                                }
                            }
                            break;
                        }
                        _ => MarkTables::<S>::advance(objects, position),
                    }
                }
            }
            GcPhase::Sealing { roots, index } => {
                if index.merge.is_some() {
                    mark_index::step(tables, index).await?;
                } else if !mark_index::seal(index)? {
                    state.phase = GcPhase::Sweeping {
                        roots: roots.clone(),
                        table: index
                            .levels
                            .iter()
                            .flatten()
                            .next()
                            .cloned()
                            .unwrap_or_else(mark::empty_table),
                        family: GcCandidateFamily::WalSegments,
                        last_key: None,
                    };
                }
            }
            GcPhase::Sweeping {
                roots,
                table,
                family,
                last_key,
            } => {
                report.retention_degraded |= roots.degraded;
                let prefix = family.prefix(namespace_id);
                match scan.next(store, &prefix, last_key.as_deref()).await? {
                    Some(key) => {
                        let upload_sweep = UploadSweepContext::new(
                            store,
                            namespace_id,
                            roots.content_store_id.clone(),
                            state.grace_window_ms,
                            context,
                        );
                        let references = References {
                            tables,
                            table,
                            roots,
                        };
                        Sweep {
                            store,
                            namespace_id,
                            grace_window_ms: state.grace_window_ms,
                            mutation: context,
                            references,
                            upload_sweep,
                            leases,
                            report,
                        }
                        .candidate(*family, &key)
                        .await?;
                        *last_key = Some(key);
                    }
                    None => match GcCandidateFamily::ALL.get(family.index() + 1) {
                        Some(next) => {
                            *family = *next;
                            *last_key = None;
                        }
                        None => state.phase = GcPhase::Cleaning { last_key: None },
                    },
                }
            }
            GcPhase::Cleaning { last_key } => {
                let prefix = scratch_prefix(namespace_id);
                match scan.next(store, &prefix, last_key.as_deref()).await? {
                    Some(key) => {
                        if scratch_page(&key) {
                            store
                                .delete(&key)
                                .await
                                .map_err(|error| CoreError::store(&key, &error))?;
                        } else {
                            report.retain(loonfs_api::RetainedReason::UnrecognizedKey);
                        }
                        *last_key = Some(key);
                    }
                    None => state.phase = GcPhase::Complete {},
                }
            }
            GcPhase::Complete {} => {}
        }
        Ok(())
    }
}

fn scratch_page(key: &str) -> bool {
    let parts: Vec<_> = key.split('/').collect();
    match parts.as_slice() {
        ["namespaces", _, "gc", "runs", run, "tables", table, page] => {
            loonfs_api::GcRunId::parse(run).is_ok()
                && loonfs_api::GcMarkTableId::parse(table).is_ok()
                && page.strip_suffix(".json").is_some_and(|number| {
                    number.len() == 20
                        && number.bytes().all(|byte| byte.is_ascii_digit())
                        && number.parse::<u64>().is_ok()
                })
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use loonfs_objectstore::local_fs_store::LocalFsStore;

    #[tokio::test]
    async fn cursor_progress_cannot_skip_server_owned_marking() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = LocalFsStore::new(dir.path()).expect("store");
        let ns = NamespaceId::parse("demo").expect("namespace");
        let context = MutationContext {
            writer_id: loonfs_api::WriterId::parse("gc-test").expect("writer"),
            now_ms: 1000,
        };
        crate::namespace::bootstrap::bootstrap_namespace(&store, &ns, &context, false)
            .await
            .expect("bootstrap");
        let mut config = GcConfig {
            max_objects: Some(1),
            ..GcConfig::default()
        };
        let first = gc_namespace(&store, &ns, &config, &context)
            .await
            .expect("start");
        let mut cursor = decode_cursor::<RunCursor>(first.next_cursor.as_deref().expect("cursor"))
            .expect("decode");
        cursor.step_no = u64::MAX;
        config.cursor = Some(encode_cursor(&cursor).expect("encode"));
        let next = gc_namespace(&store, &ns, &config, &context)
            .await
            .expect("resume server position");
        let state = load_run(&store, &ns)
            .await
            .expect("load")
            .expect("run")
            .state;
        assert_eq!(state.step_no, 2);
        assert!(matches!(state.phase, GcPhase::Marking { .. }));
        assert_eq!(next.deleted.wal_segments, 0);
        cursor.namespace_id = NamespaceId::parse("other").expect("namespace");
        config.cursor = Some(encode_cursor(&cursor).expect("encode"));
        assert!(gc_namespace(&store, &ns, &config, &context).await.is_err());
        cursor.namespace_id = ns.clone();
        cursor.gc_run_id = GcRunId::generate();
        config.cursor = Some(encode_cursor(&cursor).expect("encode"));
        assert!(gc_namespace(&store, &ns, &config, &context).await.is_err());
        assert_eq!(
            load_run(&store, &ns)
                .await
                .expect("load")
                .expect("run")
                .state,
            state
        );
    }
}
