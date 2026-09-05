//! Read-only deletion evidence from a completely marked run.
use super::mark;
use super::mark_table::MarkTables;
use super::uploads::ContentReference;
use crate::error::Result;
use loonfs_api::wire::gc::{GcMarkTable, GcReferenceAnchor, GcRoots};
use loonfs_api::{wal_segment_id_start_seq, ContentId};
use loonfs_objectstore::keys::wal_segment_id_from_key;
use loonfs_objectstore::ObjectStore;

pub(super) struct References<'a, 'store, S: ?Sized> {
    pub(super) tables: &'a mut MarkTables<'store, S>,
    pub(super) table: &'a GcMarkTable,
    pub(super) roots: &'a GcRoots,
}

impl<S: ObjectStore + ?Sized> References<'_, '_, S> {
    pub(super) async fn object(&mut self, key: &str) -> Result<bool> {
        if let GcReferenceAnchor::Manifest { head_seq } = self.roots.anchor {
            if wal_segment_id_from_key(key)
                .and_then(wal_segment_id_start_seq)
                .is_some_and(|start| start > head_seq)
            {
                return Ok(true);
            }
        }
        Ok(self
            .tables
            .lookup(self.table, &mark::object(key).key)
            .await?
            .is_some())
    }
    pub(super) async fn missing_basis(&mut self, key: &str) -> Result<bool> {
        Ok(self
            .tables
            .lookup(self.table, &format!("missing-basis/{key}"))
            .await?
            .is_some())
    }
    pub(super) async fn content(&mut self, id: &ContentId) -> Result<ContentReference> {
        if self.roots.degraded || self.roots.namespace_deleted {
            return Ok(ContentReference::Unknown);
        }
        Ok(
            if self
                .tables
                .lookup(self.table, &mark::content(id).key)
                .await?
                .is_some()
            {
                ContentReference::Referenced
            } else {
                ContentReference::Absent
            },
        )
    }
}
