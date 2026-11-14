//! The pool record that a namespace's create call writes before manifest 1.

use crate::control_update::create_control_object_under_generated_id;
use crate::error::{CoreError, Result};
use crate::namespace::control::NewestManifest;
use crate::namespace::store::NamespaceStore;
use bytes::Bytes;
use loonfs_objectstore::keys::pool_record;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::control::{encode_control_state, ControlObjectKind, PoolPayload};

impl<S: ObjectStore> NamespaceStore<S> {
    /// Writes `record` under its freshly generated pool ID (format spec,
    /// "Creating a namespace"), with
    /// [`create_control_object_under_generated_id`].
    pub(crate) async fn write_pool_record(&self, record: &PoolPayload) -> Result<()> {
        let object_key = pool_record(&record.pool_id);
        let bytes = encode_control_state(ControlObjectKind::Pool, record)
            .expect("a pool record should encode");
        create_control_object_under_generated_id(&self.store, &object_key, Bytes::from(bytes))
            .await?;
        Ok(())
    }

    /// Deletes the pool record of a create that ended with `error` when the
    /// namespace's current manifest records another pool.
    ///
    /// A failed reload or deletion is only logged, and the record stays. So
    /// does the record when the namespace is retired, because no manifest
    /// then says which pool the namespace used.
    pub(crate) async fn delete_unused_pool_record(&self, record: &PoolPayload, error: &CoreError) {
        let records_other_pool = match self.find_newest_manifest().await {
            Ok(NewestManifest::Manifest(current)) => {
                current.manifest.payload().pool_id != record.pool_id
            }
            Ok(NewestManifest::Absent | NewestManifest::Retired) => false,
            Err(check_error) => {
                tracing::warn!(
                    namespace_id = %self.namespace_id,
                    pool_id = %record.pool_id,
                    original_error = %error,
                    check_error = %check_error,
                    "kept a pool record whose namespace could not be read after its create failed"
                );
                return;
            }
        };
        if !records_other_pool {
            return;
        }
        let object_key = pool_record(&record.pool_id);
        if let Err(cleanup_error) = self.store.delete(&object_key).await {
            tracing::warn!(
                namespace_id = %self.namespace_id,
                pool_id = %record.pool_id,
                original_error = %error,
                cleanup_error = %cleanup_error,
                "failed to delete a pool record after its create failed"
            );
        }
    }
}
