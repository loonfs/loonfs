//! The namespace's entry in its pool's member list, which a create or fork
//! call writes after manifest 1 and retirement deletes.

use crate::control_update::create_control_object_under_generated_id;
use crate::error::{CoreError, Result};
use crate::namespace::store::NamespaceStore;
use bytes::Bytes;
use loonfs_objectstore::keys::pool_member;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::control::{encode_control_state, ControlObjectKind, PoolMemberPayload};
use loonfs_types::PoolId;

impl<S: ObjectStore> NamespaceStore<S> {
    /// Writes this namespace's entry in `pool_id`'s member list with
    /// put-if-absent. An entry that's already stored counts as written,
    /// because every call that finishes the namespace writes the same bytes
    /// at the same key.
    pub(crate) async fn write_member_entry(&self, pool_id: &PoolId) -> Result<()> {
        let object_key = pool_member(pool_id, &self.namespace_id);
        let entry = PoolMemberPayload {
            pool_id: pool_id.clone(),
            namespace_id: self.namespace_id.clone(),
        };
        let bytes = encode_control_state(ControlObjectKind::PoolMember, &entry)
            .expect("a member entry should encode");
        create_control_object_under_generated_id(&self.store, &object_key, Bytes::from(bytes))
            .await?;
        Ok(())
    }

    /// Deletes this namespace's entry in `pool_id`'s member list. A missing
    /// entry is deleted already.
    pub(crate) async fn delete_member_entry(&self, pool_id: &PoolId) -> Result<()> {
        let object_key = pool_member(pool_id, &self.namespace_id);
        self.store
            .delete(&object_key)
            .await
            .map_err(|error| CoreError::store(&object_key, &error))
    }
}
