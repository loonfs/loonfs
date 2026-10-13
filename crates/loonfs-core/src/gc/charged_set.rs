//! Bytes a collection pass holds, charged to the read working memory.

use crate::error::{CoreError, Result};
use crate::heap_bytes::{hash_set_table_bytes, HeapBytes};
use crate::read_working_memory::ReadWorkingMemory;
use loonfs_types::NamespaceId;
use std::collections::HashSet;
use std::sync::Arc;

/// One reservation in the read working memory, released when it drops.
pub(super) struct Charge {
    namespace_id: NamespaceId,
    memory: Arc<ReadWorkingMemory>,
    reserved_bytes: usize,
}

impl Charge {
    pub(super) fn new(namespace_id: &NamespaceId, memory: Arc<ReadWorkingMemory>) -> Self {
        Self {
            namespace_id: namespace_id.clone(),
            memory,
            reserved_bytes: 0,
        }
    }

    /// Adds `bytes` to the reservation, or fails the pass when the memory
    /// can't fit them.
    pub(super) fn add(&mut self, bytes: usize) -> Result<()> {
        if bytes == 0 {
            return Ok(());
        }
        let total = self.reserved_bytes.saturating_add(bytes);
        if !self.memory.try_reserve(bytes) {
            return Err(CoreError::LiveSetExceedsReadMemory {
                namespace_id: self.namespace_id.clone(),
                bytes: total,
                limit: self.memory.limit(),
            });
        }
        self.reserved_bytes = total;
        Ok(())
    }
}

impl Drop for Charge {
    fn drop(&mut self) {
        self.memory.release(self.reserved_bytes);
    }
}

pub(super) struct ChargedSet<T> {
    values: HashSet<T>,
    heap_bytes: usize,
    charge: Charge,
}

impl<T: Eq + std::hash::Hash + HeapBytes> ChargedSet<T> {
    pub(super) fn new(namespace_id: &NamespaceId, memory: Arc<ReadWorkingMemory>) -> Self {
        Self {
            values: HashSet::new(),
            heap_bytes: 0,
            charge: Charge::new(namespace_id, memory),
        }
    }

    pub(super) fn contains<Q: Eq + std::hash::Hash + ?Sized>(&self, value: &Q) -> bool
    where
        T: std::borrow::Borrow<Q>,
    {
        self.values.contains(value)
    }

    pub(super) fn insert(&mut self, value: T) -> Result<()> {
        if self.values.contains(&value) {
            return Ok(());
        }
        self.heap_bytes += value.heap_bytes();
        self.values.insert(value);
        let bytes = hash_set_table_bytes(&self.values) + self.heap_bytes;
        self.charge
            .add(bytes.saturating_sub(self.charge.reserved_bytes))
    }
}
