//! Sets of content roots charged to the read working memory.

use crate::error::{CoreError, Result};
use crate::heap_bytes::{hash_set_table_bytes, HeapBytes};
use crate::read_working_memory::ReadWorkingMemory;
use loonfs_types::NamespaceId;
use std::collections::HashSet;
use std::sync::Arc;

pub(super) struct ChargedSet<T> {
    namespace_id: NamespaceId,
    values: HashSet<T>,
    heap_bytes: usize,
    memory: Arc<ReadWorkingMemory>,
    reserved_bytes: usize,
}

impl<T: Eq + std::hash::Hash + HeapBytes> ChargedSet<T> {
    pub(super) fn new(namespace_id: &NamespaceId, memory: Arc<ReadWorkingMemory>) -> Self {
        Self {
            namespace_id: namespace_id.clone(),
            values: HashSet::new(),
            heap_bytes: 0,
            memory,
            reserved_bytes: 0,
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
        if bytes > self.reserved_bytes {
            if !self.memory.try_reserve(bytes - self.reserved_bytes) {
                return Err(CoreError::ContentRootsExceedReadMemory {
                    namespace_id: self.namespace_id.clone(),
                    bytes,
                    limit: self.memory.limit(),
                });
            }
            self.reserved_bytes = bytes;
        }
        Ok(())
    }
}

impl<T> Drop for ChargedSet<T> {
    fn drop(&mut self) {
        self.memory.release(self.reserved_bytes);
    }
}
