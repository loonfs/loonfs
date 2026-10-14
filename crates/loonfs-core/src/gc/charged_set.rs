//! Bytes a collection pass holds, charged to the read working memory.

use crate::error::{CoreError, Result};
use crate::heap_bytes::{hash_map_table_bytes, hash_set_table_bytes, HeapBytes};
use crate::read_working_memory::ReadWorkingMemory;
use loonfs_types::NamespaceId;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// The read working memory one collection pass charges, and the bytes all of
/// the pass's charges hold together, so a failed charge can report the whole
/// live set.
#[derive(Clone)]
pub(super) struct PassMemory {
    namespace_id: NamespaceId,
    memory: Arc<ReadWorkingMemory>,
    held_bytes: Arc<AtomicUsize>,
}

impl PassMemory {
    pub(super) fn new(namespace_id: &NamespaceId, memory: Arc<ReadWorkingMemory>) -> Self {
        Self {
            namespace_id: namespace_id.clone(),
            memory,
            held_bytes: Arc::default(),
        }
    }
}

/// One reservation in the read working memory, released when it drops.
pub(super) struct Charge {
    pass: PassMemory,
    reserved_bytes: usize,
}

impl Charge {
    pub(super) fn new(pass: &PassMemory) -> Self {
        Self {
            pass: pass.clone(),
            reserved_bytes: 0,
        }
    }

    /// Adds `bytes` to the reservation, or fails the pass when the memory
    /// can't fit them. The error reports what the pass's charges would hold
    /// together with `bytes` added.
    pub(super) fn add(&mut self, bytes: usize) -> Result<()> {
        if bytes == 0 {
            return Ok(());
        }
        if !self.pass.memory.try_reserve(bytes) {
            return Err(CoreError::LiveSetExceedsReadMemory {
                namespace_id: self.pass.namespace_id.clone(),
                bytes: self
                    .pass
                    .held_bytes
                    .load(Ordering::Relaxed)
                    .saturating_add(bytes),
                limit: self.pass.memory.limit(),
            });
        }
        self.reserved_bytes += bytes;
        self.pass.held_bytes.fetch_add(bytes, Ordering::Relaxed);
        Ok(())
    }
}

impl Drop for Charge {
    fn drop(&mut self) {
        self.pass.memory.release(self.reserved_bytes);
        self.pass
            .held_bytes
            .fetch_sub(self.reserved_bytes, Ordering::Relaxed);
    }
}

pub(super) struct ChargedSet<T> {
    values: HashSet<T>,
    heap_bytes: usize,
    charge: Charge,
}

impl<T: Eq + std::hash::Hash + HeapBytes> ChargedSet<T> {
    pub(super) fn new(pass: &PassMemory) -> Self {
        Self {
            values: HashSet::new(),
            heap_bytes: 0,
            charge: Charge::new(pass),
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

/// A map charged like `ChargedSet`. Its values are `Copy`, so they own no heap
/// and the charge counts only their inline size.
pub(super) struct ChargedMap<K, V> {
    entries: HashMap<K, V>,
    heap_bytes: usize,
    charge: Charge,
}

impl<K: Eq + std::hash::Hash + HeapBytes, V: Copy> ChargedMap<K, V> {
    pub(super) fn new(pass: &PassMemory) -> Self {
        Self {
            entries: HashMap::new(),
            heap_bytes: 0,
            charge: Charge::new(pass),
        }
    }

    pub(super) fn get(&self, key: &K) -> Option<V> {
        self.entries.get(key).copied()
    }

    pub(super) fn insert(&mut self, key: K, value: V) -> Result<()> {
        let key_heap_bytes = key.heap_bytes();
        if self.entries.insert(key, value).is_none() {
            self.heap_bytes += key_heap_bytes;
        }
        let bytes = hash_map_table_bytes(&self.entries) + self.heap_bytes;
        self.charge
            .add(bytes.saturating_sub(self.charge.reserved_bytes))
    }
}
