//! Bytes a collection keeps, charged to the read working memory.

use crate::error::{CoreError, Result};
use crate::heap_bytes::{hash_map_table_bytes, hash_set_table_bytes, HeapBytes};
use crate::read_working_memory::ReadWorkingMemory;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// The read working memory one collection charges, and the bytes all of
/// the collection's charges reserve together, so a failed charge can report the whole
/// live set.
#[derive(Clone)]
pub(super) struct CollectionMemory {
    /// The namespace or the pool that the collection collects.
    collected_id: Arc<str>,
    memory: Arc<ReadWorkingMemory>,
    held_bytes: Arc<AtomicUsize>,
}

impl CollectionMemory {
    pub(super) fn new(collected_id: String, memory: Arc<ReadWorkingMemory>) -> Self {
        Self {
            collected_id: collected_id.into(),
            memory,
            held_bytes: Arc::default(),
        }
    }

    /// The bytes that all of the collection's charges reserve now.
    pub(super) fn held_bytes(&self) -> usize {
        self.held_bytes.load(Ordering::Relaxed)
    }

    pub(super) fn limit(&self) -> usize {
        self.memory.limit()
    }
}

/// One reservation in the read working memory, released when it drops.
pub(super) struct Charge {
    memory: CollectionMemory,
    reserved_bytes: usize,
}

impl Charge {
    pub(super) fn new(memory: &CollectionMemory) -> Self {
        Self {
            memory: memory.clone(),
            reserved_bytes: 0,
        }
    }

    pub(super) fn reserved_bytes(&self) -> usize {
        self.reserved_bytes
    }

    /// Adds `bytes` to the reservation, or fails the collection when the memory
    /// can't fit them. The error reports what the collection's charges would reserve
    /// together with `bytes` added.
    pub(super) fn add(&mut self, bytes: usize) -> Result<()> {
        if bytes == 0 {
            return Ok(());
        }
        if !self.memory.memory.try_reserve(bytes) {
            return Err(CoreError::LiveSetExceedsReadMemory {
                collected_id: self.memory.collected_id.to_string(),
                bytes: self
                    .memory
                    .held_bytes
                    .load(Ordering::Relaxed)
                    .saturating_add(bytes),
                limit: self.memory.memory.limit(),
            });
        }
        self.reserved_bytes += bytes;
        self.memory.held_bytes.fetch_add(bytes, Ordering::Relaxed);
        Ok(())
    }

    /// Grows the reservation to `bytes`, or fails the collection when the memory
    /// can't fit the difference. A reservation never shrinks, so it keeps the
    /// most bytes that its collection has needed.
    fn grow_to(&mut self, bytes: usize) -> Result<()> {
        self.add(bytes.saturating_sub(self.reserved_bytes))
    }
}

impl Drop for Charge {
    fn drop(&mut self) {
        self.memory.memory.release(self.reserved_bytes);
        self.memory
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
    pub(super) fn new(memory: &CollectionMemory) -> Self {
        Self {
            values: HashSet::new(),
            heap_bytes: 0,
            charge: Charge::new(memory),
        }
    }

    pub(super) fn reserved_bytes(&self) -> usize {
        self.charge.reserved_bytes()
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
        self.charge
            .grow_to(hash_set_table_bytes(&self.values) + self.heap_bytes)
    }
}

/// A map charged like `ChargedSet`, for its table and the heap bytes of its
/// keys and values.
pub(super) struct ChargedMap<K, V> {
    entries: HashMap<K, V>,
    heap_bytes: usize,
    charge: Charge,
}

impl<K: Eq + std::hash::Hash + HeapBytes, V: HeapBytes> ChargedMap<K, V> {
    pub(super) fn new(memory: &CollectionMemory) -> Self {
        Self {
            entries: HashMap::new(),
            heap_bytes: 0,
            charge: Charge::new(memory),
        }
    }

    pub(super) fn reserved_bytes(&self) -> usize {
        self.charge.reserved_bytes()
    }

    pub(super) fn get<Q: Eq + std::hash::Hash + ?Sized>(&self, key: &Q) -> Option<&V>
    where
        K: std::borrow::Borrow<Q>,
    {
        self.entries.get(key)
    }

    pub(super) fn values(&self) -> impl Iterator<Item = &V> {
        self.entries.values()
    }

    pub(super) fn insert(&mut self, key: K, value: V) -> Result<()> {
        let key_heap_bytes = key.heap_bytes();
        let value_heap_bytes = value.heap_bytes();
        // A replaced entry keeps its original key.
        self.heap_bytes = match self.entries.insert(key, value) {
            Some(previous) => self.heap_bytes - previous.heap_bytes() + value_heap_bytes,
            None => self.heap_bytes + key_heap_bytes + value_heap_bytes,
        };
        self.charge
            .grow_to(hash_map_table_bytes(&self.entries) + self.heap_bytes)
    }
}

/// A first-in, first-out queue charged for its slots and the heap bytes of
/// the values it contains.
pub(super) struct ChargedQueue<T> {
    values: VecDeque<T>,
    heap_bytes: usize,
    charge: Charge,
}

impl<T: HeapBytes> ChargedQueue<T> {
    pub(super) fn new(memory: &CollectionMemory) -> Self {
        Self {
            values: VecDeque::new(),
            heap_bytes: 0,
            charge: Charge::new(memory),
        }
    }

    pub(super) fn reserved_bytes(&self) -> usize {
        self.charge.reserved_bytes()
    }

    pub(super) fn push_back(&mut self, value: T) -> Result<()> {
        self.heap_bytes += value.heap_bytes();
        self.values.push_back(value);
        self.charge
            .grow_to(self.values.capacity() * std::mem::size_of::<T>() + self.heap_bytes)
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = &T> {
        self.values.iter()
    }

    pub(super) fn pop_front(&mut self) -> Option<T> {
        let value = self.values.pop_front()?;
        self.heap_bytes -= value.heap_bytes();
        Some(value)
    }
}
