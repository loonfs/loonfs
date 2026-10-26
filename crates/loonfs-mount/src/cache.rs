//! The content cache: one file per content ID in `cache/`, partial while its
//! present ranges don't cover the revision it's filling, and complete once
//! they do and its checksum matches. A file that no descriptor has open,
//! partial or complete, can be evicted.

use crate::cache_file::{create_sparse, CacheFile};
use crate::database::CacheFileRow;
use crate::error::{MountError, Result};
use crate::ranges::RangeSet;
use loonfs_types::{Checksum, ContentId, ContentRef};
use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// A prefix of a cache file whose checksum the engine knows, so verifying
/// the file after an append reads only the appended bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VerifiedPrefix {
    pub(crate) size_bytes: u64,
    pub(crate) checksum: Checksum,
}

/// Whether a partial file's verification is running.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Verification {
    Idle,
    Running,
}

/// Whether a partial file is being filled for the first time, or again after
/// its checksum didn't match.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FillAttempt {
    First,
    Refetch,
}

/// A cache file that's being filled.
#[derive(Debug)]
pub(crate) struct PartialFile {
    /// The revision that the file fills.
    pub(crate) target: ContentRef,
    pub(crate) present: RangeSet,
    /// The ranges that a queued or running fetch writes.
    pub(crate) requested: RangeSet,
    pub(crate) verified_prefix: Option<VerifiedPrefix>,
    pub(crate) attempt: FillAttempt,
    pub(crate) verification: Verification,
}

/// The state of one cache file.
#[derive(Debug)]
pub(crate) enum CacheEntryState {
    Partial(PartialFile),
    /// The file is complete up to `size_bytes`, and those bytes match
    /// `checksum`.
    Complete {
        size_bytes: u64,
        checksum: Checksum,
    },
    /// The file didn't match its checksum after a refetch, so reads of it
    /// fail until the last descriptor that uses it closes.
    Failed,
}

impl CacheEntryState {
    /// Returns the partial file, or `None` for a complete or failed file.
    pub(crate) fn partial_mut(&mut self) -> Option<&mut PartialFile> {
        match self {
            Self::Partial(partial) => Some(partial),
            Self::Complete { .. } | Self::Failed => None,
        }
    }
}

/// One cache file and its state.
#[derive(Debug)]
pub(crate) struct CacheEntry {
    pub(crate) file: Arc<CacheFile>,
    pub(crate) state: CacheEntryState,
    /// When the entry was last used, as a count of uses, for eviction.
    last_used: u64,
}

impl CacheEntry {
    /// Returns whether reads can have the bytes of `range`. A file that's
    /// fetched again after a mismatch has none until it matches.
    pub(crate) fn contains(&self, range: &Range<u64>) -> bool {
        match &self.state {
            CacheEntryState::Partial(partial) => {
                partial.attempt == FillAttempt::First && partial.present.contains(range)
            }
            CacheEntryState::Complete { size_bytes, .. } => range.end <= *size_bytes,
            CacheEntryState::Failed => false,
        }
    }

    /// Returns whether the file is being fetched again after a mismatch, so
    /// its reads wait for every byte of it.
    pub(crate) fn is_refetching(&self) -> bool {
        matches!(&self.state, CacheEntryState::Partial(partial) if partial.attempt == FillAttempt::Refetch)
    }

    /// Returns the bytes that the file occupies: a partial file's present
    /// ranges, or a complete file's size.
    fn stored_bytes(&self) -> u64 {
        match &self.state {
            CacheEntryState::Partial(partial) => partial.present.len_bytes(),
            CacheEntryState::Complete { size_bytes, .. } => *size_bytes,
            CacheEntryState::Failed => 0,
        }
    }
}

/// The cache files of one mount.
#[derive(Debug)]
pub(crate) struct Cache {
    directory: PathBuf,
    entries: HashMap<ContentId, CacheEntry>,
    uses: u64,
}

fn complete_path(directory: &Path, content_id: &ContentId) -> PathBuf {
    directory.join(content_id.as_str())
}

fn partial_path(directory: &Path, content_id: &ContentId) -> PathBuf {
    directory.join(format!("{content_id}.partial"))
}

impl Cache {
    /// Opens the cache directory with the complete files that `rows` lists,
    /// and deletes every other file in it, because a partial file's present
    /// ranges were kept in memory only. Returns the cache and the rows whose
    /// files are missing.
    pub(crate) fn load(
        directory: &Path,
        rows: Vec<CacheFileRow>,
    ) -> Result<(Self, Vec<ContentId>)> {
        let mut cache = Self {
            directory: directory.to_path_buf(),
            entries: HashMap::new(),
            uses: 0,
        };
        let mut missing = Vec::new();
        for row in rows {
            let path = complete_path(directory, &row.content_id);
            let opened = OpenOptions::new().read(true).write(true).open(&path);
            match opened {
                Ok(file)
                    if file
                        .metadata()
                        .is_ok_and(|metadata| metadata.len() == row.size_bytes) =>
                {
                    cache.entries.insert(
                        row.content_id,
                        CacheEntry {
                            file: CacheFile::new(file, row.size_bytes),
                            state: CacheEntryState::Complete {
                                size_bytes: row.size_bytes,
                                checksum: row.checksum,
                            },
                            last_used: 0,
                        },
                    );
                }
                _ => missing.push(row.content_id),
            }
        }
        let listing = fs::read_dir(directory).map_err(|error| MountError::io(directory, &error))?;
        for file in listing {
            let file = file.map_err(|error| MountError::io(directory, &error))?;
            let name = file.file_name();
            let kept = name
                .to_str()
                .and_then(|name| ContentId::parse(name).ok())
                .is_some_and(|content_id| cache.entries.contains_key(&content_id));
            if !kept {
                let path = file.path();
                fs::remove_file(&path).map_err(|error| MountError::io(&path, &error))?;
            }
        }
        Ok((cache, missing))
    }

    /// Returns the entry of `content_id`.
    pub(crate) fn entry(&self, content_id: &ContentId) -> Option<&CacheEntry> {
        self.entries.get(content_id)
    }

    /// Returns the entry of `content_id`, to change.
    pub(crate) fn entry_mut(&mut self, content_id: &ContentId) -> Option<&mut CacheEntry> {
        self.entries.get_mut(content_id)
    }

    /// Records a use of an entry, for eviction.
    pub(crate) fn touch(&mut self, content_id: &ContentId) {
        self.uses += 1;
        if let Some(entry) = self.entries.get_mut(content_id) {
            entry.last_used = self.uses;
        }
    }

    /// Returns the entry that serves the revision `target`, creating a
    /// partial file for it, or turning a complete file of a shorter revision
    /// of the same chain into a partial one, when the cache doesn't cover it.
    pub(crate) fn prepare(&mut self, target: &ContentRef) -> Result<&mut CacheEntry> {
        let content_id = &target.content_id;
        let directory = self.directory.clone();
        match self.entries.get(content_id).map(|entry| &entry.state) {
            None => {
                let path = partial_path(&directory, content_id);
                let file = create_sparse(&path, target.size_bytes)?;
                self.entries.insert(
                    content_id.clone(),
                    CacheEntry {
                        file: CacheFile::new(file, 0),
                        state: CacheEntryState::Partial(PartialFile {
                            target: target.clone(),
                            present: RangeSet::default(),
                            requested: RangeSet::default(),
                            verified_prefix: None,
                            attempt: FillAttempt::First,
                            verification: Verification::Idle,
                        }),
                        last_used: 0,
                    },
                );
            }
            Some(CacheEntryState::Complete {
                size_bytes,
                checksum,
            }) if *size_bytes < target.size_bytes => {
                let prefix = VerifiedPrefix {
                    size_bytes: *size_bytes,
                    checksum: checksum.clone(),
                };
                let from = complete_path(&directory, content_id);
                let to = partial_path(&directory, content_id);
                fs::rename(&from, &to).map_err(|error| MountError::io(&from, &error))?;
                let entry = self
                    .entries
                    .get_mut(content_id)
                    .expect("the entry was just found");
                entry
                    .file
                    .file()
                    .set_len(target.size_bytes)
                    .map_err(|error| MountError::io(&to, &error))?;
                let mut present = RangeSet::default();
                present.insert(0..prefix.size_bytes);
                entry.state = CacheEntryState::Partial(PartialFile {
                    target: target.clone(),
                    present,
                    requested: RangeSet::default(),
                    verified_prefix: Some(prefix),
                    attempt: FillAttempt::First,
                    verification: Verification::Idle,
                });
            }
            Some(CacheEntryState::Partial(partial))
                if partial.target.size_bytes < target.size_bytes =>
            {
                let path = partial_path(&directory, content_id);
                let entry = self
                    .entries
                    .get_mut(content_id)
                    .expect("the entry was just found");
                entry
                    .file
                    .file()
                    .set_len(target.size_bytes)
                    .map_err(|error| MountError::io(&path, &error))?;
                entry
                    .state
                    .partial_mut()
                    .expect("the entry was just found partial")
                    .target = target.clone();
            }
            Some(_) => {}
        }
        self.touch(content_id);
        Ok(self
            .entries
            .get_mut(content_id)
            .expect("the entry was just prepared"))
    }

    /// Makes the complete file of `previous` the start of the partial file of
    /// `target`, a revision whose append started a new chain at the end of
    /// `previous`. Returns whether the cache had that file.
    pub(crate) fn continue_chain(
        &mut self,
        previous: &ContentRef,
        target: &ContentRef,
    ) -> Result<bool> {
        let covers_previous = matches!(
            self.entries.get(&previous.content_id).map(|entry| &entry.state),
            Some(CacheEntryState::Complete { size_bytes, .. }) if *size_bytes == previous.size_bytes
        );
        if !covers_previous || self.entries.contains_key(&target.content_id) {
            return Ok(false);
        }
        let entry = self
            .entries
            .remove(&previous.content_id)
            .expect("the entry was just found");
        entry.file.set_present_prefix(0);
        let from = complete_path(&self.directory, &previous.content_id);
        let to = partial_path(&self.directory, &target.content_id);
        fs::rename(&from, &to).map_err(|error| MountError::io(&from, &error))?;
        let file = entry
            .file
            .file()
            .try_clone()
            .map_err(|error| MountError::io(&to, &error))?;
        file.set_len(target.size_bytes)
            .map_err(|error| MountError::io(&to, &error))?;
        let mut present = RangeSet::default();
        present.insert(0..previous.size_bytes);
        self.entries.insert(
            target.content_id.clone(),
            CacheEntry {
                file: CacheFile::new(file, previous.size_bytes),
                state: CacheEntryState::Partial(PartialFile {
                    target: target.clone(),
                    present,
                    requested: RangeSet::default(),
                    verified_prefix: Some(VerifiedPrefix {
                        size_bytes: previous.size_bytes,
                        checksum: previous.checksum.clone(),
                    }),
                    attempt: FillAttempt::First,
                    verification: Verification::Idle,
                }),
                last_used: self.uses,
            },
        );
        Ok(true)
    }

    /// Records bytes that a fetch wrote, and publishes the present prefix to
    /// read handles. A file that's fetched again after a mismatch publishes
    /// nothing until it matches.
    pub(crate) fn record_present(&mut self, content_id: &ContentId, range: Range<u64>) {
        let Some(entry) = self.entries.get_mut(content_id) else {
            return;
        };
        if let CacheEntryState::Partial(partial) = &mut entry.state {
            partial.present.insert(range);
            if partial.attempt == FillAttempt::First {
                entry.file.set_present_prefix(partial.present.prefix_end());
            }
        }
    }

    /// Renames a verified partial file to its complete name, and returns the
    /// row that records it.
    pub(crate) fn complete(&mut self, content_id: &ContentId) -> Result<CacheFileRow> {
        let entry = self
            .entries
            .get_mut(content_id)
            .expect("a completed cache file should have an entry");
        let partial = entry
            .state
            .partial_mut()
            .expect("a completed cache file should be partial until it completes");
        let row = CacheFileRow {
            content_id: content_id.clone(),
            size_bytes: partial.target.size_bytes,
            checksum: partial.target.checksum.clone(),
        };
        let from = partial_path(&self.directory, content_id);
        let to = complete_path(&self.directory, content_id);
        fs::rename(&from, &to).map_err(|error| MountError::io(&from, &error))?;
        entry.file.set_present_prefix(row.size_bytes);
        entry.state = CacheEntryState::Complete {
            size_bytes: row.size_bytes,
            checksum: row.checksum.clone(),
        };
        Ok(row)
    }

    /// Replaces a partial file whose checksum didn't match with an empty one,
    /// for a refetch, and returns the new file.
    pub(crate) fn restart(&mut self, content_id: &ContentId) -> Result<Arc<CacheFile>> {
        let path = partial_path(&self.directory, content_id);
        let entry = self
            .entries
            .get_mut(content_id)
            .expect("a restarted cache file should have an entry");
        entry.file.set_present_prefix(0);
        let partial = entry
            .state
            .partial_mut()
            .expect("a restarted cache file should be partial");
        fs::remove_file(&path).map_err(|error| MountError::io(&path, &error))?;
        let file = create_sparse(&path, partial.target.size_bytes)?;
        entry.file = CacheFile::new(file, 0);
        partial.present = RangeSet::default();
        partial.requested = RangeSet::default();
        partial.verified_prefix = None;
        partial.attempt = FillAttempt::Refetch;
        partial.verification = Verification::Idle;
        Ok(entry.file.clone())
    }

    /// Marks a file that didn't match its checksum after a refetch.
    pub(crate) fn fail(&mut self, content_id: &ContentId) {
        if let Some(entry) = self.entries.get_mut(content_id) {
            entry.file.set_present_prefix(0);
            entry.state = CacheEntryState::Failed;
        }
    }

    /// Deletes a cache file.
    pub(crate) fn remove(&mut self, content_id: &ContentId) -> Result<()> {
        let Some(entry) = self.entries.remove(content_id) else {
            return Ok(());
        };
        entry.file.set_present_prefix(0);
        let path = match entry.state {
            CacheEntryState::Complete { .. } => complete_path(&self.directory, content_id),
            CacheEntryState::Partial(_) | CacheEntryState::Failed => {
                partial_path(&self.directory, content_id)
            }
        };
        match fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(MountError::io(&path, &error)),
        }
    }

    /// Returns the bytes of the files, partial or complete, that no
    /// descriptor has open.
    pub(crate) fn evictable_bytes(&self, pinned: impl Fn(&ContentId) -> bool) -> u64 {
        self.entries
            .iter()
            .filter(|(content_id, _)| !pinned(content_id))
            .map(|(_, entry)| entry.stored_bytes())
            .sum()
    }

    /// Returns the bytes of the files that a descriptor has open, which
    /// eviction can't remove.
    pub(crate) fn pinned_bytes(&self, pinned: impl Fn(&ContentId) -> bool) -> u64 {
        self.entries
            .iter()
            .filter(|(content_id, _)| pinned(content_id))
            .map(|(_, entry)| entry.stored_bytes())
            .sum()
    }

    /// Returns the least recently used file, partial or complete, that no
    /// descriptor has open. A partial file goes whole, because its present
    /// ranges are kept in memory only and a restart drops it anyway.
    pub(crate) fn least_recently_used(
        &self,
        pinned: impl Fn(&ContentId) -> bool,
    ) -> Option<ContentId> {
        self.entries
            .iter()
            .filter(|(content_id, _)| !pinned(content_id))
            .min_by_key(|(_, entry)| entry.last_used)
            .map(|(content_id, _)| content_id.clone())
    }

    /// Returns the cache directory.
    pub(crate) fn directory(&self) -> &Path {
        &self.directory
    }
}
