#![allow(clippy::panic, clippy::print_stdout)]
//! Checks that the metadata segment cache and the WAL-tail projection cache
//! charge their budgets with the heap their contents hold.
//!
//! This binary counts every allocation, so it keeps a process of its own.
//! Heap here means the bytes the program asks the allocator for; allocator
//! overhead is outside every budget.

use bytes::Bytes;
use loonfs::publish::{
    parse_mutation_path, CommitCandidate, CommitRequest, FilesystemOperation, InlineContent,
};
use loonfs::{
    ActorId, CommitId, ContentId, CreateNamespaceOptions, DestinationBehavior,
    ListPathEntriesOptions, LoonFs, MetadataMaintenanceOptions, MetadataSegmentCacheConfig,
    NamespaceId, PageRequest, ReadOnly, RuntimeCacheConfig, SharedObjectStore, StatPathOptions,
};
use loonfs_api::{
    AccessGrants, AccessRight, AccessRights, AttributeKey, AttributeValue, NamespaceAccess,
    PrincipalId, PrincipalScope, PrincipalSet, Subject, SubjectId, MAX_ACCESS_GRANT_ENTRIES,
    MAX_ATTRIBUTE_ENTRIES,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::ids::{
    attribute_key, attribute_text, namespace_id, page_limit, test_actor,
};
use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

struct CountingAllocator;

static LIVE: AtomicUsize = AtomicUsize::new(0);

#[allow(
    unsafe_code,
    reason = "a global allocator is the only way to observe live heap; it forwards to the system allocator unchanged"
)]
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            LIVE.fetch_add(layout.size(), Ordering::Relaxed);
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            LIVE.fetch_add(layout.size(), Ordering::Relaxed);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) };
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let moved = unsafe { System.realloc(pointer, layout, new_size) };
        if !moved.is_null() {
            LIVE.fetch_add(new_size, Ordering::Relaxed);
            LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        }
        moved
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

const DIRECTORIES: usize = 16;
const SEGMENT_BUDGET_BYTES: usize = 2 * 1024 * 1024;
const TOLERANCE: f64 = 0.10;
const PRINCIPAL_SCOPE: &str = "org_dense";

/// What each entry is.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Rows {
    /// Short directories.
    Directories,
    /// Files with 200-byte names, a 200-byte actor id, 120-byte commit ids,
    /// and 2 KiB of inline content per file in the tail.
    Large,
    /// Files with 100 attributes each, under directories whose access rows
    /// hold 1,000 grants each, in a namespace with access control. Listing
    /// leaves the attributes out, so the segment cache holds the access
    /// rows; a projection holds both.
    Dense,
}

/// One dataset shape. Every namespace holds `folded_entries` in metadata
/// segments and `tail_commits` unfolded WAL objects on top.
struct Shape {
    label: &'static str,
    namespaces: usize,
    folded_entries: usize,
    tail_commits: usize,
    tail_entries_per_commit: usize,
    rows: Rows,
}

impl Shape {
    fn namespace(&self, index: usize) -> NamespaceId {
        namespace_id(&format!("{}-{index:02}", self.label))
    }

    fn entries(&self) -> usize {
        self.folded_entries + self.tail_commits * self.tail_entries_per_commit
    }

    fn entry_path(&self, entry: usize) -> String {
        let directory = entry % DIRECTORIES;
        if self.rows == Rows::Large {
            format!("/d-{directory:04}/{:x<200}", format!("file-{entry:05}-"))
        } else {
            format!("/d-{directory:04}/entry-{entry:05}")
        }
    }

    fn actor(&self) -> ActorId {
        if self.rows == Rows::Large {
            ActorId::parse(format!("{:a<200}", "actor-")).expect("actor id")
        } else {
            test_actor()
        }
    }

    fn commit_id(&self, label: &str) -> CommitId {
        if self.rows == Rows::Large {
            CommitId::parse(format!("{:x<120}", format!("c-{label}-"))).expect("commit id")
        } else {
            CommitId::parse(label).expect("commit id")
        }
    }

    /// Reads a namespace with access control as a subject whose `read`
    /// comes from the grants, so every read walks the access rows.
    fn reader(&self, reader: LoonFs<ReadOnly>) -> LoonFs<ReadOnly> {
        match self.rows {
            Rows::Dense => reader.as_subject(subject(1)),
            Rows::Directories | Rows::Large => reader,
        }
    }

    /// Files share one small inline value unless `distinct_bytes` gives each
    /// file its own; a shared value keeps the fold to one content write.
    /// A dense commit also replaces the grants of `directories`.
    fn candidate(
        &self,
        namespace_id: &NamespaceId,
        label: &str,
        entries: std::ops::Range<usize>,
        directories: std::ops::Range<usize>,
        distinct_bytes: Option<usize>,
    ) -> CommitCandidate {
        let inline = |bytes: usize| {
            InlineContent::new(
                namespace_id.clone(),
                ContentId::generate(),
                Bytes::from(vec![b'x'; bytes]),
            )
        };
        let shared = inline(64);
        let mut inline_content = Vec::new();
        let mut operations: Vec<_> = entries
            .flat_map(|entry| {
                let path = parse_mutation_path(&self.entry_path(entry)).expect("path");
                if self.rows == Rows::Directories {
                    return vec![FilesystemOperation::CreateDirectory {
                        path,
                        parents: true,
                    }];
                }
                let content_ref = match distinct_bytes {
                    Some(bytes) => {
                        let value = inline(bytes);
                        let content_ref = value.content_ref().clone();
                        inline_content.push(value);
                        content_ref
                    }
                    None => shared.content_ref().clone(),
                };
                let put = FilesystemOperation::PutFile {
                    path: path.clone(),
                    content_ref: Some(content_ref),
                    inline_content: None,
                    behavior: DestinationBehavior::NoReplace,
                    expected_inode_id: None,
                    expected_revision_no: None,
                };
                if self.rows == Rows::Large {
                    return vec![put];
                }
                vec![
                    put,
                    FilesystemOperation::UpdateAttributes {
                        path,
                        set: full_attributes(),
                        remove: Vec::new(),
                        expected_inode_id: None,
                        expected_attributes_revision_no: None,
                    },
                ]
            })
            .collect();
        if self.rows == Rows::Dense {
            operations.extend(
                directories.map(|directory| FilesystemOperation::UpdateAccess {
                    path: parse_mutation_path(&format!("/d-{directory:04}")).expect("path"),
                    boundary: false,
                    grants: full_grants(false),
                    expected_inode_id: None,
                    expected_access_revision_no: None,
                }),
            );
        }
        CommitCandidate::with_inline_content(
            CommitRequest {
                commit_id: self.commit_id(label),
                actor_id: self.actor(),
                subject: (self.rows == Rows::Dense).then(|| subject(0)),
                message: None,
                operations,
                preconditions: Vec::new(),
            },
            Vec::new(),
            match distinct_bytes {
                None if self.rows != Rows::Directories => vec![shared],
                _ => inline_content,
            },
        )
    }
}

fn principal(index: usize) -> PrincipalId {
    PrincipalId::parse(format!("p{index:04}")).expect("principal id")
}

fn subject(index: usize) -> Subject {
    Subject {
        principal_scope: PrincipalScope::parse(PRINCIPAL_SCOPE).expect("principal scope"),
        subject_id: SubjectId::parse(principal(index).as_str()).expect("subject id"),
        principals: PrincipalSet::new(BTreeSet::from([principal(index)])).expect("principals"),
    }
}

/// A grant map at the entry limit. On the root the first principal holds
/// `admin`. Every other grant holds four rights, whose names put at most two
/// access rows in a data block, so no one block is a large part of the
/// segment budget.
fn full_grants(root: bool) -> AccessGrants {
    let rights = AccessRights::from_iter([
        AccessRight::Read,
        AccessRight::History,
        AccessRight::Create,
        AccessRight::Remove,
    ]);
    AccessGrants::new(
        (0..MAX_ACCESS_GRANT_ENTRIES)
            .map(|index| match index {
                0 if root => (principal(index), AccessRights::ADMIN),
                _ => (principal(index), rights),
            })
            .collect(),
    )
    .expect("grants")
}

/// An attribute map at the entry limit.
fn full_attributes() -> BTreeMap<AttributeKey, AttributeValue> {
    (0..MAX_ATTRIBUTE_ENTRIES)
        .map(|index| (attribute_key(&format!("k{index:02}")), attribute_text("v")))
        .collect()
}

async fn seed(store: &SharedObjectStore, shape: &Shape) {
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id("seed-writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("writer");
    let maintenance = LoonFs::builder_with_store(store.clone())
        .writer_id("seed-maintenance")
        .build()
        .await
        .expect("maintenance")
        .maintenance(loonfs_test_support::ids::writer_id("seed-maintenance"));
    let fold = MetadataMaintenanceOptions {
        max_wal_tail_objects: std::num::NonZeroU64::MIN,
        ..Default::default()
    };
    for index in 0..shape.namespaces {
        let namespace_id = shape.namespace(index);
        let mut options = CreateNamespaceOptions::new(shape.actor());
        if shape.rows == Rows::Dense {
            options.access = NamespaceAccess::Acl {
                principal_scope: PrincipalScope::parse(PRINCIPAL_SCOPE).expect("principal scope"),
                root_grants: full_grants(true),
            };
        }
        writer
            .create_namespace(&namespace_id, options)
            .await
            .expect("create namespace");
        let namespace = writer
            .open_namespace(&namespace_id)
            .expect("open namespace");
        namespace
            .commit_candidate(shape.candidate(
                &namespace_id,
                "seed",
                0..shape.folded_entries,
                0..DIRECTORIES,
                None,
            ))
            .await
            .expect("seed commit");
        maintenance
            .maintain_metadata(&namespace_id, fold.clone())
            .await
            .expect("fold");
        let mut next = shape.folded_entries;
        for commit in 0..shape.tail_commits {
            let end = next + shape.tail_entries_per_commit;
            namespace
                .commit_candidate(shape.candidate(
                    &namespace_id,
                    &format!("tail-{commit}"),
                    next..end,
                    commit..commit + 1,
                    Some(2048),
                ))
                .await
                .expect("tail commit");
            next = end;
        }
    }
}

/// Live heap the reader holds after `read` finishes, measured from just
/// after the reader is built.
async fn retained_heap<F, Fut>(
    root: &Path,
    cache: RuntimeCacheConfig,
    read: F,
) -> (usize, LoonFs<ReadOnly>)
where
    F: FnOnce(LoonFs<ReadOnly>) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let store: SharedObjectStore = Arc::new(LocalFsStore::new(root).expect("local store"));
    let reader = LoonFs::reader_with_store(store)
        .runtime_cache(cache)
        .build()
        .await
        .expect("reader");
    let baseline = LIVE.load(Ordering::SeqCst);
    read(reader.clone()).await;
    (LIVE.load(Ordering::SeqCst).saturating_sub(baseline), reader)
}

async fn list_every_directory(shape: &Shape, reader: LoonFs<ReadOnly>) {
    let reader = shape.reader(reader);
    for index in 0..shape.namespaces {
        let namespace_id = shape.namespace(index);
        let namespace = reader.namespace(&namespace_id);
        for directory in 0..DIRECTORIES {
            namespace
                .list_path_entries_page(
                    &format!("/d-{directory:04}"),
                    PageRequest {
                        limit: page_limit(1000),
                        cursor: None,
                    },
                    ListPathEntriesOptions::default(),
                )
                .await
                .expect("list");
        }
    }
}

async fn stat_one_path_per_namespace(shape: &Shape, reader: LoonFs<ReadOnly>) {
    let reader = shape.reader(reader);
    for index in 0..shape.namespaces {
        let namespace = reader.namespace(&shape.namespace(index));
        namespace
            .get_path_entry(
                &shape.entry_path(shape.entries() - 1),
                StatPathOptions::default(),
            )
            .await
            .expect("stat");
    }
}

fn ratio(heap: usize, accounted: usize) -> f64 {
    heap as f64 / accounted.max(1) as f64
}

#[tokio::test]
async fn cache_budgets_charge_the_heap_their_contents_hold() {
    let shapes = [
        Shape {
            label: "directories",
            namespaces: 8,
            folded_entries: 640,
            tail_commits: 4,
            tail_entries_per_commit: 96,
            rows: Rows::Directories,
        },
        Shape {
            label: "large",
            namespaces: 8,
            folded_entries: 256,
            tail_commits: 4,
            tail_entries_per_commit: 48,
            rows: Rows::Large,
        },
        Shape {
            label: "dense",
            namespaces: 3,
            folded_entries: 16,
            tail_commits: 1,
            tail_entries_per_commit: 8,
            rows: Rows::Dense,
        },
    ];
    let mut failures = Vec::new();
    println!("shape | segment budget | segment heap | ratio | projections accounted | projections heap | ratio");
    for shape in &shapes {
        let root = tempfile::tempdir().expect("tempdir");
        let store: SharedObjectStore =
            Arc::new(LocalFsStore::new(root.path()).expect("local store"));
        seed(&store, shape).await;
        drop(store);

        // The segment cache does not report what it holds, so this fills it
        // past its budget and compares the heap with the budget. The last
        // eviction can leave the cache up to one block short of it.
        let (segment_heap, reader) = retained_heap(
            root.path(),
            RuntimeCacheConfig {
                max_cached_namespaces: 0,
                metadata_segment_cache: MetadataSegmentCacheConfig {
                    max_decoded_bytes: SEGMENT_BUDGET_BYTES,
                    ..MetadataSegmentCacheConfig::default()
                },
                ..RuntimeCacheConfig::default()
            },
            |reader| list_every_directory(shape, reader),
        )
        .await;
        assert!(
            reader
                .runtime_cache_stats()
                .metadata_segment_cache_evictions
                > 0,
            "the {} dataset should outgrow the segment budget",
            shape.label
        );
        drop(reader);

        let projections = RuntimeCacheConfig {
            max_cached_namespaces: shape.namespaces,
            metadata_segment_cache: MetadataSegmentCacheConfig {
                max_decoded_bytes: 0,
                ..MetadataSegmentCacheConfig::default()
            },
            ..RuntimeCacheConfig::default()
        };
        let (with_projections, reader) =
            retained_heap(root.path(), projections.clone(), |reader| {
                stat_one_path_per_namespace(shape, reader)
            })
            .await;
        let accounted = reader
            .runtime_cache_stats()
            .wal_tail_projection_cache_cached_decoded_bytes;
        drop(reader);
        let (without_projections, reader) = retained_heap(
            root.path(),
            RuntimeCacheConfig {
                max_cached_wal_tail_projection_rows: 0,
                ..projections
            },
            |reader| stat_one_path_per_namespace(shape, reader),
        )
        .await;
        drop(reader);
        let projection_heap = with_projections.saturating_sub(without_projections);

        let segment_ratio = ratio(segment_heap, SEGMENT_BUDGET_BYTES);
        let projection_ratio = ratio(projection_heap, accounted);
        println!(
            "{} | {SEGMENT_BUDGET_BYTES} | {segment_heap} | {segment_ratio:.2} | {accounted} | {projection_heap} | {projection_ratio:.2}",
            shape.label
        );
        for (what, value) in [("segment", segment_ratio), ("projection", projection_ratio)] {
            if (value - 1.0).abs() > TOLERANCE {
                failures.push(format!(
                    "{} {what} heap is {value:.2}x its accounted bytes",
                    shape.label
                ));
            }
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}
