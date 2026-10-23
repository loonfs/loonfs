//! Point lookups that walk a manifest's runs newest first, checked against
//! the merge of every run, and the segment reads those lookups cost.

use super::*;
use crate::metadata::{
    AccessRevisionRecord, AttributesRevisionRecord, MetadataView, RevisionRecord,
};
use crate::test_support::ops::append_wal_commit;
use loonfs_test_support::stores::ConcurrencyWatchStore;
use loonfs_types::format::segment_blocks::key_range_may_intersect;
use loonfs_types::format::wal::WalDelta;
use loonfs_types::{
    AccessGrants, AccessRevisionNo, AccessRight, AccessRights, AttributeKey, AttributeValue,
    Attributes, AttributesRevisionNo, NamespaceNaming, PrincipalId,
};
use tempfile::TempDir;

/// Files that only the base run binds, so that a newer run's filter can
/// admit one of them by a false positive.
const FILLERS: usize = 512;

/// A namespace whose manifest has a base run and two delta runs, built by
/// ordinary commits, folds, and compactions.
///
/// - `base-only.txt` has its newest revision, attributes, and access row in
///   the base, `middle.txt` in the middle delta, and `newest.txt` in the
///   newest delta.
/// - `removed.txt` is bound in the base and unbound in the middle delta,
///   which also records its tombstone.
/// - `reused.txt` is bound in the base, unbound in the middle delta, and
///   bound to a new inode in the newest delta.
/// - `/trash/inner.txt` has its only revision in the base, and the newest
///   delta records a tombstone for `/trash`.
struct ManyRunFixture {
    _directory: TempDir,
    store: LocalFsStore,
    namespace_id: NamespaceId,
    /// The head of the base run and of each delta run, oldest first.
    run_heads: Vec<ChangeSeq>,
    docs: InodeId,
    /// Every name ever bound under `/docs`, and one that never was.
    names: Vec<NameKey>,
    /// Every inode the fixture created.
    inodes: Vec<InodeId>,
    /// The filler files, which only the base run lists.
    fillers: Vec<(NameKey, InodeId)>,
}

impl ManyRunFixture {
    fn head(&self) -> ChangeSeq {
        *self.run_heads.last().expect("the fixture folds three runs")
    }

    /// The current manifest's segments, and the merge of every row they
    /// list.
    async fn load<'a>(
        &'a self,
        cache: Option<&'a MetadataSegmentCache>,
    ) -> (
        scan::VerifiedMetadataSegments<'a, LocalFsStore>,
        MetadataState,
    ) {
        let manifest_no = current_manifest_number(&self.store, &self.namespace_id).await;
        let verified = load_manifest_segments_for_inspection(
            &self.store,
            cache,
            &self.namespace_id,
            &manifest_no,
        )
        .await
        .expect("load the manifest's segments");
        let merged = load_manifest_materialization_for_inspection(
            &self.store,
            &self.namespace_id,
            manifest_no,
        )
        .await
        .expect("merge every run")
        .metadata_state;
        (verified, merged)
    }
}

async fn head_seq<S: ObjectStore + ?Sized>(store: &S, namespace_id: &NamespaceId) -> ChangeSeq {
    load_namespace_read_state(store, namespace_id, crate::test_support::std_timer())
        .await
        .expect("read the head")
        .seq
}

/// The inode at each of `paths` at the head.
async fn inodes_at<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    paths: &[String],
) -> Vec<InodeId> {
    let (head, state) = load_manifest_projection_metadata_state(store, namespace_id)
        .await
        .expect("project the head");
    paths
        .iter()
        .map(|path| {
            state
                .resolve_visible_path(
                    NamespaceNaming::CaseInsensitive,
                    &AbsolutePath::parse(path).expect("path"),
                    head.seq,
                )
                .expect("a visible path")
                .inode_id
        })
        .collect()
}

/// Commits attribute and access revision `revision` for each of `inodes`.
async fn append_whole_state_revisions<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    inodes: &[InodeId],
    revision: u64,
) {
    let owner = Attributes::new(BTreeMap::from([(
        AttributeKey::parse("owner").expect("attribute key"),
        AttributeValue::parse(format!("revision-{revision}")).expect("attribute value"),
    )]))
    .expect("attribute map");
    let grants = AccessGrants::new(BTreeMap::from([(
        PrincipalId::parse(format!("prn_{revision}")).expect("principal id"),
        AccessRights::from_iter([AccessRight::Read]),
    )]))
    .expect("access grants");
    let mut deltas = Vec::new();
    for (inode_id, delta_index) in inodes.iter().zip((0_u32..).step_by(2)) {
        deltas.push(WalDelta::AppendAttributesRevision {
            delta_index,
            inode_id: *inode_id,
            attributes_revision_no: AttributesRevisionNo(revision),
            attributes: owner.clone(),
        });
        deltas.push(WalDelta::AppendAccessRevision {
            delta_index: delta_index + 1,
            inode_id: *inode_id,
            access_revision_no: AccessRevisionNo(revision),
            boundary: false,
            grants: grants.clone(),
        });
    }
    append_wal_commit(store, namespace_id, deltas, Vec::new())
        .await
        .expect("commit attribute and access revisions");
}

fn docs_paths(names: &[&str]) -> Vec<String> {
    names
        .iter()
        .map(|name| format!("/docs/{name}.txt"))
        .collect()
}

fn docs_names(paths: &[String]) -> impl Iterator<Item = NameKey> + '_ {
    paths.iter().filter_map(|path| {
        let name = path.strip_prefix("/docs/")?;
        Some(NameKey::parse(name).expect("name key"))
    })
}

async fn many_run_fixture() -> ManyRunFixture {
    let directory = tempdir().expect("tempdir");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let namespace_id = NamespaceId::parse("newest-first").expect("namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");

    let mut base_paths = docs_paths(&["base-only", "middle", "newest", "removed", "reused"]);
    base_paths.extend((0..FILLERS).map(|index| format!("/docs/fill-{index:04}.txt")));
    base_paths.extend([
        "/trash/inner.txt".to_owned(),
        "/docs".to_owned(),
        "/trash".to_owned(),
    ]);
    write_files_bytes(
        &store,
        &namespace_id,
        &base_paths[..base_paths.len() - 2],
        b"base\n",
        &context,
    )
    .await
    .expect("write the base files");
    let mut inodes = inodes_at(&store, &namespace_id, &base_paths).await;
    append_whole_state_revisions(&store, &namespace_id, &inodes[..3], 1).await;
    checkpoint_then_compact(
        &store,
        &namespace_id,
        &context,
        MetadataRunPolicy::default(),
    )
    .await;
    let mut run_heads = vec![head_seq(&store, &namespace_id).await];

    let middle_paths = docs_paths(&["middle", "newest", "aaa-middle", "zzz-middle"]);
    write_files_bytes(&store, &namespace_id, &middle_paths, b"middle\n", &context)
        .await
        .expect("write the middle files");
    for path in docs_paths(&["removed", "reused"]) {
        delete_path(&store, &namespace_id, &path, &context, None)
            .await
            .expect("delete a base file");
    }
    append_whole_state_revisions(&store, &namespace_id, &inodes[1..3], 2).await;
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("fold the middle delta run");
    run_heads.push(head_seq(&store, &namespace_id).await);

    let newest_paths = docs_paths(&["newest", "reused", "aaa-newest", "zzz-newest"]);
    write_files_bytes(&store, &namespace_id, &newest_paths, b"newest\n", &context)
        .await
        .expect("write the newest files");
    delete_path(&store, &namespace_id, "/trash", &context, None)
        .await
        .expect("delete the trash directory");
    append_whole_state_revisions(&store, &namespace_id, &inodes[2..3], 3).await;
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("fold the newest delta run");
    run_heads.push(head_seq(&store, &namespace_id).await);

    let docs = inodes[base_paths.len() - 2];
    let fillers = docs_names(&base_paths[5..5 + FILLERS])
        .zip(inodes[5..5 + FILLERS].iter().copied())
        .collect();
    let later_paths = docs_paths(&[
        "reused",
        "aaa-middle",
        "zzz-middle",
        "aaa-newest",
        "zzz-newest",
    ]);
    inodes.extend(inodes_at(&store, &namespace_id, &later_paths).await);
    let mut names: Vec<NameKey> = docs_names(&base_paths)
        .chain(docs_names(&later_paths))
        .chain([NameKey::parse("never.txt").expect("name key")])
        .collect();
    names.sort();
    names.dedup();
    ManyRunFixture {
        _directory: directory,
        store,
        namespace_id,
        run_heads,
        docs,
        names,
        inodes,
        fillers,
    }
}

fn newest_revision(merged: &MetadataState, inode_id: InodeId) -> Option<RevisionRecord> {
    merged
        .revisions()
        .iter()
        .filter(|record| record.inode_id == inode_id)
        .max_by_key(|record| (record.revision_no, record.committed_seq, record.delta_index))
        .cloned()
}

fn newest_attributes(
    merged: &MetadataState,
    inode_id: InodeId,
    visible_seq: ChangeSeq,
) -> Option<AttributesRevisionRecord> {
    merged
        .attributes_revisions()
        .iter()
        .filter(|record| record.inode_id == inode_id && record.committed_seq <= visible_seq)
        .max_by_key(|record| {
            (
                record.attributes_revision_no,
                record.committed_seq,
                record.delta_index,
            )
        })
        .cloned()
}

fn newest_access(
    merged: &MetadataState,
    inode_id: InodeId,
    visible_seq: ChangeSeq,
) -> Option<AccessRevisionRecord> {
    merged
        .access_revisions()
        .iter()
        .filter(|record| record.inode_id == inode_id && record.committed_seq <= visible_seq)
        .max_by_key(|record| {
            (
                record.access_revision_no,
                record.committed_seq,
                record.delta_index,
            )
        })
        .cloned()
}

/// Checks the lookups at the head, which see every manifest row, and at
/// the heads of the older runs, which read below the manifest's head.
#[tokio::test]
async fn newest_first_lookups_return_what_the_merge_of_every_run_returns() {
    let fixture = many_run_fixture().await;
    let (verified, merged) = fixture.load(None).await;
    let tiers: Vec<RunTier> = verified
        .scan_runs
        .iter()
        .filter(|run| {
            run.segments.iter().any(|family_segments| {
                family_segments.family == ApiMetadataRowFamily::Revisions
                    && !family_segments.segments.is_empty()
            })
        })
        .map(|run| run.tier)
        .collect();
    assert_eq!(tiers, [RunTier::Delta, RunTier::Delta, RunTier::Base]);

    for visible_seq in fixture.run_heads.iter().copied() {
        let view = MetadataView::over_manifest_segments(&verified, visible_seq);
        for name in &fixture.names {
            assert_eq!(
                view.bound_child(fixture.docs, name)
                    .await
                    .expect("slot lookup"),
                merged.bound_child_at_seq(fixture.docs, name, visible_seq),
                "the slot {name:?} at {visible_seq:?}"
            );
        }
        for inode_id in fixture.inodes.iter().copied() {
            let context = format!("inode {inode_id:?} at {visible_seq:?}");
            assert_eq!(
                view.latest_attributes_revision(inode_id)
                    .await
                    .expect("attributes"),
                newest_attributes(&merged, inode_id, visible_seq),
                "{context}"
            );
            assert_eq!(
                view.latest_access_revision(inode_id).await.expect("access"),
                newest_access(&merged, inode_id, visible_seq),
                "{context}"
            );
            // The revision lookup reads its manifest rows whatever the
            // view's sequence, so the merge it matches reads them all too.
            assert_eq!(
                view.latest_revision_record(inode_id)
                    .await
                    .expect("revision"),
                newest_revision(&merged, inode_id),
                "{context}"
            );
        }
    }
}

/// Checks the rules that combine lookups: an unbound name, a rebound name,
/// a deleted file's own tombstone, and a tombstone on a file's ancestor,
/// each in a newer run than the rows it hides.
#[tokio::test]
async fn visibility_over_newest_first_lookups_matches_the_merge() {
    let fixture = many_run_fixture().await;
    let (verified, merged) = fixture.load(None).await;
    let head = fixture.head();
    let view = MetadataView::over_manifest_segments(&verified, head);
    for name in &fixture.names {
        assert_eq!(
            view.visible_child(fixture.docs, name)
                .await
                .expect("visible child"),
            merged.visible_child(fixture.docs, name, head),
            "the slot {name:?}"
        );
    }
    for inode_id in fixture.inodes.iter().copied() {
        assert_eq!(
            view.current_parent_binding_for_child(inode_id)
                .await
                .expect("parent binding"),
            merged.current_parent_binding_for_child(inode_id, head),
            "inode {inode_id:?}"
        );
        assert_eq!(
            view.latest_revision_head(inode_id)
                .await
                .expect("revision head"),
            merged
                .visible_inode(inode_id, head)
                .and_then(|_| newest_revision(&merged, inode_id)),
            "inode {inode_id:?}"
        );
    }

    // The fixture lists its base files first, in the order it wrote them.
    let (removed, reused, inner) = (
        fixture.inodes[3],
        fixture.inodes[4],
        fixture.inodes[5 + FILLERS],
    );
    let slot = |name: &str| NameKey::parse(name).expect("name key");
    let unbound = view
        .bound_child(fixture.docs, &slot("removed.txt"))
        .await
        .expect("slot")
        .expect("the middle delta unbinds the name");
    assert!(!unbound.is_bound());
    let rebound = view
        .bound_child(fixture.docs, &slot("reused.txt"))
        .await
        .expect("slot")
        .expect("the newest delta binds the name again");
    assert_ne!(rebound.child_inode_id, reused);
    for hidden in [removed, inner] {
        assert!(view
            .latest_revision_record(hidden)
            .await
            .expect("revision")
            .is_some());
        assert_eq!(
            view.latest_revision_head(hidden)
                .await
                .expect("revision head"),
            None
        );
    }
}

/// Whether a delta run's filter admits `probe` for a segment whose key
/// range meets `prefix`.
async fn a_delta_run_admits(
    store: &LocalFsStore,
    verified: &scan::VerifiedMetadataSegments<'_, LocalFsStore>,
    family: ApiMetadataRowFamily,
    prefix: &str,
    probe: &str,
) -> bool {
    let upper_bound = string_prefix_upper_bound(prefix);
    let descriptors = verified
        .scan_runs
        .iter()
        .filter(|run| run.tier == RunTier::Delta)
        .flat_map(|run| &run.segments)
        .filter(|family_segments| family_segments.family == family)
        .flat_map(|family_segments| &family_segments.segments)
        .filter(|descriptor| {
            key_range_may_intersect(
                &descriptor.min_row_key,
                &descriptor.max_row_key,
                descriptor.row_count,
                prefix,
                upper_bound.as_deref(),
            )
        });
    for descriptor in descriptors {
        let filter =
            load::load_segment_filter(store, None, &load::SessionBlockMemo::default(), descriptor)
                .await
                .expect("load a filter");
        if filter.may_contain(probe) {
            return true;
        }
    }
    false
}

#[tokio::test]
async fn a_false_positive_in_a_newer_run_falls_through_to_the_run_with_the_row() {
    let fixture = many_run_fixture().await;
    let cache = MetadataSegmentCache::unshared(usize::MAX);
    let (verified, merged) = fixture.load(Some(&cache)).await;
    let docs = fixture.docs;
    let mut binding_false_positive = None;
    let mut revision_false_positive = None;
    for (name, inode_id) in &fixture.fillers {
        let prefix = lookup_keys::direntry_bind_prefix(docs, name);
        let probe = lookup_keys::direntry_bind_probe(docs, name);
        let family = ApiMetadataRowFamily::DirentryBinds;
        if binding_false_positive.is_none()
            && a_delta_run_admits(&fixture.store, &verified, family, &prefix, &probe).await
        {
            binding_false_positive = Some(name.clone());
        }
        let prefix = lookup_keys::revision_prefix(*inode_id);
        let probe = lookup_keys::revision_probe(*inode_id);
        let family = ApiMetadataRowFamily::Revisions;
        if revision_false_positive.is_none()
            && a_delta_run_admits(&fixture.store, &verified, family, &prefix, &probe).await
        {
            revision_false_positive = Some(*inode_id);
        }
    }
    let name = binding_false_positive.expect("a delta filter admits a filler's name");
    let inode_id = revision_false_positive.expect("a delta filter admits a filler's revisions");

    let head = fixture.head();
    let view = MetadataView::over_manifest_segments(&verified, head);
    let false_positives = cache.stats().filter_false_positives;
    let binding = view.bound_child(docs, &name).await.expect("slot lookup");
    assert!(binding.as_ref().is_some_and(|binding| binding.is_bound()));
    assert_eq!(binding, merged.bound_child_at_seq(docs, &name, head));
    let revision = view
        .latest_revision_record(inode_id)
        .await
        .expect("revision");
    assert!(revision.is_some());
    assert_eq!(revision, newest_revision(&merged, inode_id));
    assert!(
        cache.stats().filter_false_positives >= false_positives + 2,
        "both lookups open a delta segment that has no row for the key: {:?}",
        cache.stats()
    );
}

/// Runs that list `/docs/hot.txt`'s revisions and attributes and versions
/// of the `/docs/flip.txt` slot: the base and six delta runs.
const HOT_KEY_RUNS: u64 = 7;

/// Builds a namespace where `/docs/hot.txt` has a revision and an attribute
/// revision in each of [`HOT_KEY_RUNS`] runs, and `/docs/flip.txt` is bound
/// to a new inode in each of them. Returns the inodes of `/docs` and of
/// `/docs/hot.txt`.
async fn hot_key_fixture<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> (InodeId, InodeId) {
    let context = test_context();
    bootstrap_namespace(store, namespace_id, &context)
        .await
        .expect("bootstrap");
    let paths = docs_paths(&["hot", "flip", "aaa", "zzz"]);
    write_files_bytes(store, namespace_id, &paths, b"base\n", &context)
        .await
        .expect("write the base files");
    let inodes = inodes_at(store, namespace_id, &["/docs".to_owned(), paths[0].clone()]).await;
    let (docs, hot) = (inodes[0], inodes[1]);
    append_whole_state_revisions(store, namespace_id, &[hot], 1).await;
    checkpoint_then_compact(store, namespace_id, &context, MetadataRunPolicy::default()).await;
    for run in 2..=HOT_KEY_RUNS {
        // Names at both ends of the directory keep each run's binding range
        // across the slot, so only the filters can rule a run out.
        let paths = docs_paths(&["hot", &format!("aaa-{run}"), &format!("zzz-{run}")]);
        write_files_bytes(store, namespace_id, &paths, b"delta\n", &context)
            .await
            .expect("write the delta files");
        delete_path(store, namespace_id, "/docs/flip.txt", &context, None)
            .await
            .expect("unbind the slot");
        write_file_bytes(
            store,
            namespace_id,
            "/docs/flip.txt",
            b"flip\n",
            &context,
            None,
        )
        .await
        .expect("bind the slot to a new inode");
        append_whole_state_revisions(store, namespace_id, &[hot], run).await;
        create_checkpoint(store, namespace_id, &context)
            .await
            .expect("fold a delta run");
    }
    (docs, hot)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HotLookup {
    Revision,
    Binding,
    Attributes,
}

async fn look_up_hot_key<S: ObjectStore + ?Sized>(
    view: &MetadataView<'_, '_, S>,
    lookup: HotLookup,
    docs: InodeId,
    hot: InodeId,
) {
    match lookup {
        HotLookup::Revision => {
            let revision = view.latest_revision_record(hot).await.expect("revision");
            assert_eq!(
                revision.map(|revision| revision.revision_no),
                Some(RevisionNo(HOT_KEY_RUNS))
            );
        }
        HotLookup::Binding => {
            let slot = NameKey::parse("flip.txt").expect("name key");
            let binding = view.bound_child(docs, &slot).await.expect("slot lookup");
            assert!(binding.is_some_and(|binding| binding.is_bound()));
        }
        HotLookup::Attributes => {
            let attributes = view
                .latest_attributes_revision(hot)
                .await
                .expect("attributes");
            assert_eq!(
                attributes.map(|record| record.attributes_revision_no),
                Some(AttributesRevisionNo(HOT_KEY_RUNS))
            );
        }
    }
}

/// Measures, for a key that [`HOT_KEY_RUNS`] runs list, the segment GETs
/// that a cold lookup sends (including any that it cancels), and the decoded
/// blocks that a warm lookup reads from the block cache.
#[tokio::test]
async fn a_lookup_of_a_key_in_many_runs_reads_only_the_newest_runs() {
    let directory = tempdir().expect("tempdir");
    let store = ConcurrencyWatchStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::metadata_segment(),
    );
    let namespace_id = NamespaceId::parse("hot-key").expect("namespace id");
    let (docs, hot) = hot_key_fixture(&store, &namespace_id).await;
    let manifest_no = current_manifest_number(&store, &namespace_id).await;
    let head = head_seq(&store, &namespace_id).await;
    let cache = MetadataSegmentCache::unshared(usize::MAX);
    let mut measured = Vec::new();
    for lookup in [
        HotLookup::Revision,
        HotLookup::Binding,
        HotLookup::Attributes,
    ] {
        let verified =
            load_manifest_segments_for_inspection(&store, None, &namespace_id, &manifest_no)
                .await
                .expect("load segments");
        let sent = store.reads().total;
        look_up_hot_key(
            &MetadataView::over_manifest_segments(&verified, head),
            lookup,
            docs,
            hot,
        )
        .await;
        let cold_gets = store.reads().total - sent;
        // The first warm pass fills the block cache, and the second one,
        // on a view with an empty block memo, reads every block from it.
        let mut warm = (0, 0);
        for _ in 0..2 {
            let verified = load_manifest_segments_for_inspection(
                &store,
                Some(&cache),
                &namespace_id,
                &manifest_no,
            )
            .await
            .expect("load segments");
            let (sent, hits) = (store.reads().total, cache.stats().hits);
            look_up_hot_key(
                &MetadataView::over_manifest_segments(&verified, head),
                lookup,
                docs,
                hot,
            )
            .await;
            warm = (store.reads().total - sent, cache.stats().hits - hits);
        }
        let (warm_gets, warm_blocks) = warm;
        assert_eq!(warm_gets, 0, "a warm {lookup:?} lookup reads no segment");
        measured.push((lookup, cold_gets, warm_blocks));
    }
    assert_eq!(
        measured,
        [
            (HotLookup::Revision, 4, 2),
            (HotLookup::Binding, 4, 2),
            (HotLookup::Attributes, 4, 2),
        ],
        "(lookup, cold segment GETs, warm decoded blocks) for a key in {HOT_KEY_RUNS} runs: \
         a cold lookup sends one GET for each run in its window, and a warm one decodes the \
         index and data block of the newest run"
    );
}
