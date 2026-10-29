//! [`LoadedMetadataView`]: a verified, seq-pinned view of one namespace that
//! core reads run against, and the loads that build the view.

use super::current_files::resolve_visible_inode;
use crate::authorize::{Absence, ReadAccess};
use crate::engine::RuntimeReadContext;
use crate::error::{CoreError, Result};
use crate::manifest::load_basis_metadata_segments;
use crate::metadata::{LeafRevisionPreload, MetadataView};
use crate::namespace::basis::ReadBasis;
use crate::namespace::head_cache::WalTailCacheKey;
use crate::namespace::state::NamespaceReadState;
use crate::namespace::store::NamespaceStore;
use crate::object_cache::ObjectCache;
use crate::path::mutation_path::parse_absolute_path_for_core;
use crate::phase::Phase;
use crate::segment::VerifiedMetadataSegments;
use crate::wal::load_replayed_wal_tail;
use crate::wal::ProjectedWalTail;
use loonfs_objectstore::ObjectStore;
use loonfs_types::options::AttributeInclusion;
use loonfs_types::{AccessRight, ChangeSeq, InodeId, ManifestNo, PathEntry};
use std::collections::HashMap;
use std::sync::Arc;

/// Identifies the metadata snapshot used by one read.
///
/// The head sequence, manifest number, and manifest head sequence distinguish
/// a stale snapshot from a manifest that is missing expected data. These are
/// also the fields used to key the WAL-tail cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ReadAnchor {
    pub(crate) head_seq: ChangeSeq,
    pub(crate) manifest_no: ManifestNo,
    pub(crate) manifest_head_seq: ChangeSeq,
}

impl<S: ObjectStore> NamespaceStore<S> {
    /// Loads the view of a live namespace at `context`'s head, through this
    /// namespace's caches.
    pub(crate) async fn load_metadata_view(
        &self,
        context: &RuntimeReadContext,
    ) -> Result<LoadedMetadataView<'_, S>> {
        let head = &context.read_basis.head;
        crate::namespace::control::ensure_namespace_live(&head.namespace_id, head.status)?;
        self.load_metadata_view_for_authorization(context).await
    }

    /// Loads access rows from a deleted head when deletion does not determine authorization.
    pub(crate) async fn load_metadata_view_for_authorization(
        &self,
        context: &RuntimeReadContext,
    ) -> Result<LoadedMetadataView<'_, S>> {
        let ReadBasis { head, basis } = &context.read_basis;
        crate::namespace::control::ensure_head_identity(head, &self.namespace_id)?;
        let head = head.clone();
        let manifest_no = basis.manifest_no();
        let loaded_basis =
            load_basis_metadata_segments(&self.store, Some(&self.segment_cache), basis).await?;
        let manifest_head = loaded_basis.replay_head(&head);
        let anchor = ReadAnchor {
            head_seq: head.seq,
            manifest_no,
            manifest_head_seq: manifest_head.seq,
        };
        let wal_tail = match &context.tail {
            Some(wal_tail) => Arc::clone(wal_tail),
            None => {
                let cache_key = WalTailCacheKey {
                    namespace_id: self.namespace_id.clone(),
                    manifest_no,
                    head_seq: head.seq,
                };
                self.head_and_wal_tail_cache
                    .get_or_load_tail(&cache_key, || async {
                        load_replayed_wal_tail(
                            &self.store,
                            &manifest_head,
                            &head,
                            &loaded_basis.base_state,
                        )
                        .await
                        .map(|replayed| Arc::new(replayed.projected_tail))
                        .map_err(CoreError::MetadataProjection)
                    })
                    .await?
            }
        };
        Ok(LoadedMetadataView {
            head,
            segments: loaded_basis.segments,
            wal_tail,
            anchor,
            object_cache: &self.object_cache,
        })
    }

    #[cfg(test)]
    pub(crate) async fn load_current_metadata_view(&self) -> Result<LoadedMetadataView<'_, S>> {
        let loaded = self
            .load_read_anchor(crate::test_support::observed_now())
            .await?;
        let context = RuntimeReadContext {
            read_basis: ReadBasis {
                basis: loaded.basis(),
                head: loaded.read_state,
            },
            tail: None,
        };
        self.load_metadata_view(&context).await
    }
}

/// A coherent, seq-pinned namespace read view.
///
/// Every read the engine serves for one pinned context runs over one of
/// these; the view is crate-internal, and consumers reach it through the
/// engine's read methods.
pub(crate) struct LoadedMetadataView<'a, S: ObjectStore + ?Sized> {
    pub(super) head: NamespaceReadState,
    pub(super) segments: VerifiedMetadataSegments<'a, S>,
    pub(crate) wal_tail: Arc<ProjectedWalTail>,
    pub(crate) anchor: ReadAnchor,
    /// Where file reads look for content objects before the store.
    pub(crate) object_cache: &'a ObjectCache,
}

impl<'a, S: ObjectStore + ?Sized> LoadedMetadataView<'a, S> {
    pub(crate) fn head(&self) -> &NamespaceReadState {
        &self.head
    }

    pub(crate) fn retention_floor_seq(&self) -> ChangeSeq {
        self.segments.manifest().payload().retention_floor_seq
    }

    #[cfg(test)]
    pub(crate) fn wal_tail(&self) -> &ProjectedWalTail {
        &self.wal_tail
    }

    #[cfg(test)]
    pub(crate) fn projected_metadata_view(&self) -> MetadataView<'_, '_, S> {
        self.metadata_view()
    }

    #[tracing::instrument(
        level = "debug",
        name = "loonfs.phase",
        err(level = "debug"),
        skip_all,
        fields(
            phase = Phase::WalkPath.as_str(),
            head_seq = self.anchor.head_seq.0,
            manifest_no = self.anchor.manifest_no.0,
            manifest_head_seq = self.anchor.manifest_head_seq.0,
        )
    )]
    pub(crate) async fn resolve_path(
        &self,
        absolute_path: &str,
        attributes: AttributeInclusion,
        access: &ReadAccess<'_, S>,
    ) -> Result<PathEntry> {
        let absolute_path = parse_absolute_path_for_core(absolute_path)?;
        // One session serves the resolution and the entry build: the walk's
        // preloaded probes (including the leaf's head revision) are exactly
        // what the entry build reads back as cache hits.
        let mut session = self.metadata_view().session();
        let resolved = session
            .resolve_visible_path(&absolute_path, LeafRevisionPreload::Preload)
            .await?;
        access
            .require(
                &mut session,
                resolved.inode_id,
                AccessRight::Read.into(),
                Absence::Path(absolute_path.as_str()),
            )
            .await?;
        self.build_authoritative_path_entry_with_session(&mut session, &resolved, attributes)
            .await
    }

    /// Returns the current entry for a visible inode without listing a
    /// directory.
    pub(crate) async fn stat_inode(
        &self,
        inode_id: InodeId,
        attributes: AttributeInclusion,
        access: &ReadAccess<'_, S>,
    ) -> Result<PathEntry> {
        let mut session = self.metadata_view().session();
        let mut ancestor_paths = HashMap::new();
        let resolved = resolve_visible_inode(&mut session, &mut ancestor_paths, inode_id)
            .await?
            .ok_or(CoreError::InodeNotFound(inode_id))?;
        access
            .require(
                &mut session,
                inode_id,
                AccessRight::Read.into(),
                Absence::Inode,
            )
            .await?;
        self.build_authoritative_path_entry_with_session(&mut session, &resolved, attributes)
            .await
    }

    pub(crate) fn metadata_view(&self) -> MetadataView<'_, '_, S> {
        MetadataView::from_loaded_head(&self.head, &self.segments, &self.wal_tail)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::engine::CommitCandidate;
    use crate::path::read::listing::resolve_visible_child;
    use crate::path::write::{CommitIntent, FilesystemOperation};
    use crate::test_support::WriterAt;
    use bytes::Bytes;
    use futures::poll;
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use loonfs_test_support::ids::attribute_key;
    use loonfs_test_support::stores::{
        replace_object, BlockingStore, KeyPredicate, OperationSelector, RecordingStore,
    };
    use loonfs_types::AbsolutePath;
    use loonfs_types::NamespaceId;
    use loonfs_types::{AttributeValue, AttributesRevisionNo, CommitId, ErrorCode};
    use std::collections::BTreeMap;
    use std::pin::pin;
    use tempfile::TempDir;

    fn context() -> WriterAt {
        crate::test_support::writer_at("reader-tests", 1)
    }

    /// Bootstraps a namespace holding `/docs` with one annotated child
    /// directory and one bare child directory, so an entry build has both an
    /// inode with attributes and an inode without.
    async fn namespace_with_annotated_children() -> (TempDir, LocalFsStore, NamespaceId) {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let context = context();
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
            .create()
            .await
            .expect("bootstrap");

        for (commit_id, operation) in [
            (
                "create-docs",
                FilesystemOperation::CreateDirectory {
                    path: AbsolutePath::parse("/docs").expect("path"),
                    parents: false,
                },
            ),
            (
                "create-annotated",
                FilesystemOperation::CreateDirectory {
                    path: AbsolutePath::parse("/docs/annotated").expect("path"),
                    parents: false,
                },
            ),
            (
                "create-bare",
                FilesystemOperation::CreateDirectory {
                    path: AbsolutePath::parse("/docs/bare").expect("path"),
                    parents: false,
                },
            ),
            (
                "annotate",
                FilesystemOperation::UpdateAttributes {
                    path: AbsolutePath::parse("/docs/annotated").expect("path"),
                    set: BTreeMap::from([(
                        attribute_key("owner"),
                        AttributeValue::parse("ops").expect("valid attribute value"),
                    )]),
                    remove: Vec::new(),
                    expected_inode_id: None,
                    expected_attributes_revision_no: None,
                },
            ),
        ] {
            crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
                .publish_candidates(vec![CommitCandidate::new(CommitIntent::single(
                    CommitId::parse(commit_id).expect("commit id"),
                    loonfs_test_support::test_actor(),
                    None,
                    operation,
                ))])
                .await
                .into_iter()
                .next()
                .expect("one result")
                .expect("commit lands");
        }
        (temp_dir, store, namespace_id)
    }

    // This corruption test deliberately rewrites physical WAL bytes.
    #[allow(clippy::disallowed_methods)]
    #[tokio::test]
    async fn a_tail_that_replays_to_another_head_is_refused_by_reads_and_publishes_alike() {
        let (_temp_dir, store, namespace_id) = namespace_with_annotated_children().await;
        let head = crate::test_support::namespace_store(&store, &namespace_id)
            .load_read_state()
            .await
            .expect("state");
        let key = loonfs_objectstore::keys::wal_object(&namespace_id, &head.wal_no);
        let bytes = store.get(&key, None).await.expect("WAL").expect("exists");
        let mut payload = loonfs_types::format::wal::decode_wal_object_envelope_zstd(&bytes)
            .expect("decode")
            .into_payload();
        payload.next_inode_id = InodeId(payload.next_inode_id.0 + 1);
        let bytes = loonfs_types::format::wal::encode_wal_object_envelope_zstd(payload)
            .expect("encode")
            .into_bytes();
        replace_object(&store, &key, Bytes::from(bytes))
            .await
            .expect("corrupt allocation");

        let read = crate::test_support::namespace_store(&store, &namespace_id)
            .load_current_metadata_view()
            .await
            .err()
            .expect("a head the tail does not reconstruct is corruption");
        assert_eq!(read.code(), ErrorCode::NamespaceCorrupt);

        let published =
            crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context())
                .publish_candidates(vec![CommitCandidate::new(CommitIntent::single(
                    CommitId::parse("create-after-tamper").expect("commit id"),
                    loonfs_test_support::test_actor(),
                    None,
                    FilesystemOperation::CreateDirectory {
                        path: AbsolutePath::parse("/docs/after").expect("path"),
                        parents: false,
                    },
                ))])
                .await
                .into_iter()
                .next()
                .expect("one result")
                .expect_err("the publish path refuses the same state");
        assert_eq!(published.code(), read.code());
    }

    /// Checks that two loads at one head that both find no cached tail
    /// replay the tail once between them, with one set of WAL GETs.
    #[tokio::test]
    async fn concurrent_loads_at_one_head_replay_the_tail_once() {
        let (_temp_dir, store, namespace_id) = namespace_with_annotated_children().await;
        let wal_objects =
            || KeyPredicate::prefix(loonfs_objectstore::keys::wal_prefix(&namespace_id));
        let store = RecordingStore::new(
            BlockingStore::new(store, wal_objects(), OperationSelector::Get),
            wal_objects(),
        );
        let anchor = crate::test_support::namespace_store(&store, &namespace_id)
            .load_read_anchor(crate::test_support::observed_now())
            .await
            .expect("read anchor");
        let context = RuntimeReadContext {
            read_basis: ReadBasis {
                basis: anchor.basis(),
                head: anchor.read_state,
            },
            tail: None,
        };
        // Once the first load caches the manifest, a load reads nothing from
        // the store before the WAL objects of the tail.
        let segment_cache = Arc::new(crate::cache::MetadataSegmentCache::unshared(1 << 20));
        let (alone, shared) = (
            Arc::new(crate::cache::HeadAndWalTailCache::unshared(1 << 20)),
            Arc::new(crate::cache::HeadAndWalTailCache::unshared(1 << 20)),
        );
        let namespace = |cache: &Arc<crate::cache::HeadAndWalTailCache>| NamespaceStore {
            segment_cache: Arc::clone(&segment_cache),
            head_and_wal_tail_cache: Arc::clone(cache),
            ..crate::test_support::namespace_store(&store, &namespace_id)
        };

        store.reset();
        namespace(&alone)
            .load_metadata_view(&context)
            .await
            .expect("a load on its own");
        let one_replay = store.counts().gets;
        assert!(one_replay > 0, "the tail has WAL objects");

        store.reset();
        store.inner().arm();
        let (first_namespace, second_namespace) = (namespace(&shared), namespace(&shared));
        let mut first = pin!(first_namespace.load_metadata_view(&context));
        let mut second = pin!(second_namespace.load_metadata_view(&context));
        assert!(poll!(first.as_mut()).is_pending());
        assert!(poll!(pin!(store.inner().wait_until_blocked())).is_ready());
        assert!(poll!(second.as_mut()).is_pending());
        store.inner().release();
        let (first, second) = futures::join!(first, second);
        let (first, second) = (first.expect("first load"), second.expect("second load"));
        assert!(Arc::ptr_eq(&first.wal_tail, &second.wal_tail));
        assert_eq!(store.counts().gets, one_replay, "one set of WAL GETs");
        let stats = shared.tail_stats();
        assert_eq!((stats.misses, stats.inserts), (1, 1));
    }

    #[tokio::test]
    async fn an_entry_build_projects_grouped_attributes_or_none() {
        let (_temp_dir, store, namespace_id) = namespace_with_annotated_children().await;
        let namespace = crate::test_support::namespace_store(&store, &namespace_id);
        let view = namespace
            .load_current_metadata_view()
            .await
            .expect("load view");

        let annotated = view
            .resolve_path(
                "/docs/annotated",
                AttributeInclusion::Include,
                &ReadAccess::live(crate::authorize::Authorizer::Unrestricted),
            )
            .await
            .expect("stat annotated");
        assert_eq!(
            annotated
                .attributes
                .as_ref()
                .map(|projection| projection.attributes_revision_no),
            Some(AttributesRevisionNo(1))
        );
        assert_eq!(
            annotated
                .attributes
                .as_ref()
                .and_then(|projection| projection.attributes.get(&attribute_key("owner")))
                .cloned(),
            Some(AttributeValue::parse("ops").expect("valid attribute value"))
        );

        let bare = view
            .resolve_path(
                "/docs/bare",
                AttributeInclusion::Include,
                &ReadAccess::live(crate::authorize::Authorizer::Unrestricted),
            )
            .await
            .expect("stat bare");
        assert_eq!(
            bare.attributes.as_ref().map(|projection| {
                (
                    projection.attributes_revision_no,
                    projection.attributes.len(),
                )
            }),
            Some((AttributesRevisionNo(0), 0))
        );

        let omitted = view
            .resolve_path(
                "/docs/annotated",
                AttributeInclusion::Omit,
                &ReadAccess::live(crate::authorize::Authorizer::Unrestricted),
            )
            .await
            .expect("stat without attributes");
        assert!(omitted.attributes.is_none());
    }

    #[tokio::test]
    async fn a_listing_reads_attributes_only_when_it_projects_them() {
        let (_temp_dir, store, namespace_id) = namespace_with_annotated_children().await;
        let namespace = crate::test_support::namespace_store(&store, &namespace_id);
        let view = namespace
            .load_current_metadata_view()
            .await
            .expect("load view");
        let directory = AbsolutePath::parse("/docs").expect("path");

        for (projection, expected_calls, expect_projected) in [
            (AttributeInclusion::Omit, 0, false),
            (AttributeInclusion::Include, 2, true),
        ] {
            let mut session = view.metadata_view().session();
            let resolved = session
                .resolve_visible_path(&directory, LeafRevisionPreload::Skip)
                .await
                .expect("resolve directory");
            let children = session
                .visible_children_page_by_name_key(resolved.inode_id, None, 16)
                .await
                .expect("children");
            assert_eq!(children.len(), 2);
            for child in children {
                let entry = view
                    .build_authoritative_path_entry_with_session(
                        &mut session,
                        &resolve_visible_child(&resolved, &child.binding),
                        projection,
                    )
                    .await
                    .expect("build entry");
                assert_eq!(entry.attributes.is_some(), expect_projected);
            }
            assert_eq!(
                session.counters().latest_attributes_calls,
                expected_calls,
                "{projection:?} made the wrong number of attribute lookups"
            );
        }
    }

    #[tokio::test]
    async fn a_projected_listing_reads_its_attributes_in_one_wave() {
        let (_temp_dir, store, namespace_id) = namespace_with_annotated_children().await;
        let namespace = crate::test_support::namespace_store(&store, &namespace_id);
        let view = namespace
            .load_current_metadata_view()
            .await
            .expect("load view");

        for (projection, expected_preloads) in [
            (AttributeInclusion::Omit, 0),
            (AttributeInclusion::Include, 2),
        ] {
            let mut session = view.metadata_view().session();
            let directory = AbsolutePath::parse("/docs").expect("path");
            let resolved = session
                .resolve_visible_path(&directory, LeafRevisionPreload::Skip)
                .await
                .expect("resolve directory");
            let children = session
                .visible_children_page_by_name_key(resolved.inode_id, None, 16)
                .await
                .expect("children");
            let child_inode_ids: Vec<InodeId> = children
                .iter()
                .map(|child| child.binding.child_inode_id)
                .collect();
            if projection == AttributeInclusion::Include {
                session
                    .preload_attributes(&child_inode_ids)
                    .await
                    .expect("preload");
            }
            assert_eq!(
                session.counters().preload_attribute_lookups,
                expected_preloads
            );

            for child in children {
                view.build_authoritative_path_entry_with_session(
                    &mut session,
                    &resolve_visible_child(&resolved, &child.binding),
                    projection,
                )
                .await
                .expect("build entry");
            }
            // Nothing beyond the wave: every per-entry lookup was seeded.
            assert_eq!(
                session.counters().preload_attribute_lookups,
                expected_preloads
            );
        }
    }
}
