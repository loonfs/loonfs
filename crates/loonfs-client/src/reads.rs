//! Namespace lifecycle, path reads, revision history, trash, and change feeds.

use super::*;
use crate::transport::{QueryBuilder, SendPolicy};

/// Selects a retained revision or snapshot for a file read. Set at most one.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReadFileOptions {
    /// Read one retained revision instead of the current file.
    pub revision_no: Option<RevisionNo>,
    /// Read the file revision captured by this snapshot.
    pub snapshot_id: Option<CheckpointId>,
}

/// Optional selectors for one change-feed page.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ListChangesOptions {
    /// Maximum number of changes in the page.
    pub limit: Option<u32>,
    /// End the feed at this snapshot's captured sequence.
    pub snapshot_id: Option<CheckpointId>,
}

/// A pager over directory entries.
pub type PathEntriesPager = loonfs_api::Pager<ListPathEntriesResponse, ClientError>;
/// A pager over directory children addressed by inode.
pub type InodeChildrenPager = loonfs_api::Pager<ListInodeChildrenResponse, ClientError>;
/// A pager over retained file revisions.
pub type FileRevisionsPager = loonfs_api::Pager<ListFileRevisionsResponse, ClientError>;
/// A pager over recoverable deletions.
pub type TrashPager = loonfs_api::Pager<ListTrashResponse, ClientError>;
/// A pager over committed changes.
pub type ChangesPager = loonfs_api::Pager<ListChangesResponse, ClientError>;
/// A pager over live snapshots.
pub type SnapshotsPager = loonfs_api::Pager<ListSnapshotsResponse, ClientError>;

impl Client {
    /// Saves the namespace's current state for a limited time.
    /// Retrying this request starts a distinct attempt.
    pub async fn create_snapshot(
        &self,
        namespace_id: &NamespaceId,
        name: &str,
        ttl_ms: u64,
    ) -> Result<SnapshotSummary> {
        let url = format!("{}/v0/namespaces/{namespace_id}/snapshots", self.base_url);
        self.request_json(
            self.post(&url),
            Some(&CreateSnapshotRequest {
                name: name.to_owned(),
                ttl_ms,
            }),
            SendPolicy::Once,
        )
        .await
    }

    /// Creates a snapshot pager beginning at `cursor`.
    pub fn list_snapshots_pager(
        &self,
        namespace_id: &NamespaceId,
        page_size: Option<u32>,
        cursor: Option<String>,
    ) -> SnapshotsPager {
        let client = self.clone();
        let namespace_id = namespace_id.clone();
        loonfs_api::Pager::new(cursor, move |cursor| {
            let client = client.clone();
            let namespace_id = namespace_id.clone();
            async move {
                client
                    .list_snapshots_page(&namespace_id, page_size, cursor.as_deref())
                    .await
            }
        })
    }

    /// Lists one bounded page of available snapshots.
    pub async fn list_snapshots_page(
        &self,
        namespace_id: &NamespaceId,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<ListSnapshotsResponse> {
        let mut query = QueryBuilder::new(format!(
            "{}/v0/namespaces/{namespace_id}/snapshots",
            self.base_url
        ));
        query.pagination(limit, cursor);
        let url = query.finish();
        self.request_json::<(), ListSnapshotsResponse>(self.get(&url), None, SendPolicy::Retry)
            .await
    }

    /// Extends a snapshot's lifetime. Repeating the same request is safe.
    pub async fn extend_snapshot(
        &self,
        namespace_id: &NamespaceId,
        snapshot_id: &CheckpointId,
        ttl_ms: u64,
    ) -> Result<SnapshotSummary> {
        let url = format!(
            "{}/v0/namespaces/{namespace_id}/snapshots/{snapshot_id}/extend",
            self.base_url
        );
        self.request_json(
            self.post(&url),
            Some(&ExtendSnapshotRequest { ttl_ms }),
            SendPolicy::Retry,
        )
        .await
    }

    /// Releases a snapshot. Releasing it again succeeds.
    pub async fn release_snapshot(
        &self,
        namespace_id: &NamespaceId,
        snapshot_id: &CheckpointId,
    ) -> Result<ReleaseSnapshotResponse> {
        let url = format!(
            "{}/v0/namespaces/{namespace_id}/snapshots/{snapshot_id}/release",
            self.base_url
        );
        self.request_json::<(), ReleaseSnapshotResponse>(self.post(&url), None, SendPolicy::Retry)
            .await
    }

    /// Creates an empty namespace with the given ID and returns its genesis state.
    pub async fn create_namespace(&self, namespace_id: &NamespaceId) -> Result<Namespace> {
        let url = format!("{}/v0/namespaces", self.base_url);
        // Namespace creation has no durable request identity to reconcile an ambiguous success.
        self.request_json::<_, Namespace>(
            self.post(&url),
            Some(&CreateNamespaceRequest {
                namespace_id: namespace_id.clone(),
            }),
            SendPolicy::Once,
        )
        .await
    }

    /// Returns the namespace's current state.
    pub async fn get_namespace(&self, namespace_id: &NamespaceId) -> Result<Namespace> {
        // Validated namespace ids are URL-safe by construction, like the
        // other parsed id segments interpolated into paths here and below.
        let url = format!("{}/v0/namespaces/{namespace_id}", self.base_url);
        self.request_json::<(), Namespace>(self.get(&url), None, SendPolicy::Retry)
            .await
    }

    /// Deletes a namespace (feature `filesystem.namespaces.delete`): terminal,
    /// and the id is permanently retired. Pass `expected_head_seq` to delete
    /// only if the namespace is still where you last observed it
    /// (`stale_head` on mismatch). Deleting an already-deleted namespace
    /// fails with `namespace_deleted`.
    pub async fn delete_namespace(
        &self,
        namespace_id: &NamespaceId,
        expected_head_seq: Option<ChangeSeq>,
    ) -> Result<DeleteNamespaceResponse> {
        let mut query =
            QueryBuilder::new(format!("{}/v0/namespaces/{namespace_id}", self.base_url));
        if let Some(expected) = expected_head_seq {
            query.push("expected_head_seq", expected.0);
        }
        let url = query.finish();
        // The expected head is a precondition, not an idempotency key for an ambiguous delete.
        self.request_json::<(), DeleteNamespaceResponse>(self.delete(&url), None, SendPolicy::Once)
            .await
    }

    /// Creates a new namespace from the source namespace's current state and
    /// returns the target's state at the fork point.
    pub async fn fork_namespace(
        &self,
        source_namespace_id: &NamespaceId,
        new_namespace_id: &NamespaceId,
    ) -> Result<Namespace> {
        let url = format!(
            "{}/v0/namespaces/{source_namespace_id}/forks",
            self.base_url
        );
        // Namespace forks have no durable request identity to replay after an ambiguous success.
        self.request_json::<_, Namespace>(
            self.post(&url),
            Some(&ForkNamespaceRequest {
                new_namespace_id: new_namespace_id.clone(),
            }),
            SendPolicy::Once,
        )
        .await
    }

    /// Creates a directory pager beginning at `cursor`.
    pub fn list_path_entries_pager(
        &self,
        spec: &NamespacePath,
        page_size: Option<u32>,
        cursor: Option<String>,
        options: &ListPathEntriesOptions,
    ) -> PathEntriesPager {
        let client = self.clone();
        let spec = spec.clone();
        let options = options.clone();
        loonfs_api::Pager::new(cursor, move |cursor| {
            let client = client.clone();
            let spec = spec.clone();
            let options = options.clone();
            async move {
                client
                    .list_path_entries_page(&spec, page_size, cursor.as_deref(), &options)
                    .await
            }
        })
    }

    /// Lists one directory page using the requested projection.
    pub async fn list_path_entries_page(
        &self,
        spec: &NamespacePath,
        limit: Option<u32>,
        cursor: Option<&str>,
        options: &ListPathEntriesOptions,
    ) -> Result<ListPathEntriesResponse> {
        let mut query = QueryBuilder::new(format!(
            "{}/v0/namespaces/{}/filesystem/entries",
            self.base_url,
            spec.namespace().as_str()
        ));
        query.push("path", spec.absolute_path().as_str());
        query.pagination(limit, cursor);
        query.push("include_attributes", options.include_attributes);
        if let Some(snapshot_id) = &options.snapshot_id {
            query.push("snapshot_id", snapshot_id.as_str());
        }
        let url = query.finish();
        self.request_json::<(), _>(self.get(&url), None, SendPolicy::Retry)
            .await
    }

    /// Returns path metadata using the requested projection.
    pub async fn get_path_entry(
        &self,
        spec: &NamespacePath,
        options: &StatPathOptions,
    ) -> Result<PathEntry> {
        let mut query = QueryBuilder::new(format!(
            "{}/v0/namespaces/{}/filesystem/entry",
            self.base_url,
            spec.namespace().as_str()
        ));
        query.push("path", spec.absolute_path().as_str());
        query.push("include_attributes", options.include_attributes);
        if let Some(snapshot_id) = &options.snapshot_id {
            query.push("snapshot_id", snapshot_id.as_str());
        }
        let url = query.finish();
        self.request_json::<(), _>(self.get(&url), None, SendPolicy::Retry)
            .await
    }

    /// Returns the current entry for a visible inode.
    pub async fn get_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        options: &StatPathOptions,
    ) -> Result<PathEntry> {
        let inode_id = loonfs_api::public_inode_id::encode(inode_id);
        let mut query = QueryBuilder::new(format!(
            "{}/v0/namespaces/{namespace_id}/inodes/{inode_id}",
            self.base_url
        ));
        query.push("include_attributes", options.include_attributes);
        let url = query.finish();
        self.request_json::<(), _>(self.get(&url), None, SendPolicy::Retry)
            .await
    }

    /// Creates a children pager for one directory inode beginning at `cursor`.
    pub fn list_inode_children_pager(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        page_size: Option<u32>,
        cursor: Option<String>,
        options: &ListInodeChildrenOptions,
    ) -> InodeChildrenPager {
        let client = self.clone();
        let namespace_id = namespace_id.clone();
        let options = options.clone();
        loonfs_api::Pager::new(cursor, move |cursor| {
            let client = client.clone();
            let namespace_id = namespace_id.clone();
            let options = options.clone();
            async move {
                client
                    .list_inode_children_page(
                        &namespace_id,
                        inode_id,
                        page_size,
                        cursor.as_deref(),
                        &options,
                    )
                    .await
            }
        })
    }

    /// Creates a path-based revision pager beginning at `cursor`.
    pub fn list_file_revisions_pager(
        &self,
        spec: &NamespacePath,
        page_size: Option<u32>,
        cursor: Option<String>,
    ) -> FileRevisionsPager {
        let client = self.clone();
        let spec = spec.clone();
        loonfs_api::Pager::new(cursor, move |cursor| {
            let client = client.clone();
            let spec = spec.clone();
            async move {
                client
                    .list_file_revisions_page(&spec, page_size, cursor.as_deref())
                    .await
            }
        })
    }

    /// Creates an inode-based revision pager beginning at `cursor`.
    pub fn list_file_revisions_by_inode_pager(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        page_size: Option<u32>,
        cursor: Option<String>,
    ) -> FileRevisionsPager {
        let client = self.clone();
        let namespace_id = namespace_id.clone();
        loonfs_api::Pager::new(cursor, move |cursor| {
            let client = client.clone();
            let namespace_id = namespace_id.clone();
            async move {
                client
                    .list_file_revisions_by_inode_page(
                        &namespace_id,
                        inode_id,
                        page_size,
                        cursor.as_deref(),
                    )
                    .await
            }
        })
    }

    /// Creates a trash pager beginning at `cursor`.
    pub fn list_trash_pager(
        &self,
        namespace_id: &NamespaceId,
        page_size: Option<u32>,
        cursor: Option<String>,
    ) -> TrashPager {
        let client = self.clone();
        let namespace_id = namespace_id.clone();
        loonfs_api::Pager::new(cursor, move |cursor| {
            let client = client.clone();
            let namespace_id = namespace_id.clone();
            async move {
                client
                    .list_trash_page(&namespace_id, page_size, cursor.as_deref())
                    .await
            }
        })
    }

    /// Creates a change-feed pager beginning after `after_seq`.
    pub fn list_changes_pager(
        &self,
        namespace_id: &NamespaceId,
        after_seq: ChangeSeq,
        page_size: Option<u32>,
    ) -> ChangesPager {
        self.changes_pager(namespace_id, after_seq, page_size, None)
    }

    /// Creates a snapshot-bounded change-feed pager beginning after `after_seq`.
    pub fn list_changes_pager_at_snapshot(
        &self,
        namespace_id: &NamespaceId,
        after_seq: ChangeSeq,
        page_size: Option<u32>,
        snapshot_id: &CheckpointId,
    ) -> ChangesPager {
        self.changes_pager(
            namespace_id,
            after_seq,
            page_size,
            Some(snapshot_id.clone()),
        )
    }

    fn changes_pager(
        &self,
        namespace_id: &NamespaceId,
        after_seq: ChangeSeq,
        page_size: Option<u32>,
        snapshot_id: Option<CheckpointId>,
    ) -> ChangesPager {
        let client = self.clone();
        let namespace_id = namespace_id.clone();
        loonfs_api::Pager::new(Some(after_seq), move |after_seq| {
            let client = client.clone();
            let namespace_id = namespace_id.clone();
            let options = ListChangesOptions {
                limit: page_size,
                snapshot_id: snapshot_id.clone(),
            };
            async move {
                client
                    .list_changes(
                        &namespace_id,
                        after_seq.expect("change pager should carry a sequence"),
                        &options,
                    )
                    .await
            }
        })
    }

    /// Lists one page of a directory's children by inode, using the requested
    /// projection.
    pub async fn list_inode_children_page(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        limit: Option<u32>,
        cursor: Option<&str>,
        options: &ListInodeChildrenOptions,
    ) -> Result<ListInodeChildrenResponse> {
        let inode_id = loonfs_api::public_inode_id::encode(inode_id);
        let mut query = QueryBuilder::new(format!(
            "{}/v0/namespaces/{namespace_id}/inodes/{inode_id}/children",
            self.base_url
        ));
        query.pagination(limit, cursor);
        query.push("include_attributes", options.include_attributes);
        let url = query.finish();
        self.request_json::<(), _>(self.get(&url), None, SendPolicy::Retry)
            .await
    }

    /// Reads file content from a retained revision or snapshot.
    pub async fn get_file_bytes(
        &self,
        spec: &NamespacePath,
        options: &ReadFileOptions,
    ) -> Result<Vec<u8>> {
        let mut query = QueryBuilder::new(format!(
            "{}/v0/namespaces/{}/filesystem/content",
            self.base_url,
            spec.namespace().as_str()
        ));
        query.push("path", spec.absolute_path().as_str());
        if let Some(revision_no) = options.revision_no {
            query.push("revision_no", revision_no.0);
        }
        if let Some(snapshot_id) = &options.snapshot_id {
            query.push("snapshot_id", snapshot_id.as_str());
        }
        let url = query.finish();
        self.request_bytes(&url).await
    }

    /// Reads and verifies one retained file revision by inode identity.
    pub async fn get_file_revision_bytes_by_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        revision_no: RevisionNo,
    ) -> Result<Vec<u8>> {
        let inode_id = loonfs_api::public_inode_id::encode(inode_id);
        let url = format!(
            "{}/v0/namespaces/{namespace_id}/inodes/{inode_id}/revisions/{revision_no}/content",
            self.base_url
        );
        self.request_bytes(&url).await
    }

    /// Returns one page of revisions for a file.
    pub async fn list_file_revisions_page(
        &self,
        spec: &NamespacePath,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<ListFileRevisionsResponse> {
        let mut query = QueryBuilder::new(format!(
            "{}/v0/namespaces/{}/filesystem/revisions",
            self.base_url,
            spec.namespace().as_str()
        ));
        query.push("path", spec.absolute_path().as_str());
        query.pagination(limit, cursor);
        let url = query.finish();
        self.request_json::<(), ListFileRevisionsResponse>(self.get(&url), None, SendPolicy::Retry)
            .await
    }

    /// Returns one page of retained revisions for a file inode.
    pub async fn list_file_revisions_by_inode_page(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<ListFileRevisionsResponse> {
        let inode_id = loonfs_api::public_inode_id::encode(inode_id);
        let mut query = QueryBuilder::new(format!(
            "{}/v0/namespaces/{namespace_id}/inodes/{inode_id}/revisions",
            self.base_url
        ));
        query.pagination(limit, cursor);
        let url = query.finish();
        self.request_json::<(), ListFileRevisionsResponse>(self.get(&url), None, SendPolicy::Retry)
            .await
    }

    /// Returns one page of recoverable deletions in a namespace.
    pub async fn list_trash_page(
        &self,
        namespace_id: &NamespaceId,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<ListTrashResponse> {
        let mut query = QueryBuilder::new(format!(
            "{}/v0/namespaces/{}/filesystem/trash",
            self.base_url,
            namespace_id.as_str()
        ));
        query.pagination(limit, cursor);
        let url = query.finish();
        self.request_json::<(), ListTrashResponse>(self.get(&url), None, SendPolicy::Retry)
            .await
    }

    /// Returns committed changes using the requested page and snapshot bounds.
    pub async fn list_changes(
        &self,
        namespace_id: &NamespaceId,
        after_seq: ChangeSeq,
        options: &ListChangesOptions,
    ) -> Result<ListChangesResponse> {
        let mut query = QueryBuilder::new(format!(
            "{}/v0/namespaces/{namespace_id}/changes",
            self.base_url
        ));
        query.push("after_seq", after_seq.0);
        if let Some(limit) = options.limit {
            query.push("limit", limit);
        }
        if let Some(snapshot_id) = &options.snapshot_id {
            query.push("snapshot_id", snapshot_id.as_str());
        }
        let url = query.finish();
        self.request_json::<(), ListChangesResponse>(self.get(&url), None, SendPolicy::Retry)
            .await
    }
}
