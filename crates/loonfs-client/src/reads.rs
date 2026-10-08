//! Namespace lifecycle, path reads, revision history, trash, and change feeds.

use super::*;
use crate::transport::{QueryBuilder, SendPolicy};
use loonfs_types::PageRequest;

/// Options for a read of a file's content by path or by inode.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReadFileOptions {
    /// Read the file revision captured by this snapshot instead of the
    /// current one.
    pub snapshot_id: Option<PinId>,
}

/// Options for creating a namespace.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CreateNamespaceOptions {
    /// How sibling names compare, fixed for the namespace's life.
    pub naming: loonfs_types::NamespaceNaming,
}

/// Optional selectors for the change feed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ListChangesOptions {
    /// End the feed at this snapshot's captured sequence.
    pub snapshot_id: Option<PinId>,
}

/// A pager over directory entries.
pub type PathEntriesPager = loonfs_types::Pager<ListPathEntriesResponse, ClientError>;
/// A pager over directory children addressed by inode.
pub type InodeChildrenPager = loonfs_types::Pager<ListInodeChildrenResponse, ClientError>;
/// A pager over retained file revisions.
pub type FileRevisionsPager = loonfs_types::Pager<ListFileRevisionsResponse, ClientError>;
/// A pager over recoverable deletions.
pub type TrashPager = loonfs_types::Pager<ListTrashResponse, ClientError>;
/// A pager over committed changes.
pub type ChangesPager = loonfs_types::Pager<ListChangesResponse, ClientError>;
/// A pager over live snapshots.
pub type SnapshotsPager = loonfs_types::Pager<ListSnapshotsResponse, ClientError>;

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

    /// Lists available snapshots.
    pub fn list_snapshots(&self, namespace_id: &NamespaceId) -> SnapshotsPager {
        let client = self.clone();
        let namespace_id = namespace_id.clone();
        loonfs_types::Pager::new(move |request| {
            let client = client.clone();
            let namespace_id = namespace_id.clone();
            async move { client.snapshots_page(&namespace_id, request).await }
        })
    }

    async fn snapshots_page(
        &self,
        namespace_id: &NamespaceId,
        request: PageRequest<String>,
    ) -> Result<ListSnapshotsResponse> {
        let mut query = QueryBuilder::new(format!(
            "{}/v0/namespaces/{namespace_id}/snapshots",
            self.base_url
        ));
        query.pagination(Some(request.limit.get()), request.cursor.as_deref());
        let url = query.finish();
        self.request_json::<(), ListSnapshotsResponse>(self.get(&url), None, SendPolicy::Retry)
            .await
    }

    /// Extends a snapshot's lifetime. Repeating the same request is safe.
    pub async fn extend_snapshot(
        &self,
        namespace_id: &NamespaceId,
        snapshot_id: &PinId,
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

    /// Deletes the snapshot record. A missing id returns `snapshot_not_found`.
    pub async fn delete_snapshot(
        &self,
        namespace_id: &NamespaceId,
        snapshot_id: &PinId,
    ) -> Result<DeleteSnapshotResponse> {
        let url = format!(
            "{}/v0/namespaces/{namespace_id}/snapshots/{snapshot_id}",
            self.base_url
        );
        self.request_json::<(), DeleteSnapshotResponse>(self.delete(&url), None, SendPolicy::Once)
            .await
    }

    /// Creates an empty case-insensitive namespace with the given ID and
    /// access mode and returns its genesis state.
    pub async fn create_namespace(
        &self,
        namespace_id: &NamespaceId,
        actor_id: &loonfs_types::ActorId,
        access: loonfs_types::NamespaceAccess,
    ) -> Result<NamespaceMetadata> {
        self.create_namespace_with_options(
            namespace_id,
            actor_id,
            access,
            &CreateNamespaceOptions::default(),
        )
        .await
    }

    /// Creates an empty namespace with the given ID, access mode, and
    /// options and returns its genesis state.
    pub async fn create_namespace_with_options(
        &self,
        namespace_id: &NamespaceId,
        actor_id: &loonfs_types::ActorId,
        access: loonfs_types::NamespaceAccess,
        options: &CreateNamespaceOptions,
    ) -> Result<NamespaceMetadata> {
        let url = format!("{}/v0/namespaces", self.base_url);
        // Namespace creation has no durable request identity to reconcile an ambiguous success.
        self.request_json::<_, NamespaceMetadata>(
            self.post(&url).header("Loonfs-Actor", actor_id.as_str()),
            Some(&CreateNamespaceRequest {
                access,
                naming: options.naming,
                namespace_id: namespace_id.clone(),
            }),
            SendPolicy::Once,
        )
        .await
    }

    /// Returns the namespace's current state.
    pub async fn get_namespace(&self, namespace_id: &NamespaceId) -> Result<NamespaceMetadata> {
        // Validated namespace ids are URL-safe by construction, like the
        // other parsed id segments interpolated into paths here and below.
        let url = format!("{}/v0/namespaces/{namespace_id}", self.base_url);
        self.request_json::<(), NamespaceMetadata>(self.get(&url), None, SendPolicy::Retry)
            .await
    }

    /// Deletes the namespace (feature
    /// `filesystem.namespaces.delete`). Pass `expected_head_seq` to delete only if the namespace is
    /// still where you last observed it (`stale_head` on mismatch). Deleting an
    /// already-deleted namespace fails with `namespace_deleted`.
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

    /// Creates a new namespace from the source's current head and returns the
    /// target's state at the fork point.
    pub async fn fork_namespace(
        &self,
        source_namespace_id: &NamespaceId,
        new_namespace_id: &NamespaceId,
        actor: &loonfs_types::ActorId,
    ) -> Result<NamespaceMetadata> {
        self.fork_namespace_with_options(
            source_namespace_id,
            new_namespace_id,
            actor,
            &ForkNamespaceOptions::default(),
        )
        .await
    }

    /// Creates a new namespace from the selected current head or live snapshot and
    /// returns the target's state at the fork point.
    pub async fn fork_namespace_with_options(
        &self,
        source_namespace_id: &NamespaceId,
        new_namespace_id: &NamespaceId,
        actor: &loonfs_types::ActorId,
        options: &ForkNamespaceOptions,
    ) -> Result<NamespaceMetadata> {
        let url = format!(
            "{}/v0/namespaces/{source_namespace_id}/forks",
            self.base_url
        );
        // Namespace forks have no durable request identity to replay after an ambiguous success.
        self.request_json::<_, NamespaceMetadata>(
            self.post(&url).header("Loonfs-Actor", actor.as_str()),
            Some(&ForkNamespaceRequest {
                new_namespace_id: new_namespace_id.clone(),
                snapshot_id: options.snapshot_id.clone(),
            }),
            SendPolicy::Once,
        )
        .await
    }

    /// Lists a directory.
    pub fn list(&self, spec: &NamespacePath) -> PathEntriesPager {
        self.list_with_options(spec, &ListOptions::default())
    }

    /// Lists a directory using the requested projection.
    pub fn list_with_options(
        &self,
        spec: &NamespacePath,
        options: &ListOptions,
    ) -> PathEntriesPager {
        let client = self.clone();
        let spec = spec.clone();
        let options = options.clone();
        loonfs_types::Pager::new(move |request| {
            let client = client.clone();
            let spec = spec.clone();
            let options = options.clone();
            async move { client.path_entries_page(&spec, request, &options).await }
        })
    }

    async fn path_entries_page(
        &self,
        spec: &NamespacePath,
        request: PageRequest<String>,
        options: &ListOptions,
    ) -> Result<ListPathEntriesResponse> {
        let mut query = QueryBuilder::new(format!(
            "{}/v0/namespaces/{}/filesystem/entries",
            self.base_url,
            spec.namespace().as_str()
        ));
        query.push("path", spec.absolute_path().as_str());
        query.pagination(Some(request.limit.get()), request.cursor.as_deref());
        query.push("include_attributes", options.include_attributes);
        if let Some(snapshot_id) = &options.snapshot_id {
            query.push("snapshot_id", snapshot_id.as_str());
        }
        let url = query.finish();
        self.request_json::<(), _>(self.get(&url), None, SendPolicy::Retry)
            .await
    }

    /// Returns path metadata.
    pub async fn stat(&self, spec: &NamespacePath) -> Result<PathEntry> {
        self.stat_with_options(spec, &StatOptions::default()).await
    }

    /// Returns path metadata using the requested projection.
    pub async fn stat_with_options(
        &self,
        spec: &NamespacePath,
        options: &StatOptions,
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
    pub async fn stat_by_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
    ) -> Result<PathEntry> {
        self.stat_by_inode_with_options(namespace_id, inode_id, &StatOptions::default())
            .await
    }

    /// Returns the current entry for a visible inode using the requested
    /// projection.
    pub async fn stat_by_inode_with_options(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        options: &StatOptions,
    ) -> Result<PathEntry> {
        let inode_id = loonfs_types::public_inode_id::encode(inode_id);
        let mut query = QueryBuilder::new(format!(
            "{}/v0/namespaces/{namespace_id}/inodes/{inode_id}",
            self.base_url
        ));
        query.push("include_attributes", options.include_attributes);
        if let Some(snapshot_id) = &options.snapshot_id {
            query.push("snapshot_id", snapshot_id.as_str());
        }
        let url = query.finish();
        self.request_json::<(), _>(self.get(&url), None, SendPolicy::Retry)
            .await
    }

    /// Lists a directory's children by inode.
    pub fn list_by_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
    ) -> InodeChildrenPager {
        self.list_by_inode_with_options(namespace_id, inode_id, &ListOptions::default())
    }

    /// Lists a directory's children by inode, using the requested projection.
    pub fn list_by_inode_with_options(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        options: &ListOptions,
    ) -> InodeChildrenPager {
        let client = self.clone();
        let namespace_id = namespace_id.clone();
        let options = options.clone();
        loonfs_types::Pager::new(move |request| {
            let client = client.clone();
            let namespace_id = namespace_id.clone();
            let options = options.clone();
            async move {
                client
                    .inode_children_page(&namespace_id, inode_id, request, &options)
                    .await
            }
        })
    }

    async fn inode_children_page(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        request: PageRequest<String>,
        options: &ListOptions,
    ) -> Result<ListInodeChildrenResponse> {
        let inode_id = loonfs_types::public_inode_id::encode(inode_id);
        let mut query = QueryBuilder::new(format!(
            "{}/v0/namespaces/{namespace_id}/inodes/{inode_id}/children",
            self.base_url
        ));
        query.pagination(Some(request.limit.get()), request.cursor.as_deref());
        query.push("include_attributes", options.include_attributes);
        if let Some(snapshot_id) = &options.snapshot_id {
            query.push("snapshot_id", snapshot_id.as_str());
        }
        let url = query.finish();
        self.request_json::<(), _>(self.get(&url), None, SendPolicy::Retry)
            .await
    }

    /// Lists a file's revisions by path.
    pub fn list_file_revisions(&self, spec: &NamespacePath) -> FileRevisionsPager {
        let client = self.clone();
        let spec = spec.clone();
        loonfs_types::Pager::new(move |request| {
            let client = client.clone();
            let spec = spec.clone();
            async move { client.file_revisions_page(&spec, request).await }
        })
    }

    async fn file_revisions_page(
        &self,
        spec: &NamespacePath,
        request: PageRequest<String>,
    ) -> Result<ListFileRevisionsResponse> {
        let mut query = QueryBuilder::new(format!(
            "{}/v0/namespaces/{}/filesystem/revisions",
            self.base_url,
            spec.namespace().as_str()
        ));
        query.push("path", spec.absolute_path().as_str());
        query.pagination(Some(request.limit.get()), request.cursor.as_deref());
        let url = query.finish();
        self.request_json::<(), ListFileRevisionsResponse>(self.get(&url), None, SendPolicy::Retry)
            .await
    }

    /// Lists the retained revisions of a file inode.
    pub fn list_file_revisions_by_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
    ) -> FileRevisionsPager {
        let client = self.clone();
        let namespace_id = namespace_id.clone();
        loonfs_types::Pager::new(move |request| {
            let client = client.clone();
            let namespace_id = namespace_id.clone();
            async move {
                client
                    .file_revisions_by_inode_page(&namespace_id, inode_id, request)
                    .await
            }
        })
    }

    async fn file_revisions_by_inode_page(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        request: PageRequest<String>,
    ) -> Result<ListFileRevisionsResponse> {
        let inode_id = loonfs_types::public_inode_id::encode(inode_id);
        let mut query = QueryBuilder::new(format!(
            "{}/v0/namespaces/{namespace_id}/inodes/{inode_id}/revisions",
            self.base_url
        ));
        query.pagination(Some(request.limit.get()), request.cursor.as_deref());
        let url = query.finish();
        self.request_json::<(), ListFileRevisionsResponse>(self.get(&url), None, SendPolicy::Retry)
            .await
    }

    /// Lists the namespace's recoverable deletions.
    pub fn list_trash(&self, namespace_id: &NamespaceId) -> TrashPager {
        let client = self.clone();
        let namespace_id = namespace_id.clone();
        loonfs_types::Pager::new(move |request| {
            let client = client.clone();
            let namespace_id = namespace_id.clone();
            async move { client.trash_page(&namespace_id, request).await }
        })
    }

    async fn trash_page(
        &self,
        namespace_id: &NamespaceId,
        request: PageRequest<String>,
    ) -> Result<ListTrashResponse> {
        let mut query = QueryBuilder::new(format!(
            "{}/v0/namespaces/{}/filesystem/trash",
            self.base_url,
            namespace_id.as_str()
        ));
        query.pagination(Some(request.limit.get()), request.cursor.as_deref());
        let url = query.finish();
        self.request_json::<(), ListTrashResponse>(self.get(&url), None, SendPolicy::Retry)
            .await
    }

    /// Lists committed changes after `after_seq`.
    pub fn list_changes(&self, namespace_id: &NamespaceId, after_seq: ChangeSeq) -> ChangesPager {
        self.list_changes_with_options(namespace_id, after_seq, &ListChangesOptions::default())
    }

    /// Lists committed changes after `after_seq`, bounded by the requested
    /// snapshot.
    pub fn list_changes_with_options(
        &self,
        namespace_id: &NamespaceId,
        after_seq: ChangeSeq,
        options: &ListChangesOptions,
    ) -> ChangesPager {
        let client = self.clone();
        let namespace_id = namespace_id.clone();
        let options = options.clone();
        loonfs_types::Pager::new(move |request: PageRequest<ChangeSeq>| {
            let client = client.clone();
            let namespace_id = namespace_id.clone();
            let options = options.clone();
            async move {
                client
                    .changes_page(
                        &namespace_id,
                        request.cursor.unwrap_or(after_seq),
                        request.limit,
                        &options,
                    )
                    .await
            }
        })
    }

    async fn changes_page(
        &self,
        namespace_id: &NamespaceId,
        after_seq: ChangeSeq,
        limit: loonfs_types::EffectiveLimit,
        options: &ListChangesOptions,
    ) -> Result<ListChangesResponse> {
        let mut query = QueryBuilder::new(format!(
            "{}/v0/namespaces/{namespace_id}/changes",
            self.base_url
        ));
        query.push("after_seq", after_seq.0);
        query.push("limit", limit.get());
        if let Some(snapshot_id) = &options.snapshot_id {
            query.push("snapshot_id", snapshot_id.as_str());
        }
        let url = query.finish();
        self.request_json::<(), ListChangesResponse>(self.get(&url), None, SendPolicy::Retry)
            .await
    }

    /// Returns a file's current bytes.
    pub async fn read_file(&self, spec: &NamespacePath) -> Result<Vec<u8>> {
        self.read_file_with_options(spec, &ReadFileOptions::default())
            .await
    }

    /// Returns a file's bytes: the current revision, or the revision a
    /// snapshot captured when the options name one.
    pub async fn read_file_with_options(
        &self,
        spec: &NamespacePath,
        options: &ReadFileOptions,
    ) -> Result<Vec<u8>> {
        self.request_bytes(&self.file_content_url(spec, None, options.snapshot_id.as_ref()))
            .await
    }

    /// Returns the bytes of one retained revision of a file.
    pub async fn read_file_revision(
        &self,
        spec: &NamespacePath,
        revision_no: RevisionNo,
    ) -> Result<Vec<u8>> {
        self.request_bytes(&self.file_content_url(spec, Some(revision_no), None))
            .await
    }

    /// Streams a file's current content through the server. See
    /// [`Self::read_file_stream_with_options`].
    pub async fn read_file_stream(&self, spec: &NamespacePath) -> Result<PayloadStream> {
        self.read_file_stream_with_options(spec, &ReadFileOptions::default())
            .await
    }

    /// Streams content through the server. Successful completion means the server
    /// verified the whole object; a late verification failure aborts the body.
    /// Bytes already consumed remain provisional until the stream ends cleanly.
    pub async fn read_file_stream_with_options(
        &self,
        spec: &NamespacePath,
        options: &ReadFileOptions,
    ) -> Result<PayloadStream> {
        self.call_for_response_stream(&self.get(&self.file_content_url(
            spec,
            None,
            options.snapshot_id.as_ref(),
        )))
        .await
    }

    /// Streams one retained revision of a file through the server. See
    /// [`Self::read_file_stream_with_options`] for when the bytes are
    /// verified.
    pub async fn read_file_revision_stream(
        &self,
        spec: &NamespacePath,
        revision_no: RevisionNo,
    ) -> Result<PayloadStream> {
        self.call_for_response_stream(&self.get(&self.file_content_url(
            spec,
            Some(revision_no),
            None,
        )))
        .await
    }

    fn file_content_url(
        &self,
        spec: &NamespacePath,
        revision_no: Option<RevisionNo>,
        snapshot_id: Option<&PinId>,
    ) -> String {
        let mut query = QueryBuilder::new(format!(
            "{}/v0/namespaces/{}/filesystem/content",
            self.base_url,
            spec.namespace().as_str()
        ));
        query.push("path", spec.absolute_path().as_str());
        if let Some(revision_no) = revision_no {
            query.push("revision_no", revision_no.0);
        }
        if let Some(snapshot_id) = snapshot_id {
            query.push("snapshot_id", snapshot_id.as_str());
        }
        query.finish()
    }

    /// Returns the current bytes of a visible file inode, wherever it is bound.
    pub async fn read_file_by_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
    ) -> Result<Vec<u8>> {
        self.read_file_by_inode_with_options(namespace_id, inode_id, &ReadFileOptions::default())
            .await
    }

    /// Returns the bytes of a visible file inode, wherever it is bound: the
    /// current revision, or the revision a snapshot captured when the options
    /// name one.
    pub async fn read_file_by_inode_with_options(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        options: &ReadFileOptions,
    ) -> Result<Vec<u8>> {
        self.request_bytes(&self.inode_content_url(
            namespace_id,
            inode_id,
            options.snapshot_id.as_ref(),
        ))
        .await
    }

    /// Streams the current content of a visible file inode through the
    /// server, wherever it is bound. See [`Self::read_file_stream_with_options`]
    /// for when the bytes are verified.
    pub async fn read_file_stream_by_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
    ) -> Result<PayloadStream> {
        self.read_file_stream_by_inode_with_options(
            namespace_id,
            inode_id,
            &ReadFileOptions::default(),
        )
        .await
    }

    /// Streams the content of a visible file inode through the server,
    /// wherever it is bound: the current revision, or the revision a snapshot
    /// captured when the options name one. See
    /// [`Self::read_file_stream_with_options`] for when the bytes are
    /// verified.
    pub async fn read_file_stream_by_inode_with_options(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        options: &ReadFileOptions,
    ) -> Result<PayloadStream> {
        self.call_for_response_stream(&self.get(&self.inode_content_url(
            namespace_id,
            inode_id,
            options.snapshot_id.as_ref(),
        )))
        .await
    }

    /// Reads and verifies one retained file revision by inode identity.
    pub async fn read_file_revision_by_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        revision_no: RevisionNo,
    ) -> Result<Vec<u8>> {
        self.request_bytes(&self.inode_revision_content_url(namespace_id, inode_id, revision_no))
            .await
    }

    /// Streams one retained file revision by inode identity through the
    /// server. See [`Self::read_file_stream_with_options`] for when the bytes
    /// are verified.
    pub async fn read_file_revision_stream_by_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        revision_no: RevisionNo,
    ) -> Result<PayloadStream> {
        self.call_for_response_stream(&self.get(&self.inode_revision_content_url(
            namespace_id,
            inode_id,
            revision_no,
        )))
        .await
    }

    fn inode_content_url(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        snapshot_id: Option<&PinId>,
    ) -> String {
        let inode_id = loonfs_types::public_inode_id::encode(inode_id);
        let mut query = QueryBuilder::new(format!(
            "{}/v0/namespaces/{namespace_id}/inodes/{inode_id}/content",
            self.base_url
        ));
        if let Some(snapshot_id) = snapshot_id {
            query.push("snapshot_id", snapshot_id.as_str());
        }
        query.finish()
    }

    fn inode_revision_content_url(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        revision_no: RevisionNo,
    ) -> String {
        let inode_id = loonfs_types::public_inode_id::encode(inode_id);
        format!(
            "{}/v0/namespaces/{namespace_id}/inodes/{inode_id}/revisions/{revision_no}/content",
            self.base_url
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scripted_transport::{self, Outcome};
    use loonfs_types::api::v0::FilesystemChange;
    use loonfs_types::{ContentId, EntryInodeKind, PathEntryKind};

    fn client_for(transport: &scripted_transport::ScriptedTransport) -> Client {
        Client::with_transport(
            ClientConfig {
                server_url: "http://example.invalid".to_owned(),
                auth_token: None,
                request_timeout_ms: None,
                disable_transient_retry: false,
                ca_cert_path: None,
            },
            transport.clone(),
        )
        .expect("valid client config")
    }

    #[tokio::test]
    async fn an_unknown_inode_kind_decodes_as_unknown_and_a_known_kind_is_unchanged() {
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let listing = serde_json::json!({
            "namespace_id": "demo",
            "path": "/docs",
            "head_seq": 418,
            "entries": [
                {
                    "namespace_id": "demo",
                    "path": "/docs/report.txt",
                    "inode_id": "ino_42",
                    "created_by": "usr_8f3c",
                    "created_at_ms": 1_752_623_000_000_u64,
                    "inode_kind": "file",
                    "revision_no": 7,
                    "size_bytes": 5,
                    "content_ref": ContentRef::blob_v1(
                        namespace_id.clone(),
                        ContentId::generate(),
                        b"hello",
                    ),
                    "revision_committed_by": "render-worker",
                    "revision_committed_at_ms": 1_752_624_000_000_u64,
                    "head_seq": 418,
                    "parent_inode_id": "ino_7",
                    "display_name": "report.txt",
                    "binding_version": "abc"
                },
                {
                    "namespace_id": "demo",
                    "path": "/docs/latest",
                    "inode_id": "ino_43",
                    "created_by": "usr_8f3c",
                    "created_at_ms": 1_752_623_000_000_u64,
                    "inode_kind": "link",
                    "target_inode_id": "ino_42",
                    "head_seq": 418,
                    "parent_inode_id": "ino_7",
                    "display_name": "latest",
                    "binding_version": "def"
                }
            ]
        });
        let trash = serde_json::json!({
            "namespace_id": "demo",
            "head_seq": 418,
            "entries": [{
                "inode_id": "ino_44",
                "inode_kind": "link",
                "deletion_seq": 417,
                "deleted_at_ms": 1_752_625_000_000_u64,
                "deleted_by": "usr_8f3c",
                "deleted_binding": {
                    "parent_inode_id": "ino_7",
                    "name_key": "old",
                    "display_name": "old"
                }
            }]
        });
        let transport = scripted_transport::script([
            Outcome::Success(listing.to_string().into_bytes()),
            Outcome::Success(trash.to_string().into_bytes()),
        ]);
        let client = client_for(&transport);

        let page = client
            .list(&NamespacePath::parse("demo", "/docs").expect("namespace path"))
            .next()
            .await
            .expect("one page")
            .expect("decoded listing");
        assert_eq!(
            serde_json::to_value(&page.entries[0]).expect("serialize file entry"),
            listing["entries"][0]
        );
        let link = &page.entries[1];
        assert_eq!(link.kind, PathEntryKind::Unknown);
        assert_eq!(link.inode_kind(), EntryInodeKind::Unknown);
        assert_eq!(link.inode_id, InodeId(43));
        assert_eq!(link.path.as_str(), "/docs/latest");
        assert_eq!(
            link.display_name.as_ref().map(|name| name.as_str()),
            Some("latest")
        );
        assert_eq!(
            link.binding_version
                .as_ref()
                .map(|version| version.as_str()),
            Some("def")
        );

        let trash = client
            .list_trash(&namespace_id)
            .next()
            .await
            .expect("one page")
            .expect("decoded trash");
        assert_eq!(trash.entries[0].inode_kind, EntryInodeKind::Unknown);
    }

    #[tokio::test]
    async fn an_unknown_event_kind_decodes_as_unknown_and_a_known_event_is_unchanged() {
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let content_id =
            ContentId::parse("con_9f2a6c0e4b7d4a90b13f0d8c5e6a2b41").expect("content id");
        let content_ref = ContentRef::blob_v1(namespace_id.clone(), content_id, b"hello");
        let feed = serde_json::json!({
            "namespace_id": "demo",
            "after_seq": 418,
            "through_seq": 419,
            "changes": [{
                "namespace_id": "demo",
                "commit_id": "c_f3a9c2d4b6e8417a90c5d2f8e1b7a6c0",
                "committed_seq": 419,
                "committed_by": "usr_8f3c",
                "committed_at_ms": 1_752_624_000_000_u64,
                "events": [
                    {
                        "kind": "link_created",
                        "inode_id": "ino_43",
                        "parent_inode_id": "ino_7",
                        "display_name": "latest",
                        "binding_version": "def",
                        "target_inode_id": "ino_42"
                    },
                    {
                        "kind": "content_changed",
                        "inode_id": "ino_42",
                        "revision_no": 8,
                        "content_ref": content_ref
                    }
                ]
            }]
        });
        let transport =
            scripted_transport::script([Outcome::Success(feed.to_string().into_bytes())]);

        let page = client_for(&transport)
            .list_changes(&namespace_id, ChangeSeq(418))
            .next()
            .await
            .expect("one page")
            .expect("decoded feed");
        let events = &page.changes[0].events;
        assert_eq!(events[0], FilesystemChange::Unknown);
        assert_eq!(
            serde_json::to_value(&events[1]).expect("serialize known event"),
            feed["changes"][0]["events"][1]
        );
    }
}
