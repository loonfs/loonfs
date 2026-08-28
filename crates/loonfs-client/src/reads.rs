//! Namespace lifecycle, path reads, revision history, trash, and change feeds.

use super::*;
use crate::transport::{append_optional_pagination_query, append_query_param};

/// Optional selectors for a path-based file-content read.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReadFileOptions {
    /// Read one retained revision instead of the current file.
    pub revision_no: Option<RevisionNo>,
    /// Read the file revision captured by this live snapshot.
    ///
    /// The server rejects a request that supplies both selectors.
    pub snapshot_id: Option<CheckpointId>,
}

/// Optional selectors for one change-feed page.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ListChangesOptions {
    /// Maximum number of changes in the page.
    pub limit: Option<u32>,
    /// End the feed at this live snapshot's captured sequence.
    pub snapshot_id: Option<CheckpointId>,
}

/// Fetches directory pages as needed.
///
/// [`Self::next`] returns one page with its metadata. [`Self::collect_up_to`]
/// returns at most the requested number of entries and saves unused entries
/// for later calls.
#[must_use]
pub struct PathEntriesPager {
    client: Client,
    spec: NamespacePath,
    page_size: Option<u32>,
    cursor: Option<String>,
    options: ListPathEntriesOptions,
    pending: Option<ListPathEntriesResponse>,
    exhausted: bool,
}

impl PathEntriesPager {
    /// Returns the next directory page, or `None` after exhaustion.
    pub async fn next(&mut self) -> Option<Result<ListPathEntriesResponse>> {
        if let Some(page) = self.pending.take() {
            return Some(Ok(page));
        }
        if self.exhausted {
            return None;
        }
        let page = self
            .client
            .list_path_entries_page(
                &self.spec,
                self.page_size,
                self.cursor.as_deref(),
                &self.options,
            )
            .await;
        Some(page.inspect(|page| {
            self.cursor = page.next_cursor.clone();
            self.exhausted = self.cursor.is_none();
        }))
    }

    /// Returns at most `max_items` entries.
    ///
    /// Unused entries from the last page remain available to later calls.
    pub async fn collect_up_to(&mut self, max_items: usize) -> Result<Vec<PathEntry>> {
        let mut entries = Vec::new();
        while entries.len() < max_items {
            let Some(page) = self.next().await else {
                break;
            };
            let mut page = page?;
            let take = (max_items - entries.len()).min(page.entries.len());
            if take < page.entries.len() {
                let remaining = page.entries.split_off(take);
                entries.extend(page.entries);
                page.entries = remaining;
                self.pending = Some(page);
                break;
            }
            entries.extend(page.entries);
        }
        Ok(entries)
    }
}

/// Fetches child pages of one directory inode as needed.
///
/// [`Self::next`] returns one page with its metadata. [`Self::collect_up_to`]
/// returns at most the requested number of entries and saves unused entries
/// for later calls.
#[must_use]
pub struct InodeChildrenPager {
    client: Client,
    namespace_id: NamespaceId,
    inode_id: InodeId,
    page_size: Option<u32>,
    cursor: Option<String>,
    options: ListInodeChildrenOptions,
    pending: Option<ListInodeChildrenResponse>,
    exhausted: bool,
}

impl InodeChildrenPager {
    /// Returns the next children page, or `None` after exhaustion.
    pub async fn next(&mut self) -> Option<Result<ListInodeChildrenResponse>> {
        if let Some(page) = self.pending.take() {
            return Some(Ok(page));
        }
        if self.exhausted {
            return None;
        }
        let page = self
            .client
            .list_inode_children_page(
                &self.namespace_id,
                self.inode_id,
                self.page_size,
                self.cursor.as_deref(),
                &self.options,
            )
            .await;
        Some(page.inspect(|page| {
            self.cursor = page.next_cursor.clone();
            self.exhausted = self.cursor.is_none();
        }))
    }

    /// Returns at most `max_items` entries.
    ///
    /// Unused entries from the last page remain available to later calls.
    pub async fn collect_up_to(&mut self, max_items: usize) -> Result<Vec<PathEntry>> {
        let mut entries = Vec::new();
        while entries.len() < max_items {
            let Some(page) = self.next().await else {
                break;
            };
            let mut page = page?;
            let take = (max_items - entries.len()).min(page.entries.len());
            if take < page.entries.len() {
                let remaining = page.entries.split_off(take);
                entries.extend(page.entries);
                page.entries = remaining;
                self.pending = Some(page);
                break;
            }
            entries.extend(page.entries);
        }
        Ok(entries)
    }
}

enum FileRevisionsTarget {
    Path(NamespacePath),
    Inode {
        namespace_id: NamespaceId,
        inode_id: InodeId,
    },
}

/// Fetches file-revision pages as needed for a path or inode.
#[must_use]
pub struct FileRevisionsPager {
    client: Client,
    target: FileRevisionsTarget,
    page_size: Option<u32>,
    cursor: Option<String>,
    pending: Option<ListFileRevisionsResponse>,
    exhausted: bool,
}

impl FileRevisionsPager {
    /// Returns the next revision page, or `None` after exhaustion.
    pub async fn next(&mut self) -> Option<Result<ListFileRevisionsResponse>> {
        if let Some(page) = self.pending.take() {
            return Some(Ok(page));
        }
        if self.exhausted {
            return None;
        }
        let page = match &self.target {
            FileRevisionsTarget::Path(spec) => {
                self.client
                    .list_file_revisions_page(spec, self.page_size, self.cursor.as_deref())
                    .await
            }
            FileRevisionsTarget::Inode {
                namespace_id,
                inode_id,
            } => {
                self.client
                    .list_file_revisions_by_inode_page(
                        namespace_id,
                        *inode_id,
                        self.page_size,
                        self.cursor.as_deref(),
                    )
                    .await
            }
        };
        Some(page.inspect(|page| {
            self.cursor = page.next_cursor.clone();
            self.exhausted = self.cursor.is_none();
        }))
    }

    /// Returns at most `max_items` revisions.
    ///
    /// Unused revisions from the last page remain available to later calls.
    pub async fn collect_up_to(&mut self, max_items: usize) -> Result<Vec<FileRevision>> {
        let mut revisions = Vec::new();
        while revisions.len() < max_items {
            let Some(page) = self.next().await else {
                break;
            };
            let mut page = page?;
            let take = (max_items - revisions.len()).min(page.revisions.len());
            if take < page.revisions.len() {
                let remaining = page.revisions.split_off(take);
                revisions.extend(page.revisions);
                page.revisions = remaining;
                self.pending = Some(page);
                break;
            }
            revisions.extend(page.revisions);
        }
        Ok(revisions)
    }
}

/// Fetches recoverable-deletion pages as needed.
#[must_use]
pub struct TrashPager {
    client: Client,
    namespace_id: NamespaceId,
    page_size: Option<u32>,
    cursor: Option<String>,
    pending: Option<ListTrashResponse>,
    exhausted: bool,
}

impl TrashPager {
    /// Returns the next trash page, or `None` after exhaustion.
    pub async fn next(&mut self) -> Option<Result<ListTrashResponse>> {
        if let Some(page) = self.pending.take() {
            return Some(Ok(page));
        }
        if self.exhausted {
            return None;
        }
        let page = self
            .client
            .list_trash_page(&self.namespace_id, self.page_size, self.cursor.as_deref())
            .await;
        Some(page.inspect(|page| {
            self.cursor = page.next_cursor.clone();
            self.exhausted = self.cursor.is_none();
        }))
    }

    /// Returns at most `max_items` deletions.
    ///
    /// Unused deletions from the last page remain available to later calls.
    pub async fn collect_up_to(&mut self, max_items: usize) -> Result<Vec<TrashEntry>> {
        let mut entries = Vec::new();
        while entries.len() < max_items {
            let Some(page) = self.next().await else {
                break;
            };
            let mut page = page?;
            let take = (max_items - entries.len()).min(page.entries.len());
            if take < page.entries.len() {
                let remaining = page.entries.split_off(take);
                entries.extend(page.entries);
                page.entries = remaining;
                self.pending = Some(page);
                break;
            }
            entries.extend(page.entries);
        }
        Ok(entries)
    }
}

/// Fetches change-feed pages as needed, using sequence numbers to resume.
#[must_use]
pub struct ChangesPager {
    client: Client,
    namespace_id: NamespaceId,
    after_seq: ChangeSeq,
    page_size: Option<u32>,
    snapshot_id: Option<CheckpointId>,
    pending: Option<ListChangesResponse>,
    exhausted: bool,
}

impl ChangesPager {
    /// Returns the next change page, or `None` after exhaustion.
    pub async fn next(&mut self) -> Option<Result<ListChangesResponse>> {
        if let Some(page) = self.pending.take() {
            return Some(Ok(page));
        }
        if self.exhausted {
            return None;
        }
        let page = self
            .client
            .list_changes_with_options(
                &self.namespace_id,
                self.after_seq,
                &ListChangesOptions {
                    limit: self.page_size,
                    snapshot_id: self.snapshot_id.clone(),
                },
            )
            .await;
        Some(page.inspect(|page| {
            self.exhausted = page.next_after_seq.is_none();
            if let Some(next_after_seq) = page.next_after_seq {
                self.after_seq = next_after_seq;
            }
        }))
    }

    /// Returns at most `max_items` changes.
    ///
    /// Unused changes from the last page remain available to later calls.
    pub async fn collect_up_to(&mut self, max_items: usize) -> Result<Vec<CommittedChange>> {
        let mut changes = Vec::new();
        while changes.len() < max_items {
            let Some(page) = self.next().await else {
                break;
            };
            let mut page = page?;
            let take = (max_items - changes.len()).min(page.changes.len());
            if take < page.changes.len() {
                let remaining = page.changes.split_off(take);
                changes.extend(page.changes);
                page.changes = remaining;
                self.pending = Some(page);
                break;
            }
            changes.extend(page.changes);
        }
        Ok(changes)
    }
}

impl Client {
    /// Creates an empty namespace with the given ID and returns its genesis state.
    pub async fn create_namespace(&self, namespace_id: &NamespaceId) -> Result<Namespace> {
        let url = format!("{}/v0/namespaces", self.base_url);
        // Namespace creation has no durable request identity to reconcile an ambiguous success.
        self.request_json_once::<_, Namespace>(
            self.post(&url),
            Some(&CreateNamespaceRequest {
                namespace_id: namespace_id.clone(),
            }),
        )
        .await
    }

    /// Returns the namespace's current state.
    pub async fn get_namespace(&self, namespace_id: &NamespaceId) -> Result<Namespace> {
        // Validated namespace ids are URL-safe by construction, like the
        // other parsed id segments interpolated into paths here and below.
        let url = format!("{}/v0/namespaces/{namespace_id}", self.base_url);
        self.request_json::<(), Namespace>(self.get(&url), None)
            .await
    }

    /// Deletes a namespace (feature `core.namespaces.delete`): terminal,
    /// and the id is permanently retired. Pass `expected_head_seq` to delete
    /// only if the namespace is still where you last observed it
    /// (`stale_head` on mismatch). Deleting an already-deleted namespace
    /// fails with `namespace_deleted`.
    pub async fn delete_namespace(
        &self,
        namespace_id: &NamespaceId,
        expected_head_seq: Option<ChangeSeq>,
    ) -> Result<DeleteNamespaceResponse> {
        let mut url = format!("{}/v0/namespaces/{namespace_id}", self.base_url);
        if let Some(expected) = expected_head_seq {
            url.push_str(&format!("?expected_head_seq={}", expected.0));
        }
        // The expected head is a precondition, not an idempotency key for an ambiguous delete.
        self.request_json_once::<(), DeleteNamespaceResponse>(self.delete(&url), None)
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
        self.request_json_once::<_, Namespace>(
            self.post(&url),
            Some(&ForkNamespaceRequest {
                new_namespace_id: new_namespace_id.clone(),
            }),
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
        PathEntriesPager {
            client: self.clone(),
            spec: spec.clone(),
            page_size,
            cursor,
            options: options.clone(),
            pending: None,
            exhausted: false,
        }
    }

    /// Lists one directory page using the requested projection.
    pub async fn list_path_entries_page(
        &self,
        spec: &NamespacePath,
        limit: Option<u32>,
        cursor: Option<&str>,
        options: &ListPathEntriesOptions,
    ) -> Result<ListPathEntriesResponse> {
        let mut url = format!(
            "{}/v0/namespaces/{}/filesystem/entries?path={}",
            self.base_url,
            spec.namespace().as_str(),
            urlencoding::encode(spec.absolute_path().as_str())
        );
        let mut has_query = true;
        append_optional_pagination_query(&mut url, &mut has_query, limit, cursor);
        append_query_param(
            &mut url,
            &mut has_query,
            "include_attributes",
            &options.include_attributes.to_string(),
        );
        if let Some(snapshot_id) = &options.snapshot_id {
            append_query_param(
                &mut url,
                &mut has_query,
                "snapshot_id",
                snapshot_id.as_str(),
            );
        }
        self.request_json::<(), _>(self.get(&url), None).await
    }

    /// Returns path metadata using the requested projection.
    pub async fn get_path_entry(
        &self,
        spec: &NamespacePath,
        options: &StatPathOptions,
    ) -> Result<PathEntry> {
        let mut url = format!(
            "{}/v0/namespaces/{}/filesystem/entry?path={}",
            self.base_url,
            spec.namespace().as_str(),
            urlencoding::encode(spec.absolute_path().as_str())
        );
        let mut has_query = true;
        append_query_param(
            &mut url,
            &mut has_query,
            "include_attributes",
            &options.include_attributes.to_string(),
        );
        if let Some(snapshot_id) = &options.snapshot_id {
            append_query_param(
                &mut url,
                &mut has_query,
                "snapshot_id",
                snapshot_id.as_str(),
            );
        }
        self.request_json::<(), _>(self.get(&url), None).await
    }

    /// Returns the current entry for a visible inode.
    pub async fn get_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        options: &StatPathOptions,
    ) -> Result<PathEntry> {
        let inode_id = loonfs_api::public_inode_id::encode(inode_id);
        let mut url = format!(
            "{}/v0/namespaces/{namespace_id}/inodes/{inode_id}",
            self.base_url
        );
        let mut has_query = false;
        append_query_param(
            &mut url,
            &mut has_query,
            "include_attributes",
            &options.include_attributes.to_string(),
        );
        self.request_json::<(), _>(self.get(&url), None).await
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
        InodeChildrenPager {
            client: self.clone(),
            namespace_id: namespace_id.clone(),
            inode_id,
            page_size,
            cursor,
            options: options.clone(),
            pending: None,
            exhausted: false,
        }
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
        let mut url = format!(
            "{}/v0/namespaces/{namespace_id}/inodes/{inode_id}/children",
            self.base_url
        );
        let mut has_query = false;
        append_optional_pagination_query(&mut url, &mut has_query, limit, cursor);
        append_query_param(
            &mut url,
            &mut has_query,
            "include_attributes",
            &options.include_attributes.to_string(),
        );
        self.request_json::<(), _>(self.get(&url), None).await
    }

    /// Reads the file's current contents into memory.
    pub async fn get_file_bytes(&self, spec: &NamespacePath) -> Result<Vec<u8>> {
        self.get_file_bytes_with_options(spec, &ReadFileOptions::default())
            .await
    }

    /// Reads the requested file revision into memory.
    pub async fn get_file_revision_bytes(
        &self,
        spec: &NamespacePath,
        revision_no: RevisionNo,
    ) -> Result<Vec<u8>> {
        self.get_file_bytes_with_options(
            spec,
            &ReadFileOptions {
                revision_no: Some(revision_no),
                snapshot_id: None,
            },
        )
        .await
    }

    /// Reads path-based file content using the requested revision or snapshot.
    ///
    /// Both selectors are sent when both are present so the server remains
    /// authoritative for their mutual-exclusion error.
    pub async fn get_file_bytes_with_options(
        &self,
        spec: &NamespacePath,
        options: &ReadFileOptions,
    ) -> Result<Vec<u8>> {
        let mut url = format!(
            "{}/v0/namespaces/{}/filesystem/content?path={}",
            self.base_url,
            spec.namespace().as_str(),
            urlencoding::encode(spec.absolute_path().as_str())
        );
        let mut has_query = true;
        if let Some(revision_no) = options.revision_no {
            append_query_param(
                &mut url,
                &mut has_query,
                "revision_no",
                &revision_no.0.to_string(),
            );
        }
        if let Some(snapshot_id) = &options.snapshot_id {
            append_query_param(
                &mut url,
                &mut has_query,
                "snapshot_id",
                snapshot_id.as_str(),
            );
        }
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
        let mut url = format!(
            "{}/v0/namespaces/{}/filesystem/revisions?path={}",
            self.base_url,
            spec.namespace().as_str(),
            urlencoding::encode(spec.absolute_path().as_str())
        );
        let mut has_query = true;
        append_optional_pagination_query(&mut url, &mut has_query, limit, cursor);
        self.request_json::<(), ListFileRevisionsResponse>(self.get(&url), None)
            .await
    }

    /// Creates a path-based revision pager beginning at `cursor`.
    pub fn list_file_revisions_pager(
        &self,
        spec: &NamespacePath,
        page_size: Option<u32>,
        cursor: Option<String>,
    ) -> FileRevisionsPager {
        FileRevisionsPager {
            client: self.clone(),
            target: FileRevisionsTarget::Path(spec.clone()),
            page_size,
            cursor,
            pending: None,
            exhausted: false,
        }
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
        let mut url = format!(
            "{}/v0/namespaces/{namespace_id}/inodes/{inode_id}/revisions",
            self.base_url
        );
        let mut has_query = false;
        append_optional_pagination_query(&mut url, &mut has_query, limit, cursor);
        self.request_json::<(), ListFileRevisionsResponse>(self.get(&url), None)
            .await
    }

    /// Creates an inode-based revision pager beginning at `cursor`.
    pub fn list_file_revisions_by_inode_pager(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        page_size: Option<u32>,
        cursor: Option<String>,
    ) -> FileRevisionsPager {
        FileRevisionsPager {
            client: self.clone(),
            target: FileRevisionsTarget::Inode {
                namespace_id: namespace_id.clone(),
                inode_id,
            },
            page_size,
            cursor,
            pending: None,
            exhausted: false,
        }
    }

    /// Returns one page of recoverable deletions in a namespace.
    pub async fn list_trash_page(
        &self,
        namespace_id: &NamespaceId,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<ListTrashResponse> {
        let mut url = format!(
            "{}/v0/namespaces/{}/filesystem/trash",
            self.base_url,
            namespace_id.as_str()
        );
        let mut has_query = false;
        append_optional_pagination_query(&mut url, &mut has_query, limit, cursor);
        self.request_json::<(), ListTrashResponse>(self.get(&url), None)
            .await
    }

    /// Creates a trash pager beginning at `cursor`.
    pub fn list_trash_pager(
        &self,
        namespace_id: &NamespaceId,
        page_size: Option<u32>,
        cursor: Option<String>,
    ) -> TrashPager {
        TrashPager {
            client: self.clone(),
            namespace_id: namespace_id.clone(),
            page_size,
            cursor,
            pending: None,
            exhausted: false,
        }
    }

    /// Returns committed changes after the given sequence number.
    pub async fn list_changes(
        &self,
        namespace_id: &NamespaceId,
        after_seq: ChangeSeq,
        limit: Option<u32>,
    ) -> Result<ListChangesResponse> {
        self.list_changes_with_options(
            namespace_id,
            after_seq,
            &ListChangesOptions {
                limit,
                snapshot_id: None,
            },
        )
        .await
    }

    /// Returns committed changes using the requested page and snapshot bounds.
    pub async fn list_changes_with_options(
        &self,
        namespace_id: &NamespaceId,
        after_seq: ChangeSeq,
        options: &ListChangesOptions,
    ) -> Result<ListChangesResponse> {
        let mut url = format!(
            "{}/v0/namespaces/{namespace_id}/changes?after_seq={}",
            self.base_url, after_seq.0
        );
        if let Some(limit) = options.limit {
            url.push_str(&format!("&limit={limit}"));
        }
        if let Some(snapshot_id) = &options.snapshot_id {
            url.push_str("&snapshot_id=");
            url.push_str(snapshot_id.as_str());
        }
        self.request_json::<(), ListChangesResponse>(self.get(&url), None)
            .await
    }

    /// Creates a change-feed pager beginning after `after_seq`.
    pub fn list_changes_pager(
        &self,
        namespace_id: &NamespaceId,
        after_seq: ChangeSeq,
        page_size: Option<u32>,
    ) -> ChangesPager {
        ChangesPager {
            client: self.clone(),
            namespace_id: namespace_id.clone(),
            after_seq,
            page_size,
            snapshot_id: None,
            pending: None,
            exhausted: false,
        }
    }

    /// Creates a snapshot-bounded change-feed pager beginning after `after_seq`.
    pub fn list_changes_pager_at_snapshot(
        &self,
        namespace_id: &NamespaceId,
        after_seq: ChangeSeq,
        page_size: Option<u32>,
        snapshot_id: &CheckpointId,
    ) -> ChangesPager {
        ChangesPager {
            client: self.clone(),
            namespace_id: namespace_id.clone(),
            after_seq,
            page_size,
            snapshot_id: Some(snapshot_id.clone()),
            pending: None,
            exhausted: false,
        }
    }
}
