//! Namespace lifecycle, path reads, revision history, trash, and change feeds.

use super::*;
use crate::transport::{append_optional_pagination_query, append_query_param};

/// Lazy directory-page reader.
///
/// Each call to [`Self::next`] returns one original response envelope, so a
/// caller can observe head changes between pages. [`Self::collect_up_to`]
/// collects only entries and keeps any unconsumed part of the final page for
/// the next call to [`Self::next`].
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

    /// Collects at most `max_items` entries without losing a partially read page.
    pub async fn collect_up_to(&mut self, max_items: usize) -> Result<Vec<AuthoritativePathEntry>> {
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

/// Lazy file-revision page reader for either a path or an inode.
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

    /// Collects at most `max_items` revisions without losing a partially read page.
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

/// Lazy recoverable-deletion page reader.
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

    /// Collects at most `max_items` deletions without losing a partially read page.
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

/// Lazy change-feed page reader using sequence positions rather than opaque cursors.
#[must_use]
pub struct ChangesPager {
    client: Client,
    namespace_id: NamespaceId,
    after_seq: ChangeSeq,
    page_size: Option<u32>,
    pending: Option<ChangesResponse>,
    exhausted: bool,
}

impl ChangesPager {
    /// Returns the next change page, or `None` after exhaustion.
    pub async fn next(&mut self) -> Option<Result<ChangesResponse>> {
        if let Some(page) = self.pending.take() {
            return Some(Ok(page));
        }
        if self.exhausted {
            return None;
        }
        let page = self
            .client
            .list_changes(&self.namespace_id, self.after_seq, self.page_size)
            .await;
        Some(page.inspect(|page| {
            self.exhausted = page.next_after_seq.is_none();
            if let Some(next_after_seq) = page.next_after_seq {
                self.after_seq = next_after_seq;
            }
        }))
    }

    /// Collects at most `max_items` changes without losing a partially read page.
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
    pub async fn namespace_status(&self, namespace_id: &NamespaceId) -> Result<Namespace> {
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

    /// Creates a lazy directory pager beginning at `cursor`.
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
            "{}/v0/namespaces/{}/filesystem/list?path={}",
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
        self.request_json::<(), _>(self.get(&url), None).await
    }

    /// Returns path metadata using the requested projection.
    pub async fn stat_path(
        &self,
        spec: &NamespacePath,
        options: &StatPathOptions,
    ) -> Result<AuthoritativePathEntry> {
        let mut url = format!(
            "{}/v0/namespaces/{}/filesystem/stat?path={}",
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
        self.request_json::<(), _>(self.get(&url), None).await
    }

    /// Returns the current entry for a visible inode.
    pub async fn stat_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        options: &StatPathOptions,
    ) -> Result<AuthoritativePathEntry> {
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

    /// Reads the file's current contents into memory.
    pub async fn get_file_bytes(&self, spec: &NamespacePath) -> Result<Vec<u8>> {
        let url = format!(
            "{}/v0/namespaces/{}/filesystem/content?path={}",
            self.base_url,
            spec.namespace().as_str(),
            urlencoding::encode(spec.absolute_path().as_str())
        );
        self.request_bytes(&url).await
    }

    /// Reads the requested file revision into memory.
    pub async fn get_file_revision_bytes(
        &self,
        spec: &NamespacePath,
        revision_no: RevisionNo,
    ) -> Result<Vec<u8>> {
        let url = format!(
            "{}/v0/namespaces/{}/filesystem/content?path={}&revision_no={}",
            self.base_url,
            spec.namespace().as_str(),
            urlencoding::encode(spec.absolute_path().as_str()),
            revision_no.0
        );
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

    /// Creates a lazy path-based revision pager beginning at `cursor`.
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

    /// Creates a lazy inode-based revision pager beginning at `cursor`.
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

    /// Creates a lazy trash pager beginning at `cursor`.
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
    ) -> Result<ChangesResponse> {
        let mut url = format!(
            "{}/v0/namespaces/{namespace_id}/changes?after_seq={}",
            self.base_url, after_seq.0
        );
        if let Some(limit) = limit {
            url.push_str(&format!("&limit={limit}"));
        }
        self.request_json::<(), ChangesResponse>(self.get(&url), None)
            .await
    }

    /// Creates a lazy change-feed pager beginning after `after_seq`.
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
            pending: None,
            exhausted: false,
        }
    }
}
