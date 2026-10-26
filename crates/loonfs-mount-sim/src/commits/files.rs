//! Planning the operations that create directories and write file content: `create_directory`,
//! `put_file`, `put_file_revision_by_inode`, `append_file`, and `append_file_by_inode`.

use super::planner::{
    ensure_expected_inode, ensure_expected_revision, path_conflict, root_mutation, Planner,
};
use crate::content_store::ChainKey;
use crate::entries::{inode_not_found, path_lookup_error};
use crate::error::{MountSimError, Result};
use crate::uploads::STORE_CHECKSUM_ALGORITHM;
use loonfs_model::metadata::InodeRecord;
use loonfs_model::visibility::PathLookup;
use loonfs_types::{
    AbsolutePath, Checksum, ContentRef, ContentRefKind, DestinationBehavior,
    DestinationPrecondition, ErrorCode, InodeId, InodeKind, PreconditionFields, RevisionNo,
};

impl Planner<'_> {
    pub(super) fn create_directory(&mut self, path: &AbsolutePath, parents: bool) -> Result<()> {
        let display_name = final_name(path)?;
        let parent_inode_id = if parents {
            self.ensure_parent_directories(path)?
        } else {
            self.parent_directory(path)?
        };
        if let Some(existing) = self.view().visible_child(parent_inode_id, &display_name) {
            return Err(path_conflict(format!(
                "`{path}` already exists as `{}`",
                existing.display_name
            )));
        }
        self.create_directory_at(parent_inode_id, &display_name);
        Ok(())
    }

    pub(super) fn put_file(
        &mut self,
        path: &AbsolutePath,
        content_ref: Option<&ContentRef>,
        inline_content: Option<&[u8]>,
        behavior: DestinationBehavior,
        expected_inode_id: Option<InodeId>,
        expected_revision_no: Option<RevisionNo>,
    ) -> Result<()> {
        let display_name = final_name(path)?;
        let expected = DestinationPrecondition {
            behavior,
            expected_inode_id,
            expected_revision_no,
        }
        .resolve(PreconditionFields::Put)
        .map_err(|error| MountSimError::invalid_request(error.to_string()))?;
        let existing = match self.view().resolve_path(path) {
            PathLookup::Found { inode, .. } => Some(inode.clone()),
            PathLookup::NotFound { .. } => None,
            lookup @ PathLookup::NotADirectory { .. } => return Err(path_lookup_error(&lookup)),
        };
        let Some(existing) = existing else {
            let parent_inode_id = self.ensure_parent_directories(path)?;
            if expected.is_some() {
                return Err(MountSimError::new(
                    ErrorCode::PathNotFound,
                    format!("no file to replace at `{path}`"),
                ));
            }
            let content_ref = self.prepare_content(content_ref, inline_content)?;
            self.create_file_at(parent_inode_id, &display_name, content_ref);
            return Ok(());
        };
        if behavior == DestinationBehavior::NoReplace {
            return Err(path_conflict(format!("`{path}` already exists")));
        }
        ensure_expected_inode(
            expected.map(|expected| expected.inode_id),
            existing.inode_id,
        )?;
        let existing = file(&existing)?;
        ensure_expected_revision(
            &self.view(),
            existing.inode_id,
            expected.and_then(|expected| expected.revision_no),
        )?;
        let content_ref = self.prepare_content(content_ref, inline_content)?;
        self.add_revision(existing.inode_id, content_ref, None);
        Ok(())
    }

    pub(super) fn put_file_revision_by_inode(
        &mut self,
        inode_id: InodeId,
        content_ref: Option<&ContentRef>,
        inline_content: Option<&[u8]>,
        expected_revision_no: RevisionNo,
    ) -> Result<()> {
        let inode = self.visible(inode_id)?;
        file(&inode)?;
        ensure_expected_revision(&self.view(), inode_id, Some(expected_revision_no))?;
        let content_ref = self.prepare_content(content_ref, inline_content)?;
        self.add_revision(inode_id, content_ref, None);
        Ok(())
    }

    pub(super) fn append_file(
        &mut self,
        path: &AbsolutePath,
        bytes: &[u8],
        expected_inode_id: Option<InodeId>,
        expected_revision_no: Option<RevisionNo>,
    ) -> Result<()> {
        final_name(path)?;
        let expected = DestinationPrecondition {
            behavior: DestinationBehavior::Replace,
            expected_inode_id,
            expected_revision_no,
        }
        .resolve(PreconditionFields::Put)
        .map_err(|error| MountSimError::invalid_request(error.to_string()))?;
        let inode = match self.view().resolve_path(path) {
            PathLookup::Found { inode, .. } => inode.clone(),
            lookup => return Err(path_lookup_error(&lookup)),
        };
        ensure_expected_inode(expected.map(|expected| expected.inode_id), inode.inode_id)?;
        file(&inode)?;
        ensure_expected_revision(
            &self.view(),
            inode.inode_id,
            expected.and_then(|expected| expected.revision_no),
        )?;
        self.append(inode.inode_id, bytes)
    }

    pub(super) fn append_file_by_inode(
        &mut self,
        inode_id: InodeId,
        bytes: &[u8],
        expected_revision_no: Option<RevisionNo>,
    ) -> Result<()> {
        let inode = self.visible(inode_id)?;
        file(&inode)?;
        ensure_expected_revision(&self.view(), inode_id, expected_revision_no)?;
        self.append(inode_id, bytes)
    }

    /// Returns a visible inode, or `inode_not_found`.
    pub(super) fn visible(&self, inode_id: InodeId) -> Result<InodeRecord> {
        self.view()
            .visible_inode(inode_id)
            .cloned()
            .ok_or_else(|| inode_not_found(inode_id))
    }

    /// Publishes a revision that adds `bytes` after the file's current revision (api.md, "`POST
    /// /commits`"). The revision keeps the chain's content ID when this namespace owns the chain,
    /// the base isn't empty, and the base is the chain's full length. Otherwise it starts a new
    /// chain whose first bytes are the base's bytes.
    fn append(&mut self, inode_id: InodeId, bytes: &[u8]) -> Result<()> {
        let base = self
            .view()
            .current_revision(inode_id)
            .expect("a visible file should have a revision")
            .content_ref
            .clone();
        if base.checksum.algorithm != STORE_CHECKSUM_ALGORITHM {
            return Err(MountSimError::new(
                ErrorCode::NamespaceCorrupt,
                format!(
                    "content `{}` uses `{}`, not the store's `{STORE_CHECKSUM_ALGORITHM}`",
                    base.content_id, base.checksum.algorithm
                ),
            ));
        }
        let view = self.view();
        let chain_head = view
            .state
            .content_head(&base.content_id)
            .unwrap_or(base.size_bytes);
        let extends_chain = base.owner_namespace_id == *self.namespace_id
            && base.size_bytes > 0
            && chain_head == base.size_bytes;
        let added = u64::try_from(bytes.len()).expect("an append's length fits in u64");
        let checksum = base
            .checksum
            .crc_combine(&Checksum::compute(STORE_CHECKSUM_ALGORITHM, bytes), added)
            .expect("a stored checksum should combine with one in the same algorithm");
        let content_id = if extends_chain {
            base.content_id.clone()
        } else {
            self.ids.content_id()
        };
        let mut chain = self.content.read(self.store, &base).to_vec();
        chain.extend_from_slice(bytes);
        let content_ref = ContentRef {
            kind: ContentRefKind::BlobV1,
            owner_namespace_id: self.namespace_id.clone(),
            content_id,
            size_bytes: base.size_bytes + added,
            checksum,
        };
        self.content.write(ChainKey::of(&content_ref), chain);
        self.add_revision(inode_id, content_ref, Some(base.size_bytes));
        Ok(())
    }

    /// Returns the content reference that a put or a revision publishes: a new chain for inline
    /// bytes, or the uploaded reference.
    fn prepare_content(
        &mut self,
        content_ref: Option<&ContentRef>,
        inline_content: Option<&[u8]>,
    ) -> Result<ContentRef> {
        match (content_ref, inline_content) {
            (None, Some(bytes)) => {
                let content_ref = ContentRef::blob_v1(
                    self.namespace_id.clone(),
                    self.ids.content_id(),
                    bytes,
                    STORE_CHECKSUM_ALGORITHM,
                );
                self.content
                    .write(ChainKey::of(&content_ref), bytes.to_vec());
                Ok(content_ref)
            }
            // Admission checked every uploaded reference against the request's content tokens.
            (Some(content_ref), None) => Ok(content_ref.clone()),
            _ => Err(MountSimError::invalid_request(
                "exactly one of `content_ref` and `inline_content` is required",
            )),
        }
    }
}

/// Returns the name of a path's last component, or rejects the root.
pub(super) fn final_name(path: &AbsolutePath) -> Result<loonfs_types::DisplayName> {
    path.final_component()
        .map(|component| component.to_display_name())
        .ok_or_else(root_mutation)
}

/// Returns `inode` if it's a file, or `path_conflict`.
pub(super) fn file(inode: &InodeRecord) -> Result<&InodeRecord> {
    match inode.inode_kind {
        InodeKind::File => Ok(inode),
        InodeKind::Directory => Err(path_conflict("the target is a directory, not a file")),
    }
}
