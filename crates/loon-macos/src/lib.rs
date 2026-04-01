//! Read-only macOS File Provider bridge (experimental spike).
//!
//! Projects Finder-style provider items from the existing client SQLite state, reusing the
//! same client truth model rather than inventing a second sync model. Exports a static-library
//! C ABI with UTF-8 JSON payloads for a native macOS host app.

#![deny(unsafe_op_in_unsafe_fn)]

use loon_client::local_fs::{join_under_mirror_root, NamespacePathIndex};
use loon_client::provider::{
    materialize_inode_to_mirror_root, materialize_local_only_to_mirror_root,
    ProviderMaterializedPath, ProviderTargetedMaterializeError,
};
use loon_client::state_db::{
    ClientFileId, ClientNamespaceStateSummary, FileSyncViews, LocalOnlyParentLinkRow,
    LocalOnlyParentRef, SqliteStateDb, StateDbError,
};
use loon_ops::OpsConfig;
use loon_types::{InodeId, InodeKind, NamespaceId, RevisionNo};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{CStr, CString};
use std::fs;
use std::os::raw::c_char;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileProviderSpikeConfig {
    pub ops_config: OpsConfig,
    pub exposed_namespaces: BTreeSet<NamespaceId>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ProviderItemId {
    Root,
    NamespaceRoot {
        namespace_id: NamespaceId,
    },
    BoundInode {
        namespace_id: NamespaceId,
        inode_id: InodeId,
    },
    LocalOnly {
        namespace_id: NamespaceId,
        client_file_id: ClientFileId,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ProviderItemIdDecodeError {
    #[error("provider_item_id_invalid_prefix: `{0}`")]
    InvalidPrefix(String),
    #[error("provider_item_id_missing_parts: `{0}`")]
    MissingParts(String),
    #[error("provider_item_id_invalid_hex: `{0}`")]
    InvalidHex(String),
    #[error("provider_item_id_invalid_utf8")]
    InvalidUtf8,
    #[error("provider_item_id_invalid_inode: `{0}`")]
    InvalidInode(String),
}

impl ProviderItemId {
    pub fn to_opaque_string(&self) -> String {
        match self {
            Self::Root => "root".to_owned(),
            Self::NamespaceRoot { namespace_id } => {
                format!("ns:{}", encode_hex(namespace_id.as_str()))
            }
            Self::BoundInode {
                namespace_id,
                inode_id,
            } => format!(
                "inode:{}:{:016x}",
                encode_hex(namespace_id.as_str()),
                inode_id.0
            ),
            Self::LocalOnly {
                namespace_id,
                client_file_id,
            } => format!(
                "local:{}:{}",
                encode_hex(namespace_id.as_str()),
                encode_hex(client_file_id.as_str())
            ),
        }
    }

    pub fn from_opaque_str(value: &str) -> Result<Self, ProviderItemIdDecodeError> {
        if value == "root" {
            return Ok(Self::Root);
        }

        let mut parts = value.split(':');
        let Some(prefix) = parts.next() else {
            return Err(ProviderItemIdDecodeError::MissingParts(value.to_owned()));
        };
        match prefix {
            "ns" => {
                let namespace_hex = parts
                    .next()
                    .ok_or_else(|| ProviderItemIdDecodeError::MissingParts(value.to_owned()))?;
                if parts.next().is_some() {
                    return Err(ProviderItemIdDecodeError::MissingParts(value.to_owned()));
                }
                Ok(Self::NamespaceRoot {
                    namespace_id: NamespaceId::from(decode_hex(namespace_hex)?),
                })
            }
            "inode" => {
                let namespace_hex = parts
                    .next()
                    .ok_or_else(|| ProviderItemIdDecodeError::MissingParts(value.to_owned()))?;
                let inode_hex = parts
                    .next()
                    .ok_or_else(|| ProviderItemIdDecodeError::MissingParts(value.to_owned()))?;
                if parts.next().is_some() {
                    return Err(ProviderItemIdDecodeError::MissingParts(value.to_owned()));
                }
                let inode_id = u64::from_str_radix(inode_hex, 16)
                    .map_err(|_| ProviderItemIdDecodeError::InvalidInode(inode_hex.to_owned()))?;
                Ok(Self::BoundInode {
                    namespace_id: NamespaceId::from(decode_hex(namespace_hex)?),
                    inode_id: InodeId(inode_id),
                })
            }
            "local" => {
                let namespace_hex = parts
                    .next()
                    .ok_or_else(|| ProviderItemIdDecodeError::MissingParts(value.to_owned()))?;
                let client_file_hex = parts
                    .next()
                    .ok_or_else(|| ProviderItemIdDecodeError::MissingParts(value.to_owned()))?;
                if parts.next().is_some() {
                    return Err(ProviderItemIdDecodeError::MissingParts(value.to_owned()));
                }
                Ok(Self::LocalOnly {
                    namespace_id: NamespaceId::from(decode_hex(namespace_hex)?),
                    client_file_id: ClientFileId::new(decode_hex(client_file_hex)?),
                })
            }
            other => Err(ProviderItemIdDecodeError::InvalidPrefix(other.to_owned())),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderMaterializationState {
    SyntheticDir,
    Materialized,
    Placeholder,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderProjectionWarning {
    pub namespace_id: NamespaceId,
    pub relative_path: String,
    pub inode_kind: InodeKind,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderItemSnapshot {
    pub item_id: ProviderItemId,
    pub parent_item_id: Option<ProviderItemId>,
    pub display_name: String,
    pub inode_kind: InodeKind,
    pub materialization_state: ProviderMaterializationState,
    pub current_relative_path: Option<String>,
    pub size_bytes: Option<u64>,
    pub revision_no: Option<RevisionNo>,
    pub content_digest: Option<String>,
    pub content_manifest_digest: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderListing {
    pub items: Vec<ProviderItemSnapshot>,
    pub warnings: Vec<ProviderProjectionWarning>,
}

#[derive(Debug, Error)]
pub enum FileProviderBridgeError {
    #[error(transparent)]
    StateDb(#[from] StateDbError),
    #[error(transparent)]
    TargetedMaterialize(#[from] ProviderTargetedMaterializeError),
    #[error(transparent)]
    OpsConfig(#[from] anyhow::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("provider_state_db_missing: {0}")]
    StateDbMissing(String),
    #[error("provider_namespace_not_exposed: `{0}`")]
    NamespaceNotExposed(String),
}

pub struct FileProviderBridge {
    config: FileProviderSpikeConfig,
}

struct ProviderLookupResult {
    item: Option<ProviderItemSnapshot>,
    warnings: Vec<ProviderProjectionWarning>,
}

impl FileProviderBridge {
    pub fn new(config: FileProviderSpikeConfig) -> Result<Self, FileProviderBridgeError> {
        config.ops_config.open_store()?;
        Ok(Self { config })
    }

    pub fn list_root(&self) -> Result<ProviderListing, FileProviderBridgeError> {
        Ok(ProviderListing {
            items: self
                .config
                .exposed_namespaces
                .iter()
                .map(|namespace_id| ProviderItemSnapshot {
                    item_id: ProviderItemId::NamespaceRoot {
                        namespace_id: namespace_id.clone(),
                    },
                    parent_item_id: Some(ProviderItemId::Root),
                    display_name: namespace_id.as_str().to_owned(),
                    inode_kind: InodeKind::Dir,
                    materialization_state: ProviderMaterializationState::SyntheticDir,
                    current_relative_path: None,
                    size_bytes: None,
                    revision_no: None,
                    content_digest: None,
                    content_manifest_digest: None,
                })
                .collect(),
            warnings: Vec::new(),
        })
    }

    pub fn lookup_item(
        &self,
        item_id: &ProviderItemId,
    ) -> Result<Option<ProviderItemSnapshot>, FileProviderBridgeError> {
        Ok(self.lookup_item_with_warnings(item_id)?.item)
    }

    fn lookup_item_with_warnings(
        &self,
        item_id: &ProviderItemId,
    ) -> Result<ProviderLookupResult, FileProviderBridgeError> {
        match item_id {
            ProviderItemId::Root => Ok(ProviderLookupResult {
                item: Some(self.root_snapshot()),
                warnings: Vec::new(),
            }),
            ProviderItemId::NamespaceRoot { namespace_id } => {
                if !self.config.exposed_namespaces.contains(namespace_id) {
                    return Ok(ProviderLookupResult {
                        item: None,
                        warnings: Vec::new(),
                    });
                }
                Ok(ProviderLookupResult {
                    item: Some(self.namespace_root_snapshot(namespace_id)),
                    warnings: Vec::new(),
                })
            }
            ProviderItemId::BoundInode { namespace_id, .. }
            | ProviderItemId::LocalOnly { namespace_id, .. } => {
                self.ensure_namespace_exposed(namespace_id)?;
                let projection = self.build_namespace_projection(namespace_id)?;
                Ok(ProviderLookupResult {
                    item: projection.items.get(item_id).cloned(),
                    warnings: projection.warnings,
                })
            }
        }
    }

    pub fn list_children(
        &self,
        parent_id: &ProviderItemId,
    ) -> Result<ProviderListing, FileProviderBridgeError> {
        match parent_id {
            ProviderItemId::Root => self.list_root(),
            ProviderItemId::NamespaceRoot { namespace_id } => {
                self.ensure_namespace_exposed(namespace_id)?;
                let projection = self.build_namespace_projection(namespace_id)?;
                Ok(ProviderListing {
                    items: projection
                        .children
                        .get(parent_id)
                        .cloned()
                        .unwrap_or_default(),
                    warnings: projection.warnings,
                })
            }
            ProviderItemId::BoundInode { namespace_id, .. }
            | ProviderItemId::LocalOnly { namespace_id, .. } => {
                self.ensure_namespace_exposed(namespace_id)?;
                let projection = self.build_namespace_projection(namespace_id)?;
                Ok(ProviderListing {
                    items: projection
                        .children
                        .get(parent_id)
                        .cloned()
                        .unwrap_or_default(),
                    warnings: projection.warnings,
                })
            }
        }
    }

    pub fn materialize_item(
        &self,
        item_id: &ProviderItemId,
        now_ms: u64,
    ) -> Result<ProviderMaterializedPath, FileProviderBridgeError> {
        match item_id {
            ProviderItemId::Root => {
                fs::create_dir_all(&self.config.ops_config.client.mirror_root)?;
                Ok(ProviderMaterializedPath {
                    absolute_path: self.config.ops_config.client.mirror_root.clone(),
                    relative_path: String::new(),
                })
            }
            ProviderItemId::NamespaceRoot { namespace_id } => {
                self.ensure_namespace_exposed(namespace_id)?;
                let namespace_root = self.namespace_cache_root(namespace_id);
                fs::create_dir_all(&namespace_root)?;
                Ok(ProviderMaterializedPath {
                    absolute_path: namespace_root,
                    relative_path: String::new(),
                })
            }
            ProviderItemId::BoundInode {
                namespace_id,
                inode_id,
            } => {
                self.ensure_namespace_exposed(namespace_id)?;
                fs::create_dir_all(&self.config.ops_config.client.mirror_root)?;
                let mut db = self.open_db()?;
                let store = self.config.ops_config.open_store()?;
                materialize_inode_to_mirror_root(
                    &mut db,
                    &store,
                    namespace_id,
                    *inode_id,
                    &self.namespace_cache_root(namespace_id),
                    now_ms,
                )
                .map_err(FileProviderBridgeError::from)
            }
            ProviderItemId::LocalOnly {
                namespace_id,
                client_file_id,
            } => {
                self.ensure_namespace_exposed(namespace_id)?;
                fs::create_dir_all(&self.config.ops_config.client.mirror_root)?;
                let mut db = self.open_db()?;
                materialize_local_only_to_mirror_root(
                    &mut db,
                    client_file_id,
                    &self.namespace_cache_root(namespace_id),
                )
                .map_err(FileProviderBridgeError::from)
            }
        }
    }

    fn root_snapshot(&self) -> ProviderItemSnapshot {
        ProviderItemSnapshot {
            item_id: ProviderItemId::Root,
            parent_item_id: None,
            display_name: String::new(),
            inode_kind: InodeKind::Dir,
            materialization_state: ProviderMaterializationState::SyntheticDir,
            current_relative_path: None,
            size_bytes: None,
            revision_no: None,
            content_digest: None,
            content_manifest_digest: None,
        }
    }

    fn namespace_root_snapshot(&self, namespace_id: &NamespaceId) -> ProviderItemSnapshot {
        ProviderItemSnapshot {
            item_id: ProviderItemId::NamespaceRoot {
                namespace_id: namespace_id.clone(),
            },
            parent_item_id: Some(ProviderItemId::Root),
            display_name: namespace_id.as_str().to_owned(),
            inode_kind: InodeKind::Dir,
            materialization_state: ProviderMaterializationState::SyntheticDir,
            current_relative_path: None,
            size_bytes: None,
            revision_no: None,
            content_digest: None,
            content_manifest_digest: None,
        }
    }

    fn namespace_cache_root(&self, namespace_id: &NamespaceId) -> PathBuf {
        self.config
            .ops_config
            .client
            .mirror_root
            .join(namespace_id.as_str())
    }

    fn ensure_namespace_exposed(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<(), FileProviderBridgeError> {
        if self.config.exposed_namespaces.contains(namespace_id) {
            Ok(())
        } else {
            Err(FileProviderBridgeError::NamespaceNotExposed(
                namespace_id.as_str().to_owned(),
            ))
        }
    }

    fn open_db(&self) -> Result<SqliteStateDb, FileProviderBridgeError> {
        let db_path = &self.config.ops_config.client.state_db_path;
        if !db_path.is_file() {
            return Err(FileProviderBridgeError::StateDbMissing(
                db_path.display().to_string(),
            ));
        }
        SqliteStateDb::open(db_path).map_err(FileProviderBridgeError::from)
    }

    fn build_namespace_projection(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<NamespaceProjection, FileProviderBridgeError> {
        let db = self.open_db()?;
        let summary = db.load_namespace_state_summary(namespace_id)?;
        let parent_links = db.load_local_only_parent_links_for_namespace(namespace_id)?;
        Ok(NamespaceProjection::build(
            namespace_id,
            &self.namespace_cache_root(namespace_id),
            &summary,
            &parent_links,
        ))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct InteropEnvelope<T> {
    ok: bool,
    result: Option<T>,
    error: Option<InteropErrorPayload>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct InteropErrorPayload {
    code: String,
    message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct InteropOpenRequest {
    ops_config_path: String,
    exposed_namespaces: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct InteropBridgeHandleRequest {
    bridge_handle: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct InteropLookupRequest {
    bridge_handle: u64,
    item_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct InteropListChildrenRequest {
    bridge_handle: u64,
    parent_item_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct InteropMaterializeRequest {
    bridge_handle: u64,
    item_id: String,
    now_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct InteropOpenResult {
    bridge_handle: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct InteropCloseResult {
    closed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct InteropLookupResult {
    item: Option<InteropProviderItemSnapshot>,
    warnings: Vec<InteropProviderProjectionWarning>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct InteropProviderListing {
    items: Vec<InteropProviderItemSnapshot>,
    warnings: Vec<InteropProviderProjectionWarning>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct InteropProviderProjectionWarning {
    namespace_id: String,
    relative_path: String,
    inode_kind: String,
    reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct InteropProviderItemSnapshot {
    item_id: String,
    parent_item_id: Option<String>,
    display_name: String,
    inode_kind: String,
    materialization_state: ProviderMaterializationState,
    current_relative_path: Option<String>,
    size_bytes: Option<u64>,
    revision_no: Option<u64>,
    content_digest: Option<String>,
    content_manifest_digest: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct InteropMaterializedPath {
    absolute_path: String,
    relative_path: String,
}

#[derive(Default)]
struct BridgeRegistry {
    next_handle: u64,
    bridges: BTreeMap<u64, FileProviderBridge>,
}

impl InteropProviderProjectionWarning {
    fn from_warning(warning: ProviderProjectionWarning) -> Self {
        Self {
            namespace_id: warning.namespace_id.as_str().to_owned(),
            relative_path: warning.relative_path,
            inode_kind: inode_kind_text(&warning.inode_kind).to_owned(),
            reason: warning.reason,
        }
    }
}

impl InteropProviderItemSnapshot {
    fn from_snapshot(snapshot: ProviderItemSnapshot) -> Self {
        Self {
            item_id: snapshot.item_id.to_opaque_string(),
            parent_item_id: snapshot
                .parent_item_id
                .map(|item_id| item_id.to_opaque_string()),
            display_name: snapshot.display_name,
            inode_kind: inode_kind_text(&snapshot.inode_kind).to_owned(),
            materialization_state: snapshot.materialization_state,
            current_relative_path: snapshot.current_relative_path,
            size_bytes: snapshot.size_bytes,
            revision_no: snapshot.revision_no.map(|revision_no| revision_no.0),
            content_digest: snapshot.content_digest,
            content_manifest_digest: snapshot.content_manifest_digest,
        }
    }
}

impl InteropProviderListing {
    fn from_listing(listing: ProviderListing) -> Self {
        Self {
            items: listing
                .items
                .into_iter()
                .map(InteropProviderItemSnapshot::from_snapshot)
                .collect(),
            warnings: listing
                .warnings
                .into_iter()
                .map(InteropProviderProjectionWarning::from_warning)
                .collect(),
        }
    }
}

impl InteropLookupResult {
    fn from_lookup(lookup: ProviderLookupResult) -> Self {
        Self {
            item: lookup.item.map(InteropProviderItemSnapshot::from_snapshot),
            warnings: lookup
                .warnings
                .into_iter()
                .map(InteropProviderProjectionWarning::from_warning)
                .collect(),
        }
    }
}

impl InteropMaterializedPath {
    fn from_path(path: ProviderMaterializedPath) -> Self {
        Self {
            absolute_path: path.absolute_path.to_string_lossy().into_owned(),
            relative_path: path.relative_path,
        }
    }
}

fn bridge_registry() -> &'static Mutex<BridgeRegistry> {
    static BRIDGE_REGISTRY: OnceLock<Mutex<BridgeRegistry>> = OnceLock::new();
    BRIDGE_REGISTRY.get_or_init(|| {
        Mutex::new(BridgeRegistry {
            next_handle: 1,
            bridges: BTreeMap::new(),
        })
    })
}

fn interop_open(request: InteropOpenRequest) -> Result<InteropOpenResult, InteropErrorPayload> {
    let ops_config = OpsConfig::load(Path::new(&request.ops_config_path))
        .map_err(|error| interop_error("ops_config_load_failed", error.to_string()))?;
    let bridge = FileProviderBridge::new(FileProviderSpikeConfig {
        ops_config,
        exposed_namespaces: request
            .exposed_namespaces
            .into_iter()
            .map(NamespaceId::from)
            .collect(),
    })
    .map_err(map_bridge_error)?;

    let mut registry = lock_bridge_registry()?;
    let bridge_handle = registry.next_handle;
    registry.next_handle = registry.next_handle.saturating_add(1);
    registry.bridges.insert(bridge_handle, bridge);
    Ok(InteropOpenResult { bridge_handle })
}

fn interop_close(
    request: InteropBridgeHandleRequest,
) -> Result<InteropCloseResult, InteropErrorPayload> {
    let mut registry = lock_bridge_registry()?;
    if registry.bridges.remove(&request.bridge_handle).is_none() {
        return Err(interop_error(
            "unknown_bridge_handle",
            format!("unknown bridge handle {}", request.bridge_handle),
        ));
    }
    Ok(InteropCloseResult { closed: true })
}

fn interop_list_root(
    request: InteropBridgeHandleRequest,
) -> Result<InteropProviderListing, InteropErrorPayload> {
    with_bridge(request.bridge_handle, |bridge| {
        bridge.list_root().map(InteropProviderListing::from_listing)
    })
}

fn interop_lookup_item(
    request: InteropLookupRequest,
) -> Result<InteropLookupResult, InteropErrorPayload> {
    let item_id = ProviderItemId::from_opaque_str(&request.item_id)
        .map_err(|error| interop_error("invalid_item_id", error.to_string()))?;
    with_bridge(request.bridge_handle, |bridge| {
        bridge
            .lookup_item_with_warnings(&item_id)
            .map(InteropLookupResult::from_lookup)
    })
}

fn interop_list_children(
    request: InteropListChildrenRequest,
) -> Result<InteropProviderListing, InteropErrorPayload> {
    let parent_item_id = ProviderItemId::from_opaque_str(&request.parent_item_id)
        .map_err(|error| interop_error("invalid_item_id", error.to_string()))?;
    with_bridge(request.bridge_handle, |bridge| {
        bridge
            .list_children(&parent_item_id)
            .map(InteropProviderListing::from_listing)
    })
}

fn interop_materialize_item(
    request: InteropMaterializeRequest,
) -> Result<InteropMaterializedPath, InteropErrorPayload> {
    let item_id = ProviderItemId::from_opaque_str(&request.item_id)
        .map_err(|error| interop_error("invalid_item_id", error.to_string()))?;
    with_bridge(request.bridge_handle, |bridge| {
        bridge
            .materialize_item(&item_id, request.now_ms)
            .map(InteropMaterializedPath::from_path)
    })
}

fn with_bridge<T, F>(bridge_handle: u64, f: F) -> Result<T, InteropErrorPayload>
where
    F: FnOnce(&FileProviderBridge) -> Result<T, FileProviderBridgeError>,
{
    let registry = lock_bridge_registry()?;
    let bridge = registry.bridges.get(&bridge_handle).ok_or_else(|| {
        interop_error(
            "unknown_bridge_handle",
            format!("unknown bridge handle {}", bridge_handle),
        )
    })?;
    f(bridge).map_err(map_bridge_error)
}

fn lock_bridge_registry(
) -> Result<std::sync::MutexGuard<'static, BridgeRegistry>, InteropErrorPayload> {
    bridge_registry().lock().map_err(|error| {
        interop_error(
            "bridge_registry_unavailable",
            format!("bridge registry unavailable: {}", error),
        )
    })
}

fn map_bridge_error(error: FileProviderBridgeError) -> InteropErrorPayload {
    match error {
        FileProviderBridgeError::StateDb(error) => {
            interop_error("state_db_error", error.to_string())
        }
        FileProviderBridgeError::TargetedMaterialize(error) => {
            interop_error("targeted_materialize_failed", error.to_string())
        }
        FileProviderBridgeError::OpsConfig(error) => {
            interop_error("ops_config_failed", error.to_string())
        }
        FileProviderBridgeError::Io(error) => interop_error("io_error", error.to_string()),
        FileProviderBridgeError::StateDbMissing(path) => interop_error("state_db_missing", path),
        FileProviderBridgeError::NamespaceNotExposed(namespace_id) => {
            interop_error("namespace_not_exposed", namespace_id)
        }
    }
}

fn interop_error(code: impl Into<String>, message: impl Into<String>) -> InteropErrorPayload {
    InteropErrorPayload {
        code: code.into(),
        message: message.into(),
    }
}

fn encode_success_envelope<T: Serialize>(result: T) -> *mut c_char {
    encode_envelope(InteropEnvelope {
        ok: true,
        result: Some(result),
        error: None,
    })
}

fn encode_error_envelope(error: InteropErrorPayload) -> *mut c_char {
    encode_envelope::<()>(InteropEnvelope {
        ok: false,
        result: None,
        error: Some(error),
    })
}

fn encode_envelope<T: Serialize>(envelope: InteropEnvelope<T>) -> *mut c_char {
    let json = match serde_json::to_string(&envelope) {
        Ok(json) => json,
        Err(error) => format!(
            "{{\"ok\":false,\"result\":null,\"error\":{{\"code\":\"serialization_failed\",\"message\":{}}}}}",
            serde_json::to_string(&error.to_string()).unwrap_or_else(|_| "\"serialization_failed\"".to_owned())
        ),
    };
    CString::new(json)
        .expect("JSON envelopes should not contain interior NUL bytes")
        .into_raw()
}

fn parse_request<T: DeserializeOwned>(
    request_json: *const c_char,
) -> Result<T, InteropErrorPayload> {
    if request_json.is_null() {
        return Err(interop_error("null_request", "request pointer was null"));
    }
    let request_json = {
        // SAFETY: the caller promises a valid NUL-terminated C string pointer for the lifetime of
        // this call, and we reject null pointers above.
        let c_str = unsafe { CStr::from_ptr(request_json) };
        c_str
            .to_str()
            .map_err(|error| interop_error("invalid_utf8_request", error.to_string()))?
    };
    serde_json::from_str(request_json)
        .map_err(|error| interop_error("invalid_json_request", error.to_string()))
}

fn ffi_call<TRequest, TResult, F>(request_json: *const c_char, handler: F) -> *mut c_char
where
    TRequest: DeserializeOwned,
    TResult: Serialize,
    F: FnOnce(TRequest) -> Result<TResult, InteropErrorPayload>,
{
    match parse_request(request_json).and_then(handler) {
        Ok(result) => encode_success_envelope(result),
        Err(error) => encode_error_envelope(error),
    }
}

#[no_mangle]
pub extern "C" fn loon_file_provider_bridge_open(request_json: *const c_char) -> *mut c_char {
    ffi_call(request_json, interop_open)
}

#[no_mangle]
pub extern "C" fn loon_file_provider_bridge_close(request_json: *const c_char) -> *mut c_char {
    ffi_call(request_json, interop_close)
}

#[no_mangle]
pub extern "C" fn loon_file_provider_bridge_list_root(request_json: *const c_char) -> *mut c_char {
    ffi_call(request_json, interop_list_root)
}

#[no_mangle]
pub extern "C" fn loon_file_provider_bridge_lookup_item(
    request_json: *const c_char,
) -> *mut c_char {
    ffi_call(request_json, interop_lookup_item)
}

#[no_mangle]
pub extern "C" fn loon_file_provider_bridge_list_children(
    request_json: *const c_char,
) -> *mut c_char {
    ffi_call(request_json, interop_list_children)
}

#[no_mangle]
pub extern "C" fn loon_file_provider_bridge_materialize_item(
    request_json: *const c_char,
) -> *mut c_char {
    ffi_call(request_json, interop_materialize_item)
}

/// Frees a string previously returned by a `loon_file_provider_*` FFI function.
///
/// # Safety
///
/// `value` must be a pointer returned by a `loon_file_provider_*` function in this library
/// (i.e. produced via `CString::into_raw`), and it must not have been freed before.
#[no_mangle]
pub unsafe extern "C" fn loon_file_provider_string_free(value: *mut c_char) {
    if value.is_null() {
        return;
    }
    // SAFETY: `value` must come from `CString::into_raw` in this library. Converting it back frees
    // the allocation exactly once.
    unsafe {
        let _ = CString::from_raw(value);
    }
}

struct NamespaceProjection {
    items: BTreeMap<ProviderItemId, ProviderItemSnapshot>,
    children: BTreeMap<ProviderItemId, Vec<ProviderItemSnapshot>>,
    warnings: Vec<ProviderProjectionWarning>,
}

impl NamespaceProjection {
    fn build(
        namespace_id: &NamespaceId,
        namespace_root: &Path,
        summary: &ClientNamespaceStateSummary,
        parent_links: &[LocalOnlyParentLinkRow],
    ) -> Self {
        let path_index = NamespacePathIndex::build(summary, parent_links);
        let mut items = BTreeMap::new();
        let mut warnings = Vec::new();

        let inode_ids = summary
            .local_state
            .iter()
            .map(|row| row.inode_id)
            .chain(summary.remote_state.iter().map(|row| row.inode_id))
            .chain(summary.sync_anchors.iter().map(|row| row.inode_id))
            .collect::<BTreeSet<_>>();

        for inode_id in inode_ids {
            let views = FileSyncViews {
                namespace_id: namespace_id.clone(),
                inode_id,
                remote: summary
                    .remote_state
                    .iter()
                    .find(|row| row.inode_id == inode_id)
                    .cloned(),
                local: summary
                    .local_state
                    .iter()
                    .find(|row| row.inode_id == inode_id)
                    .cloned(),
                sync_anchor: summary
                    .sync_anchors
                    .iter()
                    .find(|row| row.inode_id == inode_id)
                    .cloned(),
            };

            if is_root_inode(&views) {
                continue;
            }

            let inode_kind = match choose_inode_kind(&views) {
                Some(kind) => kind,
                None => continue,
            };
            let relative_path = resolve_bound_relative_path(&path_index, &views, inode_id);
            if inode_kind == InodeKind::Symlink || inode_kind == InodeKind::Mount {
                warnings.push(ProviderProjectionWarning {
                    namespace_id: namespace_id.clone(),
                    relative_path: relative_path.unwrap_or_default(),
                    inode_kind: inode_kind.clone(),
                    reason: "unsupported_inode_kind".to_owned(),
                });
                continue;
            }
            if views.remote.as_ref().is_some_and(|row| row.is_deleted) {
                continue;
            }

            let Some(relative_path) = relative_path else {
                continue;
            };
            let item_id = ProviderItemId::BoundInode {
                namespace_id: namespace_id.clone(),
                inode_id,
            };
            let parent_item_id = bound_parent_item_id(namespace_id, &views);
            let materialization_state =
                bound_materialization_state(summary, namespace_id, inode_id, &views);
            items.insert(
                item_id.clone(),
                ProviderItemSnapshot {
                    item_id,
                    parent_item_id,
                    display_name: display_name_for_bound(&views),
                    inode_kind,
                    materialization_state,
                    current_relative_path: Some(relative_path.clone()),
                    size_bytes: materialized_size_bytes(namespace_root, &relative_path),
                    revision_no: bound_revision_no(&views),
                    content_digest: bound_content_digest(&views),
                    content_manifest_digest: bound_content_manifest_digest(&views),
                },
            );
        }

        for row in &summary.local_only_state {
            let relative_path = path_index
                .resolve_local_only_source_relative_path(&row.client_file_id)
                .map(str::to_owned);
            let Some(relative_path) = relative_path else {
                continue;
            };
            if row.inode_kind == InodeKind::Symlink || row.inode_kind == InodeKind::Mount {
                warnings.push(ProviderProjectionWarning {
                    namespace_id: namespace_id.clone(),
                    relative_path,
                    inode_kind: row.inode_kind.clone(),
                    reason: "unsupported_inode_kind".to_owned(),
                });
                continue;
            }

            let item_id = ProviderItemId::LocalOnly {
                namespace_id: namespace_id.clone(),
                client_file_id: row.client_file_id.clone(),
            };
            let parent_item_id = match path_index.local_only_parent_ref_for(row) {
                Some(LocalOnlyParentRef::Bound { parent_inode_id }) if parent_inode_id.0 == 1 => {
                    Some(ProviderItemId::NamespaceRoot {
                        namespace_id: namespace_id.clone(),
                    })
                }
                Some(LocalOnlyParentRef::Bound { parent_inode_id }) => {
                    Some(ProviderItemId::BoundInode {
                        namespace_id: namespace_id.clone(),
                        inode_id: parent_inode_id,
                    })
                }
                Some(LocalOnlyParentRef::LocalOnly {
                    parent_client_file_id,
                }) => Some(ProviderItemId::LocalOnly {
                    namespace_id: namespace_id.clone(),
                    client_file_id: parent_client_file_id,
                }),
                None => Some(ProviderItemId::NamespaceRoot {
                    namespace_id: namespace_id.clone(),
                }),
            };
            items.insert(
                item_id.clone(),
                ProviderItemSnapshot {
                    item_id,
                    parent_item_id,
                    display_name: row.display_name.clone(),
                    inode_kind: row.inode_kind.clone(),
                    materialization_state: if row.exists_on_disk {
                        ProviderMaterializationState::Materialized
                    } else {
                        ProviderMaterializationState::Unavailable
                    },
                    current_relative_path: Some(relative_path.clone()),
                    size_bytes: materialized_size_bytes(namespace_root, &relative_path),
                    revision_no: None,
                    content_digest: row.content_digest.clone(),
                    content_manifest_digest: None,
                },
            );
        }

        let mut children = BTreeMap::<ProviderItemId, Vec<ProviderItemSnapshot>>::new();
        for item in items.values().cloned() {
            if let Some(parent) = item.parent_item_id.clone() {
                children.entry(parent).or_default().push(item);
            }
        }
        for snapshots in children.values_mut() {
            snapshots.sort_by(|left, right| {
                left.display_name
                    .cmp(&right.display_name)
                    .then_with(|| left.item_id.cmp(&right.item_id))
            });
        }

        warnings.sort_by(|left, right| {
            left.relative_path
                .cmp(&right.relative_path)
                .then_with(|| left.inode_kind_text().cmp(right.inode_kind_text()))
        });

        Self {
            items,
            children,
            warnings,
        }
    }
}

impl ProviderProjectionWarning {
    fn inode_kind_text(&self) -> &'static str {
        inode_kind_text(&self.inode_kind)
    }
}

fn inode_kind_text(inode_kind: &InodeKind) -> &'static str {
    match inode_kind {
        InodeKind::File => "file",
        InodeKind::Dir => "dir",
        InodeKind::Symlink => "symlink",
        InodeKind::Mount => "mount",
    }
}

fn choose_inode_kind(views: &FileSyncViews) -> Option<InodeKind> {
    views
        .local
        .as_ref()
        .map(|row| row.inode_kind.clone())
        .or_else(|| views.remote.as_ref().map(|row| row.inode_kind.clone()))
        .or_else(|| views.sync_anchor.as_ref().map(|row| row.inode_kind.clone()))
}

fn is_root_inode(views: &FileSyncViews) -> bool {
    views.inode_id.0 == 1
        && views
            .local
            .as_ref()
            .map(|row| row.parent_inode_id.is_none())
            .unwrap_or_else(|| {
                views
                    .remote
                    .as_ref()
                    .map(|row| row.parent_inode_id.is_none())
                    .or_else(|| {
                        views
                            .sync_anchor
                            .as_ref()
                            .map(|row| row.parent_inode_id.is_none())
                    })
                    .unwrap_or(false)
            })
}

fn resolve_bound_relative_path(
    path_index: &NamespacePathIndex,
    views: &FileSyncViews,
    inode_id: InodeId,
) -> Option<String> {
    if let Some(path) = path_index.resolve_current_inode_relative_path(inode_id) {
        return Some(path.to_owned());
    }
    if let Some(remote) = &views.remote {
        return path_index.resolve_target_inode_relative_path(
            inode_id,
            remote.parent_inode_id,
            &remote.display_name,
        );
    }
    if let Some(anchor) = &views.sync_anchor {
        return path_index.resolve_target_inode_relative_path(
            inode_id,
            anchor.parent_inode_id,
            &anchor.display_name,
        );
    }
    if let Some(local) = &views.local {
        return path_index.resolve_target_inode_relative_path(
            inode_id,
            local.parent_inode_id,
            &local.display_name,
        );
    }
    None
}

fn display_name_for_bound(views: &FileSyncViews) -> String {
    views
        .local
        .as_ref()
        .map(|row| row.display_name.clone())
        .or_else(|| views.remote.as_ref().map(|row| row.display_name.clone()))
        .or_else(|| {
            views
                .sync_anchor
                .as_ref()
                .map(|row| row.display_name.clone())
        })
        .unwrap_or_default()
}

fn bound_parent_item_id(
    namespace_id: &NamespaceId,
    views: &FileSyncViews,
) -> Option<ProviderItemId> {
    let parent_inode_id = views
        .local
        .as_ref()
        .and_then(|row| row.parent_inode_id)
        .or_else(|| views.remote.as_ref().and_then(|row| row.parent_inode_id))
        .or_else(|| {
            views
                .sync_anchor
                .as_ref()
                .and_then(|row| row.parent_inode_id)
        });

    match parent_inode_id {
        None => None,
        Some(parent_inode_id) if parent_inode_id.0 == 1 => Some(ProviderItemId::NamespaceRoot {
            namespace_id: namespace_id.clone(),
        }),
        Some(parent_inode_id) => Some(ProviderItemId::BoundInode {
            namespace_id: namespace_id.clone(),
            inode_id: parent_inode_id,
        }),
    }
}

fn bound_materialization_state(
    summary: &ClientNamespaceStateSummary,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    views: &FileSyncViews,
) -> ProviderMaterializationState {
    if views.local.as_ref().is_some_and(|row| row.exists_on_disk) {
        return ProviderMaterializationState::Materialized;
    }
    if summary
        .conflicts_and_errors
        .iter()
        .any(|row| row.inode_id == inode_id)
        || summary
            .pending_inode_mutations
            .iter()
            .any(|row| row.inode_id == inode_id)
    {
        return ProviderMaterializationState::Unavailable;
    }
    if let Some(planned) = summary
        .planned_actions
        .iter()
        .find(|row| row.inode_id == inode_id)
    {
        if planned.decision == "download_remote_edit"
            || planned.decision == "materialize_remote_dir"
        {
            return ProviderMaterializationState::Placeholder;
        }
        return ProviderMaterializationState::Unavailable;
    }
    if bound_is_remote_placeholder(views) {
        return ProviderMaterializationState::Placeholder;
    }
    if views.remote.is_some() && views.local.is_none() {
        return ProviderMaterializationState::Placeholder;
    }
    let _ = namespace_id;
    ProviderMaterializationState::Unavailable
}

fn bound_is_remote_placeholder(views: &FileSyncViews) -> bool {
    let Some(local) = views.local.as_ref() else {
        return false;
    };
    let Some(remote) = views.remote.as_ref() else {
        return false;
    };
    !local.exists_on_disk
        && !local.dirty
        && local.inode_kind == remote.inode_kind
        && local.parent_inode_id == remote.parent_inode_id
        && local.display_name == remote.display_name
}

fn materialized_size_bytes(namespace_root: &Path, relative_path: &str) -> Option<u64> {
    let absolute_path = join_under_mirror_root(namespace_root, relative_path);
    fs::metadata(absolute_path)
        .ok()
        .and_then(|metadata| metadata.is_file().then_some(metadata.len()))
}

fn bound_revision_no(views: &FileSyncViews) -> Option<RevisionNo> {
    views
        .remote
        .as_ref()
        .map(|row| row.revision_no)
        .or_else(|| views.sync_anchor.as_ref().map(|row| row.revision_no))
}

fn bound_content_digest(views: &FileSyncViews) -> Option<String> {
    views
        .local
        .as_ref()
        .and_then(|row| row.content_digest.clone())
        .or_else(|| {
            views
                .remote
                .as_ref()
                .and_then(|row| row.content_digest.clone())
        })
        .or_else(|| {
            views
                .sync_anchor
                .as_ref()
                .and_then(|row| row.content_digest.clone())
        })
}

fn bound_content_manifest_digest(views: &FileSyncViews) -> Option<String> {
    views
        .remote
        .as_ref()
        .and_then(|row| row.content_manifest_digest.clone())
        .or_else(|| {
            views
                .sync_anchor
                .as_ref()
                .and_then(|row| row.content_manifest_digest.clone())
        })
}

fn encode_hex(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len() * 2);
    for byte in value.as_bytes() {
        encoded.push(nibble_to_hex(byte >> 4));
        encoded.push(nibble_to_hex(byte & 0x0f));
    }
    encoded
}

fn decode_hex(value: &str) -> Result<String, ProviderItemIdDecodeError> {
    if value.len() % 2 != 0 {
        return Err(ProviderItemIdDecodeError::InvalidHex(value.to_owned()));
    }
    let mut bytes = Vec::with_capacity(value.len() / 2);
    let value_bytes = value.as_bytes();
    let mut index = 0;
    while index < value_bytes.len() {
        let high = hex_to_nibble(value_bytes[index])
            .ok_or_else(|| ProviderItemIdDecodeError::InvalidHex(value.to_owned()))?;
        let low = hex_to_nibble(value_bytes[index + 1])
            .ok_or_else(|| ProviderItemIdDecodeError::InvalidHex(value.to_owned()))?;
        bytes.push((high << 4) | low);
        index += 2;
    }
    String::from_utf8(bytes).map_err(|_| ProviderItemIdDecodeError::InvalidUtf8)
}

fn nibble_to_hex(nibble: u8) -> char {
    match nibble {
        0..=9 => (b'0' + nibble) as char,
        10..=15 => (b'a' + (nibble - 10)) as char,
        _ => unreachable!("nibble must be in 0..=15"),
    }
}

fn hex_to_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}
