//! Pin creation, listing, verification, snapshot, and cleanup tests.

mod inventory;
mod pin_cleanup;
mod pin_verification;
mod snapshot_fork_gc;
mod snapshot_renewal;

use super::record;
use crate::error::{CoreError, ErrorCode};
use crate::manifest::tests::*;
use crate::manifest::{CompactionUnitOutcome, MetadataCompactionPolicy};
use crate::time::{Deadline, StdMonotonicTimer};
use crate::MutationContext;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use loonfs_objectstore::keys::{
    hint, metadata_manifest_object, metadata_manifest_prefix, metadata_segment_object_key,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::{
    ByteRange, ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode,
};
use loonfs_test_support::stores::{
    BlockingStore, FailStore, InjectedError, KeyPredicate, OperationSelector, RecordingStore,
};
use loonfs_types::{ManifestNo, NamespaceId, PinId};
use std::collections::BTreeSet;
use std::sync::Arc;
use tempfile::tempdir;
