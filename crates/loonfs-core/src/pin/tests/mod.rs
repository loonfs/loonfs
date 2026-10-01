//! Pin creation, listing, verification, snapshot, and cleanup tests.

#![allow(clippy::panic)]
// These tests use panic in impossible match arms to preserve precise failure messages.

mod inventory;
mod pin_cleanup;
mod pin_verification;
mod snapshot_fork_gc;
mod snapshot_renewal;

use super::{create, record};
use crate::error::{CoreError, ErrorCode};
use crate::manifest::tests::*;
use crate::manifest::{
    compaction_step, fold_wal, CompactionStepOutcome, MetadataCompactionPolicy, MetadataLsmPolicy,
};
use crate::namespace::control::{load_current_manifest, load_namespace_read_state};
use crate::namespace::writer_epoch::acquire_writer_epoch;
use crate::path::read::load_current_metadata_view;
use crate::pin::record::load_pin;
use crate::test_support::ops::{create, write_file_bytes};
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
    BlockingStore, FailStore, InjectedError, KeyPredicate, OperationClass, RecordingStore,
};
use loonfs_types::{ManifestNo, NamespaceId, PinId};
use std::collections::BTreeSet;
use std::sync::Arc;
use tempfile::tempdir;
