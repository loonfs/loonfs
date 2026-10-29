//! Command implementations, one submodule per command group.

mod access;
mod completion;
mod config;
mod context;
mod dispatch;
pub(crate) use context::shell_quote;
pub(crate) mod download;
mod fs;
mod inspection;
mod maintenance;
mod namespace;
mod output;
mod pagination;
mod partial;
mod profile;
mod profile_config;
mod profile_update;
mod recursive;
mod snapshot;
mod store_builders;
mod tree_failures;
pub(crate) use tree_failures::TreeTransferFailures;

pub(crate) use self::output::{
    CommandData, CommandFailure, CommandOutput, DoctorCheck, DoctorStatus, ListingHeadDrift,
    MaintenanceRan, TrashListing,
};

pub(crate) use self::dispatch::run;
