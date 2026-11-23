//! One commit's state and the writes that change it.
//!
//! [`commit_session`] layers the rows that the commit's earlier writes would
//! persist over the loaded metadata. [`writes`] defines each write that a
//! planner calls, [`checks`] the rules that the writes check against the
//! commit's state, and [`numbering`] the indexes of the commit's operations
//! and deltas.

mod checks;
mod commit_session;
mod numbering;
mod writes;

pub(crate) use commit_session::CommitSession;
pub(crate) use writes::NewChild;
