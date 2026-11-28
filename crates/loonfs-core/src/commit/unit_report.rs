//! [`UnitReport`], the value of one unit of a writer session with what the
//! value rests on, which says whether the session confirms its position
//! before it returns the value.

use crate::manifest::PublicationReport;

/// What the value of one unit of a writer session rests on: a commit batch,
/// a deletion, or a publication of the session's lane.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnitBasis {
    /// The store. The unit loaded its view from the store (a claim's
    /// discovery or the writer's load), or it sent its put, which fails when
    /// another writer's fence or claim is at the put's number.
    Store,
    /// The session's own state alone. The unit computed its value from the
    /// session's kept position, or a publication of the lane from the
    /// session's newest published manifest, and sent no put, such as a batch
    /// whose commits were all refused. Another writer may have replaced that
    /// manifest since the session last checked it, so the session confirms
    /// its position before it returns the value (format spec, "Publication
    /// and group commit").
    Session,
}

/// The value of one unit of a writer session, and what the value rests on.
#[derive(Debug)]
pub struct UnitReport<T> {
    pub value: T,
    pub basis: UnitBasis,
}

impl<T> UnitReport<T> {
    /// Returns `value` as the value of a unit that rests on the store.
    pub fn from_store(value: T) -> Self {
        Self {
            value,
            basis: UnitBasis::Store,
        }
    }

    /// Returns the response of a publication of the session's lane, which
    /// rests on the session's own state alone when it published nothing.
    pub fn from_publication(report: PublicationReport<T>) -> Self {
        Self {
            basis: match report.published {
                Some(_) => UnitBasis::Store,
                None => UnitBasis::Session,
            },
            value: report.response,
        }
    }
}
