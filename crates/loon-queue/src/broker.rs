use crate::types::{QueueJob, SeqScopedPayload};

pub fn merge_seq_payload(existing: &mut SeqScopedPayload, incoming: SeqScopedPayload) {
    if incoming.through_seq.0 > existing.through_seq.0 {
        existing.through_seq = incoming.through_seq;
    }
}

pub fn attach_follow_up(job: &mut QueueJob, incoming: SeqScopedPayload) {
    match &mut job.follow_up {
        Some(existing) => merge_seq_payload(existing, incoming),
        None => job.follow_up = Some(incoming),
    }
}
