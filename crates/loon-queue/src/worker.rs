#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerLease {
    pub worker_id: String,
    pub claim_token: String,
}
