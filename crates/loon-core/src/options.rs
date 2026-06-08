use crate::path::intent::PutFileBehavior;
use loon_api::CommitId;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BootstrapOptions {
    pub allow_existing: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ForkOptions {}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReadOptions {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteOptions {
    pub commit_id: Option<CommitId>,
    pub put_file_behavior: PutFileBehavior,
    pub recursive_delete: bool,
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self {
            commit_id: None,
            put_file_behavior: PutFileBehavior::CreateOnly,
            recursive_delete: true,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CommitOptions {}
