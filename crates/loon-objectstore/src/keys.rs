pub fn namespace_head(namespace: &str) -> String {
    format!("namespaces/{namespace}/head.json")
}

pub fn namespace_lease(namespace: &str) -> String {
    format!("namespaces/{namespace}/lease.json")
}

pub fn wal_commit(namespace: &str, seq: u64, commit_id: &str) -> String {
    format!("namespaces/{namespace}/wal/{seq:020}-{commit_id}.json")
}

pub fn blob(namespace: &str, digest: &str) -> String {
    format!("namespaces/{namespace}/blobs/{digest}")
}
