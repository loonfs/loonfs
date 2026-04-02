pub fn blob(namespace: &str, digest: &str) -> String {
    format!("namespaces/{namespace}/blobs/{digest}")
}

pub fn content_manifest(namespace: &str, digest: &str) -> String {
    format!("namespaces/{namespace}/manifests/{digest}.json")
}

pub fn conflict_artifact(namespace: &str, conflict_id: &str) -> String {
    format!("namespaces/{namespace}/conflicts/{conflict_id}.json")
}

pub fn conflict_artifact_prefix(namespace: &str) -> String {
    format!("namespaces/{namespace}/conflicts/")
}

pub fn conflict_artifact_archive(namespace: &str, conflict_id: &str) -> String {
    format!("namespaces/{namespace}/conflict-archives/{conflict_id}.json")
}

pub fn conflict_artifact_archive_prefix(namespace: &str) -> String {
    format!("namespaces/{namespace}/conflict-archives/")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_builders_match_spec_examples() {
        assert_eq!(
            blob("ns-1", "sha256:abcd"),
            "namespaces/ns-1/blobs/sha256:abcd"
        );
        assert_eq!(
            content_manifest("ns-1", "sha256:manifest-abcd"),
            "namespaces/ns-1/manifests/sha256:manifest-abcd.json"
        );
        assert_eq!(
            conflict_artifact("ns-1", "conflict-deadbeef"),
            "namespaces/ns-1/conflicts/conflict-deadbeef.json"
        );
        assert_eq!(
            conflict_artifact_prefix("ns-1"),
            "namespaces/ns-1/conflicts/"
        );
        assert_eq!(
            conflict_artifact_archive("ns-1", "conflict-deadbeef"),
            "namespaces/ns-1/conflict-archives/conflict-deadbeef.json"
        );
        assert_eq!(
            conflict_artifact_archive_prefix("ns-1"),
            "namespaces/ns-1/conflict-archives/"
        );
    }
}
