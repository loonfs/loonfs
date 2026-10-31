//! Subtree rows across folds. A fold writes the newest subtree row of each
//! directory edge among the commits that it folds, so a directory's subtree
//! sequence is exact at every manifest's head and can be older between
//! manifests.

use super::*;
use loonfs_types::DisplayName;

/// The subtree sequence of the directory at `path` in `state` at `seq`: the
/// sequence of its edge's newest slot-index row at `seq`.
fn subtree_seq_at(state: &MetadataState, path: &str, seq: ChangeSeq) -> ChangeSeq {
    let resolved = state
        .resolve_visible_path(&AbsolutePath::parse(path).expect("path"), seq)
        .expect("a visible directory");
    let display_name = DisplayName::parse(&resolved.display_name).expect("display name");
    state
        .bound_child_at_seq(
            resolved
                .parent_inode_id
                .expect("a directory below the root"),
            &NameKey::for_display_name(&display_name),
            seq,
        )
        .filter(|binding| binding.is_bound())
        .expect("the directory is bound")
        .committed_seq
}

/// Checks that a namespace folded twice reports the subtree sequences of an
/// unfolded namespace with the same commits at both manifests' heads, and
/// that a read inside the second fold's range can report the value at the
/// end of the first fold.
#[tokio::test]
async fn a_fold_keeps_subtree_sequences_exact_at_manifest_heads() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = test_context();
    let folded = NamespaceId::parse("folded").expect("namespace id");
    let unfolded = NamespaceId::parse("unfolded").expect("namespace id");
    for namespace_id in [&folded, &unfolded] {
        crate::test_support::NamespaceFixture::new(&store, namespace_id, &context)
            .create()
            .await
            .expect("bootstrap");
    }
    // Both namespaces commit the same writes at the same sequences: 1 binds
    // `/a` and `/a/b`, 2 to 4 advance both, and 5 advances `/a` alone. The
    // folded namespace folds after 2 and after 5.
    let mut manifest_heads = Vec::new();
    for (path, fold) in [
        ("/a/b/one.txt", false),
        ("/a/b/two.txt", true),
        ("/a/b/three.txt", false),
        ("/a/b/four.txt", false),
        ("/a/five.txt", true),
    ] {
        for namespace_id in [&folded, &unfolded] {
            crate::test_support::NamespaceFixture::new(&store, namespace_id, &context)
                .write_file_bytes(path, path.as_bytes(), None)
                .await
                .expect("write");
        }
        if fold {
            let folded = crate::test_support::namespace_store(&store, &folded)
                .fold_wal()
                .await
                .expect("fold");
            manifest_heads.push(folded.manifest_head_seq);
        }
    }
    assert_eq!(manifest_heads, [ChangeSeq(2), ChangeSeq(5)]);
    let folded_state = load_manifest_materialization_for_inspection(
        &store,
        &folded,
        current_manifest_number(&store, &folded).await,
    )
    .await
    .expect("materialize the folded namespace")
    .metadata_state;
    let unfolded_state = crate::test_support::namespace_store(&store, &unfolded)
        .load_manifest_projection(crate::test_support::observed_now())
        .await
        .expect("replay the unfolded namespace")
        .tail_state
        .merged_rows();

    for path in ["/a", "/a/b"] {
        for seq in &manifest_heads {
            assert_eq!(
                subtree_seq_at(&folded_state, path, *seq),
                subtree_seq_at(&unfolded_state, path, *seq),
                "{path} at {seq}"
            );
        }
    }
    // The second fold keeps `/a`'s subtree row at 5 and drops the rows at 3
    // and 4, so a read at 3 or 4 finds the first fold's row at 2. The newest
    // subtree row of `/a/b` in the second fold is at 4.
    for seq in [ChangeSeq(3), ChangeSeq(4)] {
        assert_eq!(subtree_seq_at(&unfolded_state, "/a", seq), seq);
        assert_eq!(subtree_seq_at(&folded_state, "/a", seq), ChangeSeq(2));
    }
    assert_eq!(
        subtree_seq_at(&folded_state, "/a/b", ChangeSeq(3)),
        ChangeSeq(2)
    );
    assert_eq!(
        subtree_seq_at(&folded_state, "/a/b", ChangeSeq(4)),
        ChangeSeq(4)
    );
}
