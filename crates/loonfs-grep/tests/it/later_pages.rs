//! Pages after the first: a later page decides its cut at its cursor's
//! sequence as the service, and serves each file of the cut by whether it
//! changed since then. A file whose state at the head is its state at that
//! sequence is served with the subject's rights at the head, and any other
//! file only to a subject with the `history` right.

use crate::common::{page_request, GrepHost};
use loonfs::publish::{CommitIntent, FilesystemOperation};
use loonfs::{
    CreateNamespaceOptions, LoonFs, Namespace, NamespaceId, PutFileOptions, SharedObjectStore,
    Writable,
};
use loonfs_grep::NamespaceReads;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_types::{
    AbsolutePath, AccessGrants, AccessRight, AccessRights, CommitId, GrepRequest, GrepResponse,
    GrepScope, NamespaceAccess, PrincipalId, PrincipalScope, PrincipalSet, Subject, SubjectId,
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use tempfile::TempDir;

fn subject(principal: &str) -> Subject {
    Subject {
        principal_scope: PrincipalScope::parse("org_demo").expect("scope"),
        subject_id: SubjectId::parse(principal).expect("subject"),
        principals: PrincipalSet::new(BTreeSet::from([
            PrincipalId::parse(principal).expect("principal")
        ]))
        .expect("principals"),
    }
}

fn grants(entries: &[(&str, &[AccessRight])]) -> AccessGrants {
    AccessGrants::new(
        entries
            .iter()
            .map(|(principal, rights)| {
                (
                    PrincipalId::parse(principal).expect("principal"),
                    AccessRights::from_iter(rights.iter().copied()),
                )
            })
            .collect::<BTreeMap<_, _>>(),
    )
    .expect("grants")
}

fn request(pattern: &str, scope: GrepScope) -> GrepRequest {
    GrepRequest {
        pattern: pattern.to_owned(),
        case_insensitive: false,
        scope,
        allow_stale: false,
        allow_scan: false,
    }
}

fn paths(response: &GrepResponse) -> Vec<String> {
    response
        .matches
        .iter()
        .map(|found| found.path.as_str().to_owned())
        .collect()
}

/// An ACL namespace that `prn_root` administers, with a `/team` directory
/// that `viewer` reads and `historian` reads with the `history` right.
struct AclPages {
    _directory: TempDir,
    store: SharedObjectStore,
    namespace_id: NamespaceId,
    admin: Namespace<Writable>,
    host: GrepHost,
}

impl AclPages {
    async fn new(name: &str) -> Self {
        let directory = tempfile::tempdir().expect("tempdir");
        let store: SharedObjectStore =
            Arc::new(LocalFsStore::new(directory.path()).expect("store"));
        let writer = LoonFs::builder_with_store(store.clone())
            .writer_id(loonfs_test_support::ids::writer_id(name))
            .config(loonfs::RuntimeConfig {
                min_publish_interval_ms: 0,
                ..loonfs::RuntimeConfig::default()
            })
            .build()
            .await
            .expect("writer");
        let namespace_id = NamespaceId::parse(name).expect("namespace");
        writer
            .create_namespace_with_options(
                &namespace_id,
                &loonfs_test_support::test_actor(),
                &CreateNamespaceOptions {
                    access: NamespaceAccess::Acl {
                        principal_scope: PrincipalScope::parse("org_demo").expect("scope"),
                        root_grants: grants(&[("prn_root", &[AccessRight::Admin])]),
                    },
                    ..Default::default()
                },
            )
            .await
            .expect("namespace");
        let admin = writer
            .open_namespace(&namespace_id)
            .expect("open namespace")
            .with_subject(subject("prn_root"));
        let pages = Self {
            _directory: directory,
            host: GrepHost::new(&store, name).await,
            store,
            namespace_id,
            admin,
        };
        pages.directory("/team").await;
        pages
    }

    /// Creates the directory `path`, which `viewer` reads and `historian`
    /// reads with the `history` right.
    async fn directory(&self, path: &str) {
        self.admin
            .create_directory(path, &loonfs_test_support::test_actor())
            .await
            .expect("create the directory");
        self.admin
            .commit(
                CommitIntent::single(
                    CommitId::generate(),
                    loonfs_test_support::test_actor(),
                    None,
                    FilesystemOperation::UpdateAccess {
                        path: AbsolutePath::parse(path).expect("path"),
                        boundary: false,
                        grants: grants(&[
                            ("viewer", &[AccessRight::Read]),
                            ("historian", &[AccessRight::Read, AccessRight::History]),
                        ]),
                        expected_inode_id: None,
                        expected_access_revision_no: None,
                    },
                )
                .with_subject(subject("prn_root")),
            )
            .await
            .expect("grant the directory");
    }

    async fn put(&self, path: &str, bytes: &[u8]) {
        self.admin
            .put_file_with_options(
                path,
                bytes,
                &loonfs_test_support::test_actor(),
                &PutFileOptions {
                    behavior: loonfs::DestinationBehavior::Replace,
                    ..Default::default()
                },
            )
            .await
            .expect("write the file");
    }

    async fn enable(&self) {
        self.host
            .enable_grep_index(&self.namespace_id)
            .await
            .expect("enable");
    }

    async fn page(
        &self,
        principal: &str,
        request: &GrepRequest,
        cursor: Option<String>,
        limit: u32,
    ) -> GrepResponse {
        let reads = NamespaceReads::new(self.host.reader.clone(), &self.namespace_id)
            .with_subject(subject(principal));
        self.host
            .service
            .query(request, page_request(cursor, limit), &reads, &self.store)
            .await
            .expect("grep page")
    }

    /// Returns the first page of one match for each principal, and the
    /// order in which the walk visits the files that `request` matches.
    async fn first_pages(
        &self,
        request: &GrepRequest,
        principals: &[&str],
    ) -> (Vec<String>, Vec<String>) {
        let order = paths(&self.page("prn_root", request, None, 100).await);
        let mut cursors = Vec::new();
        for principal in principals {
            let first = self.page(principal, request, None, 1).await;
            assert_eq!(paths(&first), [order[0].clone()]);
            cursors.push(first.next_cursor.expect("a cursor"));
        }
        (cursors, order)
    }
}

/// An inode scope renamed between pages, with one file changed and one
/// unchanged: the cut is the scope at the first page's sequence, so a
/// subject with `history` gets both files under their paths then, and a
/// `read`-only subject gets neither, because the rename changed both paths.
#[tokio::test]
async fn an_inode_scope_renamed_between_pages_keeps_its_cut() {
    let pages = AclPages::new("pages-renamed-scope").await;
    for name in ["a", "b", "c"] {
        pages
            .put(
                &format!("/team/{name}"),
                format!("scope needle {name}\n").as_bytes(),
            )
            .await;
    }
    pages.enable().await;
    let team = pages
        .admin
        .stat("/team")
        .await
        .expect("stat /team")
        .inode_id;
    let request = request("scope needle", GrepScope::Inode(team));
    let (cursors, order) = pages.first_pages(&request, &["historian", "viewer"]).await;
    let (unchanged, changed) = (&order[1], &order[2]);

    pages
        .admin
        .move_path("/team", "/crew", &loonfs_test_support::test_actor())
        .await
        .expect("rename the scope");
    pages
        .put(
            &changed.replace("/team", "/crew"),
            b"scope needle changed\n",
        )
        .await;

    let history = pages
        .page("historian", &request, Some(cursors[0].clone()), 10)
        .await;
    assert_eq!(paths(&history), [unchanged.clone(), changed.clone()]);
    assert_eq!(
        history.matches[1].line,
        format!("scope needle {}", &changed["/team/".len()..])
    );
    let read_only = pages
        .page("viewer", &request, Some(cursors[1].clone()), 10)
        .await;
    assert!(read_only.matches.is_empty(), "{:?}", paths(&read_only));
}

/// An unchanged file moved out of a path scope between pages is in the cut,
/// so a subject with `history` gets it under its path at the first page's
/// sequence, and a `read`-only subject doesn't, because its path changed.
#[tokio::test]
async fn a_file_moved_out_of_scope_between_pages_stays_in_the_cut() {
    let pages = AclPages::new("pages-moved-out").await;
    pages.directory("/elsewhere").await;
    for name in ["a", "b"] {
        pages
            .put(
                &format!("/team/{name}"),
                format!("moved needle {name}\n").as_bytes(),
            )
            .await;
    }
    pages.enable().await;
    let request = request(
        "moved needle",
        GrepScope::PathPrefix(AbsolutePath::parse("/team").expect("path")),
    );
    let (cursors, order) = pages.first_pages(&request, &["historian", "viewer"]).await;
    let moved = &order[1];
    pages
        .admin
        .move_path(
            moved,
            &moved.replace("/team", "/elsewhere"),
            &loonfs_test_support::test_actor(),
        )
        .await
        .expect("move the file out of the scope");

    let history = pages
        .page("historian", &request, Some(cursors[0].clone()), 10)
        .await;
    assert_eq!(paths(&history), std::slice::from_ref(moved));
    let read_only = pages
        .page("viewer", &request, Some(cursors[1].clone()), 10)
        .await;
    assert!(read_only.matches.is_empty(), "{:?}", paths(&read_only));
}

/// A scan has no index to page through, so a later page lists the scope at
/// its cursor's sequence as the service, and a `read`-only subject pages
/// through every file it can read.
#[tokio::test]
async fn a_read_only_subject_pages_through_a_scan() {
    let pages = AclPages::new("pages-scan").await;
    for name in ["a", "b", "c"] {
        pages
            .put(
                &format!("/team/{name}"),
                format!("scan needle {name}\n").as_bytes(),
            )
            .await;
    }
    pages.enable().await;
    let mut scan = request(
        "ne",
        GrepScope::PathPrefix(AbsolutePath::parse("/team").expect("path")),
    );
    scan.allow_scan = true;

    let mut walked = Vec::new();
    let mut cursor = None;
    loop {
        let page = pages.page("viewer", &scan, cursor, 1).await;
        assert_eq!(page.matches.len(), 1, "every page has a match");
        walked.extend(paths(&page));
        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    walked.sort();
    assert_eq!(walked, ["/team/a", "/team/b", "/team/c"]);
}

/// A reorganization into the base level scans the namespace's revisions as
/// the service, so it runs in an ACL namespace, whose reads otherwise need a
/// subject.
#[tokio::test]
async fn a_base_reorganization_of_an_acl_namespace_scans_as_the_service() {
    let pages = AclPages::new("pages-acl-reorganize").await;
    pages.put("/team/a", b"reorganize needle\n").await;
    pages.enable().await;
    let to_base = loonfs_grep::GramIndexBuildPolicy {
        max_delta_runs: loonfs_test_support::ids::nonzero_usize(1),
        max_mid_runs: loonfs_test_support::ids::nonzero_usize(1),
        ..loonfs_grep::GramIndexBuildPolicy::default()
    };
    let mut levels = BTreeSet::new();
    for _ in 0..4 {
        pages
            .host
            .worker
            .steps(&pages.namespace_id, 0)
            .reorganize_step(to_base)
            .await
            .expect("a reorganize step");
        levels.extend(
            pages
                .host
                .worker
                .entry(&pages.namespace_id)
                .await
                .expect("entry")
                .expect("an enabled index")
                .runs()
                .iter()
                .map(|run| run.level),
        );
    }
    assert!(
        levels.contains(&2),
        "the index reached the base level: {levels:?}"
    );
}
