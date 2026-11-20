//! Integration tests for `namespace list`.

use super::common::*;

fn listed_namespace_ids(harness: &Harness) -> Vec<String> {
    let listed = harness.run(&["--json", "namespace", "list"]);
    assert_success(&listed);
    let data = json_data(&listed);
    assert_eq!(data["kind"], "namespaces_listed");
    data["namespaces"]
        .as_array()
        .expect("namespace array")
        .iter()
        .map(|namespace| {
            namespace["namespace_id"]
                .as_str()
                .expect("namespace ID")
                .to_owned()
        })
        .collect()
}

#[test]
fn namespace_list_returns_live_namespaces_in_id_order() {
    let harness = Harness::with_demo_namespace();
    assert_success(&harness.run(&["--json", "namespace", "create", "alpha"]));
    assert_eq!(listed_namespace_ids(&harness), ["alpha", "demo"]);

    assert_success(&harness.run(&["--json", "namespace", "delete", "demo", "--yes"]));
    assert_eq!(listed_namespace_ids(&harness), ["alpha"]);

    let human = harness.run(&["namespace", "list"]);
    assert_success(&human);
    assert!(
        stdout_string(&human)
            .lines()
            .any(|line| line.starts_with("alpha\t")),
        "{}",
        stdout_string(&human)
    );
}
