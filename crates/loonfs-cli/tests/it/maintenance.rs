//! Checkpoint, maintenance, store probe, grep, and change-feed commands.

use super::common::*;

#[test]
fn maintenance_gc_reclaims_a_deleted_namespace_instead_of_refusing() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    assert_success(&harness.run(&["namespace", "create", "demo"]));
    assert_success(&harness.run(&["use", "demo"]));

    let payload = harness.temp_dir.path().join("payload.txt");
    fs::write(&payload, b"body").expect("write payload");
    assert_success(&harness.run(&["put", payload.to_str().expect("utf-8 path"), "/doc.txt"]));
    // Materialize derived state so the tombstone has something reclaimable.
    assert_success(&harness.run(&["maintenance", "flush"]));
    assert_success(&harness.run(&["--json", "namespace", "delete", "demo", "--yes"]));

    // GC is the reclamation path for a tombstoned namespace: it must run
    // and report, not refuse. (Fresh objects sit inside the grace window,
    // so this pins reachability, not byte counts.)
    let gc = harness.run(&["--json", "maintenance", "gc"]);
    assert_success(&gc);
    assert_eq!(json_data(&gc)["kind"], "garbage_collected");

    // Everything that is not the GC-only step still reports the deletion.
    let step = harness.run(&["--json", "maintenance", "step"]);
    assert_failure(&step);
    assert_eq!(json_error(&step)["code"], "namespace_deleted");
    let recreate = harness.run(&["--json", "namespace", "create", "demo"]);
    assert_failure(&recreate);
    assert_eq!(json_error(&recreate)["code"], "namespace_deleted");
}

#[test]
fn embedded_grep_works_after_index_enable() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    assert_success(&harness.run(&["namespace", "create", "code"]));
    assert_success(&harness.run(&["use", "code"]));

    let payload = harness.temp_dir.path().join("main.rs");
    fs::write(&payload, b"fn main() {}\n// TODO: expand\n").expect("write payload");
    assert_success(&harness.run(&["put", payload.to_str().expect("utf-8 path"), "/src/main.rs"]));

    // Before the index exists, grep names the missing feature.
    let before = harness.run(&["--json", "grep", "TODO"]);
    assert_failure(&before);
    assert_eq!(json_error(&before)["code"], "not_supported");

    // Enable waits for the backfill in-process: the one-shot CLI is its own
    // grep maintenance, so the query works immediately — no server, no
    // driver, no state that never resolves.
    let enabled = harness.run(&["--json", "maintenance", "index", "enable"]);
    assert_success(&enabled);
    assert_eq!(json_data(&enabled)["status"], "active");
    assert_eq!(json_data(&enabled)["waited_for_seq"], 1);
    assert_eq!(json_data(&enabled)["budget_exhausted"], false);
    let found = harness.run(&["--json", "grep", "TODO"]);
    assert_success(&found);
    assert_eq!(
        json_data(&found)["matches"]
            .as_array()
            .expect("json array")
            .len(),
        1
    );

    // Later writes catch up when enable is re-run.
    let more = harness.temp_dir.path().join("lib.rs");
    fs::write(&more, b"// TODO: also here\n").expect("write payload");
    assert_success(&harness.run(&["put", more.to_str().expect("utf-8 path"), "/src/lib.rs"]));
    let recaught = harness.run(&["--json", "maintenance", "index", "enable"]);
    assert_success(&recaught);
    assert_eq!(json_data(&recaught)["status"], "active");
    let found = harness.run(&["--json", "grep", "TODO"]);
    assert_success(&found);
    assert_eq!(
        json_data(&found)["matches"]
            .as_array()
            .expect("json array")
            .len(),
        2
    );
}

#[test]
fn grep_limit_caps_the_whole_search_while_page_size_sizes_requests() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    assert_success(&harness.run(&["namespace", "create", "code"]));
    assert_success(&harness.run(&["use", "code"]));

    let payload = harness.temp_dir.path().join("notes.txt");
    fs::write(&payload, b"TODO one\nTODO two\nTODO three\nTODO four\n").expect("write payload");
    assert_success(&harness.run(&["put", payload.to_str().expect("utf-8 path"), "/notes.txt"]));
    assert_success(&harness.run(&["--json", "maintenance", "index", "enable"]));

    let all = harness.run(&["--json", "grep", "TODO"]);
    assert_success(&all);
    let all_data = json_data(&all);
    assert_eq!(all_data["matches"].as_array().expect("json array").len(), 4);
    assert_eq!(all_data["truncated"], false);

    let capped = harness.run(&["--json", "grep", "TODO", "--limit", "2"]);
    assert_success(&capped);
    let capped_data = json_data(&capped);
    assert_eq!(
        capped_data["matches"].as_array().expect("json array").len(),
        2
    );
    assert_eq!(capped_data["truncated"], true);

    // A cap the search never reaches is not a truncation.
    let roomy = harness.run(&["--json", "grep", "TODO", "--limit", "99"]);
    assert_success(&roomy);
    assert_eq!(
        json_data(&roomy)["matches"]
            .as_array()
            .expect("json array")
            .len(),
        4
    );
    assert_eq!(json_data(&roomy)["truncated"], false);

    // A total larger than the page size follows enough pages to satisfy it.
    let paged = harness.run(&["--json", "grep", "TODO", "--limit", "3", "--page-size", "1"]);
    assert_success(&paged);
    assert_eq!(
        json_data(&paged)["matches"]
            .as_array()
            .expect("json array")
            .len(),
        3
    );
    assert_eq!(json_data(&paged)["truncated"], true);

    // A bounded total beyond the deployment page cap is legal and chunked.
    let over_page_cap = harness.run(&[
        "--json",
        "grep",
        "TODO",
        "--limit",
        "1001",
        "--page-size",
        "1",
    ]);
    assert_success(&over_page_cap);
    assert_eq!(
        json_data(&over_page_cap)["matches"]
            .as_array()
            .expect("json array")
            .len(),
        4
    );
    assert_eq!(json_data(&over_page_cap)["truncated"], false);

    let removed = harness.run(&["grep", "TODO", "--max-matches", "2"]);
    assert_failure(&removed);
    assert!(stderr_string(&removed).contains("unexpected argument '--max-matches'"));

    // The human rendering says it stopped early.
    let human = harness.run(&["grep", "TODO", "--limit", "2"]);
    assert_success(&human);
    assert!(stdout_string(&human).contains("--limit"));
}

#[test]
fn changes_checkpoints_and_grep_jsonl_follow_across_pages() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    assert_success(&harness.run(&["namespace", "create", "demo"]));
    assert_success(&harness.run(&["use", "demo"]));

    let empty_changes = harness.run(&["changes", "--page-size", "1", "--jsonl"]);
    assert_success(&empty_changes);
    assert!(stdout_string(&empty_changes).is_empty());

    let first = harness.temp_dir.path().join("first.txt");
    let second = harness.temp_dir.path().join("second.txt");
    fs::write(&first, b"TODO one\nTODO two\n").expect("first payload");
    fs::write(&second, b"second\n").expect("second payload");
    assert_success(&harness.run(&["put", first.to_str().expect("utf-8 path"), "/first.txt"]));
    assert_success(&harness.run(&["put", second.to_str().expect("utf-8 path"), "/second.txt"]));

    let changes = harness.run(&["changes", "--page-size", "1", "--jsonl"]);
    assert_success(&changes);
    assert_eq!(stdout_string(&changes).lines().count(), 2);

    assert_success(&harness.run(&["maintenance", "checkpoint", "create", "--name", "first"]));
    let dated = harness.run(&[
        "maintenance",
        "checkpoint",
        "create",
        "--name",
        "second",
        "--ttl-ms",
        "600000",
    ]);
    assert_success(&dated);
    let created_line = stdout_string(&dated);
    let expiry = created_line
        .split("expires at ")
        .nth(1)
        .expect("a checkpoint with a ttl prints when it expires")
        .trim_end()
        .trim_end_matches(')')
        .to_owned();
    let listed_expiries = harness.run(&["maintenance", "checkpoint", "list"]);
    assert_success(&listed_expiries);
    assert!(
        stdout_string(&listed_expiries).contains(&expiry),
        "create and list must spell one expiry the same way: {created_line}{}",
        stdout_string(&listed_expiries)
    );
    let one_checkpoint = harness.run(&[
        "--json",
        "maintenance",
        "checkpoint",
        "list",
        "--page-size",
        "1",
    ]);
    assert_success(&one_checkpoint);
    assert_eq!(
        json_data(&one_checkpoint)["checkpoints"]
            .as_array()
            .expect("checkpoint array")
            .len(),
        1
    );
    assert!(json_data(&one_checkpoint)["next_cursor"].is_string());
    let checkpoints = harness.run(&[
        "maintenance",
        "checkpoint",
        "list",
        "--page-size",
        "1",
        "--jsonl",
    ]);
    assert_success(&checkpoints);
    assert_eq!(stdout_string(&checkpoints).lines().count(), 2);

    assert_success(&harness.run(&["maintenance", "index", "enable"]));
    let grep = harness.run(&["grep", "TODO", "--page-size", "1", "--jsonl"]);
    assert_success(&grep);
    assert_eq!(stdout_string(&grep).lines().count(), 2);
}

#[test]
fn index_enable_leaves_core_maintenance_decoupled() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    assert_success(&harness.run(&["namespace", "create", "demo"]));
    assert_success(&harness.run(&["use", "demo"]));

    let enabled = harness.run(&["--json", "maintenance", "index", "enable"]);
    assert_success(&enabled);
    assert!(json_data(&enabled).get("backfill_step").is_none());

    let retried = harness.run(&["--json", "maintenance", "index", "enable"]);
    assert_success(&retried);
    assert_eq!(json_data(&retried)["status"], "active");
    assert!(json_data(&retried).get("backfill_step").is_none());
}

#[test]
fn index_status_reports_each_lifecycle_status_in_its_own_terms() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    assert_success(&harness.run(&["namespace", "create", "demo"]));
    assert_success(&harness.run(&["use", "demo"]));

    let disabled = harness.run(&["--json", "maintenance", "index", "status"]);
    assert_success(&disabled);
    assert_eq!(json_data(&disabled)["status"], "disabled");
    assert!(json_data(&disabled).get("built_through_seq").is_none());

    // `--no-wait` returns with the root the enable published: a backfill,
    // naming the sequence it will walk to and nothing it has indexed.
    let enabled = harness.run(&["--json", "maintenance", "index", "enable", "--no-wait"]);
    assert_success(&enabled);
    let data = json_data(&enabled);
    assert_eq!(data["status"], "backfilling");
    assert_eq!(data["target_seq"], 0);
    assert!(data.get("built_through_seq").is_none());
    assert!(json_data(&enabled).get("waited_for_seq").is_none());
    assert_eq!(json_data(&enabled)["steps"], 0);

    let backfilling = harness.run(&["--json", "maintenance", "index", "status"]);
    assert_success(&backfilling);
    assert_eq!(json_data(&backfilling)["status"], "backfilling");
    assert_eq!(json_data(&backfilling)["reorganize_pending"], false);
    assert!(backfilling_text_names_no_watermark(&harness));

    // Waiting takes it active, and only then is there a watermark.
    assert_success(&harness.run(&["maintenance", "index", "enable"]));
    let active = harness.run(&["--json", "maintenance", "index", "status"]);
    assert_success(&active);
    assert_eq!(json_data(&active)["status"], "active");
    assert_eq!(json_data(&active)["built_through_seq"], 0);
    assert!(json_data(&active).get("target_seq").is_none());

    let disabled = harness.run(&["--json", "maintenance", "index", "disable"]);
    assert_success(&disabled);
    let data = json_data(&disabled);
    assert_eq!(data["status"], "disabled");
    assert!(data.get("built_through_seq").is_none());
    assert!(data.get("next_run_no").is_some());

    let disabled_again = harness.run(&["--json", "maintenance", "index", "disable"]);
    assert_success(&disabled_again);
    assert_eq!(json_data(&disabled_again)["status"], "disabled");
}

#[test]
fn index_enable_waits_to_its_target_and_the_runner_tracks_later_writes() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    assert_success(&harness.run(&["namespace", "create", "demo"]));
    assert_success(&harness.run(&["use", "demo"]));
    let payload = harness.temp_dir.path().join("one.txt");
    fs::write(&payload, b"needle one\n").expect("write payload");
    assert_success(&harness.run(&["put", payload.to_str().expect("utf-8 path"), "/one.txt"]));

    let enabled = harness.run(&["--json", "maintenance", "index", "enable"]);
    assert_success(&enabled);
    assert_eq!(json_data(&enabled)["waited_for_seq"], 1);

    // A later command's local runner advances the enabled index as it writes.
    let more = harness.temp_dir.path().join("two.txt");
    fs::write(&more, b"needle two\n").expect("write payload");
    assert_success(&harness.run(&["put", more.to_str().expect("utf-8 path"), "/two.txt"]));
    let status = harness.run(&["--json", "maintenance", "index", "status"]);
    assert_success(&status);
    assert_eq!(
        json_data(&status)["built_through_seq"],
        2,
        "the embedded runner maintains an enabled index after each write"
    );

    // An index already at the namespace head returns without stepping.
    assert_success(&harness.run(&["maintenance", "index", "enable"]));
    let caught_up = harness.run(&["--json", "maintenance", "index", "enable"]);
    assert_success(&caught_up);
    assert_eq!(json_data(&caught_up)["status"], "active");
    assert_eq!(json_data(&caught_up)["waited_for_seq"], 2);
    assert_eq!(
        json_data(&caught_up)["steps"],
        0,
        "an index already at the captured target takes no steps"
    );
}

#[test]
fn index_enable_budgets_exit_nonzero_and_report_progress() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    assert_success(&harness.run(&["namespace", "create", "demo"]));
    assert_success(&harness.run(&["use", "demo"]));
    let payload = harness.temp_dir.path().join("one.txt");
    fs::write(&payload, b"needle\n").expect("write payload");
    assert_success(&harness.run(&["put", payload.to_str().expect("utf-8 path"), "/one.txt"]));

    for budget in [vec!["--max-steps", "0"], vec!["--deadline-ms", "0"]] {
        let mut args = vec!["--json", "maintenance", "index", "enable"];
        args.extend(budget.iter().copied());
        let stopped = harness.run(&args);
        assert_failure(&stopped);
        let data = json_data(&stopped);
        assert_eq!(data["budget_exhausted"], true, "{budget:?}");
        assert_eq!(data["steps"], 0, "{budget:?}");
        assert_eq!(data["waited_for_seq"], 1, "{budget:?}");
        assert_eq!(
            data["status"], "backfilling",
            "the report must say where the index actually is: {budget:?}"
        );
    }

    // The index is untouched by the give-up, and a plain wait still lands.
    assert_success(&harness.run(&["maintenance", "index", "enable"]));
    let found = harness.run(&["--json", "grep", "needle"]);
    assert_success(&found);
}

#[test]
fn index_status_and_enable_answer_the_same_over_the_remote_transport() {
    let harness = Harness::new();
    // A config with no `[grep]` table composes no grep at all, so this
    // deployment says so explicitly.
    let remote_server = harness.start_external_server(harness.write_server_config_with(
        "remote",
        "index-remote",
        "\n[grep]\nmode = \"serve_and_maintain\"\n",
    ));
    assert_success(&harness.run(&[
        "--json",
        "profile",
        "create",
        "remote",
        "default",
        "--server-url",
        &remote_server.server_url,
        "--auth-token",
        "test-token",
    ]));
    assert_success(&harness.run(&["namespace", "create", "demo"]));
    assert_success(&harness.run(&["use", "demo"]));
    let payload = harness.temp_dir.path().join("one.txt");
    fs::write(&payload, b"remote needle\n").expect("write payload");
    assert_success(&harness.run(&["put", payload.to_str().expect("utf-8 path"), "/one.txt"]));

    let disabled = harness.run(&["--json", "maintenance", "index", "status"]);
    assert_success(&disabled);
    assert_eq!(json_data(&disabled)["status"], "disabled");

    let enabled = harness.run(&["--json", "maintenance", "index", "enable"]);
    assert_success(&enabled);
    assert_eq!(json_data(&enabled)["waited_for_seq"], 1);
    assert_eq!(json_data(&enabled)["budget_exhausted"], false);
    assert_eq!(json_data(&enabled)["status"], "active");

    let active = harness.run(&["--json", "maintenance", "index", "status"]);
    assert_success(&active);
    assert_eq!(json_data(&active)["built_through_seq"], 1);

    let caught_up = harness.run(&["--json", "maintenance", "index", "enable"]);
    assert_success(&caught_up);
    assert_eq!(json_data(&caught_up)["waited_for_seq"], 1);
    assert_eq!(
        json_data(&caught_up)["steps"],
        0,
        "an index already at the captured target takes no steps"
    );

    let found = harness.run(&["--json", "grep", "remote needle"]);
    assert_success(&found);
    assert_eq!(
        json_data(&found)["matches"]
            .as_array()
            .expect("json array")
            .len(),
        1
    );

    let collected = harness.run(&["--json", "maintenance", "index", "gc"]);
    assert_success(&collected);
    assert_eq!(json_data(&collected)["namespace_reaped"], false);
}

#[test]
fn index_gc_loops_its_cursor_and_accumulates() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    assert_success(&harness.run(&["namespace", "create", "demo"]));
    assert_success(&harness.run(&["use", "demo"]));
    let payload = harness.temp_dir.path().join("one.txt");
    fs::write(&payload, b"needle\n").expect("write payload");
    assert_success(&harness.run(&["put", payload.to_str().expect("utf-8 path"), "/one.txt"]));
    assert_success(&harness.run(&["maintenance", "index", "enable"]));

    // Nothing here is past its grace window, so a full loop retains what it
    // examines and, having walked to the end, carries no resume cursor.
    let collected = harness.run(&["--json", "maintenance", "index", "gc"]);
    assert_success(&collected);
    let data = json_data(&collected);
    assert_eq!(data["deleted_segments"], 0);
    assert_eq!(data["namespace_reaped"], false);
    assert!(data.get("next_cursor").is_none(), "{data}");

    // One bounded pass stops early and returns a resume cursor.
    let single = harness.run(&["--json", "maintenance", "index", "gc", "--max-objects", "1"]);
    assert_success(&single);
    assert!(
        json_data(&single)["next_cursor"].is_string(),
        "{}",
        json_data(&single)
    );
    let cursor = json_data(&single)["next_cursor"]
        .as_str()
        .expect("bounded index collection returns a cursor")
        .to_owned();
    let resumed = harness.run(&[
        "--json",
        "maintenance",
        "index",
        "gc",
        "--max-objects",
        "1",
        "--cursor",
        &cursor,
    ]);
    assert_success(&resumed);
    assert_ne!(
        json_data(&resumed)
            .get("next_cursor")
            .and_then(Value::as_str),
        Some(cursor.as_str())
    );
}

#[test]
fn maintenance_run_drains_an_assignment_and_leaves_the_work_done() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    for namespace in ["alpha", "beta"] {
        assert_success(&harness.run(&["namespace", "create", namespace]));
        let payload = harness.temp_dir.path().join(format!("{namespace}.txt"));
        fs::write(&payload, b"assigned needle\n").expect("write payload");
        assert_success(&harness.run(&[
            "put",
            "--namespace",
            namespace,
            payload.to_str().expect("utf-8 path"),
            "/note.txt",
        ]));
        // Enabled and deliberately left behind: catching it up is what the
        // assignment is for.
        assert_success(&harness.run(&[
            "maintenance",
            "index",
            "enable",
            "--namespace",
            namespace,
            "--no-wait",
        ]));
    }

    let drained = harness.run(&[
        "--json",
        "maintenance",
        "run",
        "--namespaces",
        "alpha",
        "--namespaces",
        "beta",
        "--drain",
    ]);
    assert_success(&drained);
    let data = json_data(&drained);
    assert_eq!(data["kind"], "maintenance_drained");
    assert!(data.get("drained").is_none());
    assert_eq!(data["budget_exhausted"], false);
    let keys = data["keys"].as_array().expect("json array");
    assert_eq!(keys.len(), 8, "four jobs over two namespaces: {data}");
    assert!(
        keys.iter().all(|key| key["settled"] == true),
        "an unbudgeted drain settles every key: {data}"
    );
    assert_eq!(
        data["jobs"],
        serde_json::json!(["metadata", "gc", "grep-index", "grep-gc"])
    );

    for namespace in ["alpha", "beta"] {
        let status = harness.run(&[
            "--json",
            "maintenance",
            "index",
            "status",
            "--namespace",
            namespace,
        ]);
        assert_success(&status);
        assert_eq!(
            json_data(&status)["built_through_seq"],
            1,
            "the assigned index must reach the head it was behind: {namespace}"
        );
    }
}

#[test]
fn maintenance_run_budgets_exit_nonzero_and_report_per_key_progress() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    assert_success(&harness.run(&["namespace", "create", "alpha"]));

    let unstarted = harness.run(&[
        "--json",
        "maintenance",
        "run",
        "--namespaces",
        "alpha",
        "--drain",
        "--max-steps",
        "0",
    ]);
    assert_failure(&unstarted);
    let data = json_data(&unstarted);
    assert_eq!(data["budget_exhausted"], true);
    assert_eq!(data["steps"], 0);
    for key in data["keys"].as_array().expect("json array") {
        assert_eq!(key["settled"], false, "{data}");
        assert_eq!(key["steps"], 0, "{data}");
        assert!(key.get("conclusion").is_none(), "{data}");
    }

    // One step is enough for the first key on a quiet namespace and reaches
    // no other, which is exactly what the report has to say.
    let partial = harness.run(&[
        "maintenance",
        "run",
        "--namespaces",
        "alpha",
        "--drain",
        "--max-steps",
        "1",
    ]);
    assert_failure(&partial);
    let rendered = stdout_string(&partial);
    assert!(rendered.contains("alpha/metadata"), "{rendered}");
    assert!(rendered.contains("alpha/gc"), "{rendered}");
    assert!(rendered.contains("not started"), "{rendered}");
    assert!(rendered.contains("gave up"), "{rendered}");
}

#[test]
fn maintenance_run_requires_an_assignment_and_names_the_jobs_it_hosts() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");

    let unassigned = harness.run(&["maintenance", "run"]);
    assert_failure(&unassigned);
    assert!(
        stderr_string(&unassigned).contains("--namespaces"),
        "{}",
        stderr_string(&unassigned)
    );

    let unknown_job = harness.run(&[
        "maintenance",
        "run",
        "--namespaces",
        "alpha",
        "--job",
        "bogus",
    ]);
    assert_failure(&unknown_job);
    let message = stderr_string(&unknown_job);
    for job in ["metadata", "core-gc", "grep-index", "grep-gc"] {
        assert!(
            message.contains(job),
            "the valid set must be listed: {message}"
        );
    }
}

#[test]
fn maintenance_run_takes_a_poll_interval_with_a_floor_that_a_drain_ignores() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");
    for namespace in ["alpha", "beta"] {
        assert_success(&harness.run(&["namespace", "create", namespace]));
    }

    let too_fast = harness.run(&[
        "maintenance",
        "run",
        "--namespaces",
        "alpha",
        "--poll-interval-ms",
        "99",
    ]);
    assert_failure(&too_fast);
    let message = stderr_string(&too_fast);
    assert!(
        message.contains("poll-interval-ms"),
        "the rejection must name the flag: {message}"
    );
    assert!(
        message.contains("100"),
        "the rejection must name the floor: {message}"
    );

    let plain = harness.run(&[
        "--json",
        "maintenance",
        "run",
        "--namespaces",
        "alpha",
        "--drain",
    ]);
    assert_success(&plain);
    let paced = harness.run(&[
        "--json",
        "maintenance",
        "run",
        "--namespaces",
        "beta",
        "--drain",
        "--poll-interval-ms",
        "100",
    ]);
    assert_success(&paced);
    let (plain, paced) = (json_data(&plain), json_data(&paced));
    for field in ["budget_exhausted", "jobs"] {
        assert_eq!(
            paced[field], plain[field],
            "a drain reports the same `{field}` with the cadence flag as without it"
        );
    }
    let keys = paced["keys"].as_array().expect("json array");
    assert_eq!(
        keys.len(),
        plain["keys"].as_array().expect("json array").len()
    );
    assert!(
        keys.iter().all(|key| key["settled"] == true),
        "the drain still settles every key: {paced}"
    );
}

#[test]
fn maintenance_store_probe_renders_the_profile_stores_report() {
    let harness = Harness::new();
    harness.add_embedded_profile("default");

    let probe = harness.run(&["maintenance", "store", "probe"]);
    assert_success(&probe);
    let text = stdout_string(&probe);

    let json = harness.run(&["--json", "maintenance", "store", "probe"]);
    assert_success(&json);
    let data = json_data(&json);
    assert_eq!(data["kind"], "store_probed");
    let checks = data["checks"].as_array().expect("checks array");
    assert!(!checks.is_empty(), "the probe must report its work");
    let names: std::collections::BTreeSet<&str> = checks
        .iter()
        .map(|check| check["name"].as_str().expect("check name"))
        .collect();
    assert_eq!(names.len(), checks.len(), "check names must be unique");
    for check in checks {
        let name = check["name"].as_str().expect("check name");
        assert_eq!(check["outcome"], "passed", "{check}");
        assert!(text.contains(name), "missing `{name}` in: {text}");
    }
    assert!(
        text.contains(&format!("{} checks passed", checks.len())),
        "{text}"
    );

    // The probe cleans up after itself, so the store carries no probe keys.
    let probe_runs = harness.store_root("default").join("probe-runs");
    assert!(
        !probe_runs.exists() || fs::read_dir(&probe_runs).is_ok_and(|mut dir| dir.next().is_none()),
        "the probe left objects behind at {}",
        probe_runs.display()
    );
}

#[test]
fn maintenance_run_refuses_a_remote_profile() {
    let harness = Harness::new();
    assert_success(&harness.run(&[
        "--json",
        "profile",
        "create",
        "remote",
        "default",
        "--server-url",
        "http://127.0.0.1:9",
        "--auth-token",
        "test-token",
    ]));

    let refused = harness.run(&[
        "--json",
        "maintenance",
        "run",
        "--namespaces",
        "demo",
        "--drain",
    ]);
    assert_failure(&refused);
    let error = json_error(&refused);
    assert_eq!(error["code"], "not_supported");
    assert!(error["message"]
        .as_str()
        .expect("error message")
        .contains("requires an embedded profile"));
}

#[test]
fn maintenance_and_changes_commands_report_the_same_shapes_in_both_modes() {
    let harness = Harness::new();
    harness.add_embedded_profile("embedded");
    let remote_server =
        harness.start_external_server(harness.write_server_config("remote", "maintenance-parity"));
    let add_remote = harness.run(&[
        "--json",
        "profile",
        "create",
        "remote",
        "remote",
        "--server-url",
        &remote_server.server_url,
        "--auth-token",
        "test-token",
    ]);
    assert_success(&add_remote);

    let first_payload = harness.temp_dir.path().join("first.txt");
    let second_payload = harness.temp_dir.path().join("second.txt");
    fs::write(&first_payload, b"first change\n").expect("first payload");
    fs::write(&second_payload, b"second change\n").expect("second payload");

    let mut shapes_by_mode = Vec::new();
    for profile in ["embedded", "remote"] {
        assert_success(&harness.run(&["namespace", "create", "--profile", profile, "demo"]));
        assert_success(&harness.run(&["use", "--profile", profile, "demo"]));
        assert_success(&harness.run(&[
            "put",
            "--profile",
            profile,
            first_payload.to_str().expect("utf-8 path"),
            "/first.txt",
        ]));
        assert_success(&harness.run(&[
            "put",
            "--profile",
            profile,
            second_payload.to_str().expect("utf-8 path"),
            "/second.txt",
        ]));

        let changes = harness.run(&["--json", "changes", "--profile", profile]);
        assert_success(&changes);
        let changes_data = json_data(&changes);
        assert_eq!(changes_data["kind"], "changes");
        assert_eq!(changes_data["namespace_id"], "demo");
        assert_eq!(changes_data["after_seq"], 0);
        assert_eq!(changes_data["through_seq"], 2);
        assert!(changes_data["next_after_seq"].is_null());
        let listed = changes_data["changes"].as_array().expect("json array");
        assert_eq!(listed.len(), 2);
        assert_eq!(listed[0]["committed_seq"], 1);
        assert_eq!(listed[1]["committed_seq"], 2);
        assert!(listed[0]["commit_id"]
            .as_str()
            .expect("json string")
            .starts_with("c_"));
        assert_eq!(
            listed[0]["committed_by"],
            serde_json::json!({"kind":"service","id":"loonfs-cli"})
        );
        assert!(!listed[1]["events"]
            .as_array()
            .expect("json array")
            .is_empty());

        let paged = harness.run(&["--json", "changes", "--profile", profile, "--limit", "1"]);
        assert_success(&paged);
        let paged_data = json_data(&paged);
        assert_eq!(
            paged_data["changes"].as_array().expect("json array").len(),
            1
        );
        assert_eq!(paged_data["changes"][0]["committed_seq"], 1);
        assert_eq!(paged_data["next_after_seq"], 1);

        let resumed = harness.run(&["--json", "changes", "--profile", profile, "--after", "1"]);
        assert_success(&resumed);
        let resumed_data = json_data(&resumed);
        assert_eq!(resumed_data["after_seq"], 1);
        assert_eq!(
            resumed_data["changes"]
                .as_array()
                .expect("json array")
                .len(),
            1
        );
        assert_eq!(resumed_data["changes"][0]["committed_seq"], 2);

        let checkpoint = harness.run(&[
            "--json",
            "maintenance",
            "checkpoint",
            "create",
            "--name",
            "nightly",
            "--profile",
            profile,
        ]);
        assert_success(&checkpoint);
        let checkpoint_data = json_data(&checkpoint);
        assert_eq!(checkpoint_data["kind"], "checkpoint_created");
        assert_eq!(checkpoint_data["namespace_id"], "demo");
        assert_eq!(
            checkpoint_data["owner"],
            serde_json::json!({"kind": "user", "name": "nightly"})
        );
        assert!(checkpoint_data["created_at_ms"].is_u64());
        assert_eq!(checkpoint_data["checkpoint_seq"], 2);
        let checkpoint_id = checkpoint_data["checkpoint_id"]
            .as_str()
            .expect("json string")
            .to_owned();
        assert!(checkpoint_id.starts_with("chk_"));

        // Names are labels rather than unique keys. Reusing one creates a
        // second checkpoint with a different id.
        let second_checkpoint = harness.run(&[
            "--json",
            "maintenance",
            "checkpoint",
            "create",
            "--name",
            "nightly",
            "--profile",
            profile,
        ]);
        assert_success(&second_checkpoint);
        let second_checkpoint_id = json_data(&second_checkpoint)["checkpoint_id"]
            .as_str()
            .expect("json string")
            .to_owned();
        assert_ne!(second_checkpoint_id, checkpoint_id);

        let listed = harness.run(&[
            "--json",
            "maintenance",
            "checkpoint",
            "list",
            "--profile",
            profile,
        ]);
        assert_success(&listed);
        let listed_data = json_data(&listed);
        assert_eq!(listed_data["kind"], "checkpoints_listed");
        assert_eq!(listed_data["namespace_id"], "demo");
        let listed_ids = listed_data["checkpoints"]
            .as_array()
            .expect("json array")
            .iter()
            .map(|checkpoint| {
                assert_eq!(checkpoint["owner"]["kind"], "user");
                assert_eq!(checkpoint["owner"]["name"], "nightly");
                assert_eq!(checkpoint["checkpoint_seq"], 2);
                checkpoint["checkpoint_id"]
                    .as_str()
                    .expect("json string")
                    .to_owned()
            })
            .collect::<Vec<_>>();
        let mut expected_ids = vec![checkpoint_id.clone(), second_checkpoint_id.clone()];
        expected_ids.sort();
        assert_eq!(listed_ids, expected_ids);
        assert!(listed_data.get("next_cursor").is_none());

        // A page limit returns one page and a cursor that resumes at the next id.
        let first_page = harness.run(&[
            "--json",
            "maintenance",
            "checkpoint",
            "list",
            "--profile",
            profile,
            "--limit",
            "1",
        ]);
        assert_success(&first_page);
        let first_page_data = json_data(&first_page);
        assert_eq!(
            first_page_data["checkpoints"]
                .as_array()
                .expect("json array")
                .len(),
            1
        );
        assert_eq!(
            first_page_data["checkpoints"][0]["checkpoint_id"],
            expected_ids[0].as_str()
        );
        let cursor = first_page_data["next_cursor"]
            .as_str()
            .expect("partial page cursor")
            .to_owned();

        let resumed_page = harness.run(&[
            "--json",
            "maintenance",
            "checkpoint",
            "list",
            "--profile",
            profile,
            "--cursor",
            &cursor,
        ]);
        assert_success(&resumed_page);
        let resumed_page_data = json_data(&resumed_page);
        assert_eq!(
            resumed_page_data["checkpoints"][0]["checkpoint_id"],
            expected_ids[1].as_str()
        );
        assert!(resumed_page_data.get("next_cursor").is_none());

        let first_page_human = harness.run(&[
            "maintenance",
            "checkpoint",
            "list",
            "--profile",
            profile,
            "--limit",
            "1",
        ]);
        assert_success(&first_page_human);
        assert!(stdout_string(&first_page_human).contains("next_cursor:"));

        // The human rendering is the same table in both modes, and it names
        // the id the release command takes.
        let listed_human =
            harness.run(&["maintenance", "checkpoint", "list", "--profile", profile]);
        assert_success(&listed_human);
        let listed_text = stdout_string(&listed_human);
        assert!(listed_text.contains("CREATED\tEXPIRES\tSEQ\tOWNER\tCHECKPOINT"));
        assert!(listed_text.contains(&checkpoint_id));
        assert!(listed_text.contains("nightly"));

        // Releasing one leaves the other listed: a release is per record,
        // never per label.
        assert_success(&harness.run(&[
            "--json",
            "maintenance",
            "checkpoint",
            "release",
            &second_checkpoint_id,
            "--profile",
            profile,
        ]));
        let after_release = harness.run(&[
            "--json",
            "maintenance",
            "checkpoint",
            "list",
            "--profile",
            profile,
        ]);
        assert_success(&after_release);
        let remaining = json_data(&after_release);
        let remaining = remaining["checkpoints"].as_array().expect("json array");
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0]["checkpoint_id"], checkpoint_id.as_str());

        // `maintenance flush` runs metadata maintenance with a flush threshold of one.
        // segment, so it reports both halves and nothing else.
        let flush = harness.run(&["--json", "maintenance", "flush", "--profile", profile]);
        assert_success(&flush);
        let flush_data = json_data(&flush);
        assert_eq!(flush_data["kind"], "metadata");
        assert_eq!(flush_data["reorganize"]["outcome"], "not_needed");
        assert!(flush_data["wal_flush"].is_object());

        let release = harness.run(&[
            "--json",
            "maintenance",
            "checkpoint",
            "release",
            &checkpoint_id,
            "--profile",
            profile,
        ]);
        assert_success(&release);
        let release_data = json_data(&release);
        assert_eq!(release_data["kind"], "checkpoint_released");
        assert_eq!(release_data["checkpoint_id"], checkpoint_id.as_str());
        assert!(release_data.get("was_active").is_none());

        let release_again = harness.run(&[
            "--json",
            "maintenance",
            "checkpoint",
            "release",
            &checkpoint_id,
            "--profile",
            profile,
        ]);
        assert_success(&release_again);
        assert_eq!(json_data(&release_again), release_data);

        let retention = harness.run(&[
            "--json",
            "maintenance",
            "retention",
            "advance",
            "--profile",
            profile,
        ]);
        assert_success(&retention);
        let retention_data = json_data(&retention);
        assert_eq!(retention_data["kind"], "retention");
        assert_eq!(retention_data["retention_floor_seq"], 2);

        // The checkpoint above already covers the head, so a step reports
        // not-needed identically in both modes.
        let step = harness.run(&["--json", "maintenance", "step", "--profile", profile]);
        assert_success(&step);
        let step_data = json_data(&step);
        assert_eq!(step_data["kind"], "metadata");
        assert_eq!(step_data["wal_flush"]["outcome"], "not_needed");
        assert_eq!(step_data["reorganize"]["outcome"], "not_needed");

        let compact = harness.run(&["--json", "maintenance", "compact", "--profile", profile]);
        assert_success(&compact);
        let compact_data = json_data(&compact);
        assert_eq!(compact_data["kind"], "metadata_compaction");
        assert_eq!(compact_data["outcome"]["outcome"], "not_needed");

        // A fresh namespace has nothing eligible to sweep.
        let gc = harness.run(&["--json", "maintenance", "gc", "--profile", profile]);
        assert_success(&gc);
        let gc_data = json_data(&gc);
        assert_eq!(gc_data["kind"], "garbage_collected");
        assert_eq!(gc_data["namespace_id"], "demo");
        assert_eq!(gc_data["deleted"]["wal_segments"], 0);
        assert_eq!(gc_data["deleted"]["manifests"], 0);
        assert_eq!(gc_data["retention_degraded"], false);
        assert!(gc_data.get("next_cursor").is_none());
        // Every retention reason is reported whether or not it happened, so
        // a consumer reads a field rather than probing for one, and the
        // breakdown accounts for exactly the total beside it.
        let retained = gc_data["retained"].as_object().expect("json object");
        let reason_total: u64 = retained
            .values()
            .map(|count| count.as_u64().expect("json number"))
            .sum();
        assert_eq!(
            reason_total,
            gc_data["retained_candidates"]
                .as_u64()
                .expect("json number")
        );
        assert!(retained.contains_key("checkpoint_not_releasable"));

        // A run that took one pass says nothing on the way: its summary on
        // standard output is the whole report.
        let quiet_gc = harness.run(&["maintenance", "gc", "--profile", profile]);
        assert_success(&quiet_gc);
        assert!(!stderr_string(&quiet_gc).contains("pass 1:"));

        // The budget covers marking too, so one object buys the head and
        // the root beside it and nothing else. That pass says it ran out
        // rather than reporting a clean sweep, and returns no position
        // it never reached.
        let starved_gc = harness.run(&[
            "--json",
            "maintenance",
            "gc",
            "--max-objects",
            "1",
            "--profile",
            profile,
        ]);
        assert_success(&starved_gc);
        let starved_data = json_data(&starved_gc);
        assert_eq!(starved_data["budget_exhausted"], true);
        assert!(starved_data.get("next_cursor").is_none());

        // Supplying a budget with room for the roots, the seven-unit
        // compaction lease stage, and some candidates requests exactly one
        // pass and exposes the opaque cursor instead of the CLI's default
        // completion loop.
        let bounded_gc = harness.run(&[
            "--json",
            "maintenance",
            "gc",
            "--max-objects",
            "15",
            "--profile",
            profile,
        ]);
        assert_success(&bounded_gc);
        let bounded_data = json_data(&bounded_gc);
        let cursor = bounded_data["next_cursor"]
            .as_str()
            .filter(|cursor| !cursor.is_empty())
            .expect("bounded pass returns a cursor")
            .to_owned();

        // Resumption skips the candidates the first pass enumerated. A
        // restart over this unchanged keyspace would return the same token.
        let resumed_gc = harness.run(&[
            "--json",
            "maintenance",
            "gc",
            "--max-objects",
            "15",
            "--cursor",
            &cursor,
            "--profile",
            profile,
        ]);
        assert_success(&resumed_gc);
        assert_ne!(
            json_data(&resumed_gc)
                .get("next_cursor")
                .and_then(Value::as_str),
            Some(cursor.as_str())
        );

        // Maintenance failures surface the registry code in both modes.
        let missing = harness.run(&[
            "--json",
            "maintenance",
            "checkpoint",
            "create",
            "--name",
            "nightly",
            "--profile",
            profile,
            "--namespace",
            "missing",
        ]);
        assert_failure(&missing);
        assert_eq!(json_error(&missing)["code"], "namespace_not_found");
        // Both modes name the namespace: the CLI mapper mirrors the server
        // handler's scoping.
        assert_eq!(
            json_error(&missing)["message"],
            "namespace `missing` does not exist"
        );

        // The store probe is store-scoped, so it is the one maintenance command
        // that names no namespace in either mode; both modes still answer
        // with the same report shape.
        let probe = harness.run(&[
            "--json",
            "maintenance",
            "store",
            "probe",
            "--profile",
            profile,
        ]);
        assert_success(&probe);
        let probe_data = json_data(&probe);
        assert_eq!(probe_data["kind"], "store_probed");
        assert_eq!(
            probe_data["checks"].as_array().expect("checks array").len(),
            14
        );

        shapes_by_mode.push((
            sorted_object_keys(&changes_data),
            sorted_object_keys(&checkpoint_data),
            sorted_object_keys(&retention_data),
            sorted_object_keys(&step_data),
            sorted_object_keys(&gc_data),
            sorted_object_keys(&probe_data),
        ));
    }

    assert_eq!(
        shapes_by_mode[0], shapes_by_mode[1],
        "embedded and remote --json payloads diverged in shape"
    );
}
