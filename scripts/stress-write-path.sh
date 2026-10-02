#!/bin/sh
# Usage: scripts/stress-write-path.sh [rounds [copies]] (defaults: 4, 6).
# Requires Cargo and Python 3, as provided by the Linux CI image.
# Builds once, then starts separate processes for every copy of each exact test.
# All copies start before any is awaited. Each test finishes before the next starts.
# No failed copy is retried. A failure reports its test, round, copy, error codes,
# and last 60 output lines. Full logs remain in the printed temporary directory.
# A missing, renamed, or ignored test fails the gate. Success removes the logs.
# Each selected test holds competing write-path work open: a fold, a compaction,
# a permit wait, or a sweep visit. Tests that only repeat work are left out.

set -eu

ROUNDS=${1-4}
COPIES=${2-6}
if [ "$#" -gt 2 ]; then
    echo "usage: scripts/stress-write-path.sh [rounds [copies]]" >&2
    exit 2
fi
for count in "$ROUNDS" "$COPIES"; do
    case "$count" in
        ''|0*|*[!0-9]*)
            echo "rounds and copies must be positive decimal integers without leading zeros" >&2
            exit 2
            ;;
    esac
done

REPO_ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$REPO_ROOT"
RUN_DIR=$(mktemp -d "${TMPDIR:-/tmp}/loonfs-write-stress.XXXXXX")
PIDS=
cleanup() {
    status=$?
    trap - 0
    for pid in $PIDS; do
        kill "$pid" 2>/dev/null || :
    done
    for pid in $PIDS; do
        wait "$pid" 2>/dev/null || :
    done
    if [ "$status" -eq 0 ]; then
        rm -r "$RUN_DIR"
    else
        printf 'write stress logs: %s\n' "$RUN_DIR" >&2
    fi
    exit "$status"
}
trap cleanup 0
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

cargo build --locked --tests --bins \
    -p loonfs-cli -p loonfs -p loonfs-core -p loonfs-server \
    --features loonfs-core/test-support \
    --message-format=json-render-diagnostics >"$RUN_DIR/artifacts.jsonl"

python3 - "$RUN_DIR" <<'PY'
import json
from pathlib import Path
import sys

directory = Path(sys.argv[1])
targets = {
    ("crates/loonfs-cli/tests/it/main.rs", True): "cli",
    ("crates/loonfs/src/lib.rs", True): "runtime",
    ("crates/loonfs-core/src/lib.rs", True): "core",
    ("crates/loonfs-server/src/lib.rs", True): "server",
    ("crates/loonfs-cli/src/main.rs", False): "loonfs",
    ("crates/loonfs-server/src/main.rs", False): "loonfs-server",
}
found = {}
for line in (directory / "artifacts.jsonl").read_text().splitlines():
    artifact = json.loads(line)
    if artifact["reason"] != "compiler-artifact" or not artifact.get("executable"):
        continue
    source = str(Path(artifact["target"]["src_path"]).relative_to(Path.cwd()))
    name = targets.get((source, artifact["profile"]["test"]))
    if name:
        found[name] = artifact["executable"]
for name in targets.values():
    if name not in found:
        sys.exit(f"missing build artifact: {name}")
    (directory / name).write_text(found[name] + "\n")
PY

cat >"$RUN_DIR/tests" <<'TESTS'
cli|output::ls_default_all_jsonl_and_cursor_obey_page_boundaries
runtime|publisher::tests::wal_folds_share_the_writer_concurrency_bound
runtime|publisher::tests::a_late_fold_does_not_republish_an_already_folded_tail
runtime|publisher::tests::successful_delete_waits_for_fold_before_evicting_the_namespace_publisher
runtime|publisher::tests::a_delete_waiting_for_a_fold_does_not_hold_a_publication_slot
runtime|publisher::tests::inline_writer::fold_completion_reports_only_inline_bytes_published_since_it_began
runtime|publisher::tests::inline_writer::a_delayed_fold_callback_preserves_a_freshly_observed_tail
runtime|publisher::tests::inline_writer::a_delayed_fold_callback_preserves_an_uncached_tail
runtime|publisher::tests::session_compaction::a_session_streaming_compaction_survives_its_own_next_fold
runtime|publisher::tests::session_compaction::closing_a_session_cancels_its_compaction_and_returns
runtime|publisher::tests::session_compaction::shutdown_drains_session_compactions
runtime|publisher::tests::session_compaction::a_runtime_runs_no_more_merges_at_once_than_its_compaction_limit
runtime|publisher::tests::session_compaction::closing_a_session_does_not_wait_for_another_sessions_merge
core|manifest::tests::fold_races::a_fold_that_loses_to_a_compaction_reads_no_wal_object
core|manifest::tests::fold_races::a_fold_that_loses_to_a_fold_still_takes_the_cold_path
core|manifest::tests::fold_races::a_fold_survives_repeated_compaction_publications
core|manifest::tests::fold_races::a_retried_fold_leaves_the_session_where_an_unraced_fold_does
core|manifest::tests::fold_races::a_checkpoint_that_loses_its_fold_to_a_compaction_reads_no_wal_object
runtime|execution_budget::tests::runtimes_sharing_a_budget_never_exceed_its_limits
runtime|execution_budget::tests::an_idle_runtime_strands_no_capacity
runtime|execution_budget::tests::shutting_down_one_runtime_leaves_the_budget_usable
runtime|execution_budget::tests::admitted_bytes_are_shared_and_refunded
runtime|execution_budget::tests::one_namespace_id_in_two_stores_is_charged_separately
runtime|publisher::tests::pin_folds::creations_wait_for_a_fold_permit_whether_or_not_the_tail_is_folded
runtime|publisher::tests::pin_folds::creations_started_together_fold_one_at_a_time_at_a_limit_of_one
runtime|publisher::tests::pin_folds::dropped_creations_return_their_fold_permits
server|sweep::tests::a_parked_visit_does_not_hold_up_later_pages
server|sweep::tests::a_listing_failure_on_a_later_page_lets_started_visits_finish_and_fails_the_pass
TESTS

test_count=0
while IFS='|' read -r target test; do
    binary=$(cat "$RUN_DIR/$target")
    if ! "$binary" --list --exact "$test" >"$RUN_DIR/list" 2>&1 ||
        ! grep -Fx "$test: test" "$RUN_DIR/list" >/dev/null; then
        printf 'missing stress test: %s (%s)\n' "$test" "$target" >&2
        cat "$RUN_DIR/list" >&2
        exit 1
    fi
    test_count=$((test_count + 1))
done <"$RUN_DIR/tests"

report_failure() {
    printf 'FAIL: %s round %s/%s copy %s/%s (exit %s)\n' \
        "$test" "$round" "$ROUNDS" "$copy" "$COPIES" "$copy_status" >&2
    python3 - "$log" <<'PY' >&2
from pathlib import Path
import re
import sys

output = Path(sys.argv[1]).read_text(errors="replace")
registry = Path("crates/loonfs-types/src/error.rs").read_text()
codes = {
    code
    for variant, code in re.findall(r'^\s+(\w+) => "([a-z_]+)"', registry, re.M)
    if re.search(r"\b(?:" + variant + "|" + code + r")\b", output)
}
if codes:
    print("error codes in output: " + ", ".join(sorted(codes)))
PY
    tail -n 60 "$log" >&2
}

round=1
while [ "$round" -le "$ROUNDS" ]; do
    test_index=0
    while IFS='|' read -r target test; do
        test_index=$((test_index + 1))
        binary=$(cat "$RUN_DIR/$target")
        printf 'write stress: round %s/%s, %s copies of %s\n' \
            "$round" "$ROUNDS" "$COPIES" "$test"
        PIDS=
        copy=1
        while [ "$copy" -le "$COPIES" ]; do
            log="$RUN_DIR/round-$round-test-$test_index-copy-$copy.log"
            "$binary" --exact "$test" --test-threads=1 --nocapture >"$log" 2>&1 &
            PIDS="$PIDS $!"
            copy=$((copy + 1))
        done
        failed=0
        copy=1
        for pid in $PIDS; do
            log="$RUN_DIR/round-$round-test-$test_index-copy-$copy.log"
            copy_status=0
            wait "$pid" || copy_status=$?
            if [ "$copy_status" -ne 0 ] ||
                ! grep -F 'test result: ok. 1 passed; 0 failed; 0 ignored;' "$log" >/dev/null; then
                report_failure
                failed=1
            fi
            copy=$((copy + 1))
        done
        PIDS=
        [ "$failed" -eq 0 ] || exit 1
    done <"$RUN_DIR/tests"
    round=$((round + 1))
done
printf 'write stress passed: %s tests x %s rounds x %s copies = %s successful runs\n' \
    "$test_count" "$ROUNDS" "$COPIES" "$((test_count * ROUNDS * COPIES))"
