#!/usr/bin/env python3
"""Generated SDKs follow the LoonFS response and operation retry rules."""

import json
import re
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
GENERATED_ROOT = REPO_ROOT / "sdk" / "generated"
SUPPORTED_GROUPS = {"go", "python", "typescript", "typescript-client"}


def count_non_idempotent_operations(spec_path: Path) -> int:
    spec = json.loads(spec_path.read_text())
    operations = [
        operation
        for path_item in spec["paths"].values()
        for method, operation in path_item.items()
        if method in {"get", "post", "put", "patch", "delete", "options", "head"}
    ]
    non_idempotent = [
        operation
        for operation in operations
        if operation.get("x-loonfs-retry") == "not_idempotent"
    ]
    for operation in non_idempotent:
        if operation.get("x-fern-retries") != {"disabled": True}:
            operation_id = operation.get("operationId", "<unknown>")
            raise SystemExit(f"{operation_id} does not disable generated SDK retries")
    return len(non_idempotent)


def count_token(root: Path, suffix: str, token: str) -> int:
    return sum(
        path.read_text().count(token)
        for path in root.rglob(f"*{suffix}")
        if path.is_file()
    )


def verify_retry_predicate(
    path: Path,
    predicate: str,
    status_checks: tuple[str, ...],
    retry_budget: str,
    expected_budgets: int = 1,
) -> None:
    source = path.read_text()
    actual = source.count(predicate)
    if actual != 1:
        raise SystemExit(f"{path} has {actual} Retry-After predicates; expected 1")
    for status_check in status_checks:
        if status_check in source:
            raise SystemExit(f"{path} still retries on status alone: {status_check}")
    if len(re.findall(retry_budget, source, re.MULTILINE)) != expected_budgets:
        raise SystemExit(f"{path} does not default to exactly three HTTP attempts")


def verify_group(group: str) -> None:
    if group not in SUPPORTED_GROUPS:
        choices = "|".join(sorted(SUPPORTED_GROUPS))
        raise SystemExit(f"usage: {Path(sys.argv[0]).name} <{choices}>")

    proxy = group == "typescript-client"
    spec_name = "openapi-proxy.json" if proxy else "openapi.json"
    expected = count_non_idempotent_operations(REPO_ROOT / "docs" / "specs" / spec_name)
    generated = GENERATED_ROOT / group

    if group in {"typescript", "typescript-client"}:
        verify_retry_predicate(
            generated / "core" / "fetcher" / "requestWithRetries.ts",
            'response.headers.has("retry-after")',
            ("statusCode >= 500", "[408, 429]"),
            r"^\s*const DEFAULT_MAX_RETRIES\s*=\s*2;\s*$",
        )
        # Only generated endpoint clients correspond to operations in the spec.
        # Handwritten transfer helpers independently disable payload retries.
        actual = count_token(generated / "api" / "resources", ".ts", "maxRetries: 0,")
        if actual != expected:
            raise SystemExit(
                f"{group} disables retries at {actual} call sites; expected {expected}"
            )
    elif group == "python":
        verify_retry_predicate(
            generated / "core" / "http_client.py",
            'return "retry-after" in response.headers',
            ("status_code >= 500",),
            r"^\s*base_max_retries:\s*int\s*=\s*2,\s*$",
            expected_budgets=2,
        )
        actual = count_token(generated, ".py", "_request_options_with_retries_disabled:")
        expected_call_sites = expected * 2  # Synchronous and asynchronous clients.
        if actual != expected_call_sites:
            raise SystemExit(
                f"python disables retries at {actual} call sites; expected {expected_call_sites}"
            )
    else:
        retrier_path = generated / "internal" / "retrier.go"
        verify_retry_predicate(
            retrier_path,
            'response.Header.Get("Retry-After") != ""',
            ("http.StatusInternalServerError",),
            r"^\s*defaultRetryAttempts\s*=\s*3\s*$",
        )
        actual = sum(
            len(re.findall(r"\bDisableRetries:[ \t]+true,", path.read_text()))
            for path in generated.rglob("*.go")
            if path.is_file()
        )
        if actual != expected:
            raise SystemExit(f"go disables retries at {actual} call sites; expected {expected}")

    print(f"Verified generated retry safety for {group}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        choices = "|".join(sorted(SUPPORTED_GROUPS))
        raise SystemExit(f"usage: {Path(sys.argv[0]).name} <{choices}>")
    verify_group(sys.argv[1])
