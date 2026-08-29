#!/usr/bin/env python3
"""Verify generated SDKs never retry non-idempotent operations by default."""

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


def verify_group(group: str) -> None:
    if group not in SUPPORTED_GROUPS:
        choices = "|".join(sorted(SUPPORTED_GROUPS))
        raise SystemExit(f"usage: {Path(sys.argv[0]).name} <{choices}>")

    proxy = group == "typescript-client"
    spec_name = "openapi-proxy.json" if proxy else "openapi.json"
    expected = count_non_idempotent_operations(REPO_ROOT / "docs" / "specs" / spec_name)
    generated = GENERATED_ROOT / group

    if group in {"typescript", "typescript-client"}:
        actual = count_token(generated, ".ts", "maxRetries: 0,")
        if actual != expected:
            raise SystemExit(
                f"{group} disables retries at {actual} call sites; expected {expected}"
            )
    elif group == "python":
        actual = count_token(generated, ".py", "_request_options_with_retries_disabled:")
        expected_call_sites = expected * 2  # Synchronous and asynchronous clients.
        if actual != expected_call_sites:
            raise SystemExit(
                f"python disables retries at {actual} call sites; expected {expected_call_sites}"
            )
    else:
        retrier = (generated / "internal" / "retrier.go").read_text()
        if not re.search(r"defaultRetryAttempts\s*=\s*1\b", retrier):
            raise SystemExit("go does not default to exactly one HTTP attempt")

    print(f"Verified generated retry safety for {group}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        choices = "|".join(sorted(SUPPORTED_GROUPS))
        raise SystemExit(f"usage: {Path(sys.argv[0]).name} <{choices}>")
    verify_group(sys.argv[1])
