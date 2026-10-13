#!/usr/bin/env python3
"""Writes the path of each binary the write stress run needs, read from Cargo's JSON build messages."""

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
