#!/usr/bin/env python3
"""Prints the LoonFS error codes that appear in a test log."""

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
