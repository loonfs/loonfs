#!/usr/bin/env python3
"""Checks that a release README keeps its versioned links and has no relative links, local files, or picture blocks."""

import pathlib
import re
import sys
from html.parser import HTMLParser
from urllib.parse import urlsplit

readme = pathlib.Path(sys.argv[1]).read_text()
for path in ["docs/specs/README.md", "crates/loonfs-server/docs/self-hosting.md", "LICENSE"]:
    expected = f"https://github.com/loonfs/loonfs/blob/v{sys.argv[2]}/{path}"
    if expected not in readme:
        raise SystemExit(f"release README is missing its versioned link: {expected}")


def check_target(target):
    if not (urlsplit(target).scheme or target.startswith("//")):
        raise SystemExit(f"release README contains a relative link or local image: {target}")
    if urlsplit(target).scheme == "file":
        raise SystemExit(f"release README references a local file: {target}")


class CheckHTML(HTMLParser):
    def handle_starttag(self, tag, attributes):
        if tag == "picture":
            raise SystemExit("release README still contains a picture block")
        for name, value in attributes:
            if value is not None and name in {"href", "src"}:
                check_target(value)
            elif value is not None and name == "srcset":
                for candidate in value.split(","):
                    check_target(candidate.strip().split()[0])


CheckHTML().feed(readme)
for pattern in [
    r"!?\[[^\]\n]*\]\(\s*<?([^>\s)]+)",
    r"(?m)^\s{0,3}\[[^\]\n]+\]:\s*<?([^>\s]+)",
]:
    for target in re.findall(pattern, readme):
        check_target(target)
