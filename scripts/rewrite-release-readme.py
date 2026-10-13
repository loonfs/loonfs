#!/usr/bin/env python3
"""Writes the release README with picture blocks removed and relative links pointed at the version's GitHub tree."""

import pathlib
import re
import sys
from urllib.parse import urljoin, urlsplit

source, destination, version = sys.argv[1:]
base = f"https://github.com/loonfs/loonfs/blob/v{version}/"
readme = pathlib.Path(source).read_text()
readme = re.sub(r"<picture\b[^>]*>.*?</picture>", "", readme, flags=re.DOTALL | re.IGNORECASE)


def rewrite_link(match):
    prefix, target = match.groups()
    if urlsplit(target).scheme or target.startswith("//"):
        return match.group(0)
    if target.startswith("#"):
        target = "README.md" + target
    return prefix + urljoin(base, target)


for pattern in [
    r"(!?\[[^\]\n]*\]\(\s*<?)([^>\s)]+)",
    r"(?m)^(\s{0,3}\[[^\]\n]+\]:\s*<?)([^>\s]+)",
    r'''(\b(?:href|src)\s*=\s*["'])([^"']+)''',
]:
    readme = re.sub(pattern, rewrite_link, readme, flags=re.IGNORECASE)
pathlib.Path(destination).write_text(readme)
