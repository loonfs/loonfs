#!/usr/bin/env python3
"""Prints one top-level field of the JSON object read from stdin."""

import json
import sys

print(json.load(sys.stdin)[sys.argv[1]])
