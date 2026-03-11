# ADR 0008: conflict filenames remain deterministic and clean

Status: accepted

Name conflicts and edit conflicts use deterministic conflict-copy naming without device or user labels.

Consequences:
- filenames stay predictable
- richer conflict details belong in metadata, not in the filename itself
