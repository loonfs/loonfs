# loon-server

Thin server integration crate.

The current real surface here is `mutation`, which composes `loon-core` and related crates into the
authoritative create/replace execution path. The binary, app, and HTTP shells are intentionally
quarantined during the semantic-core reset so the repo does not imply a fuller server surface than
actually exists.
