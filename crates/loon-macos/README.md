# loon-macos

**Status: Experimental spike**

Read-only macOS File Provider bridge.

This crate projects Finder-style provider items from the existing client SQLite state and reuses
the same client truth model rather than inventing a second sync model.

`#![deny(unsafe_op_in_unsafe_fn)]` — this is the only crate in the workspace with unsafe code,
confined to the C FFI boundary.

## Current scope

- One account/root projection with namespaces as top-level directories
- DB-backed enumeration only
- Targeted hydration into the existing client mirror root
- Static-library C ABI with UTF-8 JSON payloads for a native macOS sample
- Checked-in developer sample app and extension shell under `native/macos/LoonFileProviderSample/`

## Current non-goals

- No create/modify/delete/rename support
- No implicit import, sync, watcher, or daemon loop
