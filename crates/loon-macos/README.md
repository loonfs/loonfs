# loon-macos

Read-only macOS File Provider bridge spike.

This crate projects Finder-style provider items from the existing client SQLite state and reuses
the same client truth model rather than inventing a second sync model.

Current scope:

- one account/root projection with namespaces as top-level directories
- DB-backed enumeration only
- targeted hydration into the existing client mirror root
- out-of-tree native app and extension shell

Current non-goals:

- no in-repo Xcode or Swift packaging
- no create/modify/delete/rename support
- no implicit import, sync, watcher, or daemon loop
