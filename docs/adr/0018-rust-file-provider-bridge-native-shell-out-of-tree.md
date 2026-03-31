# ADR 0018: keep the File Provider bridge in Rust and the native shell out of tree for the first spike

Status: accepted

The first macOS File Provider spike will land as a Rust bridge crate in `loon-macos`. The native
macOS app and File Provider extension shell will remain out of tree for this spike.

Consequences:
- the in-repo bridge must project items and materialize content from the existing client truth
  model instead of relying on native-only state
- the main repo does not gain Xcode projects, Swift sources, plists, or entitlements yet
- the first runnable Finder shell must treat bridge item ids as opaque and call into the Rust
  bridge for enumeration and hydration
- if the native shell is later brought in tree, it must keep this boundary rather than
  re-implementing provider logic
