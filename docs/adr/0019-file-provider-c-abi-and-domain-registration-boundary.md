# ADR 0019: use a C ABI + JSON boundary for the first native File Provider sample

Status: accepted

## Decision

The first native macOS File Provider sample will call `loon-macos` through a small static-library
C ABI with UTF-8 JSON payloads. The containing app owns File Provider domain registration and
reset. The File Provider extension owns enumeration, lookup, and targeted hydration.

Rules:

- the exported `loon-macos` C ABI is limited to:
  - `open`
  - `close`
  - `list_root`
  - `lookup_item`
  - `list_children`
  - `materialize_item`
  - `string_free`
- `open` accepts JSON containing:
  - `ops_config_path`
  - `exposed_namespaces`
- all exported functions return JSON envelopes containing either:
  - a success payload
  - a typed error code and message
- the native sample treats provider item ids as opaque encoded strings rather than path-derived ids
  or direct Rust enum layouts
- the containing app reads the sample config from an App Group container and owns domain
  registration with `NSFileProviderManager`
- the extension uses `NSFileProviderReplicatedExtension` and calls the Rust bridge for
  enumeration, lookup, and targeted hydration
- the first native sample remains read-only and does not auto-import or auto-sync

## Consequences

- Swift can call the Rust bridge without re-implementing projection or hydration logic
- the native sample has a stable interop contract that does not expose Rust enum layouts
- domain lifecycle is explicit and centralized in the containing app
- the repo still avoids committing Xcode projects, plist files, entitlements, or Swift sources in
  this slice
- if the native shell later comes in tree, it must keep using this bridge boundary rather than
  inventing a second provider model
