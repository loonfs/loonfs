# LoonFileProviderSample

This directory contains the first in-repo macOS File Provider developer sample for loondb.

What is here:

- a Swift package with repo-safe shared logic and unit tests
- a SwiftUI containing app for config inspection and domain registration
- a read-only `NSFileProviderReplicatedExtension`
- a build script that links the extension against `loon-macos`

What is not here:

- production packaging
- auto-import or auto-sync
- write support
- incremental File Provider change tokens

## Requirements

- macOS 15 or newer
- full Xcode for Finder-visible app/extension builds
- a local signing team configured in Xcode

Repo-safe validation still works through:

```bash
swift test --package-path native/macos/LoonFileProviderSample
cargo test -p loon-macos
```

## Local setup

1. Open `native/macos/LoonFileProviderSample/LoonFileProviderSample.xcodeproj` in Xcode.
2. Set your signing team for both targets.
3. Keep the committed App Group identifier in sync with your local entitlements if you change it.
4. Build the extension target once so the Rust static library is produced by the checked-in build script.

The containing app creates a default config file if it is missing. The file lives in the sample
App Group container and is named `loon-file-provider-sample.json`.

Expected fields:

- `ops_config_path`
- `exposed_namespaces`
- `domain_identifier`
- `domain_display_name`
- `app_group_identifier`

For the smoothest first run, point `ops_config_path` at an ops config whose client state DB,
mirror root, and object-store paths are also inside a location your local App Group setup can
access. The sample does not manage security-scoped bookmarks in this slice.

## Native workflow

1. Launch the app.
2. Reveal the config file in Finder and edit it with a valid ops config path and namespace list.
3. Click `Register Domain`.
4. Finder should show one `Loon` File Provider domain with namespaces as top-level folders.
5. Remove the domain with `Remove Domain` when you need a local reset.

Freshness still comes from the existing CLI flow:

```bash
loon ops bootstrap-namespace --config <path> --namespace <id>
loon ops import-remote-observations --config <path> --namespace <id>
loon ops sync-once --config <path> --namespace <id>
```
