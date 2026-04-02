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

The containing app creates a default config file if it is missing. The file lives under the sample
App Group support directory:

- `~/Library/Group Containers/<app-group>/Library/Application Support/LoonFileProviderSample/loon-file-provider-sample.json`

On launch and before domain registration, the app mirrors the validated config into the App Group's
shared defaults suite so the extension does not need to read the JSON file directly during Finder
enumeration.

Expected fields:

- `ops_config_path`
- `exposed_namespaces`
- `domain_identifier`
- `domain_display_name`
- `app_group_identifier`

For the smoothest first run, point `ops_config_path` at an ops config whose client state DB,
mirror root, and object-store paths are also inside that same App Group
`Library/Application Support/LoonFileProviderSample/` subtree. The sample does not manage
security-scoped bookmarks in this slice.

## Native workflow

1. Launch the app.
2. Reveal the config file in Finder and edit it with a valid ops config path and namespace list.
3. Relaunch the app or click `Register Domain` so the edited file is copied into the shared App
   Group defaults used by the extension.
4. Finder should show one `Loon` File Provider domain with namespaces as top-level folders.
5. Remove the domain with `Remove Domain` when you need a local reset.

Freshness still comes from the existing CLI flow:

```bash
loon ops bootstrap-namespace --config <path> --namespace <id>
loon ops import-remote-observations --config <path> --namespace <id>
loon ops sync-once --config <path> --namespace <id>
```
