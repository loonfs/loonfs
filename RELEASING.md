# Releasing LoonFS

Each release uses one version number for the CLI, the server, the Helm chart, and the Rust crates.

## Prepare the version

To prepare the version:

1. Start from a clean branch based on `main`, and run:

   ```sh
   scripts/prepare-release.sh --version X.Y.Z
   ```

2. Commit the changes as `chore(release): prepare vX.Y.Z`, and open a PR.
3. Merge the PR after CI passes.

## Publish the release

To publish the release:

1. Write release notes that summarize the changes and any limits on format compatibility or rollback, and update the [compatibility table](#format-compatibility). A prerelease updates the prerelease row, and a stable release adds its own row.
2. Publish the release from the updated `main` branch:

   ```sh
   gh release create vX.Y.Z --target main --title "vX.Y.Z" --notes-file notes.md
   ```

   Publishing starts the release workflow, which builds and publishes the release artifacts and packages.

3. Check the workflow result and the release assets. Archive checksums are in `SHA256SUMS`, and the server image and Helm chart digests are in `ARTIFACTS.txt`.

## Format compatibility

The table below lists, for each release, the versions of each durable family and segment encoding that the release writes and reads. Every release so far is a prerelease. A format change before the stable format is released keeps version 1, so the only objects that a prerelease is guaranteed to read are the ones that binaries of the same release wrote. After the stable format is released, a rollback to an earlier release is valid only if that release reads every stored version, as described in [Strict decoding and evolution](docs/specs/format.md#123-strict-decoding-and-evolution). Until then, a rollback is valid only to the release that wrote the stored objects.

| Release | Writes | Reads |
| --- | --- | --- |
| v0.5.0 and earlier prereleases | Version 1 of every family in [Family versions](docs/specs/format.md#a1-family-versions): the WAL object, the namespace manifest, the five control records (snapshot, upload session, upload outcome, member entry, and manifest boundary), metadata segment encoding 1, and grep segment version 1 | Version 1, written by the same release |

## Retry a failed step

If a step fails, fix the cause, and then select "Re-run failed jobs" in GitHub Actions. The workflow skips crate versions that are already published.
