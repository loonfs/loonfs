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

1. Write release notes that summarize the changes and any limits on format compatibility or rollback.
2. Publish the release from the updated `main` branch:

   ```sh
   gh release create vX.Y.Z --target main --title "vX.Y.Z" --notes-file notes.md
   ```

   Publishing starts the release workflow, which builds and publishes the release artifacts and packages.

3. Check the workflow result and the release assets. Archive checksums are in `SHA256SUMS`, and the server image and Helm chart digests are in `ARTIFACTS.txt`.

## Retry a failed step

If a step fails, fix the cause, and then select "Re-run failed jobs" in GitHub Actions. The workflow skips crate versions that are already published.
