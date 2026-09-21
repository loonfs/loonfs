# Releasing LoonFS

Each release uses one version for the CLI, server, Helm chart, and Rust crates.

## Prepare the version

Start from a clean branch based on `main`:

```sh
scripts/prepare-release.sh --version X.Y.Z
```

Commit the changes as `chore(release): prepare vX.Y.Z` and open a PR. Merge it
after CI passes.

## Publish the release

Write release notes that summarize the changes and any limits on format
compatibility or rollback. Then publish from the updated `main` branch:

```sh
gh release create vX.Y.Z --target main --title "vX.Y.Z" --notes-file notes.md
```

Publishing starts the release workflow, which builds and publishes the release
artifacts and packages.

Check the workflow result and the release assets. Archive checksums are in
`SHA256SUMS`; server image and Helm chart digests are in `ARTIFACTS.txt`.

## Retry a failed step

Fix the cause, then select **Re-run failed jobs** in GitHub Actions. Crate
versions that are already published are skipped.
