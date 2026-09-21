# Releasing LoonFS

Each release uses one version for these artifacts:

- four CLI archives and their `SHA256SUMS` on the GitHub release
- the server image `ghcr.io/loonfs/loonfs-server:vX.Y.Z` (linux/amd64 and linux/arm64)
- the Helm chart at `oci://ghcr.io/loonfs/charts/loonfs-server`
- the published workspace crates on crates.io
- the Homebrew formula `loonfs/tap/loonfs`
- the API reference on [loonfs.com](https://loonfs.com)

Publishing a GitHub release starts `.github/workflows/release-loonfs.yml`.
The workflow verifies that the tag matches the workspace and chart versions,
then builds and publishes the CLI archives, server image, and Helm chart.
After those succeed, it publishes the crates, updates Homebrew and the API
reference, and checks that the public installer installs the requested version.
The tap's CI tests the formula; the website's deployment workflow publishes
the updated reference. These run in their own repositories.

## One-time setup

- On crates.io, add a [GitHub trusted publisher](https://crates.io/docs/trusted-publishing)
  for each published crate: `loonfs-api`, `loonfs-objectstore`, `loonfs-client`,
  `loonfs-core`, `loonfs`, `loonfs-grep`, and `loonfs-cli`. Set owner and
  repository to `loonfs`, workflow to `release-loonfs.yml`, and leave environment
  empty. The workflow uses a temporary token; no crates.io secret is needed.
- In this repository's Actions secrets, set `HOMEBREW_TAP_TOKEN` to a GitHub
  token with Contents read/write access to `loonfs/homebrew-tap`, and
  `LOONFS_WWW_TOKEN` to one with Contents and Pull requests read/write access
  to `loonfs/loonfs_www`. The tap token pushes to `main`. The website token
  opens and merges a PR, as required by that repository's branch rules.
  These updates start the existing tap CI and website deployment workflows.

## 1. Prepare the version

Start from a clean branch based on `main` after CI passes:

```sh
scripts/prepare-release.sh --version X.Y.Z
```

The script updates `workspace.package.version` and the pinned registry
versions in `Cargo.toml`, updates the server chart, regenerates the OpenAPI
specification, and refreshes `Cargo.lock`. Its workspace and chart version checks
match the release workflow; its OpenAPI specification test matches CI.

Commit the result as `chore(release): prepare vX.Y.Z` and open a PR. Merge it
only after the normal PR checks pass.

## 2. Publish the GitHub release

Write the release notes before publishing. Start with a short summary of the
important changes and whether the previous release reads this release's
durable format (rollback is supported only where the notes say so), followed
by the generated PR list:

```sh
gh api repos/loonfs/loonfs/releases/generate-notes -f tag_name=vX.Y.Z --jq .body
```

Create the release from the updated `main` branch. This command publishes the
release and starts the release workflow:

```sh
gh release create vX.Y.Z --target main --title "vX.Y.Z" --notes-file notes.md
```

Watch the workflow with `gh run watch`. The release will contain four archives,
`SHA256SUMS`, and `ARTIFACTS.txt`, which records the image and chart digests.
Also check the [Homebrew CI](https://github.com/loonfs/homebrew-tap/actions)
and [website deployment](https://github.com/loonfs/loonfs_www/actions).

## If a step fails

Fix the cause, then use **Re-run failed jobs** in GitHub Actions. Crate versions
that are already published are skipped. Homebrew and website updates create a
commit only when their files change.

Homebrew and the API reference follow the latest non-prerelease GitHub release.
An older release or prerelease does not update them.
