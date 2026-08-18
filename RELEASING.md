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
Publishing crates, updating the Homebrew tap, and updating the website are
manual steps.

## 1. Prepare the version

Start from a clean branch based on `main` after CI passes:

```sh
scripts/prepare-release.sh --version X.Y.Z
```

The script updates `workspace.package.version` and the pinned registry
versions in `Cargo.toml`, updates the server chart, regenerates the OpenAPI
specification, and refreshes `Cargo.lock`. It then runs the version checks and
OpenAPI specification test that the release workflow runs for the tag.

Commit the result as `chore(release): prepare vX.Y.Z` and open a PR. Merge it
only after the normal PR checks pass.

## 2. Publish the GitHub release

Write the release notes before publishing. Start with a short summary of the
important changes, followed by the generated PR list:

```sh
gh api repos/loonfs/loonfs/releases/generate-notes -f tag_name=vX.Y.Z --jq .body
```

Create the release from the updated `main` branch. This command publishes the
release and starts the release workflow:

```sh
gh release create vX.Y.Z --target main --title "vX.Y.Z" --notes-file notes.md
```

Watch the workflow with `gh run watch`. After it completes, confirm that the
release contains the four archives, `SHA256SUMS`, and `ARTIFACTS.txt`. The
`ARTIFACTS.txt` file contains the published image and chart digests.

## 3. Publish the crates

```sh
cargo publish --workspace
```

Cargo publishes the publishable crates in dependency order and skips the
`publish = false` ones. If publishing one crate at a time instead, the order
is: `loonfs-api`, `loonfs-objectstore`, `loonfs-client`, `loonfs-core`,
`loonfs`, `loonfs-grep`, `loonfs-cli`.

## 4. Update the Homebrew tap

```sh
scripts/bump-homebrew-tap.sh --version X.Y.Z
```

The script uses the release checksums to update the formula in the tap checkout
(`../homebrew-tap` by default). Review the printed diff, then commit and push
the tap change as `chore: update to vX.Y.Z`.

## 5. Update the website

Update the API reference website through its private release process.

## 6. Verify the release

- `curl -fsSL https://install.loonfs.com | sh` installs a binary that reports
  `X.Y.Z`. The installer selects the latest release automatically.
- `brew install loonfs/tap/loonfs` (or `brew upgrade loonfs`) installs
  `X.Y.Z`.
- `docker pull ghcr.io/loonfs/loonfs-server:vX.Y.Z` downloads the digest
  recorded in `ARTIFACTS.txt`.

A package published for the first time starts private, and the organization
blocks public packages by default. Before anonymous pulls can work, enable
public packages once in the organization's package settings, then make the new
package public in its own package settings. This applies to the server image
and the chart package separately.
