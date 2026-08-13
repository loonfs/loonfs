# Releasing LoonFS

One release ships, under one version:

- four CLI archives and their `SHA256SUMS` on the GitHub release
- the server image `ghcr.io/loonfs/loonfs-server:vX.Y.Z` (linux/amd64 and linux/arm64)
- the Helm chart at `oci://ghcr.io/loonfs/charts/loonfs-server`
- the published workspace crates on crates.io
- the Homebrew formula `loonfs/tap/loonfs`
- the API reference on [loonfs.com](https://loonfs.com)

The GitHub release is the trigger. Publishing it runs
`.github/workflows/release-loonfs.yml`, which refuses a tag that disagrees
with the workspace or chart version, then builds and publishes everything
above except crates.io, the tap, and the site — those are the manual steps
that follow.

## 1. Prepare the version

From a branch cut off a clean, green main:

```sh
scripts/prepare-release.sh --version X.Y.Z
```

The script rewrites every file that names the version — `Cargo.toml`
(`workspace.package.version` and the pinned registry versions of the
published crates), the server chart, and the regenerated OpenAPI spec —
refreshes `Cargo.lock`, and then verifies the checkout with the same checks
the release workflow runs against the tag, including the spec-lock test.

Commit the result as `chore(release): prepare vX.Y.Z`, open a PR, and merge
it. CI on the PR is the real gate; the script only catches drift early.

## 2. Publish the GitHub release

Draft the notes first: a few curated highlights up top, then the generated
PR list for the long tail:

```sh
gh api repos/loonfs/loonfs/releases/generate-notes -f tag_name=vX.Y.Z --jq .body
```

Then create the release on the merged main. Creating it publishes it, and
publishing is what starts the workflow:

```sh
gh release create vX.Y.Z --target main --title "LoonFS vX.Y.Z" --notes-file notes.md
```

Watch the run with `gh run watch`, then confirm the release page carries the
four archives, `SHA256SUMS`, and `ARTIFACTS.txt`. `ARTIFACTS.txt` records
the image and chart digests the release published.

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

The script rewrites the formula in the tap checkout (`../homebrew-tap` by
default) from the release's published checksums. Review the diff it prints,
then commit and push in the tap as `chore: update to vX.Y.Z`.

## 5. Update the site

In `loonfs_www`, re-vendor the spec the site renders and open a PR:

```sh
npm run sync:openapi
```

## 6. Prove the release

- `curl -fsSL https://install.loonfs.com | sh` installs a binary that
  reports `X.Y.Z` — the installer resolves the latest release on its own.
- `brew install loonfs/tap/loonfs` (or `brew upgrade loonfs`) lands `X.Y.Z`.
- `docker pull ghcr.io/loonfs/loonfs-server:vX.Y.Z` resolves to the digest
  `ARTIFACTS.txt` records.
