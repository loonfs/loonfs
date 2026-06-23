#!/bin/sh

if [ -n "${BASH_VERSION:-}" ] && [ "${BASH_SOURCE:-$0}" != "$0" ]; then
    echo "error: install-loon.sh must be executed, not sourced" >&2
    echo "run it with: curl -fsSL https://raw.githubusercontent.com/loonfs/loonfs/main/scripts/install-loon.sh | sh" >&2
    return 1
fi

case "${ZSH_EVAL_CONTEXT:-}" in
    *:file)
        echo "error: install-loon.sh must be executed, not sourced" >&2
        echo "run it with: curl -fsSL https://raw.githubusercontent.com/loonfs/loonfs/main/scripts/install-loon.sh | sh" >&2
        return 1
        ;;
esac

set -eu

REPO_SLUG="${LOONFS_REPO_SLUG:-loonfs/loonfs}"
INSTALL_DIR="${LOONFS_INSTALL_DIR:-$HOME/.local/bin}"
VERSION=""
WITH_SKILL=""
tmpdir=""

usage() {
    cat <<'EOF'
Usage: install-loon.sh [--version <tag>] [--install-dir <path>] [--with-skill <agent>]

Options:
  --version <tag>          Install a specific release tag (default: latest)
  --install-dir <path>     Where to install the loon binary (default: $HOME/.local/bin)
  --with-skill <agent>     Also install the Loon agent skill for one of:
                             claude-code  -> $HOME/.claude/skills/loon/SKILL.md
                             codex        -> $HOME/.codex/AGENTS.md
                             agents-md    -> ./AGENTS.md
                           If the destination already exists the file is left
                           untouched and the path is printed.
EOF
}

cleanup() {
    if [ -n "$tmpdir" ] && [ -d "$tmpdir" ]; then
        rm -rf "$tmpdir"
    fi
}

die() {
    echo "error: $*" >&2
    exit 1
}

need_cmd() {
    if ! command -v "$1" >/dev/null 2>&1; then
        die "required command not found: $1"
    fi
}

download() {
    url="$1"
    destination="$2"

    if command -v curl >/dev/null 2>&1; then
        curl -fsSL "$url" -o "$destination"
        return
    fi

    if command -v wget >/dev/null 2>&1; then
        wget -qO "$destination" "$url"
        return
    fi

    die "install requires curl or wget"
}

detect_target() {
    os="${LOONFS_INSTALL_OS:-$(uname -s)}"
    arch="${LOONFS_INSTALL_ARCH:-$(uname -m)}"

    case "$os/$arch" in
        Darwin/arm64)
            printf '%s\n' "aarch64-apple-darwin"
            ;;
        Darwin/x86_64)
            printf '%s\n' "x86_64-apple-darwin"
            ;;
        Linux/x86_64)
            printf '%s\n' "x86_64-unknown-linux-gnu"
            ;;
        Linux/aarch64)
            printf '%s\n' "aarch64-unknown-linux-gnu"
            ;;
        *)
            die "unsupported platform: $os $arch
supported targets: macOS arm64, macOS x86_64, Linux arm64 GNU, Linux x86_64 GNU"
            ;;
    esac
}

checksum_file() {
    archive_path="$1"
    sums_path="$2"

    if command -v shasum >/dev/null 2>&1; then
        expected=$(awk -v name="$(basename "$archive_path")" '$2 == name { print $1 }' "$sums_path")
        actual=$(shasum -a 256 "$archive_path" | awk '{ print $1 }')
    elif command -v sha256sum >/dev/null 2>&1; then
        expected=$(awk -v name="$(basename "$archive_path")" '$2 == name { print $1 }' "$sums_path")
        actual=$(sha256sum "$archive_path" | awk '{ print $1 }')
    else
        die "install requires shasum or sha256sum"
    fi

    if [ -z "$expected" ]; then
        die "checksum entry missing for $(basename "$archive_path")"
    fi

    if [ "$expected" != "$actual" ]; then
        die "checksum verification failed for $(basename "$archive_path")"
    fi
}

install_skill() {
    agent="$1"

    case "$agent" in
        claude-code)
            src_path="examples/SKILL.md"
            dest_dir="$HOME/.claude/skills/loon"
            dest_file="$dest_dir/SKILL.md"
            ;;
        codex)
            src_path="examples/agents/agents-md/AGENTS.md"
            dest_dir="$HOME/.codex"
            dest_file="$dest_dir/AGENTS.md"
            ;;
        agents-md)
            src_path="examples/agents/agents-md/AGENTS.md"
            dest_dir="."
            dest_file="./AGENTS.md"
            ;;
        *)
            die "unsupported --with-skill agent: $agent
supported: claude-code, codex, agents-md"
            ;;
    esac

    if [ -e "$dest_file" ]; then
        printf 'skill destination already exists: %s\n' "$dest_file"
        printf 'leaving the existing file untouched. inspect it, back it up, or remove it and rerun to install the current Loon skill.\n'
        return 0
    fi

    if [ -n "$VERSION" ]; then
        skill_ref="$VERSION"
    else
        skill_ref="main"
    fi
    src_url="${LOONFS_SKILL_URL_ROOT:-https://raw.githubusercontent.com/$REPO_SLUG}/$skill_ref/$src_path"

    mkdir -p "$dest_dir"
    download "$src_url" "$dest_file"
    printf 'installed loon skill to %s\n' "$dest_file"

    case "$agent" in
        claude-code)
            printf 'restart Claude Code (or start a new thread) for the skill to be discovered.\n'
            ;;
        codex)
            printf "next Codex session will read this AGENTS.md from \$CODEX_HOME.\n"
            ;;
        agents-md)
            printf 'any AGENTS.md-aware agent run from this directory will pick this up.\n'
            ;;
    esac
}

main() {
    while [ "$#" -gt 0 ]; do
        case "$1" in
            --version)
                VERSION="${2:-}"
                shift 2
                ;;
            --install-dir)
                INSTALL_DIR="${2:-}"
                shift 2
                ;;
            --with-skill)
                WITH_SKILL="${2:-}"
                shift 2
                ;;
            -h|--help)
                usage
                exit 0
                ;;
            *)
                usage >&2
                die "unknown argument: $1"
                ;;
        esac
    done

    need_cmd uname
    need_cmd tar
    need_cmd mktemp

    target=$(detect_target)
    archive_name="loon-$target.tar.gz"

    if [ -n "$VERSION" ]; then
        base_url="${LOONFS_RELEASE_URL_ROOT:-https://github.com/$REPO_SLUG/releases}/download/$VERSION"
    else
        base_url="${LOONFS_RELEASE_URL_ROOT:-https://github.com/$REPO_SLUG/releases}/latest/download"
    fi

    tmpdir=$(mktemp -d)
    archive_path="$tmpdir/$archive_name"
    sums_path="$tmpdir/SHA256SUMS"

    download "$base_url/$archive_name" "$archive_path"
    download "$base_url/SHA256SUMS" "$sums_path"
    checksum_file "$archive_path" "$sums_path"

    mkdir -p "$INSTALL_DIR"
    tar -xzf "$archive_path" -C "$tmpdir"
    install_path="$INSTALL_DIR/loon"
    cp "$tmpdir/loon" "$install_path"
    chmod 755 "$install_path"

    printf 'installed loon to %s\n' "$install_path"
    "$install_path" version

    case ":$PATH:" in
        *:"$INSTALL_DIR":*)
            ;;
        *)
            printf 'add %s to PATH to run `loon` directly\n' "$INSTALL_DIR"
            ;;
    esac

    if [ -n "$WITH_SKILL" ]; then
        install_skill "$WITH_SKILL"
    fi
}

trap cleanup EXIT INT TERM
main "$@"
