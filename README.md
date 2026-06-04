![LoonFS Logo](assets/loonfs-wordmark-black.svg)

LoonFS is an agent-native, durable file-system backed by object storage. It is multiplayer by default. It can be used across agents, sessions, and teams in embedded/direct mode without a LoonFS server.

The Loon protocol enables version history, branching, and replays out of the box.

## Download

You can use the [install script](https://github.com/loonfs/loonfs/blob/main/scripts/install-loon.sh) by running
```bash
curl -fsSL https://install.loonfs.com | sh
```

If you use Homebrew as your package manager, you can also install it by running
```bash
brew install loonfs/tap/loon
```

Finally, you can compile it directly from source by checking out this repository and running
```bash
cargo build -p loon-cli                     # compile from source
cp ./target/debug/loon ~/.local/bin/loon    # copy it to somewhere in your $PATH
```

## Quickstart

For a durable, shared workspace you will eventually want an object storage bucket (S3 or Cloudflare R2). To just try Loon on this machine without any credentials, use the zero-config trial:

```bash
loon init default --no-input --mode embedded --store-kind local-fs --root ~/.loonfs/data
loon namespace create {namespace_id}
loon use {namespace_id}
```

When you are ready to make the workspace shareable across machines or teammates, switch to S3 / R2 by running `loon profile create <name> --mode embedded --store-kind aws-s3 ...` (or `--store-kind cloudflare-r2 ...`). For the interactive walkthrough, run `loon init` without flags.

## Use Loon with your agent

Loon ships first-class skills for the major agent CLIs — used here for **whole-file artifacts** like GTM plans, design docs, RFCs, customer proposals, exec briefs, and ops handoffs. Loon is not the right tool for editing source code (use git for that); it shines on documents that get read, drafted, branched, reviewed, and revised by both agents and humans.

Each command installs the `loon` binary **and** registers the Loon skill at the path that agent auto-discovers, so the agent picks it up after a restart with no manual file editing.

| Agent          | One-liner                                                                          |
| -------------- | ---------------------------------------------------------------------------------- |
| Claude Code    | `curl -fsSL https://install.loonfs.com \| sh -s -- --with-skill claude-code`       |
| Codex          | `curl -fsSL https://install.loonfs.com \| sh -s -- --with-skill codex`             |
| Anything else  | `curl -fsSL https://install.loonfs.com \| sh -s -- --with-skill agents-md`         |

The `agents-md` variant writes a project-scope `AGENTS.md` and works with Aider, OpenHands, GitHub Copilot, Gemini CLI, Devin, Windsurf, Zed, and any other tool that follows the [agents.md](https://agents.md) convention. The `agents-md` variant is project-scope: run it from the directory you want the file in. After install, restart the agent (or start a new thread).

If your agent cannot run shell commands, use the [paste-in install prompt](https://github.com/loonfs/loonfs/blob/main/examples/agents/install-prompt.md) instead. The per-agent skill files live under [`examples/agents/`](https://github.com/loonfs/loonfs/tree/main/examples/agents) and the canonical reference skill is [`examples/SKILL.md`](https://github.com/loonfs/loonfs/blob/main/examples/SKILL.md).

## Sample usage

Here are the main filesystem commands. See [here](https://github.com/loonfs/loonfs/tree/main/crates/loon-cli/README.md) for a comprehensive list.

```bash
loon put {LOCAL_FILE_PATH} {REMOTE_FILE_PATH}
loon ls {REMOTE_DIR_PATH}
loon cat {REMOTE_FILE_PATH}
loon stat {REMOTE_FILE_PATH}
loon get {REMOTE_FILE_PATH} {LOCAL_FILE_PATH}
loon rm {REMOTE_FILE_PATH}
```

The LoonFS CLI has two main topology modes: `embedded` and `remote`:
- `embedded`/direct mode talks to object storage directly from the current process; no LoonFS server is required.
- `remote` mode talks to a LoonFS server, which hosts the runtime and talks to object storage.

Embedded mode is production-capable when backed by an object store that satisfies the LoonFS object-store contract. `local-fs` is an object-store provider, not a mode; treat it as dev/test. Embedded mode can use `local-fs`, S3, or Cloudflare R2.

**Both modes are multi-player by default.**

## Performance tracing

Server tracing is opt-in. Set `LOONFS_TRACE=json` to emit JSON `tracing` span close events, and use `RUST_LOG` to control filters. When `LOONFS_TRACE=json` is set without `RUST_LOG`, the server defaults to `loon_server=info,loonfs=info,loon_core=info`.

```bash
LOONFS_TRACE=json RUST_LOG=loon_server=info,loonfs=info,loon_core=info \
  cargo run -p loon-server -- --config configs/loon-server.local-fs.example.toml
```

Server object-store metrics are separately opt-in. Set `LOONFS_OBJECT_STORE_METRICS_JSONL=target/loonfs-perf/object-store.ndjson` to write privacy-safe per-call samples for object-store operations.

## Core concepts

- Namespaces: a Loon namespace is the core unit of filesystem visibility and history. You can think of each namespace as a separate filesystem.
- Content stores: immutable file bytes live in content stores. A namespace points at one content store, and forked namespaces share that content store without copying bytes.

## Design philosophy

We believe that an agent-first durable file-system must have the following properties:
- Durable: avoiding data loss is the most important property of a good file system. If a write is marked as successful, the data should not be lost, ever.
- Immediately consistent: the latest state should be available to all readers without latency.
- High write throughput: many agents might attempt to update various parts of the filesystem at once. It must scale elegantly to accommodate that scenario.
- Version history: agents make mistakes. The ability to rewind to a past checkpoint is essential.
