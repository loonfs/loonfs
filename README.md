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

You will need access to an object storage bucket in the form of keys in order to set up Loon. Once installed, you can get started by running the following commands. 
```bash
loon init                   # creates your first Loon profile and sets it as the default. We recommend using embedded mode to start, with an object storage store.

loon namespace create {namespace_id}
loon use {namespace_id}   # sets the newly created namespace as the default.
```

You can add a `SKILL` to your agent of choice so that they know to use Loon when appropriate. See our [example `SKILL.md`](https://github.com.loonfs/loonfs/tree/main/examples/SKILL.md).

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

`local-fs` is an object-store provider for development, tests, or hardened local deployments. It is not the same thing as embedded mode; embedded mode can use `local-fs`, S3, or Cloudflare R2.

**Both modes are multi-player by default.**

## Core concepts

- Namespaces: a Loon namespace is the core unit of filesystem visibility and history. You can think of each namespace as a separate filesystem.
- Content stores: immutable file bytes live in content stores. A namespace points at one content store, and forked namespaces share that content store without copying bytes.

## Design philosophy

We believe that an agent-first durable file-system must have the following properties:
- Durable: avoiding data loss is the most important property of a good file system. If a write is marked as successful, the data should not be lost, ever.
- Immediately consistent: the latest state should be available to all readers without latency.
- High write throughput: many agents might attempt to update various parts of the filesystem at once. It must scale elegantly to accommodate that scenario.
- Version history: agents make mistakes. The ability to rewind to a past checkpoint is essential.
