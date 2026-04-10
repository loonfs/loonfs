![LoonFS Logo](assets/loonfs-wordmark-black.svg)

LoonFS is an agent-native, durable file-system backed by object storage. It is multiplayer by default. It can be used across agents, sessions, and teams even when running in local mode.

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
loon init                   # creates your first Loon profile and sets it as the default. We recommend using 'local' mode to start, with an object storage store.

loon namespace create {some_namespace}
loon use {some_namespace}   # sets the newly created namespace as the default.
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

The LoonFS cli has two main modes: `local` and `remote`:
- `local` interfaces with the store (object storage) directly.
- `remote` makes request to a remote LoonFS Server which is then in charge of interfacing with the store. There is no current canonical LoonFS server, as such you have to run your own in order to leverage that option.

**Both modes are multi-player by default.**

## Core concepts

- Namespaces: a Loon namespace is the core unit of filesystem visibility, history, and durability. You can think of each namespace as a separate filesystem.

## Design philosophy

We believe that an agent-first durable file-system must have the following properties:
- Durable: avoiding data loss is the most important property of a good file system. If a write is marked as successful, the data should not be lost, ever.
- Immediately consistent: the latest state should be available to all readers without latency.
- High write throughput: many agents might attempt to update various parts of the filesystem at once. It must scale elegantly to accommodate that scenario.
- Version history: agents make mistakes. The ability to rewind to a past checkpoint is essential.
