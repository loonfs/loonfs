# LoonFS code map

This document describes how the LoonFS crates fit together and where code lives. [STYLE.md](STYLE.md) describes how to write the code, and the specs in `docs/specs/` define the durable formats and the API.

## Crates

| Crate | Role |
| --- | --- |
| `loonfs-types` | Vocabulary: IDs, paths, wire shapes, and durable codecs. It has no internal dependencies. |
| `loonfs-objectstore` | The storage boundary: `ObjectStore`, keys, providers, and `StoreConfig`. |
| `loonfs-core` | The protocol engine: metadata, commits, replay, and maintenance. |
| `loonfs` | The runtime: handles, caching, background work, and the publisher. Surfaces reach core through this crate and no other. |
| `loonfs-http` | The HTTP surface, shared by the server and the conformance server. |
| `loonfs-server`, `loonfs-cli`, `loonfs-client` | Surfaces. The server's code never imports core. |
| `loonfs-grep` | An extension. Core has no code for grep. |
| `loonfs-model` | An independent implementation of the protocol (the "model oracle") that tests compare core against. |
| `loonfs-sim`, `loonfs-conformance`, `loonfs-test-support` | Harness contracts and test support. |

## Rules about where code lives

- `LoonFs`, `Namespace`, `ReadView`, and `Maintenance` are the only handle types. New public capability goes on one of them.

- Core never spawns a task, and neither do HTTP handlers, the CLI, or extensions. Two owners spawn tasks: the publisher registry and a host's scheduler. An owner registers each task it spawns, so that `LoonFs::drain` can wait for folds, then for the compactions they started, and a shutdown leaves no background work running.

- Spawned work runs on the handle's runtime, through the `Handle` that the handle keeps from the runtime it was opened in. The one other runtime is the object-store IO runtime, which keeps provider connection pools independent of a caller's runtime. The one detached task, which aborts an abandoned multipart upload, runs there. It's best effort, because the provider's lifecycle rules and the collector reclaim whatever it misses.

- Three places contain a panic, and each has a test:
  - The publisher (`crates/loonfs/src/publisher.rs`) catches a panic in publishing, folding, compaction, or deletion. It counts the panic and keeps the writer session open.
  - When an HTTP handler panics, `catch_handler_panic` in `loonfs-http` returns a 500 with the error envelope.
  - The server's maintenance sweep contains a panic in each visit to a namespace, and counts it as a `server_error` failure.

- `loonfs-objectstore::keys` builds every object key and prefix. An extension builds its prefix through it, and no other code builds a key with `format!`.

- The model oracle and grep are the two deliberate duplications of core's logic. `loonfs-model` restates the visibility rules from scratch and never shares code with core, because an oracle that shared core's code would share its bugs, and the differential suite couldn't find them. Grep keeps its own copies of the core helpers it uses.
