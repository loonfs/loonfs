# Architecture overview

LoonFS has four major roles:

```text
+---------+      commit requests      +------------------------+
| Clients  | -----------------------> | Authoritative service  |
+---------+                           +------------------------+
     |                                          |
     | upload/download content                  | WAL, head, leases
     v                                          v
+---------------------------------------------------------------+
|                         Object store                           |
|  blocks | manifests | head | lease | WAL | checkpoints | ... |
+---------------------------------------------------------------+
                ^                                  |
                | checkpoints, indices, repair     |
                +---------- Background workers -----+
```

## Component responsibilities

| Component | Responsibility |
| --- | --- |
| **Clients** | Observe local and remote state, upload or download file content, send mutation requests, and preserve enough durable local state to recover after restart. |
| **Authoritative service** | Accept mutation requests, validate them against the latest durable namespace state, write immutable WAL entries, and advance the namespace head with compare-and-swap. |
| **Background workers** | Build checkpoints and other derived data, publish progress, and repair missed queue work. They do not make metadata changes visible. |
| **Object store** | The only durable dependency. It holds both canonical objects and small control objects. |

## Durable boundaries

The architecture is built around three boundaries:

1. **Durable content boundary**  
   File-content blocks and content manifests must already exist before metadata may reference them.

2. **Visibility boundary**  
   Metadata becomes visible only when the namespace head advances successfully.

3. **Recovery boundary**  
   Readers and writers rebuild the authoritative basis from durable objects, not from process-local caches.

## Read path in one paragraph

A reader starts from `head.json`. If the head advertises a verified checkpoint, the reader loads that checkpoint and then replays WAL entries after the checkpoint’s `seq`. If no checkpoint is advertised, the reader replays the WAL from the namespace’s bootstrap state. Derived indices may be used only when durable progress objects prove they cover the requested boundary.

## Write path in one paragraph

A writer uploads any missing content, acquires or renews the namespace lease, reconstructs the latest authoritative basis from durable state, validates the request’s preconditions, writes one immutable WAL entry, and then advances the head object with compare-and-swap. Success is reported only after the head update succeeds.

## What belongs in the core spec

The core spec should define:

- the durable objects and their meaning
- how namespace state is reconstructed
- how visibility, ordering, and conflict rules work
- what a conforming client, writer, reader, and worker must preserve

It should **not** define:

- one package layout
- one thread model
- one local database schema
- one test harness structure
- one platform bridge

Those are implementation choices as long as they preserve the same durable behavior.
