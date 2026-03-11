# prompt.md

You are helping me design and implement a Dropbox-like sync system, with a strong emphasis on correctness, determinism, and small, testable iterations.

I want you to treat the following as already decided unless I explicitly change it.

# High-level product and architecture

We are building a sync platform with:

- an **object-storage-only durable backend**
- a **Rust server**
- a **macOS client**
- support for both:
  - a **local server mode** (for development and local + S3-only use cases)
  - a **remote server mode**

The system should support these client classes:

1. **Stateless API/CLI clients** for reading specific files and directories
2. **Automatic sync clients** that maintain a local mirror continuously
3. **Deterministic batch sync CLI clients** that push/pull full directories at a point in time
4. **Virtual filesystem / streamed clients** that behave more like a mounted drive

I want us to think about these as logical product buckets, inspired by Dropbox and Box, but also optimized for **agents** and **sandboxed environments**.

# Backend design decisions already made

## Core metadata model

- Canonical state is **inode-keyed**
- Paths are **derived views**, not canonical identity
- Inode identity should be treated as **(namespace_id, inode_id)**, not globally unique inode IDs across all namespaces
- Supported inode types include:
  - `FILE`
  - `DIR`
  - `SYMLINK`
  - `MOUNT`

## Namespaces and accounts

- An **account can have multiple namespaces**
- Accounts should be able to create **more than one namespace for themselves**, including many namespaces for agentic use
- Mountpoints are used to expose namespaces inside an account’s visible tree
- Cross-namespace moves are not atomic by default; copy+delete is acceptable

## Consistency and mutation semantics

- We chose **object-strong consistency**, not global-strong
- Writes are **inode-/key-addressed only**
- **No path-based mutation API**
- Mutations use **explicit preconditions**
- The server should explain the rationale behind mutation rules and the failure modes they protect against

## Conflict behavior

- Deterministic rename on name conflicts
- Conflict-copy behavior for edit conflicts
- **No device/user labels in filenames**
- Richer conflict details can live in metadata, but filenames should stay clean and deterministic

## Deletion semantics

- We initially chose subtree tombstones for recursive directory deletes
- We are interested in **delete vectors** as a possible future enhancement, especially if they make undelete easier

## Version history

- We want **native version history**
- The backend should support:
  - immutable content revisions
  - a revision index per inode
  - listing revisions
  - restoring an older revision as the current one
- Version history should be part of the design, not an afterthought

## Content storage and revisions

- Metadata WAL and content durability are separate concerns
- Content must be durable before a metadata revision is published
- Revisions should only hit the metadata WAL once the referenced content is uploaded and available
- I care a lot about avoiding “dangling revisions”

## Chunking / large file behavior

- I want a **uniform correctness policy for all files**
- We are currently leaning toward a **16 MiB default block size**
- I care about the risk of **large files causing sync to never complete**

## WAL / durability / sequencing

- The backend uses:
  - a WAL in object storage
  - snapshots
  - optional derived indices

## Object storage semantics

- Conditional writes / CAS are foundational
- Design should assume we care deeply about:
  - create-if-absent
  - compare-and-swap updates
  - lease correctness
  - head advancement correctness

# Client design decisions already made

## macOS client

- We want a **macOS client**
- It should eventually behave similarly to Dropbox Sync
- Initial UX can be **CLI-only**
- The implementation should use:
  - a **Rust daemon** running in the background
  - **File Provider** on macOS for filesystem integration
  - **not FUSE**
- For the client, we chose:
  - `type: SYMLINK` as a real inode type
  - a design that can support:
    - mirrored/full sync
    - later “just in time” / online-only loading
- The first milestone can focus on full live sync

## Client engineering style

I want the client spec and implementation plan to emphasize:

- deterministic planning/control flow
- strong local durability and recovery
- clear module boundaries
- a Rust daemon + CLI + File Provider bridge
- SQLite is acceptable for client-local state if needed
- the design should feel Dropbox/Nucleus-inspired in terms of rigor

# Engineering and repo preferences

These are important and should shape all recommendations:

- iterations should be **small**
- every change should be **tested**
- code should be **idiomatic Rust**
- avoid clever shortcuts
- prefer **type safety** wherever possible
- don’t try to solve every edge case immediately
- but **document known edge cases** clearly for future reference
- I want to evolve `agents.md` and `skills.md` over time as we learn
- I want **frequent commits**
- first-class adherence to the written spec is critical

# Existing repo / foundation work already discussed

We will be using **Quickwit** as inspiration for the Rust server foundation, especially for:

- Cargo workspace structure
- crate boundaries
- object store abstraction
- storage conformance tests
- dependency hygiene

The development path should strongly emphasize **testing inspired by Dropbox Nucleus**, including:

- deterministic simulation
- seeded randomized tests
- reducer/state machine tests
- failure injection
- reproducible failing seeds
- invariants checked at every step


