# LoonFS style

This guide describes how LoonFS code is written. Each rule is a design decision, recorded with an explanation and usually an example from the codebase. When you run into a case that no rule covers, use the closest decision and its explanation to decide, instead of guessing.

The specs in `docs/specs/` define the durable formats and the API, and they take precedence over this guide.

## Goals

LoonFS code serves four goals, in this order of priority:

1. Correctness
2. Simplicity
3. Performance
4. Developer experience

When you're deciding how to write something, choose the option that makes the code more correct, simpler, faster, or easier to work in, judged in that order.

Three rules apply everywhere:

- **Use one pattern for each decision**, so that a reader can rely on it. A convention applies to new code, and to code that a change already touches. An existing departure isn't, on its own, a reason for a change. For example, every ID is defined with `string_id!` or `numeric_id!`, and there's no other way to define one.

- **Remove the old design in the same change that adds a new one.** No setter, knob, type, or code path stays behind for compatibility, because a compatibility layer keeps two designs alive, and every later reader has to learn both. For example, the change that added the shared read cache also removed the per-handle caches and their builder setters.

- **Don't ship shortcuts.** Ship only work that meets the goals, even if a feature has to wait. A deliberate shortcut still has to be fixed later, and by then other code usually depends on it.

## Simplicity

This section describes how to keep a design simple.

- **Design types so that an invalid state can't exist**, instead of checking for it at runtime. A runtime check has to run everywhere the state is created, and a missed check is a bug. For example, `GrepBinding` in `loonfs-http` has one variant for each combination of serving grep queries and maintaining the index, and each variant contains the handles that its mode needs, so a capability that the server advertises always has its handle.

- **Remove code before you add a layer.** If a change can be made by removing code, that's better than adding an abstraction. Use few abstractions, and only ones that match a concept in the domain. For example, the fold used to pick one of three write paths based on the sizes involved (an in-memory merge, a streamed put, or a provider assembly). The provider adapters already knew how to make that choice, so the fold now writes every extent with one `assemble` call, and the three paths are gone.

- **Fix a bug by removing the wrong rule, not by adding a guard.** When a bug comes from a wrong rule or a special path, remove the rule or the path. A guard adds code, and it leaves the wrong rule in place for the next change to run into.

- **Count the costs before you design.** Count the round trips, bytes, and states that a design needs for the largest namespace the limits allow. Count requests before bytes, because one GET to object storage takes tens of milliseconds.

- **Measure a performance problem before and after you change it.** A performance change starts with a measurement that shows the problem, and it ends with the same measurement. Without the first measurement, a change can add code without making anything faster.

## Correctness

This section describes how LoonFS code handles failures and errors, and how it uses types, limits, time, concurrency, and memory to avoid whole classes of bugs.

### Failures

Three kinds of failure can happen, and the code responds to each kind differently:

| Kind | What happened | Response | Example |
| --- | --- | --- | --- |
| A bug in the process | The process's own computation reached a state that the code assumes can't happen. | Panic. | `expect("content write semaphore should remain open")` states what the engine assumes when it takes a permit. |
| Bad bytes in object storage | An object that LoonFS read from the store breaks a rule of the format, even though the process that read it is working correctly. | Return `CoreError::NamespaceCorrupt`, or a typed corruption error, scoped to the namespace that contains the object. | A manifest whose segment checksum doesn't match returns `ManifestLoadError`, which maps to the `namespace_corrupt` code. |
| A condition that stops the request | Nothing is broken, but the request can't succeed as written, or can't succeed right now (for example, a path doesn't exist, or a precondition isn't met). | Return the error code that describes the condition, so the caller can decide what to do. | A compare-and-swap that still fails after the attempt limit returns its own code, not `CoreError::Internal`. |

- **Check each assumption with `assert!`, or with `expect` and a message that says what should be true**, and keep these checks on in release builds. Don't return `CoreError::Internal` instead of panicking. That variant is reserved for a panic the runtime caught, and for a task that failed to join.

- **Check an invariant in its most direct form, at the point where it's true.** An `expect` whose message says what should be true is that form, so don't restructure code to avoid one. For example, an `expect` on each element of a list is better than `flatten()` followed by a check of the count, which can't say which element broke the invariant.

- **Don't use `unsafe`, and don't panic outside an invariant check.** `unsafe` is denied, with one allowance at the monotonic clock boundary. `unwrap`, `panic`, `todo`, `unimplemented`, and `unreachable` are lint errors in production code, so every panic is an `assert!` or an `expect` that states an invariant. `unwrap` appears only inside a `const` block, where it's checked at compile time.

- **Check bytes when they're decoded, and rely on that check where they're used.** The decoder rejects bad bytes from the store with a corruption error, and no code matches on that error to recover from it. Later code that relies on the same property uses `expect`, with a message that refers to the decode check, because by then a failure would be a bug. For example, decoding a WAL object rejects inline content that has no reference in the same commit, and `projected_tail.rs` expects that reference when it uses the content.

- **Tie related limits together with `const` assertions**, so that changing one constant without the other fails to compile. For example, `limits.rs` asserts the grace window against the publication budget.

- **Return the error for a terminal state, without retry or recovery code.** A fenced writer, a corrupt namespace, and a permission failure are terminal, which means that retrying can't change them. A transient transport failure is retried, with a count and a deadline. For example, when a read finds a metadata segment missing, `fs/read_result.rs` checks whether a newer manifest has been published. If one has, the read was stale rather than corrupt, and LoonFS retries it once. Otherwise, the corruption error stands.

- **Contain a panic only on purpose, with a test that shows what happens next.** A contained panic is counted, and the writer session stays open. Ending the session wouldn't fix anything, because a panic is a bug, and the same bug would panic again in the next session.

- **Treat a poisoned lock as a bug**, with `expect("... lock should not be poisoned")`. (A mutex is poisoned when a thread panics with the mutex locked.) Only a component that contains panics by design (the publisher registry) recovers the lock, with `into_inner` through one helper, because it has to stay usable after a contained panic.

- **Don't hold a `std::sync::Mutex` across a call into a trait object, a callback, or the metrics recorder.** Code that the lock's owner doesn't control might panic, which poisons the lock, or take the same lock, which deadlocks. Fix a lock that does this before you add an `expect` for its poisoning.

- **Never cap a decoded value below the limit the writer enforces**, because then LoonFS could write an object it can't read back. For example, the writer and the reader share one constant for the WAL object size cap.

Why does a bug panic instead of returning an error? If the code kept going after it found a bug in its own state, it could corrupt durable state in object storage, because it can't trust anything else it computes. A panic stops the task, but not always the process: Tokio contains a panic to the task that panicked. That's why each place that contains a panic has a test.

Why doesn't LoonFS panic on bad bytes too? A panic wouldn't fix anything. The durable state is stored in shared object storage, not on a local disk, so a restarted process would load the same object and panic again, and so would every other process that opens the namespace. An error scoped to the namespace reports the problem, and the process keeps serving its other namespaces.

### Errors

This section describes how errors are defined and formatted.

- **Keep one registry of error codes.** `ErrorCode` in `loonfs-types` is the registry of wire error codes. A macro generates it, so a variant, its string, and `ErrorCode::ALL` can't disagree. Clients match on the strings, so a code can't be renamed or removed after a release includes it. `ErrorCode::kind()` is the one way to group codes, and the server derives the HTTP status from `ErrorKind` in one function. (The CLI keeps a second registry, for codes that no server returns.)

- **Add a `code()` method that returns `ErrorCode` to every error that can reach the wire.** Errors from the vocabulary and storage crates don't have one, and the code that calls them maps them to a code. The Rust client's `code()` is the one exception, and it returns `Option`, because a transport failure has no code.

- **Define one error type for each crate, and match its name to the crate**: `CoreError` in `loonfs-core`, `GrepError` in `loonfs-grep`, `ClientError` in `loonfs-client`, and `ObjectStoreError` in `loonfs-objectstore`. `loonfs::Error` is the umbrella type. There's no second alias for a type, and each crate exports `pub type Result<T>`.

- **Use thiserror the same way everywhere:**
  - Use `#[from]` when an inner type maps to exactly one variant. A transparent wrapper without `#[from]` has a one-line comment that says why.
  - Include `object_key` in store and codec wrappers, so the message says which object failed.
  - Don't use a bare `#[error("{0}")]` on a `String` variant, so that every message has a prefix.
  - Match variant names to the wire codes (`PathNotFound`, `IndexCorrupt`).
  - Repeat the cause after the prefix in display text when it helps, because the server doesn't print source chains.

- **Mark every public error enum in a published crate `#[non_exhaustive]`**, so a variant can be added without breaking callers. That includes core's enums (because `loonfs` re-exports them), the registry, and the registry's classification enums. Keep other public enums exhaustive, so the compiler finds every `match` that a new variant affects. The exception is a wire enum that a client decodes, which ends in an `Unknown` catch-all, so that a newer server can't break an older client.

- **Define an error enum in the file of its subject.** When the files of a module directory share one error, define it in that directory's `error.rs`. An error moves between these two places only when its subject moves.

- **Derive `Clone` on core errors**, so that one failure can be returned to every caller waiting on the same batch. Don't derive `Serialize`, because nothing serializes them.

- **Treat a failure to encode a value that the process built as a bug.** Encoding such a value should never fail, so the code uses `expect`, and there's no `Codec` error for it.

- **Write error messages in lowercase**, with identifiers and flags in backticks, no final period, and values formatted with `Display`. Format a rejected input (an ID, a name, or a path) with `Debug`, which quotes and escapes it, so it doesn't need extra backticks. The ID validation errors in `loonfs-types/src/ids.rs` are the model.

### State and types

This section describes how to use types so that an invalid state can't be represented.

- **Represent a lifecycle, a mode, or an outcome as one enum**, where each variant contains only its own fields. A set of booleans, a bool next to an `Option`, or a sentinel value (`u64::MAX` or a zero-filled count) that stands in for a variant is a bug, because it allows combinations that don't mean anything. A value that the format or the protocol defines isn't a sentinel. Two examples are `WriterSessionStatus { Unacquired, Acquired(..), Fenced(..) }` and `PagerState { NotStarted, More(cursor), Done }`.

- **Pass a choice across a module boundary as a two-variant enum, not a bool.** The enum's name describes the choice (`DestinationBehavior`, `AttributeInclusion`), so a call site says what it's asking for instead of passing `true`. A private helper can take a bool, and so can a named field whose name says what `true` means, such as `serves_maintenance`. Don't combine two choices into one enum when the code that reads it ignores one of them.

- **Make a handle's mode part of its type.** `LoonFs<M>`, `Namespace<M>`, and the builder are generic over `ReadOnly` or `Writable` (a pattern called "typestate"), so a call to a write method on a read-only handle doesn't compile. Store data that only one mode needs in the handle, typed by the mode (`M::Session`), not in an `Option` that's read with `expect`. The mode trait doesn't extend to the builder's internals.

- **Put a durable status's transition rules on its enum.** When more than one place in the code performs a transition, the enum has a method for it that refuses an illegal move, so the rule for legal moves is written once. For example, the upload session's status has a method for the move from open to aborted.

- **Group optional response fields that go together** with `#[serde(flatten)] Option<Group>`, which doesn't change the JSON. For example, `PathEntry` flattens `AttributesProjection`. Requests validate those fields at runtime instead, because `flatten` doesn't work with `deny_unknown_fields`, and nesting the group would change the JSON, which needs a wire version change.

- **Don't derive `Clone` on a type whose size grows with the namespace.** Share it through `Arc`, and copy it only at an explicit site that a reviewer can see, because a stray `.clone()` would quietly copy a large amount of memory. `TailChunk` is an example.

- **Return a named struct, not a tuple, from a function that's visible outside its module.** A tuple's fields have no names, and a bare bool in a tuple reports a state without saying which state.

### Widths and units

This section describes which integer, ID, path, and time types to use.

| Type | Use it for |
| --- | --- |
| `u64` | Anything on the wire or in the store, and byte sizes, offsets, and sequences everywhere |
| `u32` | Positions inside one object |
| `usize` | In-memory budgets and counts, and nothing else |

- **Never put a `usize` in a wire, durable, or sidecar shape**, because its width depends on the platform. Convert a value to `usize` once, when it's loaded.

- **Narrow integers with `try_from` or a named conversion, not `as`.** `as` can widen a value (`usize as u64`), but it truncates without saying so when it narrows, and the truncation lint is on. Convert a `u64` to a `usize` with `usize_from_u64`. A `const` assertion in the same module fails the build on a target where `usize` isn't 64 bits, so the conversion can't truncate.

- **Define every ID, sequence, and validated string as a newtype** with `string_id!` or `numeric_id!`. Both macros generate the same methods for each type (`parse`, `as_str`, serde, `Ord`, `Hash`), so one kind of ID can't be passed as another. `parse()` is the one constructor that can fail. Numeric IDs are transparent (`pub u64`), because they're ordinals used in key arithmetic.

- **Generate a random ID as a prefix, an underscore, and a 26-character encoding of a version 4 UUID**, through `ids::generated_id`. For example, `con_3s0drmj8em84yb4mnetqkz2j0x` is a content ID. Each ID type that the server generates has a `generate()` constructor that calls it.
  - The prefix is a short lowercase word for the ID's type, such as `con` for a content ID or `upl` for an upload ID.
  - The suffix uses the lowercase form of Crockford's base 32 alphabet, `0123456789abcdefghjkmnpqrstvwxyz`. To encode a UUID, add two zero bits in front of its 128 bits (read most significant bit first), split the 130 bits into 26 groups of 5, and write each group as one character.
  - A parser rejects uppercase letters, and a suffix whose first character is greater than `7`, so that each UUID has exactly one spelling.

- **Parse an ID where the error can identify the input.** The CLI parses IDs at the clap boundary, so the error identifies the flag. HTTP query structs keep raw strings and parse each one through one helper per parameter, because an API error has to report the parameter's name, and serde's rejection doesn't.

- **Take paths as `&str` in the embedded handles**, and report an invalid path as the operation's own error. The Rust client takes a typed `NamespacePath`.

- **Represent a time as a `u64` number of milliseconds**, with the unit in its name, or as a `Duration` where a timer or transport API takes one. Don't use `secs`, `millis`, `micros`, or `nanos` in names. A value that's stored, or that needs more precision, keeps its own unit, such as `modified_ns` in the CLI's upload journal.

Why does a random ID use this alphabet? It leaves out `i`, `l`, and `o`, which are easy to confuse with `1` and `0`, so an ID can be read aloud or copied by hand. It also leaves out `u`, which makes it less likely that an ID spells a word by accident. IDs aren't checked against a list of words.

Why a version 4 UUID? Its 122 random bits come from the entropy boundary and nothing else. A time-ordered UUID (version 7) would read the wall clock every time it generated an ID, and every ID would reveal when it was created.

### Limits

LoonFS puts a limit on everything, so that the worst case is known in advance, and no input can make a process use unbounded memory or time.

- **Put a bound on every loop, queue, buffer, and set.** Every loop and queue has an upper bound defined as a constant, and every buffer or set that can grow is charged to a budget or capped by a named limit. Write the worst case (permits times the per-item cap) next to the limit. For example, `ExecutionBudget` defines the process-wide pools, `ReadWorkingMemory` and `ChargedSet` charge exact sets, and `BloomFilter` is the approximate, capped shape for a set that grows with the namespace during one pass.

- **Limit every retry with a count**, and with a deadline wherever a proof depends on one. For example, `with_transport_retry` retries at most ten times, within the provider deadline.

- **Don't use recursion in protocol code**, because the depth of a recursion depends on the input, and a stack overflow aborts the process. Recursion over a parsed tree (a regex or a TOML document) is allowed when the parser enforces a named depth limit.

- **Put each limit where its scope is:**
  - A process-wide resource budget goes in the runtime's `ExecutionBudget`, and core receives it by reference.
  - A bound on what a surface admits (request bytes, connections) goes in the host's config.
  - An extension (grep) defines its own budget.
  - A constant goes in `limits.rs` when more than one module uses it, or when a spec or a proof refers to it. Otherwise it stays next to the code that uses it, with a doc comment.

- **Keep fan-out constants separate from limits.** A fan-out constant sets how many requests one operation keeps in flight. Bound it by the budget's caps on operations, and write the product of the two next to it. Use one idiom for fan-out, a sliding window (`buffered`, `buffer_unordered`, `try_for_each_concurrent`), and one suffix for the constant, `*_IN_FLIGHT`. A sliding window starts a request whenever one finishes, while lockstep waves wait for each wave's slowest request.

- **Stream a listing instead of collecting it.** A pass that lists objects processes them as the listing arrives, and never keeps the whole listing in memory. Any state that it keeps across the listing has a bound that doesn't depend on the listing's length, because a listing grows with the namespace.

- **Cap whole-value reads by default** on every surface, including embedded use, because one large file could otherwise use all of the process's memory.

### Time

`clippy.toml` disallows ambient clocks, sleeps, intervals, timeouts, and `rand`, so that protocol code behaves the same way every time it runs with the same inputs.

- **Read the clock and entropy only through boundary functions.** Wall-clock time enters through named functions, each with a function-scoped allow whose reason has the form "<what boundary this is> so <why determinism survives>". `loonfs-core/src/time.rs::current_time_ms` is one of them. Write the reason in the attribute, `#[allow(clippy::disallowed_methods, reason = "...")]`, and use `#[expect]` where the lint is certain to fire, so that a stale allow warns. Production code has no file-scoped allow, because it would let every function in the file read the clock. Entropy goes through `ids::generated_id(prefix)`.

- **Make timers named configuration, not ambient sleeps.** For example, the publisher waits its configured interval between attempts to swap the namespace head.

- **Use the injected timer.** Core and extensions take the runtime's `MonotonicTimer`, and `StdMonotonicTimer::default()` appears only in builders.

- **Use a closed vocabulary for time fields and constants.** An instant ends in `_at_ms` (`now_ms`, for the current time, is the one exception). A span ends in a span word followed by `_ms`, and the span word is one of `interval`, `timeout`, `deadline`, `window`, `budget`, `lease`, `ttl`, `elapsed`, `age`, `margin`, `bound`, and `after`. `grace` qualifies a window (`GC_MIN_GRACE_WINDOW_MS`). A `Duration` keeps the span word and drops the unit.

Each span word has one meaning. These are the easiest to confuse:

| Span word | Meaning |
| --- | --- |
| `window` | A garbage collection policy span, compared against provider timestamps |
| `budget` | A limit that LoonFS enforces on itself, with the monotonic clock, on how long a publication can take |
| `timeout` | A bound on one attempt (`PROVIDER_ATTEMPT_TIMEOUT`) |
| `deadline` | A bound on the whole operation, including its retries (`PROVIDER_OPERATION_DEADLINE`) |
| `ttl` | On the wire, a lifetime that the caller declares (the server converts `ttl_ms` to an absolute `expires_at_ms`) |
| `margin` | An allowance added to another bound (`GC_SAFETY_MARGIN_MS`) |
| `bound` | A worst case derived from other limits, which nothing enforces directly (`READ_REVALIDATION_BOUND_MS`) |
| `after` | How long something stays idle before an action (`idle_fold_after_ms`) |

### Concurrency

This section describes where work can be spawned, and which locks, atomics, and channels to use.

- **Build concurrency from the operation's own futures** (`try_join`, `select!`, `buffer_unordered`), so that dropping the operation cancels its work. A future doesn't run until something polls it, so a stored future makes no progress on its own. Only the publisher registry and a host's scheduler spawn tasks.

- **Apply a result only if the attempt that produced it is still current**, so that a slow attempt can't overwrite the result of a newer one. For example, a compaction can't publish if a newer compactor has claimed the namespace since it started.

- **Use `#[async_trait]` only on a trait boundary with more than one implementation**, because it boxes every future it returns. Concrete types use native async functions.

Use these synchronization primitives:

| Primitive | Use it |
| --- | --- |
| `std::sync::Mutex` | By default. Unlock it before any `.await`, because a task suspended with it locked can deadlock another task on the same thread. |
| `tokio::sync::Mutex` | Only when a lock has to stay locked across an `.await`, with a comment that says why |
| `Relaxed` atomics | For statistics |
| `Acquire` and `Release` atomics | For flags, with a comment |
| `SeqCst` atomics | For everything else |
| `oneshot` | For a handoff from a submitter to a worker |
| `watch` | To report liveness to many observers |
| `mpsc` | Only when there really are several producers |

### Memory

- **Pass a body as a list of buffers or a stream.** Keep a whole value in one buffer only while a budget has a reservation for it. For example, `assemble` takes `Vec<Bytes>`, the local store stages its writes in a temporary file, and a long write streams. A trait's default method never buffers a whole value. Either a provider implements the streaming method, or the method has no default.

- **Test resource use, not just results.** A recording or buffer-watching store asserts that a write didn't happen, which kinds of request were made, and the size of the largest live buffer, because no functional test fails when a change adds a request or a large buffer.

## Performance

This section describes how to design for object storage, where the slowest resource is a round trip to the store. Design and measure in round trips first, then bytes, then CPU.

- **Make each decision with one round trip.** A record refers directly to everything it doesn't contain, so a reader never has to find something through a chain of earlier records (a "lookup by lineage"). For example, a revision delta includes the layout of the bytes it references. The one lookup by lineage is an append to the same chain, whose delta includes no layout, because the chain's existing layout already covers the earlier bytes.

- **Read what a provider supports from the store or the presigned URL issuer**, never from a constant in core, because providers differ. `checksum_algorithm` and `max_content_bytes` are the models, and provider multipart geometry is defined in the object-store crate.

- **Fail configuration instead of emulating a missing feature.** A provider that can't meet a contract fails configuration and reports why, and no fallback path stands in for the missing feature, because a fallback would change an operation's cost or guarantees without the operator knowing. A contract can still let the adapter choose its mechanism (one request or many), as `assemble` does, and that isn't a fallback.

- **Use what a listing returns, instead of making a request per object.** A pass that lists objects and acts on each one (garbage collection, for example) makes no per-object metadata request, which would cost a round trip per object. Object ages come from `ListedObject`. The times that LIST and HEAD report for one object can differ by less than a second, so grace windows have a minimum.

- **Batch requests.** The publisher batches commits into one WAL object, folds write extents through one assembly call, deletes run in a bounded window, and downloads keep a window of range responses open.

- **Keep the serialized path small.** Commits are serialized through one ordered WAL, so anything on that path slows every commit. Uploads, downloads, compaction, garbage collection, and indexing stay off it.

## Formats and the wire

`docs/specs/format.md` and `docs/specs/api.md` define the durable formats and the API. This section describes how the code expresses them.

- **Reject unknown fields everywhere except in wire responses.** Wire requests set `deny_unknown_fields`, every durable family rejects unknown fields (mutable and immutable alike), and config structs reject them. Wire responses accept them, so that an older client keeps working when the server adds a field.

- **Tag enums with `kind`, and write field names in `snake_case`.** Internally tagged enums use `tag = "kind"` by default, and a union can tag on its domain word where that reads better (`mode`, `status`, `outcome`, or `inode_kind`). Field names and enum values are `snake_case`. ID fields end in `_id`, and actor fields end in `_by` and are paired with an `_at_ms` field (`committed_by` and `committed_at_ms`).

- **Use `serde_bytes` for byte strings**, so that CBOR stores them as byte strings rather than arrays of integers.

- **Change a shape, its spec, and the spec-locked artifacts in the same pull request.** The spec-locked artifacts are the golden fixtures (`UPDATE_GOLDEN=1`), the spec-table sync tests, and `openapi.json`. They're how a reviewer sees a format change.

- **Follow the API grammar in `api.md`.** Handler names become `operationId`s, so a handler's name is its operation's name, with no `_handler` or `handle_`.

- **Write config files in `snake_case` throughout**, including store kinds.

## Developer experience

This section describes the conventions that make the code easier to read, change, test, and operate.

### Naming

Use each name with exactly one meaning, so that a search for a name finds every use of that meaning, and nothing else.

- **Use the glossary's terms exactly.** [`docs/specs/glossary.md`](specs/glossary.md) is the source of domain terms: namespace, head, seq, commit, WAL object, inode, direntry, binding, revision, manifest, checkpoint, tombstone, retention floor, change feed, layout, extent, span, and piece. Write "stored checksum", not "attestation". "Materialize" means writing a chain's pending pieces as its next extent.

- **Spell words out.** Write `Directory` rather than `Dir`, `request` rather than `req`, and `error` rather than `err`. The allowed short forms are glossary terms (`WAL`, `CAS`, `seq`, `gc`) and `_no` for a numbered ordinal (`revision_no`, `manifest_no`). A wire value that can't change keeps its short spelling (`"dir"`), with a serde rename and a comment. `err` and `e` are fine as closure parameters, because the rule applies to public signatures and type names.

- **Use each verb for one meaning**, from the table below, and don't introduce near-synonyms (`fetch`, `retrieve`, `remove`, `lookup`, `require`, `check`).

  | Verb | Meaning |
  | --- | --- |
  | `create` | Create a namespace or a directory |
  | `load` | Read durable state into memory |
  | `list` | Read a page |
  | `publish` | Make something durable and visible |
  | `apply` | Apply deltas onto state |
  | `materialize` | Write pending pieces into an extent |
  | `advance` | Move a monotonic floor forward |
  | `fold` | Move the WAL tail into segments |
  | `step` | Run one bounded maintenance pass |
  | `read` | Read bytes out of a file, at every Rust layer |
  | `get` | Call `ObjectStore`, or read one control object by ID |
  | `find_*` | Return an `Option` |
  | `validate` | Check the shape of input |
  | `verify` | Check bytes or durable state against stored evidence |
  | `ensure` | Check a precondition on state, returning `Result<()>` |
  | `resolve` | Turn a reference into what it refers to |
  | `effective_*` | Merge defaults |

- **Follow the public handle grammar** on `LoonFs`, `Namespace`, `ReadView`, `Maintenance`, and the Rust client:
  - A method doesn't repeat its receiver's noun, and accessors don't start with `get_`.
  - The verbs are `stat` (one entry's metadata), `list` (many entries, through one pager), `read` (bytes out), `put` (bytes in), `prepare` (stage content), `commit` (apply operations), and `get` (one control object by ID), plus `create`, `update`, `delete`, `append`, `move`, `copy`, `restore`, `undelete`, `fork`, and the maintenance verbs.
  - After the verb, the parts of a method name come in this order, and each part is optional: the object, the form (`_stream` or `_prepared`, and in-memory bytes have no suffix), the address (`_by_inode`, and a path has no suffix), the time (`_at_snapshot` or `_at_checkpoint`), and `_with_options`. For example, `read_file_stream_by_inode_with_options`.
  - The Rust client uses the handle's name wherever the runtime has the operation, and the HTTP name only where it doesn't. `api.md` has the mapping table.

- **Write each method in a plain form and an options form.** A required input is a positional parameter. Optional inputs go in one flat `*Options` struct, with public fields and `Default`, passed last. The plain form is a one-line call to its `_with_options` form, and there's no third shape (`foo_with_limit`, `foo_for_mode`). The one exception is an extra input that's a borrowed capability, such as a journal, which gets its own method, as in `put_file_stream_resumable`. One options type serves a base operation and its stream, by-inode, and time forms, and a separate by-inode type exists only when the fields differ.

- **Follow Rust's naming for constructors.** A constructor is `new` or `with_*` (`Vec::with_capacity`, `Client::with_transport`), a conversion is `from_*`, and a builder setter is the bare field name, takes `impl Into<T>`, and returns `Self`.

- **Use each type suffix for one meaning:**

  | Suffix | Meaning | Example |
  | --- | --- | --- |
  | `Config` | Construction-time configuration, deserialized (strict) or built in code | `ServerConfig`, `ProviderObjectStoreConfig` |
  | `Policy` | A rule set an engine applies | `MetadataRunPolicy` |
  | `Options` | Per-operation caller knobs, flat, with public fields and `Default` | `PutFileOptions` |
  | `Builder` | A multi-step constructor ending in `build()` | `LoonFsBuilder` |
  | `Request`, `Response` | Wire shapes for one operation | `CommitRequest` |
  | `Record` | One durable entry of one family | `RevisionRecord` |
  | `Row` | The durable union over families | `MetadataRow` |
  | `Payload` | The body of one durable object | `WalObjectPayload` |
  | `View` | A read-only projection over state | `MetadataView` |
  | `Session` | A stateful multi-call context that changes state | `MetadataViewSession` |
  | `Status` | A lifecycle enum, whether durable, wire, or in-memory | `NamespaceStatus`, `WriterSessionStatus` |
  | `State` | A struct of current values | `MetadataState` |
  | `Kind` | Which shape a value has (it can include data) | `InodeKind` |
  | `Outcome` | An enum of how one step ended | `CompactionStepOutcome` |
  | `Report` | A struct of what one pass did | `GrepGcReport` |
  | `Error` | An error enum | `CoreError` |
  | `Id` | An identifier newtype | `NamespaceId` |

  A Rust name that mirrors a serialized field, a label, or a span keeps that spelling until the format changes, as `DirentryBindingState`, `KeyClass`, and `payload_class` do. Other type names don't use `Class`, `Classification`, `Category`, `Verdict`, or `Step`, and there are no `*Result` structs, because `Result` is each crate's alias.

- **Put the bound at the start of a name and the unit at the end.** A byte quantity ends in `_bytes`. A count ends in the plural noun it counts (`_rows`, `_objects`), and `_count` is for a computed tally. `len` is used for the std method and nothing else. A bound is written `<scope>_max_<noun>_<unit>` (`max_content_bytes`, `AWS_S3_MAX_DIRECT_PUT_BYTES`). Each quantity has one unit and one name.

- **Make a helper's name say what it does**, such as `push_revision` and `push_revision_record`.

- **Match a file's name to the type or subsystem it contains**, and keep each name to one meaning across the workspace. A type name can be reused in two crates with different fields only when the two surfaces differ, and the doc says why.

Why a closed set of verbs? Naming drift usually doesn't come from carelessness. It comes from a second verb for a meaning that already has one, such as `get_file` beside `read_file_stream`, or `remove_tail` beside `invalidate_anchor`. A list of what each verb means lets a reviewer catch the second verb.

### Code shape

This section describes where code goes, and how large a module or a function can be.

- **Define a type in the lowest crate whose vocabulary it belongs to**, and re-export it from higher crates instead of defining a copy.

- **Implement each rule, constant, mapping, and helper once**, in one place that every caller uses, because when a rule has two copies, a change to one can miss the other. The two deliberate duplications are the model oracle and grep.

- **Put only docs and re-exports in `mod.rs`.** Implementation goes in named sibling files, so a file's name says what's in it. The exceptions are the two routers (`http/mod.rs` in `loonfs-http` and in `loonfs-server`) and the CLI's `commands/mod.rs` dispatcher.

- **Keep a module under about 500 non-test lines, and a function under about 100.** When a file grows past that, split it by responsibility. A function has few parameters, a simple return type, and substantial logic inside. Move branches (`if`) up into the parent function, and loops (`for`) down into helpers, so that the parent keeps the state, and the helpers compute results without applying them.

- **Keep public APIs small.** Engine and runtime crates use `pub(crate)` freely. The vocabulary crates keep their public API small, and surface crates re-export nothing internal. Harness hooks are public and documented as hooks, never `#[doc(hidden)]`.

- **Derive the standard set for each kind of type.** Wire responses derive `Debug, Clone, PartialEq, Eq, Serialize, Deserialize` and accept unknown fields. Wire requests add `deny_unknown_fields`. Durable records derive the same set and reject unknown fields. Options types derive `Debug, Clone, Default` and have public fields.

- **Start every module file with a `//!` line that says what belongs in it.** A crate root also says what doesn't belong.

### Comments and docs

- **Use comments to explain why the code is written the way it is.** Write them in full sentences. A comment explains a surprising invariant, a deliberate departure from a rule, or a safety argument. It doesn't narrate what the next line does, repeat a doc, mark a section, point to another place in the code, or record a follow-up, because those comments go stale as the code changes. For example: "Treat a poisoned lock as fatal because another thread panicked while updating the session state."

- **Write the first paragraph of a rustdoc as one sentence.** A function's doc starts with a verb in the third person ("Lists the children of a directory path."), and the doc of a type, field, or variant starts with a noun phrase. Put every identifier in backticks, and describe preconditions and failure modes in prose. Durable and API behavior is defined in `docs/specs/`, so a doc links to the spec instead of restating it. The docs on `ToSchema` types become the OpenAPI descriptions, so their wording and paragraph breaks are part of `openapi.json`, and editing one is an API change. `loonfs`, `loonfs-client`, `loonfs-types`, and `loonfs-objectstore` enforce `missing_docs`.

- **Include the reason in every allow attribute**, in `reason = "..."`.

### Tests

- **Test a contract, an invariant, or a bug fix.** Don't write a separate test for each case where a loop or a table covers the cases, a test that repeats a sibling, or a test that only checks construction. When a change needs proof, strengthen an existing test before you add one, because every test has to be read and maintained.

- **Use the smallest tier that can test the behavior:**

  | Tier | Use it for |
  | --- | --- |
  | Inline `#[cfg(test)]` modules, next to the code | Private internals |
  | `<area>/tests/<topic>.rs` | A suite that's too large to be inline |
  | One integration binary per crate, under `tests/it/` | Flows across modules |
  | Golden fixtures | Byte-exact formats |
  | Spec sync tests | The tables in the specs |
  | The differential suite | Comparing core against the model oracle |
  | Live provider conformance, behind `LOONFS_TEST_*` | Behavior against real providers |

- **Write test names as declarative sentences** in snake_case, with no `test_` prefix, such as `stale_revision_precondition_is_rejected`.

- **Check an error with one of three idioms:**
  - `assert_matches!(result, Err(X { .. }))` when the value implements `Debug`.
  - `assert!(matches!(..))` when it doesn't, because `assert_matches!` couldn't show anything more.
  - `expect_err("...")` followed by a check of the specific error, when the code under test returns one.

  Don't use `assert!(result.is_err())`, because when it fails, it doesn't show which error the code returned.

- **Put shared helpers in `loonfs-test-support`.** A helper used in two files moves there, and extend an existing store wrapper before you write a new one. `loonfs-test-support` depends only on `loonfs-types` and `loonfs-objectstore`, and the higher crates use it as a development dependency, so a helper that needs a higher crate's own types stays in that crate. Core's `test_support` module is an example.

- **Keep a test under about 100 lines.** Split a longer test by scenario.

- **Use bare `#[tokio::test]` by default**, and `multi_thread` only where a harness needs it, with one comment per harness module that says why. Use `start_paused` for timer logic.

- **Allow `clippy::panic` in test code through `clippy.toml`**, not file by file. The root of each integration test binary keeps one `clippy::panic` allow, with a reason, because `allow-panic-in-tests` doesn't reach a helper function in an integration binary.

### Observability

- **Open a root span for every public handle operation**, called `loonfs.<op>`, with an `operation` field equal to the method name. Internal stages are `loonfs.phase` spans, with a `phase` from a closed enum that `phase_span!` requires. Don't keep an `.entered()` guard across an `.await`, because the span would stay entered while the task is suspended, and it would collect other tasks' events.

- **Record IDs and values as fields, not in messages.** `namespace_id`, `seq`, `commit_id`, `object_key`, `store_kind`, `elapsed_ms`, and `error` are recorded as fields, each with one spelling (`namespace_id = %namespace_id`, `error = %error`), so a search on the field finds every event. Don't put an ID in a metric label, because each new ID would create a new metric series.

- **Use the log levels for one purpose each.** The default log filter is one directive, `loonfs=info`, which covers every LoonFS crate, because a directive matches every target that starts with it.

  | Level | Use it for |
  | --- | --- |
  | `error!` | The place where a durability failure starts, with the namespace and the key |
  | `warn!` | Retries and races |
  | `info!` | One event per lifecycle event or batch |
  | `debug!` | One event per item |

  Don't log a failure that repeats, such as a retry, on every attempt. Log the first one, then at most one per interval, with a `suppressed` count and the time since the last one logged (`since_ms`), and log the recovery when the failure stops.

- **Write CLI command output to stdout and diagnostics to stderr.** Output goes through `render`, and diagnostics go through tracing, so a script can read the output without filtering out log lines. The two exceptions are the byte stream of `cat` and the progress line.

### Tooling and dependencies

- **Configure lints once**, in the workspace `[lints]` table, `clippy.toml`, and the default rustfmt settings. Pin the toolchain to an exact version, and update it in its own change, so a new Rust release can't break CI on a change that didn't touch the code. The MSRV is the pinned toolchain.

- **Use one crate for each job.** Don't add a crate that does the same job as one already in use, and replace a dependency that fifty lines of code could replace. In the workspace table, explain a dependency in a comment when its name doesn't say why it's there, such as a pinned version or a feature it turns on. `cargo deny check bans` runs in CI over the release targets, and each duplicate version that it allows has a `skip` entry that names the crates that pull it in.

- **Write scripts in POSIX sh.** Put any Python that a script needs in its own file under `scripts/`, and call it with arguments, instead of embedding it in the shell script.

- **Run the CI checks before every push**, from the repository root: fmt, clippy with `-D warnings`, `cargo deny check bans`, and the test suites. Run them again after a rebase or a merge, because a rebase or merge that applies without conflicts can still break the build or the tests.

### Git

- **Write a commit message as one line, `type(scope): summary`**, with the summary in lowercase, starting with a verb, such as `fix(cli): rewrite the command help text`.

