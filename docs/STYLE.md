# LoonFS style

This guide explains how we write LoonFS code and why, so that every part of the codebase follows the same conventions.

The specs in `docs/specs/` define the durable formats and the API, and they take precedence over this guide.

## Goals

LoonFS code serves four goals, in this order of priority: correctness, simplicity, performance, and developer experience. When you're deciding how to write something, choose the option that makes the code more correct, simpler, faster, or easier to work in, judged in that order.

Three rules apply everywhere:

- **Solve each kind of problem the same way across the codebase**, so that a developer who has seen the pattern once recognizes it everywhere. A convention applies to new code and to code that a change already touches, but an existing departure isn't, on its own, a reason for a change.

- **Remove the old design in the same change that adds a new one.** A compatibility layer retains conflicting design decisions, and every developer has to learn both without an established preference.

- **Don't ship shortcuts.** A deliberate shortcut still has to be rewritten later, and by then other code usually depends on it.

## Simplicity

- **Design all types so that an invalid state can't be represented**, instead of checking for invalid states at runtime. A runtime check has to run everywhere the state is created, and a missed check is a bug.

- **Fix the root cause of a bug, not the symptom.** A check that catches the symptom leaves the cause available to occur again.

- **Estimate the back-of-the-envelope costs across scales before you commit to a design.** Estimate the round trips, bytes, and states that the design needs, up to the largest namespace the limits allow. Start with round trips, because one GET to object storage takes tens of milliseconds.

## Correctness

### Failures

Three kinds of failure can happen, and the code responds to each kind differently:

**A bug in the process.** The process reached a state that the code assumes can't happen, so the code panics.

**Bad bytes in object storage.** An object that LoonFS read breaks a rule of the format, so the code returns a corruption error scoped to the namespace that contains the object.

**An expected error.** Nothing is broken, but the request is invalid (a path doesn't exist, or a precondition fails) or can't succeed right now (a conflict, or a timeout). The code returns its assigned error, and the caller can decide what to do.

- **Check each assumption with `assert!`, or with `expect` and a message that says what should be true.** Correctness comes first, so when an assumption breaks, the process stops instead of continuing in a state the code wasn't written for. Keep the checks on in release builds.

- **Don't use `unsafe`, `unwrap`, `panic!`, `todo!`, `unimplemented!`, or `unreachable!` in production code.** The lints reject them, so every panic is an assertion that states an invariant.

- **Return the error for a terminal state, without retry or recovery code.** A fenced writer, a corrupt namespace, and a permission failure can't change on a retry. Retry a transient transport failure, with a count and a deadline.

- **Catch a panic only where the code is designed to continue after one, and test what happens next.** When background work panics, LoonFS records the panic in a metric for operators and keeps the writer session open. Ending the session wouldn't help, because the same bug would panic again in a new session.

- **Treat a poisoned lock as a bug**, with `expect`, unless the component contains panics by design. (A mutex is poisoned when a thread panics with the mutex locked.)

- **Don't hold a `std::sync::Mutex` across a call into a trait object, a callback, or the metrics recorder.** Code that the lock's owner doesn't control might panic, which poisons the lock, or take the same lock, which deadlocks.

- **Never cap a decoded value below the limit the writer enforces**, because then LoonFS could write an object that it can't read back.

Why does a bug panic instead of returning an error? If the code kept going after it found a bug in its own state, it could corrupt durable state in object storage, because it can't trust anything else it computes. Tokio contains a panic to the task that panicked, so a panic doesn't always stop the process, and that's why each place that catches a panic has a test.

Why doesn't LoonFS panic on bad bytes too? The durable state is stored in shared object storage, so a restarted process would load the same object and panic again, and so would every other process that opens the namespace. An error scoped to the namespace reports the problem, and the process keeps serving its other namespaces.

### Errors

- **Keep one registry of wire error codes**, `ErrorCode` in `loonfs-types`. Clients match on the strings, so a code can't be renamed or removed after a release includes it. Group codes with `ErrorCode::kind()`, and derive the HTTP status from the kind in one function.

- **Add a `code()` method to every error that can reach the wire.** Errors from the vocabulary and storage crates don't have one, and their callers map each error to a code.

- **Define one error type for each crate, and match its name to the crate**, such as `CoreError` in `loonfs-core`. Each crate exports `pub type Result<T>`. A vocabulary crate whose types every other crate uses, such as `loonfs-types`, has no crate error type or `Result` alias, and it defines each error enum next to the error-generating code.

- **Use `thiserror` the same way everywhere.** Use `#[from]` when an inner type maps to exactly one variant, match variant names to the wire codes, include `object_key` in store and codec errors, and start every message with a prefix instead of a bare `#[error("{0}")]`. Repeat the cause in the message when it helps, because the server doesn't print source chains.

- **Mark the public error enums and the registry's enums in a published crate `#[non_exhaustive]`**, so a variant can be added without breaking callers. Keep other public enums exhaustive, so the compiler finds every `match` that a new variant affects. A wire enum that a client decodes ends in an `Unknown` variant, so a newer server can't break an older client.

- **Define an error enum in the same file where the error-generating code is written.** When several files in a module generate the same error, define the enum in the module's `error.rs`.

- **Derive `Clone` on core errors**, so one failure can be returned to every caller waiting on the same batch.

- **Write error messages in lowercase**, with identifiers in backticks and no final period. Format a rejected input with `Debug`, which quotes and escapes it.

### State and types

- **Represent a lifecycle, a mode, or an outcome as one enum**, where each variant contains only its own fields. A set of booleans, a bool next to an `Option`, or a sentinel value (such as `u64::MAX`) allows combinations that can't exist.

- **Pass a choice across a module boundary as a two-variant enum, not a bool**, so the meaning of the argument is clear at the call site. A private helper can take a bool, and so can a field whose name says what `true` means. Keep two independent settings as two fields, not one enum of their combinations, when most code reads only one of them.

- **Make read-only and writable handles different types**, so that calling a write method on a read-only handle doesn't compile.

- **Put a durable status's transition rules on its enum**, so the rule for legal moves is recorded in a single place.

- **Group optional response fields that go together** with `#[serde(flatten)] Option<Group>`, which doesn't change the JSON.

- **Share a type whose size grows with the namespace through `Arc`**, and don't derive `Clone` on it, because a stray `.clone()` would copy a large amount of memory. Keep a `Weak` or an ID in a structure that refers to a value another structure owns (a back-pointer, an index, a memo, or an ordering), because an `Arc` there would keep the value alive after its owner drops it, and use an `Arc` in such a structure only with a comment that says why.

- **Return a named struct, not a tuple, from a function that's visible outside its module**, because a tuple's fields have no names.

### Widths and units

- **Use `u64` on the wire, in the store, and for byte sizes, offsets, and sequences.** Use `u32` for positions inside one object, and `usize` for in-memory budgets and counts. Never put a `usize` in a wire, durable, or sidecar shape, because its width depends on the platform.

- **Narrow integers with `try_from` or `usize_from_u64`, not `as`**, because `as` truncates without saying so.

- **Define every ID, sequence, and validated string as a newtype** with `string_id!` or `numeric_id!`, so one kind of ID can't be passed as another.

- **Generate a random ID through `ids::generated_id`**, as a prefix, an underscore, and the 26-character lowercase Crockford base 32 encoding of a version 4 UUID, such as `con_3s0drmj8em84yb4mnetqkz2j0x`. To encode the UUID, add two zero bits in front of its 128 bits and write each group of 5 bits as one character of `0123456789abcdefghjkmnpqrstvwxyz`. A parser rejects uppercase letters and a first character greater than `7`, so each UUID has exactly one spelling.
  An inode ID isn't a UUID, and its high 64 bits are the creating commit's sequence and the inode's create number, so the inodes that one commit creates have neighboring row keys, and its low 64 bits are random.

- **Parse an ID where the error can identify the input**, such as the CLI flag or the query parameter.

- **Take paths as `&str` in the embedded handles**, and report an invalid path as the operation's own error.

- **Represent a time as a `u64` number of milliseconds, with the unit in the name (`_ms`).** Use a `Duration` only to pass a time to a library function that expects a `Duration`.

Why this ID alphabet? It leaves out `i`, `l`, and `o`, which are easy to confuse with `1` and `0`, and `u`, which makes it less likely that an ID spells a word by accident. Generating an ID doesn't read the clock, so an ID doesn't reveal when it was created.

### Limits

LoonFS puts a limit on everything, so that the worst case is known in advance, and no input can make a process use unbounded memory or time.

- **Put a bound on every loop, queue, buffer, and set**, and write the worst case next to the limit.

- **Limit every retry with a count**, and with a deadline wherever a proof depends on one.

- **Don't use recursion in protocol code**, because a stack overflow aborts the process. A parser can recurse when it limits the depth.

- **Define a limit next to the applicable code.** A process-wide budget goes in `ExecutionBudget`, a limit on incoming requests goes in the host's config, and an extension defines its own budget. A constant shared by more than one module, a spec, or a proof goes in `limits.rs`.

- **Tie related limits together with `const` assertions**, so that changing one without the other fails to compile.

- **Set fan-out with a `*_IN_FLIGHT` constant and a sliding window** (`buffered` or `buffer_unordered`), bounded by the budget. A sliding window starts a request whenever one finishes, while lockstep waves wait for each wave's slowest request.

- **Stream a listing instead of collecting it**, and keep any state across the listing bounded by something other than the listing's length, because a listing grows with the namespace.

- **Cap whole-value reads by default** on every surface, because one large file could otherwise use all of the process's memory.

### Time

- **Read the clock and entropy only through boundary functions.** `clippy.toml` disallows ambient clocks, sleeps, timeouts, and `rand`, so that protocol code behaves the same way every time it runs with the same inputs. Each boundary function has a function-scoped `allow` with a reason.

- **Use the injected `MonotonicTimer`, and make every timer a configured interval** instead of an ambient sleep.

- **Use a closed vocabulary for time names.** An instant ends in `_at_ms`, except the current instant, which is `now_ms`. A span ends in a span word and `_ms`, and the span word is one of `interval`, `timeout`, `deadline`, `window`, `budget`, `lease`, `ttl`, `elapsed`, `age`, `since`, `staleness`, `backoff`, `margin`, `bound`, and `after`. A `Duration` keeps the span word and drops `_ms`.

These span words are the easiest to confuse:

| Span word | Meaning |
| --- | --- |
| `timeout` | A bound on one attempt |
| `deadline` | A bound on a whole operation, including its retries |
| `window` | A garbage collection policy span, compared against provider timestamps |
| `budget` | A limit that LoonFS enforces on itself with the monotonic clock |
| `ttl` | A lifetime that a caller declares on the wire |
| `margin` | An allowance added to another bound |
| `bound` | A worst case derived from other limits, which nothing enforces directly |
| `after` | How long something stays idle before an action |

### Concurrency

- **Build concurrency from the operation's own futures** (`try_join`, `select!`, `buffer_unordered`), so that dropping the operation cancels its work. Core, HTTP handlers, the CLI, and extensions don't spawn tasks.

- **Spawn a task only from the publisher registry or a host's scheduler**, on the handle's runtime, and register it so that `LoonFs::drain` can wait for it.

- **Apply a result only if the attempt that produced it is still current**, so that a slow attempt can't overwrite a newer one.

- **Use `#[async_trait]` only on a trait with more than one implementation**, because every call then allocates its future on the heap, and a native async function doesn't.

- **Use `std::sync::Mutex` by default, and unlock it before any `.await`**, because a task suspended with it locked can deadlock another task on the same thread. Use `tokio::sync::Mutex` only when a lock has to stay locked across an `.await`, with a comment that says why.

- **Use `Relaxed` atomics for statistics, `Acquire` and `Release` for flags with a comment, and `SeqCst` for everything else.** Use `oneshot` for a handoff, `watch` to report liveness to many observers, and `mpsc` when there are several producers.

### Memory

- **Pass a body as a list of buffers or a stream.** Keep a whole value in one buffer only while a budget has a reservation for it, and don't write a trait default method that buffers a whole value.

- **Test resource use, not just results**, with a store that records requests and buffer sizes, because no functional test fails when a change adds a request or a large buffer.

## Performance

Design and measure in round trips to object storage first, then bytes, then CPU.

- **Make each decision with one round trip.** When a record depends on data stored somewhere else, include a direct reference to that data, so that a reader doesn't have to look back through earlier records. For example, a file revision includes the layout of its bytes.

- **Don't hard-code a provider's capabilities in core**, such as its checksum algorithm or its largest object size. Get them from the provider's `ObjectStore` implementation, because providers differ.

- **Fail configuration instead of emulating a missing provider feature**, because a fallback would change an operation's cost or guarantees without the operator knowing.

- **Batch requests where possible**, such as commits into one WAL object and deletes into a bounded window.

- **Keep the commit path small.** Commits are serialized through one ordered WAL, so anything on that path slows every commit.

## Formats and the wire

`docs/specs/format.md` and `docs/specs/api.md` define the durable formats and the API. These rules describe how the code expresses them.

- **Reject unknown fields everywhere except in wire responses.** Wire requests, durable families, and config structs reject them. Wire responses accept them, so that an older client keeps working when the server adds a field. The one exception is a type that's both wire and durable (`ContentRef`, `Checksum`, `ContentToken`, `ContentRefKind`, `ChecksumAlgorithm`, and `AccessRight`), which rejects unknown fields and has no `Unknown` variant, because LoonFS rejects an unknown value in durable state.

- **Tag enums with `kind`, or with a domain word such as `status` where that reads better, and write field names in `snake_case`.** ID fields end in `_id`, and actor fields end in `_by` and are paired with an `_at_ms` field.

- **Use `serde_bytes` for byte strings**, so that CBOR stores them as byte strings rather than arrays of integers.

- **Change a shape, its spec, and the spec-locked artifacts in the same pull request.** The artifacts are the golden fixtures (`UPDATE_GOLDEN=1`), the spec-table sync tests, and `openapi.json`, and they're how a reviewer sees a format change.

- **Use the operation ID as a handler's function name**, with no `_handler` or `handle_`, because handler names become `operationId`s.

- **Write config files in `snake_case` throughout**, including store kinds.

## Developer experience

### Naming

Use each name with exactly one meaning, so that a search for a name finds every use of that meaning, and nothing else.

- **Use the terms in [`docs/specs/glossary.md`](specs/glossary.md) exactly.**

- **Spell words out in public signatures and type names**, such as `Directory` rather than `Dir`. The allowed short forms are glossary terms (`WAL`, `CAS`, `seq`, `gc`) and `_no` for a numbered ordinal.

- **Use each verb for one meaning**, from this table, and don't add near-synonyms such as `fetch`, `retrieve`, or `lookup`:

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
  | `resolve` | Turn a reference into the referenced object |
  | `effective_*` | Merge defaults |

- **Order the parts of a handle method's name** as the verb, the object, the form (`_stream` or `_prepared`), the address (`_by_inode`), the time (`_at_snapshot`), and `_with_options`, and leave out any part that doesn't apply, as in `read_file_stream_by_inode_with_options`. A method doesn't repeat its receiver's noun, and an accessor doesn't start with `get_`.

- **Write each method in a plain form and a `_with_options` form.** Required inputs are positional, and optional inputs go in one flat `*Options` struct with public fields and `Default`, which every form of the operation shares. Don't add a third shape, such as `foo_with_limit`.

- **Follow Rust's naming for constructors and conversions.** A constructor is `new`, `with_*`, `parse` when it builds a value from text, or the name of the variant or input it starts from (as in `Checksum::crc32c`). A conversion is `from_*`, and a builder setter is the bare field name.

- **Use each type suffix for one meaning:**

  | Suffix | Meaning |
  | --- | --- |
  | `Config` | Construction-time configuration |
  | `Policy` | A rule set that an engine applies |
  | `Options` | Per-operation caller settings, flat, with public fields and `Default` |
  | `Builder` | A multi-step constructor that ends in `build()` |
  | `Request`, `Response` | The wire shapes for one operation |
  | `Record` | One durable entry of one family |
  | `Row` | The durable union over families |
  | `Payload` | The body of one durable object |
  | `View` | A read-only projection over state |
  | `Session` | A stateful multi-call context that changes state |
  | `Status` | A lifecycle enum |
  | `State` | A struct of current values |
  | `Kind` | Which shape a value has |
  | `Outcome` | An enum of how one step ended |
  | `Report` | A struct of what one pass did |
  | `Error` | An error enum |
  | `Id` | An identifier newtype |

  Don't use `Class`, `Category`, `Verdict`, or `Step` in a new type name, and don't define a `*Result` struct, because `Result` is each crate's alias. A name that mirrors a serialized field keeps the field's spelling.

- **Put the bound at the start of a name and the unit at the end**, as in `max_content_bytes`. A count ends in the plural noun it counts, such as `_rows`, and `_count` is for a computed tally.

- **Match a file's name to the type or subsystem it contains, and a helper's name to what the helper does.**

### Code shape

- **Define each type once, in the lowest crate where it's needed**, and re-export it from the crates above.

- **Implement each rule, constant, mapping, and helper only once**, because when a rule has two copies, a change to one can miss the other. The model oracle (`loonfs-model`) and grep keep their own copies on purpose. The oracle never shares code with core, because an oracle that shared core's code would share its bugs.

- **Don't put code in `mod.rs`.** Every module has one, so the file name doesn't tell a developer what's inside. Put code in a file whose name describes its contents, and use `mod.rs` for docs and re-exports.

- **Keep a module under about 500 non-test lines, and a function under about 100.** Move branches up into the parent function and loops down into helpers, so that the parent keeps the state and the helpers compute results.

- **Keep public APIs small.** Use `pub(crate)` freely in engine and runtime crates, and document a harness hook as a hook instead of hiding it with `#[doc(hidden)]`.

- **Derive the standard set for each kind of type.** Wire and durable types derive `Debug, Clone, PartialEq, Eq, Serialize, Deserialize`, and options types derive `Debug, Clone, Default`.

- **Start every module file with a `//!` line that says what belongs in it.**

### Comments and docs

- **Write a comment only when the code can't explain itself**, such as a surprising invariant, a deliberate departure from a rule, or a safety argument. Write it in full sentences, and say why, not what. Don't narrate the next line, mark a section, or record a follow-up, because those comments go stale.

- **Write the first paragraph of the rustdoc on a module, type, or function as one sentence.** A function's doc starts with a verb in the third person ("Lists the children of a directory path."), and a type's doc starts with a noun phrase. A field or variant doc is one short paragraph of up to a few sentences. Link to the spec instead of restating durable or API behavior. A `ToSchema` doc is part of `openapi.json`, so editing one is an API change.

- **Include a reason in every `allow` attribute.**

### Tests

- **Test a contract, an invariant, or a bug fix.** Strengthen an existing test before you add one, and cover many cases with a loop or a table instead of one test each, because every test has to be read and maintained.

- **Use the smallest kind of test that can check the behavior.** Use an inline `#[cfg(test)]` module for private internals, the crate's integration binary under `tests/it/` for flows across modules, golden fixtures for byte formats, the differential suite to compare core with the model oracle, and live provider conformance (behind `LOONFS_TEST_*`) for real providers.

- **Write test names as declarative sentences** in snake_case, with no `test_` prefix, such as `stale_revision_precondition_is_rejected`.

- **Check an error with `assert_matches!`**, with `assert!(matches!(..))` when the value isn't `Debug`, or with `expect_err` followed by a check of the specific error. Don't use `assert!(result.is_err())`, because when it fails, it doesn't show which error the code returned.

- **Put shared test helpers in `loonfs-test-support`**, except a helper that needs types from a higher crate, which stays in that crate.

- **Keep a test under about 100 lines**, and split a longer one by scenario.

- **Use bare `#[tokio::test]` by default**, `multi_thread` only where a harness requires multiple threads (with a comment that says why), and `start_paused` for timer logic.

### Observability

- **Open a `loonfs.<op>` root span for every public handle operation**, and record internal stages as `loonfs.phase` spans from the closed enum that `phase_span!` requires. Don't keep an `.entered()` guard across an `.await`, because the span would collect other tasks' events.

- **Record IDs and values as fields, not in messages**, so that a search on the field finds every event. Don't put an ID in a metric label, because each new ID creates a new metric series.

- **Log at `error!` where a durability failure starts, at `warn!` for retries and races, at `info!` once per lifecycle event or batch, and at `debug!` for each item.**

- **Log a repeating failure once, not on every attempt.** Log the first one, then at most one per interval with a `suppressed` count and `since_ms`, and log the recovery.

- **Write CLI command output to stdout and diagnostics to stderr**, so that a script can read the output without filtering out log lines.

### Tooling and dependencies

- **Configure lints once**, in the workspace `[lints]` table and `clippy.toml`, and use the default rustfmt settings. Pin Rust to an exact version in `rust-toolchain.toml`, and update the pin in its own pull request.

- **Use one crate for each job**, and replace a dependency that fifty lines of code could replace. Explain a dependency in a comment in the workspace table when its name doesn't say why it's there. `cargo deny check bans` allows a duplicate version only with a `skip` entry that says which crates pull it in.

- **Write scripts in POSIX sh**, and put any Python that a script needs in its own file under `scripts/`.

- **Run the CI checks before every push**, and again after a rebase or a merge, because a change that applies without conflicts can still break the build.

### Git

- **Write a commit message as one line, `type(scope): summary`**, with a lowercase summary that starts with a verb.
