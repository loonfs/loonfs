# The WAL Boundary

The write-ahead log of a namespace is a sequence of numbered, immutable
objects under one prefix. A data segment carries the commits of one
publication batch; a fence carries none and advances the number and the
writer epoch. Discovery probes numbers forward from a hint, replay walks
a bounded range of them, and reclamation deletes the ones below the
folded and retention floors. This document records which module owns
those operations, why there is no trait behind it, and what a second
implementation of the log would have to answer before one could exist.

## One owner

`loonfs_core::wal` owns every operation that touches a numbered WAL
object. Nothing outside it builds a numbered key, encodes or decodes a
segment envelope, or decides what an absent successor means. The module
is split by responsibility, the same division SlateDB's pluggable-WAL
design draws, as files rather than traits:

```
frame.rs     segment and tail types, their errors
writer.rs    assemble a segment: a batch of accepted commits, or a fence
publish.rs   the numbered put and its outcome classification
discover.rs  tip discovery from the hinted or folded number; the probe
             that advances a cached view by one segment at a time
reader.rs    bounded loading of the segments between two heads
replay.rs    replay of a loaded tail onto metadata state
reclaim.rs   which numbers retention still requires
```

The callers speak in heads. The batch planner hands the segment builder
the head it extends and receives the segment and the resulting head. The
epoch acquisition asks for a fence at the current head. A reader asks for
the tail between a base head and the current head and receives the
replayed state. Garbage collection asks which number retention requires
and whether a key is above it. No caller names a WAL number to load or
publish.

The workspace's `clippy.toml` enforces the boundary the way it enforces
the clock boundary: the numbered-key builder is a disallowed method, and
only the files under `wal/` carry the allow. The envelope codecs are not
banned; they are vocabulary, tested where they are defined, and a test
that decodes a published segment to assert its shape is reading the
format, not operating the log.

## What stays outside

The hint object carries a manifest number and a WAL number in one
document. Manifest discovery reads the first; WAL discovery reads the
second, through the loaded manifest. Raising it is a compare-and-swap on
a control object, so it stays in the control module, and the runtime
paces the raises. A second log implementation would still need the
manifest half.

Family enumeration for garbage collection is key-layout vocabulary shared
by every family. The sweep and the family list stay in `gc/`; only the
rule for which WAL numbers are still required moved.

Positions are not opaque, and this change does not pretend they are. The
manifest records the folded and the retention-floor WAL numbers. The fold
trigger, the unfolded-segment diagnostic, and the write-stop bound are
differences between WAL numbers. That arithmetic stays where it was.

## Why no trait

A trait with one implementation and a test double gets its signatures
from guesses, and the guesses that matter here are exactly the ones a
hosted log would settle differently: what a position is when the
manifest still records WAL numbers, whether a fence is atomic with epoch
acquisition, and what the head of a log that is not a numbered prefix
means. Cutting the interface later, with both implementations in hand,
is a mechanical extraction from this module; cutting it now would fix
those answers before anyone knows them.

## What a second implementation has to answer

- **Position.** `last_folded_wal_no` and `retention_floor_wal_no` are
  durable manifest fields and `wal_no` is part of the read head. A log
  whose positions are not dense numbers needs either a mapping the
  manifest can store or a format change to those fields.
- **Fence and epoch.** Today the manifest carries the epoch and the fence
  is a numbered object created conditionally at the tip; the stale
  writer's put collides. A remote log has to make its fence atomic with
  the epoch or revalidate the manifest before serving.
- **Head and discovery.** The tip is discovered by probing numbers until
  one is absent, starting from the hint. A hosted log needs its own
  answer to "what is the committed tail", and the hint's WAL half becomes
  that log's concern or disappears.
- **Collection.** Reclamation is authorized by the folded and floor
  numbers with an age grace. A hosted log trims by its own positions and
  must keep its fence and high-water state after trimming.
- **Binding.** A namespace must record which log holds its unmaterialized
  history, immutably, so that an unsupported binary refuses it and no
  binary opens an empty log in place of missing acknowledged commits.

## Follow-ups

- Discovery, the incremental probe, and bounded loading are three walks
  over consecutive numbers that differ only in what an absent successor
  means and in error type. A shared primitive is possible if it needs no
  flag parameter.
- The numbered put is the one durable write that does not use the
  `CasAttempt` and `WriteEvidence` vocabulary the other conditional
  writes use; its retry lives in the commit engine. Aligning it would
  make the unknown-outcome path read like the manifest's.
- The unfolded-segment count is computed two ways: as a difference of
  numbers and as a count of publications. They agree today because the
  numbers are dense.
