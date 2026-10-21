# Read freshness

This note describes where a process keeps a namespace's current [head](../specs/glossary.md#head), how a read uses it, and how recent the state that a read sees is. Each process keeps one view of a namespace, one procedure refreshes that view, and a read uses the view as it is unless the read asks for a confirmed head. A read sends no store request if it's `eventual` and its view was confirmed within the read's maximum staleness. Otherwise, it costs one refresh, or one HEAD in the writer's process. (In this note, a process means one LoonFS runtime. Two runtimes in one process keep separate views, even when they share a metadata cache.)

## The problem

A read starts from a head, and the head moves each time a writer commits. Confirming that a head is current costs store requests. In a process without the writer session, a GET of the next WAL number finds new commits, and a HEAD of the next manifest number finds a newer manifest (a fold, a compaction, or another writer's claim). In the writer's process, the writer position includes every commit that the session made, so only the HEAD is needed, to find a manifest that another process published.

If each read confirmed its own head, every warm read in a reader process would send a GET, so a reader that walks thousands of files would send a GET per file to confirm a head that moves a few times during the walk. Each read would also send its probes at a different time, so LoonFS would need rules for which response confirms what, for a response that arrives too late, for concurrent reads that share one probe, and for a successor manifest that the process published itself. In the writer's process, a read whose probe found a fold's manifest before the session recorded it would treat the fold as another process's publication and drop the writer position.

LoonFS avoids those rules by keeping one view per process, confirming the view with one procedure, and letting most reads use the view as it is.

## One view per process

A process keeps one view of a namespace. In a process with the namespace's open writer session, once the session has published, the view is the session's [writer position](../specs/glossary.md#writer-position): the head that the session plans its next commit against, the manifest that head was read against, and the projected WAL tail above that manifest. Otherwise (in a process without the writer session, or before the session publishes, or after it's fenced or closed), the view is the read cache's cached view: a manifest and the head replayed from the WAL above it, with the projected tail cached under that head's key. This note calls either one the process's "view". It's the state that the process's reads start from, not a [read view](../specs/glossary.md#read-view), the state that one read sees.

A cached view records two times:

- The "confirmation time" is when a store observation last confirmed the head. That's the discovery that loaded the head, or the last refresh, which applies any new WAL objects that it finds.
- The "check time" is when a store observation last confirmed that the view's manifest has no successor.

A position records its check time, which is also its confirmation time. The session's check sets it, when the check finds no successor or moves the position onto successors under the session's writer epoch. A new position takes its check time from the writer's last confirmation of its manifest (see [Publication and group commit](../specs/format.md#63-publication-and-group-commit)), or from a later check by the session. A publish doesn't confirm the position's head, because a successful put confirms the WAL tip and nothing about the manifest, and another writer has to publish a claim manifest before it can commit, so for a position the check is what confirms the head.

No read starts from a view whose check time is [`READ_REVALIDATION_BOUND_MS`](../specs/glossary.md#revalidation-bound) old or older, because garbage collection can delete the segments of a superseded manifest once its grace window passes, and freshness and collection share this bound. LoonFS reloads such a view by discovery. In the writer's process, the read drops the position first, and the session's next publish loads the namespace from the store.

While a session has a position, LoonFS doesn't copy the position into the read cache. When the session closes or drops its position, the position becomes the read cache's view, with both of the view's times set to the position's check time, so the next read starts from a head at least as new as any head that a read from the position returned.

## One refresh procedure

A "refresh" confirms the view in a process without the writer session. A refresh:

1. Takes its observation time before it sends anything.
2. Sends the "successor probe", a HEAD of the manifest number after the view's manifest, if the view's check time is older than the manifest check interval (`manifest_revalidation_interval_ms`, 1,000 ms by default).
3. If the successor probe found nothing, or wasn't due, sends the "WAL probe", a GET of the WAL number after the view's tip, and applies each WAL object it finds until a number is missing.
4. Runs discovery instead if it finds a successor, a missing WAL number below a present one, a WAL object with another writer epoch, or a response that arrives `READ_REVALIDATION_BOUND_MS` or more after the view's check time. Discovery loads the current manifest from the hint and replays the WAL above it.
5. Otherwise, moves the confirmation time to its observation time, with the head at the last WAL object it applied, and moves the check time there too if it sent the successor probe.

One refresh runs per namespace per process at a time. A read that needs a refresh while one is in flight waits for that refresh, because it might have sent its probes before the read started. After that, an `eventual` read takes the view if the refresh confirmed it within the read's maximum staleness, and any other read shares the next refresh, which every read that arrives during the first refresh shares. A refresh that finished before a `strong` read started doesn't serve that read, and a wave of concurrent `strong` reads costs at most two refreshes.

Why one procedure and not a probe per question? Both probes ask one question (whether the view is current), and the rules about which response confirms what are needed when the probes are sent from different places at different times. A refresh sends both probes from one place, with one observation time, so one rule covers both responses.

## What a read promises

A read takes a [consistency](../specs/glossary.md#consistency) setting:

- `eventual` (the default): the read uses the view as it is while the view's confirmation time is younger than the read's maximum staleness, and otherwise it confirms the view first (with a refresh, or for a position, with the session's check). The default maximum staleness is 1,000 ms. A host can set it per runtime, and a read can set its own, up to the revalidation bound.
- `strong`: the read confirms the view first, so it sees every commit acknowledged before the read started.

An `eventual` read sees every commit acknowledged before the view's confirmation time, and can miss commits acknowledged since. Both settings see every commit that this process acknowledged, because the writer position moves before the acknowledgement. Neither setting returns an older head than a read that completed earlier in the same process, because a refresh replaces the view only with a newer one.

In the writer's process, an `eventual` read starts from the position and sends no request while the position was confirmed within the read's maximum staleness. The position includes this process's own commits, and all a request could add is another writer's claim, which the session's check finds once the read's maximum staleness has passed. A `strong` read waits for the session's check, so it sees another writer's claim before it reads (see the next section).

A process drops its view when it creates, forks, or deletes the namespace, when a mutation's outcome is unknown, and when it finds its writer session fenced, because it can't tell how those events moved the head. The next read loads the namespace by discovery, with either setting, so a read that follows one of those events in the same process sees the result. A refresh that started before the drop doesn't install its view when it finishes, because it might have read the namespace before the event. A manifest that a maintenance call in the process publishes doesn't drop the view, because the manifest is at the head the namespace already had, so the writer position moves onto it and a cached view stays as it is. In a process without the writer session, a read that checks authorization or a receipt before a write confirms the head whatever the caller asked for, because the check is part of the write. In the writer's process, a check before a commit reads the position as it is, because the position includes every access change and receipt that the session committed, and another writer's takeover fences the session at the commit's own publish. A check before a snapshot change or a fork, which the session doesn't publish, confirms the head in either process.

A pinned read (through a snapshot) has no consistency setting, because the pin selects its state.

Why is `eventual` the default? A read that follows a write in the same process already sees the write. A read that follows a write in another process is the case that needs a request, so that read asks for `strong`. A client that changes a permission through one server and reads through another passes `strong` on that read, and a poller that learned a change feed cursor from a fresher read retries as `strong`. A process that opens a namespace for the first time loads it by discovery, which is as fresh as `strong`. Other reads send no request within their maximum staleness. A deployment whose readers always follow writes in other processes sets `strong` as the default in the server's configuration, and pays a request on every read.

Why a maximum staleness and not an interval alone? The staleness is the promise, and a read's own setting states it in the code that issues the read. A view that nothing has read for a minute isn't refreshed every second in the background, and the first read after that minute refreshes it, so an idle process sends nothing.

## The writer's session and its position

The position belongs to the writer session, and every change to it runs under the session's engine. The session moves the position to each commit, onto each manifest this process publishes for the namespace (a fold, a compaction, a snapshot, or a retention advance), and onto a manifest that another process publishes under the session's writer epoch. It drops the position when another writer's claim or a namespace tombstone appears, and when the session is fenced or closed.

The position is a view like any other, and the same two rules cover it, with the session's check in place of a refresh. So an `eventual` read past its maximum staleness, and every `strong` read, runs the session's check and waits for it. As with a refresh, one check runs at a time per session, and a read that needs one waits for the check in flight and then shares the next one, so a wave of concurrent `strong` reads costs at most two checks. The manifest check interval doesn't apply to the position. A read waits for a check once its maximum staleness has passed since the position's check time.

The session's check:

1. Takes its observation time, and sends one HEAD for the next manifest number.
2. Drops the position if the response arrives `READ_REVALIDATION_BOUND_MS` or more after the position's check time.
3. If the HEAD finds nothing, records the check, so the position's check time becomes the check's observation time.
4. If the HEAD finds a successor, walks the manifests above the position's manifest. If the newest is an active manifest with the session's writer epoch, the check moves the position onto those manifests in order and records the check. Each of them describes a head that this session wrote, and the walk that ends on the newest manifest is that manifest's successor check.
5. If the walk finds a manifest with a later writer epoch (another writer's claim), a namespace tombstone, or more than eight manifests above the position's, drops the position. The session's next publish then runs discovery, and after a claim, it finds the fence.

A position that the check drops doesn't stay as the read cache's view either, so the next read loads the namespace by discovery.

So in the writer's process, a check costs one HEAD, or the walk when the HEAD finds a successor. A session with no reads sends no check, and a session under `eventual` reads sends one HEAD per maximum staleness, which is also the session's fencing check while it isn't publishing. No timer is involved, because a read runs the check, and the check ends with the HEAD or the walk.

For `eventual` reads in the old writer's process, the takeover window is the read's maximum staleness. For example, after another writer takes over, an `eventual` read there that starts within its maximum staleness of the position's last check returns the old writer's head. The first `eventual` read past that staleness waits for the check and finds the new writer's claim, and a `strong` read finds the claim on that read.

Why does an `eventual` read in the writer's process wait for the check only past its maximum staleness, and not on every read? A confirmation there can learn only of a takeover or of a manifest that another process published, and the staleness bounds how late the read learns of either one. Running the HEAD as the session's check keeps it off the other reads' latency, and keeps the ordering rules between reads, checks, and publications off the read.

## Costs

This table compares the store requests of common reads with those of the first design in [Alternatives considered](#alternatives-considered), where `strong` is the default and each read sends its own probes.

| Read | LoonFS | Probes on each read |
| --- | ---: | ---: |
| `eventual` `stat` in the writer's process | 0 within the maximum staleness, and 1 HEAD on the first read past it | 1 HEAD on the read that crosses the manifest check interval, and 0 otherwise |
| `strong` `stat` in the writer's process | 1 HEAD, and at most 2 for a wave of concurrent `strong` stats | 1 HEAD on the read that crosses the manifest check interval, and 0 otherwise |
| `stat` in another process, within the maximum staleness of a confirmation | 0 | 1 GET, plus 1 HEAD per interval |
| `stat` in another process, after the maximum staleness | 1 refresh: 1 GET, plus 1 HEAD per interval | the same |
| `strong` `stat` in another process | 1 GET, plus 1 HEAD per interval | the same |
| 32 concurrent `stat` calls in another process | at most 1 refresh for `eventual` reads (none within their maximum staleness), and at most 2 for `strong` reads | 32 GETs |
| A reader process that walks thousands of files (the lab's 100k sandbox restore) | 1 refresh per maximum staleness, so about one per second of the walk | 1 GET per file, plus 1 HEAD per interval |
| A process with open namespaces and no reads | 0 | 0 |
| A read after a takeover, in the old writer's process | `eventual`: sees the new writer's commits within the read's maximum staleness. `strong`: on that read | within one interval |
| A read after a fold by another process, in the writer's process | the position moves at the session's next check | the position moves at the next read's probe |

In the writer's process, `eventual` reads cost one HEAD per maximum staleness. With the default maximum staleness of 1,000 ms, the first `eventual` read after an idle second waits for one HEAD. The session runs one check at a time, so the reads that need a check share it.

## What the format and API specs say

The format's requirements cover discovery, the WAL checks, the revalidation bound, the publisher's tip observation and basis confirmation, and the rule that drops a position at the revalidation bound. [Replaying the visible WAL](../specs/format.md#42-replaying-the-visible-wal) describes how the reference runtime applies them:

- The two warm-read probes, which the runtime sends together as one refresh.
- `eventual` warm reads, which send no probe while the tip's confirmation is younger than the read's maximum staleness.
- Reads from the writer position, which the session checks with the successor probe, with the read's maximum staleness in place of the manifest check interval, and the walk that moves the position onto manifests with the session's writer epoch.
- The takeover example, in which a `strong` read finds the new writer's claim before it reads.

[Reference-implementation sizing and admission defaults](../specs/format.md#c4-reference-implementation-sizing-and-admission-defaults) lists `max_staleness_ms` next to the manifest check interval, and says that the maximum staleness applies to the last successor check of a writer position.

In the API specification, [Read consistency](../specs/api.md#read-consistency) makes `eventual` the default in the Rust API, the HTTP binding's `consistency` parameter, and the reference server. It explains `strong` as a per-read setting with the three cases above, and describes how the session's check confirms the position. The reference server's `max_staleness_ms` defaults to 1,000 ms, and its `consistency` setting lets a deployment make `strong` the default. The glossary's entries for [consistency](../specs/glossary.md#consistency) and the [writer position](../specs/glossary.md#writer-position) describe the same rules, and the [architecture overview](../specs/architecture.md) describes the probes that confirm a cached view.

## Alternatives considered

Make `strong` the default, and send each read's probes from the read. Every reader process then pays a request per read to confirm a head that rarely moves, so a walk over many files, or a burst of concurrent stats, sends one GET per read.

Poll every open namespace on a timer (for example, every second in the writer's process and every ten seconds in a reader process). A timer keeps a view fresh with no read in flight, but it costs a request per interval per namespace whether anything reads or not, so a process with a thousand open namespaces sends a thousand requests per interval.

Poll only, with no `strong` reads, so a reader lags by up to its poll interval, and a read can't see a write made in another process until the next poll. It's simpler by one setting, but it can't serve the one case that needs a request, a read that follows a write in another process. The `strong` setting keeps that case at one request, on the reads that ask for it.

Share one probe between concurrent reads, so that the read that starts the probe waits for it and later reads start without it. It keeps the probe on the read's path and needs a rule for every overlap of a read, a probe, and a publication. Under that design, a read's probe that overlaps a fold can drop the writer position.

Send a notification from the writer to readers in other processes, through an object that the writer writes on every commit. This adds a PUT to every commit to save a GET on every read, and it helps only when reads outnumber writes.
