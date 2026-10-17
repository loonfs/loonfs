# Read freshness

This note describes where a process keeps a namespace's current [head](../specs/glossary.md#head), how a read uses it, and how recent the state that a read sees is. Each process keeps one view of a namespace, one procedure refreshes that view, and a read uses the view as it is unless the read asks for a confirmed head. A read sends no store request if it's `eventual` and its view was confirmed within the read's maximum staleness. Otherwise, it costs one refresh, or one HEAD in the writer's process.

## The problem

Today, each read checks its own freshness. In a process without the writer session, a `strong` read (the default) sends a GET for the next WAL number on every warm read, and once per revalidation interval it sends a HEAD for the next manifest number. The format specification takes three sentences to say which response confirms what, and when a response arrives too late to count. In the writer's process, a read starts from the writer position, and once per interval it sends the HEAD that checks the position's manifest for a successor. That check needs a flag so that concurrent reads send one HEAD, a rule for reads that arrive while another read's check runs, and a rule for a successor that the process published itself.

The lab measures what this costs. In a reader process, 32 concurrent `strong` stats send 32 GETs. In the sandbox restore phase of the 100k workload, a reader process walks 20,385 files and sends 20,385 WAL probes and 3,044 manifest HEADs (20% of the phase's elapsed time) to confirm a head that moves a few times during the walk. In the writer's process, a read whose check runs after a fold's manifest put and before the publisher records that manifest drops the position, and in the lab a read-only phase then paid 331 GETs where reads from the position cost 177.

Each rule is correct, and they're all needed because every read checks its own freshness.

## One view per process

A process keeps one view of a namespace. In a process with the namespace's open writer session, the view is the session's [writer position](../specs/glossary.md#writer-position): the head that the session plans its next commit against, the manifest that head was read against, and the projected WAL tail above that manifest. In a process without the writer session, the view is the read cache's cached view: a manifest, the head replayed from the WAL above it, and the projected tail. This note calls either one the process's "view". It's the state that the process's reads start from, not a [read view](../specs/glossary.md#read-view), the state that one read sees.

A view records two times:

- The "confirmation time" is when a store observation last confirmed the head. For a cached view, that's the discovery that loaded the head, or a refresh that found no new WAL object.
- The "check time" is when a store observation last confirmed that the view's manifest has no successor.

For a position, the session's check sets both times, when it finds no successor or moves the position onto successors under the session's writer epoch. A new position takes both times from the session's last basis confirmation. A publish doesn't confirm the position's head, because a successful put confirms the WAL tip and nothing about the manifest, and another writer has to publish a claim manifest before it can commit, so for a position the check is what confirms the head.

No read starts from a view whose check time is [`READ_REVALIDATION_BOUND_MS`](../specs/glossary.md#revalidation-bound) old or older, because garbage collection can delete the segments of a superseded manifest once its grace window passes, and freshness and collection share this bound. LoonFS reloads such a view by discovery. In the writer's process, the read drops the position first, and the session's next publish reloads its projection.

The publisher never copies its position into the read cache. When the session closes or drops its position, the position becomes the read cache's view with both of its times, so the next read starts from a head at least as new as any head that a read from the position returned.

## One refresh procedure

A "refresh" confirms the view in a process without the writer session. A refresh:

1. Takes its observation time before it sends anything.
2. Sends the "WAL probe", a GET of the WAL number after the view's tip, and applies each WAL object it finds until a number is missing.
3. Sends the "successor probe", a HEAD of the manifest number after the view's manifest, if the view's check time is older than the manifest check interval.
4. Runs discovery instead if it finds a successor, a missing WAL number below a present one, or a response that arrives `READ_REVALIDATION_BOUND_MS` or more after the view's check time. Discovery loads the current manifest from the hint and replays the WAL above it.
5. Otherwise, moves the confirmation time to its observation time, with the head at the last WAL object it applied, and moves the check time there too if it sent the successor probe.

One refresh runs per namespace per process at a time, and a read that needs a refresh while one is in flight waits for that refresh. A refresh counts for a `strong` read only if its observation time is at or after the read's start. So a `strong` read that finds a refresh in flight waits for that refresh and then for the next one, which every read that arrives during the first refresh shares, and a wave of concurrent `strong` reads costs at most two refreshes.

Why one procedure and not a probe per question? Both probes ask one question (whether the view is current), and the rules about which response counts are needed because the probes are sent from different places at different times. A refresh sends both probes from one place, with one observation time, so one rule covers both responses.

## What a read promises

A read takes a [consistency](../specs/glossary.md#consistency) setting:

- `eventual` (the default): the read uses the view as it is while the view's confirmation time is younger than the read's maximum staleness, and otherwise it confirms the view first (with a refresh, or for a position, with the session's check). The default maximum staleness is 1,000 ms. A host can set it per runtime, and a read can set its own, up to the revalidation bound.
- `strong`: the read confirms the view first, so it sees every commit acknowledged before the read started.

An `eventual` read sees every commit acknowledged before the view's confirmation time, and can miss commits acknowledged since. Both settings see every commit that this process acknowledged, because the writer position moves before the acknowledgement. Neither setting returns an older head than a read that completed earlier in the same process, because a refresh replaces the view only with a newer one.

In the writer's process, an `eventual` read starts from the position and sends no request while the position was confirmed within the read's maximum staleness. The position includes this process's own commits, and all a request could add is another writer's claim, which the session's check finds once the read's maximum staleness has passed. A `strong` read waits for the session's check, so it sees another writer's claim before it reads (see the next section).

A process drops its view when it creates, forks, or deletes the namespace, when a mutation's outcome is unknown, and when it finds its writer session fenced, because it can't tell how those events moved the head. The next read loads the namespace by discovery, with either setting, so a read that follows one of those events in the same process sees the result. A manifest that a maintenance call in the process publishes doesn't drop the view, because the manifest is at the head the namespace already had, so the writer position moves onto it and a cached view stays as it is. In a process without the writer session, a read that checks authorization or a receipt before a write confirms the head whatever the caller asked for, because the check is part of the write. In the writer's process, a check before a commit reads the position as it is, because the position includes every access change and receipt that the session committed, and another writer's takeover fences the session at the commit's own publish. A check before a snapshot change or a fork, which the session doesn't publish, confirms the head in either process.

A pinned read (through a snapshot or a checkpoint) has no consistency setting, because the pin selects its state.

Why is `eventual` the default? A read that follows a write in the same process already sees the write. A read that follows a write in another process is the case that needs a request, so that read asks for `strong`. A client that changes a permission through one server and reads through another passes `strong` on that read, and a poller that learned a change feed cursor from a fresher read retries as `strong`. A process that opens a namespace for the first time loads it by discovery, which is as fresh as `strong`. Other reads send no request within their maximum staleness. A deployment whose readers always follow writes in other processes sets `strong` as the default in the server's configuration, and pays what it pays today.

Why a maximum staleness and not an interval alone? The staleness is the promise, and a read's own setting states it in the code that issues the read. A view that nothing has read for a minute isn't refreshed every second in the background, and the first read after that minute refreshes it, so an idle process sends nothing.

## The writer's session and its position

The position belongs to the writer session, and every change to it runs under the session's engine. The session moves the position to each commit, onto each manifest this process publishes for the namespace (a fold, a compaction, a checkpoint, a snapshot, or a retention advance), and onto a manifest that another process publishes under the session's writer epoch. It drops the position when another writer's claim or a namespace tombstone appears, and when the session is fenced or closed.

The position is a view like any other, and the same two rules cover it, with the session's check in place of a refresh. So an `eventual` read past its maximum staleness, and every `strong` read, runs the session's check and waits for it. As with a refresh, a check counts for a read only if the check's observation time is at or after the read's start. One check runs at a time per session, and a read that needs one shares the check in flight or the next one, so a wave of concurrent `strong` reads costs at most two checks. The manifest check interval doesn't apply to the position, and the read's maximum staleness takes its place.

The session's check:

1. Takes its observation time, and sends one HEAD for the next manifest number.
2. Drops the position if the response arrives `READ_REVALIDATION_BOUND_MS` or more after the position's check time.
3. If the HEAD finds nothing, records the check, so both of the position's times become the check's observation time.
4. If the HEAD finds a successor, walks the manifests above the position's manifest. If the newest is an active manifest with the session's writer epoch, the check moves the position onto those manifests in order and records the check. Each of them describes a head that this session wrote, and the walk that ends on the newest manifest is that manifest's successor check.
5. If the walk finds a manifest with a later writer epoch (another writer's claim) or a namespace tombstone, drops the position. The session's next publish then runs discovery and finds the fence.

A position that the check drops doesn't stay as the read cache's view either, so the next read loads the namespace by discovery.

So in the writer's process, a check costs one HEAD, or the walk when the HEAD finds a successor. A session with no reads sends nothing, and a session under `eventual` reads sends one HEAD per maximum staleness, which is also the session's fencing check while it isn't publishing. No timer is involved, because a read runs the check, and the check ends with the HEAD or the walk.

For `eventual` reads in the old writer's process, the takeover window is the read's maximum staleness. For example, after another writer takes over, an `eventual` read there that starts within its maximum staleness of the position's last check returns the old writer's head. The first `eventual` read past that staleness waits for the check and finds the new writer's claim, and a `strong` read finds the claim on that read.

Why does an `eventual` read in the writer's process wait for the check only past its maximum staleness, and not on every read? A confirmation there can learn only of a takeover or of a manifest that another process published, and the staleness bounds how late the read learns of either one. Running the HEAD as the session's check keeps it off the other reads' latency, and keeps the ordering rules between reads, checks, and publications off the read.

## Costs

| Read | Today | With this design |
| --- | ---: | ---: |
| `eventual` `stat` in the writer's process | 1 HEAD on the read that crosses the manifest check interval, and 0 otherwise | 0 within the maximum staleness, and 1 HEAD on the first read past it |
| `strong` `stat` in the writer's process | 1 HEAD on the read that crosses the manifest check interval, and 0 otherwise | 1 HEAD, and at most 2 for a wave of concurrent `strong` stats |
| `stat` in another process, within the maximum staleness of a confirmation | `strong` (the default): 1 GET, plus 1 HEAD per interval | 0 |
| `stat` in another process, after the maximum staleness | 1 GET, plus 1 HEAD per interval | 1 refresh: 1 GET, plus 1 HEAD per interval |
| `strong` `stat` in another process | 1 GET, plus 1 HEAD per interval | the same |
| 32 concurrent `stat` calls in another process (the lab issues them together) | 32 GETs | at most 1 refresh for `eventual` reads (none within their maximum staleness), and at most 2 for `strong` reads |
| A reader process walking 20,385 files in 327 s (the 100k sandbox restore) | 20,385 GETs and 3,044 HEADs | about 330 GETs and 330 HEADs, one refresh per second |
| A process with open namespaces and no reads | 0 | 0 |
| A read after a takeover, in the old writer's process | sees the new writer's commits within one interval | `eventual`: within the read's maximum staleness. `strong`: on that read |
| A read after a fold by another process, in the writer's process | the position moves at the next read's check | the same, at the session's next check |

For `eventual` reads, the writer's process sends one HEAD per maximum staleness, where today it sends one per manifest check interval. With the default interval and maximum staleness both at 1,000 ms, the first `eventual` read after an idle second waits for one HEAD, as the read that crosses the interval does today. The memory per view doesn't change, and the flag that let concurrent reads share one probe goes away, because the session runs one check at a time.

## What the format and API specs change

The format's requirements (discovery, the WAL checks, the revalidation bound, and the publisher's tip observation and basis confirmation) don't change, and neither does the rule that drops a position at the revalidation bound. The description of the reference runtime in [Replaying the visible WAL](../specs/format.md#42-replaying-the-visible-wal) changes:

- The two warm-read probes, and the sentences about which response confirms what, become the refresh procedure above.
- The `eventual` paragraph describes the default.
- The writer position paragraph describes the session's check instead of a probe on the read's path, with the read's maximum staleness in place of the manifest check interval. The sentence about a read that finds another read's probe running goes, and the walk replaces the rule for a successor that the runtime published itself.
- The takeover example uses the session's check and the read's maximum staleness in place of the read's probe and the interval. A `strong` read finds the claim before it reads, so the sentence that every read returns the new writer's commits with an interval of zero applies to `strong` reads.

In [Reference-implementation sizing and admission defaults](../specs/format.md#c4-reference-implementation-sizing-and-admission-defaults), the sentence that the manifest check interval applies to a writer position changes, because the maximum staleness governs the position.

In the API specification, [Read consistency](../specs/api.md#read-consistency) makes `eventual` the default in the Rust API, the HTTP binding's `consistency` parameter, and the reference server, with no compatibility mode. It keeps `strong` as a per-read setting, explained by the three cases above, and its sentence saying that both settings start from the position with no request changes. So does the API's description of the manifest check interval in the writer's runtime, including its sentence about reads that arrive while another read's check runs. The reference server's `max_staleness_ms` stays at 1,000, and the server gains a `consistency` setting for a deployment that needs `strong` by default. The glossary's entries for consistency and the writer position change to match, and the architecture overview's sentence about cached views describes a refresh.

## Alternatives considered

Keep `strong` as the default, and leave the probes where they are. Every reader process then pays a request per read to confirm a head that rarely moves, and the lab's restore walk and reader burst show that cost.

Poll every open namespace on a timer, which is SlateDB's model (its writer polls the manifest every second, and a reader process polls every ten seconds). A timer keeps a view fresh with no read in flight, but it costs a request per interval per namespace whether anything reads or not, so a process with a thousand open namespaces sends a thousand requests per interval.

Poll only, with no `strong` reads across processes, which is also SlateDB's model (a reader lags by up to its poll interval, and there's no read-your-writes across processes). It's simpler by one setting, but it can't serve the one case that needs a request, a read that follows a write in another process. The `strong` setting keeps that case at one request, on the reads that ask for it.

Share one probe between concurrent reads, so that the read that starts the probe waits for it and later reads start without it. It keeps the probe on the read's path and needs a rule for every overlap of a read, a probe, and a publication. Under that design, a read's probe that overlaps a fold is where the position gets lost.

Send a notification from the writer to readers in other processes, through an object that the writer writes on every commit. This adds a PUT to every commit to save a GET on every read, and it helps only when reads outnumber writes.
