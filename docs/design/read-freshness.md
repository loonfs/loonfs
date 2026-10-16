# Read freshness

This note describes where a process keeps a namespace's current head, how a read uses it, and what a read promises about how recent the state it sees is. Each process keeps one view of a namespace, one procedure refreshes a view, and a read takes the view as it is unless it asks for a confirmed head. A read in the writer's process costs no store request. A read in another process costs no store request while the view is younger than the read's maximum staleness, and one refresh when it isn't or when the read asks for a confirmed head.

## The problem

A read decides its own freshness today. In a process without the writer session, a `strong` read, the default, sends a GET for the next WAL number on every warm read, and once per revalidation interval it sends a HEAD for the next manifest number. Which answer confirms what, and when an answer arrives too late to count, takes three sentences of the format specification. In the writer's process a read starts from the writer position, and once per interval a read's path sends the HEAD that checks the position's manifest for a successor, with a flag so that concurrent reads send one HEAD, a rule for reads that arrive while another read's check runs, and a rule for a successor the process published itself.

The lab measures what this costs. A burst of 32 `strong` stats in a reader process sends 32 GETs. The sandbox restore phase of the 100k workload, a reader process walking 20,385 files, sends 20,385 WAL probes and 3,044 manifest HEADs, 20% of the phase's elapsed time, to confirm a head that moves a few times in the whole walk. In the writer's process the check on the read path is where the position was lost under a fold: a read whose check ran between a fold's manifest put and the publisher's record of it dropped the position, and a read-only phase then paid 331 GETs where the position costs 177.

Every rule is correct on its own, and the set exists because each read decides its own freshness in the middle of the read.

## One view per process

A process keeps one view of a namespace. In a process with an open writer session for the namespace, the view is the session's [writer position](../specs/glossary.md#writer-position): the head the session plans its next commit against and the manifest that head was read against, with the projected WAL tail above the manifest. The session moves the position to each commit before it acknowledges the commit. In a process without the session, the view is the read cache's cached view of the namespace: a manifest, the head replayed from the WAL above it, and the projected tail. This note calls either one the process's view of the namespace. It's the state the process's reads start from, not a [read view](../specs/glossary.md#read-view), which is the state one read sees.

A view records two times. The confirmation time is when a store observation last confirmed the head: the discovery that loaded it, a refresh that found no new WAL object, or, for a position, the session's last publish. The check time is when a store observation last confirmed that the view's manifest has no successor. Garbage collection can delete the segments of a superseded manifest once its grace window passes, so no read starts from a view whose check time is `READ_REVALIDATION_BOUND_MS` old or older. Such a view is reloaded by discovery first, in the writer's process too, where the session rebases its position onto the current manifest. The bound is the one rule that freshness and collection share.

The publisher never copies its position into the read cache. When the session closes, is fenced, or drops its position, the position becomes the read cache's view, with both times as they were, so the next read starts from a head at least as new as the reads from the position returned.

## One refresh procedure

A refresh confirms a view of a process without the writer session. It takes its observation time before it sends anything. It sends the WAL probe, a GET of the WAL number after the view's head, and applies each object it finds until a number is missing. It sends the successor probe, a HEAD of the manifest number after the view's manifest, when the view's check time is older than the manifest check interval. A refresh that finds no successor moves the confirmation time to its observation time, with the head at the last WAL object it applied, and moves the check time there too when it sent the successor probe. A refresh that finds a successor, a missing WAL number below a present one, or an answer that arrived `READ_REVALIDATION_BOUND_MS` or more after the view's check time runs discovery instead, which loads the current manifest from the hint and replays the WAL above it.

One refresh runs per namespace per process at a time. A read that needs a refresh while one is in flight waits for that one. A refresh only ever replaces the view with a newer one, so a read never sees an older head than a read that completed before it.

Why one procedure and not a probe per question? Because both probes ask one question, whether the view is current, and the rules about which answer counts exist only when the probes are sent from different places at different times. A refresh sends both from one place with one observation time, so one rule covers both answers.

## What a read promises

A read takes a consistency setting:

- `eventual`, the default: the read takes the view as it is while the view's confirmation time is younger than the read's maximum staleness, and it refreshes the view first otherwise. The runtime's default maximum staleness is 1,000 ms, a host sets it per runtime, and a read can set its own, up to the revalidation bound.
- `strong`: the read refreshes the view first, so it sees every commit that was acknowledged before the refresh started.

An `eventual` read sees every commit that was acknowledged before the view's confirmation time, and it may miss commits acknowledged since. Both settings see every commit this process acknowledged, because the writer position moves before the acknowledgement. Neither setting returns a head older than a read that completed earlier in the same process.

In the writer's process, a read with either setting starts from the position and sends no request. The position is this process's own writes, and all a request could add is another writer's claim, which the session's check finds within one interval, as the next section describes.

A view that this process marked unconfirmed is refreshed before the next read of either setting. A process marks a view unconfirmed when it creates, forks or deletes the namespace, when a maintenance call in the process publishes a manifest for it, and when it finds its writer session fenced, so a read that follows one of those in the same process sees it. A read that checks authorization or a receipt before a write confirms the head whatever the caller asked, because the check is part of the write.

A pinned read, through a snapshot or a checkpoint, has no consistency setting. The pin selects its state.

Why is `eventual` the default? Because a read that follows a write in the same process already sees the write, and a read that follows a write in another process is the case that needs a request, so that read says so. A client that changes a permission through one server and reads through another passes `strong` on that read. A poller that learned a change feed cursor from a fresher read retries as `strong`. A process that opens a namespace for the first time loads it by discovery, which is as fresh as `strong`. Every other read, which is nearly every read, pays nothing. A host whose readers always follow writes in other processes sets the runtime's default to `strong`, and pays what it pays today.

Why a maximum staleness and not an interval alone? Because the staleness is the promise, and a read's own setting states it where the read is written. A view that nothing has read for a minute isn't refreshed every second in the background, and the first read after that minute refreshes it, so an idle process sends nothing.

## The writer's session and its position

The session owns the position. It moves the position to each commit, onto each manifest the session publishes (a fold), onto each manifest a maintenance call in the same process publishes (a compaction, a checkpoint, a snapshot, or a retention advance), and onto a manifest another process publishes under the session's writer epoch. It drops the position when another writer's claim or a namespace tombstone appears, and when the session is fenced or closed. Every change of the position runs on a task of the session that has the session's engine.

The session checks the position's manifest for a successor once per manifest check interval while reads start from the position. A read that finds the check time older than the interval starts the session's check task, one at a time per session, and starts from the position without waiting for it. The task sends one HEAD for the next manifest number. If it finds one, it loads the manifests above the position's. While the newest is an active manifest with the session's writer epoch, each describes a head this session wrote, so the task moves the position onto them in order and records the check, because the walk that ends on the newest manifest is that manifest's successor check. A manifest with a later epoch is another writer's claim, and a tombstone ends the namespace, so the task drops the position, and the session's next publish runs discovery and finds the fence.

So a read's path never sends a request in the writer's process, a session with no reads sends nothing, and a session under reads sends one HEAD per interval, which is also the session's fencing check while it isn't publishing. The runtime has no timer. A read starts the check, and the check ends with the HEAD or the walk. The takeover window for reads in the old writer's process is one interval plus one HEAD's latency: a read that starts within the interval of the position's last check returns the old writer's head, and the check after the interval finds the new writer's claim.

Why a check that a read starts and not a poll on a timer? A writer process serves many namespaces, and a timer per open session costs a request per interval per session whether anything reads or not. A check that reads start costs the same request while reads happen and nothing when they stop. SlateDB's writer polls its manifest every second on a timer, which is cheap for a process with one database and isn't for a process with a thousand namespaces.

Why not confirm on the read's path at all, in the writer's process? Because all a confirmation can learn there is a takeover or a manifest another process published, and both are learned within one interval either way. Keeping the HEAD on a session task takes it off the read's latency, and it takes the ordering rules between reads, checks, and publications off the read.

## Costs

| Read | Today | With this design |
| --- | ---: | ---: |
| `stat` in the writer's process | 0 | 0 |
| `stat` in another process, within the maximum staleness of a confirmation | `strong` (the default): 1 GET, plus 1 HEAD per interval | 0 |
| `stat` in another process, after the maximum staleness | 1 GET, plus 1 HEAD per interval | 1 refresh: 1 GET, plus 1 HEAD per interval |
| `strong` `stat` in another process | 1 GET, plus 1 HEAD per interval | the same |
| 32 `stat` calls in a burst in another process | 32 GETs | 1 GET, or 32 with `strong` |
| A reader process walking 20,385 files in 327 s (the 100k sandbox restore) | 20,385 GETs and 3,044 HEADs | about 330 GETs and 330 HEADs, one refresh per second |
| A process with open namespaces and no reads | 0 | 0 |
| A read after a takeover, in the old writer's process | sees the new writer's commits within one interval | within one interval plus one HEAD |
| A read after a fold by another process, in the writer's process | the position moves at the next read's check | the same, on the session's check task |

The writer's process sends the same HEAD per interval it sends today, from a session task instead of a read. The memory per view is the same, and the flag that let concurrent reads share one probe goes away, because reads don't probe.

## What the format and API specs change

The format's requirements don't change: discovery, the WAL checks, the revalidation bound, and the publisher's tip observation and basis confirmation stay as they are. What changes is the description of the reference runtime in [Replaying the visible WAL](../specs/format.md#42-replaying-the-visible-wal). The two warm-read probes and the sentences about which answer confirms what become the refresh procedure above. The `eventual` paragraph describes the default. The writer position paragraph describes the session's check instead of a probe on the read's path, so the sentence about a read that finds another read's probe running goes, and every read starts from the position without waiting. The example of a takeover stays, with the session's check in place of the read's probe.

The API specification's [Read consistency](../specs/api.md#read-consistency) makes `eventual` the default and keeps `strong` as the per-read setting, with the three cases above as the reasons a read passes it. The HTTP binding's `consistency` parameter defaults to `eventual`, the reference server's `max_staleness_ms` stays at 1,000, and the server gains a `consistency` setting for hosts that want `strong` by default. The architecture overview's sentence about cached views describes a refresh.

## Alternatives considered

Keep `strong` as the default and leave the probes where they are. Every reader process pays a request per read to confirm a head that rarely moved, and the lab's restore walk and reader burst are the bill.

Poll every open namespace on a timer, which is SlateDB's model: its writer polls the manifest every second and a reader process polls every ten seconds, each on a timer. It keeps a view fresh with no read in flight, and it costs a request per interval per namespace whether anything reads or not, which a process with a thousand open namespaces can't afford. A check that reads arm costs the same while reads happen and nothing when they stop.

Poll only, with no `strong` reads across processes, which is also SlateDB's model (a reader lags by up to its poll interval, and there is no read-your-writes across processes). It is simpler by one setting, and it removes the one case that needs a request: a read that follows a write in another process. The setting keeps that case at one request, on the reads that ask.

Share one probe between concurrent reads, with the read that starts the probe waiting for it and later reads starting without it. This keeps the probe on the read's path, and it needs a rule for every pair of a read, a probe, and a publication that can overlap, which is where the position was lost under a fold.

A notification from the writer to readers in other processes, through an object the writer touches on every commit. It adds a PUT to every commit to save a GET on every read, and it only helps when reads outnumber writes.
