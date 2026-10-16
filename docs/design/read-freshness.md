# Read freshness

This note describes where a process keeps a namespace's current [head](../specs/glossary.md#head), how a read uses it, and how recent the state that a read sees is. Each process keeps one view of a namespace, one procedure refreshes that view, and a read uses the view as it is unless the read asks for a confirmed head. In the writer's process, an `eventual` read sends no store request, and a `strong` read waits for one HEAD. In another process, a read costs one refresh unless it's `eventual` and the view was confirmed within its maximum staleness.

## The problem

Today, each read checks its own freshness. In a process without the writer session, a `strong` read (the default) sends a GET for the next WAL number on every warm read, and once per revalidation interval it sends a HEAD for the next manifest number. The format specification takes three sentences to say which response confirms what, and when a response arrives too late to count. In the writer's process, a read starts from the writer position, and once per interval it sends the HEAD that checks the position's manifest for a successor. That check needs a flag so that concurrent reads send one HEAD, a rule for reads that arrive while another read's check runs, and a rule for a successor that the process published itself.

The lab measures what this costs. In a reader process, 32 concurrent `strong` stats send 32 GETs. In the sandbox restore phase of the 100k workload, a reader process walks 20,385 files and sends 20,385 WAL probes and 3,044 manifest HEADs (20% of the phase's elapsed time) to confirm a head that moves a few times during the walk. In the writer's process, a read whose check runs after a fold's manifest put and before the publisher records that manifest drops the position, and in the lab a read-only phase then paid 331 GETs where reads from the position cost 177.

Each rule is correct, and they're all needed because every read checks its own freshness.

## One view per process

A process keeps one view of a namespace. In a process with the namespace's open writer session, the view is the session's [writer position](../specs/glossary.md#writer-position): the head that the session plans its next commit against, the manifest that head was read against, and the projected WAL tail above that manifest. In a process without the writer session, the view is the read cache's cached view: a manifest, the head replayed from the WAL above it, and the projected tail. This note calls either one the process's "view". It's the state that the process's reads start from, not a [read view](../specs/glossary.md#read-view), the state that one read sees.

A view records two times:

- The "confirmation time" is when a store observation last confirmed the head: the discovery that loaded the head, a refresh that found no new WAL object, or, for a position, the session's last publish.
- The "check time" is when a store observation last confirmed that the view's manifest has no successor.

No read starts from a view whose check time is [`READ_REVALIDATION_BOUND_MS`](../specs/glossary.md#revalidation-bound) old or older, because garbage collection can delete the segments of a superseded manifest once its grace window passes, and freshness and collection share this bound. LoonFS reloads such a view by discovery. In the writer's process, the read drops the position first, and the session's next publish reloads its projection.

The publisher never copies its position into the read cache. When the session closes, is fenced, or drops its position, the position becomes the read cache's view with both of its times, so the next read starts from a head at least as new as any head that a read from the position returned.

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

- `eventual` (the default): the read uses the view as it is while the view's confirmation is younger than the read's maximum staleness, and refreshes it first otherwise. The default maximum staleness is 1,000 ms. A host can set it per runtime, and a read can set its own, up to the revalidation bound.
- `strong`: the read confirms the view first, so it sees every commit acknowledged before the read started.

An `eventual` read sees every commit acknowledged before the view's confirmation time, and can miss commits acknowledged since. Both settings see every commit that this process acknowledged, because the writer position moves before the acknowledgement. Neither setting returns an older head than a read that completed earlier in the same process, because a refresh replaces the view only with a newer one.

In the writer's process, an `eventual` read starts from the position and sends no request. The position includes this process's own commits, and all a request could add is another writer's claim, which the session's check finds within one interval. A `strong` read waits for the session's check, so it sees another writer's claim before it reads (see the next section).

A process marks a view "unconfirmed" when it creates, forks, or deletes the namespace, when a maintenance call in the process publishes a manifest for the namespace, and when it finds its writer session fenced. LoonFS refreshes an unconfirmed view before the next read, with either setting, so a read that follows one of those events in the same process sees the result. A read that checks authorization or a receipt before a write confirms the head whatever the caller asked for, because the check is part of the write.

A pinned read (through a snapshot or a checkpoint) has no consistency setting, because the pin selects its state.

Why is `eventual` the default? A read that follows a write in the same process already sees the write. A read that follows a write in another process is the case that needs a request, so that read asks for `strong`. A client that changes a permission through one server and reads through another passes `strong` on that read, and a poller that learned a change feed cursor from a fresher read retries as `strong`. A process that opens a namespace for the first time loads it by discovery, which is as fresh as `strong`. Other reads send no request within their maximum staleness. A deployment whose readers always follow writes in other processes sets `strong` as the default in the server's configuration, and pays what it pays today.

Why a maximum staleness and not an interval alone? The staleness is the promise, and a read's own setting states it in the code that issues the read. A view that nothing has read for a minute isn't refreshed every second in the background, and the first read after that minute refreshes it, so an idle process sends nothing.

## The writer's session and its position

The position belongs to the writer session, and every change to it runs on a session task that has the session's engine. The session moves the position to each commit, onto each manifest this process publishes for the namespace (a fold, a compaction, a checkpoint, a snapshot, or a retention advance), and onto a manifest that another process publishes under the session's writer epoch. It drops the position when another writer's claim or a namespace tombstone appears, and when the session is fenced or closed.

The session's check task looks for a successor to the position's manifest, and one check task runs at a time per session. An `eventual` read never waits for a check, and it always starts from the position. If the check time is older than the manifest check interval and no check task is running, the read starts one, and if a check task is running, that check covers the read. An interval of zero follows the same rule, so a session under `eventual` reads has one check in flight at a time.

A `strong` read waits for a check (one HEAD, or the walk when the HEAD finds a successor), and as with a refresh, a check counts for the read only if the check's observation time is at or after the read's start. So a `strong` read that finds a check running waits for it and then for the next one, which every read that arrives during the first check shares. The check task:

1. Takes its observation time, and sends one HEAD for the next manifest number.
2. Drops the position if the response arrives `READ_REVALIDATION_BOUND_MS` or more after the position's check time.
3. If the HEAD finds nothing, records the check, so the position's check time becomes the task's observation time.
4. If the HEAD finds a successor, loads the manifests above the position's manifest. If the newest is an active manifest with the session's writer epoch, the task moves the position onto those manifests in order and records the check. Each of them describes a head that this session wrote, and the walk that ends on the newest manifest is that manifest's successor check.
5. If the walk finds a manifest with a later writer epoch (another writer's claim) or a namespace tombstone, drops the position. The session's next publish then runs discovery and finds the fence.

A position that the task drops becomes the read cache's view, marked unconfirmed.

So in the writer's process, a session with no reads sends nothing, and a session with `eventual` reads sends one HEAD per interval, which is also the session's fencing check while it isn't publishing. No timer is involved, because a read starts the check, and the check ends with the HEAD or the walk.

For `eventual` reads in the old writer's process, the takeover window is one interval plus one HEAD's latency. For example, after another writer takes over, an `eventual` read there that starts within one interval of the position's last check returns the old writer's head, and a `strong` read finds the new writer's claim.

Why does a read start the check, and not a timer? A writer process serves many namespaces, and a timer per open session costs a request per interval per session whether anything reads or not. A check that reads start costs the same request while reads happen, and nothing when they stop. SlateDB's writer polls its manifest every second on a timer, which is cheap for a process with one database but not for a process with a thousand namespaces.

Why doesn't an `eventual` read in the writer's process confirm the head on its own path? A confirmation there can learn of a takeover or a manifest that another process published, and the session's check learns of both within one interval either way. Running the HEAD on a session task keeps it out of the read's latency, and keeps the ordering rules between reads, checks, and publications out of the read.

## Costs

| Read | Today | With this design |
| --- | ---: | ---: |
| `eventual` `stat` in the writer's process | 0 | 0 |
| `strong` `stat` in the writer's process | 0 | 1 HEAD, and at most 2 for a wave of concurrent `strong` stats |
| `stat` in another process, within the maximum staleness of a confirmation | `strong` (the default): 1 GET, plus 1 HEAD per interval | 0 |
| `stat` in another process, after the maximum staleness | 1 GET, plus 1 HEAD per interval | 1 refresh: 1 GET, plus 1 HEAD per interval |
| `strong` `stat` in another process | 1 GET, plus 1 HEAD per interval | the same |
| 32 concurrent `stat` calls in another process (the lab issues them together) | 32 GETs | at most 1 refresh for `eventual` reads (none within their maximum staleness), and at most 2 for `strong` reads |
| A reader process walking 20,385 files in 327 s (the 100k sandbox restore) | 20,385 GETs and 3,044 HEADs | about 330 GETs and 330 HEADs, one refresh per second |
| A process with open namespaces and no reads | 0 | 0 |
| A read after a takeover, in the old writer's process | sees the new writer's commits within one interval | `eventual`: within one interval plus one HEAD. `strong`: on that read |
| A read after a fold by another process, in the writer's process | the position moves at the next read's check | the same, on the session's check task |

For `eventual` reads, the writer's process sends the same HEAD per interval as today, from a session task instead of a read. The memory per view doesn't change, and the flag that let concurrent reads share one probe goes away, because reads don't send probes.

## What the format and API specs change

The format's requirements (discovery, the WAL checks, the revalidation bound, and the publisher's tip observation and basis confirmation) don't change, and neither does the rule that drops a position at the revalidation bound. The description of the reference runtime in [Replaying the visible WAL](../specs/format.md#42-replaying-the-visible-wal) changes:

- The two warm-read probes, and the sentences about which response confirms what, become the refresh procedure above.
- The `eventual` paragraph describes the default.
- The writer position paragraph describes the session's check instead of a probe on the read's path. The sentence about a read that finds another read's probe running goes, and the walk replaces the rule for a successor that the runtime published itself.
- The takeover example uses the session's check in place of the read's probe. With an interval of zero, an `eventual` read starts a check (or relies on the running one) without waiting, and a `strong` read finds the claim before it reads, so the sentence that every read returns the new writer's commits applies to `strong` reads.

In the API specification, [Read consistency](../specs/api.md#read-consistency) makes `eventual` the default in the Rust API, the HTTP binding's `consistency` parameter, and the reference server, with no compatibility mode. It keeps `strong` as a per-read setting, explained by the three cases above, and its sentence saying that both settings start from the position with no request changes. So does the API's sentence about reads that arrive while another read's check runs. The reference server's `max_staleness_ms` stays at 1,000, and the server gains a `consistency` setting for a deployment that needs `strong` by default. The glossary's entries for consistency and the writer position change to match, and the architecture overview's sentence about cached views describes a refresh.

## Alternatives considered

Keep `strong` as the default, and leave the probes where they are. Every reader process then pays a request per read to confirm a head that rarely moves, and the lab's restore walk and reader burst show that cost.

Poll every open namespace on a timer, which is SlateDB's model (its writer polls the manifest every second, and a reader process polls every ten seconds). A timer keeps a view fresh with no read in flight, but it costs a request per interval per namespace whether anything reads or not, so a process with a thousand open namespaces sends a thousand requests per interval.

Poll only, with no `strong` reads across processes, which is also SlateDB's model (a reader lags by up to its poll interval, and there's no read-your-writes across processes). It's simpler by one setting, but it can't serve the one case that needs a request, a read that follows a write in another process. The `strong` setting keeps that case at one request, on the reads that ask for it.

Share one probe between concurrent reads, so that the read that starts the probe waits for it and later reads start without it. It keeps the probe on the read's path and needs a rule for every overlap of a read, a probe, and a publication. Under that design, a read's probe that overlaps a fold is where the position gets lost.

Send a notification from the writer to readers in other processes, through an object that the writer writes on every commit. This adds a PUT to every commit to save a GET on every read, and it helps only when reads outnumber writes.
