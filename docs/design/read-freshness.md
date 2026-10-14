# Read freshness

This note describes where a process keeps a namespace's current head, and what a read promises about how recent the state it sees is. Today every warm read confirms the head with a store request. In this design each process keeps the head in one place, and a read has a freshness setting, so a read in the writer's process costs no store request and a reader in another process can choose between confirming on every read and polling on an interval.

## The problem

A namespace's current head is the newest commit sequence and the manifest it was read against. Inside one process LoonFS keeps that head in three places:

- The publisher's projection, which the writer session plans commits against.
- The read cache's anchor, which reads start from (`HeadStateCache`).
- The WAL-tail projection, the decoded WAL tail above a manifest at a head sequence.

Each publish copies the publisher's head into the read cache (a "seed"). Each fold, compaction, checkpoint, GC pass, and retention advance drops the anchor. Every warm read sends one GET for the next WAL number to confirm the anchor, and once a second it sends a HEAD to check for a newer manifest. Concurrent reads share one revalidation through a lock.

This machinery is where the recent defects were. A seed put an older manifest over a newer anchor, so every later read reloaded the hint, the manifests, and the whole unfolded WAL tail (21 to 41 requests per read, loonfs #1276). A compaction published through `Maintenance` left the publisher on a stale manifest for 60 s (loonfs #1279). The revalidation probed WAL numbers one at a time while holding the lock, so a staged write waited behind it for tens of seconds under an append load (loonfs #1281). Each fix was correct, and each one added a rule to a set of rules that only exists because three structures describe one head.

The cost shows in every read too. In the writer's process a `stat` costs one GET and a 1 KiB read costs two, and the GET learns nothing the process doesn't already know: the writer wrote the newest WAL object itself.

## One owner of the head per process

In a process with an open writer session for the namespace, the publisher is the only owner of the head. A read in that process takes the head from the publisher's projection and reads the WAL tail from the publisher's own tail. The read cache keeps no anchor for a namespace whose writer session is open in the process, and the publisher never seeds the read cache.

A read served this way sees every commit this process acknowledged, because the publisher's projection is updated when a commit is acknowledged. It sends no store request to confirm the head. It still sends requests for what it doesn't have, such as segment blocks or content objects, through the caches of immutable objects.

In a process without the writer session, the read cache keeps the head, as it does today, and the freshness setting below decides how often LoonFS confirms it.

When the writer session closes or is fenced, the publisher's projection is dropped, and reads in that process go back to the read cache.

Why not keep confirming in the writer's process? The confirmation can only disagree with the publisher when another process has become the writer, which fences this session on its next publish. Between the takeover and that publish, a read confirmed with a GET would see the other writer's commits and a read from the projection would not. The design accepts that window and bounds it with the manifest check below, because the alternative costs one request on every read for every writer that is never fenced.

Why not let readers in the writer's process share the read cache with the publisher through seeds, as today? Because a seed is a copy, and every copy needs a rule for when it is stale. The defects above were those rules going wrong.

## Freshness as a read setting

A read takes a freshness setting:

- `latest` (the default): LoonFS confirms the head before the read, as it does today. In the writer's process the confirmation is the publisher's projection, which costs nothing. In another process it is the WAL probe, plus the manifest check once per interval.
- `bounded`, with an interval: LoonFS confirms the head at most once per interval, and reads inside the interval use the head it confirmed last. In the writer's process this is the same as `latest`. In another process a read inside the interval sends no request to confirm the head.

The runtime has a default interval for `bounded` reads (1 s, the same as the manifest check today), and a host can set it per runtime. A read can give its own interval.

A pinned read (a snapshot or a checkpoint) has no freshness setting. Its state is fixed by the pin.

What a `bounded` read promises: it sees every commit that was acknowledged before the last confirmation, and it may miss commits acknowledged since. It never returns state older than a read that completed earlier on the same runtime, because the confirmed head only moves forward.

Why keep `latest` as the default? Because the applications LoonFS is built for read a change back at once: a permission change followed by a read, a fork read by a fresh process, an agent resuming a session its last turn wrote. A host that knows its readers can tolerate lag, such as a listing service or a replica, opts into `bounded` where it matters.

Why a per-read setting and not only a per-runtime one? Because one process serves both kinds of reads. A server answers a stat after a write with `latest` and a directory listing for a dashboard with `bounded`.

Why an interval and not a notification? Object storage has no channel from a writer to a reader in another process. Polling on an interval is the honest model, and the interval states the staleness bound in the API instead of hiding it in a cache.

## Costs

| Read | Today | With this design |
|---|---:|---:|
| `stat` in the writer's process | 1 GET | 0 |
| 1 KiB inline file read in the writer's process | 2 GETs | 1 GET (the content), or 0 when the content cache has it |
| `stat` in another process, `latest` | 1 GET, plus 1 HEAD per second | the same |
| `stat` in another process, `bounded` 1 s | 1 GET, plus 1 HEAD per second | 1 GET and 1 HEAD per second across all reads of the namespace |
| A read after a takeover, in the old writer's process | sees the new writer's commits at once | sees them within the manifest check interval, or when the session is fenced |

The memory used per namespace falls by one anchor in the writer's process. The seed path, the validation lock's sharing rule for seeded anchors, and the invalidations that only existed to keep the anchor in step with the publisher go away.

## What the format and API specs change

The format doesn't change. The API gains the freshness setting on reads that take options (`stat`, `list`, `read_file`, their inode-addressed forms, and the change feed), and the API specification gains a section that states the two promises above. The architecture overview's sentence on cached views ("A reader with a cached view checks the next WAL number for new commits, and it checks for a newer manifest about once a second by default") becomes a description of `latest` reads in a process without the writer session.

## Alternatives considered

Keep the three copies and fix each race as it appears. This is what the last three fixes did. It leaves the seed, the invalidations, and the lock in place, and the next interaction between a maintenance path and the read cache produces the next defect.

A cache at the object-store boundary that absorbs repeated reads. It doesn't address the problem, because the repeated reads ask a mutable question (which manifest and WAL number is current), and a cache of immutable bytes can't answer it. SlateDB built such a cache and is retiring it after measuring it.

A notification from the writer to readers in other processes, through an object the writer touches on every commit. It adds a PUT to every commit to save a GET on every read, and it only helps when reads outnumber writes.

Polling only, with no `latest` reads across processes, which is SlateDB's model (a reader lags by up to its poll interval, and there is no read-your-writes across processes). It is simpler than this design by one setting, and it removes a promise the applications above rely on.
