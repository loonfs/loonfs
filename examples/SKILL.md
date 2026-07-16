---
name: loon
description: Use the `loon` CLI (LoonFS) for durable, shared, versioned agent workspaces. Use when the user asks to read, write, list, move, copy, fork, restore, or hand off files stored in Loon; mentions Loon namespaces, shared agent state, durable handoffs, persisted project docs, artifacts that future agents should read, rolling back an agent's changes, branching a workspace for an experiment, or files that are not in the local filesystem but sound like a shared or persistent workspace.
---

# loon

Use `loon` to give an agent task a durable, versioned, shared workspace. Treat Loon state as shared team and agent state: writes, deletes, restores, and moves are immediately visible to other users and agents with access to the same namespace.

Binary: `loon` on `PATH`. Run `loon help` or `loon <subcommand> --help` if the exact CLI shape is unclear.

## When to reach for Loon

Reach for Loon, not the local filesystem, when any of the following is true:

- The user wants a file that future sessions, future agents, or teammates can read: a GTM plan, a design doc / RFC, an exec brief, a customer proposal, an incident postmortem, an ops handoff.
- The user is running, or about to run, multiple agents in parallel and wants them to coordinate without stepping on each other.
- The work involves a risky change where a rollback should be cheap (iterating on a plan, drafting an RFC, generating a brief, exploring an alternate framing).
- The user mentions branching, forking, restoring, "checkpointing the workspace", or "rolling back what the agent did".
- The user references a Loon namespace name (or a path inside one) without a local filesystem path.

Loon is **not** the right tool for editing source code — source-code workflows need partial-file/patch primitives that Loon does not provide. For code edits, use the local filesystem and git. For whole-file artifacts (docs, plans, briefs, RFCs, proposals, postmortems), prefer Loon.

For one-shot scratch work the user does not need later, use the local filesystem.

## Before using Loon

Check the CLI is installed with `loon version`. If `loon current --json` reports no configured profile, Loon is not yet set up. Two paths:

1. **Zero-config trial** (no credentials). For evaluating Loon or working durably on this machine only:

   ```bash
   loon init default --no-input --mode embedded --store-kind local-fs --root ~/.loonfs/data
   ```

   Tell the user: data lives at `~/.loonfs/data`; durable on this machine but not shareable across machines until they switch to an S3/R2 profile or hosted Loon.

2. **Durable / shared setup**. Ask the user for S3/R2 credentials (provider, bucket, region, access key, secret) or a hosted Loon server URL + auth token. Put secrets in the environment, not on the command line (argv lands in shell history): the CLI reads `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` and `LOONFS_AUTH_TOKEN` automatically. Then run `loon profile create <name> --mode embedded --store-kind aws-s3 --bucket ... --region ...` (or `--store-kind cloudflare-r2 ...`, or `--mode remote --server-url ...`). Do not invent credentials.

After init, create a namespace with `loon namespace create <namespace_id>`, or use the one the user names. Namespaces are addressed by id — there is no listing command.

## Two canonical patterns

Reach for these first when a user describes an agent-workspace task. Use them before falling back to standard read/write operations.

### Pattern A — Branch your work

Before a risky, exploratory, alternate-framing, or parallel edit, give the work an isolated branch. Loon offers two branching primitives — pick the lighter one when it fits.

**Decision rule:**
- **Single artifact, single agent iterating** → write the alternate to a different filename in the same namespace (A.1).
- **Multiple files that must stay consistent**, OR **multiple agents writing to overlapping paths**, OR **a coherent alternative tree that should be reviewable as one unit** → fork the namespace (A.2).

#### A.1 — Sibling filename in the same namespace (lightweight)

Use when one agent is iterating on one artifact. The original stays at its current path; the alternate goes to a clearly-named sibling.

Examples:
- Iterating on a Q3 GTM plan → `/plans/gtm-q3.md` stays; write the alternate to `/plans/gtm-q3-marketing-led.md`.
- Re-pitching a customer proposal → `/proposals/acme-rev3.md` alongside `/proposals/acme-rev2.md`.
- Alternate framing of an exec brief → `/briefs/q3-positioning.md` alongside `/briefs/q3-positioning-v2.md`.

```bash
loon get /plans/gtm-q3.md ./gtm-q3.md --namespace <ns>
# revise ./gtm-q3.md locally into the alternate
loon put ./gtm-q3.md /plans/gtm-q3-marketing-led.md --namespace <ns>
```

At session end, surface both paths so the user can compare. To promote the alternate to canonical after the user confirms, copy it over the original: `loon cp /plans/gtm-q3-marketing-led.md /plans/gtm-q3.md --force`. The copy lands as a new revision of the original file, so the original's full history stays restorable; optionally `loon rm` the alternate afterwards. Do not `mv` over the original — a move replaces the file's identity and makes the original's revision history unreachable. For "I wrote v2 into the original and want to undo," use Pattern B (restore), not a fork.

#### A.2 — Fork the namespace (heavyweight)

Use when the work spans multiple files that cross-reference each other (a plan plus its appendices, a design doc plus diagrams, a brief composed of multiple section files), or when multiple agents are running in parallel and would otherwise collide on overlapping paths.

```bash
loon namespace fork <source_namespace> <source_namespace>-<task_or_agent_id>
```

`fork` is O(1) and does not copy bytes — the child shares the source's content store and diverges in metadata only. Good child names: `gtm-q3-marketing-led`, `rfc-billing-event-sourced`, `acme-proposal-rev3`, `brief-2026-06-04-agent-a`.

After forking, **operate exclusively in the child namespace** — pass `--namespace <child>` on every data command. When the work is done: promote the child (leave it canonical), merge selected files back into the source by `cat` + `put`, or abandon the fork (no cleanup needed; abandoned forks cost nothing in content storage).

For agent fleets: one child namespace per agent. LoonFS is single-writer per namespace — reads are concurrent from anywhere, but only one writer owns a namespace at a time. Writing through a shared Loon server (`--mode remote`) is safe for any number of parallel agents because the server is the writer and serializes their commits; in embedded mode each CLI process is the writer, so parallel embedded writes to one namespace fail with `writer_fenced`. Forks avoid the contention in every mode.

### Pattern B — Restore on failure

Use when a write produced wrong output or an agent's earlier action needs to be undone.

```bash
loon revisions <path> --namespace <ns> --json
loon restore   <path> --namespace <ns> --revision <revision_number>
```

Show the user the candidate revisions (number, timestamp, size). Pick the revision that predates the bad write. Confirm with the user before running `restore` — `--revision` is required. Restoring writes a new revision whose contents equal the chosen prior revision; earlier revisions remain restorable.

For multi-file rollbacks: repeat per path. If the scope is large (more than ~5 files), surface that and ask whether to proceed file-by-file or to fork from a known-good revision and continue forward from there.

## Namespace discipline

Pick the namespace from the task subject, not from the current working directory — the user may be in one repo while asking about a different workspace. If the namespace is ambiguous, ask the user — namespaces are addressed by id and there is no listing command. Do not fall back to the active default.

Always pass `--namespace <ns>` explicitly on every data command. The active default (`loon current`) is shared local profile state that another terminal or agent can change. Do not run `loon use <ns>` from this skill unless the user explicitly asks to change the default.

When reporting results to the user, always state both the namespace and the path so the reference is unambiguous — for example, `Wrote /plans/gtm-q3.md in namespace gtm-2026`.

## Standard operations

All data commands require `--namespace <ns>`. Use `--json` only for commands you need to parse; raw streaming output (`cat`, `get -`) cannot use `--json`.

```bash
# read
loon ls        [path]   --namespace <ns> --json
loon stat      <path>   --namespace <ns> --json
loon revisions <path>   --namespace <ns> --json
loon cat       <path>   --namespace <ns> [--revision <n>]
loon get       <remote> <local> --namespace <ns> [--revision <n>]

# write
loon put       <local>  <remote> --namespace <ns> [--force]
loon mkdir     <dir>    --namespace <ns>
loon mv        <src>    <dst>    --namespace <ns> [--force]
loon cp        <src>    <dst>    --namespace <ns> [--force]
loon rm        <path>   --namespace <ns>
```

**Safe retries.** Every write accepts `--commit-id <id>`. When a write fails in an uncertain way (timeout, `server_busy`, killed process), retry it with the same commit id and identical arguments: a retry of a commit that already landed returns the original result instead of writing twice, and a reused id with different content fails with `commit_id_reuse_conflict`. Generate one id per logical write (for example `c-$(uuidgen)`) and hold it across attempts.

**Before any destructive operation** (`put` over an existing path, `mv`, `cp`, `rm`, `restore`): run `loon stat` first, show the user the current size and revision, and ask before proceeding. `--force` requires explicit user confirmation. Treat `rm` — and `mv --force` over an existing file — as permanent: the deleted or replaced file's revision history stops being reachable from its path, and there is no undelete.

For generated artifacts, prefer clear paths:

```text
/plans/<topic>-<date>.md          # GTM plans, project plans, migration plans
/rfcs/<topic>-<date>.md           # design docs, RFCs, ADRs, implementation plans
/briefs/<topic>-<date>.md         # exec briefs, customer briefs, account briefs
/proposals/<customer>-<date>.md   # customer-facing proposals or pitches
/handoffs/<date>-<topic>.md       # cross-agent or cross-session handoffs
/decisions/<date>-<topic>.md      # decision logs, postmortem notes
/artifacts/<task>/<filename>      # anything else generated by a task
```

After writing, report both the path and the namespace — for example, `Wrote /plans/gtm-q3.md in namespace gtm-2026`.

## Safety rules

- Never upload secrets, private keys, tokens, credentials, `.env` files, or similarly sensitive material to Loon.
- Ask before uploading customer data, personal data, or confidential documents unless the namespace is already approved for that data.
- Do not rely on local files as durable handoffs when the user asks for future agents or teammates to see the result. Write the handoff to Loon instead.
- Do not hide failed writes. If a Loon command fails, report the command intent, namespace, path, and error summary.
- For risky changes (>5 file writes, bulk edits, deletes), branch first (sibling filename or fork, per the Pattern A decision rule) rather than editing in place.
- Do not use Loon for source-code edits. Use the local filesystem and git for code; use Loon for docs, plans, briefs, RFCs, proposals, postmortems.
