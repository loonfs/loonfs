# Loon — durable, multiplayer, versioned agent workspaces

> Drop-in `AGENTS.md` snippet for any tool that follows the [agents.md](https://agents.md) convention (Codex, Aider, OpenHands, GitHub Copilot, Gemini CLI, Devin, Windsurf, Zed, Factory, Jules, VS Code, and others). Append this block to an existing `AGENTS.md`, or use this file as-is.

## What `loon` is

`loon` is a CLI for LoonFS, an object-storage-backed durable filesystem for AI agents. It provides:

- **Namespaces** — isolated, versioned workspaces backed by object storage.
- **O(1) forks** — branch a namespace when multiple files or agents would otherwise collide; no byte copy.
- **Per-file revision history + restore** — roll back any path to any prior revision.
- **Multiplayer-by-default** — multiple agents and humans can read and write the same namespace concurrently.

Binary: `loon` on `PATH`. Run `loon help` or `loon <subcommand> --help` for exact shapes.

## When to reach for `loon`

Use `loon` instead of the local filesystem when any of the following is true:

- The user wants a file that future sessions, future agents, or teammates can read — a GTM plan, a design doc / RFC, an exec brief, a customer proposal, an incident postmortem, an ops handoff.
- Multiple agents will run in parallel on the same workspace and need to coordinate.
- The work involves a risky change where rollback should be cheap (iterating on a plan, drafting an RFC, generating a brief, exploring an alternate framing).
- The user mentions namespaces, forking, restoring, or "rolling back what the agent did".

Loon is **not** the right tool for source-code edits — source code needs partial-file/patch primitives Loon does not provide. For code, use the local filesystem and git. For whole-document artifacts (docs, plans, briefs, RFCs), prefer Loon.

For one-shot scratch work, use the local filesystem.

## Setup (if not already configured)

If `loon current --json` reports no configured profile:

- **Zero-config trial** (no credentials):
  ```bash
  loon init default --no-input --mode embedded --store-kind local-fs --root ~/.loonfs/data
  ```
- **Shared / cross-machine setup**: ask the user for S3 / R2 credentials or a hosted Loon server URL + auth token; do not invent them. Put secrets in the environment, not on the command line (argv lands in shell history): the CLI reads `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` and `LOONFS_AUTH_TOKEN` automatically. Then run `loon profile create <name> --mode embedded --store-kind aws-s3 --bucket ... --region ...` (or `--store-kind cloudflare-r2 ...`, or `--mode remote --server-url ...`).

Create or pick a namespace: `loon namespace list`, `loon namespace create <id>`.

## Pattern A — Branch your work

Before a risky, exploratory, or parallel edit, give the work an isolated branch. Pick the lighter primitive when it fits.

**Decision rule:**
- **Single artifact, single agent** → write the alternate to a different filename in the same namespace (A.1).
- **Multiple cross-referencing files**, OR **multiple agents writing to overlapping paths** → fork the namespace (A.2).

**A.1 — Sibling filename (lightweight).** One agent iterating on one artifact. Keep the original at its current path; write the alternate to a clearly-named sibling. No fork, no `--namespace` change.

Examples: `/plans/gtm-q3.md` stays; alternate at `/plans/gtm-q3-marketing-led.md`. Or `/proposals/acme-rev3.md` alongside `/proposals/acme-rev2.md`. At session end, surface both paths so the user can compare and pick. To promote the alternate to canonical, `loon mv` it over the original after confirming — revision history is preserved. For "undo a write to the original," use Pattern B (restore), not a fork.

**A.2 — Fork the namespace (heavyweight).** Multi-file artifacts that cross-reference each other (plan + appendices, design doc + diagrams), or multi-agent fleets that would collide on overlapping paths.

```bash
loon namespace fork <source_namespace> <source_namespace>-<task_or_agent_id>
```

`fork` is O(1) and does not copy bytes. Operate exclusively in the child namespace by passing `--namespace <child>` on every command. Good child namespace names: `gtm-q3-marketing-led`, `rfc-billing-event-sourced`, `acme-proposal-rev3`, `brief-2026-06-04-agent-a`.

For agent fleets where work can be partitioned cleanly by path (one agent per section of a brief, each writing its own file), keep them in the same namespace and skip the fork — Loon handles non-overlapping concurrent writes natively.

## Pattern B — Restore on failure

```bash
loon revisions <path> --namespace <namespace> --json
loon restore <path> --namespace <namespace> --revision <revision_number>
```

`--revision` is required. Show candidate revisions to the user and confirm before restoring.

## Always pass `--namespace`

Never rely on the active default namespace for data commands. Pass `--namespace <namespace>` explicitly on every data command:

```bash
loon ls /docs --namespace <ns> --json
loon stat /docs/report.md --namespace <ns> --json
loon cat /docs/report.md --namespace <ns>
loon get /docs/report.md ./report.md --namespace <ns>
loon put ./report.md /docs/report.md --namespace <ns>
loon mkdir /docs --namespace <ns>
loon mv /docs/a.md /docs/b.md --namespace <ns>
loon cp /docs/a.md /archive/a.md --namespace <ns>
loon rm /docs/old.md --namespace <ns>
```

Use `--json` for parseable commands (`ls`, `stat`, `revisions`, `profile list`, `namespace list`, `current`). Do not combine `--json` with streaming output (`cat`, `get -`).

## Writes — inspect first

Before `put` over an existing path, or before `rm`, `mv`, `cp`, `restore`:

1. `loon stat <path> --namespace <ns> --json` to see size and revision.
2. Show the user; ask before overwriting / deleting / moving.
3. Only pass `--force` to `put` after explicit user confirmation.

After writing, report both the path and the namespace — for example, `Wrote /plans/gtm-q3.md in namespace gtm-2026`.

## Safety rules

- Never upload secrets, credentials, `.env` files, tokens, or private keys.
- Ask before uploading customer data unless the namespace is already approved for that data.
- Report failed Loon commands explicitly (command intent, namespace, path, error summary); do not silently retry.
- For risky changes (>5 writes, bulk edits, deletes), branch first (sibling filename or fork, per the Pattern A decision rule) rather than editing in place.
- Do not use Loon for source-code edits. Use the local filesystem and git for code; use Loon for docs, plans, briefs, RFCs, proposals, postmortems.
