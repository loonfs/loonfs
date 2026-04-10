---
name: loon
description: Read and write team-wide documents via the `loon` CLI (LoonFS) for content that should be persisted, or shared with teammates or other agents. Use when (1) the user asks to read/write/list/move docs stored in loon, (2) the user mentions "team docs", "shared docs", or docs that "other agents" should see, (3) the user references a file path that isn't in the local filesystem but sounds like a shared workspace. Handles picking the correct namespace for the task.
---

# loon

`loon` is a CLI for LoonFS, an object-storage-backed filesystem for documents that are shared across teammates and agents. Treat everything in `loon` as **shared team state**: writes and deletes are immediately visible to other people and other agents.

Binary: `loon` (on `PATH`). Run `loon help` or `loon <subcommand> --help` if anything below is unclear.

## Namespaces

Pick the namespace to use based on the **subject of the task**, not on the current working directory — the user may be in one repo while asking about the other project.

- **`{SAMPLE_NAMESPACE}`** — {add description of what this namespace ought to be used for}.

If the task's subject is ambiguous (e.g. the user just says "read `/notes/foo.md`" with no project signal), **ask the user which namespace before running any command**. Do not guess, and do not fall back to the current active namespace.

## Always pass `--namespace` explicitly

Every data subcommand (`ls`, `stat`, `cat`, `get`, `put`, `rm`, `mv`, `cp`) accepts `--namespace <NAMESPACE>`. Always pass it. Reasons:

- The active namespace (`loon current`) is global state shared with other terminals and agents. Relying on it is a race.
- Explicit `--namespace` makes every tool call self-documenting in the transcript.
- It removes any dependency on what another process may have done with `loon use`.

**Do not run `loon use <ns>` from inside this skill.** That mutates the shared default and can break a parallel session. If the user explicitly asks to change the default, then (and only then) run `loon use`.

At the very start of a loon task, you may run `loon current --json` once just to record the active default for context — but every subsequent data command still passes `--namespace` explicitly.

## Reading

```bash
loon ls [PATH] --namespace <ns> --json      # list a directory; PATH defaults to /
loon stat <PATH> --namespace <ns> --json    # size, revision, content digest
loon cat  <PATH> --namespace <ns>           # print file contents (omit --json for raw text)
loon get  <REMOTE> <LOCAL> --namespace <ns> # download to a local path
```

Prefer `--json` for `ls` / `stat` when you need to parse the result. Prefer plain `cat` (no `--json`) when the user wants to read the document.

## Writing

```bash
loon put <LOCAL> <REMOTE> --namespace <ns>           # upload
loon put <LOCAL> <REMOTE> --namespace <ns> --force   # overwrite an existing file
loon mv  <SRC>  <DST>    --namespace <ns>
loon cp  <SRC>  <DST>    --namespace <ns>
loon rm  <PATH>          --namespace <ns>
```

Rules:

1. **Before every `put`**, run `loon stat <REMOTE> --namespace <ns> --json` to check whether the remote path already exists.
   - If it does not exist, `put` without `--force`.
   - If it exists, tell the user the current size/revision and ask whether to overwrite. Only pass `--force` after explicit confirmation.
2. **Before `rm`**, show the target (path, size, revision from `stat`) and confirm with the user. Deletion affects teammates and other agents immediately.
3. **Before `mv` / `cp` over an existing destination**, stat the destination first and confirm with the user if it would overwrite.
4. **Never put secrets, credentials, customer data, or `.env` files** into loon. If a user asks you to, stop and flag it.

## Handing off to another agent

When the goal is to produce a document that another agent (or a future Claude session) should read, prefer writing it via `loon put` instead of leaving it in the local filesystem. After the upload, tell the user the **full** reference, including namespace, e.g.:

> Wrote to `loon://customer-success/handoffs/2026-04-10-support-notes.md` (namespace `customer-success`).

Downstream agents need both the path and the namespace to retrieve it.

## Quick checklist

Before running any loon command:

1. Did I identify which namespace this task belongs to?
2. If ambiguous, did I ask the user?
3. Am I passing `--namespace <ns>` explicitly on this command?
4. If this is a write or delete, did I `stat` first and (for overwrite/delete) get explicit user confirmation?
