---
name: loon
description: Use the `loon` CLI (LoonFS) for durable, shared, versioned agent workspaces. Use when the user asks to read, write, list, move, copy, restore, or hand off files stored in Loon; mentions `loon://`, Loon namespaces, shared agent state, durable handoffs, persisted project docs, artifacts that future agents should read, or files that are not in the local filesystem but sound like a shared workspace.
---

# loon

Use `loon` to read and write durable shared workspace files. Treat Loon state as shared team and agent state: writes, deletes, restores, and moves are immediately visible to other users and agents with access to the same namespace.

Binary: `loon` on `PATH`. Run `loon help` or `loon <subcommand> --help` if the exact CLI shape is unclear.

## Before using Loon

Check that the CLI is installed:

```bash
loon version
```

If Loon is not configured, ask the user for the intended profile/store details before running setup. Do not invent object-store credentials, server URLs, auth tokens, bucket names, or namespaces.

Useful inspection commands:

```bash
loon config show
loon profile list --json
loon namespace list --json
loon current --json
```

`loon config show` redacts sensitive values, but still avoid pasting config output unless it is relevant.

## Pick the namespace deliberately

Pick the namespace from the task subject, not from the current working directory. The user may be in one repo while asking about a different project or shared workspace.

If the namespace is ambiguous:

1. Run `loon namespace list --json` if that will help.
2. Ask the user which namespace to use.
3. Do not fall back to the active default namespace for data operations.

Always include the namespace in Loon references you give back to the user:

```text
loon://<namespace>/<path>
```

## Always pass `--namespace`

For data commands, always pass `--namespace <namespace>` explicitly:

```bash
loon ls /docs --namespace <namespace> --json
loon stat /docs/report.md --namespace <namespace> --json
loon cat /docs/report.md --namespace <namespace>
loon get /docs/report.md ./report.md --namespace <namespace>
loon put ./report.md /docs/report.md --namespace <namespace>
loon mkdir /docs --namespace <namespace>
loon mv /docs/a.md /docs/b.md --namespace <namespace>
loon cp /docs/a.md /archive/a.md --namespace <namespace>
loon rm /docs/old.md --namespace <namespace>
```

Reasons:

- `loon current` and `loon use` are shared local profile state and can be changed by another terminal or agent.
- Explicit namespaces make tool calls self-documenting.
- Explicit namespaces avoid accidental writes to the wrong workspace.

Do not run `loon use <namespace>` from this skill unless the user explicitly asks to change the default namespace.

## Reading

Use JSON for commands you need to parse:

```bash
loon ls [PATH] --namespace <namespace> --json
loon stat <PATH> --namespace <namespace> --json
loon revisions <PATH> --namespace <namespace> --json
```

Use streaming commands without `--json`:

```bash
loon cat <PATH> --namespace <namespace>
loon cat <PATH> --namespace <namespace> --revision <revision>
loon get <REMOTE_PATH> <LOCAL_PATH> --namespace <namespace>
loon get <REMOTE_PATH> <LOCAL_PATH> --namespace <namespace> --revision <revision>
```

`loon cat` and `loon get ... -` stream raw bytes. Do not combine streaming output commands with `--json`.

## Writing

Before creating or replacing a file, inspect the destination:

```bash
loon stat <REMOTE_PATH> --namespace <namespace> --json
```

Rules:

1. If the destination does not exist, use `loon put <LOCAL_PATH> <REMOTE_PATH> --namespace <namespace>`.
2. If the destination exists, show the current path, size, and revision to the user and ask before overwriting.
3. Only pass `--force` after explicit overwrite confirmation.
4. Create parent directories with `loon mkdir <REMOTE_DIR> --namespace <namespace>` when needed.

For generated handoff files, prefer clear paths such as:

```text
/handoffs/<date>-<topic>.md
/plans/<task>.md
/artifacts/<task>/<filename>
/decisions/<date>-<topic>.md
```

After writing, report the full reference:

```text
Wrote `loon://<namespace>/<path>`.
```

## Moving, copying, deleting, and restoring

Before `rm`, `mv`, `cp`, or `restore`, run `stat` or `revisions` as appropriate and show the target to the user.

Deletion is visible to other users and agents immediately:

```bash
loon rm <PATH> --namespace <namespace>
```

Only delete after explicit confirmation.

Restoring a prior revision changes the current file contents:

```bash
loon revisions <PATH> --namespace <namespace> --json
loon restore <PATH> --namespace <namespace> --revision <revision>
```

Only restore after the user confirms the target path and revision number.

## Forking workspaces

Use namespace forks when the user wants an alternate attempt, experiment, or rollback-safe branch of a workspace:

```bash
loon namespace fork <source_namespace> <new_namespace>
```

After forking, use the new namespace explicitly in all data commands. Explain that the fork starts with the source workspace contents and then diverges independently.

## Safety rules

- Never upload secrets, private keys, tokens, credentials, `.env` files, or similarly sensitive material to Loon.
- Ask before uploading customer data, personal data, or confidential documents unless the user has already made clear that the namespace is approved for that data.
- Do not rely on local files as durable handoffs when the user asks for future agents or teammates to see the result. Write the handoff to Loon instead.
- Do not hide failed writes. If a Loon command fails, report the command intent, the namespace, the path, and the error summary.

## Quick checklist

Before running a Loon data command:

1. Is the namespace known and explicit?
2. Is this a parseable command that should use `--json`, or a raw streaming command that must not?
3. For writes, overwrites, deletes, restores, moves, or copies, did you inspect the current target first?
4. If the operation is destructive or replaces content, did the user confirm it?
5. Will the final answer include the full `loon://<namespace>/<path>` reference when something was written?
