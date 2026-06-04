# Paste-in install prompt — install Loon and the Loon agent skill

If your coding agent can run shell commands, prefer the one-liner from the [README](../../README.md#use-loon-with-your-agent). It installs both the binary and the agent skill in a single step.

Use the prompt below when your agent **cannot** run shell commands directly (or you would rather have the agent walk through the install). Copy everything in the fenced block and paste it into the agent.

```text
Install Loon and add the Loon agent skill for me.

Source of truth: https://github.com/loonfs/loonfs

Step 1 — Identify which agent / IDE I am running you in. Use the following destination map. If you cannot identify with high confidence, ask me; do not guess.

  - Claude Code                -> $HOME/.claude/skills/loon/SKILL.md
    Source: https://raw.githubusercontent.com/loonfs/loonfs/main/examples/SKILL.md
  - Codex (OpenAI CLI)         -> $HOME/.codex/AGENTS.md
    Source: https://raw.githubusercontent.com/loonfs/loonfs/main/examples/agents/agents-md/AGENTS.md
  - Aider, OpenHands, GitHub
    Copilot, Gemini CLI, or
    anything else AGENTS.md-
    aware                      -> ./AGENTS.md                       (in the current repo)
    Source: https://raw.githubusercontent.com/loonfs/loonfs/main/examples/agents/agents-md/AGENTS.md

Step 2 — Check whether the `loon` binary is already installed: run `loon version`.

Step 3 — If `loon` is not installed, install it using the first appropriate method:
  - Preferred install script:
      curl -fsSL https://install.loonfs.com | sh
    To install both the binary AND the skill in one step (replaces the rest of this prompt for the binary + skill install), use:
      curl -fsSL https://install.loonfs.com | sh -s -- --with-skill <agent>
    where <agent> is one of: claude-code, codex, agents-md
  - If Homebrew is available and the install script is not appropriate:
      brew install loonfs/tap/loon
  - If neither works, build from source:
      clone https://github.com/loonfs/loonfs, run `cargo build -p loon-cli`,
      and place the resulting `loon` binary somewhere on PATH.

  If network access, filesystem permissions, or command approval is required, ask me for approval with a concise reason.

Step 4 — Verify the binary install: run `loon version`.

Step 5 — Install the agent skill file at the destination from Step 1.
  - Create the parent directory if it does not exist.
  - Download the matching Source URL from Step 1.
  - Save it at the destination from Step 1.
  - If the destination file already exists, do not overwrite silently. Show me that it exists and ask whether to replace it with the current public skill, append a Loon section, or leave it alone.

Step 6 — Verify the skill file contains the expected header (a YAML frontmatter block for SKILL.md and .mdc files, or a "# Loon" heading for AGENTS.md).

Step 7 — Tell me:
  - the exact destination path the skill was installed to,
  - whether I need to restart the agent or start a new thread for the skill to be discovered.

Step 8 — Do not run `loon init`, `loon profile create`, or any command that requires credentials unless I provide profile details. If I am only evaluating Loon, the safe zero-config trial command is:
    loon init default --no-input --mode embedded --store-kind local-fs --root ~/.loonfs/data
  Do not run that without my confirmation.

After setup, I should be able to ask:
  "Use Loon to draft a short Q3 GTM plan and an alternate framing into the same namespace; report both file paths along with the namespace they live in."
```
