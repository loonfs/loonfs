# Customer Prompt: Install Loon And Add The Loon Skill

Copy and paste this prompt into Codex or another coding agent to install the Loon CLI and add the Loon skill locally.

```text
Install Loon and add the Loon agent skill for me.

Goal:
- Make the `loon` CLI available on PATH.
- Use the public LoonFS repo as the source of truth: `https://github.com/loonfs/loonfs`.
- Install the Loon skill from that repo into my local agent skills directory at `$HOME/.agents/skills/loon/SKILL.md`.
- Do not configure a Loon profile unless I provide the required hosted service or object-store credentials.

Steps:
1. Check whether Loon is already installed by running:
   `loon version`

2. If `loon` is not installed, install it using the first appropriate method documented by the LoonFS repo:
   - Preferred install script from the Loon README:
     `curl -fsSL https://install.loonfs.com | sh`
   - If Homebrew is available and the install script is not appropriate:
     `brew install loonfs/tap/loon`
   - If neither works, build from source:
     clone `https://github.com/loonfs/loonfs`, run `cargo build -p loon-cli`, and place the resulting `loon` binary somewhere on PATH.

   If network access, filesystem permissions, or command approval is required, ask me for approval with a concise reason.

3. Verify installation:
   `loon version`

4. Install the Loon skill:
   - Create `$HOME/.agents/skills/loon`.
   - Download:
     `https://raw.githubusercontent.com/loonfs/loonfs/main/examples/SKILL.md`
   - Save it as:
     `$HOME/.agents/skills/loon/SKILL.md`

5. If `$HOME/.agents/skills/loon/SKILL.md` already exists, do not overwrite it silently. Show me that it exists and ask whether to replace it with the current public Loon skill.

6. Verify the skill file has YAML frontmatter with:
   - `name: loon`
   - a `description:` field

7. Tell me the exact installed skill path. If Codex does not detect the new skill immediately, tell me to restart Codex or start a new thread.

8. After the skill is installed, do not run `loon init`, `loon profile create`, or any command that requires credentials unless I provide the profile details. If I provide credentials or a hosted Loon server URL, help me configure Loon using the CLI.

After setup, I should be able to ask:
`Use $loon to create a durable handoff in namespace <namespace>.`
```
