# loon-cli

`loon-cli` provides the `loon` command for managing profiles, namespaces, and path-based
filesystem operations against LoonFS.

## Command Reference

```text
Loon CLI Command Reference

`loon` works with profiles, namespaces, and path-based filesystem operations.

Global flags
  --json
    Emit JSON for supported commands

  --no-input
    Disable interactive prompts

  -h, --help
    Show command help

Setup and configuration
  loon init [name] [profile-options]
    Create the initial CLI config and default profile

  loon config path
    Print the path to the CLI config file

  loon config show
    Show the current CLI config with sensitive values redacted

  loon version
    Print the installed CLI version

Profile management
  loon profile create <name> [profile-options]
    Create a profile

  loon profile list
    List configured profiles

  loon profile show [name]
    Show one profile, or the default profile if omitted

  loon profile update <name> [update-options]
    Update an existing profile

  loon profile remove <name>
    Remove a profile

  loon profile use <name>
    Make a profile the default

Namespace management
  loon namespace create [--profile <name>] <namespace>
    Create a namespace in a profile

  loon namespace list [--profile <name>]
    List namespaces for a profile

  loon use [--profile <name>] <namespace>
    Set the default namespace for a profile

  loon current [--profile <name>]
    Show the active profile and default namespace

Filesystem operations
  Most filesystem commands accept:
    --profile <name>
    --namespace <name>

  loon ls [--profile <name>] [--namespace <name>] [path]
    List entries at a path

  loon stat [--profile <name>] [--namespace <name>] <path>
    Show metadata for a path

  loon cat [--profile <name>] [--namespace <name>] <path>
    Print file contents to stdout

  loon get [--profile <name>] [--namespace <name>] <remote-path> [local-destination]
    Download a remote file

  loon put [--profile <name>] [--namespace <name>] <local-path> [remote-path] [--force]
    Upload a local file

  loon rm [--profile <name>] [--namespace <name>] <path>
    Remove a path

  loon mv [--profile <name>] [--namespace <name>] <source-path> <dest-path>
    Move or rename a path

  loon cp [--profile <name>] [--namespace <name>] <source-path> <dest-path>
    Copy a path

Profile options
  Used by:
    loon init
    loon profile create

  Generic profile selection:
    --mode <embedded|remote>

  Embedded/direct profile options:
    --store-kind <local-fs|aws-s3|cloudflare-r2>
    --key-prefix <prefix>

  Local-fs provider:
    --root <path>

  AWS S3 store:
    --bucket <name>
    --region <region>
    --access-key-id <id>
    --secret-access-key <secret>
    --endpoint-url <url>
    --session-token <token>
    --force-path-style

  Cloudflare R2 store:
    --bucket <name>
    --account-id <id>
    --endpoint-url <url>
    --access-key-id <id>
    --secret-access-key <secret>

  Remote profile options:
    --server-url <url>
    --auth-token <token>

Update options
  Used by:
    loon profile update <name>

  Shared:
    --key-prefix <prefix>

  Local-fs provider updates:
    --root <path>

  AWS S3 profile updates:
    --bucket <name>
    --region <region>
    --access-key-id <id>
    --secret-access-key <secret>
    --endpoint-url <url>
    --session-token <token>
    --key-prefix <prefix>

  Cloudflare R2 profile updates:
    --bucket <name>
    --account-id <id>
    --endpoint-url <url>
    --access-key-id <id>
    --secret-access-key <secret>
    --key-prefix <prefix>

  Remote profile updates:
    --server-url <url>
    --auth-token <token>

Behavior notes
  `loon cat` always streams raw bytes to stdout

  `loon get ... -` streams raw bytes to stdout

  `--json` is rejected for streaming output commands

  If `loon put` omits the remote path, the CLI uses `/<local-filename>`

  If `loon get` omits the local destination, the CLI writes to `./<remote-filename>`

  File-oriented commands do not support directory transfers
```
