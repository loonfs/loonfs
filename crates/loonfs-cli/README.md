# loonfs-cli

`loonfs-cli` provides the `loonfs` command for managing profiles, namespaces, and path-based
filesystem operations against LoonFS.

## Command Reference

```text
Loon CLI Command Reference

`loonfs` works with profiles, namespaces, and path-based filesystem operations.

Global flags
  --json
    Emit JSON for supported commands

  --no-input
    Disable interactive prompts

  -h, --help
    Show command help

Setup and configuration
  loonfs init [name] [profile-options]
    Create the initial CLI config and default profile

  loonfs config path
    Print the path to the CLI config file

  loonfs config show
    Show the current CLI config with sensitive values redacted

  loonfs version
    Print the installed CLI version

Profile management
  loonfs profile create <name> [profile-options]
    Create a profile

  loonfs profile list
    List configured profiles

  loonfs profile show [name]
    Show one profile, or the default profile if omitted

  loonfs profile update <name> [update-options]
    Update an existing profile

  loonfs profile remove <name>
    Remove a profile

  loonfs profile use <name>
    Make a profile the default

Namespace management
  loonfs namespace create [--profile <name>] <namespace>
    Create a namespace in a profile

  loonfs use [--profile <name>] <namespace>
    Set the default namespace for a profile

  loonfs current [--profile <name>]
    Show the active profile and default namespace

Filesystem operations
  Most filesystem commands accept:
    --profile <name>
    --namespace <name>

  Commands that commit (put, mkdir, rm, mv, cp, restore) also accept:
    -m, --message <message>
      Annotation recorded on the commit and shown by `loonfs changes`

  loonfs ls [--profile <name>] [--namespace <name>] [path]
    List entries at a path

  loonfs stat [--profile <name>] [--namespace <name>] <path>
    Show metadata for a path

  loonfs cat [--profile <name>] [--namespace <name>] [--revision <n>] <path>
    Print file contents to stdout

  loonfs get [--profile <name>] [--namespace <name>] [--revision <n>] <remote-path> [local-destination]
    Download a remote file

  loonfs put [--profile <name>] [--namespace <name>] <local-path|-> [remote-path] [--force] [-m <message>]
    Upload a local file, or standard input when <local-path> is `-`

  loonfs revisions [--profile <name>] [--namespace <name>] [--limit <n>] [--cursor <cursor>] <path>
    List file revisions newest-first

  loonfs mkdir [--profile <name>] [--namespace <name>] [-m <message>] <path>
    Create a directory

  loonfs restore [--profile <name>] [--namespace <name>] [-m <message>] --revision <n> <path>
    Restore a file to a prior revision

  loonfs rm [--profile <name>] [--namespace <name>] [-m <message>] <path>
    Remove a path

  loonfs mv [--profile <name>] [--namespace <name>] [-m <message>] <source-path> <dest-path>
    Move or rename a path

  loonfs cp [--profile <name>] [--namespace <name>] [-m <message>] <source-path> <dest-path>
    Copy a path

Profile options
  Used by:
    loonfs init
    loonfs profile create

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
    loonfs profile update <name>

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
  `loonfs cat` always streams raw bytes to stdout

  `loonfs get ... -` streams raw bytes to stdout

  `--json` is rejected for streaming output commands

  If `loonfs put` omits the remote path, the CLI uses `/<local-filename>`

  `loonfs put -` reads standard input and needs an explicit remote path,
  because a pipe has no local name to derive one from

  `loonfs put` reads a large file or a pipe once, a piece at a time, so what
  it costs in memory follows the transfer and not the payload's size; a
  file small enough to hold is still uploaded in one request

  If `loonfs get` omits the local destination, the CLI writes to `./<remote-filename>`

  File-oriented commands do not support directory transfers
```
