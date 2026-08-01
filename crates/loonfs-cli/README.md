# loonfs-cli

`loonfs-cli` provides the `loonfs` command for managing profiles, namespaces, path-based
filesystem operations, and namespace maintenance against LoonFS.

## Command Reference

```text
Loon CLI Command Reference

`loonfs` works with profiles, namespaces, and path-based filesystem operations.

Global flags
  --json
    Emit JSON instead of human output. A failure prints the same envelope
    on stderr with an `error` object instead of `data`, and that includes a
    command line the parser rejected — an unknown command, a bad value, a
    missing option — which carries `"kind": "parse_error"` and the code
    `invalid_usage`. A parse failure keeps its own exit status 2, distinct
    from the 1 a command that actually ran and failed reports, so the two
    stay separable without reading the message

  --no-input
    Never prompt; fail instead when a value would be asked for

  -h, --help
    Show command help

  -V, --version
    Print the version string, as `loonfs version` does

Selectors
  Every command that reaches a store accepts:
    --profile <name>
      Profile to run against, defaulting to the configured default profile

    --no-retry
      Disable bounded retry of `server_busy`, `commit_queue_full`,
      `shutting_down`, and transport errors

  Commands that address one namespace also accept:
    --namespace <name>
      Namespace to run against, defaulting to the profile's default

  Commands that commit (put, mkdir, rm, mv, cp, restore, undelete) accept:
    -m, --message <message>
      Annotation recorded on the commit and shown by `loonfs changes`

    --commit-id <id>
      Idempotency key for the commit: resubmit the same id to retry safely.
      Generated when absent and reported in the output. The message is part
      of the commit's identity, so the same id with a different message is a
      conflict

Setup and configuration
  loonfs init [name] [profile-options]
    Create the config file with a first profile and make it the default;
    fails when the config file already exists

  loonfs config path
    Print the path to the CLI config file

  loonfs config show
    Print the config file with secrets redacted

  loonfs version
    Print the version, commit, and build date

Profile management
  loonfs profile create <name> [profile-options]
    Add a profile to the config file

  loonfs profile list
    List configured profiles and mark the default

  loonfs profile show [name]
    Show one profile with secrets redacted, or the default profile

  loonfs profile update <name> [update-options]
    Change fields of an existing profile; with no flags on a terminal it
    asks for each field instead

  loonfs profile delete <name>
    Remove a profile from the config file

  loonfs profile use <name>
    Make a profile the default

Namespace management
  loonfs namespace create <namespace>
    Create a new empty namespace

  loonfs namespace fork <source> <new-namespace>
    Fork a namespace into a new one; O(1), no bytes copied

  loonfs namespace delete <namespace> [--expected-head-seq <seq>] [--yes]
    Permanently delete a namespace and retire its id; type the id back at
    the prompt or pass --yes, and --expected-head-seq deletes only while the
    head is still at that sequence

  loonfs use <namespace>
    Set the default namespace for a profile

  loonfs current
    Show the active profile and its default namespace

Reading
  loonfs ls [path] [--limit <n>] [--cursor <cursor>]
    List the entries of a directory, `/` when the path is omitted. Without
    --limit the whole directory is printed, however large; --limit stops
    after that many entries in total and reports a next_cursor, which
    --cursor resumes from

  loonfs stat <path>
    Describe one path: kind, size, revision, and content digest

  loonfs cat <path> [--revision <n>]
    Print a file's bytes to stdout; --revision prints that revision instead
    of the current content

  loonfs get <remote-path> [local-destination] [-r] [--revision <n>] [--force]
    Download one file, or with -r the directory tree rooted at the remote
    path; --force overwrites the local destination and --revision downloads
    an older revision of one file

  loonfs grep <pattern> [--path-prefix <path>] [-i] [--limit <n>]
                        [--max-matches <n>] [--allow-scan] [--allow-stale]
    Search file content through the gram index with a pattern in the Rust
    regex dialect, and print every match; -i ignores ASCII case,
    --path-prefix narrows to a subtree, --limit sizes a page (bounded by the
    deployment's query.grep.max_limit) while --max-matches caps the total
    and reports that it stopped early,
    --allow-scan permits a capped scan for a pattern with no literal bytes,
    and --allow-stale accepts indexed-only results when the unindexed tail
    exceeds the scan budget

Writing
  loonfs put <local-path|-> [remote-path] [-r] [--force] [--expected-revision <n>]
    Upload a local file, standard input when the local path is `-`, or with
    -r the directory tree rooted at the local path; --force replaces an
    existing destination, and --expected-revision replaces only while the
    file is still at that revision, so a raced write fails instead of
    stacking on it

  loonfs mkdir <path> [-p]
    Create a directory; -p creates missing parents as well and succeeds when
    the directory is already there

  loonfs rm <path> [-r]
    Delete a file, or with -r a directory and everything under it as one
    commit; the output carries the handle `loonfs undelete` needs

  loonfs mv <source> <dest> [--force]
    Move or rename a path, a directory included, in one commit; --force
    replaces an existing destination

  loonfs cp <source> <dest> [-r] [--force]
    Copy a file, or with -r the directory tree rooted at the source;
    --force replaces an existing destination

History and recovery
  loonfs revisions <path> [--limit <n>] [--cursor <cursor>]
    List a file's revision history newest first, one page at a time

  loonfs restore <path> --revision <n>
    Write a prior revision's content as the file's next revision

  loonfs trash [--limit <n>] [--cursor <cursor>]
    List recoverable deletions: what was deleted, when, and the exact
    `loonfs undelete` command that recovers each one

  loonfs undelete <path> --inode <id> --deleted-at <seq>
    Recover a deleted file or directory at <path>; --inode and --deleted-at
    come from `loonfs trash` or the `rm` output and name one exact deletion,
    so a stale command cannot cancel a later delete

  loonfs changes [--after <seq>] [--limit <n>]
    List committed changes after a sequence number, from the start of
    retained history when --after is omitted

Maintenance
  loonfs admin run --namespace <ns>... [--job <job>...] [--drain] [--max-steps <n>] [--deadline-ms <ms>]
    Host maintenance for the namespaces named here, continuously until a
    signal; nothing discovers namespaces, so at least one --namespace is
    required. --job selects `metadata`, `core-gc`, `grep-index`, or
    `grep-gc`, all four when omitted. --drain catches every assigned
    namespace up and
    exits instead, which suits cron, and --max-steps or --deadline-ms bound
    the drain, exit nonzero, and report where every namespace stopped

  loonfs admin step [--max-wal-tail-segments <n>] [--retention] [--gc]
    Run one core maintenance step (WAL flush and metadata folds), with
    --retention and --gc adding that work after the flush;
    --max-wal-tail-segments sets how many segments the tail needs before the
    step flushes it, in place of the server's default

  loonfs admin flush
    Flush the WAL tail into a durable segment, whatever the tail's length

  loonfs admin retention-advance
    Advance the retention floor, surrendering replay history below the
    flushed manifest head; file revision history is never affected

  loonfs admin gc [--grace-window-ms <ms>] [--max-objects <n>]
    Run mark-and-sweep collection, looping bounded passes through
    completion; --max-objects examines at most that many candidates and
    returns after one pass, and --grace-window-ms protects objects younger
    than the window

  loonfs admin probe-store
    Prove the profile's object store honours the contract LoonFS depends on
    (create-if-absent, compare-and-swap, visibility, listing, ranged reads)
    and print one line per check; the run writes and deletes objects under a
    scratch prefix it empties afterwards, and exits nonzero when any check
    failed

  loonfs admin checkpoint --name <label> [--ttl-ms <ms>]
    Pin the namespace's current state under a named checkpoint; --ttl-ms
    expires the pin, and an omitted TTL holds it until release

  loonfs admin checkpoint-release <checkpoint-id>
    Release a checkpoint pin

  loonfs admin index-enable [--no-wait] [--max-steps <n>] [--deadline-ms <ms>]
    Enable the gram content index and wait for its backfill to reach the
    sequence the namespace was at when the command started; --no-wait
    returns as soon as the index is enabled, and --max-steps or --deadline-ms
    give up, exit nonzero, and report how far the index got

  loonfs admin index-status
    Report where the gram content index is: disabled, backfilling, or steady
    at a watermark

  loonfs admin index-disable
    Disable the gram content index

  loonfs admin index-gc [--max-objects <n>]
    Collect the namespace's unreferenced gram-index objects, looping bounded
    passes through completion; --max-objects spends at most that many reads
    and returns after one pass

Profile options
  Used by:
    loonfs init
    loonfs profile create

  A field the store requires and the command line omits is asked for on a
  terminal, and is an error under --no-input or --json. Secrets fall back to
  the standard environment variables, so a quickstart never has to put them
  in argv. A variable a provider has no use for is ignored: an exported
  AWS key does not make a gcp-gcs profile fail as though --access-key-id
  had been passed. A flag actually typed on the command line is still
  rejected when it does not apply.

  Mode:
    --mode <embedded|remote>

  Every embedded profile:
    --store-kind <local-fs|aws-s3|cloudflare-r2|gcp-gcs|azure-abs>
    --key-prefix <prefix>              optional

  local-fs store:
    --root <path>

  aws-s3 store:
    --bucket <name>
    --region <region>
    --access-key-id <id>               env AWS_ACCESS_KEY_ID
    --secret-access-key <secret>       env AWS_SECRET_ACCESS_KEY
    --session-token <token>            optional, env AWS_SESSION_TOKEN
    --endpoint-url <url>               optional
    --force-path-style                 optional

  cloudflare-r2 store:
    --bucket <name>
    --account-id <id>
    --endpoint-url <url>
    --access-key-id <id>               env AWS_ACCESS_KEY_ID
    --secret-access-key <secret>       env AWS_SECRET_ACCESS_KEY

  gcp-gcs store:
    --bucket <name>
    --service-account-key-path <path>

  azure-abs store:
    --account-name <name>
    --container-name <name>
    --access-key <key>
    --endpoint-url <url>               optional

  Remote profile:
    --server-url <url>
    --auth-token <token>               optional, env LOONFS_AUTH_TOKEN
    --ca-cert-path <path>              optional, PEM bundle of extra
                                       certificate authorities to trust for
                                       an https server url

Update options
  Used by:
    loonfs profile update <name>

  A flag that does not apply to the profile's own store is rejected, and the
  mode, the store kind, and --force-path-style are fixed when the profile is
  created.

  Shared:
    --key-prefix <prefix>

  local-fs profile updates:
    --root <path>

  aws-s3 profile updates:
    --bucket <name>
    --region <region>
    --access-key-id <id>
    --secret-access-key <secret>
    --session-token <token>
    --endpoint-url <url>

  cloudflare-r2 profile updates:
    --bucket <name>
    --account-id <id>
    --endpoint-url <url>
    --access-key-id <id>
    --secret-access-key <secret>

  gcp-gcs profile updates:
    --bucket <name>
    --service-account-key-path <path>

  azure-abs profile updates:
    --account-name <name>
    --container-name <name>
    --access-key <key>
    --endpoint-url <url>

  Remote profile updates:
    --server-url <url>
    --auth-token <token>
    --ca-cert-path <path>

Behavior notes
  `loonfs cat` always streams raw bytes to stdout

  `loonfs get ... -` streams raw bytes to stdout

  `--json` is rejected for streaming output commands

  If `loonfs put` omits the remote path, the CLI uses `/<local-filename>`

  A remote path ending in `/` names the directory a `put`, `mv`, or `cp`
  lands in, and the item keeps its own name

  A `mv` or `cp` destination that is already a directory means the same
  thing without the trailing slash, so `loonfs cp /report.pdf /docs` lands
  at `/docs/report.pdf`. A destination that does not exist is the exact
  path given, and one that is a file keeps the overwrite rules --force
  governs

  `loonfs mkdir -p` succeeds when the target is already a directory and
  commits nothing; a file at the target is still `path_conflict`

  `loonfs put -` reads standard input and needs an explicit remote path,
  because a pipe has no local name to derive one from

  `loonfs put` reads a large file or a pipe once, a piece at a time, so what
  it costs in memory follows the transfer and not the payload's size; a
  file small enough to hold is still uploaded in one request

  If `loonfs get` omits the local destination, the CLI writes to `./<remote-filename>`

  A local destination that ends in `/` or is an existing directory means the
  download lands inside it under its remote name

  `loonfs get` refuses to overwrite an existing local file without --force,
  and writes through a temporary file so an interrupted download leaves
  nothing truncated at the target

  A recursive put, get, or cp works file by file with bounded concurrency,
  so a partial failure reruns per file; --commit-id names one commit, so
  `put -r` and `cp -r` reject it, and `get -r` rejects --revision for the
  same reason

  `loonfs mv` moves a directory in one commit and takes no -r
```
