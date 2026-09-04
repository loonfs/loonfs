# loonfs-cli

`loonfs-cli` provides the `loonfs` command for managing profiles, namespaces, path-based
filesystem operations, and namespace maintenance against LoonFS.

## Shell completion

Generate completion scripts for Bash, Zsh, Fish, PowerShell, or Elvish with:

```sh
loonfs completion --shell zsh
```

When omitted, `--shell` defaults from `$SHELL`.

For example, enable Zsh completion in the current shell with:

```sh
source <(loonfs completion --shell zsh)
```

## Command Reference

```text
LoonFS CLI Command Reference

`loonfs` works with profiles, namespaces, and path-based filesystem operations.

Global flags
  --config <path>
    Config file to use, ahead of LOONFS_CONFIG and the default location

  --json
    Emit JSON instead of human output. A failure prints the same envelope
    on stderr with an `error` object instead of `data`, and that includes a
    command line the parser rejected — an unknown command, a bad value, a
    missing option — which carries `"kind": "parse_error"` and the code
    `invalid_usage`. A parse failure keeps its own exit status 2, distinct
    from the 1 a command that actually ran and failed reports, so the two
    stay separable without reading the message

  --no-retry
    Disable bounded retry of `server_busy`, `commit_queue_full`,
    `shutting_down`, and transport errors

  --no-input
    Never prompt; fail instead when a value would be asked for

  --no-progress
    Say nothing about a transfer while it runs, on a terminal or under
    --json

  -h, --help
    Show command help

  -V, --version
    Print the version string, as `loonfs version` does

Selectors
  Every command that reaches a store accepts:
    --profile <name>
      Profile to use. If omitted, the CLI uses LOONFS_PROFILE and then the
      configured default.
    LOONFS_PROFILE=<name>
      Default profile for the current environment.

  Commands that address one namespace also accept:
    --namespace <name>
      Namespace to run against, defaulting to the profile's default
    LOONFS_NAMESPACE=<name>
      Environment default; --namespace wins, and the profile default follows

  Commands that commit (put, mkdir, rm, mv, cp, restore, undelete) accept:
    -m, --message <message>
      Annotation recorded on the commit and shown by `loonfs changes`

    --commit-id <id>
      Idempotency key for the commit: resubmit the same id to retry safely.
      Generated when absent and reported in the output. The message is part
      of the commit's identity, so the same id with a different message is a
      conflict

Setup and configuration
  loonfs init
    Interactively create the config file with a first profile and make it
    the default; fails when the config file already exists

  loonfs config path
    Print the config file this invocation uses and which rule chose it

  loonfs config show
    Print the config file with secrets redacted

  loonfs version
    Print the version

Config file location
  One config file per invocation, chosen by the first rule that applies:
    1. --config <path>
    2. LOONFS_CONFIG=<path>
    3. $XDG_CONFIG_HOME/loonfs/config.toml, when XDG_CONFIG_HOME names an
       absolute directory
    4. ~/.loonfs/config.toml
  Profile selection uses --profile, then LOONFS_PROFILE=<name>, then the
  configured default. Namespace selection uses --namespace, then
  LOONFS_NAMESPACE=<name>, then the profile default.

  A path given to --config or LOONFS_CONFIG is the file this invocation
  uses whether or not it is there yet, and `loonfs init` creates the
  directories it needs. That is the way past a config file this build will
  not read: an unknown key is an error, so every command that reads the
  file fails until it is fixed or stepped around, and each of those errors
  names the file, what in it went wrong, and both ways past it.

  `loonfs config path` reads nothing, so it keeps answering while the file
  it names is unreadable.

  While XDG_CONFIG_HOME is set but holds no config of its own, an existing
  ~/.loonfs/config.toml stays in use and `loonfs config path` names the
  preferred path to move it to.

Profile management
  loonfs profile create s3 <name> [s3-options]
  loonfs profile create r2 <name> [r2-options]
  loonfs profile create gcs <name> [gcs-options]
  loonfs profile create azure <name> [azure-options]
  loonfs profile create local <name> [local-options]
  loonfs profile create remote <name> [remote-options]
    Add a provider-specific profile to the config file

  loonfs profile list
    List configured profiles and mark the default

  loonfs profile show [name]
    Show one profile with secrets redacted. Without a name, show the profile
    selected by LOONFS_PROFILE or the configured default.

  loonfs profile update <provider> <name> [update-options]
    Change fields of an existing profile; with no flags on a terminal it
    asks for each field instead

  loonfs profile delete <name>
    Remove a profile from the config file

  loonfs profile use <name>
    Make a profile the default

Namespace management
  loonfs namespace create <namespace>
    Create a new empty namespace

  loonfs namespace show [namespace]
    Show the selected namespace's status

  loonfs namespace fork <source> <new-namespace>
    Fork a namespace into a new one; O(1), no bytes copied

  loonfs namespace delete <namespace> [--expected-head-seq <seq>] [--yes]
    Permanently delete a namespace and retire its id; type the id back at
    the prompt or pass --yes, and --expected-head-seq deletes only while the
    head is still at that sequence

  loonfs use <namespace>
    Set the default namespace for a profile

  loonfs current
    Show the selected profile and namespace

Snapshot management
  loonfs snapshot create <namespace> --name <label> --ttl-ms <ms>
    Save the namespace's current state for the requested time

  loonfs snapshot list <namespace> [--limit <n>] [--page-size <n>]
                                   [--cursor <cursor>] [--all] [--jsonl]
    List snapshots that are still available

  loonfs snapshot extend <namespace> <snapshot-id> --ttl-ms <ms>
    Keep a snapshot available for longer

  loonfs snapshot release <namespace> <snapshot-id>
    Release a snapshot. Repeating the command succeeds.

Pagination
  ls, grep, revisions, trash, changes, snapshot list, and maintenance checkpoint
  list return one page by default. --limit sets the total maximum across
  pages, --page-size controls each request, and --cursor resumes a previous
  result. --all fetches every page for human output. --json returns a bounded
  document and cannot be combined with --all. Use --jsonl to stream every
  result. changes uses --after instead of --cursor.

Reading
  loonfs ls [path] [--limit <n>] [--page-size <n>] [--cursor <cursor>]
                   [--all] [--jsonl] [--snapshot-id <id>]
    List the entries of a directory, or `/` when the path is omitted

  loonfs stat <path> [--snapshot-id <id>]
  loonfs stat --inode <inode-id>
    Describe a path or visible inode. The output includes its kind, size,
    revision, content digest, and attributes

  loonfs cat <path> [--revision <n>] [--snapshot-id <id>]
    Print a file's bytes to stdout; --revision prints that revision instead
    of the current content

  loonfs get <remote-path> [local-destination] [-r] [--revision <n>]
             [--snapshot-id <id>] [--force]
    Download one file, or use -r to download a directory. For recursive
    downloads, the local destination is the root of the downloaded contents.
    Existing files are not overwritten unless --force is set. --revision
    downloads an older revision of one file

  loonfs grep <pattern> [--path-prefix <path>] [-i] [--limit <n>]
                        [--page-size <n>] [--cursor <cursor>] [--all] [--jsonl]
                        [--allow-scan] [--allow-stale]
    Search file content through the gram index with a pattern in the Rust
    regex dialect. -i ignores ASCII case, --path-prefix limits the search to
    a subtree, --allow-scan permits a bounded scan for patterns with no
    literal bytes, and --allow-stale permits results that omit an unindexed
    tail that is too large to scan

Writing
  Every writing command accepts --actor-kind <user|service|system> together
  with --actor-id <stable-id>. Flags override LOONFS_ACTOR_KIND and
  LOONFS_ACTOR_ID, then the profile actor. Without any of them,
  service/loonfs-cli identifies the tool, not the human running it.

  loonfs put <local-path|-> [remote-path] [-r] [--force]
             [--expected-inode-id <id>] [--expected-revision <n>]
             [--actor-kind <kind> --actor-id <id>]
    Upload a local file, standard input when the local path is `-`, or with
    -r the directory tree rooted at the local path. --force replaces an
    existing destination. The expected value flags replace only if the file
    still has the inode and optional revision that you read. A revision guard
    requires the inode guard, and neither can be used with -r

  loonfs mkdir <path> [-p] [--actor-kind <kind> --actor-id <id>]
    Create a directory; -p creates missing parents as well and succeeds when
    the directory is already there

  loonfs annotate <path> [--set key=value]... [--remove key]...
                         [--attributes-json '<update>']
                         [--expected-inode-id <n>]
                         [--expected-attributes-revision <n>]
                         [--actor-kind <kind> --actor-id <id>]
    Write and remove attributes on a file or directory. --set takes
    key=value and splits on the first `=`, --remove takes a key, and both can
    be repeated. Attribute values must be strings. --attributes-json accepts
    an update document in the form {"set": {...}, "remove": [...]}. Values
    inside set must also be strings. To store JSON data, serialize it as a
    string.

      loonfs annotate /docs/report.csv --set owner=ada
      loonfs annotate /docs/report.csv --attributes-json \
        '{"set":{"tags":"[\"red\",\"blue\"]"}}'

    --attributes-json cannot be combined with --set or --remove. The expected
    value flags reject the update if the inode or attributes changed

  loonfs rm <path> [-r] [--actor-kind <kind> --actor-id <id>]
    Delete a file, or with -r a directory and everything under it as one
    commit; the output carries the handle `loonfs undelete` needs

  loonfs mv <source> <dest> [--force]
            [--expected-destination-inode-id <id>]
            [--expected-destination-revision <n>]
            [--actor-kind <kind> --actor-id <id>]
    Move or rename a path, a directory included, in one commit; --force
    replaces an existing destination. The expected value flags replace only
    if the destination still has the inode and optional revision that you read

  loonfs cp <source> <dest> [-r] [--force]
            [--expected-destination-inode-id <id>]
            [--expected-destination-revision <n>]
            [--actor-kind <kind> --actor-id <id>]
    Copy a file, or with -r the directory tree rooted at the source;
    --force replaces an existing destination. The expected value flags use
    the same checks as mv and cannot be used with -r

History and recovery
  loonfs revisions <path> [--limit <n>] [--page-size <n>] [--cursor <cursor>]
                          [--all] [--jsonl]
    List a file's revision history, newest first

  loonfs restore <path> --revision <n>
                 [--actor-kind <kind> --actor-id <id>]
    Write a prior revision's content as the file's next revision

  loonfs trash [--limit <n>] [--page-size <n>] [--cursor <cursor>]
               [--all] [--jsonl]
    List recoverable deletions, including when each item was deleted and the
    exact `loonfs undelete` command that restores it

  loonfs undelete [<path>] --inode <id> --deletion-seq <seq>
                  [--actor-kind <kind> --actor-id <id>]
    Recover a deleted file or directory; --inode and --deletion-seq come from
    `loonfs trash` or the `rm` output and name one exact deletion, so a
    stale command cannot cancel a later delete. Omit <path> to restore in
    place under the parent and name recorded with the deletion. This still
    works if an enclosing directory was renamed. Pass <path> to restore it
    somewhere else

  loonfs changes [--after <seq>] [--limit <n>] [--page-size <n>]
                 [--all] [--jsonl] [--snapshot-id <id>]
    List committed changes after a sequence number. When --after is omitted,
    start at the beginning of retained history. --snapshot-id ends the feed
    at the snapshot's captured sequence.

Inspection and diagnostics
  loonfs capabilities [--profile <name>]
    Show the protocol version, API groups, features, and limits supported by
    the selected deployment

  loonfs doctor [--profile <name>] [--namespace <name>] [--write-check]
    Check the configuration, provider, connection, authentication, server
    health, capabilities, and selected namespace. The command is read-only
    unless --write-check is set. That option also tests the object store by
    writing and deleting temporary objects. The command prints every result
    and exits nonzero if a check fails

Maintenance
  loonfs maintenance loop --namespaces <ns> [--namespaces <ns>]... [--job <job>]... [--drain] [--max-steps <n>] [--deadline-ms <ms>]
    Run maintenance for explicitly named namespaces in embedded mode. The
    command runs until stopped. With --drain, it finishes the current
    assignments and exits. --job selects metadata, metadata-compaction, gc,
    grep-index, or grep-gc; these map to the metadata, metadata_compaction,
    gc, grep_index, and grep_gc job ids. Omitting it selects all five.
    --max-steps and --deadline-ms bound a drain.

  loonfs maintenance metadata [--max-wal-tail-segments <n>]
    Run the metadata job once: flush the WAL tail when it reaches
    the threshold, then merge one reorganization unit.
    --max-wal-tail-segments overrides the default flush threshold.

  loonfs maintenance flush
    Flush the current WAL tail regardless of its length.

  loonfs maintenance compact
    Run one full metadata compaction for the namespace.

  loonfs maintenance retention advance
    Advance the retention floor. This removes change-feed replay history
    below the flushed manifest head but does not remove file revisions.

  loonfs maintenance gc [--grace-window-ms <ms>] [--max-objects <n>] [--cursor <token>]
    Run mark-and-sweep collection, looping bounded passes through
    completion; --max-objects examines at most that many candidates and
    returns after one pass, --cursor resumes from a previous pass's
    next_cursor for the same namespace, and --grace-window-ms protects
    objects younger than the window. A run that takes more than one pass
    reports each pass on standard error as it lands, and the summary says
    what the pass kept and mostly why; --json carries every retention reason

  loonfs maintenance store probe
    Test the object-store operations LoonFS requires. The command creates
    and removes temporary objects, prints each result, and exits nonzero if
    any check fails.

  loonfs maintenance checkpoint create --name <label> [--ttl-ms <ms>]
    Pin the namespace's current state. --ttl-ms sets an expiry; without it,
    the checkpoint remains until release.

  loonfs maintenance checkpoint list [--limit <n>] [--page-size <n>]
                               [--cursor <cursor>] [--all] [--jsonl]
    List active checkpoints in ID order. Expired checkpoints remain visible
    until garbage collection removes them.

  loonfs maintenance checkpoint release <checkpoint-id>
    Release a checkpoint pin

  loonfs maintenance index enable [--no-wait] [--max-steps <n>] [--deadline-ms <ms>]
    Enable the gram index and wait until it reaches the namespace sequence
    captured at startup. --no-wait returns after enabling it. --max-steps
    and --deadline-ms bound the wait and report incomplete progress.

  loonfs maintenance index status
    Show whether the gram index is disabled, backfilling, or active.

  loonfs maintenance index disable
    Disable the gram content index

  loonfs maintenance index gc [--max-objects <n>] [--cursor <token>]
    Remove unreferenced gram-index objects. Without --max-objects, continue
    until collection is complete. --max-objects limits one pass to that many
    reads, and --cursor resumes a previous pass.

Profile create options
  Used by:
    loonfs profile create <provider> <name>

  If a required value is missing, the CLI prompts for it on a terminal. Under
  --no-input or --json, the command returns an error instead. Each provider
  exposes only its own flags. S3 and R2 use ambient credentials by default,
  reading AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY when the store is
  opened. Passing static credential flags stores those credentials in the
  profile.

  Every embedded profile:
    --key-prefix <prefix>              optional

  loonfs profile create local:
    --root <path>

  loonfs profile create s3:
    --bucket <name>
    --region <region>
    --credential-source <ambient|static>
    --access-key-id <id>               static credentials only
    --secret-access-key <secret>       static credentials only
    --session-token <token>            optional static session token
    --endpoint-url <url>               optional
    --force-path-style                 optional

  loonfs profile create r2:
    --bucket <name>
    --account-id <id>
    --endpoint-url <url>
    --credential-source <ambient|static>
    --access-key-id <id>               static credentials only
    --secret-access-key <secret>       static credentials only

  loonfs profile create gcs:
    --bucket <name>
    --service-account-key-path <path>

  loonfs profile create azure:
    --account-name <name>
    --container-name <name>
    --access-key <key>
    --endpoint-url <url>               optional

  loonfs profile create remote:
    --server-url <url>
    --auth-token <token>               optional; otherwise read
                                       LOONFS_AUTH_TOKEN when used
    --ca-cert-path <path>              optional, PEM bundle of extra
                                       certificate authorities to trust for
                                       an https server url

  Mutation actor:
    --actor-kind <user|service|system> optional, requires --actor-id
    --actor-id <stable-id>             optional, requires --actor-kind
    The default service/loonfs-cli identifies the tool, not the human.

Update options
  Used by:
    loonfs profile update <provider> <name>

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
    --credential-source <ambient|static>
    --access-key-id <id>
    --secret-access-key <secret>
    --session-token <token>
    --endpoint-url <url>

  cloudflare-r2 profile updates:
    --bucket <name>
    --account-id <id>
    --endpoint-url <url>
    --credential-source <ambient|static>
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

  Mutation actor updates:
    --actor-kind <user|service|system> requires --actor-id
    --actor-id <stable-id>             requires --actor-kind

Interrupted transfers
  A transfer killed part way is picked up by rerunning the same command.
  Nothing is asked for and no flag turns it on: a rerun either finds
  something to continue from or starts over, and says which.

  A download writes into `.<name>.loonfs-partial` beside its destination,
  with a `.meta` note naming the content those bytes belong to. A rerun
  continues from them when the note still describes the file it resolves
  now, and starts over — silently, dropping the bytes — when it does not:
  a different file at that path, a different revision, or a note this
  build cannot read. Both files are removed when the download installs at
  its destination and when it fails and says so, so the only run that
  leaves anything behind is one that was killed. Verification still covers
  the whole file: the bytes already on disk are folded back into the same
  digest as the ones fetched, so a partial that is not really this file's
  fails the rerun instead of landing.

  A download the server proxies — a remote file under the deployment's
  cap — arrives in one response and has no midpoint to resume from, so it
  starts over.

  An upload resumes only where the transfer had parts to lose: a remote
  profile's direct multipart upload of a file past 8 MiB. The parts that
  landed are recorded under $XDG_STATE_HOME/loonfs/uploads (or
  ~/.loonfs/state/uploads when XDG_STATE_HOME is unset), and a rerun sends
  only the ones still missing — or, when the transfer had actually
  finished and only the commit was lost, commits what is already stored
  and sends nothing. A source that changed since the record was written
  starts over, because the parts already stored came from bytes that are
  gone. The record is removed when the upload commits. Embedded profiles,
  proxied uploads, and `loonfs put -` keep no record: there is no session
  to rejoin, and a pipe cannot be read a second time.

  A tree resumes file by file, on the same terms. Each file of a `put -r`
  keeps its own record, named by the profile, namespace, remote path, and
  local file that decide which upload it is, so rerunning the command
  picks up whichever files were interrupted and re-uploads none that
  committed.

Transfer progress
  `loonfs get` and `loonfs put`, one file or a whole tree, say where they
  have got to while they run. Never on stdout, which carries either the
  file's bytes or the JSON result.

  On a terminal, one line on stderr is redrawn in place: bytes moved, the
  total when it is known, a percentage, a rate, and how long is left at
  that rate. A tree also shows files finished out of files found. The line
  is erased when the transfer ends. When stderr is not a terminal nothing
  is printed, the way curl stays quiet in a pipeline.

  With --json, stderr carries one JSON object per line instead, so an agent
  can tell healthy work from a hang. That makes stderr a stream of JSON
  documents under --json: these events while the command runs, then the
  failure envelope if it failed. The names and fields below are stable.

    {"kind":"file_started","op":"get","path":"/docs/a.bin",
     "bytes_total":4096,"elapsed_ms":12}
      One file began moving. bytes_total is left out when nothing knows it.

    {"kind":"progress","op":"get","path":"/docs/a.bin","bytes_done":2048,
     "bytes_total":4096,"files_done":0,"files_total":1,"rate_bps":10240,
     "elapsed_ms":200}
      Where the whole operation is. `path` is the file for a single
      transfer and `<namespace>:<root>` for a tree. bytes_total and
      files_total are null when they cannot be known — a piped put, or a
      tree holding a file whose length the walk could not read. rate_bps is
      the average over the transfer so far. Reported at most four times a
      second, and whenever the whole-number percentage moves.

    {"kind":"file_finished","op":"get","path":"/docs/a.bin",
     "bytes_done":4096,"elapsed_ms":420}
      One file finished, and what it actually moved.

    {"kind":"phase","op":"put","path":"/docs/a.bin","phase":"committing",
     "elapsed_ms":900}
      A stretch where time passes and bytes do not, reported when it
      starts, with `bytes_done` as it stood at that moment. `committing`
      says an upload's payload has been read and the commit is what is
      left; `resuming` says a download picked up bytes an interrupted run
      left behind, and `bytes_done` is how many. Only a transfer of one
      file reports these: a tree has several files in flight at once, so
      no one file's stretch is the operation's.

  `op` is `get` or `put`. --no-progress silences both forms, and neither is
  produced for `loonfs cat` or for `loonfs get ... -`, which hand the file
  itself to stdout.

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
  file small enough to hold is still uploaded in one request. `put -r`
  uploads every file of a tree the same way, so a tree holding a file
  larger than this process could hold costs no more memory than a tree of
  small ones

  If a single-file `loonfs get` omits the local destination, the CLI writes to
  `./<remote-filename>`. If the destination ends in `/` or is an existing
  directory, the file is written there under its remote name

  A recursive get writes the directory contents directly into the destination:

    loonfs get -r /reports ./download

  This writes the contents of `/reports` under `./download`, not
  `./download/reports`. If the destination is omitted, the CLI uses the remote
  directory name:

    loonfs get -r /reports

  Existing files are not overwritten unless --force is set. Downloads stream
  without buffering the entire file. The CLI writes to a temporary file and
  moves it into place after verification, so a failed download does not leave
  a partial destination. When streaming to stdout with `-`, written bytes
  cannot be taken back; check the exit status to confirm verification

  A recursive put, get, or cp works file by file with bounded concurrency,
  so a partial failure reruns per file; --commit-id names one commit, so
  `put -r` and `cp -r` reject it, and `get -r` rejects --revision for the
  same reason

  `loonfs mv` moves a directory in one commit and takes no -r

  How much of a transfer is watchable follows how it travels. A streamed or
  direct download reports bytes as they land, and so does a streamed
  upload; a proxied download and a small buffered upload are one request
  each, so they go from nothing to done in one step. A tree is the same
  file by file: its byte count moves through a large file as that file is
  read, and advances a whole file at a time over the small ones

  Embedded profiles do not run continuous maintenance. Run
  `loonfs maintenance loop --namespaces <ns>` for ongoing maintenance,
  or `loonfs maintenance metadata` for one pass. Live writers fold their own WAL
  tails; explicit maintenance handles inactive namespaces and the other jobs.
  Servers maintain the namespaces they use automatically by default.
```
