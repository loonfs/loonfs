# loonfs-cli

`loonfs-cli` provides the `loonfs` command, which you can use to manage profiles, namespaces, and namespace maintenance, and to work with files and directories by path. To use LoonFS from a Rust program instead, use the `loonfs` crate to embed the runtime, or `loonfs-client` to call a LoonFS server. To run a server, use `loonfs-server`.

A "profile" is an entry in the config file that tells the CLI which object store or server to use. A remote profile sends requests to a LoonFS server. An embedded profile (`local`, `s3`, `r2`, or `gcs`) reads and writes the object store directly. It runs the HTTP binding (`loonfs-http`) in memory, on top of its own runtime handles, and the CLI sends requests to the binding through the same client that a remote profile uses. Because of that, both kinds of profile follow the same API contract. An embedded profile doesn't open a network listener and doesn't need a bearer token.

On an embedded profile, `loonfs maintenance index enable` runs its bounded index steps directly on the local runtime handles, instead of through the binding. A command on an embedded profile returns only after the [folds](../../docs/specs/glossary.md#fold) and metadata compactions that its writes started have finished.

Requests through an embedded profile work as follows:

- **Validation and errors.** Requests go through the binding's validation, error envelope, and JSON body limits. A failed request reports the binding's request ID and error details.
- **Deadline.** The binding cancels a request that runs longer than 60 seconds (the server's default deadline). Routes that stream file content and maintenance runs aren't subject to that deadline.
- **Concurrency.** The binding admits at most 8 concurrent uploads and 16 concurrent downloads, the same defaults as the server.
- **Transfer size.** The upload and download size limits are `u64::MAX`, so embedded transfers aren't limited in size.
- **Downloads.** An interrupted embedded download starts again from the beginning. Progress for a single-file `loonfs get` counts the bytes received, but doesn't show a total.
- **Uploads.** A file upload records its prepared commit request. If you rerun the command after the process restarts, the rerun submits that request again and renews its content token through the upload session, so the file isn't uploaded again.

If another writer [fences](../../docs/specs/glossary.md#fence) an embedded profile's [writer session](../../docs/specs/glossary.md#writer-session), that session can't be used again. The request fails with `writer_fenced`, the CLI doesn't resubmit it, and the embedded host drops its handle. A later request starts a new writer session, and that session's first publication takes the namespace back. If a request tries to open the namespace while the old session's work is still ending, it can fail with `writer_session_closed`. A request made after that work ends can open the namespace.

If one namespace is fenced again and again, its traffic is being sent to two writers. Send all of that namespace's traffic to one writer.

## Shell completion

`loonfs completion` prints a completion script for Bash, Zsh, Fish, PowerShell, or Elvish:

```sh
loonfs completion --shell zsh
```

If you omit `--shell`, the CLI chooses the shell from `$SHELL`.

For example, to enable Zsh completion in the current shell, run:

```sh
source <(loonfs completion --shell zsh)
```

## Command Reference

This reference lists the CLI's commands and flags, how the CLI chooses its config file, and how transfers, progress, and other behaviors work.

```text
LoonFS CLI Command Reference

`loonfs` works with profiles, namespaces, and path-based filesystem operations.

Global flags
  --config <path>
    Config file to use, ahead of LOONFS_CONFIG and the default location

  --json
    Emit machine-readable JSON instead of human output. A failure prints
    the same envelope on stderr, with an `error` object instead of `data`.
    That includes a command line the parser rejected (an unknown command, a
    bad value, or a missing option), whose error includes
    `"kind": "parse_error"` and the code `invalid_usage`. A parse failure
    exits with status 2, and a command that ran and failed exits with
    status 1, so you can tell the two apart without reading the message

  --no-retry
    Disable bounded retry of `server_busy`, `commit_queue_full`,
    `shutting_down`, and transport errors

  --no-input
    Never prompt. Fail instead when input would be required

  --no-progress
    Don't show progress while a transfer runs, either on a terminal or
    under --json

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
      Default namespace for the current environment. --namespace takes
      precedence over it, and it takes precedence over the profile default

  Commands that commit (put, mkdir, rm, mv, cp, restore, undelete) accept:
    -m, --message <message>
      Annotation recorded on the commit and shown by `loonfs changes`

    --commit-id <id>
      Idempotency key for the commit. One is generated when omitted, and
      reported in the output. To retry safely, resubmit the same ID. The
      message is part of the commit's identity, so the same ID with a
      different message is a conflict

Setup and configuration
  loonfs init
    Interactively create the config file and a first profile, and make
    that profile the default. The command fails if the config file already
    exists

  loonfs config path
    Print the path of the config file this invocation uses, and which rule
    selected it

  loonfs config show
    Print the config file (secrets redacted)

  loonfs version
    Print version and build metadata

Config file location
  Each invocation uses one config file, selected by the first rule that
  applies:
    1. --config <path>
    2. LOONFS_CONFIG=<path>
    3. $XDG_CONFIG_HOME/loonfs/config.toml, when XDG_CONFIG_HOME is an
       absolute directory path
    4. ~/.config/loonfs/config.toml
  Profile selection uses --profile, then LOONFS_PROFILE=<name>, then the
  configured default. Namespace selection uses --namespace, then
  LOONFS_NAMESPACE=<name>, then the profile default.

  A path given to --config or LOONFS_CONFIG is the file this invocation
  uses, whether or not the file exists yet, and `loonfs init` creates the
  directories it needs. You can use this to get past a config file that
  this build won't read. An unknown key is an error, so every command that
  reads the file fails until you fix the file or point the CLI at a
  different one. Each of those errors identifies the file and what's wrong
  in it, and describes both of those options.

  `loonfs config path` doesn't read the file, so it still works when the
  file is unreadable.

  Whether the file exists doesn't affect which rule applies. An unset or
  relative XDG_CONFIG_HOME uses the default in rule 4. An absolute
  XDG_CONFIG_HOME selects rule 3, even before the file is created.

Profile management
  loonfs profile create s3 <name> [s3-options]
  loonfs profile create r2 <name> [r2-options]
  loonfs profile create gcs <name> [gcs-options]
  loonfs profile create local <name> [local-options]
  loonfs profile create remote <name> [remote-options]
    Add a provider-specific profile to the config file

  loonfs profile list
    List configured profiles and mark the default

  loonfs profile show [name]
    Show one profile (secrets redacted). Without a name, show the profile
    selected by LOONFS_PROFILE or the configured default.

  loonfs profile update <provider> <name> [update-options]
    Update fields of an existing profile. If you pass no flags and the CLI
    is running on a terminal, it asks for each field instead

  loonfs profile delete <name>
    Delete a profile from the config file

  loonfs profile use <name>
    Make a profile the default

Namespace management
  loonfs namespace create <namespace> [--access acl --principal-scope <scope> --administrator <principal>...]
                            [--naming case_sensitive] [--actor-id <stable-id>]
    Create a new empty namespace. Names that differ only in case are the
    same name unless --naming case_sensitive is given. The naming mode
    can't be changed after the namespace is created

  loonfs namespace show [namespace]
    Show the selected namespace's status and creator

  loonfs namespace fork <source> <new-namespace> [--snapshot <id>] [--actor-id <stable-id>]
    Fork a namespace into a new one. The fork shares the source's existing
    content and metadata objects without copying the filesystem. Forking
    the current head might first fold the source's outstanding WAL tail.
    Use --snapshot to fork from a live snapshot instead of the current head.

  loonfs namespace delete <namespace> [--expected-head-seq <seq>] [--yes]
    Delete a namespace and retire its ID. The deletion can't be undone. To
    confirm, type the ID back at the prompt or pass --yes. With
    --expected-head-seq, the command deletes only if the namespace head is
    still at that sequence

  loonfs use <namespace>
    Set the default namespace for a profile. This sets the interactive
    default. Automation that runs concurrently should pass --namespace or
    set LOONFS_NAMESPACE instead

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

  loonfs snapshot delete <namespace> <snapshot-id>
    Delete the snapshot record. A second call returns snapshot_not_found.

Pagination
  ls, grep, revisions, trash, changes, snapshot list, and maintenance
  checkpoint list return one page by default. --limit sets the total
  maximum across pages, --page-size sets the size of each request, and
  --cursor resumes a previous result. --all fetches every page for human
  output. --json returns a bounded document and can't be combined with
  --all. Use --jsonl to stream every result. --jsonl prints only the items,
  without envelope fields such as the namespace, head sequence, or
  trailing cursor. changes uses --after instead of --cursor.

Reading
  loonfs ls [path] [--limit <n>] [--page-size <n>] [--cursor <cursor>]
                   [--all] [--jsonl] [--snapshot-id <id>]
    List the entries of a directory, or `/` when the path is omitted

  loonfs stat <path> [--snapshot-id <id>]
  loonfs stat --inode <inode-id>
    Describe a path or visible inode. The output includes its kind, size,
    revision, content digest, and attributes

  loonfs cat <path> [--revision <n>] [--snapshot-id <id>]
    Print a file's content to stdout. --revision prints that revision
    instead of the current content

  loonfs get <remote-path> [local-destination] [-r] [--revision <n>]
             [--snapshot-id <id>] [--force]
    Download a file (or directory tree with -r) to a local path. For
    recursive downloads, the local destination is the root of the
    downloaded contents. Existing files aren't overwritten unless --force
    is set. --revision downloads an older revision of one file

  loonfs grep <pattern> [--path-prefix <path>] [-i] [--limit <n>]
                        [--page-size <n>] [--cursor <cursor>] [--all] [--jsonl]
                        [--allow-scan] [--allow-stale]
    Search file content through the gram index, with a pattern in the Rust
    regex dialect (`^` and `$` match at line boundaries). -i ignores ASCII
    case, --path-prefix limits the search to a subtree, --allow-scan
    permits a bounded scan for patterns with no literal bytes, and
    --allow-stale permits results that leave out an unindexed tail that's
    too large to scan

Writing
  Every writing command, including namespace create and namespace fork,
  accepts --actor-id <stable-id>. The flag overrides LOONFS_ACTOR_ID, which
  overrides the profile's actor_id. Without any of them, the actor ID
  defaults to loonfs-cli, which identifies the tool, not the person
  running it.

  Every command that reaches a namespace accepts --subject-id <id>,
  --principal-scope <scope>, and --principals <a,b>. The flags override
  LOONFS_SUBJECT_ID, LOONFS_PRINCIPAL_SCOPE, and LOONFS_PRINCIPALS, which
  override the matching profile fields. Scope and principals are required
  together. Without them, the CLI acts as the token holder. With them, the
  subject ID defaults to the actor ID.

  loonfs put <local-path|-> [remote-path] [-r] [--force]
             [--expected-inode-id <id>] [--expected-revision <n>]
             [--actor-id <id>]
    Upload a local file, standard input when the local path is `-`, or with
    -r the directory tree rooted at the local path. --force replaces an
    existing destination. The expected value flags replace the file only if
    it still has the inode, and optionally the revision, that you read. A
    revision precondition requires the inode precondition, and neither can
    be used with -r

  loonfs append <local-path|-> <remote-path> [--expected-inode-id <id>]
                [--expected-revision <n>] [--actor-id <id>]
    Add the bytes of a local file, or of standard input when the local path
    is `-`, to the end of an existing file as its next revision. One append
    can add from 1 byte to 256 KiB. The expected value flags append only if
    the file still has the inode and optional revision that you read

  loonfs mkdir <path> [-p] [--actor-id <id>]
    Create a directory. -p creates missing parent directories as well, and
    succeeds when the directory is already there

  loonfs annotate <path> [--set key=value]... [--remove key]...
                         [--attributes-json '<update>']
                         [--expected-inode-id <n>]
                         [--expected-attributes-revision <n>]
                         [--actor-id <id>]
    Write and remove attributes on a file or directory. --set takes
    key=value, and the key ends at the first `=`, so the value can contain
    more of them. --remove takes a key. Both flags can be repeated.
    Attribute values must be strings. --attributes-json accepts an update
    document in the form {"set": {...}, "remove": [...]}. Values inside set
    must also be strings. To store JSON data, serialize it as a string.

      loonfs annotate /docs/report.csv --set owner=ada
      loonfs annotate /docs/report.csv --attributes-json \
        '{"set":{"tags":"[\"red\",\"blue\"]"}}'

    --attributes-json can't be combined with --set or --remove. The
    expected value flags reject the update if the inode or attributes
    changed

  loonfs access set <path> --grant <principal>=<right>[,<right>...]... [--boundary]
                    [--expected-inode-id <id> --expected-revision <n>]

  loonfs rm <path> [-r] [--actor-id <id>]
    Delete a file, or with -r a directory and everything under it, as one
    commit. The output includes the handle that `loonfs undelete` needs

  loonfs mv <source> <dest> [--force]
            [--expected-destination-inode-id <id>]
            [--expected-destination-revision <n>]
            [--actor-id <id>]
    Move or rename a path (a directory included) in one commit. --force
    replaces an existing destination. The expected value flags replace only
    if the destination still has the inode and optional revision that you
    read

  loonfs cp <source> <dest> [-r] [--force]
            [--expected-destination-inode-id <id>]
            [--expected-destination-revision <n>]
            [--actor-id <id>]
    Copy a file (or directory tree with -r) to another path. --force
    replaces an existing destination. The expected value flags use the same
    checks as mv and can't be used with -r

History and recovery
  loonfs revisions <path> [--limit <n>] [--page-size <n>] [--cursor <cursor>]
                          [--all] [--jsonl]
    List a file's revision history, newest first

  loonfs restore <path> --revision <n>
                 [--actor-id <id>]
    Write a prior revision's content as the file's next revision

  loonfs trash [--limit <n>] [--page-size <n>] [--cursor <cursor>]
               [--all] [--jsonl]
    List recoverable deletions, including when each item was deleted and the
    exact `loonfs undelete` command that restores it

  loonfs undelete [<path>] --inode <id> --deletion-seq <seq>
                  [--actor-id <id>]
    Recover a deleted file or directory. --inode and --deletion-seq come
    from `loonfs trash` or the `rm` output, and together they identify one
    exact deletion, so a stale command can't cancel a later delete. Omit
    <path> to restore the item in place, under the parent and name recorded
    with its deletion. That works even if an enclosing directory was
    renamed since. Pass <path> to restore it somewhere else

  loonfs changes [--after <seq>] [--limit <n>] [--page-size <n>]
                 [--all] [--jsonl] [--snapshot-id <id>]
    List committed changes after a sequence number. When --after is
    omitted, the list starts at the beginning of retained history.
    --snapshot-id ends the feed at the snapshot's captured sequence.

Inspection and diagnostics
  loonfs capabilities [--profile <name>]
    Show the protocol version, API groups, features, and limits supported by
    the selected deployment

  loonfs doctor [--profile <name>] [--namespace <name>]
    Check the configuration, provider, connection, authentication, server
    health, capabilities, and selected namespace. The command prints every
    result and exits nonzero if a check fails

Maintenance
  loonfs maintenance recover-administrator <principal>

  loonfs maintenance metadata [--max-wal-tail-objects <n>]
    Run one metadata maintenance pass, which folds the WAL tail if it has
    reached the threshold and then runs one bounded compaction step.
    --max-wal-tail-objects overrides the default fold threshold.

  loonfs maintenance fold
    Fold the WAL tail into metadata segments, whatever its length.

  loonfs maintenance compact
    Run one metadata compaction unit. A unit is one bounded merge or one
    streaming compaction of a family group. Repeat the command while it
    publishes to compact every eligible group.

  loonfs maintenance retention advance [--to-seq <seq> | --before <unix-ms>]
    Advance the retention floor and discard the change-feed replay history
    below it. A later base compaction keeps every file revision above the
    floor and each file's newest revision at or below it. The floor moves
    to the folded manifest head unless a target is given. --to-seq moves
    it to that sequence, or to the folded manifest head when that is
    lower. --before moves it to the last commit made at or before that
    Unix time in milliseconds.

  loonfs maintenance gc [--grace-window-ms <ms>]
    Run one complete pass and print its report. Each call reads the current
    manifest and hint, then lists every family from the beginning and
    sweeps each family to the end.
    --grace-window-ms protects objects younger than the window.
    --json includes every retention reason.
    deleted.content_objects counts the content objects that no retained
    view references, which are deleted once they're older than the grace
    window. Once a deleted namespace is retired, repeated GC runs delete
    its own content, and deleted.retired_content_objects counts the listed
    content objects that were deleted. A pass with nothing left under the
    content prefix reports zero.
    deleted.temporary_objects counts store temporary objects older than the
    window.

  loonfs maintenance checkpoint create --name <label> [--ttl-ms <ms>]
    Pin the namespace's current state in a new checkpoint. Every call
    creates a new checkpoint. The name is a label, not a key. --ttl-ms
    sets an expiry. Without it, the checkpoint lasts until it's deleted.

  loonfs maintenance checkpoint list [--limit <n>] [--page-size <n>]
                               [--cursor <cursor>] [--all] [--jsonl]
    List existing checkpoint pins in checkpoint ID order. Expired pins
    remain listed until garbage collection deletes them.

  loonfs maintenance checkpoint delete <checkpoint-id>
    Delete the checkpoint record. A second call returns checkpoint_not_found.

  loonfs maintenance index enable [--no-wait] [--max-steps <n>] [--deadline-ms <ms>]
    Enable the gram content index and wait for its backfill to reach the
    sequence the namespace was at when this command started. --no-wait
    returns as soon as the index is enabled. --max-steps and --deadline-ms
    limit the wait. When either limit stops the command, it exits nonzero
    and reports how far the index got.

  loonfs maintenance index status
    Show whether the gram content index is disabled, backfilling, or active.

  loonfs maintenance index disable
    Disable the gram content index

  loonfs maintenance grep-gc
    Collect the namespace's unreferenced gram-index objects in one complete
    pass. The pass reads the current manifest and hint before it deletes
    anything. For an absent or deleted namespace, it deletes aged index
    objects.

Profile create options
  Used by:
    loonfs profile create <provider> <name>

  If a required value is missing, the CLI prompts for it on a terminal.
  Under --no-input or --json, the command returns an error instead. Each
  provider accepts only its own flags. S3 and R2 use ambient credentials by
  default, and read AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY when the
  store is opened. If you pass the static credential flags, the CLI stores
  those credentials in the profile.

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

  loonfs profile create remote:
    --server-url <url>
    --auth-token <token>               optional. A profile without one uses
                                       LOONFS_AUTH_TOKEN at request time
    --ca-cert-path <path>              optional. PEM bundle of extra
                                       certificate authorities to trust for
                                       an https server URL

  Mutation actor:
    --actor-id <stable-id>             optional
    The default, loonfs-cli, identifies the tool, not the person.

Update options
  Used by:
    loonfs profile update <provider> <name>

  A flag that doesn't apply to the profile's own store is rejected. The
  mode and the store kind can't be changed after the profile is created.
  S3 updates accept --force-path-style true or false. Omit it to keep the
  stored value.

  Shared:
    --key-prefix <prefix>

  local_fs profile updates:
    --root <path>

  aws_s3 profile updates:
    --bucket <name>
    --region <region>
    --credential-source <ambient|static>
    --access-key-id <id>
    --secret-access-key <secret>
    --session-token <token>
    --endpoint-url <url>
    --force-path-style <true|false>

  cloudflare_r2 profile updates:
    --bucket <name>
    --account-id <id>
    --endpoint-url <url>
    --credential-source <ambient|static>
    --access-key-id <id>
    --secret-access-key <secret>

  gcp_gcs profile updates:
    --bucket <name>
    --service-account-key-path <path>

  Remote profile updates:
    --server-url <url>
    --auth-token <token>
    --ca-cert-path <path>

  Mutation actor updates:
    --actor-id <stable-id>

Interrupted transfers
  To pick up a transfer that was killed partway through, rerun the same
  command. The CLI doesn't ask for anything, and there's no flag to turn
  this on. A rerun either finds something to continue from or starts
  over, and it says which.

  A download writes into `.<name>.loonfs-partial` next to its destination,
  along with a `.meta` file that identifies the content those bytes belong
  to. A rerun continues from those bytes if the `.meta` file still
  describes the file that the command resolves now. Otherwise, the rerun
  silently discards the bytes and starts over. That happens when there's a
  different file at that path, a different revision, or a `.meta` file
  that this build can't read.

  Both files are removed when the download is installed at its
  destination, and when the download fails and reports the failure, so
  only a run that was killed leaves anything behind. Verification still
  covers the whole file. The bytes already on disk are included in the
  same digest as the bytes fetched, so if the partial bytes don't really
  belong to this file, the rerun fails instead of installing them.

  An embedded download, or a remote download that the server proxies,
  arrives in one response, so there's no point partway through to resume
  from, and the rerun starts over from the beginning. Only a direct
  download from the object store resumes.

  An upload resumes only when the transfer had parts to lose, which means
  a remote profile's direct multipart upload of a file larger than 8 MiB.
  The parts that were uploaded are recorded under
  $XDG_STATE_HOME/loonfs/uploads (or ~/.local/state/loonfs/uploads when
  XDG_STATE_HOME is unset), and a rerun sends only the parts that are
  still missing. If the transfer had actually finished and only the commit
  was lost, the rerun commits what's already stored and sends nothing. If
  the source changed since the record was written, the upload starts over,
  because the parts already stored came from bytes that no longer exist.

  Every file upload, including embedded and proxied uploads, also records
  its prepared commit request before it submits the commit, and a rerun
  submits that saved request again. If the server rejects the request's
  content token, the rerun gets a new token from the completed upload
  session, so the file isn't uploaded again. What happens to the record
  depends on how the commit ends:
    - If the upload commits, the record is removed. A record made with an
      explicit --commit-id is kept instead, so repeating the command
      replays the same request.
    - If the commit is refused because of the namespace's state, the
      record is removed, even with an explicit ID.
    - After a transport failure, an unknown outcome, admission pressure,
      another unavailable condition, or commit_id_reuse_conflict, the
      record is kept.
  `loonfs put -` keeps no record, because a pipe can't be read a second
  time.

  A tree resumes file by file, under the same rules. Each file of a
  `put -r` keeps its own record, identified by the profile, namespace,
  remote path, and local file of that upload, so rerunning the command
  resumes whichever files were interrupted and doesn't upload again any
  file that already committed.

Transfer progress
  `loonfs get` and `loonfs put` report their progress while they run, for
  one file or a whole tree. They never report it on stdout, which contains
  either the file's bytes or the JSON result.

  On a terminal, the CLI redraws one line on stderr in place. The line
  shows the bytes transferred, the total when it's known, a percentage, a
  rate, and how long is left at that rate. For a tree, it also shows the
  files finished out of the files found. The line is erased when the
  transfer ends. When stderr isn't a terminal, nothing is printed, the
  same way curl stays quiet in a pipeline.

  With --json, the CLI writes one JSON object per line to stderr instead,
  so an agent can tell healthy work from a hang. Under --json, stderr is a
  stream of JSON documents (these events while the command runs, and then
  the failure envelope if the command failed). The names and fields below
  are stable.

    {"kind":"file_started","op":"get","path":"/docs/a.bin",
     "bytes_total":4096,"elapsed_ms":12}
      One file started transferring. bytes_total is omitted when the size
      isn't known.

    {"kind":"progress","op":"get","path":"/docs/a.bin","bytes_done":2048,
     "bytes_total":4096,"files_done":0,"files_total":1,"rate_bps":10240,
     "elapsed_ms":200}
      The progress of the whole operation. `path` is the file for a single
      transfer, and `<namespace>:<root>` for a tree. bytes_total and
      files_total are null when they can't be known (for a piped put, or
      for a tree that contains a file whose length the walk couldn't read).
      rate_bps is the average over the transfer so far. This event is
      reported at most four times a second, and whenever the whole-number
      percentage changes.

    {"kind":"file_finished","op":"get","path":"/docs/a.bin",
     "bytes_done":4096,"elapsed_ms":420}
      One file finished. bytes_done is how many bytes it actually moved.

    {"kind":"phase","op":"put","path":"/docs/a.bin","phase":"committing",
     "elapsed_ms":900}
      A period when time passes but no bytes move, reported when it
      starts, with `bytes_done` as it was at that moment. `committing`
      means an upload's payload has been read and only the commit is left.
      `resuming` means a download picked up bytes that an interrupted run
      left behind, and `bytes_done` is how many. Only a transfer of one
      file reports these events. A tree has several files in progress at
      once, so no single file's phase describes the whole operation.

  `op` is `get` or `put`. --no-progress turns off both forms, and neither
  form is produced for `loonfs cat` or for `loonfs get ... -`, which write
  the file itself to stdout.

Behavior notes
  `loonfs cat` always streams raw bytes to stdout

  `loonfs get ... -` streams raw bytes to stdout

  `--json` is rejected for streaming output commands

  If `loonfs put` omits the remote path, the CLI uses `/<local-filename>`

  A remote path ending in `/` is the directory that a `put`, `mv`, or `cp`
  places the item in, and the item keeps its own name

  A `mv` or `cp` destination that's already a directory means the same
  thing without the trailing slash, so `loonfs cp /report.pdf /docs`
  copies the file to `/docs/report.pdf`. A destination that doesn't exist
  is used as the exact path, and a destination that's a file follows the
  overwrite rules that --force controls

  `loonfs mkdir -p` succeeds without committing anything when the target
  is already a directory. If a file is at the target, it still fails with
  `path_conflict`

  `loonfs put -` reads standard input and needs an explicit remote path,
  because a pipe has no local name to derive one from

  `loonfs put` reads a large file or a pipe once, a piece at a time, and
  never keeps it in memory whole, so a put's memory use doesn't grow with
  the size of the upload. A file that's small enough to keep in memory is
  still uploaded in one request. `put -r` uploads every file of a tree the
  same way, so a tree that contains a file too large for this process to
  keep in memory uses no more memory than a tree of small files

  If a single-file `loonfs get` omits the local destination, the CLI writes to
  `./<remote-filename>`. If the destination ends in `/` or is an existing
  directory, the file is written there under its remote name

  A recursive get writes the directory contents directly into the destination:

    loonfs get -r /reports ./download

  This writes the contents of `/reports` under `./download`, not
  `./download/reports`. If the destination is omitted, the CLI uses the remote
  directory name:

    loonfs get -r /reports

  Existing files aren't overwritten unless --force is set. Downloads
  stream without buffering the entire file. The CLI writes to a temporary
  file and moves it into place after verification, so a failed download
  doesn't leave a partial file at the destination. For `cat`, and for
  streaming to stdout with `get ... -`, bytes that were already written
  can't be taken back, so check the exit status to confirm that
  verification passed

  Recursive put, get, and cp discover entries while the transfers run,
  with at most eight pending file operations. Discovery pauses when all
  eight slots are full. Local traversal keeps one directory handle per
  depth, and remote traversal keeps up to 64 entries per depth. The
  existing path-depth limit bounds both. Overall totals become known when
  discovery finishes. Per-file progress starts right away, and the
  traversal order is unspecified.

  Successful results are counted right away. Failures are written to a
  local temporary file and printed incrementally, so every error is kept
  without keeping the report in memory. Temporary disk usage grows with
  the error report, and the file is removed when the command exits
  normally. If writing the report fails, the command stops and reports an
  I/O error. A listing failure later in the traversal keeps the work that
  was already done, and reports the affected directory alongside the file
  failures.

  Reads of the current state can see changes between pages (including a
  recursive copy's own commits), so use a snapshot for a stable recursive
  get. A recursive copy rejects destinations inside its source tree.

  Each file is its own commit, so a rerun after a partial failure works
  file by file. --commit-id identifies one commit, so `put -r` and
  `cp -r` reject it, and `get -r` rejects --revision for the same reason

  `loonfs mv` moves a directory in one commit and doesn't take -r

  Downloads report bytes as they arrive, for current, historical, and
  snapshot reads, through either direct or proxied transport. Streamed
  uploads report incremental progress too, and small buffered uploads
  report completion in one step. Recursive transfers use those same paths
  for each file

  Embedded profiles don't run maintenance on a schedule. During a write,
  LoonFS folds the namespace's WAL tail when the tail reaches the
  threshold, and compacts metadata after the fold. Everything else needs
  a one-off command: `loonfs maintenance metadata` folds an idle tail,
  `loonfs maintenance gc` collects garbage, and
  `loonfs maintenance index enable` brings the grep index up to the
  namespace head. By default, servers maintain every namespace in their
  store on a regular schedule.
```
