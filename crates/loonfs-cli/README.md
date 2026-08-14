# loonfs-cli

`loonfs-cli` provides the `loonfs` command for managing profiles, namespaces, path-based
filesystem operations, and namespace maintenance against LoonFS.

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
      Profile to run against, defaulting to the configured default profile

    --no-retry
      Disable bounded retry of `server_busy`, `commit_queue_full`,
      `shutting_down`, and transport errors

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
  loonfs init [name] [profile-options]
    Create the config file with a first profile and make it the default;
    fails when the config file already exists

  loonfs config path
    Print the config file this invocation uses and which rule chose it

  loonfs config show
    Print the config file with secrets redacted

  loonfs version
    Print the version, commit, and build date

Config file location
  One config file per invocation, chosen by the first rule that applies:
    1. --config <path>
    2. LOONFS_CONFIG=<path>
    3. $XDG_CONFIG_HOME/loonfs/config.toml, when XDG_CONFIG_HOME names an
       absolute directory
    4. ~/.loonfs/config.toml
  Namespace selection uses --namespace, then LOONFS_NAMESPACE=<name>, then
  the profile default.

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
  loonfs ls [path] [--limit <n> | --all] [--cursor <cursor>] [--jsonl]
    List the entries of a directory, `/` when the path is omitted. Without
    --limit or --all, one server page is printed. --limit stops
    after that many entries in total and reports a next_cursor, which
    --cursor resumes; --all streams pages except in JSON; --jsonl streams entries

  loonfs stat <path>
    Describe one path: kind, size, revision, content digest, and the
    inode's attributes as `attr.<key>` lines. Control characters in a
    value print escaped, so a value stays on its own line; --json carries
    the value itself

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
  Every writing command accepts --actor-kind <user|service|system> together
  with --actor-id <stable-id>. Flags override LOONFS_ACTOR_KIND and
  LOONFS_ACTOR_ID, then the profile actor. Without any of them,
  service/loonfs-cli identifies the tool, not the human running it.

  loonfs put <local-path|-> [remote-path] [-r] [--force]
             [--expected-revision <n>] [--actor-kind <kind> --actor-id <id>]
    Upload a local file, standard input when the local path is `-`, or with
    -r the directory tree rooted at the local path; --force replaces an
    existing destination, and --expected-revision replaces only while the
    file is still at that revision, so a raced write fails instead of
    stacking on it

  loonfs mkdir <path> [-p] [--actor-kind <kind> --actor-id <id>]
    Create a directory; -p creates missing parents as well and succeeds when
    the directory is already there

  loonfs annotate <path> [--set key=value]... [--remove key]...
                         [--attributes-json '<update>']
                         [--expected-inode-id <n>]
                         [--expected-attributes-revision <n>]
                         [--actor-kind <kind> --actor-id <id>]
    Write and remove attributes on a file or directory. --set takes
    key=value and splits on the first `=`, --remove takes a key, and both
    repeat. A list value needs --attributes-json, which takes the whole
    update as {"set": {...}, "remove": [...]} in the wire encoding and
    cannot be combined with --set or --remove. The two --expected flags
    refuse the update when the path has been rebound or the attributes have
    moved on

  loonfs rm <path> [-r] [--actor-kind <kind> --actor-id <id>]
    Delete a file, or with -r a directory and everything under it as one
    commit; the output carries the handle `loonfs undelete` needs

  loonfs mv <source> <dest> [--force]
            [--actor-kind <kind> --actor-id <id>]
    Move or rename a path, a directory included, in one commit; --force
    replaces an existing destination

  loonfs cp <source> <dest> [-r] [--force]
            [--actor-kind <kind> --actor-id <id>]
    Copy a file, or with -r the directory tree rooted at the source;
    --force replaces an existing destination

History and recovery
  loonfs revisions <path> [--limit <n>] [--cursor <cursor>]
    List a file's revision history newest first, one page at a time

  loonfs restore <path> --revision <n>
                 [--actor-kind <kind> --actor-id <id>]
    Write a prior revision's content as the file's next revision

  loonfs trash [--limit <n>] [--cursor <cursor>]
    List recoverable deletions: what was deleted, when, and the exact
    `loonfs undelete` command that recovers each one

  loonfs undelete [<path>] --inode <id> --deletion-seq <seq>
                  [--actor-kind <kind> --actor-id <id>]
    Recover a deleted file or directory; --inode and --deletion-seq come from
    `loonfs trash` or the `rm` output and name one exact deletion, so a
    stale command cannot cancel a later delete. Omit <path> to restore in
    place under the parent and name recorded with the deletion. This still
    works if an enclosing directory was renamed. Pass <path> to restore it
    somewhere else, or when the deletion did not record a binding

  loonfs changes [--after <seq>] [--limit <n>]
    List committed changes after a sequence number, from the start of
    retained history when --after is omitted

Maintenance
  loonfs admin run --namespace <ns>... [--job <job>...] [--drain] [--max-steps <n>] [--deadline-ms <ms>]
    This command is embedded-only; remote servers host background
    maintenance themselves
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

  loonfs admin gc [--grace-window-ms <ms>] [--max-objects <n>] [--cursor <token>]
    Run mark-and-sweep collection, looping bounded passes through
    completion; --max-objects examines at most that many candidates and
    returns after one pass, --cursor resumes from a previous pass's
    next_cursor for the same namespace, and --grace-window-ms protects
    objects younger than the window. A run that takes more than one pass
    reports each pass on standard error as it lands, and the summary says
    what the pass kept and mostly why; --json carries every retention reason

  loonfs admin store-probe
    Prove the profile's object store honours the contract LoonFS depends on
    (create-if-absent, compare-and-swap, visibility, listing, ranged reads)
    and print one line per check; the run writes and deletes objects under a
    scratch prefix it empties afterwards, and exits nonzero when any check
    failed

  loonfs admin checkpoint --name <label> [--ttl-ms <ms>]
    Pin the namespace's current state under a named checkpoint; --ttl-ms
    expires the pin, and an omitted TTL holds it until release

  loonfs admin checkpoint-list [--limit <n>] [--cursor <cursor>]
    List the namespace's active checkpoint pins in checkpoint-id order: when
    each was taken, when it expires, the sequence it pins, its label or fork
    target, and the id `checkpoint-release` takes. By default the command
    follows bounded pages to completion; --limit or --cursor requests one
    page and prints next_cursor when more keys remain. A name is a label
    rather than a key, so this is how a pin is found again once its id has
    been lost; a pin whose expiry has passed is still listed until collection
    releases it

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

  Mutation actor:
    --actor-kind <user|service|system> optional, requires --actor-id
    --actor-id <stable-id>             optional, requires --actor-kind
    The default service/loonfs-cli identifies the tool, not the human.

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

  If `loonfs get` omits the local destination, the CLI writes to `./<remote-filename>`

  A local destination that ends in `/` or is an existing directory means the
  download lands inside it under its remote name

  `loonfs get` refuses to overwrite an existing local file without --force,
  and writes through a partial file beside the target, so an interrupted
  download leaves nothing truncated at the target itself

  `loonfs get` writes a large file as it arrives and never holds it whole, so
  what it costs in memory follows the transfer and not the file's size; an
  embedded profile reads its own store in chunks, while a remote profile
  streams files past the server's proxy cap directly from object storage and
  receives smaller files in one proxied response

  A file destination is renamed into place only once the download is complete
  and its content verified, so content that fails verification leaves nothing
  at the destination. `loonfs get ... -` hands bytes to stdout as they
  arrive, so the same failure exits nonzero after part of the file has
  already been written — with `-`, the exit status is what says the content
  was verified
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

  An embedded profile's edit latency grows with the WAL tail between
  maintenance passes: every write appends, and reads replay what has not
  been folded into the metadata yet, so a namespace that is written to and
  never maintained gets slower. Embedded profiles run maintenance manually
  — `loonfs admin run --namespace <ns>` hosts it, `--drain` catches up and
  exits, and `loonfs admin step` runs one pass — while a server runs its
  own automatically for the namespaces it serves
```
