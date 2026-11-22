# loonfs-merge

`loonfs-merge` runs the loop that merges a LoonFS fork into the namespace that it was forked from. You'd use it to merge a fork that might be too large for one commit, or whose changes might conflict with changes made to the namespace since the fork. The server does the merging, the splitting, and the rebasing, so this crate keeps only the session's progress. If you only need to send one merge request, call `Client::merge_fork` on [`loonfs-client`](../loonfs-client) instead. The `loonfs merge` command in [`loonfs-cli`](../loonfs-cli) runs this loop for you.

A `MergeSession` runs the merge as a series of steps, and each step sends one or two requests. A merge that's too large is merged in pieces, through the split point that the server's `merge_too_large` error suggests. A merge that conflicts is rebased into a new fork, a `Resolver` writes resolutions into the new fork, and the new fork then merges in its place. `DefaultResolver` resolves nothing, so a session whose rebase left out a conflicting change fails with the conflict reports and the new fork for you to inspect. A session creates at most `MAX_NEW_FORKS` (8) new forks, and it deletes them once it has merged. (You delete the original fork yourself.)

The session sends its requests through a `Mechanics` implementation. The HTTP `Client` from `loonfs-client` implements `Mechanics`, and a test can implement it with scripted responses. This example, shortened from the CLI's merge command, starts a session and runs it to the end:

```rust
use loonfs_merge::{DefaultResolver, MergeSession, MergeSessionOptions};

let mut session = MergeSession::recover(
    client,
    DefaultResolver,
    namespace_id,
    fork_id,
    actor_id,
    &MergeSessionOptions::default(),
)
.await?;
while session.step().await?.is_some() {
    let saved = serde_json::to_vec(session.state())?;
    // Write `saved` somewhere durable before the next step.
}
```

Save the session's `MergeSessionState` after every step. A step updates the state only after its requests return, so if a step fails or the process stops, `MergeSession::resume` continues from the saved state, and the next step sends the same requests again with the same commit IDs. `MergeSession::recover` starts a session for a caller that has no saved state, and it finds the new forks that an earlier run created by reading the namespace's snapshot inventory. A session that fails stops in the `failed` status, and `MergeSession::abandon` then makes its next steps delete its new forks.

The design note [Merging a fork](../../docs/design/merge.md) describes the loop and the server operations that it uses.
