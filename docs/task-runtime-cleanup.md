# Terminal cleanup during ordinary task operations

Version 2.8 records the cleanup contract at admission and finalizes its deletion
manifest in the existing finish transaction. New runs use `cleanup_protocol: 2`.
A normal successful, failed or cancelled finish that adopts its output calls
maintenance after the authority receipt is confirmed. A subsequent `run start`
also retries eligible terminal runs of the same task. Worker adapters using
runtime.start/publish inherit this behavior. No daemon, extra schedule, database
or archive tag is introduced.

## Durable completion and disposable resources

In schema 9, `run.json` is the normal run-result source, with no duplicate `result.json`; recovery and checkpoint-delivery results retain their distinct evidence. Operation receipts preserve historical responses. The finish transaction records the result, releases the slot, and writes a
version-2 `cleanup.json` alongside its usual operation receipt. That manifest is
an immutable deletion authorization, not a mutable cleanup progress counter.
It binds repository/task/run/generation, accepted source, original finish
operation, expected work/heartbeat SHAs and the final sampled heartbeat state.
The finalization operation fixes these inputs before its authority CAS.

Task instructions define which output must remain. Accepted artifacts and task
data stay in the current main tree. After confirmed completion, one conditional
authority commit deletes the run's `run.json`, any legacy/recovery `result.json`, `cleanup.json`,
`io/` records and bound operation receipts. Artifact files with those names are
not execution metadata and are preserved.

Late retries and recovery read the original records from ordinary main history.
This prevents run/operation ID reuse without a permanent per-run receipt or tag.
History is not rewritten. Complete authority ancestry is required: direct Git
fetches automatically unshallow the repository; external transports must import
the complete ancestry and fail closed when it is unavailable.

For task I/O runs, the same finish commit also saves the final stream manifest,
last state, immutable events and results under the run's `io/` directory. These
records retain external-operation IDs, verified outcomes and completion markers
in main history without stream commit history or transaction journals. Cleanup compares
the saved blobs with its finish manifest before removing run refs. Completed
stream readers use main history after retirement; no per-run I/O tag is created.

Collector completion is derived from ref/file absence. A new operation ID does
not authorize deleting a changed ref. Every deletion compares the observed ref
with its expected SHA. Two collectors or a lost delete response can safely retry.
A changed ref is retained for inspection. Renewal attempts are limited to the
original terminal run's namespace and use conditional deletion.

For direct Git transport, the configured remote's fetch refspecs identify local
tracking refs under `refs/remotes/`. Matching work/run-stream snapshots and the terminal
run's renewal cache are conditionally removed even when another clone already
deleted the remote refs. Changed, symbolic, ambiguous or unrelated tracking refs
are retained. Custom and negative fetch mappings are respected; external
transports do not assume ownership of Git's tracking cache.

A stale tracking tip may be collected when it is the accepted terminal tip or a
verified ancestor of that tip. Cleanup still compares and deletes exactly the
observed local tip, preserves changed or unprovable histories, and requires the
normal remote-target and durable I/O checks. Coordinator fetches do not refresh
the caller's configured tracking refs merely to make cleanup pass.

The default preserves accepted artifacts/results, not every intermediate source
commit or raw execution log. Save required diagnostics as run artifacts before
finish. No new archive tag is created. Removing refs does not immediately remove
Git objects; host/local Git GC still determines physical object retention.

## Normal entry points and interruptions

1. Stop the worker's writes and children, resolve external operations, and finish.
   For manually managed workers, finish explicitly relinquishes use of their
   workspace. Do not call finish while another publisher still writes there.
2. Read back the original finish receipt. Unknown finish outcomes do not start
   cleanup; retry that same operation.
3. Remove matching remote run refs, then remove eligible local resources.
4. Retire execution metadata with a conditional authority commit. A failed
   retirement leaves the metadata available for retry; finish remains successful.
5. If any step is interrupted, a later start/finish or explicit cleanup retries
   from the durable manifest and the actual remaining resources.

The normal start path examines the same task's terminal manifests and existing
run refs/local sidecars, completing at most eight collections per call.
Retained workspaces, failed collection and changed refs do not consume this
quota; a pass may inspect additional retained entries to reach later runs.
Calls report their
business result independently of best-effort collection; the explicit cleanup
command remains available to inspect a plan or diagnose a retained resource.
Progress requires later calls and functioning transport. There is no background
promise to collect a task that never runs again.

Remote cleanup works from a fresh clone and never assumes that it can remove
another machine's files. Local cleanup runs when a later invocation on that
machine can access the recorded checkout.

## Local ownership and concurrent processes

Only a recorded runtime-owned linked worktree is removed. Repository identity,
run generation, symbolic branch, source HEAD, accepted artifact manifest and
absence of staged/unstaged/untracked/ignored files are checked. The invoking
checkout and current working directory are retained. A later call from a
surviving checkout can remove them. Executor-owned workspaces and reservations
remain owned by the executor; their terminal runtime sidecars are removed after
confirmed process termination.

Runtime local locks now hold a shared namespace gate. Collection takes that
fixed gate exclusively, nonblocking, so it cannot unlink a lock file while
another process holds or waits on its old inode. A running/unknown execution
receipt also retains local output. The fixed `.locks.lock` file remains for the
lifetime of the clone; it is not a per-run resource.

Once a workspace and its exact local branch are gone, the original run's local
state, observation, heartbeat, execution and cleanup records, raw logs and lock
files are removed. State is removed last, making interruption after worktree
removal resumable. Missing-but-registered worktrees are retained because their
storage could be temporarily unavailable.

Observer-only clones and finish replays can leave observation caches and idle
lock files without a workspace ownership record. These exact files are also
collected under the exclusive gate after terminal identity/generation checks.
Other unowned journals or logs, and observations for another generation, are
retained rather than inferring that unknown work is safe to discard.

Confirmed operation/publication journals are also collected under the exclusive
gate, using matching immutable remote receipt fingerprints. Unknown or mismatched
journals and candidate pins are retained. No general cache, Docker, credential,
foreign worktree or arbitrary directory cleanup is performed.

Deploy runtime 2.11 to all readers and writers together after finishing or
reconciling old runs. First admission or metadata retirement upgrades the authority
to schema 9 in the same CAS, only after all old-schema task slots are idle. Old
binaries reject schema 9 instead of creating old heartbeat refs or treating a
retired run as new. Idle slots retain only their generation counter.
Older manifests without saved I/O records keep their stream for explicit migration.
Legacy local-worktree cleanup still requires stop confirmation. To remove only
confirmed legacy execution metadata without claiming its old checkout, inspect
`run cleanup --task <id> --run-id <run> --metadata-only`, then add `--apply`.
Existing archive tags are not purged by metadata retirement.

## Failure and timeout boundaries

Failed/cancelled runs whose output is adopted use the same collection path as
successful runs. Lost/revoked runs, explicit discard-output results, unknown
external outcomes and unpreserved files are retained. Their recovery conditions
must be resolved before data can be discarded; age alone is not authorization.

This change does not move lease authority out of main. Strict fenced leases and
generation timeouts still renew via main CAS. Cleanup of a new terminal manifest
requires no fresh trusted-clock sample. Metadata retirement uses a terminal-only
guard and cannot revoke or change a replacement generation. External lease
adapters must accept the narrowly scoped `run-cleanup` / `metadata_only` deletion
of the original terminal run's records, while rejecting task output deletion.

## Validation

Recovery notices are saved on the authority in the recovery transaction. Deferred
automatic cleanup writes clone-local notices. `run report --format markdown`
lists current refs and local leftovers, including retired records read from their
original history; see [retained resource reports](task-runtime-reports.md).

`test/task_metadata_retirement_test.py` covers retained task output, history-based
replay, shallow clones, schema adoption, merge deletions and uncertain push replies.
`test/task_cleanup_test.py` covers authority-write count, automatic normal and
failed completion, repeated cleanup, deletion failure/lost replies, fresh-clone
remote cleanup, local lock exclusion, current-checkout and dirty-file retention,
unknown journals, interrupted removal and concurrent collectors. Existing
runtime tests retain explicit legacy cleanup coverage. Lease/timeout, transport,
migration, worker adapters and source/zipapp distribution also require local
regression validation. No test Actions are required.
