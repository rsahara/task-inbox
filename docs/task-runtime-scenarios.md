# Local end-to-end scenarios

Run these from the source checkout with Git and Python 3.10+ on Linux, macOS or WSL:

```sh
python3 -W error::ResourceWarning -m unittest discover -s test -p task_scenarios_test.py -v
```

These five tests also run automatically with the full suite:

```sh
python3 -W error::ResourceWarning -m unittest discover -s test -p '*test.py'
```

The five tests create a disposable local bare Git remote and two independent
checkouts. Workers run in separate processes using the public Python runtime
API. The test controller coordinates them through JSON-line barriers and kills
whole worker process groups with `SIGKILL`. No GitHub account, network, scheduler
or installed adapter is required. The source distribution includes the tests and
their worker fixture.

| Scenario | Verified behavior | Contract boundary |
| --- | --- | --- |
| 1: push, kill, retry before/after heartbeat expiry | A recent observation keeps the slot occupied; at expiry, explicit `reap --apply` releases it, fences the old writer, and permits a new generation to finish | A lost run retains its refs, workspace and recovery evidence; expiry does not authorize deletion |
| 1: verified stop, restore and finish | Another clone restores the checkpoint before revocation, adopts the output with a failed finish, and collects its run refs; the original clone collects its own workspace on the next start | This requires confirmed stop and settled operations, and cannot restore a run already marked lost |
| 2: drain task-a into task-b while receiving additions | The same task-a run refetches one item at a time, forwards all original and newly added payloads exactly, acknowledges them, and finishes with an empty inbox | A post-finish push waits for the next invocation, which the test also exercises |
| 3: parallel task-a runs share serial task-b | Both parent runs coexist; the second sees `busy` while the first owns task-b, then acquires the next generation; each parent reads its own distinct result (77 or 91) | Callers implement polling, child execution, and correlation by request ID; `wake` only admits a run |
| 3: kill the first caller inside task-b | After stale-run recovery, the waiter completes with 91; the revoked run cannot write a late result, and its request remains pending | Recovery does not invent a successful result for the killed request or automatically replay its computation |

## Heartbeat and cleanup semantics

The tests replace only the observer's monotonic clock, keeping subprocess, Git
and lock deadlines real. An initial observation at 100 seconds starts the
300-second unchanged-ref window. At 399 seconds the run is still owned; at 400 it
is eligible for Git-only recovery. They do not wait five real minutes or infer
death from a Git commit timestamp. A retained observer is required for this
policy. Ordinary `run start` does not automatically reap Git-only runs; the caller
must invoke `run reap --apply`. Manual recovery requires verified stop evidence.
Stateless recovery based on elapsed time needs an installed lease/timeout
integration; stop verification also works with disposable clones.

"Explicit reap" means the caller invokes the recovery operation separately from
`run start`. It can be called automatically at the beginning of an existing task
invocation or schedule; it does not require a person or a new background service:

```sh
python3 bin/task-inbox --repo /path/to/task-data run reap \
  --task task-a --apply --observer-profile retained
```

For the `git_only` policy exercised here, this inspects occupied slots and observes
heartbeat refs. An unchanged ref for the configured observation window permits
marking that run `lost` and releasing its slot; a recent observation keeps the
slot occupied. The first observation starts the window, so an initial call alone
does not establish expiry. Without `--apply`, the command only reports its plan.

Thus the requested sequence “expired heartbeat -> delete pushed leftovers ->
successful exit” is **not the current automatic recovery contract**. Reaping
marks the run `lost` and saves a retained cleanup record. Even an explicit cleanup
with stop confirmation refuses to delete a lost run's output. The first test
asserts that behavior, including preservation of the original work and heartbeat
SHAs after a replacement finishes.

For a worker whose process and operations are confirmed stopped, the alternative
tested path restores the still-authorized run, finishes it as failed while
adopting its checkpoint, and collects disposable refs/workspaces. Accepted
artifacts and result records remain on main. See the
[recovery protocol](task-runtime.md#restore-and-cleanup) and
[cleanup contract](task-runtime-cleanup.md#failure-and-timeout-boundaries).

## Inbox and serialized execution

The forwarding fixture pauses after acknowledging its first item. The producer
then appends two more messages from the other checkout while task-a's slot is
still occupied. The consumer refetches after each acknowledgement, so the test
checks additions during execution rather than only a fixed initial batch. A
separate invocation also handles a message appended after the first run finishes.

For serialized child work, task-a has two slots and task-b has one. Each parent
sends a unique request to task-b and polls `wake`. The controller holds the first
child until the second has reported an actual `busy` response, then allows
completion. The child acknowledges its own request and finishes before the
parent reads that request's accepted receipt. Both run streams can already have
been collected when those results are read; saved I/O and parent artifacts are
checked on main as well.

The failure variant kills the first process while it holds task-b. The controller
reaps that stale Git-only child; the second process then acquires the slot. The
first request remains pending and its recovery stream remains available. The
controller separately recovers the stopped parent, so neither task retains an
occupied slot at test completion.

Polling uses a 50 ms interval for fast tests, with a 30-second acquisition deadline.
Event barriers and process exits also have deadlines, and cleanup kills/reaps any
unfinished workers. These tests detect stalls in the exercised interleavings;
they do not establish fairness or freedom from deadlock for arbitrary caller code.
