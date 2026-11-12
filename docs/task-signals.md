# Git-backed task signals and durable delivery

A signal is persistent task-owned data, not an operating-system signal or IPC.
Each task may publish several signal types in its data area's `signals/*.json`.
Consumers receive immutable inbox references to saved Git objects. The runtime
never interprets assessment, investigation value, priorities or handler policies.
Those are application tasks and arbitrary signal fields/payloads.

## Enable and subscribe

Deploy runtime 2.12 to every reader and writer. Drain or reconcile all existing
runs with their original runtime, then enable signals once with a stable ID:

```sh
python3 bin/task-inbox --repo /path/to/tasks --operation-id enable-signals signal enable
python3 bin/task-inbox --repo /path/to/tasks --operation-id subscribe-assessment \
  signal subscribe --task producer --consumer assessor
python3 bin/task-inbox --repo /path/to/tasks --operation-id subscribe-opportunities \
  signal subscribe --task producer --consumer planner --type opportunity
```

Enable upgrades an idle authority to schema 10, fencing older binaries. Ordinary
repositories stay on their existing schema until this explicit opt-in. Task
locations and data paths remain configurable; no task number, company identity,
service account or executor is assumed. Subscriptions are explicit and stored at
`.task-system/signal-subscriptions.json`. They select the producer and optional
signal type. New subscriptions receive future changes; read `signal list` for a
bootstrap snapshot. Subscription changes do not alter already-queued recipients.
The producer cannot subscribe to itself. Cross-task feedback cycles are an
application responsibility; avoid generating changes from receipt alone.

## Update signals

After normal `run start` and `run prepare`, create a record outside the repository:

```json
{
  "schema_version": 1,
  "task_id": "producer",
  "id": "sync-errors",
  "revision": 1,
  "type": "observation",
  "status": "active",
  "payload": {"summary": "Repeated synchronization failures", "evidence": ["reports/check.json"]}
}
```

Use `signal put --task producer --run-id RUN --file /path/to/signal.json`, followed
by normal `run publish` or `run finish`. Put changes the owning prepared workspace;
it does not push data or grant a new execution slot. `signal list --task producer`
returns saved records under the `signals` key. Additional JSON fields are allowed.
IDs are stable within a task and revision is a positive integer. Changes require a newer revision. Publish
`resolved` or `withdrawn` plus a nonempty `resolution` before removing a signal.
Removal retains its last published version in the control ledger and Git history;
recreation requires a newer revision. Missing data is never inferred to mean a
resolved active signal. Duplicate IDs in one producer fail publication.

Python consumers can use `signals.catalog(snapshot, task_id, include_removed=True)`
to retain explicitly removed ended records. Its `evidence_path`, when available,
points to the durable control-ledger version even before removal. Read the logical
signal with `signals.read_value(snapshot, evidence_path)`; removing a tombstone
does not change that logical value. Default catalogs and `signal list` show only
current task data. Enable seeds evidence for existing signals.

A newer revision may change type. Both old-type and new-type subscribers receive
that change once, allowing old-type consumers to remove it from their selection.

Both CLI and Python `runtime.publish` enforce the contract, including when a worker
edits signal files directly. A discarded output does not update signals or queue
notifications. A no-change publication emits no signal event.

## Publication and recovery

Signal changes and immutable outbox records are saved in the same authority
transaction. Each event is `kind=signal-changed` and contains the producer, signal
ID/type/revision/status, path, SHA-256 and `publication_commit`. It contains no
signal payload. Recipients are pinned when publication commits. The producer then
attempts inbox delivery. Each recipient has its own idempotent delivery identity;
partial fanout and a lost push response can be retried without duplicate messages.

Data may already be saved and a finish may already have released its slot when
inbox delivery fails. Preserve the original operation ID; retry it, or recover from
another clone with `io flush`. Delivery intents live on Git, not only on the VM.
Consumers should call `io flush` before checking their inbox. Outbox records remain
on authority for recovery; this release does not garbage-collect them.

`signals.read_notice(store, message)` verifies the referenced authority commit,
owner and record hash (or explicit removal evidence). Consumers reconcile against
current data and revisions, so an old notice must not roll current state back.
Delivery is separate from execution and acknowledgement: Git push, `io flush` and
`io wake` never launch a process. The caller's executor or scheduler starts the
returned run and verifies application output before `io complete`.

## Other application notifications

The same transport supports application-defined kinds without duplicating outbox
logic. `outbox.prepare(store, task_id, run_id, operation_id, messages)` writes an
immutable intent in the owning workspace. Each message has `task_id` (recipient),
`message_id`, `kind`, and a JSON object `payload`. The normal publication captures
the intent and data together and injects their `publication_commit` at delivery.
That field is reserved. Payloads must fit the I/O limit including the injected
commit. The application owns source selection and any semantic normalization.

For data already saved on authority, `outbox.enqueue(store, operation_id, task_id,
source_commit, messages)` queues explicit references and attempts delivery. This
is useful for migration and reconciliation; prepare an intent before publication
when delivery must survive a producer disappearing immediately after its save.
Use the same operation and message identities on retries. Published intent files
are immutable and excluded from application change selection to avoid feedback.

Tests use local bare Git remotes and synthetic records:

```sh
python3 -m unittest discover -s test -p task_signals_test.py
```
