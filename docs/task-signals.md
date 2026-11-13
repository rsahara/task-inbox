# Task-owned signals

Signals are persistent task data, not a generic notification channel. Ordinary
outputs remain in each task's data area and readers inspect them on main.
A task explicitly publishes signals at `<data_path>/signals/*.json`. Each record
has `schema_version: 1`, `task_id`, stable `id`, positive integer `revision`, a
nonempty `type`, and `status` (`active`, `resolved` or `withdrawn`). Ended records
also require a nonempty `resolution`. Application fields remain caller-owned.
Multiple types may coexist; assessments and priorities are outside this runtime.

## Adoption

Deploy runtime 2.12 to every reader/writer and finish or reconcile all old runs.
Run `task-inbox --operation-id enable-signals signal enable` to opt into schema
10. Older writers are fenced. Repositories that do not enable signals retain
ordinary application-data behavior. This replaces the unmerged experimental
outbox contract; it does not migrate deployed outbox records.

## Publish and read

From an owned, prepared run, use `signal put --task TASK --run-id RUN --file FILE`,
then normal `run publish` or `run finish`. Publication validates the owner and
revision in the same transaction that saves task data to main. New IDs start at
revision 1; changing a record requires a higher revision. Keep resolved/withdrawn
records in place. Physical deletion and GC are unsupported in this release.

`signal list --task TASK` reads current main without a subscription and displays
active records. Use `--include-ended` for all records. Python `catalog` includes
ended records by default for reconciliation. No duplicate head/evidence store
exists; each ID's latest state and revision live in the task's data area.

## Subscribe and synchronize

`signal subscribe --task SOURCE --consumer TARGET` records a task-level route.
Receivers select types themselves. Subscription and reading are independent:
list existing state when beginning a subscription, then synchronize. Sync also
notifies late subscribers of current records, including ended records.

`signal sync` reads current signals from main and compares their deterministic
message IDs (source task, signal ID, revision) with existing messages in each
subscriber inbox. It sends only missing messages, reusing inbox CAS/idempotency.
There is no delivery ledger, outbox, publication intent, or per-subscriber cursor.
Completed inbox messages still suppress duplicate delivery; retain inbox messages
under the existing inbox retention contract. Conflicting IDs are reported while
other destinations continue. Transport failure may stop a pass; retry from any
clone. Runtime publication/finish sync the producer and run admission syncs the starting task's incoming/outgoing
routes, recovering a crash after saving data or a partial fanout. Explicit sync
returns `needs_attention` for collisions; publication callers fail visibly. Admission reports collisions as structured
`signal-sync-blocked` diagnostics on stderr and still permits owned repair runs;
unrelated routes do not prevent startup. A producer may publish a higher revision
to recover without changing the existing inbox message.

The guarantee is convergence to the latest state on a successful subsequent
sync, not delivery of every intermediate revision. A later revision may replace
an undelivered one. Sync alone does not execute a subscriber or guarantee timing.

## Receive

Notifications contain only task, signal ID and revision. Treat them as hints.
Capture the pending inbox subset, read current main and evaluate all current
states together (including terminal states), save the resulting application
output, then complete only those captured messages. `read_notice` validates the
hint against current main and returns current evidence references; it does not
reconstruct a historical event. New arrivals remain pending for another run.
Ownership, locks, run recovery and human approval remain the caller's contracts.
