# Task I/O

Use [`execute`](task-execution.md) as the caller entrypoint for readiness,
admission and supervision of one task per invocation. The primitives below
remain available for integrations managing those steps themselves.

`bin/task-inbox io` provides durable inter-task messages using Git and Python.
Producers can send while a consumer is stopped or paused. Delivery never starts
another task; the caller owns startup and scheduled/manual reconciliation.

This guide describes runtime 2.14 with schema 11. Adoption is explicit: follow
[main inbox migration](task-main-inbox.md#migration-and-host-integration) after
updating and draining all writers. `init` and normal admission do not enable
schema 11. See [legacy storage](#legacy-storage-before-schema-11) for repositories
that have not migrated.

## Storage and ownership

See the [responsibility and storage map](../README.md#responsibility-and-storage-map)
for how shared Git relates to callers, task definitions and signals.
The following paths use the default task root and branch names; configured task
roots, data paths and I/O namespaces still apply.

| Ref | Contents under schema 11 |
| --- | --- |
| `main` | Task definitions, slots/generations, task data and signals |
| `main` | Immutable requests at `tasks/<id>/io/inbox/messages/<message-id>.json` and accepted results at `tasks/<id>/io/receipts/<message-id>.json` |
| `main` | Immutable run events at `tasks/<id>/runs/<run-id>/io/events/<event-id>.json` and the final run I/O checkpoint; completed execution metadata is retained in ordinary Git history after retirement |
| `task-io/<id>/runs/<run-id>` | Isolated heartbeat stream with `state.json` and its run manifest |
| `task-runtime/<id>/<run-id>/work` | Explicit remote work checkpoints, when created |

Messages are immutable and retained after completion to suppress duplicate
delivery. A message is pending until main contains its accepted receipt.
Only main's current slot/run/generation can accept a new result; heartbeat
silence or an old stream does not grant ownership. Lease renewal and revocation
also use main. All refs contain data; none executes a worker.

## Run streams

Every run pins `io_protocol=1`, `io_ref` and `heartbeat_ref` at admission. There
is no `io enable` command or per-task opt-in. The separate run stream is created
when needed; its first transaction saves the manifest and initial `state.json`
together, so a visible heartbeat ref already identifies the run/generation.
`run heartbeat` updates this stream without staging business files or merging
heartbeat history into main.

Under schema 11, `io emit` and business event/result transactions use the
authority CAS. Active run I/O readers combine those authority records with the
heartbeat stream, including when no heartbeat ref exists yet. A heartbeat alone
cannot replace or hide an accepted external-operation intent.

The default I/O namespace is `task-io`. If it overlaps the authority or work
namespace, automatic selection uses `task-io-2`, `task-io-3`, and so on. Explicit
invalid or overlapping prefixes are rejected; existing pinned namespaces remain
unchanged.

## Requests and startup

Run these commands from the task-data checkout, or supply the global
`--repo /path/to/task-data` option before `io`/`run`:

```sh
python3 /path/to/task-inbox/bin/task-inbox --operation-id send-request-123 io send \
  --task example --message-id request-123 --kind render --payload-file request.json
python3 /path/to/task-inbox/bin/task-inbox io pending --task example
python3 /path/to/task-inbox/bin/task-inbox --operation-id wake-request-123 io wake \
  --task example --runner caller-123 --run-id consumer-123
```

The payload must be a JSON object of at most 256 KiB. Store larger artifacts in
Git and reference their commit/path/hash. Reusing a message ID with different
content is rejected. Keep a stable operation ID for each mutation and reuse it
when retrying an unknown outcome. Confirm delivery before starting a consumer.

`wake` acquires a normal run slot. On `action=start`, the caller begins with
`run prepare` and executes the returned run's pinned task instructions. Leave
requests pending on `busy`/`paused`, and exit on `idle`. Neither `wake` nor a push
launches a worker. Do not admit a second run for work already admitted.

Inbox consumers require `max_runners=1`; multiple producers can send concurrently.
An optional task `completion_contract` is pinned at admission. For maintenance
outside the inbox, `io wake --force` still acquires a normal slot and does not
authorize repeating external effects.

Workers capture the messages they handle and verify each result. They may fetch
additions during execution, excluding messages already captured for deferred
completion. After finish, the caller rechecks pending work and independently
invokes the next task/run. Arrivals after that read remain durable for the next
scheduled/manual reconciliation.

## Progress, external operations, and results

```sh
python3 /path/to/task-inbox/bin/task-inbox run heartbeat \
  --task example --run-id consumer-123 --progress preparing
python3 /path/to/task-inbox/bin/task-inbox --operation-id save-intent-123 io emit \
  --task example --run-id consumer-123 --event-id save-intent-123 \
  --kind external-intent --payload-file intent.json
```

Events written by `emit` are immutable authority records. Before an external
save/deploy call, emit and read back its original operation ID and input. Once
known, save the verified outcome under a different event ID, retaining the
original business-operation ID. Never replace an unknown outcome with a new
operation or acknowledge it as complete. Git generation checks alone cannot
invalidate delayed provider operations; recovery requires stop verification and
external-operation reconciliation.

Prefer atomic acceptance with successful finish. Create `results.json` mapping
only captured message IDs to verified results, for example:

```json
{"request-123": {"status": "succeeded"}}
```

Then finish with the same operation ID on retries:

```sh
python3 /path/to/task-inbox/bin/task-inbox --operation-id finish-consumer-123 \
  run finish --task example --run-id consumer-123 --outcome succeeded \
  --input-results-file results.json
```

The finish CAS saves business output, supplied input results, terminal run state,
slot release and subscriber notifications together. A conflict rejects the
whole commit and retains ownership for reconciliation. Finish never automatically
acknowledges every pending message; late arrivals remain pending. A business
completion contract can additionally require `--completion-file` evidence.

The Python `main_io.stage_results` API can defer a frozen result set locally;
normal successful finish accepts it. Intermediate publication, failure,
cancellation and discard leave these deferred results pending. Explicit finish
results can also report a verified failure. See the full
[workspace and I/O contract](task-main-inbox.md#workspace-and-io).

For an explicit mid-run acknowledgement, use:

```sh
python3 /path/to/task-inbox/bin/task-inbox --operation-id complete-request-123 \
  io complete --task example --run-id consumer-123 --message-id request-123 \
  --result-file verified-result.json
python3 /path/to/task-inbox/bin/task-inbox io status \
  --task example --message-id request-123
```

Under schema 11, `io complete` writes the accepted receipt in one authority
transaction, independently of terminal finish. It does not snapshot dirty
business files. Reuse the same operation ID and result body on retry. Accepted
statuses are `succeeded`, `failed` and `superseded`; format validation does not
verify the provider's actual state.

Normal finish freezes the final heartbeat checkpoint and authority I/O records
under `<task-root>/runs/<run-id>/io/`, with their exact manifest in `cleanup.json`.
It retains final blobs, without heartbeat ancestry, stream journals or raw logs.
After confirmed finish, cleanup verifies that manifest, conditionally deletes
eligible run refs, then retires disposable metadata. Completed records remain
readable through ordinary main history after GC; inbox messages, accepted input
receipts, task data and artifacts remain. Running, lost and discarded-output
runs retain recovery resources. See [cleanup](task-runtime-cleanup.md).

## Legacy storage before schema 11

Schemas 9/10 remain supported until explicit migration. They use separate
`task-io/<id>/inbox` branches for immutable `messages/<message-id>.json`, and
`task-io/<id>/runs/<run-id>` for heartbeat state, immutable events and result
candidates. Main remains the authority for slots and accepted receipts.

Legacy `io complete` internally reserves its public operation ID and request
fingerprint on main, saves an immutable candidate in the run stream, then accepts
it in a generation-checked main transaction. Candidate persistence alone leaves
the message pending. Retry the same public operation ID and result body after
interruption or a lost response, including from another clone. A changed body,
run, message or operation kind under that ID is rejected. Historical successful
receipts replay their response and never grant new execution authority.

Schema 9 adoption upgrades an idle schema-2–8 authority after earlier runs drain;
old heartbeat runs must be reconciled with their original runtime. Schema 10
signals use [latest-state reconciliation](task-signals.md), rather than atomic
finish/fanout. Schema 11 migration imports inbox messages and preserves accepted
receipts and legacy refs; it does not remove the old branches or authorize old
writers to resume. Never edit the schema number by hand.

## Signal delivery

Schema 11 [declared signals](task-main-inbox.md#declared-signals) support exact
output IDs, automatic revisions and `change`/`run` subscriptions. Successful
finish commits subscriber messages atomically with source output. Notifications
contain frozen signal values; consumers may instead coalesce and read latest
main state. `signal sync` can reconcile current-state deliveries, but cannot
reconstruct missed run events. No delivery command launches a consumer.

## Local validation

```sh
python3 -m unittest discover -s test -p task_main_inbox_test.py
python3 -m unittest discover -s test -p task_io_test.py
python3 -m unittest discover -s test -p task_runtime_test.py
```
