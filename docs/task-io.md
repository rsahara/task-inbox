# Task I/O

`bin/task-inbox io` provides shared inter-task communication using only Git and Python. It requires no persistent runner, Actions workflow, or new schedule. Producers can send messages while the consumer is stopped or paused. The existing permanent feedback intake through `issue-inbox/` remains separate from this feature.

## Storage and ownership

| Ref | Contents |
| --- | --- |
| `main` | Task definitions, slots/generations and accepted results at `tasks/<id>/io/receipts/<message-id>.json`; completed execution metadata is retained only in ordinary Git history |
| `task-io/<id>/inbox` | Immutable `messages/<message-id>.json` records, conditionally appended by multiple producers |
| `task-io/<id>/runs/<run-id>` | `state.json`, immutable `events/<event-id>.json` and `results/<message-id>.json` records |
| `task-runtime/<id>/<run-id>/work` | Existing artifact checkpoints |

All I/O refs contain data only; they neither execute code nor merge into main. Branch manifests bind repository UUID, authority, task, and run/generation. They reuse the existing `Store` conditional pushes, operation receipts, local journals, and reconciliation after lost responses. No separate controller ownership or Site lock is added. Transport adapters use the same environment settings as the authority.

The first run-stream transaction saves the manifest and initial state with sequence 0 together. Existing observation and stale-run recovery can therefore verify the run/generation even if emit/complete occurs before the first heartbeat, or execution stops immediately after initialization.

Even when heartbeats and results share a run stream, main alone determines slot ownership. `io complete` saves the immutable candidate internally and accepts it only after verifying the current slot/generation in a main transaction. The caller supplies one stable operation ID and the same result body on every retry. Candidate persistence alone leaves the message pending. A stream left by an old worker does not grant new ownership. Lease renewal and revocation also happen on main; elapsed time on a heartbeat ref alone does not authorize taking ownership.

## Run streams

Every run uses task I/O automatically. There is no `io enable` command or per-task opt-in. Admission pins `io_protocol=1`, `io_ref`, and `heartbeat_ref` to the run stream; `run heartbeat` updates its `state.json` alongside immutable events and results.

During migration to schema 9 (and previously schema 8), admission saves `io_ref_prefix` on the authority. Without an explicit prefix,
the default is `task-io`; if it overlaps the authority or work namespace, runtime
2.10.1 and later select `task-io-2`, `task-io-3`, and so on until they are disjoint. An
explicit prefix is preserved and invalid or overlapping explicit values are
rejected. Existing inboxes keep their namespace and pending messages.

Deploy runtime 2.11 to every reader and writer together after finishing or reconciling earlier runs with their original runtime. The first admission upgrades an idle authority to schema 9, fencing older binaries. Old run records remain readable for inspection; this runtime cannot resume execution of old heartbeat runs.

## Requests and startup

```sh
python3 bin/task-inbox --operation-id send-request-123 io send \
  --task example --message-id request-123 --kind render --payload-file request.json
python3 bin/task-inbox io pending --task example
python3 bin/task-inbox --operation-id wake-request-123 io wake \
  --task example --runner caller-123 --run-id consumer-123
```

The payload is a JSON object of at most 256 KiB. Store larger artifacts in Git and reference their commit/path/hash. Bind each message ID to its content; reusing an ID with different content is rejected. Keep a stable operation ID for each operation and reuse it when retrying an unknown outcome. Do not start the consumer before confirming delivery of the request.

`wake` acquires a slot through the normal `run start` path. On `action=start`, **the caller executes task.md using the returned run**, beginning with the normal `run prepare`. Leave the request pending on `busy` / `paused`, and exit on `idle`. `wake` itself creates neither a process nor another chat. Do not start another run for a run that has already begun.

The inbox consumer in this version requires `max_runners=1`. Multiple producers can send concurrently, and per-run heartbeat/event streams also support parallel runs. An optional `completion_contract` in the task definition is pinned as the default contract for each new run at startup. Consumers should register the contract their business operation requires to prevent successful finish without verification.

A consumer with maintenance work outside the inbox can use `io wake --force` after confirming pending work. This still acquires a normal task slot; it does not authorize repeating external effects.

Consumers process requests while fetching additions. After draining the inbox and finishing, read the inbox again and wake another run if needed. Each existing parent-task invocation must also check pending requests, even when no new request has arrived. If the executor itself stops immediately after finish, the next existing invocation resumes processing. A Git push alone does not guarantee a future execution.

## Progress, external operations, and results

```sh
python3 bin/task-inbox run heartbeat --task example --run-id consumer-123 --progress preparing
python3 bin/task-inbox --operation-id save-intent-123 io emit --task example \
  --run-id consumer-123 --event-id save-intent-123 --kind external-intent --payload-file intent.json
python3 bin/task-inbox --operation-id complete-request-123 io complete --task example \
  --run-id consumer-123 --message-id request-123 --result-file verified-result.json
python3 bin/task-inbox io status --task example --message-id request-123
```

Events written by `emit` are immutable. Before an external save/deploy call, emit and read back the original operation ID and input. Once the outcome is known, save a receipt containing the original ID under a different event ID. Do not replace an unknown outcome with a new operation. `complete` accepts only `succeeded`, `failed`, or `superseded` statuses. Do not acknowledge unknown outcomes; retain the original run and intent for reconciliation. The consumer verifies the evidence required by its business operation before supplying it to `complete`. Shared I/O format validation alone cannot establish the provider's actual state.

Completion commits the result body, message hash, and original result ref/commit/path/hash to main. The normal finish transaction also saves the final run manifest, last state, immutable events and result records under `<task-path>/runs/<run-id>/io/`. This includes original external-operation IDs, verified outcomes and batch completion records used for recovery. It copies the final records as ordinary blobs, without retaining heartbeat history, stream transaction journals or raw execution logs. The same transaction records the exact saved blob manifest in `cleanup.json`, commits the run result and releases its slot.

Before saving a candidate, `io complete` reserves the public operation ID and request fingerprint on main using an internal derived ID. Every authority transaction checks this binding, including from other runs/clones and after metadata retirement. This preparatory receipt is not an accepted result. If interrupted before or after candidate persistence, retry `io complete` with the same operation ID and body. A lost acceptance reply replays the original authority receipt even after heartbeats advance, the run ends, metadata is retired, or stream refs are collected. A changed body, run, message or operation kind under the same operation ID is rejected. Old workers cannot accept a pending candidate after ownership is revoked.

After confirming finish, cleanup verifies those records on main and conditionally deletes the original run branches. A subsequent authority commit removes disposable execution metadata, including the archived `io/` directory. A fresh clone reads completed run records from ordinary main history even after the old stream commits have been garbage collected. Inbox messages, accepted receipts, task data and artifacts retain their existing storage. No preservation tag is created.

Running, lost or discarded-output runs retain their recovery branches. Runs finished by older binaries without saved I/O records keep their existing stream instead of creating another tag. Readers still support existing legacy preservation tags; this update neither creates nor purges them. Completion events in recovery history remain available, so an old stream alone must not revive a closed batch. Deploy runtime 2.11 to all callers before the first schema-9 admission. See [rollout and record contracts](task-runtime-slim.md).

When external publication overwrites a live URL, a Git generation check alone cannot invalidate an old provider operation. A run with an unknown outcome must use normal runtime recovery with stop verification and external-operation reconciliation.

## Local validation

```sh
python3 -m unittest discover -s test -p task_io_test.py
python3 -m unittest discover -s test -p task_runtime_test.py
```
