# Business completion and runtime termination

Runtime ownership, business results, and admission state are separate. A `running` status or heartbeat alone does not prove that a process is alive. Successful termination requires reconciliation of the required persistence and external-operation results.

## Shared contract

Use `run start --completion-contract-file contract.json` to pin required steps and request identity, for example `{"required":["git","delivery"],"context":{"request":"stable-request-id"}}`. Do not embed service names or task numbers in the runtime. Company repositories can use the same source or zipapp and customize caller-side verification.

`run completion --record-file receipt.json` saves business results in the `completion` field of `tasks/<id>/runs/<run>/run.json` through a normal Git transaction with `--operation-id`. The record contains:

- `repository_id`, `task_id`, `run_id`, `generation`, and `context` matching the original run.
- `steps` with exactly the required step keys. Each step has a stable `operation_id`, a `status`, and nonempty `evidence`.
- A `status` of `succeeded`, `failed`, `unknown`, or `not_started`. Before an external operation, record its original operation ID, input, and reconciliation target as `unknown`. Use `not_started` only when the operation is confirmed not to have been issued.
- `operations_stopped`, confirming that the worker's business operations, external operations, and related children have no unfinished work. This is separate from a waiting controller or completion of independent downstream tasks.

Evidence for each successful step must be verifiable from another machine, such as a destination commit/hash, the original external-operation ID, saved version, publication ID, or readback result. Do not use secrets or a local path alone as evidence. Reconcile a lost response using the original ID; do not reissue an operation with an unknown outcome under a different ID. Do not rewrite evidence for successful steps.

The aggregate result is `succeeded` when all steps succeed and related operations have stopped. An unknown result produces `unknown`; unfinished related operations produce `partial`. Once stop and outcomes are confirmed, any failed or unexecuted step produces `failed`. `finish` accepts only the outcome matching this confirmed result. Partial or unknown completion retains ownership and evidence.

If the result-recording push response is lost, reconcile the original transaction using the same operation ID and record. For the normal terminal path, supply `run finish --completion-file receipt.json --outcome succeeded` (or the matching failed outcome). It validates the same immutable-step and original-operation rules and atomically combines completion evidence in `run.json`, output adoption, slot release, and the cleanup manifest. A separate successful `run completion` call is unnecessary. Partial or unknown evidence cannot finish; persist it with `run completion` while reconciling. Schema 9 does not write a duplicate normal-finish `result.json`; earlier schemas keep their format until drained. Subsequent cleanup deletes execution metadata from the current tree; retries and recovery read the original records from ordinary main history. Persist any task-required output as task data or artifacts before finish. No recording-only branches/tags, external database, or dedicated cleanup schedule are added.

Combining checkpoint-only delivery through `--resume-from` with `--completion-contract-file` is rejected before startup. Adopting a checkpoint does not establish completion of the entire business operation. A run with a completion contract finalizes its result through `finish --completion-file`, or uses previously recorded evidence. `run completion` remains available to persist unknown/partial outcomes before further work or to reconcile them without releasing ownership.

Do not retrofit contracts onto existing runs. During adoption, stop and reconcile existing writers, then update all writers sharing the clone together. Do not mix in older binaries that do not understand the new contract.

## Consumer-side verification

The caller implements destination/provider success checks, reconciliation of Git originals with delivered bytes, and business-lock release. Do not embed service-specific receipt formats in the runtime. The runtime validates the shared completion contract and accepts only evidence matching the original run.

With an index, records live at `<task directory>/runs/<run>/run.json`. The index selected during initialization resolves task IDs to their storage locations.
