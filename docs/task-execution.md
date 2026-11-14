# Task interfaces and caller execution

Runtime 2.13 adds one caller entrypoint, `execute`, for readiness checks, admission,
workspace preparation, worker supervision, verified finish and optional task
continuation. Existing lifecycle primitives remain public for integrations that
own those steps individually. The entrypoint does not choose an LLM provider,
detect a hosting service, dispatch Actions, or change schedules.

## Responsibilities

| Owner | Responsibilities |
| --- | --- |
| Task | Declare handled inbox kinds, read/written signal types, required business capabilities and readiness; process inputs; save owned outputs and signals; verify results before acknowledging captured requests. |
| Caller | Choose an explicit task allowlist, executor capabilities, credentials, model, host, startup triggers, continuation permission and bound. |
| task-inbox | Durable messages and signal synchronization, fixed-snapshot readiness, run admission, ownership, heartbeat, execution claims, finish, recovery and a final pending-work check. |
| Installed adapter | Translate the portable execution context into a script/agent invocation; evaluate application conditions if requested; verify business outcomes and return bound evidence. |

A task may know the schema and identity of an input producer or the recipient
of an explicit inbox request. It must not depend on knowledge of its output's
subscribers, their usage, or their scheduling. A signal states the producer's
current facts; consumers interpret them. Subscriptions live in caller-maintained
configuration. Required access to a domain target is distinct from knowing the
host running the task. Capabilities and domain targets can be declared in task
instructions; caller bootstrap installs the needed tools without host detection
in those instructions. Passing another task's admission command into a business
completion hook violates this boundary.

## Declaration

Pass a JSON file to `task add --interface-file FILE` or
`task edit --id TASK --expected-revision N --interface-file FILE`. The declaration
is persisted as `task.json.interface`; edits use the existing revision check.

```json
{
  "schema_version": 1,
  "inbox": ["feedback", "signal-changed"],
  "signals": {
    "reads": [{"type": "observation"}],
    "writes": ["report-artifact"]
  },
  "readiness": "inbox"
}
```

`inbox` lists handled message kinds. `signals.reads` lists selectors with required
`type` and optional producer `task_id` and signal `id`. `writes` lists owned
output types, without consumers. These declarations describe capabilities, not
authorization: existing `io send`, `signal put`, subscriptions and ownership
checks retain their contracts. A write type is documentation rather than a new
wire-level rejection rule. Read selectors filter `signal-changed` messages for
readiness when present; unmatched messages stay pending. An empty selector list
accepts all declared `signal-changed` messages. The worker still validates input
schemas and must explicitly reconcile unsupported historical messages.

Readiness is one of:

- `inbox`: at least one handled pending message; requires `max_runners=1`.
- `condition`: a trusted installed condition adapter returns ready for saved
  input facts at the supplied fixed commit. Without the adapter it is ineligible.
- `manual`: explicit caller request only. A missing declaration has this default.

Paused, archived, closed or full tasks are ineligible, including with `--force`.
`--force` allows the first allowlisted task once without its usual input rule.
No command in a task definition, signal or inbox is automatically executed.

## Entry and transitions

```sh
task-inbox --repo /path/to/task-data execute --task collect --task report --check
task-inbox --repo /path/to/task-data --operation-id invocation-42 execute \
  --task collect --task report --runner caller-1 --executor /opt/task-executor \
  --continue --max-tasks 4
```

Global options precede `execute`. Repeat `--task` to supply an ordered, unique
allowlist. No undeclared task is discovered for execution. `--check` returns a
read-only plan with the authority commit, eligibility/reason, handled and ignored
pending message IDs, and current signal references (path, revision, status,
SHA-256 and commit). Each plan uses one main snapshot for task definitions,
receipts and signal data; inboxes are append-only separately read streams.
A notice ahead of that snapshot requires a retry. `--check` does not run signal
sync, write shared refs, admit a run or start a worker. A condition adapter is trusted
to be read-only.

Normal execution synchronizes signals, selects the first eligible task, persists
the caller intent with the existing operation-receipt protocol, admits a run,
prepares its pinned workspace and invokes the executor through `run exec`'s
supervisor. The existing single durable execution claim prevents duplicate
launches across clones. Readiness and task availability are checked again inside
the admission transaction, including after a competing authority update. Task
definitions may change between planning and admission; the admitted run pins its
authoritative definition. Workers inspect their actual
current inputs before effects. Readiness is a scheduling hint, never a claim on
inbox messages or an authorization to reuse another run's completion evidence.

By default only one task is admitted. `--continue` permits the next eligible task
after a verified success; the default bound is 16 admissions, with `--max-tasks`
allowing 1–100. Every task has a separate run, workspace, heartbeat, execution
claim and completion. Manual/condition tasks execute at most once per invocation;
inbox tasks may run again for late arrivals, within the same bound. The caller
can use the returned `next` plan and `pending_tasks` to choose a later trigger or
existing schedule instead. The first task does not choose that transport.

The entrypoint rechecks pending inputs after releasing the last slot. Requests
arriving after that read stay durable for the next caller invocation. A scheduler
must independently inspect pending work: a lost startup hint cannot lose or
acknowledge the saved request. Git push and `GITHUB_TOKEN` event suppression are
host integration concerns; use an explicit caller dispatch plus scheduled/manual
reconciliation when push events alone cannot start the consumer.

## Executor adapter

`--executor` is an absolute executable path installed outside the task-data
checkout. It is caller-supplied, never loaded from task metadata or message data.
The process receives one argument: an absolute private JSON context path. The
protocol is `task-inbox-execution/v1`. Context contains `repository`, `remote`,
`branch`, `workspace`, `task_path`, the admitted `run`, the selection `input_plan`,
and `result_path`. The executor supplies the script/agent and tools, reads the
task instructions at the admitted definition, and performs its business work.
It may publish intermediate outputs and acknowledge captured messages **after**
durable save and readback. It leaves terminal `run finish` to this entrypoint.
Adapters using an existing task driver must configure it to return its verified
result before terminal finish. Do not give a worker authority to chain tasks.

Before exiting zero, atomically write `result_path`:

```json
{
  "protocol": "task-inbox-execution/v1",
  "task_id": "report",
  "run_id": "execute-example",
  "generation": 1,
  "outcome": "succeeded",
  "summary": "Outputs saved and verified"
}
```

`outcome` may also be `failed` or `cancelled`; those stop continuation. Optional
`completion` supplies the existing business completion receipt when applicable.
The caller can pass `--completion-contracts-file FILE`, a JSON map from allowed
task IDs to existing business completion contracts. These are pinned at admission
and bound to the invocation intent; the worker must return all required verified
steps before finish can succeed.
The result must bind the exact run generation and be at most 256 KiB. A zero
process exit alone is insufficient. Worker stdout/stderr stay in the existing
private supervisor logs; CLI stdout remains machine-readable JSON. The supervisor
retains heartbeat, cancellation and process-group handling. No external effects
are inferred from text output, a lease timeout or a missing process.

## Condition adapter

`--condition-adapter /opt/task-condition` supplies application-specific readiness
for `condition` tasks through the same entrypoint, including `--check`.
The installed executable reads one JSON request from stdin and returns one JSON
response. It receives protocol, `request_id`, `task_id`, fixed `commit`, the task
definition, handled/ignored message IDs, and selected current signal records with
their logical values and references. It can inspect further data at that commit.
Return the exact `protocol` and `request_id`, a boolean `ready`, and a nonempty
`reason` of at most 256 characters. A mismatched binding, timeout, invalid result
or nonzero exit fails closed before admission. The 60-second bounded check must
have no side effects and must not include credentials in its response.

Keep domain selection in this adapter or the consumer's reusable eligibility
module. The runtime should not know how to rank pull requests, interpret an
assessment or select a publication target. Producers only save their own facts.

## Retry and recovery

Reuse the same global operation ID for an interrupted invocation. Begin and
selection decisions use ordinary immutable authority operation receipts, without
an additional dispatch queue or claim database. The begin receipt binds the task
allowlist, runner, continuation options and adapter file hashes. Changing an
adapter or policy requires reconciling that invocation before using a new one.
Selection gives each step deterministic admission and finish operation IDs.

A previously finished step is read back, its signal delivery and normal cleanup
are reconciled, and it is never relaunched. A surviving local
`exited` execution marker and bound result can finish a save interrupted after
worker exit. A running/ambiguous launch, a nonzero exit or loss of local execution
evidence preserves ownership and returns `needs_attention`; a second clone must
not infer that the old executor stopped. Use existing `run observe`, reports,
confirmed-stop recovery and business receipt reconciliation. A failure saving
or validating results never generates an acknowledgement on the worker's behalf.
If a worker already acknowledged verified intermediate work, those receipts
remain valid independently of terminal finish. Failed adapter checks/results are
reported as command errors; unresolved launch outcomes return exit status 2.

For a new selection, configured fenced-lease/generation-timeout tasks use the
same trusted-clock reaper as normal admission. Manual and Git-only busy ownership
is retained; `--check` never reaps. Replaying an old selection does not revoke a
replacement run. The executor context includes the caller's installed
`transport_adapter` path when present, so child runtime calls use the same transport.
This is caller context, never a command supplied by task data.

The entrypoint does not automatically recover or retry external operations.
An idempotent caller intent is not an exactly-once guarantee for arbitrary
external effects; existing operation IDs and receiver receipts remain required.

## Migration and further consolidation

This is additive to schema-2 task definitions and schema-9/10 repositories; there
is no new authority or signal wire schema and no automatic enablement or schedule
change. Older task editors preserve the optional interface field. Roll out the
2.13 tool/installed adapters to participating callers before selecting the new
entrypoint. Register declarations with revision checks, initially use `--check`,
then opt individual callers into execution and, separately, bounded continuation.
Keep legacy admission/finish paths available for already-running tasks. Do not
invoke the new entrypoint for an already-admitted run.

Existing messages and receipts are retained. Declarations do not erase, re-ID or
acknowledge historical inbox requests. Inventory unmatched kinds and signal
selectors, provide a compatibility handler or reconcile them with the original
owned consumer, and verify an empty inbox before removing a legacy route.
Do not broaden an automatic execution allowlist as part of this migration.

After adopting this entrypoint, audit custom integrations in this order:

| Custom implementation | Reusable part | Keep in the application/host |
| --- | --- | --- |
| Candidate planner | This entrypoint's open/slot/inbox checks and fixed-signal plan | Business ranking in a condition adapter |
| Local launch claim | Shared operation intent plus existing durable execution claim | Verified host startup and stop adapter |
| Hand-written task chains | Ordered allowlist, explicit continuation and bound, per-task ownership | Deployment-specific profiles and deferred-task policy |
| Worker bootstrap | Common prepare/supervise/finish/recheck | Agent provider, credentials and capability provisioning |
| Signal-notice drain loop | Potential future fixed-snapshot/coalescing helper | Business transformation, durable output verification and captured-revision acknowledgement |
| Publication acceptance/restart | Existing completion and I/O receipt primitives | External receiver identity, access policy and publication deduplication |
| Event hints | Durable inbox and startup/final recheck | Actions dispatch, event suppression and schedule reconciliation |

The last three rows are audit candidates, not newly implemented generic services.
Move mechanics only after their failure/recovery tests can run with synthetic
data and no private service dependency. Avoid importing application task IDs,
instructions, payloads, logs or deployment settings into this tool repository.
