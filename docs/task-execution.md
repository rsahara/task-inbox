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

## Codex Cloud and local prompts

A Codex scheduled invocation and a local Codex session are caller integrations.
Keep their entry prompt short and version the canonical caller procedure in the
task-data repository. Resolve task directories from immutable task IDs. Fetch
and pin the adopted main commit in a clean control checkout; preserve existing
user work and unfinished caller/run state. Read the caller policy and instructions
at that commit, then use the admitted run's pinned workspace for the task itself.
Do not reset a user's primary checkout to bootstrap a scheduled run.

| Prompt | Contents | Lifecycle owner |
| --- | --- | --- |
| Caller/bootstrap prompt | Repository, allowed task IDs, canonical caller-procedure reference, how to resume the original invocation, and the authorized continuation policy | Caller chooses capabilities, model, tools and placement, and enters the runtime |
| Task/worker prompt | Admitted context, task instructions from its pinned workspace, owned inputs/outputs and required completion evidence | Existing admitted run; no second admission or subscriber startup |

Do not copy business steps, eligibility rules or recovery logic into every
schedule prompt. Keep those in task instructions and the common caller procedure.
The task prompt does not detect Cloud/local/Actions or choose output consumers.
The caller may configure a model or request another capable worker. A request
to run a task is distinct from a verified business result.

For example, a **caller bootstrap prompt** can be:

```text
Act as caller for <task-data repository>, with allowed task IDs <allowlist>.
Preserve unfinished work and the original invocation/operation IDs. Fetch and
pin main in a clean control checkout, then read <canonical caller procedure>
and its policy at that commit. Use the adopted task-inbox entrypoint and the
installed executor profile. Continue only if that policy explicitly permits it,
within its task allowlist and bound. Reconcile unknown outcomes before new work.
```

An executor using `execute` can assemble this **worker prompt** from its context:

```text
Process only the already-admitted task/run/generation in the supplied context.
Read task.md and its linked business instructions in context.workspace at
context.task_path. Use the declared inputs and preserve original operation IDs.
Save and read back outputs before acknowledging captured requests. Return the
verified result and required completion evidence to the executor. Do not admit
another run, finish the caller-owned lifecycle, or start a downstream task.
```

The executor atomically writes the bound result described above; `execute` owns
terminal finish. Adapt an older task driver that starts/finishes its own run
before using this prompt. Do not wrap that driver unchanged and acquire twice.

### When the worker is the current Codex session

`execute --executor` starts an installed executable. It does not attach to the
current conversation, interpret Markdown as a model invocation, or provide a
built-in Codex CLI/Cloud API adapter. If the caller has an installed adapter that
can invoke a Codex worker, use `execute` normally; that adapter supplies prompts,
tools and result verification. The bootstrap session remains the caller.

If work must happen directly in the current session's tools and no such adapter
is installed, `execute --check` supplies readiness only. The caller must use the
existing [individual lifecycle procedure](task-io.md#requests-and-startup) to
admit and prepare one run, supply an independent heartbeat, perform or delegate
the task, verify its evidence, and finish that same run. Follow its existing
completion contract rather than the adapter-owned finish convention above.
This is the compatibility bridge, not an implemented `execute --inline` mode.
Never call `execute`, `run start` or `io wake` from an already-admitted worker.
After finish, the caller can check again and explicitly choose another task;
each one still needs its own run, slot and heartbeat.

Cloud schedules and local invocations use the same task instructions and input
contracts. The caller supplies credentials, capability bindings and an invocation
ID saved before mutation. A resumed invocation reuses that ID; a new scheduled
tick gets a new one. Cloud/private state may disappear, so inspect main and saved
receipts instead of assuming an old process stopped. A local surviving execution
marker can support normal reconciliation. Neither prompt changes its schedule,
deploys its own prompt, or treats schedule delivery as task completion.
Scheduling does not change readiness: use the declared inbox/condition checks,
or explicitly `--force` the first manual task when caller policy permits it.

## GitHub Actions caller

The workflow is the caller. It provisions Git/Python, authentication, author
identity and the trusted runtime/executor outside the task-data checkout. Use
full history for runtime reconciliation and the transport permissions described
in [deployment acceptance](task-runtime-deployment.md). The executor supplies any
Codex prompt/model/tools; task instructions are the same as on other hosts.

This example assumes a self-hosted runner with an adopted, immutable tool and
executor already installed at the illustrative `/opt` paths. It operates on the
workflow repository as its task-data repository and an inbox-driven `report`
task. Adapt the inbox branch if its configured I/O namespace differs. This is a
documentation example, not a workflow installed by task-inbox.

```yaml
name: Process task inbox
on:
  workflow_dispatch:
  repository_dispatch:
    types: [task-input-ready]
  push:
    branches: [main, task-io/report/inbox]
permissions:
  contents: write
concurrency:
  group: task-inbox-report
  cancel-in-progress: false
jobs:
  consume:
    runs-on: [self-hosted, linux]
    steps:
      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1
        with:
          ref: main
          fetch-depth: 0
          path: task-data
      - name: Execute the allowed task
        shell: bash
        env:
          TASK_DATA_REPO: ${{ github.workspace }}/task-data
          TASK_INBOX_TOOL: /opt/task-inbox/task-inbox.pyz
          TASK_EXECUTOR: /opt/task-inbox/worker-executor
        run: |
          set -euo pipefail
          git -C "$TASK_DATA_REPO" config user.name task-worker
          git -C "$TASK_DATA_REPO" config user.email task-worker@example.invalid
          python3 "$TASK_INBOX_TOOL" --repo "$TASK_DATA_REPO" \
            --operation-id "actions-${GITHUB_RUN_ID}-report" execute \
            --task report --runner "actions-${GITHUB_RUN_ID}" \
            --executor "$TASK_EXECUTOR" \
            > "$RUNNER_TEMP/task-inbox-result.json"
```

For authorized chaining, repeat `--task` with the caller's allowed targets and
add `--continue --max-tasks N`; the default above admits at most one task.
Supply condition adapters and business completion contracts when the selected
tasks require them. GitHub concurrency only reduces competing workflow runs;
runtime slots and execution claims still govern local, Cloud and Actions callers.

`GITHUB_RUN_ID` stays the same for a workflow rerun. Excluding `GITHUB_RUN_ATTEMPT`
from the operation ID makes a rerun reconcile the original intent rather than
blindly launch the worker again. Preserve the adopted adapter hashes across that
retry. A new workflow run has a new caller ID, but cannot bypass an occupied task
slot or recover an unknown launch just because its old runner vanished.

The workflow integration must inspect the returned `pending_tasks`/`next` plan,
and check configured consumers outside its execution allowlist when routing
other tasks. Notifications saved by `signal sync` do not start Actions by
themselves. A normal authenticated Git push can trigger the `push` entry;
pushes made with `GITHUB_TOKEN` do not recursively trigger push workflows.
Use an explicit authenticated `repository_dispatch` or `workflow_dispatch` hint
from the caller when a configured consumer is ready; those dispatch events can
start workflows using `GITHUB_TOKEN` with the required repository permissions.
Do not dispatch unconditionally after every finish, which can create a loop.

For the example consumer, a caller that has confirmed ready pending work can
send the hint below. Install `gh` and bind `GH_TOKEN` to the caller's authorized
repository token; keep that binding out of task instructions and signal data.

```sh
gh api --method POST "repos/$GITHUB_REPOSITORY/dispatches" \
  -f event_type=task-input-ready
```

Keep the hint separate from durable message delivery: a failed dispatch leaves
the inbox pending, does not undo the producer's saved output and must not cause
the producer's computation to repeat. A later manual invocation or an existing
scheduled caller rechecks it. The example intentionally leaves that host-specific
follow-up integration to the deployment; it does not guarantee a further startup
on its own. A fresh runner missing execution evidence returns to the existing
recovery procedure. See [retry and recovery](#retry-and-recovery).

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
