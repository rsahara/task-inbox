# Fenced leases for disposable scheduled executors

This strict policy is unchanged by the separate [generation timeout policy](task-runtime-timeouts.md)
for CAS-only hosts. A CAS-only adapter must not advertise this policy's atomic
receiver-time capability.

Version 2.2 adds an **opt-in** `fenced_lease` recovery policy. Normal `run start`
checks remote leases, conditionally revokes expired owners and attempts admission.
It needs no retained observer directory, old VM, waiting invocation or daemon.
`run reap --apply` also supports explicit recovery. Revocation and admission are
separate authority transactions: interruption between them leaves a free slot.
Concurrent starters serialize through slot generations and exact-parent CAS.

Revocation does not prove physical process death. Old workers may still compute or
upload to isolated immutable namespaces, but must not change **accepted** reports
or pointers. Every accepted mutable destination must enforce this property.
Existing manual, retained-observer `git_only`, and verified stop-checker policies
remain available. Existing tasks and live schedules are unchanged.

## Installed environment contract

Use an explicitly installed transport selected by `--transport-adapter` or
`TASKCTL_TRANSPORT_ADAPTER`. Task data selects only an approved profile ID, never
an executable, provider, account or credential. Ordinary Git does not advertise
lease support: expected-SHA comparison has no atomic deadline condition. Reading
trusted time before an ordinary push does not reject a request arriving too late.

The existing `task-runtime-transport/v1` protocol gains optional operations below;
all ordinary transport capabilities are still required. `lease_capabilities`
receives `task_id` and returns `contract`, for example:

```json
{
  "version": 1,
  "atomic_time_cas": true,
  "enforced_writers": true,
  "destinations_fenced": true,
  "profiles": {
    "approved-reports": {
      "version": 1,
      "profile": "approved-reports",
      "clock_id": "installed-clock-v1",
      "ttl_ms": 300000,
      "max_uncertainty_ms": 100,
      "max_sample_age_ms": 10000,
      "max_request_delay_ms": 10000,
      "effects": "authority_only"
    }
  }
}
```

These numbers illustrate a profile; they are not runtime defaults. Operators
provision profiles and permitted tasks outside the repository. `effects` is:

- `authority_only`: accepted mutable output is exclusively an authority transaction.
- `immutable_outputs`: external writes are immutable and isolated by repository,
  task and run; acceptance is an authority-CAS pointer.

The environment must enforce destination restrictions with scoped credentials or
an equivalent output gateway. Capability declarations alone do not certify actual
server permissions or external effects. Mutable Site deployments, direct external
review submissions, unrelated business locks and mutable URLs do not qualify merely
because a worker checked ownership first. Keep manual recovery until their
integrations satisfy the contract. Do not select a profile just to admit a run.

Every guarded operation rechecks the installed profile against the run's pinned
policy. TTL must exceed twice the sum of uncertainty, sample-age and request-delay
bounds. Configuration also requires the strict inequality
`heartbeat_seconds * 1000 + max_uncertainty_ms + max_sample_age_ms +
2 * max_request_delay_ms < ttl_ms`. This leaves room for clock uncertainty,
sample age, the next time request and the active-update deadline margin.

`trusted_time(task_id, expected, policy, nonce)` returns `sample` containing:

- `clock_id`: installed/pinned clock identity;
- integer `lower_ms` and `upper_ms`: bounds in that stable clock domain, with width
  no larger than `max_uncertainty_ms`;
- `nonce` and `expected`: new request nonce and exact authority parent;
- `proof`: an opaque attestation, verifiable at acceptance, bound to task, policy,
  authority ref, parent, nonce and interval. This is not an access credential.

The core bounds the response round trip with this invocation's monotonic clock.
It never needs that clock in a later VM. Accepted transactions persist intervals
in `.task-system/lease-clocks/`; regressing lower or upper bounds fail closed.
The provider also detects clock loss/regression and enforces freshness at acceptance,
including after process suspension. Git author dates, worker wall clocks and cached
Date headers cannot authorize takeover.

## Atomic acceptance and ambiguous outcomes

Authority `compare_exchange` receives an extra `guard`: version, task/run/generation,
action (`start`, `active`, `revoke`, `cleanup`), pinned policy, sample, previous expiry and
original request. Candidate and expected OIDs remain exact. The receiver must do
all of the following as **one acceptance decision**, serialized with every writer:

1. Validate authenticated scope, proof, policy, exact parent, operation identity,
   slot generation and candidate transition. A guard cannot authorize changes to
   another run or destination.
2. Obtain valid current time; reject missing/regressing/inconsistent time and
   proofs exceeding sample-age or request-delay bounds. Never refresh old proofs.
3. For `active`, current upper time must be strictly before old expiry. For
   `revoke`, current lower time must reach expiry. For `start`, candidate expiry
   must still be future at acceptance. The core also leaves a request-delay margin
   before attempting an active update.
4. Accept the exact candidate atomically only while those conditions hold; retain
   its operation receipt and append-only authority history.

New version-2 cleanup manifests need no fresh clock sample. Metadata retirement
uses a conditional authority update; see [terminal cleanup](task-runtime-cleanup.md). Older run
records retain the following explicit preservation path.

The legacy `cleanup` action is a terminal-only exception to the time checks: its sample is
null, and it may update only the original finished run's cleanup receipt and
operation receipts. It must match the original generation and preserved source
identity, leave the terminal run unchanged, and neither own nor modify a slot.
It cannot renew authority, publish output or affect a replacement run. This lets
cleanup finish after expiry or clock loss. Lost runs retain their artifacts for
inspection and do not qualify for automatic cleanup.

A client time check followed by an independently delayed push is insufficient.
Use an authorized server transaction or equivalent enforced deadline primitive;
a local client lock/timeout does not satisfy this contract. Deny bypass writes by
older or uncoordinated clients. Privileged repository administration is outside
the cooperative writer protocol.

New status `guard_rejected` means this candidate was not accepted and this exact
proof can never become acceptable later. The runtime drops that pending candidate,
without silently refreshing its proof in that call. An explicit retry rechecks
current ownership. Ambiguous results must use `unknown` instead. Unknown candidates
and their proofs remain journaled; retry first reconciles the original receipt and
ancestry. An advanced append-only parent fences old in-flight requests. Lost replies
replay historical receipts, not new grants. Use `run check` for current authority;
an old successful receipt is not permission to execute, renew or publish.

Renewal sets expiry to sample lower time plus TTL and cannot revive an expired
lease. Detailed heartbeat/progress remains on its separate ref; its age is not
lease authority. `run heartbeat` renews on main first. `run renew` accepts a stable
operation ID: each actual renewal needs a new ID, reused only for that renewal's
uncertain outcome. Admission, renewal, execution claim, publication, checkpoint,
delivery, finish and revocation share the exact-parent guard.

Recovery is bounded by the next successful normal schedule after definite expiry,
assuming available time/transport and finite contention. Scheduler outages and
unavailable dependencies have no unconditional finite bound. The result reports
blocked recovery instead of inventing stop evidence.

## Coordinated adoption and rollback

Stop admission, drain **all repository runs**, reconcile pending Git/external
operations and upgrade every writer. Run adapter conformance in an authorized
test repository before rollout. Then explicitly select the installed profile:

```sh
python3 bin/task-inbox --transport-adapter /opt/integrations/transport \
  --operation-id enable-report-leases task recovery-policy --id reports \
  --expected-revision 1 --mode fenced_lease --profile approved-reports \
  --confirm-writers-drained
```

The command rejects active runs and increments the task revision. First adoption
upgrades authority config to schema 3 / lease protocol 1, which older v2 clients
reject. Task/run records retain schema 2 base fields; leased runs add pinned policy,
holder, generation, renewal and expiry. Active legacy runs never gain a lease.
`task add` still defaults to manual; enabling leases is a separate explicit step.

Rollback uses the same drain procedure and `task recovery-policy --mode manual`
without a profile. The schema 3 barrier remains to protect lease-era history.
Installing the code or merging its PR does not activate leases or change schedules.
Production provider integration and live rollout remain separate work.

## Durable checkpoints and delivery-only continuation

Only published run artifacts qualify as completed checkpoints. The caller supplies
the original computation ID and input digest; the business integration verifies
those provenance claims. The core checks exact saved entries and binds them to
repository/task/run/generation, definition, policy, source and authority parent.
Checkpoint IDs are immutable. Unsaved local bytes, unpublished work-branch bytes,
missing receipts and unknown external completion are not completed work.

```sh
python3 bin/task-inbox --operation-id checkpoint-review run checkpoint \
  --task reports --run-id old-run --checkpoint-id review \
  --computation-id original-review-request --input-digest <sha256> \
  --artifact tasks/reports/runs/old-run/artifacts/review.json

python3 bin/task-inbox --operation-id next-schedule run start \
  --task reports --runner next-runner --run-id new-run \
  --resume-from old-run --checkpoint-id review

python3 bin/task-inbox --operation-id deliver-review run deliver-checkpoint \
  --task reports --run-id new-run
```

Normal start can revoke the expired original before admitting a new generation.
After a drained directory move, checkpoint verification resolves original artifact
paths under the current task directory and checks the exact saved Git entries.
The checkpoint and its original paths remain unchanged; task revision and policy
must still match.
Continuation requires an original revoked by this policy, unchanged task revision
and policy, and matching artifact/provenance records. `run exec` rejects continuation
runs: it cannot launch the original computation again. `deliver-checkpoint` writes
`<data_path>/accepted-checkpoints/<checkpoint-id>.json`, retaining original provenance
and the accepting run/generation, then finishes in the same authority transaction.
It works without the old workspace or any local workspace. Replays reconcile the
receipt. Original artifacts remain retained; unrelated locks/workspaces are not
cleared. Late unaccepted uploads stay isolated.

This primitive accepts a Git pointer, not a live deployment. External consumers
must use that pointer and the same fencing contract. An independently mutable Site
URL does not become safe. Task9's separate dispatch/PR/Site locks still require
their original identities and settlement protocol. Core-slot recovery is not full
business recovery. Existing stop-checker/manual paths remain necessary for them.

## Local verification and distribution

```sh
python3 -m unittest discover -s test -p task_runtime_test.py
python3 -m unittest discover -s test -p task_transport_test.py
python3 -m unittest discover -s test -p task_lease_test.py
```

The lease fixture combines real Git objects/CAS with a remote deterministic clock
and serialized server acceptance. It is a test server, not a production adapter
or real trusted time source. Tests cover disposable clones, lost workspaces,
renewal/expiry races, delayed writers, clock failure/regression, concurrent reapers,
cancelled recovery, lost replies and checkpoint delivery. Production integrations
still need real permissions and effect-fencing validation. Source and zipapp exports
include the implementation; source exports include these tests.

Automatic test Actions are paused to avoid account usage. Feedback intake PR
creation Actions were removed on 2026-11-05; task #9 collects inbox branches on
its existing schedule without requiring a PR. Tests run locally with
environment/results recorded in the change; GitHub Actions are unnecessary for
task execution or lease recovery.
