# Generation timeout recovery on ordinary hosted Git

Version 2.3 adds opt-in `generation_timeout`. It uses the existing authority,
slot generations, receipts and checkpoints. It needs ordinary exact-parent Git
CAS and a separately observed trusted clock, with no server hook, ref gateway,
new storage service, daemon or extra scheduled referee.

This is a different guarantee from [strict fenced leases](task-runtime-leases.md):

| Property | fenced_lease | generation_timeout |
| --- | --- | --- |
| Receiver rejects a late current-owner update before revocation | Required | Not provided |
| Old-generation candidate cannot win after revocation/replacement | Required | Exact-parent CAS |
| Clock/deadline checked atomically at the receiver | Required | Not required |

A delayed update may win before revocation even after its observed deadline.
Expiry makes a run eligible for recovery; the successful revocation CAS removes
authority. A physical process may still exist. All accepted shared state must
come from main (or the configured authority ref), never an attempt/work ref.

## Required installed capabilities

The existing installed transport and authentication boundary is reused.
Before adoption, verify these actual production primitives and permissions:

- Exact expected-old ref CAS, including create-if-absent and conditional deletion.
  A read followed by an unconditional update is not CAS. A force flag alone is
  not evidence that the API implements expected-OID comparison.
- Immutable object transfer/read, append-only authority history and receipts,
  and access to the existing per-run ref namespace, including `renewals/*`.
- A trustworthy clock source already available to the adapter, with documented
  identity, bounded observation age/uncertainty and regression behavior.
- Every writer uses this coordinated protocol. All accepted mutable output is
  an authority transaction, or an authority pointer to run-specific immutable
  output. Existing credentials must enforce the required destination boundaries.

`lease_capabilities` returns the same installed profile format as strict leases,
with these capability fields for a CAS-only provider:

```json
{
  "version": 1,
  "atomic_time_cas": false,
  "enforced_writers": false,
  "generation_timeout": true,
  "coordinated_writers": true,
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

The profile values are examples, not production defaults. `coordinated_writers`
declares an installed, drained, cooperative writer deployment; it does not claim
that ordinary Git validates application semantics or rejects a privileged raw
force push. Older/uncoordinated writers must not retain admission rights.
The schema barrier below prevents older runtime versions from silently writing.

`trusted_time(task_id, expected, policy, nonce)` returns `clock_id`, `expected`,
`nonce`, integer `lower_ms`/`upper_ms`, and integer `sample_age_ms`.
The adapter associates this fresh clock observation with the requested parent
and nonce. It need not obtain a new server-signed Git/time proof.
`sample_age_ms` bounds the observation's age when the adapter returns it, including
any caching, and must not exceed the pinned maximum. The core bounds the round
trip, interval width and regression against accepted remote clock observations.
Active requests reserve observation-age and request-delay margin before the old
deadline. A cached HTTP Date without these guarantees is not a trusted clock.

Missing/invalid time blocks new grants, new renewals and new revocations. Exact
previously prepared requests and historical receipts do not acquire new time
evidence on replay. As with an already in-flight request, a frozen request may
still win its parent CAS when the clock later becomes unavailable; that is part
of this weaker policy, not receiver-side deadline enforcement.

The runtime sends ordinary `compare_exchange(ref, expected, candidate)`, without
a lease guard, for this policy. Strict leases still require `atomic_time_cas` and
`enforced_writers` and still send the receiver guard. Unsupported capabilities
fail at policy configuration/admission. Direct Git has no trusted-clock contract
and therefore does not automatically enable either timed policy.

If exact CAS or trustworthy time is unavailable, keep manual/verified-stop
recovery. Supplying strict lease semantics instead requires an actual production
receiver with atomic time/ownership validation; the core cannot synthesize it
from a time read plus push.

## Fixed renewal requests and recovery horizon

A new renewal checks the run generation, policy, deadline and business state
against one exact authority snapshot. It builds one candidate containing the
fixed time observation, resulting deadline and normal operation receipt. Before
attempting authority CAS it preserves that candidate through create-if-absent:

`refs/heads/<ref_prefix>/<task_id>/<run_id>/renewals/<operation_id>`

The ref is an immutable request record, not another ownership authority.
Concurrent attempts with the same operation ID use the create winner. A fresh
clone can fetch the original parent and candidate from this ref. Unknown retries
reuse that exact candidate and ID; they never obtain a newer deadline. If main
has advanced without accepting it, that renewal is permanently superseded.
It is not rebased. A genuinely new renewal needs a new ID and a complete check
of the current generation/time. Automatic supervisor heartbeats may retry such
definitively superseded requests with new IDs, with a bounded retry count;
unknown outcomes and explicitly supplied IDs are not silently replaced.

Accepted operation receipts answer later replays. Accepted attempt refs are
deleted conditionally on a best-effort basis. A failed deletion retains only an
inert ref; terminal cleanup also removes this original run's attempts. Rejected
attempts remain pinned so abandoned clones cannot turn their old IDs into fresh
renewals. Lost runs retain their records for inspection.

Let T be the pinned TTL and s be the last new renewal observation's lower clock
bound before an owner stops producing new requests. Each such candidate has
deadline at most s + T, regardless of its eventual arrival time. Replaying it
does not add another T. For candidates prepared before an existing deadline D,
the extension is less than one additional T beyond D.

After all these fixed deadlines are definitely past, any delayed renewal that
wins before a reaper still carries an already-past deadline. A reaper rereads the
new parent and can try again. Once revocation wins, every candidate based on the
old parent fails CAS. A new generation can continue the committed checkpoint
without invoking the old computation.

Recovery therefore requires the next successful normal tick after the maximum
fixed deadline, including clock uncertainty, successful transport and contention
resolution. Finite contention gives eventual progress, not a universal N-minute
wall-clock bound. A numeric bound additionally needs bounded scheduling gaps,
API completion times and contention. A worker that starts producing valid new
renewals again before revocation is not an abandoned owner under this contract.

For example, a renewal sampled at 12:04 with a five-minute TTL can still be
accepted at 12:06 before revocation, with deadline 12:09. Retrying it at 12:07
does not move the deadline to 12:12. If revocation wins first, that exact renewal
cannot alter the replacement.

Publication uses the same exact-parent transaction and rereads generation,
accepted pointer and business preconditions on a conflict. It never blindly
rebases previously approved output. Receiver ordering, not a local precheck
followed by an independent external write, determines accepted state.

## Adoption, effects and checkpoints

Drain all repository writers and settle old external operations before changing
policy. Map existing task-specific locks to the original run/generation and their
settlement protocol. A core slot does not settle a dispatch/PR/Site/business lock;
where that mapping or destination fencing is absent, retain manual recovery.

```sh
python3 bin/task-inbox --transport-adapter /opt/integrations/transport \
  --operation-id enable-timeouts task recovery-policy --id reports \
  --expected-revision 1 --mode generation_timeout --profile approved-reports \
  --confirm-writers-drained
```

First adoption sets authority schema 4 and `generation_timeout_protocol: 1`.
v2.2 and older writers reject schema 4. Existing task/run fields, authority ref,
generation and operation receipt format are retained. Active legacy runs cannot
be retrofitted. Rollback to manual uses the same drain procedure and retains the
schema barrier.

The existing checkpoint and delivery-only continuation commands are shared.
Newly admitted runs use [terminal cleanup](task-runtime-cleanup.md) during normal
start/finish, including a conditional commit to retire execution metadata. Legacy cleanup records keep
their original explicit preservation procedure. A continuation must keep the original computation/input/artifact
provenance and match the task definition and policy. Unsaved local bytes remain
unavailable. External mutable Site deployments and unfenced external effects
remain manual. Installing this code does not change any live task or schedule.

## Local evidence and distribution

`test/fixtures/task_timeout_adapter.py` deliberately performs no time/proof
evaluation at CAS acceptance. It uses real Git object transfer and ref CAS with
an independently read deterministic test clock; it declares strict capabilities
false. This fixture is not a production adapter or clock source.

```sh
python3 -m unittest discover -s test -p task_timeout_test.py
python3 -m unittest discover -s test -p task_lease_test.py
python3 -m unittest discover -s test -p task_runtime_test.py
python3 -m unittest discover -s test -p task_transport_test.py
```

Coverage includes both race orderings, frozen retries after clone loss, stopped
recovery, concurrent reapers/starters, clock failures, receipts and checkpoint
delivery. Source and zipapp share the same implementation; source also includes
both policy suites and fixtures. Test Actions are not required or enabled.

Local conformance must be followed by validation of the real hosted API's CAS,
clock guarantees, ref permissions and output restrictions before adoption.
The provider-specific adapter and live rollout remain separate integration work.
