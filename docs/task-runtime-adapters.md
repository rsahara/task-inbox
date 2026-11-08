# Portable runtime integration contracts

For an unchanged hosted Git CAS endpoint, see [generation timeout recovery](task-runtime-timeouts.md).
It reuses this transport boundary and does not advertise atomic time/CAS support.

The runtime and operation protocol are shared between independent installations.
Version 2.2 adds optional [fenced leases](task-runtime-leases.md), requiring an
additional atomic time/CAS contract and explicit coordinated adoption. Ordinary
transports and manual tasks retain the behavior described below.
Only transport and executor integrations vary. Version 2.1 adds an installed
transport executable and explicit recovery diagnostics; it does **not** add a
hosted-service dependency, a remote lease, a scheduler or a production API adapter.
Runtime 2.11 uses authority schema 9 for unified I/O and transient execution metadata; individual
run records remain version 2. Keep each repository's writers on a compatible
runtime and adapter version. Authority object imports must include complete
ancestry so retired execution records can be read from ordinary Git history.

## Transport selection and preflight

Ordinary Git is the default. An operator can select an installed adapter with
`--transport-adapter /absolute/path/to/program` or the inherited environment
variable `TASKCTL_TRANSPORT_ADAPTER`. An explicit argument takes precedence.
An empty/invalid selection fails; it never falls back to ordinary Git. The
executable must be outside the task-data checkout. Repository files do not select
or install executables. The external transport resolves the repository and
executable paths before checking containment and uses the resolved repository
for its process cwd and request context, including when called directly.
Credentials and remote/API mapping belong to that
environment's existing authenticated setup. Never copy personal configuration or
change an executor's permission boundary to make an adapter work.

```sh
python3 bin/task-inbox --repo /path/to/task-data \
  --transport-adapter /opt/task-integrations/git-api transport-check
```

This check reads the authority ref and its objects and checks the adapter's
declared capabilities for the authority, run namespace and archive namespace.
It creates no remote refs/run and does not change HEAD/index. `supported` means
the declaration and read checks passed. `write_check=declared_not_probed` is
deliberate: a read-only check cannot certify CAS, server ACLs, future connectivity
or a dishonest adapter. Test these in a separately authorized test repository.
Admission/transactions automatically perform the same check before mutation.
All capabilities below are required by the full v2 lifecycle; missing tag/delete
support is not silently treated as successful cleanup.

The direct adapter assumes normal Git CLI semantics and uses the existing
expected-old-SHA push. Its exact-ref and unique fetch-destination checks reject
gateways that ignore those reads. A gateway may still refuse a later write;
admission is not successful unless that authority transaction is confirmed.
Replacing a candidate push with mounted-HEAD push or weakening CAS is unsupported.

## Installed executable protocol

One process receives one JSON request on stdin and returns one JSON response on
stdout. There are no shell arguments. The process cwd is the task-data checkout.
The runtime bounds execution at 60 seconds, terminates its process group on
timeout/cancellation, accepts at most 1 MiB of returned JSON and omits raw stdout,
stderr and adapter error text from diagnostics. Adapters must also bound response
and diagnostic output; Git objects are transferred through the local object
database, **not** base64 in this JSON channel.

Every request contains:

```json
{
  "protocol": "task-runtime-transport/v1",
  "request_id": "sha256 of canonical request before adding this field",
  "operation": "read_ref",
  "context": {
    "repository": "/path/to/task-data",
    "remote": "origin",
    "authority_ref": "refs/heads/main",
    "object_format": "sha1"
  },
  "ref": "refs/heads/main"
}
```

Canonical encoding is the runtime's `store.encode`: sorted keys, two-space JSON
indentation, UTF-8 with unescaped Unicode, followed by newline. The response echoes
`protocol` and `request_id` exactly and includes `status`. The adapter binds every
operation to the configured repository/remote and authenticated identity. A local
path/remote name is a selector, not authorization to contact an arbitrary server.
No callback URL, executable or credential is read from task data.

| Operation | Additional request | Successful response and guarantee |
| --- | --- | --- |
| `capabilities` | `scope`: authority ref, run ref namespace, archive ref namespace | `capabilities`: booleans covering **all** requested namespaces |
| `read_ref` | full `ref` | `oid`: exact object ID, or explicit `null` for confirmed absence; access/network failure is not absence |
| `list_refs` | literal `prefixes` array | `refs`: complete mapping of matching full refs to OIDs; needed for legacy migration checks |
| `fetch_objects` | exact `oid` | `oid`: same value, after importing the exact commit and complete reachable objects into the checkout's object database |
| `compare_exchange` | full `ref`, `expected`, `candidate` | `oid`: candidate (including explicit null for deletion), after exact-object transfer and atomic expected-old comparison |

Required capability keys are `exact_ref_read`, `ref_listing`, `exact_object_fetch`,
`exact_object_push`, `compare_and_swap`, `create_only`, `conditional_delete`,
`append_only`, `branch_refs`, and `tag_refs`. Unknown or non-boolean values do not
establish support. Successful responses must include the required fields.

`expected=null` means create **only if absent**, never an unconditional overwrite.
`candidate=null` means delete **only if the current OID equals expected**. Authority
deletion is forbidden. With two non-null OIDs the candidate must descend from the
expected commit. The core checks ancestry before asking for the update; the
adapter/server must atomically compare the current ref to expected. Read-then-
unconditional-update and read-then-delete do not satisfy this contract. API
`force=false` or a field named `expected_sha` alone is not evidence of equivalence.
Audit the actual server semantics, including deletion/recreation and other writers.

An API must preserve the exact candidate OID, binary bytes, modes, trees, parent
history and commit metadata. If it silently recreates commits with other metadata,
it cannot implement this contract. An adapter must not move HEAD/index or alter
worktree files to approximate an object upload. It may import immutable objects;
the core retains its own pending candidates and verifies receipts and artifacts.

Non-success statuses:

- `conflict`: definitive expected-old rejection; **no ref update was accepted**.
- `unknown`: acceptance cannot be determined, including a lost response.
- `unsupported`: the requested primitive/scope is unavailable.
- Any other status, malformed response, process failure or timeout: failure with
  no success assumption. Raw messages are not forwarded.

After an uncertain update the core keeps the original candidate and operation ID
and reads back its receipt/ancestry. It never converts unknown into a fresh blind
push. An adapter must also reconcile any internal service requests using their
original identities. A successful invocation is not permission to replay an
external side effect.

## Conformance and distribution

`test/fixtures/task_transport_adapter.py` is a **local test API double**, not a
production adapter. It accepts only an absolute local bare origin, transfers exact
object packs and uses bare-repository `update-ref` CAS. It issues no network Git
commands. The ordinary real-Git lifecycle suite also runs through this boundary,
including competing clones, generations, late writes, workspace restoration,
binary artifacts, authority deletion, unknown outcomes and conditional cleanup.
Additional tests cover missing capabilities, candidate-vs-mounted-HEAD selection,
redacted errors and loading an adapter from the zipapp.

```sh
python3 -m unittest discover -s test -p task_runtime_test.py
python3 -m unittest discover -s test -p task_transport_test.py
```

Both source and zipapp exports include the boundary. Source exports also include
this contract and the test double/conformance tests. Provider implementations
belong in separately installed, versioned integration packages. Run the same
acceptance scenarios against the provider's authorized test repository before
rollout. Local tests certify the protocol implementation, not company permissions,
hosted cancellation behavior or live deployment.

## Recovery in short-lived scheduled environments

`run reap` accepts `--observer-profile unknown|retained|stateless` (default unknown).
This describes the **observer**, not the worker, and never changes a run's pinned
recovery policy. It does not authorize recovery by elapsed wall time.

- `retained`: the caller promises to return with the same monotonic-clock identity
  and observation state. Unchanged heartbeat OIDs can establish the existing
  `git_only` threshold. Boot/clock changes reset the observation interval.
- `stateless`: each invocation loses its prior clone/clock. Until a sufficient
  interval is actually observed, return `recovery_policy_required`,
  `recovery_horizon=not_established`, and `needs_attention=true` (CLI exit 2).
- `unknown`: first observation, changed boot or clock regression similarly asks
  the caller to declare a retained observer or install a trusted stop checker.
- `manual` runs require stop evidence even before a stale threshold. Missing
  evidence is actionable, never perpetual silent `observing`.

An observation is not proof of death. `run reap --apply --stop-check /path/checker`
uses the existing strict stop-evidence contract even in a stateless invocation;
confirmed cancellation need not wait for heartbeat expiry. Unknown children,
unsettled Git/publication operations or a terminal label alone retain authority.
The result distinguishes observation, eligibility for automatic Git-only recovery,
and external-effect safety. Successful recovery retains checkpoints and uses a
new run identity for continuation; it does not rerun an old command.

## Executor and business recovery integration

The shared `reconcile_stop` boundary already receives repository/run/generation
identity and requires writer/children stopped plus external/pending-Git settlement
and a durable evidence reference. The company-specific code maps that identity to
its executor's authenticated API and original operation receipts. Store this
mapping and selected checkpoints durably before relying on cross-VM recovery;
machine name, vanished `/tmp`, a PID or a terminal label is insufficient.

Business locks, service delivery receipts and external dispatch state belong to
the caller. Recovering a runtime slot does not settle those effects. Preserve
original operation identities and durable checkpoints, reconcile each destination,
and release only the original owner's locks. Core transport conformance does
not certify the caller's other network integrations.

## Lease prerequisites and rollout boundary

A remote observation timestamp alone cannot establish elapsed time: Git author
dates and arbitrary caller wall clocks are not trusted server receipt times.
No lease is enabled by default. The optional [lease implementation](task-runtime-leases.md) requires:

1. A trusted clock contract with identity, uncertainty bounds and failure/regression
   behavior. Unavailable or inconsistent time keeps ownership unchanged.
2. A remotely durable lease, pinned policy/version per run, and renew/revoke
   decisions serialized in the same authority transaction. Separate heartbeat
   ref checks are not an atomic lease. Lease renewal adds authority commits even
   if detailed heartbeat/progress histories remain separate.
3. Every accepted mutable destination fenced by generation. Run-specific immutable
   external outputs plus an authority-CAS accepted pointer may qualify; a Site
   deploy that independently changes the live shared URL does not automatically
   qualify. Unfenced effects still require stop/settlement evidence.
4. Tests for fresh clone per tick, valid long-run renewal, expiration/renewal races,
   clock loss/regression, stale reads, simultaneous reapers, lost replies and an
   old writer resuming after replacement. Unpublished local bytes are not durable.
5. Explicit coordinated adoption by that repository's writers and separate live
   permission/cancellation verification. No lease is retrofitted onto old runs.

Without a retained observation clock, trusted remote time or verifiable stop
evidence, safe bounded time-based automatic recovery is unsupported. The common
diagnostic makes that limit visible; an adapter cannot invent missing guarantees.
