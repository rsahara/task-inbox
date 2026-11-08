# Retained resource reports

Runtime 2.9 records resources retained after recovery or deferred cleanup, and
provides a read-only inventory. Reports do not delete resources, stop workers,
grant execution authority, or block the next run after its slot is released.

## Automatic recovery notices on main

The existing recovery transaction writes one event record alongside `run.json`,
`result.json` and `cleanup.json`:

```text
<task-path>/runs/<run-id>/retention-report.json
```

Notices are written for stale Git-only recovery, confirmed-stop recovery, lease
or generation-timeout revocation, finish with `--discard-output`, and checkpoint
delivery that retains recovery data. Custom task locations are respected.

Each notice records repository/authority, task/run/generation, owning machine,
operation ID, reason, diagnostic recording time, exact work and heartbeat refs,
and the last accepted source commit. A stale Git-only recovery also records the
observed heartbeat commit. Generation-timeout notices include the renewal ref
prefix. Unsampled heartbeat heads are explicitly unknown, not absent. Local
workspace paths are not published to main; they are available from the owning
clone's inventory.

This file is part of the same authority commit as the state change and slot
release. They create no extra authority transaction, branch or tag. A lost reply
is reconciled with the original operation ID; retries preserve the accepted
notice. Existing notices remain readable from a fresh clone after a replacement
starts. Older runs without notices are still included by the inventory.

Runtime 2.11 generates Markdown through `run report --format markdown` instead of persisting a duplicate event rendering. Existing Markdown notices are left intact; the inventory reads JSON and does not depend on them. Git-only readers can inspect the JSON directly.

The notice is an event record, not a promise that every named ref still exists.
Do not infer current resource existence or deletion permission from its age.
Successful operation receipts, accepted artifacts, and run metadata retain their
existing preservation contracts.

## Deferred cleanup notices in the current clone

When normal start/finish maintenance cannot collect an identified run, it makes a
best-effort local notice at:

```text
<git-common-dir>/task-runtime-v2/<remote-and-authority-key>/reports/<task-id>/<run-id>.json
```

Notices include changed refs, unconfirmed deletions, dirty workspaces,
unconfirmed worker termination, busy local locks and executor-owned workspaces.
They record known refs, the refs sampled for that maintenance pass, the reason,
and run identity. Ref samples can precede attempted deletions; use the inventory
for a fresh observation. Raw Git output, child logs, business payloads and raw
exception messages are not copied into notices. Unknown failures get a generic
reason. Collection of a recorded workspace saves a local confirmation, marking
an existing notice resolved or creating one, and keeps it for inspection.
Collecting only refs or retiring metadata does not confirm workspace collection.
Explicit `run cleanup --apply` saves the same confirmation. It is saved before
metadata retirement, so a later retirement failure or retry does not lose the
confirmed workspace collection.

Local notices are not pushed and cannot describe another machine's filesystem.
A filesystem/report-write failure does not change the confirmed business result
or prevent a later admission. If the whole maintenance pass fails before a run
can be identified, no per-run local notice can be guaranteed. Remote recovery
notices remain on main independently of these local files.

## Inspect and export

```sh
# JSON across all tasks, including refs with no matching run record.
python3 bin/task-inbox --repo /path/to/task-data run report

# Human-readable report; shell redirection saves it wherever the caller chooses.
python3 bin/task-inbox --repo /path/to/task-data run report \
  --format markdown > retained-resources.md

# Restrict to a task or a specific run.
python3 bin/task-inbox --repo /path/to/task-data run report \
  --task maintenance --run-id old-run --format markdown
```

The inventory anchors run metadata to a reported authority commit, then lists
current remote refs/SHAs, current-clone workspaces, local branches and tracking
refs, sidecar paths, and the last local cleanup notice. Ref and filesystem reads
are separate observations and can race with ongoing cleanup. Running runs and
ordinary terminal runs with confirmed workspace collection in this clone are
omitted. Permanent inbox branches are
not leftovers. Unattributed run refs appear separately in repository/task scope.
A run filter requires a task filter.

The report reads metadata without renewing leases, acquiring slots, releasing
ownership or deleting anything. It creates no authority commit. Normal Git fetch
may populate the local object/tracking cache. A fresh clone reports local
workspace visibility as `not_recorded_in_this_clone`, never as proof that the
owning machine's workspace is gone.
This remains unobserved after another clone collects the remote refs, even if
the owning clone subsequently removes its workspace. Clone-local confirmations
are not shared; older runs without that evidence remain unobserved as well.

A checkpoint exposed before terminal publication can be restored in another
clone. Publication records `workspace_history_unobserved` in the same authority
transaction, including when a work ref already existed from an unfinished
publication. Older/missing evidence is unknown. After checkpoint exposure, even
confirmed collection in this clone cannot establish that prior workspaces in
other clones are gone. The inventory keeps the run and marks
`local.unobserved_prior_workspaces`, including after metadata retirement. This
also applies conservatively when no restore was actually observed; the runtime
does not maintain a global registry of clone-local workspaces.

Before creating a workspace, preparation records one opaque origin identity on
the run. A second preparation identity makes workspace history unobserved even
without an earlier checkpoint; legacy runs without preparation evidence remain
unknown. The authority CAS precedes local workspace creation, and a durable local
intent makes retries reuse the same identity. Repeated preparation of the same
recorded workspace adds no authority transaction. Normal single-workspace finish
and confirmed collection can still be omitted. This stores no workspace paths,
does not enumerate other clones, and grants no cleanup or execution permission.

The single-workspace conclusion requires an authority writer fence. Runtime 2.10
uses schema 8 with `workspace_provenance_protocol=1`, rejecting older writers
before they can create a workspace or use the old heartbeat protocol. Runtime 2.11 extends this fence to schema 9 for compact records. The first
admission upgrades earlier authority schemas only after all admitted runs finish
or are reconciled. Historical runs without the protocol stamp remain unobserved
even after local collection, including old `workspace_history_unobserved=false`
values without the writer guarantee.

Exit 0 means the report was generated, even if retained resources exist. Exit 2
means inventory is incomplete, for example because ref listing failed or local
ownership records are invalid. Unknown inventory must not be treated as an empty
resource list. JSON and Markdown both include incomplete-inventory diagnostics.

## Validation

```sh
python3 -W error::ResourceWarning -m unittest discover -s test -p task_reports_test.py -v
python3 -W error::ResourceWarning -m unittest discover -s test -p '*test.py'
```

Tests use local bare Git repositories, independent clones and the existing test
lease/timeout adapters. They cover atomic recovery records, operation replay and
lost replies, cleanup failures and later resolution, changed refs, dirty local
workspaces, fresh-clone and indexed-path inventories, incomplete reads, CLI
formats, and reporting in source/zipapp distributions. The killed-worker scenario
also verifies that stale recovery leaves a durable heartbeat observation notice.
