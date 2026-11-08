# Interface and execution record decisions (runtime 2.11)

This implements the evaluated scope of issue #9 against main
`af9e6d87ac58b3e62baab5913c99d38eeff7a577`. The objective is fewer normal operations,
caller-managed identities and independent records. No performance or physical
Git-storage reduction is claimed.

## Normal operations and identities

| Work | Before | Now |
| --- | --- | --- |
| Complete one request | `io result`, then `io ack`, two operation IDs | `io complete`, one operation ID and the verified result body |
| Finish a run with terminal business evidence | `run completion`, then `run finish`, two operation IDs | `run finish --completion-file`, one operation ID |
| Persist an unknown/partial external outcome | `run completion` before reconciliation | Retained: early durable evidence is a different operation from terminal finish |
| Name a run | Optional explicit run ID plus start operation ID | Omit `--run-id` to use the stable start operation ID; consume the returned ID |
| Read run result | `run.json` and a duplicated normal `result.json` | `run show`; normal finish has one run record |
| Read retention resources | Persisted JSON and Markdown, or `run report` | One persisted JSON event, `run report --format markdown` for display/export |

Task IDs, message IDs, business-operation IDs and run IDs still have separate
meanings. One run may complete many messages. Business IDs identify external
effects; a new CLI operation ID must never replace an unknown external effect.
Generated CLI operation IDs are printed before mutation, but callers that may
lose stderr should provide their own stable ID. No new wrapper command or
background service is introduced. The former public `io result` / `io ack`
commands and Python entry points are removed; the candidate stage is private.

`io complete` retains candidate persistence and main acceptance internally. Before
writing the candidate, it reserves the public operation ID and full request
fingerprint in an authority receipt under a deterministic internal ID. All
authority transactions honor that reservation, so another run, message or kind
of operation cannot reuse the ID after interruption. A separate deterministic,
domain-separated stream operation ID identifies the candidate. This adds one
preparatory authority transaction, without adding a caller-managed ID or public
operation. Reservation receipts retire with the run and remain binding in
ordinary authority history. Retry the same command and body after interruption or a
lost reply, including from another clone. Only main's current slot/run/generation
can accept a new result. Historical successful receipts are responses, never
execution authority. A completion prepared before revocation cannot win its
old main CAS after a replacement is admitted.

`finish --completion-file` applies the same completion transition rules as early
recording, both before preparing output and inside the final authority CAS.
Unknown operations must retain their original IDs, and successful steps cannot
change. Partial/unknown evidence is rejected by finish; use `run completion`
to persist it before reconciliation. Output publication can still fail without
releasing the slot. Replay of a successful finish returns the original response.

## Record ownership and candidate decisions

| Candidate | Decision and contract |
| --- | --- |
| Request completion | Adopt the unified public operation; retain the immutable candidate and accepted receipt because they cross separate refs and ownership checks. |
| Normal run result | Adopt `run.json` as the result source in schema 9. Status, summary, artifacts, source, finish time and completion evidence are already present there. `run show` works after retirement through history. Keep recovery and checkpoint-delivery `result.json`: these carry diagnostic observations or a distinct adoption record. |
| Operation receipt results | Retain original response snapshots. Replacing these with the latest run would change start/publish/finish retry responses. The preparatory completion receipt binds an operation ID before the cross-ref candidate phase; it does not accept a message or grant authority. Local pending journals and pinned candidates also remain necessary when no remote receipt can yet establish the outcome. |
| Heartbeats | Retain the unified task-I/O writer already present on main (schema 8). Share initial/update state construction and observation validation. Do not restore the legacy plain-state writer or move notifications onto main. |
| Registry metadata | Remove `data_revision` and slot copies of `runner_id` / `machine_id` at the schema-9 boundary. The content hash still prevents shared-data write skew; the run retains ownership attributes, and slots still fence by run ID and generation. Legacy admitted runs keep their old registry fields until drained. |
| Retention Markdown | Stop writing new `.md` duplicates. Keep JSON event records and generate the live inventory's Markdown on demand. This trades direct Git Markdown browsing for one stored format. Existing `.md` files are neither required nor removed. |
| Task output bodies | Do not mandate duplicate bodies. Store a report in `runs/<run>/artifacts/report.md` and put a commit/path/hash reference in `data/latest.json` when only the latest pointer is needed. Both directories remain supported because shared mutable state and immutable run artifacts have different purposes. No existing duplicate-body example was found in this repository. |

The cleanup manifest is separate deletion authorization, not another current
run result. Accepted message receipts remain task-owned records. Recovery never
converts heartbeat silence into physical-stop proof, or `partial` / `unknown`
into success. Required run records, receipt snapshots, I/O evidence and artifacts
remain reachable through ordinary authority history after ref cleanup and GC.

## Compatibility, rollout and interrupted migration

Deploy runtime 2.11 (including copied consumer sources/zipapps, readers and
transport adapters) together. Update consumers of removed CLI/Python operations,
raw normal `result.json`, slot owner fields and `data_revision`. Consumers outside
this repository require their own adoption; no remote consumer is modified by
this source change. Drain old writers and settle their pending result/ack and
finish operations using their original IDs before admitting a new run.

Schemas 2–8 remain readable. Already admitted unified-I/O runs can finish or recover; old heartbeat runs must be reconciled with their original runtime, preserving the schema-8 rollout contract. On the first **new** admission or eligible terminal metadata retirement, the same conditional authority commit
checks every indexed/default task for occupied slots, removes the redundant
registry fields, installs schema 9 with the provenance fence, and performs that operation. Admission creates its run in this same commit.
Any occupied slot blocks this transition. Stopped/lost runs without ownership and
historical records remain untouched. The new writer does not retroactively claim
that old runs have observed workspace provenance. Newly added tasks under schema
9 start with compact registries.

There is no staged migration and no extra migration ID. Retirement uses its existing conditional cleanup operation. Before the admission CAS,
an interruption leaves the old schema; after it, schema 9 and the operation are
visible together. Retry the same admission operation ID from any clone. An
unconfirmed push keeps its original journal/candidate; normal Store reconciliation
decides its outcome. A competing old writer cannot update main against the
pre-migration head once the migration wins. Runtime versions through 2.10 reject
schema 9 on their existing supported-version check. Do not force-downgrade the
schema or rewrite authority history to roll back; adopt a compatible reader/writer
instead. Keep complete main ancestry for historical replay.

Old records are not batch-rewritten: old finish results, recovery results and
Markdown notices remain readable. New normal finishes omit `result.json`; new
retention events omit Markdown. A legacy admitted run finishing before the schema
8 transition still writes the legacy normal result. Cleanup recognizes both
formats and continues to compare terminal runs against original finish receipts.

## Validation boundaries

The integration suite uses independent local Git clones and a bare remote. It
covers conflicting publications, stale generations, candidate interruption,
acceptance response loss, unknown business outcomes, cross-clone replay, drain
barriers, metadata retirement, and fresh-clone restoration after remote GC.
Heartbeat tests cover stream initialization, updates and observation. Provider-side effects and external
consumer rollout require their environment's acceptance checks; Git-only tests
do not prove provider fencing or physical worker termination.
