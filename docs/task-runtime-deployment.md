# Deployment acceptance for disposable executors

Source/zipapp conformance and live deployment acceptance are separate. Use the
same portable runtime in personal Cloud, local company workspaces and company
agent Clouds; install transport/clock/stop adapters through each environment's
existing authenticated integration. Do not copy personal credentials or data.

Before admission run `taskctl transport-check`. Its write capabilities are
declarations, not proof of provider CAS or permissions. Validate exact candidate
object transfer, expected-old updates, create-if-absent, conditional deletion and
lost-reply reconciliation in an authorized isolated repository. A branch-only
HEAD-push gateway is unsupported; broader permissions do not restore lost CAS
semantics. Never substitute a blind update for a conditional one.

| Execution profile | Recovery choice | Required acceptance |
| --- | --- | --- |
| Ordinary Git, no trusted clock/stop integration | manual | Report missing prerequisites; retain unknown ownership |
| Verified writer/child stop and settled operations | verified-stop | Bind the provider evidence to the original run and effects |
| CAS plus trusted time and coordinated fenced destinations | generation_timeout | Fresh clone per tick, frozen uncertain renewal, both revocation race orderings |
| Receiver atomically enforces time and writer policy | fenced_lease | Strict receiver rejection of expired writes, including before revocation |

Direct Git does not provide a trusted time contract. An HTTP Date header without
documented identity, freshness and uncertainty bounds is insufficient. The local
test adapter is not a production clock or hosted API integration. Where these
primitives are unavailable, return the unsupported profile; do not enable a
timed policy by asserting capabilities that the environment does not implement.

For effects, distinguish immutable computation output, durable delivery requests,
and accepted publication. Main owns slots/generations and accepted pointers;
inbox/run streams retain requests, progress, intents and candidate results.
`io complete` accepts a result only under current authority. Git push does not itself
guarantee another executor will start: existing invocations must check pending
work and wake the consumer. No additional scheduler or daemon is required.

A producer may finish after verified Git publication and durable delivery
submission. This means producer completion, not delivery success. A publication
consumer owns provider operations and receipts. Mutable live deployments still
need provider fencing or verified stop/settlement; isolating them in a consumer
does not make a delayed old provider request safe automatically.

Checkpoint-only `--resume-from` cannot be combined with a business completion
contract. Preserve that guard. Delivery of committed producer output can instead
use a new inbox message with original source commit/path/hash and provenance;
reconcile uncertain previous delivery before submitting anything new. Never
repeat a completed review or external computation merely to regenerate local
state. Unsaved local bytes cannot be recovered from an empty VM.

Acceptance must destroy the original workspace and every observer cache; test a
normal subsequent invocation, competing starters/reapers, a paused old writer,
lost CAS replies, clock loss/regression, cancelled recovery and a committed result
delivered without repeating computation. Report local proof, live integration
proof and remaining limitations separately. Drain and settle legacy writers
before adopting a policy, and retain manual recovery for unfenced effects.

Process fixtures must separate bounded child readiness from execution deadlines.
Test cold startup on macOS and Linux, retain cancellation/ownership assertions,
and diagnose malformed CLI JSON without printing captured secrets. Foreground Git
maintenance and owned-process cleanup remain required; Linux subreaping is an
executor containment measure, not a portable replacement for generation fencing.
