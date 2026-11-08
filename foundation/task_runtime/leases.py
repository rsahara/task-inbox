"""Shared renewable recovery; strict leases additionally require receiver time/CAS enforcement."""
from time import monotonic
import uuid

from .store import CONTROL, TaskError, digest, encode, identity


MODE = "fenced_lease"
GENERATION = "generation_timeout"
MODES = (MODE, GENERATION)
POLICY_KEYS = {"version", "profile", "clock_id", "ttl_ms", "max_uncertainty_ms",
               "max_sample_age_ms", "max_request_delay_ms", "effects"}


def validate_policy(policy):
    if (not isinstance(policy, dict) or set(policy) != POLICY_KEYS or
            type(policy["version"]) is not int or policy["version"] != 1):
        raise TaskError("Unsupported lease policy")
    for name in ("profile", "clock_id"):
        identity(policy[name], "lease " + name)
    for name in ("ttl_ms", "max_uncertainty_ms", "max_sample_age_ms", "max_request_delay_ms"):
        if type(policy[name]) is not int or not 0 < policy[name] <= 86400000:
            raise TaskError("Invalid lease time bound")
    if policy["ttl_ms"] <= 2 * (policy["max_uncertainty_ms"] + policy["max_sample_age_ms"] + policy["max_request_delay_ms"]):
        raise TaskError("Lease duration must exceed clock and request uncertainty")
    if policy["effects"] not in ("authority_only", "immutable_outputs"):
        raise TaskError("Mutable unfenced effects cannot use leases")
    return policy


def policy_for(store, task_id, profile, pinned=None, *, mode=MODE):
    method = getattr(store.transport, "lease_capabilities", None)
    if not method:
        raise TaskError("Transport does not provide trusted-time recovery capabilities")
    contract = method(task_id)
    required = (("atomic_time_cas", "enforced_writers") if mode == MODE else
                ("generation_timeout", "coordinated_writers"))
    if (not isinstance(contract, dict) or contract.get("version") != 1 or
            any(contract.get(key) is not True for key in required) or
            contract.get("destinations_fenced") is not True):
        raise TaskError("Unsupported recovery policy " + mode + "; required: " + ", ".join((*required, "destinations_fenced")))
    profiles = contract.get("profiles")
    if not isinstance(profiles, dict):
        raise TaskError("Invalid installed lease profiles")
    policy = validate_policy(profiles.get(profile))
    if policy["profile"] != profile or (pinned is not None and policy != pinned):
        raise TaskError("Installed lease policy differs from the pinned policy")
    return policy


def clock_path(policy):
    return CONTROL + "/lease-clocks/" + digest(policy["clock_id"]) + ".json"


def sample(store, snapshot, task_id, policy, mode=MODE):
    policy_for(store, task_id, policy["profile"], policy, mode=mode)
    nonce = uuid.uuid4().hex
    started = monotonic()
    value = store.transport.trusted_time(task_id, snapshot.commit, policy, nonce)
    elapsed = (monotonic() - started) * 1000
    if (not isinstance(value, dict) or value.get("clock_id") != policy["clock_id"] or
            value.get("nonce") != nonce or value.get("expected") != snapshot.commit or
            (mode == MODE and (not isinstance(value.get("proof"), str) or not value["proof"]))):
        raise TaskError("Invalid trusted time identity")
    lower, upper = value.get("lower_ms"), value.get("upper_ms")
    if (type(lower) is not int or type(upper) is not int or lower < 0 or upper < lower or
            upper - lower > policy["max_uncertainty_ms"] or elapsed < 0 or
            elapsed > policy["max_request_delay_ms"]):
        raise TaskError("Trusted time is stale or outside its uncertainty bounds")
    if mode == GENERATION:
        age = value.get("sample_age_ms")
        if type(age) is not int or not 0 <= age <= policy["max_sample_age_ms"]:
            raise TaskError("Trusted time observation age is missing or stale")
    previous = snapshot.json(clock_path(policy), {})
    if previous and (lower < previous["lower_ms"] or upper < previous["upper_ms"]):
        raise TaskError("Trusted clock regressed; ownership retained")
    return value


def configure(store, operation_id, task_id, expected_revision, *, mode, profile=None, drained=False):
    from . import runtime
    if mode not in ("manual", "git_only", *MODES) or not drained:
        raise TaskError("Policy changes require coordinated writers drained confirmation")
    if (mode in MODES) != bool(profile):
        raise TaskError("A fenced lease requires exactly one installed policy profile")
    policy = policy_for(store, task_id, profile, mode=mode) if profile else None
    request = dict(kind="task-recovery-policy", task_id=task_id, expected_revision=expected_revision,
                   mode=mode, profile=profile, policy=policy, drained=True)
    def apply(snapshot):
        task, _ = runtime.load_task(snapshot, task_id)
        if task["revision"] != expected_revision:
            raise TaskError("Task definition changed")
        # This also protects the repository-wide v2 -> v3 writer barrier.
        from . import layout
        for other_path in layout.tasks(snapshot).values():
            if any(slot.get('run_id') for slot in snapshot.json(other_path + '/runtime.json')['slots'].values()):
                raise TaskError('Drain all repository runs before changing recovery policy')
        task["execution"]["recovery_mode"] = mode
        task["execution"].pop("lease_policy", None)
        if policy:
            margin = (policy["max_uncertainty_ms"] + policy["max_sample_age_ms"] +
                      2 * policy["max_request_delay_ms"])
            if task["execution"]["heartbeat_seconds"] * 1000 + margin >= policy["ttl_ms"]:
                raise TaskError("Heartbeat interval must leave the lease renewal safety margin")
            task["execution"]["lease_policy"] = policy
        task["revision"] += 1
        changes = {runtime.root(task_id, snapshot) + "/task.json": encode(task)}
        if policy:
            config = snapshot.json(CONTROL + "/config.json")
            config.update(schema_version=max(config["schema_version"], 3), lease_protocol=1)
            if mode == GENERATION:
                config.update(schema_version=max(config["schema_version"], 4), generation_timeout_protocol=1)
            changes[CONTROL + "/config.json"] = encode(config)
        return changes, task
    return store.transact(operation_id, request, apply)


def guard(store, snapshot, request):
    """Every run mutation is checked against the exact transaction parent."""
    from . import runtime
    kind = request.get("kind", "")
    if not kind.startswith("run-") or not request.get("task_id") or not request.get("run_id"):
        return None
    task_id, run_id = request["task_id"], request["run_id"]
    task, _ = runtime.load_task(snapshot, task_id)
    run = None if kind == "run-start" else runtime.load_run(snapshot, task_id, run_id)[2]
    execution = (run or task)["execution"]
    mode = execution["recovery_mode"]
    if mode not in MODES:
        return None
    policy = validate_policy(execution.get("lease_policy"))
    if snapshot.json(CONTROL + "/config.json").get("lease_protocol") != 1:
        raise TaskError("Lease writer protocol is not enabled")
    if mode == GENERATION and snapshot.json(CONTROL + "/config.json").get("generation_timeout_protocol") != 1:
        raise TaskError("Generation timeout writer protocol is not enabled")
    client_only = {"client_time_only": True} if mode == GENERATION else {}
    if kind in ("run-cleanup", "run-cleanup-complete"):
        policy_for(store, task_id, policy["profile"], policy, mode=mode)
        _, registry = runtime.load_task(snapshot, task_id)
        slot = registry["slots"].get(str(run["slot_id"]), {})
        if (run["status"] not in ("succeeded", "failed", "cancelled") or
                request.get("generation") != run["generation"] or slot.get("run_id") == run_id):
            raise TaskError("Cleanup requires the original terminal run generation")
        return {"version": 1, "task_id": task_id, "run_id": run_id,
                "generation": run["generation"], "action": "cleanup", "policy": policy,
                "sample": None, "expires_ms": run["lease"]["expires_ms"], "request": request, **client_only}
    stamp = sample(store, snapshot, task_id, policy, mode)
    action = "start" if run is None else "revoke" if kind == "run-lease-revoke" else "active"
    expiry = None
    if run:
        runtime.load_run(snapshot, task_id, run_id, live=True)
        lease = run.get("lease", {})
        expiry = lease.get("expires_ms")
        if (type(expiry) is not int or lease.get("policy_hash") != digest(policy) or
                lease.get("holder") != run_id or lease.get("generation") != run["generation"]):
            raise TaskError("Invalid durable lease identity")
        if action == "revoke":
            if stamp["lower_ms"] < expiry:
                raise TaskError("Lease has not definitely expired")
        elif stamp["upper_ms"] + stamp.get("sample_age_ms", 0) + policy["max_request_delay_ms"] >= expiry:
            raise TaskError("Lease expired or too near expiry; it cannot be revived")
    return {"version": 1, "task_id": task_id, "run_id": run_id,
            "generation": run["generation"] if run else None, "action": action,
            "policy": policy, "sample": stamp, "expires_ms": expiry, "request": request, **client_only}


def stamp_run(snapshot, run):
    value = getattr(snapshot, "lease_guard", None)
    if value:
        run["lease"] = {"holder": run["run_id"], "generation": run["generation"],
                        "policy_hash": digest(value["policy"]), "clock_id": value["policy"]["clock_id"],
                        "renewed_ms": value["sample"]["lower_ms"],
                        "expires_ms": value["sample"]["lower_ms"] + value["policy"]["ttl_ms"]}


def renewal_prefix(store, task_id, run_id):
    return ("refs/heads/" + store.ref_prefix + "/" + identity(task_id) + "/" +
            identity(run_id) + "/renewals/")


def renew(store, operation_id, task_id, run_id):
    from . import runtime
    request = dict(kind="run-lease-renew", task_id=task_id, run_id=run_id)
    def apply(snapshot):
        run = runtime.load_run(snapshot, task_id, run_id, live=True)[2]
        if run["execution"]["recovery_mode"] not in MODES:
            raise TaskError("Run does not use renewable recovery")
        stamp_run(snapshot, run)
        return {runtime.run_path(task_id, run_id, snapshot) + "/run.json": encode(run)}, run
    run = runtime.load_run(store.snapshot(), task_id, run_id)[2]
    attempt_ref = (renewal_prefix(store, task_id, run_id) + identity(operation_id)
                   if run["execution"]["recovery_mode"] == GENERATION else None)
    result = store.transact(operation_id, request, apply, attempt_ref=attempt_ref)
    if attempt_ref:
        # The durable authority receipt now answers every future replay.
        # A failed conditional deletion only retains an inert candidate ref.
        from .store import GitFailure
        try:
            candidate = store.remote_tip(attempt_ref)
            if candidate:
                store.push(attempt_ref, None, candidate)
        except (TaskError, GitFailure):
            pass
    return result


def inspect(store, task_id, run_id):
    from . import runtime
    snapshot = store.snapshot()
    run = runtime.load_run(snapshot, task_id, run_id, live=True)[2]
    policy = validate_policy(run["execution"].get("lease_policy"))
    mode = run["execution"]["recovery_mode"]
    stamp = sample(store, snapshot, task_id, policy, mode)
    expiry = run["lease"]["expires_ms"]
    return {"run_id": run_id, "generation": run["generation"], "expires_ms": expiry,
            "expired": stamp["lower_ms"] >= expiry, "clock_id": policy["clock_id"],
            "recovery_horizon": ("next_successful_schedule_after_expiry" if mode == MODE else
                                 "next_successful_schedule_after_fixed_renewal_deadlines"),
            "recovery_mode": mode,
            "external_effect_safety": policy["effects"]}


def revoke(store, operation_id, task_id, run_id):
    from . import runtime
    request = dict(kind="run-lease-revoke", task_id=task_id, run_id=run_id)
    def apply(snapshot):
        _, registry, run = runtime.load_run(snapshot, task_id, run_id, live=True)
        if not getattr(snapshot, "lease_guard", None):
            raise TaskError("Automatic lease revocation requires a fenced lease")
        run.update(status="lost", recovery_kind="expired_" + run["execution"]["recovery_mode"],
                   revoked_ms=snapshot.lease_guard["sample"]["lower_ms"])
        runtime.release_slot(registry, run)
        path = runtime.run_path(task_id, run_id, snapshot)
        changes = {runtime.root(task_id, snapshot) + "/runtime.json": encode(registry), path + "/run.json": encode(run),
                path + "/result.json": encode({"status": "lost", "run_id": run_id, "generation": run["generation"],
                    "recovery_kind": run["recovery_kind"], "physical_stop_confirmed": False}),
                path + "/cleanup.json": encode({"run_id": run_id, "status": "retained",
                    "reason": "Expired authority is fenced; retain checkpoints and isolate late output"})}
        from . import reports
        changes.update(reports.retained_changes(run, path, operation_id, 'expired_authority_fenced'))
        return changes, run
    return store.transact(operation_id, request, apply)


def checkpoint(store, operation_id, task_id, run_id, checkpoint_id, computation_id, input_digest, artifacts):
    from . import runtime
    identity(checkpoint_id, "checkpoint ID")
    identity(computation_id, "computation ID")
    if (not isinstance(input_digest, str) or len(input_digest) != 64 or
            any(c not in "0123456789abcdef" for c in input_digest) or not artifacts):
        raise TaskError("A checkpoint requires input provenance and durable artifacts")
    request = dict(kind="run-checkpoint", task_id=task_id, run_id=run_id, checkpoint_id=checkpoint_id,
                   computation_id=computation_id, input_digest=input_digest, artifacts=sorted(set(artifacts)))
    def apply(snapshot):
        run = runtime.load_run(snapshot, task_id, run_id, live=True)[2]
        if run["execution"]["recovery_mode"] not in MODES or run.get("continuation"):
            raise TaskError("Checkpoint requires an original fenced computation run")
        path = runtime.run_path(task_id, run_id, snapshot) + "/checkpoints/" + checkpoint_id + ".json"
        if snapshot.read(path) is not None:
            raise TaskError("Checkpoint IDs are immutable; replay the original operation")
        entries = {}
        for name in request["artifacts"]:
            entry = run["artifact_manifest"].get(name)
            if (not name.startswith(runtime.run_path(task_id, run_id, snapshot) + "/artifacts/") or not entry or
                    snapshot.entries.get(name) != tuple(entry)):
                raise TaskError("Checkpoint artifact is missing, changed, or not published by this run")
            entries[name] = entry
        record = {"version": 1, "checkpoint_id": checkpoint_id, "status": "completed",
                  "task_id": task_id, "run_id": run_id, "generation": run["generation"],
                  "repository_id": run["repository_id"], "definition_commit": run["definition_commit"],
                  "definition_revision": run["definition_revision"], "policy_hash": digest(run["execution"]["lease_policy"]),
                  "computation_id": computation_id, "input_digest": input_digest, "artifacts": entries,
                  "source_head": run["source_head"], "accepted_parent": snapshot.commit}
        return {path: encode(record)}, record
    return store.transact(operation_id, request, apply)


def continuation(snapshot, task_id, source_run, checkpoint_id):
    from . import runtime
    identity(checkpoint_id, "checkpoint ID")
    task, _, source = runtime.load_run(snapshot, task_id, source_run)
    path = runtime.run_path(task_id, source_run, snapshot) + "/checkpoints/" + checkpoint_id + ".json"
    record = snapshot.json(path)
    if (source["status"] != "lost" or source["execution"]["recovery_mode"] not in MODES or
            source["execution"]["recovery_mode"] != task["execution"]["recovery_mode"] or
            source.get("recovery_kind") != "expired_" + source["execution"]["recovery_mode"] or not record or
            record.get("version") != 1 or record.get("checkpoint_id") != checkpoint_id or
            record.get("status") != "completed" or record.get("run_id") != source_run or
            record.get("task_id") != task_id or record.get("generation") != source["generation"] or
            record.get("repository_id") != source["repository_id"] or
            record.get("definition_commit") != source["definition_commit"] or
            record.get("definition_revision") != task["revision"] or
            record.get("policy_hash") != digest(task["execution"].get("lease_policy"))):
        raise TaskError("Checkpoint provenance is missing or incompatible")
    identity(record.get("computation_id"), "checkpoint computation ID")
    input_digest = record.get("input_digest")
    if (not isinstance(input_digest, str) or len(input_digest) != 64 or
            any(c not in "0123456789abcdef" for c in input_digest) or
            not record.get("source_head") or not record.get("accepted_parent")):
        raise TaskError("Checkpoint input provenance is incomplete")
    original = runtime.run_path(task_id, source_run, source) + '/artifacts/'
    current = runtime.run_path(task_id, source_run, snapshot) + '/artifacts/'
    if not record.get("artifacts") or any(
            not p.startswith(original) or
            snapshot.entries.get(current + p[len(original):]) != tuple(e) for p, e in record["artifacts"].items()):
        raise TaskError("Checkpoint artifacts changed or disappeared")
    return {"source_run": source_run, "checkpoint_id": checkpoint_id, "checkpoint_path": path,
            "checkpoint_hash": digest(record), "provenance": record, "delivery_only": True}


def deliver(store, operation_id, task_id, run_id):
    """Accept an immutable checkpoint pointer and finish, without rerunning computation."""
    from . import runtime
    request = dict(kind="run-deliver-checkpoint", task_id=task_id, run_id=run_id)
    def apply(snapshot):
        _, registry, run = runtime.load_run(snapshot, task_id, run_id, live=True)
        saved = run.get("continuation")
        if not saved or continuation(snapshot, task_id, saved["source_run"], saved["checkpoint_id"]) != saved:
            raise TaskError("No matching durable continuation")
        record = dict(saved, accepted_run=run_id, accepted_generation=run["generation"])
        pointer = runtime.data_prefix(run, task_id) + "accepted-checkpoints/" + saved["checkpoint_id"] + ".json"
        run.update(status="succeeded", delivery=record)
        runtime.release_slot(registry, run)
        if 'data_revision' in registry:
            registry["data_revision"] += 1
        path = runtime.run_path(task_id, run_id, snapshot)
        changes = {pointer: encode(record), path + "/run.json": encode(run),
                runtime.root(task_id, snapshot) + "/runtime.json": encode(registry),
                path + "/result.json": encode(record),
                path + "/cleanup.json": encode({"run_id": run_id, "status": "retained",
                    "reason": "Remote checkpoint delivery; preserve original artifacts"})}
        from . import reports
        changes.update(reports.retained_changes(run, path, operation_id, 'checkpoint_delivery_preserves_artifacts'))
        return changes, record
    return store.transact(operation_id, request, apply)
