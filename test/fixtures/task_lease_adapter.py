#!/usr/bin/env python3
"""TEST ONLY: a serialized, deterministic trusted-clock/CAS server over real bare Git.

The bare-repository clock/settings are outside every disposable observer clone.
This is not a production time source or an adapter for a hosted Git provider.
"""
import fcntl
import hashlib
import hmac
import json
from pathlib import Path
import sys

from task_transport_adapter import git, handle as ordinary, transfer


def encoded(value):
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode()


def hashed(value):
    return hashlib.sha256(encoded(value)).hexdigest()


def record(server, commit, path):
    if not commit:
        return None
    result = git(server, "show", commit + ":" + path, check=False)
    return json.loads(result.stdout) if result.returncode == 0 else None


def git_names(server, *args):
    return [name.decode() for name in git(server, *args).stdout.split(b'\0') if name]


def task_path(server, commit, task_id):
    config = record(server, commit, '.task-system/config.json') or {}
    if config.get('layout'):
        candidates = [record(server, commit, config['layout']['index'])['tasks'][task_id]]
    else:
        candidates = [name.rsplit('/', 1)[0] for name in git_names(
            server, 'ls-tree', '-r', '-z', '--name-only', commit, '--', 'tasks/')
            if len(name.split('/')) == 3 and name.endswith('/task.json')]
    matches = [path for path in candidates if (record(server, commit, path + '/task.json') or {}).get('id') == task_id]
    if len(matches) != 1:
        raise ValueError('Task identity is missing or duplicated')
    return matches[0]


def profile(settings):
    return dict(version=1, profile="test", clock_id="test-clock", ttl_ms=300000,
                max_uncertainty_ms=100, max_sample_age_ms=10000, max_request_delay_ms=10000,
                effects=settings.get("effects", "authority_only"))


def sign(settings, value):
    return hmac.new(settings["key"].encode(), encoded(value), hashlib.sha256).hexdigest()


def handle(request):
    ctx = request["context"]
    repo = Path(ctx["repository"])
    server = Path(git(repo, "remote", "get-url", ctx["remote"]).stdout.decode().strip())
    if not server.is_absolute() or not server.is_dir():
        return {"status": "unsupported"}
    with (server / "lease-test.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        settings_path = server / "lease-test.json"
        if not settings_path.exists():
            return {"status": "unsupported"}
        settings = json.loads(settings_path.read_text())
        policy = profile(settings)
        op = request["operation"]
        if op == "lease_capabilities":
            return {"contract": dict(version=1, atomic_time_cas=not settings.get("no_atomic"),
                enforced_writers=True, destinations_fenced=not settings.get("unfenced"),
                profiles={"test": policy})}
        if op == "trusted_time":
            if settings.get("clock_missing"):
                return {"status": "unsupported"}
            value = dict(clock_id="test-clock", lower_ms=settings["now_ms"],
                         upper_ms=settings["now_ms"] + settings.get("uncertainty_ms", 0),
                         expected=request["expected"], nonce=request["nonce"],
                         task_id=request["task_id"], policy_hash=hashed(request["policy"]),
                         authority_ref=ctx["authority_ref"])
            value["proof"] = sign(settings, value)
            if settings.get("wrong_nonce"):
                value["nonce"] = "wrong"
            return {"sample": value}
        if op != "compare_exchange":
            return ordinary(request)
        ref, expected, candidate = request["ref"], request["expected"], request["candidate"]
        actual = git(server, "show-ref", "--verify", "--hash", ref, check=False).stdout.decode().strip() or None
        if actual != expected:
            return {"status": "conflict"}
        if candidate:
            transfer(repo, server, candidate)
        guard = request.get("guard")
        if guard:
            if ref != ctx["authority_ref"] or not candidate or guard.get("policy") != policy:
                return {"status": "guard_rejected"}
            location = task_path(server, expected, guard['task_id'])
            if guard["action"] == "cleanup":
                prefix = location + "/runs/" + guard["run_id"]
                old = record(server, expected, prefix + "/run.json")
                new = record(server, candidate, prefix + "/run.json")
                paths = git_names(server, "diff-tree", "--no-commit-id", "--name-only", "-r", '-z', expected, candidate)
                if (not old or old["status"] not in ("succeeded", "failed", "cancelled") or
                        old["generation"] != guard["generation"] or
                        guard["request"].get("generation") != old["generation"] or
                        guard["request"].get("kind") not in ("run-cleanup", "run-cleanup-complete")):
                    return {"status": "guard_rejected"}
                slots = record(server, expected, location + "/runtime.json")["slots"]
                if slots.get(str(old["slot_id"]), {}).get("run_id") == old["run_id"]:
                    return {"status": "guard_rejected"}
                if guard['request'].get('metadata_only'):
                    if new is not None:
                        return {'status': 'guard_rejected'}
                    old_config = record(server, expected, '.task-system/config.json')
                    new_config = record(server, candidate, '.task-system/config.json')
                    selected_io = new_config.get('io_ref_prefix')
                    upgrading = isinstance(selected_io, str) and old_config.get('schema_version', 0) < 9 and new_config == dict(
                        old_config, schema_version=9, workspace_provenance_protocol=1,
                        io_ref_prefix=old_config.get('io_ref_prefix', selected_io))
                    if upgrading:
                        for path in git_names(server, 'ls-tree', '-r', '-z', '--name-only', expected):
                            if path.startswith('tasks/') and path.endswith('/runtime.json') and any(
                                    slot.get('run_id') for slot in record(server, expected, path)['slots'].values()):
                                return {'status': 'guard_rejected'}
                    for path in paths:
                        after = record(server, candidate, path)
                        if path in (prefix + '/run.json', prefix + '/result.json', prefix + '/cleanup.json') or path.startswith(prefix + '/io/'):
                            if after is not None:
                                return {'status': 'guard_rejected'}
                        elif path.startswith('.task-system/operations/'):
                            before = record(server, expected, path) or {}
                            owner = before.get('scope', before.get('result', {}))
                            if after is not None or not isinstance(owner, dict) or any(owner.get(k) != guard[k] for k in ('task_id', 'run_id')):
                                return {'status': 'guard_rejected'}
                        elif path == '.task-system/config.json' and upgrading:
                            pass
                        elif upgrading and path.startswith('tasks/') and path.endswith('/runtime.json'):
                            before = record(server, expected, path)
                            compact = {k: v for k, v in before.items() if k != 'data_revision'}
                            if any(s.get('run_id') for s in before['slots'].values()) or after != dict(compact, slots={
                                    k: {'generation': s['generation'], 'run_id': None} for k, s in before['slots'].items()}):
                                return {'status': 'guard_rejected'}
                        else:
                            return {'status': 'guard_rejected'}
                elif new != old or any(path != prefix + '/cleanup.json' and not path.startswith('.task-system/operations/') for path in paths):
                    return {'status': 'guard_rejected'}
            else:
                stamp = dict(guard["sample"])
                proof = stamp.pop("proof")
                if (not hmac.compare_digest(proof, sign(settings, stamp)) or stamp["expected"] != expected or
                        stamp["task_id"] != guard["task_id"] or stamp["policy_hash"] != hashed(policy) or
                        stamp["authority_ref"] != ref):
                    return {"status": "guard_rejected"}
                lower = settings["now_ms"] + settings.pop("advance_on_cas_ms", 0)
                upper = lower + settings.get("uncertainty_ms", 0)
                if lower != settings["now_ms"]:
                    settings["now_ms"] = lower
                    settings_path.write_text(json.dumps(settings))
                if (settings.get("clock_missing") or upper - lower > policy["max_uncertainty_ms"] or
                        lower < stamp["lower_ms"] or upper < stamp["upper_ms"] or
                        upper - stamp["lower_ms"] > min(policy["max_sample_age_ms"], policy["max_request_delay_ms"])):
                    return {"status": "guard_rejected"}
                path = location + "/runs/" + guard["run_id"] + "/run.json"
                old, new = record(server, expected, path), record(server, candidate, path)
                if not new:
                    return {"status": "guard_rejected"}
                if guard["action"] == "start":
                    if old or new["lease"]["expires_ms"] <= upper:
                        return {"status": "guard_rejected"}
                else:
                    if (not old or old["status"] != "running" or old["generation"] != guard["generation"] or
                            old["lease"]["expires_ms"] != guard["expires_ms"]):
                        return {"status": "guard_rejected"}
                    slots = record(server, expected, location + "/runtime.json")["slots"]
                    if slots[str(old["slot_id"])]["run_id"] != old["run_id"] or slots[str(old["slot_id"])]["generation"] != old["generation"]:
                        return {"status": "guard_rejected"}
                    if guard["action"] == "revoke":
                        if lower < guard["expires_ms"] or new["status"] != "lost":
                            return {"status": "guard_rejected"}
                    elif upper >= guard["expires_ms"]:
                        return {"status": "guard_rejected"}
        elif ref == ctx["authority_ref"] and candidate:
            # The fixture's standard administrative operations cannot secretly
            # change a leased run without the time/CAS contract.
            paths = git_names(server, "diff-tree", "--no-commit-id", "--name-only", "-r", '-z', candidate)
            for path in paths:
                if path.endswith("/run.json") and path.startswith("tasks/"):
                    for commit in (expected, candidate):
                        value = record(server, commit, path)
                        if value and value.get("execution", {}).get("recovery_mode") == "fenced_lease":
                            return {"status": "guard_rejected"}
        if settings.pop("reject_before_cas", False):
            settings_path.write_text(json.dumps(settings))
            return {"status": "unknown"}
        result = ordinary(request)
        if result.get("status", "ok") == "ok" and settings.pop("lose_reply", False):
            settings_path.write_text(json.dumps(settings))
            return {"status": "unknown"}
        return result


if __name__ == "__main__":
    request = json.load(sys.stdin)
    try:
        result = handle(request)
    except Exception:
        result = {"status": "unknown"}
    result.setdefault("status", "ok")
    result.update(protocol="task-runtime-transport/v1", request_id=request["request_id"])
    print(json.dumps(result))
