#!/usr/bin/env python3
"""TEST ONLY: ordinary real-Git CAS and a separately observed clock.

The CAS path never inspects a deadline, proof, run or generation. No server hook,
ref gateway or time/CAS lock provides hidden enforcement.
"""
import json
from pathlib import Path
import sys

from task_transport_adapter import git, handle as ordinary
from task_lease_adapter import profile


def handle(request):
    ctx = request["context"]
    repo = Path(ctx["repository"])
    server = Path(git(repo, "remote", "get-url", ctx["remote"]).stdout.decode().strip())
    settings_path = server / "lease-test.json"
    settings = json.loads(settings_path.read_text())
    op = request["operation"]
    if op == "lease_capabilities":
        return {"contract": dict(version=1, atomic_time_cas=False, enforced_writers=False,
            generation_timeout=not settings.get("no_generation"),
            coordinated_writers=not settings.get("uncoordinated"),
            destinations_fenced=not settings.get("unfenced"), profiles={"test": profile(settings)})}
    if op == "trusted_time":
        if settings.get("clock_missing"):
            return {"status": "unsupported"}
        return {"sample": dict(clock_id="test-clock", lower_ms=settings["now_ms"],
            upper_ms=settings["now_ms"] + settings.get("uncertainty_ms", 0),
            sample_age_ms=settings.get("sample_age_ms", 0), expected=request["expected"],
            nonce="wrong" if settings.get("wrong_nonce") else request["nonce"])}
    if op == "compare_exchange":
        if "guard" in request:
            return {"status": "unsupported"}
        if request["ref"] == ctx["authority_ref"]:
            # Fault injection changes only the separately read clock.
            # The receiver still makes no time-dependent acceptance decision.
            advance = settings.pop("advance_on_cas_ms", 0)
            settings["now_ms"] += advance
            if settings.pop("reject_before_cas", False):
                settings_path.write_text(json.dumps(settings))
                return {"status": "unknown"}
            lose_reply = settings.pop("lose_reply", False)
            if advance or lose_reply:
                settings_path.write_text(json.dumps(settings))
            result = ordinary(request)
            return {"status": "unknown"} if lose_reply and result.get("status", "ok") == "ok" else result
    return ordinary(request)


if __name__ == "__main__":
    request = json.load(sys.stdin)
    try:
        result = handle(request)
    except Exception:
        result = {"status": "unknown"}
    result.setdefault("status", "ok")
    result.update(protocol="task-runtime-transport/v1", request_id=request["request_id"])
    print(json.dumps(result))
