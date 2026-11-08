#!/usr/bin/env python3
"""TEST ONLY: local bare-repository API double, never a hosted-service adapter.

No fetch/push/ls-remote commands: transfer exact object packs, and atomically
compare/update bare refs. The fixture accepts only a local absolute bare origin.
"""
import json
from pathlib import Path
import subprocess
import sys


def git(cwd, *args, data=None, check=True):
    result = subprocess.run(["git", *args], cwd=cwd, input=data, capture_output=True, timeout=30)
    if check and result.returncode:
        raise RuntimeError("fixture_git_failed")
    return result


def transfer(source, destination, oid):
    pack = git(source, "pack-objects", "--stdout", "--revs", data=(oid + "\n").encode()).stdout
    git(destination, "index-pack", "--stdin", data=pack)


def handle(request):
    context = request["context"]
    repo = Path(context["repository"]).resolve()
    remote = git(repo, "remote", "get-url", context["remote"]).stdout.decode().strip()
    server = Path(remote)
    if not server.is_absolute() or not server.is_dir() or git(server, "rev-parse", "--is-bare-repository").stdout != b"true\n":
        return {"status": "unsupported"}
    common = git(repo, "rev-parse", "--git-common-dir").stdout.decode().strip()
    settings_file = (repo / common).resolve() / "transport-fixture.json"
    settings = json.loads(settings_file.read_text()) if settings_file.exists() else {}
    if settings.get("leak"):
        print("secret-fixture-value", file=sys.stderr)
        return {"status": "error", "message": "secret-fixture-value"}
    op = request["operation"]
    if op == "capabilities":
        names = ("exact_ref_read", "ref_listing", "exact_object_fetch", "exact_object_push", "compare_and_swap",
                 "create_only", "conditional_delete", "append_only", "branch_refs", "tag_refs")
        return {"capabilities": {name: name not in settings.get("missing", []) for name in names}}
    if op == "read_ref":
        ref = request["ref"]
        git(server, "check-ref-format", ref)
        result = git(server, "show-ref", "--verify", "--hash", ref, check=False)
        if result.returncode not in (0, 1, 128):
            return {"status": "unknown"}
        return {"oid": result.stdout.decode().strip() if not result.returncode else None}
    if op == "list_refs":
        rows = git(server, "for-each-ref", "--format=%(refname) %(objectname)").stdout.decode().splitlines()
        return {"refs": {ref: oid for ref, oid in (row.split() for row in rows)
                         if any(ref.startswith(prefix) for prefix in request["prefixes"])}}
    if op == "fetch_objects":
        transfer(server, repo, request["oid"])
        return {"oid": request["oid"]}
    if op == "compare_exchange":
        ref, expected, candidate = request["ref"], request["expected"], request["candidate"]
        git(server, "check-ref-format", ref)
        if candidate:
            transfer(repo, server, candidate)
        if ref == context["authority_ref"] and not candidate:
            return {"status": "unsupported"}
        if candidate and expected and git(server, "merge-base", "--is-ancestor", expected, candidate, check=False).returncode:
            return {"status": "conflict"}
        zero = "0" * (64 if context["object_format"] == "sha256" else 40)
        args = ["update-ref", ref, candidate, expected or zero] if candidate else ["update-ref", "-d", ref, expected]
        result = git(server, *args, check=False)
        if result.returncode:
            return {"status": "conflict"}
        if settings.get("lose_reply"):
            settings.pop("lose_reply")
            settings_file.write_text(json.dumps(settings))
            return {"status": "unknown"}
        return {"oid": candidate}
    return {"status": "unsupported"}


if __name__ == "__main__":
    request = json.load(sys.stdin)
    try:
        result = handle(request)
    except Exception:
        result = {"status": "unknown"}
    result.setdefault("status", "ok")
    result.update(protocol="task-runtime-transport/v1", request_id=request["request_id"])
    print(json.dumps(result))
