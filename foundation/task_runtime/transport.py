"""Versioned, opt-in transport boundary. No credentials or provider defaults."""
import os
from pathlib import Path
import re
import subprocess
import uuid

from . import store as storage


PROTOCOL = "task-runtime-transport/v1"
CAPABILITIES = ("exact_ref_read", "ref_listing", "exact_object_fetch", "exact_object_push",
                "compare_and_swap", "create_only", "conditional_delete", "append_only",
                "branch_refs", "tag_refs")


class TransportError(storage.TaskError):
    def __init__(self, code, operation, *, missing=()):
        self.code, self.operation, self.missing = code, operation, list(missing)
        self.definite_rejection = code == "transport_conflict"
        super().__init__(code + " during " + operation +
                         (": " + ", ".join(missing) if missing else ""))


def oid(value, object_format):
    if not isinstance(value, str) or not re.fullmatch(
            r"[0-9a-f]{%d}" % (64 if object_format == "sha256" else 40), value):
        raise TransportError("invalid_transport_response", "object_id")
    return value


class DirectGit:
    name = "direct_git"

    def __init__(self, repo, remote, authority_ref):
        self.repo, self.remote, self.authority_ref = repo, remote, authority_ref
        self.shallow_file = None

    def capabilities(self, scope):
        # This is the normal Git transport contract, not a probe of server ACLs.
        return {name: True for name in CAPABILITIES}

    def read_ref(self, ref):
        rows = storage.git(self.repo, "ls-remote", "--refs", self.remote, ref).splitlines()
        if not rows:
            return None
        if len(rows) != 1 or len(rows[0].split()) != 2 or rows[0].split()[1] != ref:
            raise TransportError("unsupported_transport", "exact_ref_read")
        return rows[0].split()[0]

    def list_refs(self, prefixes):
        rows = storage.git(self.repo, "ls-remote", "--refs", self.remote,
                           *(prefix + "*" for prefix in prefixes)).splitlines()
        result = {}
        for row in rows:
            parts = row.split()
            if len(parts) != 2:
                raise TransportError("invalid_transport_response", "list_refs")
            value, ref = parts
            if any(ref.startswith(prefix) for prefix in prefixes):
                result[ref] = value
        return result

    def fetch(self, ref, missing=False):
        if missing and self.read_ref(ref) is None:
            return None
        temporary = "refs/task-runtime-fetch/" + uuid.uuid4().hex
        try:
            if self.shallow_file is None:
                self.shallow_file = (self.repo / storage.git(self.repo, 'rev-parse', '--git-path', 'shallow')).resolve()
            history = ['--unshallow'] if ref == self.authority_ref and self.shallow_file.is_file() else []
            storage.git(self.repo, "fetch", "--no-tags", "--no-write-fetch-head", "--refmap=", *history, self.remote,
                        ref + ":" + temporary)
            try:
                return storage.git(self.repo, "rev-parse", "--verify", temporary)
            except storage.GitFailure:
                raise TransportError("unsupported_transport", "exact_object_fetch") from None
        finally:
            storage.git(self.repo, "update-ref", "-d", temporary)

    def push(self, ref, candidate, expected):
        return storage.git(self.repo, "push", "--porcelain",
                           "--force-with-lease=" + ref + ":" + (expected or ""),
                           self.remote, (candidate or "") + ":" + ref)


class External:
    """Trusted, installed executable; one request/response per invocation.

    The adapter imports/exports objects through the local object database. It
    must never change HEAD, index, worktree files or unrelated local refs.
    """
    name = "external"

    def __init__(self, repo, remote, authority_ref, program):
        repo = Path(repo).resolve()
        path = Path(program)
        if (not path.is_absolute() or not path.is_file() or not os.access(path, os.X_OK)
                or path.resolve().is_relative_to(repo)):
            raise TransportError("invalid_transport_adapter", "configure")
        self.program, self.repo = str(path.resolve()), repo
        self.object_format = storage.git(repo, "rev-parse", "--show-object-format")
        self.context = {"repository": str(repo), "remote": remote, "authority_ref": authority_ref,
                        "object_format": self.object_format}

    def call(self, operation, **parameters):
        request = dict(protocol=PROTOCOL, operation=operation, context=self.context, **parameters)
        request["request_id"] = storage.digest(request)
        try:
            response = storage.git_support.run_process([self.program], cwd=self.repo,
                                                       input=storage.encode(request), timeout=60)
        except (OSError, subprocess.TimeoutExpired):
            raise TransportError("transport_outcome_unknown", operation) from None
        if response.returncode or len(response.stdout) > 1024 * 1024:
            raise TransportError("transport_outcome_unknown", operation)
        try:
            import json
            result = json.loads(response.stdout)
        except (ValueError, UnicodeError):
            raise TransportError("invalid_transport_response", operation) from None
        if (not isinstance(result, dict) or result.get("protocol") != PROTOCOL or
                result.get("request_id") != request["request_id"]):
            raise TransportError("invalid_transport_response", operation)
        status = result.get("status")
        if status != "ok":
            code = {"unsupported": "unsupported_transport", "conflict": "transport_conflict",
                    "guard_rejected": "lease_guard_rejected",
                    "unknown": "transport_outcome_unknown"}.get(status, "transport_outcome_unknown")
            # Adapter text and stderr may contain URLs, credentials or service
            # output. Only the fixed vocabulary above reaches diagnostics.
            raise TransportError(code, operation)
        return result

    def capabilities(self, scope):
        result = self.call("capabilities", scope=scope).get("capabilities")
        if not isinstance(result, dict):
            raise TransportError("invalid_transport_response", "capabilities")
        return {name: result.get(name) is True for name in CAPABILITIES}

    def read_ref(self, ref):
        result = self.call("read_ref", ref=ref)
        if "oid" not in result:
            raise TransportError("invalid_transport_response", "read_ref")
        return None if result["oid"] is None else oid(result["oid"], self.object_format)

    def list_refs(self, prefixes):
        result = self.call("list_refs", prefixes=prefixes).get("refs")
        if not isinstance(result, dict):
            raise TransportError("invalid_transport_response", "list_refs")
        for ref, value in result.items():
            if not isinstance(ref, str) or not any(ref.startswith(prefix) for prefix in prefixes):
                raise TransportError("invalid_transport_response", "list_refs")
            storage.git(self.repo, "check-ref-format", ref)
            oid(value, self.object_format)
        return result

    def fetch(self, ref, missing=False):
        tip = self.read_ref(ref)
        if tip is None:
            if missing:
                return None
            raise TransportError("transport_ref_missing", "fetch_objects")
        result = self.call("fetch_objects", oid=tip)
        if result.get("oid") != tip:
            raise TransportError("invalid_transport_response", "fetch_objects")
        # Verify the imported object identity and its complete reachable closure,
        # including binary blobs. No checkout or fetch destination is assumed.
        if storage.git(self.repo, "cat-file", "-t", tip) != "commit":
            raise TransportError("invalid_transport_response", "fetch_objects")
        objects = storage.git(self.repo, "rev-list", "--objects", "--missing=print", tip)
        if any(line.startswith("?") for line in objects.splitlines()):
            raise TransportError("invalid_transport_response", "fetch_objects")
        return tip

    def lease_capabilities(self, task_id):
        return self.call("lease_capabilities", task_id=task_id).get("contract")

    def trusted_time(self, task_id, expected, policy, nonce):
        return self.call("trusted_time", task_id=task_id, expected=expected, policy=policy, nonce=nonce).get("sample")

    def push(self, ref, candidate, expected, *, guard=None):
        parameters = {"guard": guard} if guard is not None else {}
        result = self.call("compare_exchange", ref=ref, candidate=candidate, expected=expected, **parameters)
        if "oid" not in result or result["oid"] != candidate:
            raise TransportError("invalid_transport_response", "compare_exchange")
        return result


def create(repo, remote, authority_ref, program=None):
    if program is None:
        program = os.environ.get("TASKCTL_TRANSPORT_ADAPTER")
    return (DirectGit(repo, remote, authority_ref) if program is None else
            External(repo, remote, authority_ref, program))
