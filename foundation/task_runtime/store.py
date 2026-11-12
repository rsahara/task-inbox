"""Single-ref transactions, immutable Git objects, and durable push reconciliation."""
from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import tempfile
import time

from . import git_support

git = git_support.git
GitFailure = git_support.GitFailure
CONTROL = ".task-system"


class TaskError(RuntimeError):
    pass


class SupersededRenewal(TaskError):
    """An immutable renewal can no longer win its original parent CAS."""


def now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def encode(value):
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode()


def digest(value):
    return hashlib.sha256(encode(value)).hexdigest()


def reservation_id(operation_id):
    return digest(dict(kind='operation-reservation', operation_id=operation_id))


def identity(value, label="identifier"):
    if not isinstance(value, str) or not re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,95}", value):
        raise TaskError(label + " must contain 1-96 lowercase letters, digits, hyphens or underscores")
    return value


def safe_path(value):
    path = PurePosixPath(value)
    if (not value or path.is_absolute() or str(path) != value or
            any(p in (".", "..", ".git") for p in path.parts) or
            any(ord(c) < 32 for c in value) or "\\" in value):
        raise TaskError("Invalid repository-relative path")
    return value


def refs_overlap(left, right):
    return left == right or left.startswith(right + "/") or right.startswith(left + "/")


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd, tmp = tempfile.mkstemp(prefix=".pending-", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(encode(value)); stream.flush(); os.fsync(stream.fileno())
        os.replace(tmp, path)
        with directory_fd(path.parent) as descriptor:
            os.fsync(descriptor)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


@contextmanager
def directory_fd(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        yield fd
    finally:
        os.close(fd)


@contextmanager
def local_lock(path, timeout=10, *, shared=False):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with path.open("a") as stream:
        deadline = time.monotonic() + timeout
        while True:
            try:
                fcntl.flock(stream, (fcntl.LOCK_SH if shared else fcntl.LOCK_EX) | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise TaskError("Local operation is busy; retry with the same operation ID")
                time.sleep(0.02)
        try:
            yield
        finally:
            fcntl.flock(stream, fcntl.LOCK_UN)


class Snapshot:
    def __init__(self, store, commit):
        self.store, self.commit = store, commit
        raw = git(store.repo, "ls-tree", "-r", "-z", commit, binary=True) if commit else b""
        self.entries = {}
        for row in raw.split(b"\0"):
            if row:
                header, path = row.split(b"\t", 1)
                mode, kind, oid = header.decode().split()
                self.entries[path.decode()] = (mode, kind, oid)

    def read(self, path, default=None):
        item = self.entries.get(path)
        if item is None:
            return default
        if item[0] not in ("100644", "100755") or item[1] != "blob":
            raise TaskError("Metadata must be a regular file")
        return git(self.store.repo, "cat-file", "blob", item[2], binary=True, strip=False)

    def json(self, path, default=None):
        data = self.read(path)
        return default if data is None else json.loads(data)

    def metadata_snapshot(self, path):
        """Read retired runtime metadata from the append-only authority history."""
        if path in self.entries or self.json(CONTROL + '/config.json', {}).get('schema_version', 0) < 6 or not self.commit:
            return self
        safe_path(path)
        self.store.require_history()
        if not hasattr(self, '_metadata_snapshots'):
            self._metadata_snapshots = {}
        if path not in self._metadata_snapshots:
            # A merge's first parent can hold a stale running record. Read the
            # original retirement commit, including those on merged branches.
            changed = git(self.store.repo, 'log', '--full-history', '--no-merges',
                          '--no-patch', '--diff-filter=D', '-1', '--format=%H', self.commit, '--', ':(literal)' + path)
            parents = git(self.store.repo, 'rev-list', '--parents', '-n', '1', changed).split() if changed else []
            previous = Snapshot(self.store, parents[1]) if len(parents) > 1 else self
            self._metadata_snapshots[path] = previous
        return self._metadata_snapshots[path]

    def metadata(self, path, default=None):
        return self.metadata_snapshot(path).json(path, default)

    def content_hash(self, prefix):
        return digest({p: v for p, v in self.entries.items() if p.startswith(prefix)})


class Store:
    def __init__(self, repo, remote="origin", branch="main", *, transport_adapter=None):
        self.repo = Path(repo).resolve()
        if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9._-]*", remote):
            raise TaskError("remote must be a configured remote name")
        self.remote, self.ref = remote, "refs/heads/" + branch
        git(self.repo, "check-ref-format", self.ref)
        common = Path(git(self.repo, "rev-parse", "--git-common-dir"))
        self.common = (self.repo / common).resolve()
        key = hashlib.sha256((remote + "\0" + self.ref).encode()).hexdigest()[:24]
        self.local = self.common / "task-runtime-v2" / key
        self.local.mkdir(parents=True, exist_ok=True, mode=0o700)
        from .transport import create
        self.transport = create(self.repo, remote, self.ref, transport_adapter)
        self.ref_prefix = "task-runtime"
        self._transport_checked = None

    @contextmanager
    def lock(self, path, timeout=10):
        # A fixed shared gate prevents unlinking lock files while a process
        # holds or waits on their old inode. Maintenance takes it exclusively.
        with local_lock(self.local / ".locks.lock", timeout, shared=True):
            with local_lock(path, timeout):
                yield

    def remote_tip(self, ref):
        return self.transport.read_ref(ref)

    def list_refs(self, prefixes):
        return self.transport.list_refs(prefixes)

    def fetch(self, ref=None, missing=False):
        return self.transport.fetch(ref or self.ref, missing)

    def preflight(self):
        """Read-only capability declaration + ref/object-read checks, not an ACL certification."""
        from .transport import CAPABILITIES
        tip = self.remote_tip(self.ref)
        if tip:
            snapshot = Snapshot(self, self.fetch())
            config = snapshot.json(CONTROL + "/config.json")
            if config:
                self.validate_config(config)
        scope = {"authority_ref": self.ref, "run_ref_namespace": "refs/heads/" + self.ref_prefix,
                 "archive_ref_namespace": "refs/tags/task-runs"}
        capabilities = self.transport.capabilities(scope)
        missing = [name for name in CAPABILITIES if capabilities.get(name) is not True]
        return {"transport": self.transport.name, "capabilities": capabilities,
                "scope": scope, "missing_capabilities": missing, "supported": not missing,
                "read_check": "verified", "write_check": "declared_not_probed",
                "needs_attention": bool(missing)}

    def require_transport(self):
        from .transport import TransportError
        if self._transport_checked != self.ref_prefix:
            result = self.preflight()
            if not result["supported"]:
                raise TransportError("unsupported_transport", "preflight", missing=result["missing_capabilities"])
            self._transport_checked = self.ref_prefix

    def snapshot(self):
        snapshot = Snapshot(self, self.fetch())
        self.validate_config(snapshot.json(CONTROL + "/config.json"))
        return snapshot

    def validate_config(self, config):
        if (not config or config.get("schema_version") not in (2, 3, 4, 5, 6, 7, 8, 9, 10) or config.get("authority_ref") != self.ref or
                (config.get('schema_version', 0) >= 7 and config.get('workspace_provenance_protocol') != 1) or
                (config.get("schema_version") in (3, 4) and config.get("lease_protocol") != 1) or
                (config.get("schema_version") == 4 and config.get("generation_timeout_protocol") != 1)):
            raise TaskError("This branch is not initialized as the execution authority")
        from . import layout
        layout.settings(config)
        prefix = config.get("ref_prefix")
        if not isinstance(prefix, str) or not prefix:
            raise TaskError("Invalid run ref namespace")
        run_namespace = "refs/heads/" + prefix
        git(self.repo, "check-ref-format", run_namespace)
        if refs_overlap(self.ref, run_namespace):
            raise TaskError("Execution authority overlaps the reserved run ref namespace")
        self.ref_prefix = prefix
        if config['schema_version'] >= 6:
            self.require_history()

    def require_history(self):
        if not getattr(self, '_history_checked', False):
            if git(self.repo, 'rev-parse', '--is-shallow-repository') != 'false':
                raise TaskError('Complete authority history is required to reconcile retired runtime metadata')
            self._history_checked = True

    def blob(self, content):
        return ("100644", "blob", git(self.repo, "hash-object", "-w", "--stdin", input=content))

    def empty_tree(self):
        if not hasattr(self, "_empty_tree"):
            self._empty_tree = git(self.repo, "mktree", "-z", input=b"")
        return self._empty_tree

    def tree(self, parent, changes):
        """Rewrite only affected trees; no checkout, index, or shared FETCH_HEAD."""
        grouped = {}
        for path, value in changes.items():
            safe_path(path)
            head, _, tail = path.partition("/")
            grouped.setdefault(head, {})[tail] = value
        original = {}
        if parent:
            for row in git(self.repo, "ls-tree", "-z", parent, binary=True).split(b"\0"):
                if row:
                    header, name = row.split(b"\t", 1)
                    original[name.decode()] = tuple(header.decode().split())
        for name, children in grouped.items():
            old = original.get(name)
            descendants = {path: value for path, value in children.items() if path}
            if "" in children:
                value = children[""]
                if value is None:
                    # Deleting the old file permits adding a directory at the
                    # same path in this transaction.
                    original.pop(name, None)
                    old = None
                else:
                    if any(entry is not None for entry in descendants.values()):
                        raise TaskError("File/directory collision")
                    if old and old[1] == "tree":
                        remaining = self.tree(old[2], descendants)
                        if remaining != self.empty_tree():
                            raise TaskError("Replacing a directory would discard undeleted files")
                    original[name] = self.blob(value) if isinstance(value, bytes) else value
                    continue
            if descendants:
                if old and old[1] != "tree":
                    raise TaskError("Parent path is not a directory")
                child = self.tree(old[2] if old else None, descendants)
                if child == self.empty_tree():
                    original.pop(name, None)
                else:
                    original[name] = ("040000", "tree", child)
        rows = [f"{mode} {kind} {oid}\t{name}".encode() + b"\0"
                for name, (mode, kind, oid) in sorted(original.items())]
        return git(self.repo, "mktree", "-z", input=b"".join(rows))

    def commit(self, parent, changes, message):
        tree = self.tree(parent, changes)
        parents = ["-p", parent] if parent else []
        return git(self.repo, "commit-tree", tree, *parents, input=message + "\n")

    def contains(self, head, commit):
        return bool(head and git(self.repo, "merge-base", "--is-ancestor", commit, head, predicate=True))

    def push(self, ref, candidate, expected, *, guard=None):
        self.require_transport()
        if ref == self.ref and not candidate:
            raise TaskError("Deleting the execution authority branch is not permitted")
        if candidate and expected and not self.contains(candidate, expected):
            raise TaskError("History replacement is not permitted")
        if guard is not None:
            return self.transport.push(ref, candidate, expected, guard=guard)
        return self.transport.push(ref, candidate, expected)

    def transact(self, operation_id, request, apply, *, initialize=False, attempt_ref=None):
        from . import leases
        from .transport import TransportError
        identity(operation_id, "operation ID")
        receipt_path = f"{CONTROL}/operations/{operation_id}.json"
        journal = self.local / "operations" / (operation_id + ".json")
        retention_ref = "refs/task-runtime-pending/" + self.local.name + "/" + operation_id
        fingerprint = digest(request)
        with self.lock(journal.with_suffix(".lock")):
            state = json.loads(journal.read_text()) if journal.exists() else {"request_hash": fingerprint}
            if state["request_hash"] != fingerprint:
                raise TaskError("Operation ID was already used for another request")
            for attempt in range(8):
                tip = self.fetch(missing=initialize)
                snapshot = Snapshot(self, tip)
                if not initialize:
                    self.validate_config(snapshot.json(CONTROL + "/config.json"))
                receipt = snapshot.metadata(receipt_path)
                if receipt:
                    if receipt["request_hash"] != fingerprint:
                        raise TaskError("Operation ID was already used for another request")
                    state.update(done=True, receipt=receipt)
                    atomic_json(journal, state)
                    git(self.repo, "update-ref", "-d", retention_ref)
                    return dict(receipt["result"], operation_id=operation_id, committed=True)
                # Multi-ref operations reserve their public identity before
                # writing a candidate elsewhere. Every authority mutation must
                # honor that binding, including after receipt retirement/GC.
                reservation = snapshot.metadata(CONTROL + '/operations/' + reservation_id(operation_id) + '.json')
                if reservation:
                    bound = reservation.get('result', {})
                    if bound.get('reserved_operation') != operation_id or bound.get('request_hash') != fingerprint:
                        raise TaskError('Operation ID was already used for another request')
                self.require_transport()
                pending = state.get("pending")
                if attempt_ref:
                    # A create-only remote ref binds the original renewal
                    # across lost workspaces. It grants no execution authority.
                    frozen = self.fetch(attempt_ref, missing=True)
                    if frozen:
                        saved = Snapshot(self, frozen).json(receipt_path)
                        parents = git(self.repo, "rev-list", "--parents", "-n", "1", frozen).split()[1:]
                        if not saved or saved.get("request_hash") != fingerprint or len(parents) != 1:
                            raise TaskError("Frozen renewal identity differs from this request")
                        pending = {"expected": parents[0], "candidate": frozen}
                        state["pending"] = pending
                if pending and tip != pending["expected"]:
                    advanced = tip and (pending["expected"] is None or
                                        self.contains(tip, pending["expected"]))
                    if not advanced or self.contains(tip, pending["candidate"]):
                        raise TaskError("Push outcome is unknown; retain operation " + operation_id)
                    # On append-only authority history, no receipt/candidate
                    # after an advance proves this candidate was not applied.
                    # Its old lease also fences any delayed push. Revalidate
                    # the same request against the observed snapshot.
                    if attempt_ref:
                        raise SupersededRenewal("Frozen renewal was superseded; check ownership before using a new operation ID")
                    state.pop("pending")
                    atomic_json(journal, state)
                    git(self.repo, "update-ref", "-d", retention_ref)
                    pending = None
                if pending:
                    candidate = pending["candidate"]
                    guard = pending.get("guard")
                    git(self.repo, "update-ref", retention_ref, candidate)
                else:
                    guard = leases.guard(self, snapshot, request)
                    snapshot.lease_guard = guard
                    changes, result = apply(snapshot)
                    if guard and guard["sample"]:
                        changes[leases.clock_path(guard["policy"])] = encode({
                            key: guard["sample"][key] for key in ("clock_id", "lower_ms", "upper_ms")})
                    receipt = {"schema_version": 2, "operation_id": operation_id,
                               "request_hash": fingerprint, "result": result, "created_at": now()}
                    if request.get('kind', '').startswith('run-') and request.get('task_id') and request.get('run_id'):
                        receipt['scope'] = {key: request[key] for key in ('task_id', 'run_id')}
                    changes[receipt_path] = encode(receipt)
                    candidate = self.commit(tip, changes, "Task operation " + request["kind"] + " " + operation_id)
                    # A journal SHA alone does not keep objects alive through
                    # GC. Pin the exact candidate before journaling or pushing.
                    git(self.repo, "update-ref", retention_ref, candidate)
                    if attempt_ref:
                        try:
                            self.push(attempt_ref, candidate, None)
                        except (GitFailure, TransportError):
                            pass
                        frozen = self.fetch(attempt_ref, missing=True)
                        if not frozen:
                            raise TaskError("Renewal reservation is unknown; retry the same operation ID")
                        if frozen != candidate:
                            continue
                    state["pending"] = {"expected": tip, "candidate": candidate}
                    if guard:
                        state["pending"]["guard"] = guard
                    atomic_json(journal, state)
                try:
                    if guard and not guard.get("client_time_only"):
                        self.push(self.ref, candidate, tip, guard=guard)
                    else:
                        self.push(self.ref, candidate, tip)
                except (GitFailure, TransportError) as error:
                    if isinstance(error, TransportError) and error.code == "lease_guard_rejected":
                        # Contract: this proof is permanently unusable, and this
                        # exact candidate was not accepted. Never refresh a proof
                        # for an unconfirmed candidate behind the caller's back.
                        state.pop("pending", None)
                        atomic_json(journal, state)
                        git(self.repo, "update-ref", "-d", retention_ref)
                        raise
                    if error.definite_rejection and not pending and not attempt_ref:
                        state.pop("pending", None)
                        atomic_json(journal, state)
                        git(self.repo, "update-ref", "-d", retention_ref)
                        time.sleep(min(0.02 * (attempt + 1), 0.15))
                        continue
                    # Readback resolves a successful request with a lost reply.
                    remote = Snapshot(self, self.fetch())
                    found = remote.metadata(receipt_path)
                    if found and found["request_hash"] == fingerprint and self.contains(remote.commit, candidate):
                        state.update(done=True, receipt=found)
                        atomic_json(journal, state)
                        git(self.repo, "update-ref", "-d", retention_ref)
                        return dict(found["result"], operation_id=operation_id, committed=True)
                    if (attempt_ref and remote.commit != tip and self.contains(remote.commit, tip) and
                            not self.contains(remote.commit, candidate)):
                        raise SupersededRenewal("Frozen renewal was superseded; check ownership before using a new operation ID") from None
                    raise TaskError("Push outcome is unknown; retry operation " + operation_id) from None
                confirmed = Snapshot(self, self.fetch())
                found = confirmed.metadata(receipt_path)
                if not found or found["request_hash"] != fingerprint or not self.contains(confirmed.commit, candidate):
                    raise TaskError("Publication readback failed; retain operation " + operation_id)
                state.update(done=True, receipt=found)
                atomic_json(journal, state)
                git(self.repo, "update-ref", "-d", retention_ref)
                return dict(found["result"], operation_id=operation_id, committed=True)
            raise TaskError("Concurrent updates exceeded retry limit; retry operation " + operation_id)
