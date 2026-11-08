"""Frozen 2.8 prepare/config validation from c3a674ccfbb8d52944e17084b50de9a792e58ee9.

Only the two relevant legacy methods are frozen; Git/common helpers are shared
with the current test runtime. This client does not record preparation evidence.
"""
import json
from pathlib import Path
from task_runtime.store import Store, TaskError, git, refs_overlap, atomic_json
from task_runtime.runtime import check, local_state, load_run, names


class LegacyStore(Store):
    def validate_config(self, config):
        if (not config or config.get("schema_version") not in (2, 3, 4, 5, 6) or config.get("authority_ref") != self.ref or
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


def prepare(store, task_id, run_id, path, mode="worktree", *, restore=False, stopped=False, settled=False):
    path = Path(path).absolute()
    state_path = local_state(store, task_id, run_id)
    with store.lock(state_path.with_suffix(".lock")):
        run = check(store, task_id, run_id)
        if mode not in ("worktree", "workspace"):
            raise TaskError("Unknown workspace mode")
        if path.is_symlink():
            raise TaskError("Workspace cannot be a symlink")
        old = json.loads(state_path.read_text()) if state_path.exists() else None
        if old and (old["path"] != str(path) or old["mode"] != mode or old["generation"] != run["generation"]):
            raise TaskError("Run is already bound to another workspace")
        if old and old.get("prepared"):
            if not path.exists() or git(path, "rev-parse", "--show-toplevel") != str(path.resolve()):
                raise TaskError("Registered workspace is missing or changed")
            if not git(path, "merge-base", "--is-ancestor", run["source_head"], "HEAD", predicate=True):
                raise TaskError("Workspace does not contain the saved source")
            return old
        start_head = run["definition_commit"]
        if restore:
            if not stopped or not settled:
                raise TaskError("Restore requires confirmation that the previous writer/children stopped and operations settled")
            start_head = store.fetch(run["work_ref"], missing=True)
            if not start_head or not store.contains(start_head, run["source_head"]):
                raise TaskError("No compatible remote work checkpoint to restore")
            if git(store.repo, "rev-list", "--merges", run["definition_commit"] + ".." + start_head):
                raise TaskError("Checkpoint contains unsupported merge history")
            for commit in git(store.repo, "rev-list", run["definition_commit"] + ".." + start_head).splitlines():
                paths = names(store.repo, "diff-tree", "--no-commit-id", "--name-only", "--no-renames", "-r", "-z", commit)
                if any(not allowed(task_id, run_id, p, run) for p in paths):
                    raise TaskError("Checkpoint crosses the run boundary")
        elif not old and (run["publication_count"] or store.remote_tip(run["work_ref"])):
            raise TaskError("A remote checkpoint exists; use restore after confirming the old writer stopped")
        state = old or {"path": str(path), "mode": mode, "task_id": task_id, "run_id": run_id,
                        "generation": run["generation"], "repository_id": run["repository_id"],
                        "definition_commit": run["definition_commit"], "prepared": False}
        if old and old.get("start_head", start_head) != start_head:
            raise TaskError("Interrupted preparation must retain its original checkpoint")
        state["start_head"] = start_head
        if mode == "workspace":
            if (git(path, "rev-parse", "HEAD") != start_head or
                    git(path, "status", "--porcelain", "--untracked-files=all")):
                raise TaskError("Provisioned workspace must be clean at the pinned definition commit")
            other_common = (path / git(path, "rev-parse", "--git-common-dir")).resolve()
            if other_common != store.common:
                raise TaskError("Use --repo pointing at the provisioned workspace")
            reservation = store.local / "workspace-reservation.json"
            with store.lock(reservation.with_suffix(".lock")):
                if reservation.exists():
                    owner = json.loads(reservation.read_text())
                    if owner["run_id"] != run_id or owner["task_id"] != task_id:
                        previous = load_run(store.snapshot(), owner["task_id"], owner["run_id"])[2]
                        confirmed_recovery = (previous["status"] == "lost" and
                                              previous.get("recovery_kind") == "confirmed_stopped" and
                                              previous.get("recovery_evidence"))
                        if previous["status"] not in ("succeeded", "failed", "cancelled") and not confirmed_recovery:
                            raise TaskError("Provisioned workspace is reserved by another unfinished run")
                atomic_json(reservation, state)
            atomic_json(state_path, state)
        else:
            branch = run["work_ref"].removeprefix("refs/heads/")
            if path.exists() and not old:
                raise TaskError("Worktree destination already exists")
            atomic_json(state_path, state)
            if not path.exists():
                exists = git(store.repo, "show-ref", "--verify", "--quiet", run["work_ref"], predicate=True)
                if exists:
                    if not old or git(store.repo, "rev-parse", run["work_ref"]) != start_head:
                        raise TaskError("Existing work branch needs inspection")
                    git(store.repo, "worktree", "add", str(path), branch)
                else:
                    git(store.repo, "worktree", "add", "-b", branch, str(path), start_head)
            elif (git(path, "symbolic-ref", "HEAD") != run["work_ref"] or
                  git(path, "rev-parse", "HEAD") != start_head):
                raise TaskError("Interrupted worktree setup needs inspection")
        state["prepared"] = True
        atomic_json(state_path, state)
        return state
