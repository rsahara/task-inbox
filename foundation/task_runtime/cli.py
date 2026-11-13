"""Machine-readable CLI; all service integration is optional and caller-owned."""
import argparse
import json
from pathlib import Path
import sys
import uuid

from . import VERSION, completion, leases, lifecycle, reports, runtime, task_io, signals
from .store import GitFailure, Store, TaskError, encode, identity


def parser():
    p = argparse.ArgumentParser(description="Portable Git-backed task management")
    p.add_argument("--version", action="version", version=VERSION)
    p.add_argument("--repo", default=".", help="Checkout of the task data repository")
    p.add_argument("--remote", default="origin")
    p.add_argument("--branch", default="main", help="Authoritative coordination branch")
    p.add_argument("--transport-adapter", help="Absolute path of a trusted installed transport adapter")
    p.add_argument("--operation-id", help="Stable request ID; reuse for retries")
    commands = p.add_subparsers(dest="command", required=True)
    init = commands.add_parser("init")
    init.add_argument("--index", help="Repository-relative JSON index file")
    init.add_argument("--task-root", help="Default directory for new tasks")
    commands.add_parser("transport-check", help="Read-only transport capability and ref/object diagnostics")
    signal = commands.add_parser('signal').add_subparsers(dest='action', required=True)
    signal.add_parser('enable', help='Enable schema 10 after all writers drain')
    sync = signal.add_parser('sync', help='Catch up subscribers to current signals')
    sync.add_argument('--task')
    for name in ('list', 'put', 'subscribe'):
        item = signal.add_parser(name)
        item.add_argument('--task', required=True)
        if name == 'put':
            item.add_argument('--run-id', required=True)
            item.add_argument('--file', required=True)
        if name == 'subscribe':
            item.add_argument('--consumer', required=True)
        if name == 'list':
            item.add_argument('--include-ended', action='store_true')
    io = commands.add_parser('io').add_subparsers(dest='action', required=True)
    for name in ('send', 'pending', 'status', 'complete', 'wake', 'emit'):
        item = io.add_parser(name)
        item.add_argument('--task', required=True)
        if name in ('send', 'status', 'complete'):
            item.add_argument('--message-id', required=True)
        if name in ('send', 'emit'):
            item.add_argument('--kind', required=True)
            item.add_argument('--payload-file', required=True)
        if name == 'complete':
            item.add_argument('--result-file', required=True)
        if name == 'emit':
            item.add_argument('--event-id', required=True)
        if name in ('complete', 'wake', 'emit'):
            item.add_argument('--run-id', required=name != 'wake')
        if name == 'wake':
            item.add_argument('--runner', required=True)
            item.add_argument('--force', action='store_true', help='Admit maintenance work even with an empty inbox')
    task = commands.add_parser("task").add_subparsers(dest="action", required=True)
    add = task.add_parser("add")
    add.add_argument("--id")
    add.add_argument("--title", required=True)
    add.add_argument("--body-file")
    add.add_argument("--path", help="Explicit task directory; independent of archive state")
    add.add_argument("--data-path", help="Owned output directory")
    add.add_argument("--priority", type=int, default=3)
    add.add_argument("--max-runners", type=int, default=1)
    add.add_argument("--recovery-mode", choices=("manual", "git_only"), default="manual")
    add.add_argument("--heartbeat-seconds", type=int, default=60)
    add.add_argument("--stale-seconds", type=int, default=300)
    listing = task.add_parser("list")
    listing.add_argument("--all", action="store_true")
    show = task.add_parser("show")
    show.add_argument("--id", required=True)
    policy = task.add_parser("recovery-policy")
    policy.add_argument("--id", required=True)
    policy.add_argument("--expected-revision", type=int, required=True)
    policy.add_argument("--mode", choices=("manual", "git_only", *leases.MODES), required=True)
    policy.add_argument("--profile", help="Environment-installed lease profile ID, never a program path")
    policy.add_argument("--confirm-writers-drained", action="store_true")
    edit = task.add_parser("edit")
    edit.add_argument("--id", required=True)
    edit.add_argument("--expected-revision", required=True, type=int)
    edit.add_argument("--title")
    edit.add_argument("--body-file")
    edit.add_argument("--priority", type=int)
    edit.add_argument("--max-runners", type=int)
    edit.add_argument("--status", choices=("open", "paused", "completed", "cancelled", "deleted"))
    archiving = edit.add_mutually_exclusive_group()
    archiving.add_argument("--archive", dest="archived", action="store_true", default=None)
    archiving.add_argument("--unarchive", dest="archived", action="store_false", default=None)
    runs = commands.add_parser("run").add_subparsers(dest="action", required=True)
    for name in ("start", "show", "check", "prepare", "restore", "heartbeat", "renew", "checkpoint", "deliver-checkpoint", "publish", "finish", "completion", "observe", "recover", "reap", "cleanup", "exec", "report"):
        run = runs.add_parser(name)
        run.add_argument("--task", required=name != "report")
        run.add_argument("--run-id", required=name not in ("start", "reap", "report"))
        if name == "report":
            run.add_argument("--format", choices=("json", "markdown"), default="json")
        elif name == "start":
            run.add_argument("--runner", required=True)
            run.add_argument("--machine")
            run.add_argument("--item-key")
            run.add_argument("--resume-from")
            run.add_argument("--checkpoint-id")
            run.add_argument("--completion-contract-file")
        elif name == "completion":
            run.add_argument("--record-file", required=True)
        elif name == "checkpoint":
            run.add_argument("--checkpoint-id", required=True)
            run.add_argument("--computation-id", required=True)
            run.add_argument("--input-digest", required=True)
            run.add_argument("--artifact", action="append", required=True)
        elif name in ("prepare", "restore"):
            run.add_argument("--path", required=True)
            run.add_argument("--mode", choices=("worktree", "workspace"), default="worktree")
            if name == "restore":
                run.add_argument("--confirm-stopped", action="store_true")
                run.add_argument("--confirm-operations-settled", action="store_true")
        elif name == "heartbeat":
            run.add_argument("--progress")
        elif name in ("publish", "finish"):
            run.add_argument("--summary", default="")
            if name == "finish":
                run.add_argument("--outcome", choices=("succeeded", "failed", "cancelled"), required=True)
                run.add_argument("--completion-file", help="Verified terminal evidence to commit with finish; record unknown outcomes with run completion")
                run.add_argument("--discard-output", action="store_true", help="Release failed/cancelled run, preserving unadopted work")
        elif name == "recover":
            run.add_argument("--stale", action="store_true", help="Only for observed stale Git-only runs")
            run.add_argument("--confirm-stopped", action="store_true")
            run.add_argument("--confirm-operations-settled", action="store_true")
            run.add_argument("--evidence", help="Durable stop/result evidence reference, never credentials")
            run.add_argument("--stop-check", help="Absolute path of a trusted executor stop-evidence adapter")
        elif name == "reap":
            run.add_argument("--apply", action="store_true")
            run.add_argument("--stop-check", help="Optional trusted executor adapter, checked before age")
            run.add_argument("--observer-profile", choices=("unknown", "retained", "stateless"), default="unknown",
                             help="Whether this observer's monotonic-clock state survives the next invocation")
        elif name == "cleanup":
            run.add_argument("--apply", action="store_true", help="Default is a read-only plan")
            run.add_argument("--confirm-stopped", action="store_true")
            run.add_argument('--metadata-only', action='store_true', help='Retire confirmed execution bookkeeping; preserve task output')
        elif name == "exec":
            run.add_argument("argv", nargs=argparse.REMAINDER)
    return p


def dispatch(args):
    store = Store(args.repo, args.remote, args.branch, transport_adapter=args.transport_adapter)
    if args.command == "transport-check":
        return store.preflight()
    operation = identity(args.operation_id or uuid.uuid4().hex, "operation ID")
    # Always expose a generated request identity before a remote mutation, so
    # an interrupted CLI can be retried without guessing another request ID.
    mutation = args.command == "init" or args.action in ("add", "edit", "start", "publish", "finish", "recover", "cleanup", "activate", "recovery-policy", "renew", "checkpoint", "deliver-checkpoint", "heartbeat", "completion", 'send', 'complete', 'wake', 'emit', 'enable', 'subscribe', 'put', 'sync')
    if mutation:
        print(json.dumps({"operation_id": operation}), file=sys.stderr, flush=True)
    if args.command == "init":
        return runtime.initialize(store, operation, index=args.index, task_root=args.task_root)
    if args.command == 'signal':
        if args.action == 'enable':
            return signals.enable(store, operation)
        if args.action == 'list':
            return {'signals': signals.catalog(store.snapshot(), args.task, include_ended=args.include_ended)}
        if args.action == 'put':
            return signals.put(store, args.task, args.run_id, json.loads(Path(args.file).read_text()))
        if args.action == 'sync':
            return signals.sync(store, task_id=args.task)
        return signals.subscribe(store, operation, args.task, args.consumer)
    if args.command == 'io':
        if args.action == 'send':
            return task_io.send(store, operation, args.task, args.message_id, args.kind,
                                json.loads(Path(args.payload_file).read_text()))
        if args.action == 'pending':
            return task_io.pending(store, args.task)
        if args.action == 'status':
            return task_io.status(store, args.task, args.message_id)
        if args.action == 'complete':
            return task_io.complete(store, operation, args.task, args.run_id, args.message_id,
                                  json.loads(Path(args.result_file).read_text()))
        if args.action == 'emit':
            return task_io.emit(store, operation, args.task, args.run_id, args.event_id, args.kind,
                                json.loads(Path(args.payload_file).read_text()))
        return task_io.wake(store, operation, args.task, args.runner, args.run_id, force=args.force)
    if args.command == "task":
        if args.action == "recovery-policy":
            return leases.configure(store, operation, args.id, args.expected_revision, mode=args.mode,
                                    profile=args.profile, drained=args.confirm_writers_drained)
        if args.action == "list":
            return runtime.list_tasks(store, args.all)
        if args.action == "show":
            snapshot = store.snapshot()
            task, active = runtime.load_task(snapshot, args.id)
            return {"task": task, "runtime": active, "commit": snapshot.commit,
                    "body": snapshot.read(runtime.root(args.id, snapshot) + "/task.md").decode()}
        body = Path(args.body_file).read_text() if args.body_file else None
        if args.action == "add":
            return runtime.add_task(store, operation, title=args.title, task_id=args.id, body=body or "",
                max_runners=args.max_runners, recovery_mode=args.recovery_mode,
                heartbeat_seconds=args.heartbeat_seconds, stale_seconds=args.stale_seconds, priority=args.priority, path=args.path, data_path=args.data_path)
        return runtime.edit_task(store, operation, args.id, args.expected_revision, title=args.title,
                                 body=body, status=args.status, archived=args.archived,
                                 max_runners=args.max_runners, priority=args.priority)
    task_id, run_id = args.task, args.run_id
    if args.action == "report":
        return reports.inventory(store, task_id, run_id)
    if args.action == "completion":
        return completion.record(store, operation, task_id, run_id, json.loads(Path(args.record_file).read_text()))
    if args.action == "start":
        return runtime.start(store, operation, task_id, args.runner, run_id=run_id,
                             machine_id=args.machine, item_key=args.item_key,
                             resume_from=args.resume_from, checkpoint_id=args.checkpoint_id,
                             completion_contract=json.loads(Path(args.completion_contract_file).read_text()) if args.completion_contract_file else None)
    if args.action == "renew":
        return leases.renew(store, operation, task_id, run_id)
    if args.action == "checkpoint":
        return leases.checkpoint(store, operation, task_id, run_id, args.checkpoint_id,
                                 args.computation_id, args.input_digest, args.artifact)
    if args.action == "deliver-checkpoint":
        return leases.deliver(store, operation, task_id, run_id)
    if args.action == "show":
        return runtime.load_run(store.snapshot(), task_id, run_id)[2]
    if args.action == "check":
        return runtime.check(store, task_id, run_id)
    if args.action in ("prepare", "restore"):
        return runtime.prepare(store, task_id, run_id, args.path, args.mode, restore=args.action == "restore",
            stopped=getattr(args, "confirm_stopped", False), settled=getattr(args, "confirm_operations_settled", False))
    if args.action == "heartbeat":
        return runtime.heartbeat(store, task_id, run_id, progress=args.progress, operation_id=operation)
    if args.action in ("publish", "finish"):
        return runtime.publish(store, operation, task_id, run_id,
                               outcome=getattr(args, "outcome", None), summary=args.summary,
                               discard_output=getattr(args, "discard_output", False),
                               completion_record=json.loads(Path(args.completion_file).read_text()) if getattr(args, 'completion_file', None) else None)
    if args.action == "observe":
        return runtime.observe(store, task_id, run_id)
    if args.action == "recover":
        if args.stop_check:
            return lifecycle.reconcile_stop(store, operation, task_id, run_id, args.stop_check)
        return runtime.recover(store, operation, task_id, run_id, stopped=args.confirm_stopped,
            settled=args.confirm_operations_settled, evidence=args.evidence, stale=args.stale)
    if args.action == "reap":
        return lifecycle.reap(store, task_id, apply=args.apply, stop_check=args.stop_check,
                              observer_profile=args.observer_profile)
    if args.action == "cleanup":
        return lifecycle.cleanup(store, operation, task_id, run_id, apply=args.apply, stopped=args.confirm_stopped,
                                 metadata_only=args.metadata_only)
    if args.action == "exec":
        argv = args.argv[1:] if args.argv and args.argv[0] == "--" else args.argv
        return lifecycle.execute(store, task_id, run_id, argv)
    raise TaskError("Unknown action")


def main(argv=None):
    try:
        args = parser().parse_args(argv)
        result = dispatch(args)
        if args.command == 'run' and args.action == 'report' and args.format == 'markdown':
            sys.stdout.buffer.write(reports.markdown(result).encode('utf-8'))
        else:
            sys.stdout.buffer.write(encode(result))
        if result.get("needs_attention"):
            return 2
        return 1 if result.get("phase") == "exited" and result.get("exit_code") else 0
    except (TaskError, GitFailure) as error:
        diagnostic = {"error": str(error)}
        if hasattr(error, "code"):
            diagnostic.update(code=error.code, operation=error.operation, missing_capabilities=error.missing)
        print(json.dumps(diagnostic), file=sys.stderr)
        return 1
    except (OSError, ValueError, KeyError, TypeError):
        # Raw filesystem/JSON errors can include data or credentials.
        print(json.dumps({"error": "Invalid local state, input, or filesystem operation; preserve run state"}), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
