# Git process ownership

`task_runtime.git_support` bounds Git execution, closes owned pipes, stops process
groups on cancellation, and reports structured diagnostics without raw remote
output or credentials. It is part of the distributable package.

Each Git command uses `maintenance.autoDetach=false` and `gc.autoDetach=false`.
Automatic maintenance stays enabled but remains a child of the owned Git command,
within the same timeout. Repository/global configuration is not changed.
Adapters and caller-owned Git commands must preserve the same contract.

An executor must reap orphaned descendants (for example Docker `--init`). This is
separate from the runtime's ownership protocol. A zombie already reparented to a
non-reaping PID 1 cannot be collected by another task. Do not kill or replace
PID 1, or release ownership merely because heartbeat stopped.

Cold-start fixture readiness is bounded separately from execution deadlines.
Process tests retain strict cancellation and exit checks on POSIX, with a Linux
subreaper probe for detached Git maintenance. A Linux pass alone does not certify
macOS cold-start behavior or an external executor's cancellation semantics.

```sh
python3 -W error::ResourceWarning -m unittest discover -s test -p git_process_test.py
```
