# Security Policy

## Maintenance scope

Security fixes are developed against the current default branch. Older snapshots
and downstream forks do not have a separate backport or maintenance commitment.
Consumers who copy or customize the source are responsible for incorporating
applicable updates. There is no guaranteed response time or release schedule.

## Reporting a vulnerability

Use **Report a vulnerability** in the repository's
[Security tab](https://github.com/rsahara/task-inbox/security) when available.
If private reporting is unavailable, open an issue asking only for a private
contact channel. Keep vulnerability details, exploit code, credentials, and
sensitive task data out of public issues and pull requests.

In the private report, include the affected commit or version, environment,
reproduction steps using synthetic data, expected and actual behavior, and the
potential impact. Redact secrets and personal information from all attachments.

## System boundaries

This policy covers the runtime in `foundation/task_runtime/` and the CLI and
distribution tools in `bin/`. Task messages, submitted output, paths, and
persisted metadata cross validation boundaries and may contain malformed data.

The runtime relies on the host operating system, Git access controls, and
operator-selected transport and executor integrations. It runs with the caller's
permissions and does not sandbox worker programs or installed adapters.
Authentication, scheduling, and external-service permissions belong to the host
integration. See the [runtime contract](docs/task-runtime.md) and
[adapter contract](docs/task-runtime-adapters.md).

Security reports are especially relevant when they demonstrate:

- Writes or deletion outside the permitted task, run, or workspace paths.
- A stale or replaced run changing current ownership or accepted results.
- Bypassing conditional updates, request identity checks, or result acceptance.
- Cleanup losing unpublished work or evidence required for recovery.
- Credentials or private worker or adapter output appearing in public diagnostics
  or published artifacts without being explicitly selected for publication.

Heartbeat expiry does not prove that a worker or its external operations have
stopped. Report violations of the documented recovery and fencing contracts;
external effects also require the caller's own reconciliation and access controls.
