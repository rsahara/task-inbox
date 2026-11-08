# Contributing

Use English for issues, pull requests, code, comments, and documentation.
Original quotations and multilingual test inputs may keep their source language.

## Development setup

You need Git and Python 3.10+ on Linux, macOS, or WSL. The runtime and tests use
the Python standard library; no package installation is required. Coverage.py is
optional; see [Coverage](#coverage) below.

Start with the [README](README.md) and the relevant specifications in `docs/`.
Runtime code lives in `foundation/task_runtime/`, commands in `bin/`, and
integration tests and fixtures in `test/`.

## Making changes

- Keep changes focused and follow the surrounding code style.
- Keep the runtime portable and independent of any account, hosted service,
  scheduler, or task. Put environment-specific behavior in caller integrations
  or adapters.
- Update the relevant documentation and regression tests when behavior changes.
  For concurrency, retry, or recovery changes, exercise the affected failure
  conditions with temporary repositories and worker processes.
- Explain compatibility and migration requirements when changing CLI contracts,
  persisted schemas, or coordination protocols. Account for all writers sharing
  a task-data repository.
- Use synthetic examples. Keep real task data, credentials, private logs, and
  personal environment settings out of commits, issues, and test fixtures.

## Validation

Run focused tests while developing. Before submitting code changes, run the full
suite from the repository root:

```sh
python3 -W error::ResourceWarning -m unittest discover -s test -p '*test.py'
```

Tests use temporary local Git repositories and processes. They do not require
GitHub credentials or network access. For documentation-only changes, check
links, commands, and consistency with the implementation.

The full suite includes the [local worker scenarios](docs/task-runtime-scenarios.md)
for killed workers, inbox additions during processing, and concurrent callers.

## Coverage

To measure line and branch coverage, use a virtual environment so Coverage.py can
also instrument the Python subprocesses used by the CLI and process tests:

```sh
python3 -m venv /tmp/task-inbox-coverage
/tmp/task-inbox-coverage/bin/python -m pip install 'coverage>=7.10,<8'
/tmp/task-inbox-coverage/bin/python -m coverage erase
/tmp/task-inbox-coverage/bin/python -m coverage run -m unittest discover -s test -p '*test.py'
/tmp/task-inbox-coverage/bin/python -m coverage combine
/tmp/task-inbox-coverage/bin/python -m coverage report
```

The coverage configuration measures all modules in `foundation/task_runtime/`,
including files with no executed lines. Coverage.py is only needed for measurement;
the runtime and test suite otherwise use the Python standard library and Git.

## Issues and pull requests

For bugs, include the affected version or commit, operating system, Python and
Git versions, expected behavior, and a minimal reproduction with synthetic data.
For pull requests, describe the problem, resulting behavior, relevant tests and
their results, and any compatibility impact. Link a related issue when useful.

Follow [SECURITY.md](SECURITY.md) for suspected vulnerabilities.
Unless explicitly stated otherwise, contributions are provided under this
repository's [MIT License](LICENSE).
