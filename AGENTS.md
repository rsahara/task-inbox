# Repository language

Use English by default for repository content, including code, comments,
documentation, CLI messages, commit messages, issues, and pull requests.
Preserve original text when quoting sources or testing multilingual input.
Conversation may use the user's preferred language.

# Task and caller boundaries

Follow [docs/task-execution.md](docs/task-execution.md). Tasks declare handled
inbox kinds and read/written signal types, and own their business outputs without
depending on subscribers or execution hosts. Keep readiness mechanics and bounded
continuation in the common entrypoint; keep application-specific eligibility in
trusted adapters. Task-owned prompts and preferred model/effort belong with task
definitions; callers apply them to actual workers. Host provisioning, credentials,
triggers and schedules belong to caller integrations. Never launch a subscriber from a task's signal
publication or business completion hook.

# Repository content boundaries

Keep this repository focused on the reusable task-inbox tool. Do not include
actual task definitions, instructions, payloads, outputs, or execution logs from
personal or company workflows. Store those in separate task-data repositories.

Do not include personal information, credentials, or private environment settings
in repository files, commit messages, issues, or pull requests. Use synthetic
examples in documentation and tests; preserve multilingual source text only when
it contains no personal or private data.
