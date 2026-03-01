# ghtriage

**GitHub project management and triage tool.**

This package provides a command-line interface (CLI) for:

- pulling all issue, pull request, and comment data for a GitHub repository into a local DuckDB database
- inspecting the local database schema
- querying the local database

The motivation is to provide a local snapshot of the GitHub data that an AI coding agent can cheaply query to help perform project management and triage tasks, such as identifying stale issues by their content. This data would be complemented by the actual commit history available from the local Git repository.

## Commands

```bash
ghtriage pull [--repo OWNER/REPO] [--full]
ghtriage schema [--table TABLE_NAME]
ghtriage query "SQL statement" [--format table|csv|json]
```

## Query formats

- `table`: column-aligned text output with full values.
- `csv`: header row followed by CSV rows.
- `json`: strict JSONL (one JSON object per row).

## Examples

```bash
uv run ghtriage schema
uv run ghtriage schema --table issues
uv run ghtriage query "SELECT number, title, state FROM issues LIMIT 5"
uv run ghtriage query "SELECT count(*) AS n FROM issues" --format json
```

## Exit codes

- `0`: command completed successfully.
- `1`: runtime failure (for example missing database, SQL error, unknown table).
- `2`: command usage/argument error (argparse-level failure).
