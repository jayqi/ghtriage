# ghtriage: GitHub Triage Data Warehouse

## Context

We need a CLI tool that pulls GitHub issue, PR, and comment data into a local DuckDB database so a coding agent can query it for triage and project management. The tool lives in an existing skeleton repo with `pyproject.toml` (uv_build, Python ≥3.13) and a placeholder `src/ghtriage/__init__.py`.

## Approach

Use **dlt's built-in REST API source** (`dlt.sources.rest_api.rest_api_source`) with a declarative config dict targeting GitHub's REST API. This ships with the `dlt` pip package — no vendoring. dlt handles pagination, auth, incremental loading, and schema management. Their own docstring uses GitHub as the canonical example (see lines 157-200 of `dlt/sources/rest_api/__init__.py`).

Single dependency: `dlt[duckdb]>=1.0,<2`

## CLI Interface

```
ghtriage pull [--repo OWNER/REPO] [--full]
ghtriage query "SQL statement" [--format table|csv|json]
ghtriage schema [--table TABLE_NAME]
```

- `pull`: Fetch data from GitHub into local DuckDB. Auto-detects repo from git remote; `--repo` overrides. `--full` forces complete re-fetch (drops incremental state).
- `query`: Execute raw SQL. Default format is `table`; also supports `csv` and `json` (JSONL). Sets DuckDB schema to `github` so unqualified table names work.
- `schema`: No args lists tables; `--table X` shows columns and types.

## Storage Layout

```
.ghtriage/
├── config.toml          # Committable config: [repo] default, etc.
├── .env                 # Gitignored: GITHUB_TOKEN=ghp_xxx
├── ghtriage.duckdb      # DuckDB database
└── pipelines/           # dlt pipeline state (incremental cursors)
```

Secrets (`.env`, `*.duckdb`) are separated from config (`config.toml`) so that config can be committed while secrets stay gitignored.

## Auth Resolution Order

1. `GITHUB_TOKEN` env var (standard, works with `gh auth token`)
2. `.ghtriage/.env` file → `GITHUB_TOKEN=...` (simple key=value parsing, no `python-dotenv` dependency needed)
3. Error with instructions if neither found

## Module Structure

```
src/ghtriage/
├── __init__.py      # main() entrypoint → calls cli.run()
├── cli.py           # argparse subcommands, output formatting, dispatch
├── config.py        # Token resolution, git remote parsing, .ghtriage/ dir mgmt
├── pipeline.py      # dlt REST API source config, pipeline creation, run_pull()
└── query.py         # DuckDB query execution, schema introspection
```

## Data Pipeline (`pipeline.py`)

Declarative REST API source config covering four GitHub endpoints:

| Resource | Endpoint | Incremental | Parent |
|----------|----------|-------------|--------|
| `issues` | `GET /repos/{o}/{r}/issues?state=all&sort=updated` | `updated_at` cursor, `since` param | — |
| `pulls` | `GET /repos/{o}/{r}/pulls?state=all&sort=updated` | `updated_at` cursor | — |
| `issue_comments` | `GET /repos/{o}/{r}/issues/comments?sort=updated` | `updated_at` cursor, `since` param | — |
| `pull_comments` | `GET /repos/{o}/{r}/pulls/comments?sort=updated` | `updated_at` cursor, `since` param | — |

**Key decision**: Use repo-level comment endpoints (not per-issue/per-PR child endpoints) to avoid N+1 API calls. The repo-level endpoints return all comments in a single paginated stream with `since` for incremental loading. Comments have an `issue_url` field that links back to the parent issue/PR.

Write disposition: `merge` (upsert on `id` primary key) so updates to existing items are reflected.

dlt will auto-unnest nested arrays into child tables (e.g., `issues__labels`, `issues__assignees`, `pulls__requested_reviewers`).

## Key Config Details

- Pipeline `pipelines_dir` set to `.ghtriage/pipelines/` to keep state local
- `dataset_name="github"` so tables live in a `github` schema
- `query.py` runs `SET schema = 'github'` before user SQL so unqualified names work
- Git remote parsing handles both SSH (`git@github.com:o/r.git`) and HTTPS formats

## Phase 1: Core Pipeline

Goal: Get `ghtriage pull` working end-to-end and validate that data lands correctly in DuckDB. This is the riskiest phase — the dlt REST API source config may need iteration.

### Steps

1. **`pyproject.toml`** — add `dlt[duckdb]>=1.0,<2` dependency
2. **`src/ghtriage/config.py`** — `resolve_token()`, `resolve_repo()`, `parse_git_remote()`, `get_ghtriage_dir()`, `get_db_path()`, `get_pipelines_dir()`, `.env` file parsing
3. **`src/ghtriage/pipeline.py`** — REST API source config dict, `create_pipeline()`, `run_pull()`
4. **`src/ghtriage/cli.py`** — argparse with `pull` subcommand only (+ stub `query`/`schema`)
5. **`src/ghtriage/__init__.py`** — wire `main()` to `cli.run()`

### Validate

```
uv run ghtriage pull --repo drivendataorg/cloudpathlib
```

Then inspect the DuckDB file directly to understand what dlt actually produced — table names, column names, child tables. This informs Phase 2.

---

## Phase 2: Query Interface + Polish

Goal: Build the query and schema commands on top of the confirmed data model from Phase 1.

### Steps

1. **`src/ghtriage/query.py`** — `execute_query()`, `get_tables()`, `get_table_columns()`
2. **Wire `query` and `schema` subcommands** in `cli.py`
3. **Output formatting** — table (column-aligned), csv (`csv.writer`), json (JSONL) for `query` command
4. **Error handling** — clear messages for missing token, bad repo, no DB, SQL errors
5. **`--full` flag** — implement pipeline state deletion for full refresh
6. **Incremental pull validation** — run a second pull and confirm it's faster

### Validate

```
uv run ghtriage schema
uv run ghtriage schema --table issues
uv run ghtriage query "SELECT number, title, state FROM issues WHERE state='open' LIMIT 5"
uv run ghtriage query "SELECT count(*) FROM issues" --format json
uv run ghtriage pull --repo drivendataorg/cloudpathlib   # second run, should be incremental
```

## Files Modified

- `pyproject.toml` (add dependency) — Phase 1
- `src/ghtriage/__init__.py` (update `main()`) — Phase 1
- `src/ghtriage/config.py` (new) — Phase 1
- `src/ghtriage/pipeline.py` (new) — Phase 1
- `src/ghtriage/cli.py` (new) — Phase 1, extended in Phase 2
- `src/ghtriage/query.py` (new) — Phase 2
