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

- `pull`: Fetch data from GitHub into local DuckDB. Auto-detects repo from git remote; `--repo` overrides. `--full` deletes the DuckDB file and pipeline state for a complete rebuild.
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

## Repo Resolution Order

1. `--repo OWNER/REPO` CLI flag (highest priority)
2. `.ghtriage/config.toml` → `[repo] default` value
3. Auto-detect from git remote origin URL

## Data Pipeline (`pipeline.py`)

Declarative REST API source config covering four GitHub endpoints:

| Resource | Endpoint | Incremental | Notes |
|----------|----------|-------------|-------|
| `issues` | `GET /repos/{o}/{r}/issues?state=all&sort=updated` | `updated_at` cursor, `since` param | Filtered: excludes rows where `pull_request` is not null |
| `pulls` | `GET /repos/{o}/{r}/pulls?state=all&sort=updated` | `updated_at` cursor (client-side only — no `since` param available) | Fetches sorted by updated desc; less efficient for incremental |
| `issue_comments` | `GET /repos/{o}/{r}/issues/comments?sort=updated` | `updated_at` cursor, `since` param | — |
| `pull_comments` | `GET /repos/{o}/{r}/pulls/comments?sort=updated` | `updated_at` cursor, `since` param | — |

**Issue/PR separation**: GitHub's `/issues` endpoint returns both issues and PRs (per GitHub docs: "GitHub's REST API considers every pull request an issue"). There is no server-side parameter to filter PRs out. The `issues` resource uses a dlt `processing_steps` filter to drop rows client-side where the `pull_request` key is present, before loading into DuckDB. This means PR data is fetched from `/issues` and discarded; the dedicated `/pulls` endpoint provides the canonical PR data. The overhead is minimal since both endpoints paginate in bulk (100 items per page).

**Repo-level comment endpoints**: Uses repo-level comment endpoints (not per-issue/per-PR) to avoid N+1 API calls. Comments have an `issue_url` field for joining back to parent items.

**Incremental limitation for `/pulls`**: The pulls endpoint has no `since` query parameter, so incremental loading relies on client-side cursor filtering after fetching. With `sort=updated&direction=desc`, recent items come first, but dlt may still paginate through all results. Acceptable for most repos; a known limitation for very large ones.

**Write disposition**: `merge` (upsert on `id` primary key) so updates to existing items are reflected.

**`--full` semantics**: Deletes `.ghtriage/ghtriage.duckdb` and `.ghtriage/pipelines/` entirely, then runs a fresh pull. This guarantees no stale rows from deleted upstream items.

dlt will auto-unnest nested arrays into child tables (e.g., `issues__labels`, `issues__assignees`, `pulls__requested_reviewers`).

## Key Config Details

- Pipeline `pipelines_dir` set to `.ghtriage/pipelines/` to keep state local
- `dataset_name="github"` so tables live in a `github` schema
- `query.py` runs `SET schema = 'github'` before user SQL so unqualified names work
- Git remote parsing handles both SSH (`git@github.com:o/r.git`) and HTTPS formats

## Testing Strategy

The codebase has two distinct kinds of logic with different testing needs:

**Pure functions (unit-testable):** `config.py` contains deterministic logic — git remote URL parsing, `.env` file parsing, token/repo resolution precedence. These are easy to test in isolation with pytest, no mocking needed beyond environment variables and temp files. This is where unit tests provide the most value.

**dlt pipeline + DuckDB queries (integration-testable, but costly):** `pipeline.py` and `query.py` interact with external systems (GitHub API, DuckDB). Real integration tests would require API credentials, network access, and take minutes to run. Mocking the GitHub API responses to test the dlt config is possible but brittle — we'd essentially be testing dlt's internals, not our code. The dlt REST API source config is declarative (a dict), so there isn't much logic to unit test.

**Recommendation:** Unit test `config.py` thoroughly with pytest. Validate the pipeline and query layer manually against a real repo (`drivendataorg/cloudpathlib`) during development. Defer pipeline integration tests — the ROI is low for a v1 and the manual validation steps catch real issues (wrong column names, missing tables, broken pagination) that mocked tests wouldn't.

**What to test in `tests/test_config.py`:**
- `parse_git_remote()` — SSH URLs, HTTPS URLs, HTTPS with `.git` suffix, non-GitHub URLs (should error), malformed URLs
- `resolve_token()` — env var present, env var missing + .env file present, both missing (should error)
- `resolve_repo()` — CLI override, config.toml default, git remote fallback, precedence order
- `.env` file parsing — handles `KEY=value`, ignores comments and blank lines, strips whitespace

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
5. **`--full` flag** — delete DuckDB file + pipeline state, then re-pull
6. **Incremental pull validation** — run a second pull and confirm it's faster
7. **Unit tests** for `config.py` — git remote parsing (SSH, HTTPS, malformed), token resolution (env var, .env file, missing), repo resolution precedence (CLI > config > git remote)

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
- `tests/test_config.py` (new) — Phase 2
