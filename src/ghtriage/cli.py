import argparse
from typing import Sequence

from ghtriage.config import resolve_repo, resolve_token
from ghtriage.pipeline import run_pull


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ghtriage")
    subparsers = parser.add_subparsers(dest="command", required=True)

    pull_parser = subparsers.add_parser("pull", help="Pull GitHub data into local DuckDB")
    pull_parser.add_argument("--repo", help="GitHub repository in OWNER/REPO format")
    pull_parser.add_argument(
        "--full",
        action="store_true",
        help="Delete local DB and pipeline state before pulling",
    )

    query_parser = subparsers.add_parser("query", help="Run SQL against local DuckDB")
    query_parser.add_argument("sql", help="SQL statement")
    query_parser.add_argument(
        "--format",
        choices=("table", "csv", "json"),
        default="table",
        help="Output format",
    )

    schema_parser = subparsers.add_parser("schema", help="Inspect schema")
    schema_parser.add_argument("--table", help="Table name")

    return parser


def _run_pull(args: argparse.Namespace) -> int:
    repo = resolve_repo(cli_repo=args.repo)
    token = resolve_token()
    load_info = run_pull(repo=repo, token=token, full=args.full)
    print(f"Pull completed for {repo}")
    print(load_info)
    return 0


def _not_implemented(command_name: str) -> int:
    print(f"`ghtriage {command_name}` is not implemented yet (planned for Phase 2).")
    return 2


def run(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "pull":
        return _run_pull(args)
    if args.command == "query":
        return _not_implemented("query")
    if args.command == "schema":
        return _not_implemented("schema")

    parser.error(f"Unknown command: {args.command}")
