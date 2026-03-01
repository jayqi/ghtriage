import argparse
import csv
import json
import sys
from typing import Sequence

from ghtriage.config import resolve_repo, resolve_token
from ghtriage.pipeline import run_pull
from ghtriage.query import execute_query, get_table_columns, get_tables


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


def _format_table(columns: list[str], rows: list[tuple]) -> None:
    if not columns:
        return

    string_rows = [[str(value) for value in row] for row in rows]
    widths = [len(column) for column in columns]
    for row in string_rows:
        for index, value in enumerate(row):
            widths[index] = max(widths[index], len(value))

    header = " | ".join(column.ljust(widths[index]) for index, column in enumerate(columns))
    separator = "-+-".join("-" * width for width in widths)
    print(header)
    print(separator)

    for row in string_rows:
        print(" | ".join(value.ljust(widths[index]) for index, value in enumerate(row)))


def _format_csv(columns: list[str], rows: list[tuple]) -> None:
    if not columns:
        return
    writer = csv.writer(sys.stdout)
    writer.writerow(columns)
    writer.writerows(rows)


def _format_jsonl(columns: list[str], rows: list[tuple]) -> None:
    if not columns:
        return
    for row in rows:
        record = dict(zip(columns, row, strict=True))
        print(json.dumps(record, default=str))


def _run_query(args: argparse.Namespace) -> int:
    try:
        columns, rows = execute_query(args.sql)
    except Exception as exc:
        print(f"Query failed: {exc}", file=sys.stderr)
        return 1

    if args.format == "table":
        _format_table(columns, rows)
        return 0
    if args.format == "csv":
        _format_csv(columns, rows)
        return 0
    if args.format == "json":
        _format_jsonl(columns, rows)
        return 0

    print(f"Unsupported format: {args.format}", file=sys.stderr)
    return 1


def _run_schema(args: argparse.Namespace) -> int:
    try:
        if args.table:
            columns = get_table_columns(args.table)
            print("column_name | data_type | nullable")
            print("------------+-----------+---------")
            for name, data_type, nullable in columns:
                print(f"{name} | {data_type} | {nullable}")
            return 0

        tables = get_tables()
        for table in tables:
            print(table)
        return 0
    except Exception as exc:
        print(f"Schema inspection failed: {exc}", file=sys.stderr)
        return 1


def run(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "pull":
        return _run_pull(args)
    if args.command == "query":
        return _run_query(args)
    if args.command == "schema":
        return _run_schema(args)

    parser.error(f"Unknown command: {args.command}")
