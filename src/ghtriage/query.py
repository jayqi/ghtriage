from pathlib import Path

import duckdb

from ghtriage.config import get_db_path


def _resolve_db_path(cwd: str | Path | None = None) -> Path:
    db_path = get_db_path(cwd=cwd, create=False)
    if not db_path.exists():
        raise RuntimeError(
            f"Database not found at {db_path}. Run `ghtriage pull` to create it first."
        )
    return db_path


def execute_query(sql: str, cwd: str | Path | None = None) -> tuple[list[str], list[tuple]]:
    db_path = _resolve_db_path(cwd=cwd)
    with duckdb.connect(str(db_path)) as conn:
        conn.execute("SET schema = 'github'")
        cursor = conn.execute(sql)

        if cursor.description is None:
            return [], []

        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        return columns, rows


def get_tables(
    cwd: str | Path | None = None,
    *,
    include_internal: bool = False,
) -> list[str]:
    db_path = _resolve_db_path(cwd=cwd)
    with duckdb.connect(str(db_path)) as conn:
        rows = conn.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'github'
            ORDER BY table_name
            """
        ).fetchall()

    tables = [row[0] for row in rows]
    if include_internal:
        return tables
    return [table for table in tables if not table.startswith("_dlt_")]


def get_table_columns(
    table_name: str,
    cwd: str | Path | None = None,
) -> list[tuple[str, str, bool]]:
    db_path = _resolve_db_path(cwd=cwd)
    with duckdb.connect(str(db_path)) as conn:
        rows = conn.execute(
            """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'github' AND table_name = ?
            ORDER BY ordinal_position
            """,
            [table_name],
        ).fetchall()

    if not rows:
        raise ValueError(f"Table not found in github schema: {table_name}")

    return [(name, data_type, is_nullable == "YES") for name, data_type, is_nullable in rows]
