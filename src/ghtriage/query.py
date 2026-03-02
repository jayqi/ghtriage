from dataclasses import dataclass, field
from pathlib import Path

import duckdb

from ghtriage.config import get_db_path

_MAIN_TABLES = ("issues", "pulls", "issue_comments", "pull_comments")


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


@dataclass
class StatusData:
    db_path: Path
    db_size_bytes: int
    db_repo: str | None
    last_pull_at: str | None
    last_full_pull: bool | None
    table_stats: list[tuple[str, int, str | None]] = field(default_factory=list)


def get_status_data(cwd: str | Path | None = None) -> StatusData:
    db_path = _resolve_db_path(cwd=cwd)
    db_size_bytes = db_path.stat().st_size

    with duckdb.connect(str(db_path)) as conn:
        db_repo = None
        last_pull_at = None
        last_full_pull = None
        try:
            rows = conn.execute("SELECT key, value FROM github._ghtriage_meta").fetchall()
            meta = dict(rows)
            db_repo = meta.get("repo")
            last_pull_at = meta.get("last_pull_at")
            if (raw := meta.get("last_full_pull")) is not None:
                last_full_pull = raw == "true"
        except Exception:
            pass

        table_stats = []
        for table in _MAIN_TABLES:
            try:
                row = conn.execute(
                    f"SELECT COUNT(*), MAX(updated_at) FROM github.{table}"  # noqa: S608
                ).fetchone()
                count = row[0] or 0
                max_updated_at = str(row[1])[:19] if row[1] is not None else None
                table_stats.append((table, count, max_updated_at))
            except Exception:
                pass

    return StatusData(
        db_path=db_path,
        db_size_bytes=db_size_bytes,
        db_repo=db_repo,
        last_pull_at=last_pull_at,
        last_full_pull=last_full_pull,
        table_stats=table_stats,
    )
