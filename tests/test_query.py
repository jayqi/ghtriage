from pathlib import Path
from unittest.mock import MagicMock

import duckdb
import pytest

from ghtriage.query import execute_query, get_table_columns, get_tables


def _create_sample_db(tmp_path: Path) -> Path:
    db_path = tmp_path / ".ghtriage" / "ghtriage.duckdb"
    db_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db_path))
    con.execute("CREATE SCHEMA github")
    con.execute("CREATE TABLE github.issues (id BIGINT, title VARCHAR)")
    con.execute("INSERT INTO github.issues VALUES (1, 'A'), (2, 'B')")
    con.execute("CREATE TABLE github._dlt_loads (load_id VARCHAR)")
    con.execute("CREATE TABLE github.issues__labels (issue_id BIGINT, name VARCHAR)")
    con.close()

    return db_path


def test_execute_query_uses_github_schema(tmp_path: Path) -> None:
    _create_sample_db(tmp_path)

    columns, rows = execute_query("SELECT id, title FROM issues ORDER BY id", cwd=tmp_path)

    assert columns == ["id", "title"]
    assert rows == [(1, "A"), (2, "B")]


def test_execute_query_returns_empty_when_cursor_has_no_description(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_path = tmp_path / ".ghtriage" / "ghtriage.duckdb"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    db_path.write_text("", encoding="utf-8")

    cursor = MagicMock()
    cursor.description = None

    connection = MagicMock()
    connection.execute.side_effect = [cursor, cursor]
    connection.__enter__.return_value = connection
    connection.__exit__.return_value = None

    monkeypatch.setattr("ghtriage.query.duckdb.connect", lambda _: connection)

    columns, rows = execute_query("CREATE TABLE write_probe (id BIGINT)", cwd=tmp_path)

    assert columns == []
    assert rows == []


def test_execute_query_raises_when_db_missing(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="Database not found"):
        execute_query("SELECT 1", cwd=tmp_path)


def test_execute_query_missing_db_has_no_side_effects(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="Database not found"):
        execute_query("SELECT 1", cwd=tmp_path)

    assert not (tmp_path / ".ghtriage").exists()


def test_get_tables_hides_internal_by_default(tmp_path: Path) -> None:
    _create_sample_db(tmp_path)

    assert get_tables(cwd=tmp_path) == ["issues", "issues__labels"]


def test_get_tables_can_include_internal(tmp_path: Path) -> None:
    _create_sample_db(tmp_path)

    assert get_tables(cwd=tmp_path, include_internal=True) == [
        "_dlt_loads",
        "issues",
        "issues__labels",
    ]


def test_get_table_columns_returns_column_metadata(tmp_path: Path) -> None:
    _create_sample_db(tmp_path)

    assert get_table_columns("issues", cwd=tmp_path) == [
        ("id", "BIGINT", True),
        ("title", "VARCHAR", True),
    ]


def test_get_table_columns_raises_for_missing_table(tmp_path: Path) -> None:
    _create_sample_db(tmp_path)

    with pytest.raises(ValueError, match="Table not found"):
        get_table_columns("does_not_exist", cwd=tmp_path)
