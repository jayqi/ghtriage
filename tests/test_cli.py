import csv
import io
import json
from pathlib import Path

import duckdb
import pytest

from ghtriage.cli import run


@pytest.fixture
def sample_cwd(tmp_path: Path) -> Path:
    db_path = tmp_path / ".ghtriage" / "ghtriage.duckdb"
    db_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db_path))
    con.execute("CREATE SCHEMA github")
    con.execute("CREATE TABLE github.issues (id BIGINT, title VARCHAR, state VARCHAR)")
    con.execute("INSERT INTO github.issues VALUES (1, 'First', 'open'), (2, 'Second', 'closed')")
    con.close()

    return tmp_path


def test_query_table_format_success(sample_cwd: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(sample_cwd)

    rc = run(["query", "SELECT id, title FROM issues ORDER BY id", "--format", "table"])

    captured = capsys.readouterr()
    assert rc == 0
    assert "id" in captured.out
    assert "title" in captured.out
    assert "First" in captured.out
    assert captured.err == ""


def test_query_csv_format_success(sample_cwd: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(sample_cwd)

    rc = run(["query", "SELECT id, title FROM issues ORDER BY id", "--format", "csv"])

    captured = capsys.readouterr()
    rows = list(csv.reader(io.StringIO(captured.out)))

    assert rc == 0
    assert rows[0] == ["id", "title"]
    assert rows[1] == ["1", "First"]
    assert rows[2] == ["2", "Second"]
    assert captured.err == ""


def test_query_json_format_is_strict_jsonl(sample_cwd: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(sample_cwd)

    rc = run(["query", "SELECT id, state FROM issues ORDER BY id", "--format", "json"])

    captured = capsys.readouterr()
    lines = [line for line in captured.out.splitlines() if line.strip()]
    payloads = [json.loads(line) for line in lines]

    assert rc == 0
    assert payloads == [{"id": 1, "state": "open"}, {"id": 2, "state": "closed"}]
    assert captured.err == ""


def test_query_returns_runtime_error_for_missing_db(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)

    rc = run(["query", "SELECT 1"])

    captured = capsys.readouterr()
    assert rc == 1
    assert "Database not found" in captured.err


def test_query_returns_runtime_error_for_bad_sql(sample_cwd: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(sample_cwd)

    rc = run(["query", "SELEC id FROM issues"])

    captured = capsys.readouterr()
    assert rc == 1
    assert captured.err


def test_schema_lists_user_tables(sample_cwd: Path, monkeypatch, capsys) -> None:
    db_path = sample_cwd / ".ghtriage" / "ghtriage.duckdb"
    con = duckdb.connect(str(db_path))
    con.execute("CREATE TABLE github._dlt_loads (load_id VARCHAR)")
    con.close()

    monkeypatch.chdir(sample_cwd)

    rc = run(["schema"])

    captured = capsys.readouterr()
    assert rc == 0
    assert "issues" in captured.out.splitlines()
    assert "_dlt_loads" not in captured.out


def test_schema_table_details(sample_cwd: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(sample_cwd)

    rc = run(["schema", "--table", "issues"])

    captured = capsys.readouterr()
    assert rc == 0
    assert "id" in captured.out
    assert "BIGINT" in captured.out


def test_schema_unknown_table_returns_runtime_error(
    sample_cwd: Path, monkeypatch, capsys
) -> None:
    monkeypatch.chdir(sample_cwd)

    rc = run(["schema", "--table", "missing"])

    captured = capsys.readouterr()
    assert rc == 1
    assert "Table not found" in captured.err
