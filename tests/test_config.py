from pathlib import Path

import pytest

from ghtriage.config import (
    get_ghtriage_dir,
    parse_git_remote,
    resolve_repo,
    resolve_token,
)


@pytest.mark.parametrize(
    ("remote_url", "expected"),
    [
        ("git@github.com:octocat/hello-world.git", "octocat/hello-world"),
        ("git@github.com:octocat/hello-world", "octocat/hello-world"),
        ("https://github.com/octocat/hello-world.git", "octocat/hello-world"),
        ("https://github.com/octocat/hello-world", "octocat/hello-world"),
        ("ssh://git@github.com/octocat/hello-world.git", "octocat/hello-world"),
    ],
)
def test_parse_git_remote_valid(remote_url: str, expected: str) -> None:
    assert parse_git_remote(remote_url) == expected


@pytest.mark.parametrize(
    "remote_url",
    [
        "https://gitlab.com/octocat/hello-world.git",
        "git@github.com:octocat",
        "not-a-url",
    ],
)
def test_parse_git_remote_invalid(remote_url: str) -> None:
    with pytest.raises(ValueError):
        parse_git_remote(remote_url)


def test_get_ghtriage_dir_creates_local_gitignore(tmp_path: Path) -> None:
    ghtriage_dir = get_ghtriage_dir(cwd=tmp_path, create=True)
    gitignore_path = ghtriage_dir / ".gitignore"
    assert gitignore_path.exists()
    assert gitignore_path.read_text(encoding="utf-8") == "*\n!.gitignore\n!config.toml\n"


def test_resolve_token_prefers_environment_over_token_file(tmp_path: Path) -> None:
    ghtriage_dir = get_ghtriage_dir(cwd=tmp_path)
    (ghtriage_dir / "token").write_text("file-token\n", encoding="utf-8")

    assert resolve_token(cwd=tmp_path, env={"GITHUB_TOKEN": "env-token"}) == "env-token"


def test_resolve_token_reads_token_file(tmp_path: Path) -> None:
    ghtriage_dir = get_ghtriage_dir(cwd=tmp_path)
    (ghtriage_dir / "token").write_text("file-token\n", encoding="utf-8")

    assert resolve_token(cwd=tmp_path, env={}) == "file-token"


def test_resolve_token_strips_whitespace_from_token_file(tmp_path: Path) -> None:
    ghtriage_dir = get_ghtriage_dir(cwd=tmp_path)
    (ghtriage_dir / "token").write_text("  file-token  \n", encoding="utf-8")

    assert resolve_token(cwd=tmp_path, env={}) == "file-token"


def test_resolve_token_raises_when_missing(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError):
        resolve_token(cwd=tmp_path, env={})


def test_resolve_repo_precedence_cli_over_config_over_git(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ghtriage_dir = get_ghtriage_dir(cwd=tmp_path)
    (ghtriage_dir / "config.toml").write_text(
        '[repo]\ndefault = "owner/from-config"\n', encoding="utf-8"
    )

    monkeypatch.setattr(
        "ghtriage.config.get_git_remote_origin",
        lambda cwd=None: "git@github.com:owner/from-git.git",
    )

    assert resolve_repo(cli_repo="owner/from-cli", cwd=tmp_path) == "owner/from-cli"
    assert resolve_repo(cwd=tmp_path) == "owner/from-config"


def test_resolve_repo_falls_back_to_git_remote(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "ghtriage.config.get_git_remote_origin",
        lambda cwd=None: "https://github.com/owner/from-git.git",
    )
    assert resolve_repo(cwd=tmp_path) == "owner/from-git"


def test_resolve_repo_raises_on_invalid_config_toml(tmp_path: Path) -> None:
    ghtriage_dir = get_ghtriage_dir(cwd=tmp_path)
    (ghtriage_dir / "config.toml").write_text("[repo\n", encoding="utf-8")

    with pytest.raises(RuntimeError):
        resolve_repo(cwd=tmp_path)
