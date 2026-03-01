import os
from pathlib import Path
import re
import subprocess
import textwrap

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - exercised on Python <3.11
    import tomli as tomllib

REPO_SLUG_PATTERN = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
LOCAL_GITIGNORE_CONTENT = textwrap.dedent(
    """\
    *
    !.gitignore
    !config.toml
    """
)


def _ensure_local_gitignore(ghtriage_dir: Path) -> None:
    gitignore_path = ghtriage_dir / ".gitignore"
    if gitignore_path.exists():
        return
    gitignore_path.write_text(LOCAL_GITIGNORE_CONTENT, encoding="utf-8")


def get_ghtriage_dir(cwd: str | Path | None = None, create: bool = True) -> Path:
    root = Path(cwd) if cwd is not None else Path.cwd()
    ghtriage_dir = root / ".ghtriage"
    if create:
        ghtriage_dir.mkdir(parents=True, exist_ok=True)
        _ensure_local_gitignore(ghtriage_dir)
    return ghtriage_dir


def get_db_path(cwd: str | Path | None = None) -> Path:
    return get_ghtriage_dir(cwd=cwd) / "ghtriage.duckdb"


def get_pipelines_dir(cwd: str | Path | None = None) -> Path:
    return get_ghtriage_dir(cwd=cwd) / "pipelines"


def parse_git_remote(remote_url: str) -> str:
    remote_url = remote_url.strip()
    patterns = (
        re.compile(r"^git@github\.com:(?P<owner>[^/\s]+)/(?P<repo>[^/\s]+?)(?:\.git)?/?$"),
        re.compile(r"^https://github\.com/(?P<owner>[^/\s]+)/(?P<repo>[^/\s]+?)(?:\.git)?/?$"),
        re.compile(r"^ssh://git@github\.com/(?P<owner>[^/\s]+)/(?P<repo>[^/\s]+?)(?:\.git)?/?$"),
    )

    for pattern in patterns:
        match = pattern.match(remote_url)
        if not match:
            continue
        owner = match.group("owner")
        repo = match.group("repo")
        slug = f"{owner}/{repo}"
        if REPO_SLUG_PATTERN.fullmatch(slug):
            return slug
        raise ValueError(f"Invalid GitHub repository slug: {slug}")

    if "github.com" in remote_url:
        raise ValueError(f"Unsupported GitHub remote URL format: {remote_url}")
    raise ValueError(f"Remote is not a GitHub URL: {remote_url}")


def _read_token_file(path: Path) -> str | None:
    if not path.exists():
        return None
    token = path.read_text(encoding="utf-8").strip()
    return token or None


def resolve_token(cwd: str | Path | None = None, env: dict[str, str] | None = None) -> str:
    env_data = env if env is not None else os.environ
    token = env_data.get("GITHUB_TOKEN")
    if token:
        return token

    ghtriage_dir = get_ghtriage_dir(cwd=cwd, create=False)
    token = _read_token_file(ghtriage_dir / "token")
    if token:
        return token

    raise RuntimeError(
        "Missing GitHub token. Set GITHUB_TOKEN or place a token in .ghtriage/token."
    )


def _default_repo_from_config(config_path: Path) -> str | None:
    if not config_path.exists():
        return None
    try:
        with config_path.open("rb") as file_obj:
            config_data = tomllib.load(file_obj)
    except tomllib.TOMLDecodeError as exc:
        raise RuntimeError(f"Invalid TOML in {config_path}: {exc}") from exc

    repo_data = config_data.get("repo")
    if not isinstance(repo_data, dict):
        return None
    default_repo = repo_data.get("default")
    if default_repo is None:
        return None
    if not isinstance(default_repo, str):
        raise RuntimeError(f"Invalid [repo].default in {config_path}: expected a string")
    return default_repo.strip()


def _validate_repo_slug(repo: str) -> str:
    repo = repo.strip()
    if REPO_SLUG_PATTERN.fullmatch(repo):
        return repo
    raise ValueError(f"Repository must be in OWNER/REPO format, got: {repo}")


def get_git_remote_origin(cwd: str | Path | None = None) -> str:
    proc = subprocess.run(
        ["git", "remote", "get-url", "origin"],
        cwd=str(cwd) if cwd is not None else None,
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        stderr = proc.stderr.strip()
        raise RuntimeError(f"Could not determine git origin remote: {stderr or 'unknown error'}")
    remote = proc.stdout.strip()
    if not remote:
        raise RuntimeError("Git origin remote is empty")
    return remote


def resolve_repo(cli_repo: str | None = None, cwd: str | Path | None = None) -> str:
    if cli_repo:
        return _validate_repo_slug(cli_repo)

    config_repo = _default_repo_from_config(
        get_ghtriage_dir(cwd=cwd, create=False) / "config.toml"
    )
    if config_repo:
        return _validate_repo_slug(config_repo)

    remote = get_git_remote_origin(cwd=cwd)
    return parse_git_remote(remote)
