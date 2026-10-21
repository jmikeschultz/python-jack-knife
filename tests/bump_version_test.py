import subprocess
import sys
from pathlib import Path

import pytest


SCRIPT = Path(__file__).resolve().parents[1] / "tools" / "bump_version.py"


@pytest.mark.parametrize("args,code", [
    (["--help"], 0),
    (["-h"], 0),
    ([], 2),
    (["garbage"], 2),
    (["--version"], 2),
    (["0.8.0", "extra"], 2),
])
def test_help_and_invalid_arguments_have_no_side_effects(tmp_path, args, code):
    tools = tmp_path / "tools"
    tools.mkdir()
    script = tools / SCRIPT.name
    script.write_bytes(SCRIPT.read_bytes())
    version = tmp_path / "src" / "pjk" / "version.py"
    version.parent.mkdir(parents=True)
    original = '__version__ = "0.7.7"\n'
    version.write_text(original)

    # No Git repository: any attempt to commit would also fail this assertion.
    result = subprocess.run(
        [sys.executable, str(script), *args], cwd=tmp_path,
        capture_output=True, text=True,
    )
    assert result.returncode == code
    assert version.read_text() == original
    assert "Traceback" not in result.stderr
    if code == 0:
        assert "NEW_VERSION" in result.stdout


@pytest.mark.parametrize("version", ["0.8.0", "0.8.0rc1", "0.8.0.dev1"])
def test_valid_version_updates_commits_and_tags(tmp_path, version):
    tools = tmp_path / "tools"
    tools.mkdir()
    script = tools / SCRIPT.name
    script.write_bytes(SCRIPT.read_bytes())
    version_file = tmp_path / "src" / "pjk" / "version.py"
    version_file.parent.mkdir(parents=True)
    version_file.write_text('__version__ = "0.7.7"\n')

    def git(*args):
        return subprocess.check_output([
            "git", "-c", "user.name=Test", "-c", "user.email=test@example.com",
            "-c", "commit.gpgsign=false", *args,
        ], cwd=tmp_path, text=True)

    git("init", "-q")
    git("config", "user.name", "Test")
    git("config", "user.email", "test@example.com")
    git("config", "commit.gpgsign", "false")
    git("add", ".")
    git("commit", "-qm", "Initial version")
    subprocess.run([sys.executable, str(script), version], cwd=tmp_path, check=True)

    assert version_file.read_text() == f'__version__ = "{version}"\n'
    assert git("log", "-1", "--format=%s").strip() == f"Bump version to {version}"
    assert git("rev-parse", f"v{version}") == git("rev-parse", "HEAD")
