#!/usr/bin/env python3
"""
Bump the version in src/pjk/version.py, commit, and tag it.

Usage:
    python tools/bump_version.py 0.6.0
"""

import re
import sys
import pathlib
import subprocess

VERSION_FILE = pathlib.Path(__file__).parent.parent / "src/pjk/version.py"
TAG_PREFIX = "v"  # so tags look like v0.6.0


def set_version(new_version: str):
    text = VERSION_FILE.read_text()
    new_text, count = re.subn(
        r'__version__\s*=\s*["\']([^"\']+)["\']',
        f'__version__ = "{new_version}"',
        text,
    )
    if count == 0:
        raise SystemExit(f"ERROR: no __version__ line found in {VERSION_FILE}")
    VERSION_FILE.write_text(new_text)


def git(*args):
    subprocess.run(["git"] + list(args), check=True)


def main():
    if len(sys.argv) != 2:
        print("Usage: bump_version.py NEW_VERSION")
        sys.exit(1)

    new_version = sys.argv[1]

    set_version(new_version)
    print(f"✅ Updated {VERSION_FILE} to {new_version}")

    git("add", str(VERSION_FILE))
    git("commit", "-m", f"Bump version to {new_version}")
    git("tag", f"{TAG_PREFIX}{new_version}")
    print(f"✅ Committed and tagged v{new_version}")


if __name__ == "__main__":
    main()
