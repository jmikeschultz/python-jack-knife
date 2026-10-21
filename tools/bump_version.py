#!/usr/bin/env python3
"""
Bump the version in src/pjk/version.py, commit, and tag it.

Usage:
    python tools/bump_version.py 0.6.0
"""

import argparse
import re
import pathlib
import subprocess

VERSION_FILE = pathlib.Path(__file__).parent.parent / "src/pjk/version.py"
TAG_PREFIX = "v"  # so tags look like v0.6.0


def release_version(value: str) -> str:
    if not re.fullmatch(
        r"\d+\.\d+\.\d+(?:(?:a|b|rc)\d+)?(?:\.post\d+)?(?:\.dev\d+)?",
        value,
    ):
        raise argparse.ArgumentTypeError(
            "expected a version such as 0.8.0, 0.8.0rc1, or 0.8.0.dev1"
        )
    return value


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
    parser = argparse.ArgumentParser(
        description="Update src/pjk/version.py, commit the change, and create a local version tag."
    )
    parser.add_argument("new_version", type=release_version, metavar="NEW_VERSION")
    new_version = parser.parse_args().new_version

    set_version(new_version)
    print(f"✅ Updated {VERSION_FILE} to {new_version}")

    git("add", str(VERSION_FILE))
    git("commit", "-m", f"Bump version to {new_version}")
    git("tag", f"{TAG_PREFIX}{new_version}")
    print(f"✅ Committed and tagged v{new_version}")


if __name__ == "__main__":
    main()
