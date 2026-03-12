# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

import os
import re
import shlex
from typing import Dict, List
from pjk.usage import TokenError, UsageError

PJK_END_TOKEN = 'END'
PJK_SET_TOKEN = 'SET'

# ${VAR} or $VAR - match anywhere in token (${VAR} first to avoid partial match)
VAR_REF_PATTERN = re.compile(r'\$\{([a-zA-Z_][a-zA-Z0-9_]*)\}|\$([a-zA-Z_][a-zA-Z0-9_]*)')


def _expand_token(t: str, env: Dict[str, str]) -> str:
    """Expand $VAR or ${VAR} anywhere in token; raise if undefined."""

    def repl(m):
        name = m.group(1) or m.group(2)
        if name not in env:
            raise TokenError(f"Undefined variable: ${name}")
        return env[name]

    return VAR_REF_PATTERN.sub(repl, t)


def handle_pjk_file(token: str, expanded: List[str]):
    if not token.endswith(".pjk"):
        return False

    if not os.path.isfile(token):
        raise TokenError(f"pjk file not found: {token}")

    with open(token, "r") as f:
        lines = f.readlines()

    env: Dict[str, str] = {}

    for line in lines:
        try:
            parts = shlex.split(line, comments=True, posix=True)
        except ValueError as e:
            raise UsageError(f"Error parsing {token}: {e}")

        if not parts:
            continue
        if parts[0] == PJK_END_TOKEN:
            break

        if parts[0] == PJK_SET_TOKEN:
            for p in parts[1:]:
                if '=' in p:
                    k, v = p.split('=', 1)
                    env[k.strip()] = v.strip()
            continue

        for p in parts:
            if p == PJK_END_TOKEN:
                break
            expanded.append(_expand_token(p, env))
        else:
            continue
        break
    return True
