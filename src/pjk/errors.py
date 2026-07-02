# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

from __future__ import annotations

from typing import List, Optional

import yaml

CLI_PROBLEM_HEADER = "You've got a problem here."


def _quote_token(token: str) -> str:
    if token.startswith('[') or token.startswith('{'):
        return '"' + token + '"'
    return token


def format_cli_command(tokens: List[str], token_no: int = 0, marker: str = '^') -> List[str]:
    """Return command line + caret underline for CLI errors."""
    if not tokens:
        return []
    token_copies = [_quote_token(t) for t in tokens]
    lines = ['pjk ' + ' '.join(token_copies)]
    offset = 4 + sum(len(t) + 1 for t in token_copies[:token_no])
    lines.append(' ' * offset + marker * len(token_copies[token_no]))
    return lines


def find_token_index(tokens: List[str], token: str) -> int:
    """Locate the failing pipe token in argv tokens."""
    for i, t in enumerate(tokens):
        if t == token:
            return i
    for i, t in enumerate(tokens):
        if token in t or t in token:
            return i
    return 0


class RecordProcessingError(Exception):
    """Per-record pipeline failure with CLI-friendly formatting."""

    def __init__(
        self,
        *,
        token: str,
        record: dict,
        cause: BaseException,
        message: Optional[str] = None,
        tokens: Optional[List[str]] = None,
        token_no: int = 0,
    ):
        super().__init__(message or f"Error in {token}")
        self.token = token
        self.record = record
        self.cause = cause
        self.message = message or f"Error in {token}"
        self.tokens = tokens
        self.token_no = token_no

    def __str__(self) -> str:
        lines = []
        if self.tokens:
            lines.extend(format_cli_command(self.tokens, self.token_no))
        lines.append(CLI_PROBLEM_HEADER)
        lines.append('')
        lines.append(str(self.cause))
        lines.append('')
        lines.append('On this record:')
        lines.append(
            yaml.dump(
                self.record,
                default_flow_style=False,
                sort_keys=False,
                allow_unicode=True,
            ).rstrip()
        )
        return '\n'.join(lines)
