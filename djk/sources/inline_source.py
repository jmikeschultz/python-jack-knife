# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

# djk/sources/inline_source.py

import hjson
from hjson import HjsonDecodeError
from typing import Optional
from collections import OrderedDict
from djk.base import Source, TokenError, Usage

def to_builtin(obj):
    """Recursively convert OrderedDicts to dicts and lists."""
    if isinstance(obj, OrderedDict):
        return {k: to_builtin(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [to_builtin(v) for v in obj]
    else:
        return obj

class InlineSource(Source):
    @classmethod
    def usage(cls):
        usage = Usage(
            name='inline json',
            desc="e.g. \"{hello: 'world'}\" or \"[{id:1},{id:2}]\""
        )
        return usage

    def __init__(self, inline_expr):
        self.num_recs = 0

        help_msg = [
            'HJSON input is more forgiving than standard JSON.',
            'Field names do not require quotes; string values use single quotes.',
            'Shell needs double quotes around the entire expression.',
            'Examples:',
            '"{hello: \'world\'}"',
            '"{price: 34.5}"',
            '"[{id: 14}, {id: 1}]"',
        ]

        try:
            obj = hjson.loads(inline_expr)
        except HjsonDecodeError:
            raise TokenError(f'"{inline_expr}"', '<hjson line>', help_msg)

        if isinstance(obj, dict):
            self.records = [obj]
        elif isinstance(obj, list):
            self.records = obj
        else:
            raise TokenError(f'"{inline_expr}"', '<hjson line>', help_msg)

    def __iter__(self):
        for raw in self.records:
            yield to_builtin(raw)

    @classmethod
    def is_inline(cls, token):
        if len(token) < 2:
            return False
        return (token[0], token[-1]) in {('{', '}'), ('[', ']')}
