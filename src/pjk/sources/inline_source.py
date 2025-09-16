# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

# djk/sources/inline_source.py

import hjson
from hjson import HjsonDecodeError
from typing import Optional
from collections import OrderedDict
from pjk.base import Source, TokenError, Usage

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
            name='inline',
            desc="simplified json lines format (uses hjson)",
            component_class=cls
        )
        usage.def_syntax('')
        usage.def_example(expr_tokens=["{hello: 'world!'}"], expect="{hello: 'world!'}")
        usage.def_example(expr_tokens=["[{id:1, dir:'up'},{id:2, dir:'down'}]"], expect="[{id:1, dir:'up'}, {id:2, dir:'down'}]")
        return usage

    def __init__(self, inline_expr):
        self.num_recs = 0
        try:
            obj = hjson.loads(inline_expr)
        except HjsonDecodeError:
            raise TokenError('incorrect hjson line syntax')

        if isinstance(obj, dict):
            self.records = [obj]
        elif isinstance(obj, list):
            self.records = obj
        else:
            raise TokenError(f'"{inline_expr}"')

    def __iter__(self):
        for raw in self.records:
            yield to_builtin(raw)

    @classmethod
    def is_inline(cls, token):
        if len(token) < 2:
            return False
        return (token[0], token[-1]) in {('{', '}'), ('[', ']')}
