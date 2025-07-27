# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

# djk/pipes/where.py

from djk.base import Pipe, ParsedToken, Usage, UsageError
from djk.pipes.common import SafeNamespace

class WherePipe(Pipe):
    @classmethod
    def usage(cls):
        usage = Usage(
            name='grep',
            desc="Filter records using a Python expression over fields, e.g. f.price > 100"
        )
        usage.def_arg(name='expr', usage='Python expression using `f.<field>` syntax')
        return usage

    def __init__(self, ptok: ParsedToken, usage: Usage):
        super().__init__(ptok)
        self.expr = usage.get_arg('expr').strip()
        try:
            self.code = compile(self.expr, '<grep>', 'eval')
        except Exception as e:
            raise UsageError(f"Invalid grep expression: {self.expr}") from e

    def reset(self):
        pass  # stateless

    def __iter__(self):
        for record in self.left:
            f = SafeNamespace(record)
            try:
                if eval(self.code, {}, {'f': f}):
                    yield record
            except Exception:
                continue  # ignore eval errors
