# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

# djk/pipes/where.py

from djk.base import Pipe, ParsedToken, NoBindUsage, Usage, UsageError
from djk.common import SafeNamespace

class WherePipe(Pipe):
    @classmethod
    def usage(cls):
        usage = NoBindUsage(
            name='where',
            desc="Filter records using a Python expression over fields",
            component_class=cls
        )
        usage.def_arg(name='expr', usage='Python expression using \'f.<field>\' syntax')
        usage.def_example(expr_tokens=["[{size:1}, {size:5}, {size:10}]", "where:f.size >= 5"], expect="[{size:5}, {size:10}]")
        usage.def_example(expr_tokens=["[{color:'blue'}, {color:'red'}, {color:'black'}]", "where:f.color.startswith('bl')"], expect="[{color:'blue'}, {color:'black'}]")
        return usage

    def __init__(self, ptok: ParsedToken, usage: Usage):
        super().__init__(ptok, usage)
        self.expr = ptok.whole_token.split(':', 1)[1]
        try:
            self.code = compile(self.expr, '<where>', 'eval')
        except Exception as e:
            raise UsageError(f"Invalid where expression: {self.expr}") from e

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

    def deep_copy(self):
        source_clone = self.left.deep_copy()
        if source_clone:
            pipe = WherePipe(self.ptok, self.usage)
            pipe.add_source(source_clone)
            return pipe
        else:
            return None
