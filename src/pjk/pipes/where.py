# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

# djk/pipes/where.py

from pjk.base import Pipe, ParsedToken, NoBindUsage, Usage, UsageError, DeepCopyPipe
from pjk.common import SafeNamespace
from pjk.progress import papi

class WherePipe(DeepCopyPipe):
    @classmethod
    def usage(cls):
        u = NoBindUsage(
            name='where',
            desc="Filter records using a Python expression over fields",
            component_class=cls
        )
        u.def_arg(name='expr', usage='Python expression using \'f.<field>\' syntax')
        u.def_example(expr_tokens=["[{size:1}, {size:5}, {size:10}]", "where:f.size >= 5"], expect="[{size:5}, {size:10}]")
        u.def_example(expr_tokens=["[{color:'blue'}, {color:'red'}, {color:'black'}]", "where:f.color.startswith('bl')"], expect="[{color:'blue'}, {color:'black'}]")
        return u

    def __init__(self, ptok: ParsedToken, usage: Usage):
        super().__init__(ptok, usage)
        self.expr = ptok.whole_token.split(':', 1)[1]
        self.inrecs = papi.get_counter(usage.name, var_label='in_recs')
        self.outrecs = papi.get_counter(usage.name, var_label='out_recs')
        try:
            self.code = compile(self.expr, '<where>', 'eval')
        except Exception as e:
            raise UsageError(f"Invalid where expression: {self.expr}") from e

    def reset(self):
        pass  # stateless

    def __iter__(self):
        for record in self.left:
            self.inrecs.increment()
            f = SafeNamespace(record)
            try:
                if eval(self.code, {}, {'f': f}):
                    self.outrecs.increment()
                    yield record
            except Exception:
                continue  # ignore eval errors

