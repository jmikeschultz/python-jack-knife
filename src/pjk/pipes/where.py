# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

# djk/pipes/where.py

from pjk.usage import NoBindUsage
from pjk.components import Pipe, DeepCopyPipe
from pjk.usage import ParsedToken, Usage, UsageError
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

    def __init__(self, ptok: ParsedToken, usage: Usage, root = None):
        super().__init__(ptok, usage, root)
        self.expr = ptok.whole_token.split(':', 1)[1]

        self.inrecs = papi.get_counter(self, var_label='recs_in', display=False)
        self.outrecs = papi.get_percentage_counter(self, var_label='recs_out', denom_counter=self.inrecs)
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

    