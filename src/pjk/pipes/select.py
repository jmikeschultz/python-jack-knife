# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

# djk/select_pipe.py

from pjk.base import Pipe, Usage, ParsedToken, UsageError

class SelectFields(Pipe):
    deep_copyable: bool = True

    @classmethod
    def usage(cls):
        usage = Usage(
            name='sel',
            desc='Keep only the specified fields from each record',
            component_class=cls
        )
        usage.def_arg(name='fields', usage='Comma-separated list of fields to retain')
        usage.def_example(expr_tokens=["{id:1, dir:'up', color:'blue'}", 'sel:id,color'], expect="id: 1, color:'blue'")
        return usage

    def __init__(self, ptok: ParsedToken, usage: Usage):
        super().__init__(ptok)

        arg_string = usage.get_arg('fields')
        if not arg_string:
            raise UsageError("select:<f1,f2,...> requires at least one field")

        self.keep_fields = {f.strip() for f in arg_string.split(',') if f.strip()}
        if not self.keep_fields:
            raise UsageError("select must include at least one valid field name")

    def reset(self):
        pass  # stateless

    def __iter__(self):
        for record in self.left:
            keys = list(record.keys())
            for k in keys:
                if k not in self.keep_fields:
                    record.pop(k)
            yield record
