# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

# djk/pipes/remove_field.py

from pjk.base import Pipe, ParsedToken, Usage, UsageError

class RemoveField(Pipe):
    @classmethod
    def usage(cls):
        usage = Usage(
            name='drop',
            desc='Remove one or more fields from each record',
            component_class=cls
        )
        usage.def_arg(name='fields', usage='Comma-separated list of field names to drop')
        usage.def_example(expr_tokens=["{id:1, dir:'up', color:'blue'}", 'drop:id,color'], expect="dir: 'up'")
        return usage

    def __init__(self, ptok: ParsedToken, usage: Usage):
        super().__init__(ptok)
        arg_string = usage.get_arg('fields')
        self.fields = [f.strip() for f in arg_string.split(',') if f.strip()]
        if not self.fields:
            raise UsageError("rm must include at least one valid field name")
        self.count = 0

    def reset(self):
        self.count = 0

    def __iter__(self):
        for record in self.left:
            self.count += 1
            for field in self.fields:
                record.pop(field, None)
            yield record
