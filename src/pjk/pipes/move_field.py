# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

# djk/pipes/move_field.py

from pjk.base import Pipe, ParsedToken, Usage

class MoveField(Pipe):
    @classmethod
    def usage(cls):
        usage = Usage(
            name='as',
            desc='Move one field to another key in the record',
            component_class=cls
        )
        usage.def_arg(name='src', usage='Source field name')
        usage.def_arg(name='dst', usage='Destination field name')
        usage.def_example(expr_tokens=['{up:1}', 'as:up:down'], expect="{down:1}")

        return usage

    def __init__(self, ptok: ParsedToken, usage: Usage):
        super().__init__(ptok)
        self.src = usage.get_arg('src')
        self.dst = usage.get_arg('dst')
        self.count = 0

    def reset(self):
        self.count = 0

    def __iter__(self):
        for record in self.left:
            self.count += 1
            if self.src in record:
                record[self.dst] = record.pop(self.src)
            yield record
