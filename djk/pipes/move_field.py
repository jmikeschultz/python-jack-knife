# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

# djk/pipes/move_field.py

from djk.base import Pipe, ParsedToken, Usage

class MoveField(Pipe):
    @classmethod
    def usage(cls):
        usage = Usage(
            name='move',
            desc='Move one field to another key in the record',
        )
        usage.def_arg(name='src', usage='Source field name')
        usage.def_arg(name='dst', usage='Destination field name')
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
