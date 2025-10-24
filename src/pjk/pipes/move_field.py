# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

# djk/pipes/move_field.py

from pjk.components import Pipe
from pjk.usage import ParsedToken, Usage, TokenError
from pjk.common import is_valid_field_name

class MoveField(Pipe):
    @classmethod
    def usage(cls):
        u = Usage(
            name='as',
            desc='rename a field in the record',
            component_class=cls
        )
        u.def_arg(name='src', usage='Source field name')
        u.def_arg(name='dst', usage='Destination field name')
        u.def_example(expr_tokens=['{up:1}', 'as:up:down'], expect="{down:1}")

        return u

    def __init__(self, ptok: ParsedToken, usage: Usage):
        super().__init__(ptok, usage)
        self.src = usage.get_arg('src')
        self.dst = usage.get_arg('dst')

        if not is_valid_field_name(self.dst) or not is_valid_field_name(self.src):
            raise TokenError('field names only allow letters, numbers (non-initially) and underbar')

    def __iter__(self):
        for record in self.left:
            if self.src in record:
                record[self.dst] = record.pop(self.src)
            yield record
