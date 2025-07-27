# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

# djk/pipes/sort.py

from djk.base import Pipe, ParsedToken, Usage, UsageError

class SortPipe(Pipe):
    @classmethod
    def usage(cls):
        usage = Usage(
            name='sort',
            desc="Sort records by a single field, using +field or -field syntax"
        )
        usage.def_arg(name='field', usage="Prefix '+' for ascending, '-' for descending field name")
        return usage

    def __init__(self, ptok: ParsedToken, usage: Usage):
        super().__init__(ptok)

        arg_string = usage.get_arg('field')
        if not arg_string:
            raise UsageError("sort:[+-]<field> requires direction and field name")

        if arg_string.startswith("-"):
            self.field = arg_string[1:]
            self.reverse = True
        elif arg_string.startswith("+"):
            self.field = arg_string[1:]
            self.reverse = False
        else:
            raise UsageError("sort:[+-]<field> must start with '+' or '-'")

        self._buffer = None
        self._index = 0

    def reset(self):
        self._buffer = None
        self._index = 0

    def __iter__(self):
        if self._buffer is None:
            self._buffer = list(self.left)

            self._buffer.sort(
                key=lambda r: (r.get(self.field) is None, r.get(self.field)),
                reverse=self.reverse
            )

        while self._index < len(self._buffer):
            yield self._buffer[self._index]
            self._index += 1
