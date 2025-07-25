# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

# djk/pipes/sort.py

from typing import Optional
from djk.base import Pipe, ParsedToken, Usage, UsageError

class SortPipe(Pipe):
    def __init__(self, ptok: ParsedToken, bound_usage: Usage):
        super().__init__(ptok)

        arg_string = ptok.get_arg(0)

        if not arg_string:
            raise UsageError("sort:[+-]<field> requires direction and field name")

        if arg_string.startswith("-"):
            self.field = arg_string[1:]
            self.reverse = True
        elif arg_string.startswith("+"):
            self.field = arg_string[1:]
            self.reverse = False
        else:
            raise UsageError("sort:[+-]<field> requires direction and field name")

        self._buffer = None
        self._index = 0

    def next(self) -> Optional[dict]:
        if self._buffer is None:
            self._buffer = []
            while True:
                rec = self.inputs[0].next()
                if rec is None:
                    break
                self._buffer.append(rec)

            # Sort by field, treating missing values as None (sorts to end)
            self._buffer.sort(
                key=lambda r: (r.get(self.field) is None, r.get(self.field)),
                reverse=self.reverse
            )

        if self._index >= len(self._buffer):
            return None

        out = self._buffer[self._index]
        self._index += 1
        return out
