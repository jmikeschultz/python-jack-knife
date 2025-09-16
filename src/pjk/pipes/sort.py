# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

# djk/pipes/sort.py

from pjk.base import Pipe, ParsedToken, Usage, UsageError

class SortPipe(Pipe):
    @classmethod
    def usage(cls):
        usage = Usage(
            name='sort',
            desc="Sort records by a single field (records with missing field sort last).",
            component_class=cls
        )
        usage.def_arg(name='field', usage="+name or -name for ascending or decending sort by field 'name'.")
        usage.def_example(expr_tokens=["[{id:17}, {id:10}, {id:1}]", 'sort:+id'], expect="[{id:1}, {id:10}, {id:17}]")
        usage.def_example(expr_tokens=["[{id:1}, {color:'blue'}, {color:'green'}]", 'sort:-color'], expect="[{color:'green'}, {color:'blue'}, {id:1}]")
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

            # Partition into records with and without the sort field
            present = [r for r in self._buffer if r.get(self.field) is not None]
            missing = [r for r in self._buffer if r.get(self.field) is None]

            present.sort(
                key=lambda r: r.get(self.field),
                reverse=self.reverse
            )

            self._buffer = present + missing  # always push missing to the end

        while self._index < len(self._buffer):
            yield self._buffer[self._index]
            self._index += 1


