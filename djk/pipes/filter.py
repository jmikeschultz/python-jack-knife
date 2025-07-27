# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

from djk.base import Pipe, Usage, UsageError, ParsedToken, KeyedSource

class FilterPipe(Pipe):
    arity = 2  # left = record stream, right = keyed source

    @classmethod
    def usage(cls):
        usage = Usage(
            name="filter",
            desc="Filters left records based on presence in right keyed source"
        )
        usage.def_arg("mode", "'+' to include matches, '-' to exclude matches",
                      valid_values={'+', '-'})
        return usage

    def __init__(self, ptok: ParsedToken, usage: Usage):
        super().__init__(ptok)
        self.mode = usage.get_arg('mode')
        self.left = None
        self.right = None

    def reset(self):
        pass  # stateless

    def __iter__(self):
        if not isinstance(self.right, KeyedSource):
            raise UsageError("Right input to filter must be a KeyedSource")

        for record in self.left:
            match = self.right.lookup(record)
            exists = match is not None

            if (self.mode == "+" and exists) or (self.mode == "-" and not exists):
                yield record
