# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

# djk/pipes/join.py

from djk.base import Pipe, Usage, UsageError, ParsedToken, KeyedSource

class JoinPipe(Pipe):
    arity = 2  # left = record stream, right = KeyedSource

    @classmethod
    def usage(cls):
        usage = Usage(
            name='join',
            desc="Join records against a keyed source on shared fields"
        )
        usage.def_arg(
            name='mode',
            usage="'left', 'inner', or 'outer' join behavior",
            valid_values={'left', 'inner', 'outer'}
        )
        return usage

    def __init__(self, ptok: ParsedToken, usage: Usage):
        super().__init__(ptok)

        self.mode = usage.get_arg('mode')
        self.left = None
        self.right = None
        self._pending_right = None
        self._check_right = False

    def reset(self):
        self._pending_right = None
        self._check_right = False

    def __iter__(self):
        if not isinstance(self.right, KeyedSource):
            raise UsageError("right input to join must be a KeyedSource")

        for left_rec in self.left:
            match = self.right.lookup(left_rec)

            if match is not None:
                merged = dict(left_rec)
                merged.update(match)
                yield merged
            elif self.mode == "left":
                yield left_rec
            elif self.mode == "outer":
                continue  # collect unmatched right side later
            elif self.mode == "inner":
                continue

        if self.mode == "outer":
            for right_rec in self.right.get_unlookedup_records():
                yield right_rec
