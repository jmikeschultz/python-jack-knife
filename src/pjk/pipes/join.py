# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

# djk/pipes/join.py

from pjk.base import Pipe, Usage, UsageError, ParsedToken, KeyedSource
from pjk.progress import papi

class JoinPipe(Pipe):
    arity = 2  # left = record stream, right = KeyedSource

    @classmethod
    def usage(cls):
        usage = Usage(
            name='join',
            desc="Join records against a keyed source on shared fields",
            component_class=cls
        )
        usage.def_arg(
            name='mode',
            usage="'left', 'inner', or 'outer' join behavior",
            valid_values={'left', 'inner', 'outer'}
        )
        usage.def_syntax("pjk <left_source> <map_source> [mapby|groupby]:<key> join:<mode> <sink>")

        usage.def_example(expr_tokens=
        [
            "[{color:'blue'},{color:'green'}]",
            "[{color:'blue', price:50}, {color:'red', price:20}]",
            'mapby:color',
            "join:left"
        ],
        expect="[{color:'blue', price:50}, {color:'green'}]")
        usage.def_example(expr_tokens=
        [
            "[{color:'blue'},{color:'green'}]",
            "[{color:'blue', price:50}, {color:'red', price:20}]",
            'mapby:color',
            "join:inner"
        ],
        expect="[{color:'blue', price:50}]")

        usage.def_example(expr_tokens=
        [
            "[{color:'blue'},{color:'green'}]",
            "[{color:'blue', price:50}, {color:'red', price:20}]",
            'mapby:color',
            "join:outer"
        ],
        expect="[{color:'blue', price:50}, {color:'green'}, {color:'red', price: 20}]")
        return usage

    def __init__(self, ptok: ParsedToken, usage: Usage):
        super().__init__(ptok)

        self.mode = usage.get_arg('mode')
        self.left = None
        self.right = None
        self._pending_right = None
        self._check_right = False

        self.recs_in = papi.get_counter(self, None) # don't display
        self.matches = papi.get_percentage_counter(self, 'matches', self.recs_in)
        self.recs_out = papi.get_counter(self, 'recs_out')

    def reset(self):
        self._pending_right = None
        self._check_right = False

    def __iter__(self):
        if not isinstance(self.right, KeyedSource):
            raise UsageError("right source must be a KeyedSource")

        for left_rec in self.left:
            self.recs_in.increment()
            match = self.right.lookup(left_rec)

            if match is not None:
                self.matches.increment()
                self.recs_out.increment()
                merged = dict(left_rec)
                merged.update(match)
                yield merged
            elif self.mode == "left":
                self.recs_out.increment()
                yield left_rec
            elif self.mode == "outer":
                self.recs_out.increment()
                yield left_rec 
            elif self.mode == "inner":
                continue

        if self.mode == "outer":
            for right_rec in self.right.get_unlookedup_records():
                self.recs_out.increment()
                yield right_rec
