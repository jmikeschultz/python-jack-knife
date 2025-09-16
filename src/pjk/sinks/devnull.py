# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

# djk/sinks/devnull.py

from pjk.base import Sink, Source, ParsedToken, Usage

class DevNullSink(Sink):
    @classmethod
    def usage(cls):
        usage = Usage(
            name='devnull',
            desc='Consume all input records and discard them (debug/testing)',
            component_class=cls
        )
        usage.def_example(expr_tokens=['{id:1}', 'devnull'], expect=None)
        return usage

    def __init__(self, ptok: ParsedToken, usage: Usage):
        super().__init__(ptok, usage)
        self.count = 0

    def process(self):
        for record in self.input:
            self.count += 1

    def print_info(self):
        print(f'num_recs:{self.count}')

    def deep_copy(self):
        return None  # until we implement cross-thread coordination
